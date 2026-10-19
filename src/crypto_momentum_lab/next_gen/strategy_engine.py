from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    RunMode,
    StrategyDecision,
    StrategyRunIdentity,
)
from crypto_momentum_lab.domain.strategy.entry_candidate import prepare_entry_candidate
from crypto_momentum_lab.live_rollout.runtime_config import _live_strategy_config
from crypto_momentum_lab.live_rollout.runtime_manifest import LiveRuntimeAccount
from crypto_momentum_lab.live_rollout.volume import QuoteVolume24hProvider
from crypto_momentum_lab.next_gen.client import BinanceOrderRejectedError
from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.models import generate_batch_id
from crypto_momentum_lab.next_gen.symbol_rules import SymbolRulesRegistry
from crypto_momentum_lab.strategies.registry import build_runtime_strategy

log = structlog.get_logger("next_gen_strategy")


class MomentumStrategyEngine:
    """Adapt the existing Order Flow Impulse decisions to the account executor."""

    def __init__(
        self,
        state: AccountState,
        order_executor: OrderExecutor,
        account_config: LiveRuntimeAccount,
        *,
        market_environment: str,
        symbols: list[str] | None = None,
        symbol_rules: SymbolRulesRegistry,
    ) -> None:
        self.state = state
        self.order_executor = order_executor
        self.account_config = account_config
        self.market_environment = market_environment
        self.symbols = list(symbols) if symbols is not None else []
        self._explicit_symbols = symbols is not None
        self.symbol_rules = symbol_rules
        self.quote_volume_provider: QuoteVolume24hProvider | None = None
        self.market_connected = False
        self.market_data_observation: dict[str, Any] | None = None
        self.latest_prices = state.latest_prices
        self._rejected_entry_until: dict[tuple[str, str], datetime] = {}
        self._last_state_at: dict[str, datetime] = {}
        inputs = account_config.strategy_inputs
        if inputs.entry_order_type is not EntryType.LIMIT:
            raise ValueError("NextGen requires configured LIMIT entries")
        if inputs.require_price_above_ema5 or inputs.require_price_above_ema10:
            raise ValueError("NextGen EMA admission is not implemented")
        identity = StrategyRunIdentity(
            run_id=account_config.session_id,
            strategy_name=account_config.strategy,
            strategy_version="v0",
            config_hash=account_config.strategy_config_hash,
            run_mode=RunMode.LIVE,
            code_commit=account_config.image_commit,
            created_at=datetime.now(UTC),
            source_paths=("MarketStateHub",),
        )
        self.runtime = build_runtime_strategy(
            account_config.strategy,
            config=_live_strategy_config(inputs.profile),
            identity=identity,
        )

    @property
    def is_ready(self) -> bool:
        if not self.market_connected:
            return False
        checkpoint = self.runtime.checkpoint(include_market_state_buffers=False)
        required = self.runtime.required_data().warmup_buckets
        now = datetime.now(UTC)
        return any(
            depth >= required
            and -5 <= (now - self._last_state_at[symbol]).total_seconds() <= 45
            for symbol, depth in checkpoint.warmup_buckets_by_symbol.items()
            if symbol in self._last_state_at
        )

    async def warmup_market_data(self, rest_client: Any, db: Any) -> None:
        info = await rest_client.fetch_exchange_info()
        self.symbol_rules.update_from_exchange_info(info)
        tradable = {
            row["symbol"]
            for row in info["symbols"]
            if row["status"] == "TRADING"
            and row["quoteAsset"] == "USDT"
            and row["contractType"] == "PERPETUAL"
        }
        if self._explicit_symbols:
            if not set(self.symbols) <= tradable:
                raise ValueError("Requested symbols are not tradable USDT perpetuals")
        else:
            self.symbols = sorted(
                symbol for symbol in tradable if self.symbol_rules.has_filter(symbol)
            )
        for symbol in self.symbols:
            self.symbol_rules.get_filter(symbol)
        history = await db.async_get_historical_market_states(
            self.market_environment,
            self.symbols,
            self.runtime.required_data().warmup_buckets,
        )
        for market_state in history:
            self.runtime.warm_market_state(market_state)
            self._last_state_at[market_state.symbol] = market_state.bucket_end
            if market_state.effective_price is not None:
                self.latest_prices[market_state.symbol] = market_state.effective_price

    async def on_market_state(self, market_state: MarketState15s) -> list[str]:
        if market_state.environment != self.market_environment:
            raise ValueError("Unexpected market-state environment")
        symbol = market_state.symbol
        if symbol not in self.symbols:
            return []
        previous = self._last_state_at.get(symbol)
        if previous is not None and market_state.bucket_end <= previous:
            return []
        received_at = datetime.now(UTC)
        self.market_data_observation = {
            "bucket_end": market_state.bucket_end.isoformat(),
            "received_at": received_at.isoformat(),
            "delay_ms": max(
                0.0, (received_at - market_state.bucket_end).total_seconds() * 1000
            ),
        }
        if not market_state.data_complete:
            self.runtime.reset_symbol(symbol)
            self._last_state_at[symbol] = market_state.bucket_end
            return []
        if (
            previous is not None
            and (market_state.bucket_end - previous).total_seconds()
            > self.runtime.required_data().max_gap_seconds
        ):
            self.runtime.reset_symbol(symbol)
        self._last_state_at[symbol] = market_state.bucket_end
        if market_state.effective_price is not None:
            self.latest_prices[symbol] = market_state.effective_price
        decision = self.runtime.on_market_state(market_state)
        now = datetime.now(UTC)
        age = (now - market_state.bucket_end).total_seconds()
        if not self.market_connected or age < -5 or age > 45:
            return []
        filter_context: dict[str, Any] = {
            "entry_enabled": self.state.can_submit_entry_order(),
            "entry_long_only": self.account_config.execution_inputs.entry_long_only,
            "candidate_filter_results": {},
            "gate_reasons": [],
        }
        try:
            return await self._submit_candidates(
                decision, market_state, now, filter_context=filter_context
            )
        finally:
            # Observations follow submission; their failure must not change trading.
            repository = self.state.db
            for signal in decision.signals:
                recorded_at = datetime.now(UTC)
                try:
                    context = dict(filter_context)
                    context["candidate_filter_results"] = {
                        key: value
                        for key, value in filter_context[
                            "candidate_filter_results"
                        ].items()
                        if value["signal_id"] == signal.signal_id
                    }
                    if repository is None:
                        log.warning(
                            "entry_signal_not_persisted_without_repository",
                            signal_id=signal.signal_id,
                        )
                        continue
                    context["universe"] = await repository.async_get_signal_universe(
                        market_state.bucket_end, signal.symbol
                    )
                    volume = (
                        None
                        if self.quote_volume_provider is None
                        else self.quote_volume_provider.snapshot(
                            signal.symbol, as_of=now
                        )
                    )
                    await repository.async_save_entry_signal(
                        self.state.environment,
                        self.state.account_label,
                        recorded_at,
                        {
                            "signal_id": signal.signal_id,
                            "signal_kind": "strategy_signal",
                            "strategy_name": signal.strategy_name,
                            "strategy_version": signal.strategy_version,
                            "config_hash": signal.config_hash,
                            "code_commit": self.account_config.image_commit,
                            "symbol": signal.symbol,
                            "side": signal.side.value,
                            "detected_at": signal.detected_at.isoformat(),
                            "source_state_at": signal.source_state_at.isoformat(),
                            "recorded_at": recorded_at.isoformat(),
                            "reason": signal.reason,
                            "filter_context": context,
                            "quote_volume_24h": None
                            if volume is None
                            else str(volume.quote_volume),
                            "quote_volume_24h_quote_asset": None
                            if volume is None
                            else volume.quote_asset,
                            "quote_volume_24h_source_at": None
                            if volume is None
                            else volume.source_at.isoformat(),
                            "features": signal.features,
                            "reference_prices": signal.reference_prices,
                        },
                    )
                except Exception:
                    log.exception(
                        "entry_signal_record_failed", signal_id=signal.signal_id
                    )

    async def _submit_candidates(
        self,
        decision: StrategyDecision,
        market_state: MarketState15s,
        now: datetime,
        *,
        filter_context: dict[str, Any],
    ) -> list[str]:
        symbol = market_state.symbol
        submitted = []
        repository = self.state.db
        if repository is None:
            raise RuntimeError("Strategy execution requires a durable repository")
        for raw_candidate in decision.candidates:
            result: dict[str, Any] = {
                "passed": False,
                "rejection_reason": "submission_failed",
            }
            result["signal_id"] = raw_candidate.signal_id
            filter_context["candidate_filter_results"][raw_candidate.candidate_id] = (
                result
            )
            self.state.entry_halted = await repository.async_has_active_halt(
                self.state.environment,
                self.state.account_label,
            )
            if not self.state.can_submit_entry_order():
                reason = (
                    "scheduled_risk_window"
                    if self.state.scheduled_entry_blocked()
                    else "entry_gate_closed"
                )
                result["rejection_reason"] = reason
                filter_context["entry_enabled"] = False
                filter_context["gate_reasons"].append(reason)
                break
            execution = self.account_config.execution_inputs
            if execution.entry_long_only and raw_candidate.side.value != "long":
                result["rejection_reason"] = "long_only"
                continue
            candidate = prepare_entry_candidate(
                raw_candidate,
                state=market_state,
                execution_now=now,
                entry_order_type=self.account_config.strategy_inputs.entry_order_type,
                limit_ttl_seconds=self.account_config.strategy_inputs.entry_limit_ttl_seconds,
            )
            eligible = await repository.async_get_entry_symbols(
                market_state.bucket_end,
                self.account_config.strategy_inputs.entry_positive_gainer_top_count,
            )
            if symbol not in eligible:
                result["rejection_reason"] = "outside_positive_gainer_top"
                continue
            side = candidate.side.value.upper()
            reject_until = self._rejected_entry_until.get((symbol, side))
            if reject_until is not None and now < reject_until:
                result["rejection_reason"] = "exchange_reject_cooldown"
                continue
            key = (self.state.account_label, symbol, side)
            if key in self.state.active_entry_orders:
                result["rejection_reason"] = "active_entry_order"
                continue
            price = candidate.limit_price
            if price is None or price <= 0:
                raise ValueError("A limit entry requires a positive prepared price")
            quantity = self.symbol_rules.round_quantity(
                symbol, execution.target_notional / price
            )
            price = self.symbol_rules.round_price(symbol, price)
            reference_price = market_state.mark_price or market_state.effective_price
            valid, reason = self.symbol_rules.validate_order(
                symbol, quantity, price, reference_price=reference_price
            )
            if not valid:
                result["rejection_reason"] = reason
                log.warning(
                    "entry_rejected_by_symbol_rules",
                    symbol=symbol,
                    reason=reason,
                    reference_price=None
                    if reference_price is None
                    else str(reference_price),
                )
                continue
            if not self.state.check_notional_limits(
                symbol, quantity, price, latest_prices=self.latest_prices
            ):
                result["rejection_reason"] = "notional_limit"
                continue
            batches = [
                b
                for b in self.state.batches.values()
                if b.symbol == symbol
                and b.position_side == side
                and b.status != "CLOSED"
                and b.quantity > 0
            ]
            limit = execution.max_concurrency_per_symbol
            latest = max(batches, key=lambda b: b.entry_bar_time, default=None)
            if latest is not None and latest.exit_order_submitted_at is None:
                batch_id = latest.batch_id
            else:
                if limit is not None and len(batches) >= limit:
                    result["rejection_reason"] = "batch_limit"
                    continue
                batch_id = generate_batch_id(symbol, side, self.state.account_label)
            order = self.state.create_entry_order(
                symbol, side, quantity, price, batch_id, order_type="LIMIT"
            )
            try:
                await self.order_executor.submit_order(order)
            except BinanceOrderRejectedError as error:
                reason = f"exchange_reject:{error.code}:{error.message}"
                result["rejection_reason"] = reason[:300]
                self._rejected_entry_until[(symbol, side)] = now + timedelta(seconds=30)
                log.warning(
                    "entry_order_rejected",
                    account=self.state.account_label,
                    symbol=symbol,
                    side=side,
                    code=error.code,
                    reason=error.message,
                )
                continue
            submitted.append(order.client_order_id)
            result.update(passed=True, rejection_reason=None)
        return submitted

    async def cancel_stale_entry_orders(self) -> None:
        now = datetime.now(UTC)
        ttl = self.account_config.strategy_inputs.entry_limit_ttl_seconds
        for order in list(self.state.orders.values()):
            if order.client_order_id.startswith("e_") and not order.is_terminal:
                if (now - order.created_at).total_seconds() >= ttl:
                    await self.order_executor.cancel_and_confirm(order)

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=1)
            except TimeoutError:
                await self.cancel_stale_entry_orders()
