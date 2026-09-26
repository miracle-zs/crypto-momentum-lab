"""Build FrozenDecisionInputs from the live daemon runtime context.

Never invents READY positions or cash. Missing or incomplete account facts
yield ``None`` so the decision filter can fail closed.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import structlog

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.decision.decision_engine import (
    DecisionInput,
    DecisionResult,
    FrozenDecisionInputs,
    PolicyState,
    build_decision_trace,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    FactCoverageStatus,
    PositionHealthStatus,
    PositionKey,
    PositionLedgerBatch,
    PositionView,
    compose_fact_coverage,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.revision_models import DecisionTrace
from crypto_momentum_lab.domain.risk import StrategyLiveState
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext


def _cash_balance(context: LiveDaemonRuntimeContext) -> Decimal | None:
    snapshot = context.account_snapshot
    if snapshot is None:
        return None
    usdt = [
        b.wallet_balance
        for b in snapshot.balances
        if getattr(b, "asset", "").upper() in {"USDT", "USDC", "BUSD"}
    ]
    if not usdt:
        return None
    total = sum(usdt, start=Decimal("0"))
    return total if total >= Decimal("0") else None


def _position_batches(
    context: LiveDaemonRuntimeContext,
    symbol: str,
) -> tuple[PositionLedgerBatch, ...]:
    batches: list[PositionLedgerBatch] = []
    for pos in context.managed_positions:
        if pos.symbol != symbol:
            continue
        if pos.batches:
            for b in pos.batches:
                batches.append(
                    PositionLedgerBatch(
                        batch_id=b.batch_id,
                        episode_id=f"ep_{symbol}_{pos.opened_at.date()}",
                        quantity=b.quantity,
                        original_quantity=b.quantity,
                        entry_price=b.entry_price,
                        opened_at=b.opened_at,
                    )
                )
        else:
            batches.append(
                PositionLedgerBatch(
                    batch_id=pos.batch_id or f"live_{symbol}_{pos.opened_at.date()}",
                    episode_id=f"ep_{symbol}_{pos.opened_at.date()}",
                    quantity=pos.quantity,
                    original_quantity=pos.quantity,
                    entry_price=pos.entry_price,
                    opened_at=pos.opened_at,
                )
            )
    return tuple(batches)


def _snapshot_confirms_zero_position(
    context: LiveDaemonRuntimeContext,
    symbol: str,
    *,
    account_label: str,
) -> bool:
    """Use a current complete account snapshot to prove a symbol is flat.

    Fill cursors are needed to reconstruct an existing position. A symbol with
    no position or working order can instead be admitted from the account
    snapshot, allowing its first entry without weakening checks on open lots.
    """
    snapshot = getattr(context, "account_snapshot", None)
    observed_at = getattr(context, "account_observed_at", None)
    if (
        snapshot is None
        or getattr(context, "account_snapshot_version", None) is None
        or getattr(context, "account_state", None)
        != ExecutionAccountStatus.READY_READONLY
        or observed_at is None
        or snapshot.config.environment != "live"
        or snapshot.config.account_label != account_label
        or snapshot.config.observed_at != observed_at
    ):
        return False

    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol or getattr(context, "active_halts", False):
        return False
    if normalized_symbol in (
        getattr(context, "pending_position_symbols", frozenset())
        | getattr(context, "unmanaged_position_symbols", frozenset())
    ):
        return False
    if any(
        position.symbol.strip().upper() == normalized_symbol
        for position in context.managed_positions
    ):
        return False
    if any(
        position.symbol.strip().upper() == normalized_symbol
        and position.position_amt != 0
        for position in snapshot.positions
    ):
        return False
    if any(
        order.symbol.strip().upper() == normalized_symbol
        for order in snapshot.open_orders
    ):
        return False
    for order in getattr(context, "unresolved_orders", ()) or ():
        plan = getattr(order, "plan", None)
        order_symbol = str(getattr(plan, "symbol", "")).strip().upper()
        if order_symbol == normalized_symbol:
            return False
    return True


def frozen_decision_inputs_from_context(
    context: LiveDaemonRuntimeContext,
    state: MarketState15s,
    *,
    account_label: str,
    policy_state: PolicyState | None = None,
) -> FrozenDecisionInputs | None:
    """Derive frozen decision facts from real account context.

    Returns ``None`` when cash or account identity cannot be proven so the
    filter fails closed instead of trading on synthetic facts.
    """
    cash = _cash_balance(context)
    if cash is None:
        return None

    pos_key = PositionKey(
        environment="live",
        account_label=account_label,
        symbol=state.symbol,
        position_side=FuturesPositionSide.BOTH,
    )
    batches = _position_batches(context, state.symbol)

    pending_or_unmanaged = state.symbol in (
        getattr(context, "pending_position_symbols", frozenset())
        | getattr(context, "unmanaged_position_symbols", frozenset())
    )
    evidence = getattr(context, "coverage_by_symbol", {}).get(state.symbol)
    zero_position_snapshot_confirmed = _snapshot_confirms_zero_position(
        context,
        state.symbol,
        account_label=account_label,
    )
    coverage = (
        compose_fact_coverage(
            evidence,
            start=state.bucket_start,
            end=state.bucket_end,
        )
        if evidence is not None
        else None
    )
    if context.strategy_state != StrategyLiveState.ACTIVE:
        health = PositionHealthStatus.CATCHING_UP
    elif context.active_halts or pending_or_unmanaged:
        health = PositionHealthStatus.INCOMPLETE
    elif (
        coverage is None or coverage.status != FactCoverageStatus.CONFIRMED
    ) and not zero_position_snapshot_confirmed:
        # Without fill coverage or an authoritative empty snapshot, the
        # position facts are not authoritative.
        health = PositionHealthStatus.CATCHING_UP
    else:
        health = PositionHealthStatus.READY

    projection_version = (
        f"pv_{account_label}_{state.symbol}_{context.account_snapshot_version}"
        if context.account_snapshot_version is not None
        else f"pv_{account_label}_{state.symbol}_unversioned"
    )
    risk_version = getattr(context.risk_config, "config_hash", None) or (
        f"risk_{context.risk_config.created_at.isoformat()}"
    )
    universe_version = (
        f"univ_{context.context_epoch}"
        if context.context_epoch is not None
        else "univ_live"
    )

    pos_view = PositionView(
        key=pos_key,
        projection_version=projection_version,
        input_revision=int(context.context_epoch or 0),
        event_cut=state.bucket_end,
        policy_version="live",
        schema_version="v1",
        coverage=coverage,
        active_episode=None,
        batches=batches,
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        health_status=health,
        zero_position_snapshot_confirmed=zero_position_snapshot_confirmed,
    )
    return FrozenDecisionInputs(
        position_view=pos_view,
        cash_balance=cash,
        policy_state=policy_state or PolicyState(),
        universe_version=universe_version,
        risk_config_version=str(risk_version),
    )


log = structlog.get_logger()


class LiveDecisionFactSource:
    """Mutable holder the market loop updates before each filter call."""

    def __init__(
        self,
        account_label: str,
        trace_repository: Any | None = None,
        strategy_name: str = "orderflow_impulse",
    ) -> None:
        self._account_label = account_label
        self._context: LiveDaemonRuntimeContext | None = None
        self._policy_state = PolicyState()
        self._trace_repository = trace_repository
        self._strategy_name = strategy_name

    def bind_context(self, context: LiveDaemonRuntimeContext | None) -> None:
        self._context = context

    def set_policy_state(self, state: PolicyState) -> None:
        self._policy_state = state

    def record_trace(self, trace: DecisionTrace) -> None:
        """Saves a trace via the trace repository asynchronously."""
        if self._trace_repository is None:
            return
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._safe_persist_trace(trace))
        except RuntimeError:
            pass

    async def _safe_persist_trace(self, trace: DecisionTrace) -> None:
        try:
            if self._trace_repository is not None:
                await self._trace_repository.save_decision_trace(trace)
        except Exception as exc:
            log.warning(
                "async_save_decision_trace_failed",
                decision_id=trace.decision_id,
                error=str(exc),
            )

    def on_decision_result(
        self,
        result: DecisionResult,
        decision_input: DecisionInput | None = None,
    ) -> None:
        self.set_policy_state(result.next_policy_state)
        if self._trace_repository is not None and decision_input is not None:
            trace = build_decision_trace(
                result,
                decision_input,
                strategy_name=self._strategy_name,
                account_label=self._account_label,
            )
            self.record_trace(trace)

    def build(self, state: MarketState15s) -> FrozenDecisionInputs | None:
        if self._context is None:
            return None
        return frozen_decision_inputs_from_context(
            self._context,
            state,
            account_label=self._account_label,
            policy_state=self._policy_state,
        )
