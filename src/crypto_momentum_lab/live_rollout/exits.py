import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.execution import FuturesPositionSide, OrderExecutionPlan
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.strategy_runner.position_exit import (
    ClosedCandle15m,
    PositionExitMode,
    PositionExitPolicy,
    position_exit_reason,
)


class ClosedCandle15mLoader(Protocol):
    async def load_closed_candles(
        self,
        *,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> tuple[ClosedCandle15m, ...]: ...


class ThreadedClosedCandle15mLoader:
    def __init__(self, source: object) -> None:
        load = getattr(source, "load_closed_candles", None)
        if not callable(load):
            raise TypeError("source must provide load_closed_candles")
        self._load: Callable[..., tuple[ClosedCandle15m, ...]] = load

    async def load_closed_candles(
        self,
        *,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> tuple[ClosedCandle15m, ...]:
        return await asyncio.to_thread(
            self._load,
            symbol=symbol,
            start=start,
            end=end,
        )


@dataclass(frozen=True, slots=True)
class ManagedLivePosition:
    symbol: str
    side: StrategySide
    position_side: FuturesPositionSide
    quantity: Decimal
    entry_price: Decimal
    opened_at: datetime
    closing_order_filled: bool = False
    recovery_order_client_id: str | None = None
    recovery_order_created_at: datetime | None = None
    recovery_order_plan: OrderExecutionPlan | None = None

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if not isinstance(self.side, StrategySide):
            object.__setattr__(self, "side", StrategySide(self.side))
        if not isinstance(self.position_side, FuturesPositionSide):
            object.__setattr__(
                self,
                "position_side",
                FuturesPositionSide(self.position_side),
            )
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.entry_price <= 0:
            raise ValueError("entry_price must be positive")
        if self.opened_at.tzinfo is None or self.opened_at.utcoffset() is None:
            raise ValueError("opened_at must be timezone-aware")
        for value, field_name in (
            (self.recovery_order_created_at, "recovery_order_created_at"),
        ):
            if value is not None and (
                value.tzinfo is None or value.utcoffset() is None
            ):
                raise ValueError(f"{field_name} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class LiveExitConfig:
    run_id: str
    strategy_name: str
    strategy_version: str
    strategy_config_hash: str
    policy: PositionExitPolicy
    candidate_ttl_seconds: int = 60
    candle_grace_bars: int = 0
    candle_grace_profit_pct: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.run_id, "run_id"),
            (self.strategy_name, "strategy_name"),
            (self.strategy_version, "strategy_version"),
            (self.strategy_config_hash, "strategy_config_hash"),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        if self.candidate_ttl_seconds <= 0:
            raise ValueError("candidate_ttl_seconds must be positive")
        if self.candle_grace_bars < 0:
            raise ValueError("candle_grace_bars must not be negative")
        if self.candle_grace_bars > 0 and self.candle_grace_profit_pct <= 0:
            raise ValueError(
                "candle_grace_profit_pct must be positive when grace is enabled"
            )
        if self.candle_grace_profit_pct < 0 or self.candle_grace_profit_pct >= 1:
            raise ValueError("candle_grace_profit_pct must be in [0, 1)")


@dataclass(frozen=True, slots=True)
class LiveExitOrderRequest:
    candidate: OrderIntentCandidate
    quantity: Decimal


@dataclass(frozen=True, slots=True)
class LiveExitCancellationRequest:
    cancel_plan: OrderExecutionPlan
    fallback_candidate: OrderIntentCandidate
    fallback_quantity: Decimal


LiveExitRequest = LiveExitOrderRequest | LiveExitCancellationRequest


class LiveExitManager:
    def __init__(
        self,
        *,
        config: LiveExitConfig,
        candle_loader: ClosedCandle15mLoader | None = None,
    ) -> None:
        if (
            config.policy.mode is PositionExitMode.CANDLE_15M
            and candle_loader is None
        ):
            raise ValueError("candle_15m exits require a candle loader")
        self._config = config
        self._candles = candle_loader
        self._checked_until: dict[
            tuple[str, FuturesPositionSide, datetime], datetime
        ] = {}

    async def requests_for_state(
        self,
        state: MarketState15s,
        positions: tuple[ManagedLivePosition, ...],
    ) -> tuple[LiveExitRequest, ...]:
        requests: list[LiveExitRequest] = []
        for position in positions:
            if (
                position.symbol != state.symbol
                or position.closing_order_filled
                or state.bucket_end <= position.opened_at
            ):
                continue
            if (
                position.recovery_order_client_id is not None
                and position.recovery_order_created_at is not None
            ):
                timeout_at = position.recovery_order_created_at + timedelta(
                    minutes=15 * self._config.candle_grace_bars
                )
                if state.bucket_end >= timeout_at:
                    requests.append(
                        self._build_grace_timeout_request(
                            state=state,
                            position=position,
                            reference_price=(
                                _exit_mark_price(state, position.side)
                                or position.entry_price
                            ),
                        )
                    )
                continue
            request = await self._request_for_position(state, position)
            if request is not None:
                requests.append(request)
        return tuple(requests)

    async def _request_for_position(
        self,
        state: MarketState15s,
        position: ManagedLivePosition,
    ) -> LiveExitOrderRequest | None:
        mark_price = _exit_mark_price(state, position.side)
        if mark_price is None:
            return None
        if self._config.policy.mode is PositionExitMode.CANDLE_15M:
            candle_request = await self._candle_exit_request(
                state,
                position,
                mark_price,
            )
            if candle_request is not None:
                return candle_request
        reason = position_exit_reason(
            gross_return=_gross_return(position, mark_price),
            held_until=state.bucket_end,
            opened_at=position.opened_at,
            symbol=position.symbol,
            side=position.side,
            policy=self._config.policy,
            closed_candle=None,
        )
        if reason is None:
            return None
        return self._build_request(
            state=state,
            position=position,
            reason=reason,
            trigger_at=state.bucket_end,
            reference_price=mark_price,
        )

    async def _candle_exit_request(
        self,
        state: MarketState15s,
        position: ManagedLivePosition,
        mark_price: Decimal,
    ) -> LiveExitOrderRequest | None:
        if self._candles is None:
            raise AssertionError("candle loader was validated at construction")
        closed_boundary = _candle_start_15m(state.bucket_end)
        key = (position.symbol, position.position_side, position.opened_at)
        start = self._checked_until.get(key, _candle_start_15m(position.opened_at))
        if closed_boundary <= start:
            return None
        candles = await self._candles.load_closed_candles(
            symbol=position.symbol,
            start=start,
            end=closed_boundary,
        )
        for candle in candles:
            if candle.candle_end <= position.opened_at:
                self._checked_until[key] = candle.candle_end
                continue
            reason = position_exit_reason(
                gross_return=_gross_return(position, candle.close_price),
                held_until=candle.candle_end,
                opened_at=position.opened_at,
                symbol=position.symbol,
                side=position.side,
                policy=self._config.policy,
                closed_candle=candle,
            )
            if reason is not None:
                if (
                    self._config.candle_grace_bars > 0
                    and self._config.candle_grace_profit_pct > 0
                ):
                    if _recovery_target_touched(
                        position=position,
                        mark_price=mark_price,
                        profit_pct=self._config.candle_grace_profit_pct,
                    ):
                        return self._build_request(
                            state=state,
                            position=position,
                            reason=reason,
                            trigger_at=candle.candle_end,
                            reference_price=mark_price,
                        )
                    return self._build_grace_limit_request(
                        state=state,
                        position=position,
                        reason=reason,
                        trigger_at=candle.candle_end,
                        reference_price=mark_price,
                    )
                return self._build_request(
                    state=state,
                    position=position,
                    reason=reason,
                    trigger_at=candle.candle_end,
                    reference_price=mark_price,
                )
            self._checked_until[key] = candle.candle_end
        return None

    def _build_grace_limit_request(
        self,
        *,
        state: MarketState15s,
        position: ManagedLivePosition,
        reason: str,
        trigger_at: datetime,
        reference_price: Decimal,
    ) -> LiveExitOrderRequest:
        target_price = _recovery_price(
            position,
            self._config.candle_grace_profit_pct,
        )
        return self._build_order_request(
            state=state,
            position=position,
            reason=f"{reason}_grace_limit_{self._config.candle_grace_bars}",
            trigger_at=trigger_at,
            reference_price=reference_price,
            entry_type=EntryType.LIMIT,
            limit_price=target_price,
        )

    def _build_grace_timeout_request(
        self,
        *,
        state: MarketState15s,
        position: ManagedLivePosition,
        reference_price: Decimal,
    ) -> LiveExitCancellationRequest:
        recovery_plan = position.recovery_order_plan
        if recovery_plan is None:
            raise AssertionError("grace timeout requires a recovery order")
        fallback = self._build_order_request(
            state=state,
            position=position,
            reason=f"candle_15m_grace_timeout_{self._config.candle_grace_bars}",
            trigger_at=state.bucket_end,
            reference_price=reference_price,
        )
        return LiveExitCancellationRequest(
            cancel_plan=recovery_plan,
            fallback_candidate=fallback.candidate,
            fallback_quantity=fallback.quantity,
        )

    def _build_order_request(
        self,
        *,
        state: MarketState15s,
        position: ManagedLivePosition,
        reason: str,
        trigger_at: datetime,
        reference_price: Decimal,
        entry_type: EntryType = EntryType.MARKET,
        limit_price: Decimal | None = None,
    ) -> LiveExitOrderRequest:
        identity = (
            f"{self._config.run_id}:{position.symbol}:"
            f"{position.position_side.value}:{position.opened_at.isoformat()}:"
            f"{reason}:{trigger_at.isoformat()}"
        )
        signal_id = f"live-exit-signal-{uuid5(NAMESPACE_URL, identity)}"
        candidate_id = f"live-exit-{uuid5(NAMESPACE_URL, signal_id)}"
        created_at = state.bucket_end
        return LiveExitOrderRequest(
            candidate=OrderIntentCandidate(
                candidate_id=candidate_id,
                signal_id=signal_id,
                run_id=self._config.run_id,
                strategy_name=self._config.strategy_name,
                strategy_version=self._config.strategy_version,
                config_hash=self._config.strategy_config_hash,
                symbol=position.symbol,
                side=position.side,
                entry_type=entry_type,
                limit_price=limit_price,
                desired_notional=position.quantity * reference_price,
                reduce_only=True,
                expires_at=created_at
                + timedelta(seconds=self._config.candidate_ttl_seconds),
                created_at=created_at,
                reason=reason,
                features={
                    "position_side": position.position_side.value,
                    "quantity": str(position.quantity),
                    "entry_price": str(position.entry_price),
                    "reference_price": str(reference_price),
                    "opened_at": position.opened_at.astimezone(UTC).isoformat(),
                    "trigger_at": trigger_at.astimezone(UTC).isoformat(),
                    "exit_order_type": entry_type.value,
                },
            ),
            quantity=position.quantity,
        )

    def _build_request(
        self,
        *,
        state: MarketState15s,
        position: ManagedLivePosition,
        reason: str,
        trigger_at: datetime,
        reference_price: Decimal,
    ) -> LiveExitOrderRequest:
        return self._build_order_request(
            state=state,
            position=position,
            reason=reason,
            trigger_at=trigger_at,
            reference_price=reference_price,
        )


def _gross_return(
    position: ManagedLivePosition,
    mark_price: Decimal,
) -> Decimal:
    price_return = (mark_price - position.entry_price) / position.entry_price
    return -price_return if position.side is StrategySide.SHORT else price_return


def _recovery_price(
    position: ManagedLivePosition,
    profit_pct: Decimal,
) -> Decimal:
    if position.side is StrategySide.LONG:
        return position.entry_price * (Decimal("1") + profit_pct)
    return position.entry_price * (Decimal("1") - profit_pct)


def _recovery_target_touched(
    *,
    position: ManagedLivePosition,
    mark_price: Decimal,
    profit_pct: Decimal,
) -> bool:
    target_price = _recovery_price(position, profit_pct)
    if position.side is StrategySide.LONG:
        return mark_price >= target_price
    return mark_price <= target_price


def _exit_mark_price(
    state: MarketState15s,
    side: StrategySide,
) -> Decimal | None:
    quote = (
        state.last_bid_price
        if side is StrategySide.LONG
        else state.last_ask_price
    )
    price = quote or state.mark_price or state.close_price
    return price if price is not None and price > 0 else None


def _candle_start_15m(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("candle boundary must be timezone-aware")
    utc_value = value.astimezone(UTC)
    return utc_value.replace(
        minute=utc_value.minute - utc_value.minute % 15,
        second=0,
        microsecond=0,
    )
