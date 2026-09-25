"""Pure DecisionEngine domain service.

Obeys Astra Architecture Blueprint 2026-09-25:
- decide(DecisionInput, PolicyState, EffectivePolicy) -> DecisionResult
- Pure function: no DB, no network, no implicit now(), no random IDs, no os.environ.
- Explicit ClockEvent controls time.
- Deterministic decision_id derived from input hash.
- Pointers to MarketRevisionRef and PositionView version.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionView,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocation,
    ExitAllocationPlan,
    ExitPolicyMode,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.market.market_book import compute_market_state_hash
from crypto_momentum_lab.domain.market.revision_models import (
    MarketEnvelope,
    MarketRevisionRef,
)
from crypto_momentum_lab.domain.strategy.models import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.strategy_runner.position_exit import (
    ClosedCandle15m,
    PositionExitPolicy,
    position_exit_reason,
)


def _require_aware(dt: datetime, name: str) -> datetime:
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware (UTC)")
    return dt.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class ClockEvent:
    """Explicit external clock tick driving deterministic strategy evaluation."""

    timestamp: datetime
    sequence: int
    event_type: str = "bucket_close"

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "timestamp", _require_aware(self.timestamp, "timestamp")
        )
        if self.sequence < 0:
            raise ValueError("sequence must be non-negative")


@dataclass(frozen=True, slots=True)
class DecisionInput:
    """Immutable input contract for a single strategy evaluation tick."""

    symbol: str
    market_ref: MarketRevisionRef
    market_envelope: MarketEnvelope
    position_view: PositionView
    universe_version: str
    clock_event: ClockEvent
    cash_balance: Decimal
    risk_config_version: str

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if self.market_ref.symbol != self.symbol:
            raise ValueError(
                f"Market ref symbol {self.market_ref.symbol} != {self.symbol}"
            )
        if self.market_envelope.ref != self.market_ref:
            raise ValueError(
                f"market_envelope.ref ({self.market_envelope.ref.revision_id}) must match "
                f"market_ref ({self.market_ref.revision_id})"
            )
        computed_hash = compute_market_state_hash(self.market_envelope.state)
        if computed_hash != self.market_ref.content_hash:
            raise ValueError(
                f"market_envelope state content hash ({computed_hash}) does not match "
                f"market_ref.content_hash ({self.market_ref.content_hash})"
            )
        if self.market_envelope.state.symbol != self.symbol:
            raise ValueError(
                f"market_envelope state symbol {self.market_envelope.state.symbol} != {self.symbol}"
            )
        if self.position_view.key.symbol != self.symbol:
            raise ValueError(
                f"Position view symbol {self.position_view.key.symbol} != {self.symbol}"
            )
        if self.cash_balance < Decimal("0"):
            raise ValueError("cash_balance must not be negative")


@dataclass(frozen=True, slots=True)
class PolicyState:
    """Versioned, mutable-free strategy state preserving cooldowns and anchors."""

    policy_version: int = 1
    cooldown_until_by_symbol: dict[str, datetime] = field(default_factory=dict)
    anchor_prices_by_symbol: dict[str, Decimal] = field(default_factory=dict)
    active_intent_ids_by_symbol: dict[str, str] = field(default_factory=dict)
    custom_state: dict[str, Any] = field(default_factory=dict)

    def is_in_cooldown(self, symbol: str, current_time: datetime) -> bool:
        until = self.cooldown_until_by_symbol.get(symbol)
        return until is not None and current_time < until

    def with_cooldown(self, symbol: str, until: datetime) -> PolicyState:
        new_cd = dict(self.cooldown_until_by_symbol)
        new_cd[symbol] = until
        return PolicyState(
            policy_version=self.policy_version + 1,
            cooldown_until_by_symbol=new_cd,
            anchor_prices_by_symbol=dict(self.anchor_prices_by_symbol),
            active_intent_ids_by_symbol=dict(self.active_intent_ids_by_symbol),
            custom_state=dict(self.custom_state),
        )

    def with_cleared_symbol(self, symbol: str) -> PolicyState:
        new_cd = dict(self.cooldown_until_by_symbol)
        new_cd.pop(symbol, None)
        new_anchors = dict(self.anchor_prices_by_symbol)
        new_anchors.pop(symbol, None)
        new_intents = dict(self.active_intent_ids_by_symbol)
        new_intents.pop(symbol, None)
        return PolicyState(
            policy_version=self.policy_version + 1,
            cooldown_until_by_symbol=new_cd,
            anchor_prices_by_symbol=new_anchors,
            active_intent_ids_by_symbol=new_intents,
            custom_state=dict(self.custom_state),
        )


@dataclass(frozen=True, slots=True)
class EffectivePolicy:
    """Immutable parameters governing entry, exit, and sizing rules."""

    policy_id: str
    strategy_name: str
    policy_version: int = 1
    entry_threshold: Decimal = Decimal("65000.00")
    order_type: EntryType = EntryType.MARKET
    target_notional: Decimal = Decimal("500.00")
    max_open_positions: int = 4
    exit_policy: PositionExitPolicy = field(default_factory=PositionExitPolicy)
    cooldown_duration: timedelta = timedelta(minutes=15)


@dataclass(frozen=True, slots=True)
class DecisionResult:
    """Deterministic outcome of strategy decision evaluation."""

    decision_id: str
    input_hash: str
    intent: OrderIntentCandidate | None
    exit_command: TradeCommand | None
    next_policy_state: PolicyState
    rejection_reason: str | None
    evaluated_at: datetime


def compute_decision_input_hash(
    decision_input: DecisionInput,
    policy: EffectivePolicy,
    state: PolicyState,
) -> str:
    """Calculates a deterministic cryptographic hash of all decision inputs."""
    payload = {
        "symbol": decision_input.symbol,
        "market_revision_id": decision_input.market_ref.revision_id,
        "market_content_hash": decision_input.market_ref.content_hash,
        "position_version": decision_input.position_view.projection_version,
        "position_quantity": str(decision_input.position_view.total_quantity),
        "universe_version": decision_input.universe_version,
        "clock_time": decision_input.clock_event.timestamp.isoformat(),
        "clock_sequence": decision_input.clock_event.sequence,
        "cash_balance": str(decision_input.cash_balance),
        "risk_config_version": decision_input.risk_config_version,
        "policy_id": policy.policy_id,
        "policy_version": policy.policy_version,
        "policy_state_version": state.policy_version,
    }
    dumped = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


def decide(
    decision_input: DecisionInput,
    state: PolicyState,
    policy: EffectivePolicy,
) -> DecisionResult:
    """Pure strategy decision function.

    Guarantees:
    - Zero side effects: no I/O, no DB, no network, no datetime.now();
    - Fully reproducible from frozen DecisionInput;
    - Returns updated PolicyState and deterministic decision ID.
    """
    input_hash = compute_decision_input_hash(decision_input, policy, state)
    decision_id = f"dec_{decision_input.symbol}_{input_hash[:16]}"
    clock_time = decision_input.clock_event.timestamp
    state_15s = decision_input.market_envelope.state
    pos_view = decision_input.position_view

    # 1. Check Exit condition if position is currently open
    if pos_view.total_quantity > Decimal("0"):
        closed_candle = None
        if state_15s.open_price is not None and state_15s.close_price is not None:
            closed_candle = ClosedCandle15m(
                symbol=decision_input.symbol,
                candle_start=state_15s.bucket_start,
                candle_end=state_15s.bucket_end,
                open_price=state_15s.open_price,
                close_price=state_15s.close_price,
            )

        earliest_open = min(
            (b.opened_at for b in pos_view.batches),
            default=clock_time,
        )

        exit_reason = position_exit_reason(
            held_until=clock_time,
            opened_at=earliest_open,
            symbol=decision_input.symbol,
            side=StrategySide.LONG,
            policy=policy.exit_policy,
            closed_candle=closed_candle,
        )

        if exit_reason is not None:
            allocations = tuple(
                ExitAllocation(
                    batch_id=b.batch_id,
                    allocated_quantity=b.quantity,
                    entry_price=b.entry_price,
                )
                for b in pos_view.batches
                if b.quantity > Decimal("0")
            )
            total_qty = sum(
                (a.allocated_quantity for a in allocations), start=Decimal("0")
            )
            if total_qty > Decimal("0"):
                alloc_plan = ExitAllocationPlan(
                    position_key=pos_view.key,
                    allocations=allocations,
                    total_allocated_quantity=total_qty,
                    policy=ExitPolicyMode.FULL_POSITION_CLOSE,
                    reason=exit_reason,
                    projection_version=pos_view.projection_version,
                )
                exit_cmd = TradeCommand(
                    command_id=f"cmd_exit_{decision_id}",
                    position_key=pos_view.key,
                    command_type=TradeCommandType.EXIT,
                    side=StrategySide.SHORT,
                    order_type=EntryType.MARKET,
                    requested_quantity=total_qty,
                    reduce_only=True,
                    allocation_plan=alloc_plan,
                    reason=exit_reason,
                    created_at=clock_time,
                    expected_projection_version=pos_view.projection_version,
                )
                next_state = state.with_cooldown(
                    decision_input.symbol, clock_time + policy.cooldown_duration
                )
                return DecisionResult(
                    decision_id=decision_id,
                    input_hash=input_hash,
                    intent=None,
                    exit_command=exit_cmd,
                    next_policy_state=next_state,
                    rejection_reason=None,
                    evaluated_at=clock_time,
                )

    # 2. Check Cooldown
    if state.is_in_cooldown(decision_input.symbol, clock_time):
        return DecisionResult(
            decision_id=decision_id,
            input_hash=input_hash,
            intent=None,
            exit_command=None,
            next_policy_state=state,
            rejection_reason="cooldown_active",
            evaluated_at=clock_time,
        )

    # 3. Check Entry Eligibility (when position is flat)
    if pos_view.total_quantity == Decimal("0"):
        close = state_15s.close_price or Decimal("0")
        if close > policy.entry_threshold:
            intent = OrderIntentCandidate(
                candidate_id=f"intent_{decision_id}",
                signal_id=f"sig_{decision_id}",
                run_id="run_deterministic",
                strategy_name=policy.strategy_name,
                strategy_version=f"v{policy.policy_version}",
                config_hash=policy.policy_id,
                symbol=decision_input.symbol,
                side=StrategySide.LONG,
                entry_type=policy.order_type,
                limit_price=close if policy.order_type == EntryType.LIMIT else None,
                desired_notional=policy.target_notional,
                reduce_only=False,
                expires_at=clock_time + timedelta(minutes=5),
                created_at=clock_time,
                reason="breakout_above_threshold",
                features={"close_price": str(close)},
            )
            return DecisionResult(
                decision_id=decision_id,
                input_hash=input_hash,
                intent=intent,
                exit_command=None,
                next_policy_state=state,
                rejection_reason=None,
                evaluated_at=clock_time,
            )
        else:
            return DecisionResult(
                decision_id=decision_id,
                input_hash=input_hash,
                intent=None,
                exit_command=None,
                next_policy_state=state,
                rejection_reason="below_entry_threshold",
                evaluated_at=clock_time,
            )

    return DecisionResult(
        decision_id=decision_id,
        input_hash=input_hash,
        intent=None,
        exit_command=None,
        next_policy_state=state,
        rejection_reason="holding_position_no_exit",
        evaluated_at=clock_time,
    )


class DecisionEngine:
    """Authoritative pure domain DecisionEngine service."""

    @staticmethod
    def evaluate(
        decision_input: DecisionInput,
        state: PolicyState,
        policy: EffectivePolicy,
    ) -> DecisionResult:
        """Evaluates pure strategy decision."""
        return decide(decision_input, state, policy)
