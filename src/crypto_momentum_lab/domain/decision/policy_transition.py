"""Strategy policy state transitions and timing contracts (R3).

Obeys Astra Architecture Blueprint 2026-09-25:
- transition(frame, prior_state, policy_artifact) -> PolicyTransition;
- Unified state transitions across Paper, Research, and Live dry-run;
- Explicit StrategyPositionMode (LONG_ONLY, SHORT_ONLY, BOTH), no implicit defaults;
- Clocks drive holding exits and timers even in the absence of entry candidates;
- Atomic next_state, decision_trace, and commands emission.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, fields, is_dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import Enum, StrEnum
from typing import Any

from crypto_momentum_lab.domain.decision.decision_frame import (
    DecisionFrame,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
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
from crypto_momentum_lab.domain.market.revision_models import MarketEnvelope
from crypto_momentum_lab.domain.strategy.models import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.domain.strategy.position_exit import (
    ClosedCandle15m,
    PositionExitPolicy,
    position_exit_reason,
)
from crypto_momentum_lab.domain.strategy.sizing import (
    SizingModel,
    SizingPlan,
    SizingRejection,
    SymbolLotRules,
)


class StrategyPositionMode(StrEnum):
    """Explicit allowed trading directions for a strategy policy."""

    LONG_ONLY = "long_only"
    SHORT_ONLY = "short_only"
    BOTH = "both"


@dataclass(frozen=True, slots=True)
class TimerRequest:
    """Explicit timer request emitted during policy state transition."""

    timer_id: str
    timer_type: str  # cooldown_expiry, grace_period_expiry, max_holding_expiry
    symbol: str
    due_at: datetime
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.timer_id.strip():
            raise ValueError("timer_id must not be empty")
        if not self.timer_type.strip():
            raise ValueError("timer_type must not be empty")
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if self.due_at.tzinfo is None:
            raise ValueError("due_at must be timezone-aware (UTC)")


@dataclass(frozen=True, slots=True)
class PolicyTransition:
    """Immutable outcome of a pure policy transition."""

    decision_id: str
    frame_digest: str
    input_hash: str
    prior_state_version: int
    next_state: Any  # PolicyState
    entry_candidate: OrderIntentCandidate | None = None
    exit_command: TradeCommand | None = None
    timer_requests: tuple[TimerRequest, ...] = ()
    rejection_reason: str | None = None
    transition_time: datetime = field(default_factory=lambda: datetime.now(UTC))


POLICY_SERIALIZATION_VERSION = 1
POLICY_STATE_SERIALIZATION_VERSION = 1


def _decimal_text(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("policy decimals must be finite")
    if value.is_zero():
        return "0"
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _timedelta_seconds(value: timedelta) -> int | str:
    total_microseconds = (
        value.days * 86_400 + value.seconds
    ) * 1_000_000 + value.microseconds
    seconds = Decimal(total_microseconds) / Decimal(1_000_000)
    if seconds == seconds.to_integral_value():
        return int(seconds)
    return _decimal_text(seconds)


def canonicalize_policy_value(value: Any) -> Any:
    """Convert supported policy values to deterministic JSON values.

    Dataclass and slots-backed objects retain their class name and every public
    field. Unsupported values fail closed instead of falling back to an
    implementation-dependent ``str(value)`` representation.
    """
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("policy floats must be finite")
        return format(value, ".17g")
    if isinstance(value, Enum):
        return canonicalize_policy_value(value.value)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("policy datetimes must be timezone-aware")
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, timedelta):
        return {"seconds": _timedelta_seconds(value)}
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("policy mapping keys must be strings")
        return {key: canonicalize_policy_value(value[key]) for key in sorted(value)}
    if isinstance(value, (tuple, list)):
        return [canonicalize_policy_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized = [canonicalize_policy_value(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
        )

    if not is_dataclass(value) or isinstance(value, type):
        value_type = f"{type(value).__module__}.{type(value).__qualname__}"
        raise TypeError(f"unsupported policy value type: {value_type}")
    object_fields = {
        item.name: getattr(value, item.name)
        for item in fields(value)
        if not item.name.startswith("_")
    }

    normalized_fields = {
        key: canonicalize_policy_value(item)
        for key, item in sorted(object_fields.items())
        if not callable(item)
    }
    if not normalized_fields and callable(value):
        raise TypeError("callables are not serializable policy parameters")
    return {
        "class": type(value).__name__,
        "module": type(value).__module__,
        **normalized_fields,
    }


def serialize_policy_parameters(policy: Any) -> dict[str, Any]:
    """Return versioned canonical parameters for every decision-relevant field."""
    raw_fields = {
        item.name: getattr(policy, item.name)
        for item in fields(policy)
        if not item.name.startswith("_")
    }
    res: dict[str, Any] = {"serialization_version": POLICY_SERIALIZATION_VERSION}
    for name, value in sorted(raw_fields.items()):
        # The filter injects an executable candidate callback. Its frozen input
        # candidate is recorded separately in DecisionTrace; serializing the
        # closure itself would not be reproducible.
        if name == "candidate_generator":
            continue
        if isinstance(value, timedelta):
            seconds = _timedelta_seconds(value)
            res[name] = seconds
            res[f"{name}_seconds"] = seconds
        else:
            res[name] = canonicalize_policy_value(value)
    return res


def compute_policy_parameters_digest(policy: Any) -> str:
    params = serialize_policy_parameters(policy)
    dumped = json.dumps(params, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


def serialize_policy_state(state: Any) -> dict[str, Any]:
    res: dict[str, Any] = {
        "serialization_version": POLICY_STATE_SERIALIZATION_VERSION,
    }
    aliases = {
        "cooldown_until_by_symbol": "cooldown_until",
        "anchor_prices_by_symbol": "anchor_prices",
        "active_intent_ids_by_symbol": "active_intent_ids",
        "grace_until_by_symbol": "grace_until",
        "holding_deadline_by_symbol": "holding_deadline",
        "sizing_state_by_symbol": "sizing_state",
    }
    raw_fields = {
        item.name: getattr(state, item.name)
        for item in fields(state)
        if not item.name.startswith("_")
    }
    for name, value in sorted(raw_fields.items()):
        res[aliases.get(name, name)] = canonicalize_policy_value(value)
    return res


def compute_policy_state_digest(state: Any) -> str:
    payload = serialize_policy_state(state)
    dumped = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


def compute_transition_input_hash(
    frame: DecisionFrame,
    prior_state_version: int,
    policy_id: str,
    policy_version: int,
    policy_parameters_digest: str | None = None,
    policy_state_digest: str | None = None,
) -> str:
    """Deterministic cryptographic hash binding frame and policy context."""
    param_digest = (
        policy_parameters_digest
        if policy_parameters_digest is not None
        else frame.policy_parameters_digest
    )
    state_digest = (
        policy_state_digest
        if policy_state_digest is not None
        else frame.policy_state_digest
    )
    payload = {
        "frame_digest": frame.frame_digest,
        "prior_state_version": prior_state_version,
        "policy_id": policy_id,
        "policy_version": policy_version,
        "policy_parameters_digest": param_digest,
        "policy_state_digest": state_digest,
    }
    dumped = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


def _evaluate_sizing(
    cand: OrderIntentCandidate,
    policy_artifact: Any,
    symbol: str,
    ref_price: Decimal,
    cash_balance: Decimal,
    as_of: datetime,
) -> tuple[OrderIntentCandidate | None, SizingPlan | None, str | None]:
    """Applies sizing model if present on policy_artifact.

    Returns:
        (updated_candidate, sizing_plan, rejection_reason)
    """
    sizing_model: SizingModel | None = policy_artifact.sizing_model
    if sizing_model is None:
        return cand, None, None

    lot_rules: SymbolLotRules | None = policy_artifact.symbol_lot_rules
    if lot_rules is None:
        return None, None, "sizing_missing_realtime_lot_rules"

    result = sizing_model.compute_plan(
        symbol=symbol,
        reference_price=ref_price,
        cash_balance=cash_balance,
        lot_rules=lot_rules,
        as_of=as_of,
        sizing_version=policy_artifact.policy_version,
    )
    if isinstance(result, SizingRejection):
        return None, None, f"sizing_{result.reason}"

    sizing_feat = {
        "sizing_model": str(result.features.get("model", "custom")),
        "quantized_quantity": str(result.quantized_quantity),
        "lot_remainder": str(result.lot_remainder),
        "target_notional": str(result.target_notional),
        "actual_notional": str(result.actual_notional),
        "step_size": str(result.step_size),
        "margin_required": str(result.margin_required),
    }
    updated_features = dict(cand.features)
    updated_features.update(sizing_feat)
    updated_cand = replace(
        cand,
        desired_notional=result.actual_notional,
        features=updated_features,
    )
    return updated_cand, result, None


def execute_policy_transition(
    frame: DecisionFrame,
    prior_state: Any,
    policy_artifact: Any,
    *,
    market_envelope: MarketEnvelope,
    position_view: PositionView,
    closed_candles: tuple[ClosedCandle15m, ...] = (),
    decision_input: Any | None = None,
) -> PolicyTransition:
    """Compute the canonical pure policy transition.

    Guarantees:
    - Pure function: zero I/O, no database, no network, no implicit wall clock;
    - Same output across Live dry-run, Paper, and Research for same frame + state;
    - Clocks evaluate holding exit and timers even when no entry signal exists;
    - Position mode (LONG_ONLY / SHORT_ONLY / BOTH) strictly enforced.
    """
    clock_time = frame.clock_event.timestamp
    symbol = frame.symbol

    if market_envelope.state.symbol != symbol:
        raise ValueError(
            f"market_envelope symbol {market_envelope.state.symbol} != {symbol}"
        )
    if position_view.key.symbol != symbol:
        raise ValueError(f"position_view symbol {position_view.key.symbol} != {symbol}")

    param_digest = compute_policy_parameters_digest(policy_artifact)
    state_digest = compute_policy_state_digest(prior_state)
    input_hash = compute_transition_input_hash(
        frame=frame,
        prior_state_version=prior_state.policy_version,
        policy_id=policy_artifact.policy_id,
        policy_version=policy_artifact.policy_version,
        policy_parameters_digest=param_digest,
        policy_state_digest=state_digest,
    )
    decision_id = f"dec_{symbol}_{input_hash[:16]}"
    state_15s = market_envelope.state

    # 1. Evaluate Position Holding & Exit (even when no entry signal exists)
    if position_view.total_quantity > Decimal("0"):
        closed_candle: ClosedCandle15m | None = (
            closed_candles[-1] if closed_candles else None
        )
        if (
            closed_candle is None
            and state_15s.open_price is not None
            and state_15s.close_price is not None
            and state_15s.open_price > Decimal("0")
            and state_15s.close_price > Decimal("0")
            and (state_15s.bucket_end - state_15s.bucket_start) == timedelta(minutes=15)
        ):
            closed_candle = ClosedCandle15m(
                symbol=symbol,
                candle_start=state_15s.bucket_start,
                candle_end=state_15s.bucket_end,
                open_price=state_15s.open_price,
                close_price=state_15s.close_price,
            )

        earliest_open = min(
            (b.opened_at for b in position_view.batches),
            default=clock_time,
        )

        pos_side = StrategySide.LONG
        if position_view.active_episode is not None:
            pos_side = position_view.active_episode.side
        elif position_view.key.position_side == FuturesPositionSide.SHORT:
            pos_side = StrategySide.SHORT
        elif position_view.key.position_side == FuturesPositionSide.LONG:
            pos_side = StrategySide.LONG

        exit_policy: PositionExitPolicy = policy_artifact.exit_policy
        exit_reason = position_exit_reason(
            held_until=clock_time,
            opened_at=earliest_open,
            symbol=symbol,
            side=pos_side,
            policy=exit_policy,
            closed_candle=closed_candle,
            closed_candles=closed_candles,
        )

        if exit_reason is not None:
            allocations = tuple(
                ExitAllocation(
                    batch_id=b.batch_id,
                    allocated_quantity=b.quantity,
                    entry_price=b.entry_price,
                )
                for b in position_view.batches
                if b.quantity > Decimal("0")
            )
            total_qty = sum(
                (a.allocated_quantity for a in allocations), start=Decimal("0")
            )
            if total_qty > Decimal("0"):
                alloc_plan = ExitAllocationPlan(
                    position_key=position_view.key,
                    allocations=allocations,
                    total_allocated_quantity=total_qty,
                    policy=ExitPolicyMode.FULL_POSITION_CLOSE,
                    reason=exit_reason,
                    projection_version=position_view.projection_version,
                )
                exit_cmd = TradeCommand(
                    command_id=f"cmd_exit_{decision_id}",
                    position_key=position_view.key,
                    command_type=TradeCommandType.EXIT,
                    side=pos_side,
                    order_type=EntryType.MARKET,
                    requested_quantity=total_qty,
                    reduce_only=True,
                    allocation_plan=alloc_plan,
                    reason=exit_reason,
                    created_at=clock_time,
                    expected_projection_version=position_view.projection_version,
                )
                cooldown_dur: timedelta = policy_artifact.cooldown_duration
                cd_due = clock_time + cooldown_dur
                next_state = prior_state.with_exit(symbol, cd_due)
                timer = TimerRequest(
                    timer_id=f"tm_cd_{decision_id}",
                    timer_type="cooldown_expiry",
                    symbol=symbol,
                    due_at=cd_due,
                    details={"exit_reason": exit_reason},
                )
                return PolicyTransition(
                    decision_id=decision_id,
                    frame_digest=frame.frame_digest,
                    input_hash=input_hash,
                    prior_state_version=prior_state.policy_version,
                    next_state=next_state,
                    exit_command=exit_cmd,
                    timer_requests=(timer,),
                    transition_time=clock_time,
                )
        else:
            # Position is held and not exiting; schedule max holding expiration timer
            max_holding = exit_policy.max_holding_seconds
            holding_timers: list[TimerRequest] = []
            next_state = prior_state
            if max_holding is not None and max_holding > 0:
                holding_deadline = earliest_open + timedelta(seconds=max_holding)
                holding_timers.append(
                    TimerRequest(
                        timer_id=f"tm_hold_{decision_id}",
                        timer_type="max_holding_expiry",
                        symbol=symbol,
                        due_at=holding_deadline,
                        details={"opened_at": earliest_open.isoformat()},
                    )
                )
                current_dl = prior_state.holding_deadline_by_symbol.get(symbol)
                if current_dl != holding_deadline:
                    next_state = prior_state.with_holding_deadline(
                        symbol, holding_deadline
                    )
            return PolicyTransition(
                decision_id=decision_id,
                frame_digest=frame.frame_digest,
                input_hash=input_hash,
                prior_state_version=prior_state.policy_version,
                next_state=next_state,
                timer_requests=tuple(holding_timers),
                rejection_reason="holding_position_no_exit",
                transition_time=clock_time,
            )

    # 2. Check Cooldown
    if prior_state.is_in_cooldown(symbol, clock_time):
        return PolicyTransition(
            decision_id=decision_id,
            frame_digest=frame.frame_digest,
            input_hash=input_hash,
            prior_state_version=prior_state.policy_version,
            next_state=prior_state,
            rejection_reason="cooldown_active",
            transition_time=clock_time,
        )

    # 3. Check Position Mode and Entry Evaluation (when position is flat)
    pos_mode = policy_artifact.position_mode
    if position_view.total_quantity == Decimal("0"):
        generator = policy_artifact.candidate_generator
        if generator is not None:
            arg0 = decision_input if decision_input is not None else market_envelope
            cand = generator(arg0, prior_state)
            if cand is not None:
                # Enforce position mode
                if (
                    cand.side == StrategySide.LONG
                    and pos_mode == StrategyPositionMode.SHORT_ONLY
                ):
                    return PolicyTransition(
                        decision_id=decision_id,
                        frame_digest=frame.frame_digest,
                        input_hash=input_hash,
                        prior_state_version=prior_state.policy_version,
                        next_state=prior_state,
                        rejection_reason="direction_not_permitted_by_position_mode",
                        transition_time=clock_time,
                    )
                if (
                    cand.side == StrategySide.SHORT
                    and pos_mode == StrategyPositionMode.LONG_ONLY
                ):
                    return PolicyTransition(
                        decision_id=decision_id,
                        frame_digest=frame.frame_digest,
                        input_hash=input_hash,
                        prior_state_version=prior_state.policy_version,
                        next_state=prior_state,
                        rejection_reason="direction_not_permitted_by_position_mode",
                        transition_time=clock_time,
                    )

                # Evaluate sizing if model present
                ref_price = cand.limit_price or state_15s.close_price or Decimal("0")
                effective_cash = frame.cash_balance
                if effective_cash <= Decimal("0") and decision_input is not None:
                    effective_cash = decision_input.cash_balance
                cand_sized, sizing_plan, rej_reason = _evaluate_sizing(
                    cand=cand,
                    policy_artifact=policy_artifact,
                    symbol=symbol,
                    ref_price=ref_price,
                    cash_balance=effective_cash,
                    as_of=clock_time,
                )
                if rej_reason is not None:
                    return PolicyTransition(
                        decision_id=decision_id,
                        frame_digest=frame.frame_digest,
                        input_hash=input_hash,
                        prior_state_version=prior_state.policy_version,
                        next_state=prior_state,
                        rejection_reason=rej_reason,
                        transition_time=clock_time,
                    )
                if cand_sized is not None:
                    cand = cand_sized

                grace_timers: list[TimerRequest] = []
                grace_period: timedelta = policy_artifact.grace_period
                grace_due: datetime | None = None
                if grace_period > timedelta(0):
                    grace_due = clock_time + grace_period
                    grace_timers.append(
                        TimerRequest(
                            timer_id=f"tm_grace_{decision_id}",
                            timer_type="grace_period_expiry",
                            symbol=symbol,
                            due_at=grace_due,
                            details={"candidate_id": cand.candidate_id},
                        )
                    )

                next_state = prior_state.with_anchor_and_intent(
                    symbol=symbol,
                    anchor_price=state_15s.close_price or Decimal("0"),
                    intent_id=cand.candidate_id,
                    grace_until=grace_due,
                )
                if sizing_plan is not None:
                    next_state = next_state.with_sizing_state(
                        symbol=symbol,
                        sizing_state=asdict(sizing_plan),
                    )
                return PolicyTransition(
                    decision_id=decision_id,
                    frame_digest=frame.frame_digest,
                    input_hash=input_hash,
                    prior_state_version=prior_state.policy_version,
                    next_state=next_state,
                    entry_candidate=cand,
                    timer_requests=tuple(grace_timers),
                    transition_time=clock_time,
                )
            else:
                return PolicyTransition(
                    decision_id=decision_id,
                    frame_digest=frame.frame_digest,
                    input_hash=input_hash,
                    prior_state_version=prior_state.policy_version,
                    next_state=prior_state,
                    rejection_reason="no_candidate",
                    transition_time=clock_time,
                )

        # Built-in breakout threshold evaluation
        entry_thresh: Decimal | None = policy_artifact.entry_threshold
        short_entry_thresh: Decimal | None = policy_artifact.short_entry_threshold
        close_px = state_15s.close_price or Decimal("0")
        cand = None
        if entry_thresh is not None and close_px > entry_thresh:
            if pos_mode == StrategyPositionMode.SHORT_ONLY:
                return PolicyTransition(
                    decision_id=decision_id,
                    frame_digest=frame.frame_digest,
                    input_hash=input_hash,
                    prior_state_version=prior_state.policy_version,
                    next_state=prior_state,
                    rejection_reason="direction_not_permitted_by_position_mode",
                    transition_time=clock_time,
                )
            target_notional_val = policy_artifact.target_notional
            if target_notional_val is None or target_notional_val <= Decimal("0"):
                raise ValueError(
                    "policy_artifact must provide an explicit positive target_notional"
                )
            target_notional: Decimal = target_notional_val
            order_type: EntryType = policy_artifact.order_type
            cand = OrderIntentCandidate(
                candidate_id=f"intent_{decision_id}",
                signal_id=f"sig_{decision_id}",
                run_id="run_deterministic",
                strategy_name=policy_artifact.strategy_name,
                strategy_version=f"v{policy_artifact.policy_version}",
                config_hash=policy_artifact.policy_id,
                symbol=symbol,
                side=StrategySide.LONG,
                entry_type=order_type,
                limit_price=(close_px if order_type == EntryType.LIMIT else None),
                desired_notional=target_notional,
                reduce_only=False,
                expires_at=clock_time + timedelta(minutes=5),
                created_at=clock_time,
                reason="breakout_above_threshold",
                features={"close_price": str(close_px)},
            )
        elif short_entry_thresh is not None and close_px < short_entry_thresh:
            if pos_mode == StrategyPositionMode.LONG_ONLY:
                return PolicyTransition(
                    decision_id=decision_id,
                    frame_digest=frame.frame_digest,
                    input_hash=input_hash,
                    prior_state_version=prior_state.policy_version,
                    next_state=prior_state,
                    rejection_reason="direction_not_permitted_by_position_mode",
                    transition_time=clock_time,
                )
            target_notional_val = policy_artifact.target_notional
            if target_notional_val is None or target_notional_val <= Decimal("0"):
                raise ValueError(
                    "policy_artifact must provide an explicit positive target_notional"
                )
            target_notional = target_notional_val
            order_type = policy_artifact.order_type
            cand = OrderIntentCandidate(
                candidate_id=f"intent_{decision_id}",
                signal_id=f"sig_{decision_id}",
                run_id="run_deterministic",
                strategy_name=policy_artifact.strategy_name,
                strategy_version=f"v{policy_artifact.policy_version}",
                config_hash=policy_artifact.policy_id,
                symbol=symbol,
                side=StrategySide.SHORT,
                entry_type=order_type,
                limit_price=(close_px if order_type == EntryType.LIMIT else None),
                desired_notional=target_notional,
                reduce_only=False,
                expires_at=clock_time + timedelta(minutes=5),
                created_at=clock_time,
                reason="breakout_below_short_threshold",
                features={"close_price": str(close_px)},
            )
        else:
            reason = (
                "below_entry_threshold" if entry_thresh is not None else "no_candidate"
            )
            return PolicyTransition(
                decision_id=decision_id,
                frame_digest=frame.frame_digest,
                input_hash=input_hash,
                prior_state_version=prior_state.policy_version,
                next_state=prior_state,
                rejection_reason=reason,
                transition_time=clock_time,
            )

        # Evaluate sizing if model present
        ref_price = cand.limit_price or close_px
        effective_cash = frame.cash_balance
        if effective_cash <= Decimal("0") and decision_input is not None:
            effective_cash = decision_input.cash_balance
        cand_sized, sizing_plan, rej_reason = _evaluate_sizing(
            cand=cand,
            policy_artifact=policy_artifact,
            symbol=symbol,
            ref_price=ref_price,
            cash_balance=effective_cash,
            as_of=clock_time,
        )
        if rej_reason is not None:
            return PolicyTransition(
                decision_id=decision_id,
                frame_digest=frame.frame_digest,
                input_hash=input_hash,
                prior_state_version=prior_state.policy_version,
                next_state=prior_state,
                rejection_reason=rej_reason,
                transition_time=clock_time,
            )
        if cand_sized is not None:
            cand = cand_sized

        grace_timers = []
        grace_period = policy_artifact.grace_period
        grace_due = None
        if grace_period > timedelta(0):
            grace_due = clock_time + grace_period
            grace_timers.append(
                TimerRequest(
                    timer_id=f"tm_grace_{decision_id}",
                    timer_type="grace_period_expiry",
                    symbol=symbol,
                    due_at=grace_due,
                    details={"candidate_id": cand.candidate_id},
                )
            )

        next_state = prior_state.with_anchor_and_intent(
            symbol=symbol,
            anchor_price=close_px,
            intent_id=cand.candidate_id,
            grace_until=grace_due,
        )
        if sizing_plan is not None:
            next_state = next_state.with_sizing_state(
                symbol=symbol,
                sizing_state=asdict(sizing_plan),
            )
        return PolicyTransition(
            decision_id=decision_id,
            frame_digest=frame.frame_digest,
            input_hash=input_hash,
            prior_state_version=prior_state.policy_version,
            next_state=next_state,
            entry_candidate=cand,
            timer_requests=tuple(grace_timers),
            transition_time=clock_time,
        )

    return PolicyTransition(
        decision_id=decision_id,
        frame_digest=frame.frame_digest,
        input_hash=input_hash,
        prior_state_version=prior_state.policy_version,
        next_state=prior_state,
        rejection_reason="no_action",
        transition_time=clock_time,
    )
