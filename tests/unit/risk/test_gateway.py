from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.risk.limits import FixedLiveLimits, LiveLimitContext
from crypto_momentum_lab.domain.risk.models import (
    RiskDecision,
    StrategyLiveState,
)
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.risk.gateway import RiskContext, RiskGateway
from tests.unit.domain.risk.test_models import _risk_config


def test_gateway_approves_small_entry_when_all_limits_pass() -> None:
    evaluation = RiskGateway().evaluate(_intent(), _context()).evaluation

    assert evaluation.decision is RiskDecision.APPROVED
    assert evaluation.reason == "approved"


def test_gateway_rejects_entry_with_unbounded_capacity_limits() -> None:
    context = replace(
        _context(),
        open_position_symbols=frozenset({"ETHUSDT", "SOLUSDT"}),
        risk_config=replace(
            _risk_config(max_order_notional=None),
            max_gross_notional=None,
            max_open_positions=None,
        ),
    )

    evaluation = (
        RiskGateway()
        .evaluate(
            _intent(desired_notional=Decimal("100")),
            context,
        )
        .evaluation
    )

    assert evaluation.decision is RiskDecision.REJECTED
    assert evaluation.reason == "missing_max_order_notional_limit"


def test_gateway_allows_reduce_only_while_draining() -> None:
    evaluation = (
        RiskGateway()
        .evaluate(
            _intent(reduce_only=True),
            _context(strategy_state=StrategyLiveState.DRAINING),
        )
        .evaluation
    )

    assert evaluation.decision is RiskDecision.APPROVED
    assert evaluation.reason == "reduce_only"


def test_gateway_allows_reduce_only_while_draining_when_config_flag_is_false() -> None:
    evaluation = (
        RiskGateway()
        .evaluate(
            _intent(reduce_only=True),
            RiskContext(
                now=datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC),
                open_position_symbols=frozenset(),
                active_halts=(),
                risk_config=_risk_config(
                    max_order_notional=Decimal("100"),
                    allow_reduce_only_while_draining=False,
                ),
                strategy_state=StrategyLiveState.DRAINING,
            ),
        )
        .evaluation
    )

    assert evaluation.decision is RiskDecision.APPROVED
    assert evaluation.reason == "reduce_only"


def test_gateway_does_not_cap_reduce_only_exit_notional() -> None:
    evaluation = (
        RiskGateway()
        .evaluate(
            _intent(reduce_only=True, desired_notional=Decimal("500")),
            _context(),
        )
        .evaluation
    )

    assert evaluation.decision is RiskDecision.APPROVED
    assert evaluation.reason == "reduce_only"






def test_gateway_blocks_entries_when_strategy_is_halted() -> None:
    evaluation = (
        RiskGateway()
        .evaluate(
            _intent(),
            _context(strategy_state=StrategyLiveState.HALTED),
        )
        .evaluation
    )

    assert evaluation.decision is RiskDecision.HALTED
    assert evaluation.reason == "strategy_halted"


def _entry_limit_context() -> LiveLimitContext:
    return LiveLimitContext(
        symbol="BTCUSDT",
        requested_notional=Decimal("50"),
        open_position_symbols=frozenset(),
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        gross_exposure=Decimal("0"),
        min_notional=Decimal("5"),
        has_unresolved_order=False,
    )


def _fixed_limits() -> FixedLiveLimits:
    return FixedLiveLimits(
        notional_cap=Decimal("25"),
        max_open_positions=2,
        max_daily_loss=Decimal("10"),
        max_gross_exposure=Decimal("100"),
        max_concurrency_per_symbol=2,
    )


def test_gateway_applies_notional_cap_before_order_limit() -> None:
    context = replace(
        _context(),
        risk_config=_risk_config(max_order_notional=Decimal("30")),
    )
    result = RiskGateway(limits=_fixed_limits()).evaluate(
        _intent(),
        context,
        limit_context=_entry_limit_context(),
    )
    assert result.evaluation.decision is RiskDecision.APPROVED
    assert result.candidate is not None
    assert result.candidate.desired_notional == Decimal("25")
    assert result.evaluation.details["desired_notional"] == "25"


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"gross_exposure": None}, "missing_gross_exposure"),
        ({"gross_exposure": Decimal("100")}, "max_gross_exposure_reached"),
        ({"realized_pnl": Decimal("-10")}, "max_daily_loss_reached"),
        ({"has_unresolved_order": True}, "unresolved_order_uncertainty"),
        ({"symbol_concurrency": 2}, "max_concurrency_per_symbol_exceeded"),
    ],
)
def test_gateway_rejects_fixed_limit_failures(change, reason) -> None:
    result = RiskGateway(limits=_fixed_limits()).evaluate(
        _intent(),
        _context(),
        limit_context=replace(_entry_limit_context(), **change),
    )
    assert result.candidate is None
    assert result.evaluation.decision is RiskDecision.REJECTED
    assert result.evaluation.reason == reason




def test_gateway_reduce_only_bypasses_entry_limits() -> None:
    intent = _intent(reduce_only=True, desired_notional=Decimal("500"))
    result = RiskGateway(limits=_fixed_limits()).evaluate(intent, _context())
    assert result.candidate is intent
    assert result.evaluation.decision is RiskDecision.APPROVED


def test_gateway_requires_limit_facts_for_configured_entry() -> None:
    with pytest.raises(ValueError, match="entry limit context is required"):
        RiskGateway(limits=_fixed_limits()).evaluate(_intent(), _context())


def test_gateway_uses_stricter_position_limit_once():
    limits = replace(_fixed_limits(), max_open_positions=4)
    context = replace(
        _context(),
        open_position_symbols=frozenset({"ETHUSDT"}),
        risk_config=replace(_context().risk_config, max_open_positions=1),
    )
    result = RiskGateway(limits=limits).evaluate(
        _intent(),
        context,
        limit_context=replace(
            _entry_limit_context(), open_position_symbols=context.open_position_symbols
        ),
    )
    assert result.evaluation.reason == "max_open_positions_exceeded"


def test_quantized_notional_cannot_exceed_approved_budget():
    allowed, reason = RiskGateway().validate_quantized_notional(
        Decimal("55"),
        _context(),
        gross_exposure=Decimal("0"),
        approved_notional=Decimal("50"),
    )
    assert not allowed
    assert reason == "quantized_order_notional_exceeds_approved_notional"


def _context(
    *,
    strategy_state: StrategyLiveState = StrategyLiveState.ACTIVE,
    now: datetime = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC),
) -> RiskContext:
    return RiskContext(
        now=now,
        open_position_symbols=frozenset(),
        active_halts=(),
        risk_config=_risk_config(max_order_notional=Decimal("100")),
        strategy_state=strategy_state,
    )




def _intent(
    reduce_only: bool = False,
    desired_notional: Decimal = Decimal("50"),
) -> OrderIntentCandidate:
    now = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    return OrderIntentCandidate(
        candidate_id="candidate-1",
        signal_id="signal-1",
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v0",
        config_hash="a" * 64,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=desired_notional,
        reduce_only=reduce_only,
        expires_at=now + timedelta(seconds=30),
        created_at=now,
        reason="test",
        features={},
    )
