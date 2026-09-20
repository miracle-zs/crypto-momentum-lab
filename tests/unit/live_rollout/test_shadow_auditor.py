"""Unit tests for LiveExecutionShadowAuditor."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.execution import OrderExecutionPlan
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    StrategySide,
)
from crypto_momentum_lab.execution_account.orders.ids import (
    deterministic_client_order_id,
)
from crypto_momentum_lab.execution_account.orders.quantization import (
    SymbolTradingRules,
    quantize_order_plan,
)
from crypto_momentum_lab.live_rollout.exits import ManagedLivePosition
from crypto_momentum_lab.live_rollout.shadow_auditor import (
    LiveExecutionShadowAuditor,
    ShadowAuditResult,
)
from tests.unit.shadow_operation.test_service import _intent


def _rules() -> SymbolTradingRules:
    return SymbolTradingRules(
        symbol="BTCUSDT",
        min_notional=Decimal("5.0"),
        min_quantity=Decimal("0.001"),
        max_quantity=Decimal("1000"),
        step_size=Decimal("0.001"),
        tick_size=Decimal("0.1"),
    )


def test_shadow_auditor_tracks_monotonic_revisions() -> None:
    rev1 = LiveExecutionShadowAuditor.next_revision()
    rev2 = LiveExecutionShadowAuditor.next_revision()
    assert rev2 > rev1
    stats = LiveExecutionShadowAuditor.get_stats()
    assert stats["revision"] >= rev2


def test_shadow_auditor_audit_submission_concordant() -> None:
    now = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    candidate = replace(
        _intent(),
        candidate_id="cand-1",
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        created_at=now,
        expires_at=now + timedelta(minutes=1),
        reduce_only=False,
    )
    legacy_plan = OrderExecutionPlan(
        intent_id="cand-1",
        run_id="test-run",
        client_order_id=deterministic_client_order_id("test-run", "cand-1"),
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("0.01"),
        price=None,
        reduce_only=False,
        created_at=now,
        position_side=FuturesPositionSide.BOTH,
        quantized=True,
    )

    result = LiveExecutionShadowAuditor.audit_submission(
        candidate=candidate,
        rules=_rules(),
        reference_price=Decimal("50000"),
        legacy_plan=legacy_plan,
        run_id="test-run",
        hedge_mode=False,
        requested_quantity=Decimal("0.01"),
    )

    assert isinstance(result, ShadowAuditResult)
    assert result.success is True
    assert result.is_concordant is True
    assert result.divergence_category is None
    assert result.shadow_plan is not None


def test_shadow_auditor_audit_exit_allocation_concordant() -> None:
    now = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    pos = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        quantity=Decimal("1.0"),
        entry_price=Decimal("50000"),
        opened_at=now,
    )

    result = LiveExecutionShadowAuditor.audit_exit_allocation(
        position=pos,
        order_quantity=Decimal("1.0"),
        reference_price=Decimal("51000"),
        reason="take_profit",
    )

    assert result.success is True
    assert result.is_concordant is True
    assert result.divergence_category is None
    assert result.shadow_command is not None


def test_shadow_auditor_audit_submission_detects_type_price_mismatch() -> None:
    now = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    candidate = replace(
        _intent(),
        candidate_id="cand-1",
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        created_at=now,
        expires_at=now + timedelta(minutes=1),
        reduce_only=False,
    )
    # Shadow generates MARKET order at reference_price=50000.
    # Craft legacy_plan as a LIMIT order at 49000 with matching quantity
    legacy_plan = OrderExecutionPlan(
        intent_id="cand-1",
        run_id="test-run",
        client_order_id=deterministic_client_order_id("test-run", "cand-1"),
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=Decimal("0.01"),
        price=Decimal("49000"),
        reduce_only=False,
        created_at=now,
        position_side=FuturesPositionSide.BOTH,
    )

    result = LiveExecutionShadowAuditor.audit_submission(
        candidate=candidate,
        rules=_rules(),
        reference_price=Decimal("50000"),
        legacy_plan=legacy_plan,
        run_id="test-run",
        hedge_mode=False,
        requested_quantity=Decimal("0.01"),
    )

    assert result.success is True
    # Must NOT be concordant when order_type (LIMIT vs MARKET) or price differs!
    assert result.is_concordant is False
    assert result.divergence_category == "attribute_mismatch"
    assert "order_type" in result.details or "price" in result.details


def test_shadow_auditor_audit_exit_allocation_detects_over_exit_mismatch() -> None:
    now = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    pos = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        quantity=Decimal("1.0"),
        entry_price=Decimal("50000"),
        opened_at=now,
    )

    # Calling with order_quantity 2.0 when position quantity is only 1.0
    result = LiveExecutionShadowAuditor.audit_exit_allocation(
        position=pos,
        order_quantity=Decimal("2.0"),
        reference_price=Decimal("51000"),
        reason="take_profit",
    )

    assert result.success is True
    # Shadow will allocate at most 1.0, which mismatches requested order_quantity 2.0
    assert result.is_concordant is False
    assert result.divergence_category in {"quantity_mismatch", "attribute_mismatch"}


def test_shadow_auditor_audit_submission_detects_identity_mismatch() -> None:
    now = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    candidate = replace(
        _intent(),
        candidate_id="cand-1",
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        created_at=now,
        expires_at=now + timedelta(minutes=1),
        reduce_only=False,
        features={"idempotency_key": "custom_client_id_456"},
    )
    rules = _rules()
    legacy_plan = quantize_order_plan(
        candidate,
        rules,
        reference_price=Decimal("50000"),
        resize_tolerance=Decimal("0.05"),
        requested_quantity=Decimal("0.01"),
    )

    result = LiveExecutionShadowAuditor.audit_submission(
        candidate=candidate,
        rules=rules,
        reference_price=Decimal("50000"),
        legacy_plan=legacy_plan,
        run_id="test-run",
        hedge_mode=False,
        requested_quantity=Decimal("0.01"),
    )

    assert result.success is True
    # When shadow uses custom idempotency_key but legacy uses deterministic ID,
    # it must be flagged as identity_mismatch!
    assert result.is_concordant is False
    assert result.divergence_category == "identity_mismatch"
    assert "client_order_id" in result.details
