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
    assert result.divergence_category in {
        "attribute_mismatch",
        "attribute_mismatch_price",
        "attribute_mismatch_order_type",
    }
    assert result.severity == "CRITICAL"
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
    assert result.divergence_category in {
        "quantity_mismatch",
        "attribute_mismatch",
        "attribute_mismatch_quantity",
    }
    assert result.severity == "CRITICAL"


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
    assert result.severity == "NON_CRITICAL"
    assert "client_order_id" in result.details


def test_shadow_auditor_metrics_and_classification_summary() -> None:
    LiveExecutionShadowAuditor.reset()
    now = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    candidate = replace(
        _intent(),
        candidate_id="cand-metrics",
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        created_at=now,
        expires_at=now + timedelta(minutes=1),
        reduce_only=False,
    )
    rules = _rules()
    legacy_plan = OrderExecutionPlan(
        intent_id="cand-metrics",
        run_id="test-run",
        client_order_id=deterministic_client_order_id("test-run", "cand-metrics"),
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

    # 1. Concordant audit
    res1 = LiveExecutionShadowAuditor.audit_submission(
        candidate=candidate,
        rules=rules,
        reference_price=Decimal("50000"),
        legacy_plan=legacy_plan,
        run_id="test-run",
        hedge_mode=False,
        requested_quantity=Decimal("0.01"),
    )
    assert res1.is_concordant is True

    # 2. Critical divergence (quantity mismatch)
    mismatch_plan = replace(legacy_plan, quantity=Decimal("0.05"))
    res2 = LiveExecutionShadowAuditor.audit_submission(
        candidate=candidate,
        rules=rules,
        reference_price=Decimal("50000"),
        legacy_plan=mismatch_plan,
        run_id="test-run",
        hedge_mode=False,
        requested_quantity=Decimal("0.01"),
    )
    assert res2.is_concordant is False
    assert res2.severity == "CRITICAL"
    assert res2.divergence_category == "attribute_mismatch_quantity"

    # 3. Non-critical divergence (time_in_force mismatch on limit order)
    limit_candidate = replace(
        candidate,
        candidate_id="cand-limit",
        entry_type=EntryType.LIMIT,
        limit_price=Decimal("50000"),
    )
    limit_legacy = replace(
        legacy_plan,
        intent_id="cand-limit",
        client_order_id=deterministic_client_order_id("test-run", "cand-limit"),
        order_type="LIMIT",
        price=Decimal("50000"),
        time_in_force="IOC",
    )
    res3 = LiveExecutionShadowAuditor.audit_submission(
        candidate=limit_candidate,
        rules=rules,
        reference_price=Decimal("50000"),
        legacy_plan=limit_legacy,
        run_id="test-run",
        hedge_mode=False,
        requested_quantity=Decimal("0.01"),
    )
    assert res3.is_concordant is False
    assert res3.severity == "NON_CRITICAL"
    assert res3.divergence_category == "attribute_mismatch_time_in_force"

    stats = LiveExecutionShadowAuditor.get_stats()
    assert stats["revision"] == 3
    assert stats["failure_count"] == 0
    assert stats["divergence_count"] == 2
    assert stats["audit_count_by_type"]["submission"] == 3
    assert "attribute_mismatch_quantity" in stats["divergence_by_category"]
    assert "attribute_mismatch_time_in_force" in stats["divergence_by_category"]
    assert stats["divergence_by_severity"]["CRITICAL"] == 1
    assert stats["divergence_by_severity"]["NON_CRITICAL"] == 1

    metrics = LiveExecutionShadowAuditor.get_metrics()
    assert metrics["sample_volume"] == 3
    assert metrics["concordant_count"] == 1
    assert abs(metrics["concordance_rate"] - (1 / 3)) < 1e-4
    assert metrics["failure_count"] == 0
    assert metrics["failure_rate"] == 0.0
    assert metrics["divergence_count"] == 2
    assert abs(metrics["divergence_rate"] - (2 / 3)) < 1e-4
    assert metrics["critical_divergence_count"] == 1
    assert abs(metrics["critical_divergence_rate"] - (1 / 3)) < 1e-4
    assert metrics["non_critical_divergence_count"] == 1


def test_shadow_auditor_tracks_audit_failure(monkeypatch) -> None:
    LiveExecutionShadowAuditor.reset()
    now = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    candidate = replace(
        _intent(),
        candidate_id="cand-fail",
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        created_at=now,
        expires_at=now + timedelta(minutes=1),
        reduce_only=False,
    )
    rules = _rules()
    legacy_plan = OrderExecutionPlan(
        intent_id="cand-fail",
        run_id="test-run",
        client_order_id="cid-1",
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("0.01"),
        price=None,
        reduce_only=False,
        created_at=now,
        position_side=FuturesPositionSide.BOTH,
    )

    from crypto_momentum_lab.execution_account.orders.trade_command_executor import (
        TradeCommandExecutor,
    )

    def raise_boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(TradeCommandExecutor, "plan_execution", raise_boom)

    res = LiveExecutionShadowAuditor.audit_submission(
        candidate=candidate,
        rules=rules,
        reference_price=Decimal("50000"),
        legacy_plan=legacy_plan,
        run_id="test-run",
        hedge_mode=False,
        requested_quantity=Decimal("0.01"),
    )
    assert res.success is False
    assert res.is_concordant is False
    assert "boom" in str(res.error)

    stats = LiveExecutionShadowAuditor.get_stats()
    assert stats["failure_count"] == 1
    assert stats["failure_count_by_type"]["submission"] == 1

    metrics = LiveExecutionShadowAuditor.get_metrics()
    assert metrics["sample_volume"] == 1
    assert metrics["failure_count"] == 1
    assert metrics["failure_rate"] == 1.0

