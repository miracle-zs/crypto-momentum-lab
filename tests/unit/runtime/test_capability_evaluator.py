"""Unit tests for CapabilityEvaluator per-action safety gates (R5).

Tests:
1. RECONCILE is always permitted regardless of degradation;
2. CANCEL is permitted under active lease and verified identity even when
   market is stale or ledger is discordant;
3. NORMAL_EXIT is blocked by batch attribution conflict, inflight orders,
   or excessive market staleness, but NOT blocked by expired entry approval;
4. ENTER requires valid approval, concordant ledger, zero inflight orders,
   ready universe, fresh market, and matching schema compatibility;
5. EMERGENCY_REDUCE requires explicit emergency authorization and writer lease;
6. Context and identity binding:
   scope, plan_hash, runtime_generation, fencing_epoch, source_as_of;
7. Plan hash mismatch and fencing epoch mismatch fail closed for mutating actions.
"""

from datetime import UTC, datetime

from crypto_momentum_lab.domain.execution.execution_book import ExecutionScope
from crypto_momentum_lab.domain.runtime.capability_evaluator import (
    CapabilityEvaluator,
    CapabilityEvidence,
    SystemAction,
)
from crypto_momentum_lab.domain.runtime.runtime_plan import (
    RuntimePlan,
    RuntimePlanCompiler,
)


def _make_plan() -> RuntimePlan:
    return RuntimePlanCompiler.compile(
        environment="live",
        account_label="binance_primary",
        git_commit="abcdef",
        schema_version="20260925_0043",
        fencing_epoch=2,
    )


def test_reconcile_always_allowed() -> None:
    evaluator = CapabilityEvaluator()
    plan = _make_plan()

    # Even with terrible operational state, RECONCILE must be allowed
    evidence = CapabilityEvidence(
        evidence_version="ev_01",
        market_freshness_seconds=9999.0,
        is_account_concordant=False,
        is_account_identity_verified=False,
        unresolved_inflight_orders_count=10,
        is_approval_valid=False,
        is_lease_active=False,
        observed_at=datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC),
    )

    decision = evaluator.evaluate(SystemAction.RECONCILE, evidence, plan)
    assert decision.allowed is True
    assert decision.reason == "reconcile_always_permitted"


def test_cancel_never_blocked_by_stale_market_or_batch_conflict() -> None:
    evaluator = CapabilityEvaluator()
    plan = _make_plan()

    # Market is stale (300s) and ledger is discordant, but lease and identity are valid
    evidence = CapabilityEvidence(
        evidence_version="ev_02",
        market_freshness_seconds=300.0,
        is_account_concordant=False,
        is_account_identity_verified=True,
        unresolved_inflight_orders_count=2,
        is_approval_valid=False,
        is_lease_active=True,
        is_collector_healthy=False,
        observed_at=datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC),
    )

    decision = evaluator.evaluate(SystemAction.CANCEL, evidence, plan)
    assert decision.allowed is True
    assert decision.reason == "cancel_permitted_under_active_lease"

    # However, if lease is inactive, CANCEL must be blocked
    no_lease_evidence = CapabilityEvidence(
        evidence_version="ev_03",
        market_freshness_seconds=1.0,
        is_account_concordant=True,
        is_lease_active=False,
    )
    decision_no_lease = evaluator.evaluate(SystemAction.CANCEL, no_lease_evidence, plan)
    assert decision_no_lease.allowed is False
    assert decision_no_lease.reason == "writer_lease_inactive"


def test_normal_exit_gates() -> None:
    evaluator = CapabilityEvaluator(max_exit_market_age_seconds=60.0)
    plan = _make_plan()

    # 1. Blocked when batch attribution has conflict or gap
    discordant_ev = CapabilityEvidence(
        evidence_version="ev_04",
        market_freshness_seconds=5.0,
        is_account_concordant=False,
    )
    dec1 = evaluator.evaluate(SystemAction.NORMAL_EXIT, discordant_ev, plan)
    assert dec1.allowed is False
    assert dec1.reason == "batch_attribution_conflict_or_gap"

    # 2. Blocked when unresolved inflight orders exist
    inflight_ev = CapabilityEvidence(
        evidence_version="ev_05",
        market_freshness_seconds=5.0,
        is_account_concordant=True,
        unresolved_inflight_orders_count=1,
    )
    dec2 = evaluator.evaluate(SystemAction.NORMAL_EXIT, inflight_ev, plan)
    assert dec2.allowed is False
    assert dec2.reason == "unresolved_inflight_orders_present"

    # 3. Blocked when market data exceeds exit staleness threshold
    stale_ev = CapabilityEvidence(
        evidence_version="ev_06",
        market_freshness_seconds=90.0,
        is_account_concordant=True,
        unresolved_inflight_orders_count=0,
    )
    dec3 = evaluator.evaluate(SystemAction.NORMAL_EXIT, stale_ev, plan)
    assert dec3.allowed is False
    assert dec3.reason == "market_data_too_stale_for_normal_exit"

    # 4. Allowed when all prerequisites satisfied
    # (even if live approval expired or schema outdated!)
    valid_exit_ev = CapabilityEvidence(
        evidence_version="ev_07",
        market_freshness_seconds=10.0,
        is_account_concordant=True,
        unresolved_inflight_orders_count=0,
        is_approval_valid=False,  # Normal exit does not require entry approval
        observed_database_revision="20260911_0036",  # Old schema does not block exit!
    )
    dec4 = evaluator.evaluate(SystemAction.NORMAL_EXIT, valid_exit_ev, plan)
    assert dec4.allowed is True
    assert dec4.reason == "normal_exit_prerequisites_satisfied"


def test_enter_strict_gates() -> None:
    evaluator = CapabilityEvaluator(max_entry_market_age_seconds=15.0)
    plan = _make_plan()

    # 1. Blocked if approval invalid
    no_app_ev = CapabilityEvidence(
        evidence_version="ev_08",
        market_freshness_seconds=2.0,
        is_account_concordant=True,
        is_approval_valid=False,
    )
    assert not evaluator.evaluate(SystemAction.ENTER, no_app_ev, plan).allowed

    # 2. Blocked if market slightly stale (>15s) even if <60s
    stale_entry_ev = CapabilityEvidence(
        evidence_version="ev_09",
        market_freshness_seconds=20.0,
        is_account_concordant=True,
        is_approval_valid=True,
    )
    dec_stale = evaluator.evaluate(SystemAction.ENTER, stale_entry_ev, plan)
    assert dec_stale.allowed is False
    assert dec_stale.reason == "market_data_stale_for_entry"

    # 3. Blocked if database schema revision mismatches declared plan compatibility
    mismatch_schema_ev = CapabilityEvidence(
        evidence_version="ev_schema",
        market_freshness_seconds=3.0,
        is_account_concordant=True,
        is_approval_valid=True,
        is_universe_ready=True,
        observed_database_revision="20260911_0036",  # Different from 20260925_0043
    )
    dec_schema = evaluator.evaluate(SystemAction.ENTER, mismatch_schema_ev, plan)
    assert dec_schema.allowed is False
    assert dec_schema.reason == "schema_compatibility_mismatch"

    # 4. Allowed when all healthy
    healthy_ev = CapabilityEvidence(
        evidence_version="ev_10",
        market_freshness_seconds=3.0,
        is_account_concordant=True,
        is_approval_valid=True,
        is_universe_ready=True,
        unresolved_inflight_orders_count=0,
        observed_database_revision="20260925_0043",
    )
    dec_ok = evaluator.evaluate(SystemAction.ENTER, healthy_ev, plan)
    assert dec_ok.allowed is True
    assert dec_ok.reason == "entry_prerequisites_satisfied"


def test_emergency_reduce_authorization() -> None:
    evaluator = CapabilityEvaluator()
    plan = _make_plan()

    unauth_ev = CapabilityEvidence(
        evidence_version="ev_11",
        market_freshness_seconds=120.0,
        is_account_concordant=False,
        is_emergency_authorized=False,
    )
    assert not evaluator.evaluate(
        SystemAction.EMERGENCY_REDUCE, unauth_ev, plan
    ).allowed

    auth_ev = CapabilityEvidence(
        evidence_version="ev_12",
        market_freshness_seconds=120.0,
        is_account_concordant=False,
        is_emergency_authorized=True,
    )
    dec_em = evaluator.evaluate(SystemAction.EMERGENCY_REDUCE, auth_ev, plan)
    assert dec_em.allowed is True
    assert dec_em.reason == "emergency_reduce_authorized"


def test_evidence_and_decision_context_binding() -> None:
    """CapabilityDecision binds scope, plan_hash, runtime_generation, and epoch."""
    evaluator = CapabilityEvaluator()
    plan = _make_plan()
    scope = ExecutionScope(
        environment="live",
        account_label="binance_primary",
        symbol="BTCUSDT",
    )
    t_source = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)

    evidence = CapabilityEvidence(
        evidence_version="ev_bind",
        market_freshness_seconds=2.0,
        is_account_concordant=True,
        scope=scope,
        plan_hash=plan.plan_hash,
        fencing_epoch=2,
        source_as_of=t_source,
    )

    dec = evaluator.evaluate(SystemAction.ENTER, evidence, plan)
    assert dec.allowed is True
    assert dec.scope == scope
    assert dec.plan_hash == plan.plan_hash
    assert dec.runtime_generation == plan.runtime_generation
    assert dec.fencing_epoch == 2
    assert dec.source_as_of == t_source


def test_plan_hash_and_fencing_epoch_mismatch_blocks_actions() -> None:
    evaluator = CapabilityEvaluator()
    plan = _make_plan()

    # Mismatched fencing epoch
    epoch_mismatch_ev = CapabilityEvidence(
        evidence_version="ev_epoch_bad",
        market_freshness_seconds=2.0,
        is_account_concordant=True,
        fencing_epoch=99,  # plan is 2
    )
    dec_epoch = evaluator.evaluate(SystemAction.ENTER, epoch_mismatch_ev, plan)
    assert dec_epoch.allowed is False
    assert dec_epoch.reason == "fencing_epoch_mismatch"

    # Mismatched plan hash
    hash_mismatch_ev = CapabilityEvidence(
        evidence_version="ev_hash_bad",
        market_freshness_seconds=2.0,
        is_account_concordant=True,
        plan_hash="corrupted_hash",
    )
    dec_hash = evaluator.evaluate(SystemAction.ENTER, hash_mismatch_ev, plan)
    assert dec_hash.allowed is False
    assert dec_hash.reason == "plan_hash_mismatch"
