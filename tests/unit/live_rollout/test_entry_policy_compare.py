from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.strategy import (
    EntryPolicyComparisonRequest,
    EntryType,
    OrderIntentCandidate,
    StrategySide,
    compare_entry_policy_request,
    summarize_entry_policy_comparisons,
)
from crypto_momentum_lab.live_rollout.entry_policy_compare import (
    compare_entry_candidate,
    universe_snapshot_for_symbols,
)

NOW = datetime(2026, 9, 5, 0, 0, tzinfo=UTC)


def test_compare_matches_for_candidate_inside_pool_and_ema_boundary() -> None:
    comparison = compare_entry_candidate(
        _candidate(),
        source_trace_id="state-trace-1",
        legacy_rejection_reason=None,
        gate_reasons=(),
        entry_enabled=True,
        entry_long_only=False,
        entry_symbols=frozenset({"BTCUSDT"}),
        entry_price=Decimal("101"),
        ema5=Decimal("100"),
        ema10=Decimal("99"),
        require_price_above_ema5=True,
        require_price_above_ema10=True,
        observed_at=NOW,
        ema_observed_at=NOW,
        ema_snapshot_id="ema-1",
        ema_config_hash="b" * 64,
    )

    assert comparison.matched
    assert comparison.policy_decision.eligible
    assert comparison.as_details()["source_trace_id"] == "state-trace-1"


def test_comparison_request_is_the_replayable_input_seam() -> None:
    request = EntryPolicyComparisonRequest(
        candidate=_candidate(),
        source_trace_id="state-trace-request",
        legacy_rejection_reason=None,
        gate_reasons=(),
        entry_enabled=True,
        entry_long_only=False,
        entry_symbols=frozenset({"BTCUSDT"}),
        entry_price=Decimal("101"),
        ema5=Decimal("100"),
        ema10=Decimal("99"),
        require_price_above_ema5=True,
        require_price_above_ema10=True,
        observed_at=NOW,
        ema_observed_at=NOW,
    )

    comparison = compare_entry_policy_request(request)

    assert comparison.matched
    assert comparison.policy_decision.eligible


def test_comparison_request_rejects_naive_observed_at() -> None:
    try:
        EntryPolicyComparisonRequest(
            candidate=_candidate(),
            source_trace_id="state-trace-naive",
            legacy_rejection_reason=None,
            gate_reasons=(),
            entry_enabled=True,
            entry_long_only=False,
            entry_symbols=None,
            entry_price=None,
            ema5=None,
            ema10=None,
            require_price_above_ema5=False,
            require_price_above_ema10=False,
            observed_at=datetime(2026, 9, 5, 0, 0),
        )
    except ValueError as error:
        assert str(error) == "observed_at must be timezone-aware"
    else:
        raise AssertionError("naive observed_at should be rejected")


def test_compare_preserves_legacy_pool_semantics() -> None:
    comparison = compare_entry_candidate(
        _candidate(),
        source_trace_id="state-trace-2",
        legacy_rejection_reason="outside_entry_symbol_pool",
        gate_reasons=(),
        entry_enabled=True,
        entry_long_only=False,
        entry_symbols=frozenset(),
        entry_price=None,
        ema5=None,
        ema10=None,
        require_price_above_ema5=False,
        require_price_above_ema10=False,
        observed_at=NOW,
    )

    assert comparison.matched
    assert comparison.policy_decision.reasons == ("outside_entry_universe",)


def test_compare_distinguishes_unconfigured_pool_from_empty_pool() -> None:
    comparison = compare_entry_candidate(
        _candidate(),
        source_trace_id="state-trace-3",
        legacy_rejection_reason=None,
        gate_reasons=(),
        entry_enabled=True,
        entry_long_only=False,
        entry_symbols=None,
        entry_price=None,
        ema5=None,
        ema10=None,
        require_price_above_ema5=False,
        require_price_above_ema10=False,
        observed_at=NOW,
    )

    assert comparison.matched
    assert comparison.policy_decision.eligible


def test_compare_reports_fail_closed_ema_without_changing_legacy_path() -> None:
    comparison = compare_entry_candidate(
        _candidate(),
        source_trace_id="state-trace-4",
        legacy_rejection_reason="ema_filter_failed",
        gate_reasons=(),
        entry_enabled=True,
        entry_long_only=False,
        entry_symbols=frozenset({"BTCUSDT"}),
        entry_price=None,
        ema5=None,
        ema10=None,
        require_price_above_ema5=True,
        require_price_above_ema10=False,
        observed_at=NOW,
    )

    assert comparison.matched
    assert comparison.policy_decision.reasons == ("ema_unavailable",)


def test_compare_exposes_stale_ema_from_snapshot_timestamp() -> None:
    comparison = compare_entry_candidate(
        _candidate(),
        source_trace_id="state-trace-5",
        legacy_rejection_reason=None,
        gate_reasons=(),
        entry_enabled=True,
        entry_long_only=False,
        entry_symbols=frozenset({"BTCUSDT"}),
        entry_price=Decimal("101"),
        ema5=Decimal("100"),
        ema10=None,
        require_price_above_ema5=True,
        require_price_above_ema10=False,
        observed_at=NOW,
        ema_observed_at=NOW - timedelta(minutes=16),
        ema_snapshot_id="ema-stale",
        ema_config_hash="c" * 64,
    )

    assert not comparison.matched
    assert comparison.policy_decision.reasons == ("ema_stale",)


def test_comparison_summary_matches_replay_shape_and_stays_bounded() -> None:
    matched = compare_entry_candidate(
        _candidate(),
        source_trace_id="state-trace-summary-match",
        legacy_rejection_reason=None,
        gate_reasons=(),
        entry_enabled=True,
        entry_long_only=False,
        entry_symbols=frozenset({"BTCUSDT"}),
        entry_price=Decimal("101"),
        ema5=Decimal("100"),
        ema10=None,
        require_price_above_ema5=True,
        require_price_above_ema10=False,
        observed_at=NOW,
        ema_observed_at=NOW,
    )
    mismatched = compare_entry_candidate(
        _candidate(),
        source_trace_id="state-trace-summary-mismatch",
        legacy_rejection_reason=None,
        gate_reasons=(),
        entry_enabled=True,
        entry_long_only=False,
        entry_symbols=frozenset({"BTCUSDT"}),
        entry_price=Decimal("101"),
        ema5=Decimal("100"),
        ema10=None,
        require_price_above_ema5=True,
        require_price_above_ema10=False,
        observed_at=NOW,
        ema_observed_at=NOW - timedelta(minutes=16),
    )

    summary = summarize_entry_policy_comparisons(
        (matched, mismatched),
        reduce_only_skipped=1,
    )

    assert summary.as_summary() == {
        "candidates": 2,
        "matched": 1,
        "mismatched": 1,
        "legacy_eligible": 2,
        "policy_eligible": 1,
        "reduce_only_skipped": 1,
    }
    assert summary.as_details()["policy_reasons"] == {"ema_stale": 1}
    assert summary.as_details()["mismatch_reasons"] == {"ema_stale": 1}


def test_compare_preserves_universe_snapshot_identity_and_config() -> None:
    universe = universe_snapshot_for_symbols(
        frozenset({"BTCUSDT"}),
        observed_at=NOW - timedelta(seconds=15),
        snapshot_id="universe-42",
        config_hash="d" * 64,
    )
    assert universe.snapshot_id == "universe-42"
    assert universe.observed_at == NOW - timedelta(seconds=15)
    assert universe.config_hash == "d" * 64
    comparison = compare_entry_candidate(
        _candidate(),
        source_trace_id="state-trace-6",
        legacy_rejection_reason=None,
        gate_reasons=(),
        entry_enabled=True,
        entry_long_only=False,
        entry_symbols=frozenset({"BTCUSDT"}),
        entry_price=Decimal("101"),
        ema5=None,
        ema10=None,
        require_price_above_ema5=False,
        require_price_above_ema10=False,
        observed_at=NOW,
        universe_snapshot=universe,
    )

    assert comparison.matched
    assert comparison.policy_decision.eligible


def _candidate() -> OrderIntentCandidate:
    return OrderIntentCandidate(
        candidate_id="candidate-1",
        signal_id="signal-1",
        run_id="run-1",
        strategy_name="strategy",
        strategy_version="v1",
        config_hash="a" * 64,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("25"),
        reduce_only=False,
        expires_at=NOW + timedelta(minutes=1),
        created_at=NOW - timedelta(seconds=1),
        reason="test",
        features={},
    )
