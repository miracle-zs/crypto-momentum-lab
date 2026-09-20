"""Contract test suite for market data dual-revision handling.

Demonstrates and verifies:
1. Revision identity distinction between real-time decision visibility and authoritative canonical storage.
2. Signal divergence: preventing backtest hindsight bias and trade misattribution.
3. Progress state taxonomy (observed vs materialized).
"""

from decimal import Decimal

from tests.fixtures.market_data_dual_revision import (
    build_dual_revision_dataset,
)


def test_dual_revision_identities_are_distinguishable() -> None:
    """Verify that envelopes for the same time bucket are uniquely identifiable."""
    v1, v2 = build_dual_revision_dataset()

    assert v1.symbol == v2.symbol == "BTCUSDT"
    assert v1.bucket_start == v2.bucket_start
    assert v1.bucket_end == v2.bucket_end

    # Must have distinct revisions and canonical flags
    assert v1.revision_id != v2.revision_id
    assert v1.is_canonical is False
    assert v2.is_canonical is True
    assert v1.progress_stage == "observed"
    assert v2.progress_stage == "materialized"


def test_signal_evaluation_divergence_between_revisions() -> None:
    """Demonstrate why trading decisions must reference decision-visible revision.

    Scenario: A breakout strategy enters when close_price > 65010.00.
    - In live real-time (v1), the candle closed at 65000.00 -> NO ENTRY.
    - In subsequent canonical backfill (v2), late ticks brought close to 65020.00.

    If an auditor or replay engine uses canonical data without knowing the revision,
    it would incorrectly diagnose the bot as having 'failed to execute a valid breakout'.
    """
    v1, v2 = build_dual_revision_dataset()
    threshold = Decimal("65010.00")

    # Real-time decision:
    v1_triggered = (v1.state.close_price or Decimal("0")) > threshold
    assert v1_triggered is False, "Live bot correctly did NOT trigger on v1"

    # Canonical evaluation:
    v2_triggered = (v2.state.close_price or Decimal("0")) > threshold
    assert v2_triggered is True, "Canonical backfilled data would have triggered"

    # The DecisionTrace must include revision_id to explain this divergence
    decision_trace = {
        "symbol": v1.symbol,
        "bucket_start": v1.bucket_start.isoformat(),
        "evaluated_revision": v1.revision_id,
        "observed_close": str(v1.state.close_price),
        "triggered": v1_triggered,
    }

    assert decision_trace["evaluated_revision"].endswith("v1_observed")
    assert decision_trace["triggered"] is False


def test_coverage_and_completeness_marking() -> None:
    """Verify data_complete flag correctly reflects repair status."""
    v1, v2 = build_dual_revision_dataset()

    assert v1.state.data_complete is False
    assert v1.state.missing_agg_trade_count == 2

    assert v2.state.data_complete is True
    assert v2.state.missing_agg_trade_count == 0
    assert v2.state.is_backfill is True
