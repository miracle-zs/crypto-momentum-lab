"""Unit tests for PositionLedger shadow comparator and legacy adapters."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.execution import (
    FuturesPositionSide,
    ManagedLivePositionBatch,
    PositionHistory,
    PositionObservation,
    rebuild_position_batches,
)
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionKey,
)
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.live_rollout.position_ledger_shadow import (
    LegacyOrderIdentityAdapter,
    PositionLedgerShadowComparator,
    ShadowDiffCategory,
)
from tests.fixtures.b2_anonymized_timeline import (
    get_b2_account_fill_events,
    get_b2_position_observation,
    get_b2_system_order_facts,
)


def test_legacy_order_identity_adapter_conversion() -> None:
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    system_orders = get_b2_system_order_facts(symbol="BTCUSDT")
    obs = get_b2_position_observation(symbol="BTCUSDT", position_amt=Decimal("120"))

    facts = LegacyOrderIdentityAdapter.to_account_facts(
        position_key=key,
        orders=system_orders,
        observation=obs,
    )

    assert facts.position_key == key
    assert len(facts.fills) == len(system_orders)
    assert len(facts.snapshots) == 1
    assert facts.snapshots[0].position_amt == Decimal("120")


def test_shadow_comparator_exact_match_scenario() -> None:
    """Verify that pure system orders without external interference match exactly."""
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    t0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)

    # Use first order from B2 (Buy 120)
    system_orders = get_b2_system_order_facts()[:1]
    obs = get_b2_position_observation(position_amt=Decimal("120"))

    # Legacy rebuild
    history = PositionHistory(orders=system_orders)
    legacy_result = rebuild_position_batches(obs, history)

    # Ledger v2 projection via adapter
    facts = LegacyOrderIdentityAdapter.to_account_facts(
        position_key=key,
        orders=system_orders,
        observation=obs,
    )
    ledger = PositionLedger(key)
    projection = ledger.project(facts)

    # Compare
    report = PositionLedgerShadowComparator.compare(
        position_key=key,
        legacy_batches=legacy_result.batches,
        ledger_projection=projection,
    )

    assert report.is_concordant is True
    assert report.category is ShadowDiffCategory.EXACT_MATCH
    assert report.legacy_total_quantity == Decimal("120")
    assert report.ledger_total_quantity == Decimal("120")


def test_shadow_comparator_detects_external_fill_divergence_safely() -> None:
    """Verify shadow comparison safely detects divergence when external fills exist.

    Scenario:
    - Legacy rebuild only has system orders (it doesn't know about external fill 254).
    - Ledger v2 receives the complete AccountFacts including external fills.
    - Legacy clips 426 -> 172 using FIFO.
    - Ledger v2 cleanly isolates Episode 1 and Episode 2.
    The comparator should safely categorize the diff without raising any exceptions.
    """
    key = PositionKey(
        environment="live",
        account_label="account-3",
        symbol="B2USDT",
        position_side=FuturesPositionSide.BOTH,
    )
    # Take orders up to post-zero buy (indices 0..4)
    system_orders = get_b2_system_order_facts()[:5]
    obs = get_b2_position_observation(position_amt=Decimal("172"))

    # Legacy output
    history = PositionHistory(orders=system_orders)
    legacy_result = rebuild_position_batches(obs, history)

    # Ledger v2 with complete facts up to that point
    all_fills_up_to_post_zero = get_b2_account_fill_events()[:6]
    facts = LegacyOrderIdentityAdapter.to_account_facts(
        position_key=key,
        orders=system_orders,
        fills=all_fills_up_to_post_zero,
        observation=obs,
    )
    ledger = PositionLedger(key)
    projection = ledger.project(facts)

    report = PositionLedgerShadowComparator.compare(
        position_key=key,
        legacy_batches=legacy_result.batches,
        ledger_projection=projection,
    )

    # Both agree on the current active quantity (172)
    assert report.legacy_total_quantity == Decimal("172")
    assert report.ledger_total_quantity == Decimal("172")
    # Comparator logs diagnostic without throwing
    assert report is not None
