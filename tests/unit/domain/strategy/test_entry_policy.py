from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.strategy.entry_policy import (
    EmaPolicyState,
    EmaSnapshot,
    EntryEligibilityPolicy,
    EntryGateResult,
    PolicyInputSnapshot,
    UniverseRankingEntry,
    UniverseRankingSnapshot,
)
from crypto_momentum_lab.domain.strategy.models import StrategySide

NOW = datetime(2026, 9, 4, 0, 0, tzinfo=UTC)


def _universe(
    *,
    symbol: str = "BTCUSDT",
    direction: StrategySide = StrategySide.LONG,
) -> UniverseRankingSnapshot:
    return UniverseRankingSnapshot(
        snapshot_id="universe-1",
        observed_at=NOW,
        entries=(UniverseRankingEntry(symbol, 1, direction),),
    )


def _input(
    *,
    ema_state: EmaPolicyState | None = None,
    candidate_expiry: datetime = NOW + timedelta(minutes=1),
    gate: EntryGateResult | None = None,
    universe: UniverseRankingSnapshot | None = None,
    entry_enabled: bool = True,
    allow_short: bool = True,
    direction: StrategySide = StrategySide.LONG,
    universe_required: bool = True,
    include_universe: bool = True,
) -> PolicyInputSnapshot:
    return PolicyInputSnapshot(
        symbol="BTCUSDT",
        observed_at=NOW,
        candidate_expiry=candidate_expiry,
        entry_gate_result=gate or EntryGateResult(approved=True),
        direction=direction,
        universe_snapshot=(
            (_universe() if universe is None else universe)
            if include_universe
            else None
        ),
        ema_state=ema_state or EmaPolicyState.disabled(),
        entry_enabled=entry_enabled,
        allow_short=allow_short,
        universe_required=universe_required,
    )


def test_disabled_ema_skips_only_ema_predicate() -> None:
    decision = EntryEligibilityPolicy.evaluate(_input())

    assert decision.eligible
    assert decision.reasons == ()

    blocked = EntryEligibilityPolicy.evaluate(
        _input(
            candidate_expiry=NOW,
            gate=EntryGateResult(approved=False, reasons=("lease_missing",)),
            entry_enabled=False,
        )
    )
    assert not blocked.eligible
    assert blocked.reasons == (
        "candidate_expired",
        "lease_missing",
        "entry_disabled",
    )


def test_unavailable_ema_fails_closed_after_other_predicates() -> None:
    decision = EntryEligibilityPolicy.evaluate(
        _input(ema_state=EmaPolicyState.unavailable())
    )

    assert not decision.eligible
    assert decision.reasons == ("ema_unavailable",)


def test_unconfigured_universe_skips_only_the_universe_predicate() -> None:
    decision = EntryEligibilityPolicy.evaluate(
        _input(universe_required=False, include_universe=False)
    )

    assert decision.eligible
    assert decision.reasons == ()

    unavailable = EntryEligibilityPolicy.evaluate(
        _input(universe_required=True, include_universe=False)
    )
    assert unavailable.reasons == ("universe_unavailable",)


def test_valid_ema_requires_strictly_above_each_enabled_boundary() -> None:
    ema = EmaSnapshot(
        symbol="BTCUSDT",
        observed_at=NOW,
        entry_price=Decimal("30001"),
        ema5=Decimal("30000"),
        ema10=Decimal("29999"),
    )
    state = EmaPolicyState.valid(
        ema,
        require_price_above_ema5=True,
        require_price_above_ema10=True,
    )

    assert EntryEligibilityPolicy.evaluate(_input(ema_state=state)).eligible

    failed = EmaPolicyState.valid(
        EmaSnapshot(
            symbol="BTCUSDT",
            observed_at=NOW,
            entry_price=Decimal("30000"),
            ema5=Decimal("30000"),
            ema10=Decimal("29999"),
        ),
        require_price_above_ema5=True,
        require_price_above_ema10=True,
    )
    decision = EntryEligibilityPolicy.evaluate(_input(ema_state=failed))
    assert decision.reasons == ("ema_filter_failed",)


def test_stale_ema_and_direction_or_universe_failures_are_explicit() -> None:
    stale = EmaPolicyState.valid(
        EmaSnapshot(
            symbol="BTCUSDT",
            observed_at=NOW - timedelta(minutes=16),
            entry_price=Decimal("30001"),
            ema5=Decimal("30000"),
        ),
        require_price_above_ema5=True,
    )
    decision = EntryEligibilityPolicy.evaluate(
        _input(
            ema_state=stale,
            direction=StrategySide.SHORT,
            allow_short=False,
            universe=_universe(direction=StrategySide.LONG),
        )
    )

    assert not decision.eligible
    assert decision.reasons == (
        "short_entries_disabled",
        "outside_entry_universe",
        "ema_stale",
    )


def test_universe_snapshot_has_stable_rank_tie_order() -> None:
    snapshot = UniverseRankingSnapshot(
        snapshot_id="universe-1",
        observed_at=NOW,
        entries=(
            UniverseRankingEntry("ETHUSDT", 1, StrategySide.SHORT),
            UniverseRankingEntry("BTCUSDT", 1, StrategySide.LONG),
        ),
    )

    assert tuple(entry.symbol for entry in snapshot.entries) == (
        "BTCUSDT",
        "ETHUSDT",
    )


def test_policy_input_requires_timezone_aware_timestamps() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        PolicyInputSnapshot(
            symbol="BTCUSDT",
            observed_at=datetime(2026, 9, 4),
            candidate_expiry=NOW + timedelta(minutes=1),
            entry_gate_result=EntryGateResult(approved=True),
            direction=StrategySide.LONG,
            universe_snapshot=_universe(),
            ema_state=EmaPolicyState.disabled(),
        )
