from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.universe.membership import (
    build_monitoring_memberships,
)
from crypto_momentum_lab.domain.universe.models import (
    MembershipStatus,
    RankEntry,
    RankingResult,
    RankingSide,
    TrackedMembership,
)

NOW = datetime(2026, 6, 14, 12, 1, tzinfo=UTC)


def result(
    gainers: list[str],
    losers: list[str],
) -> RankingResult:
    gain_entries = tuple(
        RankEntry(symbol, Decimal("0.1"), rank, RankingSide.GAINER)
        for rank, symbol in enumerate(gainers, start=1)
    )
    loss_entries = tuple(
        RankEntry(symbol, Decimal("-0.1"), rank, RankingSide.LOSER)
        for rank, symbol in enumerate(losers, start=1)
    )
    return RankingResult(
        candidates=(),
        gainers=gain_entries,
        losers=loss_entries,
        target_symbols=frozenset(gainers[:2] + losers[:2]),
        exclusions={},
    )


def test_current_target_is_immediately_monitored() -> None:
    memberships = build_monitoring_memberships(
        result(["A", "B", "C"], ["X", "Y", "Z"]),
        previous={},
        forced_symbols=frozenset(),
        observed_at=NOW,
        retention_rank=3,
        retention_duration=timedelta(hours=2),
    )

    assert memberships["A"].status is MembershipStatus.TARGET
    assert memberships["X"].status is MembershipStatus.TARGET


def test_symbol_is_retained_until_time_limit_is_reached() -> None:
    previous = {
        "A": TrackedMembership(
            symbol="A",
            status=MembershipStatus.TARGET,
            side=RankingSide.GAINER,
            left_target_at=None,
        )
    }
    first_exit = build_monitoring_memberships(
        result(["B", "C", "A"], ["X", "Y", "Z"]),
        previous=previous,
        forced_symbols=frozenset(),
        observed_at=NOW,
        retention_rank=3,
        retention_duration=timedelta(hours=2),
    )
    expired = build_monitoring_memberships(
        result(["B", "C", "A"], ["X", "Y", "Z"]),
        previous=first_exit,
        forced_symbols=frozenset(),
        observed_at=NOW + timedelta(hours=2),
        retention_rank=3,
        retention_duration=timedelta(hours=2),
    )

    assert first_exit["A"].status is MembershipStatus.RETAINED
    assert first_exit["A"].left_target_at == NOW
    assert "A" not in expired


def test_symbol_is_removed_when_it_leaves_retention_rank() -> None:
    previous = {
        "A": TrackedMembership(
            symbol="A",
            status=MembershipStatus.RETAINED,
            side=RankingSide.GAINER,
            left_target_at=NOW - timedelta(hours=1),
        )
    }

    memberships = build_monitoring_memberships(
        result(["B", "C", "D", "A"], ["X", "Y", "Z"]),
        previous=previous,
        forced_symbols=frozenset(),
        observed_at=NOW,
        retention_rank=3,
        retention_duration=timedelta(hours=2),
    )

    assert "A" not in memberships


def test_forced_symbol_is_monitored_without_ranking_membership() -> None:
    memberships = build_monitoring_memberships(
        result(["A", "B"], ["X", "Y"]),
        previous={},
        forced_symbols=frozenset({"POSITIONUSDT"}),
        observed_at=NOW,
        retention_rank=3,
        retention_duration=timedelta(hours=2),
    )

    assert memberships["POSITIONUSDT"].status is MembershipStatus.FORCED
