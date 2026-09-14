from decimal import Decimal

from crypto_momentum_lab.domain.universe.membership import (
    build_monitoring_memberships,
)
from crypto_momentum_lab.domain.universe.models import (
    MembershipStatus,
    RankEntry,
    RankingResult,
    RankingSide,
)


def test_persisted_extended_membership_status_is_supported() -> None:
    assert MembershipStatus("extended") is MembershipStatus.EXTENDED


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
        forced_symbols=frozenset(),
    )

    assert memberships["A"].status is MembershipStatus.TARGET
    assert memberships["X"].status is MembershipStatus.TARGET


def test_extended_memberships_cover_positive_gainers_without_changing_targets() -> None:
    memberships = build_monitoring_memberships(
        result(["A", "B", "C", "D"], ["X", "Y", "Z"]),
        forced_symbols=frozenset(),
        extended_gainer_count=4,
    )

    assert memberships["A"].status is MembershipStatus.TARGET
    assert memberships["B"].status is MembershipStatus.TARGET
    assert memberships["C"].status is MembershipStatus.EXTENDED
    assert memberships["D"].status is MembershipStatus.EXTENDED
    assert "Z" not in memberships


def test_forced_symbol_is_monitored_without_ranking_membership() -> None:
    memberships = build_monitoring_memberships(
        result(["A", "B"], ["X", "Y"]),
        forced_symbols=frozenset({"POSITIONUSDT"}),
    )

    assert memberships["POSITIONUSDT"].status is MembershipStatus.FORCED
