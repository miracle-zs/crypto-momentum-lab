from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from crypto_momentum_lab.domain.universe.models import (
    MembershipStatus,
    RankEntry,
    RankingResult,
    RankingSide,
    TrackedMembership,
    UniverseSnapshot,
)
from crypto_momentum_lab.research_collector.selection import (
    AllSymbolsSelector,
    StaticSymbolSelector,
    _build_selection,
)


async def test_all_symbols_selector_is_explicitly_unbounded() -> None:
    observed_at = datetime(2026, 7, 3, tzinfo=UTC)

    selection = await AllSymbolsSelector().selection_at(observed_at)

    assert selection.retain_all is True
    assert selection.symbols == ()
    assert selection.observed_at == observed_at


async def test_static_selector_preserves_symbols_and_reason() -> None:
    observed_at = datetime(2026, 7, 3, tzinfo=UTC)

    selection = await StaticSymbolSelector(
        frozenset({"ETHUSDT", "BTCUSDT"}),
        reason="top30",
    ).selection_at(observed_at)

    assert tuple(item.symbol for item in selection.symbols) == (
        "BTCUSDT",
        "ETHUSDT",
    )
    assert all(item.reason == "top30" for item in selection.symbols)


def test_top30_selection_keeps_retained_and_open_position_symbols() -> None:
    observed_at = datetime(2026, 7, 3, tzinfo=UTC)
    snapshot = UniverseSnapshot(
        snapshot_id=uuid4(),
        observed_at=observed_at,
        utc_day=observed_at.date(),
        config_hash="a" * 64,
        activated=True,
        ranking=RankingResult(
            candidates=(),
            gainers=(
                RankEntry(
                    "BTCUSDT",
                    Decimal("0.10"),
                    1,
                    RankingSide.GAINER,
                ),
                RankEntry(
                    "ETHUSDT",
                    Decimal("0.02"),
                    35,
                    RankingSide.GAINER,
                ),
            ),
            losers=(),
            target_symbols=frozenset({"BTCUSDT"}),
            exclusions={},
        ),
        memberships=(
            TrackedMembership(
                "ADAUSDT",
                MembershipStatus.RETAINED,
                RankingSide.GAINER,
                observed_at,
            ),
            TrackedMembership(
                "SOLUSDT",
                MembershipStatus.EXTENDED,
                RankingSide.GAINER,
                observed_at,
            ),
            TrackedMembership(
                "XRPUSDT",
                MembershipStatus.FORCED,
                None,
                None,
            ),
        ),
    )

    selection = _build_selection(
        snapshot,
        top_count=30,
        position_symbols=frozenset({"DOGEUSDT"}),
        observed_at=observed_at,
    )

    selected = {item.symbol: item.reason for item in selection.symbols}
    assert selected == {
        "ADAUSDT": MembershipStatus.RETAINED.value,
        "BTCUSDT": "top30",
        "DOGEUSDT": "open_position",
        "XRPUSDT": MembershipStatus.FORCED.value,
    }
