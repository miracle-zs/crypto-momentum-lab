from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest

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
            ),
            TrackedMembership(
                "SOLUSDT",
                MembershipStatus.EXTENDED,
                RankingSide.GAINER,
            ),
            TrackedMembership(
                "XRPUSDT",
                MembershipStatus.FORCED,
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


async def test_top30_selector_propagates_position_state_load_failure() -> None:
    from unittest.mock import AsyncMock

    from crypto_momentum_lab.research_collector.selection import (
        PostgresTop30Selector,
    )

    observed_at = datetime(2026, 7, 3, 12, 0, tzinfo=UTC)
    snapshot = UniverseSnapshot(
        snapshot_id=uuid4(),
        observed_at=observed_at,
        utc_day=observed_at.date(),
        config_hash="a" * 64,
        activated=True,
        ranking=RankingResult(
            candidates=(),
            gainers=(
                RankEntry("BTCUSDT", Decimal("0.10"), 1, RankingSide.GAINER),
            ),
            losers=(),
            target_symbols=frozenset({"BTCUSDT"}),
            exclusions={},
        ),
        memberships=(),
    )

    universe_repo = AsyncMock()
    universe_repo.load_snapshot_at = AsyncMock(return_value=snapshot)

    account_repo = AsyncMock()
    # 1. First call: returns SOLUSDT position successfully
    account_repo.load_active_position_state = AsyncMock(
        return_value=SimpleNamespace(symbols=frozenset({"SOLUSDT"}))
    )

    selector = PostgresTop30Selector(
        universe_repository=universe_repo,
        account_repository=account_repo,
        environment="research",
        account_label="paper-account",
        refresh_interval_seconds=3600,
    )

    s1 = await selector.selection_at(observed_at)
    assert "SOLUSDT" in s1.by_symbol

    # 2. Advance time past cache expiry. Position discovery now raises RuntimeError!
    t2 = observed_at + timedelta(seconds=3601)
    account_repo.load_active_position_state = AsyncMock(
        side_effect=RuntimeError("missing active position snapshots")
    )

    with pytest.raises(RuntimeError, match="missing active position snapshots"):
        await selector.selection_at(t2)
