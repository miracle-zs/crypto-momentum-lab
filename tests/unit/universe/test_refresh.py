from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.config.models import UniverseConfig
from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
    MembershipStatus,
    PricePoint,
)
from crypto_momentum_lab.universe.refresh import UniverseRefreshService


class FakeObligations:
    def __init__(self, symbols: frozenset[str]) -> None:
        self._symbols = symbols

    async def forced_symbols(self) -> frozenset[str]:
        return self._symbols


class FakeSnapshotObserver:
    def __init__(self) -> None:
        self.snapshots = []

    async def snapshot_updated(self, snapshot) -> None:
        self.snapshots.append(snapshot)


def build_service(
    market_data,
    repository,
    *,
    obligations=None,
    observer=None,
) -> UniverseRefreshService:
    return UniverseRefreshService(
        market_data=market_data,
        repository=repository,
        config=UniverseConfig(
            top_count=20,
            retention_rank=30,
            retention_hours=2,
            activation_minute=1,
        ),
        config_hash="a" * 64,
        obligations=obligations,
        observer=observer,
    )


async def test_refresh_persists_top_bottom_and_fetches_only_missing_opens(
    fake_market_data,
    fake_repository,
) -> None:
    observed_at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    fake_repository.daily_opens = {"AAAUSDT": Decimal("100")}
    fake_market_data.contracts = tuple(
        ContractMetadata(
            symbol=symbol,
            contract_type="PERPETUAL",
            status="TRADING",
            quote_asset="USDT",
            margin_asset="USDT",
            onboard_at=observed_at,
            raw={},
        )
        for symbol in ("AAAUSDT", "BBBUSDT", "CCCUSDT")
    )
    fake_market_data.prices = {
        "AAAUSDT": PricePoint("AAAUSDT", Decimal("110"), observed_at),
        "BBBUSDT": PricePoint("BBBUSDT", Decimal("90"), observed_at),
        "CCCUSDT": PricePoint("CCCUSDT", Decimal("105"), observed_at),
    }
    fake_market_data.opens = (
        DailyOpen(
            "BBBUSDT",
            date(2026, 6, 14),
            Decimal("100"),
            datetime(2026, 6, 14, tzinfo=UTC),
        ),
        DailyOpen(
            "CCCUSDT",
            date(2026, 6, 14),
            Decimal("100"),
            datetime(2026, 6, 14, tzinfo=UTC),
        ),
    )
    service = UniverseRefreshService(
        market_data=fake_market_data,
        repository=fake_repository,
        config=UniverseConfig(
            top_count=1,
            retention_rank=2,
            retention_hours=2,
            activation_minute=1,
        ),
        config_hash="a" * 64,
    )

    snapshot = await service.refresh(observed_at=observed_at)

    assert snapshot.ranking.target_symbols == frozenset(
        {"AAAUSDT", "BBBUSDT"}
    )
    assert fake_market_data.requested_open_symbols == frozenset(
        {"BBBUSDT", "CCCUSDT"}
    )
    assert fake_repository.saved_snapshot == snapshot


async def test_midnight_snapshot_is_recorded_but_not_activated(
    fake_market_data,
    fake_repository,
) -> None:
    observed_at = datetime(2026, 6, 15, 0, 1, tzinfo=UTC)
    fake_market_data.seed_single_symbol(observed_at)
    service = build_service(fake_market_data, fake_repository)

    snapshot = await service.refresh(observed_at=observed_at)

    assert snapshot.activated is False
    assert snapshot.memberships == ()


async def test_rejects_naive_refresh_time(
    fake_market_data,
    fake_repository,
) -> None:
    service = build_service(fake_market_data, fake_repository)
    with pytest.raises(ValueError, match="timezone-aware"):
        await service.refresh(observed_at=datetime(2026, 6, 14, 11, 1))


async def test_repeated_refresh_has_same_snapshot_id(
    fake_market_data,
    fake_repository,
) -> None:
    at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    fake_market_data.seed_single_symbol(at)
    service = build_service(fake_market_data, fake_repository)

    first = await service.refresh(observed_at=at)
    second = await service.refresh(observed_at=at)

    assert first.snapshot_id == second.snapshot_id


async def test_missing_price_is_recorded_as_exclusion(
    fake_market_data,
    fake_repository,
) -> None:
    at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    fake_market_data.seed_single_symbol(at)
    fake_market_data.prices = {}
    service = build_service(fake_market_data, fake_repository)

    snapshot = await service.refresh(observed_at=at)

    assert snapshot.ranking.exclusions == {
        "BTCUSDT": "missing_current_price"
    }


async def test_forced_symbol_outside_ranking_remains_monitored(
    fake_market_data,
    fake_repository,
) -> None:
    at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    fake_market_data.seed_single_symbol(at)
    service = build_service(
        fake_market_data,
        fake_repository,
        obligations=FakeObligations(frozenset({"DELISTEDUSDT"})),
    )

    snapshot = await service.refresh(observed_at=at)
    forced = next(
        item
        for item in snapshot.memberships
        if item.symbol == "DELISTEDUSDT"
    )

    assert forced.status is MembershipStatus.FORCED


async def test_0101_activates_after_midnight_snapshot(
    fake_market_data,
    fake_repository,
) -> None:
    midnight = datetime(2026, 6, 15, 0, 1, tzinfo=UTC)
    fake_market_data.seed_single_symbol(midnight)
    service = build_service(fake_market_data, fake_repository)
    first = await service.refresh(observed_at=midnight)

    one_am = datetime(2026, 6, 15, 1, 1, tzinfo=UTC)
    fake_market_data.seed_single_symbol(one_am)
    second = await service.refresh(observed_at=one_am)

    assert first.activated is False
    assert second.activated is True
    assert len(second.memberships) == 1


async def test_activated_snapshot_updates_subscriptions(
    fake_market_data,
    fake_repository,
) -> None:
    at = datetime(2026, 6, 15, 2, 1, tzinfo=UTC)
    fake_market_data.seed_single_symbol(at)
    observer = FakeSnapshotObserver()
    service = build_service(
        fake_market_data,
        fake_repository,
        observer=observer,
    )

    snapshot = await service.refresh(observed_at=at)

    assert observer.snapshots == [snapshot]


async def test_midnight_snapshot_does_not_update_subscriptions(
    fake_market_data,
    fake_repository,
) -> None:
    at = datetime(2026, 6, 15, 0, 1, tzinfo=UTC)
    fake_market_data.seed_single_symbol(at)
    observer = FakeSnapshotObserver()
    service = build_service(
        fake_market_data,
        fake_repository,
        observer=observer,
    )

    await service.refresh(observed_at=at)

    assert observer.snapshots == []
