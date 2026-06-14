import os
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

import pytest

from crypto_momentum_lab.domain.universe.models import (
    MarketCandidate,
    MembershipStatus,
    RankEntry,
    RankingResult,
    RankingSide,
    TrackedMembership,
    UniverseSnapshot,
)


@pytest.fixture(scope="session")
def database_url() -> str:
    return os.environ.get(
        "CML_TEST_DATABASE_URL",
        "postgresql+psycopg://cml:cml@localhost:54329/cml",
    )


@pytest.fixture
def snapshot_factory() -> Callable[..., UniverseSnapshot]:
    def build(
        *,
        day: int,
        hour: int,
        activated: bool,
        symbol: str,
    ) -> UniverseSnapshot:
        observed_at = datetime(2026, 6, day, hour, 1, tzinfo=UTC)
        day_return = Decimal("0.1")
        candidate = MarketCandidate(
            symbol,
            Decimal("100"),
            Decimal("110"),
            observed_at,
        )
        return UniverseSnapshot(
            snapshot_id=uuid5(
                NAMESPACE_URL,
                f"test:{observed_at.isoformat()}:{symbol}",
            ),
            observed_at=observed_at,
            utc_day=observed_at.date(),
            config_hash="a" * 64,
            activated=activated,
            ranking=RankingResult(
                candidates=(candidate,),
                gainers=(
                    RankEntry(
                        symbol,
                        day_return,
                        1,
                        RankingSide.GAINER,
                    ),
                ),
                losers=(
                    RankEntry(
                        symbol,
                        day_return,
                        1,
                        RankingSide.LOSER,
                    ),
                ),
                target_symbols=frozenset({symbol}),
                exclusions={},
            ),
            memberships=(
                TrackedMembership(
                    symbol,
                    MembershipStatus.TARGET,
                    RankingSide.GAINER,
                    None,
                ),
            ),
        )

    return build
