from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID


class RankingSide(StrEnum):
    GAINER = "gainer"
    LOSER = "loser"


class MembershipStatus(StrEnum):
    TARGET = "target"
    RETAINED = "retained"
    FORCED = "forced"


@dataclass(frozen=True, slots=True)
class ContractMetadata:
    symbol: str
    contract_type: str
    status: str
    quote_asset: str
    margin_asset: str
    onboard_at: datetime
    raw: dict[str, object]


@dataclass(frozen=True, slots=True)
class DailyOpen:
    symbol: str
    utc_day: date
    open_price: Decimal
    open_time: datetime


@dataclass(frozen=True, slots=True)
class PricePoint:
    symbol: str
    price: Decimal
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class MarketCandidate:
    symbol: str
    open_price: Decimal | None
    current_price: Decimal | None
    price_time: datetime | None


@dataclass(frozen=True, slots=True)
class RankEntry:
    symbol: str
    utc_day_return: Decimal
    rank: int
    side: RankingSide


@dataclass(frozen=True, slots=True)
class RankingResult:
    candidates: tuple[MarketCandidate, ...]
    gainers: tuple[RankEntry, ...]
    losers: tuple[RankEntry, ...]
    target_symbols: frozenset[str]
    exclusions: dict[str, str]


@dataclass(frozen=True, slots=True)
class TrackedMembership:
    symbol: str
    status: MembershipStatus
    side: RankingSide | None
    left_target_at: datetime | None


@dataclass(frozen=True, slots=True)
class UniverseSnapshot:
    snapshot_id: UUID
    observed_at: datetime
    utc_day: date
    config_hash: str
    activated: bool
    ranking: RankingResult
    memberships: tuple[TrackedMembership, ...]
