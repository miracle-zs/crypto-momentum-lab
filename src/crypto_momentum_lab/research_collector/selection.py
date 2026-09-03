"""Point-in-time symbol selection adapters for research collection."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.universe.models import (
    MembershipStatus,
    UniverseSnapshot,
)
from crypto_momentum_lab.persistence.postgres.account_repository import (
    PostgresAccountRepository,
)
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.research_collector.models import (
    SelectedSymbol,
    SelectionSnapshot,
    require_utc,
)


class AllSymbolsSelector:
    """Retain every symbol delivered by the Hub.

    This is useful for an audit run and for environments where the Hub already
    exposes exactly the desired universe.
    """

    async def selection_at(self, observed_at: datetime) -> SelectionSnapshot:
        timestamp = require_utc(observed_at, "observed_at")
        # The actual symbols are discovered from the batch by the collector.
        # An empty selection is a deliberate marker; the collector replaces it
        # with the batch symbols when this adapter is used.
        return SelectionSnapshot(
            observed_at=timestamp,
            symbols=(),
            retain_all=True,
        )


class StaticSymbolSelector:
    """Retain a fixed symbol set, mainly for deterministic tests and audits."""

    def __init__(self, symbols: frozenset[str], *, reason: str = "static") -> None:
        if not symbols:
            raise ValueError("symbols must not be empty")
        if any(not symbol.strip() for symbol in symbols):
            raise ValueError("symbols must not contain empty values")
        if not reason.strip():
            raise ValueError("reason must not be empty")
        self._symbols = tuple(sorted(symbols))
        self._reason = reason

    async def selection_at(self, observed_at: datetime) -> SelectionSnapshot:
        timestamp = require_utc(observed_at, "observed_at")
        return SelectionSnapshot(
            observed_at=timestamp,
            symbols=tuple(
                SelectedSymbol(symbol=symbol, reason=self._reason)
                for symbol in self._symbols
            ),
        )


class PostgresTop30Selector:
    """Read the active point-in-time universe and protect open positions.

    The selector deliberately queries the already persisted universe snapshot
    rather than recomputing a rank from market states.  It therefore uses the
    same ranking and retention decisions as the live market-data process.
    """

    def __init__(
        self,
        *,
        universe_repository: PostgresUniverseRepository,
        account_repository: PostgresAccountRepository | None,
        environment: str,
        top_count: int = 30,
        account_label: str | None = None,
        position_environment: str | None = None,
        refresh_interval_seconds: int = 60,
    ) -> None:
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if top_count <= 0:
            raise ValueError("top_count must be positive")
        if refresh_interval_seconds <= 0:
            raise ValueError("refresh_interval_seconds must be positive")
        if account_label is not None and not account_label.strip():
            raise ValueError("account_label must not be empty")
        if account_label is not None and account_repository is None:
            raise ValueError("account_repository is required when account_label is set")
        if position_environment is not None and not position_environment.strip():
            raise ValueError("position_environment must not be empty")
        self._universe_repository = universe_repository
        self._account_repository = account_repository
        self._environment = environment
        self._top_count = top_count
        self._account_label = account_label
        self._position_environment = position_environment or environment
        self._refresh_interval = timedelta(seconds=refresh_interval_seconds)
        self._cached_selection: SelectionSnapshot | None = None
        self._next_refresh_at: datetime | None = None

    async def selection_at(self, observed_at: datetime) -> SelectionSnapshot:
        timestamp = require_utc(observed_at, "observed_at")
        if (
            self._cached_selection is not None
            and self._next_refresh_at is not None
            and self._cached_selection.observed_at <= timestamp
            and timestamp < self._next_refresh_at
        ):
            return self._cached_selection

        universe = await self._universe_repository.load_snapshot_at(timestamp)
        if universe is None:
            raise RuntimeError(
                f"no activated universe snapshot available at {timestamp.isoformat()}"
            )
        position_symbols = await self._load_position_symbols()
        selection = _build_selection(
            universe,
            top_count=self._top_count,
            position_symbols=position_symbols,
            observed_at=timestamp,
        )
        self._cached_selection = selection
        self._next_refresh_at = timestamp + self._refresh_interval
        return selection

    async def _load_position_symbols(self) -> frozenset[str]:
        if self._account_repository is None or self._account_label is None:
            return frozenset()
        return await self._account_repository.load_active_position_symbols(
            environment=self._position_environment,
            account_label=self._account_label,
        )


def _build_selection(
    universe: UniverseSnapshot,
    *,
    top_count: int,
    position_symbols: frozenset[str],
    observed_at: datetime,
) -> SelectionSnapshot:
    gainers = {
        entry.symbol: entry
        for entry in universe.ranking.gainers
        if entry.rank <= top_count and entry.utc_day_return > Decimal("0")
    }
    memberships = {item.symbol: item for item in universe.memberships}
    selected: dict[str, SelectedSymbol] = {}

    for symbol, entry in gainers.items():
        selected[symbol] = SelectedSymbol(
            symbol=symbol,
            reason=f"top{top_count}",
            rank=entry.rank,
            utc_day_return=entry.utc_day_return,
            membership_status=(
                None if symbol not in memberships else memberships[symbol].status.value
            ),
            snapshot_id=universe.snapshot_id,
            snapshot_observed_at=universe.observed_at,
        )

    for symbol, membership in memberships.items():
        if membership.status not in {
            MembershipStatus.RETAINED,
            MembershipStatus.FORCED,
        }:
            continue
        if symbol in selected:
            continue
        selected[symbol] = SelectedSymbol(
            symbol=symbol,
            reason=membership.status.value,
            snapshot_id=universe.snapshot_id,
            snapshot_observed_at=universe.observed_at,
            membership_status=membership.status.value,
        )

    for symbol in sorted(position_symbols):
        if symbol in selected:
            continue
        selected[symbol] = SelectedSymbol(
            symbol=symbol,
            reason="open_position",
            snapshot_id=universe.snapshot_id,
            snapshot_observed_at=universe.observed_at,
            membership_status=MembershipStatus.FORCED.value,
        )

    return SelectionSnapshot(
        observed_at=observed_at,
        symbols=tuple(sorted(selected.values(), key=lambda item: item.symbol)),
        snapshot_id=universe.snapshot_id,
    )
