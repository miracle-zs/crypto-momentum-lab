from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.config.models import UniverseConfig
from crypto_momentum_lab.domain.universe.membership import (
    build_monitoring_memberships,
)
from crypto_momentum_lab.domain.universe.models import (
    MarketCandidate,
    TrackedMembership,
    UniverseSnapshot,
)
from crypto_momentum_lab.domain.universe.ports import (
    MonitoringObligationProvider,
    NoMonitoringObligations,
    UniverseMarketData,
    UniverseRepository,
)
from crypto_momentum_lab.domain.universe.ranking import rank_utc_day_returns


class UniverseRefreshService:
    def __init__(
        self,
        *,
        market_data: UniverseMarketData,
        repository: UniverseRepository,
        config: UniverseConfig,
        config_hash: str,
        obligations: MonitoringObligationProvider | None = None,
    ) -> None:
        self._market_data = market_data
        self._repository = repository
        self._config = config
        self._config_hash = config_hash
        self._obligations = obligations or NoMonitoringObligations()

    async def refresh(self, *, observed_at: datetime) -> UniverseSnapshot:
        if observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        observed_at = observed_at.astimezone(UTC).replace(
            second=0,
            microsecond=0,
        )
        utc_day = observed_at.date()

        contracts = await self._market_data.fetch_active_usdt_perpetuals()
        await self._repository.save_contract_metadata(
            contracts,
            effective_at=observed_at,
        )
        symbols = frozenset(contract.symbol for contract in contracts)
        stored_opens = await self._repository.load_daily_opens(
            utc_day,
            symbols,
        )
        missing_symbols = symbols - stored_opens.keys()
        fetched_opens = await self._market_data.fetch_daily_opens(
            frozenset(missing_symbols),
            utc_day,
        )
        await self._repository.save_daily_opens(
            fetched_opens,
            captured_at=observed_at,
        )
        opens: dict[str, Decimal] = {
            **stored_opens,
            **{item.symbol: item.open_price for item in fetched_opens},
        }
        prices = await self._market_data.fetch_latest_prices()

        candidates = [
            MarketCandidate(
                symbol=symbol,
                open_price=opens.get(symbol),
                current_price=(
                    None if symbol not in prices else prices[symbol].price
                ),
                price_time=(
                    None if symbol not in prices else prices[symbol].observed_at
                ),
            )
            for symbol in sorted(symbols)
        ]
        ranking = rank_utc_day_returns(
            candidates,
            top_count=self._config.top_count,
            ranking_depth=self._config.retention_rank,
        )
        activated = observed_at.hour != 0
        memberships: tuple[TrackedMembership, ...] = ()
        if activated:
            previous = await self._repository.load_active_memberships()
            forced = await self._obligations.forced_symbols()
            memberships = tuple(
                build_monitoring_memberships(
                    ranking,
                    previous=previous,
                    forced_symbols=forced,
                    observed_at=observed_at,
                    retention_rank=self._config.retention_rank,
                    retention_duration=timedelta(
                        hours=self._config.retention_hours
                    ),
                ).values()
            )

        snapshot = UniverseSnapshot(
            snapshot_id=uuid5(
                NAMESPACE_URL,
                f"binance-usdm:{observed_at.isoformat()}:{self._config_hash}",
            ),
            observed_at=observed_at,
            utc_day=utc_day,
            config_hash=self._config_hash,
            activated=activated,
            ranking=ranking,
            memberships=tuple(
                sorted(memberships, key=lambda item: item.symbol)
            ),
        )
        await self._repository.save_snapshot(snapshot)
        return snapshot
