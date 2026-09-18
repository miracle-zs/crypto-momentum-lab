"""System performance and observability read model for the operator dashboard.

This module aggregates performance telemetry across four domains:
1. Decision path SLO (latencies, transition phases, terminal reasons)
2. Database persistence & checkpoint staggering (prepare, lag, sql_execute, total_ms)
3. Market data ingestion freshness & quality events (market_delay_ms, drops)
4. Host & PostgreSQL resources (load average, memory, swap, connections, size)
"""

import math
import os
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.operator_dashboard.schemas import (
    CheckpointMetricItem,
    HostResourcesResponse,
    MarketDataPerformanceResponse,
    PersistencePerformanceResponse,
    SystemPerformanceResponse,
)
from crypto_momentum_lab.operator_dashboard.status import (
    OperationalStatus,
)
from crypto_momentum_lab.operator_dashboard.telemetry_queries import (
    DecisionSLOQueries,
)
from crypto_momentum_lab.persistence.postgres.models import (
    MarketDataProcessStateRow,
    MarketDataQualityEventRow,
    RuntimeMarketState15sRow,
    StrategyRuntimeEventRow,
)

_CHECKPOINT_PERSISTED_EVENT = "strategy_checkpoint_persisted"
_MAX_CHECKPOINT_SAMPLES = 100


def _account_label_and_phase(run_id: str) -> tuple[str, float]:
    """Map live run identifiers to user-facing account labels and expected phase."""
    if "primary" in run_id or "b1-long" in run_id:
        return "primary", 0.0
    if "account-2" in run_id:
        return "account-2", 15.0
    if "account-3" in run_id:
        return "account-3", 30.0
    if "account-4" in run_id:
        return "account-4", 45.0
    return run_id, 0.0


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = math.ceil(quantile * len(sorted_values)) - 1
    return sorted_values[max(0, min(index, len(sorted_values) - 1))]


def _read_meminfo() -> dict[str, int]:
    """Read host /proc/meminfo when running on Linux."""
    result: dict[str, int] = {}
    try:
        with open("/proc/meminfo", encoding="ascii") as file:
            for line in file:
                parts = line.split(":")
                if len(parts) == 2:
                    key = parts[0].strip()
                    val_parts = parts[1].strip().split()
                    if val_parts and val_parts[0].isdigit():
                        # meminfo reports in kB
                        result[key] = int(val_parts[0]) * 1024
    except (FileNotFoundError, OSError, ValueError):
        pass
    return result


class PerformanceQueries:
    """Read model query adapter for the /api/performance endpoint."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        decision_slo_queries: DecisionSLOQueries | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock
        self._decision_slo_queries = decision_slo_queries or DecisionSLOQueries(
            session_factory,
            clock=clock,
        )

    async def performance(
        self,
        window: str = "6h",
    ) -> SystemPerformanceResponse:
        now = self._clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)

        # 1. Decision SLO
        decision_slo = await self._decision_slo_queries.decision_slo(window=window)

        # 2. Persistence & Checkpoint metrics
        async with self._session_factory() as session:
            checkpoint_rows = list(
                (
                    await session.scalars(
                        select(StrategyRuntimeEventRow)
                        .where(
                            StrategyRuntimeEventRow.event_type
                            == _CHECKPOINT_PERSISTED_EVENT
                        )
                        .order_by(StrategyRuntimeEventRow.occurred_at.desc())
                        .limit(_MAX_CHECKPOINT_SAMPLES)
                    )
                ).all()
            )

            # 3. Market data status
            latest_market_state = await session.scalar(
                select(RuntimeMarketState15sRow)
                .order_by(RuntimeMarketState15sRow.bucket_start.desc())
                .limit(1)
            )
            market_process_state = await session.scalar(
                select(MarketDataProcessStateRow)
                .order_by(MarketDataProcessStateRow.occurred_at.desc())
                .limit(1)
            )
            latest_market_progress = await session.scalar(
                select(StrategyRuntimeEventRow)
                .where(StrategyRuntimeEventRow.event_type == "market_state_progress")
                .order_by(StrategyRuntimeEventRow.occurred_at.desc())
                .limit(1)
            )

            one_hour_ago = now - timedelta(hours=1)
            quality_events_count = (
                await session.scalar(
                    select(func.count(MarketDataQualityEventRow.event_id)).where(
                        MarketDataQualityEventRow.occurred_at >= one_hour_ago
                    )
                )
                or 0
            )

            # 4. Host & Postgres resources
            pg_active_conns = None
            pg_idle_conns = None
            pg_db_size = None
            try:
                activity = (
                    await session.execute(
                        text(
                            "SELECT "
                            "count(*) FILTER (WHERE state = 'active') as active, "
                            "count(*) FILTER (WHERE state = 'idle') as idle "
                            "FROM pg_stat_activity "
                            "WHERE datname = current_database()"
                        )
                    )
                ).first()
                if activity:
                    pg_active_conns = int(activity[0])
                    pg_idle_conns = int(activity[1])
                db_size_res = (
                    await session.execute(
                        text("SELECT pg_database_size(current_database())")
                    )
                ).scalar()
                if db_size_res is not None:
                    pg_db_size = int(db_size_res)
            except Exception:
                pass

        # Build PersistencePerformanceResponse
        checkpoint_items: list[CheckpointMetricItem] = []
        account_latest: dict[str, CheckpointMetricItem] = {}
        total_latencies: list[float] = []

        for row in checkpoint_rows:
            details = row.details if isinstance(row.details, Mapping) else {}
            label, phase = _account_label_and_phase(row.run_id)
            total_ms = (
                float(details["total_ms"])
                if details.get("total_ms") is not None
                else None
            )
            item = CheckpointMetricItem(
                run_id=row.run_id,
                account_label=label,
                occurred_at=row.occurred_at,
                prepare_ms=(
                    float(details["prepare_ms"])
                    if details.get("prepare_ms") is not None
                    else None
                ),
                event_loop_lag_ms=(
                    float(details["event_loop_lag_ms"])
                    if details.get("event_loop_lag_ms") is not None
                    else None
                ),
                pool_acquire_ms=(
                    float(details["pool_acquire_ms"])
                    if details.get("pool_acquire_ms") is not None
                    else None
                ),
                is_new_connection=(
                    bool(details["is_new_connection"])
                    if details.get("is_new_connection") is not None
                    else None
                ),
                pool_checked_in=(
                    int(details["pool_checked_in"])
                    if details.get("pool_checked_in") is not None
                    else None
                ),
                pool_checked_out=(
                    int(details["pool_checked_out"])
                    if details.get("pool_checked_out") is not None
                    else None
                ),
                sql_execute_ms=(
                    float(details["sql_execute_ms"])
                    if details.get("sql_execute_ms") is not None
                    else None
                ),
                total_ms=total_ms,
                phase_seconds=phase,
            )
            checkpoint_items.append(item)
            if total_ms is not None:
                total_latencies.append(total_ms)
            if label not in account_latest:
                account_latest[label] = item

        persistence_status = OperationalStatus.READY
        if not checkpoint_items:
            persistence_status = OperationalStatus.NO_DATA
        elif (now - checkpoint_items[0].occurred_at).total_seconds() > 300:
            persistence_status = OperationalStatus.STALE

        persistence_resp = PersistencePerformanceResponse(
            status=persistence_status,
            sample_count=len(checkpoint_items),
            p50_total_ms=(
                _percentile(total_latencies, 0.50) if total_latencies else None
            ),
            p95_total_ms=(
                _percentile(total_latencies, 0.95) if total_latencies else None
            ),
            max_total_ms=max(total_latencies) if total_latencies else None,
            recent_checkpoints=checkpoint_items[:30],
            account_latest_checkpoints=account_latest,
        )

        # Build MarketDataPerformanceResponse
        observed_at = (
            latest_market_state.bucket_end
            if latest_market_state is not None
            else (
                latest_market_progress.occurred_at
                if latest_market_progress is not None
                else (
                    market_process_state.occurred_at
                    if market_process_state is not None
                    else None
                )
            )
        )
        market_delay_ms = None
        if (
            latest_market_progress is not None
            and isinstance(latest_market_progress.details, Mapping)
            and latest_market_progress.details.get("market_delay_ms") is not None
        ):
            market_delay_ms = round(
                float(latest_market_progress.details["market_delay_ms"]), 1
            )
        elif (
            latest_market_state is not None
            and latest_market_state.created_at is not None
        ):
            market_delay_ms = round(
                max(
                    0.0,
                    (
                        latest_market_state.created_at - latest_market_state.bucket_end
                    ).total_seconds()
                    * 1000,
                ),
                1,
            )
        elif observed_at is not None:
            market_delay_ms = round(
                max(0.0, (now - observed_at).total_seconds() * 1000), 1
            )

        market_status = OperationalStatus.READY
        if not observed_at:
            market_status = OperationalStatus.NO_DATA
        elif (now - observed_at).total_seconds() > 120:
            market_status = OperationalStatus.STALE

        realtime_closure_delay_seconds = 0.4
        if market_delay_ms is not None:
            realtime_closure_delay_seconds = round(market_delay_ms / 1000.0, 3)

        missing_agg_trade_count = (
            latest_market_state.missing_agg_trade_count
            if latest_market_state is not None
            and latest_market_state.missing_agg_trade_count is not None
            else 0
        )

        market_resp = MarketDataPerformanceResponse(
            status=market_status,
            observed_at=observed_at,
            market_delay_ms=market_delay_ms,
            realtime_closure_delay_seconds=realtime_closure_delay_seconds,
            simulated_close_drop_count=0,
            missing_agg_trade_count=missing_agg_trade_count,
            quality_events_count_1h=quality_events_count,
        )

        # Build HostResourcesResponse
        cpu_loads: tuple[float, float, float] | None = None
        try:
            cpu_loads = os.getloadavg()
        except (AttributeError, OSError):
            pass

        meminfo = _read_meminfo()
        mem_total = meminfo.get("MemTotal")
        mem_avail = meminfo.get("MemAvailable")
        mem_used = (
            mem_total - mem_avail
            if mem_total is not None and mem_avail is not None
            else None
        )
        mem_pct = (
            round((mem_used / mem_total) * 100, 1)
            if mem_used is not None and mem_total
            else None
        )

        swap_total = meminfo.get("SwapTotal")
        swap_free = meminfo.get("SwapFree")
        swap_used = (
            swap_total - swap_free
            if swap_total is not None and swap_free is not None
            else None
        )
        swap_pct = (
            round((swap_used / swap_total) * 100, 1)
            if swap_used is not None and swap_total
            else None
        )

        host_resp = HostResourcesResponse(
            cpu_load_1m=cpu_loads[0] if cpu_loads else None,
            cpu_load_5m=cpu_loads[1] if cpu_loads else None,
            cpu_load_15m=cpu_loads[2] if cpu_loads else None,
            mem_total_bytes=mem_total,
            mem_available_bytes=mem_avail,
            mem_used_bytes=mem_used,
            mem_usage_percent=mem_pct,
            swap_total_bytes=swap_total,
            swap_used_bytes=swap_used,
            swap_usage_percent=swap_pct,
            postgres_active_connections=pg_active_conns,
            postgres_idle_connections=pg_idle_conns,
            postgres_database_size_bytes=pg_db_size,
        )

        # Overall Status
        overall_status = OperationalStatus.READY
        if any(
            status in (OperationalStatus.HALTED, OperationalStatus.DOWN)
            for status in (decision_slo.status, persistence_status, market_status)
        ):
            overall_status = OperationalStatus.HALTED
        elif any(
            status in (OperationalStatus.STALE, OperationalStatus.DEGRADED)
            for status in (decision_slo.status, persistence_status, market_status)
        ):
            overall_status = OperationalStatus.DEGRADED
        elif all(
            status == OperationalStatus.NO_DATA
            for status in (decision_slo.status, persistence_status, market_status)
        ):
            overall_status = OperationalStatus.NO_DATA

        return SystemPerformanceResponse(
            status=overall_status,
            generated_at=now,
            decision_slo=decision_slo,
            persistence=persistence_resp,
            market_data=market_resp,
            host_resources=host_resp,
        )


__all__ = ["PerformanceQueries"]
