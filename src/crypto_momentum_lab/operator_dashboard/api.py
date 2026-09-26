import asyncio
import logging
import os
import secrets
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal, Protocol, TypeVar, cast

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from starlette.types import Scope

from crypto_momentum_lab.operator_dashboard.queries import (
    FIXED_COMMON_EQUITY_START_AT,
    DashboardQueries,
    LiveCashFlowAdjustment,
)
from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    DecisionSLOResponse,
    LiveAccountMetricsResponse,
    LiveAccountsResponse,
    PaperAccountHistoryResponse,
    PaperAccountsEquityResponse,
    PaperAccountsResponse,
    ResearchCollectorResponse,
    RiskExecutionResponse,
    RunReportSummaryResponse,
    StrategyRunResponse,
    SystemOverviewResponse,
    SystemPerformanceResponse,
    SystemReadinessResponse,
    UniverseStatusResponse,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_dashboard_database_engine,
)

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).with_name("static")
_BASIC_AUTH = HTTPBasic(auto_error=False)
_PAPER_CACHE_TTL_SECONDS = 5.0
_PAPER_EQUITY_CACHE_TTL_SECONDS = 30.0
_PAPER_EQUITY_STALE_GRACE_SECONDS = 60.0
_DEFAULT_STALE_GRACE_SECONDS = 60.0
_OVERVIEW_CACHE_TTL_SECONDS = 15.0
_OVERVIEW_QUERY_TIMEOUT_SECONDS = 10.0
# Performance aggregates decision SLO + checkpoint + market + host reads.
# A cold 24h SLO scan can exceed a browser poll interval; keep a longer TTL
# and fail the request instead of leaving the UI on an infinite spinner.
_PERFORMANCE_CACHE_TTL_SECONDS = 30.0
_PERFORMANCE_QUERY_TIMEOUT_SECONDS = 10.0
_T = TypeVar("_T")


_cache_status_context: ContextVar[dict[str, str]] = ContextVar("cache_status_context")


def _set_cache_status(status: str) -> None:
    try:
        ctx = _cache_status_context.get()
        ctx["status"] = status
    except LookupError:
        pass


class _ResponseCache:
    def __init__(
        self,
        ttl_seconds: float,
        max_entries: int = 512,
        max_refresh_tasks: int = 16,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        if max_refresh_tasks <= 0:
            raise ValueError("max_refresh_tasks must be positive")
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._max_refresh_tasks = max_refresh_tasks
        self._entries: dict[str, tuple[float, object]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._refresh_tasks: dict[str, asyncio.Task[None]] = {}
        self._refresh_errors: dict[str, tuple[float, str]] = {}

    def _prune(self, now: float) -> None:
        # First remove expired entries
        expired_keys = [k for k, v in self._entries.items() if v[0] <= now]
        for k in expired_keys:
            self._entries.pop(k, None)
        # If still over capacity, remove oldest by expiry
        if len(self._entries) > self._max_entries:
            sorted_keys = sorted(
                self._entries.keys(), key=lambda k: self._entries[k][0]
            )
            for k in sorted_keys[: len(self._entries) - self._max_entries]:
                self._entries.pop(k, None)

        # Prune finished refresh tasks
        finished_tasks = [k for k, t in self._refresh_tasks.items() if t.done()]
        for k in finished_tasks:
            self._refresh_tasks.pop(k, None)

        # Prune expired or excessive refresh errors
        expired_errors = [
            k for k, v in self._refresh_errors.items() if v[0] + 300.0 <= now
        ]
        for k in expired_errors:
            self._refresh_errors.pop(k, None)
        if len(self._refresh_errors) > self._max_entries:
            sorted_errs = sorted(
                self._refresh_errors.keys(), key=lambda k: self._refresh_errors[k][0]
            )
            for k in sorted_errs[: len(self._refresh_errors) - self._max_entries]:
                self._refresh_errors.pop(k, None)

        # Prune unheld locks for keys not active in entries or tasks
        active_keys = set(self._entries) | set(self._refresh_tasks)
        unused_locks = [
            k
            for k, lock in self._locks.items()
            if k not in active_keys and not lock.locked()
        ]
        for k in unused_locks:
            self._locks.pop(k, None)

    async def get(
        self,
        key: str,
        loader: Callable[[], Awaitable[_T]],
        *,
        ttl_seconds: float | None = None,
        stale_while_revalidate_seconds: float = 0.0,
    ) -> _T:
        if stale_while_revalidate_seconds < 0:
            raise ValueError("stale_while_revalidate_seconds must not be negative")
        now = time.monotonic()
        entry = self._entries.get(key)
        if entry is not None:
            if entry[0] > now:
                _set_cache_status("HIT")
                return cast(_T, entry[1])
            if entry[0] + stale_while_revalidate_seconds > now:
                self._schedule_refresh(
                    key,
                    loader,
                    ttl_seconds=ttl_seconds,
                )
                _set_cache_status("STALE")
                return cast(_T, entry[1])
        lock = self._locks.setdefault(key, asyncio.Lock())
        try:
            async with lock:
                now = time.monotonic()
                entry = self._entries.get(key)
                if entry is not None and entry[0] > now:
                    _set_cache_status("HIT")
                    return cast(_T, entry[1])
                value = await loader()
                _set_cache_status("MISS")
                ttl = self._ttl_seconds if ttl_seconds is None else ttl_seconds
                if ttl <= 0:
                    raise ValueError("ttl_seconds must be positive")
                self._entries[key] = (time.monotonic() + ttl, value)
                self._prune(now)
                return value
        finally:
            if (
                not lock.locked()
                and key not in self._entries
                and key not in self._refresh_tasks
            ):
                self._locks.pop(key, None)

    def _schedule_refresh(
        self,
        key: str,
        loader: Callable[[], Awaitable[_T]],
        *,
        ttl_seconds: float | None,
    ) -> None:
        finished = [k for k, t in self._refresh_tasks.items() if t.done()]
        for k in finished:
            self._refresh_tasks.pop(k, None)

        if len(self._refresh_tasks) >= self._max_refresh_tasks:
            return

        task = self._refresh_tasks.get(key)
        if task is not None and not task.done():
            return
        self._refresh_tasks[key] = asyncio.create_task(
            self._refresh(
                key,
                loader,
                ttl_seconds=ttl_seconds,
            ),
            name=f"dashboard-cache-refresh:{key}",
        )

    async def _refresh(
        self,
        key: str,
        loader: Callable[[], Awaitable[_T]],
        *,
        ttl_seconds: float | None,
    ) -> None:
        try:
            lock = self._locks.setdefault(key, asyncio.Lock())
            async with lock:
                entry = self._entries.get(key)
                if entry is not None and entry[0] > time.monotonic():
                    return
                value = await loader()
                ttl = self._ttl_seconds if ttl_seconds is None else ttl_seconds
                if ttl <= 0:
                    return
                self._entries[key] = (time.monotonic() + ttl, value)
                self._refresh_errors.pop(key, None)
                self._prune(time.monotonic())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._refresh_errors[key] = (time.monotonic(), str(exc))
            logger.warning(
                "Dashboard cache background refresh failed for %s: %s",
                key,
                exc,
            )
            return
        finally:
            current = asyncio.current_task()
            if self._refresh_tasks.get(key) is current:
                self._refresh_tasks.pop(key, None)
            cached_lock = self._locks.get(key)
            if (
                cached_lock is not None
                and not cached_lock.locked()
                and key not in self._entries
            ):
                self._locks.pop(key, None)

    async def aclose(self) -> None:
        tasks = tuple(self._refresh_tasks.values())
        self._refresh_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


class DashboardQueryProtocol(Protocol):
    async def health(self) -> dict[str, str]: ...

    async def readiness(self) -> SystemReadinessResponse: ...

    async def decision_slo(
        self,
        window: str = "24h",
    ) -> DecisionSLOResponse: ...

    async def overview(self) -> SystemOverviewResponse: ...

    async def research_collector(self) -> ResearchCollectorResponse: ...

    async def universe(self) -> UniverseStatusResponse: ...

    async def strategy_run(self) -> StrategyRunResponse: ...

    async def paper_accounts(self) -> PaperAccountsResponse: ...

    async def paper_account_equity(self) -> PaperAccountsEquityResponse: ...

    async def paper_account(self, run_id: str) -> StrategyRunResponse: ...

    async def paper_history(
        self,
        run_id: str,
        *,
        full: bool = False,
    ) -> PaperAccountHistoryResponse: ...

    async def account(
        self,
        equity_range: str = "24h",
        account_label: str | None = None,
    ) -> AccountOverviewResponse: ...

    async def live_accounts(self) -> LiveAccountsResponse: ...

    async def live_account_metrics(
        self,
        equity_range: str = "24h",
    ) -> LiveAccountMetricsResponse: ...

    async def risk_execution(self) -> RiskExecutionResponse: ...

    async def reports(self) -> RunReportSummaryResponse: ...

    async def performance(
        self,
        window: str = "6h",
    ) -> SystemPerformanceResponse: ...

    async def account_performance(
        self,
        account_label: str = "primary",
        window_hours: int = 24,
        environment: str = "live",
        asset: str = "USDT",
        end_time: datetime | None = None,
    ) -> dict[str, object]: ...


def create_dashboard_app(
    *,
    database_url: str | None = None,
    queries: DashboardQueryProtocol | None = None,
    auth_username: str | None = None,
    auth_password: str | None = None,
    paper_run_ids: frozenset[str] | None = None,
    live_cash_flow_adjustments: Sequence[LiveCashFlowAdjustment] | None = None,
    common_equity_start_at: datetime | None = FIXED_COMMON_EQUITY_START_AT,
    research_collector_root: Path | None = None,
    overview_cache_ttl_seconds: float = _OVERVIEW_CACHE_TTL_SECONDS,
    overview_query_timeout_seconds: float = _OVERVIEW_QUERY_TIMEOUT_SECONDS,
    default_stale_grace_seconds: float = _DEFAULT_STALE_GRACE_SECONDS,
) -> FastAPI:
    resolved_auth_username = auth_username or os.environ.get("CML_DASHBOARD_USERNAME")
    resolved_auth_password = auth_password or os.environ.get("CML_DASHBOARD_PASSWORD")
    if (resolved_auth_username is None) != (resolved_auth_password is None):
        raise ValueError(
            "dashboard authentication requires both CML_DASHBOARD_USERNAME "
            "and CML_DASHBOARD_PASSWORD"
        )
    auth_enabled = (
        resolved_auth_username is not None and resolved_auth_password is not None
    )

    engine: AsyncEngine | None = None
    resolved_queries = queries
    if resolved_queries is None and database_url is not None:
        engine = create_dashboard_database_engine(database_url)
        resolved_queries = DashboardQueries(
            async_sessionmaker(engine, expire_on_commit=False),
            paper_run_ids=paper_run_ids,
            live_cash_flow_adjustments=live_cash_flow_adjustments,
            common_equity_start_at=common_equity_start_at,
            research_collector_root=(
                research_collector_root
                or Path(
                    os.environ.get(
                        "CML_RESEARCH_COLLECTOR_ROOT",
                        "/app/research-data",
                    )
                )
            ),
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        del app
        yield
        await response_cache.aclose()
        if engine is not None:
            await engine.dispose()

    class _CachedStaticFiles(StaticFiles):
        def file_response(
            self,
            full_path: str | os.PathLike[str],
            stat_result: os.stat_result,
            scope: Scope,
            status_code: int = 200,
        ) -> Response:
            response = super().file_response(full_path, stat_result, scope, status_code)
            path_str = str(full_path)
            if "vendor" in path_str:
                response.headers["Cache-Control"] = (
                    "public, max-age=31536000, immutable"
                )
            elif path_str.endswith((".css", ".js", ".svg", ".png", ".woff2")):
                response.headers["Cache-Control"] = "public, max-age=86400"
            return response

    dashboard = FastAPI(
        title="Crypto Momentum Operator Console",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    dashboard.add_middleware(GZipMiddleware, minimum_size=1024)

    @dashboard.middleware("http")
    async def add_cache_status_header(request: Request, call_next: Any) -> Response:
        state = {"status": ""}
        token = _cache_status_context.set(state)
        try:
            response = await call_next(request)
            status = state.get("status")
            if status:
                response.headers["X-Cache-Status"] = status
            return cast(Response, response)
        finally:
            _cache_status_context.reset(token)

    dashboard.mount("/static", _CachedStaticFiles(directory=STATIC_DIR), name="static")
    response_cache = _ResponseCache(_PAPER_CACHE_TTL_SECONDS)

    def require_dashboard_auth(
        credentials: Annotated[
            HTTPBasicCredentials | None,
            Depends(_BASIC_AUTH),
        ],
    ) -> None:
        if not auth_enabled:
            return
        assert resolved_auth_username is not None
        assert resolved_auth_password is not None
        if credentials is None or not (
            secrets.compare_digest(
                credentials.username,
                resolved_auth_username,
            )
            and secrets.compare_digest(
                credentials.password,
                resolved_auth_password,
            )
        ):
            raise HTTPException(
                status_code=401,
                detail="dashboard authentication required",
                headers={"WWW-Authenticate": "Basic"},
            )

    def query_service() -> DashboardQueryProtocol:
        if resolved_queries is None:
            raise HTTPException(
                status_code=503,
                detail={"app_status": "UP", "database_status": "DOWN"},
            )
        return resolved_queries

    @dashboard.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(
            STATIC_DIR / "index.html",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )

    @dashboard.get("/api/health", dependencies=[Depends(require_dashboard_auth)])
    async def health() -> dict[str, str]:
        service = query_service()
        try:
            return await service.health()
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail={"app_status": "UP", "database_status": "DOWN"},
            ) from exc

    @dashboard.get(
        "/api/readiness",
        response_model=SystemReadinessResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def readiness() -> SystemReadinessResponse:
        return await response_cache.get(
            "readiness",
            query_service().readiness,
            ttl_seconds=5.0,
            stale_while_revalidate_seconds=0.0,
        )

    @dashboard.get(
        "/api/decision-slo",
        response_model=DecisionSLOResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def decision_slo(
        window: Literal["1h", "6h", "24h", "7d"] = "24h",
    ) -> DecisionSLOResponse:
        return await response_cache.get(
            f"decision-slo:{window}",
            lambda: query_service().decision_slo(window),
            ttl_seconds=15.0,
            stale_while_revalidate_seconds=default_stale_grace_seconds,
        )

    @dashboard.get(
        "/api/performance",
        response_model=SystemPerformanceResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def performance(
        window: Literal["1h", "6h", "24h", "7d"] = "6h",
    ) -> SystemPerformanceResponse:
        try:
            return await asyncio.wait_for(
                response_cache.get(
                    f"performance:{window}",
                    lambda: query_service().performance(window),
                    ttl_seconds=_PERFORMANCE_CACHE_TTL_SECONDS,
                    stale_while_revalidate_seconds=default_stale_grace_seconds,
                ),
                timeout=_PERFORMANCE_QUERY_TIMEOUT_SECONDS,
            )
        except (TimeoutError, SQLAlchemyTimeoutError) as exc:
            raise HTTPException(
                status_code=504,
                detail="dashboard performance query timed out",
            ) from exc

    @dashboard.get(
        "/api/performance/accounts/{account_label}",
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def account_performance(
        account_label: str = "primary",
        window_hours: int = 24,
        environment: str = "live",
        asset: str = "USDT",
        end_time: datetime | None = None,
    ) -> dict[str, object]:
        try:
            end_key = end_time.isoformat() if end_time else "now"
            cache_key = (
                f"account_performance:{environment}:{account_label}:"
                f"{asset}:{window_hours}:{end_key}"
            )
            return await response_cache.get(
                cache_key,
                lambda: query_service().account_performance(
                    account_label=account_label,
                    window_hours=window_hours,
                    environment=environment,
                    asset=asset,
                    end_time=end_time,
                ),
                ttl_seconds=_PERFORMANCE_CACHE_TTL_SECONDS,
                stale_while_revalidate_seconds=default_stale_grace_seconds,
            )
        except (TimeoutError, SQLAlchemyTimeoutError) as exc:
            raise HTTPException(
                status_code=504,
                detail="account performance query timed out",
            ) from exc

    @dashboard.get(
        "/api/overview",
        response_model=SystemOverviewResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def overview() -> SystemOverviewResponse:
        try:
            return await asyncio.wait_for(
                response_cache.get(
                    "overview",
                    query_service().overview,
                    ttl_seconds=overview_cache_ttl_seconds,
                    stale_while_revalidate_seconds=default_stale_grace_seconds,
                ),
                timeout=overview_query_timeout_seconds,
            )
        except (TimeoutError, SQLAlchemyTimeoutError) as exc:
            raise HTTPException(
                status_code=504,
                detail="dashboard overview query timed out",
            ) from exc

    @dashboard.get(
        "/api/research-collector",
        response_model=ResearchCollectorResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def research_collector() -> ResearchCollectorResponse:
        return await response_cache.get(
            "research-collector",
            query_service().research_collector,
            ttl_seconds=15.0,
            stale_while_revalidate_seconds=default_stale_grace_seconds,
        )

    @dashboard.get(
        "/api/universe",
        response_model=UniverseStatusResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def universe() -> UniverseStatusResponse:
        return await response_cache.get(
            "universe",
            query_service().universe,
            stale_while_revalidate_seconds=default_stale_grace_seconds,
        )

    @dashboard.get(
        "/api/strategy-runs/current",
        response_model=StrategyRunResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def strategy_run() -> StrategyRunResponse:
        return await response_cache.get(
            "strategy-run",
            query_service().strategy_run,
            stale_while_revalidate_seconds=default_stale_grace_seconds,
        )

    @dashboard.get(
        "/api/paper-accounts",
        response_model=PaperAccountsResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def paper_accounts() -> PaperAccountsResponse:
        return await response_cache.get(
            "paper-accounts",
            query_service().paper_accounts,
            stale_while_revalidate_seconds=default_stale_grace_seconds,
        )

    @dashboard.get(
        "/api/paper-accounts/equity",
        response_model=PaperAccountsEquityResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def paper_account_equity() -> PaperAccountsEquityResponse:
        return await response_cache.get(
            "paper-accounts-equity",
            query_service().paper_account_equity,
            ttl_seconds=_PAPER_EQUITY_CACHE_TTL_SECONDS,
            stale_while_revalidate_seconds=_PAPER_EQUITY_STALE_GRACE_SECONDS,
        )

    @dashboard.get(
        "/api/paper-accounts/{run_id}",
        response_model=StrategyRunResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def paper_account(run_id: str) -> StrategyRunResponse:
        return await response_cache.get(
            f"paper-account:{run_id}",
            lambda: query_service().paper_account(run_id),
            stale_while_revalidate_seconds=default_stale_grace_seconds,
        )

    @dashboard.get(
        "/api/paper-accounts/{run_id}/history",
        response_model=PaperAccountHistoryResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def paper_history(
        run_id: str,
        full: bool = False,
    ) -> PaperAccountHistoryResponse:
        return await response_cache.get(
            f"paper-history:{run_id}:{'full' if full else 'recent'}",
            lambda: query_service().paper_history(run_id, full=full),
            stale_while_revalidate_seconds=default_stale_grace_seconds,
        )

    @dashboard.get(
        "/api/account",
        response_model=AccountOverviewResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def account(
        equity_range: Literal["24h", "7d", "30d", "1y"] = "24h",
        account_label: str | None = None,
    ) -> AccountOverviewResponse:
        try:
            cache_key = f"account:{account_label or 'latest'}:{equity_range}"

            async def load_account() -> AccountOverviewResponse:
                if account_label is None:
                    return await query_service().account(equity_range)
                return await query_service().account(
                    equity_range,
                    account_label=account_label,
                )

            return await response_cache.get(
                cache_key,
                load_account,
                stale_while_revalidate_seconds=default_stale_grace_seconds,
            )
        except TimeoutError as exc:
            raise HTTPException(
                status_code=504,
                detail="dashboard account query timed out",
            ) from exc

    @dashboard.get(
        "/api/live-accounts",
        response_model=LiveAccountsResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def live_accounts() -> LiveAccountsResponse:
        try:
            return await response_cache.get(
                "live-accounts",
                query_service().live_accounts,
                ttl_seconds=_OVERVIEW_CACHE_TTL_SECONDS,
                stale_while_revalidate_seconds=default_stale_grace_seconds,
            )
        except TimeoutError as exc:
            raise HTTPException(
                status_code=504,
                detail="dashboard live account query timed out",
            ) from exc

    @dashboard.get(
        "/api/live-account-metrics",
        response_model=LiveAccountMetricsResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def live_account_metrics(
        equity_range: Literal["24h", "7d", "30d", "1y"] = "24h",
    ) -> LiveAccountMetricsResponse:
        try:
            return await response_cache.get(
                f"live-account-metrics:{equity_range}",
                lambda: query_service().live_account_metrics(equity_range),
                ttl_seconds=_PAPER_EQUITY_CACHE_TTL_SECONDS,
                stale_while_revalidate_seconds=_PAPER_EQUITY_STALE_GRACE_SECONDS,
            )
        except TimeoutError as exc:
            raise HTTPException(
                status_code=504,
                detail="dashboard live account metrics query timed out",
            ) from exc

    @dashboard.get(
        "/api/account-performance",
        response_model=dict[str, object],
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def get_account_performance(
        account_label: str = "primary",
        window_hours: int = 24,
        environment: str = "live",
        asset: str = "USDT",
        end_time: datetime | None = None,
    ) -> dict[str, object]:
        try:
            end_key = end_time.isoformat() if end_time else "now"
            cache_key = (
                f"account-performance:{environment}:{account_label}:"
                f"{asset}:{window_hours}:{end_key}"
            )
            return await response_cache.get(
                cache_key,
                lambda: query_service().account_performance(
                    account_label=account_label,
                    window_hours=window_hours,
                    environment=environment,
                    asset=asset,
                    end_time=end_time,
                ),
                ttl_seconds=_PAPER_EQUITY_CACHE_TTL_SECONDS,
                stale_while_revalidate_seconds=_PAPER_EQUITY_STALE_GRACE_SECONDS,
            )
        except TimeoutError as exc:
            raise HTTPException(
                status_code=504,
                detail="dashboard account performance query timed out",
            ) from exc

    @dashboard.get(
        "/api/risk-execution",
        response_model=RiskExecutionResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def risk_execution() -> RiskExecutionResponse:
        return await response_cache.get(
            "risk-execution",
            query_service().risk_execution,
            stale_while_revalidate_seconds=default_stale_grace_seconds,
        )

    @dashboard.get(
        "/api/reports",
        response_model=RunReportSummaryResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def reports() -> RunReportSummaryResponse:
        return await response_cache.get(
            "reports",
            query_service().reports,
            stale_while_revalidate_seconds=default_stale_grace_seconds,
        )

    return dashboard
