import asyncio
import os
import secrets
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal, Protocol, TypeVar, cast

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from crypto_momentum_lab.operator_dashboard.queries import (
    FIXED_COMMON_EQUITY_START_AT,
    DashboardQueries,
    LiveCashFlowAdjustment,
)
from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    PaperAccountHistoryResponse,
    PaperAccountsEquityResponse,
    PaperAccountsResponse,
    RiskExecutionResponse,
    RunReportSummaryResponse,
    StrategyRunResponse,
    SystemOverviewResponse,
    UniverseStatusResponse,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_dashboard_database_engine,
)

STATIC_DIR = Path(__file__).with_name("static")
_BASIC_AUTH = HTTPBasic(auto_error=False)
_PAPER_CACHE_TTL_SECONDS = 5.0
_PAPER_EQUITY_CACHE_TTL_SECONDS = 30.0
_OVERVIEW_CACHE_TTL_SECONDS = 15.0
_OVERVIEW_QUERY_TIMEOUT_SECONDS = 10.0
_T = TypeVar("_T")


class _ResponseCache:
    def __init__(self, ttl_seconds: float) -> None:
        self._ttl_seconds = ttl_seconds
        self._entries: dict[str, tuple[float, object]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def get(
        self,
        key: str,
        loader: Callable[[], Awaitable[_T]],
        *,
        ttl_seconds: float | None = None,
    ) -> _T:
        now = time.monotonic()
        entry = self._entries.get(key)
        if entry is not None and entry[0] > now:
            return cast(_T, entry[1])
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            now = time.monotonic()
            entry = self._entries.get(key)
            if entry is not None and entry[0] > now:
                return cast(_T, entry[1])
            value = await loader()
            ttl = self._ttl_seconds if ttl_seconds is None else ttl_seconds
            self._entries[key] = (time.monotonic() + ttl, value)
            return value


class DashboardQueryProtocol(Protocol):
    async def health(self) -> dict[str, str]: ...

    async def overview(self) -> SystemOverviewResponse: ...

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

    async def account(self, equity_range: str = "24h") -> AccountOverviewResponse: ...

    async def risk_execution(self) -> RiskExecutionResponse: ...

    async def reports(self) -> RunReportSummaryResponse: ...


def create_dashboard_app(
    *,
    database_url: str | None = None,
    queries: DashboardQueryProtocol | None = None,
    auth_username: str | None = None,
    auth_password: str | None = None,
    paper_run_ids: frozenset[str] | None = None,
    live_cash_flow_adjustments: Sequence[LiveCashFlowAdjustment] | None = None,
    common_equity_start_at: datetime | None = FIXED_COMMON_EQUITY_START_AT,
    overview_cache_ttl_seconds: float = _OVERVIEW_CACHE_TTL_SECONDS,
    overview_query_timeout_seconds: float = _OVERVIEW_QUERY_TIMEOUT_SECONDS,
) -> FastAPI:
    resolved_auth_username = auth_username or os.environ.get(
        "CML_DASHBOARD_USERNAME"
    )
    resolved_auth_password = auth_password or os.environ.get(
        "CML_DASHBOARD_PASSWORD"
    )
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
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        del app
        yield
        if engine is not None:
            await engine.dispose()

    dashboard = FastAPI(
        title="Crypto Momentum Operator Console",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    dashboard.add_middleware(GZipMiddleware, minimum_size=1024)
    dashboard.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
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
        return FileResponse(STATIC_DIR / "index.html")

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
                ),
                timeout=overview_query_timeout_seconds,
            )
        except (TimeoutError, SQLAlchemyTimeoutError) as exc:
            raise HTTPException(
                status_code=504,
                detail="dashboard overview query timed out",
            ) from exc

    @dashboard.get(
        "/api/universe",
        response_model=UniverseStatusResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def universe() -> UniverseStatusResponse:
        return await response_cache.get("universe", query_service().universe)

    @dashboard.get(
        "/api/strategy-runs/current",
        response_model=StrategyRunResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def strategy_run() -> StrategyRunResponse:
        return await response_cache.get("strategy-run", query_service().strategy_run)

    @dashboard.get(
        "/api/paper-accounts",
        response_model=PaperAccountsResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def paper_accounts() -> PaperAccountsResponse:
        return await response_cache.get(
            "paper-accounts",
            query_service().paper_accounts,
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
        )

    @dashboard.get(
        "/api/account",
        response_model=AccountOverviewResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def account(
        equity_range: Literal["24h", "7d", "30d", "1y"] = "24h",
    ) -> AccountOverviewResponse:
        try:
            return await response_cache.get(
                f"account:{equity_range}",
                lambda: query_service().account(equity_range),
            )
        except TimeoutError as exc:
            raise HTTPException(
                status_code=504,
                detail="dashboard account query timed out",
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
        )

    @dashboard.get(
        "/api/reports",
        response_model=RunReportSummaryResponse,
        dependencies=[Depends(require_dashboard_auth)],
    )
    async def reports() -> RunReportSummaryResponse:
        return await response_cache.get("reports", query_service().reports)

    return dashboard
