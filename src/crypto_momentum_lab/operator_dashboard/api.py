from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Protocol

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from crypto_momentum_lab.operator_dashboard.queries import DashboardQueries
from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    RiskExecutionResponse,
    RunReportSummaryResponse,
    StrategyRunResponse,
    SystemOverviewResponse,
    UniverseStatusResponse,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)

STATIC_DIR = Path(__file__).with_name("static")


class DashboardQueryProtocol(Protocol):
    async def health(self) -> dict[str, str]: ...

    async def overview(self) -> SystemOverviewResponse: ...

    async def universe(self) -> UniverseStatusResponse: ...

    async def strategy_run(self) -> StrategyRunResponse: ...

    async def account(self) -> AccountOverviewResponse: ...

    async def risk_execution(self) -> RiskExecutionResponse: ...

    async def reports(self) -> RunReportSummaryResponse: ...


def create_dashboard_app(
    *,
    database_url: str | None = None,
    queries: DashboardQueryProtocol | None = None,
) -> FastAPI:
    engine: AsyncEngine | None = None
    resolved_queries = queries
    if resolved_queries is None and database_url is not None:
        engine = create_async_database_engine(database_url)
        resolved_queries = DashboardQueries(
            async_sessionmaker(engine, expire_on_commit=False)
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
    dashboard.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

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

    @dashboard.get("/api/health")
    async def health() -> dict[str, str]:
        service = query_service()
        try:
            return await service.health()
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail={"app_status": "UP", "database_status": "DOWN"},
            ) from exc

    @dashboard.get("/api/overview", response_model=SystemOverviewResponse)
    async def overview() -> SystemOverviewResponse:
        return await query_service().overview()

    @dashboard.get("/api/universe", response_model=UniverseStatusResponse)
    async def universe() -> UniverseStatusResponse:
        return await query_service().universe()

    @dashboard.get(
        "/api/strategy-runs/current",
        response_model=StrategyRunResponse,
    )
    async def strategy_run() -> StrategyRunResponse:
        return await query_service().strategy_run()

    @dashboard.get("/api/account", response_model=AccountOverviewResponse)
    async def account() -> AccountOverviewResponse:
        return await query_service().account()

    @dashboard.get("/api/risk-execution", response_model=RiskExecutionResponse)
    async def risk_execution() -> RiskExecutionResponse:
        return await query_service().risk_execution()

    @dashboard.get("/api/reports", response_model=RunReportSummaryResponse)
    async def reports() -> RunReportSummaryResponse:
        return await query_service().reports()

    return dashboard
