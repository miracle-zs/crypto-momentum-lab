import asyncio
import os
from datetime import UTC, datetime
from typing import Annotated

import typer
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.execution_account.binance import (
    BinanceUsdMPrivateReadClient,
)
from crypto_momentum_lab.execution_account.sync import (
    ExecutionAccountSyncConfig,
    ExecutionAccountSyncResult,
    ExecutionAccountSyncService,
)
from crypto_momentum_lab.persistence.postgres import (
    PostgresAccountRepository,
    create_async_database_engine,
)

app = typer.Typer(no_args_is_help=True)


@app.callback()
def execution_account_app() -> None:
    """Read-only Binance execution account synchronization."""


@app.command("sync-once")
def sync_once_command(
    database_url: Annotated[
        str | None,
        typer.Option("--database-url", help="Async PostgreSQL URL."),
    ] = None,
    environment: Annotated[str, typer.Option("--environment")] = "live",
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    base_url: Annotated[
        str,
        typer.Option("--base-url"),
    ] = "https://fapi.binance.com",
    api_key_env: Annotated[
        str,
        typer.Option("--api-key-env"),
    ] = "BINANCE_API_KEY",
    api_secret_env: Annotated[
        str,
        typer.Option("--api-secret-env"),
    ] = "BINANCE_API_SECRET",
    expected_multi_assets_mode: Annotated[
        bool,
        typer.Option("--expected-multi-assets-mode/--single-asset-mode"),
    ] = False,
) -> None:
    resolved_database_url = database_url or os.environ.get("CML_DATABASE_URL")
    if not resolved_database_url:
        raise typer.BadParameter("--database-url or CML_DATABASE_URL is required")
    api_key = os.environ.get(api_key_env)
    api_secret = os.environ.get(api_secret_env)
    if not api_key or not api_secret:
        raise typer.BadParameter(
            f"{api_key_env} and {api_secret_env} are required"
        )
    result = asyncio.run(
        sync_once(
            database_url=resolved_database_url,
            environment=environment,
            account_label=account_label,
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            expected_multi_assets_mode=expected_multi_assets_mode,
        )
    )
    typer.echo(
        "Execution account sync completed: "
        f"status={result.status.value} "
        f"mismatches={result.mismatch_count}"
    )


async def sync_once(
    *,
    database_url: str,
    environment: str,
    account_label: str,
    base_url: str,
    api_key: str,
    api_secret: str,
    expected_multi_assets_mode: bool,
) -> ExecutionAccountSyncResult:
    engine = create_async_database_engine(database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        repository = PostgresAccountRepository(factory)
        client = BinanceUsdMPrivateReadClient(
            api_key=api_key,
            api_secret=api_secret,
            environment=environment,
            account_label=account_label,
            base_url=base_url,
        )
        try:
            service = ExecutionAccountSyncService(
                client=client,
                repository=repository,
                config=ExecutionAccountSyncConfig(
                    environment=environment,
                    account_label=account_label,
                    expected_multi_assets_mode=expected_multi_assets_mode,
                    observed_at=datetime.now(tz=UTC),
                ),
            )
            return await service.sync_once()
        finally:
            await client.aclose()
    finally:
        await engine.dispose()
