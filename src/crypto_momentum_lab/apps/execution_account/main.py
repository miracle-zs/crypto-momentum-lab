import asyncio
import os
from datetime import UTC, datetime
from typing import Annotated

import typer
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.execution_account.binance import (
    BinanceUsdMPrivateReadClient,
)
from crypto_momentum_lab.execution_account.daemon import (
    ContinuousAccountSyncConfig,
    ContinuousAccountSyncDaemon,
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
    expected_hedge_mode: Annotated[
        bool,
        typer.Option("--hedge-mode/--one-way-mode"),
    ] = False,
    fill_symbols: Annotated[
        str,
        typer.Option(
            "--fill-symbols",
            help="Comma-separated symbols for recent Binance fill reconciliation.",
        ),
    ] = "",
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
            expected_hedge_mode=expected_hedge_mode,
            fill_symbols=_parse_symbols(fill_symbols),
        )
    )
    typer.echo(
        "Execution account sync completed: "
        f"status={result.status.value} "
        f"mismatches={result.mismatch_count}"
    )


@app.command("sync")
def sync_command(
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
    expected_hedge_mode: Annotated[
        bool,
        typer.Option("--hedge-mode/--one-way-mode"),
    ] = False,
    fill_symbols: Annotated[
        str,
        typer.Option("--fill-symbols"),
    ] = "",
    interval_seconds: Annotated[
        float,
        typer.Option("--interval-seconds", min=1),
    ] = 5.0,
    fill_interval_seconds: Annotated[
        float,
        typer.Option("--fill-interval-seconds", min=1),
    ] = 60.0,
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
    asyncio.run(
        sync_continuously(
            database_url=resolved_database_url,
            environment=environment,
            account_label=account_label,
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            expected_multi_assets_mode=expected_multi_assets_mode,
            expected_hedge_mode=expected_hedge_mode,
            fill_symbols=_parse_symbols(fill_symbols),
            interval_seconds=interval_seconds,
            fill_interval_seconds=fill_interval_seconds,
        )
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
    expected_hedge_mode: bool = False,
    fill_symbols: tuple[str, ...] = (),
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
                    expected_hedge_mode=expected_hedge_mode,
                    observed_at=datetime.now(tz=UTC),
                    recent_fill_symbols=fill_symbols,
                ),
            )
            return await service.sync_once()
        finally:
            await client.aclose()
    finally:
        await engine.dispose()


async def sync_continuously(
    *,
    database_url: str,
    environment: str,
    account_label: str,
    base_url: str,
    api_key: str,
    api_secret: str,
    expected_multi_assets_mode: bool,
    expected_hedge_mode: bool,
    fill_symbols: tuple[str, ...],
    interval_seconds: float,
    fill_interval_seconds: float,
) -> None:
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
                    expected_hedge_mode=expected_hedge_mode,
                    observed_at=datetime.now(tz=UTC),
                    recent_fill_symbols=fill_symbols,
                ),
            )
            daemon = ContinuousAccountSyncDaemon(
                service=service,
                config=ContinuousAccountSyncConfig(
                    interval_seconds=interval_seconds,
                    fill_interval_seconds=fill_interval_seconds,
                ),
                on_error=lambda error: typer.echo(
                    f"Execution account sync failed: {type(error).__name__}",
                    err=True,
                ),
            )
            await daemon.run()
        finally:
            await client.aclose()
    finally:
        await engine.dispose()


def _parse_symbols(value: str) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                symbol.strip().upper()
                for symbol in value.split(",")
                if symbol.strip()
            }
        )
    )
