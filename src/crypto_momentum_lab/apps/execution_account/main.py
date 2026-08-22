import asyncio
import os
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Annotated

import typer
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.execution_account.binance import (
    BinanceUsdMPrivateReadClient,
    BinanceUsdMUserDataStream,
    BinanceUserDataEvent,
)
from crypto_momentum_lab.execution_account.daemon import (
    UserDataAccountSyncConfig,
    UserDataAccountSyncDaemon,
)
from crypto_momentum_lab.execution_account.hub import (
    AccountEvent,
    AccountEventHub,
    AccountEventHubConfig,
)
from crypto_momentum_lab.execution_account.sync import (
    ExecutionAccountSyncConfig,
    ExecutionAccountSyncResult,
    ExecutionAccountSyncService,
)
from crypto_momentum_lab.persistence.postgres import (
    PostgresAccountRepository,
    create_execution_database_engine,
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
    resolved_database_url = _execution_database_url(database_url)
    if not resolved_database_url:
        raise typer.BadParameter(
            "--database-url or CML_EXECUTION_DATABASE_URL or "
            "CML_DATABASE_URL is required"
        )
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
    websocket_url: Annotated[
        str,
        typer.Option("--websocket-url"),
    ] = "wss://fstream.binance.com/ws",
    account_event_hub_host: Annotated[
        str,
        typer.Option("--account-event-hub-host"),
    ] = "0.0.0.0",
    account_event_hub_port: Annotated[
        int,
        typer.Option("--account-event-hub-port", min=0, max=65535),
    ] = 8767,
    rest_reconciliation_interval_seconds: Annotated[
        float,
        typer.Option("--rest-reconciliation-interval-seconds", min=30),
    ] = 300.0,
) -> None:
    resolved_database_url = _execution_database_url(database_url)
    if not resolved_database_url:
        raise typer.BadParameter(
            "--database-url or CML_EXECUTION_DATABASE_URL or "
            "CML_DATABASE_URL is required"
        )
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
            websocket_url=websocket_url,
            account_event_hub_host=account_event_hub_host,
            account_event_hub_port=account_event_hub_port,
            rest_reconciliation_interval_seconds=(
                rest_reconciliation_interval_seconds
            ),
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
    engine = create_execution_database_engine(database_url)
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
    websocket_url: str,
    account_event_hub_host: str,
    account_event_hub_port: int,
    rest_reconciliation_interval_seconds: float,
) -> None:
    engine = create_execution_database_engine(database_url)
    account_event_hub = AccountEventHub(
        AccountEventHubConfig(
            host=account_event_hub_host,
            port=account_event_hub_port,
        )
    )
    await account_event_hub.start()
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
            stream = BinanceUsdMUserDataStream(
                listen_key_client=client,
                websocket_url=websocket_url,
            )

            def publish_account_event(
                event: BinanceUserDataEvent,
                result: ExecutionAccountSyncResult,
            ) -> None:
                account_event_hub.publish(
                    _account_event_from_user_data(
                        event,
                        result,
                        environment=environment,
                        account_label=account_label,
                    )
                )

            daemon = UserDataAccountSyncDaemon(
                service=service,
                stream=stream,
                config=UserDataAccountSyncConfig(
                    rest_reconciliation_interval_seconds=(
                        rest_reconciliation_interval_seconds
                    ),
                ),
                on_error=lambda error: typer.echo(
                    f"Execution account sync failed: {type(error).__name__}",
                    err=True,
                ),
                on_persisted=publish_account_event,
            )
            await daemon.run()
        finally:
            await client.aclose()
    finally:
        await account_event_hub.stop()
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


def _execution_database_url(value: str | None) -> str:
    resolved = value or os.environ.get("CML_EXECUTION_DATABASE_URL") or os.environ.get(
        "CML_DATABASE_URL"
    )
    if not resolved:
        raise typer.BadParameter(
            "--database-url or CML_EXECUTION_DATABASE_URL or "
            "CML_DATABASE_URL is required"
        )
    return resolved


def _account_event_from_user_data(
    event: BinanceUserDataEvent,
    result: ExecutionAccountSyncResult,
    *,
    environment: str,
    account_label: str,
) -> AccountEvent:
    symbols: set[str] = set()
    symbol: str | None = None
    client_order_id: str | None = None
    order_status: str | None = None
    has_fill = False
    trade_id: str | None = None
    if event.event_type == "ORDER_TRADE_UPDATE":
        order = event.payload.get("o")
        if isinstance(order, dict):
            symbol = _event_text(order.get("s"))
            client_order_id = _event_text(order.get("c"))
            order_status = _event_text(order.get("X"))
            has_fill = (
                _event_text(order.get("x")) == "TRADE"
                and _is_nonzero_quantity(order.get("l"))
            )
            if has_fill:
                trade_id = _event_text(order.get("t"))
            if symbol is not None:
                symbols.add(symbol)
    elif event.event_type == "ACCOUNT_UPDATE":
        account = event.payload.get("a")
        if isinstance(account, dict):
            positions = account.get("P")
            if isinstance(positions, list):
                for position in positions:
                    if isinstance(position, dict):
                        position_symbol = _event_text(position.get("s"))
                        if position_symbol is not None:
                            symbols.add(position_symbol)
    return AccountEvent(
        environment=environment,
        account_label=account_label,
        event_type=event.event_type,
        event_id=event.event_id,
        event_at=event.event_at,
        received_at=event.received_at,
        symbols=tuple(sorted(symbols)),
        symbol=symbol,
        client_order_id=client_order_id,
        order_status=order_status,
        reason=result.status.value,
        has_fill=has_fill,
        trade_id=trade_id,
    )


def _event_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _is_nonzero_quantity(value: object) -> bool:
    try:
        return Decimal(str(value or "0")) != 0
    except (InvalidOperation, ValueError):
        return False
