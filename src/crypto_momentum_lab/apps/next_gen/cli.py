from __future__ import annotations

import asyncio
import json
import os
import signal
from collections.abc import Awaitable, Callable
from typing import Annotated, Any

import structlog
import typer

from crypto_momentum_lab.config import (
    BinanceCredentialRole,
    CredentialResolutionError,
    resolve_database_url,
    resolve_role_credentials,
)
from crypto_momentum_lab.next_gen.client import (
    BinanceRestClient,
    BinanceUserDataStream,
)
from crypto_momentum_lab.next_gen.daemon import run_nextgen_daemon
from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.exit_manager import ExitManager
from crypto_momentum_lab.next_gen.models import SystemMode
from crypto_momentum_lab.next_gen.persistence import (
    InMemoryTradingRepository,
    PostgresTradingRepository,
    TradingRepositoryProtocol,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_execution_database_engine,
)

app = typer.Typer(
    name="cml-next-gen",
    help="Next-Gen production live trading daemon CLI.",
    no_args_is_help=True,
)
log = structlog.get_logger("next_gen_cli")


def _resolve_db_url(database_url: str | None) -> str | None:
    """Resolve database URL from explicit flag or environment fallbacks."""
    return resolve_database_url(
        database_url,
        "CML_EXECUTION_DATABASE_URL",
        "CML_DATABASE_URL",
    )


def _resolve_credentials(
    api_key: str | None = None,
    api_secret: str | None = None,
    dry_run: bool = False,
) -> tuple[str, str]:
    """Resolve Binance API credentials securely."""
    if api_key and api_secret:
        return api_key, api_secret
    try:
        resolved = resolve_role_credentials(BinanceCredentialRole.TRADE)
        return resolved.api_key, resolved.api_secret
    except (CredentialResolutionError, ValueError):
        pass

    env_key = os.environ.get("BINANCE_API_KEY")
    env_secret = os.environ.get("BINANCE_API_SECRET")
    if env_key and env_secret:
        return env_key, env_secret

    if dry_run:
        log.warning("using_mock_credentials_for_dry_run")
        return "mock_api_key", "mock_api_secret"

    raise ValueError(
        "Binance API credentials missing. Provide --api-key and --api-secret "
        "or set BINANCE_TRADE_API_KEY/BINANCE_TRADE_API_SECRET or "
        "BINANCE_API_KEY/BINANCE_API_SECRET."
    )


def _resolve_repository(
    database_url: str | None = None,
    dry_run: bool = False,
) -> tuple[TradingRepositoryProtocol, Any]:
    """Resolve TradingRepository (Postgres if URL available, else InMemory)."""
    resolved_url = _resolve_db_url(database_url)
    if dry_run or not resolved_url:
        if not dry_run and not resolved_url:
            log.warning(
                "no_database_url_configured_falling_back_to_in_memory_repository"
            )
        return InMemoryTradingRepository(), None

    engine = create_execution_database_engine(resolved_url)
    return PostgresTradingRepository(engine), engine


async def _run_with_signal_handlers(
    run_fn: Callable[[asyncio.Event], Awaitable[None]],
) -> None:
    """Run an async worker while gracefully handling SIGTERM/SIGINT."""
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals: list[signal.Signals] = []

    def handle_signal(sig: signal.Signals) -> None:
        if not stop_event.is_set():
            log.warning(
                "next_gen_shutdown_signal_received",
                signal=sig.name,
            )
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, handle_signal, sig)
            registered_signals.append(sig)
        except (NotImplementedError, RuntimeError, ValueError):
            log.warning(
                "next_gen_signal_handler_unavailable",
                signal=sig.name,
            )

    try:
        await run_fn(stop_event)
    finally:
        for sig in registered_signals:
            try:
                loop.remove_signal_handler(sig)
            except Exception:
                pass


@app.command("run")
def run_command(
    environment: Annotated[
        str,
        typer.Option(
            "--environment",
            "-e",
            help="Trading environment (e.g. live, testnet)",
        ),
    ] = "live",
    account_label: Annotated[
        str,
        typer.Option("--account-label", "-a", help="Account label (e.g. a1)"),
    ] = "a1",
    symbols: Annotated[
        str | None,
        typer.Option(
            "--symbols",
            "-s",
            help="Comma-separated symbols, e.g. BTCUSDT,ETHUSDT",
        ),
    ] = None,
    database_url: Annotated[
        str | None,
        typer.Option("--database-url", help="Async PostgreSQL URL"),
    ] = None,
    api_key: Annotated[
        str | None,
        typer.Option("--api-key", help="Binance API Key"),
    ] = None,
    api_secret: Annotated[
        str | None,
        typer.Option("--api-secret", help="Binance API Secret"),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Run in dry-run mode with in-memory persistence",
        ),
    ] = False,
    keepalive_interval: Annotated[
        float,
        typer.Option(
            "--keepalive-interval",
            help="ListenKey keepalive interval in seconds",
        ),
    ] = 1800.0,
) -> None:
    """Launch Next-Gen continuous trading daemon."""
    key, secret = _resolve_credentials(api_key, api_secret, dry_run=dry_run)
    repo, engine = _resolve_repository(database_url, dry_run=dry_run)

    symbol_list = (
        [s.strip().upper() for s in symbols.split(",") if s.strip()]
        if symbols
        else None
    )

    log.info(
        "next_gen_daemon_starting",
        environment=environment,
        account=account_label,
        symbols=symbol_list,
        dry_run=dry_run,
    )

    rest_client = BinanceRestClient(api_key=key, api_secret=secret)
    ws_client = (
        BinanceUserDataStream(rest_client=rest_client, ws_connector=False)
        if dry_run
        else BinanceUserDataStream(rest_client=rest_client)
    )

    async def _runner(stop_event: asyncio.Event) -> None:
        try:
            await run_nextgen_daemon(
                environment=environment,
                account_label=account_label,
                db=repo,
                rest_client=rest_client,
                symbols=symbol_list,
                ws_client=ws_client,
                stop_event=stop_event,
                keepalive_interval_sec=keepalive_interval,
            )
        finally:
            if engine:
                await engine.dispose()

    asyncio.run(_run_with_signal_handlers(_runner))


@app.command("drain")
def drain_command(
    environment: Annotated[
        str,
        typer.Option("--environment", "-e", help="Trading environment"),
    ] = "live",
    account_label: Annotated[
        str,
        typer.Option("--account-label", "-a", help="Account label"),
    ] = "a1",
    database_url: Annotated[
        str | None,
        typer.Option("--database-url", help="Async PostgreSQL URL"),
    ] = None,
    api_key: Annotated[
        str | None,
        typer.Option("--api-key", help="Binance API Key"),
    ] = None,
    api_secret: Annotated[
        str | None,
        typer.Option("--api-secret", help="Binance API Secret"),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Dry run mode"),
    ] = False,
) -> None:
    """Liquidate all active batches cleanly and halt trading."""
    key, secret = _resolve_credentials(api_key, api_secret, dry_run=dry_run)
    repo, engine = _resolve_repository(database_url, dry_run=dry_run)

    log.info(
        "next_gen_drain_initiated",
        environment=environment,
        account=account_label,
    )

    rest_client = BinanceRestClient(api_key=key, api_secret=secret)

    async def _drainer() -> None:
        try:
            state = AccountState(
                environment=environment,
                account_label=account_label,
                db=repo,
                rest_client=rest_client,
            )
            state.set_system_mode(SystemMode.DRAINING)

            # 载入持久化活跃批次
            active_batches = await repo.async_get_active_batches(
                environment, account_label
            )
            for b in active_batches:
                state.batches[b.batch_id] = b

            executor = OrderExecutor(
                rest_client=rest_client,
                state=state,
                db=repo,
            )
            exit_mgr = ExitManager(
                rest_client=rest_client,
                state=state,
                executor=executor,
            )

            # 立即触发所有活跃批次市价退出
            exit_mgr.evaluate_active_batches()
            await exit_mgr.close()
            await executor.close()
            state.set_system_mode(SystemMode.STOPPED)
            log.info(
                "next_gen_drain_completed",
                drained_batches_count=len(active_batches),
            )
        finally:
            if engine:
                await engine.dispose()

    asyncio.run(_drainer())


@app.command("status")
def status_command(
    environment: Annotated[
        str,
        typer.Option("--environment", "-e", help="Trading environment"),
    ] = "live",
    account_label: Annotated[
        str,
        typer.Option("--account-label", "-a", help="Account label"),
    ] = "a1",
    database_url: Annotated[
        str | None,
        typer.Option("--database-url", help="Async PostgreSQL URL"),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", help="Max number of recent orders to inspect"),
    ] = 20,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output status as JSON"),
    ] = False,
) -> None:
    """Inspect active batches and recent orders from the execution database."""
    repo, engine = _resolve_repository(database_url, dry_run=False)

    async def _fetcher() -> dict[str, Any]:
        try:
            active_batches = await repo.async_get_active_batches(
                environment, account_label
            )
            recent_orders = await repo.async_get_recent_orders(
                environment, account_label, limit=limit
            )
            return {
                "environment": environment,
                "account_label": account_label,
                "active_batches_count": len(active_batches),
                "active_batches": [
                    {
                        "batch_id": b.batch_id,
                        "symbol": b.symbol,
                        "position_side": b.position_side,
                        "quantity": str(b.quantity),
                        "entry_price": str(b.entry_price),
                        "entry_bar_time": b.entry_bar_time.isoformat(),
                        "status": b.status,
                        "exiting_quantity": str(b.exiting_quantity),
                        "realized_pnl": str(b.realized_pnl),
                    }
                    for b in active_batches
                ],
                "recent_orders_count": len(recent_orders),
                "recent_orders": [
                    {
                        "client_order_id": o.client_order_id,
                        "exchange_order_id": o.exchange_order_id,
                        "batch_id": o.batch_id,
                        "symbol": o.symbol,
                        "side": o.side,
                        "position_side": o.position_side,
                        "order_type": o.order_type,
                        "quantity": str(o.quantity),
                        "price": str(o.price) if o.price is not None else None,
                        "status": o.status.name,
                        "filled_qty": str(o.filled_qty),
                        "avg_price": str(o.avg_price),
                        "created_at": o.created_at.isoformat(),
                    }
                    for o in recent_orders
                ],
            }
        finally:
            if engine:
                await engine.dispose()

    status_data = asyncio.run(_fetcher())

    if json_output:
        typer.echo(json.dumps(status_data, indent=2))
    else:
        typer.echo(
            f"=== Next-Gen Status [{environment}:{account_label}] ==="
        )
        typer.echo(f"Active Batches: {status_data['active_batches_count']}")
        for b in status_data["active_batches"]:
            typer.echo(
                f"  - [{b['batch_id']}] {b['symbol']} {b['position_side']} "
                f"qty={b['quantity']} entry={b['entry_price']} "
                f"status={b['status']} exiting={b['exiting_quantity']}"
            )
        typer.echo(f"\nRecent Orders (last {limit}):")
        for o in status_data["recent_orders"]:
            typer.echo(
                f"  - [{o['client_order_id']}] {o['symbol']} {o['side']} "
                f"{o['order_type']} qty={o['quantity']} "
                f"status={o['status']} filled={o['filled_qty']}"
            )


if __name__ == "__main__":
    app()
