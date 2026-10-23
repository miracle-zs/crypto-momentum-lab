from __future__ import annotations

import asyncio
import json
import os
import signal
from collections.abc import Awaitable, Callable
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any

import structlog
import typer

from crypto_momentum_lab.config import (
    BinanceCredentialRole,
    resolve_database_url,
    resolve_role_credentials,
)
from crypto_momentum_lab.live_rollout.runtime_manifest import load_live_runtime_manifest
from crypto_momentum_lab.next_gen.client import (
    BinanceRestClient,
    BinanceUserDataStream,
)
from crypto_momentum_lab.next_gen.daemon import run_nextgen_daemon
from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.models import SystemMode
from crypto_momentum_lab.next_gen.persistence import (
    PostgresTradingRepository,
    TradingRepositoryProtocol,
    compute_advisory_lock_key,
)
from crypto_momentum_lab.next_gen.symbol_rules import SymbolRulesRegistry
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


def _resolve_account_credentials(
    role: BinanceCredentialRole, account_label: str
) -> tuple[str, str]:
    normalized = account_label.replace("-", "_").upper()
    prefix = f"BINANCE_{role.value.upper()}"
    key_env = f"{prefix}_API_KEY_{normalized}"
    secret_env = f"{prefix}_API_SECRET_{normalized}"
    if key_env in os.environ or secret_env in os.environ:
        resolved = resolve_role_credentials(
            role, api_key_env=key_env, api_secret_env=secret_env
        )
    else:
        resolved = resolve_role_credentials(role)
    return resolved.api_key, resolved.api_secret


def _resolve_credentials(
    api_key: str | None = None,
    api_secret: str | None = None,
    account_label: str = "a1",
) -> tuple[str, str]:
    """Use an explicit pair or the configured trade role; reject partial pairs."""
    if api_key is not None or api_secret is not None:
        if not api_key or not api_secret:
            raise ValueError("API key and secret are required together")
        return api_key, api_secret
    return _resolve_account_credentials(BinanceCredentialRole.TRADE, account_label)


def _resolve_read_credentials(account_label: str = "a1") -> tuple[str, str]:
    return _resolve_account_credentials(BinanceCredentialRole.READ, account_label)


def _resolve_repository(
    database_url: str | None = None,
    environment: str = "live",
) -> tuple[TradingRepositoryProtocol, Any]:
    """Both exchange environments require persistent execution storage."""
    resolved_url = _resolve_db_url(database_url)
    if not resolved_url:
        raise ValueError(
            f"{environment} 必须配置 CML_DATABASE_URL / "
            "CML_EXECUTION_DATABASE_URL，严禁退回内存仓储！"
        )
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
            help="Validate account manifest without connecting to exchange or database",
        ),
    ] = False,
    runtime_manifest: Annotated[
        Path, typer.Option("--runtime-manifest", help="Account trading configuration")
    ] = Path("deploy/live-runtime.yaml"),
    keepalive_interval: Annotated[
        float,
        typer.Option(
            "--keepalive-interval",
            help="ListenKey keepalive interval in seconds",
        ),
    ] = 1800.0,
) -> None:
    """Launch Next-Gen continuous trading daemon."""
    account_config = load_live_runtime_manifest(runtime_manifest).account(account_label)
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
        sizing_mode=account_config.execution_inputs.sizing_mode,
        equity_fraction=account_config.execution_inputs.equity_fraction,
        target_notional=account_config.execution_inputs.target_notional,
    )

    if dry_run:
        log.info(
            "next_gen_manifest_validated",
            environment=environment,
            account=account_label,
            symbols=symbol_list,
        )
        return

    key, secret = _resolve_credentials(
        account_label=account_label,
        api_key=api_key,
        api_secret=api_secret,
    )
    read_key, read_secret = _resolve_read_credentials(
        account_label=account_label,
    )
    repo, engine = _resolve_repository(database_url, environment=environment)

    trade_client = BinanceRestClient(
        api_key=key, api_secret=secret, environment=environment
    )
    read_client = BinanceRestClient(
        api_key=read_key, api_secret=read_secret, environment=environment
    )
    ws_client = BinanceUserDataStream(
        rest_client=read_client,
        environment=environment,
    )
    hub_url = os.environ.get("CML_MARKET_STATE_HUB_URL")
    market_environment = os.environ.get("CML_MARKET_STATE_ENVIRONMENT", "research")

    async def _runner(stop_event: asyncio.Event) -> None:
        try:
            await run_nextgen_daemon(
                account_config=account_config,
                environment=environment,
                account_label=account_label,
                db=repo,
                rest_client=trade_client,
                read_client=read_client,
                symbols=symbol_list,
                ws_client=ws_client,
                stop_event=stop_event,
                keepalive_interval_sec=keepalive_interval,
                hub_url=hub_url,
                market_environment=market_environment,
            )
        finally:
            await read_client.close()
            await trade_client.close()
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
    """Liquidate all active batches cleanly, verify 100% flat, and halt trading."""
    log.info(
        "next_gen_drain_initiated",
        environment=environment,
        account=account_label,
        dry_run=dry_run,
    )

    if dry_run:
        log.info(
            "next_gen_drain_dry_run_completed",
            account=account_label,
            environment=environment,
        )
        return

    key, secret = _resolve_credentials(
        account_label=account_label,
        api_key=api_key,
        api_secret=api_secret,
    )
    read_key, read_secret = _resolve_read_credentials(
        account_label=account_label,
    )
    repo, engine = _resolve_repository(database_url, environment=environment)

    trade_client = BinanceRestClient(
        api_key=key, api_secret=secret, environment=environment
    )
    read_client = BinanceRestClient(
        api_key=read_key, api_secret=read_secret, environment=environment
    )

    async def _drainer() -> None:
        state = AccountState(
            environment, account_label, db=repo, rest_client=read_client
        )
        state.is_draining = True
        executor = OrderExecutor(trade_client, state, repo, read_client=read_client)
        ws_client = BinanceUserDataStream(read_client, environment=environment)
        ws_client.on_status_change = state.on_ws_status_change
        state.attach_ws_client(ws_client)
        stream_task = None
        try:
            lock_key = compute_advisory_lock_key(environment, account_label)
            async with repo.acquire_dedicated_advisory_lock(lock_key):
                rules = SymbolRulesRegistry()
                rules.update_from_exchange_info(await read_client.fetch_exchange_info())
                await executor.load_active_state()
                await ws_client.connect(on_event=state.on_live_event)
                stream_task = asyncio.create_task(ws_client.run_stream())
                # Reuse normal startup recovery for previously submitted exits;
                # never send a replacement before recovering their outcome.
                await executor.restore_startup_state()
                state.set_system_mode(SystemMode.DRAINING)
                drained_count = await executor.drain(rules)
                # The local ledger is not enough to declare a drain complete.
                final_snapshot = await read_client.fetch_account_information()
                final_orders = await read_client.fetch_open_orders()
                remaining_positions = [
                    p
                    for p in final_snapshot.positions
                    if p.position_amt != Decimal("0")
                ]
                if remaining_positions or final_orders:
                    state.set_system_mode(SystemMode.DEGRADED)
                    raise RuntimeError(
                        "Drain final exchange check found remaining positions or orders"
                    )
                state.set_system_mode(SystemMode.STOPPED)
                log.info(
                    "next_gen_drain_completed", drained_batches_count=drained_count
                )
        finally:
            if stream_task is not None:
                stream_task.cancel()
                await asyncio.gather(stream_task, return_exceptions=True)
            await ws_client.close()
            await executor.close()
            await read_client.close()
            await trade_client.close()
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
    repo, engine = _resolve_repository(database_url, environment=environment)

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
        typer.echo(f"=== Next-Gen Status [{environment}:{account_label}] ===")
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
