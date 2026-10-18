from __future__ import annotations

import asyncio
import json
import os
import signal
from collections.abc import Awaitable, Callable
from decimal import Decimal
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
from crypto_momentum_lab.next_gen.models import OrderStatus, SystemMode
from crypto_momentum_lab.next_gen.persistence import (
    InMemoryTradingRepository,
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


def _resolve_credentials(
    api_key: str | None = None,
    api_secret: str | None = None,
    account_label: str = "a1",
    dry_run: bool = False,
) -> tuple[str, str]:
    """Resolve Binance API credentials securely."""
    if api_key and api_secret:
        return api_key, api_secret

    # Check account-specific env variables first (e.g. BINANCE_TRADE_API_KEY_ACCOUNT_2)
    normalized = account_label.replace("-", "_").upper()
    acct_key_env = f"BINANCE_TRADE_API_KEY_{normalized}"
    acct_secret_env = f"BINANCE_TRADE_API_SECRET_{normalized}"
    if acct_key_env in os.environ and acct_secret_env in os.environ:
        try:
            resolved = resolve_role_credentials(
                BinanceCredentialRole.TRADE,
                api_key_env=acct_key_env,
                api_secret_env=acct_secret_env,
            )
            return resolved.api_key, resolved.api_secret
        except (CredentialResolutionError, ValueError):
            pass

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
    environment: str = "live",
) -> tuple[TradingRepositoryProtocol, Any]:
    """Resolve TradingRepository (Postgres or InMemory). Fail-closed in live."""
    resolved_url = _resolve_db_url(database_url)
    if dry_run:
        return InMemoryTradingRepository(), None

    if not resolved_url:
        if environment == "live":
            raise ValueError(
                "生产实盘环境 (live) 必须配置 "
                "CML_DATABASE_URL / CML_EXECUTION_DATABASE_URL，严禁退回内存仓储！"
            )
        log.warning("no_database_url_configured_falling_back_to_in_memory_repository")
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
    key, secret = _resolve_credentials(
        account_label=account_label,
        api_key=api_key,
        api_secret=api_secret,
        dry_run=dry_run,
    )
    repo, engine = _resolve_repository(
        database_url, dry_run=dry_run, environment=environment
    )

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
    hub_url = os.environ.get("CML_MARKET_STATE_HUB_URL")

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
                hub_url=hub_url,
                market_feed_connector=False if dry_run else None,
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
    """Liquidate all active batches cleanly, verify 100% flat, and halt trading."""
    key, secret = _resolve_credentials(
        account_label=account_label,
        api_key=api_key,
        api_secret=api_secret,
        dry_run=dry_run,
    )
    repo, engine = _resolve_repository(
        database_url, dry_run=dry_run, environment=environment
    )

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

    rest_client = BinanceRestClient(api_key=key, api_secret=secret)

    async def _drainer() -> None:
        try:
            lock_key = compute_advisory_lock_key(environment, account_label)
            async with repo.acquire_dedicated_advisory_lock(lock_key):
                state = AccountState(
                    environment=environment,
                    account_label=account_label,
                    db=repo,
                    rest_client=rest_client,
                )
                state.is_draining = True
                state.set_system_mode(SystemMode.DRAINING)

                # 1. 撤销交易所现存所有未结挂单 (防止与市价平仓冲突)
                try:
                    open_orders = await rest_client.fetch_open_orders()
                    for o in open_orders:
                        sym = str(o.get("symbol", ""))
                        cid = o.get("clientOrderId")
                        oid = o.get("orderId")
                        try:
                            await rest_client.cancel_order(
                                symbol=sym,
                                client_order_id=cid,
                                order_id=oid,
                            )
                            log.info(
                                "drain_cancelled_open_order",
                                symbol=sym,
                                client_order_id=cid,
                                order_id=oid,
                            )
                        except Exception as e:
                            log.warning(
                                "drain_cancel_order_failed",
                                symbol=sym,
                                client_order_id=cid,
                                err=str(e),
                            )
                except Exception as e:
                    log.warning("drain_fetch_open_orders_failed", err=str(e))

                # 2. 载入持久化活跃批次
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

                rules = SymbolRulesRegistry()
                try:
                    info = await rest_client.fetch_exchange_info()
                    rules.update_from_exchange_info(info)
                except Exception:
                    pass

                # 3. 对每个活跃批次下达市价清扫单
                drained_count = 0
                for b in active_batches:
                    remaining_qty = b.quantity - b.exiting_quantity
                    sweep_qty = rules.round_quantity(b.symbol, remaining_qty)
                    if sweep_qty > Decimal("0"):
                        exit_order = state.create_exit_order(
                            batch_id=b.batch_id,
                            quantity=sweep_qty,
                            price=None,
                            order_type="MARKET",
                        )
                        try:
                            await executor.submit_order(exit_order)
                            if not exit_order.is_terminal:
                                q_res = await rest_client.query_order(
                                    symbol=b.symbol,
                                    client_order_id=exit_order.client_order_id,
                                )
                                exec_qty = Decimal(
                                    str(q_res.get("executedQty", sweep_qty))
                                )
                                avg_px = Decimal(str(q_res.get("avgPrice", "0")))
                                await state.on_order_trade_update(
                                    client_order_id=exit_order.client_order_id,
                                    symbol=b.symbol,
                                    position_side=exit_order.position_side,
                                    side=exit_order.side,
                                    order_status=OrderStatus.FILLED,
                                    cum_filled_qty=exec_qty,
                                    avg_price=avg_px,
                                    last_filled_qty=exec_qty,
                                    last_filled_price=avg_px,
                                )
                            drained_count += 1
                        except Exception as e:
                            log.error(
                                "drain_batch_exit_failed",
                                batch_id=b.batch_id,
                                err=str(e),
                            )

                await executor.close()

                # 4. 校验账户最终是否完全空仓 (严格基线复核)
                acct_snap = await rest_client.fetch_account_information()
                non_zero_positions = [
                    p for p in acct_snap.positions if abs(p.position_amt) > Decimal("0")
                ]
                if non_zero_positions:
                    log.error(
                        "drain_finished_with_lingering_positions",
                        positions=[
                            f"{p.symbol}:{p.position_side}:{p.position_amt}"
                            for p in non_zero_positions
                        ],
                    )
                    raise RuntimeError(
                        "Drain 完成后交易所仍存在未结持仓，请人工介入核查！"
                    )

                state.set_system_mode(SystemMode.STOPPED)
                log.info(
                    "next_gen_drain_completed",
                    drained_batches_count=drained_count,
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
    repo, engine = _resolve_repository(
        database_url, dry_run=False, environment=environment
    )

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
