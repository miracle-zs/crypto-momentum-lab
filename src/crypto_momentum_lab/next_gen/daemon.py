from __future__ import annotations

import asyncio
import os
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import structlog

from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    BinanceClosedCandle15mFeed,
    ClosedCandle15mFeedConfig,
)
from crypto_momentum_lab.live_rollout.runtime_manifest import LiveRuntimeAccount
from crypto_momentum_lab.next_gen.client import (
    BinanceRestClient,
    BinanceUserDataStream,
    get_binance_urls,
)
from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.exit_manager import ExitManager
from crypto_momentum_lab.next_gen.models import SystemMode
from crypto_momentum_lab.next_gen.persistence import (
    TradingRepositoryProtocol,
    compute_advisory_lock_key,
)
from crypto_momentum_lab.next_gen.strategy_engine import MomentumStrategyEngine
from crypto_momentum_lab.next_gen.symbol_rules import SymbolRulesRegistry

log = structlog.get_logger("next_gen_daemon")


class NextGenDaemon:
    """下一代极简单进程交易守护进程 (TaskGroup 结构化生命周期)"""

    def __init__(
        self,
        environment: str,
        account_label: str,
        db: TradingRepositoryProtocol,
        rest_client: BinanceRestClient,
        account_config: LiveRuntimeAccount,
        read_client: BinanceRestClient | None = None,
        ws_client: BinanceUserDataStream | None = None,
        market_data_provider: Any = None,
        strategy_engine: MomentumStrategyEngine | None = None,
        exit_manager: ExitManager | None = None,
        symbols: list[str] | None = None,
        stop_event: asyncio.Event | None = None,
        hub_url: str | None = None,
        market_feed_connector: Any = None,
        market_environment: str = "research",
    ) -> None:
        self.account_config = account_config
        self.started_at = datetime.now(UTC)
        self.environment = environment
        self.market_environment = market_environment
        self.account_label = account_label
        self.db = db
        self.rest_client = rest_client
        self.read_client = read_client or rest_client
        self.ws_client = ws_client or BinanceUserDataStream(
            rest_client=self.read_client,
            environment=environment,
        )
        self.stop_event = stop_event or asyncio.Event()
        self._private_stop = asyncio.Event()
        self._candle_feed = BinanceClosedCandle15mFeed(
            config=ClosedCandle15mFeedConfig(
                websocket_url=get_binance_urls(environment)[2],
                environment=environment,
                consumer_id=f"nextgen-exit-{account_label}",
            )
        )

        self.state = AccountState(
            environment=environment,
            account_label=account_label,
            db=db,
            rest_client=self.read_client,
        )
        self.ws_client.on_status_change = self._on_ws_status_change
        self.state.attach_ws_client(self.ws_client)

        self.order_executor = OrderExecutor(
            rest_client=rest_client,
            state=self.state,
            db=db,
            read_client=self.read_client,
            entry_leverage=account_config.execution_inputs.entry_leverage,
            margin_type=account_config.execution_inputs.margin_type,
        )
        rules = (
            strategy_engine.symbol_rules
            if isinstance(strategy_engine, MomentumStrategyEngine)
            else SymbolRulesRegistry()
        )
        self.exit_manager = exit_manager or ExitManager(
            rest_client=rest_client,
            state=self.state,
            executor=self.order_executor,
            symbol_rules=rules,
            grace_period_sec=account_config.execution_inputs.candle_grace_bars * 900,
            decision_profit_pct=account_config.execution_inputs.candle_grace_decision_profit_pct,
            grace_profit_pct=account_config.execution_inputs.candle_grace_profit_pct,
        )
        self.strategy_engine = strategy_engine or MomentumStrategyEngine(
            state=self.state,
            order_executor=self.order_executor,
            symbols=symbols,
            symbol_rules=rules,
            account_config=account_config,
            market_environment=self.market_environment,
        )
        if market_data_provider is not None:
            self.market_data_provider = market_data_provider
        else:
            from crypto_momentum_lab.next_gen.market_feed import MarketDataFeed

            resolved_hub = (
                hub_url
                or os.environ.get("CML_MARKET_STATE_HUB_URL")
                or "ws://market-data:8766"
            )
            self.market_data_provider = MarketDataFeed(
                symbols=self.strategy_engine.symbols,
                on_market_state=self.strategy_engine.on_market_state,
                hub_url=resolved_hub,
                ws_connector=market_feed_connector,
                environment=self.strategy_engine.market_environment,
                consumer_id=f"nextgen-{environment}-{account_label}",
                on_connection_change=self._on_market_connection_change,
            )
            self.strategy_engine.market_connected = False

    def _on_market_connection_change(self, connected: bool) -> None:
        self.strategy_engine.market_connected = connected

    def _on_ws_status_change(self, connected: bool) -> None:
        self.state.on_ws_status_change(connected)

    async def _run_exit_candles(self) -> None:
        """Feed formal exchange 15m closes directly to the existing exit policy."""
        feed = self._candle_feed
        feed.set_symbols(frozenset(self.strategy_engine.symbols))
        try:
            if self.stop_event.is_set():
                return
            await feed.start()
            async for event in feed:
                self.exit_manager.evaluate_active_batches(
                    now=event.received_at,
                    current_prices={event.candle.symbol: event.candle.close_price},
                    closed_candle=event.candle,
                )
        finally:
            await feed.stop()

    async def _wait_for_stop(self) -> None:
        """等待外部停止信号并平稳触发 TaskGroup 退出"""
        await self.stop_event.wait()
        self.state.is_draining = True
        self.state.set_system_mode(SystemMode.DRAINING)
        log.info("stop_event_received_shutting_down", account=self.account_label)

        await self.market_data_provider.close()
        # Let any already-started submission reach its audited boundary before
        # cancelling exit tasks; then wait again for exit-created submissions.
        await self.order_executor.wait_for_submissions()
        await self.exit_manager.close()
        await self.order_executor.wait_for_submissions()
        for order in list(self.state.orders.values()):
            if not order.is_terminal:
                await self.order_executor.cancel_and_confirm(order)
        self._private_stop.set()
        await self.ws_client.close()
        await self.order_executor.close()

    async def _publish_runtime_status(self, ready: bool) -> None:
        unrealized: Decimal | None = Decimal("0")
        for batch in self.state.batches.values():
            if batch.quantity <= 0:
                continue
            price = self.strategy_engine.latest_prices.get(batch.symbol)
            if price is None:
                unrealized = None
                break
            direction = Decimal("1") if batch.position_side == "LONG" else Decimal("-1")
            if unrealized is None:
                break
            unrealized += direction * (price - batch.entry_price) * batch.quantity
        await self.db.async_save_runtime_status(
            self.environment,
            self.account_label,
            datetime.now(UTC),
            {
                "account_label": self.account_label,
                "environment": self.environment,
                "session_id": self.account_config.session_id,
                "started_at": self.started_at.isoformat(),
                "strategy_name": self.account_config.strategy,
                "config_hash": self.account_config.strategy_config_hash,
                "mode": self.state.system_mode.name,
                "ready": ready,
                "ws_connected": self.state.ws_connected,
                "halt_active": self.state.entry_halted,
                "entry_gate_open": ready and self.state.can_submit_entry_order(),
                "exit_gate_open": self.state.can_submit_exit_order(),
                "market_data": self.strategy_engine.market_data_observation,
                "balances": {
                    asset: str(value)
                    for asset, value in self.state.wallet_balances.items()
                },
                "unrealized_pnl": None if unrealized is None else str(unrealized),
            },
        )

    async def _run_health_heartbeat(
        self, stop_event: asyncio.Event, interval_sec: float = 15.0
    ) -> None:
        """周期性向容器共享健康目录写入心跳文件，支持本地健康监控检测"""
        health_dir_env = os.environ.get("CML_LOCAL_HEALTH_DIR", "/run/cml/health")
        health_dir = Path(health_dir_env)
        account_health_file = health_dir / f"nextgen-{self.account_label}.ok"
        status_file = health_dir / "status"
        database_file = health_dir / "database"

        health_dir.mkdir(parents=True, exist_ok=True)  # noqa: ASYNC240

        try:
            while not stop_event.is_set():
                try:
                    self.state.risk_config = await self.db.async_get_risk_config(
                        self.environment,
                        self.account_label,
                    )
                    self.state.entry_halted = await self.db.async_has_active_halt(
                        self.environment,
                        self.account_label,
                    )
                except Exception:
                    self.state.has_unrecoverable_fault = True
                    self.state.set_system_mode(SystemMode.DEGRADED)
                    log.exception("risk_configuration_refresh_failed")
                ready = (
                    self.state.system_mode == SystemMode.RUNNING
                    and self.state.can_submit_exit_order()
                    and self.strategy_engine.is_ready
                    and self._candle_feed.connection_available
                )
                try:
                    await self._publish_runtime_status(ready)
                except Exception:
                    log.exception("runtime_status_publish_failed")
                if not ready:
                    account_health_file.unlink(missing_ok=True)  # noqa: ASYNC240
                    status_file.write_text("not_ready", encoding="utf-8")  # noqa: ASYNC240
                if ready:
                    now_ts = str(int(time.time()))
                    try:
                        account_health_file.write_text(now_ts, encoding="utf-8")  # noqa: ASYNC240
                        status_file.write_text("ready", encoding="utf-8")  # noqa: ASYNC240
                        database_file.write_text(now_ts, encoding="utf-8")  # noqa: ASYNC240
                    except Exception as e:
                        log.warning("failed_to_write_healthcheck_file", err=str(e))
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
                    break
                except TimeoutError:
                    pass
        finally:
            for f in (account_health_file, status_file):
                try:
                    if f.exists():  # noqa: ASYNC240
                        f.unlink(missing_ok=True)  # noqa: ASYNC240
                except Exception:
                    pass

    async def run(self, keepalive_interval_sec: float = 1800.0) -> None:
        """端到端生命周期执行入口：
        1. 获取 PostgreSQL 会话级 Advisory Lock (防多进程开仓互斥)；
        2. 行情与策略预热 (PostgreSQL 中的真实 15s Bar)；
        3. 恢复本地责任并核对启动基线，加载账户风险和交易参数；
        4. 建立 WebSocket 长连并握手；
        5. 启动 TaskGroup 结构化并发协程；
        6. 退出后释放资源并设置 STOPPED 状态。
        """
        lock_key = compute_advisory_lock_key(self.environment, self.account_label)
        async with self.db.acquire_dedicated_advisory_lock(lock_key):
            log.info(
                "daemon_starting",
                env=self.environment,
                account=self.account_label,
            )

            try:
                await self.strategy_engine.warmup_market_data(
                    rest_client=self.read_client,
                    db=self.db,
                )
                self.market_data_provider.symbols = self.strategy_engine.symbols
                self.state.risk_config = await self.db.async_get_risk_config(
                    self.environment,
                    self.account_label,
                )
                self.state.entry_halted = await self.db.async_has_active_halt(
                    self.environment,
                    self.account_label,
                )
                await self.order_executor.load_active_state()
                async with asyncio.TaskGroup() as tg:
                    await self.ws_client.connect(on_event=self.state.on_live_event)
                    tg.create_task(
                        self.ws_client.run_stream(stop_event=self._private_stop)
                    )
                    tg.create_task(
                        self.market_data_provider.run(stop_event=self.stop_event)
                    )
                    await self.order_executor.restore_startup_state()
                    if self.state.risk_config.max_daily_loss is not None:
                        cutoff = datetime.now(UTC)
                        self.state.income_cutoff = cutoff
                        self.state.daily_realized_pnl = Decimal("0")
                        self.state.risk_day = cutoff.date()
                        self.state.daily_pnl_ready = False
                        baseline = await self.read_client.fetch_startup_realized_pnl(
                            cutoff
                        )
                        self.state.daily_realized_pnl += baseline
                        self.state.risk_day = cutoff.date()
                        self.state.daily_pnl_ready = True
                    await self.rest_client.set_position_mode(
                        dual_side=self.account_config.execution_inputs.hedge_mode,
                    )
                    # Only configure symbols that already carry responsibility.
                    # New entry symbols are configured lazily before their first
                    # order; setting every tradable symbol fails on symbols with
                    # a lower maximum leverage (-4028).
                    active_symbols = {
                        batch.symbol
                        for batch in self.state.batches.values()
                        if batch.quantity > 0
                    } | {
                        order.symbol
                        for order in self.state.orders.values()
                        if not order.is_terminal
                    }
                    for symbol in sorted(active_symbols):
                        await self.rest_client.set_margin_type(
                            symbol,
                            self.account_config.execution_inputs.margin_type,
                        )
                        await self.rest_client.set_leverage(
                            symbol,
                            leverage=self.account_config.execution_inputs.entry_leverage,
                        )
                    if (
                        not self.state.ws_connected
                        or self.state.private_stream_gap
                        or self.state.has_unrecoverable_fault
                    ):
                        raise RuntimeError(
                            "Private state became unavailable during startup"
                        )
                    self.state.set_system_mode(SystemMode.RUNNING)
                    tg.create_task(
                        self.ws_client.run_with_keepalive(
                            keepalive_interval_sec=keepalive_interval_sec,
                            stop_event=self._private_stop,
                        )
                    )
                    tg.create_task(self.strategy_engine.run(stop_event=self.stop_event))
                    tg.create_task(
                        self.exit_manager.run(
                            stop_event=self.stop_event,
                            price_provider=self.strategy_engine.latest_prices.get,
                        )
                    )
                    tg.create_task(
                        self._run_health_heartbeat(stop_event=self.stop_event)
                    )
                    candle_task = tg.create_task(self._run_exit_candles())

                    async def stop_candles() -> None:
                        await self.stop_event.wait()
                        candle_task.cancel()

                    tg.create_task(stop_candles())
                    tg.create_task(self._wait_for_stop())
            finally:
                self._private_stop.set()
                await self.market_data_provider.close()
                await self.ws_client.close()
                await self.exit_manager.close()
                await self.order_executor.close()
                self.state.set_system_mode(SystemMode.STOPPED)
                try:
                    await self._publish_runtime_status(False)
                except Exception:
                    log.exception("stopped_status_publish_failed")
                log.info("daemon_stopped", account=self.account_label)


async def run_nextgen_daemon(
    environment: str,
    account_label: str,
    db: TradingRepositoryProtocol,
    rest_client: BinanceRestClient,
    account_config: LiveRuntimeAccount,
    read_client: BinanceRestClient | None = None,
    market_data_provider: Any = None,
    strategy_engine: MomentumStrategyEngine | None = None,
    exit_manager: ExitManager | None = None,
    symbols: list[str] | None = None,
    ws_client: BinanceUserDataStream | None = None,
    stop_event: asyncio.Event | None = None,
    keepalive_interval_sec: float = 1800.0,
    hub_url: str | None = None,
    market_feed_connector: Any = None,
    market_environment: str = "research",
) -> NextGenDaemon:
    """下一代交易守护进程便捷启动函数"""
    daemon = NextGenDaemon(
        environment=environment,
        account_label=account_label,
        db=db,
        rest_client=rest_client,
        account_config=account_config,
        read_client=read_client,
        ws_client=ws_client,
        market_data_provider=market_data_provider,
        strategy_engine=strategy_engine,
        exit_manager=exit_manager,
        symbols=symbols,
        stop_event=stop_event,
        hub_url=hub_url,
        market_feed_connector=market_feed_connector,
        market_environment=market_environment,
    )
    await daemon.run(keepalive_interval_sec=keepalive_interval_sec)
    return daemon
