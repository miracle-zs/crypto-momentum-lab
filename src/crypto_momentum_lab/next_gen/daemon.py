from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Any

import structlog

from crypto_momentum_lab.next_gen.client import (
    BinanceRestClient,
    BinanceUserDataStream,
)
from crypto_momentum_lab.next_gen.engine import (
    AccountState,
    assert_flat_baseline_or_abort,
)
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.exit_manager import ExitManager
from crypto_momentum_lab.next_gen.models import SystemMode
from crypto_momentum_lab.next_gen.persistence import (
    TradingRepositoryProtocol,
    compute_advisory_lock_key,
)
from crypto_momentum_lab.next_gen.strategy_engine import MomentumStrategyEngine

log = structlog.get_logger("next_gen_daemon")


class NextGenDaemon:
    """下一代极简单进程交易守护进程 (TaskGroup 结构化生命周期)"""

    def __init__(
        self,
        environment: str,
        account_label: str,
        db: TradingRepositoryProtocol,
        rest_client: BinanceRestClient,
        ws_client: BinanceUserDataStream | None = None,
        market_data_provider: Any = None,
        strategy_engine: MomentumStrategyEngine | None = None,
        exit_manager: ExitManager | None = None,
        symbols: list[str] | None = None,
        stop_event: asyncio.Event | None = None,
        hub_url: str | None = None,
        market_feed_connector: Any = None,
    ) -> None:
        self.environment = environment
        self.account_label = account_label
        self.db = db
        self.rest_client = rest_client
        self.ws_client = ws_client or BinanceUserDataStream(
            rest_client=rest_client,
        )
        self.stop_event = stop_event or asyncio.Event()

        self.state = AccountState(
            environment=environment,
            account_label=account_label,
            db=db,
            rest_client=rest_client,
        )
        self.ws_client.on_status_change = self.state.on_ws_status_change
        self.state.attach_ws_client(self.ws_client)

        self.order_executor = OrderExecutor(
            rest_client=rest_client,
            state=self.state,
            db=db,
        )
        self.exit_manager = exit_manager or ExitManager(
            rest_client=rest_client,
            state=self.state,
            executor=self.order_executor,
        )
        self.strategy_engine = strategy_engine or MomentumStrategyEngine(
            state=self.state,
            order_executor=self.order_executor,
            symbols=symbols,
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
                on_bar=self.strategy_engine.on_bar,
                hub_url=resolved_hub,
                ws_connector=market_feed_connector,
            )

    async def _wait_for_stop(self) -> None:
        """等待外部停止信号并平稳触发 TaskGroup 退出"""
        await self.stop_event.wait()
        log.info("stop_event_received_shutting_down", account=self.account_label)

        # 1. 停机时撤销所有未完成挂单 (优先撤开仓单，防止停机时被撮合导致孤儿仓位)
        try:
            for order in list(self.state.orders.values()):
                if not order.is_terminal:
                    try:
                        await self.rest_client.cancel_order(
                            symbol=order.symbol,
                            client_order_id=order.client_order_id,
                        )
                        log.info(
                            "shutdown_cancelled_pending_order",
                            order_id=order.client_order_id,
                            symbol=order.symbol,
                        )
                    except Exception as e:
                        log.warning(
                            "shutdown_failed_to_cancel_order",
                            order_id=order.client_order_id,
                            err=str(e),
                        )
        except Exception as e:
            log.warning("shutdown_cleanup_orders_error", err=str(e))

        if self.market_data_provider and hasattr(self.market_data_provider, "close"):
            await self.market_data_provider.close()
        await self.ws_client.close()
        await self.exit_manager.close()
        await self.order_executor.close()

    async def _run_health_heartbeat(
        self, stop_event: asyncio.Event, interval_sec: float = 15.0
    ) -> None:
        """周期性向容器共享健康目录写入心跳文件，支持本地健康监控检测"""
        health_dir_env = os.environ.get("CML_LOCAL_HEALTH_DIR", "/run/cml/health")
        health_dir = Path(health_dir_env)
        account_health_file = health_dir / f"nextgen-{self.account_label}.ok"
        status_file = health_dir / "status"
        database_file = health_dir / "database"

        try:
            health_dir.mkdir(parents=True, exist_ok=True)  # noqa: ASYNC240
        except Exception as e:
            log.warning("failed_to_create_health_dir", path=str(health_dir), err=str(e))

        try:
            while not stop_event.is_set():
                if self.state.system_mode == SystemMode.RUNNING:
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
        2. 行情与策略预热 (支持本地数据库 15s Bar 与 REST K线双重兜底)；
        3. 验证/设置 Hedge Mode 与 5x 杠杆，严格空仓基线检查；
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

            # 阶段 2: 策略行情预热
            if self.market_data_provider and hasattr(
                self.market_data_provider, "warmup"
            ):
                await self.market_data_provider.warmup()
            await self.strategy_engine.warmup_market_data(
                rest_client=self.rest_client, db=self.db
            )

            # 阶段 3: 验证/设置持仓模式与杠杆倍数，严格空仓基线检查
            try:
                if hasattr(self.rest_client, "set_position_mode"):
                    await self.rest_client.set_position_mode(dual_side=True)
                    log.info(
                        "position_mode_ensured_dual_side",
                        account=self.account_label,
                    )
            except Exception as e:
                log.warning("failed_to_ensure_dual_side_position_mode", err=str(e))

            for sym in self.strategy_engine.symbols:
                try:
                    if hasattr(self.rest_client, "set_leverage"):
                        await self.rest_client.set_leverage(sym, leverage=5)
                except Exception as e:
                    log.warning("failed_to_set_symbol_leverage", symbol=sym, err=str(e))

            await assert_flat_baseline_or_abort(self.rest_client, self.state)

            # 阶段 4: WebSocket 私有流连接与握手
            await self.ws_client.connect(on_event=self.state.on_live_event)
            self.state.set_system_mode(SystemMode.RUNNING)
            log.info(
                "daemon_running",
                env=self.environment,
                account=self.account_label,
            )

            try:
                # 阶段 5: 结构化并发协同 (asyncio.TaskGroup)
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(
                        self.ws_client.run_stream(stop_event=self.stop_event)
                    )
                    tg.create_task(
                        self.ws_client.run_with_keepalive(
                            keepalive_interval_sec=keepalive_interval_sec,
                            stop_event=self.stop_event,
                        )
                    )
                    if self.market_data_provider and hasattr(
                        self.market_data_provider, "run"
                    ):
                        tg.create_task(
                            self.market_data_provider.run(stop_event=self.stop_event)
                        )
                    tg.create_task(self.strategy_engine.run(stop_event=self.stop_event))
                    tg.create_task(self.exit_manager.run(stop_event=self.stop_event))
                    tg.create_task(
                        self._run_health_heartbeat(stop_event=self.stop_event)
                    )
                    tg.create_task(self._wait_for_stop())
            finally:
                self.state.set_system_mode(SystemMode.STOPPED)
                if self.market_data_provider and hasattr(
                    self.market_data_provider, "close"
                ):
                    await self.market_data_provider.close()
                await self.exit_manager.close()
                await self.order_executor.close()
                log.info(
                    "daemon_stopped",
                    env=self.environment,
                    account=self.account_label,
                )


async def run_nextgen_daemon(
    environment: str,
    account_label: str,
    db: TradingRepositoryProtocol,
    rest_client: BinanceRestClient,
    market_data_provider: Any = None,
    strategy_engine: MomentumStrategyEngine | None = None,
    exit_manager: ExitManager | None = None,
    symbols: list[str] | None = None,
    ws_client: BinanceUserDataStream | None = None,
    stop_event: asyncio.Event | None = None,
    keepalive_interval_sec: float = 1800.0,
    hub_url: str | None = None,
    market_feed_connector: Any = None,
) -> NextGenDaemon:
    """下一代交易守护进程便捷启动函数"""
    daemon = NextGenDaemon(
        environment=environment,
        account_label=account_label,
        db=db,
        rest_client=rest_client,
        ws_client=ws_client,
        market_data_provider=market_data_provider,
        strategy_engine=strategy_engine,
        exit_manager=exit_manager,
        symbols=symbols,
        stop_event=stop_event,
        hub_url=hub_url,
        market_feed_connector=market_feed_connector,
    )
    await daemon.run(keepalive_interval_sec=keepalive_interval_sec)
    return daemon
