from __future__ import annotations

import asyncio
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
    ) -> None:
        self.environment = environment
        self.account_label = account_label
        self.db = db
        self.rest_client = rest_client
        self.ws_client = ws_client or BinanceUserDataStream(
            rest_client=rest_client,
        )
        self.market_data_provider = market_data_provider
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

    async def _wait_for_stop(self) -> None:
        """等待外部停止信号并平稳触发 TaskGroup 退出"""
        await self.stop_event.wait()
        log.info("stop_event_received_shutting_down", account=self.account_label)
        await self.ws_client.close()
        await self.exit_manager.close()
        await self.order_executor.close()

    async def run(self, keepalive_interval_sec: float = 1800.0) -> None:
        """端到端生命周期执行入口：
        1. 获取 PostgreSQL 会话级 Advisory Lock (防多进程开仓互斥)；
        2. 行情与策略预热；
        3. 严格空仓基线检查 (assert_flat_baseline_or_abort)；
        4. 建立 WebSocket 长连并握手；
        5. 启动 TaskGroup 结构化并发协程 (WS 流、Keepalive、策略计算、退出评估)；
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
            await self.strategy_engine.warmup_market_data()

            # 阶段 3: 严格空仓基线检查 (存量非零持仓/挂单则 Fail-Fast 阻断)
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
                    tg.create_task(self.strategy_engine.run(stop_event=self.stop_event))
                    tg.create_task(self.exit_manager.run(stop_event=self.stop_event))
                    tg.create_task(self._wait_for_stop())
            finally:
                self.state.set_system_mode(SystemMode.STOPPED)
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
    )
    await daemon.run(keepalive_interval_sec=keepalive_interval_sec)
    return daemon
