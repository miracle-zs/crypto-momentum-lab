from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import structlog

from crypto_momentum_lab.next_gen.client import (
    BinanceOrderRejectedError,
    BinanceRestClient,
)
from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.models import Order, OrderStatus, SystemMode
from crypto_momentum_lab.next_gen.persistence import TradingRepositoryProtocol

log = structlog.get_logger("order_executor")


class OrderExecutor:
    """下一代极简发单器 (直调 REST API + 预写库 + 延时看门狗)"""

    def __init__(
        self,
        rest_client: BinanceRestClient,
        state: AccountState,
        db: TradingRepositoryProtocol,
    ) -> None:
        self._rest = rest_client
        self._state = state
        self._db = db
        self._watchdog_tasks: set[asyncio.Task[Any]] = set()

    @property
    def rest_client(self) -> BinanceRestClient:
        return self._rest

    async def close(self) -> None:
        """取消所有正在运行的延时看门狗任务"""
        for task in list(self._watchdog_tasks):
            task.cancel()
        if self._watchdog_tasks:
            await asyncio.gather(*self._watchdog_tasks, return_exceptions=True)

    async def submit_order(self, order: Order) -> dict[str, Any]:
        """提交订单：
        1. 幂等与状态校验：必须处于 PENDING；
        2. 执行发单安全门与名义价值上限校验；
        3. 预写库落库为 SUBMITTED (失败则回滚并进入 DEGRADED，绝不外呼)；
        4. 启动 3.0s 超时探针看门狗；
        5. 发送 REST 请求。明确业务拒单 (4xx) 原子更新为 REJECTED 并释放活跃锁；
        6. 网络超时/5xx 服务端错误保持 SUBMITTED 与锁定，交由看门狗触发存疑查证。
        """
        # 1. 状态幂等检查
        if order.status != OrderStatus.PENDING:
            raise RuntimeError(
                f"只能提交处于 PENDING 状态的订单: {order.client_order_id}, "
                f"当前状态: {order.status.name}"
            )

        # 2. 发单安全门与名义价值上限校验
        if order.client_order_id.startswith("e_"):
            if not self._state.can_submit_entry_order():
                raise RuntimeError(
                    f"安全门阻断开仓单: {order.client_order_id}, "
                    f"mode={self._state.system_mode.name}, "
                    f"draining={self._state.is_draining}"
                )
            price = order.price or Decimal("0")
            if price > Decimal("0") and not self._state.check_notional_limits(
                symbol=order.symbol,
                new_quantity=order.quantity,
                current_price=price,
                exclude_order_id=order.client_order_id,
            ):
                raise RuntimeError(
                    f"名义价值上限阻断开仓单: {order.client_order_id}, "
                    f"symbol={order.symbol}"
                )
        else:
            if not self._state.can_submit_exit_order():
                raise RuntimeError(
                    f"安全门阻断平仓单: {order.client_order_id}, "
                    f"mode={self._state.system_mode.name}"
                )

        # 3. 预写库防御：落库失败则立即回滚内存状态并释放锁
        # 绝不向交易所发送不可审计订单
        order.apply_update(OrderStatus.SUBMITTED)
        try:
            await self._db.async_save_order(order)
        except Exception as e:
            log.critical(
                "db_prewrite_failed_rolling_back_order",
                order_id=order.client_order_id,
                err=str(e),
            )
            self._state.rollback_unsubmitted_order(order)
            self._state.has_unrecoverable_fault = True
            self._state.set_system_mode(SystemMode.DEGRADED)
            raise RuntimeError(
                f"订单预落库失败，拒绝向交易所发单并进入安全降级: {e}"
            ) from e

        # 启动单次延时看门狗：发一单盯一单，3.0s 超时未收到 WS 推流则转 RECONCILING
        watchdog_task = asyncio.create_task(
            self._state.watch_submission_timeout(order.client_order_id, timeout_sec=3.0)
        )
        self._watchdog_tasks.add(watchdog_task)
        watchdog_task.add_done_callback(self._watchdog_tasks.discard)

        try:
            res = await self._rest.post_order(
                symbol=order.symbol,
                side=order.side,
                position_side=order.position_side,
                order_type=order.order_type,
                quantity=order.quantity,
                price=order.price,
                client_order_id=order.client_order_id,
            )
            return res
        except BinanceOrderRejectedError as e:
            # 业务明确拒单（如 -2010/-2019 等）：交易所未建单，原子清理活跃锁与预留
            self._state.mark_order_rejected(order.client_order_id, e.message)
            await self._db.async_save_order(order)
            raise
        except Exception as e:
            # 网络超时/5xx：订单处于存疑状态，绝不清理活跃锁，由看门狗转入 RECONCILING
            log.warning(
                "order_submission_in_doubt_due_to_network_failure",
                order_id=order.client_order_id,
                error=str(e),
            )
            raise
