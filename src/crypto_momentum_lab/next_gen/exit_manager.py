from __future__ import annotations

import asyncio
from decimal import Decimal

import structlog

from crypto_momentum_lab.next_gen.client import BinanceRestClient
from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.models import OrderStatus

log = structlog.get_logger("exit_manager")


class ExitManager:
    """批次退出管理器：管理 15m 宽限期限价退出与超时市价清扫 (Sweep)"""

    def __init__(
        self,
        rest_client: BinanceRestClient,
        state: AccountState,
        executor: OrderExecutor | None = None,
    ) -> None:
        self._rest = rest_client
        self._state = state
        if executor is not None:
            self._executor = executor
        elif state.db is not None:
            self._executor = OrderExecutor(rest_client, state, state.db)
        else:
            raise ValueError("必须提供 executor 或 state.db 不能为空")

    async def execute_batch_exit(
        self,
        batch_id: str,
        limit_price: Decimal,
        grace_period_sec: float = 15.0,
    ) -> None:
        """执行批次退出：
        1. 发送限价平仓单 (LIMIT fl_{batch_id})；
        2. 经过 grace_period_sec 宽限期；
        3. 若批次仍有剩余持仓，撤销限价单；
        4. 释放原限价单未成交锁与预留，以市价单 (MARKET fm) 扫平剩余持仓。
        """
        batch = self._state.batches.get(batch_id)
        if not batch or batch.quantity <= Decimal("0"):
            return

        # 1. 提交限价退出单
        limit_order = self._state.create_exit_order(
            batch_id=batch_id,
            quantity=batch.quantity,
            price=limit_price,
            order_type="LIMIT",
        )
        await self._executor.submit_order(limit_order)

        # 2. 宽限期等待
        await asyncio.sleep(grace_period_sec)

        # 3. 检查批次是否仍有敞口
        batch = self._state.batches.get(batch_id)
        if not batch or batch.quantity <= Decimal("0"):
            return

        # 4. 宽限期超时仍未完全成交：撤销限价单
        try:
            await self._rest.cancel_order(
                symbol=batch.symbol,
                client_order_id=limit_order.client_order_id,
            )
            # 撤单确认后若尚未被推流置为终态，主动收敛订单与预留以供市价单接替
            if not limit_order.is_terminal:
                limit_order.apply_update(OrderStatus.CANCELED)
                self._state._release_active_order_lock(limit_order)
                unfilled = max(
                    Decimal("0"), limit_order.quantity - limit_order.filled_qty
                )
                batch.release_exit_reservation(unfilled)
        except Exception as e:
            log.warning(
                "exit_manager_cancel_limit_order_failed",
                order_id=limit_order.client_order_id,
                err=str(e),
            )

        # 5. 市价清扫剩余未结数量
        batch = self._state.batches.get(batch_id)
        if batch and batch.quantity > Decimal("0"):
            market_order = self._state.create_exit_order(
                batch_id=batch_id,
                quantity=batch.quantity,
                price=None,
                order_type="MARKET",
            )
            await self._executor.submit_order(market_order)
