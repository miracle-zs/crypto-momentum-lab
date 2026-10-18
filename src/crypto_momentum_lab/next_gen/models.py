from __future__ import annotations

import secrets
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum, auto


class SystemMode(Enum):
    BOOTING = auto()
    RUNNING = auto()
    DRAINING = auto()
    DEGRADED = auto()
    STOPPED = auto()


class OrderStatus(Enum):
    PENDING = auto()  # 本地待发送
    SUBMITTED = auto()  # 已发往交易所 (等待 WS 推送)
    OPEN = auto()  # 交易所确认挂单 (来自 WS 推送)
    RECONCILING = auto()  # 网络超时查证中
    PARTIALLY_FILLED = auto()  # 部分成交 (来自 WS 推送)
    FILLED = auto()  # 完全成交 (终态，来自 WS 推送)
    CANCELED = auto()  # 已撤销/过期终态 (统一映射 CANCELED / EXPIRED)
    REJECTED = auto()  # 发单未受理/提交失败 (本地终态，因 HTTP 网关明确拒单)


STATUS_RANK: dict[OrderStatus, int] = {
    OrderStatus.PENDING: 0,
    OrderStatus.SUBMITTED: 1,
    OrderStatus.OPEN: 2,
    OrderStatus.RECONCILING: 2,
    OrderStatus.PARTIALLY_FILLED: 3,
    OrderStatus.FILLED: 4,
    OrderStatus.CANCELED: 4,
    OrderStatus.REJECTED: 4,
}


@dataclass
class PositionBatch:
    """活跃持仓批次领域对象 (对应 live_position_batches 表，以 batch_id 为主键)"""

    environment: str
    account_label: str
    batch_id: str
    symbol: str
    position_side: str  # LONG / SHORT
    quantity: Decimal  # 绝对持仓数量 (>= 0)
    entry_price: Decimal
    entry_bar_time: datetime  # 15m K线入场时间戳 (对应 TIMESTAMPTZ)
    status: str = "ACTIVE"  # ACTIVE / EXITING / CLOSED
    exit_order_submitted_at: datetime | None = None
    order_counter: int = 0  # 批次内发单顺序计数器
    realized_pnl: Decimal = Decimal("0")  # 批次关闭时的名义毛盈亏 (USDT)
    exiting_quantity: Decimal = Decimal(
        "0"
    )  # 保留 Lot 预留不变量: 当前已锁定用于平仓的 Lot 数量
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def reserve_for_exit(self, qty: Decimal) -> None:
        """平仓预留防护: 严格防止多个退出规则重复锁定同一个批次的同一笔 Lot"""
        if qty <= Decimal("0"):
            raise ValueError(f"预留平仓数量必须大于 0: {qty}")
        available = self.quantity - self.exiting_quantity
        if qty > available:
            raise ValueError(
                f"批次 {self.batch_id} 可平仓量不足: 请求 {qty}, 可用 {available}"
            )
        self.exiting_quantity += qty
        self.status = "EXITING"
        self.updated_at = datetime.now(UTC)

    def release_exit_reservation(self, qty: Decimal) -> None:
        """撤单或异常时释放平仓 Lot 预留"""
        if qty <= Decimal("0"):
            return
        if qty > self.exiting_quantity:
            raise ValueError(
                f"释放预留数量超过当前已预留量: 请求 {qty}, "
                f"当前预留 {self.exiting_quantity}"
            )
        self.exiting_quantity -= qty
        if self.exiting_quantity == Decimal("0") and self.quantity > Decimal("0"):
            self.status = "ACTIVE"
        self.updated_at = datetime.now(UTC)


@dataclass
class Order:
    """统一订单生命周期领域对象 (对应 live_orders 表)"""

    environment: str
    account_label: str
    client_order_id: str
    batch_id: str
    symbol: str
    side: str  # BUY / SELL
    position_side: str  # LONG / SHORT
    order_type: str  # LIMIT / MARKET
    quantity: Decimal
    price: Decimal | None
    status: OrderStatus = OrderStatus.PENDING
    exchange_order_id: int | None = None
    filled_qty: Decimal = Decimal("0")
    avg_price: Decimal = Decimal("0")
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if len(self.client_order_id) > 36:
            raise ValueError(
                f"client_order_id 超过币安 36 字符硬上限: "
                f"{self.client_order_id} ({len(self.client_order_id)})"
            )

    @property
    def is_terminal(self) -> bool:
        return self.status in (
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
        )

    def apply_update(
        self,
        new_status: OrderStatus,
        cum_filled_qty: Decimal = Decimal("0"),
        avg_price: Decimal = Decimal("0"),
        exchange_order_id: int | None = None,
    ) -> Decimal:
        """单调性防御与成交吸收：终态不可回退，位阶不可逆退，吸收单调成交。"""
        if exchange_order_id is not None:
            self.exchange_order_id = exchange_order_id

        # 1. 单调位阶推进保护：仅当 new_status 位阶 >= 当前且非终态时才允许推进
        if not self.is_terminal and STATUS_RANK[new_status] >= STATUS_RANK[self.status]:
            self.status = new_status

        # 2. 累计成交量严格单调递增：即使已在终态，仍吸收迟到的更大成交量以防止漏计
        delta_qty = Decimal("0")
        if cum_filled_qty > self.filled_qty:
            delta_qty = cum_filled_qty - self.filled_qty
            self.filled_qty = cum_filled_qty
            self.avg_price = avg_price
        self.updated_at = datetime.now(UTC)
        return delta_qty


def generate_batch_id(
    symbol: str,
    position_side: str,
    account_label: str,
    timestamp_str: str | None = None,
    salt: str | None = None,
    counter: int = 1,
) -> str:
    """生成带多账户标签且严格控制在 <= 31 字符的 batch_id。

    格式: {base_sym}_{side}_{ts}_{acc}_{salt}_{cnt}
    始终保留随机盐与秒级时间戳，杜绝重启或并发时 ON CONFLICT 覆盖旧批次。
    """
    raw_base = symbol.removesuffix("USDT")
    side = "L" if position_side.upper() == "LONG" else "S"
    acc = account_label[:3]
    s = salt if salt is not None else secrets.token_hex(1)  # 2 hex chars
    cnt = f"b{counter}"

    if timestamp_str is None:
        ts = datetime.now(UTC).strftime("%y%m%d%H%M%S")  # 12 位
    else:
        ts = timestamp_str

    # 动态裁剪 base_sym，确保总长度严格 <= 31，且绝对保留随机盐与时间戳
    fixed_len = len(side) + len(ts) + len(acc) + len(s) + len(cnt) + 5
    max_base_len = 31 - fixed_len
    base_sym = raw_base[: max(3, max_base_len)]

    candidate = f"{base_sym}_{side}_{ts}_{acc}_{s}_{cnt}"
    if len(candidate) > 31:
        excess = len(candidate) - 31
        if len(base_sym) - excess >= 3:
            base_sym = base_sym[:-excess]
            candidate = f"{base_sym}_{side}_{ts}_{acc}_{s}_{cnt}"
        else:
            ts_short = ts[:-excess]
            candidate = f"{base_sym}_{side}_{ts_short}_{acc}_{s}_{cnt}"

    assert len(candidate) <= 31, (
        f"生成的 batch_id 超过 31 字符限制: {candidate} ({len(candidate)})"
    )
    return candidate


def generate_entry_order_id(batch_id: str, seq: int) -> str:
    """生成开仓 client_order_id (<= 36 字符): e_{batch_id}_{seq:02d}"""
    client_id = f"e_{batch_id}_{seq:02d}"
    assert len(client_id) <= 36, (
        f"开仓 client_order_id 超过 36 字符: {client_id} ({len(client_id)})"
    )
    return client_id


def generate_exit_order_id(batch_id: str, order_type: str = "LIMIT") -> str:
    """生成平仓 client_order_id (<= 36 字符): fl_{batch_id} 或 fm_{batch_id}"""
    prefix = "fl_" if order_type.upper() == "LIMIT" else "fm_"
    client_id = f"{prefix}{batch_id}"
    assert len(client_id) <= 36, (
        f"平仓 client_order_id 超过 36 字符: {client_id} ({len(client_id)})"
    )
    return client_id
