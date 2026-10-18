from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import text

from crypto_momentum_lab.next_gen.models import Order, OrderStatus, PositionBatch
from crypto_momentum_lab.next_gen.persistence import PostgresTradingRepository
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)

ENVIRONMENT = "test-nextgen-postgres"
ACCOUNT_LABEL = "a1"


@pytest.fixture
async def postgres_repo(
    async_database_url: str,
) -> AsyncIterator[PostgresTradingRepository]:
    engine = create_async_database_engine(async_database_url)
    repo = PostgresTradingRepository(engine)

    # 清理测试数据
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "DELETE FROM live_orders WHERE environment = :env;"
            ),
            {"env": ENVIRONMENT},
        )
        await conn.execute(
            text(
                "DELETE FROM live_position_batches WHERE environment = :env;"
            ),
            {"env": ENVIRONMENT},
        )

    try:
        yield repo
    finally:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "DELETE FROM live_orders WHERE environment = :env;"
                ),
                {"env": ENVIRONMENT},
            )
            await conn.execute(
                text(
                    "DELETE FROM live_position_batches WHERE environment = :env;"
                ),
                {"env": ENVIRONMENT},
            )
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_advisory_lock_mutual_exclusion(
    postgres_repo: PostgresTradingRepository,
) -> None:
    lock_key = f"{ENVIRONMENT}:{ACCOUNT_LABEL}"

    async with postgres_repo.acquire_dedicated_advisory_lock(lock_key):
        # 在第一个连接已持有锁时，第二个连接尝试获取相同锁应当抛出互斥阻断异常
        with pytest.raises(RuntimeError, match="已被另一进程持有"):
            async with postgres_repo.acquire_dedicated_advisory_lock(lock_key):
                pass

    # 退出 context 后，咨询锁应已释放，再次获取应成功
    async with postgres_repo.acquire_dedicated_advisory_lock(lock_key):
        pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_save_and_retrieve_order(
    postgres_repo: PostgresTradingRepository,
) -> None:
    now = datetime.now(UTC)
    order = Order(
        environment=ENVIRONMENT,
        account_label=ACCOUNT_LABEL,
        client_order_id="cml_ord_001",
        batch_id="b_001",
        symbol="BTCUSDT",
        side="BUY",
        position_side="LONG",
        order_type="LIMIT",
        quantity=Decimal("0.5"),
        price=Decimal("60000.0"),
        status=OrderStatus.SUBMITTED,
        exchange_order_id=123456789,
        created_at=now,
        updated_at=now,
    )

    await postgres_repo.async_save_order(order)

    loaded = await postgres_repo.async_get_order(
        ENVIRONMENT, ACCOUNT_LABEL, "cml_ord_001"
    )
    assert loaded is not None
    assert loaded.client_order_id == "cml_ord_001"
    assert loaded.symbol == "BTCUSDT"
    assert loaded.side == "BUY"
    assert loaded.position_side == "LONG"
    assert loaded.status == OrderStatus.SUBMITTED
    assert loaded.quantity == Decimal("0.5")
    assert loaded.price == Decimal("60000.0")
    assert loaded.exchange_order_id == 123456789

    # 测试 ON CONFLICT DO UPDATE
    order.status = OrderStatus.FILLED
    order.filled_qty = Decimal("0.5")
    order.avg_price = Decimal("60000.0")
    order.updated_at = datetime.now(UTC)
    await postgres_repo.async_save_order(order)

    updated = await postgres_repo.async_get_order(
        ENVIRONMENT, ACCOUNT_LABEL, "cml_ord_001"
    )
    assert updated is not None
    assert updated.status == OrderStatus.FILLED
    assert updated.filled_qty == Decimal("0.5")
    assert updated.avg_price == Decimal("60000.0")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_save_and_retrieve_batch(
    postgres_repo: PostgresTradingRepository,
) -> None:
    bar_time = datetime(2026, 10, 18, 12, 0, tzinfo=UTC)
    batch = PositionBatch(
        environment=ENVIRONMENT,
        account_label=ACCOUNT_LABEL,
        batch_id="b_002",
        symbol="ETHUSDT",
        position_side="LONG",
        quantity=Decimal("3.0"),
        entry_price=Decimal("3500.0"),
        entry_bar_time=bar_time,
        status="ACTIVE",
    )

    await postgres_repo.async_save_batch(batch)

    loaded = await postgres_repo.async_get_batch(
        ENVIRONMENT, ACCOUNT_LABEL, "b_002"
    )
    assert loaded is not None
    assert loaded.batch_id == "b_002"
    assert loaded.symbol == "ETHUSDT"
    assert loaded.position_side == "LONG"
    assert loaded.quantity == Decimal("3.0")
    assert loaded.entry_price == Decimal("3500.0")
    assert loaded.status == "ACTIVE"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_atomic_save_order_and_batch(
    postgres_repo: PostgresTradingRepository,
) -> None:
    bar_time = datetime(2026, 10, 18, 12, 15, tzinfo=UTC)
    batch = PositionBatch(
        environment=ENVIRONMENT,
        account_label=ACCOUNT_LABEL,
        batch_id="b_003",
        symbol="SOLUSDT",
        position_side="LONG",
        quantity=Decimal("20.0"),
        entry_price=Decimal("150.0"),
        entry_bar_time=bar_time,
        status="ACTIVE",
    )
    order = Order(
        environment=ENVIRONMENT,
        account_label=ACCOUNT_LABEL,
        client_order_id="cml_ord_003",
        batch_id="b_003",
        symbol="SOLUSDT",
        side="BUY",
        position_side="LONG",
        order_type="MARKET",
        quantity=Decimal("20.0"),
        price=None,
        status=OrderStatus.FILLED,
        filled_qty=Decimal("20.0"),
        avg_price=Decimal("150.0"),
    )

    await postgres_repo.async_save_order_and_batch(order, batch)

    loaded_order = await postgres_repo.async_get_order(
        ENVIRONMENT, ACCOUNT_LABEL, "cml_ord_003"
    )
    loaded_batch = await postgres_repo.async_get_batch(
        ENVIRONMENT, ACCOUNT_LABEL, "b_003"
    )

    assert loaded_order is not None
    assert loaded_order.client_order_id == "cml_ord_003"
    assert loaded_order.status == OrderStatus.FILLED

    assert loaded_batch is not None
    assert loaded_batch.batch_id == "b_003"
    assert loaded_batch.quantity == Decimal("20.0")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_get_active_batches_and_recent_orders(
    postgres_repo: PostgresTradingRepository,
) -> None:
    t0 = datetime(2026, 10, 18, 10, 0, tzinfo=UTC)
    t1 = t0 + timedelta(minutes=15)
    t2 = t1 + timedelta(minutes=15)

    b1 = PositionBatch(
        environment=ENVIRONMENT,
        account_label=ACCOUNT_LABEL,
        batch_id="b_active",
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("0.1"),
        entry_price=Decimal("60000"),
        entry_bar_time=t0,
        status="ACTIVE",
    )
    b2 = PositionBatch(
        environment=ENVIRONMENT,
        account_label=ACCOUNT_LABEL,
        batch_id="b_exiting",
        symbol="ETHUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        entry_price=Decimal("3000"),
        entry_bar_time=t1,
        status="EXITING",
        exiting_quantity=Decimal("1.0"),
    )
    b3 = PositionBatch(
        environment=ENVIRONMENT,
        account_label=ACCOUNT_LABEL,
        batch_id="b_closed",
        symbol="SOLUSDT",
        position_side="LONG",
        quantity=Decimal("0"),
        entry_price=Decimal("150"),
        entry_bar_time=t2,
        status="CLOSED",
        realized_pnl=Decimal("25.0"),
    )

    await postgres_repo.async_save_batch(b1)
    await postgres_repo.async_save_batch(b2)
    await postgres_repo.async_save_batch(b3)

    active_batches = await postgres_repo.async_get_active_batches(
        ENVIRONMENT, ACCOUNT_LABEL
    )
    assert len(active_batches) == 2
    assert [b.batch_id for b in active_batches] == ["b_active", "b_exiting"]

    # 保存 2 个订单并验证 async_get_recent_orders
    o1 = Order(
        environment=ENVIRONMENT,
        account_label=ACCOUNT_LABEL,
        client_order_id="ord_first",
        batch_id="b_active",
        symbol="BTCUSDT",
        side="BUY",
        position_side="LONG",
        order_type="LIMIT",
        quantity=Decimal("0.1"),
        price=Decimal("60000"),
        created_at=t0,
    )
    o2 = Order(
        environment=ENVIRONMENT,
        account_label=ACCOUNT_LABEL,
        client_order_id="ord_second",
        batch_id="b_exiting",
        symbol="ETHUSDT",
        side="SELL",
        position_side="LONG",
        order_type="MARKET",
        quantity=Decimal("1.0"),
        price=None,
        created_at=t1,
    )

    await postgres_repo.async_save_order(o1)
    await postgres_repo.async_save_order(o2)

    recent = await postgres_repo.async_get_recent_orders(
        ENVIRONMENT, ACCOUNT_LABEL, limit=10
    )
    assert len(recent) == 2
    # 按 created_at DESC 排序
    assert recent[0].client_order_id == "ord_second"
    assert recent[1].client_order_id == "ord_first"
