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
            text("DELETE FROM live_orders WHERE environment = :env;"),
            {"env": ENVIRONMENT},
        )
        await conn.execute(
            text("DELETE FROM live_position_batches WHERE environment = :env;"),
            {"env": ENVIRONMENT},
        )

    try:
        yield repo
    finally:
        async with engine.begin() as conn:
            await conn.execute(
                text("DELETE FROM live_orders WHERE environment = :env;"),
                {"env": ENVIRONMENT},
            )
            await conn.execute(
                text("DELETE FROM live_position_batches WHERE environment = :env;"),
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

    loaded = await postgres_repo.async_get_batch(ENVIRONMENT, ACCOUNT_LABEL, "b_002")
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_market_state_warmup_preserves_order_flow_fields(postgres_repo):
    from dataclasses import replace

    from sqlalchemy import delete, insert

    from crypto_momentum_lab.persistence.postgres.models import RuntimeMarketState15sRow
    from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
        runtime_state_row,
    )
    from tests.unit.persistence.postgres.test_runtime_state_repository import (
        fixture_state,
    )

    states = tuple(
        replace(fixture_state("BTCUSDT", i), environment=ENVIRONMENT) for i in range(3)
    )
    async with postgres_repo._engine.begin() as connection:
        await connection.execute(
            delete(RuntimeMarketState15sRow).where(
                RuntimeMarketState15sRow.environment == ENVIRONMENT,
            )
        )
        await connection.execute(
            insert(RuntimeMarketState15sRow),
            [
                runtime_state_row(
                    row,
                    source_watermark_at=row.bucket_end,
                    input_sequence_min=None,
                    input_sequence_max=None,
                )
                for row in states
            ],
        )
    try:
        loaded = await postgres_repo.async_get_historical_market_states(
            ENVIRONMENT,
            ["BTCUSDT"],
            2,
        )
        assert loaded == list(states[-2:])
        assert loaded[-1].aggressive_buy_notional == states[-1].aggressive_buy_notional
        assert loaded[-1].last_ask_price == states[-1].last_ask_price
    finally:
        async with postgres_repo._engine.begin() as connection:
            await connection.execute(
                delete(RuntimeMarketState15sRow).where(
                    RuntimeMarketState15sRow.environment == ENVIRONMENT,
                )
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_risk_configuration_is_required_and_scoped_to_account(postgres_repo):
    from dataclasses import asdict, replace

    from sqlalchemy import delete, insert

    from crypto_momentum_lab.persistence.postgres.models import RiskConfigSnapshotRow
    from tests.unit.next_gen.factories import risk_config

    config = replace(
        risk_config(ENVIRONMENT, "risk-test"), max_gross_notional=Decimal("321")
    )
    async with postgres_repo._engine.begin() as connection:
        await connection.execute(
            delete(RiskConfigSnapshotRow).where(
                RiskConfigSnapshotRow.environment == ENVIRONMENT,
            )
        )
        await connection.execute(
            insert(RiskConfigSnapshotRow).values(
                config_hash=config.config_hash,
                **asdict(config),
            )
        )
    try:
        loaded = await postgres_repo.async_get_risk_config(ENVIRONMENT, "risk-test")
        assert loaded == config
        with pytest.raises(RuntimeError, match="No persisted risk"):
            await postgres_repo.async_get_risk_config(ENVIRONMENT, "different-account")
    finally:
        async with postgres_repo._engine.begin() as connection:
            await connection.execute(
                delete(RiskConfigSnapshotRow).where(
                    RiskConfigSnapshotRow.environment == ENVIRONMENT,
                )
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dashboard_reads_nextgen_state_and_never_invents_margin(postgres_repo):
    from dataclasses import replace
    from pathlib import Path

    from sqlalchemy import delete
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from crypto_momentum_lab.live_rollout.runtime_manifest import (
        load_live_runtime_manifest,
    )
    from crypto_momentum_lab.next_gen.dashboard import NextGenDashboardQueries
    from crypto_momentum_lab.next_gen.db_models import (
        LiveOrderRow,
        LivePositionBatchRow,
    )
    from crypto_momentum_lab.operator_dashboard.status import OperationalStatus
    from crypto_momentum_lab.persistence.postgres.models import StrategyRuntimeEventRow

    label = "nextgen-dashboard-test"
    now = datetime.now(UTC)
    manifest = load_live_runtime_manifest(
        Path("deploy/live-runtime.yaml"), environment={"CML_CODE_COMMIT": "test"}
    )
    config = replace(manifest.account("primary"), label=label)
    queries = NextGenDashboardQueries(
        async_sessionmaker(postgres_repo._engine), (config,), clock=lambda: now
    )
    batch = PositionBatch(
        "live",
        label,
        "dashboard-batch",
        "BTCUSDT",
        "LONG",
        Decimal("1"),
        Decimal("100"),
        now,
    )
    order = Order(
        "live",
        label,
        "e_dashboard_batch_01",
        batch.batch_id,
        "BTCUSDT",
        "BUY",
        "LONG",
        "LIMIT",
        Decimal("1"),
        Decimal("100"),
        status=OrderStatus.OPEN,
    )

    async def cleanup():
        async with postgres_repo._engine.begin() as connection:
            await connection.execute(
                delete(StrategyRuntimeEventRow).where(
                    StrategyRuntimeEventRow.run_id == f"nextgen:live:{label}",
                )
            )
            for model in (LiveOrderRow, LivePositionBatchRow):
                await connection.execute(
                    delete(model).where(model.account_label == label)
                )

    await cleanup()
    try:
        await postgres_repo.async_save_order_and_batch(order, batch)
        await postgres_repo.async_save_entry_signal(
            "live",
            label,
            now,
            {"signal_id": "dashboard-signal", "symbol": "BTCUSDT", "side": "long"},
        )
        await postgres_repo.async_save_order(
            Order(
                "live",
                label,
                "fm_dashboard_batch_01",
                batch.batch_id,
                "BTCUSDT",
                "SELL",
                "LONG",
                "MARKET",
                Decimal("0.1"),
                None,
                status=OrderStatus.FILLED,
                filled_qty=Decimal("0.1"),
                avg_price=Decimal("105"),
            )
        )
        await postgres_repo.async_save_runtime_status(
            "live",
            label,
            now,
            {
                "halt_active": False,
                "started_at": (now - timedelta(minutes=5)).isoformat(),
                "mode": "RUNNING",
                "ready": True,
                "strategy_name": "orderflow_impulse",
                "entry_gate_open": True,
                "exit_gate_open": True,
                "ws_connected": True,
                "balances": {"USDT": "1000"},
                "unrealized_pnl": "5",
                "market_data": {
                    "bucket_end": now.isoformat(),
                    "received_at": now.isoformat(),
                    "delay_ms": 443.1,
                },
            },
        )
        account = await queries.account(account_label=label)
        assert account.status is OperationalStatus.READY
        assert account.positions[0]["batch_id"] == batch.batch_id
        assert account.positions[0]["position_amt"] == "1.0000000000"
        assert account.open_orders[0]["client_order_id"] == order.client_order_id
        assert account.balances[0]["wallet_balance"] == "1000"
        assert account.balances[0]["available_balance"] is None
        assert account.summary["usdt_wallet_balance"] == "1000"
        assert account.summary["total_unrealized_pnl"] == "5"
        assert account.summary["recent_trade_count"] == 1
        assert account.summary["gross_position_notional"] is None
        assert account.live_signals[0]["signal_id"] == "dashboard-signal"
        assert account.fills[0]["is_exit"] is True
        metrics = await queries.live_account_metrics()
        point = metrics.accounts[0].metrics_curve[-1]
        assert Decimal(point.equity) == Decimal("1005")
        assert point.margin_used is None
        import httpx

        from crypto_momentum_lab.operator_dashboard.api import create_dashboard_app
        from crypto_momentum_lab.operator_dashboard.queries import DashboardQueries

        facade = DashboardQueries(
            async_sessionmaker(postgres_repo._engine),
            clock=lambda: now,
            execution_engine="nextgen",
            nextgen_accounts=(config,),
        )
        app = create_dashboard_app(queries=facade)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get("/api/account", params={"account_label": label})
            assert response.status_code == 200, response.text
            assert response.json()["positions"][0]["batch_id"] == batch.batch_id
            response = await client.get("/api/readiness")
            assert response.status_code == 200, response.text
            assert response.json()["tradeability"]["entry_gate_open"] is True
            response = await client.get("/api/overview")
            runner = next(
                row
                for row in response.json()["services"]
                if row["name"] == "strategy-runner"
            )
            assert runner["status"] == "FRESH"
            assert runner["details"]["engine"] == "nextgen"
            assert runner["details"]["status"] == "READY"
            assert datetime.fromisoformat(
                runner["details"]["started_at"]
            ) == now - timedelta(minutes=5)
            response = await client.get("/api/performance")
            assert response.json()["market_data"]["market_delay_ms"] == 443.1
            assert (
                response.json()["market_data"]["latency_source"] == "strategy_receive"
            )
            response = await client.get(
                "/api/account",
                params={"account_label": label, "view": "legacy_history"},
            )
            assert response.status_code == 200, response.text
            assert response.json()["data_source"] == "legacy_history"
            assert response.json()["positions"] == []
        stale = NextGenDashboardQueries(
            async_sessionmaker(postgres_repo._engine),
            (config,),
            clock=lambda: now + timedelta(seconds=181),
        )
        assert (await stale.live_accounts()).accounts[
            0
        ].status is OperationalStatus.STALE
        assert (await stale.market_performance()).market_delay_ms is None
    finally:
        await cleanup()
