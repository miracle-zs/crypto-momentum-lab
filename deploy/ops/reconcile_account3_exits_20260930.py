"""Scoped incident recovery for three account-3 PHAROS exits on 2026-09-30.

Run in the existing account-3 strategy container. Default is read-only;
--apply uses the decision UoW after all exchange and durable-fact guards pass.
The trade client has submissions disabled; only exchange GET calls are used.
This closes incident outbox records, not the unresolved stream-coverage issue.
"""

import asyncio
import json
import os
import sys
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from crypto_momentum_lab.domain.execution.order_state import (
    deterministic_client_order_id,
)
from crypto_momentum_lab.execution_account.binance.client import BinanceUsdMTradeClient
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresDecisionUnitOfWork,
)

IDS = (
    "dec_PHAROSUSDT_4e49289bcf7bbced",
    "dec_PHAROSUSDT_68de065b55de733c",
    "dec_PHAROSUSDT_5ee41e9b75650f1c",
)
POLICY = "live/account-3/orderflow_impulse"
REASON = "exchange_absence_reconciled_original_batch_closed_201148019"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


async def main():
    apply = sys.argv[1:] == ["--apply"]
    engine = create_async_engine(
        os.environ.get("CML_EXECUTION_DATABASE_URL") or os.environ["CML_DATABASE_URL"]
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    client = BinanceUsdMTradeClient(
        api_key=os.environ["BINANCE_TRADE_API_KEY"],
        api_secret=os.environ["BINANCE_TRADE_API_SECRET"],
        environment="live",
        account_label="account-3",
        live_submit_enabled=False,
    )
    try:
        async with factory() as session:
            async with session.begin():
                await session.execute(text("SET TRANSACTION READ ONLY"))
                await session.execute(text("SET LOCAL statement_timeout='5s'"))
                rows = (
                    (
                        await session.execute(
                            text(
                                "SELECT decision_id,command_id,command_payload,status "
                                "FROM durable_decision_exits WHERE policy_key=:policy "
                                "AND decision_id IN (:a,:b,:c)"
                            ),
                            dict(policy=POLICY, a=IDS[0], b=IDS[1], c=IDS[2]),
                        )
                    )
                    .mappings()
                    .all()
                )
                require(len(rows) == 3, "expected exactly three incident records")
                by_id = {r["decision_id"]: r for r in rows}
                pending = (
                    (
                        await session.execute(
                            text(
                                "SELECT decision_id FROM durable_decision_exits WHERE "
                                "policy_key=:policy AND status='PENDING'"
                            ),
                            dict(policy=POLICY),
                        )
                    )
                    .scalars()
                    .all()
                )
                require(set(pending) <= set(IDS), "unexpected pending exits; stop")
                for decision in IDS:
                    row = by_id[decision]
                    payload = row["command_payload"]
                    require(
                        payload["position_key"]
                        == dict(
                            symbol="PHAROSUSDT",
                            environment="live",
                            account_label="account-3",
                            position_side="LONG",
                        ),
                        "scope mismatch",
                    )
                    require(
                        payload["reduce_only"]
                        and Decimal(payload["requested_quantity"]) == 132,
                        "quantity mismatch",
                    )
                    require(
                        len(payload["allocation_plan"]["allocations"]) == 1
                        and payload["allocation_plan"]["allocations"][0]["batch_id"]
                        == "ep_PHAROSUSDT_20260930013716_3_b1",
                        "batch mismatch",
                    )
                closed = (
                    await session.execute(
                        text(
                            "SELECT sum(quantity) FROM account_fill_events WHERE "
                            "environment='live' AND account_label='account-3' AND "
                            "symbol='PHAROSUSDT' AND order_id='201148019' AND "
                            "side='SELL'"
                        )
                    )
                ).scalar_one()
                require(closed == 132, "complete account exit fills required")
                outstanding = (
                    await session.execute(
                        text(
                            "SELECT "
                            "coalesce(sum(reserved_quantity-consumed_quantity-"
                            "released_quantity),0) "
                            "FROM position_reservations WHERE "
                            "account_label='account-3' AND symbol='PHAROSUSDT'"
                        )
                    )
                ).scalar_one()
                require(outstanding == 0, "unsettled reservation remains")
                for decision in IDS[:2]:
                    intent = "intent_exit_" + by_id[decision]["command_id"]
                    count = (
                        await session.execute(
                            text(
                                "SELECT count(*) FROM exchange_orders WHERE "
                                "intent_id=:intent"
                            ),
                            dict(intent=intent),
                        )
                    ).scalar_one()
                    require(count == 0, "early command has durable order; stop")
        positions = await client.fetch_positions(include_flat=True)
        longs = [
            p
            for p in positions
            if p.symbol == "PHAROSUSDT" and p.position_side == "LONG"
        ]
        require(
            len(longs) == 1 and longs[0].position_amt == 0,
            "explicit exchange LONG flat row required",
        )
        opens = await client.fetch_open_orders()
        require(
            not any(o.symbol == "PHAROSUSDT" for o in opens),
            "exchange open order remains",
        )
        for decision in IDS:
            row = by_id[decision]
            cid = deterministic_client_order_id("live-account-3-v1", row["command_id"])
            order = await client.query_order_by_client_id("PHAROSUSDT", cid)
            if decision == IDS[-1]:
                print(
                    json.dumps(
                        dict(
                            client_order_id=cid,
                            observed_order=None
                            if order is None
                            else dict(
                                id=order.exchange_order_id,
                                state=order.state.value,
                                quantity=str(order.executed_quantity),
                            ),
                        )
                    )
                )
                require(
                    order is not None
                    and order.exchange_order_id == "201148019"
                    and order.state.value == "filled"
                    and order.executed_quantity == 132,
                    "filled exchange receipt mismatch",
                )
                target = "DISPATCHED"
            else:
                require(order is None, "early exchange order exists; stop")
                target = "SUPERSEDED"
            require(
                row["status"] in {"PENDING", target}, "unexpected terminal disposition"
            )
            print(
                json.dumps(
                    dict(
                        decision_id=decision,
                        client_order_id=cid,
                        exchange_state="absent" if order is None else order.state.value,
                        target_status=target,
                        apply=apply,
                    )
                )
            )
        if apply:
            uow = AsyncPostgresDecisionUnitOfWork(factory)
            # All exchange and durable-fact guards above finish before any mutation.
            require(
                await uow.mark_exit_dispatched(IDS[-1], by_id[IDS[-1]]["command_id"]),
                "missing receipt row",
            )
            for decision in IDS[:2]:
                require(
                    await uow.mark_exit_superseded(
                        decision, by_id[decision]["command_id"], REASON
                    ),
                    "missing supersede row",
                )
        print(
            "PASS: exchange absence/filled receipts, explicit flat position, complete "
            "fills and zero reservations verified"
        )
    finally:
        await client.aclose()
        await engine.dispose()


asyncio.run(main())
