"""User-run emergency futures close. Default: exchange reads only.
Stop all strategy processes before --apply. No database writes or automatic resend.
"""

import argparse
import asyncio
import json
import os
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from crypto_momentum_lab.domain.execution.order_state import (
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.binance.client import BinanceUsdMTradeClient
from crypto_momentum_lab.execution_account.orders.state_machine import (
    ExchangeOrderAlreadyAbsentError,
    ExchangeSubmissionTimeoutError,
)


def emit(event, **fields):
    print(
        json.dumps(dict(event=event, **fields), ensure_ascii=False, default=str),
        flush=True,
    )


async def run(account, apply):
    client = BinanceUsdMTradeClient(
        api_key=os.environ["BINANCE_TRADE_API_KEY"],
        api_secret=os.environ["BINANCE_TRADE_API_SECRET"],
        environment="live",
        account_label=account,
        live_submit_enabled=apply,
    )
    try:
        positions = await client.fetch_positions(include_flat=True)
        orders = await client.fetch_open_orders()
        emit(
            "before",
            account=account,
            positions=[
                dict(symbol=p.symbol, side=p.position_side, quantity=p.position_amt)
                for p in positions
                if p.position_amt != 0
            ],
            open_orders=len(orders),
        )
        if not apply:
            return
        # Cancel resting orders before measuring the quantity to close.
        for order in orders:
            try:
                await client.cancel_order_by_client_id(
                    order.symbol, order.client_order_id
                )
            except ExchangeOrderAlreadyAbsentError:
                pass
        if await client.fetch_open_orders():
            raise RuntimeError("resting orders remain; refusing to close")
        positions = await client.fetch_positions(include_flat=True)
        unknown = False
        for p in positions:
            if p.position_amt == 0:
                continue
            plan = OrderExecutionPlan(
                intent_id="emergency-" + uuid4().hex,
                run_id="emergency-20261002-" + account,
                client_order_id="ef_" + uuid4().hex,
                symbol=p.symbol,
                side="SELL" if p.position_amt > 0 else "BUY",
                order_type="MARKET",
                quantity=abs(p.position_amt),
                price=None,
                reduce_only=True,
                position_side=FuturesPositionSide(p.position_side),
                created_at=datetime.now(UTC),
                quantized=True,
            )
            emit(
                "close_requested",
                account=account,
                symbol=p.symbol,
                quantity=plan.quantity,
                client_order_id=plan.client_order_id,
            )
            try:
                receipt = await client.submit_order(plan)
            except ExchangeSubmissionTimeoutError:
                # Never POST a second order after an ambiguous response.
                receipt = await client.query_order_by_client_id(
                    p.symbol, plan.client_order_id
                )
                if receipt is None:
                    unknown = True
                    emit(
                        "unknown_do_not_resubmit", client_order_id=plan.client_order_id
                    )
                    continue
            emit("receipt", client_order_id=plan.client_order_id, state=receipt.state)
        await asyncio.sleep(2)
        remaining = [
            p
            for p in await client.fetch_positions(include_flat=True)
            if p.position_amt != Decimal("0")
        ]
        open_orders = await client.fetch_open_orders()
        emit(
            "after",
            account=account,
            positions=[
                dict(symbol=p.symbol, side=p.position_side, quantity=p.position_amt)
                for p in remaining
            ],
            open_orders=len(open_orders),
            unknown=unknown,
        )
        if remaining or open_orders or unknown:
            raise RuntimeError("not confirmed flat; inspect exchange before retrying")
    finally:
        await client.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "account", choices=("primary", "account-2", "account-3", "account-4")
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args.account, args.apply))
