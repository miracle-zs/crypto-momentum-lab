"""User-run emergency futures close. Default: exchange reads only.
Stop all strategy processes before --apply. No database writes or automatic resend.
"""

import argparse
import asyncio
import json
import os
from decimal import Decimal
from uuid import uuid4

from crypto_momentum_lab.next_gen.client import (
    BinanceNetworkError,
    BinanceOrderRejectedError,
    BinanceRestClient,
)


def emit(event, **fields):
    print(
        json.dumps(dict(event=event, **fields), ensure_ascii=False, default=str),
        flush=True,
    )


async def run(account, apply):
    client = BinanceRestClient(
        api_key=os.environ["BINANCE_TRADE_API_KEY"],
        api_secret=os.environ["BINANCE_TRADE_API_SECRET"],
        environment="live",
    )
    try:
        positions = (await client.fetch_account_information()).positions
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
        if not await client.get_position_mode():
            raise RuntimeError(
                "emergency close requires hedge mode; refusing to submit orders"
            )
        # Cancel resting orders before measuring the quantity to close.
        for order in orders:
            try:
                await client.cancel_order(
                    str(order["symbol"]),
                    client_order_id=str(order["clientOrderId"]),
                )
            except BinanceOrderRejectedError as exc:
                if exc.code not in {-2011, -2013}:
                    raise
        if await client.fetch_open_orders():
            raise RuntimeError("resting orders remain; refusing to close")
        positions = (await client.fetch_account_information()).positions
        unknown = False
        for p in positions:
            if p.position_amt == 0:
                continue
            client_order_id = "ef_" + uuid4().hex
            side = "SELL" if p.position_amt > 0 else "BUY"
            quantity = abs(p.position_amt)
            emit(
                "close_requested",
                account=account,
                symbol=p.symbol,
                quantity=quantity,
                client_order_id=client_order_id,
            )
            try:
                receipt = await client.post_order(
                    symbol=p.symbol,
                    side=side,
                    position_side=p.position_side,
                    order_type="MARKET",
                    quantity=quantity,
                    client_order_id=client_order_id,
                )
            except BinanceNetworkError:
                # Never POST a second order after an ambiguous response.
                try:
                    receipt = await client.fetch_startup_order(
                        p.symbol, client_order_id
                    )
                except (BinanceNetworkError, BinanceOrderRejectedError):
                    unknown = True
                    emit(
                        "unknown_do_not_resubmit", client_order_id=client_order_id
                    )
                    continue
            emit(
                "receipt",
                client_order_id=client_order_id,
                state=receipt.get("status"),
            )
        await asyncio.sleep(2)
        remaining = [
            p
            for p in (await client.fetch_account_information()).positions
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
        await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "account", choices=("primary", "account-2", "account-3", "account-4")
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args.account, args.apply))
