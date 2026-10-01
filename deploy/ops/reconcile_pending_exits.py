"""Reconcile recent exit receipts using exchange GETs and the decision UoW.

Run inside the matching live strategy container with its account/session.
Default is read-only. --apply advances only verified terminal dispositions;
unknown outcomes remain pending. No exchange submission/cancellation is enabled.
"""

import argparse
import asyncio
import json
import os

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from crypto_momentum_lab.execution_account.binance.client import BinanceUsdMTradeClient
from crypto_momentum_lab.live_rollout.exit_receipt_recovery import (
    LiveExitReceiptRecovery,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresDecisionUnitOfWork,
)


async def reconcile(args: argparse.Namespace) -> None:
    engine = create_async_engine(
        os.environ.get("CML_EXECUTION_DATABASE_URL") or os.environ["CML_DATABASE_URL"]
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    client = BinanceUsdMTradeClient(
        api_key=os.environ["BINANCE_TRADE_API_KEY"],
        api_secret=os.environ["BINANCE_TRADE_API_SECRET"],
        environment="live",
        account_label=args.account,
        live_submit_enabled=False,
        shared_request_pacer_path=os.environ.get(
            "CML_BINANCE_SHARED_REQUEST_PACER_PATH"
        )
        or None,
    )
    try:
        uow = AsyncPostgresDecisionUnitOfWork(sessions)
        pending = await uow.load_pending_exits(f"live/{args.account}/{args.strategy}")
        recovery = LiveExitReceiptRecovery(
            sessions, exchange=client, account_label=args.account, run_id=args.session
        )
        for decision_id, command in pending:
            try:
                disposition = await recovery(command)
                applied = False
                if args.apply and disposition.status == "SUPERSEDED":
                    applied = await uow.mark_exit_superseded(
                        decision_id, command.command_id, disposition.reason
                    )
                elif args.apply and disposition.status == "DISPATCHED":
                    applied = await uow.mark_exit_dispatched(
                        decision_id, command.command_id
                    )
                print(
                    json.dumps(
                        {
                            "account": args.account,
                            "decision_id": decision_id,
                            "symbol": command.position_key.symbol,
                            "disposition": disposition.status,
                            "reason": disposition.reason,
                            "applied": applied,
                        }
                    ),
                    flush=True,
                )
            except Exception as error:
                # Exception messages can contain connection credentials. Report
                # their type only; the row retains its unresolved disposition.
                print(
                    json.dumps(
                        {
                            "account": args.account,
                            "decision_id": decision_id,
                            "disposition": "PENDING",
                            "error_type": type(error).__name__,
                        }
                    ),
                    flush=True,
                )
                raise SystemExit(1) from None
    finally:
        await client.aclose()
        await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", required=True)
    parser.add_argument("--session", required=True)
    parser.add_argument("--strategy", default="orderflow_impulse")
    parser.add_argument("--apply", action="store_true")
    asyncio.run(reconcile(parser.parse_args()))
