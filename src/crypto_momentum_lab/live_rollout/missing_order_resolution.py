"""Fail-closed operator resolution for orders missing on the exchange."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderState,
)
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.execution_account.binance import BinanceUsdMTradeClient
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PostgresOrderRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_execution_database_engine,
)


def validate_missing_order_resolution(
    *,
    state: str,
    reduce_only: bool,
    exchange_order_id: str | None,
    created_at: datetime,
    now: datetime,
    order_quantity: Decimal,
    executed_quantity: Decimal,
    position_quantity: Decimal,
    exchange_order_found: bool,
    matching_open_order_found: bool,
    min_missing_age_seconds: float,
) -> None:
    """Fail closed before an operator resolves an unknown order as absent."""
    if state != ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION.value:
        raise RuntimeError(
            "order is not pending reconciliation; no manual resolution is allowed"
        )
    if not reduce_only:
        raise RuntimeError("only reduce-only orders may be manually resolved")
    if exchange_order_id is not None:
        raise RuntimeError("exchange order id is already recorded")
    if executed_quantity != 0:
        raise RuntimeError("executed quantity is non-zero")
    age_seconds = (now - created_at).total_seconds()
    if age_seconds < min_missing_age_seconds:
        raise RuntimeError(
            f"order is younger than {min_missing_age_seconds:g} seconds"
        )
    if exchange_order_found:
        raise RuntimeError("exchange order still exists")
    if matching_open_order_found:
        raise RuntimeError("matching open order still exists")
    if position_quantity != order_quantity:
        raise RuntimeError("position quantity changed; manual resolution is unsafe")


async def resolve_missing_live_order(
    *,
    database_url: str,
    account_label: str,
    client_order_id: str,
    operator: str,
    min_missing_age_seconds: int,
    base_url: str,
    api_key: str,
    api_secret: str,
) -> dict[str, object]:
    """Resolve one stale unknown order after read-only exchange verification.

    This command intentionally never calls a write endpoint on Binance. It
    records the evidence and then appends a terminal local
    ``ABSENT_RECONCILED`` event only when the exchange has no order, no
    matching open order, and the protected position is unchanged.
    """
    now = datetime.now(tz=UTC)
    engine = create_execution_database_engine(database_url)
    client: BinanceUsdMTradeClient | None = None
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        order_repository = PostgresOrderRepository(factory)
        order = await order_repository.load_order(client_order_id)
        if order is None:
            raise RuntimeError(f"order {client_order_id} does not exist")

        client = BinanceUsdMTradeClient(
            api_key=api_key,
            api_secret=api_secret,
            environment="live",
            account_label=account_label,
            live_submit_enabled=False,
            base_url=base_url,
            request_interval_seconds=0.0,
        )
        exchange_snapshot = await client.query_order_by_client_id(
            order.plan.symbol,
            client_order_id,
        )
        positions = await client.fetch_positions()
        open_orders = await client.fetch_open_orders()
        matching_position = next(
            (
                position
                for position in positions
                if position.symbol == order.plan.symbol
                and position.position_side == order.plan.position_side.value
            ),
            None,
        )
        position_quantity = (
            abs(matching_position.position_amt)
            if matching_position is not None
            else Decimal("0")
        )
        matching_open_order = next(
            (
                open_order
                for open_order in open_orders
                if open_order.client_order_id == client_order_id
                and open_order.symbol == order.plan.symbol
            ),
            None,
        )
        validate_missing_order_resolution(
            state=order.state.value,
            reduce_only=order.plan.reduce_only,
            exchange_order_id=order.exchange_order_id,
            created_at=order.plan.created_at,
            now=now,
            order_quantity=order.plan.quantity,
            executed_quantity=order.executed_quantity,
            position_quantity=position_quantity,
            exchange_order_found=exchange_snapshot is not None,
            matching_open_order_found=matching_open_order is not None,
            min_missing_age_seconds=float(min_missing_age_seconds),
        )

        # Re-read immediately before the terminal event so a concurrent
        # reconciler cannot be overwritten by this operator action.
        current = await order_repository.load_order(client_order_id)
        if current is None:
            raise RuntimeError(
                f"order {client_order_id} disappeared during verification"
            )
        if current.state is not ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
            raise RuntimeError(
                "order state changed during verification; retry with a fresh snapshot"
            )

        verification_id = str(
            uuid5(
                NAMESPACE_URL,
                f"live-missing-order:{client_order_id}:{order.plan.created_at.isoformat()}",
            )
        )
        evidence: dict[str, JsonValue] = {
            "action": "operator_confirmed_absent",
            "account_label": account_label,
            "client_order_id": client_order_id,
            "exchange_order_found": False,
            "matching_open_order_found": False,
            "position_quantity": str(position_quantity),
            "plan_quantity": str(order.plan.quantity),
            "executed_quantity": str(order.executed_quantity),
            "verified_at": now.isoformat(),
            "operator": operator.strip(),
            "verification_id": verification_id,
        }
        await order_repository.save_execution_command(
            command_id=f"operator-resolve-{verification_id}",
            client_order_id=client_order_id,
            command="resolve_unknown_order",
            status="completed",
            requested_at=now,
            details=evidence,
        )
        await order_repository.save_reconciliation_event(
            reconciliation_event_id=f"operator-reconcile-{verification_id}",
            client_order_id=client_order_id,
            outcome="operator_confirmed_absent",
            occurred_at=now,
            details=evidence,
        )
        inserted = await order_repository.append_order_event(
            ExchangeOrderEvent(
                event_id=f"operator-absent-{verification_id}",
                client_order_id=client_order_id,
                state=ExchangeOrderState.ABSENT_RECONCILED,
                occurred_at=now,
                exchange_order_id=None,
                details=evidence,
            )
        )
        if not inserted:
            raise RuntimeError(
                "operator resolution event was already present or could not be recorded"
            )
        return {
            "resolved": True,
            "client_order_id": client_order_id,
            "symbol": order.plan.symbol,
            "state": ExchangeOrderState.ABSENT_RECONCILED.value,
            "reason": "operator_confirmed_absent",
            "verified_at": now.isoformat(),
            "position_quantity": str(position_quantity),
        }
    finally:
        if client is not None:
            await client.aclose()
        await engine.dispose()


__all__ = [
    "resolve_missing_live_order",
    "validate_missing_order_resolution",
]
