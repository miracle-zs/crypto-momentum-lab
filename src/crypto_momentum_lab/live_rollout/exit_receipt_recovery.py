"""Read-only exchange receipt recovery for obsolete flat-position exits.

No submit/cancel methods are exposed here. Unknown exchange outcomes and
unsettled reservations retain PENDING; a flat Book alone proves no receipt.
"""

import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.account.models import (
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.decision.ports import ExitRecoveryDisposition
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderSnapshot,
    ExchangeOrderState,
)
from crypto_momentum_lab.domain.execution.trade_command import TradeCommand
from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillEventRow,
    ExchangeOrderRow,
    PositionReservationRow,
)


class ExitReceiptExchange(Protocol):
    async def fetch_positions(
        self, *, include_flat: bool = False
    ) -> tuple[AccountPositionSnapshot, ...]: ...
    async def fetch_open_orders(self) -> tuple[AccountOpenOrderSnapshot, ...]: ...
    async def query_order_by_client_id(
        self, symbol: str, client_order_id: str
    ) -> ExchangeOrderSnapshot | None: ...


class LiveExitReceiptRecovery:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        exchange: ExitReceiptExchange,
        account_label: str,
        run_id: str,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._sessions = sessions
        self._exchange = exchange
        self._account = account_label
        self._run_id = run_id
        self._clock = clock
        self._retry_not_before: datetime | None = None
        self._account_cut: datetime | None = None
        self._positions: tuple[AccountPositionSnapshot, ...] = ()
        self._open_orders: tuple[AccountOpenOrderSnapshot, ...] = ()

    async def __call__(self, command: TradeCommand) -> ExitRecoveryDisposition:
        key = command.position_key

        def pending(reason: str) -> ExitRecoveryDisposition:
            return ExitRecoveryDisposition("PENDING", reason)

        if (
            key.environment != "live"
            or key.account_label != self._account
            or not command.reduce_only
        ):
            return pending("exit_scope_unconfirmed")
        now = self._clock()
        if self._retry_not_before is not None and now < self._retry_not_before:
            return pending("exchange_receipt_retry_backoff")
        # Very old/clock-skewed commands cannot use exchange order absence as
        # a reliable receipt. Recent incident commands remain inside this bound.
        if not timedelta(0) <= now - command.created_at <= timedelta(days=2):
            return pending("exchange_receipt_history_window_unconfirmed")
        expected_id = command.client_order_id(self._run_id)
        async with self._sessions() as session:
            row = await session.scalar(
                select(ExchangeOrderRow).where(
                    ExchangeOrderRow.intent_id == "intent_exit_" + command.command_id
                )
            )
            active = await session.scalar(
                select(func.count())
                .select_from(PositionReservationRow)
                .where(
                    PositionReservationRow.environment == key.environment,
                    PositionReservationRow.account_label == key.account_label,
                    PositionReservationRow.symbol == key.symbol,
                    PositionReservationRow.position_side == key.position_side.value,
                    PositionReservationRow.status == "ACTIVE",
                )
            )
            if active:
                return pending("active_position_reservation_requires_recovery")
            if row is not None and (
                row.run_id != self._run_id
                or row.symbol != key.symbol
                or (
                    command.idempotency_key is not None
                    and row.client_order_id != command.idempotency_key
                )
                or not row.reduce_only
            ):
                return pending("durable_order_identity_mismatch")
            if row is not None:
                # An already submitted order owns its durable identity. Never
                # recalculate a legacy receipt under the new submission rule.
                expected_id = row.client_order_id
        # Pending exits share one short-lived account cut. Re-fetching all
        # positions/orders per command can exhaust the private API budget.
        if (
            self._account_cut is None
            or not 0 <= (now - self._account_cut).total_seconds() <= 5
        ):
            try:
                positions = await self._exchange.fetch_positions(include_flat=True)
                open_orders = await self._exchange.fetch_open_orders()
            except Exception as error:
                self._defer_retry(error)
                raise
            self._account_cut = self._clock()
            self._positions, self._open_orders = positions, open_orders
        checked_at = max(now, self._account_cut)
        positions = self._positions
        matching = [
            position
            for position in positions
            if position.environment == key.environment
            and position.account_label == key.account_label
            and position.symbol == key.symbol
            and position.position_side == key.position_side.value
        ]
        if (
            len(matching) != 1
            or matching[0].position_amt != 0
            or not 0 <= (checked_at - matching[0].observed_at).total_seconds() <= 180
        ):
            return pending("explicit_fresh_exchange_flat_cut_required")
        if any(order.symbol == key.symbol for order in self._open_orders):
            return pending("exchange_open_order_requires_recovery")
        # Legacy internal command names were persisted as exchange identities.
        # An invalid ID cannot produce an exchange receipt. A recorded rejection
        # with no exchange ID, plus the fresh flat/no-order cut above, is terminal.
        if re.fullmatch(r"[.A-Z:/a-z0-9_-]{1,36}", expected_id) is None:
            if (
                row is not None
                and row.state == ExchangeOrderState.REJECTED.value
                and row.exchange_order_id is None
            ):
                return ExitRecoveryDisposition(
                    "SUPERSEDED", "rejected_invalid_identity_explicit_position_flat"
                )
            return pending("invalid_legacy_order_identity_requires_recovery")
        try:
            order = await self._exchange.query_order_by_client_id(
                key.symbol, expected_id
            )
        except Exception as error:
            self._defer_retry(error)
            raise
        if order is None:
            if row is not None:
                return pending("durable_order_exists_but_exchange_receipt_unknown")
            return ExitRecoveryDisposition(
                "SUPERSEDED", "exchange_absence_verified_explicit_position_flat"
            )
        if (
            order.client_order_id != expected_id
            or order.state != ExchangeOrderState.FILLED
            or order.executed_quantity != command.requested_quantity
        ):
            return pending("exchange_order_requires_reconciliation")
        if row is not None and row.exchange_order_id not in (
            None,
            order.exchange_order_id,
        ):
            return pending("exchange_receipt_identity_mismatch")
        async with self._sessions() as session:
            quantity = await session.scalar(
                select(
                    func.coalesce(func.sum(AccountFillEventRow.quantity), Decimal("0"))
                ).where(
                    AccountFillEventRow.environment == key.environment,
                    AccountFillEventRow.account_label == key.account_label,
                    AccountFillEventRow.symbol == key.symbol,
                    AccountFillEventRow.order_id == order.exchange_order_id,
                    AccountFillEventRow.side
                    == ("SELL" if command.side.value == "long" else "BUY"),
                )
            )
        if quantity != order.executed_quantity:
            return pending("complete_account_exit_trade_facts_required")
        return ExitRecoveryDisposition(
            "DISPATCHED", "exchange_filled_receipt_verified_" + order.exchange_order_id
        )

    def _defer_retry(self, error: Exception) -> None:
        delay = getattr(error, "retry_after_seconds", None)
        seconds = max(5.0, float(delay)) if isinstance(delay, (int, float)) else 5.0
        self._retry_not_before = self._clock() + timedelta(seconds=seconds)
