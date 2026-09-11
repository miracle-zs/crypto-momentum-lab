from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import (
    and_,
    case,
    delete,
    func,
    literal,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderFill,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
    ShadowSuppressionEvent,
)
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.domain.risk import RiskDecision, RiskEvaluation
from crypto_momentum_lab.domain.strategy import OrderIntentCandidate
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderPreSubmissionError,
    PreparedOrderSubmission,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeFillRow,
    ExchangeOrderEventRow,
    ExchangeOrderRow,
    ExecutionCommandRow,
    ExecutionReconciliationEventRow,
    ExitEpisodeReservationRow,
    LiveExposureClaimRow,
    OrderIntentClaimRow,
    OrderIntentExecutionRow,
    RiskHaltRow,
    ShadowSuppressionEventRow,
    TradingLeaseRow,
)
from crypto_momentum_lab.persistence.postgres.serialization import jsonable


@dataclass(frozen=True, slots=True)
class PersistedExchangeOrder:
    plan: OrderExecutionPlan
    state: ExchangeOrderState
    exchange_order_id: str | None
    updated_at: datetime
    executed_quantity: Decimal = Decimal("0")


class _SubmissionAlreadyPrepared(Exception):
    """Abort the transaction when the client order ID already exists."""


def _same_order_identity(
    existing_order: ExchangeOrderRow,
    expected_values: Mapping[str, object],
) -> bool:
    """Keep idempotency scoped to the exact durable order identity."""

    return all(
        getattr(existing_order, field_name) == expected_values[field_name]
        for field_name in (
            "intent_id",
            "run_id",
            "symbol",
            "side",
            "order_type",
            "quantity",
            "price",
            "time_in_force",
            "expires_at",
            "reduce_only",
            "position_side",
            "created_at",
        )
    )


def _same_active_reduce_only_intent(
    existing_order: ExchangeOrderRow,
    expected_values: Mapping[str, object],
) -> bool:
    """Recognize a safe retry of an already-active reduce-only intent.

    Exit plans may be repriced or switch from a resting limit to a market
    fallback while keeping the same durable intent.  The exchange-visible
    order remains the authoritative protection in that case; submitting a
    second order under the same client ID would be both invalid and unsafe.
    """

    terminal_states = {
        state.value for state in ExchangeOrderState if state.terminal
    }
    return (
        bool(expected_values["reduce_only"])
        and existing_order.reduce_only
        and existing_order.intent_id == expected_values["intent_id"]
        and existing_order.run_id == expected_values["run_id"]
        and existing_order.symbol == expected_values["symbol"]
        and existing_order.side == expected_values["side"]
        and existing_order.position_side == expected_values["position_side"]
        and existing_order.state not in terminal_states
    )


class PostgresOrderRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def save_approved_intent(
        self,
        intent: OrderIntentCandidate,
        evaluation: RiskEvaluation,
    ) -> None:
        if evaluation.decision is not RiskDecision.APPROVED:
            raise ValueError("risk evaluation must approve the intent")
        if evaluation.candidate_id != intent.candidate_id:
            raise ValueError("risk evaluation must reference the intent")
        values = {
            "intent_id": intent.candidate_id,
            "candidate_id": intent.candidate_id,
            "run_id": intent.run_id,
            "risk_evaluation_id": evaluation.evaluation_id,
            "strategy_name": intent.strategy_name,
            "symbol": intent.symbol,
            "state": ExchangeOrderState.INTENT_APPROVED.value,
            "approved_at": evaluation.evaluated_at,
            "details": jsonable(asdict(intent)),
        }
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(OrderIntentExecutionRow)
                    .values(values)
                    .on_conflict_do_nothing()
                )

    async def prepare_submission(
        self,
        *,
        intent: OrderIntentCandidate,
        evaluation: RiskEvaluation,
        plan: OrderExecutionPlan,
        prepared_at: datetime,
        environment: str | None = None,
        account_label: str | None = None,
        strategy_name: str | None = None,
        required_lease_owner: str | None = None,
        required_lease_id: str | None = None,
        max_open_positions: int | None = None,
        max_daily_loss: Decimal | None = None,
        max_gross_exposure: Decimal | None = None,
        current_daily_pnl: Decimal | None = None,
        current_gross_exposure: Decimal | None = None,
        open_position_symbols: frozenset[str] | None = None,
        exposure_notional: Decimal | None = None,
    ) -> PreparedOrderSubmission | None:
        """Grant one durable submission; existing orders must be reconciled.

        A client ID is never reusable, including after a terminal outcome or
        process restart. The unique insert arbitrates concurrent exit lanes.
        """

        if evaluation.decision is not RiskDecision.APPROVED:
            raise ValueError("risk evaluation must approve the intent")
        if evaluation.candidate_id != intent.candidate_id:
            raise ValueError("risk evaluation must reference the intent")
        if plan.intent_id != intent.candidate_id:
            raise ValueError("order plan must reference the approved intent")
        if prepared_at.tzinfo is None or prepared_at.utcoffset() is None:
            raise ValueError("prepared_at must be timezone-aware")

        intent_values = {
            "intent_id": intent.candidate_id,
            "candidate_id": intent.candidate_id,
            "run_id": intent.run_id,
            "risk_evaluation_id": evaluation.evaluation_id,
            "strategy_name": intent.strategy_name,
            "symbol": intent.symbol,
            "state": ExchangeOrderState.SUBMITTING.value,
            "approved_at": evaluation.evaluated_at,
            "details": jsonable(asdict(intent)),
        }
        order_values = {
            "client_order_id": plan.client_order_id,
            "intent_id": plan.intent_id,
            "run_id": plan.run_id,
            "exchange_order_id": None,
            "symbol": plan.symbol,
            "side": plan.side,
            "order_type": plan.order_type,
            "quantity": plan.quantity,
            "price": plan.price,
            "time_in_force": plan.time_in_force,
            "expires_at": plan.expires_at,
            "executed_quantity": Decimal("0"),
            "reduce_only": plan.reduce_only,
            "position_side": plan.position_side.value,
            "state": ExchangeOrderState.SUBMITTING.value,
            "created_at": plan.created_at,
            "updated_at": prepared_at,
        }
        submitting_event = ExchangeOrderEvent(
            event_id=_order_event_id(
                plan.client_order_id,
                ExchangeOrderState.SUBMITTING,
                prepared_at,
            ),
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.SUBMITTING,
            occurred_at=prepared_at,
            exchange_order_id=None,
            details={},
        )
        event_values = {
            "event_id": submitting_event.event_id,
            "client_order_id": submitting_event.client_order_id,
            "state": submitting_event.state.value,
            "occurred_at": submitting_event.occurred_at,
            "exchange_order_id": submitting_event.exchange_order_id,
            "details": jsonable(submitting_event.details),
        }
        try:
            async with self._session_factory() as session:
                async with session.begin():
                    fencing_fields = (
                        environment,
                        account_label,
                        strategy_name,
                        required_lease_owner,
                        required_lease_id,
                    )
                    if any(value is not None for value in fencing_fields):
                        if not all(value is not None for value in fencing_fields):
                            raise ValueError(
                                "live submission fencing fields must be "
                                "provided together"
                            )
                        assert environment is not None
                        assert account_label is not None
                        assert strategy_name is not None
                        assert required_lease_owner is not None
                        assert required_lease_id is not None
                        active_lease = await session.scalar(
                            select(TradingLeaseRow)
                            .where(
                                TradingLeaseRow.environment == environment,
                                TradingLeaseRow.account_label == account_label,
                                TradingLeaseRow.state == "active",
                                TradingLeaseRow.expires_at > prepared_at,
                            )
                            .with_for_update()
                        )
                        if (
                            active_lease is None
                            or active_lease.owner != required_lease_owner
                            or active_lease.lease_id != required_lease_id
                            or active_lease.strategy_name != strategy_name
                        ):
                            raise OrderPreSubmissionError(
                                "live lease fencing check failed"
                            )
                        active_halt = await session.scalar(
                            select(RiskHaltRow.halt_id)
                            .where(
                                RiskHaltRow.environment == environment,
                                RiskHaltRow.account_label == account_label,
                                RiskHaltRow.active.is_(True),
                            )
                            .with_for_update(read=True)
                        )
                        if active_halt is not None:
                            raise OrderPreSubmissionError("active risk halt")

                    exposure_fields = (
                        max_open_positions,
                        max_daily_loss,
                        max_gross_exposure,
                        current_daily_pnl,
                        current_gross_exposure,
                        open_position_symbols,
                        exposure_notional,
                    )
                    if any(value is not None for value in exposure_fields):
                        if plan.reduce_only:
                            raise ValueError(
                                "exposure claim fields are only valid for entries"
                            )
                        if not all(
                            value is not None
                            for value in (
                                environment,
                                account_label,
                                strategy_name,
                                current_daily_pnl,
                                current_gross_exposure,
                                open_position_symbols,
                                exposure_notional,
                            )
                        ):
                            raise ValueError(
                                "live exposure claim baseline must be provided "
                                "together"
                            )
                        assert environment is not None
                        assert account_label is not None
                        assert strategy_name is not None
                        assert current_daily_pnl is not None
                        assert current_gross_exposure is not None
                        assert open_position_symbols is not None
                        assert exposure_notional is not None
                        if exposure_notional <= 0:
                            raise ValueError("exposure_notional must be positive")
                        await session.execute(
                            text(
                                "SELECT pg_advisory_xact_lock("
                                "hashtext(:lock_key))"
                            ),
                            {
                                "lock_key": (
                                    "live-exposure:"
                                    f"{environment}:{account_label}:{strategy_name}"
                                )
                            },
                        )
                        if (
                            max_daily_loss is not None
                            and current_daily_pnl <= -max_daily_loss
                        ):
                            raise OrderPreSubmissionError(
                                "max daily loss reached"
                            )
                        active_claim_sum = await session.scalar(
                            select(
                                func.coalesce(
                                    func.sum(LiveExposureClaimRow.notional),
                                    Decimal("0"),
                                )
                            ).where(
                                LiveExposureClaimRow.environment == environment,
                                LiveExposureClaimRow.account_label
                                == account_label,
                                LiveExposureClaimRow.strategy_name
                                == strategy_name,
                                LiveExposureClaimRow.active.is_(True),
                                LiveExposureClaimRow.intent_id
                                != intent.candidate_id,
                            )
                        )
                        active_claim_symbols = set(
                            (
                                await session.scalars(
                                    select(LiveExposureClaimRow.symbol)
                                    .where(
                                        LiveExposureClaimRow.environment
                                        == environment,
                                        LiveExposureClaimRow.account_label
                                        == account_label,
                                        LiveExposureClaimRow.strategy_name
                                        == strategy_name,
                                        LiveExposureClaimRow.active.is_(True),
                                        LiveExposureClaimRow.intent_id
                                        != intent.candidate_id,
                                    )
                                    .distinct()
                                )
                            ).all()
                        )
                        if (
                            max_open_positions is not None
                            and len(
                                set(open_position_symbols)
                                | active_claim_symbols
                                | {plan.symbol}
                            )
                            > max_open_positions
                        ):
                            raise OrderPreSubmissionError(
                                "max open positions reached"
                            )
                        if (
                            max_gross_exposure is not None
                            and current_gross_exposure
                            + (active_claim_sum or Decimal("0"))
                            + exposure_notional
                            > max_gross_exposure
                        ):
                            raise OrderPreSubmissionError(
                                "max gross exposure reached"
                            )
                    await session.execute(
                        insert(OrderIntentExecutionRow)
                        .values(intent_values)
                        .on_conflict_do_nothing()
                    )
                    episode_key = _exit_episode_key(intent)
                    if (
                        plan.reduce_only
                        and episode_key is not None
                        and environment is not None
                        and account_label is not None
                        and strategy_name is not None
                    ):
                        await session.execute(
                            insert(ExitEpisodeReservationRow)
                            .values(
                                environment=environment,
                                account_label=account_label,
                                strategy_name=strategy_name,
                                symbol=plan.symbol,
                                position_side=plan.position_side.value,
                                episode_key=episode_key,
                                intent_id=plan.intent_id,
                                client_order_id=plan.client_order_id,
                                active=True,
                                state=ExchangeOrderState.SUBMITTING.value,
                                created_at=prepared_at,
                                updated_at=prepared_at,
                            )
                            .on_conflict_do_nothing()
                        )
                        reservation = await session.scalar(
                            select(ExitEpisodeReservationRow)
                            .where(
                                ExitEpisodeReservationRow.environment
                                == environment,
                                ExitEpisodeReservationRow.account_label
                                == account_label,
                                ExitEpisodeReservationRow.strategy_name
                                == strategy_name,
                                ExitEpisodeReservationRow.symbol == plan.symbol,
                                ExitEpisodeReservationRow.position_side
                                == plan.position_side.value,
                                ExitEpisodeReservationRow.episode_key
                                == episode_key,
                            )
                            .with_for_update()
                        )
                        if reservation is None:
                            raise RuntimeError(
                                "exit episode reservation disappeared"
                            )
                        if reservation.active and (
                            reservation.intent_id != plan.intent_id
                            or reservation.client_order_id != plan.client_order_id
                        ):
                            raise _SubmissionAlreadyPrepared
                        if not reservation.active:
                            await session.execute(
                                update(ExitEpisodeReservationRow)
                                .where(
                                    ExitEpisodeReservationRow.environment
                                    == environment,
                                    ExitEpisodeReservationRow.account_label
                                    == account_label,
                                    ExitEpisodeReservationRow.strategy_name
                                    == strategy_name,
                                    ExitEpisodeReservationRow.symbol == plan.symbol,
                                    ExitEpisodeReservationRow.position_side
                                    == plan.position_side.value,
                                    ExitEpisodeReservationRow.episode_key
                                    == episode_key,
                                )
                                .values(
                                    intent_id=plan.intent_id,
                                    client_order_id=plan.client_order_id,
                                    active=True,
                                    state=ExchangeOrderState.SUBMITTING.value,
                                    updated_at=prepared_at,
                                )
                            )
                    if (
                        not plan.reduce_only
                        and any(value is not None for value in exposure_fields)
                    ):
                        assert environment is not None
                        assert account_label is not None
                        assert strategy_name is not None
                        assert exposure_notional is not None
                        await session.execute(
                            insert(LiveExposureClaimRow)
                            .values(
                                intent_id=plan.intent_id,
                                environment=environment,
                                account_label=account_label,
                                strategy_name=strategy_name,
                                symbol=plan.symbol,
                                position_side=plan.position_side.value,
                                notional=exposure_notional,
                                active=True,
                                created_at=prepared_at,
                                updated_at=prepared_at,
                            )
                            .on_conflict_do_nothing()
                        )
                    inserted_order = await session.scalar(
                        insert(ExchangeOrderRow)
                        .values(order_values)
                        .on_conflict_do_nothing()
                        .returning(ExchangeOrderRow.client_order_id)
                    )
                    if inserted_order is None:
                        existing_order = await session.scalar(
                            select(ExchangeOrderRow).where(
                                ExchangeOrderRow.client_order_id
                                == plan.client_order_id
                            )
                        )
                        if existing_order is None:
                            raise RuntimeError(
                                "client order ID conflict could not be reconciled"
                            )
                        if not _same_order_identity(
                            existing_order,
                            order_values,
                        ):
                            if _same_active_reduce_only_intent(
                                existing_order,
                                order_values,
                            ):
                                # Repricing or switching the fallback order
                                # type must not duplicate an active protective
                                # order. Keep the durable exchange order and
                                # let the caller retry after it reaches a
                                # terminal state.
                                raise _SubmissionAlreadyPrepared
                            raise ValueError(
                                "client order ID is already bound to a "
                                "different order"
                            )
                        # A restarted or concurrent worker already owns the
                        # same order. Roll back any new intent atomically.
                        raise _SubmissionAlreadyPrepared
                    await session.execute(
                        insert(ExchangeOrderEventRow)
                        .values(event_values)
                        .on_conflict_do_nothing()
                    )
                    await session.execute(
                        update(OrderIntentExecutionRow)
                        .where(
                            OrderIntentExecutionRow.intent_id == plan.intent_id
                        )
                        .values(state=ExchangeOrderState.SUBMITTING.value)
                    )
        except _SubmissionAlreadyPrepared:
            return None
        return PreparedOrderSubmission(
            plan=plan,
            submitting_event=submitting_event,
        )

    async def claim_intent(
        self,
        intent_id: str,
        worker_id: str,
        claimed_at: datetime,
        expires_at: datetime,
    ) -> bool:
        if expires_at <= claimed_at:
            raise ValueError("claim expiration must be after claim time")
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    delete(OrderIntentClaimRow).where(
                        OrderIntentClaimRow.intent_id == intent_id,
                        OrderIntentClaimRow.expires_at <= claimed_at,
                    )
                )
                claimed = await session.scalar(
                    insert(OrderIntentClaimRow)
                    .values(
                        intent_id=intent_id,
                        worker_id=worker_id,
                        claimed_at=claimed_at,
                        expires_at=expires_at,
                    )
                    .on_conflict_do_nothing()
                    .returning(OrderIntentClaimRow.intent_id)
                )
                if claimed is not None:
                    await session.execute(
                        update(OrderIntentExecutionRow)
                        .where(OrderIntentExecutionRow.intent_id == intent_id)
                        .values(state=ExchangeOrderState.CLAIMED.value)
                    )
        return claimed is not None

    async def save_planned_order(self, plan: OrderExecutionPlan) -> None:
        values = {
            "client_order_id": plan.client_order_id,
            "intent_id": plan.intent_id,
            "run_id": plan.run_id,
            "exchange_order_id": None,
            "symbol": plan.symbol,
            "side": plan.side,
            "order_type": plan.order_type,
            "quantity": plan.quantity,
            "price": plan.price,
            "time_in_force": plan.time_in_force,
            "expires_at": plan.expires_at,
            "executed_quantity": Decimal("0"),
            "reduce_only": plan.reduce_only,
            "position_side": plan.position_side.value,
            "state": ExchangeOrderState.PLANNED.value,
            "created_at": plan.created_at,
            "updated_at": plan.created_at,
        }
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(ExchangeOrderRow)
                    .values(values)
                    .on_conflict_do_nothing()
                )
                await session.execute(
                    update(OrderIntentExecutionRow)
                    .where(OrderIntentExecutionRow.intent_id == plan.intent_id)
                    .values(state=ExchangeOrderState.PLANNED.value)
                )

    async def adopt_external_order_for_cancellation(
        self,
        plan: OrderExecutionPlan,
        *,
        exchange_order_id: str,
        observed_at: datetime,
    ) -> None:
        """Journal an exchange-visible order before coordinator cancellation.

        A rolling worker can discover an entry on Binance before the local
        worker that created it has committed its order row.  The synthetic
        intent gives the normal state machine a valid foreign-key target, so
        the cancel still shares the same coordinator and durable event path.
        """

        if not exchange_order_id.strip():
            raise ValueError("exchange_order_id must not be empty")
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        synthetic_intent_id = f"orphan-cancel:{plan.client_order_id}"
        intent_values = {
            "intent_id": synthetic_intent_id,
            "candidate_id": synthetic_intent_id,
            "run_id": plan.run_id,
            "risk_evaluation_id": synthetic_intent_id,
            "strategy_name": "orphan-cancel",
            "symbol": plan.symbol,
            "state": ExchangeOrderState.SUBMITTED.value,
            "approved_at": observed_at,
            "details": {
                "reason": "exchange_visible_order_missing_from_local_journal",
                "client_order_id": plan.client_order_id,
            },
        }
        order_values = {
            "client_order_id": plan.client_order_id,
            "intent_id": synthetic_intent_id,
            "run_id": plan.run_id,
            "exchange_order_id": exchange_order_id,
            "symbol": plan.symbol,
            "side": plan.side,
            "order_type": plan.order_type,
            "quantity": plan.quantity,
            "price": plan.price,
            "time_in_force": plan.time_in_force,
            "expires_at": plan.expires_at,
            "executed_quantity": Decimal("0"),
            "reduce_only": False,
            "position_side": plan.position_side.value,
            "state": ExchangeOrderState.SUBMITTED.value,
            "created_at": plan.created_at,
            "updated_at": observed_at,
        }
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(OrderIntentExecutionRow)
                    .values(intent_values)
                    .on_conflict_do_nothing()
                )
                inserted = await session.scalar(
                    insert(ExchangeOrderRow)
                    .values(order_values)
                    .on_conflict_do_nothing()
                    .returning(ExchangeOrderRow.client_order_id)
                )
                if inserted is None:
                    existing_order = await session.scalar(
                        select(ExchangeOrderRow).where(
                            ExchangeOrderRow.client_order_id
                            == plan.client_order_id
                        )
                    )
                    if existing_order is None or not _same_order_identity(
                        existing_order,
                        order_values,
                    ):
                        raise ValueError(
                            "client order ID is already bound to a "
                            "different order"
                        )

    async def append_order_event(self, event: ExchangeOrderEvent) -> bool:
        values = {
            "event_id": event.event_id,
            "client_order_id": event.client_order_id,
            "state": event.state.value,
            "occurred_at": event.occurred_at,
            "exchange_order_id": event.exchange_order_id,
            "details": jsonable(event.details),
        }
        async with self._session_factory() as session:
            async with session.begin():
                inserted = await session.scalar(
                    insert(ExchangeOrderEventRow)
                    .values(values)
                    .on_conflict_do_nothing()
                    .returning(ExchangeOrderEventRow.event_id)
                )
                if inserted is not None:
                    terminal_states = tuple(
                        state.value
                        for state in ExchangeOrderState
                        if state.terminal
                    )
                    terminal_transition = and_(
                        literal(event.state.terminal),
                        # FILLED is the strongest terminal observation; a
                        # later cancel/reject event must not erase it.
                        or_(
                            ExchangeOrderRow.state
                            != ExchangeOrderState.FILLED.value,
                            literal(event.state is ExchangeOrderState.FILLED),
                        ),
                    )
                    advance_state = and_(
                        ExchangeOrderRow.updated_at <= event.occurred_at,
                        or_(
                            ExchangeOrderRow.state.not_in(terminal_states),
                            terminal_transition,
                        ),
                    )
                    order_values: dict[str, object] = {
                        "state": case(
                            (advance_state, event.state.value),
                            else_=ExchangeOrderRow.state,
                        ),
                        "updated_at": case(
                            (
                                advance_state,
                                func.greatest(
                                    ExchangeOrderRow.updated_at,
                                    event.occurred_at,
                                ),
                            ),
                            else_=ExchangeOrderRow.updated_at,
                        ),
                    }
                    if event.exchange_order_id is not None:
                        order_values["exchange_order_id"] = event.exchange_order_id
                    executed_quantity = _event_executed_quantity(event)
                    if executed_quantity is not None:
                        # Exchange snapshots are cumulative, but callbacks can
                        # arrive out of order after a reconnect. Never let an
                        # older snapshot lower the reservation baseline.
                        order_values["executed_quantity"] = func.greatest(
                            ExchangeOrderRow.executed_quantity,
                            executed_quantity,
                        )
                    order_intent_id = await session.scalar(
                        select(ExchangeOrderRow.intent_id).where(
                            ExchangeOrderRow.client_order_id
                            == event.client_order_id
                        )
                    )
                    order_update = await session.execute(
                        update(ExchangeOrderRow)
                        .where(
                            ExchangeOrderRow.client_order_id
                            == event.client_order_id,
                            # Preserve the immutable exchange identity. Keep
                            # conflicting legacy events in the event journal,
                            # but never merge another order into this row.
                            or_(
                                literal(event.exchange_order_id is None),
                                ExchangeOrderRow.exchange_order_id.is_(None),
                                ExchangeOrderRow.exchange_order_id
                                == event.exchange_order_id,
                            ),
                        )
                        .values(order_values)
                    )
                    current_order_state = await session.scalar(
                        select(ExchangeOrderRow.state).where(
                            ExchangeOrderRow.client_order_id
                            == event.client_order_id
                        )
                    )
                    if (
                        order_update.rowcount
                        and order_intent_id is not None
                        and current_order_state is not None
                    ):
                        if current_order_state == event.state.value:
                            await session.execute(
                                update(ExitEpisodeReservationRow)
                                .where(
                                    ExitEpisodeReservationRow.intent_id
                                    == order_intent_id
                                )
                                .values(
                                    state=event.state.value,
                                    updated_at=event.occurred_at,
                                )
                            )
                        if current_order_state in terminal_states:
                            await session.execute(
                                update(ExitEpisodeReservationRow)
                                .where(
                                    ExitEpisodeReservationRow.intent_id
                                    == order_intent_id,
                                    ExitEpisodeReservationRow.active.is_(True),
                                )
                                .values(
                                    active=False,
                                    updated_at=event.occurred_at,
                                )
                            )
                            await session.execute(
                                update(LiveExposureClaimRow)
                                .where(
                                    LiveExposureClaimRow.intent_id
                                    == order_intent_id,
                                    LiveExposureClaimRow.active.is_(True),
                                )
                                .values(
                                    active=False,
                                    updated_at=event.occurred_at,
                                )
                            )
        return inserted is not None

    async def save_fill(self, fill: ExchangeOrderFill) -> bool:
        async with self._session_factory() as session:
            async with session.begin():
                inserted = await session.scalar(
                    insert(ExchangeFillRow)
                    .values(
                        fill_id=fill.fill_id,
                        client_order_id=fill.client_order_id,
                        exchange_trade_id=fill.exchange_trade_id,
                        price=fill.price,
                        quantity=fill.quantity,
                        fee=fill.fee,
                        fee_asset=fill.fee_asset,
                        filled_at=fill.filled_at,
                        details=jsonable(fill.details),
                    )
                    .on_conflict_do_nothing()
                    .returning(ExchangeFillRow.fill_id)
                )
        return inserted is not None

    async def save_shadow_suppression(
        self,
        event: ShadowSuppressionEvent,
    ) -> None:
        await self._insert_immutable(
            ShadowSuppressionEventRow,
            {
                "order_plan_id": event.order_plan_id,
                "client_order_id": event.client_order_id,
                "suppressed_at": event.suppressed_at,
                "reason": event.reason,
                "order_payload": jsonable(event.order_payload),
            },
        )

    async def load_unresolved_orders(
        self,
        run_id: str | None = None,
    ) -> tuple[PersistedExchangeOrder, ...]:
        terminal_states = tuple(
            state.value for state in ExchangeOrderState if state.terminal
        )
        async with self._session_factory() as session:
            query = select(ExchangeOrderRow).where(
                ExchangeOrderRow.state.not_in(terminal_states)
            )
            if run_id is not None:
                query = query.where(ExchangeOrderRow.run_id == run_id)
            rows = (
                await session.scalars(
                    query
                    .order_by(
                        ExchangeOrderRow.updated_at,
                        ExchangeOrderRow.client_order_id,
                    )
                )
            ).all()
        return tuple(_persisted_order(row) for row in rows)

    async def load_order(
        self,
        client_order_id: str,
    ) -> PersistedExchangeOrder | None:
        async with self._session_factory() as session:
            row = await session.scalar(
                select(ExchangeOrderRow).where(
                    ExchangeOrderRow.client_order_id == client_order_id
                )
            )
        return None if row is None else _persisted_order(row)

    async def save_execution_command(
        self,
        *,
        command_id: str,
        client_order_id: str | None,
        command: str,
        status: str,
        requested_at: datetime,
        details: dict[str, JsonValue],
    ) -> None:
        await self._insert_immutable(
            ExecutionCommandRow,
            {
                "command_id": command_id,
                "client_order_id": client_order_id,
                "command": command,
                "status": status,
                "requested_at": requested_at,
                "details": jsonable(details),
            },
        )

    async def save_reconciliation_event(
        self,
        *,
        reconciliation_event_id: str,
        client_order_id: str,
        outcome: str,
        occurred_at: datetime,
        details: dict[str, JsonValue],
    ) -> None:
        await self._insert_immutable(
            ExecutionReconciliationEventRow,
            {
                "reconciliation_event_id": reconciliation_event_id,
                "client_order_id": client_order_id,
                "outcome": outcome,
                "occurred_at": occurred_at,
                "details": jsonable(details),
            },
        )

    async def _insert_immutable(
        self,
        model: Any,
        values: dict[str, object],
    ) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(model).values(values).on_conflict_do_nothing()
                )


def _persisted_order(row: ExchangeOrderRow) -> PersistedExchangeOrder:
    return PersistedExchangeOrder(
        plan=OrderExecutionPlan(
            intent_id=row.intent_id,
            run_id=row.run_id,
            client_order_id=row.client_order_id,
            symbol=row.symbol,
            side=row.side,
            order_type=row.order_type,
            quantity=row.quantity,
            price=row.price,
            reduce_only=row.reduce_only,
            created_at=row.created_at,
            position_side=FuturesPositionSide(row.position_side),
            quantized=True,
            time_in_force=row.time_in_force,
            expires_at=row.expires_at,
        ),
        state=ExchangeOrderState(row.state),
        exchange_order_id=row.exchange_order_id,
        updated_at=row.updated_at,
        executed_quantity=row.executed_quantity or Decimal("0"),
    )


def _event_executed_quantity(event: ExchangeOrderEvent) -> Decimal | None:
    value = event.details.get("executed_quantity")
    if value is None:
        return None
    try:
        quantity = Decimal(str(value))
    except ArithmeticError:
        return None
    return quantity if quantity >= 0 else None


def _exit_episode_key(intent: OrderIntentCandidate) -> str | None:
    if not intent.reduce_only:
        return None
    opened_at = intent.features.get("opened_at")
    if not isinstance(opened_at, str) or not opened_at.strip():
        return None
    batch_id = intent.features.get("batch_id")
    if batch_id is None:
        return opened_at
    if not isinstance(batch_id, str) or not batch_id.strip():
        return None
    return f"{opened_at}:batch:{batch_id}"


def _order_event_id(
    client_order_id: str,
    state: ExchangeOrderState,
    occurred_at: datetime,
) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            f"order-event:{client_order_id}:{state.value}:{occurred_at.isoformat()}",
        )
    )
