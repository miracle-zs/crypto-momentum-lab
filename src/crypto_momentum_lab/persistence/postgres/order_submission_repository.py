"""Atomic approved-intent claims and fenced order submission preparation."""

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import (
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderAlreadyPreparedError,
    OrderPreSubmissionError,
    PreparedOrderSubmission,
)
from crypto_momentum_lab.domain.risk import RiskDecision, RiskEvaluation
from crypto_momentum_lab.domain.strategy import OrderIntentCandidate
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeOrderEventRow,
    ExchangeOrderRow,
    ExitEpisodeReservationRow,
    LiveExposureClaimRow,
    OrderIntentExecutionRow,
)
from crypto_momentum_lab.persistence.postgres.serialization import jsonable
from crypto_momentum_lab.persistence.postgres.submission_identity import (
    _same_order_identity,
)


def _serialize_candidate_intent(intent: OrderIntentCandidate) -> dict[str, object]:
    return {
        "candidate_id": intent.candidate_id,
        "signal_id": intent.signal_id,
        "run_id": intent.run_id,
        "strategy_name": intent.strategy_name,
        "strategy_version": intent.strategy_version,
        "config_hash": intent.config_hash,
        "symbol": intent.symbol,
        "side": intent.side.value
        if hasattr(intent.side, "value")
        else str(intent.side),
        "entry_type": (
            intent.entry_type.value
            if hasattr(intent.entry_type, "value")
            else str(intent.entry_type)
        ),
        "limit_price": jsonable(intent.limit_price),
        "desired_notional": jsonable(intent.desired_notional),
        "reduce_only": intent.reduce_only,
        "expires_at": jsonable(intent.expires_at),
        "created_at": jsonable(intent.created_at),
        "reason": intent.reason,
        "features": jsonable(intent.features),
    }


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

    terminal_states = {state.value for state in ExchangeOrderState if state.terminal}
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


class PostgresOrderSubmissionRepository:
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
            "details": _serialize_candidate_intent(intent),
        }
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(OrderIntentExecutionRow)
                    .values(values)
                    .on_conflict_do_nothing()
                )

    async def prepare_submission_in_session(
        self,
        session: AsyncSession,
        *,
        intent: OrderIntentCandidate,
        evaluation: RiskEvaluation,
        plan: OrderExecutionPlan,
        prepared_at: datetime,
        environment: str | None = None,
        account_label: str | None = None,
        strategy_name: str | None = None,
        max_open_positions: int | None = None,
        max_daily_loss: Decimal | None = None,
        max_gross_exposure: Decimal | None = None,
        current_daily_pnl: Decimal | None = None,
        current_gross_exposure: Decimal | None = None,
        open_position_symbols: frozenset[str] | None = None,
        exposure_notional: Decimal | None = None,
        baseline_observed_at: datetime | None = None,
    ) -> PreparedOrderSubmission | None:
        """Grant one durable submission within an active session."""
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
            "details": _serialize_candidate_intent(intent),
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
            details={
                "execution_plan": {
                    "batch_id": plan.batch_id,
                    "allocations": [
                        {
                            "batch_id": item.batch_id,
                            "allocated_quantity": str(item.allocated_quantity),
                            "entry_price": str(item.entry_price),
                        }
                        for item in plan.allocations
                    ],
                    "batch_quantities": jsonable(plan.batch_quantities),
                    "strategy_name": plan.strategy_name,
                    "strategy_version": plan.strategy_version,
                    "reference_price": jsonable(plan.reference_price),
                }
            },
        )
        event_values = {
            "event_id": submitting_event.event_id,
            "client_order_id": submitting_event.client_order_id,
            "state": submitting_event.state.value,
            "occurred_at": submitting_event.occurred_at,
            "exchange_order_id": submitting_event.exchange_order_id,
            "details": jsonable(submitting_event.details),
        }
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
                raise ValueError("exposure claim fields are only valid for entries")
            if exposure_notional is None:
                ref_price = plan.price or getattr(plan, "reference_price", None)
                if ref_price is not None and ref_price > 0 and plan.quantity > 0:
                    exposure_notional = plan.quantity * ref_price
                elif intent is not None and getattr(intent, "desired_notional", None):
                    exposure_notional = intent.desired_notional
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
                    "live exposure claim baseline must be provided together"
                )
            assert current_daily_pnl is not None
            assert current_gross_exposure is not None
            assert open_position_symbols is not None
            if exposure_notional is None or exposure_notional <= 0:
                raise ValueError("exposure_notional must be positive")
            await session.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
                {
                    "lock_key": (
                        f"live-exposure:{environment}:{account_label}:{strategy_name}"
                    )
                },
            )
            if max_daily_loss is not None and current_daily_pnl <= -max_daily_loss:
                raise OrderPreSubmissionError("max daily loss reached")
            claims_result = (
                await session.execute(
                    select(LiveExposureClaimRow, ExchangeOrderRow)
                    .outerjoin(
                        ExchangeOrderRow,
                        ExchangeOrderRow.intent_id == LiveExposureClaimRow.intent_id,
                    )
                    .where(
                        LiveExposureClaimRow.environment == environment,
                        LiveExposureClaimRow.account_label == account_label,
                        LiveExposureClaimRow.strategy_name == strategy_name,
                        LiveExposureClaimRow.active.is_(True),
                        LiveExposureClaimRow.intent_id != intent.candidate_id,
                    )
                    .with_for_update(of=LiveExposureClaimRow)
                )
            ).all()
            for claim_row, order_row in claims_result:
                if order_row is not None:
                    executed_qty = (
                        order_row.executed_quantity
                        if order_row.executed_quantity is not None
                        else Decimal("0")
                    )
                    terminal_states = (
                        ExchangeOrderState.FILLED.value,
                        ExchangeOrderState.CANCELED.value,
                        ExchangeOrderState.EXPIRED.value,
                        ExchangeOrderState.REJECTED.value,
                    )
                    if order_row.state in terminal_states and executed_qty <= Decimal(
                        "0"
                    ):
                        claim_row.active = False
                        claim_row.updated_at = prepared_at
                    elif order_row.state == ExchangeOrderState.FILLED.value or (
                        executed_qty > Decimal("0")
                        and order_row.state in terminal_states
                    ):
                        is_covered = claim_row.symbol in open_position_symbols and (
                            baseline_observed_at is None
                            or baseline_observed_at >= order_row.updated_at
                        )
                        if is_covered:
                            claim_row.active = False
                            claim_row.updated_at = prepared_at
            active_claims = [c for c, _ in claims_result if c.active]
            active_claim_sum = sum(
                (c.notional for c in active_claims),
                Decimal("0"),
            )
            active_claim_symbols = {c.symbol for c in active_claims}
            if (
                max_open_positions is not None
                and len(
                    set(open_position_symbols) | active_claim_symbols | {plan.symbol}
                )
                > max_open_positions
            ):
                raise OrderPreSubmissionError("max open positions reached")
            if (
                max_gross_exposure is not None
                and current_gross_exposure + active_claim_sum + exposure_notional
                > max_gross_exposure
            ):
                raise OrderPreSubmissionError("max gross exposure reached")
        await session.execute(
            insert(OrderIntentExecutionRow)
            .values(intent_values)
            .on_conflict_do_update(
                index_elements=[OrderIntentExecutionRow.intent_id],
                set_={"state": ExchangeOrderState.SUBMITTING.value},
            )
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
                    ExitEpisodeReservationRow.environment == environment,
                    ExitEpisodeReservationRow.account_label == account_label,
                    ExitEpisodeReservationRow.strategy_name == strategy_name,
                    ExitEpisodeReservationRow.symbol == plan.symbol,
                    ExitEpisodeReservationRow.position_side == plan.position_side.value,
                    ExitEpisodeReservationRow.episode_key == episode_key,
                )
                .with_for_update()
            )
            if reservation is None:
                raise RuntimeError("exit episode reservation disappeared")
            if reservation.active and (
                reservation.intent_id != plan.intent_id
                or reservation.client_order_id != plan.client_order_id
            ):
                raise OrderAlreadyPreparedError
            if not reservation.active:
                await session.execute(
                    update(ExitEpisodeReservationRow)
                    .where(
                        ExitEpisodeReservationRow.environment == environment,
                        ExitEpisodeReservationRow.account_label == account_label,
                        ExitEpisodeReservationRow.strategy_name == strategy_name,
                        ExitEpisodeReservationRow.symbol == plan.symbol,
                        ExitEpisodeReservationRow.position_side
                        == plan.position_side.value,
                        ExitEpisodeReservationRow.episode_key == episode_key,
                    )
                    .values(
                        intent_id=plan.intent_id,
                        client_order_id=plan.client_order_id,
                        active=True,
                        state=ExchangeOrderState.SUBMITTING.value,
                        updated_at=prepared_at,
                    )
                )
        if not plan.reduce_only and any(value is not None for value in exposure_fields):
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
                    ExchangeOrderRow.client_order_id == plan.client_order_id
                )
            )
            if existing_order is None:
                raise RuntimeError("client order ID conflict could not be reconciled")
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
                    raise OrderAlreadyPreparedError
                raise ValueError(
                    "client order ID is already bound to a different order"
                )
            # A restarted or concurrent worker already owns the
            # same order. Roll back any new intent atomically.
            raise OrderAlreadyPreparedError
        await session.execute(
            insert(ExchangeOrderEventRow).values(event_values).on_conflict_do_nothing()
        )
        return PreparedOrderSubmission(
            plan=plan,
            submitting_event=submitting_event,
        )


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
