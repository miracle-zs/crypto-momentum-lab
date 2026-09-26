import inspect
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.account_journal import (
    AccountJournal,
)
from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ExecutionCoordinator,
    ExecutionReadinessError,
    ReservationConflictError,
    VersionConflictError,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.execution.position_book import (
    PositionBook,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    ExitOrderSubmissionFact,
    FreshnessRequirement,
    PositionKey,
    PositionView,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocator,
    ExitPolicyMode,
    PositionReservation,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide


async def _maybe_await(val: Any) -> Any:
    if inspect.isawaitable(val):
        return await val
    return val


class DispatchState(StrEnum):
    """Authoritative lifecycle states for outbound trade commands."""

    PREPARED = "prepared"
    DISPATCHING = "dispatching"
    ACKNOWLEDGED = "acknowledged"
    REJECTED = "rejected"
    UNKNOWN = "unknown"
    TERMINAL = "terminal"


@dataclass(frozen=True, slots=True)
class ExecutionScope:
    environment: str
    account_label: str
    symbol: str
    position_side: FuturesPositionSide = FuturesPositionSide.BOTH

    def to_position_key(self) -> PositionKey:
        return PositionKey(
            environment=self.environment,
            account_label=self.account_label,
            symbol=self.symbol,
            position_side=self.position_side,
        )


@dataclass(frozen=True, slots=True)
class OutboxEntry:
    """Immutable dispatch record tracking external order submission attempt."""

    command_id: str
    request_id: str
    scope: ExecutionScope
    command: TradeCommand
    state: DispatchState = DispatchState.PREPARED
    attempt_count: int = 0
    external_order_id: str | None = None
    last_error: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True, slots=True)
class ExecutionRequest:
    request_id: str
    scope: ExecutionScope
    strategy_name: str
    strategy_version: str
    run_id: str
    decision_ref: str
    expected_view_token: str
    action: TradeCommandType
    requested_quantity: Decimal
    order_type: str = "MARKET"
    limit_price: Decimal | None = None
    reduce_only: bool = False
    target_batch_ids: tuple[str, ...] = ()
    exit_policy_mode: ExitPolicyMode = ExitPolicyMode.CONSOLIDATE_ELIGIBLE
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.request_id.strip():
            raise ValueError("request_id must not be empty")
        if self.requested_quantity <= 0:
            raise ValueError("requested_quantity must be positive")
        if not self.expected_view_token.strip():
            raise ValueError("expected_view_token must not be empty")


@dataclass(frozen=True, slots=True)
class ExecutionReceipt:
    request_id: str
    scope: ExecutionScope
    command: TradeCommand
    reservations: tuple[PositionReservation, ...]
    committed_at: datetime
    view_token: str
    outbox_entry: OutboxEntry | None = None


@dataclass(frozen=True, slots=True)
class Accepted:
    receipt: ExecutionReceipt


@dataclass(frozen=True, slots=True)
class AlreadyAccepted:
    receipt: ExecutionReceipt


@dataclass(frozen=True, slots=True)
class StaleView:
    expected_token: str
    current_token: str
    reason: str


@dataclass(frozen=True, slots=True)
class Blocked:
    reason: str
    diagnostics: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CommandConflict:
    request_id: str
    reason: str


ExecutionActResult = Accepted | AlreadyAccepted | StaleView | Blocked | CommandConflict


@dataclass(frozen=True, slots=True)
class ExecutionEvidence:
    evidence_id: str
    scope: ExecutionScope
    observed_at: datetime
    fill: AccountFillEvent | None = None
    snapshot: AccountPositionSnapshot | None = None
    boundary: ExitOrderSubmissionFact | None = None
    order_event: ExchangeOrderEvent | None = None


@dataclass(frozen=True, slots=True)
class Applied:
    evidence_id: str
    updated_view_token: str
    consumed_quantity: Decimal = Decimal("0")
    released_quantity: Decimal = Decimal("0")


@dataclass(frozen=True, slots=True)
class Duplicate:
    evidence_id: str
    view_token: str


@dataclass(frozen=True, slots=True)
class EvidenceConflict:
    evidence_id: str
    reason: str


ExecutionObserveResult = Applied | Duplicate | EvidenceConflict


class ExecutionBook:
    """Authoritative account execution service coordinating PositionBook, Journal,
    and Reservations.

    Implements Section 7 of RFC 2026-09-25:
    - read(scope, requirement) -> PositionView
    - act(request) -> Accepted | AlreadyAccepted | StaleView | Blocked | CommandConflict
    - observe(evidence) -> Applied | Duplicate | EvidenceConflict
    """

    def __init__(
        self,
        *,
        books_by_key: dict[str, PositionBook] | None = None,
        journals_by_key: dict[str, AccountJournal] | None = None,
        coordinator: ExecutionCoordinator | None = None,
        reservation_repository: Any | None = None,
    ) -> None:
        self._books: dict[str, PositionBook] = books_by_key or {}
        self._journals: dict[str, AccountJournal] = journals_by_key or {}
        self._coordinator = coordinator or ExecutionCoordinator(
            repository=reservation_repository
        )
        self._reservation_repo = reservation_repository
        self._requests_by_id: dict[str, ExecutionRequest] = {}
        self._receipts_by_id: dict[str, ExecutionReceipt] = {}
        self._seen_evidence_ids: set[str] = set()
        self._seen_trade_ids: set[str] = set()
        self._outbox_by_command_id: dict[str, OutboxEntry] = {}
        self._command_reservations: dict[str, list[str]] = {}
        self._order_cumulative_fills: dict[str, Decimal] = {}

    def _ensure_book(self, key: PositionKey) -> PositionBook:
        canon = key.canonical_id
        if canon not in self._books:
            if canon not in self._journals:
                self._journals[canon] = AccountJournal(key)
            self._books[canon] = PositionBook(self._journals[canon])
        return self._books[canon]

    def _ensure_journal(self, key: PositionKey) -> AccountJournal:
        canon = key.canonical_id
        if canon not in self._journals:
            self._journals[canon] = AccountJournal(key)
        return self._journals[canon]

    def _find_active_reservations_for_command(
        self, command_id: str
    ) -> list[PositionReservation]:
        res_ids = self._command_reservations.get(command_id, [])
        active: list[PositionReservation] = []
        for r_id in res_ids:
            r = self._coordinator.get_reservation(r_id)
            if r is not None and r.active_quantity > Decimal("0"):
                active.append(r)
        return active

    async def read(
        self,
        scope: ExecutionScope,
        requirement: FreshnessRequirement | None = None,
        now: datetime | None = None,
    ) -> PositionView:
        """Projects authoritative point-in-time PositionView for scope."""
        key = scope.to_position_key()
        book = self._ensure_book(key)
        return book.get_view(requirement=requirement, now=now)

    async def act(
        self,
        request: ExecutionRequest,
    ) -> ExecutionActResult:
        """Accepts a trade request, enforcing CAS view token, capacity, and outbox."""
        key = request.scope.to_position_key()
        book = self._ensure_book(key)
        view = book.get_view()

        # 1. Idempotency verification
        if request.request_id in self._requests_by_id:
            existing_req = self._requests_by_id[request.request_id]
            if existing_req == request:
                return AlreadyAccepted(self._receipts_by_id[request.request_id])
            return CommandConflict(
                request_id=request.request_id,
                reason="Conflicting payload for identical request_id",
            )

        # 2. View token CAS validation
        if request.expected_view_token != view.projection_version:
            return StaleView(
                expected_token=request.expected_view_token,
                current_token=view.projection_version,
                reason=(
                    f"Expected view token {request.expected_view_token} does not match "
                    f"current projection {view.projection_version}"
                ),
            )

        # 3. Trade readiness check
        if not view.is_ready_for_trade:
            return Blocked(
                reason=(
                    f"PositionView is not ready for trade "
                    f"(status={view.health_status})"
                ),
                diagnostics=view.diagnostics,
            )

        # 4. Command building and reservation calculation
        episode = getattr(view, "active_episode", None)
        if episode is not None and getattr(episode, "side", None) is not None:
            side = episode.side
        elif key.position_side == FuturesPositionSide.SHORT:
            side = StrategySide.SHORT
        else:
            side = StrategySide.LONG

        order_type = (
            EntryType(request.order_type.lower())
            if isinstance(request.order_type, str)
            else request.order_type
        )

        reservations: tuple[PositionReservation, ...] = ()
        if request.action == TradeCommandType.EXIT:
            alloc_plan = ExitAllocator.plan_exit(
                view,
                target_batch_ids=(request.target_batch_ids or None),
                requested_quantity=request.requested_quantity,
                policy=request.exit_policy_mode,
                reason=f"exit_{request.decision_ref}",
            )

            if (
                alloc_plan is None
                or alloc_plan.total_allocated_quantity <= Decimal("0")
            ):
                return Blocked(
                    reason=(
                        "Insufficient active batch capacity for "
                        "requested exit quantity"
                    ),
                    diagnostics=(
                        f"Requested: {request.requested_quantity}, "
                        f"Total active: {view.total_quantity}",
                    ),
                )

            command = TradeCommand(
                command_id=request.request_id,
                position_key=key,
                command_type=TradeCommandType.EXIT,
                side=side,
                order_type=order_type,
                requested_quantity=alloc_plan.total_allocated_quantity,
                limit_price=request.limit_price,
                reduce_only=True,
                expected_projection_version=view.projection_version,
                allocation_plan=alloc_plan,
                created_at=request.created_at,
            )

            try:
                reservations = self._coordinator.reserve_exit(command, view)
            except (
                ReservationConflictError,
                VersionConflictError,
                ExecutionReadinessError,
            ) as err:
                return Blocked(
                    reason=str(err),
                    diagnostics=(type(err).__name__,),
                )

            if self._reservation_repo is not None:
                saver = getattr(self._reservation_repo, "save_reservations", None)
                if callable(saver):
                    await _maybe_await(
                        saver(
                            reservations,
                            expected_projection_version=view.projection_version,
                        )
                    )
                else:
                    for res in reservations:
                        await _maybe_await(
                            self._reservation_repo.save_reservation(
                                res,
                                expected_projection_version=view.projection_version,
                            )
                        )
        else:
            command = TradeCommand(
                command_id=request.request_id,
                position_key=key,
                command_type=request.action,
                side=side,
                order_type=order_type,
                requested_quantity=request.requested_quantity,
                limit_price=request.limit_price,
                reduce_only=request.reduce_only,
                expected_projection_version=view.projection_version,
                created_at=request.created_at,
            )

        committed_at = datetime.now(UTC)
        outbox = OutboxEntry(
            command_id=command.command_id,
            request_id=request.request_id,
            scope=request.scope,
            command=command,
            state=DispatchState.PREPARED,
            created_at=committed_at,
            updated_at=committed_at,
        )
        self._outbox_by_command_id[command.command_id] = outbox
        if reservations:
            self._command_reservations[command.command_id] = [
                r.reservation_id for r in reservations
            ]

        receipt = ExecutionReceipt(
            request_id=request.request_id,
            scope=request.scope,
            command=command,
            reservations=reservations,
            committed_at=committed_at,
            view_token=view.projection_version,
            outbox_entry=outbox,
        )
        self._requests_by_id[request.request_id] = request
        self._receipts_by_id[request.request_id] = receipt
        return Accepted(receipt)

    def get_outbox(self, command_id: str) -> OutboxEntry | None:
        """Returns the outbox record for command_id if found."""
        return self._outbox_by_command_id.get(command_id)

    def list_outbox(
        self,
        scope: ExecutionScope | None = None,
        state: DispatchState | None = None,
    ) -> tuple[OutboxEntry, ...]:
        """Queries outbox records filtered by scope and dispatch state."""
        entries: list[OutboxEntry] = list(self._outbox_by_command_id.values())
        if scope is not None:
            entries = [
                e
                for e in entries
                if e.scope.to_position_key().canonical_id
                == scope.to_position_key().canonical_id
            ]
        if state is not None:
            entries = [e for e in entries if e.state == state]
        return tuple(entries)

    def mark_dispatching(
        self, command_id: str, dispatched_at: datetime | None = None
    ) -> OutboxEntry:
        """Transitions outbox from PREPARED/UNKNOWN to DISPATCHING."""
        entry = self._outbox_by_command_id.get(command_id)
        if entry is None:
            raise KeyError(f"Outbox entry {command_id} not found")
        if entry.state not in (DispatchState.PREPARED, DispatchState.UNKNOWN):
            raise ValueError(
                f"Cannot dispatch outbox entry in state {entry.state.value}"
            )
        now = dispatched_at or datetime.now(UTC)
        updated = replace(
            entry,
            state=DispatchState.DISPATCHING,
            attempt_count=entry.attempt_count + 1,
            updated_at=now,
        )
        self._outbox_by_command_id[command_id] = updated
        return updated

    def mark_acknowledged(
        self,
        command_id: str,
        external_order_id: str,
        acknowledged_at: datetime | None = None,
    ) -> OutboxEntry:
        """Transitions outbox to ACKNOWLEDGED with external exchange order ID."""
        entry = self._outbox_by_command_id.get(command_id)
        if entry is None:
            raise KeyError(f"Outbox entry {command_id} not found")
        now = acknowledged_at or datetime.now(UTC)
        updated = replace(
            entry,
            state=DispatchState.ACKNOWLEDGED,
            external_order_id=external_order_id,
            updated_at=now,
        )
        self._outbox_by_command_id[command_id] = updated
        return updated

    def mark_unknown(
        self,
        command_id: str,
        reason: str,
        unknown_at: datetime | None = None,
    ) -> OutboxEntry:
        """Transitions outbox to UNKNOWN while preserving active reservations."""
        entry = self._outbox_by_command_id.get(command_id)
        if entry is None:
            raise KeyError(f"Outbox entry {command_id} not found")
        now = unknown_at or datetime.now(UTC)
        updated = replace(
            entry,
            state=DispatchState.UNKNOWN,
            last_error=reason,
            updated_at=now,
        )
        self._outbox_by_command_id[command_id] = updated
        return updated

    def mark_rejected(
        self,
        command_id: str,
        reason: str,
        rejected_at: datetime | None = None,
    ) -> OutboxEntry:
        """Transitions outbox to REJECTED and releases all active reservations."""
        entry = self._outbox_by_command_id.get(command_id)
        if entry is None:
            raise KeyError(f"Outbox entry {command_id} not found")
        now = rejected_at or datetime.now(UTC)
        active_res = self._find_active_reservations_for_command(command_id)
        for res in active_res:
            self._coordinator.release_reservation(res.reservation_id)

        updated = replace(
            entry,
            state=DispatchState.REJECTED,
            last_error=reason,
            updated_at=now,
        )
        self._outbox_by_command_id[command_id] = updated
        return updated

    def mark_terminal(
        self,
        command_id: str,
        reason: str = "",
        terminal_at: datetime | None = None,
    ) -> OutboxEntry:
        """Transitions outbox to TERMINAL and releases remaining reservations."""
        entry = self._outbox_by_command_id.get(command_id)
        if entry is None:
            raise KeyError(f"Outbox entry {command_id} not found")
        now = terminal_at or datetime.now(UTC)
        active_res = self._find_active_reservations_for_command(command_id)
        for res in active_res:
            self._coordinator.release_reservation(res.reservation_id)

        updated = replace(
            entry,
            state=DispatchState.TERMINAL,
            last_error=reason if reason else entry.last_error,
            updated_at=now,
        )
        self._outbox_by_command_id[command_id] = updated
        return updated

    async def observe(
        self,
        evidence: ExecutionEvidence,
    ) -> ExecutionObserveResult:
        """Idempotently ingests exchange evidence and settles allocations."""
        if evidence.evidence_id in self._seen_evidence_ids:
            key = evidence.scope.to_position_key()
            book = self._ensure_book(key)
            return Duplicate(
                evidence_id=evidence.evidence_id,
                view_token=book.get_view().projection_version,
            )

        key = evidence.scope.to_position_key()
        journal = self._ensure_journal(key)
        book = self._ensure_book(key)

        consumed_qty = Decimal("0")
        released_qty = Decimal("0")

        # 1. Process Fill
        if evidence.fill is not None:
            trade_id = evidence.fill.trade_id
            order_id = evidence.fill.order_id
            is_new_trade = trade_id not in self._seen_trade_ids

            if is_new_trade:
                accepted = journal.append_fill(evidence.fill)
                if not accepted and journal.has_conflicts:
                    return EvidenceConflict(
                        evidence_id=evidence.evidence_id,
                        reason=(
                            f"Fill {evidence.fill.trade_id} conflicted with "
                            "existing journal records"
                        ),
                    )
                self._seen_trade_ids.add(trade_id)

                # Deduplicate cumulative vs incremental fill quantity
                # Invariant: 3 -> 3 -> 5 only consumes 5 total
                fill_qty = evidence.fill.quantity
                if (
                    isinstance(evidence.fill.raw_payload, dict)
                    and (
                        evidence.fill.raw_payload.get("is_cumulative")
                        or "cum_qty" in evidence.fill.raw_payload
                    )
                ):
                    cum_val = Decimal(
                        str(evidence.fill.raw_payload.get("cum_qty", fill_qty))
                    )
                    prev_cum = self._order_cumulative_fills.get(
                        order_id, Decimal("0")
                    )
                    delta_qty = max(Decimal("0"), cum_val - prev_cum)
                    self._order_cumulative_fills[order_id] = cum_val
                else:
                    delta_qty = fill_qty

                # Reconcile active reservations if this is an exit / reduction fill
                is_exit_fill = (
                    (
                        evidence.fill.side.upper() == "SELL"
                        and key.position_side == FuturesPositionSide.LONG
                    )
                    or (
                        evidence.fill.side.upper() == "BUY"
                        and key.position_side == FuturesPositionSide.SHORT
                    )
                )

                if is_exit_fill and delta_qty > Decimal("0"):
                    cand_res = self._find_active_reservations_for_command(
                        order_id
                    )
                    if not cand_res:
                        cand_res = list(
                            self._coordinator.get_active_reservations(key)
                        )

                    remaining = delta_qty
                    for res in cand_res:
                        if remaining <= Decimal("0"):
                            break
                        consume_amt = min(remaining, res.active_quantity)
                        if consume_amt > Decimal("0"):
                            updated_res = self._coordinator.reconcile_fill(
                                res.reservation_id, consume_amt
                            )
                            if self._reservation_repo is not None:
                                updater = getattr(
                                    self._reservation_repo, "update_reservation", None
                                )
                                if callable(updater):
                                    await _maybe_await(updater(updated_res))
                            consumed_qty += consume_amt
                            remaining -= consume_amt

        # 2. Process Snapshot
        if evidence.snapshot is not None:
            journal.record_snapshot(evidence.snapshot)

        # 3. Process Boundary
        if evidence.boundary is not None:
            journal.record_boundary(evidence.boundary)

        # 4. Process Order Event
        if evidence.order_event is not None:
            ev_state = evidence.order_event.state
            cmd_id = evidence.order_event.client_order_id
            outbox = self._outbox_by_command_id.get(cmd_id)

            if ev_state in (
                ExchangeOrderState.ACKNOWLEDGED,
                ExchangeOrderState.SUBMITTED,
            ):
                if outbox is not None and outbox.state in (
                    DispatchState.PREPARED,
                    DispatchState.DISPATCHING,
                    DispatchState.UNKNOWN,
                ):
                    self._outbox_by_command_id[cmd_id] = replace(
                        outbox,
                        state=DispatchState.ACKNOWLEDGED,
                        updated_at=evidence.observed_at,
                    )
            elif ev_state in (
                ExchangeOrderState.CANCELED,
                ExchangeOrderState.EXPIRED,
                ExchangeOrderState.REJECTED,
                ExchangeOrderState.ABSENT_RECONCILED,
            ):
                # Terminal non-filled state: release remaining active reservations
                active_res = self._find_active_reservations_for_command(cmd_id)
                for res in active_res:
                    to_release = res.active_quantity
                    released_res = self._coordinator.release_reservation(
                        res.reservation_id, to_release
                    )
                    if self._reservation_repo is not None:
                        updater = getattr(
                            self._reservation_repo, "update_reservation", None
                        )
                        if callable(updater):
                            await _maybe_await(
                                updater(
                                    released_res,
                                    release_reason=f"order_finished_residual_release_{ev_state.value}",
                                )
                            )
                    released_qty += to_release

                if outbox is not None:
                    target_state = (
                        DispatchState.REJECTED
                        if ev_state == ExchangeOrderState.REJECTED
                        else DispatchState.TERMINAL
                    )
                    self._outbox_by_command_id[cmd_id] = replace(
                        outbox,
                        state=target_state,
                        last_error=f"Order {ev_state.value}",
                        updated_at=evidence.observed_at,
                    )
            elif ev_state == ExchangeOrderState.FILLED:
                active_res = self._find_active_reservations_for_command(cmd_id)
                for res in active_res:
                    to_release = res.active_quantity
                    released_res = self._coordinator.release_reservation(
                        res.reservation_id, to_release
                    )
                    if self._reservation_repo is not None:
                        updater = getattr(
                            self._reservation_repo, "update_reservation", None
                        )
                        if callable(updater):
                            await _maybe_await(
                                updater(
                                    released_res,
                                    release_reason="order_finished_residual_release_filled",
                                )
                            )
                    released_qty += to_release

                if outbox is not None:
                    self._outbox_by_command_id[cmd_id] = replace(
                        outbox,
                        state=DispatchState.TERMINAL,
                        updated_at=evidence.observed_at,
                    )
            elif ev_state == ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
                if outbox is not None:
                    self._outbox_by_command_id[cmd_id] = replace(
                        outbox,
                        state=DispatchState.UNKNOWN,
                        last_error="Pending reconciliation",
                        updated_at=evidence.observed_at,
                    )

        self._seen_evidence_ids.add(evidence.evidence_id)
        updated_view = book.get_view(now=evidence.observed_at)

        return Applied(
            evidence_id=evidence.evidence_id,
            updated_view_token=updated_view.projection_version,
            consumed_quantity=consumed_qty,
            released_quantity=released_qty,
        )


__all__ = [
    "Accepted",
    "AlreadyAccepted",
    "Blocked",
    "CommandConflict",
    "DispatchState",
    "Duplicate",
    "EvidenceConflict",
    "ExecutionActResult",
    "ExecutionBook",
    "ExecutionEvidence",
    "ExecutionObserveResult",
    "ExecutionReceipt",
    "ExecutionRequest",
    "ExecutionScope",
    "OutboxEntry",
    "StaleView",
]
