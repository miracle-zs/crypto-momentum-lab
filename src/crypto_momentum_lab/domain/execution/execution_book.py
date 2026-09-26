from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
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
        self._coordinator = coordinator or ExecutionCoordinator()
        self._reservation_repo = reservation_repository
        self._requests_by_id: dict[str, ExecutionRequest] = {}
        self._receipts_by_id: dict[str, ExecutionReceipt] = {}
        self._seen_evidence_ids: set[str] = set()

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
        """Accepts a trade request, enforcing CAS view token and capacity."""
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
                for res in reservations:
                    self._reservation_repo.save_reservation(
                        res,
                        expected_projection_version=view.projection_version,
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

        receipt = ExecutionReceipt(
            request_id=request.request_id,
            scope=request.scope,
            command=command,
            reservations=reservations,
            committed_at=datetime.now(UTC),
            view_token=view.projection_version,
        )
        self._requests_by_id[request.request_id] = request
        self._receipts_by_id[request.request_id] = receipt
        return Accepted(receipt)

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

        if evidence.fill is not None:
            accepted = journal.append_fill(evidence.fill)
            if not accepted and journal.has_conflicts:
                return EvidenceConflict(
                    evidence_id=evidence.evidence_id,
                    reason=(
                        f"Fill {evidence.fill.trade_id} conflicted with "
                        "existing journal records"
                    ),
                )
            if (
                evidence.fill.side.upper() == "SELL"
                and key.position_side == FuturesPositionSide.LONG
            ):
                consumed_qty = evidence.fill.quantity

        if evidence.snapshot is not None:
            journal.record_snapshot(evidence.snapshot)

        if evidence.boundary is not None:
            journal.record_boundary(evidence.boundary)

        self._seen_evidence_ids.add(evidence.evidence_id)
        updated_view = book.get_view(now=evidence.observed_at)

        return Applied(
            evidence_id=evidence.evidence_id,
            updated_view_token=updated_view.projection_version,
            consumed_quantity=consumed_qty,
            released_quantity=released_qty,
        )


__all__ = [
    "ExecutionScope",
    "ExecutionRequest",
    "ExecutionReceipt",
    "Accepted",
    "AlreadyAccepted",
    "StaleView",
    "Blocked",
    "CommandConflict",
    "ExecutionActResult",
    "ExecutionEvidence",
    "Applied",
    "Duplicate",
    "EvidenceConflict",
    "ExecutionObserveResult",
    "ExecutionBook",
]
