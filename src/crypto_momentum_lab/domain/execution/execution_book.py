from __future__ import annotations

import asyncio
import copy
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import structlog

from crypto_momentum_lab.domain.execution.account_journal import (
    AccountJournal,
)
from crypto_momentum_lab.domain.execution.command_codec import encode_outbox_details
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionEvidence,
)
from crypto_momentum_lab.domain.execution.execution_action_models import (
    Accepted,
    AlreadyAccepted,
    Blocked,
    CommandConflict,
    ExecutionActResult,
    ExecutionReceipt,
    ExecutionRequest,
    StaleView,
)
from crypto_momentum_lab.domain.execution.execution_action_processor import (
    ExecutionActionDependencies,
    accept_execution_command,
)
from crypto_momentum_lab.domain.execution.execution_action_state import (
    ExecutionActionState,
    register_prepared_outbox,
)
from crypto_momentum_lab.domain.execution.execution_action_transaction import (
    accept_action_transaction,
)
from crypto_momentum_lab.domain.execution.execution_book_commands import (
    CommandLifecycleState,
    CommandPersistenceDependencies,
    apply_command_transition,
    persist_command_transition,
    release_command_reservations,
    settle_command_reservations,
)
from crypto_momentum_lab.domain.execution.execution_book_recovery import (
    ExecutionRecoveryState,
    RecoveryDependencies,
    ReloadDependencies,
    apply_loaded_execution_position,
    restore_execution_state,
)
from crypto_momentum_lab.domain.execution.execution_checkpoint_reader import (
    load_recovery_checkpoint as read_recovery_checkpoint,
)
from crypto_momentum_lab.domain.execution.execution_evidence_processor import (
    EvidenceMutationDependencies,
    mutate_evidence,
)
from crypto_momentum_lab.domain.execution.execution_evidence_state import (
    EvidenceState,
)
from crypto_momentum_lab.domain.execution.execution_evidence_transaction import (
    observe_evidence_transaction,
)
from crypto_momentum_lab.domain.execution.execution_head import (
    build_execution_head_payload,
)
from crypto_momentum_lab.domain.execution.execution_position_reader import (
    list_position_views as read_position_views,
)
from crypto_momentum_lab.domain.execution.observation_models import (
    Duplicate,
    EvidenceConflict,
    ExecutionObserveResult,
    WaitingForEvidence,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderRejectionRefusedError,
    PreparedOrderSubmission,
)
from crypto_momentum_lab.domain.execution.ports import (
    CommandRepository,
    DecisionCommitConflict,
    DurableExecutionPositionState,
    ExecutionTransactionPort,
    ExecutionUnitOfWorkPort,
    ReservationRepository,
)
from crypto_momentum_lab.domain.execution.position_book import (
    PositionBook,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    FreshnessRequirement,
    PositionKey,
    PositionStreamMismatchError,
    PositionView,
)
from crypto_momentum_lab.domain.execution.position_repair import (
    build_position_repair,
)
from crypto_momentum_lab.domain.execution.position_repair_models import (
    PositionRepairBlocked,
    PositionRepairRequest,
    PositionRepairUnitOfWork,
    PublishedPositionRepair,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    PositionRecoveryCheckpoint,
)
from crypto_momentum_lab.domain.execution.reservation_registry import (
    ReservationRegistry,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    PositionReservation,
    TradeCommand,
    TradeCommandType,
)

log = structlog.get_logger(__name__)


class _AbortObservation(Exception):
    def __init__(self, result: ExecutionObserveResult) -> None:
        self.result = result


def _is_unfilled_terminal_order(evidence: ExecutionEvidence) -> bool:
    return (
        evidence.order_event is not None
        and evidence.order_event.state.terminal
        and not evidence.settlement_fills
        and not evidence.fills
        and evidence.fill is None
        and (
            evidence.cumulative_order is None
            or evidence.cumulative_order.cumulative_quantity == Decimal("0")
        )
    )


def _reservations_held_by_terminal_order(
    evidence: ExecutionEvidence,
    reservations: tuple[PositionReservation, ...],
) -> bool:
    event = evidence.order_event
    return (
        _is_unfilled_terminal_order(evidence)
        and event is not None
        and all(
            reservation.command_id == event.client_order_id
            for reservation in reservations
        )
    )


class ExecutionBook:
    """Authoritative account execution service coordinating PositionBook, Journal,
    and Reservations.

    Implements Section 7 of RFC 2026-09-25:
    - read(scope, requirement) -> PositionView
    - act(request) -> Accepted | AlreadyAccepted | StaleView | Blocked | CommandConflict
    - observe(evidence) -> Applied | Duplicate | WaitingForEvidence | EvidenceConflict
    """

    def __init__(
        self,
        *,
        books_by_key: dict[str, PositionBook] | None = None,
        journals_by_key: dict[str, AccountJournal] | None = None,
        coordinator: ReservationRegistry | None = None,
        reservation_repository: ReservationRepository | None = None,
        command_repository: CommandRepository | None = None,
        execution_unit_of_work: ExecutionUnitOfWorkPort | None = None,
    ) -> None:
        self._books: dict[str, PositionBook] = books_by_key or {}
        self._journals: dict[str, AccountJournal] = journals_by_key or {}
        # Default coordinator owns memory only. Durable/async restoration is
        # explicitly awaited by restore.
        self._coordinator = coordinator or ReservationRegistry()
        self._reservation_repo = reservation_repository
        self._command_repo = command_repository
        self._execution_unit_of_work = execution_unit_of_work
        self._active_transaction: ExecutionTransactionPort | None = None
        self._stream_scopes: dict[str, AccountFactStreamScope] = {}
        self._active_streams: set[tuple[str, str, str, str]] = set()
        self._latest_active_streams: dict[tuple[str, str], tuple[str, str]] = {}
        self._head_revisions: dict[str, int] = {}
        self._head_expected_reservation_ids: dict[str, set[str]] = {}
        self._journal_revisions: dict[str, int] = {}
        self._last_sequences: dict[str, int] = {}
        self._recovery_adoption_scope: AccountFactStreamScope | None = None
        self._process_mutation_lock = asyncio.Lock()
        self._mutation_locks: dict[str, asyncio.Lock] = {}
        self._requests_by_id: dict[str, ExecutionRequest] = {}
        self._receipts_by_id: dict[str, ExecutionReceipt] = {}
        self._seen_evidence_ids: set[str] = set()
        self._seen_trade_ids: set[str] = set()
        self._outbox_by_command_id: dict[str, OutboxEntry] = {}
        self._command_reservations: dict[str, list[str]] = {}
        self._order_cumulative_fills: dict[str, Decimal] = {}
        self._order_cumulative_quotes: dict[str, Decimal] = {}
        self._persistence_failed = execution_unit_of_work is not None
        self._recovery_required_commands: set[str] = set()
        self._external_recovery_positions: dict[str, PositionKey] = {}
        self._dispatch_reconciliation_required_commands: set[str] = set()
        self._transaction_journal_keys: tuple[str, ...] = ()
        self._initial_latest_active_streams: dict[tuple[str, str], tuple[str, str]] = {}
        self._initial_recovery_adoption_scope: AccountFactStreamScope | None = None
        self._staged_recovery_required_commands: set[str] = set()
        self._staged_dispatch_reconciliation_required_commands: set[str] = set()
        self._staged_external_recovery_positions: dict[str, PositionKey] = {}
        self._context_revision = 0
        self._action_state = ExecutionActionState(
            requests_by_id=self._requests_by_id,
            receipts_by_id=self._receipts_by_id,
            outbox_by_command_id=self._outbox_by_command_id,
            command_reservations=self._command_reservations,
            recovery_required_commands=self._recovery_required_commands,
            dispatch_reconciliation_required_commands=(
                self._dispatch_reconciliation_required_commands
            ),
        )
        self._evidence_state = EvidenceState(
            books=self._books,
            journals=self._journals,
            stream_scopes=self._stream_scopes,
            active_streams=self._active_streams,
            head_revisions=self._head_revisions,
            journal_revisions=self._journal_revisions,
            last_sequences=self._last_sequences,
            seen_evidence_ids=self._seen_evidence_ids,
            seen_trade_ids=self._seen_trade_ids,
            outbox_by_command_id=self._outbox_by_command_id,
            command_reservations=self._command_reservations,
            order_cumulative_fills=self._order_cumulative_fills,
            order_cumulative_quotes=self._order_cumulative_quotes,
            recovery_required_commands=self._recovery_required_commands,
            external_recovery_positions=self._external_recovery_positions,
            dispatch_reconciliation_required_commands=(
                self._dispatch_reconciliation_required_commands
            ),
            coordinator=self._coordinator,
            recovery_adoption_scope=self._recovery_adoption_scope,
        )
        self._command_state = CommandLifecycleState(
            outbox_by_command_id=self._outbox_by_command_id,
            dispatch_reconciliation_required_commands=(
                self._dispatch_reconciliation_required_commands
            ),
            recovery_required_commands=self._recovery_required_commands,
        )
        self._recovery_state = ExecutionRecoveryState(
            books=self._books,
            journals=self._journals,
            stream_scopes=self._stream_scopes,
            head_revisions=self._head_revisions,
            head_expected_reservation_ids=self._head_expected_reservation_ids,
            journal_revisions=self._journal_revisions,
            last_sequences=self._last_sequences,
            seen_evidence_ids=self._seen_evidence_ids,
            seen_trade_ids=self._seen_trade_ids,
            order_cumulative_fills=self._order_cumulative_fills,
            order_cumulative_quotes=self._order_cumulative_quotes,
            outbox_by_command_id=self._outbox_by_command_id,
            command_reservations=self._command_reservations,
            recovery_required_commands=self._recovery_required_commands,
            external_recovery_positions=self._external_recovery_positions,
            dispatch_reconciliation_required_commands=(
                self._dispatch_reconciliation_required_commands
            ),
            coordinator=self._coordinator,
        )

    @property
    def context_revision(self) -> int:
        """Published facts/commands generation used by operational read caches."""
        return self._context_revision

    @property
    def recovery_state(self) -> ExecutionRecoveryState:
        """State consumed by the recovery module during startup only."""
        return self._recovery_state

    @property
    def evidence_state(self) -> EvidenceState:
        """State consumed by the evidence processor inside one transaction."""
        return self._evidence_state

    def register_active_stream(
        self,
        *,
        environment: str,
        account_label: str,
        stream_id: str,
        stream_epoch: str,
    ) -> None:
        """Register an authoritative account fact stream as active for this book."""
        if not stream_id or not stream_epoch:
            return
        self._active_streams.add((environment, account_label, stream_id, stream_epoch))
        self._latest_active_streams[(environment, account_label)] = (
            stream_id,
            stream_epoch,
        )

    def get_active_stream(
        self,
        environment: str,
        account_label: str,
    ) -> tuple[str, str] | None:
        """Return active (stream_id, stream_epoch) for this account, if known."""
        latest = self._latest_active_streams.get((environment, account_label))
        if latest is not None:
            return latest
        for env, acc, sid, epoch in self._active_streams:
            if env == environment and acc == account_label:
                return (sid, epoch)
        for s in self._stream_scopes.values():
            if s.environment == environment and s.account_label == account_label:
                return (s.stream_id, s.stream_epoch)
        return None

    async def repair_position(
        self,
        request: PositionRepairRequest,
        *,
        uow: PositionRepairUnitOfWork,
        is_current: Callable[[], bool] | None = None,
    ) -> PublishedPositionRepair:
        """Serialize fact load, durable repair commit and publication with observe.

        Every writer takes the Book lock before the database lock. No observer
        can see a committed repair head against the old in-memory projection.
        """
        for attempt in range(3):
            try:
                async with uow.transaction(request.key) as tx:
                    loaded = await tx.load_repair_facts(request)
                    if is_current is not None and not is_current():
                        raise PositionRepairBlocked(
                            "repair context advanced during fact load"
                        )
                    repair = build_position_repair(request, loaded)
                    receipt = await tx.persist_repair(repair)
                break
            except DecisionCommitConflict:
                if attempt == 2:
                    raise

        try:
            view = await self.reload_position(
                request.key,
                expected_scope=request.scope,
                expected_quantity=request.expected_quantity,
            )
            if (
                view is None
                or view.stream_scope != receipt.scope
                or view.total_quantity != request.expected_quantity
                or not view.is_ready_for_trade
            ):
                raise PositionRepairBlocked(
                    "durable repair committed but Book reload is not ready"
                )
        except PositionRepairBlocked as err:
            target_state = await self._load_position_durable(request.key)
            target_rev = (
                target_state.head.revision
                if target_state is not None and target_state.head is not None
                else 0
            )
            if target_rev > receipt.head_revision:
                view = await self.reload_position(
                    request.key,
                    expected_scope=request.scope,
                    expected_quantity=None,
                )
                if view is None or not view.is_ready_for_trade:
                    self._persistence_failed = True
                    raise PositionRepairBlocked(
                        "durable repair committed but subsequent Book reload "
                        "is not ready"
                    ) from err
            else:
                self._persistence_failed = True
                raise
        except Exception:
            # Commit succeeded; admission must remain sealed until restore
            # if the committed projection cannot be published.
            self._persistence_failed = True
            raise
        return PublishedPositionRepair(receipt, view, repair.new_facts)

    async def reconcile_reservation_divergence(
        self,
        key: PositionKey,
        *,
        as_of: datetime | None = None,
        force: bool = False,
    ) -> bool:
        """Attempt to reconcile reservation divergence for a position.

        If force=True, acknowledges current reservations and clears the gate.
        Otherwise, reloads durable storage to verify whether active reservations
        now match the head expectation.
        """
        canon = key.canonical_id
        divergence_identity = f"reservation_divergence:{canon}"
        if divergence_identity not in self._recovery_required_commands:
            return True
        if force:
            async with self._mutation_lock(key):
                self._recovery_required_commands.discard(divergence_identity)
                self._external_recovery_positions.pop(divergence_identity, None)
                self._head_expected_reservation_ids.pop(canon, None)
                return True
        for attempt in range(3):
            target_state = await self._load_position_durable(key, as_of=as_of)
            async with self._mutation_lock(key):
                current_rev = self._head_revisions.get(canon, 0)
                target_rev = (
                    target_state.head.revision
                    if target_state is not None and target_state.head is not None
                    else 0
                )
                if target_state is not None and target_rev < current_rev:
                    if attempt < 2:
                        continue
                    return False
                view = self._apply_loaded_position(key, target_state)
                if view is None:
                    return False
                expected_ids = self._head_expected_reservation_ids.get(canon)
                if expected_ids is None:
                    return False
                actual_ids = {
                    r.reservation_id for r in self.get_active_reservations(key)
                }
                if actual_ids == expected_ids:
                    self._recovery_required_commands.discard(divergence_identity)
                    self._external_recovery_positions.pop(divergence_identity, None)
                    self._head_expected_reservation_ids.pop(canon, None)
                    return True
                return False
        return False

    async def reload_position(
        self,
        key: PositionKey,
        *,
        as_of: datetime | None = None,
        expected_scope: AccountFactStreamScope | None = None,
        expected_quantity: Decimal | None = None,
    ) -> PositionView | None:
        canon = key.canonical_id
        for attempt in range(3):
            target_state = await self._load_position_durable(key, as_of=as_of)
            async with self._mutation_lock(key):
                current_rev = self._head_revisions.get(canon, 0)
                target_rev = (
                    target_state.head.revision
                    if target_state is not None and target_state.head is not None
                    else 0
                )
                if target_state is not None and target_rev < current_rev:
                    if attempt < 2:
                        continue
                    existing_book = self._books.get(canon)
                    return (
                        existing_book.get_view()
                        if existing_book is not None
                        else None
                    )
                return self._apply_loaded_position(
                    key,
                    target_state,
                    expected_scope=expected_scope,
                    expected_quantity=expected_quantity,
                )
        return None

    _reload_position = reload_position

    async def _load_position_durable(
        self,
        key: PositionKey,
        *,
        as_of: datetime | None = None,
    ) -> DurableExecutionPositionState | None:
        load_fn = getattr(self._execution_unit_of_work, "load_position", None)
        if load_fn is None:
            return None
        as_of = as_of or datetime.now(UTC)
        result = await load_fn(key, as_of=as_of)
        return cast(DurableExecutionPositionState | None, result)

    def _apply_loaded_position(
        self,
        key: PositionKey,
        target_state: DurableExecutionPositionState | None,
        *,
        expected_scope: AccountFactStreamScope | None = None,
        expected_quantity: Decimal | None = None,
    ) -> PositionView | None:
        return cast(
            PositionView | None,
            apply_loaded_execution_position(
                self._recovery_state,
                ReloadDependencies(
                    load_position=getattr(
                        self._execution_unit_of_work, "load_position", None
                    ),
                    active_reservations=self.get_active_reservations,
                    order_watermark_key=self._order_watermark_key,
                    advance_context_revision=lambda: setattr(
                        self, "_context_revision", self._context_revision + 1
                    ),
                ),
                key,
                target_state,
                expected_scope=expected_scope,
                expected_quantity=expected_quantity,
            ),
        )

    @property
    def coordinator(self) -> ReservationRegistry:
        return self._coordinator

    @property
    def has_reservation_repository(self) -> bool:
        return self._reservation_repo is not None

    @property
    def has_command_repository(self) -> bool:
        return self._command_repo is not None

    @property
    def has_execution_unit_of_work(self) -> bool:
        """Whether mutations require the durable PostgreSQL commit path."""
        return self._execution_unit_of_work is not None

    def _mutation_lock(self, key: PositionKey | None = None) -> asyncio.Lock:
        """Process-local mutation lock for this ExecutionBook instance.

        Each position key is isolated with its own asyncio.Lock to avoid
        cross-symbol lock contention during lock-free I/O.
        If key is None, falls back to the instance-level process lock.
        """
        if key is None:
            return self._process_mutation_lock
        canon = key.canonical_id
        if canon not in self._mutation_locks:
            self._mutation_locks[canon] = asyncio.Lock()
        return self._mutation_locks[canon]

    def _staged_copy(self, key: PositionKey | None = None) -> ExecutionBook:
        """Copy published domain state before entering a durable transaction."""
        candidate = copy.copy(self)
        candidate._books = dict(self._books)
        candidate._journals = dict(self._journals)
        if key is not None:
            canon = key.canonical_id
            journal = self._journals.get(canon)
            if journal is not None:
                copied_journal = journal.copy_for_transaction()
                candidate._journals[canon] = copied_journal
                copied_book = self._books.get(canon)
                if copied_book is not None:
                    candidate._books[canon] = copied_book.copy_for_transaction(
                        copied_journal
                    )
        else:
            # Container-level copies for every position: recorded facts are
            # frozen and never mutated in place, so sharing them keeps the
            # candidate isolated without an O(history) deepcopy.
            candidate._journals = {
                canon: journal.copy_for_transaction()
                for canon, journal in self._journals.items()
            }
            candidate._books = {
                canon: book.copy_for_transaction(candidate._journals.get(canon))
                for canon, book in self._books.items()
            }
        candidate._transaction_journal_keys = (
            (key.canonical_id,) if key is not None else tuple(candidate._journals)
        )
        candidate._requests_by_id = dict(self._requests_by_id)
        candidate._receipts_by_id = dict(self._receipts_by_id)
        candidate._seen_evidence_ids = set(self._seen_evidence_ids)
        candidate._seen_trade_ids = set(self._seen_trade_ids)
        candidate._outbox_by_command_id = dict(self._outbox_by_command_id)
        candidate._command_reservations = dict(self._command_reservations)
        candidate._order_cumulative_fills = dict(self._order_cumulative_fills)
        candidate._order_cumulative_quotes = dict(self._order_cumulative_quotes)
        candidate._recovery_required_commands = set(self._recovery_required_commands)
        candidate._external_recovery_positions = dict(self._external_recovery_positions)
        candidate._dispatch_reconciliation_required_commands = set(
            self._dispatch_reconciliation_required_commands
        )
        candidate._stream_scopes = dict(self._stream_scopes)
        candidate._active_streams = set(self._active_streams)
        candidate._latest_active_streams = dict(self._latest_active_streams)
        candidate._head_revisions = dict(self._head_revisions)
        candidate._head_expected_reservation_ids = dict(
            self._head_expected_reservation_ids
        )
        candidate._journal_revisions = dict(self._journal_revisions)
        candidate._last_sequences = dict(self._last_sequences)
        candidate._recovery_adoption_scope = self._recovery_adoption_scope
        candidate._initial_latest_active_streams = dict(self._latest_active_streams)
        candidate._initial_recovery_adoption_scope = self._recovery_adoption_scope
        candidate._staged_recovery_required_commands = set(
            self._recovery_required_commands
        )
        candidate._staged_dispatch_reconciliation_required_commands = set(
            self._dispatch_reconciliation_required_commands
        )
        candidate._staged_external_recovery_positions = dict(
            self._external_recovery_positions
        )
        candidate._coordinator = self._coordinator.copy_for_transaction()
        candidate._command_state = CommandLifecycleState(
            outbox_by_command_id=candidate._outbox_by_command_id,
            dispatch_reconciliation_required_commands=(
                candidate._dispatch_reconciliation_required_commands
            ),
            recovery_required_commands=candidate._recovery_required_commands,
        )
        candidate._refresh_action_state()
        candidate._refresh_evidence_state()
        candidate._reservation_repo = None
        candidate._command_repo = None
        candidate._active_transaction = None
        candidate._process_mutation_lock = self._process_mutation_lock
        candidate._mutation_locks = self._mutation_locks
        return candidate

    def _publish_candidate(
        self,
        candidate: ExecutionBook,
        key: PositionKey | None = None,
    ) -> None:
        """Publish a candidate only after its Postgres transaction committed."""
        canons: tuple[str, ...]
        if key is not None:
            canons = (key.canonical_id,)
        else:
            canons = candidate._transaction_journal_keys or tuple(candidate._journals)

        for canon in canons:
            current_head = self._head_revisions.get(canon, 0)
            cand_head = candidate._head_revisions.get(canon, 0)
            if cand_head < current_head:
                continue
            if canon in candidate._books:
                self._books[canon] = candidate._books[canon]
            current_journal = self._journal_revisions.get(canon, 0)
            cand_journal = candidate._journal_revisions.get(canon, 0)
            if cand_journal >= current_journal and canon in candidate._journals:
                self._journals[canon] = candidate._journals[canon]
            if canon in candidate._stream_scopes:
                self._stream_scopes[canon] = candidate._stream_scopes[canon]
            if canon in candidate._head_revisions:
                self._head_revisions[canon] = candidate._head_revisions[canon]
            if (
                cand_journal >= current_journal
                and canon in candidate._journal_revisions
            ):
                self._journal_revisions[canon] = candidate._journal_revisions[canon]
            if canon in candidate._last_sequences:
                self._last_sequences[canon] = candidate._last_sequences[canon]
            else:
                self._last_sequences.pop(canon, None)
            if canon in candidate._head_expected_reservation_ids:
                self._head_expected_reservation_ids[canon] = (
                    candidate._head_expected_reservation_ids[canon]
                )
            else:
                self._head_expected_reservation_ids.pop(canon, None)

        self._requests_by_id.update(candidate._requests_by_id)
        self._receipts_by_id.update(candidate._receipts_by_id)
        self._seen_evidence_ids.update(candidate._seen_evidence_ids)
        self._seen_trade_ids.update(candidate._seen_trade_ids)
        self._active_streams.update(candidate._active_streams)
        initial_streams = getattr(candidate, "_initial_latest_active_streams", {})
        for acc_k, stream_val in candidate._latest_active_streams.items():
            if stream_val != initial_streams.get(acc_k):
                if key is None or acc_k == (key.environment, key.account_label):
                    self._latest_active_streams[acc_k] = stream_val
        initial_scope = getattr(candidate, "_initial_recovery_adoption_scope", None)
        if candidate._recovery_adoption_scope != initial_scope:
            if key is None or (
                candidate._recovery_adoption_scope is not None
                and candidate._recovery_adoption_scope.symbol == key.symbol
                and (
                    candidate._recovery_adoption_scope.position_side
                    == key.position_side
                )
                and candidate._recovery_adoption_scope.environment == key.environment
                and (
                    candidate._recovery_adoption_scope.account_label
                    == key.account_label
                )
            ):
                self._recovery_adoption_scope = candidate._recovery_adoption_scope
        if key is not None:
            canon = key.canonical_id
            prefix = f"{canon}\x1f"
            for k, v in candidate._order_cumulative_fills.items():
                if k.startswith(prefix):
                    self._order_cumulative_fills[k] = max(
                        self._order_cumulative_fills.get(k, Decimal("0")), v
                    )
            for k, v in candidate._order_cumulative_quotes.items():
                if k.startswith(prefix):
                    self._order_cumulative_quotes[k] = max(
                        self._order_cumulative_quotes.get(k, Decimal("0")), v
                    )
            for cmd_id, entry in candidate._outbox_by_command_id.items():
                if entry.command.position_key.canonical_id == canon:
                    existing = self._outbox_by_command_id.get(cmd_id)
                    if (
                        existing is not None
                        and existing.state
                        in (DispatchState.TERMINAL, DispatchState.REJECTED)
                        and entry.state
                        not in (DispatchState.TERMINAL, DispatchState.REJECTED)
                    ):
                        continue
                    self._outbox_by_command_id[cmd_id] = entry
            for cmd_id, res_list in candidate._command_reservations.items():
                cand_res_entry = candidate._outbox_by_command_id.get(cmd_id)
                if (
                    cand_res_entry is not None
                    and cand_res_entry.command.position_key.canonical_id == canon
                ):
                    existing = self._outbox_by_command_id.get(cmd_id)
                    if (
                        existing is not None
                        and existing.state
                        in (DispatchState.TERMINAL, DispatchState.REJECTED)
                        and cand_res_entry.state
                        not in (DispatchState.TERMINAL, DispatchState.REJECTED)
                    ):
                        continue
                    self._command_reservations[cmd_id] = res_list
            divergence_identity = f"reservation_divergence:{canon}"
            staged_recovery: set[str] = getattr(
                candidate, "_staged_recovery_required_commands", set()
            )
            if divergence_identity in candidate._recovery_required_commands:
                self._recovery_required_commands.add(divergence_identity)
            elif divergence_identity in staged_recovery:
                self._recovery_required_commands.discard(divergence_identity)
            for identity, pkey in candidate._external_recovery_positions.items():
                if pkey.canonical_id == canon:
                    self._external_recovery_positions[identity] = pkey
            staged_ext: dict[str, PositionKey] = getattr(
                candidate, "_staged_external_recovery_positions", {}
            )
            for identity, pkey in list(self._external_recovery_positions.items()):
                if (
                    pkey.canonical_id == canon
                    and identity in staged_ext
                    and identity not in candidate._external_recovery_positions
                ):
                    self._external_recovery_positions.pop(identity, None)
            for cmd_id in candidate._recovery_required_commands:
                cand_rec_entry = candidate._outbox_by_command_id.get(cmd_id)
                if (
                    cand_rec_entry is not None
                    and cand_rec_entry.command.position_key.canonical_id == canon
                ):
                    self._recovery_required_commands.add(cmd_id)
            for cmd_id in list(self._recovery_required_commands):
                self_rec_entry = self._outbox_by_command_id.get(cmd_id)
                if (
                    self_rec_entry is not None
                    and self_rec_entry.command.position_key.canonical_id == canon
                ):
                    if (
                        cmd_id in staged_recovery
                        and cmd_id not in candidate._recovery_required_commands
                    ):
                        self._recovery_required_commands.discard(cmd_id)
            for cmd_id in candidate._dispatch_reconciliation_required_commands:
                cand_disp_entry = candidate._outbox_by_command_id.get(cmd_id)
                if (
                    cand_disp_entry is not None
                    and cand_disp_entry.command.position_key.canonical_id == canon
                ):
                    self._dispatch_reconciliation_required_commands.add(cmd_id)
            staged_reconciliation: set[str] = getattr(
                candidate, "_staged_dispatch_reconciliation_required_commands", set()
            )
            for cmd_id in list(self._dispatch_reconciliation_required_commands):
                self_disp_entry = self._outbox_by_command_id.get(cmd_id)
                if (
                    self_disp_entry is not None
                    and self_disp_entry.command.position_key.canonical_id == canon
                ):
                    if (
                        cmd_id in staged_reconciliation
                        and cmd_id
                        not in candidate._dispatch_reconciliation_required_commands
                    ):
                        self._dispatch_reconciliation_required_commands.discard(
                            cmd_id
                        )
        else:
            for k, v in candidate._order_cumulative_fills.items():
                self._order_cumulative_fills[k] = max(
                    self._order_cumulative_fills.get(k, Decimal("0")), v
                )
            for k, v in candidate._order_cumulative_quotes.items():
                self._order_cumulative_quotes[k] = max(
                    self._order_cumulative_quotes.get(k, Decimal("0")), v
                )
            self._outbox_by_command_id = dict(candidate._outbox_by_command_id)
            self._command_reservations = dict(candidate._command_reservations)
            staged_recovery = getattr(
                candidate, "_staged_recovery_required_commands", set()
            )
            self._recovery_required_commands = (
                (self._recovery_required_commands - staged_recovery)
                | candidate._recovery_required_commands
            )
            staged_ext = getattr(
                candidate, "_staged_external_recovery_positions", {}
            )
            self._external_recovery_positions = {
                k: v
                for k, v in self._external_recovery_positions.items()
                if k not in staged_ext or k in candidate._external_recovery_positions
            }
            self._external_recovery_positions.update(
                candidate._external_recovery_positions
            )
            staged_reconciliation = getattr(
                candidate, "_staged_dispatch_reconciliation_required_commands", set()
            )
            self._dispatch_reconciliation_required_commands = (
                (
                    self._dispatch_reconciliation_required_commands
                    - staged_reconciliation
                )
                | candidate._dispatch_reconciliation_required_commands
            )

        self._coordinator.publish_from(candidate._coordinator, key=key)
        self._command_state = CommandLifecycleState(
            outbox_by_command_id=self._outbox_by_command_id,
            dispatch_reconciliation_required_commands=(
                self._dispatch_reconciliation_required_commands
            ),
            recovery_required_commands=self._recovery_required_commands,
        )
        self._refresh_action_state()

        self._refresh_evidence_state()
        self._context_revision += 1
        # The candidate's append-only delta is now durable, so it must not be
        # re-sent by the next observation. A candidate that rolled back is
        # never published, which keeps its pending events queued for retry.
        for canon in canons:
            journal = self._journals.get(canon)
            if journal is not None:
                journal.mark_facts_persisted()

    def _refresh_evidence_state(self) -> None:
        self._evidence_state = EvidenceState(
            books=self._books,
            journals=self._journals,
            stream_scopes=self._stream_scopes,
            active_streams=self._active_streams,
            head_revisions=self._head_revisions,
            journal_revisions=self._journal_revisions,
            last_sequences=self._last_sequences,
            seen_evidence_ids=self._seen_evidence_ids,
            seen_trade_ids=self._seen_trade_ids,
            outbox_by_command_id=self._outbox_by_command_id,
            command_reservations=self._command_reservations,
            order_cumulative_fills=self._order_cumulative_fills,
            order_cumulative_quotes=self._order_cumulative_quotes,
            recovery_required_commands=self._recovery_required_commands,
            external_recovery_positions=self._external_recovery_positions,
            dispatch_reconciliation_required_commands=(
                self._dispatch_reconciliation_required_commands
            ),
            coordinator=self._coordinator,
        )

    def _refresh_action_state(self) -> None:
        self._action_state = ExecutionActionState(
            requests_by_id=self._requests_by_id,
            receipts_by_id=self._receipts_by_id,
            outbox_by_command_id=self._outbox_by_command_id,
            command_reservations=self._command_reservations,
            recovery_required_commands=self._recovery_required_commands,
            dispatch_reconciliation_required_commands=(
                self._dispatch_reconciliation_required_commands
            ),
        )

    def _journal_for_scope(
        self,
        key: PositionKey,
        scope: AccountFactStreamScope,
    ) -> AccountJournal:
        canon = key.canonical_id
        existing = self._journals.get(canon)
        if existing is not None:
            if existing.stream_scope == scope:
                return existing
            if existing.stream_scope is not None:
                raise RuntimeError(
                    "execution stream changed; a validated recovery checkpoint "
                    "must be adopted before this position can continue"
                )
            facts = existing.read_cut()
            if (
                facts.fills
                or facts.snapshots
                or facts.exit_boundaries
                or facts.coverage is not None
                or facts.fact_conflicts
                or facts.recovery_checkpoint is not None
            ):
                raise RuntimeError(
                    "unscoped position facts cannot be adopted by a durable stream"
                )
        journal = AccountJournal(key, stream_scope=scope)
        self._journals[canon] = journal
        self._books[canon] = PositionBook(journal)
        self._stream_scopes[canon] = scope
        return journal

    async def _persist_outbox_state(self, entry: OutboxEntry) -> None:
        command_repository = self._command_repo
        if command_repository is None and self._active_transaction is None:
            return
        watermark_key = self._order_watermark_key(
            entry.scope.to_position_key(), entry.command.command_id
        )
        details = encode_outbox_details(
            entry,
            reservation_ids=tuple(self._command_reservations.get(entry.command_id, ())),
            cumulative_quantity=self._order_cumulative_fills.get(
                watermark_key, Decimal("0")
            ),
            cumulative_quote=self._order_cumulative_quotes.get(
                watermark_key, Decimal("0")
            ),
        )
        try:
            if self._active_transaction is not None:
                await self._active_transaction.upsert_outbox(
                    command_id=entry.command_id,
                    client_order_id=entry.command.command_id,
                    command=entry.command.command_type.value,
                    status=entry.state.value,
                    requested_at=entry.created_at,
                    details=details,
                )
            elif command_repository is not None:
                await command_repository.upsert_execution_command(
                    command_id=entry.command_id,
                    client_order_id=entry.command.command_id,
                    command=entry.command.command_type.value,
                    status=entry.state.value,
                    requested_at=entry.created_at,
                    details=details,
                )
        except Exception as err:
            self._persistence_failed = True
            log.error(
                "persist_outbox_state_failed",
                command_id=entry.command_id,
                error=str(err),
            )
            raise

    async def restore(
        self,
        account_label: str | None = None,
        *,
        environment: str = "live",
        as_of: datetime | None = None,
    ) -> None:
        """Restore durable execution state before accepting live mutations."""
        await restore_execution_state(
            self.recovery_state,
            RecoveryDependencies(
                command_repository=self._command_repo,
                reservation_repository=self._reservation_repo,
                unit_of_work=self._execution_unit_of_work,
                clear_requests=self._clear_recovery_requests,
                persist_outbox_state=self._persist_outbox_state,
                mark_unknown=self.mark_unknown,
                set_persistence_failed=lambda failed: setattr(
                    self, "_persistence_failed", failed
                ),
                watermark_key=self._order_watermark_key,
            ),
            account_label,
            environment=environment,
            as_of=as_of,
        )

    def _clear_recovery_requests(self) -> None:
        self._requests_by_id.clear()
        self._receipts_by_id.clear()

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
            self._journals[canon] = AccountJournal(
                key,
                stream_scope=self._stream_scopes.get(canon),
            )
        return self._journals[canon]

    def _requires_verified_stream_adoption(self, key: PositionKey) -> bool:
        journal = self._journals.get(key.canonical_id)
        if self._head_revisions.get(key.canonical_id, 0) == 0 or journal is None:
            return False
        facts = journal.read_cut()
        return bool(
            facts.fills
            or facts.recovery_checkpoint
            or facts.exit_boundaries
            or facts.conflicting_fills
            or facts.fact_conflicts
            or facts.integrity_issues
            or facts.has_late_events
            or any(snapshot.position_amt != 0 for snapshot in facts.snapshots)
        )

    @staticmethod
    def _order_watermark_key(key: PositionKey, order_id: str) -> str:
        return f"{key.canonical_id}\x1f{order_id}"

    def _find_active_reservations_for_command(
        self, command_id: str
    ) -> tuple[PositionReservation, ...]:
        res_ids = self._command_reservations.get(command_id, [])
        active: list[PositionReservation] = []
        for r_id in res_ids:
            r = self._coordinator.get_reservation(r_id)
            if r is not None and r.active_quantity > Decimal("0"):
                active.append(r)
        return tuple(active)

    def _command_order_id(self, key: PositionKey, order_id: str) -> str:
        matches = [
            entry.command_id
            for entry in self._outbox_by_command_id.values()
            if entry.scope.to_position_key() == key
            and order_id in (entry.command_id, entry.external_order_id)
        ]
        if len(matches) > 1:
            raise ValueError("exchange order identity belongs to multiple commands")
        return matches[0] if matches else order_id

    def _command_order_ids(self, key: PositionKey, order_id: str) -> frozenset[str]:
        command_id = self._command_order_id(key, order_id)
        entry = self._outbox_by_command_id.get(command_id)
        if entry is not None and entry.external_order_id is not None:
            return frozenset({command_id, entry.external_order_id})
        return frozenset({command_id})

    def get_active_reservations(
        self, key: PositionKey | None = None
    ) -> tuple[PositionReservation, ...]:
        """Returns active reservations tracked by the domain coordinator."""
        return self._coordinator.get_active_reservations(key)

    async def read(
        self,
        scope: ExecutionScope,
        requirement: FreshnessRequirement | None = None,
        now: datetime | None = None,
        *,
        event_cut: datetime | None = None,
        stream_id: str | None = None,
        stream_epoch: str | None = None,
    ) -> PositionView:
        """Read a published position or historical cut without mutation."""
        if (stream_id is None) != (stream_epoch is None):
            raise ValueError("stream_id and stream_epoch must be supplied together")
        if (
            stream_id is not None
            and stream_epoch is not None
            and (not stream_id.strip() or not stream_epoch.strip())
        ):
            raise ValueError("stream_id and stream_epoch must not be empty")
        if event_cut is not None and (
            event_cut.tzinfo is None or event_cut.utcoffset() is None
        ):
            raise ValueError("event_cut must be timezone-aware")
        if (
            self._execution_unit_of_work is not None
            and self._persistence_failed
            and not self._stream_scopes
        ):
            raise RuntimeError("execution facts require successful durable restoration")
        key = PositionKey(
            environment=scope.environment,
            account_label=scope.account_label,
            symbol=scope.symbol,
            position_side=scope.position_side,
        )
        canon = key.canonical_id
        source_scope = self._stream_scopes.get(canon)
        if (
            stream_id is not None
            and stream_epoch is not None
            and (
                source_scope is None
                or source_scope.stream_id != stream_id
                or source_scope.stream_epoch != stream_epoch
            )
        ):
            current_book = self._books.get(canon)
            is_flat = current_book is None or (
                current_book.get_view().total_quantity == Decimal("0")
                and not current_book.get_view().batches
                and not current_book.get_view().unallocated_quantity
            )
            has_no_reservations = not bool(self.get_active_reservations(key))
            has_no_commands = not any(
                cmd.scope.to_position_key() == key
                for cmd in self._outbox_by_command_id.values()
            )
            is_known_active_stream = (
                scope.environment,
                scope.account_label,
                stream_id,
                stream_epoch,
            ) in self._active_streams or any(
                s.account_label == scope.account_label
                and s.environment == scope.environment
                and s.stream_id == stream_id
                and s.stream_epoch == stream_epoch
                for s in self._stream_scopes.values()
            )
            if (
                is_flat
                and has_no_reservations
                and has_no_commands
                and is_known_active_stream
            ):
                if self._requires_verified_stream_adoption(key):
                    raise PositionStreamMismatchError(
                        "requested account stream does not match the restored "
                        "position; durable history requires a verified "
                        "source-anchored scan"
                    )
                target_scope = AccountFactStreamScope.for_position_key(
                    key, stream_id=stream_id, stream_epoch=stream_epoch
                )
                self._stream_scopes[canon] = target_scope
                if (
                    canon not in self._journals
                    or self._journals[canon].stream_scope != target_scope
                ):
                    self._journals[canon] = AccountJournal(
                        key, stream_scope=target_scope
                    )
                    self._books[canon] = PositionBook(self._journals[canon])
                    self._journal_revisions[canon] = 0
                    self._last_sequences.pop(canon, None)
                source_scope = target_scope
            else:
                raise PositionStreamMismatchError(
                    "requested account stream does not match the restored position"
                )
        book = self._books.get(canon)
        if book is None:
            # An unknown position is incomplete; reading it must not create a
            # journal or silently establish an account stream for future writes.
            journal = self._journals.get(canon)
            if journal is not None:
                book = PositionBook(journal)
            else:
                book = PositionBook(AccountJournal(key, stream_scope=source_scope))
        if event_cut is None:
            view = book.get_view(requirement=requirement, now=now)
            entries = self.list_outbox(scope=scope)
            return replace(
                view,
                reservations=self.get_active_reservations(key),
                pending_command_ids=tuple(
                    sorted(
                        entry.command_id
                        for entry in entries
                        if self.command_requires_recovery(entry.command_id)
                    )
                ),
                active_entry_command_ids=tuple(
                    sorted(
                        entry.command_id
                        for entry in entries
                        if entry.command.command_type == TradeCommandType.ENTRY
                        and entry.state
                        not in {DispatchState.REJECTED, DispatchState.TERMINAL}
                    )
                ),
            )
        current_view = book.get_view(requirement=requirement, now=now)
        if (
            self._execution_unit_of_work is not None
            and source_scope is not None
            and current_view.event_cut is not None
            and event_cut < current_view.event_cut
        ):
            cut = await self._execution_unit_of_work.load_journal_cut(
                scope=source_scope, as_of=event_cut
            )
            return book.get_historical_view(
                cut, event_cut=event_cut, requirement=requirement, now=now
            )
        return book.get_view(cut=event_cut, requirement=requirement, now=now)

    async def load_recovery_checkpoint(
        self,
        scope: AccountFactStreamScope,
        *,
        as_of: datetime | None = None,
    ) -> PositionRecoveryCheckpoint | None:
        """Load the latest verified checkpoint for one exact source stream.

        Runtime stream adoption uses this as an immutable parent anchor. The
        method never relabels a checkpoint to the caller's new epoch.
        """
        return await read_recovery_checkpoint(
            journals=self._journals,
            unit_of_work=self._execution_unit_of_work,
            scope=scope,
            as_of=as_of,
        )

    async def list_position_views(
        self,
        *,
        environment: str,
        account_label: str,
        event_cut: datetime | None = None,
        stream_id: str | None = None,
        stream_epoch: str | None = None,
        symbols: frozenset[str] | None = None,
    ) -> tuple[PositionView, ...]:
        return await read_position_views(
            books=self._books,
            stream_scopes=self._stream_scopes,
            read=self.read,
            durable_restore_failed=(
                self._execution_unit_of_work is not None and self._persistence_failed
            ),
            environment=environment,
            account_label=account_label,
            event_cut=event_cut,
            stream_id=stream_id,
            stream_epoch=stream_epoch,
            symbols=symbols,
        )

    async def act(
        self,
        request: ExecutionRequest,
        *,
        prepare_submission: Callable[
            [ExecutionTransactionPort | None], Awaitable[PreparedOrderSubmission]
        ]
        | None = None,
    ) -> ExecutionActResult:
        return await accept_action_transaction(
            self, request, prepare_submission=prepare_submission
        )

    async def _act_mutating(
        self,
        request: ExecutionRequest,
    ) -> ExecutionActResult:
        """Delegate command rules; this facade owns only transaction orchestration."""
        return await accept_execution_command(
            self._action_state,
            ExecutionActionDependencies(
                book_for_key=self._ensure_book,
                coordinator=self._coordinator,
                reservation_repository=self._reservation_repo,
                persistence_failed=lambda: self._persistence_failed,
                mark_persistence_failed=lambda: setattr(
                    self, "_persistence_failed", True
                ),
                persist_outbox=self._persist_outbox_state,
                advance_context_revision=lambda: setattr(
                    self, "_context_revision", self._context_revision + 1
                ),
            ),
            request,
            stream_scopes_present=bool(self._stream_scopes),
        )

    def register_prepared_command(
        self,
        command: TradeCommand,
        scope: ExecutionScope,
        reservation_ids: list[str] | tuple[str, ...] = (),
    ) -> OutboxEntry:
        """Registers a prepared command into the outbox and links reservations."""
        entry = register_prepared_outbox(
            self._action_state,
            request_id=command.command_id,
            scope=scope,
            command=command,
            created_at=command.created_at,
            reservation_ids=tuple(reservation_ids),
        )
        self._context_revision += 1
        return entry

    def get_outbox(self, command_id: str) -> OutboxEntry | None:
        """Returns the outbox record for command_id if found."""
        return self._outbox_by_command_id.get(command_id)

    def require_command_recovery(self, command_id: str) -> None:
        """Keep settlement work recoverable without rewriting the order fact."""
        if not command_id.strip():
            raise ValueError("recovery command identity must not be empty")
        self._recovery_required_commands.add(command_id)

    def command_requires_recovery(self, command_id: str) -> bool:
        """Expose the command's dispatch/settlement gate to its repair owner."""
        return (
            command_id in self._recovery_required_commands
            or command_id in self._dispatch_reconciliation_required_commands
        )

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

    async def _durable_command_mutation(
        self,
        command_id: str,
        target: DispatchState,
        *,
        unit_of_work: ExecutionUnitOfWorkPort,
        at: datetime | None = None,
        external_order_id: str | None = None,
        reason: str = "",
        intent_id: str | None = None,
    ) -> OutboxEntry:
        entry = self._outbox_by_command_id.get(command_id)
        if entry is None:
            raise KeyError(f"Outbox entry {command_id} not found")
        key = entry.scope.to_position_key()
        canon = key.canonical_id
        async with self._mutation_lock(key):
            stream_scope = self._stream_scopes.get(canon)
            if stream_scope is None:
                raise RuntimeError("Execution command has no durable stream scope")
            candidate = self._staged_copy(key=key)
            base_head_revision = self._head_revisions.get(canon, 0)

        # Phase 2: Lock-free durable I/O and CAS commit (I2)
        try:
            async with unit_of_work.transaction(key) as tx:
                head = await tx.load_head(key)
                # Legacy journals can exist before the first Book head.
                # Revision zero is valid only after that journal was restored;
                # persist_head still performs the first-writer CAS in this tx.
                expected_head_revision = head.revision if head is not None else 0
                if base_head_revision != expected_head_revision:
                    raise RuntimeError(
                        "durable execution head changed; restore is required"
                    )
                if head is not None and (
                    head.stream_id != stream_scope.stream_id
                    or head.stream_epoch != stream_scope.stream_epoch
                ):
                    raise RuntimeError(
                        "durable execution stream changed; restore is required"
                    )
                candidate._active_transaction = tx
                result = await candidate._apply_command_transition(
                    command_id,
                    target,
                    at=at,
                    external_order_id=external_order_id,
                    reason=reason,
                )
                if target == DispatchState.REJECTED and hasattr(
                    tx, "mark_order_rejected"
                ):
                    rejected = await tx.mark_order_rejected(
                        command_id,
                        reason=reason,
                        occurred_at=at,
                        intent_id=intent_id or entry.intent_id,
                    )
                    if rejected is False:
                        # The order ledger already holds a terminal (or absent)
                        # order for this identity. Publishing a rejected command
                        # would contradict it and release exposure that may be
                        # live, so refuse the transition and let the caller keep
                        # the command unresolved for recovery.
                        raise OrderRejectionRefusedError(
                            f"durable order {command_id} is already terminal; "
                            "command rejection refused"
                        )
                facts = candidate._ensure_journal(key).read_cut()
                head_payload = build_execution_head_payload(
                    candidate.evidence_state,
                    key,
                    facts.compute_facts_hash(),
                    ensure_book=candidate._ensure_book,
                    ensure_journal=candidate._ensure_journal,
                    active_reservations=candidate.get_active_reservations,
                )
                candidate._head_revisions[canon] = await tx.persist_head(
                    key=key,
                    stream_id=stream_scope.stream_id,
                    stream_epoch=stream_scope.stream_epoch,
                    expected_revision=expected_head_revision,
                    projection_version=candidate._ensure_book(key)
                    .get_view()
                    .projection_version,
                    state_payload=head_payload,
                    updated_at=datetime.now(UTC),
                )
            candidate._active_transaction = None

            # Phase 3: Short lock for candidate publication & validation
            async with self._mutation_lock(key):
                if self._head_revisions.get(canon, 0) != base_head_revision:
                    raise RuntimeError(
                        "durable execution head changed concurrently; restore required"
                    )
                self._publish_candidate(candidate, key=key)
            return result
        except Exception:
            self._persistence_failed = True
            raise

    async def mark_dispatching(
        self, command_id: str, dispatched_at: datetime | None = None
    ) -> OutboxEntry:
        return await self._transition_command(
            command_id, DispatchState.DISPATCHING, at=dispatched_at
        )

    async def mark_acknowledged(
        self,
        command_id: str,
        external_order_id: str,
        acknowledged_at: datetime | None = None,
    ) -> OutboxEntry:
        return await self._transition_command(
            command_id,
            DispatchState.ACKNOWLEDGED,
            external_order_id=external_order_id,
            at=acknowledged_at,
        )

    async def mark_unknown(
        self,
        command_id: str,
        reason: str,
        unknown_at: datetime | None = None,
    ) -> OutboxEntry:
        return await self._transition_command(
            command_id,
            DispatchState.UNKNOWN,
            reason=reason,
            at=unknown_at,
        )

    async def mark_rejected(
        self,
        command_id: str,
        reason: str,
        rejected_at: datetime | None = None,
        intent_id: str | None = None,
    ) -> OutboxEntry:
        return await self._transition_command(
            command_id,
            DispatchState.REJECTED,
            reason=reason,
            at=rejected_at,
            intent_id=intent_id,
        )

    async def mark_terminal(
        self,
        command_id: str,
        reason: str = "",
        terminal_at: datetime | None = None,
    ) -> OutboxEntry:
        return await self._transition_command(
            command_id,
            DispatchState.TERMINAL,
            reason=reason,
            at=terminal_at,
        )

    async def _transition_command(
        self,
        command_id: str,
        target: DispatchState,
        *,
        at: datetime | None = None,
        external_order_id: str | None = None,
        reason: str = "",
        intent_id: str | None = None,
    ) -> OutboxEntry:
        if self._execution_unit_of_work is not None:
            return await self._durable_command_mutation(
                command_id,
                target,
                unit_of_work=self._execution_unit_of_work,
                at=at,
                external_order_id=external_order_id,
                reason=reason,
                intent_id=intent_id,
            )
        return await self._apply_command_transition(
            command_id,
            target,
            at=at,
            external_order_id=external_order_id,
            reason=reason,
        )

    async def _apply_command_transition(
        self,
        command_id: str,
        target: DispatchState,
        *,
        at: datetime | None = None,
        external_order_id: str | None = None,
        reason: str = "",
    ) -> OutboxEntry:
        return await apply_command_transition(
            self._command_state,
            command_id,
            target,
            at=at,
            external_order_id=external_order_id,
            reason=reason,
            persist_transition=self._persist_transition,
            release_reservations=lambda identity, release_reason: (
                self._release_command_reservations(identity, reason=release_reason)
            ),
            seal_persistence=self._seal_persistence,
        )

    def _seal_persistence(self) -> None:
        self._persistence_failed = True

    def _command_persistence_dependencies(self) -> CommandPersistenceDependencies:
        return CommandPersistenceDependencies(
            persist_outbox=self._persist_outbox_state,
            persist_reservation=lambda reservation, reason: (
                self._persist_reservation_update(reservation, release_reason=reason)
            ),
            active_reservations_for_command=(
                self._find_active_reservations_for_command
            ),
            order_watermark_key=self._order_watermark_key,
            advance_context_revision=lambda: setattr(
                self, "_context_revision", self._context_revision + 1
            ),
            external_recovery_positions=self._external_recovery_positions,
        )

    async def _persist_transition(
        self,
        updated: OutboxEntry,
    ) -> None:
        await persist_command_transition(
            self._command_state,
            self._command_persistence_dependencies(),
            updated,
        )

    async def _release_command_reservations(
        self,
        command_id: str,
        *,
        reason: str,
    ) -> Decimal:
        return await release_command_reservations(
            self._command_persistence_dependencies(), command_id, reason=reason
        )

    async def _persist_reservation_update(
        self,
        reservation: PositionReservation,
        *,
        release_reason: str | None = None,
    ) -> None:
        if self._active_transaction is not None:
            await self._active_transaction.update_reservation(
                reservation,
                release_reason=release_reason,
            )
        elif self._reservation_repo is not None:
            try:
                await self._reservation_repo.update_reservation(
                    reservation,
                    release_reason=release_reason,
                )
            except Exception:
                self._persistence_failed = True
                self._recovery_required_commands.add(reservation.command_id)
                raise
        self._coordinator.update_reservation(reservation)

    async def _settle_reservation_quantity(
        self,
        order_id: str,
        quantity: Decimal,
        *,
        reported_quantity: Decimal,
        key: PositionKey,
    ) -> tuple[Decimal, bool, str | None]:
        return await settle_command_reservations(
            self._command_state,
            self._command_persistence_dependencies(),
            order_id,
            quantity,
            reported_quantity=reported_quantity,
            key=key,
        )

    async def observe(self, evidence: ExecutionEvidence) -> ExecutionObserveResult:
        return await observe_evidence_transaction(self, evidence)

    async def _observe_mutating(
        self,
        evidence: ExecutionEvidence,
    ) -> ExecutionObserveResult:
        """Delegate evidence rules; this facade owns transaction and publication."""
        return await mutate_evidence(
            self._evidence_state,
            EvidenceMutationDependencies(
                ensure_book=self._ensure_book,
                ensure_journal=self._ensure_journal,
                journal_for_scope=self._journal_for_scope,
                command_order_id=self._command_order_id,
                command_order_ids=self._command_order_ids,
                order_watermark_key=self._order_watermark_key,
                active_reservations_for_command=(
                    self._find_active_reservations_for_command
                ),
                active_reservations=self.get_active_reservations,
                settle_reservation_quantity=self._settle_reservation_quantity,
                persist_transition=self._persist_transition,
                release_command_reservations=self._release_command_reservations,
                persist_outbox_state=self._persist_outbox_state,
                durable=self._execution_unit_of_work is not None,
                in_transaction=self._active_transaction is not None,
            ),
            evidence,
        )


__all__ = [
    "Accepted",
    "AlreadyAccepted",
    "Blocked",
    "CommandConflict",
    "DispatchState",
    "Duplicate",
    "EvidenceConflict",
    "WaitingForEvidence",
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
