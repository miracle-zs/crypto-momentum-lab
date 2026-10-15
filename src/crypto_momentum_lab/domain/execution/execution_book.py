from __future__ import annotations

import asyncio
import copy
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import TypeVar, cast

import structlog

from crypto_momentum_lab.domain.execution.command_codec import encode_outbox_details
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionEvidence,
)
from crypto_momentum_lab.domain.execution.exchange_contract import (
    ExchangeOrderRejectedError,
    LiveSubmissionDisabledError,
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
    register_prepared_outbox,
)
from crypto_momentum_lab.domain.execution.execution_action_transaction import (
    accept_action_transaction,
)
from crypto_momentum_lab.domain.execution.execution_book_commands import (
    CommandPersistenceDependencies,
    apply_command_transition,
    persist_command_transition,
    release_command_reservations,
    settle_command_reservations,
)
from crypto_momentum_lab.domain.execution.execution_book_recovery import (
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
    OrderAlreadyPreparedError,
    OrderCommandConflictError,
    OrderPreSubmissionError,
    PreparedOrderSubmission,
    blocks_local_rejection,
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

T = TypeVar("T")


class ExecutionBook:
    """Authoritative account service for current positions and reservations.

    Implements Section 7 of RFC 2026-09-25:
    - read(scope, requirement) -> PositionView
    - act(request) -> Accepted | AlreadyAccepted | StaleView | Blocked | CommandConflict
    - observe(evidence) -> Applied | Duplicate | WaitingForEvidence | EvidenceConflict
    """

    def __init__(
        self,
        *,
        books_by_key: dict[str, PositionBook] | None = None,
        coordinator: ReservationRegistry | None = None,
        reservation_repository: ReservationRepository | None = None,
        command_repository: CommandRepository | None = None,
        execution_unit_of_work: ExecutionUnitOfWorkPort | None = None,
    ) -> None:
        self._books: dict[str, PositionBook] = books_by_key or {}
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
        self._last_sequences: dict[str, int] = {}
        self._recovery_adoption_scope: AccountFactStreamScope | None = None
        self._process_mutation_lock = asyncio.Lock()
        self._mutation_locks: dict[str, asyncio.Lock] = {}
        self._position_write_locks: dict[str, asyncio.Lock] = {}
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
        self._transaction_position_keys: tuple[str, ...] = ()
        self._initial_latest_active_streams: dict[tuple[str, str], tuple[str, str]] = {}
        self._initial_recovery_adoption_scope: AccountFactStreamScope | None = None
        self._staged_recovery_required_commands: set[str] = set()
        self._staged_dispatch_reconciliation_required_commands: set[str] = set()
        self._staged_external_recovery_positions: dict[str, PositionKey] = {}
        self._context_revision = 0

    @property
    def context_revision(self) -> int:
        """Published facts/commands generation used by operational read caches."""
        return self._context_revision

    def position_keys_for_account(
        self, environment: str, account_label: str
    ) -> tuple[PositionKey, ...]:
        """Return restored position keys for one account."""
        return tuple(
            sorted(
                (
                    position_book.position_key
                    for position_book in self._books.values()
                    if position_book.position_key.environment == environment
                    and position_book.position_key.account_label == account_label
                ),
                key=lambda key: key.canonical_id,
            )
        )

    @property
    def coordinator(self) -> ReservationRegistry:
        return self._coordinator

    @property
    def recovery_adoption_scope(self) -> AccountFactStreamScope | None:
        return self._recovery_adoption_scope

    @recovery_adoption_scope.setter
    def recovery_adoption_scope(self, value: AccountFactStreamScope | None) -> None:
        self._recovery_adoption_scope = value

    def clear(self, *, clear_requests: Callable[[], None] | None = None) -> None:
        for values in (
            self._books,
            self._stream_scopes,
            self._head_revisions,
            self._head_expected_reservation_ids,
            self._last_sequences,
            self._seen_evidence_ids,
            self._seen_trade_ids,
            self._order_cumulative_fills,
            self._order_cumulative_quotes,
            self._outbox_by_command_id,
            self._command_reservations,
            self._recovery_required_commands,
            self._external_recovery_positions,
            self._dispatch_reconciliation_required_commands,
        ):
            values.clear()
        if clear_requests is not None:
            clear_requests()
        self._coordinator.clear_reservations()

    def register_active_stream(
        self,
        evidence: ExecutionEvidence | None = None,
        *,
        environment: str | None = None,
        account_label: str | None = None,
        stream_id: str | None = None,
        stream_epoch: str | None = None,
    ) -> None:
        """Register an authoritative account fact stream as active for this book."""
        if evidence is not None:
            if evidence.stream_id and evidence.stream_epoch:
                self._active_streams.add(
                    (
                        evidence.scope.environment,
                        evidence.scope.account_label,
                        evidence.stream_id,
                        evidence.stream_epoch,
                    )
                )
            return
        if not stream_id or not stream_epoch:
            return
        assert environment is not None and account_label is not None
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
        async with self._position_write_lock(request.key):
            return await self._repair_position_locked(
                request,
                uow=uow,
                is_current=is_current,
            )

    async def _repair_position_locked(
        self,
        request: PositionRepairRequest,
        *,
        uow: PositionRepairUnitOfWork,
        is_current: Callable[[], bool] | None = None,
    ) -> PublishedPositionRepair:
        """Serialize fact load, durable repair commit and publication with observe.

        The caller holds this position's write lock until the durable head has
        been committed and reloaded into the in-memory projection.
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
                        existing_book.get_view() if existing_book is not None else None
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
                self,
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
        """Short lock for publishing process-local state in this Book.

        Durable writers use ``_position_write_lock`` to prevent stale staging;
        this lock still protects brief in-memory reads and publications.
        If key is None, falls back to the instance-level process lock.
        """
        if key is None:
            return self._process_mutation_lock
        canon = key.canonical_id
        if canon not in self._mutation_locks:
            self._mutation_locks[canon] = asyncio.Lock()
        return self._mutation_locks[canon]

    def _position_write_lock(self, key: PositionKey) -> asyncio.Lock:
        """Serialize one position's durable write from staging through publish."""
        canon = key.canonical_id
        if canon not in self._position_write_locks:
            self._position_write_locks[canon] = asyncio.Lock()
        return self._position_write_locks[canon]

    def _staged_copy(self, key: PositionKey | None = None) -> ExecutionBook:
        """Stage the state a durable transaction is allowed to change."""
        candidate = copy.copy(self)
        if key is not None:
            canon = key.canonical_id
            candidate._books = {}
            position_book = self._books.get(canon)
            if position_book is not None:
                candidate._books[canon] = position_book.copy_for_transaction()
            candidate._stream_scopes = (
                {canon: self._stream_scopes[canon]}
                if canon in self._stream_scopes
                else {}
            )
            candidate._head_revisions = (
                {canon: self._head_revisions[canon]}
                if canon in self._head_revisions
                else {}
            )
            candidate._head_expected_reservation_ids = (
                {canon: set(self._head_expected_reservation_ids[canon])}
                if canon in self._head_expected_reservation_ids
                else {}
            )
            candidate._last_sequences = (
                {canon: self._last_sequences[canon]}
                if canon in self._last_sequences
                else {}
            )
        else:
            candidate._books = {
                canon: book.copy_for_transaction()
                for canon, book in self._books.items()
            }
            candidate._stream_scopes = dict(self._stream_scopes)
            candidate._head_revisions = dict(self._head_revisions)
            candidate._head_expected_reservation_ids = dict(
                self._head_expected_reservation_ids
            )
            candidate._last_sequences = dict(self._last_sequences)
        candidate._transaction_position_keys = (
            (key.canonical_id,) if key is not None else tuple(candidate._books)
        )
        candidate._requests_by_id = (
            dict(self._requests_by_id) if key is None else self._requests_by_id
        )
        candidate._receipts_by_id = (
            dict(self._receipts_by_id) if key is None else self._receipts_by_id
        )
        candidate._seen_evidence_ids = (
            set() if key is not None else set(self._seen_evidence_ids)
        )
        candidate._seen_trade_ids = (
            set() if key is not None else set(self._seen_trade_ids)
        )
        candidate._outbox_by_command_id = dict(self._outbox_by_command_id)
        candidate._command_reservations = dict(self._command_reservations)
        candidate._order_cumulative_fills = dict(self._order_cumulative_fills)
        candidate._order_cumulative_quotes = dict(self._order_cumulative_quotes)
        candidate._recovery_required_commands = set(self._recovery_required_commands)
        candidate._external_recovery_positions = dict(self._external_recovery_positions)
        candidate._dispatch_reconciliation_required_commands = set(
            self._dispatch_reconciliation_required_commands
        )
        candidate._active_streams = (
            set(self._active_streams) if key is None else self._active_streams
        )
        candidate._latest_active_streams = (
            dict(self._latest_active_streams)
            if key is None
            else self._latest_active_streams
        )
        candidate._recovery_adoption_scope = self._recovery_adoption_scope
        candidate._initial_latest_active_streams = dict(
            candidate._latest_active_streams
        )
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
        candidate._reservation_repo = None
        candidate._command_repo = None
        candidate._active_transaction = None
        candidate._process_mutation_lock = self._process_mutation_lock
        candidate._mutation_locks = self._mutation_locks
        candidate._position_write_locks = self._position_write_locks
        return candidate

    async def _publish_committed_candidate(
        self,
        candidate: ExecutionBook,
        *,
        key: PositionKey,
        after: Callable[[], T] | None = None,
    ) -> T | None:
        """Publish one committed candidate under the short per-key lock.

        Every durable transaction flow (command mutation, acceptance, evidence)
        publishes through this single owner so the monotonic merge and the
        terminal-winner rule cannot drift between callers. ``after`` runs inside
        the same critical section for callers that must read back what they
        just published.
        """
        async with self._mutation_lock(key):
            self._publish_candidate(candidate, key=key)
            return None if after is None else after()

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
            canons = candidate._transaction_position_keys or tuple(candidate._books)

        for canon in canons:
            current_head = self._head_revisions.get(canon, 0)
            cand_head = candidate._head_revisions.get(canon, 0)
            if cand_head < current_head:
                continue
            if canon in candidate._books:
                self._books[canon] = candidate._books[canon]
            if canon in candidate._stream_scopes:
                self._stream_scopes[canon] = candidate._stream_scopes[canon]
            if canon in candidate._head_revisions:
                self._head_revisions[canon] = candidate._head_revisions[canon]
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

        if candidate._requests_by_id is not self._requests_by_id:
            self._requests_by_id.update(candidate._requests_by_id)
        if candidate._receipts_by_id is not self._receipts_by_id:
            self._receipts_by_id.update(candidate._receipts_by_id)
        if key is None or self._execution_unit_of_work is None:
            self._seen_evidence_ids.update(candidate._seen_evidence_ids)
            self._seen_trade_ids.update(candidate._seen_trade_ids)
        if candidate._active_streams is not self._active_streams:
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
                        self._dispatch_reconciliation_required_commands.discard(cmd_id)
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
                self._recovery_required_commands - staged_recovery
            ) | candidate._recovery_required_commands
            staged_ext = getattr(candidate, "_staged_external_recovery_positions", {})
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
                self._dispatch_reconciliation_required_commands - staged_reconciliation
            ) | candidate._dispatch_reconciliation_required_commands

        self._coordinator.publish_from(candidate._coordinator, key=key)
        self._context_revision += 1

    def _book_for_scope(
        self,
        key: PositionKey,
        scope: AccountFactStreamScope,
    ) -> PositionBook:
        canon = key.canonical_id
        existing = self._books.get(canon)
        if existing is not None:
            if existing.stream_scope == scope:
                return existing
            if existing.stream_scope is not None:
                raise RuntimeError(
                    "execution stream changed; a validated recovery checkpoint "
                    "must be adopted before this position can continue"
                )
            facts = existing.current_facts
            if (
                existing.projection.high_watermark_trade_at is not None
                or facts.coverage is not None
                or facts.fact_conflicts
                or facts.recovery_checkpoint is not None
                or any(snapshot.position_amt != 0 for snapshot in facts.snapshots)
            ):
                raise RuntimeError(
                    "unscoped position facts cannot be adopted by a durable stream"
                )
            existing.reset_stream(scope)
            position_book = existing
        else:
            position_book = PositionBook(key, stream_scope=scope)
            self._books[canon] = position_book
        self._stream_scopes[canon] = scope
        return position_book

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
        """Restore durable execution state before accepting live mutations.

        A durable unit of work is required: an in-memory book has no durable
        facts to restore, and a silent in-memory fallback would let a
        misconfigured process trade without its recovery evidence.
        """
        await restore_execution_state(
            self,
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
            self._books[canon] = PositionBook(
                key, stream_scope=self._stream_scopes.get(canon)
            )
        return self._books[canon]

    def _requires_verified_stream_adoption(self, key: PositionKey) -> bool:
        canon = key.canonical_id
        position_book = self._books.get(canon)
        if self._head_revisions.get(canon, 0) == 0 or position_book is None:
            return False
        facts = position_book.current_facts
        return bool(
            position_book.projection.high_watermark_trade_at is not None
            or facts.recovery_checkpoint
            or facts.coverage
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
            current_view = current_book.get_view() if current_book is not None else None
            is_flat = current_view is None or (
                current_view.total_quantity == Decimal("0")
                and not current_view.batches
                and not current_view.unallocated_quantity
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
                if canon not in self._books:
                    self._books[canon] = PositionBook(key, stream_scope=target_scope)
                else:
                    self._books[canon].reset_stream(target_scope)
                    self._last_sequences.pop(canon, None)
                source_scope = target_scope
            else:
                raise PositionStreamMismatchError(
                    "requested account stream does not match the restored position"
                )
        book = self._books.get(canon)
        if book is None:
            book = PositionBook(key, stream_scope=source_scope)
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
            books=self._books,
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
        key = request.scope.to_position_key()
        if self._execution_unit_of_work is None:
            return await accept_action_transaction(
                self, request, prepare_submission=prepare_submission
            )
        async with self._position_write_lock(key):
            return await accept_action_transaction(
                self, request, prepare_submission=prepare_submission
            )

    async def _act_mutating(
        self,
        request: ExecutionRequest,
    ) -> ExecutionActResult:
        """Delegate command rules; this facade owns only transaction orchestration."""
        return await accept_execution_command(
            self,
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
            self,
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

    def flag_projection_divergence(self, command_id: str, *, reason: str = "") -> None:
        """Domain API: record that command projection diverged from external state
        and requires recovery.
        """
        self.require_command_recovery(command_id)

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
        async with self._position_write_lock(entry.scope.to_position_key()):
            return await self._commit_durable_command_mutation(
                command_id,
                target,
                unit_of_work=unit_of_work,
                at=at,
                external_order_id=external_order_id,
                reason=reason,
                intent_id=intent_id,
            )

    async def _commit_durable_command_mutation(
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
        result: OutboxEntry = entry
        async with self._mutation_lock(key):
            stream_scope = self._stream_scopes.get(canon)
            if stream_scope is None:
                raise RuntimeError("Execution command has no durable stream scope")
            candidate = self._staged_copy(key=key)
            base_head_revision = self._head_revisions.get(canon, 0)

        # The caller holds this position's write lock through the database
        # transaction and publication; no exchange request runs in this path.
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
                rejection_refused = False
                if target == DispatchState.REJECTED and hasattr(
                    tx, "mark_order_rejected"
                ):
                    # Ask the durable order ledger first: a committed terminal
                    # order (or one bound to another intent) must not be
                    # contradicted by a local rejection, and the mutation must
                    # not blow up the caller for that recoverable condition.
                    outcome = await tx.mark_order_rejected(
                        command_id,
                        reason=reason,
                        occurred_at=at,
                        intent_id=intent_id or entry.intent_id,
                    )
                    if blocks_local_rejection(outcome):
                        rejection_refused = True
                        candidate.require_command_recovery(command_id)
                        log.warning(
                            "order_rejection_refused_keep_recovery",
                            command_id=command_id,
                            position_key=key.canonical_id,
                            outcome=outcome.value,
                            reason=reason,
                        )
                if not rejection_refused:
                    result = await candidate._apply_command_transition(
                        command_id,
                        target,
                        at=at,
                        external_order_id=external_order_id,
                        reason=reason,
                    )
                head_payload = build_execution_head_payload(
                    candidate,
                    key,
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

            # Phase 3: Short lock for candidate publication & validation.
            # The transaction has committed. A newer publication is not a
            # rollback: merge monotonically and preserve any terminal winner.
            return cast(
                OutboxEntry,
                await self._publish_committed_candidate(
                    candidate,
                    key=key,
                    after=lambda: self._outbox_by_command_id.get(command_id, result),
                ),
            )
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

    async def record_dispatch(
        self, command_id: str, *, at: datetime | None = None
    ) -> OutboxEntry:
        """Domain API: record that an accepted command is entering network dispatch."""
        return await self._transition_command(
            command_id, DispatchState.DISPATCHING, at=at
        )

    async def abort_prepared_command(
        self,
        command_id: str,
        *,
        reason: str = "cancelled_before_submission",
        intent_id: str | None = None,
        at: datetime | None = None,
    ) -> OutboxEntry:
        """Domain API: abort a prepared command before network dispatch,
        releasing reservations.
        """
        return await self._transition_command(
            command_id,
            DispatchState.REJECTED,
            reason=reason,
            intent_id=intent_id,
            at=at,
        )

    async def compensate_submission_failure(
        self,
        command_id: str,
        error: BaseException,
        *,
        before_exchange_post: bool,
        intent_id: str | None = None,
        occurred_at: datetime | None = None,
        submission_repository: object | None = None,
    ) -> None:
        """Compensate a submission failure within the engine boundary."""

        def _is_identity_collision(err: BaseException | None) -> bool:
            if err is None:
                return False
            if isinstance(err, (OrderAlreadyPreparedError, OrderCommandConflictError)):
                return True
            if "client order ID is already bound" in str(err):
                return True
            cause = getattr(err, "__cause__", None)
            return bool(cause and _is_identity_collision(cause))

        try:
            if _is_identity_collision(error):
                return

            if self.get_outbox(command_id) is None:
                if self.has_command_repository or self.has_reservation_repository:
                    try:
                        await self.mark_rejected(
                            command_id,
                            reason=str(error),
                            rejected_at=occurred_at,
                            intent_id=intent_id,
                        )
                    except Exception:
                        pass
                if before_exchange_post and submission_repository is not None:
                    if hasattr(submission_repository, "mark_submission_rejected"):
                        try:
                            await submission_repository.mark_submission_rejected(
                                command_id,
                                reason=str(error),
                                occurred_at=occurred_at,
                                intent_id=intent_id,
                            )
                        except Exception:
                            pass
                return

            if before_exchange_post or isinstance(
                error,
                (
                    OrderAlreadyPreparedError,
                    OrderPreSubmissionError,
                    ExchangeOrderRejectedError,
                    LiveSubmissionDisabledError,
                    ValueError,
                ),
            ):
                await self.mark_rejected(
                    command_id,
                    reason=str(error),
                    rejected_at=occurred_at,
                    intent_id=intent_id,
                )
                if before_exchange_post and submission_repository is not None:
                    if hasattr(submission_repository, "mark_submission_rejected"):
                        try:
                            await submission_repository.mark_submission_rejected(
                                command_id,
                                reason=str(error),
                                occurred_at=occurred_at,
                                intent_id=intent_id,
                            )
                        except Exception:
                            pass
                return

            await self.mark_unknown(
                command_id,
                reason=str(error) or "submission outcome unknown",
                unknown_at=occurred_at,
            )
        except Exception as compensation_err:
            try:
                self.require_command_recovery(command_id)
            except Exception:
                pass
            raise compensation_err

    async def record_indeterminate_observation(
        self,
        command_id: str,
        *,
        error: BaseException,
        occurred_at: datetime | None = None,
    ) -> None:
        """Domain API: handle observation persistence failure by marking UNKNOWN."""
        if self.get_outbox(command_id) is not None:
            try:
                await self.mark_unknown(
                    command_id,
                    reason=f"exchange result could not be persisted: {error}",
                    unknown_at=occurred_at,
                )
            except Exception as transition_err:
                raise RuntimeError(
                    "exchange returned a result, fact persistence failed, and "
                    "the UNKNOWN outbox transition also failed: "
                    f"{transition_err}"
                ) from transition_err

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
            self,
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
            self,
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
            self,
            self._command_persistence_dependencies(),
            order_id,
            quantity,
            reported_quantity=reported_quantity,
            key=key,
        )

    async def observe(self, evidence: ExecutionEvidence) -> ExecutionObserveResult:
        try:
            if self._execution_unit_of_work is None:
                result = await observe_evidence_transaction(self, evidence)
            else:
                key = evidence.scope.to_position_key()
                async with self._position_write_lock(key):
                    result = await observe_evidence_transaction(self, evidence)
        except Exception as err:
            if (
                evidence.order_event is not None
                and self.get_outbox(evidence.order_event.client_order_id) is not None
            ):
                try:
                    await self.mark_unknown(
                        evidence.order_event.client_order_id,
                        reason=f"exchange result could not be persisted: {err}",
                        unknown_at=evidence.observed_at,
                    )
                except Exception as transition_err:
                    raise RuntimeError(
                        "exchange returned a result, fact persistence failed, and "
                        "the UNKNOWN outbox transition also failed: "
                        f"{transition_err}"
                    ) from transition_err
            raise
        if (
            isinstance(result, WaitingForEvidence)
            and evidence.order_event is not None
            and evidence.order_event.state.terminal
        ):
            self.require_command_recovery(evidence.order_event.client_order_id)
        return result

    async def _observe_mutating(
        self,
        evidence: ExecutionEvidence,
    ) -> ExecutionObserveResult:
        """Delegate evidence rules; this facade owns transaction and publication."""
        return await mutate_evidence(
            self,
            EvidenceMutationDependencies(
                ensure_book=self._ensure_book,
                book_for_scope=self._book_for_scope,
                command_order_id=self._command_order_id,
                order_watermark_key=self._order_watermark_key,
                active_reservations_for_command=(
                    self._find_active_reservations_for_command
                ),
                active_reservations=self.get_active_reservations,
                settle_reservation_quantity=self._settle_reservation_quantity,
                persist_transition=self._persist_transition,
                release_command_reservations=self._release_command_reservations,
                persist_outbox_state=self._persist_outbox_state,
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
