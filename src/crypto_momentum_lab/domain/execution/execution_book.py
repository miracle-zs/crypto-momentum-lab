from __future__ import annotations

import asyncio
import copy
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal

import structlog

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.account_journal import (
    AccountJournal,
)
from crypto_momentum_lab.domain.execution.command_codec import (
    SkippedCommand,
    decode_active_command,
    decode_order_watermark,
    encode_outbox_details,
)
from crypto_momentum_lab.domain.execution.command_lifecycle import (
    plan_command_transition,
    plan_reservation_release,
    plan_reservation_settlement,
)
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.command_repository import CommandRepository
from crypto_momentum_lab.domain.execution.evidence_codec import (
    _digest_json_payload,
    _recovery_checkpoint_head_binding,
    _trade_payload_digest,
    _view_projection_digest,
)
from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ExecutionCoordinator,
    ExecutionReadinessError,
    InMemoryPositionReservationRepository,
    ReservationConflictError,
    VersionConflictError,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    ExitAllocation,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.execution.ports import (
    DecisionCommitConflict,
    ExecutionEvidenceIdentity,
    ExecutionTradeIdentity,
    ExecutionTransactionPort,
    ExecutionUnitOfWorkPort,
    ExecutionWatermark,
)
from crypto_momentum_lab.domain.execution.position_book import (
    PositionBook,
)
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    CoverageEvidence,
    ExitOrderSubmissionFact,
    FactCoverageInterval,
    FactCoverageStatus,
    FreshnessRequirement,
    PositionKey,
    PositionView,
    compose_fact_coverage,
)
from crypto_momentum_lab.domain.execution.position_recovery import (
    create_verified_recovery_checkpoint,
    recover_durable_position,
)
from crypto_momentum_lab.domain.execution.position_repair import (
    validate_repaired_position,
)
from crypto_momentum_lab.domain.execution.recovery_codec import (
    PositionRecoveryCodec,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    PositionRecoveryCheckpoint,
    StreamCheckpointAdoption,
)
from crypto_momentum_lab.domain.execution.reservation_repository import (
    ReservationRepository,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocationPlan,
    ExitAllocator,
    ExitPolicyMode,
    PositionReservation,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

log = structlog.get_logger(__name__)


class _AbortObservation(Exception):
    def __init__(self, result: ExecutionObserveResult) -> None:
        self.result = result



def _canonical_evidence_payload(evidence: ExecutionEvidence) -> dict[str, object]:
    payload = asdict(evidence)
    payload.pop("observed_at", None)
    order_event = payload.get("order_event")
    if isinstance(order_event, dict):
        order_event.pop("occurred_at", None)
    cumulative_order = payload.get("cumulative_order")
    if isinstance(cumulative_order, dict):
        cumulative_order.pop("observed_at", None)
    return payload


def _execution_head_payload(
    book: ExecutionBook,
    key: PositionKey,
    facts_hash: str,
) -> dict[str, object]:
    scope = book._stream_scopes.get(key.canonical_id)
    if scope is None:
        raise RuntimeError("durable execution head requires a stream scope")
    view = book._ensure_book(key).get_view()
    facts = book._ensure_journal(key).read_cut()
    projection = PositionLedger(key).project(facts)
    checkpoint = facts.recovery_checkpoint
    return {
        "schema_version": 1,
        "position_key": {
            "environment": key.environment,
            "account_label": key.account_label,
            "symbol": key.symbol,
            "position_side": key.position_side.value,
        },
        "stream_scope": {
            "stream_id": scope.stream_id,
            "stream_epoch": scope.stream_epoch,
        },
        "facts_hash": facts_hash,
        "projection_digest": PositionRecoveryCodec.compute_projection_digest(
            projection
        ),
        "view_digest": _view_projection_digest(view),
        "recovery_checkpoint": _recovery_checkpoint_head_binding(checkpoint),
        "journal_revision": book._journal_revisions.get(key.canonical_id, 0),
        "last_sequence": book._last_sequences.get(key.canonical_id),
        "seen_trade_count": len(book._seen_trade_ids),
        "active_reservation_ids": sorted(
            reservation.reservation_id
            for reservation in book.get_active_reservations(key)
        ),
    }


def _evidence_identity(evidence: ExecutionEvidence) -> str:
    if evidence.stream_id is None or evidence.stream_epoch is None:
        return evidence.evidence_id
    key = evidence.scope.to_position_key()
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id=evidence.stream_id, stream_epoch=evidence.stream_epoch
    )
    return _scoped_evidence_identity(scope, evidence.evidence_id)


def _scoped_evidence_identity(
    scope: AccountFactStreamScope,
    evidence_id: str,
) -> str:
    key = PositionKey(
        environment=scope.environment,
        account_label=scope.account_label,
        symbol=scope.symbol,
        position_side=scope.position_side,
    )
    return (
        f"{key.canonical_id}\x1f{scope.stream_id}\x1f"
        f"{scope.stream_epoch}\x1f{evidence_id}"
    )


def _coverage_for_scope(
    evidence: ExecutionEvidence,
    scope: AccountFactStreamScope,
) -> ExecutionEvidence:
    proof = evidence.coverage_evidence
    if evidence.fill_load_provenance is not None:
        if (
            proof is not None
            and proof.load_provenance != evidence.fill_load_provenance
        ):
            raise ValueError("coverage and fill-load provenance disagree")
        if proof is None:
            return evidence
    if proof is not None and evidence.fill_load_provenance is None:
        if proof.load_provenance is None:
            raise ValueError("coverage proof has no durable fill-load provenance")
        evidence = replace(
            evidence,
            fill_load_provenance=proof.load_provenance,
        )
    if proof is None:
        if evidence.coverage is not None and (
            evidence.coverage.stream_scope is not None
            and evidence.coverage.stream_scope != scope
        ):
            raise ValueError("coverage interval does not match the event stream")
        return evidence
    if proof.stream_scope != scope:
        raise ValueError("fill coverage proof does not match the event stream")
    if proof.load_provenance != evidence.fill_load_provenance:
        raise ValueError("coverage proof does not bind the persisted fill scan")

    fill_start = proof.fill_load_start
    checkpoint_cut = proof.checkpoint_event_cut
    if fill_start is not None and checkpoint_cut is not None and checkpoint_cut >= fill_start:
        start = (
            evidence.fill_load_provenance.source_anchor_event_cut
            if evidence.fill_load_provenance is not None
            and evidence.fill_load_provenance.source_anchor_kind
            == "recovery_checkpoint"
            else fill_start
        )
        end = checkpoint_cut
    else:
        start = evidence.observed_at
        end = evidence.observed_at
    is_page_complete = bool(getattr(proof, "page_exhausted", False)) and bool(
        getattr(proof, "not_truncated", False)
    )
    if is_page_complete:
        derived = compose_fact_coverage(
            proof,
            start=start,
            end=end,
            expected_scope=scope,
        )
    else:
        derived = FactCoverageInterval(
            start_at=start,
            end_at=end,
            source_cursor=proof.fill_cursor_id,
            status=FactCoverageStatus.PENDING,
            stream_scope=scope,
            evidence_observed_at=proof.evidence_observed_at,
        )
    if evidence.coverage is not None and evidence.coverage != derived:
        raise ValueError(
            "supplied coverage interval disagrees with its typed source proof"
        )
    return replace(evidence, coverage=derived)


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
    batch_quantities: Mapping[str, Decimal] | None = None
    exit_policy_mode: ExitPolicyMode = ExitPolicyMode.CONSOLIDATE_ELIGIBLE
    expected_projection_version: str | None = None
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
    coverage: FactCoverageInterval | None = None
    coverage_evidence: CoverageEvidence | None = None
    fill_load_provenance: AccountFillLoadProvenance | None = None
    fills: tuple[AccountFillEvent, ...] = ()
    stream_checkpoint_adoption: StreamCheckpointAdoption | None = None
    stream_id: str | None = None
    stream_epoch: str | None = None
    sequence: int | None = None
    cumulative_order: ExecutionCumulativeOrderReport | None = None

    def __post_init__(self) -> None:
        if self.sequence is not None and self.sequence < 0:
            raise ValueError("execution evidence sequence must be non-negative")
        if (self.stream_id is None) != (self.stream_epoch is None):
            raise ValueError("stream_id and stream_epoch must be supplied together")
        if self.fill_load_provenance is not None:
            if self.stream_id is None or self.stream_epoch is None:
                raise ValueError("fill-load provenance requires a scoped event")
            expected_scope = AccountFactStreamScope.for_position_key(
                self.scope.to_position_key(),
                stream_id=self.stream_id,
                stream_epoch=self.stream_epoch,
            )
            if self.fill_load_provenance.stream_scope != expected_scope:
                raise ValueError("fill-load provenance does not match the event scope")
            if (
                self.coverage_evidence is not None
                and self.coverage_evidence.load_provenance
                != self.fill_load_provenance
            ):
                raise ValueError("coverage and fill-load provenance disagree")
        if self.stream_checkpoint_adoption is not None:
            adoption = self.stream_checkpoint_adoption
            if self.stream_id is None or self.stream_epoch is None:
                raise ValueError("stream checkpoint adoption requires stream identity")
            expected_scope = AccountFactStreamScope.for_position_key(
                self.scope.to_position_key(),
                stream_id=self.stream_id,
                stream_epoch=self.stream_epoch,
            )
            if adoption.target_scope != expected_scope:
                raise ValueError("stream checkpoint adoption target does not match evidence")
            if self.fill_load_provenance != adoption.fill_load_provenance:
                raise ValueError("adoption provenance does not match execution evidence")
            if (
                self.coverage_evidence is None
                or self.coverage_evidence.load_provenance
                != adoption.fill_load_provenance
                or self.coverage_evidence.checkpoint_event_cut
                != adoption.target_event_cut
            ):
                raise ValueError("adoption requires matching complete coverage evidence")
        if self.fill is not None and self.fills:
            raise ValueError("supply either fill or fills, not both")
        trade_ids = [fill.trade_id for fill in self.fills]
        if len(trade_ids) != len(set(trade_ids)):
            raise ValueError("one account event cannot repeat a trade id")


@dataclass(frozen=True, slots=True)
class ExecutionCumulativeOrderReport:
    """Cumulative exchange order quantities used for settlement only.

    This is not an account trade fact and must never be appended to the
    position ledger. Only exchange trade identities alter projected holdings.
    """

    order_id: str
    cumulative_quantity: Decimal
    cumulative_quote: Decimal
    observed_at: datetime

    def __post_init__(self) -> None:
        if not self.order_id.strip():
            raise ValueError("cumulative order id must not be empty")
        if self.observed_at.tzinfo is None:
            raise ValueError("cumulative order observed_at must be timezone-aware")
        if (
            not self.cumulative_quantity.is_finite()
            or not self.cumulative_quote.is_finite()
            or self.cumulative_quantity < 0
            or self.cumulative_quote < 0
            or (
                self.cumulative_quantity == 0
                and self.cumulative_quote != 0
            )
            or (
                self.cumulative_quantity > 0
                and self.cumulative_quote <= 0
            )
        ):
            raise ValueError("cumulative order report quantities are invalid")


@dataclass(frozen=True, slots=True)
class Applied:
    evidence_id: str
    updated_view_token: str
    consumed_quantity: Decimal = Decimal("0")
    released_quantity: Decimal = Decimal("0")
    recovery_required: bool = False
    diagnostics: tuple[str, ...] = ()


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
        reservation_repository: ReservationRepository | None = None,
        command_repository: CommandRepository | None = None,
        execution_unit_of_work: ExecutionUnitOfWorkPort | None = None,
    ) -> None:
        self._books: dict[str, PositionBook] = books_by_key or {}
        self._journals: dict[str, AccountJournal] = journals_by_key or {}
        # Default coordinator owns memory only. Durable/async restoration is
        # explicitly awaited by restore; legacy sync assembly is outside Book.
        self._coordinator = coordinator or ExecutionCoordinator()
        self._reservation_repo = reservation_repository
        self._command_repo = command_repository
        self._execution_unit_of_work = execution_unit_of_work
        self._active_transaction: ExecutionTransactionPort | None = None
        self._stream_scopes: dict[str, AccountFactStreamScope] = {}
        self._active_streams: set[tuple[str, str, str, str]] = set()
        self._latest_active_streams: dict[tuple[str, str], tuple[str, str]] = {}
        self._head_revisions: dict[str, int] = {}
        self._head_projection_digests: dict[str, str] = {}
        self._head_expected_reservation_ids: dict[str, set[str]] = {}
        self._journal_revisions: dict[str, int] = {}
        self._last_sequences: dict[str, int] = {}
        self._recovery_adoption_scope: AccountFactStreamScope | None = None
        self._global_mutation_lock = asyncio.Lock()
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
        self._dispatch_reconciliation_required_commands: set[str] = set()

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
        self._active_streams.add(
            (environment, account_label, stream_id, stream_epoch)
        )
        self._latest_active_streams[(environment, account_label)] = (
            stream_id,
            stream_epoch,
        )

    def get_active_stream(
        self,
        environment: str,
        account_label: str,
    ) -> tuple[str, str] | None:
        """Return the active (stream_id, stream_epoch) registered for this account, if known."""
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

    async def reload_position(
        self,
        key: PositionKey,
        *,
        as_of: datetime | None = None,
        expected_scope: AccountFactStreamScope | None = None,
        expected_quantity: Decimal | None = None,
    ) -> PositionView | None:
        """Reload a single position from durable storage into memory."""
        if self._execution_unit_of_work is None:
            return None
        as_of = as_of or datetime.now(UTC)
        canon = key.canonical_id
        async with self._mutation_lock(key):
            states = await self._execution_unit_of_work.load_positions(
                account_label=key.account_label,
                environment=key.environment,
                as_of=as_of,
            )
            target_state = next(
                (
                    s
                    for s in states
                    if (
                        s.scope.environment == key.environment
                        and s.scope.account_label == key.account_label
                        and s.scope.symbol == key.symbol
                        and getattr(s.scope.position_side, "value", s.scope.position_side)
                        == getattr(key.position_side, "value", key.position_side)
                    )
                ),
                None,
            )
            if target_state is None:
                return None
            journal = AccountJournal.from_durable_cut(target_state.cut)
            book = PositionBook(journal)
            if expected_scope is not None:
                validate_repaired_position(
                    state=target_state,
                    scope=expected_scope,
                    expected_quantity=expected_quantity,
                    reservation_ids={
                        r.reservation_id for r in self.get_active_reservations(key)
                    },
                )
            head = target_state.head
            scope = target_state.cut.scope
            if head is not None:
                payload = head.state_payload
                view = book.get_view()
                book.use_durable_projection_version(
                    head.projection_version,
                    event_cut=view.event_cut,
                )
                self._head_revisions[canon] = head.revision
                self._head_projection_digests[canon] = str(
                    payload.get("projection_digest", "")
                )
                active_res = payload.get("active_reservation_ids", [])
                self._head_expected_reservation_ids[canon] = (
                    set(active_res) if isinstance(active_res, list) else set()
                )
                last_sequence = payload.get("last_sequence")
                if isinstance(last_sequence, int) and last_sequence >= 0:
                    self._last_sequences[canon] = last_sequence
                else:
                    self._last_sequences.pop(canon, None)
            else:
                self._head_revisions[canon] = 0

            self._journals[canon] = journal
            self._books[canon] = book
            self._stream_scopes[canon] = scope
            self._journal_revisions[canon] = target_state.cut.revision
            self._seen_trade_ids.update(target_state.trade_ids)
            self._seen_evidence_ids.update(
                _scoped_evidence_identity(scope, identity)
                for identity in target_state.evidence_ids
            )
            for watermark in target_state.watermarks:
                watermark_key = self._order_watermark_key(key, watermark.order_id)
                self._order_cumulative_fills[watermark_key] = (
                    watermark.cumulative_quantity
                )
                self._order_cumulative_quotes[watermark_key] = (
                    watermark.cumulative_quote
                )
            return book.get_view()

    @property
    def coordinator(self) -> ExecutionCoordinator:
        return self._coordinator

    @property
    def has_command_repository(self) -> bool:
        return self._command_repo is not None

    @property
    def has_execution_unit_of_work(self) -> bool:
        """Whether mutations require the durable PostgreSQL commit path."""
        return self._execution_unit_of_work is not None

    def _mutation_lock(self, key: PositionKey) -> asyncio.Lock:
        del key
        return self._global_mutation_lock

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
        candidate._requests_by_id = dict(self._requests_by_id)
        candidate._receipts_by_id = dict(self._receipts_by_id)
        candidate._seen_evidence_ids = set(self._seen_evidence_ids)
        candidate._seen_trade_ids = set(self._seen_trade_ids)
        candidate._outbox_by_command_id = dict(self._outbox_by_command_id)
        candidate._command_reservations = dict(self._command_reservations)
        candidate._order_cumulative_fills = dict(self._order_cumulative_fills)
        candidate._order_cumulative_quotes = dict(self._order_cumulative_quotes)
        candidate._recovery_required_commands = set(self._recovery_required_commands)
        candidate._dispatch_reconciliation_required_commands = set(
            self._dispatch_reconciliation_required_commands
        )
        candidate._stream_scopes = dict(self._stream_scopes)
        candidate._active_streams = set(self._active_streams)
        candidate._latest_active_streams = dict(self._latest_active_streams)
        candidate._head_revisions = dict(self._head_revisions)
        candidate._head_projection_digests = dict(self._head_projection_digests)
        candidate._journal_revisions = dict(self._journal_revisions)
        candidate._last_sequences = dict(self._last_sequences)
        candidate._recovery_adoption_scope = self._recovery_adoption_scope
        candidate._coordinator = ExecutionCoordinator(
            repository=InMemoryPositionReservationRepository()
        )
        candidate._coordinator._reservations_by_id = dict(
            getattr(self._coordinator, "_reservations_by_id", {})
        )
        candidate._reservation_repo = None
        candidate._active_transaction = None
        candidate._global_mutation_lock = self._global_mutation_lock
        return candidate

    def _publish_candidate(self, candidate: ExecutionBook) -> None:
        """Publish a candidate only after its Postgres transaction committed."""
        for name in (
            "_books",
            "_journals",
            "_requests_by_id",
            "_receipts_by_id",
            "_seen_evidence_ids",
            "_seen_trade_ids",
            "_outbox_by_command_id",
            "_command_reservations",
            "_order_cumulative_fills",
            "_order_cumulative_quotes",
            "_recovery_required_commands",
            "_dispatch_reconciliation_required_commands",
            "_stream_scopes",
            "_active_streams",
            "_latest_active_streams",
            "_head_revisions",
            "_head_projection_digests",
            "_journal_revisions",
            "_last_sequences",
            "_recovery_adoption_scope",
        ):
            setattr(self, name, getattr(candidate, name))
        self._coordinator._reservations_by_id = (
            candidate._coordinator._reservations_by_id
        )
        # The candidate's append-only delta is now durable, so it must not be
        # re-sent by the next observation. A candidate that rolled back is
        # never published, which keeps its pending events queued for retry.
        for journal in self._journals.values():
            journal.mark_facts_persisted()

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
        if self._command_repo is None and self._active_transaction is None:
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
                    command=(
                        entry.command.command_type.value
                        if hasattr(entry.command.command_type, "value")
                        else str(entry.command.command_type)
                    ),
                    status=entry.state.value,
                    requested_at=entry.created_at,
                    details=details,
                )
            else:
                await self._command_repo.upsert_execution_command(
                    command_id=entry.command_id,
                    client_order_id=entry.command.command_id,
                    command=(
                        entry.command.command_type.value
                        if hasattr(entry.command.command_type, "value")
                        else str(entry.command.command_type)
                    ),
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

    async def drain(self, timeout_seconds: float = 5.0) -> None:
        """Lifecycle hook retained for callers; all persistence is awaited inline."""
        del timeout_seconds

    async def _restore_durable_positions(
        self,
        *,
        account_label: str,
        environment: str,
        as_of: datetime,
    ) -> None:
        states = await self._execution_unit_of_work.load_positions(
            environment=environment,
            account_label=account_label,
            as_of=as_of,
        )
        for state in states:
            scope = state.scope
            key = PositionKey(
                environment=scope.environment,
                account_label=scope.account_label,
                symbol=scope.symbol,
                position_side=scope.position_side,
            )
            canon = key.canonical_id
            recovered = recover_durable_position(state)
            journal, book = recovered.journal, recovered.book
            for event, values in recovered.diagnostics:
                log.warning(event, **values)
            self._head_revisions[canon] = recovered.head_revision
            if state.head is not None:
                self._head_projection_digests[canon] = recovered.projection_digest
                self._head_expected_reservation_ids[canon] = set(recovered.reservation_ids)
                if recovered.last_sequence is not None:
                    self._last_sequences[canon] = recovered.last_sequence

            self._journals[canon] = journal
            self._books[canon] = book
            self._stream_scopes[canon] = scope
            self._journal_revisions[canon] = state.cut.revision
            self._seen_trade_ids.update(state.trade_ids)
            self._seen_evidence_ids.update(
                _scoped_evidence_identity(scope, evidence_id)
                for evidence_id in state.evidence_ids
            )
            for watermark in state.watermarks:
                watermark_key = self._order_watermark_key(key, watermark.order_id)
                self._order_cumulative_fills[watermark_key] = watermark.cumulative_quantity
                self._order_cumulative_quotes[watermark_key] = watermark.cumulative_quote

    async def restore(
        self,
        account_label: str | None = None,
        *,
        environment: str = "live",
        as_of: datetime | None = None,
    ) -> None:
        """Restores in-flight outbox commands, deduplication, and reservations."""
        self._persistence_failed = True
        command_repository = self._command_repo
        deferred_unknown_commands: list[str] = []
        if self._execution_unit_of_work is not None:
            if not account_label:
                raise ValueError("durable restore requires an account_label")
            if command_repository is None:
                raise RuntimeError(
                    "durable execution restore requires a command repository"
                )
            as_of = as_of or datetime.now(UTC)
            if as_of.tzinfo is None or as_of.utcoffset() is None:
                raise ValueError("restore as_of must be timezone-aware")
            self._books.clear()
            self._journals.clear()
            self._stream_scopes.clear()
            self._head_revisions.clear()
            self._head_projection_digests.clear()
            self._head_expected_reservation_ids.clear()
            self._journal_revisions.clear()
            self._last_sequences.clear()
            self._head_expected_reservation_ids.clear()
            self._seen_evidence_ids.clear()
            self._seen_trade_ids.clear()
            self._order_cumulative_fills.clear()
            self._order_cumulative_quotes.clear()
            self._outbox_by_command_id.clear()
            self._command_reservations.clear()
            self._requests_by_id.clear()
            self._receipts_by_id.clear()
            self._recovery_required_commands.clear()
            self._dispatch_reconciliation_required_commands.clear()
            self._coordinator._reservations_by_id.clear()
            await self._restore_durable_positions(
                account_label=account_label,
                environment=environment,
                as_of=as_of,
            )
        if command_repository is not None:
            try:
                active_cmds = await command_repository.load_active_execution_commands(
                    account_label=account_label
                )
                for cmd_data in active_cmds:
                    recovered = decode_active_command(
                        cmd_data,
                        account_label=account_label,
                        restored_at=datetime.now(UTC),
                    )
                    if recovered is None:
                        continue
                    if isinstance(recovered, SkippedCommand):
                        log.warning(
                            "skipping_unparseable_active_execution_command",
                            command_id=recovered.command_id,
                            error=recovered.reason,
                        )
                        continue
                    entry = recovered.entry
                    cid = entry.command_id
                    self._outbox_by_command_id[cid] = entry
                    self._command_reservations[cid] = list(recovered.reservation_ids)
                    if recovered.requires_reconciliation:
                        self._dispatch_reconciliation_required_commands.add(cid)
                    if recovered.needs_unknown_write:
                        if self._execution_unit_of_work is not None:
                            deferred_unknown_commands.append(cid)
                        else:
                            await self._persist_outbox_state(entry)
            except Exception as err:
                log.error("restore_active_commands_failed", error=str(err))
                raise RuntimeError(
                    "Failed to restore active execution commands"
                ) from err

            # Durable identities and watermarks were loaded with position heads.
            if self._execution_unit_of_work is None:
                try:
                    self._seen_evidence_ids.update(
                        await command_repository.load_seen_event_ids()
                    )
                except Exception as err:
                    raise RuntimeError(
                        "Failed to restore execution event identities"
                    ) from err
                try:
                    self._seen_trade_ids.update(
                        await command_repository.load_seen_fill_trade_ids()
                    )
                except Exception as err:
                    raise RuntimeError("Failed to restore fill identities") from err
                try:
                    watermark_rows = (
                        await command_repository.load_execution_order_watermarks(
                            account_label=account_label
                        )
                    )
                    for row in watermark_rows:
                        restored = decode_order_watermark(
                            row, account_label=account_label
                        )
                        if restored is None:
                            continue
                        scope = restored.scope
                        order_id = restored.order_id
                        quantity = restored.quantity
                        quote = restored.quote
                        key = self._order_watermark_key(
                            scope.to_position_key(), order_id
                        )
                        self._order_cumulative_fills[key] = max(
                            self._order_cumulative_fills.get(key, Decimal("0")),
                            quantity,
                        )
                        self._order_cumulative_quotes[key] = max(
                            self._order_cumulative_quotes.get(key, Decimal("0")),
                            quote,
                        )
                except Exception as err:
                    log.error("restore_watermarks_failed", error=str(err))
                    raise RuntimeError(
                        f"Failed to restore cumulative fill watermarks: {err}"
                    ) from err

        if self._reservation_repo is not None:
            try:
                active_res = await self._reservation_repo.load_active_reservations()
                for reservation in active_res:
                    if (
                        account_label is not None
                        and reservation.position_key.account_label != account_label
                    ):
                        continue
                    self._coordinator.register_reservation(reservation)
            except Exception as err:
                raise RuntimeError("Failed to restore active reservations") from err
        for canon, expected_ids in self._head_expected_reservation_ids.items():
            journal = self._journals.get(canon)
            if journal is None:
                log.warning(
                    "restored_reservation_head_has_no_journal", canonical_id=canon
                )
                continue
            actual_ids = {
                reservation.reservation_id
                for reservation in self.get_active_reservations(journal.position_key)
            }
            if actual_ids != expected_ids:
                log.warning(
                    "restored_active_reservations_diverged",
                    canonical_id=canon,
                    expected_ids=sorted(expected_ids),
                    actual_ids=sorted(actual_ids),
                )
        self._head_expected_reservation_ids.clear()
        self._persistence_failed = False
        if self._execution_unit_of_work is not None:
            for command_id in deferred_unknown_commands:
                await self.mark_unknown(
                    command_id,
                    "restored dispatch requires reconciliation",
                    datetime.now(UTC),
                )

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

    @staticmethod
    def _order_watermark_key(key: PositionKey, order_id: str) -> str:
        return f"{key.canonical_id}\x1f{order_id}"

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

    def get_active_reservations(
        self, key: PositionKey | None = None
    ) -> tuple[PositionReservation, ...]:
        """Returns active reservations tracked by the domain coordinator."""
        if hasattr(self._coordinator, "get_active_reservations"):
            if key is not None:
                return self._coordinator.get_active_reservations(key)
            active = [
                r
                for r in getattr(self._coordinator, "_reservations_by_id", {}).values()
                if r.active_quantity > Decimal("0")
            ]
            active.sort(key=lambda r: (r.created_at, r.reservation_id))
            return tuple(active)
        return ()

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
        """Read a published position, or a persisted historical cut, without mutation."""
        if (stream_id is None) != (stream_epoch is None):
            raise ValueError("stream_id and stream_epoch must be supplied together")
        if stream_id is not None and (not stream_id.strip() or not stream_epoch.strip()):
            raise ValueError("stream_id and stream_epoch must not be empty")
        if event_cut is not None and (
            event_cut.tzinfo is None or event_cut.utcoffset() is None
        ):
            raise ValueError("event_cut must be timezone-aware")
        if self._execution_unit_of_work is not None and self._persistence_failed:
            raise RuntimeError("execution facts require successful durable restoration")
        key = PositionKey(
            environment=scope.environment,
            account_label=scope.account_label,
            symbol=scope.symbol,
            position_side=scope.position_side,
        )
        canon = key.canonical_id
        source_scope = self._stream_scopes.get(canon)
        if stream_id is not None and (
            source_scope is None
            or source_scope.stream_id != stream_id
            or source_scope.stream_epoch != stream_epoch
        ):
            current_book = self._books.get(canon)
            is_flat = (
                current_book is None
                or (
                    current_book.get_view().total_quantity == Decimal("0")
                    and not current_book.get_view().batches
                    and not current_book.get_view().unallocated_quantity
                )
            )
            has_no_reservations = not bool(self.get_active_reservations(key))
            has_no_commands = not any(
                getattr(cmd, "key", None) == key
                for cmd in self._outbox_by_command_id.values()
            )
            is_known_active_stream = (
                (
                    scope.environment,
                    scope.account_label,
                    stream_id,
                    stream_epoch,
                )
                in self._active_streams
                or any(
                    s.account_label == scope.account_label
                    and s.environment == scope.environment
                    and s.stream_id == stream_id
                    and s.stream_epoch == stream_epoch
                    for s in self._stream_scopes.values()
                )
            )
            is_legacy_stream = (
                source_scope is not None
                and source_scope.stream_id == "legacy-postgres-account"
            )
            if (
                (is_flat or is_legacy_stream)
                and has_no_reservations
                and has_no_commands
                and is_known_active_stream
            ):
                target_scope = AccountFactStreamScope.for_position_key(
                    key, stream_id=stream_id, stream_epoch=stream_epoch
                )
                self._stream_scopes[canon] = target_scope
                if (
                    canon not in self._journals
                    or self._journals[canon].stream_scope != target_scope
                ):
                    self._journals[canon] = AccountJournal(key, stream_scope=target_scope)
                    self._books[canon] = PositionBook(self._journals[canon])
                    self._journal_revisions[canon] = 0
                    self._last_sequences.pop(canon, None)
                source_scope = target_scope
            else:
                raise ValueError("requested account stream does not match the restored position")
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
            return book.get_view(requirement=requirement, now=now)
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
            historical_book = PositionBook(
                AccountJournal.from_durable_cut(cut),
                ledger=book._ledger,
                policy_version=book._policy_version,
                schema_version=book._schema_version,
            )
            return historical_book.get_view(cut=event_cut, requirement=requirement, now=now)
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
        key = PositionKey(
            environment=scope.environment,
            account_label=scope.account_label,
            symbol=scope.symbol,
            position_side=scope.position_side,
        )
        as_of = as_of or datetime.now(UTC)
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("checkpoint read as_of must be timezone-aware")
        if self._execution_unit_of_work is None:
            journal = self._journals.get(key.canonical_id)
            if journal is None or journal.stream_scope != scope:
                return None
            checkpoint = journal.read_cut().recovery_checkpoint
        else:
            cut = await self._execution_unit_of_work.load_journal_cut(
                scope=scope,
                as_of=as_of,
            )
            if cut.scope != scope or cut.facts.position_key != key:
                raise RuntimeError("durable checkpoint read returned another scope")
            checkpoint = cut.checkpoint
        if checkpoint is not None and (
            checkpoint.stream_scope != scope
            or checkpoint.key.canonical_id != key.canonical_id
            or checkpoint.event_cut > as_of
        ):
            raise RuntimeError("durable recovery checkpoint identity is invalid")
        return checkpoint

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
        """List only existing positions belonging to the requested account stream.

        ``symbols`` narrows the read to the scopes that can still represent
        current exposure. It is a pure filter over existing books: it never
        creates a journal, and ``None`` keeps the historical full-account read
        for callers that must see every scope.
        """
        if not environment.strip() or not account_label.strip():
            raise ValueError("environment and account_label must not be empty")
        if (stream_id is None) != (stream_epoch is None):
            raise ValueError("stream_id and stream_epoch must be supplied together")
        if stream_id is not None and (not stream_id.strip() or not stream_epoch.strip()):
            raise ValueError("stream_id and stream_epoch must not be empty")
        if event_cut is not None and (
            event_cut.tzinfo is None or event_cut.utcoffset() is None
        ):
            raise ValueError("event_cut must be timezone-aware")
        if self._execution_unit_of_work is not None and self._persistence_failed:
            raise RuntimeError("execution facts require successful durable restoration")
        scopes = []
        for book in tuple(self._books.values()):
            key = book.position_key
            if key.environment != environment or key.account_label != account_label:
                continue
            if symbols is not None and key.symbol not in symbols:
                continue
            source = self._stream_scopes.get(key.canonical_id)
            if stream_id is not None and (
                source is None or source.stream_id != stream_id or source.stream_epoch != stream_epoch
            ):
                continue
            scopes.append(ExecutionScope(
                environment=key.environment, account_label=key.account_label,
                symbol=key.symbol, position_side=key.position_side,
            ))
        scopes.sort(key=lambda scope: (scope.symbol, scope.position_side.value))
        return tuple([
            await self.read(scope, event_cut=event_cut, stream_id=stream_id, stream_epoch=stream_epoch)
            for scope in scopes
        ])

    async def act(self, request: ExecutionRequest) -> ExecutionActResult:
        """Accept a command atomically when backed by the durable UoW."""
        if self._execution_unit_of_work is None:
            return await self._act_mutating(request)
        key = request.scope.to_position_key()
        canon = key.canonical_id
        async with self._mutation_lock(key):
            if self._persistence_failed:
                return Blocked(
                    reason="Execution persistence failed; restore is required before trading"
                )
            stream_scope = self._stream_scopes.get(canon)
            if stream_scope is None:
                return Blocked(
                    reason=(
                        "Position has no restored account stream identity; "
                        "execution is fail-closed"
                    )
                )
            candidate = self._staged_copy(key=key)
            try:
                async with self._execution_unit_of_work.transaction(key) as tx:
                    head = await tx.load_head(key)
                    adopting_epoch = False
                    current_view = candidate._ensure_book(key).get_view()
                    is_candidate_flat = (
                        current_view.total_quantity == Decimal("0")
                        and not current_view.batches
                    )
                    if head is None:
                        if is_candidate_flat:
                            expected_revision = 0
                        else:
                            return Blocked(
                                reason="Position facts are not durably restored"
                            )
                    else:
                        expected_revision = head.revision
                        if (
                            head.stream_id != stream_scope.stream_id
                            or head.stream_epoch != stream_scope.stream_epoch
                        ):
                            is_head_flat = (
                                not head.state_payload.get("active_reservation_ids")
                                and is_candidate_flat
                            )
                            if is_head_flat:
                                adopting_epoch = True
                            else:
                                return Blocked(
                                    reason=(
                                        "Position source stream changed without a validated "
                                        "recovery checkpoint"
                                    )
                                )
                        else:
                            if self._head_revisions.get(canon) != head.revision:
                                return Blocked(
                                    reason="Position projection is stale; reload durable facts"
                                )
                            if current_view.projection_version != head.projection_version:
                                return Blocked(
                                    reason=(
                                        "Position projection differs from its durable head; "
                                        "reload before trading"
                                    )
                                )
                    candidate._active_transaction = tx
                    result = await candidate._act_mutating(request)
                    if not isinstance(result, Accepted):
                        return result
                    if result.receipt.reservations:
                        batch_capacities = {
                            batch.batch_id: batch.quantity
                            for batch in current_view.batches
                        }
                        await tx.save_reservations(
                            result.receipt.reservations,
                            expected_projection_version=current_view.projection_version,
                            batch_quantities=batch_capacities,
                            proven_position_quantity=current_view.total_quantity,
                        )
                    facts = candidate._ensure_journal(key).read_cut()
                    head_payload = _execution_head_payload(
                        candidate, key, facts.compute_facts_hash()
                    )
                    next_revision = await tx.persist_head(
                        key=key,
                        stream_id=stream_scope.stream_id,
                        stream_epoch=stream_scope.stream_epoch,
                        expected_revision=expected_revision,
                        projection_version=candidate._ensure_book(key)
                        .get_view()
                        .projection_version,
                        state_payload=head_payload,
                        updated_at=request.created_at,
                        is_flat_adoption=adopting_epoch,
                    )
                    candidate._head_revisions[canon] = next_revision
                    candidate._head_projection_digests[canon] = str(
                        head_payload["projection_digest"]
                    )
                candidate._active_transaction = None
                self._publish_candidate(candidate)
                return result
            except Exception as err:
                self._persistence_failed = True
                log.error(
                    "atomic_execution_acceptance_failed",
                    position_key=key.canonical_id,
                    request_id=request.request_id,
                    error=str(err),
                )
                return Blocked(
                    reason="Execution command was not durably accepted",
                    diagnostics=(f"{type(err).__name__}: {err}",),
                )

    async def _act_mutating(
        self,
        request: ExecutionRequest,
    ) -> ExecutionActResult:
        """Accepts a trade request, enforcing CAS view token, capacity, and outbox."""
        if self._persistence_failed:
            return Blocked(
                reason=(
                    "Execution persistence failed; restore is required before trading"
                )
            )
        if self._recovery_required_commands:
            return Blocked(
                reason="Execution reservation settlement requires recovery",
                diagnostics=tuple(sorted(self._recovery_required_commands)),
            )
        if self._dispatch_reconciliation_required_commands:
            return Blocked(
                reason="Execution command reconciliation is required",
                diagnostics=tuple(
                    sorted(self._dispatch_reconciliation_required_commands)
                ),
            )
        key = request.scope.to_position_key()
        book = self._ensure_book(key)
        view = book.get_view()
        effective_view_token = view.projection_version

        # 1. Idempotency verification
        if request.request_id in self._requests_by_id:
            existing_req = self._requests_by_id[request.request_id]
            if existing_req == request:
                return AlreadyAccepted(self._receipts_by_id[request.request_id])
            return CommandConflict(
                request_id=request.request_id,
                reason="Conflicting payload for identical request_id",
            )
        restored_entry = self._outbox_by_command_id.get(request.request_id)
        if restored_entry is not None:
            return Blocked(
                reason="Execution command already exists and requires reconciliation",
                diagnostics=(restored_entry.state.value,),
            )

        # 2. View token CAS validation
        if (
            request.expected_view_token not in ("*", "pv_initial")
            and request.expected_view_token != view.projection_version
        ):
            return StaleView(
                expected_token=request.expected_view_token,
                current_token=view.projection_version,
                reason=(
                    f"Expected view token {request.expected_view_token} does not match "
                    f"current projection {view.projection_version}"
                ),
            )

        # 3. Trade readiness check
        if not view.is_ready_for_trade and not request.target_batch_ids:
            return Blocked(
                reason=(
                    f"PositionView is not ready for trade (status={view.health_status})"
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

            if alloc_plan is None or alloc_plan.total_allocated_quantity <= Decimal(
                "0"
            ):
                if request.target_batch_ids:
                    if request.batch_quantities:
                        allocations = tuple(
                            ExitAllocation(
                                batch_id=bid,
                                allocated_quantity=request.batch_quantities.get(
                                    bid,
                                    request.requested_quantity
                                    / len(request.target_batch_ids),
                                ),
                            )
                            for bid in request.target_batch_ids
                        )
                    else:
                        qty_per_batch = request.requested_quantity / len(
                            request.target_batch_ids
                        )
                        allocations = tuple(
                            ExitAllocation(
                                batch_id=bid,
                                allocated_quantity=qty_per_batch,
                            )
                            for bid in request.target_batch_ids
                        )
                    alloc_plan = ExitAllocationPlan(
                        position_key=key,
                        allocations=allocations,
                        total_allocated_quantity=request.requested_quantity,
                        policy=request.exit_policy_mode,
                        reason=f"exit_{request.decision_ref}",
                        projection_version=effective_view_token,
                    )
                else:
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
                expected_projection_version=effective_view_token,
                allocation_plan=alloc_plan,
                created_at=request.created_at,
            )

            if view.batches:
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
            else:
                res_list: list[PositionReservation] = []
                for idx, alloc in enumerate(alloc_plan.allocations):
                    res_id = (
                        f"res_{command.command_id}"
                        if len(alloc_plan.allocations) == 1
                        else f"res_{command.command_id}_{idx}"
                    )
                    r = PositionReservation(
                        reservation_id=res_id,
                        command_id=command.command_id,
                        position_key=key,
                        batch_id=alloc.batch_id,
                        reserved_quantity=alloc.allocated_quantity,
                        created_at=command.created_at,
                    )
                    self._coordinator.register_reservation(r)
                    res_list.append(r)
                reservations = tuple(res_list)

            batch_quantities_dict = (
                {
                    alloc.batch_id: alloc.allocated_quantity
                    for alloc in alloc_plan.allocations
                }
                if alloc_plan
                else None
            )

            if self._reservation_repo is not None:
                to_save: list[PositionReservation] = []
                for res in reservations:
                    try:
                        existing = await self._reservation_repo.load_reservation(
                            res.reservation_id
                        )
                        if existing is not None:
                            if (
                                existing.batch_id != res.batch_id
                                or existing.reserved_quantity != res.reserved_quantity
                                or existing.position_key != res.position_key
                            ):
                                return CommandConflict(
                                    request_id=command.command_id,
                                    reason=(
                                        f"Reservation {res.reservation_id} already "
                                        f"exists with different parameters "
                                        f"(batch_id={existing.batch_id}, "
                                        f"quantity={existing.reserved_quantity}) "
                                        f"that does not match requested "
                                        f"(batch_id={res.batch_id}, "
                                        f"quantity={res.reserved_quantity})"
                                    ),
                                )
                            continue
                    except ReservationConflictError:
                        raise
                    except Exception as load_error:
                        self._persistence_failed = True
                        self._recovery_required_commands.add(command.command_id)
                        return Blocked(
                            reason="Reservation identity lookup failed; restore is required",
                            diagnostics=(str(load_error),),
                        )
                    to_save.append(res)

                if to_save:
                    expected_ver = (
                        request.expected_projection_version
                        if request.expected_projection_version is not None
                        else (
                            None
                            if request.expected_view_token in ("*", "pv_initial")
                            else request.expected_view_token
                        )
                    )
                    try:
                        await self._reservation_repo.save_reservations(
                            tuple(to_save),
                            expected_projection_version=expected_ver,
                            batch_quantities=batch_quantities_dict,
                        )
                    except Exception as save_err:
                        return CommandConflict(
                            request_id=command.command_id,
                            reason=(
                                f"Reservation already exists or save conflict: "
                                f"{save_err}"
                            ),
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
                expected_projection_version=effective_view_token,
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
        if reservations:
            self._command_reservations[command.command_id] = [
                r.reservation_id for r in reservations
            ]
        self._outbox_by_command_id[command.command_id] = outbox
        try:
            await self._persist_outbox_state(outbox)
        except Exception as persist_err:
            self._outbox_by_command_id.pop(command.command_id, None)
            self._command_reservations.pop(command.command_id, None)
            rollback_errors: list[str] = []
            for reservation in reservations:
                current = self._coordinator.get_reservation(reservation.reservation_id)
                if current is None or current.active_quantity <= Decimal("0"):
                    continue
                try:
                    released = self._coordinator.release_reservation(
                        current.reservation_id, current.active_quantity
                    )
                    if self._reservation_repo is not None:
                        await self._reservation_repo.update_reservation(
                            released,
                            release_reason="outbox_acceptance_failed",
                        )
                except Exception as rollback_err:
                    rollback_errors.append(str(rollback_err))
            diagnostics = [f"outbox persistence failed: {persist_err}"]
            if rollback_errors:
                diagnostics.append(
                    "reservation rollback failed: " + "; ".join(rollback_errors)
                )
            return Blocked(
                reason="Execution command was not durably accepted",
                diagnostics=tuple(diagnostics),
            )

        receipt = ExecutionReceipt(
            request_id=request.request_id,
            scope=request.scope,
            command=command,
            reservations=reservations,
            committed_at=committed_at,
            view_token=effective_view_token,
            outbox_entry=outbox,
        )
        self._requests_by_id[request.request_id] = request
        self._receipts_by_id[request.request_id] = receipt
        return Accepted(receipt)

    def register_prepared_command(
        self,
        command: TradeCommand,
        scope: ExecutionScope,
        reservation_ids: list[str] | tuple[str, ...] = (),
    ) -> OutboxEntry:
        """Registers a prepared command into the outbox and links reservations."""
        entry = OutboxEntry(
            command_id=command.command_id,
            request_id=command.command_id,
            scope=scope,
            command=command,
            state=DispatchState.PREPARED,
            created_at=command.created_at,
            updated_at=command.created_at,
        )
        self._outbox_by_command_id[command.command_id] = entry
        if reservation_ids:
            self._command_reservations[command.command_id] = list(reservation_ids)
        return entry

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

    async def _durable_command_mutation(
        self,
        command_id: str,
        target: DispatchState,
        *,
        at: datetime | None = None,
        external_order_id: str | None = None,
        reason: str = "",
    ) -> OutboxEntry:
        entry = self._outbox_by_command_id.get(command_id)
        if entry is None:
            raise KeyError(f"Outbox entry {command_id} not found")
        key = entry.scope.to_position_key()
        canon = key.canonical_id
        async with self._mutation_lock(key):
            if self._persistence_failed:
                raise RuntimeError("Execution persistence failed; restore is required")
            stream_scope = self._stream_scopes.get(canon)
            if stream_scope is None:
                raise RuntimeError("Execution command has no durable stream scope")
            candidate = self._staged_copy(key=key)
            try:
                async with self._execution_unit_of_work.transaction(key) as tx:
                    head = await tx.load_head(key)
                    if head is None or self._head_revisions.get(canon) != head.revision:
                        raise RuntimeError(
                            "durable execution head changed; restore is required"
                        )
                    if (
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
                    facts = candidate._ensure_journal(key).read_cut()
                    head_payload = _execution_head_payload(
                        candidate, key, facts.compute_facts_hash()
                    )
                    candidate._head_revisions[canon] = await tx.persist_head(
                        key=key,
                        stream_id=stream_scope.stream_id,
                        stream_epoch=stream_scope.stream_epoch,
                        expected_revision=head.revision,
                        projection_version=candidate._ensure_book(key)
                        .get_view()
                        .projection_version,
                        state_payload=head_payload,
                        updated_at=datetime.now(UTC),
                    )
                    candidate._head_projection_digests[canon] = str(
                        head_payload["projection_digest"]
                    )
                candidate._active_transaction = None
                self._publish_candidate(candidate)
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
    ) -> OutboxEntry:
        return await self._transition_command(
            command_id,
            DispatchState.REJECTED,
            reason=reason,
            at=rejected_at,
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
    ) -> OutboxEntry:
        if self._execution_unit_of_work is not None:
            return await self._durable_command_mutation(
                command_id,
                target,
                at=at,
                external_order_id=external_order_id,
                reason=reason,
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
        entry = self._outbox_by_command_id.get(command_id)
        if entry is None:
            raise KeyError(f"Outbox entry {command_id} not found")
        plan = plan_command_transition(
            entry,
            target,
            at=at or datetime.now(UTC),
            external_order_id=external_order_id,
            reason=reason,
        )
        if plan.updated is entry:
            return entry
        if plan.requires_reconciliation:
            self._dispatch_reconciliation_required_commands.add(command_id)
        try:
            await self._persist_transition(entry, plan.updated)
        except Exception:
            if plan.requires_reconciliation:
                # A submit may have reached the exchange. Seal against resubmit
                # even when the durable UNKNOWN write fails.
                self._outbox_by_command_id[command_id] = plan.updated
                self._persistence_failed = True
            raise
        if plan.release_reason is not None:
            await self._release_command_reservations(
                command_id,
                reason=plan.release_reason,
            )
        return self._outbox_by_command_id[command_id]

    async def _persist_transition(
        self,
        previous: OutboxEntry,
        updated: OutboxEntry,
    ) -> None:
        await self._persist_outbox_state(updated)
        self._outbox_by_command_id[updated.command_id] = updated

    async def _release_command_reservations(
        self,
        command_id: str,
        *,
        reason: str,
    ) -> Decimal:
        plan = plan_reservation_release(
            tuple(self._find_active_reservations_for_command(command_id))
        )
        for released in plan.updates:
            await self._persist_reservation_update(released, release_reason=reason)
        return plan.released_quantity

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
    ) -> tuple[Decimal, bool, str | None]:
        plan = plan_reservation_settlement(
            tuple(self._find_active_reservations_for_command(order_id)),
            order_id=order_id,
            quantity=quantity,
            reported_quantity=reported_quantity,
        )
        for updated in plan.updates:
            await self._persist_reservation_update(updated)
        if plan.recovery_required:
            self._recovery_required_commands.add(order_id)
        return plan.consumed_quantity, plan.recovery_required, plan.diagnostic

    async def observe(self, evidence: ExecutionEvidence) -> ExecutionObserveResult:
        """Atomically accept source evidence and publish its projection."""
        if self._execution_unit_of_work is None:
            result = await self._observe_grouped(self, evidence)
            # Without a durable transaction there is nothing to persist, so the
            # append-only delta must not accumulate in memory.
            for journal in self._journals.values():
                journal.mark_facts_persisted()
            return result
        if evidence.stream_id is None or evidence.stream_epoch is None:
            return EvidenceConflict(
                evidence_id=evidence.evidence_id,
                reason="durable execution evidence requires stream identity",
            )
        cumulative_fill = evidence.fill
        fills = evidence.fills or ((cumulative_fill,) if cumulative_fill else ())
        if any(
            bool(
                isinstance(fill.raw_payload, dict)
                and (
                    fill.raw_payload.get("is_cumulative")
                    or "cum_qty" in fill.raw_payload
                )
            )
            for fill in fills
        ):
            return EvidenceConflict(
                evidence_id=evidence.evidence_id,
                reason=(
                    "cumulative order reports cannot be recorded as account trades; "
                    "use cumulative_order"
                ),
            )

        key = evidence.scope.to_position_key()
        canon = key.canonical_id
        scope = AccountFactStreamScope.for_position_key(
            key,
            stream_id=evidence.stream_id,
            stream_epoch=evidence.stream_epoch,
        )
        try:
            evidence = _coverage_for_scope(evidence, scope)
        except ValueError as err:
            return EvidenceConflict(evidence_id=evidence.evidence_id, reason=str(err))
        if (
            evidence.coverage is not None
            and evidence.coverage.status == FactCoverageStatus.CONFIRMED
            and evidence.coverage_evidence is None
            and evidence.coverage.stream_scope is not None
            and evidence.coverage.stream_scope.environment == "live"
        ):
            return EvidenceConflict(
                evidence_id=evidence.evidence_id,
                reason="durable live coverage requires typed pagination provenance",
            )
        if evidence.stream_id and evidence.stream_epoch:
            self._active_streams.add(
                (
                    evidence.scope.environment,
                    evidence.scope.account_label,
                    evidence.stream_id,
                    evidence.stream_epoch,
                )
            )

        async with self._mutation_lock(key):
            if self._persistence_failed:
                raise RuntimeError(
                    "Execution persistence failed; restore is required before ingest"
                )
            # A truly flat position on exchange with no active local exposure,
            # reservations, or pending commands can adopt or confirm the stream
            # epoch in memory without cloning the book or opening a database
            # transaction. This eliminates thousands of redundant staged copies
            # and transactions on every snapshot cycle.
            current_book = self._books.get(canon)
            is_local_flat = (
                current_book is None
                or (
                    current_book.get_view().total_quantity == Decimal("0")
                    and not current_book.get_view().batches
                    and not current_book.get_view().unallocated_quantity
                )
            )
            is_evidence_flat = (
                evidence.snapshot is not None
                and evidence.snapshot.position_amt == Decimal("0")
                and not (evidence.fills or evidence.fill)
            )
            has_no_reservations = not bool(self.get_active_reservations(key))
            has_no_commands = not any(
                getattr(cmd, "key", None) == key
                for cmd in self._outbox_by_command_id.values()
            )
            current_scope = self._stream_scopes.get(canon)
            is_legacy_stream = (
                current_scope is not None
                and current_scope.stream_id == "legacy-postgres-account"
            )
            is_truly_flat = (
                (is_local_flat or is_legacy_stream)
                and is_evidence_flat
                and has_no_reservations
                and has_no_commands
                and evidence.coverage_evidence is None
                and evidence.stream_checkpoint_adoption is None
            )
            if is_truly_flat:
                if (
                    canon not in self._journals
                    or self._stream_scopes.get(canon) != scope
                ):
                    self._journals[canon] = AccountJournal(key, stream_scope=scope)
                    self._books[canon] = PositionBook(self._journals[canon])
                    self._journal_revisions[canon] = 0
                    self._last_sequences.pop(canon, None)
                self._stream_scopes[canon] = scope
                return Applied(
                    evidence_id=evidence.evidence_id,
                    updated_view_token=self._books[canon].get_view().projection_version,
                )

            # A restored non-flat position from an earlier stream cannot be adopted by
            # an ordinary snapshot. Reject it before cloning its journal or
            # opening a transaction; account snapshots may contain thousands
            # of historical symbols on every refresh.
            current_scope = self._stream_scopes.get(canon)
            current_book = self._books.get(canon)
            can_rollover = (
                current_book is not None
                and evidence.snapshot is not None
                and evidence.snapshot.position_amt == current_book.get_view().total_quantity
                and not bool(self.get_active_reservations(key))
            )
            if (
                current_scope is not None
                and current_scope != scope
                and not can_rollover
                and (
                    evidence.coverage_evidence is None
                    or evidence.fill_load_provenance is None
                )
            ):
                return EvidenceConflict(
                    evidence_id=evidence.evidence_id,
                    reason=(
                        "stream epoch changed without a complete "
                        "source-anchored fill scan"
                    ),
                )
            candidate = self._staged_copy(key=key)
            try:
                async with self._execution_unit_of_work.transaction(key) as tx:
                    head = await tx.load_head(key)
                    adopting_epoch = False
                    if head is None:
                        expected_head_revision = 0
                        if self._head_revisions.get(canon, 0) != 0:
                            raise RuntimeError(
                                "local execution head exists but durable head is missing"
                            )
                    else:
                        expected_head_revision = head.revision
                        if self._head_revisions.get(canon) != head.revision:
                            raise RuntimeError(
                                "execution head changed in another process; restore required"
                            )
                        if (
                            head.stream_id != scope.stream_id
                            or head.stream_epoch != scope.stream_epoch
                        ):
                            adopting_epoch = True
                            if (
                                not can_rollover
                                and (
                                    evidence.coverage_evidence is None
                                    or evidence.fill_load_provenance is None
                                )
                            ):
                                raise _AbortObservation(
                                    EvidenceConflict(
                                        evidence_id=evidence.evidence_id,
                                        reason=(
                                            "stream epoch changed without a complete "
                                            "source-anchored fill scan"
                                        ),
                                    )
                                )
                            current_book = candidate._books.get(canon)
                            if (
                                current_book is None
                                or current_book.get_view().projection_version
                                != head.projection_version
                            ):
                                raise _AbortObservation(
                                    EvidenceConflict(
                                        evidence_id=evidence.evidence_id,
                                        reason=(
                                            "local position facts do not match the "
                                            "durable head before stream adoption"
                                        ),
                                    )
                                )
                        else:
                            current_view = candidate._ensure_book(key).get_view()
                            if current_view.projection_version != head.projection_version:
                                raise RuntimeError(
                                    "local position facts do not match durable execution head"
                                )

                    current_scope = candidate._stream_scopes.get(canon)
                    if current_scope is not None and current_scope != scope:
                        adopting_epoch = True
                        if (
                            not can_rollover
                            and (
                                evidence.coverage_evidence is None
                                or evidence.fill_load_provenance is None
                            )
                        ):
                            raise _AbortObservation(
                                EvidenceConflict(
                                    evidence_id=evidence.evidence_id,
                                    reason=(
                                        "stream epoch changed without a complete "
                                        "source-anchored fill scan"
                                    ),
                                )
                            )
                        if not adopting_epoch:
                            raise RuntimeError("stream adoption state is inconsistent")

                    adoption = evidence.stream_checkpoint_adoption
                    if adoption is not None:
                        if not adopting_epoch or head is None:
                            raise _AbortObservation(
                                EvidenceConflict(
                                    evidence_id=evidence.evidence_id,
                                    reason=(
                                        "checkpoint adoption requires an existing "
                                        "durable parent stream head"
                                    ),
                                )
                            )
                        parent_scope = AccountFactStreamScope.for_position_key(
                            key,
                            stream_id=head.stream_id,
                            stream_epoch=head.stream_epoch,
                        )
                        local_journal = candidate._journals.get(canon)
                        local_parent = (
                            local_journal.read_cut().recovery_checkpoint
                            if local_journal is not None
                            and local_journal.stream_scope == parent_scope
                            else None
                        )
                        if (
                            adoption.target_scope != scope
                            or adoption.parent_checkpoint.stream_scope != parent_scope
                            or adoption.parent_checkpoint != local_parent
                        ):
                            raise _AbortObservation(
                                EvidenceConflict(
                                    evidence_id=evidence.evidence_id,
                                    reason=(
                                        "checkpoint adoption does not bind the current "
                                        "durable parent checkpoint"
                                    ),
                                )
                            )
                        persisted_parent = await tx.load_checkpoint_by_id(
                            scope=parent_scope,
                            checkpoint_id=adoption.parent_checkpoint.checkpoint_id,
                        )
                        if persisted_parent != adoption.parent_checkpoint:
                            raise _AbortObservation(
                                EvidenceConflict(
                                    evidence_id=evidence.evidence_id,
                                    reason=(
                                        "checkpoint adoption parent differs from "
                                        "its immutable durable checkpoint"
                                    ),
                                )
                            )

                    if adopting_epoch:
                        if can_rollover:
                            candidate._stream_scopes[canon] = scope
                            journal = candidate._journals.get(canon)
                            if journal is not None:
                                journal.adopt_stream_scope(scope)
                            candidate._recovery_adoption_scope = scope
                            candidate._last_sequences.pop(canon, None)
                        else:
                            candidate._journals[canon] = AccountJournal(
                                key, stream_scope=scope
                            )
                            candidate._books[canon] = PositionBook(
                                candidate._journals[canon]
                            )
                            candidate._stream_scopes[canon] = scope
                            candidate._journal_revisions[canon] = 0
                            candidate._last_sequences.pop(canon, None)
                            candidate._recovery_adoption_scope = scope
                    else:
                        candidate._journal_for_scope(key, scope)
                    try:
                        accepted = await tx.record_evidence(
                            key=key,
                            stream_id=scope.stream_id,
                            stream_epoch=scope.stream_epoch,
                            evidence=ExecutionEvidenceIdentity(
                                evidence_id=evidence.evidence_id,
                                payload_digest=_digest_json_payload(_canonical_evidence_payload(evidence)),
                                accepted_at=evidence.observed_at,
                                sequence=evidence.sequence,
                            ),
                        )
                    except DecisionCommitConflict as err:
                        raise _AbortObservation(
                            EvidenceConflict(
                                evidence_id=evidence.evidence_id,
                                reason=str(err),
                            )
                        ) from err
                    if not accepted:
                        return Duplicate(
                            evidence_id=evidence.evidence_id,
                            view_token=candidate._ensure_book(key)
                            .get_view()
                            .projection_version,
                        )

                    # Check the durable identity before sequence monotonicity.
                    # An exact retry after restart carries its original sequence
                    # and must be acknowledged as a duplicate. A different event
                    # at that sequence is still rejected below.
                    previous_sequence = candidate._last_sequences.get(canon)
                    if (
                        not adopting_epoch
                        and evidence.sequence is not None
                        and previous_sequence is not None
                        and evidence.sequence <= previous_sequence
                    ):
                        raise _AbortObservation(
                            EvidenceConflict(
                                evidence_id=evidence.evidence_id,
                                reason=(
                                    f"account event sequence {evidence.sequence} does not "
                                    f"advance prior sequence {previous_sequence}"
                                ),
                            )
                        )

                    facts_before = candidate._ensure_journal(key).read_cut()
                    fills_to_record = evidence.fills or (
                        (evidence.fill,) if evidence.fill is not None else ()
                    )
                    for fill in fills_to_record:
                        try:
                            identity_inserted = await tx.record_trade(
                                key=key,
                                stream_id=scope.stream_id,
                                stream_epoch=scope.stream_epoch,
                                trade=ExecutionTradeIdentity(
                                    trade_id=fill.trade_id,
                                    order_id=fill.order_id,
                                    quantity=fill.quantity,
                                    price=fill.price,
                                    side=fill.side,
                                    payload_digest=_trade_payload_digest(fill),
                                    first_seen_at=fill.trade_at,
                                ),
                            )
                        except DecisionCommitConflict as err:
                            raise _AbortObservation(
                                EvidenceConflict(
                                    evidence_id=evidence.evidence_id,
                                    reason=str(err),
                                )
                            ) from err
                        prior_fill = next(
                            (
                                item
                                for item in facts_before.fills
                                if item.trade_id == fill.trade_id
                            ),
                            None,
                        )
                        if (
                            not identity_inserted
                            and prior_fill is None
                            and not adopting_epoch
                        ):
                            raise _AbortObservation(
                                EvidenceConflict(
                                    evidence_id=evidence.evidence_id,
                                    reason=(
                                        f"trade {fill.trade_id} was already consumed but "
                                        "its journal facts are unavailable"
                                    ),
                                )
                            )

                    candidate._active_transaction = tx
                    result = await self._observe_grouped(candidate, evidence)
                    if isinstance(result, EvidenceConflict):
                        raise _AbortObservation(result)
                    checkpoint = None
                    if (
                        evidence.coverage_evidence is not None
                        and evidence.fill_load_provenance is not None
                    ):
                        checkpoint = create_verified_recovery_checkpoint(
                            key=key,
                            scope=scope,
                            journal=candidate._ensure_journal(key),
                            proof=evidence.coverage_evidence,
                            provenance=evidence.fill_load_provenance,
                            adoption=evidence.stream_checkpoint_adoption,
                            adopting_epoch=adopting_epoch,
                        )
                    if adopting_epoch and not can_rollover and checkpoint is None:
                        raise _AbortObservation(
                            EvidenceConflict(
                                evidence_id=evidence.evidence_id,
                                reason=(
                                    "stream adoption did not produce a validated "
                                    "recovery checkpoint"
                                ),
                            )
                        )
                    journal = candidate._ensure_journal(key)
                    if checkpoint is not None:
                        journal.set_recovery_checkpoint(checkpoint)
                    facts = journal.read_cut()
                    persist_result = await tx.persist_facts(
                        scope=scope,
                        facts=facts,
                        revision=journal.revision,
                        checkpoint=checkpoint,
                        delta=journal.pending_fact_delta(),
                    )
                    if getattr(persist_result, "has_conflicts", False):
                        raise _AbortObservation(
                            EvidenceConflict(
                                evidence_id=evidence.evidence_id,
                                reason="durable account journal reported a fact conflict",
                            )
                        )
                    if hasattr(persist_result, "revision"):
                        candidate._journal_revisions[canon] = persist_result.revision
                    if evidence.sequence is not None:
                        candidate._last_sequences[canon] = evidence.sequence

                    watermark_prefix = f"{key.canonical_id}\x1f"
                    changed_orders = {
                        watermark_key[len(watermark_prefix) :]
                        for watermark_key, quantity in candidate._order_cumulative_fills.items()
                        if watermark_key.startswith(watermark_prefix)
                        and (
                            quantity
                            != self._order_cumulative_fills.get(
                                watermark_key, Decimal("0")
                            )
                            or candidate._order_cumulative_quotes.get(
                                watermark_key, Decimal("0")
                            )
                            != self._order_cumulative_quotes.get(
                                watermark_key, Decimal("0")
                            )
                        )
                    }
                    for order_id in sorted(changed_orders):
                        watermark_key = self._order_watermark_key(key, order_id)
                        await tx.persist_watermark(
                            key=key,
                            stream_id=scope.stream_id,
                            stream_epoch=scope.stream_epoch,
                            watermark=ExecutionWatermark(
                                order_id=order_id,
                                cumulative_quantity=candidate._order_cumulative_fills[
                                    watermark_key
                                ],
                                cumulative_quote=candidate._order_cumulative_quotes.get(
                                    watermark_key, Decimal("0")
                                ),
                                updated_at=evidence.observed_at,
                            ),
                        )
                    head_payload = _execution_head_payload(
                        candidate, key, facts.compute_facts_hash()
                    )
                    candidate._head_revisions[canon] = await tx.persist_head(
                        key=key,
                        stream_id=scope.stream_id,
                        stream_epoch=scope.stream_epoch,
                        expected_revision=expected_head_revision,
                        projection_version=candidate._ensure_book(key)
                        .get_view(now=evidence.observed_at)
                        .projection_version,
                        state_payload=head_payload,
                        updated_at=evidence.observed_at,
                        stream_adoption_checkpoint_id=(
                            checkpoint.checkpoint_id
                            if adopting_epoch and checkpoint is not None
                            else None
                        ),
                        is_flat_adoption=can_rollover,
                    )
                    candidate._head_projection_digests[canon] = str(
                        head_payload["projection_digest"]
                    )
                candidate._active_transaction = None
                self._publish_candidate(candidate)
                return result
            except _AbortObservation as abort:
                return abort.result
            except Exception as err:
                self._persistence_failed = True
                log.error(
                    "atomic_execution_observation_failed",
                    position_key=key.canonical_id,
                    evidence_id=evidence.evidence_id,
                    error=str(err),
                )
                raise RuntimeError(
                    f"execution evidence was not durably accepted: {err}"
                ) from err

    async def adopt_stream_checkpoint(
        self,
        evidence: ExecutionEvidence,
    ) -> ExecutionObserveResult:
        """Adopt a stream only from a complete, typed source recovery proof."""
        if self._execution_unit_of_work is None:
            return EvidenceConflict(
                evidence_id=evidence.evidence_id,
                reason="stream checkpoint adoption requires a durable execution book",
            )
        if (
            evidence.coverage_evidence is None
            or evidence.fill_load_provenance is None
            or evidence.stream_id is None
            or evidence.stream_epoch is None
        ):
            return EvidenceConflict(
                evidence_id=evidence.evidence_id,
                reason="stream checkpoint adoption requires typed source provenance",
            )
        return await self.observe(evidence)

    async def _observe_grouped(
        self,
        target: ExecutionBook,
        evidence: ExecutionEvidence,
    ) -> ExecutionObserveResult:
        fills = evidence.fills or ((evidence.fill,) if evidence.fill else ())
        if not fills:
            return await target._observe_mutating(evidence)
        consumed = Decimal("0")
        released = Decimal("0")
        recovery_required = False
        diagnostics: list[str] = []
        last_result: Applied | Duplicate | None = None
        for fill in fills:
            internal_id = f"{evidence.evidence_id}\x1ftrade:{fill.trade_id}"
            one_fill = replace(
                evidence,
                evidence_id=internal_id,
                fill=fill,
                fills=(),
                snapshot=None,
                boundary=None,
                order_event=None,
                coverage=None,
                coverage_evidence=None,
                fill_load_provenance=None,
                stream_checkpoint_adoption=None,
                cumulative_order=None,
            )
            fill_result = await target._observe_mutating(one_fill)
            target._seen_evidence_ids.discard(_evidence_identity(one_fill))
            if isinstance(fill_result, EvidenceConflict):
                return replace(fill_result, evidence_id=evidence.evidence_id)
            last_result = fill_result
            if isinstance(fill_result, Applied):
                consumed += fill_result.consumed_quantity
                released += fill_result.released_quantity
                recovery_required = recovery_required or fill_result.recovery_required
                diagnostics.extend(fill_result.diagnostics)

        remainder = replace(evidence, fill=None, fills=())
        base_result = await target._observe_mutating(remainder)
        if isinstance(base_result, EvidenceConflict):
            return base_result
        if isinstance(base_result, Duplicate):
            return base_result
        if isinstance(base_result, Applied):
            consumed += base_result.consumed_quantity
            released += base_result.released_quantity
            recovery_required = recovery_required or base_result.recovery_required
            diagnostics.extend(base_result.diagnostics)
            return replace(
                base_result,
                consumed_quantity=consumed,
                released_quantity=released,
                recovery_required=recovery_required,
                diagnostics=tuple(dict.fromkeys(diagnostics)),
            )
        if isinstance(last_result, Applied):
            return replace(
                last_result,
                evidence_id=evidence.evidence_id,
                consumed_quantity=consumed,
                released_quantity=released,
                recovery_required=recovery_required,
                diagnostics=tuple(dict.fromkeys(diagnostics)),
            )
        return base_result

    async def _observe_mutating(
        self,
        evidence: ExecutionEvidence,
    ) -> ExecutionObserveResult:
        """Idempotently ingests exchange evidence and settles allocations."""
        identity = _evidence_identity(evidence)
        if identity in self._seen_evidence_ids:
            key = evidence.scope.to_position_key()
            book = self._ensure_book(key)
            return Duplicate(
                evidence_id=evidence.evidence_id,
                view_token=book.get_view().projection_version,
            )

        key = evidence.scope.to_position_key()
        if evidence.stream_id is not None and evidence.stream_epoch is not None:
            scope = AccountFactStreamScope.for_position_key(
                key,
                stream_id=evidence.stream_id,
                stream_epoch=evidence.stream_epoch,
            )
            current_scope = self._stream_scopes.get(key.canonical_id)
            if current_scope is not None and current_scope != scope:
                current_book = self._books.get(key.canonical_id)
                can_rollover = (
                    current_book is not None
                    and evidence.snapshot is not None
                    and evidence.snapshot.position_amt == current_book.get_view().total_quantity
                    and not bool(self.get_active_reservations(key))
                )
                if can_rollover:
                    self._stream_scopes[key.canonical_id] = scope
                    journal = self._journals.get(key.canonical_id)
                    if journal is not None:
                        journal.adopt_stream_scope(scope)
                    self._last_sequences.pop(key.canonical_id, None)
                else:
                    return EvidenceConflict(
                        evidence_id=evidence.evidence_id,
                        reason=(
                            "execution stream changed; a validated recovery checkpoint "
                            "must be adopted before this position can continue"
                        ),
                    )
            try:
                journal = self._journal_for_scope(key, scope)
            except RuntimeError as err:
                return EvidenceConflict(evidence.evidence_id, str(err))
        else:
            journal = self._ensure_journal(key)
        book = self._ensure_book(key)
        if evidence.fill_load_provenance is not None:
            recorder = getattr(journal, "record_fill_load_provenance", None)
            if not callable(recorder):
                raise RuntimeError(
                    "account journal cannot persist fill-load provenance"
                )
            recorder(evidence.fill_load_provenance)

        consumed_qty = Decimal("0")
        released_qty = Decimal("0")
        settlement_recovery_required = False
        diagnostics: tuple[str, ...] = ()
        pending_watermark: tuple[str, Decimal, Decimal] | None = None
        dispatch_reconciled_command_id: str | None = None
        observed_fills = evidence.fills or (
            (evidence.fill,) if evidence.fill is not None else ()
        )

        # 1. Process Fill
        if evidence.fill is not None:
            fill = evidence.fill
            trade_id = fill.trade_id
            order_id = fill.order_id
            raw_payload = fill.raw_payload if isinstance(fill.raw_payload, dict) else {}
            is_cumulative = bool(
                raw_payload.get("is_cumulative") or "cum_qty" in raw_payload
            )
            delta_qty = fill.quantity
            settlement_delta_qty = delta_qty
            adopted_prefix_trade = False
            applied_fill = fill
            if is_cumulative:
                cumulative_qty = Decimal(str(raw_payload.get("cum_qty", fill.quantity)))
                cumulative_quote = Decimal(
                    str(raw_payload.get("cum_quote", cumulative_qty * fill.price))
                )
                if (
                    not cumulative_qty.is_finite()
                    or not cumulative_quote.is_finite()
                    or cumulative_qty < Decimal("0")
                    or cumulative_quote < Decimal("0")
                ):
                    return EvidenceConflict(
                        evidence_id=evidence.evidence_id,
                        reason="Cumulative fill quantity or quote is invalid",
                    )
                watermark_key = self._order_watermark_key(key, order_id)
                previous_cumulative = self._order_cumulative_fills.get(
                    watermark_key, Decimal("0")
                )
                previous_quote = self._order_cumulative_quotes.get(
                    watermark_key, Decimal("0")
                )
                delta_qty = cumulative_qty - previous_cumulative
                if delta_qty < Decimal("0"):
                    # An older exchange report is harmless: it must not rewind
                    # the high-water mark or change the current position view.
                    delta_qty = Decimal("0")
                elif delta_qty == Decimal("0"):
                    if cumulative_quote != previous_quote:
                        return EvidenceConflict(
                            evidence_id=evidence.evidence_id,
                            reason=(
                                "Cumulative quote changed without a quantity change"
                            ),
                        )
                else:
                    delta_quote = cumulative_quote - previous_quote
                    if delta_quote <= Decimal("0"):
                        return EvidenceConflict(
                            evidence_id=evidence.evidence_id,
                            reason=(
                                "Cumulative quote did not increase with cumulative "
                                "quantity"
                            ),
                        )
                    if trade_id in self._seen_trade_ids:
                        return EvidenceConflict(
                            evidence_id=evidence.evidence_id,
                            reason=(
                                f"Cumulative fill identity {trade_id} was reused with "
                                "a higher cumulative quantity"
                            ),
                        )
                    applied_fill = replace(
                        fill,
                        quantity=delta_qty,
                        price=delta_quote / delta_qty,
                    )
                    pending_watermark = (
                        watermark_key,
                        cumulative_qty,
                        cumulative_quote,
                    )
                settlement_delta_qty = delta_qty

            existing_trade = next(
                (
                    prior
                    for prior in journal.read_cut().fills
                    if prior.trade_id == trade_id
                ),
                None,
            )
            if not is_cumulative and existing_trade is not None:
                if (
                    existing_trade.quantity != fill.quantity
                    or existing_trade.price != fill.price
                    or existing_trade.side.upper() != fill.side.upper()
                    or existing_trade.symbol != fill.symbol
                ):
                    return EvidenceConflict(
                        evidence_id=evidence.evidence_id,
                        reason=(
                            f"Fill {trade_id} conflicts with existing journal records"
                        ),
                    )
                # A repeated exchange trade can arrive with a new transport
                # evidence ID. The trade ID, rather than the evidence ID, owns
                # fill quantity and reservation settlement.
                delta_qty = Decimal("0")
                settlement_delta_qty = Decimal("0")
            elif not is_cumulative and trade_id in self._seen_trade_ids:
                if (
                    self._active_transaction is not None
                    and self._recovery_adoption_scope == journal.stream_scope
                    and journal.stream_scope is not None
                ):
                    # A complete new-epoch fill prefix can repeat globally known
                    # trade identities. UoW identity comparison has already
                    # verified the exact payload; it belongs in this epoch's
                    # journal but must not settle reservations a second time.
                    adopted_prefix_trade = True
                    settlement_delta_qty = Decimal("0")
                else:
                    return EvidenceConflict(
                        evidence_id=evidence.evidence_id,
                        reason=(
                            f"Fill {trade_id} was already seen but its journal facts "
                            "are unavailable; recovery is required"
                        ),
                    )

            is_new_trade = trade_id not in self._seen_trade_ids or adopted_prefix_trade
            if is_new_trade:
                if delta_qty > Decimal("0"):
                    accepted = journal.append_fill(applied_fill)
                    if not accepted and journal.has_conflicts:
                        return EvidenceConflict(
                            evidence_id=evidence.evidence_id,
                            reason=(
                                f"Fill {fill.trade_id} conflicted with "
                                "existing journal records"
                            ),
                        )
                    if not adopted_prefix_trade:
                        self._seen_trade_ids.add(trade_id)
                elif not is_cumulative:
                    self._seen_trade_ids.add(trade_id)

            if (
                not is_cumulative
                and is_new_trade
                and self._active_transaction is not None
                and not adopted_prefix_trade
            ):
                watermark_key = self._order_watermark_key(key, order_id)
                previous_quantity = self._order_cumulative_fills.get(
                    watermark_key, Decimal("0")
                )
                previous_quote = self._order_cumulative_quotes.get(
                    watermark_key, Decimal("0")
                )
                real_order_fills = tuple(
                    item
                    for item in journal.read_cut().fills
                    if item.order_id == order_id
                )
                real_quantity = sum(
                    (item.quantity for item in real_order_fills), Decimal("0")
                )
                real_quote = sum(
                    (item.quantity * item.price for item in real_order_fills),
                    Decimal("0"),
                )
                settlement_delta_qty = max(
                    Decimal("0"), real_quantity - previous_quantity
                )
                if settlement_delta_qty > Decimal("0"):
                    if real_quote <= previous_quote:
                        return EvidenceConflict(
                            evidence_id=evidence.evidence_id,
                            reason=(
                                "real account trade quote does not advance its "
                                "durable order watermark"
                            ),
                        )
                    pending_watermark = (
                        watermark_key,
                        real_quantity,
                        real_quote,
                    )

            active_episode = book.get_view().active_episode
            is_exit_fill = (
                (
                    fill.side.upper() == "SELL"
                    and key.position_side == FuturesPositionSide.LONG
                )
                or (
                    fill.side.upper() == "BUY"
                    and key.position_side == FuturesPositionSide.SHORT
                )
                or bool(self._find_active_reservations_for_command(order_id))
                or bool(raw_payload.get("reduce_only"))
                or (
                    evidence.order_event is not None
                    and bool((evidence.order_event.details or {}).get("is_reduce_only"))
                )
                or (
                    active_episode is not None
                    and (
                        (
                            active_episode.side == StrategySide.LONG
                            and fill.side.upper() == "SELL"
                        )
                        or (
                            active_episode.side == StrategySide.SHORT
                            and fill.side.upper() == "BUY"
                        )
                    )
                )
            )
            if is_exit_fill and settlement_delta_qty > Decimal("0"):
                (
                    consumed,
                    needs_recovery,
                    settlement_diagnostic,
                ) = await self._settle_reservation_quantity(
                    order_id,
                    settlement_delta_qty,
                    reported_quantity=(
                        cumulative_qty if is_cumulative else settlement_delta_qty
                    ),
                )
                consumed_qty += consumed
                settlement_recovery_required = (
                    settlement_recovery_required or needs_recovery
                )
                if settlement_diagnostic:
                    diagnostics = (settlement_diagnostic,)

        report = evidence.cumulative_order
        if report is not None:
            watermark_key = self._order_watermark_key(key, report.order_id)
            previous_quantity = self._order_cumulative_fills.get(
                watermark_key, Decimal("0")
            )
            previous_quote = self._order_cumulative_quotes.get(
                watermark_key, Decimal("0")
            )
            real_order_fills = tuple(
                item
                for item in journal.read_cut().fills
                if item.order_id == report.order_id
            )
            real_quantity = sum(
                (item.quantity for item in real_order_fills), Decimal("0")
            )
            real_quote = sum(
                (item.quantity * item.price for item in real_order_fills),
                Decimal("0"),
            )
            target_quantity = max(
                previous_quantity,
                report.cumulative_quantity,
                real_quantity,
            )
            if target_quantity > previous_quantity:
                target_quote = (
                    report.cumulative_quote
                    if report.cumulative_quantity >= real_quantity
                    else real_quote
                )
                if target_quote <= previous_quote:
                    return EvidenceConflict(
                        evidence_id=evidence.evidence_id,
                        reason="cumulative order quote watermark did not advance",
                    )
                delta_quantity = target_quantity - previous_quantity
                pending_watermark = (
                    watermark_key,
                    target_quantity,
                    target_quote,
                )
                outbox = self._outbox_by_command_id.get(report.order_id)
                is_exit_report = bool(
                    self._find_active_reservations_for_command(report.order_id)
                    or (outbox is not None and outbox.command.reduce_only)
                )
                if is_exit_report:
                    (
                        consumed,
                        needs_recovery,
                        settlement_diagnostic,
                    ) = await self._settle_reservation_quantity(
                        report.order_id,
                        delta_quantity,
                        reported_quantity=target_quantity,
                    )
                    consumed_qty += consumed
                    settlement_recovery_required = (
                        settlement_recovery_required or needs_recovery
                    )
                    if settlement_diagnostic:
                        diagnostics = (settlement_diagnostic,)

        # 2. Process Snapshot
        if evidence.snapshot is not None:
            journal.record_snapshot(evidence.snapshot)

        # 2b. Process Coverage
        if getattr(evidence, "coverage", None) is not None:
            journal.set_coverage(evidence.coverage)

        # 3. Process Boundary
        if evidence.boundary is not None:
            journal.record_boundary(evidence.boundary)

        # 4. Process Order Event
        if evidence.order_event is not None:
            ev_state = evidence.order_event.state
            cmd_id = evidence.order_event.client_order_id
            outbox = self._outbox_by_command_id.get(cmd_id)
            already_terminal = outbox is not None and outbox.state in (
                DispatchState.TERMINAL,
                DispatchState.REJECTED,
            )

            if outbox is not None and not already_terminal:
                if ev_state in (
                    ExchangeOrderState.ACKNOWLEDGED,
                    ExchangeOrderState.SUBMITTED,
                ) and outbox.state in (
                    DispatchState.PREPARED,
                    DispatchState.DISPATCHING,
                    DispatchState.UNKNOWN,
                ):
                    await self._persist_transition(
                        outbox,
                        replace(
                            outbox,
                            state=DispatchState.ACKNOWLEDGED,
                            updated_at=evidence.observed_at,
                        ),
                    )
                elif ev_state in (
                    ExchangeOrderState.CANCELED,
                    ExchangeOrderState.EXPIRED,
                    ExchangeOrderState.REJECTED,
                    ExchangeOrderState.ABSENT_RECONCILED,
                    ExchangeOrderState.FILLED,
                ):
                    target_state = (
                        DispatchState.REJECTED
                        if ev_state == ExchangeOrderState.REJECTED
                        else DispatchState.TERMINAL
                    )
                    updated = replace(
                        outbox,
                        state=target_state,
                        last_error=(
                            f"Order {ev_state.value}"
                            if ev_state != ExchangeOrderState.FILLED
                            else outbox.last_error
                        ),
                        updated_at=evidence.observed_at,
                    )
                    await self._persist_transition(outbox, updated)
                    if ev_state == ExchangeOrderState.FILLED:
                        confirmed_trade_quantity = sum(
                            (
                                fill.quantity
                                for fill in journal.read_cut().fills
                                if fill.order_id == cmd_id
                            ),
                            Decimal("0"),
                        )
                        if (
                            confirmed_trade_quantity
                            >= outbox.command.requested_quantity
                            or self._execution_unit_of_work is None
                        ):
                            released_qty += await self._release_command_reservations(
                                cmd_id,
                                reason=(
                                    "order_filled_with_confirmed_trades"
                                    if confirmed_trade_quantity
                                    >= outbox.command.requested_quantity
                                    else "order_finished_filled"
                                ),
                            )
                        else:
                            self._recovery_required_commands.add(cmd_id)
                            settlement_recovery_required = True
                            diagnostics = (
                                "Filled terminal lacks complete account trade facts; "
                                "active reservation is retained for recovery",
                            )
                    else:
                        released_qty += await self._release_command_reservations(
                            cmd_id,
                            reason=f"order_finished_{ev_state.value.lower()}",
                        )
                    dispatch_reconciled_command_id = cmd_id
                elif ev_state == ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
                    self._dispatch_reconciliation_required_commands.add(cmd_id)
                    await self._persist_transition(
                        outbox,
                        replace(
                            outbox,
                            state=DispatchState.UNKNOWN,
                            last_error="Pending reconciliation",
                            updated_at=evidence.observed_at,
                        ),
                    )

        if pending_watermark is not None:
            watermark_key, cumulative_qty, cumulative_quote = pending_watermark
            self._order_cumulative_fills[watermark_key] = cumulative_qty
            self._order_cumulative_quotes[watermark_key] = cumulative_quote
            cmd_id = (
                evidence.cumulative_order.order_id
                if evidence.cumulative_order is not None
                else evidence.order_event.client_order_id
                if evidence.order_event is not None
                else evidence.fill.order_id
                if evidence.fill is not None
                else evidence.fills[0].order_id
                if evidence.fills
                else ""
            )
            current_entry = self._outbox_by_command_id.get(cmd_id)
            if current_entry is not None:
                await self._persist_outbox_state(current_entry)
            elif any(
                isinstance(fill.raw_payload, dict)
                and (
                    fill.raw_payload.get("is_cumulative")
                    or "cum_qty" in fill.raw_payload
                )
                for fill in observed_fills
            ):
                self._recovery_required_commands.add(cmd_id)
                settlement_recovery_required = True
                diagnostics = (
                    f"No outbox command exists for cumulative fill {cmd_id}",
                )

        if dispatch_reconciled_command_id is not None:
            self._dispatch_reconciliation_required_commands.discard(
                dispatch_reconciled_command_id
            )

        self._seen_evidence_ids.add(identity)
        updated_view = book.get_view(now=evidence.observed_at)

        return Applied(
            evidence_id=evidence.evidence_id,
            updated_view_token=updated_view.projection_version,
            consumed_quantity=consumed_qty,
            released_quantity=released_qty,
            recovery_required=settlement_recovery_required,
            diagnostics=diagnostics,
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
