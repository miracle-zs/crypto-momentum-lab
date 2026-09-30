"""Position repair values and transaction seam; no database or runtime imports."""

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.evidence_codec import (
    _recovery_checkpoint_head_binding,
    _view_projection_digest,
)
from crypto_momentum_lab.domain.execution.ports import (
    DurableExecutionPositionState,
    ExecutionHeadSnapshot,
)
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    AccountFactStreamScope,
    JournalFactDelta,
    PositionKey,
    PositionView,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut


class PositionRepairBlocked(RuntimeError):
    """Facts do not prove a safe repair; leave the position unmanaged."""


@dataclass(frozen=True, slots=True)
class PositionRepairRequest:
    key: PositionKey
    run_id: str
    scope: AccountFactStreamScope
    expected_quantity: Decimal
    observed_at: datetime

    def __post_init__(self):
        if not self.scope.matches(self.key) or not self.run_id.strip():
            raise ValueError("exact position, run and stream identities are required")
        if not self.expected_quantity.is_finite() or self.expected_quantity <= 0:
            raise ValueError("repair requires positive actual account exposure")
        if self.observed_at.tzinfo is None:
            raise ValueError("repair observation must be timezone-aware")


@dataclass(frozen=True, slots=True)
class PositionRepairFacts:
    cut: DurableJournalCut
    head: ExecutionHeadSnapshot | None
    account_fills: tuple[AccountFillEvent, ...]
    owned_order_ids: frozenset[str]


@dataclass(frozen=True, slots=True)
class PositionRepair:
    request: PositionRepairRequest
    facts: AccountFacts
    delta: JournalFactDelta
    revision: int
    expected_head_revision: int
    head_payload: dict[str, object]
    projection_version: str
    new_facts: int
    needs_write: bool


@dataclass(frozen=True, slots=True)
class PositionRepairReceipt:
    scope: AccountFactStreamScope
    head_revision: int
    projection_version: str
    changed: bool


class PositionRepairTransaction(Protocol):
    async def load_repair_facts(
        self, request: PositionRepairRequest
    ) -> PositionRepairFacts: ...
    async def persist_repair(self, repair: PositionRepair) -> PositionRepairReceipt: ...


class PositionRepairUnitOfWork(Protocol):
    # Lock is the SAME execution_position lock as normal execution writes.
    # All loads/writes share one transaction; success commits, exceptions roll back.
    def transaction(
        self, key: PositionKey
    ) -> AbstractAsyncContextManager[PositionRepairTransaction]: ...


class PositionRepairBook(Protocol):
    async def reload_position(
        self,
        key: PositionKey,
        *,
        expected_scope: AccountFactStreamScope,
        expected_quantity: Decimal,
    ) -> PositionView | None: ...


def build_position_repair(
    request: PositionRepairRequest, loaded: PositionRepairFacts
) -> PositionRepair:
    if not loaded.owned_order_ids:
        raise PositionRepairBlocked(
            "position ownership is not proven for this account/run"
        )
    if (
        loaded.cut.scope != request.scope
        or loaded.cut.facts.position_key != request.key
    ):
        raise PositionRepairBlocked(
            "repair facts have a different position/stream identity"
        )
    head = loaded.head
    if head is not None and (head.stream_id, head.stream_epoch) != (
        request.scope.stream_id,
        request.scope.stream_epoch,
    ):
        raise PositionRepairBlocked(
            "cross-epoch repair requires normal verified stream recovery"
        )
    if not loaded.account_fills:
        raise PositionRepairBlocked("complete account fill facts are missing")
    journal = AccountJournal.from_durable_cut(loaded.cut)
    inserted = sum(journal.append_fill(fill) for fill in loaded.account_fills)
    view = PositionBook(journal).get_view()
    if not view.is_ready_for_trade or view.total_quantity != request.expected_quantity:
        raise PositionRepairBlocked(
            "repair projection does not match actual account exposure"
        )
    facts = journal.read_cut()
    projection = PositionLedger(request.key).project(facts)
    episode = projection.active_episode
    opening_side = "SELL" if request.key.position_side.value == "SHORT" else "BUY"
    current_opening_fills = tuple(
        fill
        for fill in facts.fills
        if episode is not None
        and fill.trade_at >= episode.opened_at
        and fill.side.upper() == opening_side
    )
    if not current_opening_fills or any(
        fill.order_id not in loaded.owned_order_ids for fill in current_opening_fills
    ):
        raise PositionRepairBlocked(
            "current entry episode is not owned by this strategy run"
        )
    previous = head.state_payload if head is not None else {}
    reservations = previous.get("active_reservation_ids", [])
    if not isinstance(reservations, list) or any(
        not isinstance(r, str) or not r for r in reservations
    ):
        raise PositionRepairBlocked("durable reservation identities are malformed")
    sequence = previous.get("last_sequence")
    if sequence is not None and (type(sequence) is not int or sequence < 0):
        raise PositionRepairBlocked("durable sequence is malformed")
    payload = {
        "schema_version": 1,
        "position_key": dict(
            environment=request.key.environment,
            account_label=request.key.account_label,
            symbol=request.key.symbol,
            position_side=request.key.position_side.value,
        ),
        "stream_scope": dict(
            stream_id=request.scope.stream_id, stream_epoch=request.scope.stream_epoch
        ),
        "facts_hash": facts.compute_facts_hash(),
        "projection_digest": PositionRecoveryCodec.compute_projection_digest(
            PositionLedger(request.key).project(facts)
        ),
        "view_digest": _view_projection_digest(view),
        "recovery_checkpoint": _recovery_checkpoint_head_binding(
            facts.recovery_checkpoint
        ),
        "journal_revision": journal.revision,
        "last_sequence": sequence,
        "seen_trade_count": len(facts.fills),
        "active_reservation_ids": list(reservations),
    }
    needs_write = (
        head is None
        or inserted > 0
        or head.projection_version != view.projection_version
        or any(previous.get(k) != v for k, v in payload.items())
    )
    return PositionRepair(
        request,
        facts,
        journal.pending_fact_delta(),
        journal.revision,
        head.revision if head else 0,
        payload,
        view.projection_version,
        inserted,
        needs_write,
    )


def validate_repaired_position(
    *,
    state: DurableExecutionPositionState,
    scope: AccountFactStreamScope,
    expected_quantity: Decimal | None,
    reservation_ids: set[str],
) -> None:
    """Strict repair reload; failure must precede publication of mutable Book state."""
    key = PositionKey(
        scope.environment, scope.account_label, scope.symbol, scope.position_side
    )
    if (
        state.scope != scope
        or state.cut.scope != scope
        or state.cut.facts.position_key != key
    ):
        raise PositionRepairBlocked("reload position/stream identity mismatch")
    head = state.head
    if (
        head is None
        or head.revision < 1
        or (head.stream_id, head.stream_epoch) != (scope.stream_id, scope.stream_epoch)
    ):
        raise PositionRepairBlocked("reload requires a matching durable head")
    journal = AccountJournal.from_durable_cut(state.cut)
    view = PositionBook(journal).get_view()
    if (
        expected_quantity is None
        or view.total_quantity != expected_quantity
        or not view.is_ready_for_trade
    ):
        raise PositionRepairBlocked("reload does not prove current exposure")
    facts = journal.read_cut()
    payload = head.state_payload
    expected = {
        "schema_version": 1,
        "position_key": dict(
            environment=key.environment,
            account_label=key.account_label,
            symbol=key.symbol,
            position_side=key.position_side.value,
        ),
        "stream_scope": dict(
            stream_id=scope.stream_id, stream_epoch=scope.stream_epoch
        ),
        "facts_hash": facts.compute_facts_hash(),
        "projection_digest": PositionRecoveryCodec.compute_projection_digest(
            PositionLedger(key).project(facts)
        ),
        "view_digest": _view_projection_digest(view),
        "recovery_checkpoint": _recovery_checkpoint_head_binding(
            facts.recovery_checkpoint
        ),
        "journal_revision": journal.revision,
    }
    if any(payload.get(k) != v for k, v in expected.items()):
        raise PositionRepairBlocked(
            "reloaded durable head failed complete projection validation"
        )
    if head.projection_version != view.projection_version:
        raise PositionRepairBlocked("reloaded projection token mismatch")
    sequence = payload.get("last_sequence")
    if sequence is not None and (type(sequence) is not int or sequence < 0):
        raise PositionRepairBlocked("reloaded sequence is malformed")
    active = payload.get("active_reservation_ids")
    if (
        not isinstance(active, list)
        or any(not isinstance(value, str) or not value for value in active)
        or set(active) != reservation_ids
    ):
        raise PositionRepairBlocked("reloaded reservation links require full recovery")
