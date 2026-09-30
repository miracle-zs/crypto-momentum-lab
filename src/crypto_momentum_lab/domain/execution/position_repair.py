"""Compute safe position repairs and validate committed reloads."""

from decimal import Decimal

from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.evidence_codec import (
    recovery_checkpoint_head_binding,
)
from crypto_momentum_lab.domain.execution.evidence_digest import view_projection_digest
from crypto_momentum_lab.domain.execution.ports import (
    DurableExecutionPositionState,
)
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.position_repair_models import (
    PositionRepair,
    PositionRepairBlocked,
    PositionRepairFacts,
    PositionRepairRequest,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec


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
        "view_digest": view_projection_digest(view),
        "recovery_checkpoint": recovery_checkpoint_head_binding(
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
        "view_digest": view_projection_digest(view),
        "recovery_checkpoint": recovery_checkpoint_head_binding(
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
