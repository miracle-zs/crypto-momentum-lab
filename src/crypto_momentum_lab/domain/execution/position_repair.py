"""Compute safe position repairs and validate committed reloads."""

from dataclasses import replace
from decimal import Decimal

from crypto_momentum_lab.domain.execution.execution_head import (
    EXECUTION_HEAD_SCHEMA_VERSION,
    decode_execution_head_state,
)
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
from crypto_momentum_lab.domain.execution.projection_codec import encode_position_key


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
    fills_by_id = {fill.trade_id: fill for fill in loaded.cut.facts.fills}
    inserted = 0
    for fill in loaded.account_fills:
        previous = fills_by_id.get(fill.trade_id)
        if previous is not None:
            if previous != fill:
                raise PositionRepairBlocked(
                    "account fill identity conflicts with loaded position facts"
                )
            continue
        fills_by_id[fill.trade_id] = fill
        inserted += 1
    facts = replace(
        loaded.cut.facts,
        fills=tuple(
            sorted(fills_by_id.values(), key=lambda fill: (fill.trade_at, fill.trade_id))
        ),
    )
    view = PositionBook.from_facts(facts).get_view()
    if not view.is_ready_for_trade or view.total_quantity != request.expected_quantity:
        raise PositionRepairBlocked(
            "repair projection does not match actual account exposure"
        )
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
    if head is None:
        reservations = []
        sequence = None
        recovery_command_ids = []
        external_recovery_ids = []
    else:
        reservations = previous.get("active_reservation_ids")
        if not isinstance(reservations, list) or any(
            not isinstance(r, str) or not r for r in reservations
        ):
            raise PositionRepairBlocked("durable reservation identities are malformed")
        recovery_command_ids = previous.get("recovery_command_ids")
        if not isinstance(recovery_command_ids, list) or any(
            not isinstance(identity, str) or not identity
            for identity in recovery_command_ids
        ):
            raise PositionRepairBlocked(
                "durable recovery command identities are malformed"
            )
        external_recovery_ids = previous.get("external_recovery_ids")
        if not isinstance(external_recovery_ids, list) or any(
            not isinstance(identity, str) or not identity
            for identity in external_recovery_ids
        ):
            raise PositionRepairBlocked(
                "durable external recovery identities are malformed"
            )
        if "last_sequence" not in previous:
            raise PositionRepairBlocked("durable sequence is missing")
        sequence = previous["last_sequence"]
        if sequence is not None and (type(sequence) is not int or sequence < 0):
            raise PositionRepairBlocked("durable sequence is malformed")
    payload = {
        "schema_version": EXECUTION_HEAD_SCHEMA_VERSION,
        "position_key": encode_position_key(request.key),
        "stream_scope": dict(
            stream_id=request.scope.stream_id, stream_epoch=request.scope.stream_epoch
        ),
        "position_state": PositionBook.from_facts(facts).encode_current_state(),
        "last_sequence": sequence,
        "active_reservation_ids": list(reservations),
        "recovery_command_ids": list(recovery_command_ids),
        "external_recovery_ids": list(external_recovery_ids),
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
    if head is None or head.revision < 1 or (
        head.stream_id,
        head.stream_epoch,
    ) != (scope.stream_id, scope.stream_epoch):
        raise PositionRepairBlocked("reload requires a matching durable head")
    book = decode_execution_head_state(head.state_payload, key=key, scope=scope)
    view = book.get_view()
    if (
        expected_quantity is None
        or view.total_quantity != expected_quantity
        or not view.is_ready_for_trade
    ):
        raise PositionRepairBlocked("reload does not prove current exposure")
    payload = head.state_payload
    if book.current_facts != state.cut.facts:
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
