"""Durable command payload encoding and recovery, without storage or Book state.

Malformed identity fails restoration. Existing skippable payload diagnostics are
returned explicitly, so callers preserve their logging and recovery policy.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution.command_lifecycle import (
    plan_command_transition,
)
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide


def _required_text(values: Mapping[str, object], field_name: str) -> str:
    value = values.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"execution command {field_name} is missing or invalid")
    return value


def _optional_text(values: Mapping[str, object], field_name: str) -> str | None:
    value = values.get(field_name)
    if value is not None and not isinstance(value, str):
        raise ValueError(f"execution command {field_name} must be text or null")
    return value


@dataclass(frozen=True, slots=True)
class SkippedCommand:
    command_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class RestoredCommand:
    entry: OutboxEntry
    reservation_ids: tuple[str, ...]
    requires_reconciliation: bool
    needs_unknown_write: bool


@dataclass(frozen=True, slots=True)
class RestoredWatermark:
    scope: ExecutionScope
    order_id: str
    quantity: Decimal
    quote: Decimal


def encode_outbox_details(
    entry: OutboxEntry,
    *,
    reservation_ids: tuple[str, ...],
    cumulative_quantity: Decimal,
    cumulative_quote: Decimal,
) -> dict[str, JsonValue]:
    return {
        "scope": {
            "environment": entry.scope.environment,
            "account_label": entry.scope.account_label,
            "symbol": entry.scope.symbol,
            "position_side": entry.scope.position_side.value,
        },
        "request_id": entry.request_id,
        "attempt_count": entry.attempt_count,
        "external_order_id": entry.external_order_id,
        "last_error": entry.last_error,
        "quantity": str(entry.command.requested_quantity),
        "side": entry.command.side.value,
        "order_type": entry.command.order_type.value,
        "limit_price": (
            str(entry.command.limit_price)
            if entry.command.limit_price is not None
            else None
        ),
        "reduce_only": entry.command.reduce_only,
        "expected_projection_version": entry.command.expected_projection_version,
        "reservations": list(reservation_ids),
        "cumulative_filled_quantity": str(cumulative_quantity),
        "cumulative_filled_quote": str(cumulative_quote),
    }


def decode_active_command(
    cmd_data: Mapping[str, object],
    *,
    account_label: str | None,
    restored_at: datetime,
) -> RestoredCommand | SkippedCommand | None:
    if not isinstance(cmd_data, Mapping):
        raise TypeError("execution command row must be a mapping")
    cid = _required_text(cmd_data, "command_id")
    client_order_id = _required_text(cmd_data, "client_order_id")
    if cid != client_order_id:
        raise ValueError("execution command_id must match client_order_id")
    status_str = _required_text(cmd_data, "status")
    disp_state = DispatchState(status_str)
    dtls = cmd_data.get("details")
    if not isinstance(dtls, Mapping):
        raise TypeError("execution command details must be a mapping")
    scope_data = dtls.get("scope")
    if not isinstance(scope_data, Mapping):
        raise TypeError("execution command scope must be a mapping")
    environment = _required_text(scope_data, "environment")
    acc = _required_text(scope_data, "account_label")
    symbol = _required_text(scope_data, "symbol")
    position_side = FuturesPositionSide(_required_text(scope_data, "position_side"))
    if account_label is not None and acc != account_label:
        return None
    scope = ExecutionScope(
        environment=environment,
        account_label=acc,
        symbol=symbol,
        position_side=position_side,
    )
    try:
        side = StrategySide(_required_text(dtls, "side"))
        order_type = EntryType(_required_text(dtls, "order_type").lower())
        command_type = TradeCommandType(_required_text(cmd_data, "command").lower())
        quantity = Decimal(_required_text(dtls, "quantity"))
        if not quantity.is_finite() or quantity <= Decimal("0"):
            raise ValueError("execution command quantity must be positive")
        if "reduce_only" not in dtls or not isinstance(dtls["reduce_only"], bool):
            raise ValueError("execution command reduce_only must be persisted as bool")
        raw_res_ids = dtls.get("reservations")
        if not isinstance(raw_res_ids, (list, tuple)) or any(
            not isinstance(res_id, str) or not res_id for res_id in raw_res_ids
        ):
            raise ValueError(
                "execution command reservation links are missing or invalid"
            )
        request_id = _required_text(dtls, "request_id")
        requested_at = cmd_data.get("requested_at")
        if not isinstance(requested_at, datetime) or requested_at.tzinfo is None:
            raise ValueError("execution command requested_at must be timezone-aware")
        attempt_count = dtls.get("attempt_count")
        if not isinstance(attempt_count, int) or attempt_count < 0:
            raise ValueError("execution command attempt_count is missing or invalid")
        expected_projection_version = _optional_text(
            dtls, "expected_projection_version"
        )
        external_order_id = _optional_text(dtls, "external_order_id")
        last_error = _optional_text(dtls, "last_error")
        limit_price_val = dtls.get("limit_price")
        limit_price = (
            Decimal(str(limit_price_val)) if limit_price_val is not None else None
        )
    except (KeyError, ValueError, TypeError) as parse_err:
        return SkippedCommand(cid, str(parse_err))

    cmd = TradeCommand(
        command_id=cid,
        position_key=scope.to_position_key(),
        command_type=command_type,
        side=side,
        order_type=order_type,
        requested_quantity=quantity,
        limit_price=limit_price,
        reduce_only=dtls["reduce_only"],
        expected_projection_version=expected_projection_version,
        created_at=requested_at,
    )
    entry = OutboxEntry(
        command_id=cid,
        request_id=request_id,
        scope=scope,
        command=cmd,
        state=disp_state,
        attempt_count=attempt_count,
        external_order_id=external_order_id,
        last_error=last_error,
        created_at=requested_at,
        updated_at=requested_at,
    )

    requires_reconciliation = disp_state in (
        DispatchState.UNKNOWN,
        DispatchState.DISPATCHING,
    )
    needs_unknown_write = disp_state == DispatchState.DISPATCHING
    if needs_unknown_write:
        entry = plan_command_transition(
            entry,
            DispatchState.UNKNOWN,
            at=restored_at,
            reason="restored dispatch requires reconciliation",
        ).updated
    return RestoredCommand(
        entry, tuple(raw_res_ids), requires_reconciliation, needs_unknown_write
    )


def decode_order_watermark(
    row: Mapping[str, object],
    *,
    account_label: str | None,
) -> RestoredWatermark | None:
    scope_data = row["scope"]
    if not isinstance(scope_data, Mapping):
        raise TypeError("order watermark scope must be a mapping")
    scope = ExecutionScope(
        environment=_required_text(scope_data, "environment"),
        account_label=_required_text(scope_data, "account_label"),
        symbol=_required_text(scope_data, "symbol"),
        position_side=FuturesPositionSide(_required_text(scope_data, "position_side")),
    )
    if account_label is not None and scope.account_label != account_label:
        return None
    order_id = _required_text(row, "client_order_id")
    quantity = Decimal(str(row["cumulative_filled_quantity"]))
    if not quantity.is_finite() or quantity < Decimal("0"):
        raise ValueError("cumulative fill watermark cannot be negative")
    quote = Decimal(str(row["cumulative_filled_quote"]))
    if not quote.is_finite() or quote < Decimal("0"):
        raise ValueError("cumulative quote watermark cannot be negative")
    if quantity == Decimal("0") and quote != Decimal("0"):
        raise ValueError("zero-quantity order cannot have cumulative quote")
    if quantity > Decimal("0") and quote <= Decimal("0"):
        raise ValueError("positive cumulative quantity requires positive quote")

    return RestoredWatermark(scope, order_id, quantity, quote)
