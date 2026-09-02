from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountFillEvent,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.execution_account.binance.user_data import (
    BinanceUserDataEvent,
)
from crypto_momentum_lab.execution_account.expectations import (
    AccountPositionExpectationRegistry,
)
from crypto_momentum_lab.execution_account.sync import (
    AccountSnapshot,
    AccountSnapshotDelta,
    diff_account_snapshots,
)


class UserDataStateError(ValueError):
    """An event is too incomplete to apply without a REST reconciliation."""


@dataclass(frozen=True, slots=True)
class AccountUserDataUpdate:
    event: BinanceUserDataEvent
    snapshot: AccountSnapshot
    fills: tuple[AccountFillEvent, ...]
    needs_reconciliation: bool
    reason: str | None
    changed: bool
    delta: AccountSnapshotDelta | None = None


class AccountUserDataState:
    """Merge Binance partial account events onto the latest REST snapshot."""

    _OPEN_ORDER_STATUSES = frozenset({"NEW", "PARTIALLY_FILLED"})
    _NO_FILL_TERMINAL_ORDER_STATUSES = frozenset(
        {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}
    )

    def __init__(
        self,
        snapshot: AccountSnapshot,
        *,
        expected_position_registry: AccountPositionExpectationRegistry | None = None,
    ) -> None:
        self._config = snapshot.config
        self._expected_position_registry = expected_position_registry
        self._balances = {item.asset: item for item in snapshot.balances}
        self._positions = {
            (item.symbol, item.position_side): item for item in snapshot.positions
        }
        self._open_orders = {
            (item.symbol, item.order_id): item for item in snapshot.open_orders
        }
        self._last_order_received_at = {
            key: item.observed_at for key, item in self._open_orders.items()
        }
        self._seen_event_ids: deque[str] = deque(maxlen=4096)
        self._seen_event_id_set: set[str] = set()
        self._seen_trade_ids: set[tuple[str, str]] = set()

    def replace_snapshot(self, snapshot: AccountSnapshot) -> None:
        self._config = snapshot.config
        self._balances = {item.asset: item for item in snapshot.balances}
        self._positions = {
            (item.symbol, item.position_side): item for item in snapshot.positions
        }
        self._open_orders = {
            (item.symbol, item.order_id): item for item in snapshot.open_orders
        }
        self._last_order_received_at = {
            key: item.observed_at for key, item in self._open_orders.items()
        }

    def apply(self, event: BinanceUserDataEvent) -> AccountUserDataUpdate:
        previous_snapshot = self.snapshot(event.received_at)
        if event.event_id in self._seen_event_id_set:
            snapshot = self.snapshot(event.received_at)
            return AccountUserDataUpdate(
                event=event,
                snapshot=snapshot,
                fills=(),
                needs_reconciliation=False,
                reason=None,
                changed=False,
                delta=diff_account_snapshots(previous_snapshot, snapshot),
            )

        needs_reconciliation = False
        reason: str | None = None
        fills: tuple[AccountFillEvent, ...] = ()
        changed = False
        if event.event_type == "ACCOUNT_UPDATE":
            changed, reason = self._apply_account_update(event)
            needs_reconciliation = reason is not None
        elif event.event_type == "ORDER_TRADE_UPDATE":
            changed, fills, reason = self._apply_order_trade_update(event)
            needs_reconciliation = reason is not None
        elif event.event_type == "ACCOUNT_CONFIG_UPDATE":
            needs_reconciliation = True
            reason = "account_config_update"
        elif event.event_type == "listenKeyExpired":
            needs_reconciliation = True
            reason = "listen_key_expired"
        self._remember_event(event.event_id)
        snapshot = self.snapshot(event.received_at)
        return AccountUserDataUpdate(
            event=event,
            snapshot=snapshot,
            fills=fills,
            needs_reconciliation=needs_reconciliation,
            reason=reason,
            changed=changed,
            delta=diff_account_snapshots(previous_snapshot, snapshot),
        )

    def snapshot(self, observed_at: datetime) -> AccountSnapshot:
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        return AccountSnapshot(
            config=replace(self._config, observed_at=observed_at),
            balances=tuple(
                replace(item, observed_at=observed_at)
                for item in sorted(self._balances.values(), key=lambda item: item.asset)
            ),
            positions=tuple(
                replace(item, observed_at=observed_at)
                for item in sorted(
                    self._positions.values(),
                    key=lambda item: (item.symbol, item.position_side),
                )
            ),
            open_orders=tuple(
                replace(item, observed_at=observed_at)
                for item in sorted(
                    self._open_orders.values(),
                    key=lambda item: (item.symbol, item.order_id),
                )
            ),
        )

    def _apply_account_update(
        self,
        event: BinanceUserDataEvent,
    ) -> tuple[bool, str | None]:
        account = _require_mapping(event.payload.get("a"), "ACCOUNT_UPDATE.a")
        balance_rows = _require_mapping_list(account.get("B"), "ACCOUNT_UPDATE.a.B")
        position_rows = _require_mapping_list(account.get("P"), "ACCOUNT_UPDATE.a.P")
        reason: str | None = None
        changed = bool(balance_rows or position_rows)

        for row in balance_rows:
            asset = _required_text(row.get("a"), "ACCOUNT_UPDATE balance asset")
            wallet_balance = _decimal(row.get("wb"), "ACCOUNT_UPDATE wallet balance")
            existing = self._balances.get(asset)
            if existing is None:
                available_balance = _decimal(
                    row.get("cw", "0"),
                    "ACCOUNT_UPDATE cross wallet balance",
                )
                reason = reason or "unknown_balance"
            else:
                available_balance = existing.available_balance
            self._balances[asset] = AccountBalanceSnapshot(
                environment=self._config.environment,
                account_label=self._config.account_label,
                asset=asset,
                wallet_balance=wallet_balance,
                available_balance=available_balance,
                unrealized_pnl=(
                    existing.unrealized_pnl if existing is not None else Decimal("0")
                ),
                observed_at=event.received_at,
                raw_payload=_event_raw_payload(event, "balance", row),
            )

        for row in position_rows:
            symbol = _required_text(row.get("s"), "ACCOUNT_UPDATE position symbol")
            position_side = str(row.get("ps", "BOTH"))
            if not position_side.strip():
                raise UserDataStateError("ACCOUNT_UPDATE position side is empty")
            position_amt = _decimal(row.get("pa"), "ACCOUNT_UPDATE position amount")
            existing_position = self._positions.get((symbol, position_side))
            if existing_position is None and position_amt != 0:
                expected_position = None
                if self._expected_position_registry is not None:
                    expected_position = self._expected_position_registry.consume(
                        symbol=symbol,
                        position_side=position_side,
                        position_amt=position_amt,
                        observed_at=event.received_at,
                    )
                if expected_position is None:
                    reason = reason or "unknown_position"
            if existing_position is None and position_amt == 0:
                continue
            entry_price = _decimal(
                row.get("ep", "0"),
                "ACCOUNT_UPDATE entry price",
            )
            unrealized_pnl = _decimal(
                row.get("up", "0"),
                "ACCOUNT_UPDATE unrealized pnl",
            )
            mark_price = (
                existing_position.mark_price
                if existing_position is not None
                else _initial_mark_price(
                    entry_price=entry_price,
                    position_amt=position_amt,
                    unrealized_pnl=unrealized_pnl,
                )
            )
            self._positions[(symbol, position_side)] = AccountPositionSnapshot(
                environment=self._config.environment,
                account_label=self._config.account_label,
                symbol=symbol,
                position_side=position_side,
                position_amt=position_amt,
                entry_price=entry_price,
                mark_price=mark_price,
                unrealized_pnl=unrealized_pnl,
                notional=(
                    existing_position.notional
                    if existing_position is not None
                    else abs(position_amt * mark_price)
                ),
                leverage=(
                    existing_position.leverage
                    if existing_position is not None
                    else None
                ),
                margin_type=(
                    str(row.get("mt"))
                    if row.get("mt") is not None
                    else (
                        existing_position.margin_type
                        if existing_position is not None
                        else None
                    )
                ),
                observed_at=event.received_at,
                raw_payload=_event_raw_payload(event, "position", row),
            )
        return changed, reason

    def _apply_order_trade_update(
        self,
        event: BinanceUserDataEvent,
    ) -> tuple[bool, tuple[AccountFillEvent, ...], str | None]:
        row = _require_mapping(event.payload.get("o"), "ORDER_TRADE_UPDATE.o")
        symbol = _required_text(row.get("s"), "ORDER_TRADE_UPDATE symbol")
        order_id = _required_text(row.get("i"), "ORDER_TRADE_UPDATE order id")
        key = (symbol, order_id)
        last_received_at = self._last_order_received_at.get(key)
        if last_received_at is not None and event.received_at < last_received_at:
            return False, (), None
        self._last_order_received_at[key] = event.received_at

        status = _required_text(row.get("X"), "ORDER_TRADE_UPDATE status")
        order = AccountOpenOrderSnapshot(
            environment=self._config.environment,
            account_label=self._config.account_label,
            symbol=symbol,
            order_id=order_id,
            client_order_id=_required_text(
                row.get("c"),
                "ORDER_TRADE_UPDATE client order id",
            ),
            side=_required_text(row.get("S"), "ORDER_TRADE_UPDATE side"),
            order_type=_required_text(row.get("o"), "ORDER_TRADE_UPDATE order type"),
            status=status,
            price=_decimal(row.get("p", "0"), "ORDER_TRADE_UPDATE price"),
            original_quantity=_decimal(
                row.get("q", "0"),
                "ORDER_TRADE_UPDATE original quantity",
            ),
            executed_quantity=_decimal(
                row.get("z", "0"),
                "ORDER_TRADE_UPDATE executed quantity",
            ),
            reduce_only=_bool(row.get("R", False)),
            observed_at=event.received_at,
            raw_payload=_event_raw_payload(event, "order", row),
        )
        if (
            self._expected_position_registry is not None
            and status in self._NO_FILL_TERMINAL_ORDER_STATUSES
            and order.executed_quantity == 0
        ):
            self._expected_position_registry.discard(order.client_order_id)
        if status in self._OPEN_ORDER_STATUSES:
            self._open_orders[key] = order
        else:
            self._open_orders.pop(key, None)

        execution_type = str(row.get("x", ""))
        last_quantity = _decimal(
            row.get("l", "0"),
            "ORDER_TRADE_UPDATE last fill quantity",
        )
        if execution_type != "TRADE" or last_quantity == 0:
            return True, (), None

        trade_id = str(row.get("t", "")).strip()
        fee_asset = str(row.get("N", "")).strip()
        if not trade_id or trade_id == "-1" or not fee_asset:
            raise UserDataStateError("trade event is missing trade id or fee asset")
        trade_key = (symbol, trade_id)
        if trade_key in self._seen_trade_ids:
            return True, (), None
        fee = _decimal(row.get("n", "0"), "ORDER_TRADE_UPDATE fee")
        if fee < 0:
            raise UserDataStateError("trade event contains a negative commission")
        fill = AccountFillEvent(
            environment=self._config.environment,
            account_label=self._config.account_label,
            symbol=symbol,
            trade_id=trade_id,
            order_id=order_id,
            side=_required_text(row.get("S"), "ORDER_TRADE_UPDATE side"),
            price=_decimal(
                row.get("L", row.get("p", "0")),
                "ORDER_TRADE_UPDATE last fill price",
            ),
            quantity=last_quantity,
            realized_pnl=_decimal(
                row.get("rp", "0"),
                "ORDER_TRADE_UPDATE realized pnl",
            ),
            fee=fee,
            fee_asset=fee_asset,
            trade_at=_timestamp(
                row.get("T"),
                fallback=event.event_at,
                field_name="ORDER_TRADE_UPDATE trade time",
            ),
            raw_payload=_event_raw_payload(event, "fill", row),
        )
        self._seen_trade_ids.add(trade_key)
        return True, (fill,), None

    def _remember_event(self, event_id: str) -> None:
        if len(self._seen_event_ids) == self._seen_event_ids.maxlen:
            expired = self._seen_event_ids.popleft()
            self._seen_event_id_set.discard(expired)
        self._seen_event_ids.append(event_id)
        self._seen_event_id_set.add(event_id)


def _initial_mark_price(
    *,
    entry_price: Decimal,
    position_amt: Decimal,
    unrealized_pnl: Decimal,
) -> Decimal:
    """Build a safe provisional mark for a position-only account update.

    Binance does not include mark price in ``ACCOUNT_UPDATE``.  Deriving it
    from unrealized PnL is exact when PnL is non-zero; entry price is the
    conservative provisional mark at a flat PnL boundary.  The next REST
    snapshot replaces this value with the exchange mark.
    """
    if position_amt != 0 and unrealized_pnl != 0:
        derived = entry_price + unrealized_pnl / position_amt
        if derived > 0:
            return derived
    return entry_price if entry_price > 0 else Decimal("0")


def _require_mapping(value: object, field_name: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise UserDataStateError(f"{field_name} must be an object")
    return {str(key): item for key, item in value.items()}


def _require_mapping_list(
    value: object,
    field_name: str,
) -> tuple[dict[str, object], ...]:
    if not isinstance(value, list):
        raise UserDataStateError(f"{field_name} must be an array")
    return tuple(_require_mapping(item, field_name) for item in value)


def _required_text(value: object, field_name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise UserDataStateError(f"{field_name} must not be empty")
    return text


def _decimal(value: object, field_name: str) -> Decimal:
    if value is None:
        raise UserDataStateError(f"{field_name} is missing")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise UserDataStateError(f"{field_name} is not numeric") from error


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes"}
    return bool(value)


def _timestamp(
    value: object,
    *,
    fallback: datetime,
    field_name: str,
) -> datetime:
    if value is None:
        return fallback
    try:
        result = datetime.fromtimestamp(float(str(value)) / 1000, tz=UTC)
    except (TypeError, ValueError, OverflowError, OSError) as error:
        raise UserDataStateError(f"{field_name} is not a valid timestamp") from error
    return result


def _event_raw_payload(
    event: BinanceUserDataEvent,
    section: str,
    row: Mapping[str, object],
) -> dict[str, JsonValue]:
    return {
        "source": "user_data_stream",
        "section": section,
        "event_id": event.event_id,
        "event": event.payload,
        "row": {str(key): _json_value(item) for key, item in row.items()},
    }


def _json_value(value: object) -> JsonValue:
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_value(item) for item in value]
    return str(value)
