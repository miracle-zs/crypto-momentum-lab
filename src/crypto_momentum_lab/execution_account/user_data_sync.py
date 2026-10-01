from collections import deque
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import (
    AccountBalanceSnapshot,
    AccountFillEvent,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.execution_account.binance.order_status import (
    is_open_order_status,
    should_discard_position_expectation,
)
from crypto_momentum_lab.execution_account.binance.user_data_models import (
    BinanceUserDataEvent,
)
from crypto_momentum_lab.execution_account.expectations import (
    AccountPositionExpectationRegistry,
)
from crypto_momentum_lab.execution_account.snapshot_changes import (
    build_account_snapshot,
    diff_account_snapshots,
)
from crypto_momentum_lab.execution_account.snapshot_models import (
    AccountSnapshot,
)
from crypto_momentum_lab.execution_account.user_data_fields import (
    event_raw_payload,
    initial_mark_price,
    parse_bool,
    parse_decimal,
    parse_timestamp,
    require_mapping,
    require_mapping_list,
    required_text,
)
from crypto_momentum_lab.execution_account.user_data_models import (
    AccountUserDataUpdate,
    UserDataStateError,
)
from crypto_momentum_lab.execution_account.user_data_sequence import (
    positive_exchange_milliseconds,
    stale_user_data_reason,
    validate_exchange_update_watermark,
)


class AccountUserDataState:
    """Merge Binance partial account events onto the latest REST snapshot."""

    _SEEN_TRADE_CACHE_SIZE = 8192

    def __init__(
        self,
        snapshot: AccountSnapshot,
        *,
        expected_position_registry: AccountPositionExpectationRegistry | None = None,
    ) -> None:
        self._expected_position_registry = expected_position_registry
        self.replace_snapshot(snapshot)
        self._last_account_exchange_event_at: datetime | None = None
        self._last_order_exchange_event_at: dict[tuple[str, str], datetime] = {}
        self._last_exchange_update_id: dict[str, int] = {}
        self._seen_event_ids: deque[str] = deque(maxlen=4096)
        self._seen_event_id_set: set[str] = set()
        self._seen_trade_ids: deque[tuple[str, str]] = deque(
            maxlen=self._SEEN_TRADE_CACHE_SIZE
        )
        self._seen_trade_id_set: set[tuple[str, str]] = set()

    def replace_snapshot(self, snapshot: AccountSnapshot) -> None:
        self._config = snapshot.config
        self._balances = {item.asset: item for item in snapshot.balances}
        self._positions = {
            (item.symbol, item.position_side): item for item in snapshot.positions
        }
        self._open_orders = {
            (item.symbol, item.order_id): item for item in snapshot.open_orders
        }
        self._baseline_balance_times = {
            item.asset: positive_exchange_milliseconds(
                item.raw_payload.get("updateTime")
            )
            for item in snapshot.balances
        }
        self._baseline_position_times = {
            (item.symbol, item.position_side): positive_exchange_milliseconds(
                item.raw_payload.get("updateTime")
            )
            for item in snapshot.positions
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

        validate_exchange_update_watermark(
            event, self._last_exchange_update_id.get(event.event_type)
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
        self._remember_exchange_update_watermark(event)
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
        return build_account_snapshot(
            self._config,
            balances=self._balances.values(),
            positions=self._positions.values(),
            open_orders=self._open_orders.values(),
            observed_at=observed_at,
        )

    def _apply_account_update(
        self,
        event: BinanceUserDataEvent,
    ) -> tuple[bool, str | None]:
        stale_reason = stale_user_data_reason(
            event,
            last_exchange_event_at=self._last_account_exchange_event_at,
        )
        if stale_reason is not None:
            return False, stale_reason
        if event.exchange_event_at is not None:
            self._last_account_exchange_event_at = event.exchange_event_at
        account = require_mapping(event.payload.get("a"), "ACCOUNT_UPDATE.a")
        balance_rows = require_mapping_list(account.get("B"), "ACCOUNT_UPDATE.a.B")
        position_rows = require_mapping_list(account.get("P"), "ACCOUNT_UPDATE.a.P")
        reason: str | None = None
        changed = False
        # E / local receipt time cannot prove inclusion in a REST entity baseline.
        transaction_time = positive_exchange_milliseconds(event.payload.get("T"))

        for row in balance_rows:
            asset = required_text(row.get("a"), "ACCOUNT_UPDATE balance asset")
            wallet_balance = parse_decimal(
                row.get("wb"), "ACCOUNT_UPDATE wallet balance"
            )
            baseline_time = self._baseline_balance_times.get(asset)
            if transaction_time is not None and baseline_time is not None:
                if transaction_time < baseline_time:
                    continue
            changed = True
            existing = self._balances.get(asset)
            if existing is None:
                available_balance = parse_decimal(
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
                raw_payload=event_raw_payload(event, "balance", row),
            )

        for row in position_rows:
            symbol = required_text(row.get("s"), "ACCOUNT_UPDATE position symbol")
            position_side = str(row.get("ps", "BOTH"))
            if not position_side.strip():
                raise UserDataStateError("ACCOUNT_UPDATE position side is empty")
            baseline_time = self._baseline_position_times.get((symbol, position_side))
            if transaction_time is not None and baseline_time is not None:
                if transaction_time < baseline_time:
                    continue
            changed = True
            position_amt = parse_decimal(
                row.get("pa"), "ACCOUNT_UPDATE position amount"
            )
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
            entry_price = parse_decimal(
                row.get("ep", "0"),
                "ACCOUNT_UPDATE entry price",
            )
            unrealized_pnl = parse_decimal(
                row.get("up", "0"),
                "ACCOUNT_UPDATE unrealized pnl",
            )
            mark_price = (
                existing_position.mark_price
                if existing_position is not None
                else initial_mark_price(
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
                raw_payload=event_raw_payload(event, "position", row),
            )
        return changed, reason

    def _apply_order_trade_update(
        self,
        event: BinanceUserDataEvent,
    ) -> tuple[bool, tuple[AccountFillEvent, ...], str | None]:
        row = require_mapping(event.payload.get("o"), "ORDER_TRADE_UPDATE.o")
        symbol = required_text(row.get("s"), "ORDER_TRADE_UPDATE symbol")
        order_id = required_text(row.get("i"), "ORDER_TRADE_UPDATE order id")
        key = (symbol, order_id)
        stale_reason = stale_user_data_reason(
            event,
            last_exchange_event_at=self._last_order_exchange_event_at.get(key),
            last_received_at=self._last_order_received_at.get(key),
        )
        if stale_reason is not None:
            return False, (), stale_reason
        if event.exchange_event_at is not None:
            self._last_order_exchange_event_at[key] = event.exchange_event_at
        else:
            self._last_order_received_at[key] = event.received_at

        status = required_text(row.get("X"), "ORDER_TRADE_UPDATE status")
        order = AccountOpenOrderSnapshot(
            environment=self._config.environment,
            account_label=self._config.account_label,
            symbol=symbol,
            order_id=order_id,
            client_order_id=required_text(
                row.get("c"),
                "ORDER_TRADE_UPDATE client order id",
            ),
            side=required_text(row.get("S"), "ORDER_TRADE_UPDATE side"),
            order_type=required_text(row.get("o"), "ORDER_TRADE_UPDATE order type"),
            status=status,
            price=parse_decimal(row.get("p", "0"), "ORDER_TRADE_UPDATE price"),
            original_quantity=parse_decimal(
                row.get("q", "0"),
                "ORDER_TRADE_UPDATE original quantity",
            ),
            executed_quantity=parse_decimal(
                row.get("z", "0"),
                "ORDER_TRADE_UPDATE executed quantity",
            ),
            reduce_only=parse_bool(row.get("R", False)),
            observed_at=event.received_at,
            raw_payload=event_raw_payload(event, "order", row),
        )
        if (
            self._expected_position_registry is not None
            and should_discard_position_expectation(status, order.executed_quantity)
        ):
            self._expected_position_registry.discard(order.client_order_id)
        if is_open_order_status(status):
            self._open_orders[key] = order
        else:
            self._open_orders.pop(key, None)

        execution_type = str(row.get("x", ""))
        last_quantity = parse_decimal(
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
        if trade_key in self._seen_trade_id_set:
            return True, (), None
        fee = parse_decimal(row.get("n", "0"), "ORDER_TRADE_UPDATE fee")
        if fee < 0:
            raise UserDataStateError("trade event contains a negative commission")
        fill = AccountFillEvent(
            environment=self._config.environment,
            account_label=self._config.account_label,
            symbol=symbol,
            trade_id=trade_id,
            order_id=order_id,
            side=required_text(row.get("S"), "ORDER_TRADE_UPDATE side"),
            price=parse_decimal(
                row.get("L", row.get("p", "0")),
                "ORDER_TRADE_UPDATE last fill price",
            ),
            quantity=last_quantity,
            realized_pnl=parse_decimal(
                row.get("rp", "0"),
                "ORDER_TRADE_UPDATE realized pnl",
            ),
            fee=fee,
            fee_asset=fee_asset,
            trade_at=parse_timestamp(
                row.get("T"),
                fallback=event.event_at,
                field_name="ORDER_TRADE_UPDATE trade time",
            ),
            raw_payload=event_raw_payload(event, "fill", row),
        )
        self._remember_trade(trade_key)
        return True, (fill,), None

    def _remember_exchange_update_watermark(
        self,
        event: BinanceUserDataEvent,
    ) -> None:
        update_id = event.exchange_update_id
        if update_id is None:
            return
        self._last_exchange_update_id[event.event_type] = update_id

    def _remember_event(self, event_id: str) -> None:
        if len(self._seen_event_ids) == self._seen_event_ids.maxlen:
            expired = self._seen_event_ids.popleft()
            self._seen_event_id_set.discard(expired)
        self._seen_event_ids.append(event_id)
        self._seen_event_id_set.add(event_id)

    def _remember_trade(self, trade_key: tuple[str, str]) -> None:
        if len(self._seen_trade_ids) == self._seen_trade_ids.maxlen:
            expired = self._seen_trade_ids.popleft()
            self._seen_trade_id_set.discard(expired)
        self._seen_trade_ids.append(trade_key)
        self._seen_trade_id_set.add(trade_key)
