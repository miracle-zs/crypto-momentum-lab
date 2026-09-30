"""Pure account snapshot comparison and delta application."""

from dataclasses import replace

from crypto_momentum_lab.domain.account.models import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.execution_account.snapshot_models import (
    AccountSnapshot,
    AccountSnapshotDelta,
)


def diff_account_snapshots(
    previous: AccountSnapshot,
    current: AccountSnapshot,
) -> AccountSnapshotDelta:
    """Calculate a material delta while ignoring observation metadata."""
    _require_snapshot_scope_match(previous, current)
    previous_balances = {item.asset: item for item in previous.balances}
    current_balances = {item.asset: item for item in current.balances}
    previous_positions = {
        (item.symbol, item.position_side): item for item in previous.positions
    }
    current_positions = {
        (item.symbol, item.position_side): item for item in current.positions
    }
    previous_open_orders = {
        (item.symbol, item.order_id): item for item in previous.open_orders
    }
    current_open_orders = {
        (item.symbol, item.order_id): item for item in current.open_orders
    }
    return AccountSnapshotDelta(
        observed_at=current.config.observed_at,
        config=(
            current.config
            if _config_signature(previous.config) != _config_signature(current.config)
            else None
        ),
        balances=tuple(
            current_balances[key]
            for key in sorted(current_balances)
            if key not in previous_balances
            or _balance_signature(previous_balances[key])
            != _balance_signature(current_balances[key])
        ),
        removed_balance_assets=tuple(
            sorted(set(previous_balances) - set(current_balances))
        ),
        positions=tuple(
            current_positions[key]
            for key in sorted(current_positions)
            if key not in previous_positions
            or _position_signature(previous_positions[key])
            != _position_signature(current_positions[key])
        ),
        removed_positions=tuple(
            sorted(set(previous_positions) - set(current_positions))
        ),
        open_orders=tuple(
            current_open_orders[key]
            for key in sorted(current_open_orders)
            if key not in previous_open_orders
            or _open_order_signature(previous_open_orders[key])
            != _open_order_signature(current_open_orders[key])
        ),
        removed_open_orders=tuple(
            sorted(set(previous_open_orders) - set(current_open_orders))
        ),
    )


def apply_account_snapshot_delta(
    snapshot: AccountSnapshot,
    delta: AccountSnapshotDelta,
) -> AccountSnapshot:
    """Apply a delta and return a fresh, normalized effective snapshot."""
    scope = (snapshot.config.environment, snapshot.config.account_label)
    if delta.config is not None and _config_scope(delta.config) != scope:
        raise ValueError("account delta config scope does not match snapshot")
    balances: dict[str, AccountBalanceSnapshot] = {
        item.asset: item for item in snapshot.balances
    }
    positions: dict[tuple[str, str], AccountPositionSnapshot] = {
        (item.symbol, item.position_side): item for item in snapshot.positions
    }
    open_orders: dict[tuple[str, str], AccountOpenOrderSnapshot] = {
        (item.symbol, item.order_id): item for item in snapshot.open_orders
    }
    for balance in delta.balances:
        _require_item_scope(balance, scope, "account delta balance")
        balances[balance.asset] = balance
    for asset in delta.removed_balance_assets:
        balances.pop(asset, None)
    for position in delta.positions:
        _require_item_scope(position, scope, "account delta position")
        positions[(position.symbol, position.position_side)] = position
    for key in delta.removed_positions:
        positions.pop(key, None)
    for order in delta.open_orders:
        _require_item_scope(order, scope, "account delta open order")
        open_orders[(order.symbol, order.order_id)] = order
    for key in delta.removed_open_orders:
        open_orders.pop(key, None)
    config = snapshot.config if delta.config is None else delta.config
    config = replace(config, observed_at=delta.observed_at)
    return AccountSnapshot(
        config=config,
        balances=tuple(
            replace(item, observed_at=delta.observed_at)
            for item in sorted(balances.values(), key=lambda item: item.asset)
        ),
        positions=tuple(
            replace(
                item,
                observed_at=delta.observed_at,
            )
            for item in sorted(
                positions.values(),
                key=lambda item: (item.symbol, item.position_side),
            )
        ),
        open_orders=tuple(
            replace(item, observed_at=delta.observed_at)
            for item in sorted(
                open_orders.values(),
                key=lambda item: (item.symbol, item.order_id),
            )
        ),
    )


def _require_snapshot_scope_match(
    previous: AccountSnapshot,
    current: AccountSnapshot,
) -> None:
    if _config_scope(previous.config) != _config_scope(current.config):
        raise ValueError("account snapshot scopes do not match")


def _config_scope(config: AccountConfigSnapshot) -> tuple[str, str]:
    return config.environment, config.account_label


def _require_item_scope(
    item: AccountBalanceSnapshot | AccountPositionSnapshot | AccountOpenOrderSnapshot,
    scope: tuple[str, str],
    field_name: str,
) -> None:
    item_scope = (item.environment, item.account_label)
    if item_scope != scope:
        raise ValueError(f"{field_name} scope does not match snapshot")


def _config_signature(config: AccountConfigSnapshot) -> tuple[object, ...]:
    return (
        config.environment,
        config.account_label,
        config.multi_assets_mode,
        config.hedge_mode,
        config.fee_tier,
    )


def _balance_signature(snapshot: AccountBalanceSnapshot) -> tuple[object, ...]:
    return (
        snapshot.environment,
        snapshot.account_label,
        snapshot.asset,
        snapshot.wallet_balance,
        snapshot.available_balance,
        snapshot.unrealized_pnl,
    )


def _position_signature(snapshot: AccountPositionSnapshot) -> tuple[object, ...]:
    return (
        snapshot.environment,
        snapshot.account_label,
        snapshot.symbol,
        snapshot.position_side,
        snapshot.position_amt,
        snapshot.entry_price,
        snapshot.mark_price,
        snapshot.unrealized_pnl,
        snapshot.notional,
        snapshot.leverage,
        snapshot.margin_type,
    )


def _open_order_signature(snapshot: AccountOpenOrderSnapshot) -> tuple[object, ...]:
    return (
        snapshot.environment,
        snapshot.account_label,
        snapshot.symbol,
        snapshot.order_id,
        snapshot.client_order_id,
        snapshot.side,
        snapshot.order_type,
        snapshot.status,
        snapshot.price,
        snapshot.original_quantity,
        snapshot.executed_quantity,
        snapshot.reduce_only,
    )
