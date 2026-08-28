from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountFillEvent,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
    AccountReconciliationRun,
    ExecutionAccountProcessState,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.execution_account.binance.user_data import (
    BinanceUserDataEvent,
)

type FillKey = tuple[str, str]

_FILL_KEY_CACHE_SIZE = 8192
_FILL_FETCH_OVERLAP_MS = 60_000


@dataclass(frozen=True, slots=True)
class _FillCursor:
    from_id: int | None = None
    start_time_ms: int | None = None


class ReadOnlyAccountClient(Protocol):
    async def fetch_account_config(self) -> AccountConfigSnapshot:
        pass

    async def fetch_balances(self) -> tuple[AccountBalanceSnapshot, ...]:
        pass

    async def fetch_positions(self) -> tuple[AccountPositionSnapshot, ...]:
        pass

    async def fetch_open_orders(self) -> tuple[AccountOpenOrderSnapshot, ...]:
        pass

    async def fetch_recent_fills(
        self,
        symbols: tuple[str, ...] = (),
        *,
        from_id_by_symbol: Mapping[str, int] | None = None,
        start_time_by_symbol: Mapping[str, int] | None = None,
    ) -> tuple[AccountFillEvent, ...]:
        pass


class AccountSyncRepository(Protocol):
    async def save_process_state(self, state: ExecutionAccountProcessState) -> None:
        pass

    async def save_balance_snapshot(self, snapshot: AccountBalanceSnapshot) -> None:
        pass

    async def save_position_snapshot(self, snapshot: AccountPositionSnapshot) -> None:
        pass

    async def save_balance_position_snapshot(
        self,
        *,
        balances: tuple[AccountBalanceSnapshot, ...],
        positions: tuple[AccountPositionSnapshot, ...],
    ) -> None:
        pass

    async def upsert_open_order(self, order: AccountOpenOrderSnapshot) -> None:
        pass

    async def save_fill_event(self, fill: AccountFillEvent) -> None:
        pass

    async def save_config_snapshot(self, snapshot: AccountConfigSnapshot) -> None:
        pass

    async def save_reconciliation_run(self, run: AccountReconciliationRun) -> None:
        pass

    async def save_reconciliation_snapshot(
        self,
        *,
        config: AccountConfigSnapshot,
        balances: tuple[AccountBalanceSnapshot, ...],
        positions: tuple[AccountPositionSnapshot, ...],
        open_orders: tuple[AccountOpenOrderSnapshot, ...],
        fills: tuple[AccountFillEvent, ...],
        run: AccountReconciliationRun,
    ) -> None:
        pass


@dataclass(frozen=True, slots=True)
class ExecutionAccountSyncConfig:
    environment: str
    account_label: str
    expected_multi_assets_mode: bool
    expected_hedge_mode: bool
    observed_at: datetime
    recent_fill_symbols: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.environment.strip():
            raise ValueError("environment must not be empty")
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        if any(not symbol.strip() for symbol in self.recent_fill_symbols):
            raise ValueError("recent_fill_symbols must not contain empty values")


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    config: AccountConfigSnapshot
    balances: tuple[AccountBalanceSnapshot, ...]
    positions: tuple[AccountPositionSnapshot, ...]
    open_orders: tuple[AccountOpenOrderSnapshot, ...]


@dataclass(frozen=True, slots=True)
class ExecutionAccountSyncResult:
    status: ExecutionAccountStatus
    reconciliation_id: str
    mismatch_count: int
    snapshot: AccountSnapshot | None = None
    fill_count: int = 0
    new_fill_keys: frozenset[FillKey] = frozenset()
    fill_count_by_symbol: tuple[tuple[str, int], ...] = ()


class ExecutionAccountSyncService:
    def __init__(
        self,
        *,
        client: ReadOnlyAccountClient,
        repository: AccountSyncRepository,
        config: ExecutionAccountSyncConfig,
    ) -> None:
        self._client = client
        self._repository = repository
        self._config = config
        self._tracked_fill_symbols = {
            symbol.strip().upper() for symbol in config.recent_fill_symbols
        }
        self._active_position_keys: set[tuple[str, str]] = set()
        self._fill_cursors: dict[str, _FillCursor] = {}
        self._known_fill_keys: set[FillKey] = set()
        self._known_fill_key_order: deque[FillKey] = deque(
            maxlen=_FILL_KEY_CACHE_SIZE
        )

    async def snapshot_once(self, *, observed_at: datetime | None = None) -> None:
        """Persist a lightweight balance/position observation.

        This path deliberately skips account configuration, open orders, and
        historical fill lookups.  Those endpoints remain part of the slower
        authoritative reconciliation loop.
        """
        resolved_observed_at = (
            self._config.observed_at
            if observed_at is None
            else observed_at
        )
        if (
            resolved_observed_at.tzinfo is None
            or resolved_observed_at.utcoffset() is None
        ):
            raise ValueError("observed_at must be timezone-aware")
        balances = await self._client.fetch_balances()
        positions = await self._client.fetch_positions()
        active_positions = tuple(
            position for position in positions if position.position_amt != 0
        )
        active_position_keys = _position_keys(active_positions)
        closed_position_keys = self._active_position_keys - active_position_keys
        positions_to_save = tuple(
            position
            for position in positions
            if (
                (position.symbol, position.position_side) in active_position_keys
                or (
                    (position.symbol, position.position_side)
                    in closed_position_keys
                )
            )
        )
        await self._repository.save_balance_position_snapshot(
            balances=tuple(
                replace(balance, observed_at=resolved_observed_at)
                for balance in balances
            ),
            positions=tuple(
                replace(position, observed_at=resolved_observed_at)
                for position in positions_to_save
            ),
        )
        self._active_position_keys = active_position_keys

    async def sync_once(
        self,
        *,
        observed_at: datetime | None = None,
        publish_transient_states: bool = True,
        include_fills: bool = True,
    ) -> ExecutionAccountSyncResult:
        config = (
            self._config
            if observed_at is None
            else replace(self._config, observed_at=observed_at)
        )
        if publish_transient_states:
            await self._save_state(
                ExecutionAccountStatus.STARTING,
                config=replace(
                    config,
                    observed_at=config.observed_at - timedelta(microseconds=2),
                ),
            )
            await self._save_state(
                ExecutionAccountStatus.SYNCING,
                config=replace(
                    config,
                    observed_at=config.observed_at - timedelta(microseconds=1),
                ),
            )
        try:
            account_config = await self._client.fetch_account_config()
            reconciliation_id = _reconciliation_id(config)
            mismatches: list[str] = []
            if account_config.multi_assets_mode != (
                config.expected_multi_assets_mode
            ):
                mismatches.append("multi_assets_mode_mismatch")
            if account_config.hedge_mode != config.expected_hedge_mode:
                mismatches.append("hedge_mode_mismatch")
            if mismatches:
                reason = ",".join(mismatches)
                mismatch_details: list[JsonValue] = []
                mismatch_details.extend(mismatches)
                await self._repository.save_reconciliation_snapshot(
                    config=account_config,
                    balances=(),
                    positions=(),
                    open_orders=(),
                    fills=(),
                    run=_reconciliation_run(
                        config,
                        reconciliation_id=reconciliation_id,
                        status="halted",
                        mismatch_count=len(mismatches),
                        details={"reasons": mismatch_details},
                    ),
                )
                await self._save_state(
                    ExecutionAccountStatus.HALTED_READONLY,
                    reason,
                    config=config,
                )
                return ExecutionAccountSyncResult(
                    status=ExecutionAccountStatus.HALTED_READONLY,
                    reconciliation_id=reconciliation_id,
                    mismatch_count=len(mismatches),
                )

            balances = await self._client.fetch_balances()
            positions = await self._client.fetch_positions()
            active_positions = tuple(
                position for position in positions if position.position_amt != 0
            )
            self._active_position_keys = _position_keys(active_positions)
            open_orders = await self._client.fetch_open_orders()
            self._tracked_fill_symbols.update(
                position.symbol.strip().upper() for position in active_positions
            )
            self._tracked_fill_symbols.update(
                order.symbol.strip().upper() for order in open_orders
            )
            tracked_fill_symbols = tuple(sorted(self._tracked_fill_symbols))
            previous_fill_cursors = dict(self._fill_cursors)
            fills = (
                await self._client.fetch_recent_fills(
                    tracked_fill_symbols,
                    from_id_by_symbol={
                        symbol: cursor.from_id
                        for symbol, cursor in previous_fill_cursors.items()
                        if cursor.from_id is not None
                    },
                    start_time_by_symbol={
                        symbol: cursor.start_time_ms
                        for symbol, cursor in previous_fill_cursors.items()
                        if (
                            cursor.from_id is None
                            and cursor.start_time_ms is not None
                        )
                    },
                )
                if include_fills and tracked_fill_symbols
                else ()
            )
            fill_keys = _fill_keys(fills)
            new_fill_keys = frozenset(
                key
                for key in fill_keys
                if (
                    key[0] in previous_fill_cursors
                    and key not in self._known_fill_keys
                )
            )
            fill_count_by_symbol = _fill_counts_by_symbol(fills)
            next_fill_cursors = (
                _advance_fill_cursors(
                    previous_fill_cursors,
                    tracked_fill_symbols,
                    fills,
                    observed_at=config.observed_at,
                )
                if include_fills
                else previous_fill_cursors
            )
            await self._repository.save_reconciliation_snapshot(
                config=account_config,
                balances=balances,
                positions=active_positions,
                open_orders=open_orders,
                fills=fills,
                run=_reconciliation_run(
                    config,
                    reconciliation_id=reconciliation_id,
                    status="ready",
                    mismatch_count=0,
                    details={},
                    balance_count=len(balances),
                    position_count=len(active_positions),
                    open_order_count=len(open_orders),
                    fill_count=len(fills),
                ),
            )
            self._fill_cursors = next_fill_cursors
            for key in fill_keys:
                self._remember_fill_key(key)
            await self._save_state(
                ExecutionAccountStatus.READY_READONLY,
                config=config,
            )
            return ExecutionAccountSyncResult(
                status=ExecutionAccountStatus.READY_READONLY,
                reconciliation_id=reconciliation_id,
                mismatch_count=0,
                snapshot=AccountSnapshot(
                    config=account_config,
                    balances=balances,
                    positions=active_positions,
                    open_orders=open_orders,
                ),
                fill_count=len(fills),
                new_fill_keys=new_fill_keys,
                fill_count_by_symbol=fill_count_by_symbol,
            )
        except Exception as error:
            try:
                await self._save_state(
                    ExecutionAccountStatus.DEGRADED,
                    f"sync_failed:{type(error).__name__}",
                    config=config,
                )
            except Exception:
                pass
            raise

    async def persist_user_data_event(
        self,
        *,
        snapshot: AccountSnapshot,
        event: BinanceUserDataEvent,
        fills: tuple[AccountFillEvent, ...] = (),
    ) -> ExecutionAccountSyncResult:
        """Persist a fully merged WebSocket account observation atomically."""
        if snapshot.config.environment != self._config.environment:
            raise ValueError("account snapshot environment does not match sync config")
        if snapshot.config.account_label != self._config.account_label:
            raise ValueError(
                "account snapshot account label does not match sync config"
            )
        config = replace(self._config, observed_at=event.received_at)
        active_positions = tuple(
            position for position in snapshot.positions if position.position_amt != 0
        )
        self._active_position_keys = _position_keys(active_positions)
        reconciliation_id = _user_data_reconciliation_id(
            config,
            event.event_id,
        )
        await self._repository.save_reconciliation_snapshot(
            config=replace(snapshot.config, observed_at=event.received_at),
            balances=snapshot.balances,
            positions=snapshot.positions,
            open_orders=snapshot.open_orders,
            fills=fills,
            run=_reconciliation_run(
                config,
                reconciliation_id=reconciliation_id,
                status="ready",
                mismatch_count=0,
                details={
                    "source": "user_data_stream",
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "event_at": event.event_at.isoformat(),
                },
                balance_count=len(snapshot.balances),
                position_count=len(active_positions),
                open_order_count=len(snapshot.open_orders),
                fill_count=len(fills),
            ),
        )
        await self._save_state(
            ExecutionAccountStatus.READY_READONLY,
            config=config,
        )
        return ExecutionAccountSyncResult(
            status=ExecutionAccountStatus.READY_READONLY,
            reconciliation_id=reconciliation_id,
            mismatch_count=0,
            snapshot=snapshot,
            fill_count=len(fills),
            new_fill_keys=frozenset(_fill_keys(fills)),
            fill_count_by_symbol=_fill_counts_by_symbol(fills),
        )

    def _remember_fill_key(self, key: FillKey) -> None:
        if key in self._known_fill_keys:
            return
        if len(self._known_fill_key_order) == self._known_fill_key_order.maxlen:
            oldest = self._known_fill_key_order.popleft()
            self._known_fill_keys.discard(oldest)
        self._known_fill_key_order.append(key)
        self._known_fill_keys.add(key)

    async def publish_user_data_heartbeat(
        self,
        *,
        observed_at: datetime,
    ) -> None:
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        await self._save_state(
            ExecutionAccountStatus.READY_READONLY,
            config=replace(self._config, observed_at=observed_at),
        )

    async def _save_state(
        self,
        state: ExecutionAccountStatus,
        reason: str | None = None,
        *,
        config: ExecutionAccountSyncConfig | None = None,
    ) -> None:
        resolved_config = config or self._config
        await self._repository.save_process_state(
            ExecutionAccountProcessState(
                environment=resolved_config.environment,
                account_label=resolved_config.account_label,
                state=state,
                occurred_at=resolved_config.observed_at,
                reason=reason,
            )
        )


def _fill_keys(fills: tuple[AccountFillEvent, ...]) -> set[FillKey]:
    return {
        (fill.symbol.strip().upper(), fill.trade_id.strip())
        for fill in fills
    }


def _fill_counts_by_symbol(
    fills: tuple[AccountFillEvent, ...],
) -> tuple[tuple[str, int], ...]:
    counts: dict[str, int] = {}
    for fill in fills:
        symbol = fill.symbol.strip().upper()
        counts[symbol] = counts.get(symbol, 0) + 1
    return tuple(sorted(counts.items()))


def _advance_fill_cursors(
    previous: dict[str, _FillCursor],
    symbols: tuple[str, ...],
    fills: tuple[AccountFillEvent, ...],
    *,
    observed_at: datetime,
) -> dict[str, _FillCursor]:
    next_cursors = dict(previous)
    max_trade_id_by_symbol: dict[str, int] = {}
    for fill in fills:
        try:
            trade_id = int(fill.trade_id)
        except (TypeError, ValueError):
            continue
        symbol = fill.symbol.strip().upper()
        current = max_trade_id_by_symbol.get(symbol)
        if current is None or trade_id > current:
            max_trade_id_by_symbol[symbol] = trade_id

    observed_at_ms = int(observed_at.timestamp() * 1000)
    for symbol in symbols:
        max_trade_id = max_trade_id_by_symbol.get(symbol)
        if max_trade_id is not None:
            next_cursors[symbol] = _FillCursor(from_id=max_trade_id + 1)
            continue
        cursor = previous.get(symbol)
        if cursor is None:
            next_cursors[symbol] = _FillCursor(start_time_ms=observed_at_ms)
        elif cursor.from_id is None and cursor.start_time_ms is not None:
            next_cursors[symbol] = _FillCursor(
                start_time_ms=max(0, observed_at_ms - _FILL_FETCH_OVERLAP_MS)
            )
    return next_cursors


def _reconciliation_id(config: ExecutionAccountSyncConfig) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            "account-reconciliation:"
            f"{config.environment}:{config.account_label}:"
            f"{config.observed_at.isoformat()}",
        )
    )


def _position_keys(
    positions: tuple[AccountPositionSnapshot, ...],
) -> set[tuple[str, str]]:
    return {(position.symbol, position.position_side) for position in positions}


def _user_data_reconciliation_id(
    config: ExecutionAccountSyncConfig,
    event_id: str,
) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            "account-user-data-event:"
            f"{config.environment}:{config.account_label}:{event_id}",
        )
    )


def _reconciliation_run(
    config: ExecutionAccountSyncConfig,
    *,
    reconciliation_id: str,
    status: str,
    mismatch_count: int,
    details: dict[str, JsonValue],
    balance_count: int = 0,
    position_count: int = 0,
    open_order_count: int = 0,
    fill_count: int = 0,
) -> AccountReconciliationRun:
    return AccountReconciliationRun(
        reconciliation_id=reconciliation_id,
        environment=config.environment,
        account_label=config.account_label,
        status=status,
        observed_at=config.observed_at,
        balance_count=balance_count,
        position_count=position_count,
        open_order_count=open_order_count,
        fill_count=fill_count,
        mismatch_count=mismatch_count,
        details=details,
    )
