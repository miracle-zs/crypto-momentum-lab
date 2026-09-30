from collections import deque
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountFillEvent,
    AccountFillLoadScan,
    AccountFillReconciliationCursor,
    AccountPositionSnapshot,
    ExecutionAccountProcessState,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.execution_account.balance_history import (
    BalanceValue,
    balance_value,
    select_balance_history,
)
from crypto_momentum_lab.execution_account.binance.user_data import (
    BinanceUserDataEvent,
)
from crypto_momentum_lab.execution_account.client_compat import (
    fetch_positions_for_reconciliation,
    incomplete_fill_symbols,
    optional_fill_provenance_fetcher,
)
from crypto_momentum_lab.execution_account.fill_progress import (
    FillCursor,
    account_fill_keys,
    advance_fill_cursors,
    fill_counts_by_symbol,
    merge_fill_cursor,
)
from crypto_momentum_lab.execution_account.position_history import (
    PositionSignature,
    should_persist_position,
)
from crypto_momentum_lab.execution_account.reconciliation_records import (
    account_reconciliation_id,
    position_state_details,
    reconciliation_run,
    user_data_reconciliation_id,
)
from crypto_momentum_lab.execution_account.snapshot_models import (
    AccountSnapshot,
)
from crypto_momentum_lab.execution_account.sync_models import (
    ExecutionAccountSyncConfig,
    ExecutionAccountSyncResult,
    FillKey,
)
from crypto_momentum_lab.execution_account.sync_ports import (
    AccountSyncRepository,
    ReadOnlyAccountClient,
)

_FILL_KEY_CACHE_SIZE = 8192
_NEW_POSITION_FILL_LOOKBACK = timedelta(minutes=30)
# Never issue an unbounded userTrades pull.  Symbols without a fromId cursor
# and without a prior startTime are historical; scanning a week is enough to
# catch a still-open lot without replaying every prior episode on the symbol.
_HISTORICAL_FILL_LOOKBACK = timedelta(days=7)
# Dashboard and ops-monitor only need a fresh enough "still ready" sample.
# Writing every ~30s heartbeat produced ~12k identical ready_readonly rows
# per day per account.  ops-monitor fires live_account_lifecycle_not_ready
# when the latest row is older than 300s, so the refresh window must stay
# well below that threshold -- 5 minutes raced the alert and flapped
# critical every cycle.
_PROCESS_STATE_REFRESH = timedelta(seconds=60)


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
        self._fill_cursors: dict[str, FillCursor] = {
            symbol.strip().upper(): FillCursor(
                from_id=cursor.from_id,
                start_time_ms=cursor.start_time_ms,
            )
            for symbol, cursor in config.recent_fill_cursors.items()
        }
        self._fill_cursor_checked_at: dict[str, datetime] = {
            symbol.strip().upper(): cursor.last_checked_at
            for symbol, cursor in config.recent_fill_cursors.items()
        }
        self._tracked_fill_symbols.update(self._fill_cursors)
        self._historical_fill_reconciliation_interval = timedelta(
            seconds=config.historical_fill_reconciliation_interval_seconds
        )
        self._last_balance_values: dict[str, BalanceValue] = {}
        self._last_position_signatures: dict[
            tuple[str, str], PositionSignature
        ] = {}
        self._known_fill_keys: set[FillKey] = set()
        self._known_fill_key_order: deque[FillKey] = deque(maxlen=_FILL_KEY_CACHE_SIZE)
        self._has_completed_sync = False
        self._latest_observation_at: datetime | None = None
        self._latest_rest_account_config: AccountConfigSnapshot | None = None
        self._last_persisted_process_state: ExecutionAccountStatus | None = None
        self._last_persisted_process_state_reason: str | None = None
        self._last_persisted_process_state_at: datetime | None = None

    async def snapshot_once(self, *, observed_at: datetime | None = None) -> None:
        """Persist a lightweight balance/position observation.

        This path deliberately skips account configuration, open orders, and
        historical fill lookups.  Those endpoints remain part of the slower
        authoritative reconciliation loop.
        """
        resolved_observed_at = (
            self._config.observed_at if observed_at is None else observed_at
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
                or ((position.symbol, position.position_side) in closed_position_keys)
            )
        )
        normalized_balances = tuple(
            replace(balance, observed_at=resolved_observed_at) for balance in balances
        )
        persisted_balances = select_balance_history(normalized_balances, self._last_balance_values)
        normalized_positions = tuple(
            replace(position, observed_at=resolved_observed_at)
            for position in positions_to_save
        )
        persisted_positions = self._positions_to_persist(
            normalized_positions,
            observed_at=resolved_observed_at,
        )
        await self._repository.save_balance_position_snapshot(
            balances=persisted_balances,
            positions=persisted_positions,
        )
        self._remember_balance_values(normalized_balances)
        self._remember_position_signatures(
            persisted_positions,
            observed_at=resolved_observed_at,
        )
        self._active_position_keys = active_position_keys

    async def sync_once(
        self,
        *,
        observed_at: datetime | None = None,
        publish_transient_states: bool = True,
        include_fills: bool = True,
    ) -> ExecutionAccountSyncResult:
        return await self._sync_once(
            observed_at=observed_at,
            publish_transient_states=publish_transient_states,
            include_fills=include_fills,
            persist=True,
        )

    async def sync_once_for_realtime(
        self,
        *,
        observed_at: datetime | None = None,
        publish_transient_states: bool = False,
        include_fills: bool = True,
    ) -> ExecutionAccountSyncResult:
        """Fetch an authoritative snapshot without waiting on PostgreSQL.

        The caller owns publication of the returned in-memory snapshot.  A
        later call to :meth:`persist_reconciliation_result` writes the same
        result to the durable database in the background.
        """
        return await self._sync_once(
            observed_at=observed_at,
            publish_transient_states=publish_transient_states,
            include_fills=include_fills,
            persist=False,
        )

    async def _sync_once(
        self,
        *,
        observed_at: datetime | None,
        publish_transient_states: bool,
        include_fills: bool,
        persist: bool,
    ) -> ExecutionAccountSyncResult:
        config = (
            self._config
            if observed_at is None
            else replace(self._config, observed_at=observed_at)
        )
        if publish_transient_states and persist:
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
            self._latest_rest_account_config = account_config
            reconciliation_id = account_reconciliation_id(config)
            mismatches: list[str] = []
            if account_config.multi_assets_mode != (config.expected_multi_assets_mode):
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
                    run=reconciliation_run(
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
            previous_active_position_keys = set(self._active_position_keys)
            positions = tuple(
                _position_cut_for_trade_scan(item)
                for item in await fetch_positions_for_reconciliation(self._client)
            )
            active_positions = tuple(
                position for position in positions if position.position_amt != 0
            )
            active_position_keys = _position_keys(active_positions)
            self._active_position_keys = active_position_keys
            open_orders = await self._client.fetch_open_orders()
            active_fill_symbols = {
                position.symbol.strip().upper() for position in active_positions
            }
            active_fill_symbols.update(
                order.symbol.strip().upper() for order in open_orders
            )
            active_fill_symbols.update(
                symbol for symbol, _side in self._config.fill_source_anchors
            )
            self._tracked_fill_symbols.update(active_fill_symbols)
            tracked_fill_symbols = self._fill_symbols_for_reconciliation(
                active_fill_symbols=active_fill_symbols,
                observed_at=config.observed_at,
            )
            previous_fill_cursors = dict(self._fill_cursors)
            previous_active_symbols = {
                symbol.strip().upper()
                for symbol, _position_side in previous_active_position_keys
            }
            newly_active_symbols = (
                {position.symbol.strip().upper() for position in active_positions}
                - previous_active_symbols
                if self._has_completed_sync
                else set()
            )
            start_time_by_symbol = {
                symbol: cursor.start_time_ms
                for symbol, cursor in previous_fill_cursors.items()
                if (cursor.from_id is None and cursor.start_time_ms is not None)
            }
            from_id_by_symbol = {
                symbol: cursor.from_id
                for symbol, cursor in previous_fill_cursors.items()
                if (cursor.from_id is not None and symbol in tracked_fill_symbols)
            }
            new_position_start_at = int(
                (config.observed_at - _NEW_POSITION_FILL_LOOKBACK).timestamp() * 1000
            )
            for symbol in newly_active_symbols:
                if symbol not in previous_fill_cursors:
                    start_time_by_symbol[symbol] = max(0, new_position_start_at)
            # Any remaining tracked symbol still has no positional cursor.
            # Bound it explicitly so userTrades never falls back to "latest
            # 1000 fills of every prior episode" for that symbol.
            historical_start_at = int(
                (config.observed_at - _HISTORICAL_FILL_LOOKBACK).timestamp() * 1000
            )
            for symbol in tracked_fill_symbols:
                if symbol in from_id_by_symbol:
                    continue
                if symbol not in start_time_by_symbol:
                    start_time_by_symbol[symbol] = historical_start_at
            fills_by_key: dict[FillKey, AccountFillEvent] = {}
            fill_load_scans: list[AccountFillLoadScan] = []
            scan_fetcher = optional_fill_provenance_fetcher(self._client)
            if include_fills and scan_fetcher is not None:
                for position in positions:
                    symbol = position.symbol.strip().upper()
                    side = position.position_side.strip().upper()
                    source_anchor = self._config.fill_source_anchors.get((symbol, side))
                    if source_anchor is not None:
                        source_anchor_id = source_anchor.checkpoint_id
                        source_anchor_cut = source_anchor.event_cut
                        source_anchor_kind = "recovery_checkpoint"
                        source_stream_id = source_anchor.stream_id
                        source_stream_epoch = source_anchor.stream_epoch
                    elif position.position_amt == Decimal("0"):
                        source_anchor_id = (
                            PositionRecoveryCodec.stable_snapshot_anchor_id(position)
                        )
                        source_anchor_cut = position.observed_at
                        source_anchor_kind = "zero_snapshot"
                        source_stream_id = None
                        source_stream_epoch = None
                    else:
                        # A non-flat snapshot alone cannot invent the missing
                        # cost basis, batch identities, or fill history.
                        continue
                    # A zero snapshot is its own recovery cut. Scanning fills
                    # from that timestamp through the same timestamp has no
                    # interval to reconcile, so treat the anchor as complete
                    # locally instead of issuing one private REST request per
                    # flat position row (Binance V2 returns hundreds of them).
                    if source_anchor_cut >= position.observed_at:
                        continue
                    origin_ms = int(source_anchor_cut.timestamp() * 1000)
                    if origin_ms > int(position.observed_at.timestamp() * 1000):
                        continue
                    scoped_fills, page_scan = await scan_fetcher(
                        symbol,
                        start_time_ms=origin_ms,
                        checked_through=position.observed_at,
                    )
                    for fill in scoped_fills:
                        if fill.trade_at <= position.observed_at:
                            fills_by_key[(fill.symbol, fill.trade_id)] = fill
                    fill_load_scans.append(
                        AccountFillLoadScan(
                            environment=config.environment,
                            account_label=config.account_label,
                            symbol=symbol,
                            position_side=side,
                            page_scan=page_scan,
                            observed_at=position.observed_at,
                            source_anchor_id=source_anchor_id,
                            source_anchor_event_cut=source_anchor_cut,
                            source_anchor_kind=source_anchor_kind,
                            source_stream_id=source_stream_id,
                            source_stream_epoch=source_stream_epoch,
                        )
                    )

            # The older cursor path still imports real trade rows for symbols
            # without a verified cut, but it never emits completeness proof.
            complete_scan_symbols = {
                item.symbol
                for item in fill_load_scans
                if item.page_scan.page_exhausted and not item.page_scan.truncated
            }
            fallback_symbols = tuple(
                sorted(set(tracked_fill_symbols) - complete_scan_symbols)
            )
            fallback_from_ids = {
                symbol: cursor
                for symbol, cursor in from_id_by_symbol.items()
                if symbol in fallback_symbols
            }
            fallback_start_times = {
                symbol: cursor
                for symbol, cursor in start_time_by_symbol.items()
                if symbol in fallback_symbols
            }
            fallback_fills = (
                await self._client.fetch_recent_fills(
                    fallback_symbols,
                    from_id_by_symbol=fallback_from_ids,
                    start_time_by_symbol=fallback_start_times,
                )
                if include_fills and fallback_symbols
                else ()
            )
            for fill in fallback_fills:
                fills_by_key[(fill.symbol, fill.trade_id)] = fill
            fills = tuple(
                sorted(
                    fills_by_key.values(),
                    key=lambda item: (item.trade_at, item.symbol, item.trade_id),
                )
            )
            incomplete_symbols: set[str] = set(
                incomplete_fill_symbols(self._client)
            )
            fills_catching_up = bool(incomplete_symbols) or any(
                not scan.page_scan.page_exhausted or scan.page_scan.truncated
                for scan in fill_load_scans
            )
            fill_keys = account_fill_keys(fills)
            new_fill_keys = frozenset(
                key
                for key in fill_keys
                if (key[0] in previous_fill_cursors or key[0] in newly_active_symbols)
                and key not in self._known_fill_keys
            )
            fill_count_by_symbol = fill_counts_by_symbol(fills)
            next_fill_cursors = (
                advance_fill_cursors(
                    previous_fill_cursors,
                    tracked_fill_symbols,
                    fills,
                    observed_at=config.observed_at,
                )
                if include_fills
                else previous_fill_cursors
            )
            fill_cursor_updates = (
                tuple(
                    AccountFillReconciliationCursor(
                        environment=config.environment,
                        account_label=config.account_label,
                        symbol=symbol,
                        from_id=cursor.from_id,
                        start_time_ms=(
                            None if cursor.from_id is not None else cursor.start_time_ms
                        ),
                        last_checked_at=config.observed_at,
                    )
                    for symbol in tracked_fill_symbols
                    if (cursor := next_fill_cursors.get(symbol)) is not None
                    and (cursor.from_id is not None or cursor.start_time_ms is not None)
                )
                if include_fills
                else ()
            )
            new_fills = tuple(
                fill
                for fill in fills
                if (fill.symbol.strip().upper(), fill.trade_id.strip()) in new_fill_keys
            )
            self._remember_balance_values(balances)
            for key in fill_keys:
                self._remember_fill_key(key)
            self._has_completed_sync = not fills_catching_up
            result = ExecutionAccountSyncResult(
                status=(
                    ExecutionAccountStatus.SYNCING
                    if fills_catching_up
                    else ExecutionAccountStatus.READY_READONLY
                ),
                reconciliation_id=reconciliation_id,
                mismatch_count=0,
                snapshot=AccountSnapshot(
                    config=account_config,
                    balances=balances,
                    # Keep every explicit V2 row. Absence in a response is
                    # never converted into a zero position.
                    positions=positions,
                    open_orders=open_orders,
                ),
                fill_count=len(fills),
                fills=fills,
                new_fills=new_fills,
                new_fill_keys=new_fill_keys,
                fill_count_by_symbol=fill_count_by_symbol,
                fill_cursor_updates=fill_cursor_updates,
                fill_load_scans=tuple(fill_load_scans),
                fills_catching_up=fills_catching_up,
            )
            if result.snapshot is not None:
                self._remember_observation(result.snapshot.config.observed_at)
            if persist:
                await self.persist_reconciliation_result(
                    result,
                    source="rest_reconciliation",
                )
            return result
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

    async def persist_reconciliation_result(
        self,
        result: ExecutionAccountSyncResult,
        *,
        source: str | None = "rest_reconciliation",
    ) -> None:
        """Persist a ready result after its real-time publication.

        This method deliberately accepts an already materialized result so
        the account daemon can publish the fresh state first and let a slow
        database catch up independently.
        """
        if (
            result.status
            not in (
                ExecutionAccountStatus.READY_READONLY,
                ExecutionAccountStatus.SYNCING,
            )
            or result.snapshot is None
        ):
            raise ValueError("only a ready or syncing account result can be persisted")
        snapshot = result.snapshot
        if (
            self._latest_observation_at is not None
            and snapshot.config.observed_at < self._latest_observation_at
        ):
            # Stale snapshot: skip snapshot, balances, positions, open orders,
            # and status update.  HOWEVER, fills and cursor updates are immutable
            # or monotonic progress: persist them atomically without the stale state.
            if result.fills or result.fill_cursor_updates:
                await self._repository.save_reconciliation_fills_and_cursors(
                    fills=result.fills,
                    cursors=result.fill_cursor_updates,
                )
                if result.fill_cursor_updates:
                    self._update_fill_cursors_monotonically(result.fill_cursor_updates)
            return
        config = replace(
            self._config,
            observed_at=snapshot.config.observed_at,
        )
        details: dict[str, JsonValue] = {}
        if source is not None:
            details["source"] = source
        if result.fills_catching_up:
            details["fills_catching_up"] = True
            details["incomplete_symbols"] = [
                symbol for symbol in sorted(incomplete_fill_symbols(self._client))
            ]
        details.update(position_state_details(snapshot.positions))
        # Same sparsify rule as snapshot_once / user-data persist: the in-memory
        # snapshot keeps every asset, but durable history only stores non-zero
        # balances and the zero that closes a previously non-zero asset.
        # persist_reconciliation_result is the daemon's main write path and had
        # been inserting the full multi-asset zero set every cycle.
        persisted_balances = select_balance_history(snapshot.balances, self._last_balance_values)
        persisted_positions = self._positions_to_persist(
            snapshot.positions,
            observed_at=snapshot.config.observed_at,
        )
        await self._repository.save_reconciliation_snapshot(
            config=snapshot.config,
            balances=persisted_balances,
            positions=persisted_positions,
            open_orders=snapshot.open_orders,
            fills=result.fills,
            cursors=result.fill_cursor_updates,
            run=reconciliation_run(
                config,
                reconciliation_id=result.reconciliation_id,
                status="catching_up" if result.fills_catching_up else "ready",
                mismatch_count=result.mismatch_count,
                details=details,
                balance_count=len(persisted_balances),
                position_count=sum(
                    1 for p in snapshot.positions if p.position_amt != Decimal("0")
                ),
                open_order_count=len(snapshot.open_orders),
                fill_count=result.fill_count,
            ),
        )
        self._remember_position_signatures(
            persisted_positions,
            observed_at=snapshot.config.observed_at,
        )
        if result.fill_cursor_updates:
            self._update_fill_cursors_monotonically(result.fill_cursor_updates)
        await self._save_state(
            (
                ExecutionAccountStatus.SYNCING
                if result.fills_catching_up
                else ExecutionAccountStatus.READY_READONLY
            ),
            reason="fills_catching_up" if result.fills_catching_up else None,
            config=config,
        )

    def _update_fill_cursors_monotonically(
        self,
        cursors: Sequence[AccountFillReconciliationCursor],
    ) -> None:
        """Update in-memory fill cursors monotonically to prevent regression."""
        for cursor in cursors:
            sym = cursor.symbol.strip().upper()
            current = self._fill_cursors.get(sym)
            merged = merge_fill_cursor(current, cursor)
            if merged is None:
                continue
            self._fill_cursors[sym] = merged
            current_checked_at = self._fill_cursor_checked_at.get(sym)
            if (
                current_checked_at is None
                or cursor.last_checked_at >= current_checked_at
            ):
                self._fill_cursor_checked_at[sym] = cursor.last_checked_at

    async def persist_user_data_event(
        self,
        *,
        snapshot: AccountSnapshot,
        event: BinanceUserDataEvent,
        fills: tuple[AccountFillEvent, ...] = (),
    ) -> ExecutionAccountSyncResult:
        """Persist a fully merged WebSocket account observation atomically.

        The in-memory snapshot remains complete; only the high-frequency
        balance history is stored sparsely.
        """
        if snapshot.config.environment != self._config.environment:
            raise ValueError("account snapshot environment does not match sync config")
        if snapshot.config.account_label != self._config.account_label:
            raise ValueError(
                "account snapshot account label does not match sync config"
            )
        config = replace(self._config, observed_at=event.received_at)
        self._remember_observation(event.received_at)
        active_positions = tuple(
            position for position in snapshot.positions if position.position_amt != 0
        )
        self._active_position_keys = _position_keys(active_positions)
        reconciliation_id = user_data_reconciliation_id(
            config,
            event.event_id,
        )
        persisted_balances = select_balance_history(snapshot.balances, self._last_balance_values)
        persisted_positions = self._positions_to_persist(
            snapshot.positions,
            observed_at=event.received_at,
        )
        event_state = (
            ExecutionAccountStatus.SYNCING
            if (
                not self._has_completed_sync
                or self._last_persisted_process_state
                is ExecutionAccountStatus.SYNCING
            )
            else ExecutionAccountStatus.READY_READONLY
        )
        event_reason = (
            self._last_persisted_process_state_reason
            if event_state is self._last_persisted_process_state
            else (
                "fills_catching_up"
                if event_state is ExecutionAccountStatus.SYNCING
                else None
            )
        )
        account_config = self._latest_rest_account_config or snapshot.config
        await self._repository.save_reconciliation_snapshot(
            # Keep the last REST account-config observation as the identity of
            # the account-level margin snapshot. A WebSocket event only
            # changes balances/positions; stamping the stale REST payload with
            # the event time would make it look like a fresh margin reading.
            config=account_config,
            balances=persisted_balances,
            positions=persisted_positions,
            open_orders=snapshot.open_orders,
            fills=fills,
            run=reconciliation_run(
                config,
                reconciliation_id=reconciliation_id,
                status="ready",
                mismatch_count=0,
                details={
                    "source": "user_data_stream",
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "event_at": event.event_at.isoformat(),
                    **position_state_details(snapshot.positions),
                },
                balance_count=len(persisted_balances),
                position_count=len(active_positions),
                open_order_count=len(snapshot.open_orders),
                fill_count=len(fills),
            ),
        )
        self._remember_balance_values(snapshot.balances)
        self._remember_position_signatures(
            persisted_positions,
            observed_at=event.received_at,
        )
        await self._save_state(
            event_state,
            reason=event_reason,
            config=config,
        )
        return ExecutionAccountSyncResult(
            status=event_state,
            reconciliation_id=reconciliation_id,
            mismatch_count=0,
            snapshot=snapshot,
            fill_count=len(fills),
            fills=fills,
            new_fills=fills,
            new_fill_keys=frozenset(account_fill_keys(fills)),
            fill_count_by_symbol=fill_counts_by_symbol(fills),
            fills_catching_up=event_state is ExecutionAccountStatus.SYNCING,
        )

    def _fill_symbols_for_reconciliation(
        self,
        *,
        active_fill_symbols: set[str],
        observed_at: datetime,
    ) -> tuple[str, ...]:
        historical_cutoff = observed_at - self._historical_fill_reconciliation_interval
        due_historical_symbols = {
            symbol
            for symbol in self._tracked_fill_symbols
            if (
                symbol not in active_fill_symbols
                and (
                    self._fill_cursor_checked_at.get(symbol) is None
                    or self._fill_cursor_checked_at[symbol] <= historical_cutoff
                )
            )
        }
        historical_symbols = sorted(
            due_historical_symbols,
            key=lambda symbol: (
                self._fill_cursor_checked_at.get(symbol)
                or datetime.min.replace(tzinfo=UTC),
                symbol,
            ),
        )[: self._config.historical_fill_reconciliation_batch_size]
        return tuple(sorted(active_fill_symbols | set(historical_symbols)))

    def _positions_to_persist(
        self,
        positions: tuple[AccountPositionSnapshot, ...],
        *,
        observed_at: datetime,
    ) -> tuple[AccountPositionSnapshot, ...]:
        """Drop zero rows and collapse identical short-window observations.

        A single close can fan out into many ACCOUNT_UPDATE events.  Keeping
        every intermediate view stamped the same symbol with 266 / 240 / 17 /
        0 rows microseconds apart and made the latest-position view ambiguous.
        """

        persisted: list[AccountPositionSnapshot] = []
        for position in positions:
            key = (position.symbol, position.position_side)
            last = self._last_position_signatures.get(key)
            if not should_persist_position(position, last, observed_at=observed_at):
                continue
            persisted.append(position)
            self._last_position_signatures[key] = (
                position.position_amt,
                position.entry_price,
                observed_at,
            )
        return tuple(persisted)

    def _remember_position_signatures(
        self,
        positions: tuple[AccountPositionSnapshot, ...],
        *,
        observed_at: datetime,
    ) -> None:
        for position in positions:
            key = (position.symbol, position.position_side)
            self._last_position_signatures[key] = (
                position.position_amt,
                position.entry_price,
                observed_at,
            )

    def _remember_balance_values(
        self,
        balances: tuple[AccountBalanceSnapshot, ...],
    ) -> None:
        for balance in balances:
            self._last_balance_values[balance.asset] = balance_value(balance)

    def _remember_observation(self, observed_at: datetime) -> None:
        if (
            self._latest_observation_at is None
            or observed_at > self._latest_observation_at
        ):
            self._latest_observation_at = observed_at

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
        state: ExecutionAccountStatus | None = None,
    ) -> None:
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        target_state = state
        if target_state is None:
            if (
                not self._has_completed_sync
                or self._last_persisted_process_state == ExecutionAccountStatus.SYNCING
            ):
                target_state = ExecutionAccountStatus.SYNCING
            else:
                target_state = ExecutionAccountStatus.READY_READONLY
        reason = (
            self._last_persisted_process_state_reason
            if target_state == self._last_persisted_process_state
            else (
                "fills_catching_up"
                if target_state == ExecutionAccountStatus.SYNCING
                else None
            )
        )
        await self._save_state(
            target_state,
            reason=reason,
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
        observed_at = resolved_config.observed_at
        # Persist every state transition.  Re-persist the same state only
        # after the refresh window so "latest row" age checks stay meaningful
        # without writing a heartbeat every poll.
        if (
            state == self._last_persisted_process_state
            and reason == self._last_persisted_process_state_reason
            and self._last_persisted_process_state_at is not None
            and observed_at - self._last_persisted_process_state_at
            < _PROCESS_STATE_REFRESH
        ):
            return
        await self._repository.save_process_state(
            ExecutionAccountProcessState(
                environment=resolved_config.environment,
                account_label=resolved_config.account_label,
                state=state,
                occurred_at=observed_at,
                reason=reason,
            )
        )
        self._last_persisted_process_state = state
        self._last_persisted_process_state_reason = reason
        self._last_persisted_process_state_at = observed_at


def _position_cut_for_trade_scan(
    snapshot: AccountPositionSnapshot,
) -> AccountPositionSnapshot:
    """Align account facts to Binance's millisecond trade-query boundary."""
    cut_ms = int(snapshot.observed_at.timestamp() * 1000)
    cut = datetime.fromtimestamp(cut_ms / 1000, tz=UTC)
    return replace(snapshot, observed_at=cut)


def _position_keys(
    positions: tuple[AccountPositionSnapshot, ...],
) -> set[tuple[str, str]]:
    return {(position.symbol, position.position_side) for position in positions}
