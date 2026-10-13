"""In-process private-stream producer for the merged single-account runtime.

The account-sync process used to own the merged account projection (balances,
positions, open orders, account state) and the strategy process used to own
execution.  After the merge, this producer is the one current-account fact
producer: it consumes the Binance user-data stream, merges every frame onto an
accepted REST baseline, and publishes both the execution frames and the merged
account projection to the in-process event source.

Continuity is explicit.  A successful connection is not fact continuity: the
source only admits new risk after a baseline has been accepted for the current
connection, and a disconnect, epoch change or sequence gap marks the affected
scope as a fact gap until a bounded reconciliation pass clears it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import structlog

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.domain.account.snapshot_models import (
    AccountSnapshot,
)
from crypto_momentum_lab.execution_account.binance.user_data import (
    BinanceUserDataListenKeyClient,
    BinanceUsdMUserDataStream,
)
from crypto_momentum_lab.execution_account.binance.user_data_models import (
    BinanceUserDataEvent,
)
from crypto_momentum_lab.execution_account.expectations import (
    AccountPositionExpectationRegistry,
)
from crypto_momentum_lab.execution_account.hub import (
    AccountEvent,
    InProcessAccountEventSource,
)
from crypto_momentum_lab.execution_account.sync_ports import (
    ReadOnlyAccountClient,
)
from crypto_momentum_lab.execution_account.user_data_sync import (
    AccountUserDataState,
)

log = structlog.get_logger(__name__)

# Persisting a merged observation is a durable side effect, not a transport
# step; a slow database must not stall the private stream reader.
BaselinePublisher = Callable[[AccountSnapshot, AccountEvent], Awaitable[None]]
SnapshotPersister = Callable[[AccountSnapshot, AccountEvent], Awaitable[None]]


def _is_nonzero_quantity(value: object) -> bool:
    try:
        return Decimal(str(value)) != 0
    except Exception:
        return False


def _event_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def convert_user_data_event_to_account_event(
    event: BinanceUserDataEvent,
    *,
    environment: str,
    account_label: str,
    sequence: int,
    stream_epoch: str,
    account_snapshot: AccountSnapshot | None = None,
    account_state: ExecutionAccountStatus | None = None,
) -> AccountEvent:
    symbols: set[str] = set()
    symbol: str | None = None
    client_order_id: str | None = None
    order_status: str | None = None
    order_update: dict[str, object] | None = None
    has_fill = False
    trade_id: str | None = None
    fills: list[AccountFillEvent] = []

    if event.event_type == "ORDER_TRADE_UPDATE":
        order = event.payload.get("o")
        if isinstance(order, dict):
            order_update = dict(order)
            symbol = _event_text(order.get("s"))
            client_order_id = _event_text(order.get("c"))
            order_status = _event_text(order.get("X"))
            exec_type = _event_text(order.get("x"))
            has_fill = exec_type == "TRADE" and _is_nonzero_quantity(order.get("l"))
            if has_fill:
                trade_id = _event_text(order.get("t"))
                try:
                    qty = Decimal(str(order.get("l", "0")))
                    price = Decimal(str(order.get("L", "0")))
                    realized_pnl = Decimal(str(order.get("rp", "0")))
                    fee = Decimal(str(order.get("n", "0")))
                    fee_asset = str(order.get("N", "USDT"))
                    side = str(order.get("S", "BUY"))
                    pos_side = str(order.get("ps", "BOTH"))
                    fill_event = AccountFillEvent(
                        environment=environment,
                        account_label=account_label,
                        symbol=symbol or "",
                        trade_id=trade_id or f"t-{uuid4().hex[:8]}",
                        order_id=str(order.get("i", "")),
                        side=side,
                        price=price,
                        quantity=qty,
                        realized_pnl=realized_pnl,
                        fee=fee,
                        fee_asset=fee_asset,
                        trade_at=event.event_at,
                        raw_payload={"positionSide": pos_side, **order},
                    )
                    fills.append(fill_event)
                except Exception as fill_err:
                    log.warning(
                        "user_data_fill_event_parse_failed", error=str(fill_err)
                    )
            if symbol is not None:
                symbols.add(symbol)
    elif event.event_type == "ACCOUNT_UPDATE":
        account = event.payload.get("a")
        if isinstance(account, dict):
            positions = account.get("P")
            if isinstance(positions, list):
                for position in positions:
                    if isinstance(position, dict):
                        position_symbol = _event_text(position.get("s"))
                        if position_symbol is not None:
                            symbols.add(position_symbol)
    if account_snapshot is not None:
        symbols.update(position.symbol for position in account_snapshot.positions)
        symbols.update(order.symbol for order in account_snapshot.open_orders)

    return AccountEvent(
        environment=environment,
        account_label=account_label,
        event_type=event.event_type,
        event_id=event.event_id,
        event_at=event.event_at,
        received_at=event.received_at,
        symbols=tuple(sorted(symbols)),
        symbol=symbol,
        client_order_id=client_order_id,
        order_status=order_status,
        order_update=order_update,
        has_fill=has_fill,
        trade_id=trade_id,
        fills=tuple(fills),
        sequence=sequence,
        stream_epoch=stream_epoch,
        account_snapshot=account_snapshot,
        account_state=account_state,
    )


def build_snapshot_account_event(
    snapshot: AccountSnapshot,
    *,
    event_id: str,
    sequence: int,
    stream_epoch: str,
    account_state: ExecutionAccountStatus = ExecutionAccountStatus.RUNNING,
) -> AccountEvent:
    """Build the full projection frame published for an accepted baseline."""

    observed_at = snapshot.config.observed_at
    symbols = tuple(
        sorted(
            {
                *(position.symbol for position in snapshot.positions),
                *(order.symbol for order in snapshot.open_orders),
            }
        )
    )
    return AccountEvent(
        environment=snapshot.config.environment,
        account_label=snapshot.config.account_label,
        event_type="ACCOUNT_SNAPSHOT",
        event_id=event_id,
        event_at=observed_at,
        received_at=observed_at,
        symbols=symbols,
        reason=account_state.value,
        sequence=sequence,
        stream_epoch=stream_epoch,
        account_snapshot=snapshot,
        account_state=account_state,
    )


class InProcessAccountStreamProducer:
    """Consumes private user data events, verifies position expectations, and publishes to InProcessAccountEventSource."""

    def __init__(
        self,
        *,
        client: BinanceUserDataListenKeyClient,
        registry: AccountPositionExpectationRegistry,
        source: InProcessAccountEventSource,
        environment: str = "live",
        account_label: str = "primary",
        stream_epoch: str | None = None,
        websocket_url: str | None = None,
        require_live_stream: bool = False,
        on_unverified_position: Callable[[str, str, Decimal], None] | None = None,
        read_client: ReadOnlyAccountClient | None = None,
        expected_multi_assets_mode: bool = True,
        expected_hedge_mode: bool = True,
        publish_baseline: BaselinePublisher | None = None,
        persist_snapshot: SnapshotPersister | None = None,
    ) -> None:
        if require_live_stream and (websocket_url is None or not websocket_url.strip()):
            raise ValueError(
                "websocket_url must be provided for live in-process account stream producer"
            )
        self._client = client
        self._registry = registry
        self._source = source
        self._environment = environment
        self._account_label = account_label
        self._stream_epoch = stream_epoch or f"epoch-{uuid4().hex[:8]}"
        self._sequence = 0
        self._stopping = False
        self._task: asyncio.Task[None] | None = None
        self._events_processed_count = 0
        self._stream: BinanceUsdMUserDataStream | None = None
        self._stream_connected = False
        self._on_unverified_position = on_unverified_position
        self._read_client = read_client
        self._expected_multi_assets_mode = expected_multi_assets_mode
        self._expected_hedge_mode = expected_hedge_mode
        self._publish_baseline = publish_baseline
        self._persist_snapshot = persist_snapshot
        self._state: AccountUserDataState | None = None
        self._account_state = ExecutionAccountStatus.STARTING
        self._require_baseline = True
        self._baseline_observed_at: datetime | None = None
        self._last_persisted_observed_at: datetime | None = None
        self._baseline_lock: asyncio.Lock | None = None
        self._connected_event: asyncio.Event | None = None
        self._baseline_task: asyncio.Task[None] | None = None
        if websocket_url and websocket_url.strip():
            self._stream = BinanceUsdMUserDataStream(
                listen_key_client=client,
                websocket_url=websocket_url.strip(),
                on_event=self.handle_user_data_event,
                on_connection_change=self._on_stream_connection_change,
            )

    @property
    def stream_epoch(self) -> str:
        return self._stream_epoch

    @property
    def sequence(self) -> int:
        return self._sequence

    @property
    def account_state(self) -> ExecutionAccountStatus:
        return self._account_state

    @property
    def account_snapshot(self) -> AccountSnapshot | None:
        return (
            None
            if self._state is None
            else self._state.snapshot(
                self._baseline_observed_at or datetime.now(tz=UTC)
            )
        )

    async def baseline(self, *, force: bool = False) -> AccountSnapshot:
        """Fetch and accept the authoritative REST baseline for this account.

        The startup (and post-reconnect) baseline is what makes an existing
        position or balance trusted; without it no frame is admissible.
        """

        if self._read_client is None:
            raise ValueError("read_client is required to establish an account baseline")
        if self._baseline_lock is None:
            self._baseline_lock = asyncio.Lock()
        async with self._baseline_lock:
            if force:
                self._require_baseline = True
            if not self._require_baseline:
                snapshot = self.account_snapshot
                if snapshot is not None:
                    return snapshot
            epoch = self._stream_epoch
            account_config = await self._read_client.fetch_account_config()
            if account_config.multi_assets_mode != self._expected_multi_assets_mode:
                raise RuntimeError(
                    "account multi-assets mode does not match the runtime expectation"
                )
            if account_config.hedge_mode != self._expected_hedge_mode:
                raise RuntimeError(
                    "account position mode does not match the runtime expectation"
                )
            balances = await self._read_client.fetch_balances()
            positions = await self._read_client.fetch_positions(include_flat=True)
            open_orders = await self._read_client.fetch_open_orders()
            snapshot = AccountSnapshot(
                config=account_config,
                balances=balances,
                positions=positions,
                open_orders=open_orders,
            )
            if self._state is None:
                self._state = AccountUserDataState(
                    snapshot,
                    expected_position_registry=self._registry,
                )
            else:
                self._state.replace_snapshot(snapshot)
            if epoch != self._stream_epoch:
                raise RuntimeError("account connection changed during baseline load")
            await self._accept_baseline(snapshot, stream_epoch=epoch)
            self._baseline_observed_at = account_config.observed_at
            self._require_baseline = False
            return snapshot

    def _next_sequence(self) -> int:
        """Assign the next frame sequence on the current connection epoch."""

        self._sequence += 1
        return self._sequence

    async def _accept_baseline(
        self, snapshot: AccountSnapshot, *, stream_epoch: str
    ) -> None:
        self._account_state = ExecutionAccountStatus.RUNNING
        # The baseline is itself a frame on the new epoch and consumes a
        # sequence number, so the first business frame after it is sequence 2
        # and the source's watermark exactly matches the frames it will see.
        sequence = self._next_sequence()
        # An accepted REST baseline is a complete authoritative account view, so
        # it resolves the fact gaps opened by the transport break that forced it.
        event = build_snapshot_account_event(
            snapshot,
            event_id=f"baseline:{stream_epoch}:{sequence}",
            sequence=sequence,
            stream_epoch=stream_epoch,
            account_state=self._account_state,
        )
        if self._publish_baseline is not None:
            await self._publish_baseline(snapshot, event)
        if stream_epoch != self._stream_epoch or (
            self._stream is not None and not self._stream_connected
        ):
            raise RuntimeError("account connection changed during baseline acceptance")
        self._source.clear_fact_gap()
        self._source.mark_continuity_verified(
            stream_epoch=stream_epoch,
            sequence=sequence,
        )
        if self._persist_snapshot is not None:
            await self._schedule_persistence(snapshot, event)

    def _on_stream_connection_change(
        self,
        connected: bool,
        reason: str | None,
    ) -> None:
        """React to transport changes without equating a connect with continuity."""
        self._stream_connected = connected
        if connected:
            self._require_baseline = True
            self._stream_epoch = f"epoch-{uuid4().hex[:8]}"
            self._sequence = 0
            self._source.mark_stream_connected(needs_recovery=True)
            if self._connected_event is None:
                self._connected_event = asyncio.Event()
            self._connected_event.set()
            log.warning(
                "in_process_account_stream_connected_recovering",
                account_label=self._account_label,
                reason=reason,
            )
        else:
            self._require_baseline = True
            self._source.mark_stream_disrupted(reason or "account_stream_disconnected")
            log.warning(
                "in_process_account_stream_disrupted",
                account_label=self._account_label,
                reason=reason,
            )

    async def handle_user_data_event(self, event: BinanceUserDataEvent) -> None:
        if self._stopping:
            return
        self._events_processed_count += 1
        if self._read_client is None:
            # No REST baseline is configured (manual/fake dispatch).  The
            # producer still merges frames, but it cannot claim fact continuity.
            self._require_baseline = False
        attempts = 0
        while self._require_baseline:
            attempts += 1
            if attempts > 2:
                raise RuntimeError("account baseline could not be established")
            try:
                await self.baseline()
            except Exception as error:
                self._require_baseline = True
                log.error(
                    "in_process_account_baseline_failed",
                    account_label=self._account_label,
                    error=str(error),
                )
                raise
        state = self._state
        if state is None:
            raise RuntimeError("account state is not available for event processing")
        previous = state.snapshot(event.received_at)
        try:
            update = state.apply(event)
        except Exception as error:
            # A frame that cannot be merged is a fact gap for its symbols, not
            # a locally ignored message.
            self._source.mark_stream_disrupted(
                f"account_event_merge_failed:{type(error).__name__}"
            )
            log.error(
                "in_process_account_event_merge_failed",
                account_label=self._account_label,
                event_type=event.event_type,
                error=str(error),
            )
            raise
        if update.reason is not None:
            # Unknown account state cannot be admitted.  The isolation scope is
            # the affected range: an unknown position isolates its own symbol,
            # while an account-level reason (unknown balance, config change)
            # can affect every symbol and therefore stays account-wide.
            unverified_positions = state.new_unverified_positions(previous)
            if unverified_positions:
                affected = tuple(
                    sorted({position.symbol for position in unverified_positions})
                )
            else:
                affected = ("*",)
            self._source._set_fact_gap(affected)
            self._source._request_recovery(affected)
            log.warning(
                "in_process_account_event_requires_reconciliation",
                account_label=self._account_label,
                event_type=event.event_type,
                reason=update.reason,
                symbols=list(affected),
            )
            for position in unverified_positions:
                if self._on_unverified_position is not None:
                    self._on_unverified_position(
                        position.symbol,
                        position.position_side,
                        position.position_amt,
                    )
        sequence = self._next_sequence()
        account_event = convert_user_data_event_to_account_event(
            event,
            environment=self._environment,
            account_label=self._account_label,
            sequence=sequence,
            stream_epoch=self._stream_epoch,
            account_snapshot=update.snapshot,
            account_state=self._account_state,
        )
        if not self._source.validate_stream_continuity(account_event):
            log.warning(
                "in_process_account_event_continuity_gap",
                account_label=self._account_label,
                event_type=event.event_type,
                sequence=self._sequence,
            )
        self._source.publish(account_event)
        if (
            update.changed
            and self._persist_snapshot is not None
            and self._is_new_observation(update.snapshot.config.observed_at)
        ):
            await self._schedule_persistence(update.snapshot, account_event)

    def _is_new_observation(self, observed_at: datetime) -> bool:
        """Only a newer merged observation may be written; older frames cannot roll back."""

        return (
            self._last_persisted_observed_at is None
            or observed_at > self._last_persisted_observed_at
        )

    async def _schedule_persistence(
        self,
        snapshot: AccountSnapshot,
        event: AccountEvent,
    ) -> None:
        persister = self._persist_snapshot
        if persister is None:
            return
        try:
            await persister(snapshot, event)
        except Exception as error:
            # Persistence of the account projection must not fabricate a trade
            # result, but a lost observation is a fact gap for the dashboard and
            # for restart continuity.
            log.error(
                "in_process_account_snapshot_persist_failed",
                account_label=self._account_label,
                event_id=event.event_id,
                error=str(error),
            )
            self._source._set_fact_gap(("*",))
            return
        observed_at = snapshot.config.observed_at
        if (
            self._last_persisted_observed_at is None
            or observed_at > self._last_persisted_observed_at
        ):
            self._last_persisted_observed_at = observed_at

    async def run(self) -> None:
        if self._stream is None:
            # Idle wait until stopped when using manual/fake event dispatch
            while not self._stopping:
                await asyncio.sleep(1.0)
            return
        self._baseline_task = asyncio.create_task(
            self._run_baseline_on_connect(),
            name=f"in-process-account-baseline:{self._account_label}",
        )
        try:
            await self._stream.run()
        finally:
            if self._baseline_task is not None and not self._baseline_task.done():
                self._baseline_task.cancel()
                try:
                    await self._baseline_task
                except (asyncio.CancelledError, Exception):
                    pass

    async def _run_baseline_on_connect(self) -> None:
        """Establish the authoritative baseline as soon as the stream connects.

        Waiting for the first business frame would leave the account view stale
        between connect and the first event; the REST baseline is the accepted
        starting point for every subsequent delta.
        """

        if self._connected_event is None:
            self._connected_event = asyncio.Event()
        while not self._stopping:
            try:
                await asyncio.wait_for(self._connected_event.wait(), timeout=60.0)
            except TimeoutError:
                if not self._stream_connected:
                    continue
            self._connected_event.clear()
            if self._stopping:
                return
            if self._read_client is None:
                return
            try:
                await self.baseline(force=True)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                log.error(
                    "in_process_account_baseline_task_failed",
                    account_label=self._account_label,
                    error=str(error),
                )
                self._source.mark_stream_disrupted(
                    f"account_baseline_failed:{type(error).__name__}"
                )

    def start(self) -> asyncio.Task[None]:
        self._task = asyncio.create_task(
            self.run(),
            name=f"in-process-account-producer:{self._account_label}",
        )
        return self._task

    async def aclose(self) -> None:
        self._stopping = True
        if self._connected_event is None:
            self._connected_event = asyncio.Event()
        self._connected_event.set()
        if self._stream is not None:
            await self._stream.stop()
        for task in (self._baseline_task, self._task):
            if task is None or task.done():
                continue
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
            except (TimeoutError, asyncio.CancelledError, Exception):
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    def stop(self) -> None:
        self._stopping = True
        if self._connected_event is None:
            self._connected_event = asyncio.Event()
        self._connected_event.set()
        if self._stream is not None:
            self._stream._stopping = True
        if self._task is not None and not self._task.done():
            self._task.cancel()


__all__ = [
    "BaselinePublisher",
    "InProcessAccountStreamProducer",
    "SnapshotPersister",
    "build_snapshot_account_event",
    "convert_user_data_event_to_account_event",
]
