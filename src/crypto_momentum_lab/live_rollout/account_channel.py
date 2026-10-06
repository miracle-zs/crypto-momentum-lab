"""Runtime policy for account-event fan-out in live execution."""

import asyncio
from collections import deque
from collections.abc import AsyncIterable, Awaitable, Callable
from dataclasses import replace
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.market.models import (
    MarketState15s,
    RealtimeMarketQuote,
)
from crypto_momentum_lab.execution_account.hub import AccountEvent
from crypto_momentum_lab.live_rollout.exit_failure_policy import (
    ORDER_IDENTITY_CONFLICT_REASON,
    is_pending_exit_evaluation,
)
from crypto_momentum_lab.live_rollout.market_cache import (
    LatestMarketQuoteCache,
    LatestMarketStateCache,
)
from crypto_momentum_lab.live_rollout.stream_recovery import (
    resilient_account_event_stream,
)
from crypto_momentum_lab.live_rollout.telemetry_ports import AccountFillSink

log = structlog.get_logger()

_MAX_SEEN_FILL_KEYS = 8192
_DEFAULT_INGRESS_QUEUE_SIZE = 256


class _StreamEnded:
    pass


_STREAM_ENDED = _StreamEnded()


class AccountEventExitProcessor(Protocol):
    async def process_account_event(
        self,
        state: MarketState15s,
        *,
        quote: RealtimeMarketQuote | None = None,
    ) -> str | None: ...


class AccountEventOrderReconciler(Protocol):
    @property
    def run_id(self) -> str: ...

    async def reconcile_account_event(self, event: AccountEvent) -> None: ...


def _never_order_identity_conflict(_error: Exception) -> bool:
    return False


class LiveAccountEventRuntime:
    """Reconcile and fan out account events without losing exit safety."""

    def __init__(
        self,
        *,
        daemon: AccountEventExitProcessor,
        latest_market_states: LatestMarketStateCache,
        latest_market_quotes: LatestMarketQuoteCache,
        order_reconciliation: AccountEventOrderReconciler | None = None,
        run_id: str,
        telemetry: AccountFillSink | None = None,
        is_transient_error: Callable[[Exception], bool],
        is_order_identity_conflict: Callable[[Exception], bool] | None = None,
        on_exit_failure: Callable[[str, str | None], None] | None = None,
        on_account_snapshot: Callable[[AccountEvent], Awaitable[None]]
        | None = None,
        on_account_snapshot_recovery: Callable[[str], None] | None = None,
        ingress_queue_size: int = _DEFAULT_INGRESS_QUEUE_SIZE,
    ) -> None:
        if ingress_queue_size <= 0:
            raise ValueError("ingress_queue_size must be positive")
        self._daemon = daemon
        self._latest_market_states = latest_market_states
        self._latest_market_quotes = latest_market_quotes
        self._order_reconciliation = order_reconciliation
        self._run_id = run_id
        self._telemetry = telemetry
        self._is_transient_error = is_transient_error
        self._is_order_identity_conflict = (
            is_order_identity_conflict or _never_order_identity_conflict
        )
        self._on_exit_failure = on_exit_failure
        self._on_account_snapshot = on_account_snapshot
        self._on_account_snapshot_recovery = on_account_snapshot_recovery
        self._ingress_queue_size = ingress_queue_size
        self._seen_fill_keys: set[tuple[str, str]] = set()
        self._seen_fill_order: deque[tuple[str, str]] = deque(
            maxlen=_MAX_SEEN_FILL_KEYS
        )

    async def run(self, source: AsyncIterable[AccountEvent]) -> None:
        """Consume a reconnecting account stream until it closes.

        Network intake must continue while an order update waits on durable
        reconciliation.  The dedicated ingress queue isolates the WebSocket
        reader from that I/O.  Its bounded size remains a safety valve; the
        upstream source owns the protocol-level recovery if both buffers fill.
        """

        ingress: asyncio.Queue[AccountEvent | Exception | _StreamEnded] = asyncio.Queue(
            maxsize=self._ingress_queue_size
        )
        reader = asyncio.create_task(
            self._copy_stream_to_ingress(source, ingress),
            name=f"live-account-ingress:{self._run_id}",
        )
        try:
            while True:
                item = await ingress.get()
                if isinstance(item, _StreamEnded):
                    return
                if isinstance(item, Exception):
                    raise item
                batch = [item]
                while not ingress.empty():
                    next_item = ingress.get_nowait()
                    if isinstance(next_item, _StreamEnded):
                        batch = list(_coalesce_account_event_burst(batch))
                        for event in batch:
                            await self._process_event_with_retries(event)
                        return
                    if isinstance(next_item, Exception):
                        raise next_item
                    batch.append(next_item)
                for event in _coalesce_account_event_burst(batch):
                    await self._process_event_with_retries(event)
        finally:
            if not reader.done():
                reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)

    async def _copy_stream_to_ingress(
        self,
        source: AsyncIterable[AccountEvent],
        ingress: asyncio.Queue[AccountEvent | Exception | _StreamEnded],
    ) -> None:
        try:
            async for event in resilient_account_event_stream(source):
                await ingress.put(event)
        except asyncio.CancelledError:
            # The supervisor is already stopping the consumer. Do not wait to
            # append a sentinel to a queue it will no longer drain.
            return
        except Exception as error:
            await ingress.put(error)
        await ingress.put(_STREAM_ENDED)

    async def _process_event_with_retries(self, event: AccountEvent) -> None:
        """Retain one semantic account fact until it is durably applied."""

        for attempt in range(3):
            error = await self._process_event(event)
            if error is None:
                return
            if attempt == 2:
                raise error
            await asyncio.sleep(0.1 * (attempt + 1))

    async def _process_event(self, event: AccountEvent) -> Exception | None:
        reconciliation_run_id = self._run_id
        applying_snapshot = False
        reconciling_order = False
        try:
            if event.event_type == "ORDER_TRADE_UPDATE" and event.client_order_id:
                if self._order_reconciliation is None:
                    log.warning(
                        "live_account_event_order_reconciliation_unavailable",
                        run_id=reconciliation_run_id,
                        client_order_id=event.client_order_id,
                    )
                else:
                    reconciling_order = True
                    await self._order_reconciliation.reconcile_account_event(event)
                    reconciling_order = False
            event_run_id = self._run_id
            # ORDER_TRADE_UPDATE can carry both the account projection and
            # the order identity. Reconcile the order first so the live
            # context cannot observe a newly opened position before its
            # matching entry fill/order state is durable.
            if self._on_account_snapshot is not None:
                applying_snapshot = True
                await self._on_account_snapshot(event)
                applying_snapshot = False
            # Observability must not prevent real trade facts from reaching
            # the durable Book and the account projection.
            if self._telemetry is not None:
                for fill_event in _fill_telemetry_events(event):
                    if not self._remember_fill(fill_event):
                        continue
                    await self._telemetry.account_fill(
                        fill_event,
                        occurred_at=fill_event.received_at,
                    )
            for state in self._latest_market_states.for_symbols(event.symbols):
                quote = next(
                    iter(self._latest_market_quotes.for_symbols((state.symbol,))),
                    None,
                )
                failure = await self._daemon.process_account_event(
                    state,
                    quote=quote,
                )
                if is_pending_exit_evaluation(failure):
                    # Context publication already sets the pending-position
                    # entry gate. Later account/market events refresh that view;
                    # waiting here would prevent newer account facts arriving.
                    log.warning(
                        "live_account_event_position_sync_pending",
                        run_id=event_run_id,
                        symbol=state.symbol,
                        reason=failure,
                    )
                    continue
                if failure is not None:
                    if self._on_exit_failure is not None:
                        self._on_exit_failure(state.symbol, failure)
                    log.error(
                        "live_account_event_exit_degraded",
                        symbol=state.symbol,
                        reason=failure,
                    )
                    continue
        except Exception as error:
            if (
                reconciling_order
                or applying_snapshot
                or not self._is_transient_error(error)
            ):
                self._request_account_snapshot_recovery(
                    f"account_event_processing_failed:{type(error).__name__}"
                )
            if self._is_order_identity_conflict(error):
                failure = ORDER_IDENTITY_CONFLICT_REASON
                if self._on_exit_failure is not None:
                    for symbol in event.symbols:
                        self._on_exit_failure(symbol, failure)
                log.warning(
                    "live_account_event_processing_degraded",
                    run_id=reconciliation_run_id,
                    event_type=event.event_type,
                    error_type=type(error).__name__,
                    reason=failure,
                )

                return None
            if not self._is_transient_error(error):
                raise
            # Pre-publication failures must retain this envelope for retry.
            # Only failures after fact publication can await later evaluation.
            log.warning(
                "live_account_event_processing_degraded",
                run_id=reconciliation_run_id,
                event_type=event.event_type,
                error_type=type(error).__name__,
            )
            if reconciling_order or applying_snapshot:
                return error
        return None

    def _remember_fill(self, event: AccountEvent) -> bool:
        """Return false for a replayed fill while keeping the stream live."""

        trade_id = event.trade_id
        if trade_id is None:
            log.warning(
                "live_account_fill_missing_trade_id",
                run_id=self._run_id,
                symbol=event.symbol,
            )
            return True
        key = (event.symbol or "UNKNOWN", trade_id)
        if key in self._seen_fill_keys:
            log.info(
                "live_account_fill_duplicate_ignored",
                run_id=self._run_id,
                symbol=key[0],
                trade_id=trade_id,
            )
            return False
        if len(self._seen_fill_order) == self._seen_fill_order.maxlen:
            expired = self._seen_fill_order.popleft()
            self._seen_fill_keys.discard(expired)
        self._seen_fill_order.append(key)
        self._seen_fill_keys.add(key)
        return True

    def _request_account_snapshot_recovery(self, reason: str) -> None:
        if self._on_account_snapshot_recovery is None:
            return
        try:
            self._on_account_snapshot_recovery(reason)
        except Exception:
            # Recovery notification is a safety side effect.  Preserve the
            # original processing error so the supervisor can restart the
            # worker and rebuild all durable projections.
            log.exception(
                "live_account_snapshot_recovery_callback_failed",
                run_id=self._run_id,
                reason=reason,
            )


def _coalesce_account_event_burst(
    events: list[AccountEvent],
) -> tuple[AccountEvent, ...]:
    """Collapse adjacent superseded state frames without losing each fill.

    A Binance fill burst commonly ends with a run of ``ACCOUNT_UPDATE`` frames.
    Those frames contain no fill evidence themselves; after the source has
    materialized their deltas, only the newest complete snapshot is useful.
    Consecutive updates for the same order are similarly reduced to the latest
    cumulative order state, while their distinct fills are retained together.
    """

    account_reduced = _coalesce_adjacent_account_updates(events)
    reduced: list[AccountEvent] = []
    pending: AccountEvent | None = None
    coalesced_order_count = 0
    for event in account_reduced:
        if (
            not isinstance(event, AccountEvent)
            or
            event.event_type != "ORDER_TRADE_UPDATE"
            or not event.client_order_id
            or event.fill_load_scans
        ):
            if pending is not None:
                reduced.append(pending)
                pending = None
            reduced.append(event)
            continue
        if pending is None:
            pending = event
            continue
        if pending.client_order_id != event.client_order_id:
            reduced.append(pending)
            pending = event
            continue
        coalesced_order_count += 1
        pending = _merge_order_trade_updates(pending, event)
    if pending is not None:
        reduced.append(pending)
    if coalesced_order_count:
        log.info(
            "live_account_event_order_updates_coalesced",
            input_event_count=len(account_reduced),
            coalesced_order_update_count=coalesced_order_count,
            output_event_count=len(reduced),
        )
    return tuple(reduced)


def _coalesce_adjacent_account_updates(
    events: list[AccountEvent],
) -> tuple[AccountEvent, ...]:
    reduced: list[AccountEvent] = []
    pending: AccountEvent | None = None
    coalesced_count = 0
    for event in events:
        if (
            not isinstance(event, AccountEvent)
            or event.event_type != "ACCOUNT_UPDATE"
            or event.account_snapshot is None
        ):
            if pending is not None:
                reduced.append(pending)
                pending = None
            reduced.append(event)
            continue
        if pending is None:
            pending = event
            continue
        coalesced_count += 1
        pending = replace(
            event,
            symbols=tuple(sorted(set(pending.symbols) | set(event.symbols))),
            # The source applied every delta before this reducer ran. A full
            # projection makes the skipped transport sequences explicit to
            # the durable consumer instead of forging continuity.
            snapshot_kind="full",
            account_delta=None,
        )
    if pending is not None:
        reduced.append(pending)
    if coalesced_count:
        log.info(
            "live_account_event_account_updates_coalesced",
            input_event_count=len(events),
            coalesced_account_update_count=coalesced_count,
            output_event_count=len(reduced),
        )
    return tuple(reduced)


def _merge_order_trade_updates(
    previous: AccountEvent,
    current: AccountEvent,
) -> AccountEvent:
    fills_by_key = {
        (fill.symbol, fill.trade_id): fill
        for fill in (*previous.fills, *current.fills)
    }
    return replace(
        current,
        symbols=tuple(sorted(set(previous.symbols) | set(current.symbols))),
        fills=tuple(fills_by_key.values()),
        has_fill=previous.has_fill or current.has_fill or bool(fills_by_key),
        # The source applied every delta before this reducer ran. A full
        # projection makes the skipped transport sequences explicit to the
        # durable consumer instead of forging continuity.
        snapshot_kind="full",
        account_delta=None,
    )


def _fill_telemetry_events(event: AccountEvent) -> tuple[AccountEvent, ...]:
    fills = getattr(event, "fills", ())
    if not fills:
        return (event,) if event.has_fill else ()
    return tuple(
        replace(
            event,
            symbol=fill.symbol,
            has_fill=True,
            trade_id=fill.trade_id,
            fills=(fill,),
        )
        for fill in fills
    )


__all__ = ["LiveAccountEventRuntime"]
