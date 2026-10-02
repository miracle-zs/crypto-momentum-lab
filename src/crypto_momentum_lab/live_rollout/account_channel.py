"""Runtime policy for account-event fan-out in live execution."""

import asyncio
import inspect
from collections import deque
from collections.abc import AsyncIterable, Awaitable, Callable

import structlog

from crypto_momentum_lab.execution_account.hub import AccountEvent
from crypto_momentum_lab.live_rollout.account_event_ports import (
    AccountEventExitProcessor,
    AccountEventOrderReconciler,
)
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
        run_id: str | None = None,
        telemetry: AccountFillSink | None = None,
        is_transient_error: Callable[[Exception], bool],
        is_order_identity_conflict: Callable[[Exception], bool] | None = None,
        on_exit_failure: Callable[[str, str | None], None] | None = None,
        on_account_snapshot: Callable[[AccountEvent], Awaitable[None] | None]
        | None = None,
        on_account_snapshot_recovery: Callable[[str], None] | None = None,
    ) -> None:
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
        self._seen_fill_keys: set[tuple[str, str]] = set()
        self._seen_fill_order: deque[tuple[str, str]] = deque(
            maxlen=_MAX_SEEN_FILL_KEYS
        )

    async def run(self, source: AsyncIterable[AccountEvent]) -> None:
        """Consume a reconnecting account stream until it closes."""

        async for event in resilient_account_event_stream(source):
            # Retain this exact envelope until its account facts are applied.
            # A later full snapshot does not necessarily include these fills.
            for attempt in range(3):
                error = await self._process_event(event)
                if error is None:
                    break
                if attempt == 2:
                    raise error
                await asyncio.sleep(0.1 * (attempt + 1))

    async def _process_event(self, event: AccountEvent) -> Exception | None:
        reconciliation_run_id = self._reconciliation_run_id
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
            event_run_id = self._reconciliation_run_id
            # ORDER_TRADE_UPDATE can carry both the account projection and
            # the order identity. Reconcile the order first so the live
            # context cannot observe a newly opened position before its
            # matching entry fill/order state is durable.
            if self._on_account_snapshot is not None:
                applying_snapshot = True
                result = self._on_account_snapshot(event)
                if inspect.isawaitable(result):
                    await result
                applying_snapshot = False
            # Observability must not prevent real trade facts from reaching
            # the durable Book and the account projection.
            if (
                self._telemetry is not None
                and event.has_fill
                and self._remember_fill(event)
            ):
                await self._telemetry.account_fill(
                    event,
                    occurred_at=event.received_at,
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
        except asyncio.CancelledError:
            raise
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
                run_id=self._reconciliation_run_id,
                symbol=event.symbol,
            )
            return True
        key = (event.symbol or "UNKNOWN", trade_id)
        if key in self._seen_fill_keys:
            log.info(
                "live_account_fill_duplicate_ignored",
                run_id=self._reconciliation_run_id,
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
                run_id=self._reconciliation_run_id,
                reason=reason,
            )

    @property
    def _reconciliation_run_id(self) -> str:
        if self._order_reconciliation is not None:
            return self._order_reconciliation.run_id
        return self._run_id or "unknown"


__all__ = ["LiveAccountEventRuntime"]
