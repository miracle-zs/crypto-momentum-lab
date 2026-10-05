import asyncio
import fcntl
import hashlib
import heapq
import hmac
import math
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TypedDict
from urllib.parse import urlencode

import httpx
import structlog

from crypto_momentum_lab.domain.account.models import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountFillEvent,
    AccountFillPageScan,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.exchange_contract import (
    ExchangeBoundaryCallback,
    ExchangeCancellationUnknownError,
    ExchangeOrderAlreadyAbsentError,
    ExchangeOrderQueryUnknownError,
    ExchangeOrderRejectedError,
    ExchangeSubmissionTimeoutError,
    LiveSubmissionDisabledError,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderSnapshot,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError,
)
from crypto_momentum_lab.domain.live_rollout import RollbackCommand
from crypto_momentum_lab.domain.live_rollout.authorization import (
    EMERGENCY_FLATTEN_CONFIRMATION,
    require_authorized_command,
)
from crypto_momentum_lab.execution_account.binance.exit_recovery_rules import (
    exit_position_quantity,
    open_order_matches_exit,
)
from crypto_momentum_lab.execution_account.binance.request_rules import (
    entry_leverage_candidates,
    normalize_fill_cursors,
    normalize_symbols,
    require_margin_type,
)
from crypto_momentum_lab.execution_account.binance.response_rules import (
    exchange_error_code,
    exchange_error_message,
    is_invalid_leverage_rejection,
    retry_after_seconds,
)
from crypto_momentum_lab.execution_account.binance.rest_parser import (
    account_fill_from_trade_item,
    account_open_order_from_item,
    balances_from_response,
    json_mapping,
    order_snapshot_from_response,
    positions_from_response,
    rest_optional_int,
    rest_require_bool,
    rest_require_int,
    rest_require_mapping,
    rest_require_sequence_of_mappings,
    rest_require_string,
)
from crypto_momentum_lab.execution_account.fill_progress import fill_scan_load_id
from crypto_momentum_lab.domain.execution.exit_recovery import (
    ExitRecoveryInspectionUnknownError,
    ExitRecoveryObservation,
)

log = structlog.get_logger(__name__)

# Official Binance USD-M Futures USER_DATA endpoints verified 2026-07-04:
# /fapi/v1/accountConfig, /fapi/v3/balance, /fapi/v3/positionRisk,
# /fapi/v1/openOrders, /fapi/v1/userTrades.

_DEFAULT_REQUEST_TIMEOUT_SECONDS = 5.0
_DEFAULT_CONNECT_TIMEOUT_SECONDS = 5.0
_DEFAULT_POOL_TIMEOUT_SECONDS = 5.0
_FILL_SCAN_WINDOW_MS = 7 * 24 * 60 * 60 * 1000
_FILL_SCAN_RETENTION_MS = 90 * 24 * 60 * 60 * 1000


class _EndpointMetric(TypedDict):
    """One per-endpoint counter row reported by ``endpoint_metrics``."""

    count: int
    error_count: int
    total_ms: float
    last_status: int | None


_COMMAND_EXIT_PRIORITY = 0
_COMMAND_ENTRY_PRIORITY = 10
_COMMAND_BACKGROUND_PRIORITY = 20
_ENTRY_LEVERAGE_WARMUP_CONCURRENCY = 3


class BinanceRateLimitError(httpx.HTTPStatusError):
    """A Binance REST rate-limit response that callers should back off from."""

    def __init__(self, response: httpx.Response) -> None:
        super().__init__(
            f"Binance REST request rate limited with HTTP {response.status_code}",
            request=response.request,
            response=response,
        )
        self.retry_after_seconds = retry_after_seconds(response)


class _AsyncRequestPacer:
    """Rate-limit starts while allowing safety-critical work to jump ahead.

    A lock-based pacer reserves future slots in arrival order.  That is unsafe
    for a live trading client: a burst of entry requests can reserve every
    upcoming slot while a reduce-only exit is waiting.  This pacer keeps the
    same account-wide rate limit but chooses the next queued request by
    priority.  The request itself is still never retried here; the order state
    machine owns ambiguous-outcome reconciliation.
    """

    def __init__(self, min_interval_seconds: float) -> None:
        if min_interval_seconds < 0:
            raise ValueError("min_interval_seconds must not be negative")
        self._min_interval_seconds = min_interval_seconds
        self._condition = asyncio.Condition()
        self._queue: list[tuple[int, int, asyncio.Future[None]]] = []
        self._sequence = 0
        self._worker: asyncio.Task[None] | None = None
        self._active_future: asyncio.Future[None] | None = None
        self._closed = False
        self._next_allowed_at = 0.0

    async def wait(self, *, priority: int = 10) -> None:
        if self._closed:
            raise RuntimeError("request pacer is closed")
        if self._min_interval_seconds == 0:
            return
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        async with self._condition:
            if self._closed:
                raise RuntimeError("request pacer is closed")
            sequence = self._sequence
            self._sequence += 1
            heapq.heappush(self._queue, (priority, sequence, future))
            if self._worker is None:
                self._worker = asyncio.create_task(
                    self._run(),
                    name="binance-request-pacer",
                )
            self._condition.notify()
        try:
            await future
        except asyncio.CancelledError:
            # The worker can safely discard a cancelled future when it reaches
            # it; removing it from the heap here would require another O(n)
            # scan and adds no latency benefit.
            if not future.done():
                future.cancel()
            raise

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        queued = [future for _priority, _sequence, future in self._queue]
        self._queue.clear()
        for future in queued:
            if not future.done():
                future.cancel()
        if self._active_future is not None and not self._active_future.done():
            self._active_future.cancel()
        worker = self._worker
        if worker is None:
            return
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
        self._worker = None

    async def _run(self) -> None:
        try:
            while True:
                async with self._condition:
                    while not self._queue and not self._closed:
                        await self._condition.wait()
                    if self._closed:
                        return
                    _priority, _sequence, future = heapq.heappop(self._queue)
                    if future.done():
                        continue
                    self._active_future = future
                    now = asyncio.get_running_loop().time()
                    delay = max(0.0, self._next_allowed_at - now)
                    self._next_allowed_at = (
                        max(now, self._next_allowed_at) + self._min_interval_seconds
                    )
                if delay > 0:
                    await asyncio.sleep(delay)
                if not future.done():
                    future.set_result(None)
                self._active_future = None
        except asyncio.CancelledError:
            if self._active_future is not None and not self._active_future.done():
                self._active_future.cancel()
            self._active_future = None
            return


class _FileRequestPacer:
    """Coordinate request starts across account processes on one host.

    The execution-account services run in separate containers, so an
    in-process asyncio pacer cannot protect the aggregate Binance request
    budget.  A short-lived advisory lock reserves the next wall-clock slot in
    a shared volume; the network request starts after the lock is released.
    Corrupt or stale state is treated as an empty schedule, which fails safe
    by preserving the configured interval rather than blocking forever.
    """

    def __init__(self, path: str | Path, min_interval_seconds: float) -> None:
        if min_interval_seconds < 0:
            raise ValueError("min_interval_seconds must not be negative")
        path_text = str(path).strip()
        if not path_text:
            raise ValueError("path must not be empty")
        self._path = Path(path_text)
        self._min_interval_seconds = min_interval_seconds

    async def wait(self, *, priority: int = _COMMAND_ENTRY_PRIORITY) -> None:
        del priority
        if self._min_interval_seconds == 0:
            return
        delay = await asyncio.to_thread(self._reserve_slot)
        if delay > 0:
            await asyncio.sleep(delay)

    async def aclose(self) -> None:
        """Match the in-process pacer interface; the lock is per request."""

    def _reserve_slot(self) -> float:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a+", encoding="ascii") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                handle.seek(0)
                raw_next_allowed_at = handle.read().strip()
                try:
                    next_allowed_at = float(raw_next_allowed_at or "0")
                except ValueError:
                    next_allowed_at = 0.0
                now = time.time()
                reserved_at = max(now, next_allowed_at)
                handle.seek(0)
                handle.truncate()
                handle.write(f"{reserved_at + self._min_interval_seconds:.9f}\n")
                handle.flush()
                return max(0.0, reserved_at - now)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class BinanceUsdMPrivateReadClient:
    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        environment: str,
        account_label: str,
        base_url: str = "https://fapi.binance.com",
        http_client: httpx.AsyncClient | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        recv_window_ms: int = 10000,
        request_interval_seconds: float = 0.2,
        command_request_interval_seconds: float | None = None,
        shared_request_pacer_path: str | Path | None = None,
        shared_command_request_pacer_path: str | Path | None = None,
        request_timeout_seconds: float = _DEFAULT_REQUEST_TIMEOUT_SECONDS,
        connect_timeout_seconds: float = _DEFAULT_CONNECT_TIMEOUT_SECONDS,
        pool_timeout_seconds: float = _DEFAULT_POOL_TIMEOUT_SECONDS,
    ) -> None:
        if not api_key.strip():
            raise ValueError("api_key must not be empty")
        if not api_secret.strip():
            raise ValueError("api_secret must not be empty")
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        if recv_window_ms <= 0:
            raise ValueError("recv_window_ms must be positive")
        if request_interval_seconds < 0:
            raise ValueError("request_interval_seconds must not be negative")
        if command_request_interval_seconds is not None and (
            command_request_interval_seconds < 0
        ):
            raise ValueError("command_request_interval_seconds must not be negative")
        if request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be positive")
        if connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be positive")
        if pool_timeout_seconds <= 0:
            raise ValueError("pool_timeout_seconds must be positive")
        self._api_key = api_key
        self._api_secret = api_secret
        self._environment = environment
        self._account_label = account_label
        self._clock = clock
        self._recv_window_ms = recv_window_ms
        self._read_request_pacer = (
            _AsyncRequestPacer(request_interval_seconds)
            if shared_request_pacer_path is None
            else _FileRequestPacer(
                shared_request_pacer_path,
                request_interval_seconds,
            )
        )
        command_interval = (
            request_interval_seconds
            if command_request_interval_seconds is None
            else command_request_interval_seconds
        )
        self._command_request_pacer = (
            _AsyncRequestPacer(command_interval)
            if shared_command_request_pacer_path is None
            else _FileRequestPacer(
                shared_command_request_pacer_path,
                command_interval,
            )
        )
        self._client = http_client or httpx.AsyncClient(
            base_url=base_url,
            timeout=httpx.Timeout(
                request_timeout_seconds,
                connect=connect_timeout_seconds,
                pool=pool_timeout_seconds,
            ),
            trust_env=False,
        )
        self._endpoint_metrics: dict[str, _EndpointMetric] = {}
        self._incomplete_fill_symbols: frozenset[str] = frozenset()

    @property
    def endpoint_metrics(self) -> dict[str, _EndpointMetric]:
        """Return request counts and latency totals without credentials."""
        return {
            path: _EndpointMetric(
                count=values["count"],
                error_count=values["error_count"],
                total_ms=round(values["total_ms"], 3),
                last_status=values["last_status"],
            )
            for path, values in self._endpoint_metrics.items()
        }

    def _record_endpoint(
        self,
        path: str,
        *,
        elapsed_ms: float,
        status: int | None,
        error: bool,
    ) -> None:
        values = self._endpoint_metrics.setdefault(
            path,
            {"count": 0, "error_count": 0, "total_ms": 0.0, "last_status": None},
        )
        values["count"] = int(values["count"]) + 1
        values["error_count"] = int(values["error_count"]) + int(error)
        values["total_ms"] = float(values["total_ms"]) + elapsed_ms
        values["last_status"] = status

    async def fetch_account_config(self) -> AccountConfigSnapshot:
        payload = await self._signed_get("/fapi/v1/accountConfig")
        data = rest_require_mapping(payload)
        hedge_mode = rest_require_bool(data, "dualSidePosition")
        multi_assets_mode = rest_require_bool(data, "multiAssetsMargin")
        raw_payload = json_mapping(data)
        observed_at = self._now()
        return AccountConfigSnapshot(
            environment=self._environment,
            account_label=self._account_label,
            multi_assets_mode=multi_assets_mode,
            hedge_mode=hedge_mode,
            fee_tier=rest_optional_int(data.get("feeTier")),
            observed_at=observed_at,
            raw_payload=raw_payload,
        )

    async def fetch_balances(self) -> tuple[AccountBalanceSnapshot, ...]:
        payload = await self._signed_get("/fapi/v3/balance")
        observed_at = self._now()
        return balances_from_response(
            payload,
            environment=self._environment,
            account_label=self._account_label,
            observed_at=observed_at,
        )

    async def fetch_positions(
        self, *, include_flat: bool = False
    ) -> tuple[AccountPositionSnapshot, ...]:
        """Read actual position rows; V2 retains explicit flat rows for bootstrap.

        V3 omits symbols without positions/open orders. Absence in that response
        must never be converted into an invented zero-position observation.
        """
        path = "/fapi/v2/positionRisk" if include_flat else "/fapi/v3/positionRisk"
        payload = await self._signed_get(path)
        observed_at = self._now()
        return positions_from_response(
            payload,
            environment=self._environment,
            account_label=self._account_label,
            observed_at=observed_at,
        )

    async def fetch_symbol_margin_type(self, symbol: str) -> str | None:
        """Read the exchange's symbol-level futures margin mode."""
        normalized_symbol = normalize_symbols((symbol,))[0]
        payload = await self._signed_get(
            "/fapi/v1/symbolConfig",
            {"symbol": normalized_symbol},
        )
        for item in rest_require_sequence_of_mappings(payload):
            response_symbol = normalize_symbols((rest_require_string(item, "symbol"),))[
                0
            ]
            if response_symbol != normalized_symbol:
                raise ValueError(
                    "Binance symbolConfig response contained another symbol"
                )
            return require_margin_type(rest_require_string(item, "marginType"))
        return None

    async def fetch_symbol_margin_types(self) -> dict[str, str]:
        """Read all exchange symbol-level futures margin modes at once."""
        payload = await self._signed_get("/fapi/v1/symbolConfig")
        return {
            normalize_symbols((rest_require_string(item, "symbol"),))[0]: (
                require_margin_type(rest_require_string(item, "marginType"))
            )
            for item in rest_require_sequence_of_mappings(payload)
        }

    async def fetch_open_orders(
        self,
        symbol: str | None = None,
    ) -> tuple[AccountOpenOrderSnapshot, ...]:
        params: dict[str, str | int | float | bool | None] | None = None
        expected_symbol = None
        if symbol is not None:
            if not symbol.strip():
                raise ValueError("symbol must not be empty")
            expected_symbol = symbol.strip().upper()
            params = {"symbol": expected_symbol}
        payload = await self._signed_get("/fapi/v1/openOrders", params)
        observed_at = self._now()
        return tuple(
            account_open_order_from_item(
                item,
                environment=self._environment,
                account_label=self._account_label,
                observed_at=observed_at,
                expected_symbol=expected_symbol,
            )
            for item in rest_require_sequence_of_mappings(payload)
        )

    @property
    def incomplete_fill_symbols(self) -> frozenset[str]:
        return self._incomplete_fill_symbols

    async def fetch_recent_fills(
        self,
        symbols: tuple[str, ...] = (),
        *,
        from_id_by_symbol: Mapping[str, int] | None = None,
        start_time_by_symbol: Mapping[str, int] | None = None,
        max_pages_per_symbol: int = 10,
    ) -> tuple[AccountFillEvent, ...]:
        normalized_symbols = normalize_symbols(symbols)
        normalized_from_ids = normalize_fill_cursors(from_id_by_symbol)
        normalized_start_times = normalize_fill_cursors(start_time_by_symbol)
        fills: dict[tuple[str, str], AccountFillEvent] = {}
        incomplete_symbols: set[str] = set()
        for symbol in normalized_symbols:
            if symbol in normalized_from_ids and symbol in normalized_start_times:
                raise ValueError(
                    f"from_id and start_time cannot both be set for {symbol}"
                )
            current_from_id = normalized_from_ids.get(symbol)
            current_start_time = normalized_start_times.get(symbol)
            reached_short_page = False
            for _ in range(max_pages_per_symbol):
                params: dict[str, str | int | float | bool | None] = {
                    "symbol": symbol,
                    "limit": 1000,
                }
                if current_from_id is not None:
                    params["fromId"] = current_from_id
                elif current_start_time is not None:
                    params["startTime"] = current_start_time

                payload = await self._signed_get(
                    "/fapi/v1/userTrades",
                    params,
                )
                items = rest_require_sequence_of_mappings(payload)
                if not items:
                    reached_short_page = True
                    break

                max_seen_trade_id: int | None = None
                for item in items:
                    fill = account_fill_from_trade_item(
                        item,
                        environment=self._environment,
                        account_label=self._account_label,
                        expected_symbol=symbol,
                    )
                    trade_id = int(fill.trade_id)
                    if max_seen_trade_id is None or trade_id > max_seen_trade_id:
                        max_seen_trade_id = trade_id
                    fills[(fill.symbol, fill.trade_id)] = fill

                # If the returned batch is less than limit, symbol coverage is complete
                if len(items) < 1000:
                    reached_short_page = True
                    break

                # Advance cursor for next page
                if max_seen_trade_id is not None:
                    current_from_id = max_seen_trade_id + 1
                    current_start_time = None
                else:
                    reached_short_page = True
                    break

            if not reached_short_page:
                incomplete_symbols.add(symbol)

        self._incomplete_fill_symbols = frozenset(incomplete_symbols)
        return tuple(
            sorted(
                fills.values(),
                key=lambda fill: (fill.trade_at, fill.symbol, fill.trade_id),
            )
        )

    async def fetch_fills_with_provenance(
        self,
        symbol: str,
        *,
        start_time_ms: int,
        checked_through: datetime,
        max_pages_per_window: int = 10,
    ) -> tuple[tuple[AccountFillEvent, ...], AccountFillPageScan]:
        """Fetch a bounded time range and report whether every page was read.

        Binance limits time-based trade queries to seven-day windows and only
        exposes the most recent three months. A complete response for one
        bounded window is not enough to claim earlier account history; callers
        must supply a previously verified position anchor at ``start_time_ms``.
        """
        normalized_symbol = normalize_symbols((symbol,))[0]
        if type(start_time_ms) is not int or start_time_ms < 0:
            raise ValueError("start_time_ms must be a non-negative integer")
        if checked_through.tzinfo is None or checked_through.utcoffset() is None:
            raise ValueError("checked_through must be timezone-aware")
        if max_pages_per_window <= 0:
            raise ValueError("max_pages_per_window must be positive")
        end_time_ms = math.ceil(checked_through.timestamp() * 1000)
        if start_time_ms > end_time_ms:
            raise ValueError("fill scan start must not follow its checked-through cut")

        if start_time_ms < end_time_ms - _FILL_SCAN_RETENTION_MS:
            return (), AccountFillPageScan(
                symbol=normalized_symbol,
                load_id=fill_scan_load_id(
                    normalized_symbol, start_time_ms, end_time_ms, ()
                ),
                scan_origin_start_time_ms=start_time_ms,
                next_from_id=None,
                page_count=0,
                page_exhausted=False,
                truncated=True,
                checked_through=None,
            )

        fills: dict[str, AccountFillEvent] = {}
        page_count = 0
        cursor_after: int | None = None
        last_complete_cut_ms: int | None = None
        truncated = False
        window_start_ms = start_time_ms

        while window_start_ms <= end_time_ms:
            window_end_ms = min(
                window_start_ms + _FILL_SCAN_WINDOW_MS - 1,
                end_time_ms,
            )
            window_page_count = 0
            cursor: int | None = None
            window_complete = False

            while window_page_count < max_pages_per_window:
                params: dict[str, str | int | float | bool | None] = {
                    "symbol": normalized_symbol,
                    "limit": 1000,
                }
                if cursor is None:
                    params.update(
                        {"startTime": window_start_ms, "endTime": window_end_ms}
                    )
                else:
                    # Binance disallows combining fromId with a time window.
                    # Trade ids are monotone; stop once the bounded window is
                    # crossed and keep the following time window separate.
                    params["fromId"] = cursor
                payload = await self._signed_get("/fapi/v1/userTrades", params)
                items = rest_require_sequence_of_mappings(payload)
                page_count += 1
                window_page_count += 1
                if not items:
                    window_complete = True
                    break

                parsed: list[AccountFillEvent] = []
                max_trade_id: int | None = None
                crossed_window_end = False
                for item in items:
                    fill = account_fill_from_trade_item(
                        item,
                        environment=self._environment,
                        account_label=self._account_label,
                        expected_symbol=normalized_symbol,
                    )
                    fill_time_ms = int(fill.trade_at.timestamp() * 1000)
                    if fill_time_ms > window_end_ms:
                        crossed_window_end = True
                        break
                    if fill_time_ms >= window_start_ms:
                        parsed.append(fill)
                    if fill.trade_id.isdigit():
                        trade_id = int(fill.trade_id)
                        max_trade_id = (
                            trade_id
                            if max_trade_id is None
                            else max(max_trade_id, trade_id)
                        )
                for fill in parsed:
                    fills[fill.trade_id] = fill

                if crossed_window_end or len(items) < 1000:
                    window_complete = True
                    break
                if max_trade_id is None:
                    truncated = True
                    break
                cursor = max_trade_id + 1

            if truncated:
                cursor_after = cursor
                break
            if not window_complete:
                truncated = True
                cursor_after = cursor
                break
            last_complete_cut_ms = window_end_ms
            window_start_ms = window_end_ms + 1

        page_exhausted = not truncated and last_complete_cut_ms == end_time_ms
        checked_cut = (
            checked_through
            if page_exhausted
            else (
                None
                if last_complete_cut_ms is None
                else datetime.fromtimestamp(last_complete_cut_ms / 1000, tz=UTC)
            )
        )
        ordered_fills = tuple(
            sorted(
                fills.values(),
                key=lambda fill: (fill.trade_at, fill.symbol, fill.trade_id),
            )
        )
        scan = AccountFillPageScan(
            symbol=normalized_symbol,
            load_id=fill_scan_load_id(
                normalized_symbol,
                start_time_ms,
                end_time_ms,
                ordered_fills,
            ),
            scan_origin_start_time_ms=start_time_ms,
            next_from_id=cursor_after,
            page_count=page_count,
            page_exhausted=page_exhausted,
            truncated=truncated,
            checked_through=checked_cut,
        )
        return ordered_fills, scan

    async def start_user_data_stream(self) -> str:
        """Create a Binance USD-M Futures listen key for account events."""
        payload = await self._user_data_request("POST", "/fapi/v1/listenKey")
        data = rest_require_mapping(payload)
        listen_key = rest_require_string(data, "listenKey").strip()
        if not listen_key:
            raise ValueError("Binance listen-key response did not contain listenKey")
        return listen_key

    async def keepalive_user_data_stream(self, listen_key: str) -> None:
        if not listen_key.strip():
            raise ValueError("listen_key must not be empty")
        await self._user_data_request(
            "PUT",
            "/fapi/v1/listenKey",
            data={"listenKey": listen_key},
        )

    async def close_user_data_stream(self, listen_key: str) -> None:
        if not listen_key.strip():
            raise ValueError("listen_key must not be empty")
        await self._user_data_request(
            "DELETE",
            "/fapi/v1/listenKey",
            data={"listenKey": listen_key},
        )

    async def aclose(self) -> None:
        await self._read_request_pacer.aclose()
        await self._command_request_pacer.aclose()
        await self._client.aclose()

    async def _signed_get(
        self,
        path: str,
        params: dict[str, str | int | float | bool | None] | None = None,
    ) -> object:
        await self._read_request_pacer.wait()
        signed_params = self._signed_params(params or {})
        started = time.perf_counter()
        response: httpx.Response | None = None
        try:
            response = await self._client.get(
                path,
                params=signed_params,
                headers={"X-MBX-APIKEY": self._api_key},
            )
            _raise_for_status(response)
            return response.json()
        except Exception:
            raise
        finally:
            self._record_endpoint(
                path,
                elapsed_ms=(time.perf_counter() - started) * 1000,
                status=response.status_code if response is not None else None,
                error=response is None or response.status_code >= 400,
            )

    async def _signed_post(
        self,
        path: str,
        params: dict[str, str | int | float | bool | None],
        *,
        priority: int = _COMMAND_BACKGROUND_PRIORITY,
        on_request_started: Callable[[], Awaitable[None]] | None = None,
        on_response_received: Callable[[], Awaitable[None]] | None = None,
    ) -> object:
        await self._command_request_pacer.wait(priority=priority)
        if on_request_started is not None:
            await on_request_started()
        signed_params = self._signed_params(params)
        try:
            response = await self._client.post(
                path,
                data=signed_params,
                headers={"X-MBX-APIKEY": self._api_key},
            )
            _raise_for_status(response)
            return response.json()
        finally:
            if on_response_received is not None:
                await on_response_received()

    async def _signed_delete(
        self,
        path: str,
        params: dict[str, str | int | float | bool | None],
        *,
        priority: int = _COMMAND_BACKGROUND_PRIORITY,
    ) -> object:
        await self._command_request_pacer.wait(priority=priority)
        signed_params = self._signed_params(params)
        response = await self._client.delete(
            path,
            params=signed_params,
            headers={"X-MBX-APIKEY": self._api_key},
        )
        _raise_for_status(response)
        return response.json()

    async def _user_data_request(
        self,
        method: str,
        path: str,
        *,
        data: dict[str, str] | None = None,
    ) -> object:
        """Call an unsigned listen-key endpoint authenticated by API key."""
        await self._command_request_pacer.wait(priority=_COMMAND_BACKGROUND_PRIORITY)
        response = await self._client.request(
            method,
            path,
            data=data,
            headers={"X-MBX-APIKEY": self._api_key},
        )
        _raise_for_status(response)
        if response.content:
            return response.json()
        return {}

    def _signed_params(
        self,
        params: dict[str, str | int | float | bool | None],
    ) -> dict[str, str | int | float | bool | None]:
        payload = {key: value for key, value in params.items() if value is not None}
        payload.update(
            {
                "timestamp": int(self._now().timestamp() * 1000),
                "recvWindow": self._recv_window_ms,
            }
        )
        query = urlencode(payload)
        signature = hmac.new(
            self._api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {**payload, "signature": signature}

    def _now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock must return timezone-aware datetime")
        return now


class BinanceUsdMTradeClient(BinanceUsdMPrivateReadClient):
    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        environment: str,
        account_label: str,
        live_submit_enabled: bool,
        base_url: str = "https://fapi.binance.com",
        http_client: httpx.AsyncClient | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        recv_window_ms: int = 10000,
        request_interval_seconds: float = 0.2,
        command_request_interval_seconds: float | None = None,
        shared_request_pacer_path: str | Path | None = None,
        shared_command_request_pacer_path: str | Path | None = None,
        request_timeout_seconds: float = _DEFAULT_REQUEST_TIMEOUT_SECONDS,
        connect_timeout_seconds: float = _DEFAULT_CONNECT_TIMEOUT_SECONDS,
        pool_timeout_seconds: float = _DEFAULT_POOL_TIMEOUT_SECONDS,
        entry_leverage: int | None = None,
        margin_type: str | None = None,
        on_exchange_request: ExchangeBoundaryCallback | None = None,
        on_exchange_response: ExchangeBoundaryCallback | None = None,
    ) -> None:
        if entry_leverage is not None and not 1 <= entry_leverage <= 125:
            raise ValueError("entry_leverage must be between 1 and 125")
        normalized_margin_type = (
            None if margin_type is None else require_margin_type(margin_type)
        )
        super().__init__(
            api_key=api_key,
            api_secret=api_secret,
            environment=environment,
            account_label=account_label,
            base_url=base_url,
            http_client=http_client,
            clock=clock,
            recv_window_ms=recv_window_ms,
            request_interval_seconds=request_interval_seconds,
            command_request_interval_seconds=command_request_interval_seconds,
            shared_request_pacer_path=shared_request_pacer_path,
            shared_command_request_pacer_path=shared_command_request_pacer_path,
            request_timeout_seconds=request_timeout_seconds,
            connect_timeout_seconds=connect_timeout_seconds,
            pool_timeout_seconds=pool_timeout_seconds,
        )
        self._live_submit_enabled = live_submit_enabled
        self._entry_leverage = entry_leverage
        self._entry_margin_type = normalized_margin_type
        self._configured_leverage_by_symbol: dict[str, int] = {}
        self._configured_margin_type_by_symbol: dict[str, str] = {}
        self._margin_type_lock = asyncio.Lock()
        self._on_exchange_request = on_exchange_request
        self._on_exchange_response = on_exchange_response

    def set_exchange_boundary_callbacks(
        self,
        *,
        on_request: ExchangeBoundaryCallback | None = None,
        on_response: ExchangeBoundaryCallback | None = None,
    ) -> None:
        self._on_exchange_request = on_request
        self._on_exchange_response = on_response

    @property
    def configured_margin_type_count(self) -> int:
        """Return the number of symbols confirmed for the entry mode."""
        return len(self._configured_margin_type_by_symbol)

    def is_entry_leverage_configured(self, symbol: str) -> bool:
        """Return whether entry leverage has been confirmed for symbol."""
        if self._entry_leverage is None:
            return True
        return symbol in self._configured_leverage_by_symbol

    def is_entry_margin_type_configured(self, symbol: str) -> bool:
        """Return whether entry margin mode has been confirmed for symbol."""
        if self._entry_margin_type is None:
            return True
        return symbol in self._configured_margin_type_by_symbol

    async def warm_entry_leverage(self, symbols: Iterable[str]) -> None:
        """Confirm entry leverage before the live market loop can submit."""

        if not self._live_submit_enabled:
            raise LiveSubmissionDisabledError(
                "Binance trade client requires explicit live submit enablement"
            )
        if self._entry_leverage is None:
            return
        normalized_symbols = normalize_symbols(symbols)
        for offset in range(
            0,
            len(normalized_symbols),
            _ENTRY_LEVERAGE_WARMUP_CONCURRENCY,
        ):
            batch = normalized_symbols[
                offset : offset + _ENTRY_LEVERAGE_WARMUP_CONCURRENCY
            ]
            results = await asyncio.gather(
                *(self._ensure_entry_leverage(symbol) for symbol in batch),
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, BaseException):
                    raise result

    async def warm_entry_margin_type(
        self,
        symbols: Iterable[str] = (),
    ) -> None:
        """Preload all confirmed entry modes before the live market loop."""

        if not self._live_submit_enabled:
            raise LiveSubmissionDisabledError(
                "Binance trade client requires explicit live submit enablement"
            )
        if self._entry_margin_type is None:
            return
        normalized_symbols = normalize_symbols(symbols)
        desired_margin_type = self._entry_margin_type
        try:
            all_margin_types = await self.fetch_symbol_margin_types()
        except httpx.TimeoutException as exc:
            raise ExchangeOrderRejectedError(
                "Binance entry margin type warmup could not read symbol configs"
            ) from exc
        except httpx.HTTPStatusError as exc:
            raise ExchangeOrderRejectedError(exchange_error_message(exc)) from exc
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            raise ExchangeOrderRejectedError(
                "Binance entry margin type warmup could not read symbol configs"
            ) from exc

        async with self._margin_type_lock:
            self._configured_margin_type_by_symbol.update(
                {
                    symbol: margin_type
                    for symbol, margin_type in all_margin_types.items()
                    if margin_type == desired_margin_type
                }
            )

        errors: list[str] = []
        for symbol in normalized_symbols:
            if self._configured_margin_type_by_symbol.get(symbol) == (
                desired_margin_type
            ):
                continue
            try:
                await self._ensure_entry_margin_type(symbol)
            except ExchangeOrderRejectedError as error:
                errors.append(f"{symbol}: {error}")
        if errors:
            raise ExchangeOrderRejectedError(
                "Binance entry margin type warmup failed: " + "; ".join(errors)
            )

    async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
        if not self._live_submit_enabled:
            raise LiveSubmissionDisabledError(
                "Binance trade client requires explicit live submit enablement"
            )
        entry_leverage = None
        if not plan.reduce_only:
            await self._ensure_entry_margin_type(plan.symbol)
            entry_leverage = await self._ensure_entry_leverage(plan.symbol)
        params: dict[str, str | int | float | bool | None] = {
            "symbol": plan.symbol,
            "side": plan.side,
            "type": plan.order_type,
            "quantity": format(plan.quantity, "f"),
            "newClientOrderId": plan.client_order_id,
            "newOrderRespType": "RESULT",
        }
        if plan.position_side.value == "BOTH":
            params["reduceOnly"] = str(plan.reduce_only).lower()
        else:
            params["positionSide"] = plan.position_side.value
        if plan.price is not None:
            params["price"] = format(plan.price, "f")
            time_in_force = plan.time_in_force
            if not time_in_force:
                raise OrderPreSubmissionError(
                    f"Limit order {plan.client_order_id} is missing required "
                    "time_in_force"
                )
            params["timeInForce"] = time_in_force
            if time_in_force == "GTD":
                if plan.expires_at is None:
                    raise OrderPreSubmissionError("GTD order is missing expires_at")
                if plan.expires_at <= self._now() + timedelta(seconds=600):
                    raise OrderPreSubmissionError(
                        "GTD order must expire more than 600 seconds from now"
                    )
                params["goodTillDate"] = int(plan.expires_at.timestamp() * 1000)

        async def _notify_request_started() -> None:
            if self._on_exchange_request is not None:
                try:
                    await self._on_exchange_request(
                        plan, "submit_request_started", self._now()
                    )
                except Exception as exc:
                    log.warning(
                        "exchange_boundary_telemetry_failed",
                        client_order_id=plan.client_order_id,
                        phase="submit_request_started",
                        error_type=type(exc).__name__,
                    )

        async def _notify_response_received() -> None:
            if self._on_exchange_response is not None:
                try:
                    await self._on_exchange_response(
                        plan, "submit_response_received", self._now()
                    )
                except Exception as exc:
                    log.warning(
                        "exchange_boundary_telemetry_failed",
                        client_order_id=plan.client_order_id,
                        phase="submit_response_received",
                        error_type=type(exc).__name__,
                    )

        try:
            payload = await self._signed_post(
                "/fapi/v1/order",
                params,
                priority=(
                    _COMMAND_EXIT_PRIORITY
                    if plan.reduce_only
                    else _COMMAND_ENTRY_PRIORITY
                ),
                on_request_started=_notify_request_started,
                on_response_received=_notify_response_received,
            )
        except ValueError as exc:
            # A decode failure escaping the order POST cannot prove rejection.
            raise ExchangeSubmissionTimeoutError(
                "Binance order submit returned an unreadable response; outcome unknown"
            ) from exc
        except httpx.TimeoutException as exc:
            raise ExchangeSubmissionTimeoutError(
                "Binance order submit timed out"
            ) from exc
        except httpx.RequestError as exc:
            raise ExchangeSubmissionTimeoutError(
                "Binance order submit failed with an unknown outcome"
            ) from exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code >= 500:
                raise ExchangeSubmissionTimeoutError(
                    "Binance order submit returned an unknown server outcome"
                ) from exc
            raise ExchangeOrderRejectedError(exchange_error_message(exc)) from exc
        try:
            snapshot = order_snapshot_from_response(
                rest_require_mapping(payload),
                observed_at=self._now(),
                entry_leverage=entry_leverage,
            )
            if snapshot.executed_quantity > Decimal("0") and (
                snapshot.average_price is None or snapshot.average_price <= Decimal("0")
            ):
                snapshot = await self._resolve_filled_order_snapshot_with_retry(
                    plan=plan,
                    initial_snapshot=snapshot,
                    entry_leverage=entry_leverage,
                )
            return snapshot
        except (ValueError, TypeError, KeyError) as parse_exc:
            log.warning(
                "binance_order_submit_response_parse_error",
                client_order_id=plan.client_order_id,
                error=str(parse_exc),
            )
            raise ExchangeSubmissionTimeoutError(
                f"Binance order submitted but response parsing failed: {parse_exc}"
            ) from parse_exc

    async def _resolve_filled_order_snapshot_with_retry(
        self,
        plan: OrderExecutionPlan,
        initial_snapshot: ExchangeOrderSnapshot,
        *,
        entry_leverage: int | None = None,
        delays: tuple[float, ...] = (0.05, 0.1, 0.2),
    ) -> ExchangeOrderSnapshot:
        """Resolve authoritative average_price when Binance POST returns 0
        on immediate fills.
        """
        log.info(
            "binance_submit_order_resolving_fill_price",
            symbol=plan.symbol,
            client_order_id=plan.client_order_id,
            executed_quantity=str(initial_snapshot.executed_quantity),
        )
        for attempt, delay in enumerate(delays, start=1):
            await asyncio.sleep(delay)
            try:
                queried = await self.query_order_by_client_id(
                    symbol=plan.symbol,
                    client_order_id=plan.client_order_id,
                )
                if (
                    queried is not None
                    and queried.average_price is not None
                    and queried.average_price > Decimal("0")
                ):
                    log.info(
                        "binance_submit_order_fill_price_resolved",
                        symbol=plan.symbol,
                        client_order_id=plan.client_order_id,
                        attempt=attempt,
                        average_price=str(queried.average_price),
                    )
                    return replace(
                        queried,
                        entry_leverage=entry_leverage
                        if queried.entry_leverage is None
                        else queried.entry_leverage,
                    )
            except ExchangeOrderQueryUnknownError as exc:
                log.warning(
                    "binance_submit_avg_price_query_retry_failed",
                    symbol=plan.symbol,
                    client_order_id=plan.client_order_id,
                    attempt=attempt,
                    error=str(exc),
                )
        return initial_snapshot

    async def inspect_exit_order(
        self,
        plan: OrderExecutionPlan,
    ) -> ExitRecoveryObservation:
        """Read all exchange facts required before retrying an unknown exit."""
        if not plan.reduce_only:
            raise ValueError("exit recovery inspection requires a reduce-only plan")
        try:
            order, positions, open_orders = await asyncio.gather(
                self.query_order_by_client_id(
                    plan.symbol,
                    plan.client_order_id,
                ),
                self.fetch_positions(),
                self.fetch_open_orders(symbol=plan.symbol),
            )
        except Exception as exc:
            raise ExitRecoveryInspectionUnknownError(
                "Binance exit recovery inspection could not be completed"
            ) from exc

        try:
            position_quantity = sum(
                (exit_position_quantity(position, plan) for position in positions),
                start=Decimal("0"),
            )
            active_client_order_ids = {
                open_order.client_order_id
                for open_order in open_orders
                if open_order_matches_exit(open_order, plan)
            }
        except ExitRecoveryInspectionUnknownError:
            raise
        except Exception as exc:
            raise ExitRecoveryInspectionUnknownError(
                "Binance exit recovery observation could not be interpreted"
            ) from exc
        if order is not None and not order.state.terminal:
            active_client_order_ids.add(order.client_order_id)
        return ExitRecoveryObservation(
            order=order,
            position_quantity=position_quantity,
            active_exit_order_client_ids=tuple(sorted(active_client_order_ids)),
            observed_at=self._now(),
        )

    async def _ensure_entry_margin_type(self, symbol: str) -> str | None:
        desired_margin_type = self._entry_margin_type
        if desired_margin_type is None:
            return None
        normalized_symbol = normalize_symbols((symbol,))[0]
        async with self._margin_type_lock:
            configured = self._configured_margin_type_by_symbol.get(normalized_symbol)
            if configured is not None:
                return configured
            try:
                current = await self.fetch_symbol_margin_type(normalized_symbol)
                if current == desired_margin_type:
                    self._configured_margin_type_by_symbol[normalized_symbol] = current
                    return current
                try:
                    await self._signed_post(
                        "/fapi/v1/marginType",
                        {
                            "symbol": normalized_symbol,
                            "marginType": desired_margin_type,
                        },
                        priority=_COMMAND_ENTRY_PRIORITY,
                    )
                except httpx.HTTPStatusError as exc:
                    # Binance reports an already-selected mode as an error.
                    # Treat it as a successful confirmation; another read-back
                    # here only adds latency and can observe a stale projection.
                    if exchange_error_code(exc) != -4046:
                        raise
                # The successful write (or -4046 "already selected") is the
                # exchange acknowledgement. Do not perform a second
                # symbolConfig read on the order-critical path: that
                # projection can lag and would leave the symbol uncached,
                # causing the next order to repeat the margin-type write.
                self._configured_margin_type_by_symbol[normalized_symbol] = (
                    desired_margin_type
                )
                return desired_margin_type
            except httpx.TimeoutException as exc:
                raise ExchangeOrderRejectedError(
                    "Binance entry margin type was not confirmed; order was not sent"
                ) from exc
            except httpx.HTTPStatusError as exc:
                raise ExchangeOrderRejectedError(exchange_error_message(exc)) from exc
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                raise ExchangeOrderRejectedError(
                    "Binance entry margin type was not confirmed; order was not sent"
                ) from exc

    async def _ensure_entry_leverage(self, symbol: str) -> int | None:
        if self._entry_leverage is None:
            return None
        configured = self._configured_leverage_by_symbol.get(symbol)
        if configured is not None:
            return configured

        candidates = entry_leverage_candidates(self._entry_leverage)
        last_rejection: str | None = None
        for leverage in candidates:
            if leverage != self._entry_leverage:
                log.warning(
                    "binance_entry_leverage_fallback_attempt",
                    symbol=symbol,
                    requested_leverage=self._entry_leverage,
                    attempted_leverage=leverage,
                    account_label=self._account_label,
                )
            try:
                payload = await self._signed_post(
                    "/fapi/v1/leverage",
                    {"symbol": symbol, "leverage": leverage},
                    priority=_COMMAND_ENTRY_PRIORITY,
                )
                response = rest_require_mapping(payload)
                if (
                    rest_require_string(response, "symbol") != symbol
                    or rest_require_int(response, "leverage") != leverage
                ):
                    raise ValueError("unexpected leverage response")
            except httpx.HTTPStatusError as exc:
                if not is_invalid_leverage_rejection(exc):
                    raise ExchangeOrderRejectedError(
                        exchange_error_message(exc)
                    ) from exc
                last_rejection = exchange_error_message(exc)
                continue
            except (httpx.HTTPError, ValueError) as exc:
                raise ExchangeOrderRejectedError(
                    "Binance entry leverage was not confirmed; order was not sent"
                ) from exc
            if leverage != self._entry_leverage:
                log.warning(
                    "binance_entry_leverage_fallback_accepted",
                    symbol=symbol,
                    requested_leverage=self._entry_leverage,
                    accepted_leverage=leverage,
                    account_label=self._account_label,
                )
            self._configured_leverage_by_symbol[symbol] = leverage
            return leverage

        attempted = ", ".join(f"{value}x" for value in candidates)
        reason = f": {last_rejection}" if last_rejection is not None else ""
        raise ExchangeOrderRejectedError(
            "Binance entry leverage was not confirmed; "
            f"tried {attempted}; order was not sent{reason}"
        )

    async def query_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot | None:
        try:
            payload = await self._signed_get(
                "/fapi/v1/order",
                {
                    "symbol": symbol,
                    "origClientOrderId": client_order_id,
                },
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 400 and exchange_error_code(exc) == -2013:
                return None
            if exc.response.status_code in {418, 429} or (
                exc.response.status_code >= 500
            ):
                raise ExchangeOrderQueryUnknownError(
                    "Binance order lookup returned a transient HTTP error; "
                    "order state requires reconciliation",
                    retry_after_seconds=retry_after_seconds(exc.response),
                ) from exc
            raise
        except httpx.TimeoutException as exc:
            raise ExchangeOrderQueryUnknownError(
                "Binance order lookup timed out; order state requires reconciliation"
            ) from exc
        except httpx.RequestError as exc:
            raise ExchangeOrderQueryUnknownError(
                "Binance order lookup failed; order state requires reconciliation"
            ) from exc
        return order_snapshot_from_response(
            rest_require_mapping(payload), observed_at=self._now()
        )

    async def cancel_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot:
        """Cancel one known order for normal executor lifecycle management."""
        if not self._live_submit_enabled:
            raise LiveSubmissionDisabledError(
                "Binance trade client requires explicit live submit enablement"
            )
        try:
            payload = await self._signed_delete(
                "/fapi/v1/order",
                {
                    "symbol": symbol,
                    "origClientOrderId": client_order_id,
                },
                priority=_COMMAND_EXIT_PRIORITY,
            )
        except httpx.TimeoutException as exc:
            raise ExchangeCancellationUnknownError(
                "Binance cancel request timed out; order state must be reconciled"
            ) from exc
        except httpx.RequestError as exc:
            raise ExchangeCancellationUnknownError(
                "Binance cancel request failed; order state must be reconciled"
            ) from exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in {418, 429}:
                raise ExchangeCancellationUnknownError(
                    "Binance cancel request was rate limited; "
                    "order state must be reconciled",
                    retry_after_seconds=retry_after_seconds(exc.response),
                ) from exc
            if exc.response.status_code >= 500:
                raise ExchangeCancellationUnknownError(
                    "Binance cancel request returned an unknown server outcome"
                ) from exc
            if exchange_error_code(exc) in {-2011, -2013}:
                exchange_message = exchange_error_message(exc)
                try:
                    existing = await self.query_order_by_client_id(
                        symbol,
                        client_order_id,
                    )
                except ExchangeOrderQueryUnknownError as query_error:
                    raise ExchangeCancellationUnknownError(
                        "Binance cancel result could not be reconciled"
                    ) from query_error
                if existing is not None:
                    return existing
                try:
                    open_orders = await self.fetch_open_orders(symbol=symbol)
                except httpx.TimeoutException as query_error:
                    raise ExchangeCancellationUnknownError(
                        "Binance open-order check timed out; "
                        "order state must be reconciled"
                    ) from query_error
                except httpx.RequestError as query_error:
                    raise ExchangeCancellationUnknownError(
                        "Binance open-order check failed; "
                        "order state must be reconciled"
                    ) from query_error
                except httpx.HTTPStatusError as query_error:
                    raise ExchangeCancellationUnknownError(
                        "Binance open-order check returned an error; "
                        "order state must be reconciled",
                        retry_after_seconds=retry_after_seconds(query_error.response),
                    ) from query_error
                if any(
                    order.symbol == symbol and order.client_order_id == client_order_id
                    for order in open_orders
                ):
                    raise ExchangeCancellationUnknownError(
                        "Binance cancel result is inconsistent; "
                        "matching open order still exists"
                    ) from exc
                raise ExchangeOrderAlreadyAbsentError(
                    exchange_message,
                    exchange_code=exchange_error_code(exc),
                    exchange_message=exchange_message,
                    http_status=exc.response.status_code,
                    open_orders_checked=True,
                ) from exc
            raise ExchangeCancellationUnknownError(
                "Binance cancel request was rejected; order state must be reconciled"
            ) from exc
        return order_snapshot_from_response(
            rest_require_mapping(payload), observed_at=self._now()
        )

    async def emergency_flatten(
        self,
        *,
        plan: OrderExecutionPlan,
        command: RollbackCommand | None,
    ) -> ExchangeOrderSnapshot:
        require_authorized_command(
            command,
            command_type="emergency_flatten",
            confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
        )
        if not plan.reduce_only:
            raise ValueError("emergency flatten plan must be reduce-only")
        return await self.submit_order(plan)


def _raise_for_status(response: httpx.Response) -> None:
    if response.status_code in {418, 429}:
        raise BinanceRateLimitError(response)
    response.raise_for_status()
