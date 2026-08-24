import asyncio
import time
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from crypto_momentum_lab.domain.market.models import (
    AggTradeGap,
    CaptureRoute,
    CaptureStream,
    QualityCategory,
    QualityEvent,
    RawEnvelope,
)
from crypto_momentum_lab.market_data.binance.rest import BinanceAggTrade


class AggTradeHistory(Protocol):
    async def fetch_agg_trades(
        self,
        symbol: str,
        *,
        from_id: int,
        limit: int,
    ) -> tuple[BinanceAggTrade, ...]: ...


@dataclass(frozen=True, slots=True)
class AggTradeRecoveryBatch:
    envelopes: tuple[RawEnvelope, ...]
    unrecovered_gaps: tuple[AggTradeGap, ...]


@dataclass(frozen=True, slots=True)
class AggTradeRecoveryMetrics:
    detected_gap_count: int
    recovered_gap_count: int
    unrecovered_gap_count: int
    recovered_trade_count: int
    missing_trade_count: int
    duplicate_trade_count: int


@dataclass(frozen=True, slots=True)
class _SeenTrade:
    aggregate_trade_id: int
    event_at: datetime
    connection_session_id: UUID


@dataclass(frozen=True, slots=True)
class _GapRequest:
    index: int
    previous: _SeenTrade
    current_id: int
    current: RawEnvelope

    @property
    def missing_count(self) -> int:
        return self.current_id - self.previous.aggregate_trade_id - 1


@dataclass(frozen=True, slots=True)
class _RecoveryResult:
    request: _GapRequest
    trades: tuple[BinanceAggTrade, ...]
    failure_reason: str | None


class AggTradeGapRecoverer:
    """Expand live batches with exact REST history across aggTrade ID gaps."""

    def __init__(
        self,
        history: AggTradeHistory,
        *,
        max_gap_trades: int = 10000,
        max_concurrency: int = 8,
        recovery_timeout_seconds: float = 1.0,
        max_requests_per_minute: int = 100,
    ) -> None:
        if max_gap_trades <= 0:
            raise ValueError("max_gap_trades must be positive")
        if max_concurrency <= 0:
            raise ValueError("max_concurrency must be positive")
        if recovery_timeout_seconds <= 0:
            raise ValueError("recovery_timeout_seconds must be positive")
        if max_requests_per_minute <= 0:
            raise ValueError("max_requests_per_minute must be positive")
        self._history = history
        self._max_gap_trades = max_gap_trades
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._recovery_timeout_seconds = recovery_timeout_seconds
        self._max_requests_per_minute = max_requests_per_minute
        self._request_timestamps: deque[float] = deque()
        self._request_budget_lock = asyncio.Lock()
        self._last_seen: dict[tuple[str, str], _SeenTrade] = {}
        self._monitored_symbols: frozenset[str] | None = None
        self._detected_gap_count = 0
        self._recovered_gap_count = 0
        self._unrecovered_gap_count = 0
        self._recovered_trade_count = 0
        self._missing_trade_count = 0
        self._duplicate_trade_count = 0

    @property
    def metrics(self) -> AggTradeRecoveryMetrics:
        return AggTradeRecoveryMetrics(
            detected_gap_count=self._detected_gap_count,
            recovered_gap_count=self._recovered_gap_count,
            unrecovered_gap_count=self._unrecovered_gap_count,
            recovered_trade_count=self._recovered_trade_count,
            missing_trade_count=self._missing_trade_count,
            duplicate_trade_count=self._duplicate_trade_count,
        )

    def set_monitored_symbols(self, symbols: frozenset[str]) -> None:
        """Forget continuity for intentionally unsubscribed symbols."""
        normalized = frozenset(symbol.upper() for symbol in symbols)
        self._monitored_symbols = normalized
        self._last_seen = {
            key: seen
            for key, seen in self._last_seen.items()
            if key[1] in normalized
        }

    async def expand(
        self,
        batch: Sequence[RawEnvelope],
    ) -> AggTradeRecoveryBatch:
        accepted_indices: set[int] = set()
        requests: list[_GapRequest] = []
        for index, envelope in enumerate(batch):
            parsed = _agg_trade_identity(envelope)
            if parsed is None:
                accepted_indices.add(index)
                continue
            key, current_id, event_at = parsed
            if (
                self._monitored_symbols is not None
                and key[1] not in self._monitored_symbols
            ):
                accepted_indices.add(index)
                continue
            previous = self._last_seen.get(key)
            if previous is None:
                self._last_seen[key] = _SeenTrade(
                    current_id,
                    event_at,
                    envelope.connection_session_id,
                )
                accepted_indices.add(index)
                continue
            if current_id <= previous.aggregate_trade_id:
                self._duplicate_trade_count += 1
                continue
            if current_id > previous.aggregate_trade_id + 1:
                self._detected_gap_count += 1
                requests.append(
                    _GapRequest(
                        index=index,
                        previous=previous,
                        current_id=current_id,
                        current=envelope,
                    )
                )
            self._last_seen[key] = _SeenTrade(
                current_id,
                event_at,
                envelope.connection_session_id,
            )
            accepted_indices.add(index)

        results = await asyncio.gather(
            *(self._recover(request) for request in requests)
        )
        recovered_before: dict[int, tuple[RawEnvelope, ...]] = {}
        gaps: list[AggTradeGap] = []
        for result in results:
            request = result.request
            if result.failure_reason is None:
                recovered = tuple(
                    _recovered_envelope(trade, current=request.current)
                    for trade in result.trades
                )
                recovered_before[request.index] = recovered
                self._recovered_gap_count += 1
                self._recovered_trade_count += len(recovered)
                continue
            self._unrecovered_gap_count += 1
            self._missing_trade_count += request.missing_count
            assert request.current.symbol is not None
            assert request.current.exchange_event_at is not None
            failure_reason = result.failure_reason
            if (
                request.previous.connection_session_id
                != request.current.connection_session_id
            ):
                failure_reason = f"reconnect_{failure_reason}"
            gaps.append(
                AggTradeGap(
                    environment=request.current.environment,
                    symbol=request.current.symbol,
                    previous_id=request.previous.aggregate_trade_id,
                    current_id=request.current_id,
                    previous_event_at=request.previous.event_at,
                    current_event_at=request.current.exchange_event_at,
                    missing_count=request.missing_count,
                    reason=failure_reason,
                )
            )

        envelopes: list[RawEnvelope] = []
        for index, envelope in enumerate(batch):
            envelopes.extend(recovered_before.get(index, ()))
            if index in accepted_indices:
                envelopes.append(envelope)
        return AggTradeRecoveryBatch(tuple(envelopes), tuple(gaps))

    async def _recover(self, request: _GapRequest) -> _RecoveryResult:
        if request.missing_count > self._max_gap_trades:
            return _RecoveryResult(request, (), "gap_too_large")
        assert request.current.symbol is not None
        next_id = request.previous.aggregate_trade_id + 1
        trades: list[BinanceAggTrade] = []
        try:
            async with asyncio.timeout(self._recovery_timeout_seconds):
                async with self._semaphore:
                    while next_id < request.current_id:
                        limit = min(1000, request.current_id - next_id)
                        if not await self._reserve_request_budget():
                            return _RecoveryResult(
                                request,
                                (),
                                "request_budget_exhausted",
                            )
                        page = await self._history.fetch_agg_trades(
                            request.current.symbol,
                            from_id=next_id,
                            limit=limit,
                        )
                        expected_ids = tuple(
                            range(next_id, next_id + min(limit, len(page)))
                        )
                        actual_ids = tuple(
                            trade.aggregate_trade_id for trade in page
                        )
                        if not page or actual_ids != expected_ids:
                            return _RecoveryResult(
                                request,
                                (),
                                "history_incomplete",
                            )
                        trades.extend(page)
                        next_id = page[-1].aggregate_trade_id + 1
        except TimeoutError:
            return _RecoveryResult(request, (), "history_timeout")
        except Exception as error:
            return _RecoveryResult(
                request,
                (),
                f"history_error:{error.__class__.__name__}",
            )
        if next_id != request.current_id or len(trades) != request.missing_count:
            return _RecoveryResult(request, (), "history_incomplete")
        return _RecoveryResult(request, tuple(trades), None)

    async def _reserve_request_budget(self) -> bool:
        async with self._request_budget_lock:
            now = time.monotonic()
            cutoff = now - 60
            while (
                self._request_timestamps
                and self._request_timestamps[0] <= cutoff
            ):
                self._request_timestamps.popleft()
            if len(self._request_timestamps) >= self._max_requests_per_minute:
                return False
            self._request_timestamps.append(now)
            return True


def _agg_trade_identity(
    envelope: RawEnvelope,
) -> tuple[tuple[str, str], int, datetime] | None:
    if (
        envelope.stream is not CaptureStream.AGG_TRADE
        or envelope.symbol is None
        or envelope.exchange_sequence is None
        or envelope.exchange_event_at is None
    ):
        return None
    try:
        aggregate_trade_id = int(envelope.exchange_sequence)
    except ValueError:
        return None
    return (
        (envelope.environment, envelope.symbol),
        aggregate_trade_id,
        envelope.exchange_event_at,
    )


def _recovered_envelope(
    trade: BinanceAggTrade,
    *,
    current: RawEnvelope,
) -> RawEnvelope:
    assert current.symbol is not None
    received_at = datetime.now(UTC)
    return RawEnvelope(
        schema_version=current.schema_version,
        exchange=current.exchange,
        environment=current.environment,
        route=current.route,
        stream=CaptureStream.AGG_TRADE,
        symbol=current.symbol,
        exchange_event_at=trade.event_at,
        received_at=received_at,
        received_monotonic_ns=time.monotonic_ns(),
        connection_session_id=uuid5(
            NAMESPACE_URL,
            (
                "binance-agg-trade-recovery:"
                f"{current.environment}:{current.symbol}"
            ),
        ),
        local_sequence=trade.aggregate_trade_id + 1,
        exchange_sequence=str(trade.aggregate_trade_id),
        subscription_generation=current.subscription_generation,
        raw_payload=trade.payload(current.symbol),
        recovered=True,
    )


def agg_trade_gap_quality_event(gap: AggTradeGap) -> QualityEvent:
    """Build a stable operator-facing event for an unrecovered source gap."""
    category = (
        QualityCategory.RECONNECT_GAP
        if gap.reason.startswith("reconnect_")
        else QualityCategory.SEQUENCE_GAP
    )
    return QualityEvent(
        event_id=uuid5(
            NAMESPACE_URL,
            (
                "binance-agg-trade-gap:"
                f"{gap.environment}:{gap.symbol}:"
                f"{gap.previous_id}:{gap.current_id}"
            ),
        ),
        category=category,
        occurred_at=gap.current_event_at,
        route=CaptureRoute.MARKET,
        stream=CaptureStream.AGG_TRADE,
        symbol=gap.symbol,
        connection_session_id=None,
        local_sequence=None,
        details={
            "previous": gap.previous_id,
            "current": gap.current_id,
            "missing_count": gap.missing_count,
            "reason": gap.reason,
            "recovered": False,
        },
    )
