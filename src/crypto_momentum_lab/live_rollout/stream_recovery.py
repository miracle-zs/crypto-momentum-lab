"""Retry policy for live Hub-backed async streams."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator

import structlog

from crypto_momentum_lab.domain.market.models import (
    MarketState15s,
    RealtimeMarketQuote,
)
from crypto_momentum_lab.execution_account.hub import (
    AccountEvent,
    AccountEventHubError,
)
from crypto_momentum_lab.execution_account.risk_control_hub import (
    RiskControlEvent,
    RiskControlHubError,
)
from crypto_momentum_lab.market_data.hub import (
    MarketStateHubEpochError,
    MarketStateHubError,
    MarketStateHubReplayUnavailable,
)
from crypto_momentum_lab.market_data.quote_hub import MarketQuoteHubError

log = structlog.get_logger(__name__)

async def _resilient_stream[StreamItem](
    source: AsyncIterable[StreamItem],
    *,
    error_type: type[Exception],
    fatal_error_type: type[Exception] | tuple[type[Exception], ...] | None = None,
    retry_event: str,
    retry_delay_seconds: float,
) -> AsyncIterator[StreamItem]:
    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds must not be negative")
    while True:
        try:
            async for item in source:
                yield item
        except asyncio.CancelledError:
            raise
        except error_type as error:
            if fatal_error_type is not None and isinstance(
                error,
                fatal_error_type,
            ):
                raise
            log.warning(
                retry_event,
                error_type=type(error).__name__,
                error=str(error),
                retry_delay_seconds=retry_delay_seconds,
            )
            if retry_delay_seconds > 0:
                await asyncio.sleep(retry_delay_seconds)
        else:
            return


async def resilient_market_state_stream(
    states: AsyncIterable[MarketState15s],
    *,
    retry_delay_seconds: float = 1.0,
) -> AsyncIterator[MarketState15s]:
    """Keep the live process alive while the market state Hub reconnects."""

    async for state in _resilient_stream(
        states,
        error_type=MarketStateHubError,
        fatal_error_type=(
            MarketStateHubReplayUnavailable,
            MarketStateHubEpochError,
        ),
        retry_event="live_market_state_stream_retry",
        retry_delay_seconds=retry_delay_seconds,
    ):
        yield state


async def resilient_market_quote_stream(
    quotes: AsyncIterable[RealtimeMarketQuote],
    *,
    retry_delay_seconds: float = 1.0,
) -> AsyncIterator[RealtimeMarketQuote]:
    """Keep the quote exit lane alive while the quote Hub reconnects."""

    async for quote in _resilient_stream(
        quotes,
        error_type=MarketQuoteHubError,
        retry_event="live_market_quote_stream_retry",
        retry_delay_seconds=retry_delay_seconds,
    ):
        yield quote


async def resilient_account_event_stream(
    events: AsyncIterable[AccountEvent],
    *,
    retry_delay_seconds: float = 1.0,
) -> AsyncIterator[AccountEvent]:
    """Keep account-event delivery alive while the execution Hub reconnects."""

    async for event in _resilient_stream(
        events,
        error_type=AccountEventHubError,
        retry_event="live_account_event_stream_retry",
        retry_delay_seconds=retry_delay_seconds,
    ):
        yield event


async def resilient_risk_control_stream(
    events: AsyncIterable[RiskControlEvent],
    *,
    retry_delay_seconds: float = 1.0,
) -> AsyncIterator[RiskControlEvent]:
    """Keep durable risk-control notifications reconnecting independently."""

    async for event in _resilient_stream(
        events,
        error_type=RiskControlHubError,
        retry_event="live_risk_control_stream_retry",
        retry_delay_seconds=retry_delay_seconds,
    ):
        yield event


__all__ = [
    "resilient_account_event_stream",
    "resilient_market_quote_stream",
    "resilient_market_state_stream",
    "resilient_risk_control_stream",
]
