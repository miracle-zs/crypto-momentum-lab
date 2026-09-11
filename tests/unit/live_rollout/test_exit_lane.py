from dataclasses import replace
from datetime import timedelta
from typing import cast

from crypto_momentum_lab.domain.market.models import (
    MarketState15s,
    RealtimeMarketQuote,
)
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.exit_lane import (
    ExitExecutionLane,
    ExitLaneOutcome,
)
from tests.unit.shadow_operation.test_service import _state


async def test_market_lane_keeps_latest_state_and_merges_outcomes() -> None:
    processed_states: list[MarketState15s] = []
    processed_quotes: list[RealtimeMarketQuote] = []
    context = cast(LiveDaemonRuntimeContext, object())

    async def process_market(
        state: MarketState15s,
        received_context: LiveDaemonRuntimeContext,
    ) -> ExitLaneOutcome:
        assert received_context is context
        processed_states.append(state)
        return ExitLaneOutcome(approved_intent_count=1)

    async def process_quote(
        quote: RealtimeMarketQuote,
        state: MarketState15s,
        received_context: LiveDaemonRuntimeContext,
    ) -> ExitLaneOutcome:
        assert state.symbol == quote.symbol
        assert received_context is context
        processed_quotes.append(quote)
        return ExitLaneOutcome(submitted_order_count=1)

    lane = ExitExecutionLane(process_market, process_quote)
    await lane.start()

    first_state = _state()
    latest_state = replace(
        first_state,
        bucket_start=first_state.bucket_start + timedelta(seconds=15),
        bucket_end=first_state.bucket_end + timedelta(seconds=15),
    )
    await lane.submit_market(first_state, context)
    await lane.submit_market(latest_state, context)

    quote = RealtimeMarketQuote(
        exchange=latest_state.exchange,
        environment=latest_state.environment,
        symbol=latest_state.symbol,
        event_at=latest_state.bucket_end,
        received_at=latest_state.bucket_end,
        bid_price=latest_state.last_bid_price or latest_state.close_price,
        ask_price=latest_state.last_ask_price or latest_state.close_price,
    )
    quote_outcome = await lane.submit_quote(
        quote,
        latest_state,
        context,
        wait=True,
    )
    outcome = await lane.stop()

    assert processed_states == [latest_state]
    assert processed_quotes == [quote]
    assert quote_outcome == ExitLaneOutcome(submitted_order_count=1)
    assert outcome == ExitLaneOutcome(
        approved_intent_count=1,
        submitted_order_count=1,
    )
