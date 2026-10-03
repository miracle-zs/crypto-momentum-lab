import asyncio
from dataclasses import replace
from datetime import timedelta

from crypto_momentum_lab.domain.market.models import (
    MarketState15s,
    RealtimeMarketQuote,
)
from crypto_momentum_lab.live_rollout.exit_lane import (
    ExitExecutionLane,
    ExitLaneOutcome,
)
from tests.fixtures.live_market import _state


async def test_market_lane_keeps_latest_state_and_merges_outcomes() -> None:
    processed_states: list[MarketState15s] = []
    processed_quotes: list[RealtimeMarketQuote] = []

    async def process_market(
        state: MarketState15s,
    ) -> ExitLaneOutcome:
        processed_states.append(state)
        return ExitLaneOutcome(approved_intent_count=1)

    async def process_quote(
        quote: RealtimeMarketQuote,
        state: MarketState15s,
    ) -> ExitLaneOutcome:
        assert state.symbol == quote.symbol
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
    await lane.submit_market(first_state)
    await lane.submit_market(latest_state)

    quote = RealtimeMarketQuote(
        exchange=latest_state.exchange,
        environment=latest_state.environment,
        symbol=latest_state.symbol,
        event_at=latest_state.bucket_end,
        received_at=latest_state.bucket_end,
        bid_price=latest_state.last_bid_price or latest_state.close_price,
        ask_price=latest_state.last_ask_price or latest_state.close_price,
    )
    await lane.submit_quote(quote, latest_state)
    outcome = await lane.stop()

    assert processed_states == [latest_state]
    assert processed_quotes == [quote]
    assert outcome == ExitLaneOutcome(
        approved_intent_count=1,
        submitted_order_count=1,
    )


async def test_outcome_callback_failure_does_not_leave_shutdown_waiting_forever():
    async def process(state):
        return ExitLaneOutcome()

    async def process_quote(quote, state):
        return ExitLaneOutcome()

    def failed_callback(symbol, outcome):
        raise RuntimeError("outcome publication failed")

    lane = ExitExecutionLane(process, process_quote, on_outcome=failed_callback)
    await lane.start()
    await lane.submit_market(_state())
    outcome = await asyncio.wait_for(lane.stop(), timeout=1)
    assert outcome.fatal_failure
    assert outcome.failure == "exit_outcome_publication_failed:RuntimeError"


async def test_older_account_trigger_keeps_newer_price_and_state():
    states = []
    quotes = []
    old_state = _state()
    newer_state = replace(
        old_state,
        bucket_start=old_state.bucket_start + timedelta(seconds=15),
        bucket_end=old_state.bucket_end + timedelta(seconds=15),
    )
    old_quote = RealtimeMarketQuote(
        exchange=old_state.exchange,
        environment=old_state.environment,
        symbol=old_state.symbol,
        event_at=old_state.bucket_end,
        received_at=old_state.bucket_end,
        bid_price=old_state.close_price,
        ask_price=old_state.close_price,
    )
    newer_quote = replace(
        old_quote, event_at=newer_state.bucket_end, received_at=newer_state.bucket_end
    )

    async def process(state):
        states.append(state)
        return ExitLaneOutcome()

    async def process_quote(quote, state):
        quotes.append((quote, state))
        return ExitLaneOutcome()

    lane = ExitExecutionLane(process, process_quote)
    await lane.start()
    await lane.submit_market(newer_state)
    await lane.submit_market(old_state)
    await lane.submit_quote(newer_quote, newer_state)
    await lane.submit_quote(old_quote, old_state)
    await lane.stop()
    assert states == [newer_state]
    assert quotes == [(newer_quote, newer_state)]


def test_real_fault_takes_priority_over_pending_evaluation_in_both_orders():
    pending = ExitLaneOutcome(failure="pending_live_context:BTCUSDT")
    fatal = ExitLaneOutcome(failure="exit_execution_failed:RuntimeError", fatal_failure=True)
    assert pending.merge(fatal) == fatal
    assert fatal.merge(pending) == fatal
