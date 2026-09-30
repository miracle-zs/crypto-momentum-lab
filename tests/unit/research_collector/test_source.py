"""Backfill pagination contracts using a reader without recovery-window APIs."""

import asyncio
from datetime import datetime

import pytest

from crypto_momentum_lab.domain.market.runtime_state_models import RuntimeStateCursor
from crypto_momentum_lab.research_collector.source import (
    RuntimeMarketStateBackfillSource,
)
from tests.unit.persistence.postgres.test_runtime_state_repository import fixture_state


class PageReader:
    def __init__(self, pages=(), *, latest=None):
        self.pages = iter(pages)
        self.latest = latest
        self.calls = []

    async def load_latest_bucket(self, *, environment):
        self.calls.append(environment)
        return self.latest

    async def load_after(self, *, environment, cursor, limit):
        self.calls.append((environment, cursor, limit))
        return next(self.pages)


@pytest.mark.parametrize("environment,page_size", [("", 2), ("research", 0)])
def test_invalid_source_configuration_is_rejected(environment, page_size):
    with pytest.raises(ValueError):
        RuntimeMarketStateBackfillSource(
            PageReader(), environment=environment, page_size=page_size
        )


async def test_latest_bucket_uses_only_page_reader_capability():
    at = fixture_state("A", 0).bucket_start
    reader = PageReader(latest=at)
    source = RuntimeMarketStateBackfillSource(reader, environment="research")
    assert await source.latest_bucket() == at
    assert reader.calls == ["research"]
    assert not hasattr(reader, "load_recovery_window")


async def test_bucket_split_across_pages_keeps_every_state_and_cursor_order():
    a, b, c, next_bucket = (
        fixture_state("A", 0),
        fixture_state("B", 0),
        fixture_state("C", 0),
        fixture_state("A", 1),
    )
    reader = PageReader(((a, b), (c, next_bucket), ()))
    source = RuntimeMarketStateBackfillSource(
        reader, environment="research", page_size=2
    )
    batches = [
        batch
        async for batch in source.batches_after(
            RuntimeStateCursor(), until=next_bucket.bucket_start
        )
    ]
    assert [batch.states for batch in batches] == [(a, b), (c,), (next_bucket,)]
    assert [call[1] for call in reader.calls] == [
        RuntimeStateCursor(),
        RuntimeStateCursor(a.bucket_start, "B"),
        RuntimeStateCursor(next_bucket.bucket_start, "A"),
    ]
    assert all(call[0] == "research" and call[2] == 2 for call in reader.calls)
    assert all(batch.stream_id is None and batch.sequence == 0 for batch in batches)


async def test_inclusive_cutoff_filters_future_rows_and_stops_paging():
    eligible, future = fixture_state("A", 0), fixture_state("B", 2)
    reader = PageReader(((eligible, future),))
    source = RuntimeMarketStateBackfillSource(
        reader, environment="research", page_size=2
    )
    batches = [
        batch
        async for batch in source.batches_after(
            RuntimeStateCursor(), until=eligible.bucket_start
        )
    ]
    assert [batch.states for batch in batches] == [(eligible,)]
    assert len(reader.calls) == 1


@pytest.mark.parametrize("empty", [False, True])
async def test_short_or_empty_page_terminates_without_another_read(empty):
    state = fixture_state("A", 0)
    reader = PageReader((() if empty else (state,),))
    source = RuntimeMarketStateBackfillSource(
        reader, environment="research", page_size=2
    )
    batches = [
        batch
        async for batch in source.batches_after(
            RuntimeStateCursor(), until=state.bucket_start
        )
    ]
    assert len(batches) == (0 if empty else 1)
    assert len(reader.calls) == 1


@pytest.mark.parametrize("naive_cursor", [False, True])
async def test_naive_bounds_are_rejected_before_read(naive_cursor):
    reader = PageReader()
    source = RuntimeMarketStateBackfillSource(reader, environment="research")
    cursor = (
        RuntimeStateCursor(datetime(2026, 9, 30))
        if naive_cursor
        else RuntimeStateCursor()
    )
    until = (
        fixture_state("A", 0).bucket_start if naive_cursor else datetime(2026, 9, 30)
    )
    with pytest.raises(ValueError):
        _ = [batch async for batch in source.batches_after(cursor, until=until)]
    assert reader.calls == []


@pytest.mark.parametrize(
    "error", [RuntimeError("read failed"), asyncio.CancelledError()]
)
async def test_read_failure_or_cancellation_propagates(error):
    class FailingReader(PageReader):
        async def load_after(self, **kwargs):
            raise error

    source = RuntimeMarketStateBackfillSource(FailingReader(), environment="research")
    with pytest.raises(type(error)):
        _ = [
            batch
            async for batch in source.batches_after(
                RuntimeStateCursor(), until=fixture_state("A", 0).bucket_start
            )
        ]
