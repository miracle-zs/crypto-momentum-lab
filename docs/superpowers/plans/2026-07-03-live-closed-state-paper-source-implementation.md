# Live Closed-State Paper Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist live-style closed 15-second market states from `market-data` and let `strategy-runner` run bounded paper sessions from those PostgreSQL rows.

**Architecture:** Add a runtime state handoff table and repository under the existing PostgreSQL persistence boundary. Add a `market_data` closed-state publisher that normalizes archived raw envelopes, keeps event-time bucket accumulators, and writes only closed `MarketState15s` rows. Add a bounded paper polling source and separate `cml-strategy-runner paper-live-source` CLI command that reuses the existing paper runner and optional paper-report persistence.

**Tech Stack:** Python 3.13, SQLAlchemy 2 async ORM, Alembic migrations, PostgreSQL JSONB/Numeric/timestamptz, Typer CLI, pytest, ruff, mypy.

---

## File Structure

- Modify: `src/crypto_momentum_lab/persistence/postgres/models.py`
  - Add `RuntimeMarketState15sRow`.
- Create: `alembic/versions/20260703_0004_runtime_market_states.py`
  - Create `runtime_market_states_15s` table and indexes.
- Create: `src/crypto_momentum_lab/persistence/postgres/runtime_state_repository.py`
  - Map `MarketState15s` to rows, validate closed states, save idempotently, and load in cursor order.
- Modify: `src/crypto_momentum_lab/persistence/postgres/__init__.py`
  - Export `PostgresRuntimeMarketStateRepository`.
- Modify: `tests/conftest.py`
  - Clear runtime state rows in PostgreSQL-backed tests.
- Modify: `tests/integration/persistence/test_migrations.py`
  - Assert migration creates `runtime_market_states_15s`.
- Create: `tests/unit/persistence/postgres/test_runtime_state_repository.py`
  - Unit-test row mapping and validation.
- Create: `tests/integration/persistence/test_runtime_state_repository.py`
  - Integration-test save/load/idempotency when PostgreSQL is available.
- Create: `src/crypto_momentum_lab/market_data/runtime_states.py`
  - Add `ClosedMarketStatePublisher`, config, metrics, and publisher repository protocol.
- Modify: `src/crypto_momentum_lab/market_data/capture/coordinator.py`
  - Add optional archived-envelope sink called only after raw archive append succeeds.
- Modify: `src/crypto_momentum_lab/apps/market_data/main.py`
  - Wire runtime state repository and publisher into the market-data runtime composition root.
- Create: `tests/unit/market_data/test_runtime_states.py`
  - Unit-test watermark closure, late event handling, and repository calls.
- Modify: `tests/unit/market_data/capture/test_coordinator.py`
  - Verify archived-envelope sink sequencing.
- Create: `src/crypto_momentum_lab/strategy_runner/live_source.py`
  - Add `PostgresPaperMarketStateSource`, `PaperLiveSourceConfig`, and sync polling adapter around the async repository.
- Modify: `src/crypto_momentum_lab/strategy_runner/__init__.py`
  - Export live-source classes.
- Modify: `src/crypto_momentum_lab/apps/strategy_runner/main.py`
  - Add `paper-live-source` command.
- Modify: `tests/unit/apps/strategy_runner/test_strategy_runner_main.py`
  - Test CLI validation and command composition.
- Create: `tests/unit/strategy_runner/test_live_source.py`
  - Unit-test polling order, cursor movement, `max_states`, and idle timeout.
- Modify: `README.md`
  - Document `paper-live-source` usage and clarify that it is still paper-only.

---

### Task 1: Runtime Market State Table And Repository

**Files:**
- Modify: `src/crypto_momentum_lab/persistence/postgres/models.py`
- Create: `alembic/versions/20260703_0004_runtime_market_states.py`
- Create: `src/crypto_momentum_lab/persistence/postgres/runtime_state_repository.py`
- Modify: `src/crypto_momentum_lab/persistence/postgres/__init__.py`
- Modify: `tests/conftest.py`
- Modify: `tests/integration/persistence/test_migrations.py`
- Create: `tests/unit/persistence/postgres/test_runtime_state_repository.py`
- Create: `tests/integration/persistence/test_runtime_state_repository.py`

- [ ] **Step 1: Write failing unit tests for row mapping and validation**

Create `tests/unit/persistence/postgres/test_runtime_state_repository.py`:

```python
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    runtime_state_row,
    validate_closed_states,
)


def test_runtime_state_row_preserves_market_state_values() -> None:
    state = fixture_state("BTCUSDT", 0)
    row = runtime_state_row(
        state,
        source_watermark_at=datetime(2026, 7, 3, 0, 0, 45, tzinfo=UTC),
        input_sequence_min=1,
        input_sequence_max=3,
    )

    assert row["environment"] == "research"
    assert row["symbol"] == "BTCUSDT"
    assert row["bucket_start"] == datetime(2026, 7, 3, 0, 0, tzinfo=UTC)
    assert row["close_price"] == Decimal("100")
    assert row["source_watermark_at"] == datetime(2026, 7, 3, 0, 0, 45, tzinfo=UTC)
    assert row["closure_reason"] == "watermark_elapsed"
    assert row["input_sequence_min"] == 1
    assert row["input_sequence_max"] == 3


def test_validate_closed_states_rejects_duplicate_primary_key() -> None:
    state = fixture_state("BTCUSDT", 0)

    with pytest.raises(ValueError, match="duplicate runtime market state"):
        validate_closed_states((state, state))


def test_validate_closed_states_rejects_naive_timestamp() -> None:
    state = fixture_state("BTCUSDT", 0)
    naive = object.__new__(MarketState15s)
    for field in state.__dataclass_fields__:
        object.__setattr__(naive, field, getattr(state, field))
    object.__setattr__(naive, "bucket_start", datetime(2026, 7, 3, 0, 0))

    with pytest.raises(ValueError, match="bucket_start must be timezone-aware"):
        validate_closed_states((naive,))


def fixture_state(symbol: str, bucket_index: int) -> MarketState15s:
    start = datetime(2026, 7, 3, 0, 0, tzinfo=UTC) + timedelta(seconds=15 * bucket_index)
    end = start + timedelta(seconds=15)
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol=symbol,
        bucket_start=start,
        bucket_end=end,
        open_price=Decimal("100"),
        high_price=Decimal("101"),
        low_price=Decimal("99"),
        close_price=Decimal("100"),
        trade_count=2,
        trade_notional=Decimal("200"),
        aggressive_buy_notional=Decimal("120"),
        aggressive_sell_notional=Decimal("80"),
        last_bid_price=Decimal("99.99"),
        last_ask_price=Decimal("100.01"),
        spread=Decimal("0.02"),
        midpoint=Decimal("100"),
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=Decimal("100"),
        closed_kline_count=0,
        source_event_count=3,
        first_received_at=start,
        last_received_at=end,
    )
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/unit/persistence/postgres/test_runtime_state_repository.py -v
```

Expected: FAIL with `ModuleNotFoundError` for `runtime_state_repository`.

- [ ] **Step 3: Add ORM model and migration**

Add `RuntimeMarketState15sRow` to `src/crypto_momentum_lab/persistence/postgres/models.py` with all `MarketState15s` fields plus `created_at`, `updated_at`, `source_watermark_at`, `closure_reason`, `input_sequence_min`, and `input_sequence_max`. Use `String(32)` for `environment`, `symbol`, `exchange`, and `closure_reason`; `DateTime(timezone=True)` for timestamps; `Numeric(38, 18)` for decimal values; nullable columns only where the domain model permits `None`.

Create `alembic/versions/20260703_0004_runtime_market_states.py` with revision `20260703_0004` and down revision `20260702_0003`. The migration creates:

```text
runtime_market_states_15s
primary key: environment, symbol, bucket_start
index: ix_runtime_market_states_15s_polling on environment, bucket_start, symbol
index: ix_runtime_market_states_15s_symbol_time on environment, symbol, bucket_start
index: ix_runtime_market_states_15s_created on environment, created_at
```

Modify `tests/integration/persistence/test_migrations.py` so the expected table
set includes `runtime_market_states_15s`.

- [ ] **Step 4: Implement repository mapping and validation**

Create `src/crypto_momentum_lab/persistence/postgres/runtime_state_repository.py` with these public APIs:

```python
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.persistence.postgres.models import RuntimeMarketState15sRow


@dataclass(frozen=True, slots=True)
class RuntimeStateCursor:
    bucket_start: datetime | None = None
    symbol: str | None = None


@dataclass(frozen=True, slots=True)
class RuntimeStateSequenceRange:
    minimum: int | None = None
    maximum: int | None = None


def validate_closed_states(states: tuple[MarketState15s, ...]) -> None:
    seen: set[tuple[str, str, datetime]] = set()
    for state in states:
        _require_aware(state.bucket_start, "bucket_start")
        _require_aware(state.bucket_end, "bucket_end")
        if state.first_received_at is not None:
            _require_aware(state.first_received_at, "first_received_at")
        if state.last_received_at is not None:
            _require_aware(state.last_received_at, "last_received_at")
        if state.bucket_end <= state.bucket_start:
            raise ValueError("bucket_end must be after bucket_start")
        if not state.environment.strip():
            raise ValueError("environment must not be empty")
        if not state.symbol.strip():
            raise ValueError("symbol must not be empty")
        key = (state.environment, state.symbol, state.bucket_start)
        if key in seen:
            raise ValueError("duplicate runtime market state")
        seen.add(key)
```

Implement `runtime_state_row()`, `market_state_from_row()`, and
`PostgresRuntimeMarketStateRepository.save_closed_states()`,
`load_after()`, and `load_latest_bucket()`. `load_after()` must order by
`bucket_start`, then `symbol`, and use the cursor predicate:

```text
bucket_start > cursor.bucket_start
OR (bucket_start = cursor.bucket_start AND symbol > cursor.symbol)
```

For idempotency, an existing row matches only when all persisted fields except
`created_at` and `updated_at` are equal after Decimal and datetime
normalization. A mismatch raises `ValueError("runtime market state conflict")`.

- [ ] **Step 5: Export repository and clear test fixtures**

Export `PostgresRuntimeMarketStateRepository`, `RuntimeStateCursor`, and
`RuntimeStateSequenceRange` from `src/crypto_momentum_lab/persistence/postgres/__init__.py`.

Modify `tests/conftest.py` to delete `RuntimeMarketState15sRow` before the
existing repository and capture repository fixtures yield.

- [ ] **Step 6: Add PostgreSQL integration tests**

Create `tests/integration/persistence/test_runtime_state_repository.py`:

```python
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.persistence.postgres.models import RuntimeMarketState15sRow
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
    RuntimeStateCursor,
    RuntimeStateSequenceRange,
)
from crypto_momentum_lab.persistence.postgres.session import create_async_database_engine
from tests.unit.persistence.postgres.test_runtime_state_repository import fixture_state


@pytest.fixture
async def runtime_state_repository(
    async_database_url: str,
) -> AsyncIterator[PostgresRuntimeMarketStateRepository]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            await session.execute(delete(RuntimeMarketState15sRow))
    yield PostgresRuntimeMarketStateRepository(factory)
    await engine.dispose()


async def test_save_closed_states_is_idempotent_and_ordered(
    runtime_state_repository: PostgresRuntimeMarketStateRepository,
) -> None:
    first = fixture_state("ETHUSDT", 0)
    second = fixture_state("BTCUSDT", 0)

    await runtime_state_repository.save_closed_states(
        (first, second),
        source_watermark_at=datetime(2026, 7, 3, 0, 0, 45, tzinfo=UTC),
        sequence_range=RuntimeStateSequenceRange(1, 2),
    )
    await runtime_state_repository.save_closed_states(
        (first, second),
        source_watermark_at=datetime(2026, 7, 3, 0, 0, 45, tzinfo=UTC),
        sequence_range=RuntimeStateSequenceRange(1, 2),
    )

    rows = await runtime_state_repository.load_after(
        environment="research",
        cursor=RuntimeStateCursor(),
        limit=10,
    )

    assert tuple(row.symbol for row in rows) == ("BTCUSDT", "ETHUSDT")


async def test_conflicting_closed_state_fails(
    runtime_state_repository: PostgresRuntimeMarketStateRepository,
) -> None:
    state = fixture_state("BTCUSDT", 0)
    await runtime_state_repository.save_closed_states(
        (state,),
        source_watermark_at=datetime(2026, 7, 3, 0, 0, 45, tzinfo=UTC),
        sequence_range=RuntimeStateSequenceRange(1, 1),
    )

    conflicting = replace(state, close_price=state.close_price + 1)

    with pytest.raises(ValueError, match="runtime market state conflict"):
        await runtime_state_repository.save_closed_states(
            (conflicting,),
            source_watermark_at=datetime(2026, 7, 3, 0, 0, 45, tzinfo=UTC),
            sequence_range=RuntimeStateSequenceRange(1, 1),
        )
```

- [ ] **Step 7: Run targeted verification**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/unit/persistence/postgres/test_runtime_state_repository.py -v
.venv/bin/ruff check alembic src/crypto_momentum_lab/persistence/postgres \
  tests/unit/persistence/postgres tests/integration/persistence
.venv/bin/mypy src
```

If PostgreSQL is available, also run:

```bash
CML_TEST_ASYNC_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml \
CML_TEST_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml \
  PYTHONPATH=src .venv/bin/python -m pytest \
  tests/integration/persistence/test_migrations.py \
  tests/integration/persistence/test_runtime_state_repository.py -v
```

- [ ] **Step 8: Commit**

```bash
git add alembic/versions/20260703_0004_runtime_market_states.py \
  src/crypto_momentum_lab/persistence/postgres \
  tests/conftest.py \
  tests/unit/persistence/postgres/test_runtime_state_repository.py \
  tests/integration/persistence/test_migrations.py \
  tests/integration/persistence/test_runtime_state_repository.py
git commit -m "feat: persist runtime market states"
```

---

### Task 2: Closed-State Publisher And Coordinator Hook

**Files:**
- Create: `src/crypto_momentum_lab/market_data/runtime_states.py`
- Modify: `src/crypto_momentum_lab/market_data/capture/coordinator.py`
- Modify: `src/crypto_momentum_lab/apps/market_data/main.py`
- Create: `tests/unit/market_data/test_runtime_states.py`
- Modify: `tests/unit/market_data/capture/test_coordinator.py`

- [ ] **Step 1: Write failing publisher tests**

Create `tests/unit/market_data/test_runtime_states.py`:

```python
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    RawEnvelope,
)
from crypto_momentum_lab.market_data.runtime_states import (
    ClosedMarketStatePublisher,
    ClosedMarketStatePublisherConfig,
)


class FakeRuntimeStateRepository:
    def __init__(self) -> None:
        self.saved_symbols: list[tuple[str, ...]] = []

    async def save_closed_states(self, states, *, source_watermark_at, sequence_range):
        self.saved_symbols.append(tuple(state.symbol for state in states))


async def test_publisher_closes_only_buckets_behind_watermark() -> None:
    repository = FakeRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(
        repository=repository,
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )

    await publisher.observe(fixture_trade(0, price="100", sequence=1))
    await publisher.observe(fixture_trade(1, price="101", sequence=2))
    await publisher.observe(fixture_trade(3, price="102", sequence=3))

    assert repository.saved_symbols == [("BTCUSDT",)]
    assert publisher.metrics.closed_state_count == 1


async def test_late_event_for_closed_bucket_is_rejected() -> None:
    repository = FakeRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(
        repository=repository,
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )

    await publisher.observe(fixture_trade(0, price="100", sequence=1))
    await publisher.observe(fixture_trade(3, price="102", sequence=2))
    await publisher.observe(fixture_trade(0, price="99", sequence=3))

    assert publisher.metrics.late_event_count == 1
    assert repository.saved_symbols == [("BTCUSDT",)]


def fixture_trade(bucket_index: int, *, price: str, sequence: int) -> RawEnvelope:
    event_at = datetime(2026, 7, 3, 0, 0, tzinfo=UTC) + timedelta(seconds=15 * bucket_index)
    return RawEnvelope(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        route=CaptureRoute.MARKET,
        stream=CaptureStream.AGG_TRADE,
        symbol="BTCUSDT",
        exchange_event_at=event_at,
        received_at=event_at,
        received_monotonic_ns=sequence,
        connection_session_id=UUID(int=1),
        local_sequence=sequence,
        exchange_sequence=str(sequence),
        subscription_generation=1,
        raw_payload={
            "e": "aggTrade",
            "s": "BTCUSDT",
            "a": sequence,
            "p": price,
            "q": "1",
            "T": int(event_at.timestamp() * 1000),
            "m": False,
        },
    )
```

- [ ] **Step 2: Run publisher tests to verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/unit/market_data/test_runtime_states.py -v
```

Expected: FAIL with `ModuleNotFoundError` for `market_data.runtime_states`.

- [ ] **Step 3: Implement publisher**

Create `src/crypto_momentum_lab/market_data/runtime_states.py` with:

```python
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from crypto_momentum_lab.domain.market.models import MarketState15s, RawEnvelope
from crypto_momentum_lab.market_data.aggregation import aggregate_market_states_15s, bucket_start_15s
from crypto_momentum_lab.market_data.normalization import BinanceNormalizationError, normalize_binance_envelope
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import RuntimeStateSequenceRange


class ClosedStateRepository(Protocol):
    async def save_closed_states(
        self,
        states: tuple[MarketState15s, ...],
        *,
        source_watermark_at: datetime,
        sequence_range: RuntimeStateSequenceRange,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class ClosedMarketStatePublisherConfig:
    closure_delay_seconds: int = 30

    def __post_init__(self) -> None:
        if self.closure_delay_seconds <= 0:
            raise ValueError("closure_delay_seconds must be positive")


@dataclass(frozen=True, slots=True)
class ClosedMarketStatePublisherMetrics:
    received_envelope_count: int
    normalized_event_count: int
    closed_state_count: int
    rejected_envelope_count: int
    late_event_count: int
    latest_watermark_at: datetime | None
```

`ClosedMarketStatePublisher.observe(envelope)` must normalize one envelope,
buffer it by `(environment, symbol, bucket_start)`, compute the event-time
watermark, close all buckets with `bucket_end <= watermark`, and call
`repository.save_closed_states()` with states sorted by `(bucket_start, symbol)`.
Use `aggregate_market_states_15s()` for each closed bucket's buffered events so
the runtime state semantics match offline derivation.

- [ ] **Step 4: Write failing coordinator sequencing test**

Modify `tests/unit/market_data/capture/test_coordinator.py` to add:

```python
async def test_archived_envelope_sink_runs_after_archive_append(raw_envelope) -> None:
    queue = BoundedEnvelopeQueue(max_events=10, max_bytes=10000)
    archive = FakeArchive()
    quality = FakeQualityTracker()
    repository = FakeQualityRepository()
    published: list[RawEnvelope] = []
    coordinator = CaptureCoordinator(
        queue=queue,
        archive=archive,
        quality=quality,
        repository=repository,
        archived_envelope_sink=published.append,
    )

    await coordinator.submit(raw_envelope)
    await coordinator.stop()

    assert archive.appended == [raw_envelope]
    assert published == [raw_envelope]
```

If the existing fake archive uses different attribute names, adapt the asserts
to the existing fake names while preserving the behavior under test.

- [ ] **Step 5: Implement coordinator hook and market-data wiring**

Modify `src/crypto_momentum_lab/market_data/capture/coordinator.py`:

- add `type ArchivedEnvelopeSink = Callable[[RawEnvelope], object]`;
- add constructor parameter `archived_envelope_sink: ArchivedEnvelopeSink | None = None`;
- store it on `self`;
- keep `_process_envelope()` responsible for archive append, quality
  persistence, and acknowledgement persistence only;
- after `_process_batch()` has awaited all `_process_envelope()` tasks and
  confirmed there were no failures, iterate the original `batch` order and call
  the archived-envelope sink for each envelope, awaiting the result if it is
  awaitable;
- keep the existing acknowledgement sink unchanged.

This preserves both guarantees from the design: the publisher sees only
successfully archived envelopes, and it receives them in dequeue order even
though archive writes inside the batch are concurrent.

Modify `src/crypto_momentum_lab/apps/market_data/main.py`:

- instantiate `PostgresRuntimeMarketStateRepository(sessions)`;
- instantiate `ClosedMarketStatePublisher(repository=runtime_state_repository)`;
- pass `archived_envelope_sink=publisher.observe` to `CaptureCoordinator`.

- [ ] **Step 6: Run targeted verification**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/unit/market_data/test_runtime_states.py \
  tests/unit/market_data/capture/test_coordinator.py -v
.venv/bin/ruff check src/crypto_momentum_lab/market_data \
  src/crypto_momentum_lab/apps/market_data tests/unit/market_data
.venv/bin/mypy src
```

- [ ] **Step 7: Commit**

```bash
git add src/crypto_momentum_lab/market_data/runtime_states.py \
  src/crypto_momentum_lab/market_data/capture/coordinator.py \
  src/crypto_momentum_lab/apps/market_data/main.py \
  tests/unit/market_data/test_runtime_states.py \
  tests/unit/market_data/capture/test_coordinator.py
git commit -m "feat: publish closed runtime market states"
```

---

### Task 3: PostgreSQL Paper Polling Source

**Files:**
- Create: `src/crypto_momentum_lab/strategy_runner/live_source.py`
- Modify: `src/crypto_momentum_lab/strategy_runner/__init__.py`
- Create: `tests/unit/strategy_runner/test_live_source.py`

- [ ] **Step 1: Write failing source tests**

Create `tests/unit/strategy_runner/test_live_source.py`:

```python
from datetime import UTC, datetime, timedelta

from crypto_momentum_lab.persistence.postgres.runtime_state_repository import RuntimeStateCursor
from crypto_momentum_lab.strategy_runner.live_source import (
    PaperLiveSourceConfig,
    PostgresPaperMarketStateSource,
)
from tests.unit.persistence.postgres.test_runtime_state_repository import fixture_state


class FakeLoader:
    def __init__(self, batches):
        self.batches = list(batches)
        self.cursors: list[RuntimeStateCursor] = []

    def load_after(self, *, cursor: RuntimeStateCursor, limit: int):
        self.cursors.append(cursor)
        if not self.batches:
            return ()
        return self.batches.pop(0)


def test_postgres_paper_source_yields_in_order_and_advances_cursor() -> None:
    first = fixture_state("BTCUSDT", 0)
    second = fixture_state("ETHUSDT", 0)
    loader = FakeLoader([tuple(sorted((second, first), key=lambda item: (item.bucket_start, item.symbol)))])
    source = PostgresPaperMarketStateSource(
        loader=loader,
        config=PaperLiveSourceConfig(
            environment="research",
            start_at=None,
            poll_interval_seconds=0,
            idle_timeout_seconds=1,
            max_states=2,
            batch_size=10,
        ),
    )

    states = tuple(source)

    assert tuple(state.symbol for state in states) == ("BTCUSDT", "ETHUSDT")
    assert loader.cursors[0] == RuntimeStateCursor()


def test_postgres_paper_source_stops_after_idle_timeout() -> None:
    loader = FakeLoader([()])
    source = PostgresPaperMarketStateSource(
        loader=loader,
        config=PaperLiveSourceConfig(
            environment="research",
            start_at=datetime(2026, 7, 3, 0, 0, tzinfo=UTC),
            poll_interval_seconds=0,
            idle_timeout_seconds=0,
            max_states=10,
            batch_size=10,
        ),
    )

    assert tuple(source) == ()
```

- [ ] **Step 2: Run source tests to verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/strategy_runner/test_live_source.py -v
```

Expected: FAIL with `ModuleNotFoundError` for `strategy_runner.live_source`.

- [ ] **Step 3: Implement polling source**

Create `src/crypto_momentum_lab/strategy_runner/live_source.py` with:

```python
import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
    RuntimeStateCursor,
)


class RuntimeStateLoader(Protocol):
    def load_after(
        self,
        *,
        cursor: RuntimeStateCursor,
        limit: int,
    ) -> tuple[MarketState15s, ...]: ...


@dataclass(frozen=True, slots=True)
class PaperLiveSourceConfig:
    environment: str
    start_at: datetime | None
    poll_interval_seconds: float
    idle_timeout_seconds: float
    max_states: int
    batch_size: int
```

Implement `PostgresPaperMarketStateSource.__iter__()` as a bounded sync
iterator. It calls `loader.load_after(cursor=cursor, limit=batch_size)`, yields
states in the returned order, advances the cursor after each yield, and stops
after `max_states` or after `idle_timeout_seconds` without new rows. Validate
positive `max_states` and `batch_size`, non-negative poll and idle values, and
timezone-aware `start_at` when present.

Also add `AsyncPostgresRuntimeStateLoader`, which wraps
`PostgresRuntimeMarketStateRepository` for the CLI by calling
`asyncio.run(repository.load_after(...))`.

- [ ] **Step 4: Export source classes**

Modify `src/crypto_momentum_lab/strategy_runner/__init__.py` to export:

- `AsyncPostgresRuntimeStateLoader`;
- `PaperLiveSourceConfig`;
- `PostgresPaperMarketStateSource`.

- [ ] **Step 5: Run targeted verification**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/strategy_runner/test_live_source.py -v
.venv/bin/ruff check src/crypto_momentum_lab/strategy_runner tests/unit/strategy_runner
.venv/bin/mypy src
```

- [ ] **Step 6: Commit**

```bash
git add src/crypto_momentum_lab/strategy_runner/live_source.py \
  src/crypto_momentum_lab/strategy_runner/__init__.py \
  tests/unit/strategy_runner/test_live_source.py
git commit -m "feat: add postgres paper market state source"
```

---

### Task 4: `paper-live-source` CLI

**Files:**
- Modify: `src/crypto_momentum_lab/apps/strategy_runner/main.py`
- Modify: `tests/unit/apps/strategy_runner/test_strategy_runner_main.py`
- Modify: `README.md`

- [ ] **Step 1: Write failing CLI tests**

Modify `tests/unit/apps/strategy_runner/test_strategy_runner_main.py` to add:

```python
def test_paper_live_source_requires_database_url(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CML_DATABASE_URL", raising=False)

    result = runner.invoke(
        main.app,
        [
            "paper-live-source",
            "--strategy",
            "compression_breakout",
            "--environment",
            "research",
            "--output",
            str(tmp_path / "paper-live.json"),
        ],
    )

    assert result.exit_code != 0
    assert "--database-url or CML_DATABASE_URL is required" in result.output


def test_paper_live_source_runs_bounded_source(tmp_path: Path, monkeypatch) -> None:
    output_path = tmp_path / "paper-live.json"
    calls: list[object] = []
    report = SimpleNamespace(
        input_state_count=2,
        signals=(),
        candidates=(),
        paper_fills=(),
    )

    def fake_build_source(*, database_url: str, environment: str, start_at, poll_interval_seconds: float, idle_timeout_seconds: float, max_states: int, batch_size: int):
        calls.append((database_url, environment, max_states, batch_size))
        return object()

    monkeypatch.setattr(main, "build_postgres_paper_source", fake_build_source)
    monkeypatch.setattr(main, "run_paper_trading", lambda **kwargs: report)
    monkeypatch.setattr(main, "write_paper_trading_report", lambda report, path: None)

    result = runner.invoke(
        main.app,
        [
            "paper-live-source",
            "--strategy",
            "compression_breakout",
            "--database-url",
            "postgresql+asyncpg://cml:cml@localhost:54329/cml",
            "--environment",
            "research",
            "--output",
            str(output_path),
            "--max-states",
            "2",
            "--batch-size",
            "5",
            "--idle-timeout-seconds",
            "0",
        ],
    )

    assert result.exit_code == 0
    assert calls == [("postgresql+asyncpg://cml:cml@localhost:54329/cml", "research", 2, 5)]
    assert "Paper live-source run completed: states=2 signals=0 candidates=0 fills=0 persisted=false" in result.stdout
```

- [ ] **Step 2: Run CLI tests to verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/unit/apps/strategy_runner/test_strategy_runner_main.py -v
```

Expected: FAIL because `paper-live-source` does not exist.

- [ ] **Step 3: Implement command and source builder**

Modify `src/crypto_momentum_lab/apps/strategy_runner/main.py`:

- import `PostgresRuntimeMarketStateRepository`, `AsyncPostgresRuntimeStateLoader`, `PaperLiveSourceConfig`, and `PostgresPaperMarketStateSource`;
- add helper `build_postgres_paper_source(...)`;
- add command `paper_live_source_command`.

The command should:

1. resolve `database_url` from option or `CML_DATABASE_URL`;
2. build the polling source;
3. construct `PaperRunnerConfig` with `run_id` defaulting to `paper-live-{uuid4()}`;
4. call `run_paper_trading(source=source, config=config)`;
5. write JSON report;
6. optionally call existing `persist_paper_report()`;
7. print:

```text
Paper live-source run completed: states=<n> signals=<n> candidates=<n> fills=<n> persisted=<true|false>
```

Keep existing `paper` command unchanged.

- [ ] **Step 4: Update README**

Add a `Paper Live Source` subsection:

```bash
.venv/bin/cml-strategy-runner paper-live-source \
  --strategy compression_breakout \
  --database-url "$CML_DATABASE_URL" \
  --environment research \
  --output reports/compression-breakout-paper-live-source.json \
  --max-states 1000 \
  --idle-timeout-seconds 60 \
  --persist
```

State that this is still simulated paper execution and does not connect to
Binance private APIs or submit orders.

- [ ] **Step 5: Run targeted verification**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/unit/apps/strategy_runner/test_strategy_runner_main.py \
  tests/unit/strategy_runner/test_live_source.py -v
.venv/bin/cml-strategy-runner paper-live-source --help
.venv/bin/ruff check src/crypto_momentum_lab/apps/strategy_runner \
  src/crypto_momentum_lab/strategy_runner tests/unit/apps/strategy_runner \
  tests/unit/strategy_runner
.venv/bin/mypy src
```

- [ ] **Step 6: Commit**

```bash
git add README.md src/crypto_momentum_lab/apps/strategy_runner/main.py \
  tests/unit/apps/strategy_runner/test_strategy_runner_main.py
git commit -m "feat: add paper live-source cli"
```

---

### Task 5: Final Verification And Merge

**Files:**
- No planned file edits unless verification exposes a defect.

- [ ] **Step 1: Run offline verification**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit -q
.venv/bin/ruff check .
.venv/bin/mypy src
```

Expected:

- all unit tests pass;
- ruff reports `All checks passed!`;
- mypy reports no issues.

- [ ] **Step 2: Run PostgreSQL verification when available**

If Docker is running, run:

```bash
docker compose up -d postgres
CML_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml \
  .venv/bin/alembic upgrade head
CML_TEST_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml \
CML_TEST_ASYNC_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml \
  PYTHONPATH=src .venv/bin/python -m pytest \
  tests/integration/persistence/test_migrations.py \
  tests/integration/persistence/test_runtime_state_repository.py \
  tests/integration/persistence/test_strategy_run_repository.py -v
```

Expected: targeted integration tests pass. If Docker is not running, record the
environment limitation and keep the offline verification result.

- [ ] **Step 3: Verify CLI help from installed package**

If the shared `.venv` editable install points at a worktree during
implementation, reinstall from the final main checkout after merging:

```bash
.venv/bin/python -m pip install -e .
.venv/bin/cml-strategy-runner paper-live-source --help
```

Expected: help shows `--database-url`, `--environment`, `--max-states`,
`--idle-timeout-seconds`, `--batch-size`, and `--persist`.

- [ ] **Step 4: Run final status check**

Run:

```bash
git status --short --branch
git log --oneline --decorate -10
git worktree list
```

Expected: clean `main`, implementation commits present, no stale feature
worktree after merge cleanup.

- [ ] **Step 5: Report outcome**

Summarize:

- runtime market state table/repository;
- closed-state publisher and archive-then-publish sequencing;
- PostgreSQL paper polling source;
- `paper-live-source` CLI;
- verification results and PostgreSQL integration limitation if Docker is not
  available;
- remaining excluded scope: no Binance private API, no real orders, no account
  state, no risk engine.
