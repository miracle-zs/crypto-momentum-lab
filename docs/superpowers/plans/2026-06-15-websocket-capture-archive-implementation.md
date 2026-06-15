# WebSocket Capture and Raw Archive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add dynamic Binance USD-M WebSocket capture for the active monitoring universe, durable Zstandard JSONL archival, quality tracking, recovery, and a long-running `run-market-data` process.

**Architecture:** The existing market-data process owns universe refresh and a new capture pipeline. WebSocket receivers place uniform raw envelopes on a bounded queue; a single capture coordinator archives and quality-checks each envelope, and only emits durable acknowledgements after grouped `fsync`. PostgreSQL stores low-volume manifests, quality events, and process state, while raw high-volume messages remain in append-only files.

**Tech Stack:** Python 3.12+, asyncio, websockets 16, python-zstandard 0.25, SQLAlchemy 2, PostgreSQL 16, Alembic, Pydantic 2, pytest, pytest-asyncio, Docker Compose.

---

## File Map

Create these focused modules:

```text
src/crypto_momentum_lab/
  domain/market/
    __init__.py
    models.py              # raw envelopes, manifests, quality and state types
    ports.py               # archive and capture persistence protocols
  market_data/
    capture/
      __init__.py
      queue.py             # bounded event and byte accounting
      subscriptions.py     # desired sets, grouping and add-before-remove plans
      coordinator.py       # sole queue consumer and durable ack fan-out
      service.py           # process-level lifecycle and shutdown
    quality/
      __init__.py
      tracker.py           # duplicate, gap, regression and silence rules
    binance/
      websocket.py         # route-aware connection and payload envelopes
      connection_pool.py   # connection grouping, reconciliation and reconnect
  persistence/
    raw_files/
      __init__.py
      archive.py           # Zstandard JSONL writers, group commit and rotation
      journal.py           # durable pending-manifest journal
      recovery.py          # temporary-file recovery and quarantine
    postgres/
      capture_repository.py # manifests, quality events and process state
```

Modify:

```text
pyproject.toml
configs/environments/research.yaml
configs/capture/binance_usdm.yaml
src/crypto_momentum_lab/config/models.py
src/crypto_momentum_lab/config/loader.py
src/crypto_momentum_lab/apps/market_data/main.py
src/crypto_momentum_lab/universe/refresh.py
src/crypto_momentum_lab/persistence/postgres/models.py
compose.yaml
Dockerfile
README.md
```

The plan deliberately defers normalized market events, 15-second states,
Parquet derivation, strategy features, authenticated account streams, and
execution.

### Task 1: Capture Dependencies and Immutable Configuration

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/crypto_momentum_lab/config/models.py`
- Modify: `src/crypto_momentum_lab/config/loader.py`
- Modify: `configs/environments/research.yaml`
- Create: `configs/capture/binance_usdm.yaml`
- Modify: `tests/unit/config/test_loader.py`

- [ ] **Step 1: Write failing configuration tests**

Append to `tests/unit/config/test_loader.py`:

```python
def test_loads_websocket_capture_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "CML_DATABASE_URL",
        "postgresql+asyncpg://cml:cml@localhost:54329/cml",
    )

    config = load_runtime_config(
        Path("configs/environments/research.yaml")
    )

    assert str(config.capture.market_websocket_url) == (
        "wss://fstream.binance.com/market/ws"
    )
    assert str(config.capture.public_websocket_url) == (
        "wss://fstream.binance.com/public/ws"
    )
    assert config.capture.enabled_streams == (
        "aggTrade",
        "bookTicker",
        "forceOrder",
        "markPrice@1s",
        "kline_1m",
    )
    assert config.capture.max_subscriptions_per_connection == 100
    assert config.capture.archive.group_commit_max_events == 250
    assert config.capture.archive.group_commit_max_milliseconds == 250


def test_capture_config_rejects_invalid_disk_hysteresis() -> None:
    with pytest.raises(
        ValueError,
        match="recovery_free_bytes must be greater",
    ):
        CaptureConfig.model_validate(
            {
                "market_websocket_url": "wss://example.test/market/ws",
                "public_websocket_url": "wss://example.test/public/ws",
                "enabled_streams": ["aggTrade"],
                "max_subscriptions_per_connection": 100,
                "control_messages_per_second": 5,
                "connection_lifetime_seconds": 82800,
                "open_timeout_seconds": 10,
                "ping_interval_seconds": 180,
                "ping_timeout_seconds": 600,
                "silence_timeout_seconds": 30,
                "queue_max_events": 1000,
                "queue_max_bytes": 1000000,
                "shutdown_timeout_seconds": 30,
                "archive": {
                    "root": "data/raw",
                    "zstd_level": 3,
                    "rotation_uncompressed_bytes": 1000000,
                    "max_open_writers": 64,
                    "group_commit_max_events": 250,
                    "group_commit_max_milliseconds": 250,
                    "warning_free_bytes": 300,
                    "halt_free_bytes": 200,
                    "recovery_free_bytes": 100,
                    "disk_check_interval_seconds": 10,
                    "pending_manifest_max_age_seconds": 300,
                },
            }
        )
```

Add imports:

```python
from crypto_momentum_lab.config.models import CaptureConfig
```

- [ ] **Step 2: Run the tests and confirm RED**

Run:

```bash
.venv/bin/python -m pytest tests/unit/config/test_loader.py -v
```

Expected: collection or assertion failure because capture configuration does
not exist.

- [ ] **Step 3: Add dependencies**

Add to production dependencies in `pyproject.toml`:

```toml
  "websockets>=16,<17",
  "zstandard>=0.25,<1",
```

Run:

```bash
.venv/bin/python -m pip install -e ".[dev]"
```

Expected: editable installation succeeds with `websockets` and `zstandard`.

- [ ] **Step 4: Implement immutable configuration models**

Add to `src/crypto_momentum_lab/config/models.py`:

```python
class ArchiveConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    root: Path
    zstd_level: int = Field(ge=1, le=19)
    rotation_uncompressed_bytes: int = Field(gt=0)
    max_open_writers: int = Field(gt=0)
    group_commit_max_events: int = Field(gt=0)
    group_commit_max_milliseconds: int = Field(gt=0)
    warning_free_bytes: int = Field(gt=0)
    halt_free_bytes: int = Field(gt=0)
    recovery_free_bytes: int = Field(gt=0)
    disk_check_interval_seconds: float = Field(gt=0)
    pending_manifest_max_age_seconds: float = Field(gt=0)

    @model_validator(mode="after")
    def validate_disk_thresholds(self) -> "ArchiveConfig":
        if self.warning_free_bytes <= self.halt_free_bytes:
            raise ValueError(
                "warning_free_bytes must be greater than halt_free_bytes"
            )
        if self.recovery_free_bytes <= self.halt_free_bytes:
            raise ValueError(
                "recovery_free_bytes must be greater than halt_free_bytes"
            )
        return self


class CaptureConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    market_websocket_url: HttpUrl
    public_websocket_url: HttpUrl
    enabled_streams: tuple[str, ...]
    max_subscriptions_per_connection: int = Field(gt=0, le=100)
    control_messages_per_second: float = Field(gt=0, le=5)
    connection_lifetime_seconds: float = Field(gt=0, lt=86400)
    open_timeout_seconds: float = Field(gt=0)
    ping_interval_seconds: float = Field(gt=0)
    ping_timeout_seconds: float = Field(gt=0)
    silence_timeout_seconds: float = Field(gt=0)
    queue_max_events: int = Field(gt=0)
    queue_max_bytes: int = Field(gt=0)
    shutdown_timeout_seconds: float = Field(gt=0)
    archive: ArchiveConfig
```

Extend `EnvironmentFile`:

```python
    capture_config: Path
```

Extend `RuntimeConfig`:

```python
    capture: CaptureConfig
```

Update `load_runtime_config()`:

```python
    capture = CaptureConfig.model_validate(
        _read_yaml(environment.capture_config)
    )
    return RuntimeConfig(
        environment=environment.environment,
        database_url=database_url,
        binance_base_url=environment.binance_base_url,
        universe=universe,
        capture=capture,
    )
```

Include capture in `behavior_hash()`:

```python
        "capture": config.capture.model_dump(mode="json"),
```

- [ ] **Step 5: Add concrete configuration**

Create `configs/capture/binance_usdm.yaml`:

```yaml
market_websocket_url: wss://fstream.binance.com/market/ws
public_websocket_url: wss://fstream.binance.com/public/ws
enabled_streams:
  - aggTrade
  - bookTicker
  - forceOrder
  - markPrice@1s
  - kline_1m
max_subscriptions_per_connection: 100
control_messages_per_second: 5
connection_lifetime_seconds: 82800
open_timeout_seconds: 10
ping_interval_seconds: 180
ping_timeout_seconds: 600
silence_timeout_seconds: 30
queue_max_events: 100000
queue_max_bytes: 268435456
shutdown_timeout_seconds: 30
archive:
  root: data/raw
  zstd_level: 3
  rotation_uncompressed_bytes: 268435456
  max_open_writers: 128
  group_commit_max_events: 250
  group_commit_max_milliseconds: 250
  warning_free_bytes: 107374182400
  halt_free_bytes: 53687091200
  recovery_free_bytes: 80530636800
  disk_check_interval_seconds: 10
  pending_manifest_max_age_seconds: 300
```

Add to `configs/environments/research.yaml`:

```yaml
capture_config: configs/capture/binance_usdm.yaml
```

- [ ] **Step 6: Verify and commit**

Run:

```bash
.venv/bin/python -m pytest tests/unit/config/test_loader.py -v
.venv/bin/ruff check src/crypto_momentum_lab/config tests/unit/config
.venv/bin/mypy src/crypto_momentum_lab/config
```

Expected: all commands exit with status 0.

Commit:

```bash
git add pyproject.toml configs src/crypto_momentum_lab/config \
  tests/unit/config
git commit -m "feat: configure websocket capture"
```

### Task 2: Raw Capture Domain Types and State Machine

**Files:**
- Create: `src/crypto_momentum_lab/domain/market/__init__.py`
- Create: `src/crypto_momentum_lab/domain/market/models.py`
- Create: `src/crypto_momentum_lab/domain/market/ports.py`
- Create: `tests/unit/domain/market/test_models.py`

- [ ] **Step 1: Write failing domain tests**

Create `tests/unit/domain/market/test_models.py`:

```python
from datetime import UTC, datetime
from uuid import UUID

import pytest

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    MarketDataState,
    RawEnvelope,
    transition_market_data_state,
)


def test_raw_envelope_requires_aware_receive_time() -> None:
    with pytest.raises(ValueError, match="received_at"):
        RawEnvelope(
            schema_version=1,
            exchange="binance-usdm",
            environment="research",
            route=CaptureRoute.MARKET,
            stream=CaptureStream.AGG_TRADE,
            symbol="BTCUSDT",
            exchange_event_at=None,
            received_at=datetime(2026, 6, 15, 2, 0),
            received_monotonic_ns=1,
            connection_session_id=UUID(int=1),
            local_sequence=1,
            exchange_sequence="42",
            subscription_generation=1,
            raw_payload={"e": "aggTrade"},
        )


def test_halted_state_requires_explicit_recovery() -> None:
    assert transition_market_data_state(
        MarketDataState.READY,
        MarketDataState.HALTED,
    ) is MarketDataState.HALTED
    with pytest.raises(ValueError, match="HALTED"):
        transition_market_data_state(
            MarketDataState.HALTED,
            MarketDataState.READY,
        )
    assert transition_market_data_state(
        MarketDataState.HALTED,
        MarketDataState.SYNCING,
        recovery=True,
    ) is MarketDataState.SYNCING
```

- [ ] **Step 2: Run the tests and confirm RED**

Run:

```bash
.venv/bin/python -m pytest tests/unit/domain/market/test_models.py -v
```

Expected: import failure because `domain.market` does not exist.

- [ ] **Step 3: Implement domain models**

Create `src/crypto_momentum_lab/domain/market/models.py`:

```python
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from typing import TypeAlias
from uuid import UUID


JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = (
    JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
)


class CaptureRoute(StrEnum):
    MARKET = "market"
    PUBLIC = "public"


class CaptureStream(StrEnum):
    AGG_TRADE = "aggTrade"
    BOOK_TICKER = "bookTicker"
    FORCE_ORDER = "forceOrder"
    MARK_PRICE = "markPrice@1s"
    KLINE_1M = "kline_1m"


class MarketDataState(StrEnum):
    STARTING = "starting"
    SYNCING = "syncing"
    READY = "ready"
    DEGRADED = "degraded"
    HALTED = "halted"
    STOPPED = "stopped"


class QualityCategory(StrEnum):
    CONNECTION_OPENED = "connection_opened"
    CONNECTION_CLOSED = "connection_closed"
    RECONNECT_GAP = "reconnect_gap"
    ACK_FAILURE = "ack_failure"
    UNEXPECTED_STREAM = "unexpected_stream"
    DUPLICATE = "duplicate"
    SEQUENCE_GAP = "sequence_gap"
    EVENT_TIME_REGRESSION = "event_time_regression"
    SILENCE = "silence"
    MALFORMED_PAYLOAD = "malformed_payload"
    QUEUE_HIGH_WATERMARK = "queue_high_watermark"
    QUEUE_OVERFLOW = "queue_overflow"
    ARCHIVE_FAILURE = "archive_failure"
    MANIFEST_BACKLOG = "manifest_backlog"
    DISK_THRESHOLD = "disk_threshold"


@dataclass(frozen=True, slots=True)
class ConnectionLifecycleEvent:
    session_id: UUID
    route: CaptureRoute
    stream: CaptureStream
    symbols: tuple[str, ...]
    occurred_at: datetime
    opened: bool
    reason: str | None


@dataclass(frozen=True, slots=True)
class RawEnvelope:
    schema_version: int
    exchange: str
    environment: str
    route: CaptureRoute
    stream: CaptureStream
    symbol: str | None
    exchange_event_at: datetime | None
    received_at: datetime
    received_monotonic_ns: int
    connection_session_id: UUID
    local_sequence: int
    exchange_sequence: str | None
    subscription_generation: int
    raw_payload: JsonValue

    def __post_init__(self) -> None:
        if self.received_at.tzinfo is None:
            raise ValueError("received_at must be timezone-aware")
        if (
            self.exchange_event_at is not None
            and self.exchange_event_at.tzinfo is None
        ):
            raise ValueError("exchange_event_at must be timezone-aware")
        if self.local_sequence <= 0:
            raise ValueError("local_sequence must be positive")
        if self.subscription_generation <= 0:
            raise ValueError("subscription_generation must be positive")


@dataclass(frozen=True, slots=True)
class ArchiveManifest:
    manifest_id: UUID
    schema_version: int
    exchange: str
    environment: str
    route: CaptureRoute
    stream: CaptureStream
    symbol: str | None
    utc_date: date
    utc_hour: int
    relative_path: Path
    connection_session_id: UUID
    subscription_generation_min: int
    subscription_generation_max: int
    row_count: int
    compressed_bytes: int
    first_exchange_event_at: datetime | None
    last_exchange_event_at: datetime | None
    first_received_at: datetime
    last_received_at: datetime
    sha256: str
    capture_version: str
    recovery_status: str
    known_gap_count: int
    created_at: datetime


@dataclass(frozen=True, slots=True)
class DurableArchiveAcknowledgement:
    connection_session_id: UUID
    local_sequence: int
    relative_path: Path
    committed_at: datetime


@dataclass(frozen=True, slots=True)
class QualityEvent:
    event_id: UUID
    category: QualityCategory
    occurred_at: datetime
    route: CaptureRoute | None
    stream: CaptureStream | None
    symbol: str | None
    connection_session_id: UUID | None
    local_sequence: int | None
    details: dict[str, JsonValue]


def transition_market_data_state(
    current: MarketDataState,
    target: MarketDataState,
    *,
    recovery: bool = False,
) -> MarketDataState:
    if current is MarketDataState.HALTED:
        if not recovery or target is not MarketDataState.SYNCING:
            raise ValueError("HALTED state requires explicit recovery to SYNCING")
    if current is MarketDataState.STOPPED:
        raise ValueError("STOPPED state is terminal")
    return target
```

Create `src/crypto_momentum_lab/domain/market/ports.py`:

```python
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Protocol

from crypto_momentum_lab.domain.market.models import (
    ArchiveManifest,
    DurableArchiveAcknowledgement,
    MarketDataState,
    QualityEvent,
    RawEnvelope,
)


class RawArchive(Protocol):
    async def append(
        self,
        envelope: RawEnvelope,
    ) -> DurableArchiveAcknowledgement:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError


class CaptureRepository(Protocol):
    async def save_manifest(self, manifest: ArchiveManifest) -> None:
        raise NotImplementedError

    async def save_quality_event(self, event: QualityEvent) -> None:
        raise NotImplementedError

    async def save_process_state(
        self,
        *,
        state: MarketDataState,
        occurred_at: datetime,
        reason: str | None,
    ) -> None:
        raise NotImplementedError


ArchiveAcknowledgementSink = Callable[
    [DurableArchiveAcknowledgement],
    Awaitable[None],
]
ArchiveManifestSink = Callable[[ArchiveManifest], Awaitable[None]]
```

Export public types from `domain/market/__init__.py`.

- [ ] **Step 4: Verify and commit**

Run:

```bash
.venv/bin/python -m pytest tests/unit/domain/market -v
.venv/bin/ruff check src/crypto_momentum_lab/domain/market \
  tests/unit/domain/market
.venv/bin/mypy src/crypto_momentum_lab/domain/market
```

Expected: all commands pass.

Commit:

```bash
git add src/crypto_momentum_lab/domain/market tests/unit/domain/market
git commit -m "feat: define raw capture domain"
```

### Task 3: Subscription Planning and Connection Grouping

**Files:**
- Create: `src/crypto_momentum_lab/market_data/capture/__init__.py`
- Create: `src/crypto_momentum_lab/market_data/capture/subscriptions.py`
- Create: `tests/unit/market_data/capture/test_subscriptions.py`

- [ ] **Step 1: Write failing subscription tests**

Create `tests/unit/market_data/capture/test_subscriptions.py`:

```python
from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
)
from crypto_momentum_lab.market_data.capture.subscriptions import (
    Subscription,
    build_subscription_groups,
    plan_subscription_change,
)


def test_stream_routes_and_names_follow_binance_contract() -> None:
    agg = Subscription.for_symbol(CaptureStream.AGG_TRADE, "BTCUSDT")
    book = Subscription.for_symbol(CaptureStream.BOOK_TICKER, "BTCUSDT")

    assert agg.route is CaptureRoute.MARKET
    assert agg.binance_name == "btcusdt@aggTrade"
    assert book.route is CaptureRoute.PUBLIC
    assert book.binance_name == "btcusdt@bookTicker"


def test_groups_are_stable_and_capped() -> None:
    subscriptions = frozenset(
        Subscription.for_symbol(CaptureStream.AGG_TRADE, f"S{i:03d}USDT")
        for i in range(205)
    )

    groups = build_subscription_groups(
        subscriptions,
        max_per_connection=100,
    )

    assert [len(group.subscriptions) for group in groups] == [100, 100, 5]
    assert groups == tuple(sorted(groups, key=lambda item: item.group_id))


def test_change_plan_adds_before_removing() -> None:
    old = frozenset(
        {
            Subscription.for_symbol(
                CaptureStream.AGG_TRADE,
                "BTCUSDT",
            ),
        }
    )
    new = frozenset(
        {
            Subscription.for_symbol(
                CaptureStream.AGG_TRADE,
                "ETHUSDT",
            ),
        }
    )

    plan = plan_subscription_change(old, new, generation=2)

    assert [step.method for step in plan.steps] == [
        "SUBSCRIBE",
        "UNSUBSCRIBE",
    ]
```

- [ ] **Step 2: Confirm RED**

Run:

```bash
.venv/bin/python -m pytest \
  tests/unit/market_data/capture/test_subscriptions.py -v
```

Expected: import failure.

- [ ] **Step 3: Implement deterministic subscription planning**

Create `subscriptions.py` with:

```python
from dataclasses import dataclass
from hashlib import sha256

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
)


_ROUTES = {
    CaptureStream.AGG_TRADE: CaptureRoute.MARKET,
    CaptureStream.BOOK_TICKER: CaptureRoute.PUBLIC,
    CaptureStream.FORCE_ORDER: CaptureRoute.MARKET,
    CaptureStream.MARK_PRICE: CaptureRoute.MARKET,
    CaptureStream.KLINE_1M: CaptureRoute.MARKET,
}


@dataclass(frozen=True, slots=True, order=True)
class Subscription:
    route: CaptureRoute
    stream: CaptureStream
    symbol: str
    binance_name: str

    @classmethod
    def for_symbol(
        cls,
        stream: CaptureStream,
        symbol: str,
    ) -> "Subscription":
        normalized = symbol.upper()
        return cls(
            route=_ROUTES[stream],
            stream=stream,
            symbol=normalized,
            binance_name=f"{normalized.lower()}@{stream.value}",
        )


@dataclass(frozen=True, slots=True)
class SubscriptionGroup:
    group_id: str
    route: CaptureRoute
    stream: CaptureStream
    subscriptions: tuple[Subscription, ...]


@dataclass(frozen=True, slots=True)
class SubscriptionCommand:
    method: str
    names: tuple[str, ...]
    generation: int


@dataclass(frozen=True, slots=True)
class SubscriptionChangePlan:
    generation: int
    desired: frozenset[Subscription]
    steps: tuple[SubscriptionCommand, ...]


def build_subscription_groups(
    subscriptions: frozenset[Subscription],
    *,
    max_per_connection: int,
) -> tuple[SubscriptionGroup, ...]:
    buckets: dict[
        tuple[CaptureRoute, CaptureStream],
        list[Subscription],
    ] = {}
    for item in sorted(subscriptions):
        buckets.setdefault((item.route, item.stream), []).append(item)

    groups: list[SubscriptionGroup] = []
    for (route, stream), items in sorted(
        buckets.items(),
        key=lambda item: (item[0][0].value, item[0][1].value),
    ):
        for offset in range(0, len(items), max_per_connection):
            chunk = tuple(items[offset : offset + max_per_connection])
            digest = sha256(
                "\n".join(item.binance_name for item in chunk).encode()
            ).hexdigest()[:12]
            groups.append(
                SubscriptionGroup(
                    group_id=f"{route.value}:{stream.value}:{digest}",
                    route=route,
                    stream=stream,
                    subscriptions=chunk,
                )
            )
    return tuple(sorted(groups, key=lambda item: item.group_id))


def plan_subscription_change(
    active: frozenset[Subscription],
    desired: frozenset[Subscription],
    *,
    generation: int,
) -> SubscriptionChangePlan:
    additions = tuple(sorted(desired - active))
    removals = tuple(sorted(active - desired))
    steps = []
    if additions:
        steps.append(
            SubscriptionCommand(
                "SUBSCRIBE",
                tuple(item.binance_name for item in additions),
                generation,
            )
        )
    if removals:
        steps.append(
            SubscriptionCommand(
                "UNSUBSCRIBE",
                tuple(item.binance_name for item in removals),
                generation,
            )
        )
    return SubscriptionChangePlan(generation, desired, tuple(steps))
```

- [ ] **Step 4: Verify and commit**

Run:

```bash
.venv/bin/python -m pytest tests/unit/market_data/capture -v
.venv/bin/ruff check src/crypto_momentum_lab/market_data/capture \
  tests/unit/market_data/capture
.venv/bin/mypy src/crypto_momentum_lab/market_data/capture
```

Commit:

```bash
git add src/crypto_momentum_lab/market_data/capture \
  tests/unit/market_data/capture
git commit -m "feat: plan dynamic market subscriptions"
```

### Task 4: Bounded Queue and Fail-Closed State

**Files:**
- Create: `src/crypto_momentum_lab/market_data/capture/queue.py`
- Create: `tests/unit/market_data/capture/test_queue.py`

- [ ] **Step 1: Write failing queue tests**

Create `tests/unit/market_data/capture/test_queue.py`:

```python
import json

import pytest

from crypto_momentum_lab.market_data.capture.queue import (
    BoundedEnvelopeQueue,
    CaptureQueueFull,
)


async def test_queue_enforces_event_and_byte_limits(raw_envelope) -> None:
    queue = BoundedEnvelopeQueue(max_events=1, max_bytes=100000)
    await queue.put_nowait(raw_envelope)

    with pytest.raises(CaptureQueueFull):
        await queue.put_nowait(raw_envelope)

    item = await queue.get()
    assert item == raw_envelope
    queue.task_done(item)
    assert queue.current_bytes == 0


async def test_queue_rejects_single_oversized_envelope(raw_envelope) -> None:
    size = len(
        json.dumps(
            raw_envelope.raw_payload,
            separators=(",", ":"),
        ).encode()
    )
    queue = BoundedEnvelopeQueue(max_events=10, max_bytes=size - 1)

    with pytest.raises(CaptureQueueFull, match="byte limit"):
        await queue.put_nowait(raw_envelope)
```

Add a reusable `raw_envelope` fixture to `tests/conftest.py`.

- [ ] **Step 2: Confirm RED**

Run:

```bash
.venv/bin/python -m pytest \
  tests/unit/market_data/capture/test_queue.py -v
```

Expected: missing queue implementation.

- [ ] **Step 3: Implement queue accounting**

Create `queue.py`:

```python
import asyncio
import json

from crypto_momentum_lab.domain.market.models import RawEnvelope


class CaptureQueueFull(RuntimeError):
    pass


class BoundedEnvelopeQueue:
    def __init__(self, *, max_events: int, max_bytes: int) -> None:
        self._queue: asyncio.Queue[tuple[RawEnvelope, int]] = asyncio.Queue(
            maxsize=max_events
        )
        self._max_bytes = max_bytes
        self._current_bytes = 0
        self._lock = asyncio.Lock()

    @property
    def current_bytes(self) -> int:
        return self._current_bytes

    @property
    def size(self) -> int:
        return self._queue.qsize()

    async def put_nowait(self, envelope: RawEnvelope) -> None:
        encoded_size = len(
            json.dumps(
                envelope.raw_payload,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode()
        )
        async with self._lock:
            if encoded_size > self._max_bytes:
                raise CaptureQueueFull("envelope exceeds queue byte limit")
            if self._queue.full():
                raise CaptureQueueFull("queue event limit reached")
            if self._current_bytes + encoded_size > self._max_bytes:
                raise CaptureQueueFull("queue byte limit reached")
            self._queue.put_nowait((envelope, encoded_size))
            self._current_bytes += encoded_size

    async def get(self) -> RawEnvelope:
        envelope, _ = await self._queue.get()
        return envelope

    def task_done(self, envelope: RawEnvelope) -> None:
        encoded_size = len(
            json.dumps(
                envelope.raw_payload,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode()
        )
        self._current_bytes -= encoded_size
        self._queue.task_done()

    async def join(self) -> None:
        await self._queue.join()
```

- [ ] **Step 4: Verify and commit**

Run:

```bash
.venv/bin/python -m pytest tests/unit/market_data/capture -v
.venv/bin/ruff check src/crypto_momentum_lab/market_data/capture \
  tests/unit/market_data/capture
.venv/bin/mypy src/crypto_momentum_lab/market_data/capture
```

Commit:

```bash
git add src/crypto_momentum_lab/market_data/capture/queue.py \
  tests/conftest.py tests/unit/market_data/capture/test_queue.py
git commit -m "feat: add bounded raw event queue"
```

### Task 5: Zstandard JSONL Archive and Durable Group Commit

**Files:**
- Create: `src/crypto_momentum_lab/persistence/raw_files/__init__.py`
- Create: `src/crypto_momentum_lab/persistence/raw_files/archive.py`
- Create: `tests/integration/raw_files/test_archive.py`

- [ ] **Step 1: Write failing archive integration tests**

Create `tests/integration/raw_files/test_archive.py`:

```python
import hashlib
import json
from datetime import UTC, datetime

import zstandard

from crypto_momentum_lab.persistence.raw_files.archive import (
    ZstdJsonlArchive,
)


async def test_archive_commits_rotates_and_checksums(
    tmp_path,
    raw_envelope,
) -> None:
    manifests = []

    async def save_manifest(manifest: ArchiveManifest) -> None:
        manifests.append(manifest)

    archive = ZstdJsonlArchive(
        root=tmp_path,
        environment="test",
        capture_version="test",
        manifest_sink=save_manifest,
        known_gap_count_provider=lambda key: 0,
        zstd_level=1,
        rotation_uncompressed_bytes=10_000_000,
        max_open_writers=4,
        group_commit_max_events=1,
        group_commit_max_milliseconds=10_000,
    )

    acknowledgement = await archive.append(raw_envelope)
    await archive.close()

    assert acknowledgement.local_sequence == 1
    assert len(manifests) == 1
    manifest = manifests[0]
    final_path = tmp_path / manifest.relative_path
    assert final_path.suffixes[-2:] == [".jsonl", ".zst"]
    assert hashlib.sha256(final_path.read_bytes()).hexdigest() == manifest.sha256

    with zstandard.open(final_path, "rt", encoding="utf-8") as stream:
        rows = [json.loads(line) for line in stream]
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["local_sequence"] == 1


async def test_session_change_rotates_file(
    tmp_path,
    raw_envelope,
) -> None:
    manifests = []

    async def save_manifest(manifest: ArchiveManifest) -> None:
        manifests.append(manifest)

    archive = ZstdJsonlArchive(
        root=tmp_path,
        environment="test",
        capture_version="test",
        manifest_sink=save_manifest,
        known_gap_count_provider=lambda key: 0,
        zstd_level=1,
        rotation_uncompressed_bytes=10_000_000,
        max_open_writers=4,
        group_commit_max_events=1,
        group_commit_max_milliseconds=10_000,
    )
    await archive.append(raw_envelope)
    await archive.append(
        replace(
            raw_envelope,
            connection_session_id=UUID(int=2),
            local_sequence=1,
            received_at=datetime(2026, 6, 15, 2, 0, 1, tzinfo=UTC),
        )
    )

    await archive.close()

    assert len(manifests) == 2
    assert {
        item.connection_session_id for item in manifests
    } == {UUID(int=1), UUID(int=2)}
```

Add imports:

```python
from dataclasses import replace
from uuid import UUID
```

- [ ] **Step 2: Confirm RED**

Run:

```bash
.venv/bin/python -m pytest tests/integration/raw_files/test_archive.py -v
```

Expected: archive module missing.

- [ ] **Step 3: Implement serialization and partition keys**

In `archive.py`, define:

```python
@dataclass(frozen=True, slots=True)
class PartitionKey:
    utc_date: date
    utc_hour: int
    route: CaptureRoute
    stream: CaptureStream
    symbol: str
    connection_session_id: UUID


def partition_key(envelope: RawEnvelope) -> PartitionKey:
    received = envelope.received_at.astimezone(UTC)
    return PartitionKey(
        utc_date=received.date(),
        utc_hour=received.hour,
        route=envelope.route,
        stream=envelope.stream,
        symbol=envelope.symbol or "_global",
        connection_session_id=envelope.connection_session_id,
    )


def serialize_envelope(envelope: RawEnvelope) -> bytes:
    payload = {
        "schema_version": envelope.schema_version,
        "exchange": envelope.exchange,
        "environment": envelope.environment,
        "route": envelope.route.value,
        "stream": envelope.stream.value,
        "symbol": envelope.symbol,
        "exchange_event_at": (
            None
            if envelope.exchange_event_at is None
            else envelope.exchange_event_at.isoformat()
        ),
        "received_at": envelope.received_at.isoformat(),
        "received_monotonic_ns": envelope.received_monotonic_ns,
        "connection_session_id": str(envelope.connection_session_id),
        "local_sequence": envelope.local_sequence,
        "exchange_sequence": envelope.exchange_sequence,
        "subscription_generation": envelope.subscription_generation,
        "raw_payload": envelope.raw_payload,
    }
    return (
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        + b"\n"
    )
```

- [ ] **Step 4: Implement writers, grouped `fsync`, rotation and manifests**

Implement `ZstdJsonlArchive.append(envelope)` returning
`DurableArchiveAcknowledgement`. The constructor requires an async
`manifest_sink`. Every rotation awaits that sink after the atomic rename and
directory `fsync`; therefore manifests are persisted during long-running
operation rather than only at shutdown. `close()` finalizes all writers,
delivers their manifests through the same sink, and returns `None`.
`append()` waits for the grouped `fsync` that contains the envelope before
returning. The constructor also requires
`known_gap_count_provider(PartitionKey) -> int`; the finalized manifest records
the provider's current count for that session, stream, and symbol.

Required implementation rules:

```python
# Active file naming
relative_directory = Path(
    "exchange=binance-usdm",
    f"date={key.utc_date.isoformat()}",
    f"stream={key.stream.value}",
    f"symbol={key.symbol}",
    f"hour={key.utc_hour:02d}",
)
base = f"{key.connection_session_id}-{first_sequence:020d}"
temporary = relative_directory / f"{base}.jsonl.zst.tmp"
final = relative_directory / f"{base}.jsonl.zst"
```

Use `zstandard.ZstdCompressor(level=...).stream_writer(raw_file)` and
`zstandard.FLUSH_BLOCK` for group commits. Perform blocking file operations
through `asyncio.to_thread`.

A group commit must:

```python
compressor.flush(zstandard.FLUSH_BLOCK)
raw_file.flush()
os.fsync(raw_file.fileno())
```

Resolve all acknowledgement futures in that batch only after `os.fsync`
returns. Rotate on partition/session change, byte limit, LRU eviction, and
close. Finalization finishes the frame, `fsync`s the file, computes SHA-256,
uses `os.replace`, then `fsync`s the containing directory.

When the first event enters an empty batch, schedule a commit task for
`group_commit_max_milliseconds`. Commit immediately when
`group_commit_max_events` is reached. Cancel the timer after a size-triggered
commit. Protect each partition writer with an `asyncio.Lock` so timer, append,
rotation, and shutdown cannot commit the same batch twice.

Use deterministic `uuid5` manifest IDs derived from final relative path and
checksum.

- [ ] **Step 5: Verify and commit**

Run:

```bash
.venv/bin/python -m pytest tests/integration/raw_files/test_archive.py -v
.venv/bin/ruff check src/crypto_momentum_lab/persistence/raw_files \
  tests/integration/raw_files
.venv/bin/mypy src/crypto_momentum_lab/persistence/raw_files
```

Commit:

```bash
git add src/crypto_momentum_lab/persistence/raw_files \
  tests/integration/raw_files
git commit -m "feat: archive raw websocket events"
```

### Task 6: Capture Persistence Schema and Repository

**Files:**
- Create: `alembic/versions/20260615_0002_capture_runtime.py`
- Modify: `src/crypto_momentum_lab/persistence/postgres/models.py`
- Create: `src/crypto_momentum_lab/persistence/postgres/capture_repository.py`
- Modify: `tests/integration/conftest.py`
- Create: `tests/integration/persistence/test_capture_repository.py`

- [ ] **Step 1: Write failing repository tests**

Create `tests/integration/persistence/test_capture_repository.py`:

```python
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from crypto_momentum_lab.domain.market.models import (
    ArchiveManifest,
    CaptureRoute,
    CaptureStream,
    MarketDataState,
    QualityCategory,
    QualityEvent,
)


async def test_manifest_is_idempotent_but_checksum_conflict_fails(
    capture_repository,
) -> None:
    manifest = fixture_manifest()
    await capture_repository.save_manifest(manifest)
    await capture_repository.save_manifest(manifest)

    with pytest.raises(ValueError, match="checksum conflict"):
        await capture_repository.save_manifest(
            replace(manifest, sha256="b" * 64)
        )


async def test_quality_and_process_state_are_persisted(
    capture_repository,
) -> None:
    at = datetime(2026, 6, 15, 2, 0, tzinfo=UTC)
    event = QualityEvent(
        event_id=UUID(int=10),
        category=QualityCategory.SILENCE,
        occurred_at=at,
        route=CaptureRoute.MARKET,
        stream=CaptureStream.AGG_TRADE,
        symbol="BTCUSDT",
        connection_session_id=UUID(int=1),
        local_sequence=5,
        details={"seconds": 31},
    )
    await capture_repository.save_quality_event(event)
    await capture_repository.save_process_state(
        state=MarketDataState.DEGRADED,
        occurred_at=at,
        reason="silence",
    )

    assert await capture_repository.count_quality_events() == 1
    assert await capture_repository.latest_process_state() is (
        MarketDataState.DEGRADED
    )
```

Define `fixture_manifest()` in the same test module with all required fields.

- [ ] **Step 2: Confirm RED**

Run:

```bash
CML_TEST_ASYNC_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml \
  .venv/bin/python -m pytest \
  tests/integration/persistence/test_capture_repository.py -v
```

Expected: fixture or table failure.

- [ ] **Step 3: Add SQLAlchemy rows and migration**

Add rows:

```python
class RawArchiveManifestRow(Base):
    __tablename__ = "raw_archive_manifests"

    manifest_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
    )
    relative_path: Mapped[str] = mapped_column(Text, unique=True)
    sha256: Mapped[str] = mapped_column(String(64))
    schema_version: Mapped[int] = mapped_column(Integer)
    exchange: Mapped[str] = mapped_column(String(32))
    environment: Mapped[str] = mapped_column(String(32))
    route: Mapped[str] = mapped_column(String(16))
    stream: Mapped[str] = mapped_column(String(32))
    symbol: Mapped[str | None] = mapped_column(String(32))
    utc_date: Mapped[date] = mapped_column(Date)
    utc_hour: Mapped[int] = mapped_column(Integer)
    connection_session_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True))
    subscription_generation_min: Mapped[int] = mapped_column(Integer)
    subscription_generation_max: Mapped[int] = mapped_column(Integer)
    row_count: Mapped[int] = mapped_column(Integer)
    compressed_bytes: Mapped[int] = mapped_column(Integer)
    first_exchange_event_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    last_exchange_event_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    first_received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    capture_version: Mapped[str] = mapped_column(String(64))
    recovery_status: Mapped[str] = mapped_column(String(32))
    known_gap_count: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MarketDataQualityEventRow(Base):
    __tablename__ = "market_data_quality_events"

    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    category: Mapped[str] = mapped_column(String(64))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    route: Mapped[str | None] = mapped_column(String(16))
    stream: Mapped[str | None] = mapped_column(String(32))
    symbol: Mapped[str | None] = mapped_column(String(32))
    connection_session_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True)
    )
    local_sequence: Mapped[int | None] = mapped_column(Integer)
    details: Mapped[dict[str, object]] = mapped_column(JSONB)


class MarketDataProcessStateRow(Base):
    __tablename__ = "market_data_process_states"

    state_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    state: Mapped[str] = mapped_column(String(16))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    reason: Mapped[str | None] = mapped_column(Text)
```

Create migration `20260615_0002_capture_runtime.py` with
`down_revision = "20260614_0001"` and matching create/drop operations.

- [ ] **Step 4: Implement repository**

Implement `PostgresCaptureRepository` using `async_sessionmaker`:

```python
class PostgresCaptureRepository:
    async def save_manifest(self, manifest: ArchiveManifest) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                existing = await session.scalar(
                    select(RawArchiveManifestRow).where(
                        RawArchiveManifestRow.relative_path
                        == str(manifest.relative_path)
                    )
                )
                if existing is not None:
                    if existing.sha256 != manifest.sha256:
                        raise ValueError(
                            "archive manifest checksum conflict"
                        )
                    return
                session.add(_manifest_row(manifest))

    async def save_quality_event(self, event: QualityEvent) -> None:
        statement = insert(MarketDataQualityEventRow).values(
            **_quality_values(event)
        ).on_conflict_do_nothing(index_elements=["event_id"])
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(statement)

    async def save_process_state(
        self,
        *,
        state: MarketDataState,
        occurred_at: datetime,
        reason: str | None,
    ) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                session.add(
                    MarketDataProcessStateRow(
                        state=state.value,
                        occurred_at=occurred_at,
                        reason=reason,
                    )
                )
```

Add test-only query helpers `count_quality_events()` and
`latest_process_state()` to the concrete repository, not the domain protocol.

Extend the root `repository` cleanup fixture in `tests/conftest.py` so capture
tables are deleted before universe tables:

```python
for model in (
    MarketDataQualityEventRow,
    MarketDataProcessStateRow,
    RawArchiveManifestRow,
    MonitoringMembershipRow,
    UniverseEntryRow,
    UniverseSnapshotRow,
    DailyOpenRow,
    ContractMetadataRow,
):
    await session.execute(delete(model))
```

- [ ] **Step 5: Verify migration and repository**

Run:

```bash
CML_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml \
  .venv/bin/alembic upgrade head
CML_TEST_ASYNC_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml \
  .venv/bin/python -m pytest \
  tests/integration/persistence/test_capture_repository.py \
  tests/integration/persistence/test_migrations.py -v
CML_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml \
  .venv/bin/alembic downgrade 20260614_0001
CML_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml \
  .venv/bin/alembic upgrade head
```

Expected: tests pass and migration downgrade/upgrade succeeds.

- [ ] **Step 6: Static checks and commit**

Run:

```bash
.venv/bin/ruff check alembic src/crypto_momentum_lab/persistence \
  tests/integration
.venv/bin/mypy src/crypto_momentum_lab/persistence
```

Commit:

```bash
git add alembic src/crypto_momentum_lab/persistence \
  tests/integration
git commit -m "feat: persist capture manifests and quality"
```

### Task 7: Pending Manifest Journal and Archive Recovery

**Files:**
- Create: `src/crypto_momentum_lab/persistence/raw_files/journal.py`
- Create: `src/crypto_momentum_lab/persistence/raw_files/recovery.py`
- Create: `tests/integration/raw_files/test_journal.py`
- Create: `tests/integration/raw_files/test_recovery.py`

- [ ] **Step 1: Write failing journal tests**

```python
async def test_journal_replays_manifests_in_order(
    tmp_path,
    fixture_manifests,
) -> None:
    journal = PendingManifestJournal(tmp_path / "pending")
    for manifest in fixture_manifests:
        await journal.append(manifest)

    saved = []
    await journal.replay(saved.append)

    assert saved == list(fixture_manifests)
    assert await journal.oldest_age_seconds(now=fixture_now) is None


async def test_process_state_journal_replays_critical_transition(
    tmp_path,
) -> None:
    journal = PendingProcessStateJournal(tmp_path / "pending-state")
    record = PendingProcessState(
        state=MarketDataState.HALTED,
        occurred_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        reason="archive failure",
    )
    await journal.append(record)
    saved = []

    async def save(item: PendingProcessState) -> None:
        saved.append(item)

    await journal.replay(save)

    assert saved == [record]
```

Use an async `save()` function rather than `list.append` in final test:

```python
    async def save(manifest):
        saved.append(manifest)
```

- [ ] **Step 2: Write failing recovery test**

```python
async def test_recovery_preserves_complete_records_and_quarantines_source(
    tmp_path,
    raw_envelope,
) -> None:
    temporary = write_truncated_archive(tmp_path, raw_envelope)

    result = await recover_temporary_archive(
        temporary,
        archive_root=tmp_path,
        environment="test",
        capture_version="test",
    )

    assert result.manifest.row_count == 1
    assert result.discarded_bytes > 0
    assert result.quarantined_path.exists()
    assert not temporary.exists()
```

- [ ] **Step 3: Confirm RED**

Run:

```bash
.venv/bin/python -m pytest \
  tests/integration/raw_files/test_journal.py \
  tests/integration/raw_files/test_recovery.py -v
```

- [ ] **Step 4: Implement durable journal**

Use one JSON file per manifest under:

```text
data/raw/.pending-manifests/<created-at>-<manifest-id>.json
```

`append()` writes a temporary JSON file, flushes and `fsync`s it, atomically
renames it, and `fsync`s the directory. `replay()` processes files in lexical
order, awaits the supplied save callback, then deletes and directory-`fsync`s
each acknowledged journal entry.

Expose:

```python
class PendingManifestJournal:
    async def append(self, manifest: ArchiveManifest) -> None:
        raise NotImplementedError

    async def replay(
        self,
        save: Callable[[ArchiveManifest], Awaitable[None]],
    ) -> int:
        raise NotImplementedError

    async def oldest_age_seconds(
        self,
        *,
        now: datetime,
    ) -> float | None:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class PendingProcessState:
    state: MarketDataState
    occurred_at: datetime
    reason: str | None


class PendingProcessStateJournal:
    async def append(self, record: PendingProcessState) -> None:
        raise NotImplementedError

    async def replay(
        self,
        save: Callable[[PendingProcessState], Awaitable[None]],
    ) -> int:
        raise NotImplementedError
```

The process-state journal uses
`data/raw/.pending-process-state/<occurred-at>-<state>.json` and the same
temporary-write, file-`fsync`, atomic-rename, directory-`fsync`, ordered replay,
and delete-after-acknowledgement rules as the manifest journal.

- [ ] **Step 5: Implement recovery**

Recovery must:

1. stream-decompress all complete Zstandard frames;
2. parse complete JSONL records;
3. write recovered records through a fresh `ZstdJsonlArchive`;
4. finalize a manifest with `recovery_status="recovered"`;
5. atomically move the original temporary file under
   `.recovery-quarantine/`;
6. return discarded compressed-byte count.

Define:

```python
@dataclass(frozen=True, slots=True)
class RecoveryResult:
    manifest: ArchiveManifest
    quarantined_path: Path
    discarded_bytes: int


async def recover_archive_root(
    root: Path,
    *,
    environment: str,
    capture_version: str,
) -> tuple[RecoveryResult, ...]:
    raise NotImplementedError
```

- [ ] **Step 6: Verify and commit**

Run:

```bash
.venv/bin/python -m pytest tests/integration/raw_files -v
.venv/bin/ruff check src/crypto_momentum_lab/persistence/raw_files \
  tests/integration/raw_files
.venv/bin/mypy src/crypto_momentum_lab/persistence/raw_files
```

Commit:

```bash
git add src/crypto_momentum_lab/persistence/raw_files \
  tests/integration/raw_files
git commit -m "feat: recover raw archives and manifests"
```

### Task 8: Stream Quality Tracker

**Files:**
- Create: `src/crypto_momentum_lab/market_data/quality/__init__.py`
- Create: `src/crypto_momentum_lab/market_data/quality/tracker.py`
- Create: `tests/unit/market_data/quality/test_tracker.py`

- [ ] **Step 1: Write failing quality tests**

Create tests:

```python
def test_duplicate_exchange_sequence_is_recorded(raw_envelope) -> None:
    tracker = StreamQualityTracker()

    assert tracker.observe(raw_envelope) == ()
    events = tracker.observe(raw_envelope)

    assert [event.category for event in events] == [
        QualityCategory.DUPLICATE
    ]


def test_numeric_sequence_gap_is_recorded(raw_envelope) -> None:
    tracker = StreamQualityTracker()
    tracker.observe(replace(raw_envelope, exchange_sequence="10"))

    events = tracker.observe(
        replace(
            raw_envelope,
            local_sequence=2,
            exchange_sequence="12",
        )
    )

    assert events[0].category is QualityCategory.SEQUENCE_GAP
    assert events[0].details == {"previous": "10", "current": "12"}


def test_event_time_regression_and_silence_are_recorded(
    raw_envelope,
) -> None:
    tracker = StreamQualityTracker(silence_timeout_seconds=30)
    tracker.observe(raw_envelope)

    regression = tracker.observe(
        replace(
            raw_envelope,
            local_sequence=2,
            exchange_event_at=raw_envelope.exchange_event_at
            - timedelta(seconds=1),
        )
    )
    silence = tracker.check_silence(
        now=raw_envelope.received_at + timedelta(seconds=31)
    )

    assert regression[0].category is QualityCategory.EVENT_TIME_REGRESSION
    assert silence[0].category is QualityCategory.SILENCE
```

- [ ] **Step 2: Confirm RED**

Run:

```bash
.venv/bin/python -m pytest tests/unit/market_data/quality -v
```

- [ ] **Step 3: Implement tracker**

Track state by `(session_id, stream, symbol)`:

```python
@dataclass(slots=True)
class _ObservedState:
    last_exchange_sequence: str | None
    last_exchange_event_at: datetime | None
    last_received_at: datetime
    silence_reported: bool = False
```

`observe()` emits immutable `QualityEvent` instances with deterministic UUID5
IDs derived from category, session, local sequence, and symbol.

Only compute an exact contiguous sequence gap for `aggTrade`, whose aggregate
trade ID is the phase's exact sequence source. For `bookTicker`, retain update
ID `u` as metadata but do not claim that skipped values are message loss.
Repeated `kline.t` values are expected while the same minute is still open and
must not be classified as duplicates.

For `aggTrade`, equal identifiers with identical payloads create a duplicate
event; a current ID greater than previous + 1 creates a sequence-gap event.
Lower identifiers create a regression detail but never a positive loss count.
For streams without exact contiguous identifiers, detect only byte-identical
consecutive payloads, event-time regression, silence, reconnect intervals, and
acknowledgement anomalies.

`check_silence(now=...)` emits at most one silence event until a new message
clears `silence_reported`.

Expose:

```python
def known_gap_count(
    self,
    *,
    connection_session_id: UUID,
    stream: CaptureStream,
    symbol: str,
) -> int:
    return self._known_gap_counts.get(
        (connection_session_id, stream, symbol),
        0,
    )
```

Increment this count for exact `aggTrade` sequence gaps and reconnect gap
events. Do not increment it for duplicates, event-time regressions, or silence
because those conditions do not prove a missing-message count.

- [ ] **Step 4: Verify and commit**

Run:

```bash
.venv/bin/python -m pytest tests/unit/market_data/quality -v
.venv/bin/ruff check src/crypto_momentum_lab/market_data/quality \
  tests/unit/market_data/quality
.venv/bin/mypy src/crypto_momentum_lab/market_data/quality
```

Commit:

```bash
git add src/crypto_momentum_lab/market_data/quality \
  tests/unit/market_data/quality
git commit -m "feat: track websocket data quality"
```

### Task 9: Binance WebSocket Envelope Adapter

**Files:**
- Create: `src/crypto_momentum_lab/market_data/binance/websocket.py`
- Create: `tests/unit/market_data/binance/test_websocket.py`

- [ ] **Step 1: Write failing adapter tests**

Create tests for all five payloads:

```python
@pytest.mark.parametrize(
    ("stream_name", "payload", "expected_stream", "expected_symbol"),
    [
        (
            "btcusdt@aggTrade",
            {"e": "aggTrade", "E": 1781488800000, "s": "BTCUSDT", "a": 42},
            CaptureStream.AGG_TRADE,
            "BTCUSDT",
        ),
        (
            "btcusdt@bookTicker",
            {"e": "bookTicker", "E": 1781488800000, "s": "BTCUSDT", "u": 7},
            CaptureStream.BOOK_TICKER,
            "BTCUSDT",
        ),
        (
            "btcusdt@forceOrder",
            {
                "e": "forceOrder",
                "E": 1781488800000,
                "o": {"s": "BTCUSDT", "T": 1781488799000},
            },
            CaptureStream.FORCE_ORDER,
            "BTCUSDT",
        ),
        (
            "btcusdt@markPrice@1s",
            {"e": "markPriceUpdate", "E": 1781488800000, "s": "BTCUSDT"},
            CaptureStream.MARK_PRICE,
            "BTCUSDT",
        ),
        (
            "btcusdt@kline_1m",
            {
                "e": "kline",
                "E": 1781488800000,
                "s": "BTCUSDT",
                "k": {"t": 1781488800000},
            },
            CaptureStream.KLINE_1M,
            "BTCUSDT",
        ),
    ],
)
def test_parses_combined_stream_payloads(
    stream_name,
    payload,
    expected_stream,
    expected_symbol,
) -> None:
    envelope = parse_binance_message(
        route=route_for(expected_stream),
        message=json.dumps({"stream": stream_name, "data": payload}),
        environment="test",
        connection_session_id=UUID(int=1),
        local_sequence=1,
        subscription_generation=3,
        received_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        received_monotonic_ns=10,
    )

    assert envelope.stream is expected_stream
    assert envelope.symbol == expected_symbol
    assert envelope.exchange_event_at is not None
```

Add a malformed payload test that expects `BinancePayloadError`.

- [ ] **Step 2: Confirm RED**

Run:

```bash
.venv/bin/python -m pytest \
  tests/unit/market_data/binance/test_websocket.py -v
```

- [ ] **Step 3: Implement route-aware parsing**

Define:

```python
class BinancePayloadError(ValueError):
    pass


def route_for(stream: CaptureStream) -> CaptureRoute:
    return (
        CaptureRoute.PUBLIC
        if stream is CaptureStream.BOOK_TICKER
        else CaptureRoute.MARKET
    )


def parse_binance_message(
    *,
    route: CaptureRoute,
    message: str | bytes,
    environment: str,
    connection_session_id: UUID,
    local_sequence: int,
    subscription_generation: int,
    received_at: datetime,
    received_monotonic_ns: int,
) -> RawEnvelope:
    decoded = json.loads(message)
    if not isinstance(decoded, dict):
        raise BinancePayloadError("message must be an object")
    stream_name = decoded.get("stream")
    payload = decoded.get("data")
    if not isinstance(stream_name, str) or not isinstance(payload, dict):
        raise BinancePayloadError("combined stream envelope is invalid")
    stream = _stream_from_name(stream_name)
    if route_for(stream) is not route:
        raise BinancePayloadError("stream arrived on unexpected route")
    symbol = _symbol(payload)
    event_ms = _event_time_ms(payload)
    exchange_sequence = _exchange_sequence(stream, payload)
    return RawEnvelope(
        schema_version=1,
        exchange="binance-usdm",
        environment=environment,
        route=route,
        stream=stream,
        symbol=symbol,
        exchange_event_at=(
            None if event_ms is None else _utc_from_ms(event_ms)
        ),
        received_at=received_at,
        received_monotonic_ns=received_monotonic_ns,
        connection_session_id=connection_session_id,
        local_sequence=local_sequence,
        exchange_sequence=exchange_sequence,
        subscription_generation=subscription_generation,
        raw_payload=payload,
    )
```

Use aggregate trade ID `a`, book ticker update ID `u`, kline start time `k.t`,
and no exact sequence for mark price or force order.

- [ ] **Step 4: Verify and commit**

Run:

```bash
.venv/bin/python -m pytest tests/unit/market_data/binance -v
.venv/bin/ruff check src/crypto_momentum_lab/market_data/binance \
  tests/unit/market_data/binance
.venv/bin/mypy src/crypto_momentum_lab/market_data/binance
```

Commit:

```bash
git add src/crypto_momentum_lab/market_data/binance \
  tests/unit/market_data/binance
git commit -m "feat: parse Binance websocket payloads"
```

### Task 10: WebSocket Connection and Connection Pool

**Files:**
- Modify: `src/crypto_momentum_lab/market_data/binance/websocket.py`
- Create: `src/crypto_momentum_lab/market_data/binance/connection_pool.py`
- Create: `tests/unit/market_data/binance/test_connection_pool.py`
- Create: `tests/e2e/fake_binance_websocket.py`
- Create: `tests/e2e/test_websocket_connection.py`

- [ ] **Step 1: Write connection-pool unit tests**

Test that:

```python
async def test_pool_applies_additions_before_removals(fake_connection) -> None:
    pool = BinanceConnectionPool(
        connection_factory=lambda group: fake_connection,
        max_subscriptions_per_connection=100,
        control_messages_per_second=5,
    )
    await pool.apply_symbols(
        frozenset({"BTCUSDT"}),
        streams=(CaptureStream.AGG_TRADE,),
        generation=1,
    )
    await pool.apply_symbols(
        frozenset({"ETHUSDT"}),
        streams=(CaptureStream.AGG_TRADE,),
        generation=2,
    )

    assert fake_connection.methods == ["SUBSCRIBE", "SUBSCRIBE", "UNSUBSCRIBE"]


def test_connection_is_replaced_before_lifetime() -> None:
    assert should_replace_connection(
        opened_at=100.0,
        now=82900.0,
        lifetime_seconds=82800.0,
    )
```

- [ ] **Step 2: Write local WebSocket end-to-end test**

The fake server must:

- accept `/market/ws` and `/public/ws`;
- acknowledge `SUBSCRIBE` and `UNSUBSCRIBE`;
- emit a combined `aggTrade` payload;
- close the first connection after one message;
- accept the reconnect and another full subscription set.

Test:

```python
@pytest.mark.e2e
async def test_connection_reconnects_with_new_session_and_full_set(
    fake_binance_server,
) -> None:
    received = []
    lifecycle = []

    async def receive(envelope: RawEnvelope) -> None:
        received.append(envelope)

    async def observe(event: ConnectionLifecycleEvent) -> None:
        lifecycle.append(event)

    connection = BinanceWebSocketConnection(
        base_url=fake_binance_server.market_url,
        route=CaptureRoute.MARKET,
        environment="test",
        desired_names=("btcusdt@aggTrade",),
        generation=1,
        on_envelope=receive,
        on_lifecycle=observe,
        reconnect_delays=(0.0, 0.0),
        connection_lifetime_seconds=60,
        open_timeout_seconds=1,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        silence_timeout_seconds=2,
    )

    task = asyncio.create_task(connection.run())
    await fake_binance_server.wait_for_connections(2)
    await connection.stop()
    await task

    assert len({event.session_id for event in lifecycle if event.opened}) == 2
    assert fake_binance_server.subscribe_requests == [
        ("btcusdt@aggTrade",),
        ("btcusdt@aggTrade",),
    ]
```

- [ ] **Step 3: Confirm RED**

Run:

```bash
.venv/bin/python -m pytest \
  tests/unit/market_data/binance/test_connection_pool.py \
  tests/e2e/test_websocket_connection.py -v
```

- [ ] **Step 4: Implement the connection**

Use the current asyncio API:

```python
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed
```

Connect to the configured route URL exactly:

```python
uri = base_url.rstrip("/")
```

Disable library automatic reconnect because the application must assign
session IDs and quality intervals itself. Set `max_queue` to a small bounded
frame buffer and keep the application queue authoritative.

On each connection:

1. create a UUID session ID;
2. send the complete desired subscription set through the rate limiter;
3. wait for acknowledgement;
4. receive exactly one message at a time;
5. capture `datetime.now(UTC)` and `time.monotonic_ns()` before parsing;
6. increment local sequence only for market payloads, not acknowledgements;
7. reconnect on `ConnectionClosed`, timeout, or proactive lifetime expiry;
8. emit lifecycle events with disconnect start/end.

Control messages:

```python
{
    "method": "SUBSCRIBE",
    "params": ["btcusdt@aggTrade"],
    "id": 1,
}
```

Use a token interval of `1 / control_messages_per_second` and a single send
lock per connection.

- [ ] **Step 5: Implement connection pool reconciliation**

`BinanceConnectionPool.apply_symbols()` builds all stream subscriptions,
groups them deterministically, starts missing groups, synchronizes additions,
then retires obsolete groups.

Expose:

```python
class BinanceConnectionPool:
    async def start(self) -> None:
        raise NotImplementedError

    async def apply_symbols(
        self,
        symbols: frozenset[str],
        *,
        streams: tuple[CaptureStream, ...],
        generation: int,
    ) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError
```

If a replacement connection does not reach synchronized state, retain the old
connection and report degradation.

- [ ] **Step 6: Verify and commit**

Run:

```bash
.venv/bin/python -m pytest \
  tests/unit/market_data/binance \
  tests/e2e/test_websocket_connection.py -v
.venv/bin/ruff check src/crypto_momentum_lab/market_data/binance \
  tests/unit/market_data/binance tests/e2e
.venv/bin/mypy src/crypto_momentum_lab/market_data/binance
```

Commit:

```bash
git add src/crypto_momentum_lab/market_data/binance \
  tests/unit/market_data/binance tests/e2e
git commit -m "feat: manage Binance websocket connections"
```

### Task 11: Capture Coordinator, Disk Safety, and Halt Semantics

**Files:**
- Create: `src/crypto_momentum_lab/market_data/capture/coordinator.py`
- Create: `src/crypto_momentum_lab/market_data/capture/service.py`
- Create: `tests/unit/market_data/capture/test_coordinator.py`
- Create: `tests/unit/market_data/capture/test_service.py`

- [ ] **Step 1: Write failing coordinator tests**

```python
async def test_ack_is_emitted_only_after_archive_returns(
    raw_envelope,
) -> None:
    archive = ControlledArchive()
    acknowledgements = []
    coordinator = CaptureCoordinator(
        queue=BoundedEnvelopeQueue(max_events=10, max_bytes=100000),
        archive=archive,
        quality=FakeQualityTracker(),
        repository=FakeCaptureRepository(),
        acknowledgement_sink=acknowledgements.append,
    )

    task = asyncio.create_task(coordinator.run())
    await coordinator.submit(raw_envelope)
    await archive.wait_until_append_started()
    assert acknowledgements == []

    archive.release_append()
    await coordinator.stop()
    await task
    assert acknowledgements[0].local_sequence == 1


async def test_queue_overflow_halts_service(raw_envelope) -> None:
    service = build_service(queue_max_events=1)
    await service.submit(raw_envelope)

    with pytest.raises(CaptureQueueFull):
        await service.submit(raw_envelope)

    assert service.state is MarketDataState.HALTED


def test_metrics_snapshot_reports_queue_and_state() -> None:
    service = build_service(queue_max_events=10)

    snapshot = service.metrics_snapshot()

    assert snapshot.state is MarketDataState.STARTING
    assert snapshot.queue_events == 0
    assert snapshot.queue_bytes == 0
    assert snapshot.desired_subscriptions == 0
    assert snapshot.active_connections == 0
```

- [ ] **Step 2: Write failing disk hysteresis test**

```python
def test_disk_halt_requires_recovery_threshold() -> None:
    guard = DiskSpaceGuard(
        warning_free_bytes=300,
        halt_free_bytes=200,
        recovery_free_bytes=250,
    )

    assert guard.evaluate(190) is DiskStatus.HALT
    assert guard.evaluate(220) is DiskStatus.HALT
    assert guard.evaluate(260) is DiskStatus.HEALTHY
```

- [ ] **Step 3: Confirm RED**

Run:

```bash
.venv/bin/python -m pytest tests/unit/market_data/capture -v
```

- [ ] **Step 4: Implement coordinator**

The coordinator is the only queue consumer:

```python
async def run(self) -> None:
    while not self._stopping or self._queue.size:
        envelope = await self._queue.get()
        try:
            quality_events = self._quality.observe(envelope)
            acknowledgement = await self._archive.append(envelope)
            for event in quality_events:
                await self._repository.save_quality_event(event)
            if self._acknowledgement_sink is not None:
                await self._acknowledgement_sink(acknowledgement)
        except Exception as error:
            await self._halt(f"archive failure: {error}")
            raise
        finally:
            self._queue.task_done(envelope)
```

`submit()` uses `put_nowait`; overflow writes a quality event and process state
before raising.

- [ ] **Step 5: Implement service state and disk guard**

`MarketDataCaptureService` owns:

- startup recovery;
- pending journal replay;
- process-state persistence;
- connection pool start/stop;
- disk checks;
- quality silence checks;
- graceful queue drain and archive close.

`DiskSpaceGuard` uses `shutil.disk_usage(root).free`. Once halted, it remains
halted until free bytes reach `recovery_free_bytes`.

On PostgreSQL failure while saving a manifest, append the manifest to the local
journal. If the oldest journal entry exceeds the configured age, transition to
`HALTED`.

Persist process-state transitions through:

```python
async def persist_process_state(
    self,
    *,
    state: MarketDataState,
    occurred_at: datetime,
    reason: str | None,
) -> None:
    try:
        await self._repository.save_process_state(
            state=state,
            occurred_at=occurred_at,
            reason=reason,
        )
    except SQLAlchemyError:
        await self._process_state_journal.append(
            PendingProcessState(state, occurred_at, reason)
        )
```

At startup, replay both process-state and manifest journals before opening
WebSocket connections. If a journal write itself fails, transition in memory
to `HALTED`, log the fault to stderr, and do not start the connection pool.

`observe_lifecycle()` converts connection open/close events to quality events.
A close followed by a new session creates one `RECONNECT_GAP` event per
subscribed symbol containing the disconnect and reconnect timestamps. The
quality tracker attributes the gap count to the new session so every file
created after reconnect records the known interval gap.

Expose:

```python
async def apply_symbols(
    self,
    symbols: frozenset[str],
    *,
    streams: tuple[CaptureStream, ...],
    generation: int,
) -> None:
    await self._connection_pool.apply_symbols(
        symbols,
        streams=streams,
        generation=generation,
    )
```

Define an immutable operational snapshot:

```python
@dataclass(frozen=True, slots=True)
class CaptureMetricsSnapshot:
    state: MarketDataState
    monitoring_generation: int
    monitoring_symbols: int
    desired_subscriptions: int
    active_subscriptions: int
    active_connections: int
    reconnect_count: int
    received_messages: int
    received_bytes: int
    queue_events: int
    queue_bytes: int
    archived_rows: int
    archived_bytes: int
    open_writers: int
    pending_manifests: int
    oldest_pending_manifest_seconds: float | None
    disk_free_bytes: int
```

`MarketDataCaptureService.metrics_snapshot()` composes counters exposed by the
queue, connection pool, archive, journals, and disk guard. A background task
logs this snapshot as structured fields every 10 seconds. Halt, checksum,
archive, disk, all-connections-down, and reconnect-loop faults log at error
level immediately rather than waiting for the periodic snapshot.

- [ ] **Step 6: Verify and commit**

Run:

```bash
.venv/bin/python -m pytest tests/unit/market_data/capture -v
.venv/bin/ruff check src/crypto_momentum_lab/market_data/capture \
  tests/unit/market_data/capture
.venv/bin/mypy src/crypto_momentum_lab/market_data/capture
```

Commit:

```bash
git add src/crypto_momentum_lab/market_data/capture \
  tests/unit/market_data/capture
git commit -m "feat: orchestrate fail-closed raw capture"
```

### Task 12: Universe-to-Subscription Integration and CLI

**Files:**
- Modify: `src/crypto_momentum_lab/universe/refresh.py`
- Modify: `src/crypto_momentum_lab/apps/market_data/main.py`
- Modify: `compose.yaml`
- Modify: `Dockerfile`
- Modify: `README.md`
- Modify: `tests/unit/universe/test_refresh.py`
- Modify: `tests/unit/apps/market_data/test_main.py`
- Create: `tests/e2e/test_market_data_process.py`

- [ ] **Step 1: Add a post-refresh observer port**

Add to `domain/universe/ports.py`:

```python
class UniverseSnapshotObserver(Protocol):
    async def snapshot_updated(
        self,
        snapshot: UniverseSnapshot,
    ) -> None:
        raise NotImplementedError


class NoUniverseSnapshotObserver:
    async def snapshot_updated(
        self,
        snapshot: UniverseSnapshot,
    ) -> None:
        return None
```

Inject it into `UniverseRefreshService`:

```python
def __init__(
    self,
    *,
    market_data: UniverseMarketData,
    repository: UniverseRepository,
    config: UniverseConfig,
    config_hash: str,
    obligations: MonitoringObligationProvider | None = None,
    observer: UniverseSnapshotObserver | None = None,
) -> None:
    self._market_data = market_data
    self._repository = repository
    self._config = config
    self._config_hash = config_hash
    self._obligations = obligations or NoMonitoringObligations()
    self._observer = observer or NoUniverseSnapshotObserver()
```

After `save_snapshot()`:

```python
        if snapshot.activated:
            await self._observer.snapshot_updated(snapshot)
```

- [ ] **Step 2: Write failing observer test**

```python
async def test_activated_snapshot_updates_subscriptions(
    fake_market_data,
    fake_repository,
) -> None:
    at = datetime(2026, 6, 15, 2, 1, tzinfo=UTC)
    fake_market_data.seed_single_symbol(at)
    observer = FakeSnapshotObserver()
    service = build_service(
        fake_market_data,
        fake_repository,
        observer=observer,
    )

    snapshot = await service.refresh(observed_at=at)

    assert observer.snapshots == [snapshot]
```

Add a midnight test asserting the observer is not called for an unactivated
00:01 snapshot.

- [ ] **Step 3: Add long-running command tests**

Test CLI composition through injected factory functions:

```python
def test_run_market_data_uses_combined_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = []

    async def fake_run(config_path: Path) -> None:
        called.append(config_path)

    monkeypatch.setattr(main, "run_market_data", fake_run)
    result = runner.invoke(
        main.app,
        ["run-market-data", "--config", "configs/environments/research.yaml"],
    )

    assert result.exit_code == 0
    assert called == [Path("configs/environments/research.yaml")]
```

- [ ] **Step 4: Implement composition root**

Build:

```python
from sqlalchemy.exc import SQLAlchemyError


@dataclass(frozen=True, slots=True)
class MarketDataRuntime:
    capture: MarketDataCaptureService
    universe: UniverseRefreshService
    universe_activation_minute: int
    enabled_streams: tuple[CaptureStream, ...]
    initial_symbols: frozenset[str]


class CaptureUniverseObserver:
    def __init__(
        self,
        capture: MarketDataCaptureService,
        *,
        streams: tuple[CaptureStream, ...],
        initial_generation: int,
    ) -> None:
        self._capture = capture
        self._streams = streams
        self._generation = initial_generation
        self._lock = asyncio.Lock()

    async def snapshot_updated(
        self,
        snapshot: UniverseSnapshot,
    ) -> None:
        symbols = frozenset(item.symbol for item in snapshot.memberships)
        async with self._lock:
            self._generation += 1
            await self._capture.apply_symbols(
                symbols,
                streams=self._streams,
                generation=self._generation,
            )


@asynccontextmanager
async def build_market_data_runtime(
    config_path: Path,
) -> AsyncIterator[MarketDataRuntime]:
    runtime = load_runtime_config(config_path)
    engine = create_async_database_engine(runtime.database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    universe_repository = PostgresUniverseRepository(sessions)
    capture_repository = PostgresCaptureRepository(sessions)
    initial_memberships = (
        await universe_repository.load_active_memberships()
    )
    initial_symbols = frozenset(initial_memberships)
    enabled_streams = tuple(
        CaptureStream(item)
        for item in runtime.capture.enabled_streams
    )
    archive_config = runtime.capture.archive
    manifest_journal = PendingManifestJournal(
        runtime.capture.archive.root / ".pending-manifests"
    )
    process_state_journal = PendingProcessStateJournal(
        runtime.capture.archive.root / ".pending-process-state"
    )
    quality = StreamQualityTracker(
        silence_timeout_seconds=runtime.capture.silence_timeout_seconds
    )
    queue = BoundedEnvelopeQueue(
        max_events=runtime.capture.queue_max_events,
        max_bytes=runtime.capture.queue_max_bytes,
    )

    async def save_manifest(manifest: ArchiveManifest) -> None:
        try:
            await capture_repository.save_manifest(manifest)
        except SQLAlchemyError:
            await manifest_journal.append(manifest)

    archive = ZstdJsonlArchive(
        root=archive_config.root,
        environment=runtime.environment,
        capture_version=behavior_hash(runtime),
        manifest_sink=save_manifest,
        known_gap_count_provider=lambda key: quality.known_gap_count(
            connection_session_id=key.connection_session_id,
            stream=key.stream,
            symbol=key.symbol,
        ),
        zstd_level=archive_config.zstd_level,
        rotation_uncompressed_bytes=(
            archive_config.rotation_uncompressed_bytes
        ),
        max_open_writers=archive_config.max_open_writers,
        group_commit_max_events=archive_config.group_commit_max_events,
        group_commit_max_milliseconds=(
            archive_config.group_commit_max_milliseconds
        ),
    )
    coordinator = CaptureCoordinator(
        queue=queue,
        archive=archive,
        quality=quality,
        repository=capture_repository,
        acknowledgement_sink=None,
    )
    connection_pool = BinanceConnectionPool(
        market_websocket_url=str(
            runtime.capture.market_websocket_url
        ),
        public_websocket_url=str(
            runtime.capture.public_websocket_url
        ),
        environment=runtime.environment,
        max_subscriptions_per_connection=(
            runtime.capture.max_subscriptions_per_connection
        ),
        control_messages_per_second=(
            runtime.capture.control_messages_per_second
        ),
        connection_lifetime_seconds=(
            runtime.capture.connection_lifetime_seconds
        ),
        open_timeout_seconds=runtime.capture.open_timeout_seconds,
        ping_interval_seconds=runtime.capture.ping_interval_seconds,
        ping_timeout_seconds=runtime.capture.ping_timeout_seconds,
        silence_timeout_seconds=runtime.capture.silence_timeout_seconds,
        on_envelope=coordinator.submit,
        on_lifecycle=coordinator.observe_lifecycle,
    )
    capture = MarketDataCaptureService(
        config=runtime.capture,
        coordinator=coordinator,
        connection_pool=connection_pool,
        repository=capture_repository,
        manifest_journal=manifest_journal,
        process_state_journal=process_state_journal,
    )
    observer = CaptureUniverseObserver(
        capture,
        streams=enabled_streams,
        initial_generation=1,
    )
    rest_client = BinanceUsdMRestClient(
        str(runtime.binance_base_url)
    )
    universe = UniverseRefreshService(
        market_data=rest_client,
        repository=universe_repository,
        config=runtime.universe,
        config_hash=behavior_hash(runtime),
        observer=observer,
    )
    try:
        yield MarketDataRuntime(
            capture=capture,
            universe=universe,
            universe_activation_minute=(
                runtime.universe.activation_minute
            ),
            enabled_streams=enabled_streams,
            initial_symbols=initial_symbols,
        )
    finally:
        await rest_client.aclose()
        await engine.dispose()
```

Add:

```python
async def run_market_data(config_path: Path) -> None:
    async with build_market_data_runtime(config_path) as runtime:
        await runtime.capture.start(
            symbols=runtime.initial_symbols,
            streams=runtime.enabled_streams,
            generation=1,
        )
        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(runtime.capture.run())
                tasks.create_task(
                    run_scheduler_loop(
                        LoggingRefreshService(runtime.universe),
                        activation_minute=(
                            runtime.universe_activation_minute
                        ),
                    )
                )
        finally:
            await runtime.capture.stop()


@app.command()
def run_market_data_command(
    config: Path | None = typer.Option(None, "--config"),
) -> None:
    try:
        asyncio.run(run_market_data(resolve_config_path(config)))
    except KeyboardInterrupt:
        log.info("market_data_stopped")
```

Set the Typer command name explicitly:

```python
@app.command("run-market-data")
```

`MarketDataCaptureService.start()` performs archive recovery, replays both
journals, starts the connection pool, applies the initial subscription set,
and transitions to `READY` or `DEGRADED`. Its `run()` method owns the disk,
silence, metrics, manifest-replay, and process-state-replay background loops.
Its `stop()` method stops new subscription changes, stops the pool, drains the
coordinator queue, closes archives, persists `STOPPED`, and respects the
configured shutdown deadline.

- [ ] **Step 5: Add process-level end-to-end test**

Use the fake WebSocket server and a temporary archive root. Seed the repository
with an active BTC membership, run the process, emit aggregate trade and book
ticker messages, then trigger a universe observer update to ETH.

Assert:

- BTC subscriptions are acknowledged initially;
- ETH additions are acknowledged before BTC removals;
- both symbols have raw files in the expected partitions;
- manifests exist in PostgreSQL;
- shutdown leaves no `.tmp` files;
- process state ends at `STOPPED`.

- [ ] **Step 6: Update Docker and operations**

Change Compose:

```yaml
    command: ["run-market-data"]
    volumes:
      - ./data:/app/data
```

Ensure the image includes `configs/capture`.

Change the Dockerfile default:

```dockerfile
CMD ["run-market-data"]
```

Document:

```bash
docker compose up --build market-data
```

and raw archive inspection:

```bash
find data/raw -name '*.jsonl.zst' -type f
```

- [ ] **Step 7: Verify and commit**

Run:

```bash
CML_TEST_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml \
CML_TEST_ASYNC_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml \
  .venv/bin/python -m pytest \
  tests/unit/universe \
  tests/unit/apps/market_data \
  tests/e2e/test_market_data_process.py -v
.venv/bin/ruff check src tests
.venv/bin/mypy src
docker compose config
```

Commit:

```bash
git add src tests compose.yaml Dockerfile README.md
git commit -m "feat: run universe and websocket capture together"
```

### Task 13: Complete Verification and 30-Minute Live Smoke

**Files:**
- Create: `scripts/run_market_data_smoke.py`
- Create: `tests/smoke/test_live_capture_manifest.py`
- Modify: `README.md`

- [ ] **Step 1: Add a bounded smoke runner**

Create `scripts/run_market_data_smoke.py`:

```python
import argparse
import asyncio
from pathlib import Path

from crypto_momentum_lab.apps.market_data.main import run_market_data_for


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/environments/research.yaml"),
    )
    parser.add_argument("--seconds", type=int, default=1800)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    await run_market_data_for(args.config, seconds=args.seconds)


if __name__ == "__main__":
    asyncio.run(main())
```

`run_market_data_for()` uses the same composition root and a timeout-driven
graceful shutdown; it is not a separate implementation.

- [ ] **Step 2: Add smoke verification query**

Create `tests/smoke/test_live_capture_manifest.py` as an opt-in test marked
`live` that checks:

- at least one active manifest per enabled non-sparse stream;
- manifest files exist and match SHA-256;
- row counts are positive;
- latest process state is `READY` or `DEGRADED`, never `HALTED`;
- no pending manifest is older than the configured maximum;
- no `.tmp` file remains after graceful shutdown.

Register:

```toml
  "live: requires public Binance connectivity and a 30-minute runtime",
```

- [ ] **Step 3: Run complete automated verification**

Run:

```bash
CML_TEST_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml \
CML_TEST_ASYNC_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml \
  .venv/bin/python -m pytest -m "not live" -v
.venv/bin/ruff check .
.venv/bin/mypy src
CML_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml \
  .venv/bin/alembic downgrade base
CML_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml \
  .venv/bin/alembic upgrade head
docker compose config
docker build -t crypto-momentum-lab:capture .
git diff --check
```

Expected:

- all non-live tests pass;
- Ruff and mypy pass;
- the full migration chain rebuilds from empty;
- Compose validates;
- the Docker image builds;
- the worktree has no whitespace errors.

- [ ] **Step 4: Run the live smoke**

Run:

```bash
docker compose up -d postgres
export CML_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml
.venv/bin/alembic upgrade head
export CML_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml
.venv/bin/python scripts/run_market_data_smoke.py --seconds 1800
CML_TEST_ASYNC_DATABASE_URL="$CML_DATABASE_URL" \
  .venv/bin/python -m pytest \
  tests/smoke/test_live_capture_manifest.py -m live -v
```

Expected:

- the process runs for 30 minutes and shuts down cleanly;
- monitoring-universe changes do not restart the process;
- active subscriptions cover all current monitoring symbols;
- `aggTrade`, `bookTicker`, `markPrice@1s`, and `kline_1m` have non-empty
  archive files;
- `forceOrder` is required to be subscribed and healthy but may remain empty;
- all finalized files have matching manifests and checksums;
- queue depth remains bounded and no halt fault occurs.

If Binance connectivity is unavailable, record the exact HTTP, DNS, TLS, or
WebSocket failure. Do not replace the live result with fixture success.

- [ ] **Step 5: Commit the completed phase**

```bash
git add scripts tests/smoke README.md pyproject.toml
git commit -m "test: verify live websocket capture"
```

## Phase Completion Checklist

Before declaring this phase complete, verify each requirement:

1. Dynamic active monitoring memberships drive all five stream subscriptions.
2. `bookTicker` uses the public route and the other four streams use market.
3. Connection groups never exceed 100 subscriptions.
4. Additions are synchronized before removals.
5. New sessions are created for reconnect and rolling lifetime replacement.
6. Raw envelopes contain wall-clock, monotonic, session, sequence, generation,
   exchange metadata, and unmodified payload.
7. The application queue is bounded by events and bytes and fails closed.
8. Durable acknowledgement occurs only after grouped flush and `fsync`.
9. Files rotate by hour, size, session, LRU eviction, and shutdown.
10. Final files use atomic rename, directory `fsync`, SHA-256, and immutable
    manifests.
11. PostgreSQL outages use a durable pending-manifest journal with a bounded
    grace period.
12. Startup recovery quarantines damaged temporary input and preserves complete
    records.
13. Quality events cover reconnects, acknowledgements, duplicates, numeric
    gaps, time regressions, silence, malformed payloads, queue, archive,
    manifests, and disk.
14. Disk halt and recovery use hysteresis.
15. Graceful shutdown drains the queue or leaves recoverable temporary files.
16. No normalized events, 15-second states, strategy features, account streams,
    or execution logic are added.
17. Unit, integration, end-to-end, migration, image-build, and 30-minute live
    smoke checks pass.
