# Strategy Runner Replay Scaffold Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first deterministic strategy-runner replay scaffold for `compression_breakout`, producing standardized signals and order-intent candidates from `market_states_15s` input.

**Architecture:** Add stable strategy domain contracts, a stateful compression-breakout runtime adapter, a replay runner/report writer, and a `cml-strategy-runner replay` CLI. Keep execution, account state, risk approval, paper fills, and live Binance private APIs out of this phase.

**Tech Stack:** Python 3.13, dataclasses, Decimal, Typer, pyarrow-backed Parquet reader, pytest, ruff, mypy.

---

## Scope Guard

This plan implements the approved spec:

`docs/superpowers/specs/2026-06-21-strategy-runner-replay-scaffold-design.md`

Do not add:

- authenticated Binance clients;
- simulated fills or PnL;
- account balances, positions, leverage, margin, or order quantization;
- PostgreSQL migrations for strategy state;
- multi-strategy combination;
- optimized parameters.

The only runtime strategy wired in this phase is `compression_breakout`.

## File Structure

- Create: `src/crypto_momentum_lab/domain/strategy/__init__.py`
  - Export strategy domain contracts.
- Create: `src/crypto_momentum_lab/domain/strategy/models.py`
  - Define run identity, strategy metadata, data requirements, signal, candidate, rejection, checkpoint, and decision models.
- Create: `src/crypto_momentum_lab/strategies/compression_breakout/runtime.py`
  - Stateful runtime adapter that consumes one `MarketState15s` at a time and emits live-style decisions.
- Create: `src/crypto_momentum_lab/strategy_runner/__init__.py`
  - Export replay APIs.
- Create: `src/crypto_momentum_lab/strategy_runner/replay.py`
  - Read states, run a selected strategy, validate IDs, summarize decisions, and write JSON reports.
- Create: `src/crypto_momentum_lab/apps/strategy_runner/__init__.py`
  - Application package marker.
- Create: `src/crypto_momentum_lab/apps/strategy_runner/main.py`
  - Typer CLI for `cml-strategy-runner replay`.
- Modify: `src/crypto_momentum_lab/strategies/compression_breakout/__init__.py`
  - Export runtime adapter classes.
- Modify: `pyproject.toml`
  - Add `cml-strategy-runner` console script.
- Create: `tests/unit/domain/strategy/test_models.py`
  - Domain model validation and deterministic ID tests.
- Create: `tests/unit/strategies/compression_breakout/test_runtime.py`
  - Runtime adapter tests for signals, candidates, warm-up, cooldown, and no future-state usage.
- Create: `tests/unit/strategy_runner/test_replay.py`
  - Replay runner and report serialization tests.
- Create: `tests/unit/apps/strategy_runner/test_main.py`
  - CLI parsing and output tests.

## Task 1: Strategy Domain Contracts

**Files:**
- Create: `src/crypto_momentum_lab/domain/strategy/__init__.py`
- Create: `src/crypto_momentum_lab/domain/strategy/models.py`
- Create: `tests/unit/domain/strategy/test_models.py`

- [ ] **Step 1: Write failing domain model tests**

Create `tests/unit/domain/strategy/test_models.py` with these tests:

```python
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    RejectionReason,
    RunMode,
    StrategyCheckpoint,
    StrategyDataRequirement,
    StrategyDecision,
    StrategyMetadata,
    StrategyRejection,
    StrategyRunIdentity,
    StrategySide,
    StrategySignal,
    deterministic_candidate_id,
    deterministic_config_hash,
    deterministic_signal_id,
)


def test_strategy_run_identity_requires_aware_created_at() -> None:
    with pytest.raises(ValueError, match="created_at must be timezone-aware"):
        StrategyRunIdentity(
            run_id="run-1",
            strategy_name="compression_breakout",
            strategy_version="v0",
            config_hash="abc",
            run_mode=RunMode.REPLAY,
            code_commit="unknown",
            created_at=datetime(2026, 6, 22, 0, 0),
            source_paths=("states",),
        )


def test_deterministic_config_hash_is_order_stable() -> None:
    left = deterministic_config_hash({"b": "2", "a": {"x": "1"}})
    right = deterministic_config_hash({"a": {"x": "1"}, "b": "2"})

    assert left == right
    assert len(left) == 64


def test_deterministic_signal_and_candidate_ids_are_stable() -> None:
    identity = _identity()
    detected_at = datetime(2026, 6, 22, 0, 1, tzinfo=UTC)

    signal_id = deterministic_signal_id(
        identity=identity,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        detected_at=detected_at,
        sequence=1,
    )
    candidate_id = deterministic_candidate_id(signal_id=signal_id, sequence=1)

    assert signal_id == deterministic_signal_id(
        identity=identity,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        detected_at=detected_at,
        sequence=1,
    )
    assert signal_id.startswith("sig_")
    assert candidate_id == deterministic_candidate_id(
        signal_id=signal_id,
        sequence=1,
    )
    assert candidate_id.startswith("cand_")


def test_strategy_records_validate_timestamps_and_relationships() -> None:
    identity = _identity()
    detected_at = datetime(2026, 6, 22, 0, 1, tzinfo=UTC)
    signal = StrategySignal(
        signal_id="sig_1",
        run_id=identity.run_id,
        strategy_name=identity.strategy_name,
        strategy_version=identity.strategy_version,
        config_hash=identity.config_hash,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        detected_at=detected_at,
        source_state_at=detected_at,
        reason="compression_breakout",
        features={"range_high": "100"},
        reference_prices={"breakout_price": "101"},
    )
    candidate = OrderIntentCandidate(
        candidate_id="cand_1",
        signal_id=signal.signal_id,
        run_id=identity.run_id,
        strategy_name=identity.strategy_name,
        strategy_version=identity.strategy_version,
        config_hash=identity.config_hash,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("100"),
        reduce_only=False,
        expires_at=detected_at + timedelta(seconds=30),
        created_at=detected_at,
        reason="compression_breakout",
        features=signal.features,
    )
    rejection = StrategyRejection(
        reason=RejectionReason.NO_SIGNAL,
        symbol="ETHUSDT",
        bucket_start=detected_at,
        details={"state": "evaluated"},
    )
    checkpoint = StrategyCheckpoint(
        last_processed_at_by_symbol={"BTCUSDT": detected_at},
        warmup_buckets_by_symbol={"BTCUSDT": 4},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 0},
        payload={"buffer_sizes": {"BTCUSDT": 4}},
    )

    decision = StrategyDecision(
        signals=(signal,),
        candidates=(candidate,),
        rejections=(rejection,),
        checkpoint=checkpoint,
    )

    assert decision.signals == (signal,)
    assert decision.candidates == (candidate,)
    assert decision.rejections == (rejection,)
    assert decision.checkpoint == checkpoint


def test_data_requirement_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="warmup_buckets must be positive"):
        StrategyDataRequirement(
            base_state_interval_seconds=15,
            warmup_buckets=0,
            required_fields=("close_price",),
            max_gap_seconds=30,
            allow_entries_before_warmup=False,
        )


def test_metadata_rejects_empty_name_and_version() -> None:
    with pytest.raises(ValueError, match="strategy name must not be empty"):
        StrategyMetadata(name="", version="v0")
    with pytest.raises(ValueError, match="strategy version must not be empty"):
        StrategyMetadata(name="compression_breakout", version="")


def _identity() -> StrategyRunIdentity:
    return StrategyRunIdentity(
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v0",
        config_hash="abc",
        run_mode=RunMode.REPLAY,
        code_commit="unknown",
        created_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
        source_paths=("states",),
    )
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/domain/strategy/test_models.py -v
```

Expected: fail with `ModuleNotFoundError: No module named 'crypto_momentum_lab.domain.strategy'`.

- [ ] **Step 3: Implement domain model exports**

Create `src/crypto_momentum_lab/domain/strategy/__init__.py`:

```python
from crypto_momentum_lab.domain.strategy.models import (
    EntryType,
    OrderIntentCandidate,
    RejectionReason,
    RunMode,
    StrategyCheckpoint,
    StrategyDataRequirement,
    StrategyDecision,
    StrategyMetadata,
    StrategyRejection,
    StrategyRunIdentity,
    StrategySide,
    StrategySignal,
    deterministic_candidate_id,
    deterministic_config_hash,
    deterministic_signal_id,
)

__all__ = [
    "EntryType",
    "OrderIntentCandidate",
    "RejectionReason",
    "RunMode",
    "StrategyCheckpoint",
    "StrategyDataRequirement",
    "StrategyDecision",
    "StrategyMetadata",
    "StrategyRejection",
    "StrategyRunIdentity",
    "StrategySide",
    "StrategySignal",
    "deterministic_candidate_id",
    "deterministic_config_hash",
    "deterministic_signal_id",
]
```

- [ ] **Step 4: Implement strategy domain models**

Create `src/crypto_momentum_lab/domain/strategy/models.py` with:

```python
import hashlib
import json
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.market.models import JsonValue


class RunMode(StrEnum):
    REPLAY = "replay"
    PAPER = "paper"
    LIVE = "live"


class StrategySide(StrEnum):
    LONG = "long"
    SHORT = "short"


class EntryType(StrEnum):
    MARKET = "market"


class RejectionReason(StrEnum):
    INSUFFICIENT_WARMUP = "insufficient_warmup"
    MISSING_REQUIRED_PRICE = "missing_required_price"
    MISSING_REQUIRED_FIELD = "missing_required_field"
    COOLDOWN_ACTIVE = "cooldown_active"
    NO_SIGNAL = "no_signal"
    CANDIDATE_EXPIRED = "candidate_expired"


@dataclass(frozen=True, slots=True)
class StrategyMetadata:
    name: str
    version: str

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("strategy name must not be empty")
        if not self.version:
            raise ValueError("strategy version must not be empty")


@dataclass(frozen=True, slots=True)
class StrategyRunIdentity:
    run_id: str
    strategy_name: str
    strategy_version: str
    config_hash: str
    run_mode: RunMode
    code_commit: str
    created_at: datetime
    source_paths: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if not self.strategy_name:
            raise ValueError("strategy_name must not be empty")
        if not self.strategy_version:
            raise ValueError("strategy_version must not be empty")
        if not self.config_hash:
            raise ValueError("config_hash must not be empty")
        if not _is_aware(self.created_at):
            raise ValueError("created_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class StrategyDataRequirement:
    base_state_interval_seconds: int
    warmup_buckets: int
    required_fields: tuple[str, ...]
    max_gap_seconds: int
    allow_entries_before_warmup: bool

    def __post_init__(self) -> None:
        if self.base_state_interval_seconds <= 0:
            raise ValueError("base_state_interval_seconds must be positive")
        if self.warmup_buckets <= 0:
            raise ValueError("warmup_buckets must be positive")
        if not self.required_fields:
            raise ValueError("required_fields must not be empty")
        if self.max_gap_seconds <= 0:
            raise ValueError("max_gap_seconds must be positive")


@dataclass(frozen=True, slots=True)
class StrategySignal:
    signal_id: str
    run_id: str
    strategy_name: str
    strategy_version: str
    config_hash: str
    symbol: str
    side: StrategySide
    detected_at: datetime
    source_state_at: datetime
    reason: str
    features: dict[str, JsonValue]
    reference_prices: dict[str, JsonValue]

    def __post_init__(self) -> None:
        if not self.signal_id:
            raise ValueError("signal_id must not be empty")
        if not _is_aware(self.detected_at):
            raise ValueError("detected_at must be timezone-aware")
        if not _is_aware(self.source_state_at):
            raise ValueError("source_state_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class OrderIntentCandidate:
    candidate_id: str
    signal_id: str
    run_id: str
    strategy_name: str
    strategy_version: str
    config_hash: str
    symbol: str
    side: StrategySide
    entry_type: EntryType
    limit_price: Decimal | None
    desired_notional: Decimal | None
    reduce_only: bool
    expires_at: datetime
    created_at: datetime
    reason: str
    features: dict[str, JsonValue]

    def __post_init__(self) -> None:
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
        if not self.signal_id:
            raise ValueError("signal_id must not be empty")
        if not _is_aware(self.created_at):
            raise ValueError("created_at must be timezone-aware")
        if not _is_aware(self.expires_at):
            raise ValueError("expires_at must be timezone-aware")
        if self.expires_at <= self.created_at:
            raise ValueError("expires_at must be after created_at")
        if self.desired_notional is not None and self.desired_notional <= 0:
            raise ValueError("desired_notional must be positive")
        if self.reduce_only:
            raise ValueError("reduce_only candidates are out of scope for V0")


@dataclass(frozen=True, slots=True)
class StrategyRejection:
    reason: RejectionReason
    symbol: str
    bucket_start: datetime
    details: dict[str, JsonValue]

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("symbol must not be empty")
        if not _is_aware(self.bucket_start):
            raise ValueError("bucket_start must be timezone-aware")


@dataclass(frozen=True, slots=True)
class StrategyCheckpoint:
    last_processed_at_by_symbol: dict[str, datetime]
    warmup_buckets_by_symbol: dict[str, int]
    cooldown_buckets_remaining_by_symbol: dict[str, int]
    payload: dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class StrategyDecision:
    signals: tuple[StrategySignal, ...]
    candidates: tuple[OrderIntentCandidate, ...]
    rejections: tuple[StrategyRejection, ...]
    checkpoint: StrategyCheckpoint


def deterministic_config_hash(config: object) -> str:
    payload = _jsonable(config)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def deterministic_signal_id(
    *,
    identity: StrategyRunIdentity,
    symbol: str,
    side: StrategySide,
    detected_at: datetime,
    sequence: int,
) -> str:
    if sequence <= 0:
        raise ValueError("sequence must be positive")
    value = "|".join(
        (
            identity.run_id,
            identity.strategy_name,
            symbol,
            side.value,
            detected_at.isoformat(),
            str(sequence),
        )
    )
    return f"sig_{uuid5(NAMESPACE_URL, value)}"


def deterministic_candidate_id(*, signal_id: str, sequence: int) -> str:
    if sequence <= 0:
        raise ValueError("sequence must be positive")
    return f"cand_{uuid5(NAMESPACE_URL, f'{signal_id}|{sequence}')}"


def _jsonable(value: object) -> JsonValue:
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(value))
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)


def _is_aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None
```

- [ ] **Step 5: Run domain model tests**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/domain/strategy/test_models.py -v
```

Expected: all tests pass.

- [ ] **Step 6: Commit domain contracts**

Run:

```bash
git add src/crypto_momentum_lab/domain/strategy tests/unit/domain/strategy/test_models.py
git commit -m "feat: add strategy domain contracts"
```

## Task 2: Compression Breakout Runtime Adapter

**Files:**
- Create: `src/crypto_momentum_lab/strategies/compression_breakout/runtime.py`
- Modify: `src/crypto_momentum_lab/strategies/compression_breakout/__init__.py`
- Create: `tests/unit/strategies/compression_breakout/test_runtime.py`

- [ ] **Step 1: Write failing runtime adapter tests**

Create `tests/unit/strategies/compression_breakout/test_runtime.py` with:

```python
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    RejectionReason,
    RunMode,
    StrategyRunIdentity,
    StrategySide,
    deterministic_config_hash,
)
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
    CompressionBreakoutRuntimeConfig,
    CompressionBreakoutRuntimeStrategy,
)


def test_runtime_emits_upward_signal_after_acceptance_window() -> None:
    strategy = _strategy()
    states = (
        _state(0, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(1, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(2, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(3, close=Decimal("101.0")),
        _state(4, close=Decimal("101.2")),
    )

    decisions = tuple(strategy.on_market_state(state) for state in states)
    decision = decisions[-1]

    assert len(decision.signals) == 1
    assert len(decision.candidates) == 1
    signal = decision.signals[0]
    candidate = decision.candidates[0]
    assert signal.symbol == "BTCUSDT"
    assert signal.side is StrategySide.LONG
    assert signal.detected_at == states[-1].bucket_start
    assert signal.source_state_at == states[-1].bucket_start
    assert signal.features["direction"] == "up"
    assert signal.features["range_high"] == "100.1"
    assert signal.features["range_low"] == "99.9"
    assert signal.features["breakout_price"] == "101.2"
    assert candidate.signal_id == signal.signal_id
    assert candidate.side is StrategySide.LONG
    assert candidate.desired_notional == Decimal("100")
    assert candidate.created_at == states[-1].bucket_start
    assert candidate.expires_at == states[-1].bucket_start + timedelta(seconds=30)


def test_runtime_emits_downward_signal_after_acceptance_window() -> None:
    strategy = _strategy()
    states = (
        _state(0, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(1, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(2, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(3, close=Decimal("98.8")),
        _state(4, close=Decimal("98.6")),
    )

    decision = _last_decision(strategy, states)

    assert len(decision.signals) == 1
    assert decision.signals[0].side is StrategySide.SHORT
    assert decision.signals[0].features["direction"] == "down"
    assert decision.signals[0].features["breakout_price"] == "98.6"


def test_runtime_records_warmup_and_missing_price_rejections() -> None:
    strategy = _strategy()

    first = strategy.on_market_state(_state(0, close=Decimal("100")))
    missing = strategy.on_market_state(_state(1, close=None))

    assert first.rejections[0].reason is RejectionReason.INSUFFICIENT_WARMUP
    assert missing.rejections[0].reason is RejectionReason.MISSING_REQUIRED_PRICE


def test_runtime_applies_cooldown_after_signal() -> None:
    strategy = _strategy(cooldown_buckets=2)
    first_breakout = (
        _state(0, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(1, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(2, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(3, close=Decimal("101.0")),
        _state(4, close=Decimal("101.2")),
    )
    _last_decision(strategy, first_breakout)

    cooldown_decision = strategy.on_market_state(_state(5, close=Decimal("102.0")))

    assert cooldown_decision.signals == ()
    assert cooldown_decision.candidates == ()
    assert cooldown_decision.rejections[0].reason is RejectionReason.COOLDOWN_ACTIVE


def test_runtime_checkpoint_contains_symbol_state() -> None:
    strategy = _strategy()
    decision = _last_decision(
        strategy,
        (
            _state(0, close=Decimal("100")),
            _state(1, close=Decimal("100")),
            _state(2, close=Decimal("100")),
        ),
    )

    checkpoint = decision.checkpoint

    assert checkpoint.last_processed_at_by_symbol["BTCUSDT"] == _state(
        2,
        close=Decimal("100"),
    ).bucket_start
    assert checkpoint.warmup_buckets_by_symbol["BTCUSDT"] == 3
    assert checkpoint.payload["buffer_sizes"] == {"BTCUSDT": 3}


def test_runtime_does_not_need_future_rows_for_detection() -> None:
    base_states = (
        _state(0, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(1, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(2, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(3, close=Decimal("101.0")),
        _state(4, close=Decimal("101.2")),
    )
    with_future = base_states + (_state(5, close=Decimal("120")),)

    base_signal = _last_decision(_strategy(), base_states).signals[0]
    future_signal = _last_decision(_strategy(), with_future).signals[0]

    assert base_signal.signal_id == future_signal.signal_id
    assert base_signal.features == future_signal.features


def _strategy(
    *,
    cooldown_buckets: int = 3,
) -> CompressionBreakoutRuntimeStrategy:
    event_config = CompressionBreakoutConfig(
        compression_window_buckets=3,
        max_range_width_pct=Decimal("0.01"),
        min_breakout_pct=Decimal("0.001"),
        acceptance_buckets=2,
        cooldown_buckets=cooldown_buckets,
        forward_horizon_buckets=(1,),
    )
    runtime_config = CompressionBreakoutRuntimeConfig(
        event_config=event_config,
        candidate_notional=Decimal("100"),
        candidate_ttl_buckets=2,
    )
    identity = StrategyRunIdentity(
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v0",
        config_hash=deterministic_config_hash(runtime_config),
        run_mode=RunMode.REPLAY,
        code_commit="unknown",
        created_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
        source_paths=("memory",),
    )
    return CompressionBreakoutRuntimeStrategy(
        config=runtime_config,
        identity=identity,
    )


def _last_decision(
    strategy: CompressionBreakoutRuntimeStrategy,
    states: tuple[MarketState15s, ...],
):
    decision = None
    for state in states:
        decision = strategy.on_market_state(state)
    assert decision is not None
    return decision


def _state(
    bucket_index: int,
    *,
    close: Decimal | None,
    high: Decimal | None = None,
    low: Decimal | None = None,
) -> MarketState15s:
    bucket_start = datetime(2026, 6, 22, 0, 0, 15 * bucket_index, tzinfo=UTC)
    bucket_end = bucket_start + timedelta(seconds=15)
    bid = close - Decimal("0.01") if close is not None else None
    ask = close + Decimal("0.01") if close is not None else None
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        open_price=close,
        high_price=high if high is not None else close,
        low_price=low if low is not None else close,
        close_price=close,
        trade_count=10,
        trade_notional=Decimal("1000"),
        aggressive_buy_notional=Decimal("600"),
        aggressive_sell_notional=Decimal("400"),
        last_bid_price=bid,
        last_ask_price=ask,
        spread=Decimal("0.02") if close is not None else None,
        midpoint=close,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=close,
        closed_kline_count=0,
        source_event_count=10,
        first_received_at=bucket_start,
        last_received_at=bucket_end,
    )
```

- [ ] **Step 2: Run runtime tests to verify failure**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategies/compression_breakout/test_runtime.py -v
```

Expected: fail with missing `CompressionBreakoutRuntimeConfig` or missing `runtime.py`.

- [ ] **Step 3: Implement runtime adapter**

Create `src/crypto_momentum_lab/strategies/compression_breakout/runtime.py` with these public APIs:

```python
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import JsonValue, MarketState15s
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    RejectionReason,
    StrategyCheckpoint,
    StrategyDataRequirement,
    StrategyDecision,
    StrategyMetadata,
    StrategyRejection,
    StrategyRunIdentity,
    StrategySide,
    StrategySignal,
    deterministic_candidate_id,
    deterministic_signal_id,
)
from crypto_momentum_lab.strategies.compression_breakout.event_study import (
    BreakoutDirection,
    CompressionBreakoutConfig,
)


@dataclass(frozen=True, slots=True)
class CompressionBreakoutRuntimeConfig:
    event_config: CompressionBreakoutConfig
    candidate_notional: Decimal | None
    candidate_ttl_buckets: int

    def __post_init__(self) -> None:
        if self.candidate_notional is not None and self.candidate_notional <= 0:
            raise ValueError("candidate_notional must be positive")
        if self.candidate_ttl_buckets <= 0:
            raise ValueError("candidate_ttl_buckets must be positive")


class CompressionBreakoutRuntimeStrategy:
    def __init__(
        self,
        *,
        config: CompressionBreakoutRuntimeConfig,
        identity: StrategyRunIdentity,
    ) -> None:
        self._config = config
        self._identity = identity
        self._buffers: dict[str, deque[MarketState15s]] = {}
        self._warmup: dict[str, int] = {}
        self._cooldown_remaining: dict[str, int] = {}
        self._last_processed: dict[str, datetime] = {}
        self._signal_sequence = 0

    def metadata(self) -> StrategyMetadata:
        return StrategyMetadata(name="compression_breakout", version="v0")

    def required_data(self) -> StrategyDataRequirement:
        event_config = self._config.event_config
        return StrategyDataRequirement(
            base_state_interval_seconds=15,
            warmup_buckets=(
                event_config.compression_window_buckets
                + event_config.acceptance_buckets
            ),
            required_fields=("close_price", "high_price", "low_price"),
            max_gap_seconds=30,
            allow_entries_before_warmup=False,
        )

    def restore(self, checkpoint: StrategyCheckpoint) -> None:
        self._warmup = dict(checkpoint.warmup_buckets_by_symbol)
        self._cooldown_remaining = dict(
            checkpoint.cooldown_buckets_remaining_by_symbol
        )

    def on_market_state(self, state: MarketState15s) -> StrategyDecision:
        self._last_processed[state.symbol] = state.bucket_start
        if _state_price(state) is None:
            return self._decision(
                rejections=(
                    StrategyRejection(
                        reason=RejectionReason.MISSING_REQUIRED_PRICE,
                        symbol=state.symbol,
                        bucket_start=state.bucket_start,
                        details={"field": "close_price"},
                    ),
                ),
            )

        buffer = self._buffers.setdefault(
            state.symbol,
            deque(maxlen=self.required_data().warmup_buckets),
        )
        buffer.append(state)
        self._warmup[state.symbol] = len(buffer)

        if len(buffer) < self.required_data().warmup_buckets:
            return self._decision(
                rejections=(
                    StrategyRejection(
                        reason=RejectionReason.INSUFFICIENT_WARMUP,
                        symbol=state.symbol,
                        bucket_start=state.bucket_start,
                        details={
                            "have": len(buffer),
                            "need": self.required_data().warmup_buckets,
                        },
                    ),
                ),
            )

        cooldown = self._cooldown_remaining.get(state.symbol, 0)
        if cooldown > 0:
            self._cooldown_remaining[state.symbol] = cooldown - 1
            return self._decision(
                rejections=(
                    StrategyRejection(
                        reason=RejectionReason.COOLDOWN_ACTIVE,
                        symbol=state.symbol,
                        bucket_start=state.bucket_start,
                        details={"remaining": cooldown},
                    ),
                ),
            )

        evaluation = _evaluate_buffer(tuple(buffer), self._config.event_config)
        if evaluation is None:
            return self._decision(
                rejections=(
                    StrategyRejection(
                        reason=RejectionReason.NO_SIGNAL,
                        symbol=state.symbol,
                        bucket_start=state.bucket_start,
                        details={"state": "evaluated"},
                    ),
                ),
            )

        signal, candidate = self._build_signal_and_candidate(state, evaluation)
        self._cooldown_remaining[state.symbol] = (
            self._config.event_config.cooldown_buckets
        )
        return self._decision(signals=(signal,), candidates=(candidate,))

    def checkpoint(self) -> StrategyCheckpoint:
        return StrategyCheckpoint(
            last_processed_at_by_symbol=dict(self._last_processed),
            warmup_buckets_by_symbol=dict(self._warmup),
            cooldown_buckets_remaining_by_symbol=dict(self._cooldown_remaining),
            payload={
                "buffer_sizes": {
                    symbol: len(buffer) for symbol, buffer in self._buffers.items()
                }
            },
        )
```

Complete the same file with private helpers:

- `_evaluate_buffer(buffer, config)`:
  - split `lookback = buffer[:compression_window_buckets]`;
  - split `acceptance = buffer[compression_window_buckets:]`;
  - compute `range_high`, `range_low`, `range_midpoint`, and `range_width_pct`;
  - return no event when range width exceeds `max_range_width_pct`;
  - use the first acceptance state as breakout candidate;
  - require all acceptance prices to remain outside the frozen boundary;
  - return direction, range values, breakout price, breakout distance, and range timestamps.
- `_state_price(state)`: return `state.close_price or state.midpoint or state.mark_price`.
- `_state_high(state)`: return `state.high_price or _state_price(state)`.
- `_state_low(state)`: return `state.low_price or _state_price(state)`.
- `_breakout_distance_pct(...)`: use the same formulas as `event_study.py`.
- `_decimal_features(...)`: serialize Decimal values with `str(value)`.

`_build_signal_and_candidate()` must:

- increment `_signal_sequence`;
- map `BreakoutDirection.UP` to `StrategySide.LONG`;
- map `BreakoutDirection.DOWN` to `StrategySide.SHORT`;
- create `signal_id` with `deterministic_signal_id`;
- create `candidate_id` with `deterministic_candidate_id`;
- use `created_at=state.bucket_start`;
- use `expires_at=state.bucket_start + timedelta(seconds=15 * candidate_ttl_buckets)`;
- set `entry_type=EntryType.MARKET`;
- set `reduce_only=False`.

- [ ] **Step 4: Export runtime classes**

Modify `src/crypto_momentum_lab/strategies/compression_breakout/__init__.py` to export:

```python
from crypto_momentum_lab.strategies.compression_breakout.event_study import (
    BreakoutDirection,
    CompressionBreakoutConfig,
    CompressionBreakoutDirectionSummary,
    CompressionBreakoutEvent,
    CompressionBreakoutSummary,
    find_compression_breakouts,
    summarize_compression_breakouts,
)
from crypto_momentum_lab.strategies.compression_breakout.runtime import (
    CompressionBreakoutRuntimeConfig,
    CompressionBreakoutRuntimeStrategy,
)

__all__ = [
    "BreakoutDirection",
    "CompressionBreakoutConfig",
    "CompressionBreakoutDirectionSummary",
    "CompressionBreakoutEvent",
    "CompressionBreakoutRuntimeConfig",
    "CompressionBreakoutRuntimeStrategy",
    "CompressionBreakoutSummary",
    "find_compression_breakouts",
    "summarize_compression_breakouts",
]
```

- [ ] **Step 5: Run runtime tests**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategies/compression_breakout/test_runtime.py -v
```

Expected: all tests pass.

- [ ] **Step 6: Run domain and runtime tests together**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/domain/strategy/test_models.py tests/unit/strategies/compression_breakout/test_runtime.py -v
```

Expected: all tests pass.

- [ ] **Step 7: Commit runtime adapter**

Run:

```bash
git add src/crypto_momentum_lab/strategies/compression_breakout tests/unit/strategies/compression_breakout/test_runtime.py
git commit -m "feat: add compression breakout runtime adapter"
```

## Task 3: Replay Runner and Report Writer

**Files:**
- Create: `src/crypto_momentum_lab/strategy_runner/__init__.py`
- Create: `src/crypto_momentum_lab/strategy_runner/replay.py`
- Create: `tests/unit/strategy_runner/test_replay.py`

- [ ] **Step 1: Write failing replay tests**

Create `tests/unit/strategy_runner/test_replay.py` with:

```python
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.persistence.parquet import write_market_states_15s_dataset
from crypto_momentum_lab.strategies.compression_breakout import CompressionBreakoutConfig
from crypto_momentum_lab.strategy_runner import (
    ReplayConfig,
    ReplayError,
    build_strategy_replay_report,
    run_strategy_replay,
    write_strategy_replay_report,
)


def test_run_strategy_replay_emits_report_from_in_memory_states() -> None:
    report = run_strategy_replay(
        states=_breakout_states(),
        source_paths=("memory",),
        config=ReplayConfig(
            strategy_name="compression_breakout",
            run_id="run-1",
            code_commit="unknown",
            generated_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
            compression_breakout=CompressionBreakoutConfig(
                compression_window_buckets=3,
                max_range_width_pct=Decimal("0.01"),
                min_breakout_pct=Decimal("0.001"),
                acceptance_buckets=2,
                cooldown_buckets=3,
                forward_horizon_buckets=(1,),
            ),
            candidate_notional=Decimal("100"),
            candidate_ttl_buckets=2,
        ),
    )

    assert report.schema_version == 1
    assert report.run.strategy_name == "compression_breakout"
    assert report.input_state_count == len(_breakout_states())
    assert report.processed_symbol_count == 1
    assert len(report.signals) == 1
    assert len(report.candidates) == 1
    assert report.signals[0].side is StrategySide.LONG
    assert report.candidates[0].signal_id == report.signals[0].signal_id
    assert report.summary_counts["signals_by_side"] == {"long": 1}
    assert report.summary_counts["signals_by_symbol"] == {"BTCUSDT": 1}


def test_build_strategy_replay_report_reads_parquet_states(tmp_path: Path) -> None:
    input_path = tmp_path / "raw.jsonl.zst"
    input_path.write_bytes(b"raw")
    derived_root = tmp_path / "derived"
    write_market_states_15s_dataset(
        root=derived_root,
        states=_breakout_states(),
        input_paths=(input_path,),
    )

    report = build_strategy_replay_report(
        state_paths=(derived_root / "market_states_15s",),
        config=ReplayConfig(
            strategy_name="compression_breakout",
            run_id="run-parquet",
            code_commit="unknown",
            generated_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
            compression_breakout=CompressionBreakoutConfig(
                compression_window_buckets=3,
                max_range_width_pct=Decimal("0.01"),
                min_breakout_pct=Decimal("0.001"),
                acceptance_buckets=2,
                cooldown_buckets=3,
                forward_horizon_buckets=(1,),
            ),
            candidate_notional=Decimal("100"),
            candidate_ttl_buckets=2,
        ),
    )

    assert report.input_state_count == len(_breakout_states())
    assert len(report.signals) == 1
    assert report.source_paths == ((derived_root / "market_states_15s").as_posix(),)


def test_write_strategy_replay_report_serializes_decimal_and_timestamps(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "report.json"
    report = run_strategy_replay(
        states=_breakout_states(),
        source_paths=("memory",),
        config=_replay_config(),
    )

    write_strategy_replay_report(report, output_path)

    payload = json.loads(output_path.read_text())
    assert payload["schema_version"] == 1
    assert payload["run"]["created_at"] == "2026-06-22T00:00:00+00:00"
    assert payload["signals"][0]["features"]["breakout_price"] == "101.2"
    assert payload["candidates"][0]["desired_notional"] == "100"
    assert payload["final_checkpoint"]["payload"]["buffer_sizes"] == {"BTCUSDT": 5}


def test_replay_rejects_unknown_strategy() -> None:
    with pytest.raises(ReplayError, match="unsupported strategy"):
        run_strategy_replay(
            states=_breakout_states(),
            source_paths=("memory",),
            config=ReplayConfig(
                strategy_name="order_flow_impulse",
                run_id="run-1",
                code_commit="unknown",
                generated_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
                compression_breakout=_replay_config().compression_breakout,
                candidate_notional=Decimal("100"),
                candidate_ttl_buckets=2,
            ),
        )


def test_replay_rejects_empty_input() -> None:
    with pytest.raises(ReplayError, match="no market states"):
        run_strategy_replay(
            states=(),
            source_paths=("memory",),
            config=_replay_config(),
        )


def test_replay_rejects_naive_state_timestamp() -> None:
    state = _state(0, close=Decimal("100"))
    naive = replace(state, bucket_start=datetime(2026, 6, 22, 0, 0))

    with pytest.raises(ReplayError, match="bucket_start must be timezone-aware"):
        run_strategy_replay(
            states=(naive,),
            source_paths=("memory",),
            config=_replay_config(),
        )


def _replay_config() -> ReplayConfig:
    return ReplayConfig(
        strategy_name="compression_breakout",
        run_id="run-1",
        code_commit="unknown",
        generated_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
        compression_breakout=CompressionBreakoutConfig(
            compression_window_buckets=3,
            max_range_width_pct=Decimal("0.01"),
            min_breakout_pct=Decimal("0.001"),
            acceptance_buckets=2,
            cooldown_buckets=3,
            forward_horizon_buckets=(1,),
        ),
        candidate_notional=Decimal("100"),
        candidate_ttl_buckets=2,
    )


def _breakout_states() -> tuple[MarketState15s, ...]:
    return (
        _state(0, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(1, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(2, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(3, close=Decimal("101.0")),
        _state(4, close=Decimal("101.2")),
    )


def _state(
    bucket_index: int,
    *,
    close: Decimal,
    high: Decimal | None = None,
    low: Decimal | None = None,
) -> MarketState15s:
    bucket_start = datetime(2026, 6, 22, 0, 0, 15 * bucket_index, tzinfo=UTC)
    bucket_end = bucket_start + timedelta(seconds=15)
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        open_price=close,
        high_price=high if high is not None else close,
        low_price=low if low is not None else close,
        close_price=close,
        trade_count=10,
        trade_notional=Decimal("1000"),
        aggressive_buy_notional=Decimal("600"),
        aggressive_sell_notional=Decimal("400"),
        last_bid_price=close - Decimal("0.01"),
        last_ask_price=close + Decimal("0.01"),
        spread=Decimal("0.02"),
        midpoint=close,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=close,
        closed_kline_count=0,
        source_event_count=10,
        first_received_at=bucket_start,
        last_received_at=bucket_end,
    )
```

- [ ] **Step 2: Run replay tests to verify failure**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategy_runner/test_replay.py -v
```

Expected: fail with `ModuleNotFoundError: No module named 'crypto_momentum_lab.strategy_runner'`.

- [ ] **Step 3: Implement replay exports**

Create `src/crypto_momentum_lab/strategy_runner/__init__.py`:

```python
from crypto_momentum_lab.strategy_runner.replay import (
    ReplayConfig,
    ReplayError,
    StrategyReplayReport,
    build_strategy_replay_report,
    run_strategy_replay,
    write_strategy_replay_report,
)

__all__ = [
    "ReplayConfig",
    "ReplayError",
    "StrategyReplayReport",
    "build_strategy_replay_report",
    "run_strategy_replay",
    "write_strategy_replay_report",
]
```

- [ ] **Step 4: Implement replay runner**

Create `src/crypto_momentum_lab/strategy_runner/replay.py` with these public APIs:

```python
import json
from collections import Counter
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from crypto_momentum_lab.domain.market.models import JsonValue, MarketState15s
from crypto_momentum_lab.domain.strategy import (
    OrderIntentCandidate,
    RunMode,
    StrategyCheckpoint,
    StrategyRejection,
    StrategyRunIdentity,
    StrategySignal,
    deterministic_config_hash,
)
from crypto_momentum_lab.persistence.parquet import read_market_states_15s_dataset
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
    CompressionBreakoutRuntimeConfig,
    CompressionBreakoutRuntimeStrategy,
)


class ReplayError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ReplayConfig:
    strategy_name: str
    run_id: str
    code_commit: str
    generated_at: datetime
    compression_breakout: CompressionBreakoutConfig
    candidate_notional: Decimal | None
    candidate_ttl_buckets: int


@dataclass(frozen=True, slots=True)
class StrategyReplayReport:
    schema_version: int
    generated_at: datetime
    run: StrategyRunIdentity
    source_paths: tuple[str, ...]
    input_state_count: int
    processed_symbol_count: int
    signals: tuple[StrategySignal, ...]
    candidates: tuple[OrderIntentCandidate, ...]
    rejection_summary: dict[str, dict[str, int]]
    final_checkpoint: StrategyCheckpoint
    summary_counts: dict[str, dict[str, int]]
```

Implement:

- `build_strategy_replay_report(state_paths, config)`:
  - read states with `read_market_states_15s_dataset(state_paths)`;
  - pass `source_paths=tuple(path.as_posix() for path in state_paths)`.
- `run_strategy_replay(states, source_paths, config)`:
  - reject empty states;
  - reject unsupported strategy names other than `"compression_breakout"`;
  - reject naive `bucket_start` or `bucket_end`;
  - sort states by `(state.bucket_start, state.symbol)`;
  - create `StrategyRunIdentity` with `RunMode.REPLAY`;
  - create `CompressionBreakoutRuntimeStrategy`;
  - collect all signals, candidates, and rejections from each decision;
  - validate every candidate references a signal in the same report;
  - validate signal and candidate IDs are unique;
  - produce `StrategyReplayReport`.
- `write_strategy_replay_report(report, output_path)`:
  - create parent directory;
  - write stable JSON with `indent=2` and `sort_keys=True`;
  - serialize Decimal values as strings and datetimes as ISO strings.

Use this serializer:

```python
def _jsonable(value: object) -> JsonValue:
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(value))
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "value") and isinstance(value.value, str):
        return value.value
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)
```

Build summaries as:

```python
def _rejection_summary(
    rejections: tuple[StrategyRejection, ...],
) -> dict[str, dict[str, int]]:
    counts: dict[str, Counter[str]] = {}
    for rejection in rejections:
        counts.setdefault(rejection.reason.value, Counter())
        counts[rejection.reason.value][rejection.symbol] += 1
    return {
        reason: dict(sorted(symbol_counts.items()))
        for reason, symbol_counts in sorted(counts.items())
    }


def _summary_counts(
    signals: tuple[StrategySignal, ...],
) -> dict[str, dict[str, int]]:
    by_side = Counter(signal.side.value for signal in signals)
    by_symbol = Counter(signal.symbol for signal in signals)
    return {
        "signals_by_side": dict(sorted(by_side.items())),
        "signals_by_symbol": dict(sorted(by_symbol.items())),
    }
```

- [ ] **Step 5: Run replay tests**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategy_runner/test_replay.py -v
```

Expected: all tests pass.

- [ ] **Step 6: Run strategy-runner related tests**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/domain/strategy/test_models.py tests/unit/strategies/compression_breakout/test_runtime.py tests/unit/strategy_runner/test_replay.py -v
```

Expected: all tests pass.

- [ ] **Step 7: Commit replay runner**

Run:

```bash
git add src/crypto_momentum_lab/strategy_runner tests/unit/strategy_runner/test_replay.py
git commit -m "feat: add deterministic strategy replay runner"
```

## Task 4: Strategy Runner CLI and Console Script

**Files:**
- Create: `src/crypto_momentum_lab/apps/strategy_runner/__init__.py`
- Create: `src/crypto_momentum_lab/apps/strategy_runner/main.py`
- Modify: `pyproject.toml`
- Create: `tests/unit/apps/strategy_runner/test_main.py`

- [ ] **Step 1: Write failing CLI tests**

Create `tests/unit/apps/strategy_runner/test_main.py` with:

```python
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from crypto_momentum_lab.apps.strategy_runner import main
from crypto_momentum_lab.strategies.compression_breakout import CompressionBreakoutConfig
from crypto_momentum_lab.strategy_runner import ReplayConfig

runner = CliRunner()


def test_replay_command_writes_report(tmp_path: Path, monkeypatch) -> None:
    states_root = tmp_path / "states"
    states_root.mkdir()
    output_path = tmp_path / "report.json"
    calls: list[tuple[tuple[Path, ...], ReplayConfig]] = []
    writes: list[tuple[object, Path]] = []

    def fake_build_strategy_replay_report(
        *,
        state_paths: tuple[Path, ...],
        config: ReplayConfig,
    ) -> object:
        calls.append((state_paths, config))
        return SimpleNamespace(
            input_state_count=5,
            signals=(object(), object()),
            candidates=(object(),),
        )

    def fake_write_strategy_replay_report(report: object, path: Path) -> None:
        writes.append((report, path))

    monkeypatch.setattr(
        main,
        "build_strategy_replay_report",
        fake_build_strategy_replay_report,
    )
    monkeypatch.setattr(
        main,
        "write_strategy_replay_report",
        fake_write_strategy_replay_report,
    )

    result = runner.invoke(
        main.app,
        [
            "replay",
            "--strategy",
            "compression_breakout",
            "--states-root",
            str(states_root),
            "--output",
            str(output_path),
            "--run-id",
            "run-cli",
            "--generated-at",
            "2026-06-22T00:00:00+00:00",
            "--compression-window-buckets",
            "3",
            "--max-range-width-pct",
            "0.01",
            "--min-breakout-pct",
            "0.001",
            "--acceptance-buckets",
            "2",
            "--cooldown-buckets",
            "4",
            "--candidate-notional",
            "250",
            "--candidate-ttl-buckets",
            "3",
        ],
    )

    assert result.exit_code == 0
    assert calls == [
        (
            (states_root,),
            ReplayConfig(
                strategy_name="compression_breakout",
                run_id="run-cli",
                code_commit="unknown",
                generated_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
                compression_breakout=CompressionBreakoutConfig(
                    compression_window_buckets=3,
                    max_range_width_pct=Decimal("0.01"),
                    min_breakout_pct=Decimal("0.001"),
                    acceptance_buckets=2,
                    cooldown_buckets=4,
                    forward_horizon_buckets=(1,),
                ),
                candidate_notional=Decimal("250"),
                candidate_ttl_buckets=3,
            ),
        )
    ]
    assert writes[0][1] == output_path
    assert "Replay completed: states=5 signals=2 candidates=1" in result.stdout
    assert output_path.as_posix() in result.stdout


def test_replay_command_generates_default_run_id(tmp_path: Path, monkeypatch) -> None:
    states_root = tmp_path / "states"
    states_root.mkdir()
    output_path = tmp_path / "report.json"
    configs: list[ReplayConfig] = []

    def fake_build_strategy_replay_report(
        *,
        state_paths: tuple[Path, ...],
        config: ReplayConfig,
    ) -> object:
        configs.append(config)
        return SimpleNamespace(input_state_count=0, signals=(), candidates=())

    monkeypatch.setattr(
        main,
        "build_strategy_replay_report",
        fake_build_strategy_replay_report,
    )
    monkeypatch.setattr(main, "write_strategy_replay_report", lambda report, path: None)

    result = runner.invoke(
        main.app,
        [
            "replay",
            "--strategy",
            "compression_breakout",
            "--states-root",
            str(states_root),
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert configs[0].run_id.startswith("replay-")
    assert configs[0].generated_at.tzinfo is not None
```

- [ ] **Step 2: Run CLI tests to verify failure**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/apps/strategy_runner/test_main.py -v
```

Expected: fail with missing `crypto_momentum_lab.apps.strategy_runner`.

- [ ] **Step 3: Implement app package**

Create `src/crypto_momentum_lab/apps/strategy_runner/__init__.py` as an empty package marker.

- [ ] **Step 4: Implement Typer CLI**

Create `src/crypto_momentum_lab/apps/strategy_runner/main.py`:

```python
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated
from uuid import uuid4

import typer

from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
)
from crypto_momentum_lab.strategy_runner import (
    ReplayConfig,
    build_strategy_replay_report,
    write_strategy_replay_report,
)

app = typer.Typer(no_args_is_help=True)


@app.callback()
def strategy_runner_app() -> None:
    """Strategy runner utilities."""


@app.command("replay")
def replay_command(
    strategy_name: Annotated[
        str,
        typer.Option("--strategy", help="Strategy name. V0 supports compression_breakout."),
    ],
    states_root: Annotated[
        Path,
        typer.Option(
            "--states-root",
            exists=True,
            file_okay=False,
            readable=True,
            help="Root directory containing market_states_15s Parquet files.",
        ),
    ],
    output_path: Annotated[
        Path,
        typer.Option("--output", dir_okay=False, help="JSON replay report output path."),
    ],
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="Optional deterministic run ID."),
    ] = None,
    generated_at: Annotated[
        datetime | None,
        typer.Option("--generated-at", help="Optional ISO timestamp for tests."),
    ] = None,
    compression_window_buckets: Annotated[
        int,
        typer.Option("--compression-window-buckets", min=1),
    ] = 20,
    max_range_width_pct: Annotated[
        str,
        typer.Option("--max-range-width-pct"),
    ] = "0.005",
    min_breakout_pct: Annotated[
        str,
        typer.Option("--min-breakout-pct"),
    ] = "0.001",
    acceptance_buckets: Annotated[
        int,
        typer.Option("--acceptance-buckets", min=1),
    ] = 1,
    cooldown_buckets: Annotated[
        int,
        typer.Option("--cooldown-buckets", min=0),
    ] = 8,
    candidate_notional: Annotated[
        str,
        typer.Option("--candidate-notional"),
    ] = "100",
    candidate_ttl_buckets: Annotated[
        int,
        typer.Option("--candidate-ttl-buckets", min=1),
    ] = 4,
) -> None:
    created_at = generated_at or datetime.now(tz=UTC)
    config = ReplayConfig(
        strategy_name=strategy_name,
        run_id=run_id or f"replay-{uuid4()}",
        code_commit="unknown",
        generated_at=created_at,
        compression_breakout=CompressionBreakoutConfig(
            compression_window_buckets=compression_window_buckets,
            max_range_width_pct=Decimal(max_range_width_pct),
            min_breakout_pct=Decimal(min_breakout_pct),
            acceptance_buckets=acceptance_buckets,
            cooldown_buckets=cooldown_buckets,
            forward_horizon_buckets=(1,),
        ),
        candidate_notional=Decimal(candidate_notional),
        candidate_ttl_buckets=candidate_ttl_buckets,
    )
    report = build_strategy_replay_report(
        state_paths=(states_root,),
        config=config,
    )
    write_strategy_replay_report(report, output_path)
    typer.echo(
        "Replay completed: "
        f"states={report.input_state_count} "
        f"signals={len(report.signals)} "
        f"candidates={len(report.candidates)}"
    )
    typer.echo(output_path.as_posix())
```

- [ ] **Step 5: Add console script**

Modify `[project.scripts]` in `pyproject.toml`:

```toml
[project.scripts]
cml-market-data = "crypto_momentum_lab.apps.market_data.main:app"
cml-research = "crypto_momentum_lab.apps.research.main:app"
cml-strategy-runner = "crypto_momentum_lab.apps.strategy_runner.main:app"
```

- [ ] **Step 6: Run CLI tests**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/apps/strategy_runner/test_main.py -v
```

Expected: all tests pass.

- [ ] **Step 7: Run related tests**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/domain/strategy/test_models.py tests/unit/strategies/compression_breakout/test_runtime.py tests/unit/strategy_runner/test_replay.py tests/unit/apps/strategy_runner/test_main.py -v
```

Expected: all tests pass.

- [ ] **Step 8: Commit CLI**

Run:

```bash
git add pyproject.toml src/crypto_momentum_lab/apps/strategy_runner tests/unit/apps/strategy_runner/test_main.py
git commit -m "feat: add strategy runner replay cli"
```

## Task 5: Verification, Polish, and Merge

**Files:**
- Review all files created or modified in Tasks 1-4.
- No new source files should be added in this task unless a verification failure requires a targeted fix.

- [ ] **Step 1: Run targeted strategy-runner tests**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/domain/strategy/test_models.py tests/unit/strategies/compression_breakout/test_runtime.py tests/unit/strategy_runner/test_replay.py tests/unit/apps/strategy_runner/test_main.py -v
```

Expected: all tests pass.

- [ ] **Step 2: Run broader affected tests**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategies/compression_breakout tests/unit/persistence/parquet/test_read_states.py tests/unit/apps/research/test_research_main.py -v
```

Expected: all tests pass.

- [ ] **Step 3: Run static checks**

Run:

```bash
/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/ruff check .
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/mypy src
```

Expected:

```text
All checks passed!
Success: no issues found in <n> source files
```

- [ ] **Step 4: Verify Typer app import**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -c "from crypto_momentum_lab.apps.strategy_runner.main import app; print(app.info.name)"
```

Expected: command exits `0`. The printed value may be `None`; the import success is the verification target.

- [ ] **Step 5: Run full non-live tests**

Run:

```bash
env -u ALL_PROXY -u all_proxy -u HTTP_PROXY -u http_proxy -u HTTPS_PROXY -u https_proxy -u NO_PROXY -u no_proxy PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest -m "not live" -v
```

Expected: all selected tests pass.

- [ ] **Step 6: Inspect final diff**

Run:

```bash
git status --short --branch
git log --oneline -5 --decorate
git diff --stat main
```

Expected:

- current branch contains only Strategy Runner V0 implementation commits;
- no unrelated files are modified;
- no generated reports, cache files, or local datasets are staged.

- [ ] **Step 7: Commit any verification fixes**

If Step 1-5 required fixes, commit only those fixes:

```bash
git add src tests pyproject.toml
git commit -m "fix: stabilize strategy runner replay scaffold"
```

Skip this commit when there were no verification fixes.

- [ ] **Step 8: Merge locally**

From the main workspace, fast-forward merge the feature branch:

```bash
git checkout main
git merge --ff-only strategy-runner-replay-scaffold
```

Expected: fast-forward merge succeeds.

- [ ] **Step 9: Verify merged main**

Run the same verification on `main`:

```bash
/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/ruff check .
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/mypy src
env -u ALL_PROXY -u all_proxy -u HTTP_PROXY -u http_proxy -u HTTPS_PROXY -u https_proxy -u NO_PROXY -u no_proxy PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest -m "not live" -v
```

Expected: all commands pass.

- [ ] **Step 10: Clean temporary worktree and branch**

If implementation used `.worktrees/strategy-runner-replay-scaffold`, remove it after merge:

```bash
git worktree remove .worktrees/strategy-runner-replay-scaffold
git worktree prune
git branch -d strategy-runner-replay-scaffold
```

Expected: only the main workspace remains in `git worktree list`.
