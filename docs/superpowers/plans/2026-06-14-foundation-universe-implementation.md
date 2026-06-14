# Foundation and Dynamic Universe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the project foundation and a production-shaped, point-in-time universe service that ranks all active Binance USD-M USDT perpetual contracts by UTC-day return and persists deterministic target and monitoring universes.

**Architecture:** Implement a Python modular monolith with domain code independent of Binance and PostgreSQL. A Binance REST adapter supplies contract metadata, current prices, and UTC-day opens; a universe application service performs deterministic ranking and retention; a PostgreSQL repository atomically persists snapshots. A Typer command supports one-shot refreshes and an hourly scheduler, providing a complete vertical slice before high-frequency capture is added.

**Tech Stack:** Python 3.12+, Hatchling, Pydantic 2, PyYAML, HTTPX, Typer, SQLAlchemy 2 async, asyncpg, Alembic, PostgreSQL 16, pytest, pytest-asyncio, respx, Ruff, mypy, Docker Compose.

---

## Scope Boundary

This is implementation plan 1 of the approved architecture. It delivers:

- package and test foundations;
- validated and hashed universe configuration;
- deterministic UTC-day ranking;
- target, retention, and forced-monitoring membership;
- point-in-time PostgreSQL persistence;
- current Binance public REST integration;
- a one-shot refresh command and hourly scheduler;
- Docker-based local operation and an end-to-end fixture test.

It intentionally does not implement websocket capture, raw JSONL archives,
15-second aggregation, readiness, strategies, authenticated account access,
orders, or live risk. Those are separate plans in this order:

1. Market-data capture and quality.
2. Deterministic aggregation and replay.
3. One independent event-study plan for each strategy.
4. Paper execution and strategy runtime.
5. Authenticated account, risk, and order execution.
6. Shadow and small-capital deployment hardening.

## File Map

```text
pyproject.toml
README.md
.env.example
compose.yaml
Dockerfile
alembic.ini
alembic/
  env.py
  versions/20260614_0001_universe_foundation.py
configs/
  environments/research.yaml
  universe/utc_day_top_bottom.yaml
src/crypto_momentum_lab/
  __init__.py
  apps/__init__.py
  config/
    __init__.py
    loader.py
    models.py
  domain/
    __init__.py
  domain/universe/
    __init__.py
    membership.py
    models.py
    ports.py
    ranking.py
  market_data/
    __init__.py
  market_data/binance/
    __init__.py
    rest.py
  persistence/
    __init__.py
  persistence/postgres/
    __init__.py
    base.py
    models.py
    repository.py
    session.py
  universe/
    __init__.py
    refresh.py
    scheduler.py
  apps/market_data/
    __init__.py
    main.py
tests/
  conftest.py
  unit/
    config/test_loader.py
    domain/universe/test_membership.py
    domain/universe/test_ranking.py
    market_data/binance/test_rest.py
    universe/test_refresh.py
    universe/test_scheduler.py
  integration/
    conftest.py
    persistence/test_migrations.py
    persistence/test_repository.py
  e2e/test_universe_refresh.py
```

## Official API References

- Exchange information: <https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information>
- Symbol price ticker V2: <https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Symbol-Price-Ticker-v2>
- Kline data: <https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data>

### Task 1: Python Project and Quality Tooling

**Files:**
- Create: `pyproject.toml`
- Create: `README.md`
- Create: `.env.example`
- Modify: `.gitignore`
- Create: `src/crypto_momentum_lab/__init__.py`
- Create: `src/crypto_momentum_lab/apps/__init__.py`
- Create: `src/crypto_momentum_lab/domain/__init__.py`
- Create: `src/crypto_momentum_lab/market_data/__init__.py`
- Create: `src/crypto_momentum_lab/persistence/__init__.py`
- Create: `tests/unit/test_package.py`

- [ ] **Step 1: Write the failing package test**

```python
# tests/unit/test_package.py
from crypto_momentum_lab import __version__


def test_package_exposes_version() -> None:
    assert __version__ == "0.1.0"
```

- [ ] **Step 2: Run the test and verify the package is missing**

Run:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pytest tests/unit/test_package.py -v
```

Expected: the final command fails because `pytest` or
`crypto_momentum_lab` is not installed.

- [ ] **Step 3: Add project metadata and dependencies**

```toml
# pyproject.toml
[build-system]
requires = ["hatchling>=1.27,<2"]
build-backend = "hatchling.build"

[project]
name = "crypto-momentum-lab"
version = "0.1.0"
description = "Short-horizon cryptocurrency momentum research and trading infrastructure"
readme = "README.md"
requires-python = ">=3.12,<3.14"
dependencies = [
  "alembic>=1.14,<2",
  "asyncpg>=0.30,<1",
  "httpx>=0.28,<1",
  "pydantic>=2.10,<3",
  "psycopg[binary]>=3.2,<4",
  "PyYAML>=6.0,<7",
  "sqlalchemy[asyncio]>=2.0,<3",
  "structlog>=25.1,<26",
  "typer>=0.16,<1",
]

[project.optional-dependencies]
dev = [
  "mypy>=1.14,<2",
  "pytest>=8.3,<9",
  "pytest-asyncio>=0.25,<1",
  "respx>=0.22,<1",
  "ruff>=0.9,<1",
]

[project.scripts]
cml-market-data = "crypto_momentum_lab.apps.market_data.main:app"

[tool.hatch.build.targets.wheel]
packages = ["src/crypto_momentum_lab"]

[tool.pytest.ini_options]
addopts = "-ra"
asyncio_mode = "auto"
testpaths = ["tests"]
markers = [
  "integration: requires the local PostgreSQL test service",
  "e2e: exercises a full application slice",
]

[tool.ruff]
line-length = 88
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "ASYNC"]

[tool.ruff.lint.flake8-bugbear]
extend-immutable-calls = ["typer.Option"]

[tool.mypy]
python_version = "3.12"
strict = true
packages = ["crypto_momentum_lab"]

[[tool.mypy.overrides]]
module = ["yaml"]
ignore_missing_imports = true
```

```python
# src/crypto_momentum_lab/__init__.py
__version__ = "0.1.0"
```

Create empty parent package files:

```python
# src/crypto_momentum_lab/apps/__init__.py
# src/crypto_momentum_lab/domain/__init__.py
# src/crypto_momentum_lab/market_data/__init__.py
# src/crypto_momentum_lab/persistence/__init__.py
```

```dotenv
# .env.example
CML_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml
CML_ENVIRONMENT_CONFIG=configs/environments/research.yaml
```

Append these entries to `.gitignore`:

```gitignore
.env
.mypy_cache/
.pytest_cache/
.ruff_cache/
.venv/
__pycache__/
*.egg-info/
data/
runs/
```

Create `README.md` with:

```markdown
# Crypto Momentum Lab

Research and trading infrastructure for independent short-horizon momentum
strategies on Binance USD-M perpetual futures.

The approved architecture is documented in
`docs/superpowers/specs/2026-06-14-project-architecture-design.md`.
```

- [ ] **Step 4: Install the editable package**

Run:

```bash
.venv/bin/python -m pip install -e ".[dev]"
```

Expected: installation succeeds and reports `crypto-momentum-lab-0.1.0`.

- [ ] **Step 5: Run the package test and static checks**

Run:

```bash
.venv/bin/python -m pytest tests/unit/test_package.py -v
.venv/bin/ruff check .
.venv/bin/mypy src
```

Expected: one test passes; Ruff and mypy exit with status 0.

- [ ] **Step 6: Commit the foundation**

```bash
git add pyproject.toml README.md .env.example .gitignore src tests/unit/test_package.py
git commit -m "build: initialize Python project"
```

### Task 2: Immutable Configuration and Behavior Hash

**Files:**
- Create: `configs/environments/research.yaml`
- Create: `configs/universe/utc_day_top_bottom.yaml`
- Create: `src/crypto_momentum_lab/config/__init__.py`
- Create: `src/crypto_momentum_lab/config/models.py`
- Create: `src/crypto_momentum_lab/config/loader.py`
- Create: `tests/unit/config/test_loader.py`

- [ ] **Step 1: Write failing configuration tests**

```python
# tests/unit/config/test_loader.py
from pathlib import Path

import pytest
from pydantic import ValidationError

from crypto_momentum_lab.config.loader import (
    behavior_hash,
    load_runtime_config,
)


def test_load_runtime_config_is_frozen_and_hash_is_stable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    universe_path = tmp_path / "universe.yaml"
    universe_path.write_text(
        "\n".join(
            [
                "top_count: 20",
                "retention_rank: 30",
                "retention_hours: 2",
                "activation_minute: 1",
            ]
        ),
        encoding="utf-8",
    )
    environment_path = tmp_path / "research.yaml"
    environment_path.write_text(
        "\n".join(
            [
                "environment: research",
                "binance_base_url: https://fapi.binance.com",
                f"universe_config: {universe_path}",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "CML_DATABASE_URL",
        "postgresql+asyncpg://user:secret@localhost/db",
    )

    first = load_runtime_config(environment_path)
    second = load_runtime_config(environment_path)

    assert first.universe.top_count == 20
    assert behavior_hash(first) == behavior_hash(second)
    assert "secret" not in behavior_hash(first)
    with pytest.raises(ValidationError):
        first.universe.top_count = 10  # type: ignore[misc]


def test_rejects_retention_rank_smaller_than_target_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    universe_path = tmp_path / "universe.yaml"
    universe_path.write_text(
        "top_count: 20\nretention_rank: 10\nretention_hours: 2\n"
        "activation_minute: 1\n",
        encoding="utf-8",
    )
    environment_path = tmp_path / "research.yaml"
    environment_path.write_text(
        "environment: research\n"
        "binance_base_url: https://fapi.binance.com\n"
        f"universe_config: {universe_path}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "CML_DATABASE_URL",
        "postgresql+asyncpg://user:secret@localhost/db",
    )

    with pytest.raises(ValidationError):
        load_runtime_config(environment_path)
```

- [ ] **Step 2: Run the tests and verify imports fail**

Run:

```bash
.venv/bin/python -m pytest tests/unit/config/test_loader.py -v
```

Expected: collection fails because `crypto_momentum_lab.config` does not exist.

- [ ] **Step 3: Implement frozen configuration models**

```python
# src/crypto_momentum_lab/config/models.py
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


class UniverseConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    top_count: int = Field(gt=0)
    retention_rank: int = Field(gt=0)
    retention_hours: int = Field(gt=0)
    activation_minute: int = Field(ge=0, le=59)

    @model_validator(mode="after")
    def validate_retention_rank(self) -> "UniverseConfig":
        if self.retention_rank < self.top_count:
            raise ValueError("retention_rank must be >= top_count")
        return self


class EnvironmentFile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    environment: str
    binance_base_url: HttpUrl
    universe_config: Path


class RuntimeConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    environment: str
    database_url: str
    binance_base_url: HttpUrl
    universe: UniverseConfig
```

```python
# src/crypto_momentum_lab/config/loader.py
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import yaml

from crypto_momentum_lab.config.models import (
    EnvironmentFile,
    RuntimeConfig,
    UniverseConfig,
)


def _read_yaml(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return loaded


def load_runtime_config(environment_path: Path) -> RuntimeConfig:
    environment = EnvironmentFile.model_validate(_read_yaml(environment_path))
    database_url = os.environ["CML_DATABASE_URL"]
    universe = UniverseConfig.model_validate(_read_yaml(environment.universe_config))
    return RuntimeConfig(
        environment=environment.environment,
        database_url=database_url,
        binance_base_url=environment.binance_base_url,
        universe=universe,
    )


def behavior_hash(config: RuntimeConfig) -> str:
    payload = {
        "environment": config.environment,
        "binance_base_url": str(config.binance_base_url),
        "universe": config.universe.model_dump(mode="json"),
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()
```

```python
# src/crypto_momentum_lab/config/__init__.py
from crypto_momentum_lab.config.loader import behavior_hash, load_runtime_config
from crypto_momentum_lab.config.models import RuntimeConfig, UniverseConfig

__all__ = [
    "RuntimeConfig",
    "UniverseConfig",
    "behavior_hash",
    "load_runtime_config",
]
```

```yaml
# configs/universe/utc_day_top_bottom.yaml
top_count: 20
retention_rank: 30
retention_hours: 2
activation_minute: 1
```

```yaml
# configs/environments/research.yaml
environment: research
binance_base_url: https://fapi.binance.com
universe_config: configs/universe/utc_day_top_bottom.yaml
```

- [ ] **Step 4: Run the configuration tests and static checks**

Run:

```bash
.venv/bin/python -m pytest tests/unit/config/test_loader.py -v
.venv/bin/ruff check src/crypto_momentum_lab/config tests/unit/config
.venv/bin/mypy src/crypto_momentum_lab/config
```

Expected: two tests pass; Ruff and mypy exit with status 0.

- [ ] **Step 5: Commit configuration**

```bash
git add configs src/crypto_momentum_lab/config tests/unit/config
git commit -m "feat: add immutable runtime configuration"
```

### Task 3: UTC-Day Ranking Domain

**Files:**
- Create: `src/crypto_momentum_lab/domain/universe/__init__.py`
- Create: `src/crypto_momentum_lab/domain/universe/models.py`
- Create: `src/crypto_momentum_lab/domain/universe/ranking.py`
- Create: `tests/unit/domain/universe/test_ranking.py`

- [ ] **Step 1: Write failing ranking tests**

```python
# tests/unit/domain/universe/test_ranking.py
from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.universe.models import MarketCandidate
from crypto_momentum_lab.domain.universe.ranking import rank_utc_day_returns


def candidate(
    symbol: str,
    open_price: str | None,
    current_price: str | None,
) -> MarketCandidate:
    now = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    return MarketCandidate(
        symbol=symbol,
        open_price=None if open_price is None else Decimal(open_price),
        current_price=None if current_price is None else Decimal(current_price),
        price_time=now,
    )


def test_ranks_gainers_and_losers_with_deterministic_ties() -> None:
    result = rank_utc_day_returns(
        [
            candidate("CCCUSDT", "100", "110"),
            candidate("AAAUSDT", "100", "110"),
            candidate("BBBUSDT", "100", "90"),
            candidate("DDDUSDT", "100", "95"),
        ],
        top_count=2,
        ranking_depth=2,
    )

    assert [entry.symbol for entry in result.gainers] == ["AAAUSDT", "CCCUSDT"]
    assert [entry.symbol for entry in result.losers] == ["BBBUSDT", "DDDUSDT"]
    assert result.target_symbols == frozenset(
        {"AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT"}
    )


def test_excludes_missing_or_non_positive_prices_with_reason() -> None:
    result = rank_utc_day_returns(
        [
            candidate("GOODUSDT", "10", "11"),
            candidate("NOOPENUSDT", None, "11"),
            candidate("NOPRICEUSDT", "10", None),
            candidate("ZEROOPENUSDT", "0", "1"),
        ],
        top_count=20,
        ranking_depth=30,
    )

    assert result.target_symbols == frozenset({"GOODUSDT"})
    assert result.exclusions == {
        "NOOPENUSDT": "missing_open_price",
        "NOPRICEUSDT": "missing_current_price",
        "ZEROOPENUSDT": "non_positive_open_price",
    }


def test_small_population_deduplicates_target_union() -> None:
    result = rank_utc_day_returns(
        [
            candidate("AAAUSDT", "100", "101"),
            candidate("BBBUSDT", "100", "99"),
        ],
        top_count=20,
        ranking_depth=30,
    )

    assert result.target_symbols == frozenset({"AAAUSDT", "BBBUSDT"})
```

- [ ] **Step 2: Run tests and verify the domain is missing**

Run:

```bash
.venv/bin/python -m pytest tests/unit/domain/universe/test_ranking.py -v
```

Expected: collection fails on the missing universe domain modules.

- [ ] **Step 3: Implement domain types and ranking**

```python
# src/crypto_momentum_lab/domain/universe/models.py
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID


class RankingSide(StrEnum):
    GAINER = "gainer"
    LOSER = "loser"


class MembershipStatus(StrEnum):
    TARGET = "target"
    RETAINED = "retained"
    FORCED = "forced"


@dataclass(frozen=True, slots=True)
class ContractMetadata:
    symbol: str
    contract_type: str
    status: str
    quote_asset: str
    margin_asset: str
    onboard_at: datetime
    raw: dict[str, object]


@dataclass(frozen=True, slots=True)
class DailyOpen:
    symbol: str
    utc_day: date
    open_price: Decimal
    open_time: datetime


@dataclass(frozen=True, slots=True)
class MarketCandidate:
    symbol: str
    open_price: Decimal | None
    current_price: Decimal | None
    price_time: datetime | None


@dataclass(frozen=True, slots=True)
class RankEntry:
    symbol: str
    utc_day_return: Decimal
    rank: int
    side: RankingSide


@dataclass(frozen=True, slots=True)
class RankingResult:
    candidates: tuple[MarketCandidate, ...]
    gainers: tuple[RankEntry, ...]
    losers: tuple[RankEntry, ...]
    target_symbols: frozenset[str]
    exclusions: dict[str, str]


@dataclass(frozen=True, slots=True)
class TrackedMembership:
    symbol: str
    status: MembershipStatus
    side: RankingSide | None
    left_target_at: datetime | None


@dataclass(frozen=True, slots=True)
class UniverseSnapshot:
    snapshot_id: UUID
    observed_at: datetime
    utc_day: date
    config_hash: str
    activated: bool
    ranking: RankingResult
    memberships: tuple[TrackedMembership, ...]
```

```python
# src/crypto_momentum_lab/domain/universe/ranking.py
from decimal import Decimal

from crypto_momentum_lab.domain.universe.models import (
    MarketCandidate,
    RankEntry,
    RankingResult,
    RankingSide,
)


def rank_utc_day_returns(
    candidates: list[MarketCandidate],
    *,
    top_count: int,
    ranking_depth: int,
) -> RankingResult:
    if ranking_depth < top_count:
        raise ValueError("ranking_depth must be >= top_count")
    valid: list[tuple[MarketCandidate, Decimal]] = []
    exclusions: dict[str, str] = {}

    for candidate in sorted(candidates, key=lambda item: item.symbol):
        if candidate.open_price is None:
            exclusions[candidate.symbol] = "missing_open_price"
            continue
        if candidate.current_price is None:
            exclusions[candidate.symbol] = "missing_current_price"
            continue
        if candidate.open_price <= 0:
            exclusions[candidate.symbol] = "non_positive_open_price"
            continue
        if candidate.current_price <= 0:
            exclusions[candidate.symbol] = "non_positive_current_price"
            continue
        day_return = candidate.current_price / candidate.open_price - Decimal(1)
        valid.append((candidate, day_return))

    descending = sorted(valid, key=lambda item: (-item[1], item[0].symbol))
    ascending = sorted(valid, key=lambda item: (item[1], item[0].symbol))

    gainers = tuple(
        RankEntry(
            symbol=candidate.symbol,
            utc_day_return=day_return,
            rank=index,
            side=RankingSide.GAINER,
        )
        for index, (candidate, day_return) in enumerate(
            descending[:ranking_depth],
            start=1,
        )
    )
    losers = tuple(
        RankEntry(
            symbol=candidate.symbol,
            utc_day_return=day_return,
            rank=index,
            side=RankingSide.LOSER,
        )
        for index, (candidate, day_return) in enumerate(
            ascending[:ranking_depth],
            start=1,
        )
    )
    target_symbols = frozenset(
        entry.symbol
        for entry in (*gainers[:top_count], *losers[:top_count])
    )
    return RankingResult(
        candidates=tuple(sorted(candidates, key=lambda item: item.symbol)),
        gainers=gainers,
        losers=losers,
        target_symbols=target_symbols,
        exclusions=exclusions,
    )
```

```python
# src/crypto_momentum_lab/domain/universe/__init__.py
from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
    MarketCandidate,
    MembershipStatus,
    RankEntry,
    RankingResult,
    RankingSide,
    TrackedMembership,
    UniverseSnapshot,
)
from crypto_momentum_lab.domain.universe.ranking import rank_utc_day_returns

__all__ = [
    "ContractMetadata",
    "DailyOpen",
    "MarketCandidate",
    "MembershipStatus",
    "RankEntry",
    "RankingResult",
    "RankingSide",
    "TrackedMembership",
    "UniverseSnapshot",
    "rank_utc_day_returns",
]
```

- [ ] **Step 4: Run ranking tests and static checks**

Run:

```bash
.venv/bin/python -m pytest tests/unit/domain/universe/test_ranking.py -v
.venv/bin/ruff check src/crypto_momentum_lab/domain tests/unit/domain
.venv/bin/mypy src/crypto_momentum_lab/domain
```

Expected: three tests pass; Ruff and mypy exit with status 0.

- [ ] **Step 5: Commit ranking**

```bash
git add src/crypto_momentum_lab/domain tests/unit/domain
git commit -m "feat: add deterministic UTC-day ranking"
```

### Task 4: Monitoring Membership and Retention

**Files:**
- Create: `src/crypto_momentum_lab/domain/universe/membership.py`
- Create: `tests/unit/domain/universe/test_membership.py`
- Modify: `src/crypto_momentum_lab/domain/universe/__init__.py`

- [ ] **Step 1: Write failing membership tests**

```python
# tests/unit/domain/universe/test_membership.py
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.universe.membership import (
    build_monitoring_memberships,
)
from crypto_momentum_lab.domain.universe.models import (
    MembershipStatus,
    RankEntry,
    RankingResult,
    RankingSide,
    TrackedMembership,
)


NOW = datetime(2026, 6, 14, 12, 1, tzinfo=UTC)


def result(
    gainers: list[str],
    losers: list[str],
) -> RankingResult:
    gain_entries = tuple(
        RankEntry(symbol, Decimal("0.1"), rank, RankingSide.GAINER)
        for rank, symbol in enumerate(gainers, start=1)
    )
    loss_entries = tuple(
        RankEntry(symbol, Decimal("-0.1"), rank, RankingSide.LOSER)
        for rank, symbol in enumerate(losers, start=1)
    )
    return RankingResult(
        candidates=(),
        gainers=gain_entries,
        losers=loss_entries,
        target_symbols=frozenset(gainers[:2] + losers[:2]),
        exclusions={},
    )


def test_current_target_is_immediately_monitored() -> None:
    memberships = build_monitoring_memberships(
        result(["A", "B", "C"], ["X", "Y", "Z"]),
        previous={},
        forced_symbols=frozenset(),
        observed_at=NOW,
        retention_rank=3,
        retention_duration=timedelta(hours=2),
    )

    assert memberships["A"].status is MembershipStatus.TARGET
    assert memberships["X"].status is MembershipStatus.TARGET


def test_symbol_is_retained_until_rank_or_time_limit_is_breached() -> None:
    previous = {
        "A": TrackedMembership(
            symbol="A",
            status=MembershipStatus.TARGET,
            side=RankingSide.GAINER,
            left_target_at=None,
        )
    }
    first_exit = build_monitoring_memberships(
        result(["B", "C", "A"], ["X", "Y", "Z"]),
        previous=previous,
        forced_symbols=frozenset(),
        observed_at=NOW,
        retention_rank=3,
        retention_duration=timedelta(hours=2),
    )
    expired = build_monitoring_memberships(
        result(["B", "C", "A"], ["X", "Y", "Z"]),
        previous=first_exit,
        forced_symbols=frozenset(),
        observed_at=NOW + timedelta(hours=2),
        retention_rank=3,
        retention_duration=timedelta(hours=2),
    )

    assert first_exit["A"].status is MembershipStatus.RETAINED
    assert first_exit["A"].left_target_at == NOW
    assert "A" not in expired


def test_forced_symbol_is_monitored_without_target_membership() -> None:
    memberships = build_monitoring_memberships(
        result(["A", "B"], ["X", "Y"]),
        previous={},
        forced_symbols=frozenset({"POSITIONUSDT"}),
        observed_at=NOW,
        retention_rank=3,
        retention_duration=timedelta(hours=2),
    )

    assert memberships["POSITIONUSDT"].status is MembershipStatus.FORCED
```

- [ ] **Step 2: Run tests and verify membership code is missing**

Run:

```bash
.venv/bin/python -m pytest tests/unit/domain/universe/test_membership.py -v
```

Expected: collection fails because `membership.py` does not exist.

- [ ] **Step 3: Implement retention and forced membership**

```python
# src/crypto_momentum_lab/domain/universe/membership.py
from datetime import datetime, timedelta

from crypto_momentum_lab.domain.universe.models import (
    MembershipStatus,
    RankingResult,
    RankingSide,
    TrackedMembership,
)


def _target_side(result: RankingResult, symbol: str) -> RankingSide:
    gainer = next((entry for entry in result.gainers if entry.symbol == symbol), None)
    loser = next((entry for entry in result.losers if entry.symbol == symbol), None)
    if gainer is not None and loser is not None:
        return (
            RankingSide.GAINER
            if gainer.utc_day_return >= 0
            else RankingSide.LOSER
        )
    if gainer is not None:
        return RankingSide.GAINER
    return RankingSide.LOSER


def _rank_for_side(
    result: RankingResult,
    symbol: str,
    side: RankingSide,
) -> int | None:
    entries = result.gainers if side is RankingSide.GAINER else result.losers
    entry = next((item for item in entries if item.symbol == symbol), None)
    return None if entry is None else entry.rank


def build_monitoring_memberships(
    result: RankingResult,
    *,
    previous: dict[str, TrackedMembership],
    forced_symbols: frozenset[str],
    observed_at: datetime,
    retention_rank: int,
    retention_duration: timedelta,
) -> dict[str, TrackedMembership]:
    memberships: dict[str, TrackedMembership] = {}

    for symbol in sorted(result.target_symbols):
        memberships[symbol] = TrackedMembership(
            symbol=symbol,
            status=MembershipStatus.TARGET,
            side=_target_side(result, symbol),
            left_target_at=None,
        )

    for symbol, old in sorted(previous.items()):
        if symbol in memberships or old.side is None:
            continue
        left_target_at = old.left_target_at or observed_at
        rank = _rank_for_side(result, symbol, old.side)
        if (
            rank is not None
            and rank <= retention_rank
            and observed_at - left_target_at < retention_duration
        ):
            memberships[symbol] = TrackedMembership(
                symbol=symbol,
                status=MembershipStatus.RETAINED,
                side=old.side,
                left_target_at=left_target_at,
            )

    for symbol in sorted(forced_symbols):
        if symbol not in memberships:
            old = previous.get(symbol)
            memberships[symbol] = TrackedMembership(
                symbol=symbol,
                status=MembershipStatus.FORCED,
                side=None if old is None else old.side,
                left_target_at=None if old is None else old.left_target_at,
            )

    return memberships
```

Export `build_monitoring_memberships` from
`src/crypto_momentum_lab/domain/universe/__init__.py`.

- [ ] **Step 4: Run membership and ranking tests**

Run:

```bash
.venv/bin/python -m pytest tests/unit/domain/universe -v
.venv/bin/ruff check src/crypto_momentum_lab/domain tests/unit/domain
.venv/bin/mypy src/crypto_momentum_lab/domain
```

Expected: all universe domain tests pass.

- [ ] **Step 5: Commit membership**

```bash
git add src/crypto_momentum_lab/domain/universe tests/unit/domain/universe
git commit -m "feat: add universe retention membership"
```

### Task 5: PostgreSQL Schema and Alembic Migration

**Files:**
- Create: `compose.yaml`
- Create: `alembic.ini`
- Create: `alembic/env.py`
- Create: `alembic/versions/20260614_0001_universe_foundation.py`
- Create: `src/crypto_momentum_lab/persistence/postgres/__init__.py`
- Create: `src/crypto_momentum_lab/persistence/postgres/base.py`
- Create: `src/crypto_momentum_lab/persistence/postgres/models.py`
- Create: `src/crypto_momentum_lab/persistence/postgres/session.py`
- Create: `tests/integration/conftest.py`
- Create: `tests/integration/persistence/test_migrations.py`

- [ ] **Step 1: Add a local PostgreSQL service**

```yaml
# compose.yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: cml
      POSTGRES_USER: cml
      POSTGRES_PASSWORD: cml
    ports:
      - "54329:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U cml -d cml"]
      interval: 2s
      timeout: 2s
      retries: 20
    volumes:
      - postgres-data:/var/lib/postgresql/data

volumes:
  postgres-data:
```

- [ ] **Step 2: Write the failing migration test**

```python
# tests/integration/persistence/test_migrations.py
from sqlalchemy import inspect

from crypto_momentum_lab.persistence.postgres.session import create_sync_engine


def test_initial_migration_creates_universe_tables(database_url: str) -> None:
    engine = create_sync_engine(database_url)
    try:
        table_names = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()

    assert {
        "contract_metadata",
        "daily_open_prices",
        "universe_snapshots",
        "universe_entries",
        "monitoring_memberships",
    } <= table_names
```

```python
# tests/integration/conftest.py
import os

import pytest


@pytest.fixture(scope="session")
def database_url() -> str:
    return os.environ.get(
        "CML_TEST_DATABASE_URL",
        "postgresql+psycopg://cml:cml@localhost:54329/cml",
    )
```

`psycopg[binary]` is already a runtime dependency because the production
`migrate` container uses it.

- [ ] **Step 3: Start PostgreSQL and verify the test fails before migration**

Run:

```bash
docker compose up -d postgres
docker compose ps
.venv/bin/python -m pip install -e ".[dev]"
.venv/bin/python -m pytest tests/integration/persistence/test_migrations.py -v
```

Expected: PostgreSQL reports healthy and the test fails because the tables do
not exist.

- [ ] **Step 4: Define SQLAlchemy metadata**

```python
# src/crypto_momentum_lab/persistence/postgres/base.py
from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase


NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
```

```python
# src/crypto_momentum_lab/persistence/postgres/models.py
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from crypto_momentum_lab.persistence.postgres.base import Base


class ContractMetadataRow(Base):
    __tablename__ = "contract_metadata"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    effective_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
    )
    contract_type: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(32))
    quote_asset: Mapped[str] = mapped_column(String(16))
    margin_asset: Mapped[str] = mapped_column(String(16))
    onboard_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    raw_payload: Mapped[dict[str, object]] = mapped_column(JSONB)


class DailyOpenRow(Base):
    __tablename__ = "daily_open_prices"

    utc_day: Mapped[date] = mapped_column(Date, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    open_price: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    open_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class UniverseSnapshotRow(Base):
    __tablename__ = "universe_snapshots"

    snapshot_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        unique=True,
    )
    utc_day: Mapped[date] = mapped_column(Date)
    config_hash: Mapped[str] = mapped_column(String(64))
    activated: Mapped[bool] = mapped_column(Boolean)


class UniverseEntryRow(Base):
    __tablename__ = "universe_entries"

    snapshot_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("universe_snapshots.snapshot_id", ondelete="CASCADE"),
        primary_key=True,
    )
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    open_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    current_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    price_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    utc_day_return: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    gainer_rank: Mapped[int | None] = mapped_column(Integer)
    loser_rank: Mapped[int | None] = mapped_column(Integer)
    is_target: Mapped[bool] = mapped_column(Boolean)
    exclusion_reason: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        Index("ix_universe_entries_snapshot_target", "snapshot_id", "is_target"),
    )


class MonitoringMembershipRow(Base):
    __tablename__ = "monitoring_memberships"

    snapshot_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("universe_snapshots.snapshot_id", ondelete="CASCADE"),
        primary_key=True,
    )
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    status: Mapped[str] = mapped_column(String(16))
    side: Mapped[str | None] = mapped_column(String(16))
    left_target_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )

```

```python
# src/crypto_momentum_lab/persistence/postgres/session.py
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


def create_async_database_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(database_url, pool_pre_ping=True)


def create_sync_engine(database_url: str) -> Engine:
    return create_engine(database_url, pool_pre_ping=True)
```

- [ ] **Step 5: Configure Alembic and create the explicit migration**

```ini
# alembic.ini
[alembic]
script_location = alembic
prepend_sys_path = .

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console
qualname =

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
datefmt = %H:%M:%S
```

```python
# alembic/env.py
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from crypto_momentum_lab.persistence.postgres.base import Base
from crypto_momentum_lab.persistence.postgres import models  # noqa: F401


config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def sync_database_url() -> str:
    return os.environ["CML_DATABASE_URL"].replace(
        "postgresql+asyncpg",
        "postgresql+psycopg",
    )


def run_migrations_offline() -> None:
    context.configure(
        url=sync_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = sync_database_url()
    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

```python
# alembic/versions/20260614_0001_universe_foundation.py
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260614_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "contract_metadata",
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("effective_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("contract_type", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("quote_asset", sa.String(16), nullable=False),
        sa.Column("margin_asset", sa.String(16), nullable=False),
        sa.Column("onboard_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_payload", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint(
            "symbol",
            "effective_at",
            name="pk_contract_metadata",
        ),
    )
    op.create_table(
        "daily_open_prices",
        sa.Column("utc_day", sa.Date(), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("open_price", sa.Numeric(38, 18), nullable=False),
        sa.Column("open_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "utc_day",
            "symbol",
            name="pk_daily_open_prices",
        ),
    )
    op.create_table(
        "universe_snapshots",
        sa.Column(
            "snapshot_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("utc_day", sa.Date(), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("activated", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint(
            "snapshot_id",
            name="pk_universe_snapshots",
        ),
        sa.UniqueConstraint(
            "observed_at",
            name="uq_universe_snapshots_observed_at",
        ),
    )
    op.create_table(
        "universe_entries",
        sa.Column(
            "snapshot_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("open_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("current_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("price_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("utc_day_return", sa.Numeric(38, 18), nullable=True),
        sa.Column("gainer_rank", sa.Integer(), nullable=True),
        sa.Column("loser_rank", sa.Integer(), nullable=True),
        sa.Column("is_target", sa.Boolean(), nullable=False),
        sa.Column("exclusion_reason", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["universe_snapshots.snapshot_id"],
            name="fk_universe_entries_snapshot_id_universe_snapshots",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "snapshot_id",
            "symbol",
            name="pk_universe_entries",
        ),
    )
    op.create_index(
        "ix_universe_entries_snapshot_target",
        "universe_entries",
        ["snapshot_id", "is_target"],
    )
    op.create_table(
        "monitoring_memberships",
        sa.Column(
            "snapshot_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("side", sa.String(16), nullable=True),
        sa.Column(
            "left_target_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["universe_snapshots.snapshot_id"],
            name="fk_monitoring_memberships_snapshot_id_universe_snapshots",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "snapshot_id",
            "symbol",
            name="pk_monitoring_memberships",
        ),
    )


def downgrade() -> None:
    op.drop_table("monitoring_memberships")
    op.drop_index(
        "ix_universe_entries_snapshot_target",
        table_name="universe_entries",
    )
    op.drop_table("universe_entries")
    op.drop_table("universe_snapshots")
    op.drop_table("daily_open_prices")
    op.drop_table("contract_metadata")
```

Memberships deliberately reference only their snapshot: a forced monitoring
obligation can refer to an open position whose contract is no longer in the
active ranking population.

- [ ] **Step 6: Apply and verify the migration**

Run:

```bash
export CML_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml
.venv/bin/alembic upgrade head
.venv/bin/python -m pytest tests/integration/persistence/test_migrations.py -v
.venv/bin/alembic downgrade base
.venv/bin/alembic upgrade head
```

Expected: the migration test passes; downgrade and re-upgrade both succeed.

- [ ] **Step 7: Run quality checks and commit**

Run:

```bash
.venv/bin/ruff check alembic src/crypto_momentum_lab/persistence tests/integration
.venv/bin/mypy src/crypto_momentum_lab/persistence
```

Expected: both commands exit with status 0.

```bash
git add pyproject.toml compose.yaml alembic.ini alembic \
  src/crypto_momentum_lab/persistence tests/integration
git commit -m "feat: add universe persistence schema"
```

### Task 6: PostgreSQL Universe Repository

**Files:**
- Create: `src/crypto_momentum_lab/domain/universe/ports.py`
- Create: `src/crypto_momentum_lab/persistence/postgres/repository.py`
- Create: `tests/integration/persistence/test_repository.py`
- Modify: `src/crypto_momentum_lab/persistence/postgres/__init__.py`

- [ ] **Step 1: Define repository behavior with failing tests**

```python
# tests/integration/persistence/test_repository.py
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from crypto_momentum_lab.domain.universe.models import (
    MarketCandidate,
    MembershipStatus,
    RankEntry,
    RankingResult,
    RankingSide,
    TrackedMembership,
    UniverseSnapshot,
)
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)


async def test_save_snapshot_is_idempotent(
    repository: PostgresUniverseRepository,
) -> None:
    observed_at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    snapshot = UniverseSnapshot(
        snapshot_id=uuid4(),
        observed_at=observed_at,
        utc_day=observed_at.date(),
        config_hash="a" * 64,
        activated=True,
        ranking=RankingResult(
            candidates=(
                MarketCandidate(
                    "BTCUSDT",
                    Decimal("100"),
                    Decimal("110"),
                    observed_at,
                ),
            ),
            gainers=(
                RankEntry(
                    "BTCUSDT",
                    Decimal("0.1"),
                    1,
                    RankingSide.GAINER,
                ),
            ),
            losers=(
                RankEntry(
                    "BTCUSDT",
                    Decimal("0.1"),
                    1,
                    RankingSide.LOSER,
                ),
            ),
            target_symbols=frozenset({"BTCUSDT"}),
            exclusions={},
        ),
        memberships=(
            TrackedMembership(
                "BTCUSDT",
                MembershipStatus.TARGET,
                RankingSide.GAINER,
                None,
            ),
        ),
    )

    await repository.save_snapshot(snapshot)
    await repository.save_snapshot(snapshot)
    loaded = await repository.load_snapshot(observed_at)

    assert loaded is not None
    assert loaded.snapshot_id == snapshot.snapshot_id
    assert loaded.memberships == snapshot.memberships


async def test_load_active_memberships_ignores_unactivated_snapshot(
    repository: PostgresUniverseRepository,
    snapshot_factory,
) -> None:
    active = snapshot_factory(
        day=14,
        hour=23,
        activated=True,
        symbol="BTCUSDT",
    )
    midnight = snapshot_factory(
        day=15,
        hour=0,
        activated=False,
        symbol="ETHUSDT",
    )

    await repository.save_snapshot(active)
    await repository.save_snapshot(midnight)

    memberships = await repository.load_active_memberships()

    assert set(memberships) == {"BTCUSDT"}
```

Extend `tests/integration/conftest.py` with:

```python
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from decimal import Decimal
import os
from uuid import NAMESPACE_URL, uuid5

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.universe.models import (
    MarketCandidate,
    MembershipStatus,
    RankEntry,
    RankingResult,
    RankingSide,
    TrackedMembership,
    UniverseSnapshot,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ContractMetadataRow,
    DailyOpenRow,
    MonitoringMembershipRow,
    UniverseEntryRow,
    UniverseSnapshotRow,
)
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


@pytest.fixture(scope="session")
def async_database_url() -> str:
    return os.environ.get(
        "CML_TEST_ASYNC_DATABASE_URL",
        "postgresql+asyncpg://cml:cml@localhost:54329/cml",
    )


@pytest.fixture
async def repository(
    async_database_url: str,
) -> AsyncIterator[PostgresUniverseRepository]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            for model in (
                MonitoringMembershipRow,
                UniverseEntryRow,
                UniverseSnapshotRow,
                DailyOpenRow,
                ContractMetadataRow,
            ):
                await session.execute(delete(model))
    yield PostgresUniverseRepository(factory)
    await engine.dispose()


@pytest.fixture
def snapshot_factory() -> Callable[..., UniverseSnapshot]:
    def build(
        *,
        day: int,
        hour: int,
        activated: bool,
        symbol: str,
    ) -> UniverseSnapshot:
        observed_at = datetime(2026, 6, day, hour, 1, tzinfo=UTC)
        day_return = Decimal("0.1")
        candidate = MarketCandidate(
            symbol,
            Decimal("100"),
            Decimal("110"),
            observed_at,
        )
        return UniverseSnapshot(
            snapshot_id=uuid5(
                NAMESPACE_URL,
                f"test:{observed_at.isoformat()}:{symbol}",
            ),
            observed_at=observed_at,
            utc_day=observed_at.date(),
            config_hash="a" * 64,
            activated=activated,
            ranking=RankingResult(
                candidates=(candidate,),
                gainers=(
                    RankEntry(
                        symbol,
                        day_return,
                        1,
                        RankingSide.GAINER,
                    ),
                ),
                losers=(
                    RankEntry(
                        symbol,
                        day_return,
                        1,
                        RankingSide.LOSER,
                    ),
                ),
                target_symbols=frozenset({symbol}),
                exclusions={},
            ),
            memberships=(
                TrackedMembership(
                    symbol,
                    MembershipStatus.TARGET,
                    RankingSide.GAINER,
                    None,
                ),
            ),
        )

    return build
```

- [ ] **Step 2: Run tests and verify repository is missing**

Run:

```bash
.venv/bin/python -m pytest \
  tests/integration/persistence/test_repository.py -v
```

Expected: collection fails because the repository is not implemented.

- [ ] **Step 3: Define the repository port**

```python
# src/crypto_momentum_lab/domain/universe/ports.py
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol

from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
    TrackedMembership,
    UniverseSnapshot,
)


class UniverseRepository(Protocol):
    async def save_contract_metadata(
        self,
        contracts: tuple[ContractMetadata, ...],
        *,
        effective_at: datetime,
    ) -> None: ...

    async def load_daily_opens(
        self,
        utc_day: date,
        symbols: frozenset[str],
    ) -> dict[str, Decimal]: ...

    async def save_daily_opens(
        self,
        opens: tuple[DailyOpen, ...],
        *,
        captured_at: datetime,
    ) -> None: ...

    async def load_active_memberships(
        self,
    ) -> dict[str, TrackedMembership]: ...

    async def save_snapshot(self, snapshot: UniverseSnapshot) -> None: ...

    async def load_snapshot(
        self,
        observed_at: datetime,
    ) -> UniverseSnapshot | None: ...
```

- [ ] **Step 4: Implement transactional persistence**

```python
# src/crypto_momentum_lab/persistence/postgres/repository.py
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
    MarketCandidate,
    MembershipStatus,
    RankEntry,
    RankingResult,
    RankingSide,
    TrackedMembership,
    UniverseSnapshot,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ContractMetadataRow,
    DailyOpenRow,
    MonitoringMembershipRow,
    UniverseEntryRow,
    UniverseSnapshotRow,
)


def _return_by_symbol(snapshot: UniverseSnapshot) -> dict[str, Decimal]:
    return {
        entry.symbol: entry.utc_day_return
        for entry in (*snapshot.ranking.gainers, *snapshot.ranking.losers)
    }


def _rank_by_symbol(
    entries: tuple[RankEntry, ...],
) -> dict[str, int]:
    return {entry.symbol: entry.rank for entry in entries}


def _candidate_return(candidate: MarketCandidate) -> Decimal | None:
    if (
        candidate.open_price is None
        or candidate.current_price is None
        or candidate.open_price <= 0
        or candidate.current_price <= 0
    ):
        return None
    return candidate.current_price / candidate.open_price - Decimal(1)


class PostgresUniverseRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def save_contract_metadata(
        self,
        contracts: tuple[ContractMetadata, ...],
        *,
        effective_at: datetime,
    ) -> None:
        if not contracts:
            return
        statement = insert(ContractMetadataRow).values(
            [
                {
                    "symbol": item.symbol,
                    "effective_at": effective_at,
                    "contract_type": item.contract_type,
                    "status": item.status,
                    "quote_asset": item.quote_asset,
                    "margin_asset": item.margin_asset,
                    "onboard_at": item.onboard_at,
                    "raw_payload": item.raw,
                }
                for item in contracts
            ]
        )
        statement = statement.on_conflict_do_update(
            index_elements=["symbol", "effective_at"],
            set_={
                "contract_type": statement.excluded.contract_type,
                "status": statement.excluded.status,
                "quote_asset": statement.excluded.quote_asset,
                "margin_asset": statement.excluded.margin_asset,
                "onboard_at": statement.excluded.onboard_at,
                "raw_payload": statement.excluded.raw_payload,
            },
        )
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(statement)

    async def load_daily_opens(
        self,
        utc_day: date,
        symbols: frozenset[str],
    ) -> dict[str, Decimal]:
        if not symbols:
            return {}
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    select(DailyOpenRow).where(
                        DailyOpenRow.utc_day == utc_day,
                        DailyOpenRow.symbol.in_(symbols),
                    )
                )
            ).scalars()
            return {row.symbol: row.open_price for row in rows}

    async def save_daily_opens(
        self,
        opens: tuple[DailyOpen, ...],
        *,
        captured_at: datetime,
    ) -> None:
        if not opens:
            return
        statement = insert(DailyOpenRow).values(
            [
                {
                    "utc_day": item.utc_day,
                    "symbol": item.symbol,
                    "open_price": item.open_price,
                    "open_time": item.open_time,
                    "captured_at": captured_at,
                }
                for item in opens
            ]
        )
        statement = statement.on_conflict_do_update(
            index_elements=["utc_day", "symbol"],
            set_={
                "open_price": statement.excluded.open_price,
                "open_time": statement.excluded.open_time,
                "captured_at": statement.excluded.captured_at,
            },
        )
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(statement)

    async def load_active_memberships(
        self,
    ) -> dict[str, TrackedMembership]:
        async with self._session_factory() as session:
            snapshot_id = await session.scalar(
                select(UniverseSnapshotRow.snapshot_id)
                .where(UniverseSnapshotRow.activated.is_(True))
                .order_by(UniverseSnapshotRow.observed_at.desc())
                .limit(1)
            )
            if snapshot_id is None:
                return {}
            rows = (
                await session.execute(
                    select(MonitoringMembershipRow)
                    .where(MonitoringMembershipRow.snapshot_id == snapshot_id)
                    .order_by(MonitoringMembershipRow.symbol)
                )
            ).scalars()
            return {
                row.symbol: TrackedMembership(
                    symbol=row.symbol,
                    status=MembershipStatus(row.status),
                    side=None if row.side is None else RankingSide(row.side),
                    left_target_at=row.left_target_at,
                )
                for row in rows
            }

    async def save_snapshot(self, snapshot: UniverseSnapshot) -> None:
        returns = _return_by_symbol(snapshot)
        gainer_ranks = _rank_by_symbol(snapshot.ranking.gainers)
        loser_ranks = _rank_by_symbol(snapshot.ranking.losers)

        async with self._session_factory() as session:
            async with session.begin():
                statement = insert(UniverseSnapshotRow).values(
                    snapshot_id=snapshot.snapshot_id,
                    observed_at=snapshot.observed_at,
                    utc_day=snapshot.utc_day,
                    config_hash=snapshot.config_hash,
                    activated=snapshot.activated,
                )
                await session.execute(
                    statement.on_conflict_do_update(
                        index_elements=["snapshot_id"],
                        set_={
                            "observed_at": statement.excluded.observed_at,
                            "utc_day": statement.excluded.utc_day,
                            "config_hash": statement.excluded.config_hash,
                            "activated": statement.excluded.activated,
                        },
                    )
                )
                await session.execute(
                    delete(MonitoringMembershipRow).where(
                        MonitoringMembershipRow.snapshot_id
                        == snapshot.snapshot_id
                    )
                )
                await session.execute(
                    delete(UniverseEntryRow).where(
                        UniverseEntryRow.snapshot_id == snapshot.snapshot_id
                    )
                )

                entries = [
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "symbol": candidate.symbol,
                        "open_price": candidate.open_price,
                        "current_price": candidate.current_price,
                        "price_time": candidate.price_time,
                        "utc_day_return": returns.get(
                            candidate.symbol,
                            _candidate_return(candidate),
                        ),
                        "gainer_rank": gainer_ranks.get(candidate.symbol),
                        "loser_rank": loser_ranks.get(candidate.symbol),
                        "is_target": (
                            candidate.symbol
                            in snapshot.ranking.target_symbols
                        ),
                        "exclusion_reason": (
                            snapshot.ranking.exclusions.get(candidate.symbol)
                        ),
                    }
                    for candidate in snapshot.ranking.candidates
                ]
                if entries:
                    await session.execute(
                        insert(UniverseEntryRow).values(entries)
                    )
                memberships = [
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "symbol": item.symbol,
                        "status": item.status.value,
                        "side": None if item.side is None else item.side.value,
                        "left_target_at": item.left_target_at,
                    }
                    for item in snapshot.memberships
                ]
                if memberships:
                    await session.execute(
                        insert(MonitoringMembershipRow).values(memberships)
                    )

    async def load_snapshot(
        self,
        observed_at: datetime,
    ) -> UniverseSnapshot | None:
        async with self._session_factory() as session:
            snapshot_row = await session.scalar(
                select(UniverseSnapshotRow).where(
                    UniverseSnapshotRow.observed_at == observed_at
                )
            )
            if snapshot_row is None:
                return None
            entries = tuple(
                (
                    await session.execute(
                        select(UniverseEntryRow)
                        .where(
                            UniverseEntryRow.snapshot_id
                            == snapshot_row.snapshot_id
                        )
                        .order_by(UniverseEntryRow.symbol)
                    )
                ).scalars()
            )
            membership_rows = tuple(
                (
                    await session.execute(
                        select(MonitoringMembershipRow)
                        .where(
                            MonitoringMembershipRow.snapshot_id
                            == snapshot_row.snapshot_id
                        )
                        .order_by(MonitoringMembershipRow.symbol)
                    )
                ).scalars()
            )

        candidates = tuple(
            MarketCandidate(
                row.symbol,
                row.open_price,
                row.current_price,
                row.price_time,
            )
            for row in entries
        )
        gainers = tuple(
            RankEntry(
                row.symbol,
                row.utc_day_return,
                row.gainer_rank,
                RankingSide.GAINER,
            )
            for row in sorted(
                (item for item in entries if item.gainer_rank is not None),
                key=lambda item: item.gainer_rank or 0,
            )
            if row.utc_day_return is not None
            and row.gainer_rank is not None
        )
        losers = tuple(
            RankEntry(
                row.symbol,
                row.utc_day_return,
                row.loser_rank,
                RankingSide.LOSER,
            )
            for row in sorted(
                (item for item in entries if item.loser_rank is not None),
                key=lambda item: item.loser_rank or 0,
            )
            if row.utc_day_return is not None
            and row.loser_rank is not None
        )
        ranking = RankingResult(
            candidates=candidates,
            gainers=gainers,
            losers=losers,
            target_symbols=frozenset(
                row.symbol for row in entries if row.is_target
            ),
            exclusions={
                row.symbol: row.exclusion_reason
                for row in entries
                if row.exclusion_reason is not None
            },
        )
        return UniverseSnapshot(
            snapshot_id=snapshot_row.snapshot_id,
            observed_at=snapshot_row.observed_at,
            utc_day=snapshot_row.utc_day,
            config_hash=snapshot_row.config_hash,
            activated=snapshot_row.activated,
            ranking=ranking,
            memberships=tuple(
                TrackedMembership(
                    row.symbol,
                    MembershipStatus(row.status),
                    None if row.side is None else RankingSide(row.side),
                    row.left_target_at,
                )
                for row in membership_rows
            ),
        )
```

Export `PostgresUniverseRepository` from
`src/crypto_momentum_lab/persistence/postgres/__init__.py`.

- [ ] **Step 5: Run integration and domain tests**

Run:

```bash
.venv/bin/python -m pytest \
  tests/unit/domain/universe \
  tests/integration/persistence -v
.venv/bin/ruff check src/crypto_momentum_lab/persistence tests/integration
.venv/bin/mypy src/crypto_momentum_lab/persistence \
  src/crypto_momentum_lab/domain/universe
```

Expected: all selected tests pass and static checks exit with status 0.

- [ ] **Step 6: Commit repository**

```bash
git add src/crypto_momentum_lab/domain/universe/ports.py \
  src/crypto_momentum_lab/persistence/postgres \
  tests/integration
git commit -m "feat: persist point-in-time universes"
```

### Task 7: Binance Public REST Adapter

**Files:**
- Create: `src/crypto_momentum_lab/market_data/binance/__init__.py`
- Create: `src/crypto_momentum_lab/market_data/binance/rest.py`
- Create: `tests/unit/market_data/binance/test_rest.py`

- [ ] **Step 1: Write failing REST adapter tests**

```python
# tests/unit/market_data/binance/test_rest.py
from datetime import UTC, date, datetime
from decimal import Decimal

import httpx
import respx

from crypto_momentum_lab.market_data.binance.rest import BinanceUsdMRestClient


@respx.mock
async def test_lists_only_active_usdt_perpetuals() -> None:
    respx.get("https://fapi.binance.com/fapi/v1/exchangeInfo").mock(
        return_value=httpx.Response(
            200,
            json={
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "contractType": "PERPETUAL",
                        "status": "TRADING",
                        "quoteAsset": "USDT",
                        "marginAsset": "USDT",
                        "onboardDate": 1598252400000,
                        "filters": [],
                    },
                    {
                        "symbol": "BTCUSDC",
                        "contractType": "PERPETUAL",
                        "status": "TRADING",
                        "quoteAsset": "USDC",
                        "marginAsset": "USDC",
                        "onboardDate": 1598252400000,
                        "filters": [],
                    },
                ]
            },
        )
    )
    async with BinanceUsdMRestClient("https://fapi.binance.com") as client:
        contracts = await client.fetch_active_usdt_perpetuals()

    assert [contract.symbol for contract in contracts] == ["BTCUSDT"]


@respx.mock
async def test_fetches_all_latest_prices_from_v2() -> None:
    respx.get("https://fapi.binance.com/fapi/v2/ticker/price").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "BTCUSDT",
                    "price": "60000.5",
                    "time": 1781415660000,
                }
            ],
        )
    )
    async with BinanceUsdMRestClient("https://fapi.binance.com") as client:
        prices = await client.fetch_latest_prices()

    assert prices["BTCUSDT"].price == Decimal("60000.5")
    assert prices["BTCUSDT"].observed_at.tzinfo is UTC


@respx.mock
async def test_fetches_current_utc_day_open() -> None:
    route = respx.get("https://fapi.binance.com/fapi/v1/klines").mock(
        return_value=httpx.Response(
            200,
            json=[
                [
                    1781395200000,
                    "59000.0",
                    "61000",
                    "58000",
                    "60000",
                    "1",
                    1781481599999,
                    "1",
                    1,
                    "1",
                    "1",
                    "0",
                ]
            ],
        )
    )
    async with BinanceUsdMRestClient("https://fapi.binance.com") as client:
        daily_open = await client.fetch_daily_open(
            "BTCUSDT",
            date(2026, 6, 14),
        )

    assert daily_open is not None
    assert daily_open.open_price == Decimal("59000.0")
    assert route.calls[0].request.url.params["interval"] == "1d"
    assert route.calls[0].request.url.params["limit"] == "1"
```

- [ ] **Step 2: Run tests and verify the adapter is missing**

Run:

```bash
.venv/bin/python -m pytest tests/unit/market_data/binance/test_rest.py -v
```

Expected: collection fails because the Binance REST adapter does not exist.

- [ ] **Step 3: Implement the REST adapter**

Add this price type to `domain/universe/models.py`:

```python
@dataclass(frozen=True, slots=True)
class PricePoint:
    symbol: str
    price: Decimal
    observed_at: datetime
```

Implement:

```python
# src/crypto_momentum_lab/market_data/binance/rest.py
import asyncio
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Self

import httpx

from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
    PricePoint,
)


def _utc_from_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=UTC)


class BinanceUsdMRestClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 10.0,
        daily_open_concurrency: int = 10,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout_seconds,
        )
        self._daily_open_concurrency = daily_open_concurrency

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def fetch_active_usdt_perpetuals(
        self,
    ) -> tuple[ContractMetadata, ...]:
        response = await self._client.get("/fapi/v1/exchangeInfo")
        response.raise_for_status()
        contracts = []
        for item in response.json()["symbols"]:
            if not (
                item["contractType"] == "PERPETUAL"
                and item["status"] == "TRADING"
                and item["quoteAsset"] == "USDT"
                and item["marginAsset"] == "USDT"
            ):
                continue
            contracts.append(
                ContractMetadata(
                    symbol=item["symbol"],
                    contract_type=item["contractType"],
                    status=item["status"],
                    quote_asset=item["quoteAsset"],
                    margin_asset=item["marginAsset"],
                    onboard_at=_utc_from_ms(item["onboardDate"]),
                    raw=item,
                )
            )
        return tuple(sorted(contracts, key=lambda item: item.symbol))

    async def fetch_latest_prices(self) -> dict[str, PricePoint]:
        response = await self._client.get("/fapi/v2/ticker/price")
        response.raise_for_status()
        return {
            item["symbol"]: PricePoint(
                symbol=item["symbol"],
                price=Decimal(item["price"]),
                observed_at=_utc_from_ms(item["time"]),
            )
            for item in response.json()
        }

    async def fetch_daily_open(
        self,
        symbol: str,
        utc_day: date,
    ) -> DailyOpen | None:
        start = datetime.combine(utc_day, time.min, tzinfo=UTC)
        end = start + timedelta(days=1) - timedelta(milliseconds=1)
        response = await self._client.get(
            "/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": "1d",
                "startTime": int(start.timestamp() * 1000),
                "endTime": int(end.timestamp() * 1000),
                "limit": 1,
            },
        )
        response.raise_for_status()
        rows = response.json()
        if not rows:
            return None
        row = rows[0]
        return DailyOpen(
            symbol=symbol,
            utc_day=utc_day,
            open_price=Decimal(row[1]),
            open_time=_utc_from_ms(row[0]),
        )

    async def fetch_daily_opens(
        self,
        symbols: frozenset[str],
        utc_day: date,
    ) -> tuple[DailyOpen, ...]:
        semaphore = asyncio.Semaphore(self._daily_open_concurrency)

        async def fetch(symbol: str) -> DailyOpen | None:
            async with semaphore:
                return await self.fetch_daily_open(symbol, utc_day)

        results = await asyncio.gather(*(fetch(symbol) for symbol in sorted(symbols)))
        return tuple(item for item in results if item is not None)
```

Export the adapter from `market_data/binance/__init__.py`.

- [ ] **Step 4: Add retry boundaries without retrying semantic errors**

Add these tests:

```python
@respx.mock
async def test_retries_server_error_then_succeeds() -> None:
    route = respx.get(
        "https://fapi.binance.com/fapi/v2/ticker/price"
    ).mock(
        side_effect=[
            httpx.Response(500),
            httpx.Response(200, json=[]),
        ]
    )
    async with BinanceUsdMRestClient("https://fapi.binance.com") as client:
        client._retry_delays = (0.0, 0.0, 0.0)
        assert await client.fetch_latest_prices() == {}

    assert route.call_count == 2


@respx.mock
async def test_does_not_retry_bad_request() -> None:
    route = respx.get(
        "https://fapi.binance.com/fapi/v2/ticker/price"
    ).mock(return_value=httpx.Response(400))
    async with BinanceUsdMRestClient("https://fapi.binance.com") as client:
        with pytest.raises(httpx.HTTPStatusError):
            await client.fetch_latest_prices()

    assert route.call_count == 1


@respx.mock
async def test_limits_daily_open_concurrency() -> None:
    active = 0
    maximum = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0)
        active -= 1
        return httpx.Response(
            200,
            json=[
                [
                    1781395200000,
                    "100",
                    "100",
                    "100",
                    "100",
                    "1",
                    1781481599999,
                    "1",
                    1,
                    "1",
                    "1",
                    "0",
                ]
            ],
        )

    respx.get("https://fapi.binance.com/fapi/v1/klines").mock(
        side_effect=handler
    )
    async with BinanceUsdMRestClient(
        "https://fapi.binance.com",
        daily_open_concurrency=2,
    ) as client:
        opens = await client.fetch_daily_opens(
            frozenset({"AUSDT", "BUSDT", "CUSDT", "DUSDT"}),
            date(2026, 6, 14),
        )

    assert len(opens) == 4
    assert maximum == 2
```

Add `import pytest` to the test module. Add `_retry_delays` in the constructor
and this method:

```python
        self._retry_delays = (0.25, 0.5, 1.0)

    async def _get(
        self,
        path: str,
        *,
        params: dict[str, object] | None = None,
    ) -> httpx.Response:
        for attempt in range(len(self._retry_delays) + 1):
            try:
                response = await self._client.get(path, params=params)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as error:
                retryable = (
                    error.response.status_code == 429
                    or error.response.status_code >= 500
                )
                if not retryable or attempt == len(self._retry_delays):
                    raise
                await asyncio.sleep(self._retry_delays[attempt])
            except (httpx.ConnectError, httpx.ReadTimeout):
                if attempt == len(self._retry_delays):
                    raise
                await asyncio.sleep(self._retry_delays[attempt])
        raise AssertionError("retry loop exhausted")
```

Replace all three direct `self._client.get(...)` calls with
`self._get(...)`. Preserve the same path and parameter dictionaries.

- [ ] **Step 5: Run adapter tests and static checks**

Run:

```bash
.venv/bin/python -m pytest tests/unit/market_data/binance/test_rest.py -v
.venv/bin/ruff check src/crypto_momentum_lab/market_data tests/unit/market_data
.venv/bin/mypy src/crypto_momentum_lab/market_data
```

Expected: all REST adapter tests pass; static checks exit with status 0.

- [ ] **Step 6: Commit the adapter**

```bash
git add src/crypto_momentum_lab/domain/universe/models.py \
  src/crypto_momentum_lab/market_data tests/unit/market_data
git commit -m "feat: add Binance universe data adapter"
```

### Task 8: Universe Refresh Application Service

**Files:**
- Modify: `src/crypto_momentum_lab/domain/universe/ports.py`
- Create: `src/crypto_momentum_lab/universe/__init__.py`
- Create: `src/crypto_momentum_lab/universe/refresh.py`
- Create: `tests/unit/universe/test_refresh.py`

- [ ] **Step 1: Write failing refresh-service tests**

```python
# tests/unit/universe/test_refresh.py
from datetime import UTC, date, datetime
from decimal import Decimal

from crypto_momentum_lab.config.models import UniverseConfig
from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
    PricePoint,
)
from crypto_momentum_lab.universe.refresh import UniverseRefreshService


async def test_refresh_persists_top_bottom_and_fetches_only_missing_opens(
    fake_market_data,
    fake_repository,
) -> None:
    observed_at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    fake_repository.daily_opens = {"AAAUSDT": Decimal("100")}
    fake_market_data.contracts = tuple(
        ContractMetadata(
            symbol=symbol,
            contract_type="PERPETUAL",
            status="TRADING",
            quote_asset="USDT",
            margin_asset="USDT",
            onboard_at=observed_at,
            raw={},
        )
        for symbol in ("AAAUSDT", "BBBUSDT", "CCCUSDT")
    )
    fake_market_data.prices = {
        "AAAUSDT": PricePoint("AAAUSDT", Decimal("110"), observed_at),
        "BBBUSDT": PricePoint("BBBUSDT", Decimal("90"), observed_at),
        "CCCUSDT": PricePoint("CCCUSDT", Decimal("105"), observed_at),
    }
    fake_market_data.opens = (
        DailyOpen(
            "BBBUSDT",
            date(2026, 6, 14),
            Decimal("100"),
            datetime(2026, 6, 14, tzinfo=UTC),
        ),
        DailyOpen(
            "CCCUSDT",
            date(2026, 6, 14),
            Decimal("100"),
            datetime(2026, 6, 14, tzinfo=UTC),
        ),
    )
    service = UniverseRefreshService(
        market_data=fake_market_data,
        repository=fake_repository,
        config=UniverseConfig(
            top_count=1,
            retention_rank=2,
            retention_hours=2,
            activation_minute=1,
        ),
        config_hash="a" * 64,
    )

    snapshot = await service.refresh(observed_at=observed_at)

    assert snapshot.ranking.target_symbols == frozenset(
        {"AAAUSDT", "BBBUSDT"}
    )
    assert fake_market_data.requested_open_symbols == frozenset(
        {"BBBUSDT", "CCCUSDT"}
    )
    assert fake_repository.saved_snapshot == snapshot


async def test_midnight_snapshot_is_recorded_but_not_activated(
    fake_market_data,
    fake_repository,
) -> None:
    observed_at = datetime(2026, 6, 15, 0, 1, tzinfo=UTC)
    fake_market_data.seed_single_symbol(observed_at)
    service = UniverseRefreshService(
        market_data=fake_market_data,
        repository=fake_repository,
        config=UniverseConfig(
            top_count=20,
            retention_rank=30,
            retention_hours=2,
            activation_minute=1,
        ),
        config_hash="a" * 64,
    )

    snapshot = await service.refresh(observed_at=observed_at)

    assert snapshot.activated is False
    assert snapshot.memberships == ()
```

Create focused fakes in `tests/conftest.py`:

```python
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
    PricePoint,
    TrackedMembership,
    UniverseSnapshot,
)


class FakeUniverseMarketData:
    def __init__(self) -> None:
        self.contracts: tuple[ContractMetadata, ...] = ()
        self.prices: dict[str, PricePoint] = {}
        self.opens: tuple[DailyOpen, ...] = ()
        self.requested_open_symbols = frozenset[str]()

    async def fetch_active_usdt_perpetuals(
        self,
    ) -> tuple[ContractMetadata, ...]:
        return self.contracts

    async def fetch_latest_prices(self) -> dict[str, PricePoint]:
        return self.prices

    async def fetch_daily_opens(
        self,
        symbols: frozenset[str],
        utc_day: date,
    ) -> tuple[DailyOpen, ...]:
        self.requested_open_symbols = symbols
        return tuple(item for item in self.opens if item.symbol in symbols)

    def seed_single_symbol(self, observed_at: datetime) -> None:
        symbol = "BTCUSDT"
        self.contracts = (
            ContractMetadata(
                symbol,
                "PERPETUAL",
                "TRADING",
                "USDT",
                "USDT",
                observed_at,
                {},
            ),
        )
        self.prices = {
            symbol: PricePoint(symbol, Decimal("110"), observed_at)
        }
        self.opens = (
            DailyOpen(
                symbol,
                observed_at.date(),
                Decimal("100"),
                datetime.combine(
                    observed_at.date(),
                    datetime.min.time(),
                    tzinfo=UTC,
                ),
            ),
        )


class FakeUniverseRepository:
    def __init__(self) -> None:
        self.daily_opens: dict[str, Decimal] = {}
        self.active_memberships: dict[str, TrackedMembership] = {}
        self.saved_snapshot: UniverseSnapshot | None = None

    async def save_contract_metadata(
        self,
        contracts: tuple[ContractMetadata, ...],
        *,
        effective_at: datetime,
    ) -> None:
        return None

    async def load_daily_opens(
        self,
        utc_day: date,
        symbols: frozenset[str],
    ) -> dict[str, Decimal]:
        return {
            symbol: price
            for symbol, price in self.daily_opens.items()
            if symbol in symbols
        }

    async def save_daily_opens(
        self,
        opens: tuple[DailyOpen, ...],
        *,
        captured_at: datetime,
    ) -> None:
        self.daily_opens.update(
            {item.symbol: item.open_price for item in opens}
        )

    async def load_active_memberships(
        self,
    ) -> dict[str, TrackedMembership]:
        return self.active_memberships

    async def save_snapshot(self, snapshot: UniverseSnapshot) -> None:
        self.saved_snapshot = snapshot

    async def load_snapshot(
        self,
        observed_at: datetime,
    ) -> UniverseSnapshot | None:
        if (
            self.saved_snapshot is not None
            and self.saved_snapshot.observed_at == observed_at
        ):
            return self.saved_snapshot
        return None


@pytest.fixture
def fake_market_data() -> FakeUniverseMarketData:
    return FakeUniverseMarketData()


@pytest.fixture
def fake_repository() -> FakeUniverseRepository:
    return FakeUniverseRepository()
```

- [ ] **Step 2: Run tests and verify refresh service is missing**

Run:

```bash
.venv/bin/python -m pytest tests/unit/universe/test_refresh.py -v
```

Expected: collection fails because the refresh service does not exist.

- [ ] **Step 3: Add market-data and monitoring-obligation ports**

Append to `domain/universe/ports.py`:

```python
from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
    PricePoint,
    TrackedMembership,
    UniverseSnapshot,
)

class UniverseMarketData(Protocol):
    async def fetch_active_usdt_perpetuals(
        self,
    ) -> tuple[ContractMetadata, ...]: ...

    async def fetch_latest_prices(self) -> dict[str, PricePoint]: ...

    async def fetch_daily_opens(
        self,
        symbols: frozenset[str],
        utc_day: date,
    ) -> tuple[DailyOpen, ...]: ...


class MonitoringObligationProvider(Protocol):
    async def forced_symbols(self) -> frozenset[str]: ...


class NoMonitoringObligations:
    async def forced_symbols(self) -> frozenset[str]:
        return frozenset()
```

- [ ] **Step 4: Implement the refresh service**

```python
# src/crypto_momentum_lab/universe/refresh.py
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.config.models import UniverseConfig
from crypto_momentum_lab.domain.universe.membership import (
    build_monitoring_memberships,
)
from crypto_momentum_lab.domain.universe.models import (
    MarketCandidate,
    UniverseSnapshot,
)
from crypto_momentum_lab.domain.universe.ports import (
    MonitoringObligationProvider,
    NoMonitoringObligations,
    UniverseMarketData,
    UniverseRepository,
)
from crypto_momentum_lab.domain.universe.ranking import rank_utc_day_returns


class UniverseRefreshService:
    def __init__(
        self,
        *,
        market_data: UniverseMarketData,
        repository: UniverseRepository,
        config: UniverseConfig,
        config_hash: str,
        obligations: MonitoringObligationProvider | None = None,
    ) -> None:
        self._market_data = market_data
        self._repository = repository
        self._config = config
        self._config_hash = config_hash
        self._obligations = obligations or NoMonitoringObligations()

    async def refresh(self, *, observed_at: datetime) -> UniverseSnapshot:
        if observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        observed_at = observed_at.astimezone(UTC).replace(second=0, microsecond=0)
        utc_day = observed_at.date()

        contracts = await self._market_data.fetch_active_usdt_perpetuals()
        await self._repository.save_contract_metadata(
            contracts,
            effective_at=observed_at,
        )
        symbols = frozenset(contract.symbol for contract in contracts)
        stored_opens = await self._repository.load_daily_opens(utc_day, symbols)
        missing_symbols = symbols - stored_opens.keys()
        fetched_opens = await self._market_data.fetch_daily_opens(
            frozenset(missing_symbols),
            utc_day,
        )
        await self._repository.save_daily_opens(
            fetched_opens,
            captured_at=observed_at,
        )
        opens: dict[str, Decimal] = {
            **stored_opens,
            **{item.symbol: item.open_price for item in fetched_opens},
        }
        prices = await self._market_data.fetch_latest_prices()

        candidates = [
            MarketCandidate(
                symbol=symbol,
                open_price=opens.get(symbol),
                current_price=None if symbol not in prices else prices[symbol].price,
                price_time=None if symbol not in prices else prices[symbol].observed_at,
            )
            for symbol in sorted(symbols)
        ]
        ranking = rank_utc_day_returns(
            candidates,
            top_count=self._config.top_count,
            ranking_depth=self._config.retention_rank,
        )
        activated = observed_at.hour != 0
        memberships = ()
        if activated:
            previous = await self._repository.load_active_memberships()
            forced = await self._obligations.forced_symbols()
            memberships = tuple(
                build_monitoring_memberships(
                    ranking,
                    previous=previous,
                    forced_symbols=forced,
                    observed_at=observed_at,
                    retention_rank=self._config.retention_rank,
                    retention_duration=timedelta(
                        hours=self._config.retention_hours
                    ),
                ).values()
            )

        snapshot = UniverseSnapshot(
            snapshot_id=uuid5(
                NAMESPACE_URL,
                f"binance-usdm:{observed_at.isoformat()}:{self._config_hash}",
            ),
            observed_at=observed_at,
            utc_day=utc_day,
            config_hash=self._config_hash,
            activated=activated,
            ranking=ranking,
            memberships=tuple(sorted(memberships, key=lambda item: item.symbol)),
        )
        await self._repository.save_snapshot(snapshot)
        return snapshot
```

- [ ] **Step 5: Add edge-case tests**

Add this setup and the edge-case tests:

```python
import pytest

from crypto_momentum_lab.domain.universe.models import MembershipStatus


class FakeObligations:
    def __init__(self, symbols: frozenset[str]) -> None:
        self._symbols = symbols

    async def forced_symbols(self) -> frozenset[str]:
        return self._symbols


def build_service(
    market_data,
    repository,
    *,
    obligations=None,
) -> UniverseRefreshService:
    return UniverseRefreshService(
        market_data=market_data,
        repository=repository,
        config=UniverseConfig(
            top_count=20,
            retention_rank=30,
            retention_hours=2,
            activation_minute=1,
        ),
        config_hash="a" * 64,
        obligations=obligations,
    )


async def test_rejects_naive_refresh_time(
    fake_market_data,
    fake_repository,
) -> None:
    service = build_service(fake_market_data, fake_repository)
    with pytest.raises(ValueError, match="timezone-aware"):
        await service.refresh(observed_at=datetime(2026, 6, 14, 11, 1))


async def test_repeated_refresh_has_same_snapshot_id(
    fake_market_data,
    fake_repository,
) -> None:
    at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    fake_market_data.seed_single_symbol(at)
    service = build_service(fake_market_data, fake_repository)

    first = await service.refresh(observed_at=at)
    second = await service.refresh(observed_at=at)

    assert first.snapshot_id == second.snapshot_id


async def test_missing_price_is_recorded_as_exclusion(
    fake_market_data,
    fake_repository,
) -> None:
    at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    fake_market_data.seed_single_symbol(at)
    fake_market_data.prices = {}
    service = build_service(fake_market_data, fake_repository)

    snapshot = await service.refresh(observed_at=at)

    assert snapshot.ranking.exclusions == {
        "BTCUSDT": "missing_current_price"
    }


async def test_forced_symbol_outside_ranking_remains_monitored(
    fake_market_data,
    fake_repository,
) -> None:
    at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    fake_market_data.seed_single_symbol(at)
    service = build_service(
        fake_market_data,
        fake_repository,
        obligations=FakeObligations(frozenset({"DELISTEDUSDT"})),
    )

    snapshot = await service.refresh(observed_at=at)
    forced = next(
        item
        for item in snapshot.memberships
        if item.symbol == "DELISTEDUSDT"
    )

    assert forced.status is MembershipStatus.FORCED


async def test_0101_activates_after_midnight_snapshot(
    fake_market_data,
    fake_repository,
) -> None:
    midnight = datetime(2026, 6, 15, 0, 1, tzinfo=UTC)
    fake_market_data.seed_single_symbol(midnight)
    service = build_service(fake_market_data, fake_repository)
    first = await service.refresh(observed_at=midnight)

    one_am = datetime(2026, 6, 15, 1, 1, tzinfo=UTC)
    fake_market_data.seed_single_symbol(one_am)
    second = await service.refresh(observed_at=one_am)

    assert first.activated is False
    assert second.activated is True
    assert len(second.memberships) == 1
```

- [ ] **Step 6: Run refresh, domain, and repository tests**

Run:

```bash
.venv/bin/python -m pytest \
  tests/unit/domain/universe \
  tests/unit/universe \
  tests/integration/persistence -v
.venv/bin/ruff check src/crypto_momentum_lab/universe tests/unit/universe
.venv/bin/mypy src/crypto_momentum_lab/universe
```

Expected: all selected tests pass and static checks exit with status 0.

- [ ] **Step 7: Commit refresh service**

```bash
git add src/crypto_momentum_lab/domain/universe/ports.py \
  src/crypto_momentum_lab/universe tests/conftest.py tests/unit/universe
git commit -m "feat: add universe refresh service"
```

### Task 9: Scheduler and Market-Data CLI

**Files:**
- Create: `src/crypto_momentum_lab/universe/scheduler.py`
- Create: `src/crypto_momentum_lab/apps/market_data/__init__.py`
- Create: `src/crypto_momentum_lab/apps/market_data/main.py`
- Create: `tests/unit/universe/test_scheduler.py`
- Create: `tests/unit/apps/market_data/test_main.py`

- [ ] **Step 1: Write failing scheduler tests**

```python
# tests/unit/universe/test_scheduler.py
from datetime import UTC, datetime

from crypto_momentum_lab.universe.scheduler import next_refresh_at


def test_next_refresh_is_first_configured_minute_after_hour() -> None:
    assert next_refresh_at(
        datetime(2026, 6, 14, 10, 30, tzinfo=UTC),
        activation_minute=1,
    ) == datetime(2026, 6, 14, 11, 1, tzinfo=UTC)


def test_exact_refresh_time_advances_to_next_hour() -> None:
    assert next_refresh_at(
        datetime(2026, 6, 14, 11, 1, tzinfo=UTC),
        activation_minute=1,
    ) == datetime(2026, 6, 14, 12, 1, tzinfo=UTC)
```

- [ ] **Step 2: Run tests and verify scheduler is missing**

Run:

```bash
.venv/bin/python -m pytest tests/unit/universe/test_scheduler.py -v
```

Expected: collection fails because `scheduler.py` does not exist.

- [ ] **Step 3: Implement deterministic scheduling**

```python
# src/crypto_momentum_lab/universe/scheduler.py
import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol

from crypto_momentum_lab.domain.universe.models import UniverseSnapshot


class RefreshService(Protocol):
    async def refresh(
        self,
        *,
        observed_at: datetime,
    ) -> UniverseSnapshot: ...


def next_refresh_at(
    now: datetime,
    *,
    activation_minute: int,
) -> datetime:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    utc_now = now.astimezone(UTC)
    candidate = utc_now.replace(
        minute=activation_minute,
        second=0,
        microsecond=0,
    )
    if candidate <= utc_now:
        candidate += timedelta(hours=1)
    return candidate


async def run_scheduler_loop(
    service: RefreshService,
    *,
    activation_minute: int,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    while True:
        now = clock()
        scheduled = next_refresh_at(
            now,
            activation_minute=activation_minute,
        )
        await sleeper(max(0.0, (scheduled - now).total_seconds()))
        await service.refresh(observed_at=scheduled)
```

- [ ] **Step 4: Write CLI tests around an injected service factory**

```python
# tests/unit/apps/market_data/test_main.py
import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from typer.testing import CliRunner

from crypto_momentum_lab.apps.market_data import main
from crypto_momentum_lab.domain.universe.models import (
    MarketCandidate,
    MembershipStatus,
    RankEntry,
    RankingResult,
    RankingSide,
    TrackedMembership,
    UniverseSnapshot,
)
from crypto_momentum_lab.universe.scheduler import run_scheduler_loop


runner = CliRunner()


def fixture_snapshot() -> UniverseSnapshot:
    at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    candidate = MarketCandidate(
        "BTCUSDT",
        Decimal("100"),
        Decimal("110"),
        at,
    )
    rank = RankEntry(
        "BTCUSDT",
        Decimal("0.1"),
        1,
        RankingSide.GAINER,
    )
    return UniverseSnapshot(
        snapshot_id=UUID("00000000-0000-0000-0000-000000000001"),
        observed_at=at,
        utc_day=at.date(),
        config_hash="a" * 64,
        activated=True,
        ranking=RankingResult(
            candidates=(candidate,),
            gainers=(rank,),
            losers=(),
            target_symbols=frozenset({"BTCUSDT"}),
            exclusions={},
        ),
        memberships=(
            TrackedMembership(
                "BTCUSDT",
                MembershipStatus.TARGET,
                RankingSide.GAINER,
                None,
            ),
        ),
    )


def test_refresh_command_prints_snapshot_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[datetime] = []

    async def fake_refresh_once(config_path, observed_at):
        calls.append(observed_at)
        return fixture_snapshot()

    monkeypatch.setattr(main, "refresh_once", fake_refresh_once)
    result = runner.invoke(
        main.app,
        [
            "refresh-universe",
            "--config",
            "configs/environments/research.yaml",
            "--at",
            "2026-06-14T11:01:00Z",
        ],
    )

    assert result.exit_code == 0
    assert calls == [datetime(2026, 6, 14, 11, 1, tzinfo=UTC)]
    assert "target=1" in result.stdout
    assert "monitoring=1" in result.stdout
    assert "excluded=0" in result.stdout


def test_refresh_command_rejects_invalid_timestamp() -> None:
    result = runner.invoke(
        main.app,
        ["refresh-universe", "--at", "not-a-time"],
    )

    assert result.exit_code != 0


async def test_scheduler_propagates_cancellation_cleanly() -> None:
    class FakeService:
        async def refresh(self, *, observed_at: datetime) -> UniverseSnapshot:
            raise AssertionError("refresh must not run after cancellation")

    async def cancelled_sleep(seconds: float) -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await run_scheduler_loop(
            FakeService(),
            activation_minute=1,
            clock=lambda: datetime(2026, 6, 14, 10, 30, tzinfo=UTC),
            sleeper=cancelled_sleep,
        )
```

- [ ] **Step 5: Implement the CLI and composition root**

```python
# src/crypto_momentum_lab/apps/market_data/main.py
import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
import os
from pathlib import Path
from typing import AsyncIterator

from sqlalchemy.ext.asyncio import async_sessionmaker
import structlog
import typer

from crypto_momentum_lab.config.loader import (
    behavior_hash,
    load_runtime_config,
)
from crypto_momentum_lab.domain.universe.models import UniverseSnapshot
from crypto_momentum_lab.market_data.binance.rest import BinanceUsdMRestClient
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from crypto_momentum_lab.universe.refresh import UniverseRefreshService
from crypto_momentum_lab.universe.scheduler import run_scheduler_loop


app = typer.Typer(no_args_is_help=True)
log = structlog.get_logger()


def parse_observed_at(value: str | None) -> datetime:
    if value is None:
        return datetime.now(UTC).replace(second=0, microsecond=0)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("--at must include a timezone")
    return parsed.astimezone(UTC).replace(second=0, microsecond=0)


def resolve_config_path(value: Path | None) -> Path:
    if value is not None:
        return value
    return Path(
        os.environ.get(
            "CML_ENVIRONMENT_CONFIG",
            "configs/environments/research.yaml",
        )
    )


@asynccontextmanager
async def build_refresh_service(
    config_path: Path,
) -> AsyncIterator[tuple[UniverseRefreshService, int]]:
    runtime = load_runtime_config(config_path)
    engine = create_async_database_engine(runtime.database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    repository = PostgresUniverseRepository(session_factory)
    client = BinanceUsdMRestClient(str(runtime.binance_base_url))
    try:
        yield (
            UniverseRefreshService(
                market_data=client,
                repository=repository,
                config=runtime.universe,
                config_hash=behavior_hash(runtime),
            ),
            runtime.universe.activation_minute,
        )
    finally:
        await client.aclose()
        await engine.dispose()


async def refresh_once(
    config_path: Path,
    observed_at: datetime,
) -> UniverseSnapshot:
    async with build_refresh_service(config_path) as (service, _):
        snapshot = await service.refresh(observed_at=observed_at)
        log_snapshot(snapshot)
        return snapshot


def format_snapshot(snapshot: UniverseSnapshot) -> str:
    eligible = (
        len(snapshot.ranking.candidates)
        - len(snapshot.ranking.exclusions)
    )
    return " ".join(
        [
            f"snapshot_id={snapshot.snapshot_id}",
            f"observed_at={snapshot.observed_at.isoformat()}",
            f"activated={str(snapshot.activated).lower()}",
            f"eligible={eligible}",
            f"target={len(snapshot.ranking.target_symbols)}",
            f"monitoring={len(snapshot.memberships)}",
            f"excluded={len(snapshot.ranking.exclusions)}",
        ]
    )


def log_snapshot(snapshot: UniverseSnapshot) -> None:
    log.info(
        "universe_refreshed",
        snapshot_id=str(snapshot.snapshot_id),
        observed_at=snapshot.observed_at.isoformat(),
        activated=snapshot.activated,
        eligible=(
            len(snapshot.ranking.candidates)
            - len(snapshot.ranking.exclusions)
        ),
        target=len(snapshot.ranking.target_symbols),
        monitoring=len(snapshot.memberships),
        excluded=len(snapshot.ranking.exclusions),
    )


class LoggingRefreshService:
    def __init__(self, delegate: UniverseRefreshService) -> None:
        self._delegate = delegate

    async def refresh(
        self,
        *,
        observed_at: datetime,
    ) -> UniverseSnapshot:
        snapshot = await self._delegate.refresh(observed_at=observed_at)
        log_snapshot(snapshot)
        return snapshot


@app.command()
def refresh_universe(
    at: str | None = typer.Option(None, "--at"),
    config: Path | None = typer.Option(None, "--config"),
) -> None:
    try:
        observed_at = parse_observed_at(at)
        snapshot = asyncio.run(
            refresh_once(resolve_config_path(config), observed_at)
        )
    except Exception as error:
        typer.echo(f"refresh failed: {error}", err=True)
        raise typer.Exit(code=1) from error
    typer.echo(format_snapshot(snapshot))


async def run_scheduler(config_path: Path) -> None:
    async with build_refresh_service(config_path) as (
        service,
        activation_minute,
    ):
        log.info(
            "universe_scheduler_started",
            activation_minute=activation_minute,
        )
        await run_scheduler_loop(
            LoggingRefreshService(service),
            activation_minute=activation_minute,
        )


@app.command()
def run_universe_scheduler(
    config: Path | None = typer.Option(None, "--config"),
) -> None:
    try:
        asyncio.run(run_scheduler(resolve_config_path(config)))
    except KeyboardInterrupt:
        log.info("universe_scheduler_stopped")
```

Create empty package files:

```python
# src/crypto_momentum_lab/apps/market_data/__init__.py
```

The resulting commands are:

```text
cml-market-data refresh-universe
cml-market-data run-universe-scheduler
```

- [ ] **Step 6: Run CLI and scheduler tests**

Run:

```bash
.venv/bin/python -m pytest \
  tests/unit/universe/test_scheduler.py \
  tests/unit/apps/market_data/test_main.py -v
.venv/bin/ruff check src/crypto_momentum_lab/apps \
  src/crypto_momentum_lab/universe tests/unit/apps
.venv/bin/mypy src/crypto_momentum_lab/apps \
  src/crypto_momentum_lab/universe
```

Expected: all selected tests pass and static checks exit with status 0.

- [ ] **Step 7: Commit scheduler and CLI**

```bash
git add src/crypto_momentum_lab/apps src/crypto_momentum_lab/universe \
  tests/unit/apps tests/unit/universe/test_scheduler.py
git commit -m "feat: add hourly universe scheduler"
```

### Task 10: End-to-End Determinism and Docker Runtime

**Files:**
- Create: `tests/e2e/test_universe_refresh.py`
- Create: `Dockerfile`
- Modify: `compose.yaml`
- Modify: `README.md`

- [ ] **Step 1: Write a failing end-to-end fixture test**

```python
# tests/e2e/test_universe_refresh.py
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.config.models import UniverseConfig
from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
    MembershipStatus,
    PricePoint,
)
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.universe.refresh import UniverseRefreshService


class FixtureMarketData:
    def __init__(self, observed_at: datetime) -> None:
        self.observed_at = observed_at
        self.symbols = tuple(f"S{index:02d}USDT" for index in range(45))
        self.contracts = tuple(
            ContractMetadata(
                symbol,
                "PERPETUAL",
                "TRADING",
                "USDT",
                "USDT",
                observed_at,
                {},
            )
            for symbol in self.symbols
        )
        self.price_values = {
            symbol: Decimal(80 + index)
            for index, symbol in enumerate(self.symbols)
        }

    async def fetch_active_usdt_perpetuals(
        self,
    ) -> tuple[ContractMetadata, ...]:
        return self.contracts

    async def fetch_latest_prices(self) -> dict[str, PricePoint]:
        return {
            symbol: PricePoint(
                symbol,
                price,
                self.observed_at,
            )
            for symbol, price in self.price_values.items()
        }

    async def fetch_daily_opens(
        self,
        symbols: frozenset[str],
        utc_day: date,
    ) -> tuple[DailyOpen, ...]:
        open_time = datetime.combine(
            utc_day,
            datetime.min.time(),
            tzinfo=UTC,
        )
        return tuple(
            DailyOpen(
                symbol,
                utc_day,
                Decimal("100"),
                open_time,
            )
            for symbol in sorted(symbols)
        )


def build_fixture_service(
    repository: PostgresUniverseRepository,
    market_data: FixtureMarketData,
) -> UniverseRefreshService:
    return UniverseRefreshService(
        market_data=market_data,
        repository=repository,
        config=UniverseConfig(
            top_count=20,
            retention_rank=30,
            retention_hours=2,
            activation_minute=1,
        ),
        config_hash="a" * 64,
    )


@pytest.mark.e2e
async def test_refresh_is_deterministic_and_persists_point_in_time(
    repository: PostgresUniverseRepository,
) -> None:
    at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    market_data = FixtureMarketData(at)
    service = build_fixture_service(repository, market_data)

    first = await service.refresh(observed_at=at)
    second = await service.refresh(observed_at=at)
    loaded = await repository.load_snapshot(at)

    assert first == second
    assert loaded == first
    assert len(first.ranking.target_symbols) == 40
    assert len(first.memberships) == 40


@pytest.mark.e2e
async def test_rank_21_former_target_is_retained(
    repository: PostgresUniverseRepository,
) -> None:
    first_at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    market_data = FixtureMarketData(first_at)
    service = build_fixture_service(repository, market_data)
    first = await service.refresh(observed_at=first_at)
    assert "S25USDT" in first.ranking.target_symbols

    second_at = datetime(2026, 6, 14, 12, 1, tzinfo=UTC)
    market_data.observed_at = second_at
    market_data.price_values["S24USDT"] = Decimal("105.5")
    market_data.price_values["S25USDT"] = Decimal("104.5")
    second = await service.refresh(observed_at=second_at)

    membership = next(
        item for item in second.memberships if item.symbol == "S25USDT"
    )
    assert membership.status is MembershipStatus.RETAINED
    assert len(second.ranking.target_symbols) == 40
    assert len(second.memberships) == 41
```

- [ ] **Step 2: Run the end-to-end test and fix only integration defects**

Run:

```bash
.venv/bin/python -m pytest tests/e2e/test_universe_refresh.py -v
```

Expected before fixes: any serialization, transaction, rank-30, or equality
defects are exposed. Correct the production implementation without weakening
assertions. Final result: both end-to-end tests pass.

- [ ] **Step 3: Add the application image**

```dockerfile
# Dockerfile
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY configs ./configs
COPY alembic.ini ./
COPY alembic ./alembic

RUN python -m pip install --no-cache-dir .

ENTRYPOINT ["cml-market-data"]
CMD ["run-universe-scheduler"]
```

Extend `compose.yaml`:

```yaml
  migrate:
    build: .
    entrypoint: ["alembic"]
    command: ["upgrade", "head"]
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      CML_DATABASE_URL: postgresql+asyncpg://cml:cml@postgres:5432/cml

  market-data:
    build: .
    depends_on:
      migrate:
        condition: service_completed_successfully
    environment:
      CML_DATABASE_URL: postgresql+asyncpg://cml:cml@postgres:5432/cml
      CML_ENVIRONMENT_CONFIG: configs/environments/research.yaml
    command: ["run-universe-scheduler"]
    restart: unless-stopped
```

- [ ] **Step 4: Document exact local operations**

Expand `README.md` with:

````markdown
## Local Setup

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e ".[dev]"
docker compose up -d postgres
export CML_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml
.venv/bin/alembic upgrade head
```

## One-Shot Universe Refresh

```bash
export CML_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml
export CML_ENVIRONMENT_CONFIG=configs/environments/research.yaml
.venv/bin/cml-market-data refresh-universe
```

## Hourly Service

```bash
docker compose up --build market-data
```

The UTC 00:01 snapshot is recorded but not activated. The previous day's
23:01 universe remains active until the 01:01 snapshot succeeds.
````

- [ ] **Step 5: Run the complete phase verification**

Run:

```bash
.venv/bin/python -m pytest -v
.venv/bin/ruff check .
.venv/bin/mypy src
docker compose config
docker build -t crypto-momentum-lab:phase1 .
git diff --check
```

Expected:

- all tests pass;
- Ruff and mypy exit with status 0;
- Compose configuration is valid;
- Docker image builds successfully;
- Git whitespace check is clean.

- [ ] **Step 6: Run one live public-data smoke refresh**

Run:

```bash
docker compose up -d postgres
export CML_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml
.venv/bin/alembic upgrade head
export CML_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml
export CML_ENVIRONMENT_CONFIG=configs/environments/research.yaml
.venv/bin/cml-market-data refresh-universe
```

Expected:

- command exits with status 0;
- output reports `activated=true` except during the UTC 00 hour;
- `eligible` is greater than 40 under normal Binance market conditions;
- `target=40` when at least 40 valid contracts are available;
- a row exists in `universe_snapshots`;
- all ranking exclusions include an explicit reason.

If Binance or local network access is unavailable, record that as an
environmental verification gap; do not substitute fixture success for this
live smoke result.

- [ ] **Step 7: Commit the completed vertical slice**

```bash
git add Dockerfile compose.yaml README.md tests/e2e \
  src configs alembic pyproject.toml
git commit -m "feat: complete dynamic universe foundation"
```

## Phase Completion Criteria

This plan is complete only when:

1. `pytest`, Ruff, and mypy pass from a clean checkout.
2. Alembic upgrades from an empty PostgreSQL database and downgrades to base.
3. Repeating a refresh for the same timestamp and inputs produces the same
   snapshot ID and persisted contents.
4. A historical snapshot contains the full ranking population, explicit
   exclusions, target ranks, and monitoring membership.
5. The UTC 00:01 snapshot is persisted without replacing the active universe.
6. A former target at rank 21 is retained and expires by rank or elapsed time.
7. The official Binance V2 price endpoint is used; the deprecated V1 ticker is
   absent from production code.
8. Docker Compose validates and the application image builds.
9. The live public-data smoke refresh is either successful or explicitly
   reported as blocked by external connectivity.
