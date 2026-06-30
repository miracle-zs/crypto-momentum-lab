# Paper Trading Runner Scaffold Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first paper-trading runner scaffold for `compression_breakout`, driven by ordered closed `MarketState15s` records and producing deterministic paper run artifacts without authenticated exchange access.

**Architecture:** Extract the replay fill simulation into a reusable `strategy_runner.fills` module, then add a `strategy_runner.paper` runner that feeds states into the existing compression-breakout runtime strategy and resolves newly pending candidates as later states arrive. The CLI gets a `cml-strategy-runner paper` command that writes a local JSON report; no Binance private API, account state, real orders, risk engine, or PostgreSQL persistence is added.

**Tech Stack:** Python 3.13, dataclasses, Decimal, Typer, pyarrow-backed Parquet reader, pytest, ruff, mypy.

---

## Scope Guard

Implement the approved spec:

`docs/superpowers/specs/2026-06-30-paper-trading-runner-scaffold-design.md`

Do not add:

- authenticated Binance clients;
- balances, positions, margin, leverage, or account reconciliation;
- real orders, cancels, emergency flattening, or order IDs intended for Binance;
- PostgreSQL migrations or paper-run persistence tables;
- multi-strategy arbitration or account leases;
- PnL, exits, inventory, or position netting.

## File Structure

- Create: `src/crypto_momentum_lab/strategy_runner/fills.py`
  - Own `ReplayExecutionConfig`, `SimulatedFillStatus`, `SimulatedFill`, fill-summary types, deterministic fill IDs, and reusable candidate-to-fill simulation helpers.
- Modify: `src/crypto_momentum_lab/strategy_runner/replay.py`
  - Import fill models/helpers from `fills.py`; keep replay orchestration and JSON writing in this file.
- Create: `src/crypto_momentum_lab/strategy_runner/paper.py`
  - Own paper runner config, source interfaces, in-memory/parquet sources, pending candidate tracking, paper report creation, validation, and JSON writer.
- Modify: `src/crypto_momentum_lab/strategy_runner/__init__.py`
  - Export fill and paper runner APIs.
- Modify: `src/crypto_momentum_lab/apps/strategy_runner/main.py`
  - Add `paper` Typer command while keeping `replay` behavior unchanged.
- Modify: `README.md`
  - Add the paper runner command example.
- Create: `tests/unit/strategy_runner/test_fills.py`
  - Unit tests for fill simulation and summary helpers.
- Create: `tests/unit/strategy_runner/test_paper.py`
  - Unit tests for paper runner behavior.
- Modify: `tests/unit/strategy_runner/test_replay.py`
  - Update imports and expected fill schema after extraction.
- Modify: `tests/unit/apps/strategy_runner/test_strategy_runner_main.py`
  - Add paper CLI parsing tests.

## Task 1: Extract Reusable Fill Simulation

**Files:**
- Create: `src/crypto_momentum_lab/strategy_runner/fills.py`
- Create: `tests/unit/strategy_runner/test_fills.py`
- Modify: `src/crypto_momentum_lab/strategy_runner/replay.py`
- Modify: `src/crypto_momentum_lab/strategy_runner/__init__.py`
- Modify: `tests/unit/strategy_runner/test_replay.py`

- [ ] **Step 1: Write failing fill simulation tests**

Create `tests/unit/strategy_runner/test_fills.py` with tests covering:

```python
def test_simulate_candidate_fill_uses_latency_fee_spread_and_fill_id() -> None:
    identity = _identity()
    signal = _signal(identity)
    candidate = _candidate(identity=identity, signal_id=signal.signal_id)
    state = _state(5, close=Decimal("101.4"))

    fill = simulate_candidate_fill(
        candidate=candidate,
        states=(state,),
        execution=ReplayExecutionConfig(
            latency_buckets=1,
            taker_fee_rate=Decimal("0.0004"),
            slippage_bps=Decimal("1"),
        ),
    )

    assert fill.fill_id.startswith("fill_")
    assert fill.fill_id == deterministic_fill_id(candidate_id=candidate.candidate_id)
    assert fill.status is SimulatedFillStatus.FILLED
    assert fill.target_fill_at == datetime(2026, 6, 22, 0, 1, 15, tzinfo=UTC)
    assert fill.filled_at == datetime(2026, 6, 22, 0, 1, 15, tzinfo=UTC)
    assert fill.fill_price == Decimal("101.420141")
    assert fill.fee == Decimal("0.0400")
    assert fill.total_cost > fill.fee


def test_pending_fill_records_source_ended_before_fill() -> None:
    candidate = _candidate(
        identity=_identity(),
        signal_id="sig_1",
        created_at=datetime(2026, 6, 22, 0, 1, tzinfo=UTC),
    )

    fill = pending_candidate_fill(
        candidate=candidate,
        execution=ReplayExecutionConfig(latency_buckets=2),
        reason="source_ended_before_fill",
    )

    assert fill.status is SimulatedFillStatus.PENDING
    assert fill.filled_at is None
    assert fill.reason == "source_ended_before_fill"


def test_fill_summary_counts_status_and_costs() -> None:
    filled = _filled_fill(symbol="BTCUSDT", notional=Decimal("100"))
    pending = _pending_fill(symbol="BTCUSDT")

    summary = fill_summary((filled, pending))

    assert summary["fills_by_status"] == {"filled": 1, "pending": 1}
    assert summary["filled_notional_by_symbol"] == {"BTCUSDT": Decimal("100")}
```

Include local helpers `_identity`, `_signal`, `_candidate`, `_state`,
`_filled_fill`, and `_pending_fill` in the test file. Reuse the existing
`MarketState15s`, `StrategyRunIdentity`, `StrategySignal`, and
`OrderIntentCandidate` constructors already used in replay tests.

- [ ] **Step 2: Run fill tests to verify failure**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategy_runner/test_fills.py -v
```

Expected: fail during import because `crypto_momentum_lab.strategy_runner.fills`
does not exist.

- [ ] **Step 3: Implement `fills.py`**

Create `src/crypto_momentum_lab/strategy_runner/fills.py` by moving the fill
models and helper logic out of `replay.py`. Add:

```python
class SimulatedFillStatus(StrEnum):
    FILLED = "filled"
    EXPIRED = "expired"
    REJECTED = "rejected"
    PENDING = "pending"


def deterministic_fill_id(*, candidate_id: str) -> str:
    if not candidate_id:
        raise ValueError("candidate_id must not be empty")
    return f"fill_{uuid5(NAMESPACE_URL, candidate_id)}"
```

Add `fill_id: str` to `SimulatedFill` and set it in every fill outcome using
`deterministic_fill_id(candidate_id=candidate.candidate_id)`.

Expose these public helpers:

```python
def candidate_target_fill_at(
    candidate: OrderIntentCandidate,
    execution: ReplayExecutionConfig,
) -> datetime


def simulate_candidate_fill(
    *,
    candidate: OrderIntentCandidate,
    states: tuple[MarketState15s, ...],
    execution: ReplayExecutionConfig,
) -> SimulatedFill


def simulate_candidate_fills(
    *,
    candidates: tuple[OrderIntentCandidate, ...],
    ordered_states: tuple[MarketState15s, ...],
    execution: ReplayExecutionConfig | None,
) -> tuple[SimulatedFill, ...]


def pending_candidate_fill(
    *,
    candidate: OrderIntentCandidate,
    execution: ReplayExecutionConfig,
    reason: str,
) -> SimulatedFill


def fill_summary(
    simulated_fills: tuple[SimulatedFill, ...],
) -> dict[str, dict[str, int | Decimal]]
```

- [ ] **Step 4: Update replay to use `fills.py`**

In `src/crypto_momentum_lab/strategy_runner/replay.py`:

- remove local definitions of `SimulatedFillStatus`, `ReplayExecutionConfig`,
  `SimulatedFill`, `FillSummaryValue`, `_simulate_candidate_fills`,
  `_simulate_candidate_fill`, `_unfilled`, `_marketable_quote`,
  `_apply_slippage`, `_market_cost`, and `_fill_summary`;
- import `FillSummaryValue`, `ReplayExecutionConfig`, `SimulatedFill`,
  `fill_summary`, and `simulate_candidate_fills` from
  `crypto_momentum_lab.strategy_runner.fills`;
- call `simulate_candidate_fills(...)` and `fill_summary(...)` in
  `run_strategy_replay`.

In `src/crypto_momentum_lab/strategy_runner/__init__.py`, export:

```python
ReplayExecutionConfig
SimulatedFill
SimulatedFillStatus
deterministic_fill_id
fill_summary
simulate_candidate_fill
simulate_candidate_fills
pending_candidate_fill
```

- [ ] **Step 5: Update replay tests for fill IDs**

In `tests/unit/strategy_runner/test_replay.py`, add assertions:

```python
assert report.simulated_fills[0].fill_id.startswith("fill_")
assert payload["simulated_fills"][0]["fill_id"].startswith("fill_")
```

- [ ] **Step 6: Verify Task 1**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategy_runner/test_fills.py tests/unit/strategy_runner/test_replay.py -v
/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/ruff check .
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/mypy src
```

Expected: fill and replay tests pass; ruff and mypy pass.

- [ ] **Step 7: Commit Task 1**

```bash
git add src/crypto_momentum_lab/strategy_runner/fills.py src/crypto_momentum_lab/strategy_runner/replay.py src/crypto_momentum_lab/strategy_runner/__init__.py tests/unit/strategy_runner/test_fills.py tests/unit/strategy_runner/test_replay.py
git commit -m "refactor: share strategy fill simulation"
```

## Task 2: Paper Runner Core

**Files:**
- Create: `src/crypto_momentum_lab/strategy_runner/paper.py`
- Create: `tests/unit/strategy_runner/test_paper.py`
- Modify: `src/crypto_momentum_lab/strategy_runner/__init__.py`

- [ ] **Step 1: Write failing paper runner tests**

Create `tests/unit/strategy_runner/test_paper.py` with tests covering:

```python
def test_run_paper_trading_emits_incremental_fill_after_latency() -> None:
    report = run_paper_trading(
        source=InMemoryPaperMarketStateSource(_breakout_states() + (_state(5, close=Decimal("101.4")),)),
        config=_paper_config(),
    )

    assert report.schema_version == 1
    assert report.run.run_mode is RunMode.PAPER
    assert report.input_state_count == 6
    assert len(report.signals) == 1
    assert len(report.candidates) == 1
    assert len(report.paper_fills) == 1
    assert report.paper_fills[0].status is SimulatedFillStatus.FILLED
    assert report.pending_candidate_count == 0
    assert report.fill_summary["fills_by_status"] == {"filled": 1}


def test_run_paper_trading_leaves_candidate_pending_when_source_ends() -> None:
    report = run_paper_trading(
        source=InMemoryPaperMarketStateSource(_breakout_states()),
        config=_paper_config(),
    )

    assert len(report.paper_fills) == 1
    assert report.paper_fills[0].status is SimulatedFillStatus.PENDING
    assert report.paper_fills[0].reason == "source_ended_before_fill"
    assert report.pending_candidate_count == 1


def test_run_paper_trading_rejects_missing_fill_price() -> None:
    missing_price_state = _without_fill_price(_state(5, close=Decimal("101.4")))

    report = run_paper_trading(
        source=InMemoryPaperMarketStateSource(_breakout_states() + (missing_price_state,)),
        config=_paper_config(),
    )

    assert report.paper_fills[0].status is SimulatedFillStatus.REJECTED
    assert report.paper_fills[0].reason == "missing_fill_price"


def test_run_paper_trading_respects_max_states() -> None:
    report = run_paper_trading(
        source=InMemoryPaperMarketStateSource(_breakout_states() + (_state(5, close=Decimal("101.4")),)),
        config=_paper_config(max_states=4),
    )

    assert report.input_state_count == 4
    assert report.signals == ()
    assert report.candidates == ()
    assert report.paper_fills == ()


def test_run_paper_trading_rejects_backward_symbol_state() -> None:
    states = (_state(1, close=Decimal("100")), _state(0, close=Decimal("100")))

    with pytest.raises(PaperRunnerError, match="state moved backward"):
        run_paper_trading(
            source=InMemoryPaperMarketStateSource(states),
            config=_paper_config(),
        )
```

Include helpers `_paper_config`, `_breakout_states`, `_state`, and
`_without_fill_price`. Keep helper data aligned with `test_replay.py`.

- [ ] **Step 2: Run paper tests to verify failure**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategy_runner/test_paper.py -v
```

Expected: fail during import because `crypto_momentum_lab.strategy_runner.paper`
does not exist.

- [ ] **Step 3: Implement `paper.py`**

Create `src/crypto_momentum_lab/strategy_runner/paper.py` with:

```python
class PaperRunnerError(RuntimeError):
    pass


class PaperMarketStateSource(Protocol):
    description: str

    def __iter__(self) -> Iterator[MarketState15s]:
        ...


@dataclass(frozen=True, slots=True)
class InMemoryPaperMarketStateSource:
    states: tuple[MarketState15s, ...]
    description: str = "memory"

    def __iter__(self) -> Iterator[MarketState15s]:
        return iter(self.states)


@dataclass(frozen=True, slots=True)
class PaperRunnerConfig:
    strategy_name: str
    run_id: str
    code_commit: str
    generated_at: datetime
    compression_breakout: CompressionBreakoutConfig
    candidate_notional: Decimal | None
    candidate_ttl_buckets: int
    execution: ReplayExecutionConfig = field(default_factory=ReplayExecutionConfig)
    max_states: int | None = None
```

Add `PaperTradingRunReport` with:

```python
schema_version: int
generated_at: datetime
run: StrategyRunIdentity
execution_config: ReplayExecutionConfig
source_description: str
input_state_count: int
processed_symbol_count: int
signals: tuple[StrategySignal, ...]
candidates: tuple[OrderIntentCandidate, ...]
paper_fills: tuple[SimulatedFill, ...]
pending_candidate_count: int
rejection_summary: dict[str, dict[str, int]]
final_checkpoint: StrategyCheckpoint
summary_counts: dict[str, dict[str, int]]
fill_summary: dict[str, dict[str, int | Decimal]]
```

Implement:

```python
def run_paper_trading(
    *,
    source: PaperMarketStateSource,
    config: PaperRunnerConfig,
) -> PaperTradingRunReport
```

Use `RunMode.PAPER` in `StrategyRunIdentity`. For each incoming state, validate
timezone-aware timestamps and per-symbol monotonic bucket order. Resolve pending
candidates before processing the current state through the strategy. Add newly
emitted candidates to the pending list. After the source ends, convert remaining
pending candidates to `pending_candidate_fill(..., reason="source_ended_before_fill")`
unless the last processed symbol time is already after `candidate.expires_at`, in
which case use `simulate_candidate_fill(...)` with no states so the outcome is
expired.

Add duplicate validation for signal IDs, candidate IDs, and fill IDs.

- [ ] **Step 4: Export paper APIs**

In `src/crypto_momentum_lab/strategy_runner/__init__.py`, export:

```python
InMemoryPaperMarketStateSource
PaperMarketStateSource
PaperRunnerConfig
PaperRunnerError
PaperTradingRunReport
run_paper_trading
write_paper_trading_report
```

- [ ] **Step 5: Verify Task 2**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategy_runner/test_fills.py tests/unit/strategy_runner/test_paper.py tests/unit/strategy_runner/test_replay.py -v
/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/ruff check .
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/mypy src
```

Expected: fill, paper, and replay tests pass; ruff and mypy pass.

- [ ] **Step 6: Commit Task 2**

```bash
git add src/crypto_momentum_lab/strategy_runner/paper.py src/crypto_momentum_lab/strategy_runner/__init__.py tests/unit/strategy_runner/test_paper.py
git commit -m "feat: add paper trading runner core"
```

## Task 3: Paper CLI and Documentation

**Files:**
- Modify: `src/crypto_momentum_lab/apps/strategy_runner/main.py`
- Modify: `tests/unit/apps/strategy_runner/test_strategy_runner_main.py`
- Modify: `README.md`

- [ ] **Step 1: Write failing CLI tests**

In `tests/unit/apps/strategy_runner/test_strategy_runner_main.py`, add:

```python
def test_paper_command_writes_report(tmp_path: Path, monkeypatch) -> None:
    states_root = tmp_path / "states"
    states_root.mkdir()
    output_path = tmp_path / "paper.json"
    calls: list[tuple[object, PaperRunnerConfig]] = []
    writes: list[tuple[object, Path]] = []

    def fake_build_source(state_paths: tuple[Path, ...]) -> object:
        assert state_paths == (states_root,)
        return object()

    def fake_run_paper_trading(*, source: object, config: PaperRunnerConfig) -> object:
        calls.append((source, config))
        return SimpleNamespace(
            input_state_count=6,
            signals=(object(),),
            candidates=(object(),),
            paper_fills=(object(),),
        )

    def fake_write_paper_trading_report(report: object, path: Path) -> None:
        writes.append((report, path))

    monkeypatch.setattr(main, "build_paper_state_source", fake_build_source)
    monkeypatch.setattr(main, "run_paper_trading", fake_run_paper_trading)
    monkeypatch.setattr(main, "write_paper_trading_report", fake_write_paper_trading_report)

    result = runner.invoke(
        main.app,
        [
            "paper",
            "--strategy",
            "compression_breakout",
            "--states-root",
            str(states_root),
            "--output",
            str(output_path),
            "--run-id",
            "paper-cli",
            "--generated-at",
            "2026-06-30T00:00:00+00:00",
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
            "--execution-latency-buckets",
            "2",
            "--taker-fee-rate",
            "0.0005",
            "--slippage-bps",
            "1.5",
            "--max-states",
            "10",
        ],
    )

    assert result.exit_code == 0
    assert calls[0][1].run_id == "paper-cli"
    assert calls[0][1].max_states == 10
    assert calls[0][1].execution.latency_buckets == 2
    assert writes[0][1] == output_path
    assert "Paper run completed: states=6 signals=1 candidates=1 fills=1" in result.stdout
```

- [ ] **Step 2: Run CLI tests to verify failure**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/apps/strategy_runner/test_strategy_runner_main.py::test_paper_command_writes_report -v
```

Expected: fail because the `paper` command and imports do not exist.

- [ ] **Step 3: Implement CLI source builder and command**

In `src/crypto_momentum_lab/apps/strategy_runner/main.py`, import paper APIs and
add:

```python
def build_paper_state_source(
    state_paths: tuple[Path, ...],
) -> InMemoryPaperMarketStateSource:
    states = tuple(
        sorted(
            read_market_states_15s_dataset(state_paths),
            key=lambda item: (item.bucket_start, item.symbol),
        )
    )
    return InMemoryPaperMarketStateSource(
        states=states,
        description=",".join(path.as_posix() for path in state_paths),
    )
```

Add `@app.command("paper")` with the options from the spec. Build
`PaperRunnerConfig`, run `run_paper_trading`, write the report, and print:

```text
Paper run completed: states=<n> signals=<n> candidates=<n> fills=<n>
```

- [ ] **Step 4: Update README**

Add a short `Paper Trading Runner` section with:

```bash
.venv/bin/cml-strategy-runner paper \
  --strategy compression_breakout \
  --states-root data/derived/market_states_15s \
  --output reports/compression-breakout-paper.json \
  --execution-latency-buckets 1 \
  --taker-fee-rate 0.0004 \
  --slippage-bps 0
```

Mention that it is simulated paper mode and does not connect to a Binance
account.

- [ ] **Step 5: Verify Task 3**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/apps/strategy_runner/test_strategy_runner_main.py tests/unit/strategy_runner/test_paper.py -v
.venv/bin/cml-strategy-runner paper --help
/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/ruff check .
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/mypy src
```

Expected: CLI and paper tests pass; help output includes `paper`; ruff and mypy pass.

- [ ] **Step 6: Commit Task 3**

```bash
git add README.md src/crypto_momentum_lab/apps/strategy_runner/main.py tests/unit/apps/strategy_runner/test_strategy_runner_main.py
git commit -m "feat: add paper trading runner cli"
```

## Task 4: Final Verification

**Files:**
- No planned source edits unless verification reveals a bug.

- [ ] **Step 1: Run targeted paper/replay tests**

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategy_runner/test_fills.py tests/unit/strategy_runner/test_paper.py tests/unit/strategy_runner/test_replay.py tests/unit/apps/strategy_runner/test_strategy_runner_main.py -v
```

Expected: all targeted tests pass.

- [ ] **Step 2: Run adjacent strategy runtime tests**

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/domain/strategy/test_strategy_models.py tests/unit/strategies/compression_breakout/test_runtime.py -v
```

Expected: all adjacent tests pass.

- [ ] **Step 3: Run static checks**

```bash
/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/ruff check .
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/mypy src
```

Expected: ruff and mypy pass.

- [ ] **Step 4: Run full non-live tests**

```bash
env -u ALL_PROXY -u all_proxy -u HTTP_PROXY -u http_proxy -u HTTPS_PROXY -u https_proxy -u NO_PROXY -u no_proxy PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest -m "not live" -v
```

Expected: all non-live tests pass, with the live smoke test deselected.

- [ ] **Step 5: Commit verification fixes only if needed**

If verification reveals a bug, write a focused failing regression test in the
test file that owns the behavior, run that test to verify the failure, patch the
smallest affected source file, and rerun the failing command plus the full Task
4 verification set. Stage only the concrete files changed by that fix and commit
with:

```bash
git commit -m "fix: stabilize paper trading runner"
```

If no fixes are needed, do not create an empty commit.
