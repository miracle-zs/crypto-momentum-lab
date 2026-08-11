from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import hashlib
import heapq
import json
import math
import os
import sqlite3
import subprocess
import time
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path

STATE_INTERVAL_SECONDS = 15
MAX_GAP_SECONDS = 30
MAX_HOLD_SECONDS = 24 * 60 * 60
PURGE_SECONDS = 24 * 60 * 60
POSITION_NOTIONAL = 100.0
BASE_FEE_RATE = 0.0004
STRESS_2BP_FEE_RATE = BASE_FEE_RATE + 0.0002
STRESS_5BP_FEE_RATE = BASE_FEE_RATE + 0.0005
SHANGHAI_OFFSET_SECONDS = 8 * 60 * 60

BASELINE_RUN_ID = "paper-account-03-liquidation-v1"

SLIM_COLUMNS = (
    "symbol",
    "bucket_start",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "trade_count",
    "trade_notional",
    "aggressive_buy_notional",
    "aggressive_sell_notional",
    "last_bid_price",
    "last_ask_price",
    "midpoint",
    "liquidation_count",
    "liquidation_notional",
    "mark_price",
    "closed_kline_count",
    "closed_kline_1m_open_time",
    "closed_kline_1m_close_time",
    "closed_kline_1m_open_price",
    "closed_kline_1m_close_price",
    "source_event_count",
)


def _float(value: str) -> float | None:
    if not value:
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def _float0(value: str) -> float:
    parsed = _float(value)
    return parsed if parsed is not None else 0.0


def _int0(value: str) -> int:
    if not value:
        return 0
    try:
        return int(value)
    except ValueError:
        return 0


_DAY_EPOCH_CACHE: dict[str, int] = {}


def parse_utc_seconds(value: str) -> int:
    """Parse the PostgreSQL UTC timestamps used in the snapshot.

    Bucket timestamps account for most calls and only span a few dates, so a
    date cache is substantially cheaper than millions of ``fromisoformat``
    calls. The fallback keeps the parser correct for unexpected offsets.
    """

    if (
        len(value) >= 19
        and value[10] in {" ", "T"}
        and (value.endswith("+00") or value.endswith("+00:00") or value.endswith("Z"))
    ):
        day = value[:10]
        base = _DAY_EPOCH_CACHE.get(day)
        if base is None:
            base = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp())
            _DAY_EPOCH_CACHE[day] = base
        return (
            base + int(value[11:13]) * 3600 + int(value[14:16]) * 60 + int(value[17:19])
        )
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp())


def iso_utc(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, UTC).isoformat()


def shanghai_day(value: int) -> str:
    return (
        datetime.fromtimestamp(value + SHANGHAI_OFFSET_SECONDS, UTC).date().isoformat()
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temporary.replace(path)


@dataclass(frozen=True, slots=True)
class SplitBoundaries:
    first_state_at: int
    last_state_at: int
    train_boundary: int
    validation_boundary: int
    train_end: int
    validation_start: int
    validation_end: int
    test_start: int

    @classmethod
    def from_range(cls, first_state_at: int, last_state_at: int) -> SplitBoundaries:
        span = last_state_at - first_state_at
        train_boundary = first_state_at + span // 2
        validation_boundary = first_state_at + (span * 3) // 4
        return cls(
            first_state_at=first_state_at,
            last_state_at=last_state_at,
            train_boundary=train_boundary,
            validation_boundary=validation_boundary,
            train_end=train_boundary - PURGE_SECONDS,
            validation_start=train_boundary + PURGE_SECONDS,
            validation_end=validation_boundary - PURGE_SECONDS,
            test_start=validation_boundary + PURGE_SECONDS,
        )

    def split_for(self, opened_at: int) -> str | None:
        if opened_at < self.train_end:
            return "train"
        if self.validation_start <= opened_at < self.validation_end:
            return "validation"
        if opened_at >= self.test_start:
            return "test"
        return None

    def payload(self) -> dict[str, str | None]:
        return {key: iso_utc(value) for key, value in asdict(self).items()}


def prepare_sorted_states(
    source_path: Path,
    work_dir: Path,
    *,
    force: bool = False,
) -> tuple[Path, dict[str, object]]:
    """Create a slim, externally sorted TSV without loading the export in memory."""

    work_dir.mkdir(parents=True, exist_ok=True)
    sorted_path = work_dir / "states.slim.sorted.tsv"
    manifest_path = work_dir / "prepare_manifest.json"
    if sorted_path.exists() and manifest_path.exists() and not force:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("source_sha256") == sha256_file(source_path):
            return sorted_path, manifest

    unsorted_path = work_dir / "states.slim.unsorted.tsv"
    sorted_temporary = work_dir / "states.slim.sorted.tsv.tmp"
    sort_temp_dir = work_dir / "sort-tmp"
    sort_temp_dir.mkdir(exist_ok=True)

    row_count = 0
    quote_valid_count = 0
    closed_kline_row_count = 0
    closed_kline_count = 0
    liquidation_nonzero_bucket_count = 0
    source_event_count = 0
    first_state: str | None = None
    last_state: str | None = None
    started = time.monotonic()

    with gzip.open(source_path, "rt", encoding="utf-8", newline="") as source:
        reader = csv.reader(source)
        header = next(reader)
        indices = {name: header.index(name) for name in SLIM_COLUMNS}
        with unsorted_path.open("w", encoding="utf-8", newline="") as target:
            writer = csv.writer(target, delimiter="\t", lineterminator="\n")
            for row in reader:
                values = [row[indices[name]] for name in SLIM_COLUMNS]
                writer.writerow(values)
                row_count += 1
                bucket_start = values[1]
                if first_state is None or bucket_start < first_state:
                    first_state = bucket_start
                if last_state is None or bucket_start > last_state:
                    last_state = bucket_start
                bid = _float(values[10])
                ask = _float(values[11])
                if bid is not None and ask is not None and ask >= bid > 0:
                    quote_valid_count += 1
                kline_count = _int0(values[16])
                if kline_count:
                    closed_kline_row_count += 1
                    closed_kline_count += kline_count
                if _int0(values[13]) > 0:
                    liquidation_nonzero_bucket_count += 1
                source_event_count += _int0(values[21])
                if row_count % 500_000 == 0:
                    print(
                        f"prepared {row_count:,} rows in "
                        f"{time.monotonic() - started:.1f}s",
                        flush=True,
                    )

    env = dict(os.environ)
    env["LC_ALL"] = "C"
    subprocess.run(
        [
            "sort",
            "-t",
            "\t",
            "-k1,1",
            "-k2,2",
            "-S",
            "64M",
            "--parallel=1",
            "-T",
            str(sort_temp_dir),
            "-o",
            str(sorted_temporary),
            str(unsorted_path),
        ],
        check=True,
        env=env,
    )
    sorted_temporary.replace(sorted_path)
    unsorted_path.unlink()

    if first_state is None or last_state is None:
        raise RuntimeError("state export is empty")
    first_seconds = parse_utc_seconds(first_state)
    last_seconds = parse_utc_seconds(last_state)
    boundaries = SplitBoundaries.from_range(first_seconds, last_seconds)
    manifest: dict[str, object] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "source_path": str(source_path),
        "source_sha256": sha256_file(source_path),
        "sorted_path": str(sorted_path),
        "sorted_sha256": sha256_file(sorted_path),
        "row_count": row_count,
        "quote_valid_count": quote_valid_count,
        "quote_coverage": quote_valid_count / row_count,
        "closed_kline_row_count": closed_kline_row_count,
        "closed_kline_count": closed_kline_count,
        "liquidation_nonzero_bucket_count": liquidation_nonzero_bucket_count,
        "source_event_count": source_event_count,
        "first_state_at": iso_utc(first_seconds),
        "last_state_at": iso_utc(last_seconds),
        "splits": boundaries.payload(),
        "elapsed_seconds": time.monotonic() - started,
    }
    write_json(manifest_path, manifest)
    return sorted_path, manifest


@dataclass(slots=True)
class SymbolStates:
    symbol: str
    at: list[int] = field(default_factory=list)
    open_price: list[float | None] = field(default_factory=list)
    high_price: list[float | None] = field(default_factory=list)
    low_price: list[float | None] = field(default_factory=list)
    close_price: list[float | None] = field(default_factory=list)
    trade_count: list[int] = field(default_factory=list)
    trade_notional: list[float] = field(default_factory=list)
    aggressive_buy: list[float] = field(default_factory=list)
    aggressive_sell: list[float] = field(default_factory=list)
    bid: list[float | None] = field(default_factory=list)
    ask: list[float | None] = field(default_factory=list)
    midpoint: list[float | None] = field(default_factory=list)
    liquidation_count: list[int] = field(default_factory=list)
    liquidation_notional: list[float] = field(default_factory=list)
    mark_price: list[float | None] = field(default_factory=list)
    kline_count: list[int] = field(default_factory=list)
    kline_open_at: list[int | None] = field(default_factory=list)
    kline_close_at: list[int | None] = field(default_factory=list)
    kline_open: list[float | None] = field(default_factory=list)
    kline_close: list[float | None] = field(default_factory=list)
    source_event_count: list[int] = field(default_factory=list)
    segment: list[int] = field(default_factory=list)
    strategy_rows: list[int] = field(default_factory=list)
    strategy_position: list[int | None] = field(default_factory=list)
    duplicate_count: int = 0
    gap_count: int = 0
    max_gap_seconds: int = 0

    def add_row(self, row: list[str]) -> None:
        observed_at = parse_utc_seconds(row[1])
        if self.at and observed_at == self.at[-1]:
            self.duplicate_count += 1
            return
        segment = self.segment[-1] if self.segment else 0
        if self.at:
            gap = observed_at - self.at[-1]
            if gap > MAX_GAP_SECONDS or gap <= 0:
                segment += 1
                self.gap_count += 1
                self.max_gap_seconds = max(self.max_gap_seconds, gap)
        raw_index = len(self.at)
        self.at.append(observed_at)
        self.open_price.append(_float(row[2]))
        self.high_price.append(_float(row[3]))
        self.low_price.append(_float(row[4]))
        close_price = _float(row[5])
        self.close_price.append(close_price)
        self.trade_count.append(_int0(row[6]))
        self.trade_notional.append(_float0(row[7]))
        self.aggressive_buy.append(_float0(row[8]))
        self.aggressive_sell.append(_float0(row[9]))
        self.bid.append(_float(row[10]))
        self.ask.append(_float(row[11]))
        self.midpoint.append(_float(row[12]))
        self.liquidation_count.append(_int0(row[13]))
        self.liquidation_notional.append(_float0(row[14]))
        self.mark_price.append(_float(row[15]))
        self.kline_count.append(_int0(row[16]))
        self.kline_open_at.append(parse_utc_seconds(row[17]) if row[17] else None)
        self.kline_close_at.append(parse_utc_seconds(row[18]) if row[18] else None)
        self.kline_open.append(_float(row[19]))
        self.kline_close.append(_float(row[20]))
        self.source_event_count.append(_int0(row[21]))
        self.segment.append(segment)
        if close_price is None:
            self.strategy_position.append(None)
        else:
            self.strategy_position.append(len(self.strategy_rows))
            self.strategy_rows.append(raw_index)

    def price(self, index: int) -> float | None:
        return (
            self.close_price[index]
            if self.close_price[index] is not None
            else self.midpoint[index]
            if self.midpoint[index] is not None
            else self.mark_price[index]
        )

    def high(self, index: int) -> float | None:
        return (
            self.high_price[index]
            if self.high_price[index] is not None
            else self.price(index)
        )

    def low(self, index: int) -> float | None:
        return (
            self.low_price[index]
            if self.low_price[index] is not None
            else self.price(index)
        )

    def valid_quote(self, index: int) -> bool:
        bid = self.bid[index]
        ask = self.ask[index]
        return bid is not None and ask is not None and ask >= bid > 0


def iter_symbol_states(path: Path) -> Iterator[SymbolStates]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        current: SymbolStates | None = None
        for row in reader:
            if len(row) != len(SLIM_COLUMNS):
                raise ValueError(f"invalid slim row with {len(row)} columns")
            symbol = row[0]
            if current is None or symbol != current.symbol:
                if current is not None:
                    yield current
                current = SymbolStates(symbol=symbol)
            current.add_row(row)
        if current is not None:
            yield current


@dataclass(frozen=True, slots=True)
class DetectionKey:
    liquidation_window: int
    breakout_window: int
    min_move: float
    min_imbalance: float

    @property
    def suffix(self) -> str:
        move_bp = round(self.min_move * 10_000)
        imbalance = round(self.min_imbalance * 100)
        return (
            f"lw{self.liquidation_window}_bw{self.breakout_window}_"
            f"mv{move_bp}bp_ai{imbalance}"
        )


@dataclass(frozen=True, slots=True)
class CascadeEvent:
    index: int
    strategy_index: int
    segment: int
    detected_at: int
    direction: str
    breakout_level: float
    cluster_move: float
    breakout_distance: float
    liquidation_count: int
    liquidation_notional: float
    cluster_trade_count: int
    cluster_trade_notional: float
    aggressive_imbalance: float
    confirmation_imbalance: float


@dataclass(frozen=True, slots=True)
class Candidate:
    candidate_id: str
    family: str
    detection: DetectionKey
    cooldown_seconds: int
    cooldown_buckets: int | None = None
    delay_buckets: int | None = None
    entry_imbalance: float | None = None
    observation_buckets: int | None = None
    exhaustion: str | None = None

    @property
    def complexity(self) -> tuple[int, int, int, int]:
        family_rank = {"C0": 0, "C1": 1, "C2": 1}[self.family]
        timing = self.delay_buckets or self.observation_buckets or 0
        flow = round((self.entry_imbalance or 0) * 100)
        return family_rank, timing, flow, len(self.candidate_id)


def build_candidates() -> tuple[Candidate, ...]:
    candidates: list[Candidate] = []
    detections = [
        DetectionKey(liquidation_window, breakout_window, min_move, min_imbalance)
        for liquidation_window in (2, 4)
        for breakout_window in (4, 12)
        for min_move in (0.005, 0.01)
        for min_imbalance in (0.20, 0.33)
    ]
    cooldowns = ((300, "5m"), (600, "10m"), (900, "15m"))
    for detection in detections:
        candidates.append(
            Candidate(
                candidate_id=f"C0_{detection.suffix}_cd30s",
                family="C0",
                detection=detection,
                cooldown_seconds=30,
                cooldown_buckets=2,
            )
        )
        for cooldown, label in cooldowns:
            candidates.append(
                Candidate(
                    candidate_id=f"C0_{detection.suffix}_cd{label}",
                    family="C0",
                    detection=detection,
                    cooldown_seconds=cooldown,
                )
            )
        for delay in (1, 2, 4, 8):
            for entry_imbalance in (0.0, 0.20, 0.33):
                flow_label = round(entry_imbalance * 100)
                for cooldown, label in cooldowns:
                    candidates.append(
                        Candidate(
                            candidate_id=(
                                f"C1_{detection.suffix}_d{delay}_"
                                f"ei{flow_label}_cd{label}"
                            ),
                            family="C1",
                            detection=detection,
                            cooldown_seconds=cooldown,
                            delay_buckets=delay,
                            entry_imbalance=entry_imbalance,
                        )
                    )
        for observation in (2, 4, 8, 12):
            for exhaustion in ("below33", "below15", "flipped"):
                for cooldown, label in cooldowns:
                    candidates.append(
                        Candidate(
                            candidate_id=(
                                f"C2_{detection.suffix}_o{observation}_"
                                f"ex{exhaustion}_cd{label}"
                            ),
                            family="C2",
                            detection=detection,
                            cooldown_seconds=cooldown,
                            observation_buckets=observation,
                            exhaustion=exhaustion,
                        )
                    )
    return tuple(candidates)


BASELINE_DETECTION = DetectionKey(2, 4, 0.01, 0.33)
BASELINE_CANDIDATE_ID = f"C0_{BASELINE_DETECTION.suffix}_cd30s"
ALL_CANDIDATES = build_candidates()
DETECTION_KEYS = tuple({candidate.detection for candidate in ALL_CANDIDATES})


def _prefix(values: Iterable[float | int]) -> list[float]:
    output = [0.0]
    total = 0.0
    for value in values:
        total += value
        output.append(total)
    return output


def _window_sum(prefix: list[float], start: int, end: int) -> float:
    return prefix[end] - prefix[start]


def aggressive_imbalance(buy: float, sell: float) -> float:
    total = buy + sell
    return 0.0 if total == 0 else (buy - sell) / total


def detect_events(states: SymbolStates) -> dict[DetectionKey, list[CascadeEvent]]:
    output: dict[DetectionKey, list[CascadeEvent]] = {key: [] for key in DETECTION_KEYS}
    strategy_rows = states.strategy_rows
    n = len(strategy_rows)
    if n == 0:
        return output
    buy_prefix = _prefix(states.aggressive_buy[index] for index in strategy_rows)
    sell_prefix = _prefix(states.aggressive_sell[index] for index in strategy_rows)
    liquidation_count_prefix = _prefix(
        states.liquidation_count[index] for index in strategy_rows
    )
    liquidation_notional_prefix = _prefix(
        states.liquidation_notional[index] for index in strategy_rows
    )
    trade_count_prefix = _prefix(states.trade_count[index] for index in strategy_rows)
    trade_notional_prefix = _prefix(
        states.trade_notional[index] for index in strategy_rows
    )

    for liquidation_window in (2, 4):
        seed_indices: set[int] = set()
        for liquidation_index, raw_index in enumerate(strategy_rows):
            count = states.liquidation_count[raw_index]
            if count <= 0:
                continue
            seed_indices.update(
                range(liquidation_index, min(n, liquidation_index + liquidation_window))
            )
        for breakout_window in (4, 12):
            first_index = max(breakout_window, liquidation_window - 1)
            for index in sorted(seed_indices):
                if index < first_index:
                    continue
                cluster_start = index - liquidation_window + 1
                breakout_start = index - breakout_window
                raw_index = strategy_rows[index]
                cluster_start_raw = strategy_rows[cluster_start]
                breakout_start_raw = strategy_rows[breakout_start]
                segment = states.segment[raw_index]
                if (
                    states.segment[cluster_start_raw] != segment
                    or states.segment[breakout_start_raw] != segment
                ):
                    continue
                start_price = states.price(cluster_start_raw)
                end_price = states.price(raw_index)
                if start_price is None or end_price is None or start_price <= 0:
                    continue
                breakout_rows = strategy_rows[breakout_start:index]
                highs = [states.high(item) for item in breakout_rows]
                lows = [states.low(item) for item in breakout_rows]
                if any(value is None for value in highs + lows):
                    continue
                breakout_high = max(value for value in highs if value is not None)
                breakout_low = min(value for value in lows if value is not None)
                liquidation_count = round(
                    _window_sum(
                        liquidation_count_prefix,
                        cluster_start,
                        index + 1,
                    )
                )
                liquidation_notional = _window_sum(
                    liquidation_notional_prefix,
                    cluster_start,
                    index + 1,
                )
                if liquidation_count < 1 or liquidation_notional < 500:
                    continue
                aggressive_buy = _window_sum(
                    buy_prefix,
                    cluster_start,
                    index + 1,
                )
                aggressive_sell = _window_sum(
                    sell_prefix,
                    cluster_start,
                    index + 1,
                )
                imbalance = aggressive_imbalance(aggressive_buy, aggressive_sell)
                confirmation_imbalance = _single_bucket_imbalance(
                    states,
                    raw_index,
                )
                raw_move = (end_price - start_price) / start_price
                cluster_trade_count = round(
                    _window_sum(trade_count_prefix, cluster_start, index + 1)
                )
                cluster_trade_notional = _window_sum(
                    trade_notional_prefix,
                    cluster_start,
                    index + 1,
                )
                for min_move in (0.005, 0.01):
                    for min_imbalance in (0.20, 0.33):
                        direction: str | None = None
                        breakout_level = 0.0
                        directional_move = 0.0
                        if (
                            raw_move >= min_move
                            and imbalance >= min_imbalance
                            and confirmation_imbalance >= min_imbalance
                            and end_price > breakout_high
                        ):
                            direction = "up"
                            breakout_level = breakout_high
                            directional_move = raw_move
                        elif (
                            -raw_move >= min_move
                            and imbalance <= -min_imbalance
                            and confirmation_imbalance <= -min_imbalance
                            and end_price < breakout_low
                        ):
                            direction = "down"
                            breakout_level = breakout_low
                            directional_move = -raw_move
                        if direction is None:
                            continue
                        distance = (
                            (end_price - breakout_level) / breakout_level
                            if direction == "up"
                            else (breakout_level - end_price) / breakout_level
                        )
                        key = DetectionKey(
                            liquidation_window,
                            breakout_window,
                            min_move,
                            min_imbalance,
                        )
                        output[key].append(
                            CascadeEvent(
                                index=raw_index,
                                strategy_index=index,
                                segment=segment,
                                detected_at=states.at[raw_index],
                                direction=direction,
                                breakout_level=breakout_level,
                                cluster_move=directional_move,
                                breakout_distance=distance,
                                liquidation_count=liquidation_count,
                                liquidation_notional=liquidation_notional,
                                cluster_trade_count=cluster_trade_count,
                                cluster_trade_notional=cluster_trade_notional,
                                aggressive_imbalance=imbalance,
                                confirmation_imbalance=confirmation_imbalance,
                            )
                        )
    return output


def gate_events(
    events: Iterable[CascadeEvent], candidate: Candidate
) -> list[CascadeEvent]:
    accepted: list[CascadeEvent] = []
    last_strategy_index: int | None = None
    last_at: int | None = None
    last_segment: int | None = None
    for event in events:
        if last_segment is not None and event.segment != last_segment:
            last_strategy_index = None
            last_at = None
        if candidate.cooldown_buckets is not None:
            if (
                last_strategy_index is not None
                and event.strategy_index
                <= last_strategy_index + candidate.cooldown_buckets
            ):
                continue
        elif (
            last_at is not None
            and event.detected_at - last_at <= candidate.cooldown_seconds
        ):
            continue
        accepted.append(event)
        last_strategy_index = event.strategy_index
        last_at = event.detected_at
        last_segment = event.segment
    return accepted


@dataclass(frozen=True, slots=True)
class Entry:
    condition_index: int
    quote_index: int
    opened_at: int
    side: str
    entry_price: float


@dataclass(frozen=True, slots=True)
class ExitOpportunity:
    completed_at: int
    quote_index: int
    quote_known_at: int
    candle_close: float


@dataclass(frozen=True, slots=True)
class Trade:
    candidate_id: str
    family: str
    symbol: str
    split: str
    detected_at: int
    opened_at: int
    closed_at: int | None
    side: str
    entry_price: float
    exit_price: float | None
    official_exit_price: float | None
    quantity: float
    exit_notional: float | None
    pnl: float | None
    pnl_stress_2bp: float | None
    pnl_stress_5bp: float | None
    official_close_pnl: float | None
    close_reason: str
    condition_index: int
    quote_index: int
    exit_quote_index: int | None
    event: CascadeEvent


@dataclass(slots=True)
class ExitIndex:
    quote_indices: list[int]
    quote_known_at: list[int]
    adverse_long: list[ExitOpportunity]
    adverse_short: list[ExitOpportunity]
    adverse_long_completed: list[int]
    adverse_short_completed: list[int]
    complete_15m_bars: int
    partial_15m_bars: int

    @classmethod
    def build(cls, states: SymbolStates) -> ExitIndex:
        quote_indices = [
            index for index in range(len(states.at)) if states.valid_quote(index)
        ]
        quote_known_at = [
            states.at[index] + STATE_INTERVAL_SECONDS for index in quote_indices
        ]

        minute_bars: dict[int, dict[int, tuple[int, float, float]]] = defaultdict(dict)
        for index, open_at in enumerate(states.kline_open_at):
            close_at = states.kline_close_at[index]
            open_price = states.kline_open[index]
            close_price = states.kline_close[index]
            if (
                open_at is None
                or close_at is None
                or open_price is None
                or close_price is None
                or open_price <= 0
            ):
                continue
            group_start = open_at - open_at % 900
            minute = (open_at - group_start) // 60
            if 0 <= minute < 15:
                minute_bars[group_start][minute] = (
                    close_at,
                    open_price,
                    close_price,
                )

        adverse_long: list[ExitOpportunity] = []
        adverse_short: list[ExitOpportunity] = []
        complete = 0
        partial = 0
        for group in sorted(minute_bars):
            rows = minute_bars[group]
            if set(rows) != set(range(15)):
                partial += 1
                continue
            complete += 1
            bar_open = rows[0][1]
            bar_close = rows[14][2]
            completed_at = rows[14][0]
            quote_position = bisect.bisect_right(quote_known_at, completed_at)
            if quote_position >= len(quote_indices):
                continue
            quote_index = quote_indices[quote_position]
            opportunity = ExitOpportunity(
                completed_at=completed_at,
                quote_index=quote_index,
                quote_known_at=quote_known_at[quote_position],
                candle_close=bar_close,
            )
            if bar_close < bar_open:
                adverse_long.append(opportunity)
            elif bar_close > bar_open:
                adverse_short.append(opportunity)
        return cls(
            quote_indices=quote_indices,
            quote_known_at=quote_known_at,
            adverse_long=adverse_long,
            adverse_short=adverse_short,
            adverse_long_completed=[
                opportunity.completed_at for opportunity in adverse_long
            ],
            adverse_short_completed=[
                opportunity.completed_at for opportunity in adverse_short
            ],
            complete_15m_bars=complete,
            partial_15m_bars=partial,
        )

    def first_quote_after_index(
        self,
        states: SymbolStates,
        condition_index: int,
    ) -> int | None:
        position = bisect.bisect_right(self.quote_indices, condition_index)
        if position >= len(self.quote_indices):
            return None
        quote_index = self.quote_indices[position]
        if states.segment[quote_index] != states.segment[condition_index]:
            return None
        return quote_index

    def quote_at_or_after(self, known_at: int) -> int | None:
        position = bisect.bisect_left(self.quote_known_at, known_at)
        return (
            self.quote_indices[position] if position < len(self.quote_indices) else None
        )


def _single_bucket_imbalance(states: SymbolStates, index: int) -> float:
    return aggressive_imbalance(
        states.aggressive_buy[index],
        states.aggressive_sell[index],
    )


def _outside_breakout(
    price: float | None,
    *,
    direction: str,
    breakout_level: float,
) -> bool:
    if price is None:
        return False
    if direction == "up":
        return price > breakout_level
    return price < breakout_level


def _inside_breakout(
    price: float | None,
    *,
    direction: str,
    breakout_level: float,
) -> bool:
    if price is None:
        return False
    if direction == "up":
        return price <= breakout_level
    return price >= breakout_level


def candidate_entry(
    states: SymbolStates,
    exits: ExitIndex,
    event: CascadeEvent,
    candidate: Candidate,
) -> Entry | None:
    condition_index = event.index
    side = "long" if event.direction == "up" else "short"
    segment = states.segment[event.index]

    if candidate.family == "C1":
        assert candidate.delay_buckets is not None
        assert candidate.entry_imbalance is not None
        condition_strategy_index = event.strategy_index + candidate.delay_buckets
        if (
            condition_strategy_index >= len(states.strategy_rows)
            or states.segment[states.strategy_rows[condition_strategy_index]]
            != segment
        ):
            return None
        condition_index = states.strategy_rows[condition_strategy_index]
        for strategy_index in range(
            event.strategy_index + 1,
            condition_strategy_index + 1,
        ):
            index = states.strategy_rows[strategy_index]
            if not _outside_breakout(
                states.price(index),
                direction=event.direction,
                breakout_level=event.breakout_level,
            ):
                return None
        directional_imbalance = _single_bucket_imbalance(states, condition_index)
        if event.direction == "down":
            directional_imbalance = -directional_imbalance
        if directional_imbalance < candidate.entry_imbalance:
            return None
    elif candidate.family == "C2":
        assert candidate.observation_buckets is not None
        assert candidate.exhaustion is not None
        side = "short" if event.direction == "up" else "long"
        condition_index = -1
        end = min(
            len(states.strategy_rows) - 1,
            event.strategy_index + candidate.observation_buckets,
        )
        for strategy_index in range(event.strategy_index + 1, end + 1):
            index = states.strategy_rows[strategy_index]
            if states.segment[index] != segment:
                break
            if not _inside_breakout(
                states.price(index),
                direction=event.direction,
                breakout_level=event.breakout_level,
            ):
                continue
            directional_imbalance = _single_bucket_imbalance(states, index)
            if event.direction == "down":
                directional_imbalance = -directional_imbalance
            exhausted = (
                directional_imbalance < 0.33
                if candidate.exhaustion == "below33"
                else directional_imbalance < 0.15
                if candidate.exhaustion == "below15"
                else directional_imbalance < 0
            )
            if exhausted:
                condition_index = index
                break
        if condition_index < 0:
            return None

    quote_index = exits.first_quote_after_index(states, condition_index)
    if quote_index is None:
        return None
    bid = states.bid[quote_index]
    ask = states.ask[quote_index]
    assert bid is not None and ask is not None
    entry_price = ask if side == "long" else bid
    return Entry(
        condition_index=condition_index,
        quote_index=quote_index,
        opened_at=states.at[quote_index] + STATE_INTERVAL_SECONDS,
        side=side,
        entry_price=entry_price,
    )


def _pnl(
    *,
    side: str,
    entry_price: float,
    exit_price: float,
    quantity: float,
    fee_rate: float,
) -> float:
    gross = (
        (exit_price - entry_price) * quantity
        if side == "long"
        else (entry_price - exit_price) * quantity
    )
    return gross - POSITION_NOTIONAL * fee_rate - abs(exit_price * quantity) * fee_rate


def simulate_trade(
    states: SymbolStates,
    exits: ExitIndex,
    event: CascadeEvent,
    candidate: Candidate,
    entry: Entry,
    split: str,
) -> Trade:
    if entry.side == "long":
        opportunities = exits.adverse_long
        completed = exits.adverse_long_completed
    else:
        opportunities = exits.adverse_short
        completed = exits.adverse_short_completed
    position = bisect.bisect_right(completed, entry.opened_at)
    adverse = opportunities[position] if position < len(opportunities) else None

    deadline = entry.opened_at + MAX_HOLD_SECONDS
    deadline_quote_index = exits.quote_at_or_after(deadline)
    deadline_quote_at = (
        states.at[deadline_quote_index] + STATE_INTERVAL_SECONDS
        if deadline_quote_index is not None
        else None
    )
    adverse_is_first = (
        adverse is not None
        and adverse.quote_known_at <= deadline
        and (deadline_quote_at is None or adverse.quote_known_at <= deadline_quote_at)
    )
    if adverse_is_first:
        assert adverse is not None
        exit_index = adverse.quote_index
        closed_at = adverse.quote_known_at
        official_exit_price = adverse.candle_close
        close_reason = "first_adverse_15m"
    elif deadline_quote_index is not None:
        exit_index = deadline_quote_index
        closed_at = states.at[exit_index] + STATE_INTERVAL_SECONDS
        official_exit_price = None
        close_reason = "max_holding_24h"
    else:
        return Trade(
            candidate_id=candidate.candidate_id,
            family=candidate.family,
            symbol=states.symbol,
            split=split,
            detected_at=event.detected_at,
            opened_at=entry.opened_at,
            closed_at=None,
            side=entry.side,
            entry_price=entry.entry_price,
            exit_price=None,
            official_exit_price=None,
            quantity=POSITION_NOTIONAL / entry.entry_price,
            exit_notional=None,
            pnl=None,
            pnl_stress_2bp=None,
            pnl_stress_5bp=None,
            official_close_pnl=None,
            close_reason="open_at_cutoff",
            condition_index=entry.condition_index,
            quote_index=entry.quote_index,
            exit_quote_index=None,
            event=event,
        )

    bid = states.bid[exit_index]
    ask = states.ask[exit_index]
    assert bid is not None and ask is not None
    exit_price = bid if entry.side == "long" else ask
    quantity = POSITION_NOTIONAL / entry.entry_price
    official_price = (
        official_exit_price if official_exit_price is not None else exit_price
    )
    return Trade(
        candidate_id=candidate.candidate_id,
        family=candidate.family,
        symbol=states.symbol,
        split=split,
        detected_at=event.detected_at,
        opened_at=entry.opened_at,
        closed_at=closed_at,
        side=entry.side,
        entry_price=entry.entry_price,
        exit_price=exit_price,
        official_exit_price=official_exit_price,
        quantity=quantity,
        exit_notional=abs(exit_price * quantity),
        pnl=_pnl(
            side=entry.side,
            entry_price=entry.entry_price,
            exit_price=exit_price,
            quantity=quantity,
            fee_rate=BASE_FEE_RATE,
        ),
        pnl_stress_2bp=_pnl(
            side=entry.side,
            entry_price=entry.entry_price,
            exit_price=exit_price,
            quantity=quantity,
            fee_rate=STRESS_2BP_FEE_RATE,
        ),
        pnl_stress_5bp=_pnl(
            side=entry.side,
            entry_price=entry.entry_price,
            exit_price=exit_price,
            quantity=quantity,
            fee_rate=STRESS_5BP_FEE_RATE,
        ),
        official_close_pnl=_pnl(
            side=entry.side,
            entry_price=entry.entry_price,
            exit_price=official_price,
            quantity=quantity,
            fee_rate=BASE_FEE_RATE,
        ),
        close_reason=close_reason,
        condition_index=entry.condition_index,
        quote_index=entry.quote_index,
        exit_quote_index=exit_index,
        event=event,
    )


@dataclass(slots=True)
class SplitMetrics:
    entries: int = 0
    closed: int = 0
    open: int = 0
    wins: int = 0
    net_pnl: float = 0.0
    gross_gains: float = 0.0
    gross_losses: float = 0.0
    stress_2bp_pnl: float = 0.0
    stress_5bp_pnl: float = 0.0
    official_close_pnl: float = 0.0
    duration_seconds: float = 0.0
    top_five_positive: list[float] = field(default_factory=list)
    day_net: dict[str, float] = field(default_factory=dict)
    positive_symbol_total: float = 0.0
    top_symbols: list[tuple[float, str]] = field(default_factory=list)
    pending_symbol_net: float = 0.0

    def add(self, trade: Trade) -> None:
        self.entries += 1
        if trade.pnl is None or trade.closed_at is None:
            self.open += 1
            return
        assert trade.pnl_stress_2bp is not None
        assert trade.pnl_stress_5bp is not None
        assert trade.official_close_pnl is not None
        self.closed += 1
        self.net_pnl += trade.pnl
        self.pending_symbol_net += trade.pnl
        self.stress_2bp_pnl += trade.pnl_stress_2bp
        self.stress_5bp_pnl += trade.pnl_stress_5bp
        self.official_close_pnl += trade.official_close_pnl
        self.duration_seconds += trade.closed_at - trade.opened_at
        if trade.pnl > 0:
            self.wins += 1
            self.gross_gains += trade.pnl
            if len(self.top_five_positive) < 5:
                heapq.heappush(self.top_five_positive, trade.pnl)
            elif trade.pnl > self.top_five_positive[0]:
                heapq.heapreplace(self.top_five_positive, trade.pnl)
        elif trade.pnl < 0:
            self.gross_losses += -trade.pnl
        day = shanghai_day(trade.closed_at)
        self.day_net[day] = self.day_net.get(day, 0.0) + trade.pnl

    def finish_symbol(self, symbol: str) -> None:
        value = self.pending_symbol_net
        self.pending_symbol_net = 0.0
        if value <= 0:
            return
        self.positive_symbol_total += value
        item = (value, symbol)
        if len(self.top_symbols) < 8:
            heapq.heappush(self.top_symbols, item)
        elif item > self.top_symbols[0]:
            heapq.heapreplace(self.top_symbols, item)

    def profit_factor(self) -> float | None:
        if self.gross_losses == 0:
            return math.inf if self.gross_gains > 0 else None
        return self.gross_gains / self.gross_losses

    def payload(self) -> dict[str, object]:
        positive_days = [value for value in self.day_net.values() if value > 0]
        total_positive_days = sum(positive_days)
        top_symbol = max(self.top_symbols, default=(0.0, ""))
        return {
            "entries": self.entries,
            "closed_trades": self.closed,
            "open_trades": self.open,
            "net_pnl": self.net_pnl,
            "profit_factor": self.profit_factor(),
            "win_rate": self.wins / self.closed if self.closed else None,
            "expectancy": self.net_pnl / self.closed if self.closed else None,
            "average_duration_minutes": (
                self.duration_seconds / self.closed / 60 if self.closed else None
            ),
            "stress_2bp_pnl": self.stress_2bp_pnl,
            "stress_5bp_pnl": self.stress_5bp_pnl,
            "official_close_pnl": self.official_close_pnl,
            "net_without_top_5_trades": self.net_pnl - sum(self.top_five_positive),
            "top_positive_symbol": top_symbol[1] or None,
            "top_positive_symbol_pnl": top_symbol[0],
            "top_positive_symbol_share": (
                top_symbol[0] / self.positive_symbol_total
                if self.positive_symbol_total > 0
                else 0.0
            ),
            "top_positive_day_share": (
                max(positive_days, default=0.0) / total_positive_days
                if total_positive_days > 0
                else 0.0
            ),
            "daily": dict(sorted(self.day_net.items())),
            "best_symbols": [
                {"symbol": symbol, "net_pnl": value}
                for value, symbol in sorted(self.top_symbols, reverse=True)
            ],
        }


@dataclass(slots=True)
class CandidateMetrics:
    candidate: Candidate
    splits: dict[str, SplitMetrics] = field(
        default_factory=lambda: {
            "train": SplitMetrics(),
            "validation": SplitMetrics(),
            "test": SplitMetrics(),
        }
    )

    def payload(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate.candidate_id,
            "family": self.candidate.family,
            "parameters": asdict(self.candidate),
            "splits": {
                name: metrics.payload() for name, metrics in self.splits.items()
            },
        }


def passes_statistical_gates(metrics: CandidateMetrics) -> tuple[bool, list[str]]:
    failures: list[str] = []
    for split in ("train", "validation"):
        values = metrics.splits[split].payload()
        prefix = f"{split}:"
        if values["closed_trades"] < 50:
            failures.append(prefix + "closed_trades<50")
        if values["net_pnl"] <= 0:
            failures.append(prefix + "net_pnl<=0")
        profit_factor = values["profit_factor"]
        if profit_factor is None or profit_factor <= 1:
            failures.append(prefix + "profit_factor<=1")
        if values["net_without_top_5_trades"] <= 0:
            failures.append(prefix + "net_without_top_5<=0")
        if values["top_positive_symbol_share"] > 0.35:
            failures.append(prefix + "symbol_concentration>35%")
        if values["top_positive_day_share"] > 0.50:
            failures.append(prefix + "day_concentration>50%")
        if values["stress_2bp_pnl"] <= 0:
            failures.append(prefix + "stress_2bp_pnl<=0")
    return not failures, failures


@dataclass(slots=True)
class Coverage:
    symbols: int = 0
    deduplicated_rows: int = 0
    duplicate_rows: int = 0
    gap_count: int = 0
    max_gap_seconds: int = 0
    valid_quote_rows: int = 0
    strategy_eligible_rows: int = 0
    kline_rows: int = 0
    liquidation_nonzero_rows: int = 0
    complete_15m_bars: int = 0
    partial_15m_bars: int = 0

    def observe(self, states: SymbolStates, exits: ExitIndex) -> None:
        self.symbols += 1
        self.deduplicated_rows += len(states.at)
        self.duplicate_rows += states.duplicate_count
        self.gap_count += states.gap_count
        self.max_gap_seconds = max(self.max_gap_seconds, states.max_gap_seconds)
        self.valid_quote_rows += sum(
            states.valid_quote(index) for index in range(len(states.at))
        )
        self.strategy_eligible_rows += len(states.strategy_rows)
        self.kline_rows += sum(count > 0 for count in states.kline_count)
        self.liquidation_nonzero_rows += sum(
            count > 0 for count in states.liquidation_count
        )
        self.complete_15m_bars += exits.complete_15m_bars
        self.partial_15m_bars += exits.partial_15m_bars

    def payload(self) -> dict[str, object]:
        return {
            **asdict(self),
            "quote_coverage": (
                self.valid_quote_rows / self.deduplicated_rows
                if self.deduplicated_rows
                else None
            ),
            "strategy_eligible_coverage": (
                self.strategy_eligible_rows / self.deduplicated_rows
                if self.deduplicated_rows
                else None
            ),
        }


@dataclass(frozen=True, slots=True)
class MembershipTimeline:
    observed_at: tuple[int, ...]
    symbols_by_snapshot: tuple[frozenset[str], ...]

    def contains(self, symbol: str, observed_at: int) -> bool:
        index = bisect.bisect_right(self.observed_at, observed_at) - 1
        if index < 0:
            return False
        return symbol in self.symbols_by_snapshot[index]

    def payload(self) -> dict[str, object]:
        return {
            "activated_snapshot_count": len(self.observed_at),
            "first_snapshot_at": iso_utc(self.observed_at[0])
            if self.observed_at
            else None,
            "last_snapshot_at": iso_utc(self.observed_at[-1])
            if self.observed_at
            else None,
            "minimum_membership_count": min(
                (len(symbols) for symbols in self.symbols_by_snapshot),
                default=0,
            ),
            "maximum_membership_count": max(
                (len(symbols) for symbols in self.symbols_by_snapshot),
                default=0,
            ),
        }


def read_membership_timeline(
    snapshots_path: Path,
    memberships_path: Path,
) -> MembershipTimeline:
    activated: list[tuple[int, str]] = []
    with gzip.open(snapshots_path, "rt", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["activated"].lower() not in {"t", "true", "1"}:
                continue
            activated.append(
                (parse_utc_seconds(row["observed_at"]), row["snapshot_id"])
            )
    activated.sort()
    activated_ids = {snapshot_id for _, snapshot_id in activated}
    symbols: dict[str, set[str]] = defaultdict(set)
    with gzip.open(memberships_path, "rt", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            snapshot_id = row["snapshot_id"]
            if snapshot_id in activated_ids:
                symbols[snapshot_id].add(row["symbol"])
    return MembershipTimeline(
        observed_at=tuple(observed_at for observed_at, _ in activated),
        symbols_by_snapshot=tuple(
            frozenset(symbols[snapshot_id]) for _, snapshot_id in activated
        ),
    )


class TradeStore:
    def __init__(self, path: Path, *, reset: bool) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if reset and path.exists():
            path.unlink()
        self.connection = sqlite3.connect(path)
        self.connection.execute("pragma journal_mode=off")
        self.connection.execute("pragma synchronous=off")
        self.connection.execute("pragma temp_store=file")
        self.connection.execute(
            """
            create table if not exists trades (
                candidate_id text not null,
                family text not null,
                split text not null,
                symbol text not null,
                detected_at integer not null,
                opened_at integer not null,
                closed_at integer,
                side text not null,
                entry_price real not null,
                exit_price real,
                official_exit_price real,
                quantity real not null,
                exit_notional real,
                pnl real,
                pnl_stress_2bp real,
                pnl_stress_5bp real,
                official_close_pnl real,
                close_reason text not null,
                cluster_move real not null,
                breakout_distance real not null,
                liquidation_count integer not null,
                liquidation_notional real not null,
                cluster_trade_count integer not null,
                cluster_trade_notional real not null,
                aggressive_imbalance real not null
            )
            """
        )
        self.pending: list[tuple[object, ...]] = []

    def add(self, trade: Trade) -> None:
        self.pending.append(
            (
                trade.candidate_id,
                trade.family,
                trade.split,
                trade.symbol,
                trade.detected_at,
                trade.opened_at,
                trade.closed_at,
                trade.side,
                trade.entry_price,
                trade.exit_price,
                trade.official_exit_price,
                trade.quantity,
                trade.exit_notional,
                trade.pnl,
                trade.pnl_stress_2bp,
                trade.pnl_stress_5bp,
                trade.official_close_pnl,
                trade.close_reason,
                trade.event.cluster_move,
                trade.event.breakout_distance,
                trade.event.liquidation_count,
                trade.event.liquidation_notional,
                trade.event.cluster_trade_count,
                trade.event.cluster_trade_notional,
                trade.event.aggressive_imbalance,
            )
        )
        if len(self.pending) >= 2_000:
            self.flush()

    def flush(self) -> None:
        if not self.pending:
            return
        self.connection.executemany(
            "insert into trades values "
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            self.pending,
        )
        self.connection.commit()
        self.pending.clear()

    def create_indexes(self) -> None:
        self.flush()
        self.connection.execute(
            "create index if not exists ix_trades_candidate_split_close "
            "on trades(candidate_id, split, closed_at, opened_at)"
        )
        self.connection.execute(
            "create index if not exists ix_trades_candidate_split_open "
            "on trades(candidate_id, split, opened_at, closed_at)"
        )
        self.connection.commit()

    def risk(self, candidate_id: str, split: str) -> dict[str, object]:
        self.flush()
        cumulative = 0.0
        peak = 0.0
        max_drawdown = 0.0
        for (pnl,) in self.connection.execute(
            "select pnl from trades "
            "where candidate_id=? and split=? and closed_at is not null "
            "order by closed_at, opened_at, symbol",
            (candidate_id, split),
        ):
            cumulative += float(pnl)
            peak = max(peak, cumulative)
            max_drawdown = min(max_drawdown, cumulative - peak)

        exposure = 0.0
        peak_exposure = 0.0
        peak_concurrent = 0
        concurrent = 0
        query = """
            select event_at, event_order, delta_notional, delta_count
            from (
                select opened_at as event_at, 1 as event_order,
                       ? as delta_notional, 1 as delta_count
                from trades where candidate_id=? and split=?
                union all
                select closed_at as event_at, 0 as event_order,
                       -? as delta_notional, -1 as delta_count
                from trades
                where candidate_id=? and split=? and closed_at is not null
            ) events
            order by event_at, event_order
        """
        for _, _, delta_notional, delta_count in self.connection.execute(
            query,
            (
                POSITION_NOTIONAL,
                candidate_id,
                split,
                POSITION_NOTIONAL,
                candidate_id,
                split,
            ),
        ):
            exposure += float(delta_notional)
            concurrent += int(delta_count)
            peak_exposure = max(peak_exposure, exposure)
            peak_concurrent = max(peak_concurrent, concurrent)
        return {
            "max_drawdown": max_drawdown,
            "peak_notional_exposure": peak_exposure,
            "peak_concurrent_positions": peak_concurrent,
        }

    def export_candidate(self, candidate_id: str, path: Path) -> None:
        self.flush()
        columns = [
            row[1]
            for row in self.connection.execute("pragma table_info(trades)").fetchall()
        ]
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(columns)
            cursor = self.connection.execute(
                "select * from trades where candidate_id=? order by opened_at, symbol",
                (candidate_id,),
            )
            writer.writerows(cursor)

    def close(self) -> None:
        self.flush()
        self.connection.close()


def runtime_replication_events(
    states: SymbolStates,
    events: list[CascadeEvent],
    run_started_at: int,
    memberships: MembershipTimeline,
) -> set[tuple[str, str, int]]:
    strategy_times = [states.at[index] for index in states.strategy_rows]
    start_strategy_index = bisect.bisect_left(strategy_times, run_started_at)
    if start_strategy_index >= len(states.strategy_rows):
        return set()
    eligible: list[CascadeEvent] = []
    segment_first: dict[int, int] = {}
    for strategy_index, raw_index in enumerate(states.strategy_rows):
        segment_first.setdefault(states.segment[raw_index], strategy_index)
    warmup_prior_buckets = max(
        BASELINE_DETECTION.breakout_window,
        BASELINE_DETECTION.liquidation_window - 1,
    )
    for event in events:
        if event.detected_at < run_started_at:
            continue
        effective_start = max(
            start_strategy_index,
            segment_first[event.segment],
        )
        if event.strategy_index - effective_start < warmup_prior_buckets:
            continue
        eligible.append(event)
    baseline = next(
        candidate
        for candidate in ALL_CANDIDATES
        if candidate.candidate_id == BASELINE_CANDIDATE_ID
    )
    return {
        (
            states.symbol,
            "long" if event.direction == "up" else "short",
            event.detected_at,
        )
        for event in gate_events(eligible, baseline)
        if memberships.contains(states.symbol, event.detected_at)
    }


def replay_pass(
    sorted_states_path: Path,
    candidates: tuple[Candidate, ...],
    boundaries: SplitBoundaries,
    *,
    allowed_splits: set[str],
    metrics_by_id: dict[str, CandidateMetrics] | None = None,
    trade_store: TradeStore | None = None,
    collect_coverage: bool = False,
    replication_start: int | None = None,
    memberships: MembershipTimeline | None = None,
) -> tuple[Coverage, set[tuple[str, str, int]]]:
    coverage = Coverage()
    replication: set[tuple[str, str, int]] = set()
    by_detection: dict[DetectionKey, list[Candidate]] = defaultdict(list)
    for candidate in candidates:
        by_detection[candidate.detection].append(candidate)
    started = time.monotonic()
    for symbol_number, states in enumerate(
        iter_symbol_states(sorted_states_path), start=1
    ):
        exits = ExitIndex.build(states)
        if collect_coverage:
            coverage.observe(states, exits)
        detected = detect_events(states)
        if replication_start is not None and memberships is not None:
            replication.update(
                runtime_replication_events(
                    states,
                    detected[BASELINE_DETECTION],
                    replication_start,
                    memberships,
                )
            )
        touched: set[tuple[str, str]] = set()
        for detection, detection_candidates in by_detection.items():
            events = detected[detection]
            if not events:
                continue
            gated_cache: dict[
                tuple[int, int | None],
                list[CascadeEvent],
            ] = {}
            trade_cache: dict[tuple[object, ...], Trade | None] = {}
            for candidate in detection_candidates:
                cooldown_key = (
                    candidate.cooldown_seconds,
                    candidate.cooldown_buckets,
                )
                if cooldown_key not in gated_cache:
                    gated_cache[cooldown_key] = gate_events(events, candidate)
                entry_signature = (
                    candidate.family,
                    candidate.delay_buckets,
                    candidate.entry_imbalance,
                    candidate.observation_buckets,
                    candidate.exhaustion,
                )
                for event in gated_cache[cooldown_key]:
                    trade_key = (
                        event.index,
                        event.direction,
                        event.breakout_level,
                        *entry_signature,
                    )
                    if trade_key not in trade_cache:
                        entry = candidate_entry(states, exits, event, candidate)
                        trade: Trade | None = None
                        if entry is not None and (
                            memberships is None
                            or memberships.contains(
                                states.symbol,
                                states.at[entry.condition_index],
                            )
                        ):
                            split = boundaries.split_for(entry.opened_at)
                            if split is not None and split in allowed_splits:
                                trade = simulate_trade(
                                    states,
                                    exits,
                                    event,
                                    candidate,
                                    entry,
                                    split,
                                )
                        trade_cache[trade_key] = trade
                    prototype = trade_cache[trade_key]
                    if prototype is None:
                        continue
                    trade = (
                        prototype
                        if prototype.candidate_id == candidate.candidate_id
                        else replace(
                            prototype,
                            candidate_id=candidate.candidate_id,
                        )
                    )
                    if metrics_by_id is not None:
                        metrics_by_id[candidate.candidate_id].splits[trade.split].add(
                            trade
                        )
                        touched.add((candidate.candidate_id, trade.split))
                    if trade_store is not None:
                        trade_store.add(trade)
        if metrics_by_id is not None:
            for candidate_id, split in touched:
                metrics_by_id[candidate_id].splits[split].finish_symbol(states.symbol)
        if symbol_number % 25 == 0:
            print(
                f"replayed {symbol_number} symbols in "
                f"{time.monotonic() - started:.1f}s",
                flush=True,
            )
    if trade_store is not None:
        trade_store.flush()
    return coverage, replication


def read_run_start(runs_path: Path, run_id: str) -> int:
    with gzip.open(runs_path, "rt", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["run_id"] == run_id:
                return parse_utc_seconds(row["created_at"])
    raise RuntimeError(f"run not found in snapshot: {run_id}")


def read_actual_signals(
    signals_path: Path,
    run_id: str,
) -> set[tuple[str, str, int]]:
    output: set[tuple[str, str, int]] = set()
    with gzip.open(signals_path, "rt", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["run_id"] != run_id:
                continue
            output.add(
                (
                    row["symbol"],
                    row["side"],
                    parse_utc_seconds(row["source_state_at"]),
                )
            )
    return output


def signal_replication_payload(
    actual: set[tuple[str, str, int]],
    replayed: set[tuple[str, str, int]],
) -> dict[str, object]:
    matched = actual & replayed
    missing = actual - replayed
    extra = replayed - actual

    def examples(values: set[tuple[str, str, int]]) -> list[dict[str, object]]:
        return [
            {"symbol": symbol, "side": side, "detected_at": iso_utc(detected_at)}
            for symbol, side, detected_at in sorted(values, key=lambda item: item[2])
        ]

    return {
        "run_id": BASELINE_RUN_ID,
        "actual_signal_count": len(actual),
        "replayed_signal_count": len(replayed),
        "matched_signal_count": len(matched),
        "recall": len(matched) / len(actual) if actual else None,
        "precision": len(matched) / len(replayed) if replayed else None,
        "missing_count": len(missing),
        "extra_count": len(extra),
        "missing_examples": examples(missing),
        "extra_examples": examples(extra),
    }


def state_visibility_diagnostics(
    source_path: Path,
    actual: set[tuple[str, str, int]],
    replayed: set[tuple[str, str, int]],
) -> dict[str, object]:
    extra_keys = {(symbol, detected_at) for symbol, _, detected_at in replayed - actual}
    matched_keys = {
        (symbol, detected_at) for symbol, _, detected_at in replayed & actual
    }
    target_keys = extra_keys | matched_keys
    extra_rows: list[dict[str, object]] = []
    matched_lags: list[float] = []
    extra_lags: list[float] = []
    with gzip.open(source_path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            bucket_start = parse_utc_seconds(row["bucket_start"])
            key = (row["symbol"], bucket_start)
            if key not in target_keys:
                continue
            bucket_end = parse_utc_seconds(row["bucket_end"])
            created_at = parse_utc_seconds(row["created_at"])
            lag = created_at - bucket_end
            if key in matched_keys:
                matched_lags.append(float(lag))
            if key in extra_keys:
                extra_lags.append(float(lag))
                extra_rows.append(
                    {
                        "symbol": row["symbol"],
                        "bucket_start": iso_utc(bucket_start),
                        "created_at": row["created_at"],
                        "availability_lag_seconds": lag,
                        "closure_reason": row["closure_reason"],
                        "source_watermark_at": row["source_watermark_at"],
                    }
                )

    def lag_summary(values: list[float]) -> dict[str, object]:
        ordered = sorted(values)

        def percentile(fraction: float) -> float | None:
            if not ordered:
                return None
            index = round((len(ordered) - 1) * fraction)
            return ordered[index]

        return {
            "count": len(ordered),
            "minimum_seconds": ordered[0] if ordered else None,
            "p50_seconds": percentile(0.50),
            "p90_seconds": percentile(0.90),
            "p99_seconds": percentile(0.99),
            "maximum_seconds": ordered[-1] if ordered else None,
            "over_120_seconds": sum(value > 120 for value in ordered),
        }

    return {
        "max_market_state_age_seconds": 120,
        "matched": lag_summary(matched_lags),
        "extra": lag_summary(extra_lags),
        "extra_rows": sorted(extra_rows, key=lambda row: str(row["bucket_start"])),
    }


def risk_passes(
    candidate_risk: dict[str, dict[str, object]],
    baseline_risk: dict[str, dict[str, object]],
) -> tuple[bool, list[str]]:
    failures: list[str] = []
    for split in ("train", "validation"):
        candidate = candidate_risk[split]
        baseline = baseline_risk[split]
        if (
            abs(float(candidate["max_drawdown"]))
            > abs(float(baseline["max_drawdown"])) + 1e-9
        ):
            failures.append(f"{split}:max_drawdown_worse_than_C0")
        if (
            float(candidate["peak_notional_exposure"])
            > float(baseline["peak_notional_exposure"]) + 1e-9
        ):
            failures.append(f"{split}:peak_exposure_worse_than_C0")
    return not failures, failures


def candidate_rank_key(
    metrics: CandidateMetrics,
    risk: dict[str, dict[str, object]],
) -> tuple[object, ...]:
    train_pf = metrics.splits["train"].profit_factor() or 0.0
    validation_pf = metrics.splits["validation"].profit_factor() or 0.0
    minimum_pf = min(train_pf, validation_pf)
    minimum_stress = min(
        metrics.splits["train"].stress_2bp_pnl,
        metrics.splits["validation"].stress_2bp_pnl,
    )
    worst_drawdown = max(
        abs(float(risk[split]["max_drawdown"])) for split in ("train", "validation")
    )
    return (
        -minimum_pf,
        -minimum_stress,
        worst_drawdown,
        metrics.candidate.complexity,
        metrics.candidate.candidate_id,
    )


def test_passes(metrics: CandidateMetrics) -> tuple[bool, list[str]]:
    values = metrics.splits["test"].payload()
    failures: list[str] = []
    if values["net_pnl"] <= 0:
        failures.append("test:net_pnl<=0")
    profit_factor = values["profit_factor"]
    if profit_factor is None or profit_factor <= 1:
        failures.append("test:profit_factor<=1")
    if values["net_without_top_5_trades"] <= 0:
        failures.append("test:net_without_top_5<=0")
    if values["stress_2bp_pnl"] <= 0:
        failures.append("test:stress_2bp_pnl<=0")
    return not failures, failures


def flat_candidate_row(
    metrics: CandidateMetrics,
    gate_failures: list[str],
    risk: dict[str, dict[str, object]] | None,
    risk_failures: list[str],
) -> dict[str, object]:
    row: dict[str, object] = {
        "candidate_id": metrics.candidate.candidate_id,
        "family": metrics.candidate.family,
        "statistical_gate_pass": not gate_failures,
        "statistical_gate_failures": "|".join(gate_failures),
        "risk_gate_pass": risk is not None and not risk_failures,
        "risk_gate_failures": "|".join(risk_failures),
    }
    for split in ("train", "validation"):
        payload = metrics.splits[split].payload()
        for name in (
            "entries",
            "closed_trades",
            "open_trades",
            "net_pnl",
            "profit_factor",
            "win_rate",
            "expectancy",
            "stress_2bp_pnl",
            "stress_5bp_pnl",
            "official_close_pnl",
            "net_without_top_5_trades",
            "top_positive_symbol_share",
            "top_positive_day_share",
        ):
            row[f"{split}_{name}"] = payload[name]
        if risk is not None:
            row[f"{split}_max_drawdown"] = risk[split]["max_drawdown"]
            row[f"{split}_peak_notional_exposure"] = risk[split][
                "peak_notional_exposure"
            ]
            row[f"{split}_peak_concurrent_positions"] = risk[split][
                "peak_concurrent_positions"
            ]
    return row


def write_candidate_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_replay(
    snapshot_dir: Path,
    work_dir: Path,
    output_dir: Path,
    *,
    force_prepare: bool = False,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    source_path = snapshot_dir / "runtime_market_states_15s.csv.gz"
    signals_path = snapshot_dir / "strategy_signals.csv.gz"
    runs_path = snapshot_dir / "strategy_runs.csv.gz"
    snapshots_path = snapshot_dir / "universe_snapshots.csv.gz"
    memberships_path = snapshot_dir / "monitoring_memberships.csv.gz"
    sorted_path, prepare_manifest = prepare_sorted_states(
        source_path,
        work_dir,
        force=force_prepare,
    )
    first_at = parse_utc_seconds(str(prepare_manifest["first_state_at"]))
    last_at = parse_utc_seconds(str(prepare_manifest["last_state_at"]))
    boundaries = SplitBoundaries.from_range(first_at, last_at)
    candidates = ALL_CANDIDATES
    memberships = read_membership_timeline(snapshots_path, memberships_path)
    metrics_by_id = {
        candidate.candidate_id: CandidateMetrics(candidate) for candidate in candidates
    }
    run_started_at = read_run_start(runs_path, BASELINE_RUN_ID)

    print("phase 1/3: train and validation replay", flush=True)
    coverage, replayed_signals = replay_pass(
        sorted_path,
        candidates,
        boundaries,
        allowed_splits={"train", "validation"},
        metrics_by_id=metrics_by_id,
        collect_coverage=True,
        replication_start=run_started_at,
        memberships=memberships,
    )
    actual_signals = read_actual_signals(signals_path, BASELINE_RUN_ID)
    replication = signal_replication_payload(actual_signals, replayed_signals)
    visibility = state_visibility_diagnostics(
        source_path,
        actual_signals,
        replayed_signals,
    )

    gate_results: dict[str, list[str]] = {}
    preliminary: list[Candidate] = []
    for candidate in candidates:
        passed, failures = passes_statistical_gates(
            metrics_by_id[candidate.candidate_id]
        )
        gate_results[candidate.candidate_id] = failures
        if passed:
            preliminary.append(candidate)

    baseline_candidate = next(
        candidate
        for candidate in candidates
        if candidate.candidate_id == BASELINE_CANDIDATE_ID
    )
    detailed_candidates = tuple(
        {
            candidate.candidate_id: candidate
            for candidate in [*preliminary, baseline_candidate]
        }.values()
    )
    store = TradeStore(work_dir / "shortlist_trades.sqlite3", reset=True)
    print(
        f"phase 2/3: risk replay for {len(detailed_candidates)} candidates",
        flush=True,
    )
    replay_pass(
        sorted_path,
        detailed_candidates,
        boundaries,
        allowed_splits={"train", "validation"},
        trade_store=store,
        memberships=memberships,
    )
    store.create_indexes()
    risk_by_id = {
        candidate.candidate_id: {
            split: store.risk(candidate.candidate_id, split)
            for split in ("train", "validation")
        }
        for candidate in detailed_candidates
    }
    baseline_risk = risk_by_id[BASELINE_CANDIDATE_ID]
    risk_failures: dict[str, list[str]] = {}
    risk_passed: list[Candidate] = []
    for candidate in preliminary:
        passed, failures = risk_passes(
            risk_by_id[candidate.candidate_id],
            baseline_risk,
        )
        risk_failures[candidate.candidate_id] = failures
        if passed:
            risk_passed.append(candidate)

    ranked = sorted(
        risk_passed,
        key=lambda candidate: candidate_rank_key(
            metrics_by_id[candidate.candidate_id],
            risk_by_id[candidate.candidate_id],
        ),
    )
    evaluated: Candidate | None = ranked[0] if ranked else None
    test_failures: list[str] = []
    frozen: Candidate | None = None
    if evaluated is not None:
        print(
            f"phase 3/3: blind test for {evaluated.candidate_id}",
            flush=True,
        )
        replay_pass(
            sorted_path,
            (evaluated,),
            boundaries,
            allowed_splits={"test"},
            metrics_by_id=metrics_by_id,
            trade_store=store,
            memberships=memberships,
        )
        passed, test_failures = test_passes(metrics_by_id[evaluated.candidate_id])
        if passed:
            frozen = evaluated

    candidate_payloads = {
        candidate.candidate_id: metrics_by_id[candidate.candidate_id].payload()
        for candidate in candidates
    }
    for candidate_id, payload in candidate_payloads.items():
        payload["statistical_gate_failures"] = gate_results[candidate_id]
        if candidate_id in risk_by_id:
            payload["risk"] = risk_by_id[candidate_id]
        if candidate_id in risk_failures:
            payload["risk_gate_failures"] = risk_failures[candidate_id]

    rows = [
        flat_candidate_row(
            metrics_by_id[candidate.candidate_id],
            gate_results[candidate.candidate_id],
            risk_by_id.get(candidate.candidate_id),
            risk_failures.get(candidate.candidate_id, []),
        )
        for candidate in candidates
    ]
    write_candidate_csv(output_dir / "candidate_metrics.csv", rows)
    write_json(output_dir / "candidate_metrics.json", candidate_payloads)
    write_json(output_dir / "data_quality.json", coverage.payload())
    write_json(output_dir / "signal_replication.json", replication)
    write_json(output_dir / "signal_visibility.json", visibility)

    selection = {
        "status": "freeze_parameters"
        if frozen is not None
        else "no_liquidation_account",
        "baseline_candidate_id": BASELINE_CANDIDATE_ID,
        "candidate_count": len(candidates),
        "statistical_gate_pass_count": len(preliminary),
        "risk_gate_pass_count": len(risk_passed),
        "evaluated_on_test": evaluated.candidate_id if evaluated else None,
        "test_gate_failures": test_failures,
        "frozen_candidate_id": frozen.candidate_id if frozen else None,
        "frozen_parameters": asdict(frozen) if frozen else None,
        "splits": boundaries.payload(),
        "ranking": [candidate.candidate_id for candidate in ranked],
    }
    if evaluated is not None:
        selection["evaluated_metrics"] = metrics_by_id[evaluated.candidate_id].payload()
        store.export_candidate(
            evaluated.candidate_id,
            output_dir / "evaluated_candidate_trades.csv",
        )
    write_json(output_dir / "selection.json", selection)
    store.close()

    run_manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "snapshot_dir": str(snapshot_dir),
        "work_dir": str(work_dir),
        "output_dir": str(output_dir),
        "prepare_manifest": prepare_manifest,
        "coverage": coverage.payload(),
        "memberships": memberships.payload(),
        "signal_replication": replication,
        "signal_visibility": visibility,
        "selection": selection,
    }
    write_json(output_dir / "run_manifest.json", run_manifest)
    return run_manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Chronological replay of Liquidation entry families",
    )
    parser.add_argument("--snapshot-dir", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--force-prepare", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = run_replay(
        args.snapshot_dir,
        args.work_dir,
        args.output_dir,
        force_prepare=args.force_prepare,
    )
    print(json.dumps(result["selection"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
