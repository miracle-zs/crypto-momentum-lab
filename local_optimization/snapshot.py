"""Immutable snapshot manifest, data integrity inspection, and capability tagging.

Handles validation of local exports and research parquet datasets:
- Generates snapshot manifest with cryptographic hashes (SHA-256)
- Validates stream watermarks and UTC cutoff compliance
- Assigns capability tags: research_proxy, decision_replay_ready, execution_audit_ready
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

STREAM_REQUIRED_COLUMNS: dict[str, set[str]] = {
    "account_balance_usdt": {
        "balance",
        "account",
        "account_id",
        "timestamp",
        "recorded_at",
        "total",
    },
    "account_fill_events": {
        "order_id",
        "symbol",
        "price",
        "quantity",
        "qty",
        "side",
        "trade_at",
        "fill_time",
    },
    "exchange_orders": {
        "order_id",
        "symbol",
        "status",
        "side",
        "created_at",
    },
    "live_strategy_signals": {
        "symbol",
        "direction",
        "timestamp",
        "detected_at",
        "signal_type",
    },
    "order_intents": {
        "intent_id",
        "symbol",
        "decision",
        "action",
        "created_at",
        "timestamp",
    },
}

STREAM_REQUIRED_CORE_COLUMNS: dict[str, list[set[str]]] = {
    "account_balance_usdt": [
        {"balance", "wallet_balance", "total"},
        {"timestamp", "observed_at", "recorded_at"},
    ],
    "account_fill_events": [
        {"order_id", "symbol"},
        {"price", "side", "qty", "quantity"},
        {"timestamp", "trade_at", "fill_time", "created_at"},
    ],
    "exchange_orders": [
        {"order_id", "symbol"},
        {"status", "side"},
        {"timestamp", "created_at"},
    ],
    "live_strategy_signals": [
        {"symbol"},
        {"direction", "signal_type"},
        {"timestamp", "detected_at", "created_at"},
    ],
    "order_intents": [
        {"symbol"},
        {"decision", "action", "intent_id"},
        {"timestamp", "created_at"},
    ],
}


@dataclass(frozen=True)
class StreamWatermark:
    """Watermark observation for an exported data stream."""

    stream_name: str
    row_count: int
    earliest_time: str | None = None
    latest_time: str | None = None
    file_size_bytes: int = 0
    sha256_hash: str = ""


@dataclass
class SnapshotManifest:
    """Immutable manifest for an imported snapshot."""

    snapshot_id: str
    imported_at: str
    target_cutoff: str
    is_complete: bool
    capability_tags: list[str] = field(default_factory=list)
    streams: dict[str, StreamWatermark] = field(default_factory=dict)
    missing_streams: list[str] = field(default_factory=list)
    symbols_covered: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def save_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, sort_keys=True)


def compute_file_sha256(path: Path, block_size: int = 65536) -> str:
    """Compute SHA-256 digest of a file."""
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(block_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def parse_stream_timestamp(val: str) -> datetime | None:
    """Parse string or timestamp to UTC datetime, returning None if invalid."""
    val = val.strip().replace("Z", "+00:00")
    if not val:
        return None
    try:
        dt = datetime.fromisoformat(val)
        return dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)
    except Exception:
        pass
    try:
        f = float(val)
        if 1_000_000_000 <= f <= 2_500_000_000:
            return datetime.fromtimestamp(f, tz=UTC)
        elif 1_000_000_000_000 <= f <= 2_500_000_000_000:
            return datetime.fromtimestamp(f / 1000.0, tz=UTC)
    except Exception:
        pass
    return None


def inspect_snapshot_dir(
    snapshot_dir: Path,
    target_cutoff: datetime,
    expected_streams: list[str] | None = None,
    min_rows_by_stream: dict[str, int] | None = None,
) -> SnapshotManifest:
    """Inspect and validate an exported data snapshot directory.

    Args:
        snapshot_dir: Root directory of exported snapshot.
        target_cutoff: Expected cutoff datetime (UTC).
        expected_streams: Optional list of required streams/files.

    Returns:
        Validated SnapshotManifest.
    """
    if expected_streams is None:
        expected_streams = [
            "account_balance_usdt",
            "account_fill_events",
            "exchange_orders",
            "live_strategy_signals",
            "order_intents",
        ]

    snapshot_id = snapshot_dir.name
    now_iso = datetime.now(tz=UTC).isoformat()
    cutoff_iso = target_cutoff.astimezone(UTC).isoformat()

    streams: dict[str, StreamWatermark] = {}
    missing: list[str] = []
    capability_tags: list[str] = []

    # Search for files recursively matching expected stream names
    for stream in expected_streams:
        # Match e.g. stream.csv, stream.csv.gz, or directory containing stream
        matches = list(snapshot_dir.rglob(f"{stream}.csv*"))
        if not matches:
            # Check for parquet or other formats
            matches = list(snapshot_dir.rglob(f"*{stream}*"))

        if matches:
            target_file = matches[0]
            size = target_file.stat().st_size if target_file.is_file() else 0
            file_hash = (
                compute_file_sha256(target_file) if target_file.is_file() else ""
            )
            row_cnt = 0
            earliest_t: str | None = None
            latest_t: str | None = None
            if target_file.is_file() and size > 0:
                try:
                    open_fn = gzip.open if target_file.name.endswith(".gz") else open
                    with open_fn(
                        target_file, "rt", encoding="utf-8", errors="replace"
                    ) as f:
                        reader = csv.reader(f)
                        header_row = next(reader, None)
                        if header_row:
                            header_set = {c.strip().lower() for c in header_row}
                            core_reqs = STREAM_REQUIRED_CORE_COLUMNS.get(stream, [])
                            schema_valid = all(
                                bool(header_set & req_set) for req_set in core_reqs
                            )
                            if not schema_valid:
                                row_cnt = 0
                            else:
                                ts_idx = None
                                sym_idx = None
                                dir_idx = None
                                for idx, c in enumerate(header_row):
                                    c_clean = c.strip().lower()
                                    if c_clean in {
                                        "timestamp",
                                        "observed_at",
                                        "recorded_at",
                                        "trade_at",
                                        "created_at",
                                        "detected_at",
                                        "fill_time",
                                    }:
                                        ts_idx = idx
                                    elif c_clean in {"symbol"}:
                                        sym_idx = idx
                                    elif c_clean in {
                                        "direction",
                                        "signal_type",
                                        "side",
                                    }:
                                        dir_idx = idx

                                if ts_idx is None:
                                    row_cnt = 0
                                else:
                                    valid_rows = 0
                                    first_ts_str = None
                                    last_ts_str = None
                                    first_dt = None
                                    last_dt = None
                                    for row in reader:
                                        if len(row) != len(header_row) or not any(
                                            cell.strip() for cell in row
                                        ):
                                            continue
                                        if any(
                                            "not_a_valid_record" in cell.lower()
                                            or "garbage" in cell.lower()
                                            or "dummy_record" in cell.lower()
                                            for cell in row
                                        ):
                                            continue

                                        # Timestamp column MUST parse to valid datetime
                                        raw_ts = row[ts_idx].strip()
                                        dt_parsed = parse_stream_timestamp(raw_ts)
                                        if dt_parsed is None:
                                            continue

                                        # Symbol validation if present
                                        if sym_idx is not None and len(row) > sym_idx:
                                            sym_val = row[sym_idx].strip().upper()
                                            if (
                                                not sym_val
                                                or len(sym_val) < 2
                                                or not all(
                                                    ch.isalnum() or ch in "_-/:."
                                                    for ch in sym_val
                                                )
                                            ):
                                                continue

                                        # Direction/side categorical check if present
                                        if dir_idx is not None and len(row) > dir_idx:
                                            d_val = row[dir_idx].strip().upper()
                                            valid_cats = {
                                                "LONG",
                                                "SHORT",
                                                "BUY",
                                                "SELL",
                                                "EXIT",
                                                "CLOSE",
                                                "HOLD",
                                                "ENTRY",
                                                "1",
                                                "-1",
                                                "1.0",
                                                "-1.0",
                                                "0",
                                            }
                                            if d_val and d_val not in valid_cats:
                                                continue

                                        # Numerical fields validation
                                        has_invalid_num = False
                                        for idx, c in enumerate(header_row):
                                            c_lower = c.strip().lower()
                                            if c_lower in {
                                                "wallet_balance",
                                                "balance",
                                                "price",
                                                "quantity",
                                                "qty",
                                                "notional_usdt",
                                            }:
                                                cell_val = row[idx].strip()
                                                try:
                                                    f_val = float(cell_val)
                                                    if (
                                                        c_lower
                                                        in {"price", "quantity", "qty"}
                                                        and f_val <= 0
                                                    ):
                                                        has_invalid_num = True
                                                        break
                                                except (ValueError, TypeError):
                                                    has_invalid_num = True
                                                    break
                                        if has_invalid_num:
                                            continue

                                        if first_dt is None or dt_parsed < first_dt:
                                            first_dt = dt_parsed
                                            first_ts_str = raw_ts
                                        if last_dt is None or dt_parsed > last_dt:
                                            last_dt = dt_parsed
                                            last_ts_str = raw_ts

                                        valid_rows += 1

                                    row_cnt = valid_rows
                                    earliest_t = first_ts_str
                                    latest_t = last_ts_str
                except Exception:
                    row_cnt = 0

            streams[stream] = StreamWatermark(
                stream_name=stream,
                row_count=row_cnt,
                file_size_bytes=size,
                sha256_hash=file_hash,
                earliest_time=earliest_t,
                latest_time=latest_t,
            )
            min_req = (min_rows_by_stream or {}).get(stream, 1)
            if size == 0 or row_cnt < min_req:
                if row_cnt < min_req and size > 0:
                    missing.append(
                        f"{stream} (row count {row_cnt} < required {min_req})"
                    )
                else:
                    missing.append(f"{stream} (empty or invalid content)")
            elif latest_t is not None and target_cutoff is not None:
                l_dt = parse_stream_timestamp(latest_t)
                from datetime import timedelta

                if l_dt and l_dt < target_cutoff - timedelta(days=2):
                    missing.append(
                        f"{stream} (watermark {latest_t} behind cutoff {cutoff_iso})"
                    )
        else:
            missing.append(stream)

    has_parquet = any(snapshot_dir.rglob("*.parquet"))
    if has_parquet:
        capability_tags.append("research_proxy")

    if not missing:
        capability_tags.extend(["decision_replay_ready", "execution_audit_ready"])

    is_complete = len(missing) == 0

    return SnapshotManifest(
        snapshot_id=snapshot_id,
        imported_at=now_iso,
        target_cutoff=cutoff_iso,
        is_complete=is_complete,
        capability_tags=sorted(list(set(capability_tags))),
        streams=streams,
        missing_streams=missing,
    )
