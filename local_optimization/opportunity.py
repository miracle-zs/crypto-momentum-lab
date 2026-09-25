"""Raw Opportunity domain model, schema, and manifest validation.

Implements parameter-independent candidate opportunity abstraction (`RawOpportunity`)
and integrity validation gates according to the 2026-09-21 repair design specification.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any

import pandas as pd


class OpportunityStatus(StrEnum):
    """Lifecycle state of an opportunity."""

    DETECTED = "DETECTED"
    FILTERED = "FILTERED"
    ENTRY_REJECTED = "ENTRY_REJECTED"
    ENTERED = "ENTERED"
    EXITED = "EXITED"
    EXPIRED = "EXPIRED"
    CARRY_IN = "CARRY_IN"


def generate_opportunity_id(
    symbol: str,
    detected_epoch: float,
    *args: Any,
    direction: str = "LONG",
    impulse_window_buckets: int | None = None,
    confirmation_buckets: int | None = None,
    **kwargs: Any,
) -> str:
    """Generate a stable, deterministic opportunity ID.

    The identity of an opportunity depends strictly on the underlying market
    signal's atomic identity: (symbol, detected_epoch, direction, w, c).
    It is invariant to downstream candidate execution thresholds (min_return,
    min_imbalance, min_intensity, volume_ratio, cooldown, slots, etc.).
    """
    w_val = impulse_window_buckets or kwargs.get("w")
    c_val = confirmation_buckets or kwargs.get("c")
    dir_val = direction

    if len(args) == 1 and isinstance(args[0], str):
        dir_val = args[0]
    elif len(args) >= 3:
        if isinstance(args[0], int) and isinstance(args[1], int):
            w_val = args[0]
            c_val = args[1]
        if isinstance(args[2], str):
            dir_val = args[2]

    if w_val is not None and c_val is not None:
        token = (
            f"{symbol.upper()}_{detected_epoch:.1f}_w{w_val}c{c_val}_{dir_val.upper()}"
        )
        digest = hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]
        return f"opp_{symbol.lower()}_{int(detected_epoch)}_w{w_val}c{c_val}_{digest}"

    token = f"{symbol.upper()}_{detected_epoch:.1f}_{dir_val.upper()}"
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]
    return f"opp_{symbol.lower()}_{int(detected_epoch)}_{digest}"


def parse_utc_timestamp(value: Any) -> datetime:
    """Parse string or timestamp to timezone-aware UTC datetime."""
    if value is None or pd.isna(value) or value == "":
        raise ValueError(f"Invalid timestamp value: {value}")
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=UTC)
    s = str(value).strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    return dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)


@dataclass(frozen=True)
class RawOpportunity:
    """A parameter-independent trading opportunity detected from market state."""

    opportunity_id: str
    symbol: str
    direction: str  # "LONG" or "SHORT"
    detected_at: datetime
    detected_epoch: float
    entry_eligible_at: datetime
    entry_reference_price: float

    # Signal & impulse properties
    impulse_window_buckets: int
    confirmation_buckets: int
    impulse_return_pct: float
    aggressive_imbalance: float
    confirmation_min_imbalance: float
    notional_intensity: float
    volume_ratio: float  # e.g. 5m vs 30m

    # Exit specification
    exit_time: datetime | None = None
    exit_submitted_at: datetime | None = None
    exit_price: float | None = None
    exit_rule: str = "candle_15m"

    # Execution economics
    fee_rate: float = 0.0005  # 0.05% per side
    slippage_rate: float = 0.0002  # 0.02% estimated
    funding_cost_usdt: float = 0.0
    net_pnl_usdt: float | None = None

    # Lineage and governance
    strategy_version: str = "v1.0"
    market_data_version: str = "15s_v1"
    status: OpportunityStatus = OpportunityStatus.DETECTED
    entry_epoch: float = 0.0
    exit_epoch: float | None = None

    def __post_init__(self) -> None:
        if self.entry_epoch == 0.0 and self.entry_eligible_at is not None:
            object.__setattr__(self, "entry_epoch", self.entry_eligible_at.timestamp())
        if self.exit_epoch is None and self.exit_time is not None:
            object.__setattr__(self, "exit_epoch", self.exit_time.timestamp())

    @property
    def entry_price(self) -> float:
        return self.entry_reference_price

    @property
    def entry_at(self) -> datetime:
        return self.entry_eligible_at

    @property
    def exit_submitted_epoch(self) -> float | None:
        if self.exit_submitted_at:
            return self.exit_submitted_at.timestamp()
        return self.exit_epoch

    @property
    def exit_at(self) -> datetime | None:
        return self.exit_time

    def __contains__(self, item: Any) -> bool:
        if isinstance(item, str):
            return hasattr(self, item)
        return False

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)

    def get(self, item: str, default: Any = None) -> Any:
        return getattr(self, item, default)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["detected_at"] = self.detected_at.isoformat()
        d["entry_eligible_at"] = self.entry_eligible_at.isoformat()
        d["exit_time"] = self.exit_time.isoformat() if self.exit_time else None
        d["exit_submitted_at"] = (
            self.exit_submitted_at.isoformat() if self.exit_submitted_at else None
        )
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RawOpportunity:
        def _is_valid(val: Any) -> bool:
            return val is not None and not pd.isna(val) and val != ""

        det_at = parse_utc_timestamp(data["detected_at"])
        det_epoch = float(data.get("detected_epoch", det_at.timestamp()))
        entry_el = (
            parse_utc_timestamp(data["entry_eligible_at"])
            if _is_valid(data.get("entry_eligible_at"))
            else det_at
        )
        exit_t = (
            parse_utc_timestamp(data["exit_time"])
            if _is_valid(data.get("exit_time"))
            else None
        )
        exit_sub_t = (
            parse_utc_timestamp(data["exit_submitted_at"])
            if _is_valid(data.get("exit_submitted_at"))
            else None
        )
        if exit_sub_t is None and exit_t is not None:
            exit_rule_str = str(data.get("exit_rule", ""))
            if exit_rule_str == "candle_15m_bearish":
                exit_sub_t = exit_t
            elif "candle_15m_grace_timeout_" in exit_rule_str:
                exit_sub_t = exit_t - timedelta(seconds=7200)
            elif "candle_15m_bearish_grace_limit_" in exit_rule_str:
                ts_sec = exit_t.timestamp()
                sub_sec = math.floor(ts_sec / 900.0) * 900.0
                if sub_sec >= entry_el.timestamp():
                    exit_sub_t = datetime.fromtimestamp(sub_sec, tz=UTC)
                else:
                    exit_sub_t = entry_el
            else:
                exit_sub_t = exit_t

        sym = str(data["symbol"])
        req_w = int(data.get("impulse_window_buckets", 2))
        req_c = int(data.get("confirmation_buckets", 1))
        direction = str(data.get("direction", "LONG")).upper()

        opp_id = data.get("opportunity_id")
        if not opp_id:
            opp_id = generate_opportunity_id(sym, det_epoch, req_w, req_c, direction)

        raw_status = data.get("status", OpportunityStatus.DETECTED.value)
        status = (
            OpportunityStatus(raw_status)
            if isinstance(raw_status, str)
            else OpportunityStatus.DETECTED
        )

        def _to_f(val: Any, default: float = 0.0) -> float:
            if not _is_valid(val):
                return default
            return float(val)

        return cls(
            opportunity_id=opp_id,
            symbol=sym,
            direction=direction,
            detected_at=det_at,
            detected_epoch=det_epoch,
            entry_eligible_at=entry_el,
            entry_reference_price=_to_f(data.get("entry_reference_price")),
            impulse_window_buckets=req_w,
            confirmation_buckets=req_c,
            impulse_return_pct=_to_f(data.get("impulse_return_pct")),
            aggressive_imbalance=_to_f(data.get("aggressive_imbalance")),
            confirmation_min_imbalance=_to_f(data.get("confirmation_min_imbalance")),
            notional_intensity=_to_f(data.get("notional_intensity")),
            volume_ratio=_to_f(
                data.get("volume_ratio", data.get("notional_5m_vs_30m", 0.0))
            ),
            exit_time=exit_t,
            exit_submitted_at=exit_sub_t,
            exit_price=_to_f(data.get("exit_price")) if exit_t else None,
            exit_rule=str(data.get("exit_rule", "candle_15m")),
            fee_rate=_to_f(data.get("fee_rate", 0.0005), 0.0005),
            slippage_rate=_to_f(data.get("slippage_rate", 0.0002), 0.0002),
            funding_cost_usdt=_to_f(data.get("funding_cost_usdt", 0.0), 0.0),
            net_pnl_usdt=(
                _to_f(data.get("net_pnl_usdt"))
                if _is_valid(data.get("net_pnl_usdt"))
                else None
            ),
            strategy_version=str(data.get("strategy_version", "v1.0")),
            market_data_version=str(data.get("market_data_version", "15s_v1")),
            status=status,
            entry_epoch=entry_el.timestamp(),
            exit_epoch=exit_t.timestamp() if exit_t is not None else None,
        )


@dataclass(frozen=True)
class OpportunityPoolManifest:
    """Metadata and integrity governance descriptor for an opportunity pool."""

    snapshot_id: str
    created_at: datetime
    symbol_count: int
    row_count: int
    watermark_start: datetime
    watermark_end: datetime
    content_hash: str
    pool_type: str = "raw_parameter_independent"
    strategy_version: str = "v1.0"
    schema_version: str = "1.0"

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "created_at": self.created_at.isoformat(),
            "symbol_count": self.symbol_count,
            "row_count": self.row_count,
            "watermark_start": self.watermark_start.isoformat(),
            "watermark_end": self.watermark_end.isoformat(),
            "content_hash": self.content_hash,
            "pool_type": self.pool_type,
            "strategy_version": self.strategy_version,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> OpportunityPoolManifest:
        return cls(
            snapshot_id=str(d["snapshot_id"]),
            created_at=parse_utc_timestamp(d["created_at"]),
            symbol_count=int(d["symbol_count"]),
            row_count=int(d["row_count"]),
            watermark_start=parse_utc_timestamp(d["watermark_start"]),
            watermark_end=parse_utc_timestamp(d["watermark_end"]),
            content_hash=str(d["content_hash"]),
            pool_type=str(d.get("pool_type", "raw_parameter_independent")),
            strategy_version=str(d.get("strategy_version", "v1.0")),
            schema_version=str(d.get("schema_version", "1.0")),
        )


def validate_opportunity_pool(
    opportunities: Sequence[RawOpportunity],
    manifest: OpportunityPoolManifest | None = None,
) -> list[str]:
    """Validate completeness, causality, and integrity of the opportunity pool.

    Returns a list of error strings; empty list indicates validation passed.
    """
    errors: list[str] = []
    if not opportunities:
        errors.append("Opportunity pool is completely empty.")
        return errors

    seen_ids: set[str] = set()
    prev_epoch = -float("inf")
    symbols: set[str] = set()

    for i, opp in enumerate(opportunities):
        if not opp.opportunity_id:
            errors.append(f"Row {i}: Missing opportunity_id.")
        elif opp.opportunity_id in seen_ids:
            errors.append(f"Row {i}: Duplicate opportunity_id '{opp.opportunity_id}'.")
        seen_ids.add(opp.opportunity_id)

        if opp.entry_eligible_at < opp.detected_at:
            errors.append(
                f"Row {i} ({opp.opportunity_id}): Causality violation: "
                f"entry_eligible_at ({opp.entry_eligible_at}) "
                f"< detected_at ({opp.detected_at})."
            )

        if opp.exit_time and opp.exit_time < opp.entry_eligible_at:
            errors.append(
                f"Row {i} ({opp.opportunity_id}): Causality violation: "
                f"exit_time ({opp.exit_time}) "
                f"< entry_eligible_at ({opp.entry_eligible_at})."
            )

        if (
            opp.entry_reference_price is None
            or math.isnan(opp.entry_reference_price)
            or not math.isfinite(opp.entry_reference_price)
            or opp.entry_reference_price <= 0
        ):
            errors.append(
                f"Row {i} ({opp.opportunity_id}): Invalid entry_reference_price "
                f"'{opp.entry_reference_price}'. Must be finite and strictly > 0."
            )

        if opp.status == OpportunityStatus.EXITED:
            if opp.exit_time is None:
                errors.append(
                    f"Row {i} ({opp.opportunity_id}): Status is EXITED but "
                    "exit_time is None."
                )
            if (
                opp.exit_price is None
                or math.isnan(opp.exit_price)
                or not math.isfinite(opp.exit_price)
                or opp.exit_price <= 0
            ):
                errors.append(
                    f"Row {i} ({opp.opportunity_id}): Status is EXITED but "
                    f"invalid exit_price '{opp.exit_price}'. "
                    "Must be finite and strictly > 0."
                )

        for feat_name, feat_val in (
            ("impulse_return_pct", opp.impulse_return_pct),
            ("aggressive_imbalance", opp.aggressive_imbalance),
            ("confirmation_min_imbalance", opp.confirmation_min_imbalance),
            ("notional_intensity", opp.notional_intensity),
            ("volume_ratio", opp.volume_ratio),
        ):
            if feat_val is None or math.isnan(feat_val) or not math.isfinite(feat_val):
                errors.append(
                    f"Row {i} ({opp.opportunity_id}): Invalid feature '{feat_name}' "
                    f"({feat_val}). Must be finite float, cannot be NaN/None."
                )

        if opp.impulse_window_buckets <= 0:
            errors.append(
                f"Row {i} ({opp.opportunity_id}): impulse_window_buckets must be > 0, "
                f"got {opp.impulse_window_buckets}."
            )
        if opp.confirmation_buckets <= 0:
            errors.append(
                f"Row {i} ({opp.opportunity_id}): confirmation_buckets must be > 0, "
                f"got {opp.confirmation_buckets}."
            )

        if opp.direction not in ("LONG", "SHORT", "UP", "DOWN"):
            errors.append(
                f"Row {i} ({opp.opportunity_id}): Invalid direction '{opp.direction}'."
            )

        if opp.detected_epoch < prev_epoch:
            errors.append(
                f"Row {i} ({opp.opportunity_id}): Chronological ordering violation: "
                f"detected_epoch {opp.detected_epoch} < previous {prev_epoch}."
            )
        prev_epoch = opp.detected_epoch
        symbols.add(opp.symbol)

    if manifest is not None:
        if manifest.pool_type != "raw_parameter_independent":
            errors.append(
                f"Invalid pool_type '{manifest.pool_type}': formal optimization "
                "requires 'raw_parameter_independent'."
            )
        if manifest.row_count != len(opportunities):
            errors.append(
                f"Row count mismatch: manifest {manifest.row_count} "
                f"!= pool {len(opportunities)}."
            )
        if manifest.symbol_count != len(symbols):
            errors.append(
                f"Symbol count mismatch: manifest {manifest.symbol_count} "
                f"!= pool {len(symbols)}."
            )
        min_detected = min(opp.detected_at for opp in opportunities)
        max_detected = max(opp.detected_at for opp in opportunities)
        if min_detected < manifest.watermark_start:
            errors.append(
                f"Watermark start violation: {min_detected} "
                f"< {manifest.watermark_start}."
            )
        if max_detected > manifest.watermark_end:
            errors.append(
                f"Watermark end violation: {max_detected} > {manifest.watermark_end}."
            )
        if manifest.content_hash:
            computed_hash = compute_pool_content_hash(opportunities)
            if manifest.content_hash != computed_hash:
                errors.append(
                    f"Content hash mismatch: manifest '{manifest.content_hash}' "
                    f"!= computed '{computed_hash}'."
                )

    return errors


def compute_pool_content_hash(opportunities: Sequence[RawOpportunity]) -> str:
    """Compute deterministic SHA256 digest covering all normalized fields."""
    hasher = hashlib.sha256()
    for opp in sorted(
        opportunities,
        key=lambda x: (x.opportunity_id, x.detected_epoch, x.symbol),
    ):
        exit_time_str = opp.exit_time.isoformat() if opp.exit_time else ""
        exit_price_str = f"{opp.exit_price:.6f}" if opp.exit_price is not None else ""
        net_pnl_str = f"{opp.net_pnl_usdt:.4f}" if opp.net_pnl_usdt is not None else ""
        token = (
            f"{opp.opportunity_id}|{opp.symbol}|{opp.direction}|"
            f"{opp.detected_epoch:.1f}|{opp.entry_eligible_at.timestamp():.1f}|"
            f"{opp.entry_reference_price:.6f}|{opp.impulse_window_buckets}|"
            f"{opp.confirmation_buckets}|{opp.impulse_return_pct:.6f}|"
            f"{opp.aggressive_imbalance:.6f}|{opp.confirmation_min_imbalance:.6f}|"
            f"{opp.notional_intensity:.6f}|{opp.volume_ratio:.6f}|"
            f"{exit_time_str}|{exit_price_str}|{opp.exit_rule}|"
            f"{opp.fee_rate:.6f}|{opp.slippage_rate:.6f}|"
            f"{opp.funding_cost_usdt:.4f}|{net_pnl_str}|"
            f"{opp.strategy_version}|{opp.market_data_version}\n"
        )
        hasher.update(token.encode("utf-8"))
    return hasher.hexdigest()


def load_opportunity_pool(
    target_path_or_dir: Path,
    manifest_path: Path | None = None,
    *,
    require_manifest: bool = False,
) -> tuple[list[RawOpportunity], OpportunityPoolManifest]:
    """Load opportunity pool and manifest from parquet, jsonl, CSV, or directory."""
    target = Path(target_path_or_dir)

    opp_file: Path | None = None
    man_file: Path | None = manifest_path

    if target.is_dir():
        for candidate_name in [
            "raw_opportunities.parquet",
            "opportunities.parquet",
            "opportunity_pool.parquet",
            "opportunity_pool.jsonl",
            "raw_opportunities.jsonl",
            "opportunities.jsonl",
            "opportunity_pool.csv",
            "raw_opportunities.csv",
            "opportunities.csv",
        ]:
            p = target / candidate_name
            if p.exists():
                opp_file = p
                break
        if man_file is None:
            mp = target / "manifest.json"
            if mp.exists():
                man_file = mp
    else:
        opp_file = target
        if man_file is None:
            mp = target.parent / "manifest.json"
            if mp.exists():
                man_file = mp

    if opp_file is None or not opp_file.exists():
        raise FileNotFoundError(f"No opportunity pool file found at {target}")

    if require_manifest and (man_file is None or not man_file.exists()):
        raise FileNotFoundError(
            f"Manifest file required but not found for opportunity pool at {target}"
        )

    opportunities: list[RawOpportunity] = []

    if opp_file.suffix == ".parquet":
        df = pd.read_parquet(opp_file)
        for _, row in df.iterrows():
            opportunities.append(RawOpportunity.from_dict(row.to_dict()))
    elif opp_file.suffix in (".jsonl", ".ndjson"):
        with opp_file.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                if "entry_at" in row and "entry_eligible_at" not in row:
                    row["entry_eligible_at"] = row["entry_at"]
                if "entry_price" in row and "entry_reference_price" not in row:
                    row["entry_reference_price"] = row["entry_price"]
                if "exit_at" in row and "exit_time" not in row:
                    row["exit_time"] = row["exit_at"]
                if "aggressive_imbalance" in row and "imbalance" not in row:
                    row["aggressive_imbalance"] = row["aggressive_imbalance"]
                if "volume_ratio" not in row and "notional_5m_vs_30m" in row:
                    row["volume_ratio"] = row["notional_5m_vs_30m"]
                opportunities.append(RawOpportunity.from_dict(row))
    else:
        with opp_file.open("r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "entry_at" in row and "entry_eligible_at" not in row:
                    row["entry_eligible_at"] = row["entry_at"]
                if "entry_price" in row and "entry_reference_price" not in row:
                    row["entry_reference_price"] = row["entry_price"]
                if "exit_at" in row and "exit_time" not in row:
                    row["exit_time"] = row["exit_at"]
                if "aggressive_imbalance" in row and "imbalance" not in row:
                    row["aggressive_imbalance"] = row["aggressive_imbalance"]
                if "volume_ratio" not in row and "notional_5m_vs_30m" in row:
                    row["volume_ratio"] = row["notional_5m_vs_30m"]
                opportunities.append(RawOpportunity.from_dict(row))

    # Sort deterministically by detected_epoch, then opportunity_id
    opportunities.sort(key=lambda x: (x.detected_epoch, x.opportunity_id))

    manifest: OpportunityPoolManifest
    if man_file is not None and man_file.exists():
        with man_file.open("r", encoding="utf-8") as f:
            manifest = OpportunityPoolManifest.from_dict(json.load(f))
    elif require_manifest:
        raise FileNotFoundError(
            f"Manifest file required but not found for opportunity pool at {target}"
        )
    else:
        # Synthesize manifest from the loaded opportunities
        c_hash = compute_pool_content_hash(opportunities)
        w_start = (
            min(opp.detected_at for opp in opportunities)
            if opportunities
            else datetime.now(UTC)
        )
        w_end = (
            max(opp.detected_at for opp in opportunities)
            if opportunities
            else datetime.now(UTC)
        )
        sym_count = len({opp.symbol for opp in opportunities})
        manifest = OpportunityPoolManifest(
            snapshot_id=opp_file.stem,
            created_at=datetime.now(UTC),
            symbol_count=sym_count,
            row_count=len(opportunities),
            watermark_start=w_start,
            watermark_end=w_end,
            content_hash=c_hash,
        )

    return opportunities, manifest


def load_top10_lookup(
    parquet_dir: Path | None = None,
    *,
    cache_path: Path | None = None,
    max_rank: int = 10,
    environment: str = "research",
    force_rebuild: bool = False,
) -> set[tuple[str, str]]:
    """Build or load cached (symbol, bucket_start_str) lookup for Top-N universe.

    Uses pyarrow.dataset predicate pushdown for sub-second extraction across
    parquet files. Result format is set of (symbol, '%Y-%m-%d %H:%M:%S+00:00').
    Automatically detects stale caches if parquet partitions are newer.
    """
    import pickle

    if cache_path and cache_path.exists() and not force_rebuild:
        # Check staleness against parquet partition directories
        is_stale = False
        if parquet_dir and parquet_dir.exists():
            c_mtime = cache_path.stat().st_mtime
            for d in parquet_dir.glob("date=*"):
                if d.stat().st_mtime > c_mtime:
                    is_stale = True
                    break
        if not is_stale:
            try:
                with cache_path.open("rb") as f:
                    return pickle.load(f)
            except Exception:
                pass

    if parquet_dir is None or not parquet_dir.exists():
        return set()

    import pyarrow.compute as pc
    import pyarrow.dataset as ds

    dataset = ds.dataset(str(parquet_dir), format="parquet")
    expr = pc.field("gainer_rank").is_valid() & (pc.field("gainer_rank") <= max_rank)
    if environment and "environment" in dataset.schema.names:
        expr = expr & (pc.field("environment") == environment)

    scanner = dataset.scanner(columns=["symbol", "bucket_start"], filter=expr)
    table = scanner.to_table()
    df = table.to_pandas()
    if df.empty:
        return set()
    s = set(zip(df["symbol"], df["bucket_start"].astype(str), strict=False))
    if cache_path:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with cache_path.open("wb") as f:
                pickle.dump(s, f)
        except Exception:
            pass
    return s


def filter_opportunities_by_top10(
    opportunities: Sequence[Any],
    top10_lookup: set[Any],
) -> list[Any]:
    """Filter raw opportunities strictly to those occurring within Top 10 universe."""
    if not top10_lookup:
        return list(opportunities)
    filtered: list[Any] = []
    for opp in opportunities:
        sym = getattr(opp, "symbol", None) or (
            opp.get("symbol") if isinstance(opp, dict) else None
        )
        t_val = getattr(opp, "detected_at", None) or (
            opp.get("detected_at") if isinstance(opp, dict) else None
        )
        if not sym or not t_val:
            continue
        if isinstance(t_val, datetime):
            minute = (t_val.minute // 15) * 15
            bucket = t_val.replace(minute=minute, second=0, microsecond=0)
            bucket_str = bucket.strftime("%Y-%m-%d %H:%M:%S+00:00")
        else:
            bucket_str = str(t_val)
            bucket = t_val
        if (sym, bucket_str) in top10_lookup or (sym, bucket) in top10_lookup:
            filtered.append(opp)
    return filtered
