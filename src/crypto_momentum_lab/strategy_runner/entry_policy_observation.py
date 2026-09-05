"""Non-order persistence for Paper entry-policy observations.

The Paper daemon already has a compare-only hook.  This module gives that
hook an explicit, bounded local sink without coupling observation data to the
Paper artifact repository or to the order/position tables.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Self, TextIO

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    EntryPolicyComparison,
    EntryPolicyComparisonSummary,
    summarize_entry_policy_comparisons,
)


class PaperEntryPolicyObservationError(ValueError):
    """The JSONL observation stream is malformed or unsupported."""


@dataclass(slots=True)
class PaperEntryPolicyComparisonJsonlSink:
    """Append one bounded compare-only observation batch per JSONL record.

    The aggregate summary always covers every comparison in the batch.  The
    individual details are capped so a pathological state cannot make one
    telemetry record unbounded.  This sink is deliberately separate from the
    Paper artifact repository: it records no order, fill, or position state.
    """

    path: Path
    run_id: str | None = None
    max_comparison_details: int = 128
    _stream: TextIO = field(init=False, repr=False)
    _closed: bool = field(init=False, default=False, repr=False)

    def __post_init__(self) -> None:
        if self.run_id is not None and not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        if self.max_comparison_details <= 0:
            raise ValueError("max_comparison_details must be positive")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._stream = self.path.open("a", encoding="utf-8")

    def __call__(
        self,
        state: MarketState15s,
        comparisons: tuple[EntryPolicyComparison, ...],
    ) -> None:
        self.record(state, comparisons)

    def record(
        self,
        state: MarketState15s,
        comparisons: tuple[EntryPolicyComparison, ...],
    ) -> None:
        """Persist one candidate observation and flush it for monitoring.

        Empty batches are intentionally ignored: compare-only is interested in
        candidate eligibility, not one mostly-empty line for every market
        state processed by the daemon.
        """

        if self._closed:
            raise RuntimeError("observation sink is closed")
        if not comparisons:
            return
        summary = summarize_entry_policy_comparisons(comparisons)
        details = comparisons[: self.max_comparison_details]
        payload: dict[str, object] = {
            "schema_version": 1,
            "observed_at": state.bucket_end.isoformat(),
            "bucket_start": state.bucket_start.isoformat(),
            "bucket_end": state.bucket_end.isoformat(),
            "environment": state.environment,
            "symbol": state.symbol,
            "source_trace_id": (
                comparisons[0].source_trace_id if comparisons else None
            ),
            "summary": summary.as_details(),
            "comparison_detail_count": len(comparisons),
            "comparisons_truncated": len(details) < len(comparisons),
            "comparisons": [comparison.as_details() for comparison in details],
        }
        if self.run_id is not None:
            payload["run_id"] = self.run_id
        self._stream.write(
            json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n"
        )
        self._stream.flush()

    def flush(self) -> None:
        """Flush pending bytes without closing the observation stream."""

        if self._closed:
            return
        self._stream.flush()

    def close(self) -> None:
        """Close the sink; repeated closes are harmless."""

        if self._closed:
            return
        self._stream.close()
        self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


@dataclass(frozen=True, slots=True)
class PaperEntryPolicyObservationRecord:
    """One parsed candidate batch from the non-order observation stream."""

    observed_at: datetime
    summary: EntryPolicyComparisonSummary


@dataclass(frozen=True, slots=True)
class PaperEntryPolicyObservationThreshold:
    """Alert limits for one aggregated observation window."""

    max_mismatches: int = 0
    max_mismatch_rate: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        if isinstance(self.max_mismatches, bool) or self.max_mismatches < 0:
            raise ValueError("max_mismatches must be a non-negative integer")
        if (
            not self.max_mismatch_rate.is_finite()
            or self.max_mismatch_rate < 0
            or self.max_mismatch_rate > 1
        ):
            raise ValueError("max_mismatch_rate must be between 0 and 1")

    def alert_reasons(
        self,
        summary: EntryPolicyComparisonSummary,
    ) -> tuple[str, ...]:
        reasons: list[str] = []
        if summary.mismatched > self.max_mismatches:
            reasons.append(
                "mismatched="
                f"{summary.mismatched} exceeds max_mismatches={self.max_mismatches}"
            )
        mismatch_rate = _mismatch_rate(summary)
        if mismatch_rate > self.max_mismatch_rate:
            reasons.append(
                "mismatch_rate="
                f"{_decimal_text(mismatch_rate)} exceeds "
                f"max_mismatch_rate={_decimal_text(self.max_mismatch_rate)}"
            )
        return tuple(reasons)

    def as_details(self) -> dict[str, object]:
        return {
            "max_mismatches": self.max_mismatches,
            "max_mismatch_rate": _decimal_text(self.max_mismatch_rate),
        }


@dataclass(frozen=True, slots=True)
class PaperEntryPolicyObservationReport:
    """Aggregated, JSON-friendly result for one observation file/window."""

    record_count: int
    summary: EntryPolicyComparisonSummary
    first_observed_at: datetime | None
    last_observed_at: datetime | None
    threshold: PaperEntryPolicyObservationThreshold
    alert_reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.record_count < 0:
            raise ValueError("record_count must not be negative")
        for field_name, value in (
            ("first_observed_at", self.first_observed_at),
            ("last_observed_at", self.last_observed_at),
        ):
            if value is not None and (
                value.tzinfo is None or value.utcoffset() is None
            ):
                raise ValueError(f"{field_name} must be timezone-aware")
        if (
            self.first_observed_at is None
        ) != (self.last_observed_at is None):
            raise ValueError("observation window timestamps must be paired")
        if (
            self.first_observed_at is not None
            and self.last_observed_at is not None
            and self.last_observed_at < self.first_observed_at
        ):
            raise ValueError("observation window timestamps must be ordered")

    @property
    def status(self) -> str:
        return "alert" if self.alert_reasons else "ok"

    def as_details(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "record_count": self.record_count,
            "first_observed_at": _optional_datetime_text(
                self.first_observed_at
            ),
            "last_observed_at": _optional_datetime_text(
                self.last_observed_at
            ),
            "summary": self.summary.as_details(),
            "mismatch_rate": _decimal_text(_mismatch_rate(self.summary)),
            "threshold": self.threshold.as_details(),
            "status": self.status,
            "alert_reasons": list(self.alert_reasons),
        }


def read_paper_entry_policy_observations(
    input_path: Path,
) -> tuple[PaperEntryPolicyObservationRecord, ...]:
    """Read and validate sink records without silently skipping corruption."""

    try:
        raw_text = input_path.read_text(encoding="utf-8")
    except OSError as error:
        raise PaperEntryPolicyObservationError(
            f"unable to read observation input {input_path}: {error}"
        ) from error

    records: list[PaperEntryPolicyObservationRecord] = []
    for line_number, line in enumerate(raw_text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as error:
            raise PaperEntryPolicyObservationError(
                f"invalid JSON at observation line {line_number}: {error}"
            ) from error
        try:
            records.append(_parse_observation_record(payload))
        except PaperEntryPolicyObservationError as error:
            raise PaperEntryPolicyObservationError(
                f"invalid observation line {line_number}: {error}"
            ) from error
    return tuple(records)


def summarize_paper_entry_policy_observations(
    records: Iterable[PaperEntryPolicyObservationRecord],
    *,
    threshold: PaperEntryPolicyObservationThreshold | None = None,
) -> PaperEntryPolicyObservationReport:
    """Aggregate low-cardinality counts and evaluate optional alert limits."""

    record_tuple = tuple(records)
    summary = _sum_observation_summaries(
        record.summary for record in record_tuple
    )
    resolved_threshold = threshold or PaperEntryPolicyObservationThreshold()
    observed_at_values = tuple(record.observed_at for record in record_tuple)
    first_observed_at = min(observed_at_values) if observed_at_values else None
    last_observed_at = max(observed_at_values) if observed_at_values else None
    return PaperEntryPolicyObservationReport(
        record_count=len(record_tuple),
        summary=summary,
        first_observed_at=first_observed_at,
        last_observed_at=last_observed_at,
        threshold=resolved_threshold,
        alert_reasons=resolved_threshold.alert_reasons(summary),
    )


def write_paper_entry_policy_observation_report(
    report: PaperEntryPolicyObservationReport,
    output_path: Path,
) -> None:
    """Write a deterministic aggregate report for operators or CI."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report.as_details(), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


def _parse_observation_record(
    payload: object,
) -> PaperEntryPolicyObservationRecord:
    row = _mapping(payload, "observation")
    if row.get("schema_version") != 1:
        raise PaperEntryPolicyObservationError(
            "schema_version must be 1"
        )
    observed_at = _required_datetime(row, "observed_at")
    _required_datetime(row, "bucket_start")
    _required_datetime(row, "bucket_end")
    for field_name in ("environment", "symbol", "source_trace_id"):
        _required_text(row, field_name)
    summary_row = _mapping(row.get("summary"), "summary")
    summary = EntryPolicyComparisonSummary(
        candidates=_required_count(summary_row, "candidates"),
        matched=_required_count(summary_row, "matched"),
        mismatched=_required_count(summary_row, "mismatched"),
        legacy_eligible=_required_count(summary_row, "legacy_eligible"),
        policy_eligible=_required_count(summary_row, "policy_eligible"),
        reduce_only_skipped=_required_count(
            summary_row, "reduce_only_skipped"
        ),
        policy_reasons=_reason_counts(summary_row, "policy_reasons"),
        mismatch_reasons=_reason_counts(summary_row, "mismatch_reasons"),
    )
    detail_count = _required_count(row, "comparison_detail_count")
    comparisons = row.get("comparisons")
    if not isinstance(comparisons, list):
        raise PaperEntryPolicyObservationError("comparisons must be a list")
    truncated = row.get("comparisons_truncated")
    if not isinstance(truncated, bool):
        raise PaperEntryPolicyObservationError(
            "comparisons_truncated must be a boolean"
        )
    if len(comparisons) > detail_count:
        raise PaperEntryPolicyObservationError(
            "comparisons cannot exceed comparison_detail_count"
        )
    if not truncated and len(comparisons) != detail_count:
        raise PaperEntryPolicyObservationError(
            "untruncated comparisons must include every detail"
        )
    if summary.candidates <= 0:
        raise PaperEntryPolicyObservationError(
            "observation summary must contain at least one candidate"
        )
    return PaperEntryPolicyObservationRecord(
        observed_at=observed_at,
        summary=summary,
    )


def _sum_observation_summaries(
    summaries: Iterable[EntryPolicyComparisonSummary],
) -> EntryPolicyComparisonSummary:
    candidates = 0
    matched = 0
    mismatched = 0
    legacy_eligible = 0
    policy_eligible = 0
    reduce_only_skipped = 0
    policy_reasons: dict[str, int] = {}
    mismatch_reasons: dict[str, int] = {}
    for summary in summaries:
        candidates += summary.candidates
        matched += summary.matched
        mismatched += summary.mismatched
        legacy_eligible += summary.legacy_eligible
        policy_eligible += summary.policy_eligible
        reduce_only_skipped += summary.reduce_only_skipped
        _merge_counts(policy_reasons, summary.policy_reasons)
        _merge_counts(mismatch_reasons, summary.mismatch_reasons)
    return EntryPolicyComparisonSummary(
        candidates=candidates,
        matched=matched,
        mismatched=mismatched,
        legacy_eligible=legacy_eligible,
        policy_eligible=policy_eligible,
        reduce_only_skipped=reduce_only_skipped,
        policy_reasons=policy_reasons,
        mismatch_reasons=mismatch_reasons,
    )


def _merge_counts(target: dict[str, int], values: Mapping[str, int]) -> None:
    for key, value in values.items():
        target[key] = target.get(key, 0) + value


def _mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise PaperEntryPolicyObservationError(f"{field_name} must be an object")
    return value


def _required_datetime(
    mapping: Mapping[str, object],
    field_name: str,
) -> datetime:
    value = mapping.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise PaperEntryPolicyObservationError(
            f"{field_name} must be a non-empty ISO timestamp"
        )
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise PaperEntryPolicyObservationError(
            f"{field_name} must be a valid ISO timestamp"
        ) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PaperEntryPolicyObservationError(
            f"{field_name} must be timezone-aware"
        )
    return parsed


def _required_count(mapping: Mapping[str, object], field_name: str) -> int:
    value = mapping.get(field_name)
    return _non_negative_int(value, field_name)


def _required_text(mapping: Mapping[str, object], field_name: str) -> str:
    value = mapping.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise PaperEntryPolicyObservationError(
            f"{field_name} must be a non-empty string"
        )
    return value


def _non_negative_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PaperEntryPolicyObservationError(
            f"{field_name} must be a non-negative integer"
        )
    return value


def _reason_counts(
    mapping: Mapping[str, object],
    field_name: str,
) -> dict[str, int]:
    raw_counts = _mapping(mapping.get(field_name), field_name)
    parsed: dict[str, int] = {}
    for key, value in raw_counts.items():
        if not isinstance(key, str) or not key.strip():
            raise PaperEntryPolicyObservationError(
                f"{field_name} keys must be non-empty strings"
            )
        parsed[key] = _non_negative_int(value, f"{field_name}.{key}")
    return parsed


def _mismatch_rate(summary: EntryPolicyComparisonSummary) -> Decimal:
    if summary.candidates == 0:
        return Decimal("0")
    return Decimal(summary.mismatched) / Decimal(summary.candidates)


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def _optional_datetime_text(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


__all__ = [
    "PaperEntryPolicyComparisonJsonlSink",
    "PaperEntryPolicyObservationError",
    "PaperEntryPolicyObservationRecord",
    "PaperEntryPolicyObservationReport",
    "PaperEntryPolicyObservationThreshold",
    "read_paper_entry_policy_observations",
    "summarize_paper_entry_policy_observations",
    "write_paper_entry_policy_observation_report",
]
