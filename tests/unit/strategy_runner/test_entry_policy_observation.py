import json
from decimal import Decimal
from pathlib import Path

import pytest

from crypto_momentum_lab.domain.strategy import (
    EntryEligibilityDecision,
    EntryPolicyComparison,
)
from crypto_momentum_lab.strategy_runner.entry_policy_observation import (
    PaperEntryPolicyComparisonJsonlSink,
    PaperEntryPolicyObservationError,
    PaperEntryPolicyObservationThreshold,
    read_paper_entry_policy_observations,
    summarize_paper_entry_policy_observations,
    write_paper_entry_policy_observation_report,
)
from tests.unit.persistence.postgres.test_runtime_state_repository import (
    fixture_state,
)


def _comparison(candidate_id: str, *, eligible: bool = True) -> EntryPolicyComparison:
    return EntryPolicyComparison(
        source_trace_id="paper-entry:BTCUSDT:2026-07-03T00:00:00+00:00",
        candidate_id=candidate_id,
        legacy_rejection_reason=None if eligible else "ema_filter_failed",
        policy_decision=EntryEligibilityDecision(
            eligible=eligible,
            reasons=() if eligible else ("ema_filter_failed",),
        ),
    )


def _mismatch(candidate_id: str) -> EntryPolicyComparison:
    return EntryPolicyComparison(
        source_trace_id="paper-entry:BTCUSDT:2026-07-03T00:00:00+00:00",
        candidate_id=candidate_id,
        legacy_rejection_reason=None,
        policy_decision=EntryEligibilityDecision(
            eligible=False,
            reasons=("ema_stale",),
        ),
    )


def test_jsonl_sink_writes_bounded_observation_record(tmp_path: Path) -> None:
    output_path = tmp_path / "nested" / "entry-policy.jsonl"
    state = fixture_state("BTCUSDT", 0)
    comparisons = (
        _comparison("candidate-1"),
        _comparison("candidate-2", eligible=False),
    )

    with PaperEntryPolicyComparisonJsonlSink(
        output_path,
        run_id="paper-run",
        max_comparison_details=1,
    ) as sink:
        sink(state, comparisons)

    records = [json.loads(line) for line in output_path.read_text().splitlines()]
    assert len(records) == 1
    record = records[0]
    assert record["schema_version"] == 1
    assert record["run_id"] == "paper-run"
    assert record["symbol"] == "BTCUSDT"
    assert record["summary"] == {
        "candidates": 2,
        "matched": 2,
        "mismatched": 0,
        "legacy_eligible": 1,
        "policy_eligible": 1,
        "reduce_only_skipped": 0,
        "policy_reasons": {"ema_filter_failed": 1},
        "mismatch_reasons": {},
    }
    assert record["comparison_detail_count"] == 2
    assert record["comparisons_truncated"] is True
    assert len(record["comparisons"]) == 1
    assert "ema5" not in record["comparisons"][0]


def test_jsonl_sink_rejects_invalid_limits_and_closed_writes(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="max_comparison_details"):
        PaperEntryPolicyComparisonJsonlSink(
            tmp_path / "entry-policy.jsonl",
            max_comparison_details=0,
        )

    output_path = tmp_path / "entry-policy.jsonl"
    sink = PaperEntryPolicyComparisonJsonlSink(output_path)
    sink(fixture_state("BTCUSDT", 0), ())
    sink.flush()
    assert output_path.read_text() == ""
    sink.close()
    with pytest.raises(RuntimeError, match="closed"):
        sink(fixture_state("BTCUSDT", 0), ())


def test_observation_report_aggregates_reasons_and_thresholds(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "entry-policy.jsonl"
    first_state = fixture_state("BTCUSDT", 0)
    second_state = fixture_state("BTCUSDT", 1)
    with PaperEntryPolicyComparisonJsonlSink(input_path) as sink:
        sink(first_state, (_comparison("matched"), _mismatch("stale-1")))
        sink(second_state, (_mismatch("stale-2"),))

    records = read_paper_entry_policy_observations(input_path)
    report = summarize_paper_entry_policy_observations(
        records,
        threshold=PaperEntryPolicyObservationThreshold(
            max_mismatches=1,
            max_mismatch_rate=Decimal("1"),
        ),
    )

    assert report.record_count == 2
    assert report.summary.candidates == 3
    assert report.summary.matched == 1
    assert report.summary.mismatched == 2
    assert report.summary.policy_reasons == {"ema_stale": 2}
    assert report.summary.mismatch_reasons == {"ema_stale": 2}
    assert report.status == "alert"
    assert report.alert_reasons == ("mismatched=2 exceeds max_mismatches=1",)
    output_path = tmp_path / "report.json"
    write_paper_entry_policy_observation_report(report, output_path)
    payload = json.loads(output_path.read_text())
    assert payload["summary"]["mismatched"] == 2
    assert payload["mismatch_rate"] == "0.6666666666666666666666666667"
    assert payload["status"] == "alert"


def test_observation_reader_rejects_invalid_schema(tmp_path: Path) -> None:
    input_path = tmp_path / "invalid.jsonl"
    input_path.write_text('{"schema_version": 2}\n')

    with pytest.raises(PaperEntryPolicyObservationError, match="line 1"):
        read_paper_entry_policy_observations(input_path)
