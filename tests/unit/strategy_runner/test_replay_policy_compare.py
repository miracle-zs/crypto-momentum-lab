import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import EntryPolicyComparisonRequest
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
)
from crypto_momentum_lab.strategy_runner import (
    EntryPolicyReplayError,
    ReplayConfig,
    build_entry_policy_replay_report,
    read_entry_policy_comparison_requests,
    run_strategy_replay,
    write_entry_policy_replay_report,
)


def test_replay_policy_report_is_complete_and_serializable(
    tmp_path: Path,
) -> None:
    replay = _replay_report()
    candidate = replay.candidates[0]
    request = _request(candidate)

    report = build_entry_policy_replay_report(
        replay_report=replay,
        requests=(request,),
    )

    assert report.source_run_id == replay.run.run_id
    assert report.summary == {
        "candidates": 1,
        "matched": 1,
        "mismatched": 0,
        "legacy_eligible": 1,
        "policy_eligible": 1,
        "reduce_only_skipped": 0,
    }
    assert report.policy_reasons == {}
    assert report.mismatch_reasons == {}

    output = tmp_path / "entry-policy-report.json"
    write_entry_policy_replay_report(report, output)
    payload = json.loads(output.read_text())
    assert payload["schema_version"] == 1
    assert payload["source_run_id"] == replay.run.run_id
    assert payload["summary"]["matched"] == 1
    assert payload["comparisons"][0]["matched"] is True
    assert payload["replay_options"] == {
        "reset_on_gap": True,
        "max_gap_seconds": 30,
    }


def test_replay_policy_report_surfaces_stale_snapshot_mismatch() -> None:
    replay = _replay_report()
    candidate = replay.candidates[0]
    request = _request(
        candidate,
        legacy_rejection_reason=None,
        require_price_above_ema5=True,
        entry_price=Decimal("101"),
        ema5=Decimal("100"),
        ema_observed_at=candidate.created_at - timedelta(minutes=16),
    )

    report = build_entry_policy_replay_report(
        replay_report=replay,
        requests=(request,),
    )

    assert report.summary["mismatched"] == 1
    assert report.policy_reasons == {"ema_stale": 1}
    assert report.mismatch_reasons == {"ema_stale": 1}


def test_replay_policy_report_rejects_partial_requests() -> None:
    replay = _replay_report()

    with pytest.raises(EntryPolicyReplayError, match="missing candidate"):
        build_entry_policy_replay_report(replay_report=replay, requests=())


def test_replay_policy_input_binds_candidate_from_fresh_replay(
    tmp_path: Path,
) -> None:
    replay = _replay_report()
    candidate = replay.candidates[0]
    input_path = tmp_path / "comparison-input.json"
    input_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "requests": [_input_row(candidate)],
            }
        )
    )

    requests = read_entry_policy_comparison_requests(
        input_path=input_path,
        candidates=replay.candidates,
    )

    assert len(requests) == 1
    assert requests[0].candidate == candidate
    assert requests[0].source_trace_id == "replay-state-trace"
    assert requests[0].entry_price == Decimal("101.3")


def test_replay_policy_input_rejects_unknown_candidate(tmp_path: Path) -> None:
    replay = _replay_report()
    input_path = tmp_path / "comparison-input.json"
    row = _input_row(replay.candidates[0])
    row["candidate_id"] = "unknown-candidate"
    input_path.write_text(
        json.dumps({"schema_version": 1, "requests": [row]})
    )

    with pytest.raises(EntryPolicyReplayError, match="unknown candidate"):
        read_entry_policy_comparison_requests(
            input_path=input_path,
            candidates=replay.candidates,
        )


def test_replay_policy_input_rejects_unknown_schema_field(tmp_path: Path) -> None:
    replay = _replay_report()
    input_path = tmp_path / "comparison-input.json"
    row = _input_row(replay.candidates[0])
    row["unexpected"] = "reject-me"
    input_path.write_text(
        json.dumps({"schema_version": 1, "requests": [row]})
    )

    with pytest.raises(EntryPolicyReplayError, match="unknown=unexpected"):
        read_entry_policy_comparison_requests(
            input_path=input_path,
            candidates=replay.candidates,
        )


def test_replay_policy_input_rejects_non_finite_decimal(tmp_path: Path) -> None:
    replay = _replay_report()
    input_path = tmp_path / "comparison-input.json"
    row = _input_row(replay.candidates[0])
    row["entry_price"] = "NaN"
    input_path.write_text(
        json.dumps({"schema_version": 1, "requests": [row]})
    )

    with pytest.raises(EntryPolicyReplayError, match="finite decimal"):
        read_entry_policy_comparison_requests(
            input_path=input_path,
            candidates=replay.candidates,
        )


def _replay_report():
    return run_strategy_replay(
        states=_breakout_states(),
        source_paths=("memory",),
        config=ReplayConfig(
            strategy_name="compression_breakout",
            run_id="replay-policy-test",
            code_commit="unknown",
            generated_at=datetime(2026, 9, 5, tzinfo=UTC),
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
            signal_interval_seconds=15,
            execution=None,
        ),
    )


def _request(candidate, **changes):
    values = {
        "candidate": candidate,
        "source_trace_id": "replay-state-trace",
        "legacy_rejection_reason": None,
        "gate_reasons": (),
        "entry_enabled": True,
        "entry_long_only": False,
        "entry_symbols": None,
        "entry_price": Decimal("101.3"),
        "ema5": None,
        "ema10": None,
        "require_price_above_ema5": False,
        "require_price_above_ema10": False,
        "observed_at": candidate.created_at,
        "ema_observed_at": None,
    }
    values.update(changes)
    return EntryPolicyComparisonRequest(**values)


def _input_row(candidate) -> dict[str, object]:
    return {
        "candidate_id": candidate.candidate_id,
        "source_trace_id": "replay-state-trace",
        "legacy_rejection_reason": None,
        "gate_reasons": [],
        "entry_enabled": True,
        "entry_long_only": False,
        "entry_symbols": None,
        "entry_price": "101.3",
        "ema5": None,
        "ema10": None,
        "require_price_above_ema5": False,
        "require_price_above_ema10": False,
        "observed_at": candidate.created_at.isoformat(),
        "universe_snapshot": None,
        "ema_observed_at": None,
        "ema_snapshot_id": None,
        "ema_config_hash": None,
    }


def _breakout_states() -> tuple[MarketState15s, ...]:
    return (
        _state(0, Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(1, Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(2, Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(3, Decimal("101.0")),
        _state(4, Decimal("101.2")),
    )


def _state(
    bucket_index: int,
    close: Decimal,
    *,
    high: Decimal | None = None,
    low: Decimal | None = None,
) -> MarketState15s:
    bucket_start = datetime(2026, 9, 5, tzinfo=UTC) + timedelta(
        seconds=15 * bucket_index
    )
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
