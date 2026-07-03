from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    RunMode,
    StrategyCheckpoint,
    StrategyRunIdentity,
    StrategySide,
    StrategySignal,
)
from crypto_momentum_lab.persistence.postgres.strategy_run_repository import (
    core_fields_match,
    strategy_run_report_rows,
    validate_paper_report,
)
from crypto_momentum_lab.strategy_runner.fills import (
    ReplayExecutionConfig,
    SimulatedFill,
    SimulatedFillStatus,
)
from crypto_momentum_lab.strategy_runner.paper import PaperTradingRunReport


def test_report_rows_convert_decimals_and_enums_to_json_values() -> None:
    report = fixture_paper_report()

    rows = strategy_run_report_rows(report)

    assert rows.run["run_id"] == "paper-test-run"
    assert rows.run["run_mode"] == "paper"
    assert rows.run["execution_config"] == {
        "latency_buckets": 1,
        "slippage_bps": "0",
        "state_interval_seconds": 15,
        "taker_fee_rate": "0.0004",
    }
    assert rows.signals[0]["side"] == "long"
    assert rows.candidates[0]["entry_type"] == "market"
    assert rows.fills[0]["status"] == "filled"
    assert rows.checkpoint["last_processed_at_by_symbol"] == {
        "BTCUSDT": "2026-06-22T00:01:15+00:00"
    }


def test_report_validation_rejects_unknown_candidate_signal() -> None:
    report = fixture_paper_report()
    invalid = replace(
        report,
        candidates=(replace(report.candidates[0], signal_id="missing"),),
    )

    with pytest.raises(ValueError, match="candidate references unknown signal_id"):
        validate_paper_report(invalid)


def test_report_validation_rejects_unknown_fill_candidate() -> None:
    report = fixture_paper_report()
    invalid = replace(
        report,
        paper_fills=(
            replace(report.paper_fills[0], candidate_id="missing"),
        ),
    )

    with pytest.raises(ValueError, match="fill references unknown candidate_id"):
        validate_paper_report(invalid)


def test_core_field_match_detects_conflict() -> None:
    assert core_fields_match(
        {"run_id": "a", "config_hash": "x"},
        {"run_id": "a", "config_hash": "x"},
    )
    assert not core_fields_match(
        {"run_id": "a", "config_hash": "x"},
        {"run_id": "a", "config_hash": "y"},
    )


def test_core_field_match_normalizes_decimal_scale() -> None:
    assert core_fields_match(
        {"candidate_id": "cand_1", "desired_notional": Decimal("100.0")},
        {"candidate_id": "cand_1", "desired_notional": Decimal("100")},
    )


def fixture_paper_report() -> PaperTradingRunReport:
    identity = StrategyRunIdentity(
        run_id="paper-test-run",
        strategy_name="compression_breakout",
        strategy_version="v0",
        config_hash="a" * 64,
        run_mode=RunMode.PAPER,
        code_commit="unknown",
        created_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
        source_paths=("memory",),
    )
    signal = StrategySignal(
        signal_id="sig_1",
        run_id=identity.run_id,
        strategy_name=identity.strategy_name,
        strategy_version=identity.strategy_version,
        config_hash=identity.config_hash,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        detected_at=datetime(2026, 6, 22, 0, 1, tzinfo=UTC),
        source_state_at=datetime(2026, 6, 22, 0, 1, tzinfo=UTC),
        reason="compression_breakout",
        features={"breakout_distance": "0.002"},
        reference_prices={"midpoint": "101.2"},
    )
    candidate = OrderIntentCandidate(
        candidate_id="cand_1",
        signal_id=signal.signal_id,
        run_id=identity.run_id,
        strategy_name=identity.strategy_name,
        strategy_version=identity.strategy_version,
        config_hash=identity.config_hash,
        symbol=signal.symbol,
        side=signal.side,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("100"),
        reduce_only=False,
        expires_at=signal.detected_at + timedelta(seconds=30),
        created_at=signal.detected_at,
        reason="compression_breakout",
        features={"breakout_distance": "0.002"},
    )
    fill = SimulatedFill(
        fill_id="fill_1",
        candidate_id=candidate.candidate_id,
        signal_id=signal.signal_id,
        symbol=signal.symbol,
        side=signal.side,
        status=SimulatedFillStatus.FILLED,
        target_fill_at=datetime(2026, 6, 22, 0, 1, 15, tzinfo=UTC),
        filled_at=datetime(2026, 6, 22, 0, 1, 15, tzinfo=UTC),
        requested_notional=Decimal("100"),
        filled_notional=Decimal("100"),
        quantity=Decimal("0.986"),
        reference_midpoint=Decimal("101.4"),
        spread=Decimal("0.02"),
        fill_price=Decimal("101.41"),
        fee=Decimal("0.0400"),
        total_cost=Decimal("0.04986"),
        cost_bps=Decimal("4.986"),
        reason="filled",
    )
    checkpoint = StrategyCheckpoint(
        last_processed_at_by_symbol={
            "BTCUSDT": datetime(2026, 6, 22, 0, 1, 15, tzinfo=UTC)
        },
        warmup_buckets_by_symbol={"BTCUSDT": 4},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 3},
        payload={"last_signal_id": signal.signal_id},
    )
    return PaperTradingRunReport(
        schema_version=1,
        generated_at=datetime(2026, 6, 22, 0, 2, tzinfo=UTC),
        run=identity,
        execution_config=ReplayExecutionConfig(
            latency_buckets=1,
            taker_fee_rate=Decimal("0.0004"),
            slippage_bps=Decimal("0"),
        ),
        source_description="memory",
        input_state_count=6,
        processed_symbol_count=1,
        signals=(signal,),
        candidates=(candidate,),
        paper_fills=(fill,),
        pending_candidate_count=0,
        rejection_summary={},
        final_checkpoint=checkpoint,
        summary_counts={
            "signals_by_side": {"long": 1},
            "signals_by_symbol": {"BTCUSDT": 1},
        },
        fill_summary={
            "fills_by_status": {"filled": 1},
            "filled_notional_by_symbol": {"BTCUSDT": Decimal("100")},
            "fee_by_symbol": {"BTCUSDT": Decimal("0.0400")},
            "cost_by_symbol": {"BTCUSDT": Decimal("0.04986")},
        },
    )
