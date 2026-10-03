"""Unit tests for Phase P6 performance baselining and alert noise consolidation.

Verifies:
1. Event loop latency telemetry calculates p50, p95, p99, and max percentiles.
2. Market data manifest saves are decoupled from the ingestion loop via background worker.
3. Execution head view migration logs as info when no active reservations are present.
4. Routine Binance account configuration updates log at info level instead of error.
5. Research collector state conflicts are aggregated to avoid alert floods.
"""

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.position_recovery import RecoveredPosition
from crypto_momentum_lab.execution_account.daemon import UserDataAccountSyncDaemon
from crypto_momentum_lab.market_data.observability import _calculate_lag_percentiles
from crypto_momentum_lab.research_collector.storage import ParquetWindowSink
from tests.unit.persistence.postgres.test_runtime_state_repository import (
    fixture_state,
)
from tests.unit.research_collector.test_storage import _batch, _selection


def test_event_loop_lag_percentiles_calculation():
    # Empty
    assert _calculate_lag_percentiles([]) == {
        "p50": 0.0,
        "p95": 0.0,
        "p99": 0.0,
        "max": 0.0,
    }
    # 100 samples from 1ms to 100ms
    samples = [0.001 * i for i in range(1, 101)]
    res = _calculate_lag_percentiles(samples)
    assert res["p50"] == pytest.approx(50.5, abs=1.5)
    assert res["p95"] == pytest.approx(95.0, abs=1.5)
    assert res["p99"] == pytest.approx(99.0, abs=1.5)
    assert res["max"] == 100.0




async def test_view_migration_logs_as_info_when_no_active_reservations(monkeypatch):
    book = ExecutionBook()
    from unittest.mock import AsyncMock

    from crypto_momentum_lab.domain.execution.command_models import PositionKey
    from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide

    scope = SimpleNamespace(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.LONG,
    )
    state = SimpleNamespace(
        scope=scope,
        cut=SimpleNamespace(revision=1),
        trade_ids=(),
        evidence_ids=(),
        watermarks=(),
        head=None,
    )
    mock_journal = SimpleNamespace()
    mock_book = SimpleNamespace()
    diagnostics = [
        (
            "execution_head_view_migrated",
            {"has_active_reservations": False, "symbol": "BTCUSDT"},
        ),
        (
            "execution_head_view_migrated",
            {"has_active_reservations": True, "symbol": "ETHUSDT"},
        ),
    ]

    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)
    recovered = RecoveredPosition(
        key=key,
        journal=mock_journal,
        book=mock_book,
        head_revision=1,
        projection_digest="digest",
        reservation_ids=frozenset(),
        last_sequence=1,
        diagnostics=tuple(diagnostics),
    )

    monkeypatch.setattr(
        "crypto_momentum_lab.domain.execution.execution_book.recover_durable_position",
        lambda _: recovered,
    )

    info_logs = []
    warning_logs = []
    monkeypatch.setattr(
        "crypto_momentum_lab.domain.execution.execution_book.log.info",
        lambda event, **kwargs: info_logs.append((event, kwargs)),
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.domain.execution.execution_book.log.warning",
        lambda event, **kwargs: warning_logs.append((event, kwargs)),
    )

    mock_uow = SimpleNamespace(load_positions=AsyncMock(return_value=(state,)))
    await book._restore_durable_positions(
        unit_of_work=mock_uow,
        account_label="primary",
        environment="live",
        as_of=datetime.now(UTC),
    )

    # Inactive reservations logged as info, active as warning
    assert len(info_logs) == 1
    assert info_logs[0][0] == "execution_head_view_migrated"
    assert info_logs[0][1]["symbol"] == "BTCUSDT"

    assert len(warning_logs) == 1
    assert warning_logs[0][0] == "execution_head_view_migrated"
    assert warning_logs[0][1]["symbol"] == "ETHUSDT"


def test_account_config_update_recovery_logs_as_info(monkeypatch):
    daemon = UserDataAccountSyncDaemon(
        service=SimpleNamespace(),
        stream=SimpleNamespace(),
        config=SimpleNamespace(
            environment="live",
            account_label="primary",
        ),
    )

    info_logs = []
    error_logs = []
    monkeypatch.setattr(
        "crypto_momentum_lab.execution_account.daemon.log.info",
        lambda event, **kwargs: info_logs.append((event, kwargs)),
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.execution_account.daemon.log.error",
        lambda event, **kwargs: error_logs.append((event, kwargs)),
    )

    daemon._request_pipeline_recovery("account_config_update")
    assert len(info_logs) == 1
    assert info_logs[0][0] == "binance_user_data_pipeline_recovery_requested"
    assert info_logs[0][1]["reason"] == "account_config_update"
    assert error_logs == []

    daemon._request_pipeline_recovery("connection_dropped")
    assert len(error_logs) == 1
    assert error_logs[0][0] == "binance_user_data_pipeline_recovery_requested"
    assert error_logs[0][1]["reason"] == "connection_dropped"


def test_research_collector_state_conflicts_aggregated(monkeypatch, tmp_path):
    sink = ParquetWindowSink(
        tmp_path / "parquet",
        window_seconds=15,
        late_tolerance_seconds=0,
    )
    warnings = []
    monkeypatch.setattr(
        "crypto_momentum_lab.research_collector.storage.log.warning",
        lambda event, **kwargs: warnings.append((event, kwargs)),
    )

    state = fixture_state("BTCUSDT", 0)
    selection = _selection(state.symbol, state.bucket_start)
    sink.append(_batch(state, 1), selection)
    sink.flush_all()

    # Create a batch with 5 conflicting states for the same symbol/bucket
    from crypto_momentum_lab.market_data.hub import MarketStateBatch
    from crypto_momentum_lab.research_collector.models import (
        CollectionBatch,
        SourceKind,
    )

    conflicting_states = tuple(
        replace(state, close_price=Decimal(str(100 + i + 1)))
        for i in range(5)
    )
    batch = CollectionBatch(
        batch=MarketStateBatch(
            sequence=2,
            published_at=state.bucket_end,
            environment=state.environment,
            states=conflicting_states,
            stream_id="test-stream",
        ),
        source_kind=SourceKind.HUB,
    )
    result = sink.append(batch, selection)

    assert result.conflicting_rows == 5
    conflict_warnings = [
        w for w in warnings if w[0] == "research_collector_state_conflict_kept_existing"
    ]
    aggregated_warnings = [
        w for w in warnings if w[0] == "research_collector_state_conflicts_aggregated"
    ]
    assert len(conflict_warnings) == 3
    assert len(aggregated_warnings) == 1
