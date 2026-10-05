import asyncio

import pytest

import crypto_momentum_lab.market_data.observability as observability
from crypto_momentum_lab.domain.market.models import CaptureStream, MarketDataState
from crypto_momentum_lab.market_data.binance.websocket import (
    BinanceWebSocketMetricsSnapshot,
)


def _capture_metrics(**overrides) -> observability.CaptureMetricsSnapshot:
    values = dict(
        state=MarketDataState.READY,
        monitoring_generation=1,
        monitoring_symbols=0,
        desired_subscriptions=0,
        active_subscriptions=0,
        active_connections=0,
        reconnect_count=0,
        received_messages=0,
        received_bytes=0,
        queue_events=0,
        queue_bytes=0,
        archived_rows=0,
        archived_bytes=0,
        open_writers=0,
        pending_manifests=0,
        oldest_pending_manifest_seconds=None,
        disk_free_bytes=0,
    )
    values.update(overrides)
    return observability.CaptureMetricsSnapshot(**values)


def test_event_loop_lag_level_uses_warning_and_critical_thresholds() -> None:
    assert observability._event_loop_lag_level(0.049, 0.05, 0.5) is None
    assert observability._event_loop_lag_level(0.05, 0.05, 0.5) == "warning"
    assert observability._event_loop_lag_level(0.5, 0.05, 0.5) == "critical"


@pytest.mark.asyncio
async def test_market_data_health_emits_task_diagnostics_for_critical_lag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reported = asyncio.Event()
    warnings: list[tuple[str, dict[str, object]]] = []

    class FakeLog:
        def info(self, event: str, **fields: object) -> None:
            del event, fields

        def warning(self, event: str, **fields: object) -> None:
            warnings.append((event, fields))
            if event == "market_data_critical_event_loop_lag_diagnostics":
                reported.set()

    monkeypatch.setattr(observability, "log", FakeLog())
    task = asyncio.create_task(
        observability.monitor_market_data_health(
            capture_metrics=_capture_metrics,
            connection_metrics=lambda: (
                observability.BinanceConnectionPoolMetricsSnapshot(
                    active_connections=0,
                    ready_connections=0,
                    desired_subscriptions=0,
                    reconnect_count=0,
                    ack_mismatch_count=0,
                    control_commands_sent=0,
                    received_messages=0,
                )
            ),
            report_interval_seconds=0.01,
            sample_interval_seconds=0.001,
            event_loop_lag_warning_seconds=0.000001,
            event_loop_lag_critical_seconds=0.000002,
        )
    )
    try:
        await asyncio.wait_for(reported.wait(), timeout=1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    diagnostics = next(
        fields
        for event, fields in warnings
        if event == "market_data_critical_event_loop_lag_diagnostics"
    )
    task_diagnostics = diagnostics["task_diagnostics"]
    assert isinstance(task_diagnostics, tuple)
    assert task_diagnostics
    assert all("stack" in item for item in task_diagnostics)


@pytest.mark.asyncio
async def test_market_data_health_monitor_reports_runtime_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reported = asyncio.Event()
    records: list[dict[str, object]] = []

    class FakeLog:
        def info(self, event: str, **fields: object) -> None:
            assert event == "market_data_health_snapshot"
            records.append(fields)
            reported.set()

    monkeypatch.setattr(observability, "log", FakeLog())
    monkeypatch.setattr(
        observability,
        "cgroup_memory_snapshot",
        lambda: {
            "cgroup_memory_current_bytes": 1000,
            "cgroup_memory_limit_bytes": 2000,
        },
    )
    monkeypatch.setattr(
        observability,
        "tracemalloc_memory_snapshot",
        lambda: {
            "tracemalloc_enabled": True,
            "tracemalloc_current_bytes": 123,
            "tracemalloc_peak_bytes": 456,
        },
    )

    task = asyncio.create_task(
        observability.monitor_market_data_health(
            capture_metrics=lambda: _capture_metrics(
                queue_events=7,
                queue_bytes=1024,
                monitoring_symbols=125,
            ),
            connection_metrics=lambda: (
                observability.BinanceConnectionPoolMetricsSnapshot(
                    desired_subscriptions=100,
                    active_connections=3,
                    ready_connections=2,
                    reconnect_count=4,
                    ack_mismatch_count=1,
                    control_commands_sent=9,
                    received_messages=100,
                )
            ),
            report_interval_seconds=0.01,
            sample_interval_seconds=0.001,
        )
    )
    try:
        await asyncio.wait_for(reported.wait(), timeout=1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert records
    assert records[0]["reconnect_count"] == 4
    assert records[0]["ack_mismatch_count"] == 1
    assert records[0]["queue_events"] == 7
    assert records[0]["received_message_rate"] > 0
    assert records[0]["event_loop_lag_ms"] is not None
    assert records[0]["event_loop_lag_p50_ms"] is not None
    assert records[0]["event_loop_lag_p95_ms"] is not None
    assert records[0]["event_loop_lag_p99_ms"] is not None
    assert records[0]["event_loop_lag_max_ms"] is not None
    assert records[0]["rss_bytes"] is not None
    assert records[0]["cgroup_memory_current_bytes"] == 1000
    assert records[0]["cgroup_memory_limit_bytes"] == 2000
    assert records[0]["tracemalloc_current_bytes"] == 123
    assert records[0]["tracemalloc_peak_bytes"] == 456


def test_calculate_lag_percentiles() -> None:
    assert observability._calculate_lag_percentiles([]) == {
        "p50": 0.0,
        "p95": 0.0,
        "p99": 0.0,
        "max": 0.0,
    }
    samples = [0.001 * i for i in range(1, 101)]
    res = observability._calculate_lag_percentiles(samples)
    assert res["p50"] == pytest.approx(50.5, abs=1.5)
    assert res["p95"] == pytest.approx(95.0, abs=1.5)
    assert res["p99"] == pytest.approx(99.0, abs=1.5)
    assert res["max"] == 100.0


@pytest.mark.asyncio
async def test_market_data_health_monitor_alerts_on_pressure_and_dead_dispatcher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alerted = asyncio.Event()
    warnings: list[tuple[str, dict[str, object]]] = []
    errors: list[tuple[str, dict[str, object]]] = []

    class FakeLog:
        def info(self, event: str, **fields: object) -> None:
            del event, fields

        def warning(self, event: str, **fields: object) -> None:
            warnings.append((event, fields))

        def error(self, event: str, **fields: object) -> None:
            errors.append((event, fields))
            alerted.set()

    monkeypatch.setattr(observability, "log", FakeLog())
    connection = BinanceWebSocketMetricsSnapshot(
        connection_attempts=1,
        control_commands_sent=1,
        group_id="aggTrade:0001",
        stream=CaptureStream.AGG_TRADE,
        desired_subscriptions=100,
        active=True,
        ready=True,
        reconnect_count=0,
        ack_mismatch_count=0,
        received_messages=100,
        received_bytes=1000,
        last_message_age_seconds=0.1,
        last_close_code=None,
        last_reason=None,
        reader_task_alive=True,
        dispatch_task_alive=False,
    )
    task = asyncio.create_task(
        observability.monitor_market_data_health(
            capture_metrics=lambda: _capture_metrics(
                queue_events=9,
                queue_bytes=90,
                queue_max_events=10,
                queue_max_bytes=100,
                monitoring_symbols=125,
            ),
            connection_metrics=lambda: (
                observability.BinanceConnectionPoolMetricsSnapshot(
                    desired_subscriptions=100,
                    active_connections=1,
                    ready_connections=1,
                    reconnect_count=0,
                    ack_mismatch_count=0,
                    control_commands_sent=1,
                    received_messages=100,
                    connection_snapshots=(connection,),
                )
            ),
            report_interval_seconds=0.01,
            sample_interval_seconds=0.001,
        )
    )
    try:
        await asyncio.wait_for(alerted.wait(), timeout=1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert warnings[0][0] == "market_data_queue_pressure"
    assert errors[0][0] == "market_data_connection_task_not_alive"
