import asyncio
from types import SimpleNamespace

import pytest

import crypto_momentum_lab.market_data.observability as observability


def test_event_loop_lag_level_uses_warning_and_critical_thresholds() -> None:
    assert observability._event_loop_lag_level(0.049, 0.05, 0.5) is None
    assert observability._event_loop_lag_level(0.05, 0.05, 0.5) == "warning"
    assert observability._event_loop_lag_level(0.5, 0.05, 0.5) == "critical"


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

    task = asyncio.create_task(
        observability.monitor_market_data_health(
            capture_metrics=lambda: SimpleNamespace(
                queue_events=7,
                queue_bytes=1024,
                monitoring_symbols=125,
            ),
            connection_metrics=lambda: SimpleNamespace(
                active_connections=3,
                ready_connections=2,
                reconnect_count=4,
                ack_mismatch_count=1,
                control_commands_sent=9,
                received_messages=100,
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
    assert records[0]["rss_bytes"] is not None


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
    connection = SimpleNamespace(
        group_id="aggTrade:0001",
        stream=SimpleNamespace(value="aggTrade"),
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
            capture_metrics=lambda: SimpleNamespace(
                queue_events=9,
                queue_bytes=90,
                queue_max_events=10,
                queue_max_bytes=100,
                monitoring_symbols=125,
            ),
            connection_metrics=lambda: SimpleNamespace(
                active_connections=1,
                ready_connections=1,
                reconnect_count=0,
                ack_mismatch_count=0,
                control_commands_sent=1,
                received_messages=100,
                connection_snapshots=(connection,),
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
