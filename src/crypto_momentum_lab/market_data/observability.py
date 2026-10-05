"""Low-overhead runtime signals for the market-data process."""

import asyncio
from collections.abc import Callable, Mapping

import structlog

from crypto_momentum_lab.health.memory import (
    cgroup_memory_snapshot,
    current_rss_bytes,
    tracemalloc_memory_snapshot,
)
from crypto_momentum_lab.health.resources import ProcessResourceSampler
from crypto_momentum_lab.market_data.agg_trade_recovery import AggTradeRecoveryMetrics
from crypto_momentum_lab.market_data.binance.connection_pool import (
    BinanceConnectionPoolMetricsSnapshot,
)
from crypto_momentum_lab.market_data.capture.service import (
    CaptureMetricsSnapshot,
)

log = structlog.get_logger(__name__)


async def monitor_market_data_health(
    *,
    capture_metrics: Callable[[], CaptureMetricsSnapshot],
    connection_metrics: Callable[[], BinanceConnectionPoolMetricsSnapshot],
    runtime_state_metrics: Callable[[], dict[str, object]] | None = None,
    recovery_metrics: Callable[[], AggTradeRecoveryMetrics] | None = None,
    resource_snapshot: Callable[[], dict[str, int | float | None]] | None = None,
    report_interval_seconds: float = 30.0,
    sample_interval_seconds: float = 1.0,
    queue_warning_utilization: float = 0.75,
    queue_critical_utilization: float = 0.90,
    event_loop_lag_warning_seconds: float = 0.05,
    event_loop_lag_critical_seconds: float = 0.50,
) -> None:
    """Log bounded queues, WebSocket churn, event-loop lag, and RSS.

    The sampler intentionally runs in the same event loop as the ingestion
    actor. A delayed sample therefore measures the exact scheduling pressure
    that can starve WebSocket heartbeats.
    """
    if report_interval_seconds <= 0:
        raise ValueError("report_interval_seconds must be positive")
    if sample_interval_seconds <= 0:
        raise ValueError("sample_interval_seconds must be positive")
    if not 0 < queue_warning_utilization < queue_critical_utilization <= 1:
        raise ValueError("queue utilization thresholds are invalid")
    if not (0 < event_loop_lag_warning_seconds < event_loop_lag_critical_seconds):
        raise ValueError("event-loop lag thresholds are invalid")

    loop = asyncio.get_running_loop()
    next_sample_at = loop.time() + sample_interval_seconds
    next_report_at = loop.time() + report_interval_seconds
    previous_report_at = loop.time()
    previous_received_messages = 0
    previous_processing_count = 0
    previous_unrecovered_gap_count = 0
    previous_backpressure_wait_count = 0
    observe_resources = (
        ProcessResourceSampler().snapshot
        if resource_snapshot is None
        else resource_snapshot
    )
    maximum_lag_seconds = 0.0
    lag_samples: list[float] = []
    while True:
        await asyncio.sleep(max(0.0, next_sample_at - loop.time()))
        now = loop.time()
        current_lag = max(0.0, now - next_sample_at)
        lag_samples.append(current_lag)
        maximum_lag_seconds = max(maximum_lag_seconds, current_lag)
        next_sample_at += sample_interval_seconds
        if next_sample_at <= now:
            next_sample_at = now + sample_interval_seconds

        if now < next_report_at:
            continue

        capture = capture_metrics()
        connections = connection_metrics()
        runtime_snapshot = (
            None if runtime_state_metrics is None else runtime_state_metrics()
        )
        recovery_snapshot = None if recovery_metrics is None else recovery_metrics()
        report_elapsed_seconds = max(now - previous_report_at, 0.000001)
        received_message_rate = _counter_rate(
            connections.received_messages,
            previous_received_messages,
            report_elapsed_seconds,
        )
        processing_count = _runtime_processing_count(runtime_snapshot)
        aggregation_event_rate = _counter_rate(
            processing_count,
            previous_processing_count,
            report_elapsed_seconds,
        )
        queue_utilization = _queue_utilization(capture)
        connection_details = tuple(
            {
                "group_id": snapshot.group_id,
                "stream": snapshot.stream.value,
                "desired_subscriptions": snapshot.desired_subscriptions,
                "active": snapshot.active,
                "ready": snapshot.ready,
                "reconnect_count": snapshot.reconnect_count,
                "ack_mismatch_count": snapshot.ack_mismatch_count,
                "received_messages": snapshot.received_messages,
                "received_bytes": snapshot.received_bytes,
                "last_message_age_seconds": (
                    None
                    if snapshot.last_message_age_seconds is None
                    else round(snapshot.last_message_age_seconds, 3)
                ),
                "last_close_code": snapshot.last_close_code,
                "last_reason": snapshot.last_reason,
                "phase": snapshot.phase,
                "pending_control_id": snapshot.pending_control_id,
                "pending_control_method": snapshot.pending_control_method,
                "ingress_queue_events": snapshot.ingress_queue_events,
                "ingress_queue_dropped_events": snapshot.ingress_queue_dropped_events,
                "ingress_queue_max_events": snapshot.ingress_queue_max_events,
                "ingress_queue_high_watermark_events": (
                    snapshot.ingress_queue_high_watermark_events
                ),
                "reader_task_alive": snapshot.reader_task_alive,
                "dispatch_task_alive": snapshot.dispatch_task_alive,
                "realtime_queue_events": snapshot.realtime_queue_events,
                "realtime_queue_dropped_events": snapshot.realtime_queue_dropped_events,
                "realtime_queue_max_events": snapshot.realtime_queue_max_events,
                "realtime_queue_high_watermark_events": (
                    snapshot.realtime_queue_high_watermark_events
                ),
                "realtime_dispatch_task_alive": snapshot.realtime_dispatch_task_alive,
            }
            for snapshot in connections.connection_snapshots
        )
        lag_percentiles = _calculate_lag_percentiles(lag_samples)
        lag_samples.clear()
        log.info(
            "market_data_health_snapshot",
            rss_bytes=current_rss_bytes(),
            **cgroup_memory_snapshot(),
            **tracemalloc_memory_snapshot(),
            **observe_resources(),
            event_loop_lag_ms=round(maximum_lag_seconds * 1000, 3),
            event_loop_lag_p50_ms=lag_percentiles["p50"],
            event_loop_lag_p95_ms=lag_percentiles["p95"],
            event_loop_lag_p99_ms=lag_percentiles["p99"],
            event_loop_lag_max_ms=lag_percentiles["max"],
            queue_events=capture.queue_events,
            queue_bytes=capture.queue_bytes,
            queue_max_events=capture.queue_max_events,
            queue_max_bytes=capture.queue_max_bytes,
            queue_utilization=round(queue_utilization, 6),
            queue_high_watermark_events=capture.queue_high_watermark_events,
            queue_high_watermark_bytes=capture.queue_high_watermark_bytes,
            queue_backpressure_wait_count=capture.queue_backpressure_wait_count,
            queue_backpressure_wait_seconds=round(
                capture.queue_backpressure_wait_seconds,
                6,
            ),
            queue_waiting_producers=capture.queue_waiting_producers,
            queue_coalesced_replacements=(capture.queue_coalesced_replacements),
            queue_dropped_events=capture.queue_dropped_events,
            queue_pending_coalesced_events=capture.queue_pending_coalesced_events,
            filtered_book_ticker_events=capture.filtered_book_ticker_events,
            monitoring_symbols=capture.monitoring_symbols,
            active_connections=connections.active_connections,
            ready_connections=connections.ready_connections,
            reconnect_count=connections.reconnect_count,
            ack_mismatch_count=connections.ack_mismatch_count,
            control_commands_sent=connections.control_commands_sent,
            received_messages=connections.received_messages,
            received_message_rate=round(received_message_rate, 3),
            aggregation_event_rate=round(aggregation_event_rate, 3),
            connection_details=connection_details,
            runtime_state_lateness=runtime_snapshot,
            agg_trade_recovery=_recovery_snapshot(recovery_snapshot),
        )
        lag_level = _event_loop_lag_level(
            maximum_lag_seconds,
            event_loop_lag_warning_seconds,
            event_loop_lag_critical_seconds,
        )
        if lag_level is not None:
            log.warning(
                "market_data_event_loop_lag",
                level=lag_level,
                lag_ms=round(maximum_lag_seconds * 1000, 3),
                p50_ms=lag_percentiles["p50"],
                p95_ms=lag_percentiles["p95"],
                p99_ms=lag_percentiles["p99"],
                max_ms=lag_percentiles["max"],
                warning_threshold_ms=round(
                    event_loop_lag_warning_seconds * 1000,
                    3,
                ),
                critical_threshold_ms=round(
                    event_loop_lag_critical_seconds * 1000,
                    3,
                ),
            )
        if lag_level == "critical":
            # A lag sample only says that the loop did not get scheduled.  Keep a
            # bounded view of the tasks that were still pending once it did run
            # again, so a later incident can be tied to a concrete coroutine
            # instead of guessing from subscription or queue correlations.
            log.warning(
                "market_data_critical_event_loop_lag_diagnostics",
                lag_ms=round(maximum_lag_seconds * 1000, 3),
                task_diagnostics=_pending_task_diagnostics(),
            )
        dead_dispatchers = tuple(
            detail["group_id"]
            for detail in connection_details
            if detail["active"]
            and (
                detail["reader_task_alive"] is False
                or detail["dispatch_task_alive"] is False
            )
        )
        pressured_ingress_groups = tuple(
            detail["group_id"]
            for detail in connection_details
            if _connection_ingress_utilization(detail) >= queue_warning_utilization
        )
        if queue_utilization >= queue_warning_utilization:
            level = (
                "critical"
                if queue_utilization >= queue_critical_utilization
                else "warning"
            )
            log.warning(
                "market_data_queue_pressure",
                level=level,
                utilization=round(queue_utilization, 6),
                queue_events=capture.queue_events,
                queue_bytes=capture.queue_bytes,
            )
        if dead_dispatchers:
            log.error(
                "market_data_connection_task_not_alive",
                group_ids=dead_dispatchers,
            )
        if pressured_ingress_groups:
            log.warning(
                "market_data_websocket_ingress_pressure",
                group_ids=pressured_ingress_groups,
            )
        backpressure_wait_count = int(capture.queue_backpressure_wait_count)
        if backpressure_wait_count > previous_backpressure_wait_count:
            log.warning(
                "market_data_backpressure_observed",
                new_wait_count=(
                    backpressure_wait_count - previous_backpressure_wait_count
                ),
                total_wait_count=backpressure_wait_count,
                total_wait_seconds=round(
                    capture.queue_backpressure_wait_seconds,
                    6,
                ),
            )
        unrecovered_gap_count = int(
            recovery_snapshot.unrecovered_gap_count
            if recovery_snapshot is not None
            else 0
        )
        if unrecovered_gap_count > previous_unrecovered_gap_count:
            log.warning(
                "market_data_unrecovered_agg_trade_gap",
                new_gap_count=(unrecovered_gap_count - previous_unrecovered_gap_count),
                total_gap_count=unrecovered_gap_count,
            )
        previous_report_at = now
        previous_received_messages = connections.received_messages
        previous_processing_count = processing_count
        previous_unrecovered_gap_count = unrecovered_gap_count
        previous_backpressure_wait_count = backpressure_wait_count
        maximum_lag_seconds = 0.0
        next_report_at = now + report_interval_seconds


def _counter_rate(current: int, previous: int, elapsed_seconds: float) -> float:
    return max(0, current - previous) / elapsed_seconds


def _event_loop_lag_level(
    lag_seconds: float,
    warning_threshold_seconds: float,
    critical_threshold_seconds: float,
) -> str | None:
    if lag_seconds < warning_threshold_seconds:
        return None
    return "critical" if lag_seconds >= critical_threshold_seconds else "warning"


def _pending_task_diagnostics(
    *,
    task_limit: int = 16,
    frame_limit: int = 3,
) -> tuple[dict[str, object], ...]:
    """Return a bounded, value-free view of pending tasks in this event loop.

    Stack locals are deliberately excluded: besides being high-cardinality, they
    may contain credentials or market payloads.  This function runs only after a
    critical lag sample, not on the normal health-reporting path.
    """
    current_task = asyncio.current_task()
    tasks = sorted(
        (
            task
            for task in asyncio.all_tasks()
            if task is not current_task and not task.done()
        ),
        key=lambda task: task.get_name(),
    )
    diagnostics: list[dict[str, object]] = []
    for task in tasks[:task_limit]:
        coroutine = task.get_coro()
        frames = task.get_stack(limit=frame_limit)
        diagnostics.append(
            {
                "task_name": task.get_name(),
                "coroutine": getattr(
                    coroutine,
                    "__qualname__",
                    type(coroutine).__name__,
                ),
                "stack": tuple(
                    {
                        "function": frame.f_code.co_name,
                        "file": frame.f_code.co_filename.rsplit("/", 1)[-1],
                        "line": frame.f_lineno,
                    }
                    for frame in frames
                ),
            }
        )
    return tuple(diagnostics)


def _queue_utilization(capture: CaptureMetricsSnapshot) -> float:
    max_events = int(capture.queue_max_events)
    max_bytes = int(capture.queue_max_bytes)
    event_ratio = 0.0 if max_events <= 0 else capture.queue_events / max_events
    byte_ratio = 0.0 if max_bytes <= 0 else capture.queue_bytes / max_bytes
    return max(event_ratio, byte_ratio)


def _runtime_processing_count(snapshot: dict[str, object] | None) -> int:
    if snapshot is None:
        return 0
    aggregation = snapshot.get("aggregation")
    if not isinstance(aggregation, dict):
        return 0
    value = aggregation.get("processing_count", 0)
    return value if isinstance(value, int) else 0


def _connection_ingress_utilization(detail: Mapping[str, object]) -> float:
    events = detail.get("ingress_queue_events")
    maximum = detail.get("ingress_queue_max_events")
    if not isinstance(events, int) or not isinstance(maximum, int) or maximum <= 0:
        return 0.0
    return events / maximum


def _recovery_snapshot(
    snapshot: AggTradeRecoveryMetrics | None,
) -> dict[str, int] | None:
    if snapshot is None:
        return None
    return {
        name: int(getattr(snapshot, name))
        for name in (
            "detected_gap_count",
            "recovered_gap_count",
            "unrecovered_gap_count",
            "recovered_trade_count",
            "missing_trade_count",
            "duplicate_trade_count",
        )
    }


def _calculate_lag_percentiles(samples: list[float]) -> dict[str, float]:
    if not samples:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    sorted_samples = sorted(samples)
    n = len(sorted_samples)

    def _percentile(p: float) -> float:
        idx = int(round((n - 1) * p))
        return sorted_samples[min(max(idx, 0), n - 1)]

    return {
        "p50": round(_percentile(0.50) * 1000, 3),
        "p95": round(_percentile(0.95) * 1000, 3),
        "p99": round(_percentile(0.99) * 1000, 3),
        "max": round(sorted_samples[-1] * 1000, 3),
    }
