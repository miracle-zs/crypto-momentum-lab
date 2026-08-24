"""Low-overhead runtime signals for the market-data process."""

import asyncio
import os
import resource
import sys
from collections.abc import Callable

import structlog

from crypto_momentum_lab.market_data.binance.connection_pool import (
    BinanceConnectionPoolMetricsSnapshot,
)
from crypto_momentum_lab.market_data.capture.service import (
    CaptureMetricsSnapshot,
)

log = structlog.get_logger(__name__)


def current_rss_bytes() -> int | None:
    """Return the current process RSS when the platform exposes it."""
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        with open("/proc/self/statm", encoding="ascii") as statm:
            resident_pages = int(statm.read().split()[1])
        return resident_pages * page_size
    except (FileNotFoundError, IndexError, OSError, ValueError):
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if usage <= 0:
            return None
        return usage if sys.platform == "darwin" else usage * 1024


async def monitor_market_data_health(
    *,
    capture_metrics: Callable[[], CaptureMetricsSnapshot],
    connection_metrics: Callable[
        [], BinanceConnectionPoolMetricsSnapshot
    ],
    runtime_state_metrics: Callable[[], dict[str, object]] | None = None,
    recovery_metrics: Callable[[], object] | None = None,
    report_interval_seconds: float = 30.0,
    sample_interval_seconds: float = 1.0,
    queue_warning_utilization: float = 0.75,
    queue_critical_utilization: float = 0.90,
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

    loop = asyncio.get_running_loop()
    next_sample_at = loop.time() + sample_interval_seconds
    next_report_at = loop.time() + report_interval_seconds
    previous_report_at = loop.time()
    previous_received_messages = 0
    previous_processing_count = 0
    previous_unrecovered_gap_count = 0
    previous_backpressure_wait_count = 0
    maximum_lag_seconds = 0.0
    while True:
        await asyncio.sleep(max(0.0, next_sample_at - loop.time()))
        now = loop.time()
        maximum_lag_seconds = max(
            maximum_lag_seconds,
            max(0.0, now - next_sample_at),
        )
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
        recovery_snapshot = (
            None if recovery_metrics is None else recovery_metrics()
        )
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
                "phase": getattr(snapshot, "phase", None),
                "pending_control_id": getattr(
                    snapshot,
                    "pending_control_id",
                    None,
                ),
                "pending_control_method": getattr(
                    snapshot,
                    "pending_control_method",
                    None,
                ),
                "ingress_queue_events": getattr(
                    snapshot,
                    "ingress_queue_events",
                    None,
                ),
                "ingress_queue_dropped_events": getattr(
                    snapshot,
                    "ingress_queue_dropped_events",
                    None,
                ),
                "ingress_queue_max_events": getattr(
                    snapshot,
                    "ingress_queue_max_events",
                    None,
                ),
                "ingress_queue_high_watermark_events": getattr(
                    snapshot,
                    "ingress_queue_high_watermark_events",
                    None,
                ),
                "reader_task_alive": getattr(
                    snapshot,
                    "reader_task_alive",
                    None,
                ),
                "dispatch_task_alive": getattr(
                    snapshot,
                    "dispatch_task_alive",
                    None,
                ),
            }
            for snapshot in getattr(connections, "connection_snapshots", ())
        )
        log.info(
            "market_data_health_snapshot",
            rss_bytes=current_rss_bytes(),
            event_loop_lag_ms=round(maximum_lag_seconds * 1000, 3),
            queue_events=capture.queue_events,
            queue_bytes=capture.queue_bytes,
            queue_max_events=getattr(capture, "queue_max_events", 0),
            queue_max_bytes=getattr(capture, "queue_max_bytes", 0),
            queue_utilization=round(queue_utilization, 6),
            queue_high_watermark_events=getattr(
                capture,
                "queue_high_watermark_events",
                0,
            ),
            queue_high_watermark_bytes=getattr(
                capture,
                "queue_high_watermark_bytes",
                0,
            ),
            queue_backpressure_wait_count=getattr(
                capture,
                "queue_backpressure_wait_count",
                0,
            ),
            queue_backpressure_wait_seconds=round(
                getattr(capture, "queue_backpressure_wait_seconds", 0.0),
                6,
            ),
            queue_waiting_producers=getattr(
                capture,
                "queue_waiting_producers",
                0,
            ),
            queue_coalesced_replacements=(
                getattr(capture, "queue_coalesced_replacements", 0)
            ),
            queue_dropped_events=getattr(capture, "queue_dropped_events", 0),
            queue_pending_coalesced_events=getattr(
                capture,
                "queue_pending_coalesced_events",
                0,
            ),
            filtered_book_ticker_events=getattr(
                capture,
                "filtered_book_ticker_events",
                0,
            ),
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
            if _connection_ingress_utilization(detail)
            >= queue_warning_utilization
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
        backpressure_wait_count = int(
            getattr(capture, "queue_backpressure_wait_count", 0)
        )
        if backpressure_wait_count > previous_backpressure_wait_count:
            log.warning(
                "market_data_backpressure_observed",
                new_wait_count=(
                    backpressure_wait_count
                    - previous_backpressure_wait_count
                ),
                total_wait_count=backpressure_wait_count,
                total_wait_seconds=round(
                    getattr(
                        capture,
                        "queue_backpressure_wait_seconds",
                        0.0,
                    ),
                    6,
                ),
            )
        unrecovered_gap_count = int(
            getattr(recovery_snapshot, "unrecovered_gap_count", 0)
        )
        if unrecovered_gap_count > previous_unrecovered_gap_count:
            log.warning(
                "market_data_unrecovered_agg_trade_gap",
                new_gap_count=(
                    unrecovered_gap_count - previous_unrecovered_gap_count
                ),
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


def _queue_utilization(capture: CaptureMetricsSnapshot) -> float:
    max_events = int(getattr(capture, "queue_max_events", 0))
    max_bytes = int(getattr(capture, "queue_max_bytes", 0))
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


def _connection_ingress_utilization(detail: dict[str, object]) -> float:
    events = detail.get("ingress_queue_events")
    maximum = detail.get("ingress_queue_max_events")
    if not isinstance(events, int) or not isinstance(maximum, int) or maximum <= 0:
        return 0.0
    return events / maximum


def _recovery_snapshot(snapshot: object | None) -> dict[str, int] | None:
    if snapshot is None:
        return None
    return {
        name: int(getattr(snapshot, name, 0))
        for name in (
            "detected_gap_count",
            "recovered_gap_count",
            "unrecovered_gap_count",
            "recovered_trade_count",
            "missing_trade_count",
            "duplicate_trade_count",
        )
    }
