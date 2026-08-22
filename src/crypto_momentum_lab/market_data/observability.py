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
    report_interval_seconds: float = 30.0,
    sample_interval_seconds: float = 1.0,
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

    loop = asyncio.get_running_loop()
    next_sample_at = loop.time() + sample_interval_seconds
    next_report_at = loop.time() + report_interval_seconds
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
            }
            for snapshot in getattr(connections, "connection_snapshots", ())
        )
        log.info(
            "market_data_health_snapshot",
            rss_bytes=current_rss_bytes(),
            event_loop_lag_ms=round(maximum_lag_seconds * 1000, 3),
            queue_events=capture.queue_events,
            queue_bytes=capture.queue_bytes,
            queue_coalesced_replacements=(
                getattr(capture, "queue_coalesced_replacements", 0)
            ),
            queue_dropped_events=getattr(capture, "queue_dropped_events", 0),
            queue_pending_coalesced_events=getattr(
                capture,
                "queue_pending_coalesced_events",
                0,
            ),
            monitoring_symbols=capture.monitoring_symbols,
            active_connections=connections.active_connections,
            ready_connections=connections.ready_connections,
            reconnect_count=connections.reconnect_count,
            ack_mismatch_count=connections.ack_mismatch_count,
            control_commands_sent=connections.control_commands_sent,
            received_messages=connections.received_messages,
            connection_details=connection_details,
        )
        maximum_lag_seconds = 0.0
        next_report_at = now + report_interval_seconds
