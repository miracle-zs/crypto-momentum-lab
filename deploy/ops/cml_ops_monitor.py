#!/usr/bin/env python3
"""Low-dependency operational checks for the single-host deployment.

The monitor deliberately runs outside the trading processes.  It reads Docker
state, recent structured logs, and a few PostgreSQL counters, then emits a
single JSON alert stream to journald/stdout.  An HTTPS webhook is optional and
is never required for the trading stack to start.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

_DEFAULT_SERVICES = (
    "postgres",
    "market-data",
    "execution-account-live",
    "live-strategy",
)
_DEFAULT_INTERVAL_SECONDS = 60.0
_DEFAULT_LOG_WINDOW_SECONDS = 120.0
_DEFAULT_TELEMETRY_STALE_AFTER_SECONDS = 900.0
_DEFAULT_RSS_WARNING_FRACTION = 0.75
_DEFAULT_RSS_CRITICAL_FRACTION = 0.90
_DEFAULT_RSS_GROWTH_BYTES = 64 * 1024 * 1024
_DEFAULT_RSS_GROWTH_WINDOW_SECONDS = 1_800.0
_DEFAULT_MEMORY_GROWTH_REQUIRED_SAMPLES = 3
_DEFAULT_ALERT_COOLDOWN_SECONDS = 900.0
_DEFAULT_COMMAND_TIMEOUT_SECONDS = 15.0
_DEFAULT_LIVE_RESTART_COOLDOWN_SECONDS = 900.0
_DEFAULT_LIVE_RESTART_MAX_ATTEMPTS = 3
_DEFAULT_MARKET_STATE_STALE_AFTER_SECONDS = 120.0
_DEFAULT_MARKET_DELAY_WARNING_MS = 30_000.0
_DEFAULT_MARKET_DELAY_CRITICAL_MS = 120_000.0
_DEFAULT_ACCOUNT_STATE_STALE_AFTER_SECONDS = 120.0
_DEFAULT_POSITION_STALE_AFTER_SECONDS = 120.0
_DEFAULT_POSITION_QUANTITY_TOLERANCE = Decimal("0.00000001")
_DEFAULT_CONSISTENCY_WINDOW_SECONDS = 300.0
_BEIJING_TIMEZONE = timezone(timedelta(hours=8), "Asia/Shanghai")
_SEVERITY_LABELS = {
    "critical": "严重",
    "warning": "警告",
    "info": "提示",
}
_ALERT_LABELS = {
    "container_missing": "服务容器缺失",
    "container_unhealthy": "服务健康检查失败",
    "container_oom_killed": "服务被内存限制杀死",
    "container_memory_high": "服务内存占用过高",
    "container_memory_growth": "服务内存趋势异常",
    "container_memory_pressure": "服务触碰内存上限",
    # Keep the legacy label so an alert written by an older monitor can still
    # be rendered correctly while its recovery record is being drained.
    "rss_growth": "服务内存持续增长",
    "telemetry_persist_failure": "运行时遥测写入失败",
    "live_legacy_order_identity_conflict": "订单身份发生冲突",
    "market_task_not_alive": "行情连接任务停止",
    "live_session_not_ready": "实时会话未就绪",
    "live_checkpoint_stale": "实时状态 checkpoint 已过期",
    "live_account_lifecycle_not_ready": "账户生命周期未就绪",
    "live_account_reconciliation_stale": "账户对账状态过期或失败",
    "live_market_state_stale": "行情进度过期",
    "live_market_state_delay": "行情延迟过高",
    "live_signal_divergence": "账户信号发生分叉",
    "live_position_divergence": "账户持仓发生差异",
    "live_unknown_orders": "存在未确认在途订单",
    "live_consistency_check_failed": "跨账户一致性检查失败",
    "database_check_failed": "数据库健康检查失败",
    "database_query_stats_unavailable": "数据库查询统计不可用",
    "database_io_timing_disabled": "数据库 I/O 耗时监控未开启",
    "database_parallel_maintenance_enabled": "数据库并行维护超过护栏",
    "live_heartbeat_stale": "实时策略心跳过期",
    "live_heartbeat_auto_restarted": "实时策略已触发自动重启",
    "live_heartbeat_restart_failed": "实时策略自动重启失败",
    "live_heartbeat_restart_suppressed": "实时策略自动重启已达上限",
    "live_crash_log_archive_failed": "worker 崩溃日志归档失败",
    "ops_monitor_failed": "运维监控自身异常",
}
_ALERT_IMPACTS = {
    "container_missing": "对应服务未运行，相关功能不可用。",
    "container_unhealthy": "对应服务可能无法正常处理行情、订单或账户任务。",
    "container_oom_killed": "对应服务已被系统终止，相关任务已中断。",
    "container_memory_high": "服务可能出现性能下降，继续增长可能触发 OOM。",
    "container_memory_growth": (
        "服务内存相对趋势基线持续上升，需要确认缓存、查询和进程数量。"
    ),
    "container_memory_pressure": (
        "容器已触碰 cgroup 内存上限，可能发生回收或换页。"
    ),
    "rss_growth": "服务内存持续增长，后续可能出现性能下降或 OOM。",
    "telemetry_persist_failure": "运行时诊断数据可能不完整，不代表交易一定已停止。",
    "live_legacy_order_identity_conflict": "订单与交易所订单的归属可能无法安全关联。",
    "market_task_not_alive": "策略可能无法持续接收行情，开平仓判断可能受影响。",
    "live_session_not_ready": "该实时账户未处于可安全运行状态。",
    "live_checkpoint_stale": "策略状态可能没有及时持久化，重启恢复风险增加。",
    "live_account_lifecycle_not_ready": "该账户没有处于可安全交易的生命周期状态。",
    "live_account_reconciliation_stale": (
        "该账户的交易所快照或对账结果不新鲜，持仓和订单归属无法确认。"
    ),
    "live_market_state_stale": "该账户没有持续推进完整行情桶，策略已进入风险状态。",
    "live_market_state_delay": (
        "该账户收到的行情相对桶结束时间明显滞后，信号时点可能失真。"
    ),
    "live_signal_divergence": "相同策略配置的账户对同一行情桶产生了不同输出。",
    "live_position_divergence": "可比账户的交易所持仓快照不一致，存在分叉风险。",
    "live_unknown_orders": "交易所订单状态未能与本地订单安全对齐。",
    "live_consistency_check_failed": "无法确认账户之间的信号和持仓是否一致。",
    "database_check_failed": "暂时无法确认实时会话、租约和 checkpoint 是否健康。",
    "database_query_stats_unavailable": (
        "不影响交易本身，但会降低数据库问题的定位能力。"
    ),
    "database_io_timing_disabled": "无法准确判断数据库 I/O 延迟对交易服务的影响。",
    "database_parallel_maintenance_enabled": "维护任务可能与交易查询争用数据库资源。",
    "live_heartbeat_stale": "该账户的行情处理和开平仓任务可能已经停止。",
    "live_heartbeat_auto_restarted": "该账户可能经历了短暂中断，正在等待健康检查恢复。",
    "live_heartbeat_restart_failed": "该账户仍可能无法处理行情和订单，需要人工介入。",
    "live_heartbeat_restart_suppressed": (
        "该账户仍处于异常状态，监控已停止继续自动重启。"
    ),
    "live_crash_log_archive_failed": (
        "worker 重启前的日志没有可靠留存，故障根因可能无法复盘。"
    ),
    "ops_monitor_failed": "监控自身可能无法继续发现新的异常。",
}
_ALERT_ACTIONS = {
    "container_missing": "未自动修复，请检查 Compose 服务和容器状态。",
    "container_unhealthy": (
        "已记录健康检查失败；若同时存在心跳告警，将由心跳恢复流程定向重启。"
    ),
    "container_oom_killed": (
        "未在此告警中自动处理，请检查内存占用、容器限制和最近日志。"
    ),
    "container_memory_high": "当前未自动重启，请继续观察内存趋势并检查泄漏或缓存增长。",
    "container_memory_growth": (
        "当前未自动重启，已改为等待连续趋势证据并保留内存压力详情。"
    ),
    "container_memory_pressure": (
        "当前未自动重启，请检查 cgroup 事件、swap、临时查询和连接池。"
    ),
    "rss_growth": "当前未自动重启，请检查内存趋势和进程堆积情况。",
    "telemetry_persist_failure": (
        "已保留告警并继续运行，建议检查 PostgreSQL 延迟和连接池。"
    ),
    "live_legacy_order_identity_conflict": (
        "请暂停相关排障范围内的自动处理并核对订单归属。"
    ),
    "market_task_not_alive": "请检查行情连接、网络和策略进程；本告警不代表已自动恢复。",
    "live_session_not_ready": (
        "请检查实时会话、租约和 checkpoint；未确认安全前不要扩大交易范围。"
    ),
    "live_checkpoint_stale": "请检查 PostgreSQL、策略进程和 checkpoint 写入延迟。",
    "live_account_lifecycle_not_ready": (
        "请检查 execution account 的状态转换、账户同步和 worker 日志。"
    ),
    "live_account_reconciliation_stale": (
        "请检查交易所 REST 同步、成交回调和 account_reconciliation_runs。"
    ),
    "live_market_state_stale": "请检查行情断流、缺桶、durable rewarm 和策略进程日志。",
    "live_market_state_delay": "请检查行情连接、事件循环阻塞、数据库负载和网络延迟。",
    "live_signal_divergence": (
        "先暂停扩大仓位，核对两账户的 config hash、checkpoint、行情桶和信号明细。"
    ),
    "live_position_divergence": (
        "先以交易所快照为准核对仓位，确认归属后再做补单或退出。"
    ),
    "live_unknown_orders": (
        "禁止重发同一意图；先按 client_order_id 查询交易所并完成人工或自动对账。"
    ),
    "live_consistency_check_failed": (
        "请检查监控查询权限、PostgreSQL 连接和一致性查询耗时。"
    ),
    "database_check_failed": "请检查 PostgreSQL 容器、连接和监控查询权限。",
    "database_query_stats_unavailable": "请在低风险窗口启用 pg_stat_statements。",
    "database_io_timing_disabled": "请核对 PostgreSQL 的 I/O timing 配置。",
    "database_parallel_maintenance_enabled": (
        "请核对维护参数，避免与实时交易查询争用资源。"
    ),
    "live_crash_log_archive_failed": (
        "请检查 crash log 目录权限、磁盘空间和 Docker 日志读取权限。"
    ),
    "live_heartbeat_stale": "已发现心跳过期；若自动恢复未启用，需要人工检查策略进程。",
    "ops_monitor_failed": "请检查 cml-ops-monitor.service 和 journald 日志。",
}
_COMPOSE_SERVICE_HEADER = re.compile(
    r"^  (?P<service>[A-Za-z0-9][A-Za-z0-9_-]*):\s*$"
)


def _live_strategy_service(account_label: str) -> str:
    """Map a configured account label to its Compose strategy service."""

    return (
        "live-strategy"
        if account_label == "primary"
        else f"live-strategy-{account_label}"
    )


@dataclass(frozen=True, slots=True)
class Alert:
    """One condition that needs operator attention."""

    name: str
    severity: str
    summary: str
    details: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ContainerSnapshot:
    service: str
    container_id: str
    health: str | None
    oom_killed: bool
    restart_count: int
    memory_bytes: int | None
    memory_limit_bytes: int | None
    memory_source: str = "docker_stats_working_set"
    memory_working_set_bytes: int | None = None
    memory_current_bytes: int | None = None
    memory_peak_bytes: int | None = None
    memory_swap_current_bytes: int | None = None
    memory_events_max: int | None = None


@dataclass(frozen=True, slots=True)
class ContainerMemoryStats:
    """Container memory values from Docker and the cgroup v2 controller."""

    observed_bytes: int | None
    memory_limit_bytes: int | None
    source: str
    working_set_bytes: int | None = None
    current_bytes: int | None = None
    peak_bytes: int | None = None
    swap_current_bytes: int | None = None
    events_max: int | None = None


@dataclass(frozen=True, slots=True)
class LogSignals:
    telemetry_persist_failures: int = 0
    legacy_order_identity_conflicts: int = 0
    dead_connection_tasks: tuple[str, ...] = ()
    latest_rss_bytes: int | None = None
    rss_observed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class DatabaseState:
    latest_checkpoint_age_seconds: float | None
    live_session_ready: bool
    pg_stat_statements_ready: bool
    track_io_timing: bool
    track_wal_io_timing: bool
    max_parallel_maintenance_workers: int | None
    latest_market_progress_age_seconds: float | None = None
    latest_market_delay_ms: float | None = None
    account_process_state: str | None = None
    account_process_age_seconds: float | None = None
    latest_reconciliation_status: str | None = None
    latest_reconciliation_age_seconds: float | None = None
    unknown_order_count: int = 0
    oldest_unknown_order_age_seconds: float | None = None


@dataclass(frozen=True, slots=True)
class SignalObservation:
    """One account's durable output for one market bucket."""

    account_label: str
    symbol: str
    bucket_start: str
    strategy_config_hash: str
    signal_count: int
    candidate_count: int
    fingerprint: str | None = None


@dataclass(frozen=True, slots=True)
class PositionObservation:
    """One row from the latest authoritative account reconciliation snapshot."""

    account_label: str
    status: str
    age_seconds: float | None
    symbol: str = ""
    position_side: str = ""
    position_amt: Decimal = Decimal("0")


def evaluate_signal_divergence(
    observations: Sequence[SignalObservation],
) -> tuple[Alert, ...]:
    """Detect different outputs for the same symbol/bucket/config group.

    A configuration hash is part of the comparison key.  Accounts with
    intentionally different strategy parameters therefore do not create a
    false positive; accounts claiming the same config must agree on both the
    output count and the durable content fingerprint.
    """

    groups: dict[tuple[str, str, str], dict[str, SignalObservation]] = {}
    for observation in observations:
        if not observation.account_label.strip() or not observation.symbol.strip():
            continue
        key = (
            observation.symbol,
            observation.bucket_start,
            observation.strategy_config_hash,
        )
        groups.setdefault(key, {})[observation.account_label] = observation

    differences: list[dict[str, object]] = []
    for (symbol, bucket_start, config_hash), account_values in sorted(groups.items()):
        if len(account_values) < 2:
            continue
        outputs = tuple(account_values.values())
        fingerprints = {value.fingerprint for value in outputs}
        counts = {
            (value.signal_count, value.candidate_count) for value in outputs
        }
        if len(fingerprints) <= 1 and len(counts) <= 1:
            continue
        differences.append(
            {
                "symbol": symbol,
                "bucket_start": bucket_start,
                "strategy_config_hash": config_hash,
                "accounts": [
                    {
                        "account_label": value.account_label,
                        "signal_count": value.signal_count,
                        "candidate_count": value.candidate_count,
                        "fingerprint": value.fingerprint,
                    }
                    for value in sorted(outputs, key=lambda item: item.account_label)
                ],
            }
        )

    if not differences:
        return ()
    return (
        Alert(
            "live_signal_divergence",
            "critical",
            "Comparable live accounts produced divergent strategy output",
            {
                "group_count": len(differences),
                "differences": differences[:20],
            },
        ),
    )


def evaluate_position_divergence(
    observations: Sequence[PositionObservation],
    *,
    stale_after_seconds: float,
    quantity_tolerance: Decimal = _DEFAULT_POSITION_QUANTITY_TOLERANCE,
) -> tuple[Alert, ...]:
    """Compare the latest ready account snapshots without trusting stale rows."""

    if stale_after_seconds <= 0:
        raise ValueError("stale_after_seconds must be positive")
    if quantity_tolerance < 0:
        raise ValueError("quantity_tolerance must not be negative")

    positions_by_account: dict[str, dict[tuple[str, str], Decimal]] = {}
    freshness_by_account: dict[str, tuple[str, float | None]] = {}
    for observation in observations:
        if not observation.account_label.strip():
            continue
        freshness_by_account[observation.account_label] = (
            observation.status,
            observation.age_seconds,
        )
        if observation.status != "ready":
            continue
        if (
            observation.age_seconds is None
            or observation.age_seconds < 0
            or observation.age_seconds > stale_after_seconds
        ):
            continue
        account_positions = positions_by_account.setdefault(
            observation.account_label,
            {},
        )
        if not observation.symbol.strip() or not observation.position_side.strip():
            continue
        if observation.position_amt == 0:
            continue
        key = (observation.symbol, observation.position_side)
        account_positions[key] = (
            account_positions.get(key, Decimal("0")) + observation.position_amt
        )

    account_labels = tuple(sorted(positions_by_account))
    differences: list[dict[str, object]] = []
    for index, left_label in enumerate(account_labels):
        for right_label in account_labels[index + 1 :]:
            left = positions_by_account[left_label]
            right = positions_by_account[right_label]
            keys = sorted(set(left) | set(right))
            quantity_differences = []
            for symbol, position_side in keys:
                left_quantity = left.get((symbol, position_side), Decimal("0"))
                right_quantity = right.get((symbol, position_side), Decimal("0"))
                if abs(left_quantity - right_quantity) <= quantity_tolerance:
                    continue
                quantity_differences.append(
                    {
                        "symbol": symbol,
                        "position_side": position_side,
                        "left_quantity": str(left_quantity),
                        "right_quantity": str(right_quantity),
                    }
                )
            if quantity_differences:
                differences.append(
                    {
                        "accounts": [left_label, right_label],
                        "quantity_differences": quantity_differences[:50],
                    }
                )

    if not differences:
        return ()
    return (
        Alert(
            "live_position_divergence",
            "critical",
            "Comparable live account position snapshots diverged",
            {
                "pair_count": len(differences),
                "differences": differences[:20],
                "freshness": {
                    account: {
                        "status": status,
                        "age_seconds": age,
                    }
                    for account, (status, age) in sorted(freshness_by_account.items())
                },
            },
        ),
    )


def evaluate_database_state(
    *,
    now: datetime,
    latest_checkpoint_age_seconds: float | None,
    live_session_ready: bool,
    pg_stat_statements_ready: bool,
    track_io_timing: bool,
    track_wal_io_timing: bool,
    max_parallel_maintenance_workers: int | None,
    stale_after_seconds: float,
    market_state_stale_after_seconds: float = (
        _DEFAULT_MARKET_STATE_STALE_AFTER_SECONDS
    ),
    market_delay_warning_ms: float = _DEFAULT_MARKET_DELAY_WARNING_MS,
    market_delay_critical_ms: float = _DEFAULT_MARKET_DELAY_CRITICAL_MS,
    account_state_stale_after_seconds: float = (
        _DEFAULT_ACCOUNT_STATE_STALE_AFTER_SECONDS
    ),
    account_process_state: str | None = None,
    account_process_age_seconds: float | None = None,
    latest_reconciliation_status: str | None = None,
    latest_reconciliation_age_seconds: float | None = None,
    latest_market_progress_age_seconds: float | None = None,
    latest_market_delay_ms: float | None = None,
    unknown_order_count: int = 0,
    oldest_unknown_order_age_seconds: float | None = None,
) -> tuple[Alert, ...]:
    """Return alerts for live liveness and PostgreSQL observability."""

    del now
    alerts: list[Alert] = []
    if account_process_state != "ready_readonly" or (
        account_process_age_seconds is None
        or account_process_age_seconds < 0
        or account_process_age_seconds > account_state_stale_after_seconds
    ):
        alerts.append(
            Alert(
                "live_account_lifecycle_not_ready",
                "critical",
                "Execution account lifecycle is not ready",
                {
                    "state": account_process_state,
                    "age_seconds": account_process_age_seconds,
                    "threshold_seconds": account_state_stale_after_seconds,
                },
            )
        )
    if latest_reconciliation_status != "ready" or (
        latest_reconciliation_age_seconds is None
        or latest_reconciliation_age_seconds < 0
        or latest_reconciliation_age_seconds > account_state_stale_after_seconds
    ):
        alerts.append(
            Alert(
                "live_account_reconciliation_stale",
                "critical",
                "Latest account reconciliation is missing, stale, or failed",
                {
                    "status": latest_reconciliation_status,
                    "age_seconds": latest_reconciliation_age_seconds,
                    "threshold_seconds": account_state_stale_after_seconds,
                },
            )
        )
    if (
        latest_market_progress_age_seconds is None
        or latest_market_progress_age_seconds < 0
        or latest_market_progress_age_seconds > market_state_stale_after_seconds
    ):
        alerts.append(
            Alert(
                "live_market_state_stale",
                "critical",
                "Durable market-state progress is missing or stale",
                {
                    "age_seconds": latest_market_progress_age_seconds,
                    "threshold_seconds": market_state_stale_after_seconds,
                },
            )
        )
    if latest_market_delay_ms is not None and latest_market_delay_ms >= 0:
        if latest_market_delay_ms >= market_delay_critical_ms:
            alerts.append(
                Alert(
                    "live_market_state_delay",
                    "critical",
                    "Market-state receive delay exceeded the critical budget",
                    {
                        "delay_ms": round(latest_market_delay_ms, 3),
                        "warning_threshold_ms": market_delay_warning_ms,
                        "critical_threshold_ms": market_delay_critical_ms,
                    },
                )
            )
        elif latest_market_delay_ms >= market_delay_warning_ms:
            alerts.append(
                Alert(
                    "live_market_state_delay",
                    "warning",
                    "Market-state receive delay exceeded the warning budget",
                    {
                        "delay_ms": round(latest_market_delay_ms, 3),
                        "warning_threshold_ms": market_delay_warning_ms,
                        "critical_threshold_ms": market_delay_critical_ms,
                    },
                )
            )
    if unknown_order_count > 0:
        alerts.append(
            Alert(
                "live_unknown_orders",
                "critical",
                "Exchange order state is pending reconciliation",
                {
                    "unknown_order_count": unknown_order_count,
                    "oldest_age_seconds": oldest_unknown_order_age_seconds,
                },
            )
        )
    if not live_session_ready:
        alerts.append(
            Alert(
                "live_session_not_ready",
                "critical",
                "Live session checkpoint or lease is not ready",
            )
        )
    elif (
        latest_checkpoint_age_seconds is None
        or latest_checkpoint_age_seconds < 0
        or latest_checkpoint_age_seconds > stale_after_seconds
    ):
        alerts.append(
            Alert(
                "live_checkpoint_stale",
                "critical",
                "Live strategy checkpoint is older than the freshness budget",
                {
                    "age_seconds": (
                        None
                        if latest_checkpoint_age_seconds is None
                        else round(latest_checkpoint_age_seconds, 3)
                    ),
                    "threshold_seconds": stale_after_seconds,
                },
            )
        )
    if not pg_stat_statements_ready:
        alerts.append(
            Alert(
                "database_query_stats_unavailable",
                "warning",
                "pg_stat_statements is not loaded",
            )
        )
    if not track_io_timing or not track_wal_io_timing:
        alerts.append(
            Alert(
                "database_io_timing_disabled",
                "warning",
                "PostgreSQL I/O timing is disabled",
                {
                    "track_io_timing": track_io_timing,
                    "track_wal_io_timing": track_wal_io_timing,
                },
            )
        )
    if (
        max_parallel_maintenance_workers is not None
        and max_parallel_maintenance_workers > 0
    ):
        alerts.append(
            Alert(
                "database_parallel_maintenance_enabled",
                "warning",
                "Parallel maintenance is enabled above the OOM guardrail",
                {
                    "max_parallel_maintenance_workers": (
                        max_parallel_maintenance_workers
                    )
                },
            )
        )
    return tuple(alerts)


def evaluate_log_signals(signals: LogSignals) -> tuple[Alert, ...]:
    """Return alerts represented by recent structured application logs."""

    alerts: list[Alert] = []
    if signals.telemetry_persist_failures:
        severity = "critical" if signals.telemetry_persist_failures >= 3 else "warning"
        alerts.append(
            Alert(
                "telemetry_persist_failure",
                severity,
                "Runtime telemetry batches failed to persist",
                {"failure_count": signals.telemetry_persist_failures},
            )
        )
    if signals.legacy_order_identity_conflicts:
        alerts.append(
            Alert(
                "live_legacy_order_identity_conflict",
                "critical",
                "Live order identity was reused across multiple exchange orders",
                {
                    "conflict_count": (
                        signals.legacy_order_identity_conflicts
                    )
                },
            )
        )
    if signals.dead_connection_tasks:
        alerts.append(
            Alert(
                "market_task_not_alive",
                "critical",
                "A market-data connection task reported not alive",
                {"group_ids": signals.dead_connection_tasks},
            )
        )
    return tuple(alerts)


def evaluate_container(
    snapshot: ContainerSnapshot,
    *,
    rss_warning_fraction: float,
    rss_critical_fraction: float,
) -> tuple[Alert, ...]:
    """Return alerts for Docker lifecycle and memory state."""

    alerts: list[Alert] = []
    memory_details = {
        "service": snapshot.service,
        "memory_bytes": snapshot.memory_bytes,
        "memory_limit_bytes": snapshot.memory_limit_bytes,
        "memory_source": snapshot.memory_source,
        "memory_working_set_bytes": snapshot.memory_working_set_bytes,
        "memory_current_bytes": snapshot.memory_current_bytes,
        "memory_peak_bytes": snapshot.memory_peak_bytes,
        "memory_swap_current_bytes": snapshot.memory_swap_current_bytes,
        "memory_events_max": snapshot.memory_events_max,
    }
    if snapshot.oom_killed:
        alerts.append(
            Alert(
                "container_oom_killed",
                "critical",
                f"Container {snapshot.service} was killed by the OOM controller",
                {
                    "service": snapshot.service,
                    "restart_count": snapshot.restart_count,
                },
            )
        )
    if snapshot.health in {"unhealthy", "dead"}:
        alerts.append(
            Alert(
                "container_unhealthy",
                "critical",
                f"Container {snapshot.service} is {snapshot.health}",
                {"service": snapshot.service, "health": snapshot.health},
            )
        )
    if (
        snapshot.memory_bytes is not None
        and snapshot.memory_limit_bytes is not None
        and snapshot.memory_limit_bytes > 0
    ):
        fraction = snapshot.memory_bytes / snapshot.memory_limit_bytes
        if fraction >= rss_critical_fraction:
            alerts.append(
                Alert(
                    "container_memory_high",
                    "critical",
                    f"Container {snapshot.service} memory is near its cgroup limit",
                    {
                        **memory_details,
                        "fraction": round(fraction, 4),
                    },
                )
            )
        elif fraction >= rss_warning_fraction:
            alerts.append(
                Alert(
                    "container_memory_high",
                    "warning",
                    (
                        f"Container {snapshot.service} memory is above the "
                        "warning threshold"
                    ),
                    {
                        **memory_details,
                        "fraction": round(fraction, 4),
                    },
                )
            )
    return tuple(alerts)


def evaluate_container_memory_growth(
    *,
    service: str,
    current_bytes: int | None,
    baseline_bytes: int | None,
    baseline_age_seconds: float | None,
    consecutive_samples: int,
    required_samples: int,
    growth_bytes: int,
    growth_window_seconds: float,
    metric_source: str,
) -> tuple[Alert, ...]:
    """Alert after a sustained container-memory increase over a trend baseline."""

    if (
        current_bytes is None
        or baseline_bytes is None
        or baseline_age_seconds is None
        or baseline_age_seconds < 0
        or consecutive_samples < required_samples
        or required_samples <= 0
        or growth_bytes <= 0
        or current_bytes - baseline_bytes < growth_bytes
    ):
        return ()
    return (
        Alert(
            "container_memory_growth",
            "warning",
            (
                f"Container {service} memory grew beyond the configured "
                "trend window"
            ),
            {
                "service": service,
                "baseline_bytes": baseline_bytes,
                "current_bytes": current_bytes,
                "growth_bytes": current_bytes - baseline_bytes,
                "threshold_bytes": growth_bytes,
                "growth_window_seconds": growth_window_seconds,
                "baseline_age_seconds": round(baseline_age_seconds, 3),
                "consecutive_samples": consecutive_samples,
                "required_samples": required_samples,
                "metric_source": metric_source,
            },
        ),
    )


def evaluate_rss_growth(
    *,
    service: str,
    current_bytes: int | None,
    previous_bytes: int | None,
    growth_bytes: int,
) -> tuple[Alert, ...]:
    """Backward-compatible one-sample helper for older callers.

    The production monitor uses :func:`evaluate_container_memory_growth`,
    which has a time-window baseline and consecutive-sample guard.  Keeping
    this helper avoids breaking small external diagnostics that imported the
    old function name.
    """

    if (
        current_bytes is None
        or previous_bytes is None
        or growth_bytes <= 0
        or current_bytes - previous_bytes < growth_bytes
    ):
        return ()
    return (
        Alert(
            "rss_growth",
            "warning",
            f"Container {service} memory grew beyond the previous sample",
            {
                "service": service,
                "previous_bytes": previous_bytes,
                "current_bytes": current_bytes,
                "growth_bytes": current_bytes - previous_bytes,
                "threshold_bytes": growth_bytes,
                "metric_source": "legacy_compatibility_helper",
            },
        ),
    )


class CommandRunner(Protocol):
    def run(self, args: Sequence[str], *, timeout_seconds: float) -> str: ...


class SubprocessRunner:
    def run(self, args: Sequence[str], *, timeout_seconds: float) -> str:
        result = subprocess.run(
            list(args),
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            raise RuntimeError(
                f"command failed ({result.returncode}): {' '.join(args)}"
                + (f": {stderr}" if stderr else "")
            )
        return result.stdout


@dataclass(frozen=True, slots=True)
class MonitorConfig:
    project_directory: Path = Path("/opt/crypto-momentum-lab")
    compose_file: Path = Path("/opt/crypto-momentum-lab/compose.server.yaml")
    compose_files: tuple[Path, ...] = ()
    compose_profiles: tuple[str, ...] = ()
    compose_env_file: Path | None = Path("/opt/crypto-momentum-lab/.env.server")
    services: tuple[str, ...] = _DEFAULT_SERVICES
    live_accounts: tuple[tuple[str, str, str], ...] = (
        ("primary", "live-primary-v1", "live-worker"),
    )
    live_run_id: str = "live-primary-v1"
    live_account_label: str = "primary"
    live_lease_owner: str = "live-worker"
    interval_seconds: float = _DEFAULT_INTERVAL_SECONDS
    log_window_seconds: float = _DEFAULT_LOG_WINDOW_SECONDS
    telemetry_stale_after_seconds: float = _DEFAULT_TELEMETRY_STALE_AFTER_SECONDS
    rss_warning_fraction: float = _DEFAULT_RSS_WARNING_FRACTION
    rss_critical_fraction: float = _DEFAULT_RSS_CRITICAL_FRACTION
    rss_growth_bytes: int = _DEFAULT_RSS_GROWTH_BYTES
    rss_growth_window_seconds: float = _DEFAULT_RSS_GROWTH_WINDOW_SECONDS
    memory_growth_required_samples: int = _DEFAULT_MEMORY_GROWTH_REQUIRED_SAMPLES
    alert_cooldown_seconds: float = _DEFAULT_ALERT_COOLDOWN_SECONDS
    command_timeout_seconds: float = _DEFAULT_COMMAND_TIMEOUT_SECONDS
    state_path: Path = Path("/var/lib/crypto-momentum-lab/ops-monitor.json")
    # ``None`` means a persistent sibling of state_path.  This keeps the
    # default durable on both the production host and local test hosts.
    crash_log_directory: Path | None = None
    webhook_url: str | None = None
    serverchan_sendkey: str | None = None
    external_heartbeat_url: str | None = None
    external_heartbeat_token: str | None = None
    external_heartbeat_timeout_seconds: float = 5.0
    auto_restart_stale_live_services: bool = True
    live_restart_cooldown_seconds: float = _DEFAULT_LIVE_RESTART_COOLDOWN_SECONDS
    live_restart_max_attempts: int = _DEFAULT_LIVE_RESTART_MAX_ATTEMPTS
    market_state_stale_after_seconds: float = (
        _DEFAULT_MARKET_STATE_STALE_AFTER_SECONDS
    )
    market_delay_warning_ms: float = _DEFAULT_MARKET_DELAY_WARNING_MS
    market_delay_critical_ms: float = _DEFAULT_MARKET_DELAY_CRITICAL_MS
    account_state_stale_after_seconds: float = (
        _DEFAULT_ACCOUNT_STATE_STALE_AFTER_SECONDS
    )
    position_stale_after_seconds: float = _DEFAULT_POSITION_STALE_AFTER_SECONDS
    position_quantity_tolerance: Decimal = _DEFAULT_POSITION_QUANTITY_TOLERANCE
    consistency_window_seconds: float = _DEFAULT_CONSISTENCY_WINDOW_SECONDS


class OpsMonitor:
    def __init__(
        self,
        config: MonitorConfig,
        *,
        runner: CommandRunner | None = None,
        clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if config.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if config.log_window_seconds <= 0:
            raise ValueError("log_window_seconds must be positive")
        if not 0 < config.rss_warning_fraction < config.rss_critical_fraction <= 1:
            raise ValueError("RSS thresholds are invalid")
        if config.memory_growth_required_samples <= 0:
            raise ValueError("memory_growth_required_samples must be positive")
        if bool(config.external_heartbeat_url) != bool(
            config.external_heartbeat_token
        ):
            raise ValueError(
                "external heartbeat URL and token must be configured together"
            )
        if config.external_heartbeat_url is not None:
            parsed_url = urllib.parse.urlparse(config.external_heartbeat_url)
            if parsed_url.scheme != "https" or not parsed_url.netloc:
                raise ValueError("external heartbeat URL must be an HTTPS URL")
        if config.external_heartbeat_timeout_seconds <= 0:
            raise ValueError("external heartbeat timeout must be positive")
        if config.live_restart_cooldown_seconds <= 0:
            raise ValueError("live_restart_cooldown_seconds must be positive")
        if config.live_restart_max_attempts <= 0:
            raise ValueError("live_restart_max_attempts must be positive")
        if config.market_state_stale_after_seconds <= 0:
            raise ValueError("market_state_stale_after_seconds must be positive")
        if not (
            0 < config.market_delay_warning_ms < config.market_delay_critical_ms
        ):
            raise ValueError("market delay thresholds are invalid")
        if config.account_state_stale_after_seconds <= 0:
            raise ValueError("account_state_stale_after_seconds must be positive")
        if config.position_stale_after_seconds <= 0:
            raise ValueError("position_stale_after_seconds must be positive")
        if config.position_quantity_tolerance < 0:
            raise ValueError("position_quantity_tolerance must not be negative")
        if config.consistency_window_seconds <= 0:
            raise ValueError("consistency_window_seconds must be positive")
        self._config = config
        self._runner = runner or SubprocessRunner()
        self._clock = clock
        self._sleeper = sleeper
        self._state = _load_state(config.state_path)
        self._crash_log_directory = config.crash_log_directory or (
            config.state_path.parent / "crash-logs"
        )

    def run_forever(self) -> None:
        while True:
            started_at = self._clock()
            try:
                self.run_once()
            except Exception as error:  # pragma: no cover - last-resort guard
                self._emit(
                    Alert(
                        "ops_monitor_failed",
                        "critical",
                        "Operational monitor iteration failed",
                        {"error_type": type(error).__name__, "error": str(error)},
                    ),
                    now=started_at,
                )
            elapsed = self._clock() - started_at
            self._sleeper(max(0.0, self._config.interval_seconds - elapsed))

    def run_once(self) -> tuple[Alert, ...]:
        now = self._clock()
        alerts: list[Alert] = []
        containers = self._container_snapshots()
        live_strategy_accounts = {
            _live_strategy_service(account_label): account_label
            for account_label, _run_id, _lease_owner in self._config.live_accounts
        }
        seen_services = {snapshot.service for snapshot in containers}
        for service in self._config.services:
            if service not in seen_services:
                alerts.append(
                    Alert(
                        "container_missing",
                        "critical",
                        f"Container {service} is missing from the Compose project",
                        {"service": service},
                    )
                )
        for snapshot in containers:
            alerts.extend(
                evaluate_container(
                    snapshot,
                    rss_warning_fraction=self._config.rss_warning_fraction,
                    rss_critical_fraction=self._config.rss_critical_fraction,
                )
            )
            alerts.extend(
                self._memory_pressure_alerts(snapshot)
            )
            alerts.extend(
                self._memory_growth_alerts(
                    snapshot.service,
                    snapshot.memory_bytes,
                    now,
                    container_id=snapshot.container_id,
                    metric_source=snapshot.memory_source,
                )
            )
            account_label = live_strategy_accounts.get(snapshot.service)
            if account_label is not None:
                alerts.extend(
                    self._live_heartbeat_alerts(snapshot, account_label, now)
                )

        market_id = self._container_id("market-data")
        strategy_services = tuple(
            _live_strategy_service(account_label)
            for account_label, _run_id, _lease_owner in self._config.live_accounts
        )
        combined_signals = LogSignals()
        for strategy_service in strategy_services:
            live_id = self._container_id(strategy_service)
            signals = self._log_signals(
                market_id,
                live_id,
                since_seconds=self._config.log_window_seconds,
            )
            combined_signals = LogSignals(
                telemetry_persist_failures=(
                    combined_signals.telemetry_persist_failures
                    + signals.telemetry_persist_failures
                ),
                legacy_order_identity_conflicts=(
                    combined_signals.legacy_order_identity_conflicts
                    + signals.legacy_order_identity_conflicts
                ),
                dead_connection_tasks=(
                    *combined_signals.dead_connection_tasks,
                    *signals.dead_connection_tasks,
                ),
                latest_rss_bytes=(
                    signals.latest_rss_bytes
                    if signals.latest_rss_bytes is not None
                    else combined_signals.latest_rss_bytes
                ),
                rss_observed_at=(
                    signals.rss_observed_at
                    if signals.rss_observed_at is not None
                    else combined_signals.rss_observed_at
                ),
            )
        alerts.extend(evaluate_log_signals(combined_signals))
        if combined_signals.latest_rss_bytes is not None:
            alerts.extend(
                self._memory_growth_alerts(
                    "market-data-process-memory",
                    combined_signals.latest_rss_bytes,
                    now,
                    container_id=market_id,
                    metric_source="process_rss_log",
                )
            )

        postgres_id = self._container_id("postgres")
        if postgres_id is not None:
            for account_label, run_id, lease_owner in self._config.live_accounts:
                try:
                    database_state = self._database_state(
                        postgres_id,
                        live_run_id=run_id,
                        live_account_label=account_label,
                        live_lease_owner=lease_owner,
                    )
                except Exception as error:
                    alerts.append(
                        Alert(
                            f"database_check_failed:{account_label}",
                            "critical",
                            "PostgreSQL observability query failed",
                            {
                                "account_label": account_label,
                                "error_type": type(error).__name__,
                                "error": str(error),
                            },
                        )
                    )
                else:
                    account_alerts = evaluate_database_state(
                        now=datetime.fromtimestamp(now, UTC),
                        latest_checkpoint_age_seconds=(
                            database_state.latest_checkpoint_age_seconds
                        ),
                        live_session_ready=database_state.live_session_ready,
                        pg_stat_statements_ready=database_state.pg_stat_statements_ready,
                        track_io_timing=database_state.track_io_timing,
                        track_wal_io_timing=database_state.track_wal_io_timing,
                        max_parallel_maintenance_workers=(
                            database_state.max_parallel_maintenance_workers
                        ),
                        stale_after_seconds=self._config.telemetry_stale_after_seconds,
                        market_state_stale_after_seconds=(
                            self._config.market_state_stale_after_seconds
                        ),
                        market_delay_warning_ms=self._config.market_delay_warning_ms,
                        market_delay_critical_ms=self._config.market_delay_critical_ms,
                        account_state_stale_after_seconds=(
                            self._config.account_state_stale_after_seconds
                        ),
                        account_process_state=database_state.account_process_state,
                        account_process_age_seconds=(
                            database_state.account_process_age_seconds
                        ),
                        latest_reconciliation_status=(
                            database_state.latest_reconciliation_status
                        ),
                        latest_reconciliation_age_seconds=(
                            database_state.latest_reconciliation_age_seconds
                        ),
                        latest_market_progress_age_seconds=(
                            database_state.latest_market_progress_age_seconds
                        ),
                        latest_market_delay_ms=database_state.latest_market_delay_ms,
                        unknown_order_count=database_state.unknown_order_count,
                        oldest_unknown_order_age_seconds=(
                            database_state.oldest_unknown_order_age_seconds
                        ),
                    )
                    alerts.extend(
                        replace(
                            alert,
                            name=f"{alert.name}:{account_label}",
                            details={
                                **alert.details,
                                "account_label": account_label,
                            },
                        )
                        for alert in account_alerts
                    )

            try:
                signal_observations, position_observations = (
                    self._consistency_observations(postgres_id)
                )
            except Exception as error:
                alerts.append(
                    Alert(
                        "live_consistency_check_failed",
                        "critical",
                        "Cross-account consistency query failed",
                        {
                            "error_type": type(error).__name__,
                            "error": str(error),
                            "accounts": [
                                account_label
                                for account_label, _run_id, _lease_owner in (
                                    self._config.live_accounts
                                )
                            ],
                        },
                    )
                )
            else:
                alerts.extend(evaluate_signal_divergence(signal_observations))
                alerts.extend(
                    evaluate_position_divergence(
                        position_observations,
                        stale_after_seconds=self._config.position_stale_after_seconds,
                        quantity_tolerance=self._config.position_quantity_tolerance,
                    )
                )

        active_keys = {alert.name for alert in alerts}
        for alert in alerts:
            self._emit(alert, now=now)
        self._emit_resolutions(active_keys, now=now)
        _save_state(self._config.state_path, self._state)
        self._send_external_heartbeat(
            build_deadman_heartbeat_payload(
                now=datetime.fromtimestamp(now, UTC),
                alerts=alerts,
            )
        )
        return tuple(alerts)

    def _send_external_heartbeat(
        self,
        payload: Mapping[str, object],
    ) -> None:
        if (
            self._config.external_heartbeat_url is None
            or self._config.external_heartbeat_token is None
        ):
            return
        try:
            _deliver_external_heartbeat(
                self._config.external_heartbeat_url,
                self._config.external_heartbeat_token,
                payload,
                timeout_seconds=self._config.external_heartbeat_timeout_seconds,
            )
        except Exception as error:  # pragma: no cover - external endpoint
            print(
                json.dumps(
                    {
                        "event": "ops_deadman_heartbeat_failed",
                        "error_type": type(error).__name__,
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )

    def _live_heartbeat_alerts(
        self,
        snapshot: ContainerSnapshot,
        account_label: str,
        now: float,
    ) -> tuple[Alert, ...]:
        """Alert on a stale live marker and restart that account's service.

        Docker's healthcheck reads the worker's local heartbeat marker, so an
        ``unhealthy`` live strategy is the host-side representation of a
        stale heartbeat.  Restart state is kept per Compose service so one
        frozen account cannot restart another account or consume its retry
        budget.
        """

        restart_states = self._state.setdefault("live_restart_state", {})
        if not isinstance(restart_states, dict):
            restart_states = {}
            self._state["live_restart_state"] = restart_states

        state = restart_states.get(snapshot.service)
        if not isinstance(state, dict):
            state = {}
            restart_states[snapshot.service] = state

        if snapshot.health == "healthy":
            restart_states.pop(snapshot.service, None)
            return ()

        last_restart_at = state.get("last_restart_at")
        if not isinstance(last_restart_at, int | float) or isinstance(
            last_restart_at, bool
        ):
            last_restart_at = None
        restart_attempts = state.get("restart_attempts", 0)
        if not isinstance(restart_attempts, int) or isinstance(
            restart_attempts, bool
        ):
            restart_attempts = 0

        details: dict[str, object] = {
            "account_label": account_label,
            "service": snapshot.service,
            "health": snapshot.health,
            "container_id": snapshot.container_id,
            "restart_count": snapshot.restart_count,
        }
        if "first_unhealthy_at" not in state:
            state["first_unhealthy_at"] = now
        stale = snapshot.health in {"unhealthy", "dead"}
        if not stale:
            if last_restart_at is None:
                restart_states.pop(snapshot.service, None)
                return ()
            details.update(
                {
                    "restart_attempts": restart_attempts,
                    "last_restart_at": last_restart_at,
                }
            )
            if state.get("last_restart_succeeded") is False:
                return (
                    Alert(
                        f"live_heartbeat_restart_failed:{account_label}",
                        "critical",
                        "Automatic live strategy restart failed",
                        {
                            **details,
                            "error_type": state.get("last_restart_error_type"),
                            "error": state.get("last_restart_error"),
                        },
                    ),
                )
            return (
                Alert(
                    f"live_heartbeat_auto_restarted:{account_label}",
                    "warning",
                    "Live strategy restart is in progress",
                    details,
                ),
            )

        stale_alert = Alert(
            f"live_heartbeat_stale:{account_label}",
            "critical",
            "Live strategy heartbeat is stale",
            {
                **details,
                "first_unhealthy_at": state["first_unhealthy_at"],
            },
        )
        alerts = [stale_alert]
        if state.get("log_archive_container_id") != snapshot.container_id:
            archive_path, archive_error = self._archive_container_logs(
                snapshot,
                observed_at=now,
            )
            if archive_path is not None:
                state["log_archive_container_id"] = snapshot.container_id
                state["log_archive_at"] = now
                details["crash_log_archive"] = str(archive_path)
            if archive_error is not None:
                alerts.append(
                    Alert(
                        f"live_crash_log_archive_failed:{account_label}",
                        "critical",
                        "Worker crash logs could not be archived before recovery",
                        {
                            **details,
                            "error_type": type(archive_error).__name__,
                        },
                    )
                )
        if not self._config.auto_restart_stale_live_services:
            return tuple(alerts)

        if restart_attempts >= self._config.live_restart_max_attempts:
            alerts.append(
                Alert(
                    f"live_heartbeat_restart_suppressed:{account_label}",
                    "critical",
                    "Automatic live strategy restart limit reached",
                    {
                        **details,
                        "restart_attempts": restart_attempts,
                        "max_attempts": self._config.live_restart_max_attempts,
                        "cooldown_seconds": (
                            self._config.live_restart_cooldown_seconds
                        ),
                    },
                )
            )
            return tuple(alerts)

        if (
            last_restart_at is not None
            and now - last_restart_at < self._config.live_restart_cooldown_seconds
        ):
            details.update(
                {
                    "restart_attempts": restart_attempts,
                    "last_restart_at": last_restart_at,
                    "cooldown_seconds": self._config.live_restart_cooldown_seconds,
                }
            )
            if state.get("last_restart_succeeded") is False:
                alerts.append(
                    Alert(
                        f"live_heartbeat_restart_failed:{account_label}",
                        "critical",
                        "Automatic live strategy restart failed",
                        {
                            **details,
                            "error_type": state.get("last_restart_error_type"),
                            "error": state.get("last_restart_error"),
                        },
                    )
                )
            else:
                alerts.append(
                    Alert(
                        f"live_heartbeat_auto_restarted:{account_label}",
                        "warning",
                        "Live strategy restart is awaiting health recovery",
                        details,
                    )
                )
            return tuple(alerts)

        attempt = restart_attempts + 1
        state.update(
            {
                "last_restart_at": now,
                "restart_attempts": attempt,
                "last_restart_succeeded": False,
            }
        )
        restart_command = [
            *self._compose_prefix(),
            "restart",
            snapshot.service,
        ]
        try:
            self._runner.run(
                restart_command,
                timeout_seconds=self._config.command_timeout_seconds,
            )
        except Exception as error:
            state.update(
                {
                    "last_restart_error_type": type(error).__name__,
                    "last_restart_error": str(error),
                }
            )
            alerts.append(
                Alert(
                    f"live_heartbeat_restart_failed:{account_label}",
                    "critical",
                    "Automatic live strategy restart failed",
                    {
                        **details,
                        "attempt": attempt,
                        "error_type": type(error).__name__,
                        "error": str(error),
                    },
                )
            )
        else:
            state["last_restart_succeeded"] = True
            state.pop("last_restart_error_type", None)
            state.pop("last_restart_error", None)
            alerts.append(
                Alert(
                    f"live_heartbeat_auto_restarted:{account_label}",
                    "warning",
                    "Stale live strategy heartbeat triggered an automatic restart",
                    {
                        **details,
                        "attempt": attempt,
                        "cooldown_seconds": (
                            self._config.live_restart_cooldown_seconds
                        ),
                    },
                )
            )
        return tuple(alerts)

    def _archive_container_logs(
        self,
        snapshot: ContainerSnapshot,
        *,
        observed_at: float,
    ) -> tuple[Path | None, Exception | None]:
        """Copy Docker's retained log stream before a worker recovery.

        A Compose restart normally keeps the container, but a deployment or a
        crash-loop can recreate it and erase the only useful traceback.  The
        archive is therefore a best-effort side effect and never blocks the
        actual recovery command on a logging failure.
        """

        directory = self._crash_log_directory
        if directory is None:  # pragma: no cover - explicit disablement hook
            return None, None
        safe_service = re.sub(r"[^A-Za-z0-9_.-]+", "_", snapshot.service)
        safe_container = re.sub(r"[^A-Za-z0-9_.-]+", "_", snapshot.container_id)
        timestamp = datetime.fromtimestamp(observed_at, UTC).strftime(
            "%Y%m%dT%H%M%S.%fZ"
        )
        destination = directory / (
            f"{timestamp}_{safe_service}_{safe_container[:24]}.log"
        )
        temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
        try:
            directory.mkdir(parents=True, exist_ok=True)
            output = self._runner.run(
                ["docker", "logs", "--timestamps", snapshot.container_id],
                timeout_seconds=self._config.command_timeout_seconds,
            )
            temporary.write_text(
                "# service="
                + snapshot.service
                + " container_id="
                + snapshot.container_id
                + " observed_at="
                + datetime.fromtimestamp(observed_at, UTC).isoformat()
                + "\n"
                + output,
                encoding="utf-8",
            )
            os.replace(temporary, destination)
        except Exception as error:
            try:
                temporary.unlink()
            except OSError:
                pass
            return None, error
        return destination, None

    def _container_id(self, service: str) -> str | None:
        # Docker labels avoid re-interpolating every Compose file on each
        # monitor tick. An optional live overlay may contain required secret
        # variables for accounts that are not enabled on this host.
        command = [
            "docker",
            "ps",
            "--filter",
            f"label=com.docker.compose.service={service}",
            "--format",
            "{{.ID}}",
        ]
        try:
            output = self._runner.run(
                command,
                timeout_seconds=self._config.command_timeout_seconds,
            )
        except Exception:
            return None
        value = next((line.strip() for line in output.splitlines() if line.strip()), "")
        return value or None

    def _container_snapshots(self) -> tuple[ContainerSnapshot, ...]:
        snapshots: list[ContainerSnapshot] = []
        for service in self._config.services:
            container_id = self._container_id(service)
            if container_id is None:
                continue
            payload = json.loads(
                self._runner.run(
                    ["docker", "inspect", container_id],
                    timeout_seconds=self._config.command_timeout_seconds,
                )
            )[0]
            state = payload.get("State", {})
            health = state.get("Health") or {}
            memory = self._memory_stats(container_id)
            snapshots.append(
                ContainerSnapshot(
                    service=service,
                    container_id=container_id,
                    health=health.get("Status"),
                    oom_killed=bool(state.get("OOMKilled", False)),
                    restart_count=int(payload.get("RestartCount", 0)),
                    memory_bytes=memory.observed_bytes,
                    memory_limit_bytes=memory.memory_limit_bytes,
                    memory_source=memory.source,
                    memory_working_set_bytes=memory.working_set_bytes,
                    memory_current_bytes=memory.current_bytes,
                    memory_peak_bytes=memory.peak_bytes,
                    memory_swap_current_bytes=memory.swap_current_bytes,
                    memory_events_max=memory.events_max,
                )
            )
        return tuple(snapshots)

    def _memory_stats(self, container_id: str) -> ContainerMemoryStats:
        payload = json.loads(
            self._runner.run(
                ["docker", "inspect", container_id],
                timeout_seconds=self._config.command_timeout_seconds,
            )
        )[0]
        memory_limit = int(payload.get("HostConfig", {}).get("Memory", 0) or 0)
        try:
            stats = self._runner.run(
                [
                    "docker",
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{.MemUsage}}",
                    container_id,
                ],
                timeout_seconds=self._config.command_timeout_seconds,
            ).strip()
            memory_text = stats.split("/", 1)[0].strip()
            working_set_bytes = _parse_size(memory_text)
        except Exception:
            working_set_bytes = None

        cgroup = self._cgroup_memory_stats(container_id)
        current_bytes = cgroup.get("memory.current")
        memory_bytes = (
            current_bytes if current_bytes is not None else working_set_bytes
        )
        source = (
            "cgroup_memory_current"
            if current_bytes is not None
            else "docker_stats_working_set"
        )
        return ContainerMemoryStats(
            observed_bytes=memory_bytes,
            memory_limit_bytes=memory_limit or cgroup.get("memory.max"),
            source=source,
            working_set_bytes=working_set_bytes,
            current_bytes=current_bytes,
            peak_bytes=cgroup.get("memory.peak"),
            swap_current_bytes=cgroup.get("memory.swap.current"),
            events_max=cgroup.get("memory.events.max"),
        )

    def _cgroup_memory_stats(self, container_id: str) -> dict[str, int]:
        """Read cgroup v2 memory counters without touching the database."""

        try:
            output = self._runner.run(
                [
                    "docker",
                    "exec",
                    container_id,
                    "sh",
                    "-c",
                    (
                        "for name in memory.current memory.peak "
                        "memory.max memory.swap.current; do "
                        "if [ -r \"/sys/fs/cgroup/$name\" ]; then "
                        "printf '%s=%s\\n' \"$name\" "
                        "\"$(cat \"/sys/fs/cgroup/$name\")\"; fi; "
                        "done; "
                        "if [ -r /sys/fs/cgroup/memory.events ]; then "
                        "while read -r key value _; do "
                        "if [ \"$key\" = max ]; then "
                        "printf 'memory.events.max=%s\\n' \"$value\"; "
                        "fi; done < /sys/fs/cgroup/memory.events; fi"
                    ),
                ],
                timeout_seconds=self._config.command_timeout_seconds,
            )
        except Exception:
            return {}
        values: dict[str, int] = {}
        for line in output.splitlines():
            key, separator, raw_value = line.partition("=")
            if not separator:
                continue
            try:
                value = int(raw_value.strip())
            except ValueError:
                continue
            if key in {
                "memory.current",
                "memory.peak",
                "memory.max",
                "memory.swap.current",
                "memory.events.max",
            } and value >= 0:
                values[key] = value
        return values

    def _log_signals(
        self,
        market_id: str | None,
        live_id: str | None,
        *,
        since_seconds: float,
    ) -> LogSignals:
        telemetry_failures = 0
        legacy_order_identity_conflicts = 0
        dead_tasks: list[str] = []
        latest_rss: int | None = None
        latest_rss_at: datetime | None = None
        for container_id in (market_id, live_id):
            if container_id is None:
                continue
            try:
                output = self._runner.run(
                    [
                        "docker",
                        "logs",
                        "--since",
                        f"{int(since_seconds)}s",
                        "--timestamps",
                        container_id,
                    ],
                    timeout_seconds=self._config.command_timeout_seconds,
                )
            except Exception:
                continue
            for line in output.splitlines():
                record = _parse_log_record(line)
                event = str(record.get("event", ""))
                if event == "live_runtime_telemetry_persist_failed":
                    telemetry_failures += 1
                elif event == "live_legacy_order_identity_conflict":
                    legacy_order_identity_conflicts += 1
                elif event == "market_data_connection_task_not_alive":
                    values = record.get("group_ids")
                    if isinstance(values, list | tuple):
                        dead_tasks.extend(str(value) for value in values)
                    elif values:
                        dead_tasks.append(str(values))
                elif event == "market_data_health_snapshot":
                    value = record.get("rss_bytes")
                    if isinstance(value, int) and (
                        latest_rss_at is None
                        or _record_timestamp(record) >= latest_rss_at
                    ):
                        latest_rss = value
                        latest_rss_at = _record_timestamp(record)
        return LogSignals(
            telemetry_persist_failures=telemetry_failures,
            legacy_order_identity_conflicts=legacy_order_identity_conflicts,
            dead_connection_tasks=tuple(sorted(set(dead_tasks))),
            latest_rss_bytes=latest_rss,
            rss_observed_at=latest_rss_at,
        )

    def _database_state(
        self,
        container_id: str,
        *,
        live_run_id: str | None = None,
        live_account_label: str | None = None,
        live_lease_owner: str | None = None,
    ) -> DatabaseState:
        run_id = _sql_literal(live_run_id or self._config.live_run_id)
        account_label = _sql_literal(
            live_account_label or self._config.live_account_label
        )
        lease_owner = _sql_literal(
            live_lease_owner or self._config.live_lease_owner
        )
        sql = f"""
SELECT 'checkpoint_age' || E'\\t' || COALESCE(
  EXTRACT(EPOCH FROM (clock_timestamp() - max(saved_at)))::text, '-1'
)
FROM strategy_runtime_checkpoints WHERE run_id = {run_id};
SELECT 'live_ready' || E'\\t' || (
  EXISTS (
    SELECT 1 FROM live_session_transitions
    WHERE session_id = {run_id}
      AND state IN ('live_enabled', 'draining')
  )
  AND EXISTS (
    SELECT 1 FROM trading_leases
    WHERE environment = 'live'
      AND account_label = {account_label}
      AND owner = {lease_owner}
      AND state = 'active'
      AND expires_at > clock_timestamp()
  )
  AND EXISTS (
    SELECT 1 FROM strategy_runtime_checkpoints
    WHERE run_id = {run_id}
  )
);
SELECT 'pg_stat_statements' || E'\\t' || (
  EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
  AND position('pg_stat_statements' in current_setting('shared_preload_libraries')) > 0
);
SELECT 'track_io_timing' || E'\\t' || current_setting('track_io_timing');
SELECT 'track_wal_io_timing' || E'\\t' || current_setting('track_wal_io_timing');
SELECT 'parallel_maintenance' || E'\\t' || current_setting(
  'max_parallel_maintenance_workers'
);
SELECT 'market_progress_age' || E'\\t' || COALESCE(
  EXTRACT(EPOCH FROM (clock_timestamp() - max(occurred_at)))::text, '-1'
)
FROM strategy_runtime_events
WHERE run_id = {run_id} AND event_type = 'market_state_progress';
SELECT 'market_delay_ms' || E'\\t' || COALESCE(
  (
    SELECT details->>'market_delay_ms'
    FROM strategy_runtime_events
    WHERE run_id = {run_id} AND event_type = 'market_state_progress'
    ORDER BY occurred_at DESC
    LIMIT 1
  ), '-1'
);
SELECT 'account_process_state' || E'\\t' || COALESCE(
  (
    SELECT state
    FROM execution_account_process_states
    WHERE environment = 'live' AND account_label = {account_label}
    ORDER BY occurred_at DESC
    LIMIT 1
  ), ''
);
SELECT 'account_process_age' || E'\\t' || COALESCE(
  (
    SELECT EXTRACT(EPOCH FROM (clock_timestamp() - occurred_at))::text
    FROM execution_account_process_states
    WHERE environment = 'live' AND account_label = {account_label}
    ORDER BY occurred_at DESC
    LIMIT 1
  ), '-1'
);
SELECT 'reconciliation_status' || E'\\t' || COALESCE(
  (
    SELECT status
    FROM account_reconciliation_runs
    WHERE environment = 'live' AND account_label = {account_label}
    ORDER BY observed_at DESC
    LIMIT 1
  ), ''
);
SELECT 'reconciliation_age' || E'\\t' || COALESCE(
  (
    SELECT EXTRACT(EPOCH FROM (clock_timestamp() - observed_at))::text
    FROM account_reconciliation_runs
    WHERE environment = 'live' AND account_label = {account_label}
    ORDER BY observed_at DESC
    LIMIT 1
  ), '-1'
);
SELECT 'unknown_order_count' || E'\\t' || count(*)::text
FROM exchange_orders
WHERE run_id = {run_id} AND state = 'unknown_pending_reconciliation';
SELECT 'oldest_unknown_order_age' || E'\\t' || COALESCE(
  EXTRACT(EPOCH FROM (clock_timestamp() - min(created_at)))::text, '-1'
)
FROM exchange_orders
WHERE run_id = {run_id} AND state = 'unknown_pending_reconciliation';
"""
        output = self._runner.run(
            [
                "docker",
                "exec",
                container_id,
                "psql",
                "-At",
                "-q",
                "-U",
                "cml",
                "-d",
                "cml",
                "-c",
                sql,
            ],
            timeout_seconds=self._config.command_timeout_seconds,
        )
        values: dict[str, str] = {}
        for line in output.splitlines():
            key, separator, value = line.partition("\t")
            if separator:
                values[key] = value.strip()
        age = _parse_float(values.get("checkpoint_age"))
        market_progress_age = _parse_float(values.get("market_progress_age"))
        market_delay_ms = _parse_float(values.get("market_delay_ms"))
        account_process_age = _parse_float(values.get("account_process_age"))
        reconciliation_age = _parse_float(values.get("reconciliation_age"))
        oldest_unknown_order_age = _parse_float(
            values.get("oldest_unknown_order_age")
        )
        return DatabaseState(
            latest_checkpoint_age_seconds=None if age is None or age < 0 else age,
            live_session_ready=_parse_bool(values.get("live_ready")),
            pg_stat_statements_ready=_parse_bool(values.get("pg_stat_statements")),
            track_io_timing=_parse_bool(values.get("track_io_timing")),
            track_wal_io_timing=_parse_bool(values.get("track_wal_io_timing")),
            max_parallel_maintenance_workers=_parse_int(
                values.get("parallel_maintenance")
            ),
            latest_market_progress_age_seconds=(
                None
                if market_progress_age is None or market_progress_age < 0
                else market_progress_age
            ),
            latest_market_delay_ms=(
                None
                if market_delay_ms is None or market_delay_ms < 0
                else market_delay_ms
            ),
            account_process_state=values.get("account_process_state") or None,
            account_process_age_seconds=(
                None
                if account_process_age is None or account_process_age < 0
                else account_process_age
            ),
            latest_reconciliation_status=(
                values.get("reconciliation_status") or None
            ),
            latest_reconciliation_age_seconds=(
                None
                if reconciliation_age is None or reconciliation_age < 0
                else reconciliation_age
            ),
            unknown_order_count=_parse_int(values.get("unknown_order_count")) or 0,
            oldest_unknown_order_age_seconds=(
                None
                if oldest_unknown_order_age is None or oldest_unknown_order_age < 0
                else oldest_unknown_order_age
            ),
        )

    def _consistency_observations(
        self,
        container_id: str,
    ) -> tuple[tuple[SignalObservation, ...], tuple[PositionObservation, ...]]:
        """Read a bounded cross-account consistency window from PostgreSQL."""

        accounts = tuple(self._config.live_accounts)
        account_labels = tuple(account_label for account_label, _, _ in accounts)
        run_ids = tuple(run_id for _, run_id, _ in accounts)
        account_sql = _sql_list(account_labels)
        run_sql = _sql_list(run_ids)
        window_seconds = _sql_numeric(self._config.consistency_window_seconds)
        sql = f"""
SELECT 'signal' || E'\\t' || account_label || E'\\t' || symbol || E'\\t'
  || source_state_at::text || E'\\t' || config_hash || E'\\t'
  || count(*)::text || E'\\t'
  || md5(string_agg(
    signal_kind || ':' || side || ':' || reason || ':'
      || features::text || ':' || reference_prices::text,
    E'\\x1f'
    ORDER BY signal_kind, side, reason, features::text, reference_prices::text
  ))
FROM live_strategy_signals
WHERE account_label IN ({account_sql})
  AND run_id IN ({run_sql})
  AND source_state_at >= clock_timestamp()
    - ({window_seconds} * interval '1 second')
GROUP BY account_label, symbol, source_state_at, config_hash;
SELECT 'output' || E'\\t' || run_id || E'\\t' || symbol || E'\\t'
  || bucket_start::text || E'\\t'
  || COALESCE(details->>'strategy_config_hash', '') || E'\\t'
  || COALESCE(details->>'signal_count', '0') || E'\\t'
  || COALESCE(details->>'candidate_count', '0')
FROM (
  SELECT DISTINCT ON (run_id, symbol, bucket_start)
    run_id, symbol, bucket_start, details, occurred_at
  FROM strategy_runtime_events
  WHERE event_type = 'strategy_output_observed'
    AND run_id IN ({run_sql})
    AND bucket_start IS NOT NULL
    AND occurred_at >= clock_timestamp()
      - ({window_seconds} * interval '1 second')
  ORDER BY run_id, symbol, bucket_start, occurred_at DESC
) latest_output;
WITH latest_reconciliation AS (
  SELECT DISTINCT ON (account_label)
    account_label, status, observed_at
  FROM account_reconciliation_runs
  WHERE environment = 'live' AND account_label IN ({account_sql})
  ORDER BY account_label, observed_at DESC
), position_cutoff AS (
  SELECT
    r.account_label,
    r.status,
    r.observed_at,
    max(p.observed_at) AS position_observed_at
  FROM latest_reconciliation r
  LEFT JOIN account_position_snapshots p
    ON p.environment = 'live'
   AND p.account_label = r.account_label
   AND p.observed_at <= r.observed_at
  GROUP BY r.account_label, r.status, r.observed_at
)
SELECT 'position' || E'\\t' || r.account_label || E'\\t' || r.status
  || E'\\t' || EXTRACT(EPOCH FROM (clock_timestamp() - r.observed_at))::text
  || E'\\t' || COALESCE(p.symbol, '') || E'\\t'
  || COALESCE(p.position_side, '') || E'\\t'
  || COALESCE(p.position_amt::text, '0')
FROM position_cutoff r
LEFT JOIN account_position_snapshots p
  ON p.environment = 'live'
 AND p.account_label = r.account_label
 AND p.observed_at = r.position_observed_at
WHERE p.position_amt IS NULL OR p.position_amt <> 0;
"""
        output = self._runner.run(
            [
                "docker",
                "exec",
                container_id,
                "psql",
                "-At",
                "-q",
                "-U",
                "cml",
                "-d",
                "cml",
                "-c",
                sql,
            ],
            timeout_seconds=self._config.command_timeout_seconds,
        )
        account_by_run_id = {
            run_id: account_label
            for account_label, run_id, _ in accounts
        }
        signals_by_key: dict[
            tuple[str, str, str, str], SignalObservation
        ] = {}
        positions: list[PositionObservation] = []
        for line in output.splitlines():
            parts = line.split("\t")
            if not parts:
                continue
            if parts[0] == "signal" and len(parts) == 7:
                key = (parts[1], parts[2], parts[3], parts[4])
                previous = signals_by_key.get(key)
                signals_by_key[key] = SignalObservation(
                    account_label=parts[1],
                    symbol=parts[2],
                    bucket_start=parts[3],
                    strategy_config_hash=parts[4],
                    signal_count=int(parts[5]),
                    candidate_count=(
                        0 if previous is None else previous.candidate_count
                    ),
                    fingerprint=parts[6] or None,
                )
                continue
            if parts[0] == "output" and len(parts) == 7:
                account_label = account_by_run_id.get(parts[1])
                if account_label is None:
                    continue
                key = (account_label, parts[2], parts[3], parts[4])
                previous = signals_by_key.get(key)
                signals_by_key[key] = SignalObservation(
                    account_label=account_label,
                    symbol=parts[2],
                    bucket_start=parts[3],
                    strategy_config_hash=parts[4],
                    signal_count=int(parts[5]),
                    candidate_count=int(parts[6]),
                    fingerprint=(
                        None if previous is None else previous.fingerprint
                    ),
                )
                continue
            if parts[0] == "position" and len(parts) == 7:
                positions.append(
                    PositionObservation(
                        account_label=parts[1],
                        status=parts[2],
                        age_seconds=_parse_float(parts[3]),
                        symbol=parts[4],
                        position_side=parts[5],
                        position_amt=Decimal(parts[6]),
                    )
                )
        return tuple(signals_by_key.values()), tuple(positions)

    def _memory_pressure_alerts(
        self,
        snapshot: ContainerSnapshot,
    ) -> tuple[Alert, ...]:
        """Alert when the cgroup max counter advances since the last check."""

        current = snapshot.memory_events_max
        if current is None:
            return ()
        counters = self._state.setdefault("memory_events_max", {})
        if not isinstance(counters, dict):
            counters = {}
            self._state["memory_events_max"] = counters
        previous = counters.get(snapshot.service)
        counters[snapshot.service] = current
        if not isinstance(previous, int) or current <= previous:
            return ()
        return (
            Alert(
                "container_memory_pressure",
                "warning",
                f"Container {snapshot.service} reached its cgroup memory limit",
                {
                    "service": snapshot.service,
                    "memory_bytes": snapshot.memory_bytes,
                    "memory_limit_bytes": snapshot.memory_limit_bytes,
                    "memory_source": snapshot.memory_source,
                    "memory_current_bytes": snapshot.memory_current_bytes,
                    "memory_peak_bytes": snapshot.memory_peak_bytes,
                    "memory_swap_current_bytes": (
                        snapshot.memory_swap_current_bytes
                    ),
                    "memory_events_max": current,
                    "memory_events_max_delta": current - previous,
                },
            ),
        )

    def _memory_growth_alerts(
        self,
        service: str,
        current_bytes: int | None,
        now: float,
        *,
        container_id: str | None = None,
        metric_source: str,
    ) -> tuple[Alert, ...]:
        samples_by_service = self._state.setdefault("memory_samples", {})
        if not isinstance(samples_by_service, dict):
            samples_by_service = {}
            self._state["memory_samples"] = samples_by_service

        growth_breaches = self._state.setdefault("memory_growth_breaches", {})
        if not isinstance(growth_breaches, dict):
            growth_breaches = {}
            self._state["memory_growth_breaches"] = growth_breaches

        if container_id is not None:
            sample_container_ids = self._state.setdefault(
                "memory_sample_container_ids",
                {},
            )
            if not isinstance(sample_container_ids, dict):
                sample_container_ids = {}
                self._state["memory_sample_container_ids"] = (
                    sample_container_ids
                )
            if sample_container_ids.get(service) != container_id:
                samples_by_service[service] = []
                growth_breaches[service] = 0
            sample_container_ids[service] = container_id

        samples = samples_by_service.setdefault(service, [])
        if not isinstance(samples, list):
            samples = []
            samples_by_service[service] = samples
        cutoff = now - self._config.rss_growth_window_seconds
        retained: list[list[float | int]] = []
        for sample in samples:
            if (
                isinstance(sample, list)
                and len(sample) == 2
                and isinstance(sample[0], int | float)
                and isinstance(sample[1], int)
                and sample[0] >= cutoff
                and sample[0] <= now
            ):
                retained.append(sample)

        baseline = retained[0] if retained else None
        baseline_bytes = int(baseline[1]) if baseline is not None else None
        baseline_age_seconds = (
            now - baseline[0] if baseline is not None else None
        )
        previous_breaches = growth_breaches.get(service, 0)
        if not isinstance(previous_breaches, int) or isinstance(
            previous_breaches, bool
        ):
            previous_breaches = 0
        breached = (
            current_bytes is not None
            and baseline_bytes is not None
            and current_bytes - baseline_bytes >= self._config.rss_growth_bytes
        )
        consecutive_samples = previous_breaches + 1 if breached else 0
        growth_breaches[service] = consecutive_samples
        if current_bytes is not None:
            retained.append([now, current_bytes])
        sample_limit = max(
            120,
            int(
                self._config.rss_growth_window_seconds
                / self._config.interval_seconds
            )
            + 2,
        )
        samples_by_service[service] = retained[-sample_limit:]
        return evaluate_container_memory_growth(
            service=service,
            current_bytes=current_bytes,
            baseline_bytes=baseline_bytes,
            baseline_age_seconds=baseline_age_seconds,
            consecutive_samples=consecutive_samples,
            required_samples=self._config.memory_growth_required_samples,
            growth_bytes=self._config.rss_growth_bytes,
            growth_window_seconds=self._config.rss_growth_window_seconds,
            metric_source=metric_source,
        )

    def _compose_prefix(self) -> list[str]:
        command = [
            "docker",
            "compose",
            "--project-directory",
            str(self._config.project_directory),
        ]
        if self._config.compose_env_file is not None:
            command.extend(["--env-file", str(self._config.compose_env_file)])
        compose_files = self._config.compose_files or (self._config.compose_file,)
        for compose_file in compose_files:
            command.extend(["-f", str(compose_file)])
        for profile in self._config.compose_profiles:
            command.extend(["--profile", profile])
        return command

    def _emit(self, alert: Alert, *, now: float) -> None:
        active = self._state.setdefault("active_alerts", {})
        previous = active.get(alert.name)
        if isinstance(previous, (int, float)) and (
            now - previous < self._config.alert_cooldown_seconds
        ):
            return
        active[alert.name] = now
        contexts = self._state.setdefault("active_alert_context", {})
        if isinstance(contexts, dict):
            contexts[alert.name] = {
                "severity": alert.severity,
                "summary": alert.summary,
                "details": dict(alert.details),
            }
        payload = {
            "event": "ops_alert",
            "observed_at": datetime.fromtimestamp(now, UTC).isoformat(),
            "alert_name": alert.name,
            "severity": alert.severity,
            "summary": alert.summary,
            "details": dict(alert.details),
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)
        _deliver_notification(
            self._config.webhook_url,
            self._config.serverchan_sendkey,
            payload,
        )

    def _emit_resolutions(self, active_keys: set[str], *, now: float) -> None:
        active = self._state.setdefault("active_alerts", {})
        contexts = self._state.setdefault("active_alert_context", {})
        for name in tuple(active):
            if name in active_keys:
                continue
            previous = active.get(name)
            duration_seconds = (
                round(now - previous, 3)
                if isinstance(previous, (int, float))
                else None
            )
            context = contexts.get(name) if isinstance(contexts, dict) else None
            context = context if isinstance(context, Mapping) else {}
            payload = {
                "event": "ops_alert_resolved",
                "observed_at": datetime.fromtimestamp(now, UTC).isoformat(),
                "alert_name": name,
                "severity": context.get("severity", "critical"),
                "summary": context.get("summary", ""),
                "details": context.get("details", {}),
                "duration_seconds": duration_seconds,
            }
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)
            _deliver_notification(
                self._config.webhook_url,
                self._config.serverchan_sendkey,
                payload,
            )
            active.pop(name, None)
            if isinstance(contexts, dict):
                contexts.pop(name, None)


def _parse_log_record(line: str) -> dict[str, object]:
    start = line.find("{")
    if start < 0:
        return {"event": line}
    try:
        value = json.loads(line[start:])
    except json.JSONDecodeError:
        return {"event": line}
    return value if isinstance(value, dict) else {"event": line}


def _record_timestamp(record: Mapping[str, object]) -> datetime:
    value = record.get("timestamp") or record.get("asctime")
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            pass
    return datetime.min.replace(tzinfo=UTC)


_SIZE_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)?\s*$")


def _parse_size(value: str) -> int | None:
    match = _SIZE_RE.match(value)
    if match is None:
        return None
    number = float(match.group(1))
    suffix = (match.group(2) or "B").lower()
    multipliers = {
        "b": 1,
        "kb": 1_000,
        "kib": 1_024,
        "mb": 1_000_000,
        "mib": 1_048_576,
        "gb": 1_000_000_000,
        "gib": 1_073_741_824,
        "tb": 1_000_000_000_000,
        "tib": 1_099_511_627_776,
    }
    multiplier = multipliers.get(suffix)
    return None if multiplier is None else int(number * multiplier)


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _parse_bool(value: str | None) -> bool:
    """Parse the boolean spellings emitted by PostgreSQL's text output."""

    return (value or "").strip().lower() in {"1", "on", "t", "true", "yes"}


def _parse_env_bool(value: str | None, *, default: bool) -> bool:
    """Parse a monitor boolean and fail closed on an invalid override."""

    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "on", "t", "true", "yes"}:
        return True
    if normalized in {"0", "off", "f", "false", "no"}:
        return False
    raise ValueError(f"invalid boolean value: {value!r}")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_list(values: Sequence[str]) -> str:
    normalized = tuple(value.strip() for value in values if value.strip())
    if not normalized:
        raise ValueError("SQL IN list must not be empty")
    return ", ".join(_sql_literal(value) for value in normalized)


def _sql_numeric(value: float) -> str:
    if value <= 0 or value != value or value in {float("inf"), float("-inf")}:
        raise ValueError("SQL numeric value must be finite and positive")
    return format(value, ".6f")


def _load_state(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _save_state(path: Path, state: Mapping[str, object]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(state, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    except OSError as error:
        print(
            json.dumps(
                {
                    "event": "ops_monitor_state_write_failed",
                    "error_type": type(error).__name__,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )


def _deliver_notification(
    webhook_url: str | None,
    serverchan_sendkey: str | None,
    payload: Mapping[str, object],
) -> None:
    if serverchan_sendkey:
        _deliver_serverchan(serverchan_sendkey, payload)
        return
    _deliver_webhook(webhook_url, payload)


def build_deadman_heartbeat_payload(
    *,
    now: datetime,
    alerts: Sequence[Alert],
) -> dict[str, object]:
    """Build a low-sensitivity heartbeat payload for an external observer."""

    critical_alerts = tuple(
        alert.name for alert in alerts if alert.severity.lower() == "critical"
    )
    warning_alerts = tuple(
        alert.name for alert in alerts if alert.severity.lower() == "warning"
    )
    status = (
        "critical"
        if critical_alerts
        else "warning"
        if warning_alerts
        else "healthy"
    )
    return {
        "event": "ops_heartbeat",
        "source": "cml-ops-monitor",
        "observed_at": now.astimezone(UTC).isoformat(),
        "status": status,
        "alert_count": len(alerts),
        "critical_alerts": critical_alerts,
        "warning_alerts": warning_alerts,
    }


def _deliver_external_heartbeat(
    url: str,
    token: str,
    payload: Mapping[str, object],
    *,
    timeout_seconds: float,
) -> None:
    """Send one authenticated heartbeat without putting the token in JSON."""

    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        status = getattr(response, "status", 200)
        if not 200 <= status < 300:
            raise RuntimeError(f"external heartbeat returned HTTP {status}")


def _deliver_webhook(url: str | None, payload: Mapping[str, object]) -> None:
    if not url:
        return
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5):
            pass
    except Exception as error:  # pragma: no cover - external endpoint
        print(
            json.dumps(
                {
                    "event": "ops_alert_delivery_failed",
                    "error_type": type(error).__name__,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )


def _deliver_serverchan(
    sendkey: str,
    payload: Mapping[str, object],
) -> None:
    try:
        request = urllib.request.Request(
            _serverchan_endpoint(sendkey),
            data=urllib.parse.urlencode(
                _serverchan_form(payload),
                doseq=False,
            ).encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            result = json.loads(response.read().decode("utf-8"))
        if not isinstance(result, dict) or result.get("code") not in {0, "0"}:
            raise RuntimeError("Server酱 returned a non-zero response")
    except Exception as error:  # pragma: no cover - external endpoint
        print(
            json.dumps(
                {
                    "event": "ops_alert_delivery_failed",
                    "error_type": type(error).__name__,
                    "provider": "serverchan",
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )


def _serverchan_endpoint(sendkey: str) -> str:
    key = sendkey.strip()
    if not key:
        raise ValueError("Server酱 SendKey must not be empty")
    sc3_match = re.match(r"^sctp([0-9]+)t", key)
    if sc3_match is not None:
        uid = sc3_match.group(1)
        return (
            f"https://{uid}.push.ft07.com/send/"
            f"{urllib.parse.quote(key, safe='')}.send"
        )
    return f"https://sctapi.ftqq.com/{urllib.parse.quote(key, safe='')}.send"


def _split_alert_name(alert_name: str) -> tuple[str, str | None]:
    base_name, separator, scope = alert_name.partition(":")
    return base_name, scope if separator and scope else None


def _service_scope(service: object) -> str | None:
    if not isinstance(service, str) or not service:
        return None
    if service == "live-strategy":
        return "primary"
    for prefix in ("live-strategy-", "execution-account-live-"):
        if service.startswith(prefix):
            return service.removeprefix(prefix)
    return service


def _alert_scope(alert_name: str, details: Mapping[str, object]) -> str | None:
    account_label = details.get("account_label")
    if account_label:
        return str(account_label)
    service_scope = _service_scope(details.get("service"))
    if service_scope:
        return service_scope
    _base_name, suffix = _split_alert_name(alert_name)
    return suffix


def _friendly_alert_label(alert_name: str, summary: str = "") -> str:
    base_name, _scope = _split_alert_name(alert_name)
    return _ALERT_LABELS.get(base_name, summary or base_name)


def _severity_label(severity: object) -> str:
    normalized = str(severity or "critical").lower()
    return _SEVERITY_LABELS.get(normalized, normalized.upper())


def _format_alert_time(value: object) -> str:
    raw_value = str(value or "")
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return raw_value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(_BEIJING_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")


def _format_duration(seconds: object) -> str:
    if not isinstance(seconds, (int, float)) or isinstance(seconds, bool):
        return "未知"
    total_seconds = max(0, int(round(seconds)))
    minutes, remainder = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours} 小时 {minutes} 分钟"
    if minutes:
        return f"{minutes} 分钟 {remainder} 秒"
    return f"{remainder} 秒"


def _alert_impact(alert_name: str, details: Mapping[str, object]) -> str:
    base_name, _scope = _split_alert_name(alert_name)
    impact = _ALERT_IMPACTS.get(base_name)
    if impact is not None:
        return impact
    service = _alert_scope(alert_name, details) or "相关服务"
    return f"{service} 可能存在异常，需要进一步确认。"


def _alert_action(alert_name: str, details: Mapping[str, object]) -> str:
    base_name, _scope = _split_alert_name(alert_name)
    if base_name == "live_heartbeat_stale":
        attempt = details.get("attempt")
        if attempt:
            return f"已触发定向重启（第 {attempt} 次），等待健康检查恢复。"
        restart_attempts = details.get("restart_attempts")
        if restart_attempts:
            return "已触发过自动重启，目前正在等待冷却或健康检查恢复。"
    if base_name == "live_heartbeat_auto_restarted":
        return "已执行定向重启，目前等待健康检查恢复。"
    if base_name == "live_heartbeat_restart_failed":
        return "自动重启失败，需要人工检查容器、日志和数据库。"
    if base_name == "live_heartbeat_restart_suppressed":
        return "已达到自动重启上限，不再继续重启，需要人工处理。"
    return _ALERT_ACTIONS.get(
        base_name,
        "已记录告警，建议结合技术详情检查相关服务。",
    )


def _serverchan_form(payload: Mapping[str, object]) -> dict[str, str]:
    event = str(payload.get("event", "ops_alert"))
    alert_name = str(payload.get("alert_name", "ops_monitor"))
    summary = str(payload.get("summary", ""))
    raw_details = payload.get("details", {})
    details = raw_details if isinstance(raw_details, Mapping) else {}
    scope = _alert_scope(alert_name, details)
    label = _friendly_alert_label(alert_name, summary)
    if event == "ops_alert":
        severity = _severity_label(payload.get("severity", "critical"))
        title_parts = ["CML", severity]
        if scope:
            title_parts.append(scope)
        title_parts.append(label)
        title = " | ".join(title_parts)
        body = [
            f"## [{severity}] {scope + '：' if scope else ''}{label}",
            "- **发生时间**："
            f"{_format_alert_time(payload.get('observed_at'))}（北京时间）",
            f"- **影响**：{_alert_impact(alert_name, details)}",
            f"- **处置**：{_alert_action(alert_name, details)}",
            f"- **事件编号**：`{alert_name}`",
        ]
        if details:
            body.append(
                "- **技术详情**：\n```json\n"
                + json.dumps(details, ensure_ascii=False, sort_keys=True)
                + "\n```"
            )
    else:
        title_parts = ["CML", "恢复"]
        if scope:
            title_parts.append(scope)
        title_parts.append(label)
        title = " | ".join(title_parts)
        body = [
            f"## [恢复] {scope + '：' if scope else ''}{label}",
            "- **恢复时间**："
            f"{_format_alert_time(payload.get('observed_at'))}（北京时间）",
            f"- **持续时间**：{_format_duration(payload.get('duration_seconds'))}",
            "- **当前状态**：监控已恢复，后续将继续观察。",
            f"- **原告警编号**：`{alert_name}`",
        ]
    return {
        "title": " ".join(title.split())[:32],
        "desp": "\n".join(body),
    }


def _env_path(name: str, default: Path | None) -> Path | None:
    value = os.environ.get(name)
    if value is None:
        return default
    return Path(value) if value else None


def _read_env_value(path: Path | None, name: str) -> str | None:
    """Read one non-secret value from a Compose-style environment file."""

    if path is None:
        return None
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in lines:
        candidate = line.strip()
        if not candidate or candidate.startswith("#"):
            continue
        if candidate.startswith("export "):
            candidate = candidate[7:].lstrip()
        key, separator, value = candidate.partition("=")
        if separator and key.strip() == name:
            value = value.strip()
            if value[:1] == value[-1:] and value[:1] in {"'", '"'}:
                value = value[1:-1]
            return value
    return None


def _parse_live_accounts(raw_value: str | None) -> tuple[tuple[str, str, str], ...]:
    if raw_value is None or not raw_value.strip():
        return (("primary", "live-primary-v1", "live-worker"),)
    accounts: list[tuple[str, str, str]] = []
    for item in raw_value.split(","):
        parts = tuple(part.strip() for part in item.split("|"))
        if len(parts) != 3 or any(not part for part in parts):
            raise ValueError(
                "CML_MONITOR_LIVE_ACCOUNTS must use "
                "label|session-id|lease-owner entries"
            )
        accounts.append((parts[0], parts[1], parts[2]))
    return tuple(accounts)


def _compose_service_names(compose_files: Sequence[Path]) -> tuple[str, ...]:
    """Read top-level service names without interpolating Compose secrets."""

    service_names: list[str] = []
    for compose_file in compose_files:
        try:
            lines = compose_file.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        in_services = False
        for line in lines:
            if line.strip() == "services:" and not line.startswith(" "):
                in_services = True
                continue
            if in_services and line and not line.startswith(" "):
                in_services = False
            if not in_services:
                continue
            match = _COMPOSE_SERVICE_HEADER.match(line)
            if match is not None:
                service = match.group("service")
                if service not in service_names:
                    service_names.append(service)
    return tuple(service_names)


def _live_account_label_for_service(service: str) -> str | None:
    for prefix in ("execution-account-live", "live-strategy"):
        if service == prefix:
            return "primary"
        prefix_with_separator = f"{prefix}-"
        if service.startswith(prefix_with_separator):
            return service[len(prefix_with_separator) :]
    return None


def _discover_live_account_labels(
    compose_files: Sequence[Path],
) -> tuple[str, ...]:
    labels: list[str] = []
    for service in _compose_service_names(compose_files):
        label = _live_account_label_for_service(service)
        if label is not None and label not in labels:
            labels.append(label)
    if "primary" in labels:
        labels.remove("primary")
        labels.insert(0, "primary")
    return tuple(labels) or ("primary",)


def _live_account_env_suffix(account_label: str) -> str:
    if account_label == "primary":
        return ""
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", account_label).strip("_")
    return f"_{normalized.upper()}"


def _configured_env_value(
    compose_env_file: Path | None,
    name: str,
) -> str | None:
    return os.environ.get(name) or _read_env_value(compose_env_file, name)


def _discover_live_accounts(
    compose_files: Sequence[Path],
    compose_env_file: Path | None,
) -> tuple[tuple[str, str, str], ...]:
    accounts: list[tuple[str, str, str]] = []
    for account_label in _discover_live_account_labels(compose_files):
        suffix = _live_account_env_suffix(account_label)
        default_session = (
            "live-primary-v1"
            if account_label == "primary"
            else f"live-{account_label}-v1"
        )
        default_lease_owner = (
            "live-worker"
            if account_label == "primary"
            else f"live-worker-{account_label}"
        )
        session_id = (
            _configured_env_value(
                compose_env_file,
                f"CML_LIVE_SESSION_ID{suffix}",
            )
            or default_session
        )
        lease_owner = (
            _configured_env_value(
                compose_env_file,
                f"CML_LIVE_LEASE_OWNER{suffix}",
            )
            or default_lease_owner
        )
        accounts.append((account_label, session_id, lease_owner))
    return tuple(accounts)


def _monitor_services_for_accounts(
    live_accounts: Sequence[tuple[str, str, str]],
) -> tuple[str, ...]:
    services = ["postgres", "market-data"]
    for account_label, _run_id, _lease_owner in live_accounts:
        suffix = "" if account_label == "primary" else f"-{account_label}"
        services.extend(
            (
                f"execution-account-live{suffix}",
                f"live-strategy{suffix}",
            )
        )
    return tuple(dict.fromkeys(services))


def build_config(args: argparse.Namespace) -> MonitorConfig:
    compose_env_file = _env_path("CML_COMPOSE_ENV_FILE", None)
    crash_log_directory_value = getattr(
        args,
        "crash_log_directory",
        os.environ.get("CML_CRASH_LOG_DIRECTORY"),
    )
    live_run_id = (
        args.live_run_id
        or os.environ.get("CML_LIVE_SESSION_ID")
        or _read_env_value(compose_env_file, "CML_LIVE_SESSION_ID")
        or "live-primary-v1"
    )
    live_account_label = (
        getattr(args, "live_account_label", None)
        or os.environ.get("CML_LIVE_ACCOUNT_LABEL")
        or _read_env_value(compose_env_file, "CML_LIVE_ACCOUNT_LABEL")
        or "primary"
    )
    live_lease_owner = (
        getattr(args, "live_lease_owner", None)
        or os.environ.get("CML_LIVE_LEASE_OWNER")
        or _read_env_value(compose_env_file, "CML_LIVE_LEASE_OWNER")
        or "live-worker"
    )
    compose_file_values = tuple(
        item.strip()
        for item in str(args.compose_file).split(",")
        if item.strip()
    )
    compose_files = tuple(Path(item) for item in compose_file_values)
    profile_values = tuple(
        item.strip()
        for item in os.environ.get("CML_COMPOSE_PROFILES", "").split(",")
        if item.strip()
    )
    configured_live_accounts = os.environ.get("CML_MONITOR_LIVE_ACCOUNTS")
    live_accounts = (
        _parse_live_accounts(configured_live_accounts)
        if configured_live_accounts is not None
        else _discover_live_accounts(compose_files, compose_env_file)
    )
    configured_services = getattr(args, "services", None)
    if configured_services is None:
        configured_services = os.environ.get("CML_MONITOR_SERVICES")
    if configured_services:
        services = tuple(
            item.strip() for item in configured_services.split(",") if item.strip()
        )
    else:
        services = _monitor_services_for_accounts(live_accounts)
    return MonitorConfig(
        project_directory=Path(args.project_directory),
        compose_file=(
            compose_files[0]
            if compose_files
            else Path(args.compose_file)
        ),
        compose_files=compose_files,
        compose_profiles=profile_values,
        compose_env_file=compose_env_file,
        services=services or _DEFAULT_SERVICES,
        live_accounts=live_accounts,
        live_run_id=live_run_id,
        live_account_label=live_account_label,
        live_lease_owner=live_lease_owner,
        interval_seconds=args.interval_seconds,
        log_window_seconds=args.log_window_seconds,
        telemetry_stale_after_seconds=args.telemetry_stale_after_seconds,
        rss_warning_fraction=args.rss_warning_fraction,
        rss_critical_fraction=args.rss_critical_fraction,
        rss_growth_bytes=args.rss_growth_bytes,
        rss_growth_window_seconds=args.rss_growth_window_seconds,
        memory_growth_required_samples=getattr(
            args,
            "memory_growth_required_samples",
            _DEFAULT_MEMORY_GROWTH_REQUIRED_SAMPLES,
        ),
        alert_cooldown_seconds=args.alert_cooldown_seconds,
        command_timeout_seconds=args.command_timeout_seconds,
        auto_restart_stale_live_services=_parse_env_bool(
            os.environ.get("CML_AUTO_RESTART_STALE_LIVE_SERVICES"),
            default=True,
        ),
        live_restart_cooldown_seconds=float(
            os.environ.get(
                "CML_LIVE_RESTART_COOLDOWN_SECONDS",
                _DEFAULT_LIVE_RESTART_COOLDOWN_SECONDS,
            )
        ),
        live_restart_max_attempts=int(
            os.environ.get(
                "CML_LIVE_RESTART_MAX_ATTEMPTS",
                _DEFAULT_LIVE_RESTART_MAX_ATTEMPTS,
            )
        ),
        market_state_stale_after_seconds=float(
            getattr(
                args,
                "market_state_stale_after_seconds",
                os.environ.get(
                    "CML_MARKET_STATE_STALE_AFTER_SECONDS",
                    _DEFAULT_MARKET_STATE_STALE_AFTER_SECONDS,
                ),
            )
        ),
        market_delay_warning_ms=float(
            getattr(
                args,
                "market_delay_warning_ms",
                os.environ.get(
                    "CML_MARKET_DELAY_WARNING_MS",
                    _DEFAULT_MARKET_DELAY_WARNING_MS,
                ),
            )
        ),
        market_delay_critical_ms=float(
            getattr(
                args,
                "market_delay_critical_ms",
                os.environ.get(
                    "CML_MARKET_DELAY_CRITICAL_MS",
                    _DEFAULT_MARKET_DELAY_CRITICAL_MS,
                ),
            )
        ),
        account_state_stale_after_seconds=float(
            getattr(
                args,
                "account_state_stale_after_seconds",
                os.environ.get(
                    "CML_ACCOUNT_STATE_STALE_AFTER_SECONDS",
                    _DEFAULT_ACCOUNT_STATE_STALE_AFTER_SECONDS,
                ),
            )
        ),
        position_stale_after_seconds=float(
            getattr(
                args,
                "position_stale_after_seconds",
                os.environ.get(
                    "CML_POSITION_STALE_AFTER_SECONDS",
                    _DEFAULT_POSITION_STALE_AFTER_SECONDS,
                ),
            )
        ),
        position_quantity_tolerance=Decimal(
            str(
                getattr(
                    args,
                    "position_quantity_tolerance",
                    os.environ.get(
                        "CML_POSITION_QUANTITY_TOLERANCE",
                        _DEFAULT_POSITION_QUANTITY_TOLERANCE,
                    ),
                )
            )
        ),
        consistency_window_seconds=float(
            getattr(
                args,
                "consistency_window_seconds",
                os.environ.get(
                    "CML_CONSISTENCY_WINDOW_SECONDS",
                    _DEFAULT_CONSISTENCY_WINDOW_SECONDS,
                ),
            )
        ),
        state_path=Path(args.state_path),
        crash_log_directory=(
            Path(crash_log_directory_value)
            if crash_log_directory_value
            else None
        ),
        webhook_url=os.environ.get("CML_ALERT_WEBHOOK_URL") or None,
        serverchan_sendkey=(
            os.environ.get("SERVERCHAN_SENDKEY")
            or os.environ.get("CML_SERVERCHAN_SENDKEY")
            or None
        ),
        external_heartbeat_url=(
            os.environ.get("CML_OPS_EXTERNAL_HEARTBEAT_URL") or None
        ),
        external_heartbeat_token=(
            os.environ.get("CML_OPS_EXTERNAL_HEARTBEAT_TOKEN") or None
        ),
        external_heartbeat_timeout_seconds=float(
            os.environ.get("CML_OPS_EXTERNAL_HEARTBEAT_TIMEOUT_SECONDS", "5")
        ),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-directory",
        default=os.environ.get("CML_PROJECT_DIRECTORY", "/opt/crypto-momentum-lab"),
    )
    parser.add_argument(
        "--compose-file",
        default=os.environ.get(
            "CML_COMPOSE_FILE",
            "/opt/crypto-momentum-lab/compose.server.yaml",
        ),
    )
    parser.add_argument(
        "--services",
        default=None,
    )
    parser.add_argument(
        "--live-run-id",
        default=None,
    )
    parser.add_argument(
        "--live-account-label",
        default=None,
    )
    parser.add_argument(
        "--live-lease-owner",
        default=None,
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=_DEFAULT_INTERVAL_SECONDS,
    )
    parser.add_argument(
        "--log-window-seconds",
        type=float,
        default=_DEFAULT_LOG_WINDOW_SECONDS,
    )
    parser.add_argument(
        "--telemetry-stale-after-seconds",
        type=float,
        default=_DEFAULT_TELEMETRY_STALE_AFTER_SECONDS,
    )
    parser.add_argument(
        "--market-state-stale-after-seconds",
        type=float,
        default=float(
            os.environ.get(
                "CML_MARKET_STATE_STALE_AFTER_SECONDS",
                _DEFAULT_MARKET_STATE_STALE_AFTER_SECONDS,
            )
        ),
    )
    parser.add_argument(
        "--market-delay-warning-ms",
        type=float,
        default=float(
            os.environ.get(
                "CML_MARKET_DELAY_WARNING_MS",
                _DEFAULT_MARKET_DELAY_WARNING_MS,
            )
        ),
    )
    parser.add_argument(
        "--market-delay-critical-ms",
        type=float,
        default=float(
            os.environ.get(
                "CML_MARKET_DELAY_CRITICAL_MS",
                _DEFAULT_MARKET_DELAY_CRITICAL_MS,
            )
        ),
    )
    parser.add_argument(
        "--account-state-stale-after-seconds",
        type=float,
        default=float(
            os.environ.get(
                "CML_ACCOUNT_STATE_STALE_AFTER_SECONDS",
                _DEFAULT_ACCOUNT_STATE_STALE_AFTER_SECONDS,
            )
        ),
    )
    parser.add_argument(
        "--position-stale-after-seconds",
        type=float,
        default=float(
            os.environ.get(
                "CML_POSITION_STALE_AFTER_SECONDS",
                _DEFAULT_POSITION_STALE_AFTER_SECONDS,
            )
        ),
    )
    parser.add_argument(
        "--position-quantity-tolerance",
        default=os.environ.get(
            "CML_POSITION_QUANTITY_TOLERANCE",
            str(_DEFAULT_POSITION_QUANTITY_TOLERANCE),
        ),
    )
    parser.add_argument(
        "--consistency-window-seconds",
        type=float,
        default=float(
            os.environ.get(
                "CML_CONSISTENCY_WINDOW_SECONDS",
                _DEFAULT_CONSISTENCY_WINDOW_SECONDS,
            )
        ),
    )
    parser.add_argument(
        "--rss-warning-fraction",
        type=float,
        default=_DEFAULT_RSS_WARNING_FRACTION,
    )
    parser.add_argument(
        "--rss-critical-fraction",
        type=float,
        default=_DEFAULT_RSS_CRITICAL_FRACTION,
    )
    parser.add_argument(
        "--rss-growth-bytes",
        type=int,
        default=_DEFAULT_RSS_GROWTH_BYTES,
    )
    parser.add_argument(
        "--rss-growth-window-seconds",
        type=float,
        default=_DEFAULT_RSS_GROWTH_WINDOW_SECONDS,
    )
    parser.add_argument(
        "--memory-growth-required-samples",
        type=int,
        default=int(
            os.environ.get(
                "CML_MEMORY_GROWTH_REQUIRED_SAMPLES",
                _DEFAULT_MEMORY_GROWTH_REQUIRED_SAMPLES,
            )
        ),
    )
    parser.add_argument(
        "--alert-cooldown-seconds",
        type=float,
        default=float(
            os.environ.get(
                "CML_ALERT_COOLDOWN_SECONDS",
                _DEFAULT_ALERT_COOLDOWN_SECONDS,
            )
        ),
    )
    parser.add_argument(
        "--command-timeout-seconds",
        type=float,
        default=_DEFAULT_COMMAND_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--state-path",
        default=os.environ.get(
            "CML_OPS_MONITOR_STATE_PATH",
            "/var/lib/crypto-momentum-lab/ops-monitor.json",
        ),
    )
    parser.add_argument(
        "--crash-log-directory",
        default=os.environ.get("CML_CRASH_LOG_DIRECTORY"),
    )
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    monitor = OpsMonitor(build_config(args))
    if args.once:
        monitor.run_once()
    else:
        monitor.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
