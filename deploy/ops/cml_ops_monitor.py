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

# Run as a script (`python3 deploy/ops/cml_ops_monitor.py`, which is how the
# systemd unit starts it) the repository root is not on sys.path, so importing
# the sibling package below would fail with ModuleNotFoundError.  Put it back.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from deploy.ops.maintenance_window import (
    default_maintenance_path,
    now_utc,
    read_maintenance_window,
)

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
# Per-service warning overrides.  postgres is judged on its working set, which
# includes its page cache: under load it legitimately climbs past the generic
# threshold while its durable footprint (anon + shared_buffers) stays under half
# the limit, and it has never been OOM-killed.  Raising only its warning keeps
# the pressure/growth signals -- the ones that mean real trouble -- intact.
_SERVICE_RSS_WARNING_FRACTION_OVERRIDES: Mapping[str, float] = {
    "postgres": 0.85,
}
# Services whose memory thresholds read anonymous memory instead of the Docker
# working set.  A database keeps most of its cgroup usage as reclaimable page
# cache: postgres reported 90-96% of its limit while `anon` sat near 15%, so
# every deploy -- which refills that cache -- produced a false "memory high".
_SERVICE_ANON_PRESSURE_SERVICES: frozenset[str] = frozenset({"postgres"})
_DEFAULT_RSS_GROWTH_BYTES = 64 * 1024 * 1024
# Anonymous memory pushed into swap is the cost that means "the process could
# not keep the memory it asked for".  Anything smaller is noise.
_DEFAULT_SWAP_GROWTH_BYTES = 32 * 1024 * 1024
_DEFAULT_RSS_GROWTH_WINDOW_SECONDS = 1_800.0
_DEFAULT_MEMORY_GROWTH_REQUIRED_SAMPLES = 3
_DEFAULT_ALERT_COOLDOWN_SECONDS = 900.0
_DEFAULT_COMMAND_TIMEOUT_SECONDS = 15.0
_DEFAULT_LIVE_RESTART_COOLDOWN_SECONDS = 900.0
_DEFAULT_LIVE_RESTART_MAX_ATTEMPTS = 3
# How long after a container starts lifecycle alerts stay quiet.  Every deploy
# recreates containers, and a booting container is unhealthy and silent by
# definition; without this each deploy reports a fake crash.
_DEFAULT_START_GRACE_SECONDS = 180.0
# The monitor polls every 60s and the underlying progress signals are themselves
# sampled on a ~60s cadence, so a 120s budget left only two samples of headroom
# and flapped on every tail-latency spike.  Measured live medians sit around
# 70-140s, so allow several samples before declaring staleness.
_DEFAULT_MARKET_STATE_STALE_AFTER_SECONDS = 300.0
_DEFAULT_MARKET_DELAY_WARNING_MS = 30_000.0
_DEFAULT_MARKET_DELAY_CRITICAL_MS = 120_000.0
_DEFAULT_ACCOUNT_STATE_STALE_AFTER_SECONDS = 300.0
_DEFAULT_POSITION_STALE_AFTER_SECONDS = 300.0
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
    "container_oom_killed": "服务触发 OOM 终止",
    "container_memory_high": "服务内存占用过高",
    "container_memory_growth": "服务内存趋势异常",
    "container_memory_pressure": "服务匿名内存被换出",
    "live_position_intent_divergence": "账户下单意图发生分叉",
    # Keep the legacy label so an alert written by an older monitor can still
    # be rendered correctly while its recovery record is being drained.
    "rss_growth": "服务内存持续增长",
    "telemetry_persist_failure": "运行时遥测写入失败",
    "live_legacy_order_identity_conflict": "订单身份发生冲突",
    "live_exit_processing_degraded": "平仓处理降级",
    "market_task_not_alive": "行情连接任务无响应",
    "live_session_not_ready": "实时会话未就绪",
    "live_checkpoint_stale": "策略检查点过期",
    "live_account_lifecycle_not_ready": "账户生命周期未就绪",
    "live_account_reconciliation_stale": "账户对账状态失步",
    "live_market_state_stale": "行情桶推进中断",
    "live_market_state_delay": "行情延迟过高",
    "live_signal_divergence": "账户信号发生分叉",
    "live_position_divergence": "账户持仓发生差异",
    "live_unknown_orders": "存在未确认在途订单",
    "live_consistency_check_failed": "跨账户一致性检查失败",
    "database_check_failed": "数据库健康检查失败",
    "database_query_stats_unavailable": "数据库查询统计不可用",
    "database_io_timing_disabled": "数据库 I/O 耗时监控未开启",
    "database_parallel_maintenance_enabled": "数据库并行维护超限",
    "live_heartbeat_stale": "实时策略心跳过期",
    "live_heartbeat_auto_restarted": "实时策略已自动重启",
    "live_heartbeat_restart_failed": "实时策略自动重启失败",
    "live_heartbeat_restart_suppressed": "策略自愈超限熔断",
    "live_crash_log_archive_failed": "worker 崩溃日志归档失败",
    "ops_monitor_failed": "运维监控自身异常",
}
_ALERT_IMPACTS = {
    "container_missing": "对应服务未运行，相关功能不可用。",
    "container_unhealthy": "容器健康检查探针持续超时，服务可能处于假死或无法正常响应状态。",
    "container_oom_killed": "对应服务已被系统内核强制终止，相关任务已中断。",
    "container_memory_high": "服务内存占用接近上限，继续增长可能触发 OOM 强杀。",
    "container_memory_growth": (
        "服务内存相对基线持续上升，需排查缓存泄漏与未释放连接。"
    ),
    "container_memory_pressure": (
        "进程匿名内存被换出到磁盘，再次访问需要读盘，服务响应可能因此变慢。"
    ),
    "rss_growth": "服务内存持续增长，后续可能出现性能下降或 OOM。",
    "telemetry_persist_failure": "运行时诊断数据可能不完整，不代表交易一定已停止。",
    "live_legacy_order_identity_conflict": "订单与交易所订单的归属可能无法安全关联。",
    "live_exit_processing_degraded": (
        "退出流程反复失败，持仓无法按策略平掉，浮亏可能持续扩大。"
    ),
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
    "live_position_intent_divergence": (
        "相同策略配置的账户向交易所下达了不同的订单意图，执行与风控路径可能已经分叉。"
    ),
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
    "container_missing": "检查 Docker Compose 编排状态与服务日志，确认服务退出原因并重新拉起。",
    "container_unhealthy": (
        "排查容器 recent logs 与 /health 端点响应耗时，确认服务是否假死或死锁。"
    ),
    "container_oom_killed": (
        "调大容器内存限额或排查泄漏；核对 dmesg OOM 现场日志后重启服务。"
    ),
    "container_memory_high": "排查服务内部堆内存、缓存积压与未释放资源，必要时调大容器内存限额。",
    "container_memory_growth": (
        "分析进程内存增长趋势与慢查询，排查未关闭的连接池或缓存泄漏。"
    ),
    "container_memory_pressure": (
        "若物理内存充足以观察为主；若持续换出且影响延迟，调大限额或排查冷页。"
    ),
    "rss_growth": "分析进程 RSS 内存增长趋势，排查连接泄漏与队列积压。",
    "telemetry_persist_failure": (
        "检查 PostgreSQL 慢查询、数据库连接池耗尽或遥测批量写入缓冲队列。"
    ),
    "live_legacy_order_identity_conflict": (
        "核对本地 client_order_id 与交易所 order_id 绑定关系，排查跨会话重复委托。"
    ),
    "live_exit_processing_degraded": (
        "退出委托受阻；紧急核对交易所实际持仓，必要时在交易所后台手动干预平仓。"
    ),
    "market_task_not_alive": "排查对应币种 WebSocket 任务心跳、宿主机网络延迟及交易所接口连通性。",
    "live_session_not_ready": (
        "暂停交易推进；排查分布式租约有效性、会话初始化状态及检查点完整性。"
    ),
    "live_checkpoint_stale": "排查策略持久化任务阻塞、数据库写入延迟或事务死锁等待。",
    "live_account_lifecycle_not_ready": (
        "检查账户状态机流转、API 凭据有效性及 strategy worker 启动状态。"
    ),
    "live_account_reconciliation_stale": (
        "排查交易所 REST API 限频、成交回报 WebSocket 回调及对账记录表。"
    ),
    "live_market_state_stale": "检查行情是否断流、是否存在缺桶及 durable rewarm 推进进度。",
    "live_market_state_delay": "检查交易所网络往返延迟（RTT）、事件循环调度耗时及数据库写入排队。",
    "live_signal_divergence": (
        "暂停扩仓；核对各账户策略配置差异、K 线数据新鲜度及近期事件循环日志。"
    ),
    "live_position_divergence": (
        "核对各账户交易所实际持仓快照，确认是否存在漏平仓；确认仓位前暂停扩仓。"
    ),
    "live_position_intent_divergence": (
        "暂停扩仓；逐账户核对 exchange_orders 的委托方向、数量与价格是否一致。"
    ),
    "live_unknown_orders": (
        "严禁重发相同订单；立即按 client_order_id 查交易所确认真实状态并对齐账目。"
    ),
    "live_consistency_check_failed": (
        "检查监控查询连接、一致性比对 SQL 耗时及数据库并发负载。"
    ),
    "database_check_failed": "检查 PostgreSQL 进程存活、磁盘剩余空间及监控用户只读查询权限。",
    "database_query_stats_unavailable": "在非交易活跃窗口于 postgresql.conf 启用 shared_preload_libraries='pg_stat_statements'。",
    "database_io_timing_disabled": "在数据库配置中启用 track_io_timing = on，以支持磁盘 I/O 延迟定位。",
    "database_parallel_maintenance_enabled": (
        "将 max_parallel_maintenance_workers 调低至 0 或 1，防止维护任务争用 CPU。"
    ),
    "live_crash_log_archive_failed": (
        "检查 /var/log 目录写入权限、磁盘剩余空间及 Docker 日志输出。"
    ),
    "live_heartbeat_stale": "主事件循环已失联；核对是否正在自动重启，若未自愈请人工排查阻塞或崩溃日志。",
    "ops_monitor_failed": "检查 cml-ops-monitor 自身运行日志与未捕获异常堆栈，必要时重启监控服务。",
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
    memory_anon_bytes: int | None = None
    memory_current_bytes: int | None = None
    memory_peak_bytes: int | None = None
    memory_swap_current_bytes: int | None = None
    memory_events_max: int | None = None
    # When the container started.  One that was just (re)created -- by a deploy
    # or by anything else -- is legitimately unhealthy and silent for a while,
    # so lifecycle alerts wait out a grace period before firing.
    started_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ContainerMemoryStats:
    """Container memory values from Docker and the cgroup v2 controller."""

    observed_bytes: int | None
    memory_limit_bytes: int | None
    source: str
    working_set_bytes: int | None = None
    anon_bytes: int | None = None
    current_bytes: int | None = None
    peak_bytes: int | None = None
    swap_current_bytes: int | None = None
    events_max: int | None = None


@dataclass(frozen=True, slots=True)
class LogSignals:
    telemetry_persist_failures: int = 0
    legacy_order_identity_conflicts: int = 0
    exit_processing_degraded_symbols: tuple[str, ...] = ()
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
    # The three arms of live_session_ready, reported separately so a not-ready
    # alert can name the failing one.  Default True keeps older SQL output
    # (which omits them) from being read as "not ready".
    live_session_state_ready: bool = True
    live_lease_active: bool = True
    live_checkpoint_present: bool = True
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
    # Accounts are only comparable when they run the same strategy config; an
    # empty hash means "unknown" and keeps the row in its own group.
    strategy_config_hash: str = ""


@dataclass(frozen=True, slots=True)
class OrderIntentObservation:
    """One account's order intent for one symbol inside the comparison window.

    Intent -- what an account asked the exchange to do -- is what comparable
    accounts must agree on.  How much of that intent filled is execution: a
    partially filled limit order legitimately leaves two correct accounts
    holding different positions, so quantity held is the wrong thing to
    compare.
    """

    account_label: str
    symbol: str
    order_count: int
    # Borrowed from the same account's signal rows: the order table carries a
    # run id, not a strategy config, and accounts running different strategies
    # must never be compared.
    strategy_config_hash: str = ""
    fingerprint: str | None = None
    intent_summary: str | None = None


def evaluate_signal_divergence(
    observations: Sequence[SignalObservation],
) -> tuple[Alert, ...]:
    """Detect different outputs for the same symbol/bucket/config group.

    A configuration hash is part of the comparison key. Accounts with
    intentionally different strategy parameters therefore do not create a
    false positive; accounts claiming the same config must agree on the signal
    count and durable content fingerprint. Candidate persistence is checked
    asynchronously and is deliberately excluded from this alert.

    The durable fingerprint covers the decision -- signal kind, side, reason,
    reference prices, and the account-stable features named in
    ``_SIGNAL_FINGERPRINT_FEATURE_KEYS`` -- not every recorded feature. Rolling
    ratios such as ``notional_5m_vs_30m`` are recomputed per run from that
    run's own market-state window, so two correct accounts sharing a config
    disagree on them by a rounding-level amount and must not alert.

    Opening signals only.  ``reduce_only_candidate`` carries the position into
    the comparison -- it exists because the account *holds* the symbol -- so
    including it would make this alert fire whenever the two accounts' fills
    differed, which is execution, not decision.  Where an account *asked the
    exchange* to close is compared by ``evaluate_position_intent_divergence``,
    which compares order intent and deliberately ignores the closing quantity.
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
        signal_outputs = {
            (value.signal_count, value.fingerprint) for value in outputs
        }
        if len(signal_outputs) <= 1:
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

    positions_by_account: dict[tuple[str, str], dict[tuple[str, str], Decimal]] = {}
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
            (observation.account_label, observation.strategy_config_hash),
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

    account_groups = tuple(sorted(positions_by_account))
    differences: list[dict[str, object]] = []
    for index, left_group in enumerate(account_groups):
        for right_group in account_groups[index + 1 :]:
            # Only accounts running the SAME strategy config are comparable;
            # comparing across configs produces false divergence.
            if left_group[1] != right_group[1]:
                continue
            left = positions_by_account[left_group]
            right = positions_by_account[right_group]
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
                        "accounts": [left_group[0], right_group[0]],
                        "strategy_config_hash": left_group[1],
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


def evaluate_position_intent_divergence(
    observations: Sequence[OrderIntentObservation],
) -> tuple[Alert, ...]:
    """Detect divergent *order intent* for one symbol/config group.

    Accounts sharing a strategy config must ask the exchange to do the same
    thing: same symbol, side, type, quantity and price.  What actually filled
    is not comparable -- a limit order that only partially fills, or one that
    expires before it fills, leaves two accounts with identical intent and
    different positions.  That is execution, and reporting it as a divergence
    tells the operator to investigate something no one can act on.
    """

    groups: dict[tuple[str, str], dict[str, OrderIntentObservation]] = {}
    for observation in observations:
        if not observation.account_label.strip() or not observation.symbol.strip():
            continue
        key = (observation.symbol, observation.strategy_config_hash)
        groups.setdefault(key, {})[observation.account_label] = observation

    differences: list[dict[str, object]] = []
    for (symbol, config_hash), by_account in sorted(groups.items()):
        if len(by_account) < 2:
            continue
        intents = {
            (value.order_count, value.fingerprint) for value in by_account.values()
        }
        if len(intents) <= 1:
            continue
        differences.append(
            {
                "symbol": symbol,
                "strategy_config_hash": config_hash,
                "accounts": [
                    {
                        "account_label": value.account_label,
                        "order_count": value.order_count,
                        "fingerprint": value.fingerprint,
                        "intent_summary": value.intent_summary,
                    }
                    for value in sorted(
                        by_account.values(),
                        key=lambda item: item.account_label,
                    )
                ],
            }
        )

    if not differences:
        return ()
    return (
        Alert(
            "live_position_intent_divergence",
            "critical",
            "Comparable live accounts sent divergent order intent",
            {
                "group_count": len(differences),
                "differences": differences[:20],
            },
        ),
    )


def evaluate_database_state(
    *,
    now: datetime,
    latest_checkpoint_age_seconds: float | None,
    live_session_ready: bool,
    live_session_state_ready: bool = True,
    live_lease_active: bool = True,
    live_checkpoint_present: bool = True,
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
                    "age_human": _human_seconds(account_process_age_seconds),
                    "threshold_seconds": account_state_stale_after_seconds,
                    "threshold_human": _human_seconds(
                        account_state_stale_after_seconds
                    ),
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
                    "age_human": _human_seconds(
                        latest_reconciliation_age_seconds
                    ),
                    "threshold_seconds": account_state_stale_after_seconds,
                    "threshold_human": _human_seconds(
                        account_state_stale_after_seconds
                    ),
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
                    "age_human": _human_seconds(
                        latest_market_progress_age_seconds
                    ),
                    "threshold_seconds": market_state_stale_after_seconds,
                    "threshold_human": _human_seconds(
                        market_state_stale_after_seconds
                    ),
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
                        "delay_human": _human_seconds(
                            latest_market_delay_ms / 1000
                        ),
                        "warning_threshold_ms": market_delay_warning_ms,
                        "warning_threshold_human": _human_seconds(
                            market_delay_warning_ms / 1000
                        ),
                        "critical_threshold_ms": market_delay_critical_ms,
                        "critical_threshold_human": _human_seconds(
                            market_delay_critical_ms / 1000
                        ),
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
                        "delay_human": _human_seconds(
                            latest_market_delay_ms / 1000
                        ),
                        "warning_threshold_ms": market_delay_warning_ms,
                        "warning_threshold_human": _human_seconds(
                            market_delay_warning_ms / 1000
                        ),
                        "critical_threshold_ms": market_delay_critical_ms,
                        "critical_threshold_human": _human_seconds(
                            market_delay_critical_ms / 1000
                        ),
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
                    "oldest_age_human": _human_seconds(
                        oldest_unknown_order_age_seconds
                    ),
                },
            )
        )
    if not live_session_ready:
        alerts.append(
            Alert(
                "live_session_not_ready",
                "critical",
                "Live session checkpoint or lease is not ready",
                {
                    # Which of the three arms of live_ready actually failed.
                    "session_state_ready": live_session_state_ready,
                    "lease_active": live_lease_active,
                    "checkpoint_present": live_checkpoint_present,
                    "checkpoint_age_seconds": (
                        None
                        if latest_checkpoint_age_seconds is None
                        else round(latest_checkpoint_age_seconds, 3)
                    ),
                    "checkpoint_age_human": _human_seconds(
                        latest_checkpoint_age_seconds
                    ),
                    "stale_after_seconds": stale_after_seconds,
                },
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
                    "age_human": _human_seconds(latest_checkpoint_age_seconds),
                    "threshold_seconds": stale_after_seconds,
                    "threshold_human": _human_seconds(stale_after_seconds),
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


def _merge_log_signals(left: LogSignals, right: LogSignals) -> LogSignals:
    """Combine two accounts' log signals without dropping any field.

    The live accounts are scanned one container at a time and merged.  Listing
    the fields by hand here means a newly added field is silently discarded for
    every account but the last, which is exactly how a stuck exit stayed
    invisible.  Keep this in step with LogSignals.
    """

    return LogSignals(
        telemetry_persist_failures=(
            left.telemetry_persist_failures + right.telemetry_persist_failures
        ),
        legacy_order_identity_conflicts=(
            left.legacy_order_identity_conflicts
            + right.legacy_order_identity_conflicts
        ),
        exit_processing_degraded_symbols=(
            *left.exit_processing_degraded_symbols,
            *right.exit_processing_degraded_symbols,
        ),
        dead_connection_tasks=(
            *left.dead_connection_tasks,
            *right.dead_connection_tasks,
        ),
        latest_rss_bytes=(
            right.latest_rss_bytes
            if right.latest_rss_bytes is not None
            else left.latest_rss_bytes
        ),
        rss_observed_at=(
            right.rss_observed_at
            if right.rss_observed_at is not None
            else left.rss_observed_at
        ),
    )


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
    if signals.exit_processing_degraded_symbols:
        alerts.append(
            Alert(
                "live_exit_processing_degraded",
                "critical",
                "Live exit processing is degraded and positions may not close",
                {"symbols": signals.exit_processing_degraded_symbols},
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


def rss_warning_fraction_for(service: str, default: float) -> float:
    """Return the memory warning threshold that applies to one service."""

    return _SERVICE_RSS_WARNING_FRACTION_OVERRIDES.get(service, default)


def _memory_pressure_reading(
    snapshot: ContainerSnapshot,
) -> tuple[int | None, str]:
    """Return the (bytes, source) behind this container's memory alerts.

    Anonymous memory for the services in ``_SERVICE_ANON_PRESSURE_SERVICES`` --
    what the process actually allocated -- and the reported working set for
    everything else.  When the cgroup counter is unavailable this falls back to
    the reported value rather than to ``None``, so an unreadable file cannot
    silence a real alert.
    """

    if (
        snapshot.service in _SERVICE_ANON_PRESSURE_SERVICES
        and snapshot.memory_anon_bytes is not None
    ):
        return snapshot.memory_anon_bytes, "cgroup_memory_anon"
    return snapshot.memory_bytes, snapshot.memory_source


def evaluate_container(
    snapshot: ContainerSnapshot,
    *,
    rss_warning_fraction: float,
    rss_critical_fraction: float,
) -> tuple[Alert, ...]:
    """Return alerts for Docker lifecycle and memory state."""

    alerts: list[Alert] = []
    pressure_bytes, pressure_source = _memory_pressure_reading(snapshot)
    # Byte counts carry a MiB companion: these are read on a phone, where
    # "951437312" is not a number anyone can size up at a glance.
    memory_details = {
        "service": snapshot.service,
        "memory_bytes": snapshot.memory_bytes,
        "memory_mb": _mib(snapshot.memory_bytes),
        "memory_limit_bytes": snapshot.memory_limit_bytes,
        "memory_limit_mb": _mib(snapshot.memory_limit_bytes),
        "memory_source": snapshot.memory_source,
        "memory_working_set_bytes": snapshot.memory_working_set_bytes,
        "memory_working_set_mb": _mib(snapshot.memory_working_set_bytes),
        "memory_anon_bytes": snapshot.memory_anon_bytes,
        "memory_anon_mb": _mib(snapshot.memory_anon_bytes),
        "memory_pressure_bytes": pressure_bytes,
        "memory_pressure_mb": _mib(pressure_bytes),
        "memory_pressure_source": pressure_source,
        "memory_current_bytes": snapshot.memory_current_bytes,
        "memory_current_mb": _mib(snapshot.memory_current_bytes),
        "memory_peak_bytes": snapshot.memory_peak_bytes,
        "memory_peak_mb": _mib(snapshot.memory_peak_bytes),
        "memory_swap_current_bytes": snapshot.memory_swap_current_bytes,
        "memory_swap_current_mb": _mib(snapshot.memory_swap_current_bytes),
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
        pressure_bytes is not None
        and snapshot.memory_limit_bytes is not None
        and snapshot.memory_limit_bytes > 0
    ):
        fraction = pressure_bytes / snapshot.memory_limit_bytes
        if fraction >= rss_critical_fraction:
            alerts.append(
                Alert(
                    "container_memory_high",
                    "critical",
                    f"Container {snapshot.service} memory is near its cgroup limit",
                    {
                        **memory_details,
                        "fraction": round(fraction, 4),
                        "fraction_percent": _percent(fraction),
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
                        "fraction_percent": _percent(fraction),
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
    memory_limit_bytes: int | None = None,
    warning_fraction: float = 1.0,
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
        # A container that just started grows from near-empty to its steady
        # state, and the trend baseline is reset whenever the container changes
        # -- so a deploy looks exactly like a leak.  Only treat the trend as a
        # problem when the container is also approaching its limit; below that
        # `container_memory_high` is the signal that matters.
        or (
            memory_limit_bytes is not None
            and memory_limit_bytes > 0
            and current_bytes < memory_limit_bytes * warning_fraction
        )
    ):
        return ()
    growth = current_bytes - baseline_bytes
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
                "growth_bytes": growth,
                "threshold_bytes": growth_bytes,
                # Human-readable companions: these alerts are triaged on a phone.
                "baseline_mb": _mib(baseline_bytes),
                "current_mb": _mib(current_bytes),
                "growth_mb": _mib(growth),
                "threshold_mb": _mib(growth_bytes),
                "growth_window_seconds": growth_window_seconds,
                "growth_window_human": _human_seconds(growth_window_seconds),
                "baseline_age_seconds": round(baseline_age_seconds, 3),
                "baseline_age_human": _human_seconds(baseline_age_seconds),
                "consecutive_samples": consecutive_samples,
                "required_samples": required_samples,
                "metric_source": metric_source,
            },
        ),
    )


def _mib(value: int | None) -> float | None:
    """Return bytes as MiB, rounded for display.

    Named ``_mb`` in the alert payloads for continuity; the divisor is 1024.
    """

    if value is None:
        return None
    return round(value / 1024 / 1024, 1)


def _human_seconds(seconds: float | None) -> str | None:
    """Render a duration the way a person would say it out loud.

    Alerts are triaged on a phone: "900.0" asks the reader to know it means
    fifteen minutes, and "1111093" as milliseconds means nothing at all.
    """

    if seconds is None:
        return None
    if seconds < 1:
        return f"{seconds * 1000:.0f} 毫秒"
    if seconds < 90:
        return f"{seconds:.1f} 秒"
    if seconds < 5400:
        return f"{seconds / 60:.1f} 分钟"
    return f"{seconds / 3600:.1f} 小时"


def _percent(fraction: float | None) -> float | None:
    """Render a 0-1 fraction as a percentage, which is what readers expect."""

    return None if fraction is None else round(fraction * 100, 2)


def _is_within_start_grace(
    started_at: datetime | None,
    *,
    now: datetime,
    grace_seconds: float,
) -> bool:
    """Report whether a container is still inside its post-start grace period.

    A container that was just (re)created is legitimately unhealthy and silent
    while it boots, and every deploy recreates containers -- so lifecycle alerts
    that fire on "unhealthy" or "no heartbeat" have to wait this out, or each
    deploy produces a fake crash report.
    """

    if started_at is None or grace_seconds <= 0:
        return False
    age_seconds = (now - started_at).total_seconds()
    return 0 <= age_seconds < grace_seconds


# Lifecycle alerts describe container churn, and a deploy IS container churn, so
# they are pure noise inside a declared maintenance window.  Memory and database
# alerts are deliberately not listed: a deploy can genuinely cause those.
_MAINTENANCE_SILENCED_PREFIXES = (
    "container_missing",
    "container_unhealthy",
    "container_oom_killed",
    "live_heartbeat_stale",
    "live_heartbeat_auto_restarted",
    "live_crash_log_archive_failed",
)


def _is_maintenance_noise(name: str) -> bool:
    """Report whether an alert should be silenced during a maintenance window."""

    return name.startswith(_MAINTENANCE_SILENCED_PREFIXES)


def _parse_started_at(raw: object) -> datetime | None:
    """Parse Docker's ``State.StartedAt``, tolerating its nanosecond precision."""

    if not isinstance(raw, str) or not raw.strip():
        return None
    text = raw.strip().replace("Z", "+00:00")
    if "." in text:
        # Docker reports nanoseconds; fromisoformat wants microseconds.
        head, _, rest = text.partition(".")
        fraction, _, offset = rest.partition("+")
        text = f"{head}.{fraction[:6]}"
        if offset:
            text = f"{text}+{offset}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


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
        """Evaluate one cycle, honouring any declared maintenance window."""

        alerts = self._evaluate_once()
        window = read_maintenance_window(default_maintenance_path())
        if window is not None and window.is_active(now=now_utc()):
            # A deploy declared this churn.  Only lifecycle alerts are noise
            # during it; memory and database alerts still carry meaning.
            return tuple(
                alert for alert in alerts if not _is_maintenance_noise(alert.name)
            )
        return alerts

    def _evaluate_once(self) -> tuple[Alert, ...]:
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
                    rss_warning_fraction=rss_warning_fraction_for(
                        snapshot.service,
                        self._config.rss_warning_fraction,
                    ),
                    rss_critical_fraction=self._config.rss_critical_fraction,
                )
            )
            alerts.extend(
                self._memory_pressure_alerts(snapshot)
            )
            pressure_bytes, pressure_source = _memory_pressure_reading(snapshot)
            alerts.extend(
                self._memory_growth_alerts(
                    snapshot.service,
                    pressure_bytes,
                    now,
                    container_id=snapshot.container_id,
                    metric_source=pressure_source,
                    memory_limit_bytes=snapshot.memory_limit_bytes,
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
            combined_signals = _merge_log_signals(combined_signals, signals)
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
                        live_session_state_ready=(
                            database_state.live_session_state_ready
                        ),
                        live_lease_active=database_state.live_lease_active,
                        live_checkpoint_present=(
                            database_state.live_checkpoint_present
                        ),
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
                (
                    signal_observations,
                    position_observations,
                    order_intent_observations,
                ) = self._consistency_observations(postgres_id)
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
                    evaluate_position_intent_divergence(order_intent_observations)
                )
                # The spread between accounts is recorded, not alerted: it is
                # the *result* of execution, so a difference here is the normal
                # outcome of a partially filled order, not a fault.
                self._record_position_spread(position_observations)

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

        # A container that was just recreated is booting, not frozen.  Deploys
        # recreate every live container, so without this each deploy reports a
        # stale heartbeat and tries to restart a container that is still starting.
        if _is_within_start_grace(
            snapshot.started_at,
            now=datetime.now(tz=UTC),
            grace_seconds=_DEFAULT_START_GRACE_SECONDS,
        ):
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
                    memory_anon_bytes=memory.anon_bytes,
                    memory_current_bytes=memory.current_bytes,
                    memory_peak_bytes=memory.peak_bytes,
                    memory_swap_current_bytes=memory.swap_current_bytes,
                    memory_events_max=memory.events_max,
                    started_at=_parse_started_at(state.get("StartedAt")),
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
        # Prefer the working set over cgroup memory.current: that counter counts
        # reclaimable page cache in full, so a database that mostly caches files
        # reports near its limit.  Services that still cache heavily are judged
        # on `anon` instead -- see _SERVICE_ANON_PRESSURE_SERVICES.
        memory_bytes = (
            working_set_bytes if working_set_bytes is not None else current_bytes
        )
        source = (
            "docker_stats_working_set"
            if working_set_bytes is not None
            else "cgroup_memory_current"
        )
        return ContainerMemoryStats(
            observed_bytes=memory_bytes,
            memory_limit_bytes=memory_limit or cgroup.get("memory.max"),
            source=source,
            working_set_bytes=working_set_bytes,
            anon_bytes=cgroup.get("memory.stat.anon"),
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
                        "fi; done < /sys/fs/cgroup/memory.events; fi; "
                        "if [ -r /sys/fs/cgroup/memory.stat ]; then "
                        "while read -r key value _; do "
                        "if [ \"$key\" = anon ]; then "
                        "printf 'memory.stat.anon=%s\\n' \"$value\"; "
                        "fi; done < /sys/fs/cgroup/memory.stat; fi"
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
                "memory.stat.anon",
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
        degraded_exit_symbols: set[str] = set()
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
                elif event == "live_grace_timeout_processing_degraded":
                    symbol = record.get("symbol")
                    if symbol:
                        degraded_exit_symbols.add(str(symbol))
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
            exit_processing_degraded_symbols=tuple(sorted(degraded_exit_symbols)),
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
      AND expires_at > now()
  )
  AND EXISTS (
    SELECT 1 FROM strategy_runtime_checkpoints
    WHERE run_id = {run_id}
  )
);
  -- The three arms of live_ready are reported separately so a not-ready alert can
  -- name the failing arm instead of sending the operator to three tables.
  SELECT 'live_session_state_ready' || E'\t' || (
    EXISTS (
      SELECT 1 FROM live_session_transitions
      WHERE session_id = {run_id}
        AND state IN ('live_enabled', 'draining')
    )
  )::text;
  SELECT 'live_lease_active' || E'\t' || (
    EXISTS (
      SELECT 1 FROM trading_leases
      WHERE environment = 'live'
        AND account_label = {account_label}
        AND owner = {lease_owner}
        AND state = 'active'
        AND expires_at > now()
    )
  )::text;
  SELECT 'live_checkpoint_present' || E'\t' || (
    EXISTS (
      SELECT 1 FROM strategy_runtime_checkpoints
      WHERE run_id = {run_id}
    )
  )::text;
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
    -- Only buckets a real event advanced carry a meaningful delay.  The
    -- market layer materializes zero-event buckets for quiet symbols so
    -- consumers see a dense 15-second clock, and every one of those sits at a
    -- past bucket_end by construction.  Measuring received_at - bucket_end on
    -- such a bucket reports how old it is, not how late data arrived, which is
    -- how an 18-minute and a 65-minute "delay" appeared while healthy buckets
    -- sat at 1.3 seconds.  A feed that genuinely stops is covered by
    -- market_task_not_alive and the market-data gap counters.
    SELECT details->>'market_delay_ms'
    FROM strategy_runtime_events
    WHERE run_id = {run_id} AND event_type = 'market_state_progress'
      AND COALESCE((details->>'source_event_count')::int, 0) > 0
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
            live_session_state_ready=_parse_bool_default(
                values.get("live_session_state_ready"), True
            ),
            live_lease_active=_parse_bool_default(
                values.get("live_lease_active"), True
            ),
            live_checkpoint_present=_parse_bool_default(
                values.get("live_checkpoint_present"), True
            ),
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
    ) -> tuple[
        tuple[SignalObservation, ...],
        tuple[PositionObservation, ...],
        tuple[OrderIntentObservation, ...],
    ]:
        """Read a bounded cross-account consistency window from PostgreSQL."""

        accounts = tuple(self._config.live_accounts)
        account_labels = tuple(account_label for account_label, _, _ in accounts)
        run_ids = tuple(run_id for _, run_id, _ in accounts)
        account_sql = _sql_list(account_labels)
        run_sql = _sql_list(run_ids)
        window_seconds = _sql_numeric(self._config.consistency_window_seconds)
        # Fingerprint only the decision-relevant features: the full ``features``
        # blob carries rolling ratios that differ between two correct accounts
        # on the same configuration (see _SIGNAL_FINGERPRINT_FEATURE_KEYS).
        features_sql = _fingerprint_features_sql()
        prices_sql = _fingerprint_reference_prices_sql()
        sql = f"""
SELECT 'signal' || E'\\t' || account_label || E'\\t' || symbol || E'\\t'
  || source_state_at::text || E'\\t' || config_hash || E'\\t'
  || count(*)::text || E'\\t'
  || md5(string_agg(
    signal_kind || ':' || side || ':' || reason || ':'
      || {features_sql} || ':' || {prices_sql},
    E'\\x1f'
    ORDER BY signal_kind, side, reason, {features_sql}, {prices_sql}
  ))
FROM live_strategy_signals
WHERE account_label IN ({account_sql})
  AND run_id IN ({run_sql})
  -- reduce_only_candidate is not a strategy decision: it means "this account
  -- holds the symbol and a close condition fired", so its presence depends on
  -- what filled -- and fills legitimately differ between two correct accounts.
  -- Requiring it to match reports the position difference through the signal
  -- table, the same way comparing a closing order's quantity would.
  AND signal_kind <> {_sql_literal(_REDUCE_ONLY_SIGNAL_KIND)}
  AND source_state_at >= now()
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
    -- now() rather than clock_timestamp(): the planner can fold a stable value
    -- into an index range condition, but clock_timestamp() is re-evaluated per
    -- row, so it cannot -- the result is a sequential scan.  Measured on this
    -- table: 3721 ms / 74,825 pages read with clock_timestamp(), 9 ms / 0
    -- pages with now().  These queries are single autocommit statements, so
    -- the two timestamps differ by well under a millisecond.
    AND occurred_at >= now()
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
SELECT 'order' || E'\\t' || s.account_label || E'\\t' || s.symbol || E'\\t'
  || s.config_hash || E'\\t' || COALESCE(o.order_count, 0)::text || E'\\t'
  || COALESCE(o.fingerprint, '') || E'\\t' || COALESCE(o.intent_summary, '')
FROM (
  SELECT DISTINCT account_label, run_id, symbol, config_hash
  FROM live_strategy_signals
  WHERE account_label IN ({account_sql})
    AND run_id IN ({run_sql})
    AND source_state_at >= now()
      - ({window_seconds} * interval '1 second')
) s
LEFT JOIN (
  SELECT run_id, symbol,
    count(*) AS order_count,
    string_agg(
      (CASE WHEN upper(side) = 'BUY' THEN '买入' WHEN upper(side) = 'SELL' THEN '卖出' ELSE side END)
      || ' '
      || CASE
           WHEN reduce_only THEN '平仓'
           ELSE trim_scale(quantity)::text || ' @ ' || COALESCE(trim_scale(price)::text, '市价')
         END
      || '（' || (CASE WHEN upper(order_type) = 'LIMIT' THEN '限价' WHEN upper(order_type) = 'MARKET' THEN '市价' ELSE order_type END) || '）',
      E'\\x1e'
      ORDER BY side, order_type, reduce_only, quantity, price
    ) AS intent_summary,
    -- An opening order states a quantity the strategy chose, so it takes part
    -- in the fingerprint.  A closing order does not: how much to sell is a
    -- function of how much is held, and two accounts whose *fills* differed
    -- hold different amounts.  Comparing those quantities would report the
    -- fill difference again, through a different column.
    md5(string_agg(
      side || ':' || order_type || ':'
        || CASE
             WHEN reduce_only THEN 'close'
             ELSE quantity::text || ':' || COALESCE(price::text, '')
           END,
      E'\\x1f'
      ORDER BY side, order_type, reduce_only, quantity::text, price::text
    )) AS fingerprint
  FROM exchange_orders
  WHERE run_id IN ({run_sql})
    AND created_at >= now()
      - ({window_seconds} * interval '1 second')
  GROUP BY run_id, symbol
) o ON o.run_id = s.run_id AND o.symbol = s.symbol;
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
        order_intents: list[OrderIntentObservation] = []
        config_by_account: dict[str, str] = {}
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
                config_by_account.setdefault(account_label, parts[4])
                key = (account_label, parts[2], parts[3], parts[4])
                previous = signals_by_key.get(key)
                # Only the candidate count is taken from the observed event.
                # Its ``signal_count`` is not trustworthy: the same
                # (run, bucket) writes it repeatedly with a value that
                # disagrees with the durable signal rows, so borrowing it made
                # two accounts look divergent when both had exactly one signal.
                signals_by_key[key] = SignalObservation(
                    account_label=account_label,
                    symbol=parts[2],
                    bucket_start=parts[3],
                    strategy_config_hash=parts[4],
                    signal_count=(
                        0 if previous is None else previous.signal_count
                    ),
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
                continue
            if parts[0] == "order" and len(parts) >= 6:
                order_intents.append(
                    OrderIntentObservation(
                        account_label=parts[1],
                        symbol=parts[2],
                        strategy_config_hash=parts[3],
                        order_count=int(parts[4]),
                        fingerprint=parts[5] or None,
                        intent_summary=(
                            parts[6] if len(parts) >= 7 and parts[6] else None
                        ),
                    )
                )
        # Position rows carry no strategy config hash of their own (the
        # snapshot table has no such column), so borrow it from the same
        # account's signal rows.  Without this, accounts running different
        # strategies would be compared against each other.
        if config_by_account:
            positions = [
                replace(
                    observation,
                    strategy_config_hash=config_by_account.get(
                        observation.account_label,
                        "",
                    ),
                )
                for observation in positions
            ]
        return (
            tuple(signals_by_key.values()),
            tuple(positions),
            tuple(order_intents),
        )

    def _record_position_spread(
        self,
        observations: Sequence[PositionObservation],
    ) -> None:
        """Record the cross-account position spread without alerting on it.

        What an account *holds* is the result of execution, so a difference
        between two accounts with identical intent is the expected outcome of a
        limit order that only partially filled.  The operator cannot act on it,
        so it is kept as state for inspection rather than raised as alert.
        """

        by_group: dict[str, dict[str, Decimal]] = {}
        for observation in observations:
            if observation.status != "ready":
                continue
            if (
                observation.age_seconds is None
                or observation.age_seconds < 0
                or observation.age_seconds > self._config.position_stale_after_seconds
            ):
                continue
            if not observation.symbol.strip() or not observation.position_side.strip():
                continue
            if observation.position_amt == 0:
                continue
            account_positions = by_group.setdefault(
                f"{observation.symbol}|{observation.strategy_config_hash}",
                {},
            )
            label = f"{observation.account_label}:{observation.position_side}"
            account_positions[label] = (
                account_positions.get(label, Decimal("0")) + observation.position_amt
            )

        spread: list[dict[str, object]] = []
        for group, by_account in sorted(by_group.items()):
            if len(by_account) < 2:
                continue
            quantities = sorted(set(by_account.values()))
            if len(quantities) < 2:
                continue
            symbol, _, config_hash = group.partition("|")
            spread.append(
                {
                    "symbol": symbol,
                    "strategy_config_hash": config_hash,
                    "min_quantity": str(quantities[0]),
                    "max_quantity": str(quantities[-1]),
                    "accounts": {
                        label: str(quantity)
                        for label, quantity in sorted(by_account.items())
                    },
                }
            )
        self._state["position_spread"] = spread[:20]

    def _memory_pressure_alerts(
        self,
        snapshot: ContainerSnapshot,
    ) -> tuple[Alert, ...]:
        """Alert when a container is *paying* for memory, not merely caching.

        The old trigger was the cgroup ``memory.events.max`` counter advancing.
        That counter moves every time the kernel reclaims something to stay
        under the limit -- and for a database that mostly caches files that is
        constant, normal housekeeping: the page cache refills the limit by
        design, and stealing the oldest pages is how it makes room.  Measured
        on the live host it read 50394 "reaches" while 40 seconds of sampling
        showed zero page scans, zero steals, an unchanged counter, no OOM kill,
        and a swap level drifting *down*.  The signal was the kernel doing its
        job.

        Anonymous memory pushed into swap is the cost that actually matters:
        it means the process could not keep memory it asked for.  The baseline
        follows the minimum so slow, steady growth still accumulates into an
        alert, and it is re-based after reporting so the same swap is not
        reported twice.
        """

        current = snapshot.memory_swap_current_bytes
        if current is None:
            return ()
        counters = self._state.setdefault("memory_swap_bytes", {})
        if not isinstance(counters, dict):
            counters = {}
            self._state["memory_swap_bytes"] = counters
        previous = counters.get(snapshot.service)
        if not isinstance(previous, int):
            # First observation: establish the baseline, nothing to compare.
            counters[snapshot.service] = current
            return ()
        if current < previous:
            # Swap draining back: carry the baseline down with it.
            counters[snapshot.service] = current
            return ()
        growth = current - previous
        if growth < _DEFAULT_SWAP_GROWTH_BYTES:
            return ()
        counters[snapshot.service] = current
        return (
            Alert(
                "container_memory_pressure",
                "warning",
                (
                    f"Container {snapshot.service} pushed anonymous memory "
                    "into swap"
                ),
                {
                    "service": snapshot.service,
                    "memory_bytes": snapshot.memory_bytes,
                    "memory_mb": _mib(snapshot.memory_bytes),
                    "memory_limit_bytes": snapshot.memory_limit_bytes,
                    "memory_limit_mb": _mib(snapshot.memory_limit_bytes),
                    "memory_source": snapshot.memory_source,
                    "memory_current_bytes": snapshot.memory_current_bytes,
                    "memory_current_mb": _mib(snapshot.memory_current_bytes),
                    "memory_peak_bytes": snapshot.memory_peak_bytes,
                    "memory_peak_mb": _mib(snapshot.memory_peak_bytes),
                    "memory_swap_current_bytes": current,
                    "memory_swap_current_mb": _mib(current),
                    "memory_swap_growth_bytes": growth,
                    "memory_swap_growth_mb": _mib(growth),
                    "memory_events_max": snapshot.memory_events_max,
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
        memory_limit_bytes: int | None = None,
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
            memory_limit_bytes=memory_limit_bytes,
            warning_fraction=rss_warning_fraction_for(
                service,
                self._config.rss_warning_fraction,
            ),
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


# structlog's console renderer -- what the containers actually emit -- looks like
#   2026-09-15 12:20:54 [warning  ] event_name  key=value key=value
# The JSON branch below never matched it, so every event comparison in
# _log_signals was silently false: no log-based alert could ever fire.
# docker logs --timestamps prepends its own RFC3339 stamp, so a real line holds
# two timestamps before the structlog level marker.  Anchor on the marker rather
# than the line start so both spellings parse.
_CONSOLE_LOG_HEAD_RE = re.compile(
    r"\[\s*(?P<level>[A-Za-z]+)\s*\]\s+(?P<event>\S+)\s*(?P<fields>.*)$"
)
_CONSOLE_LOG_FIELD_RE = re.compile(
    r'(?P<key>[A-Za-z_][A-Za-z0-9_.]*)=(?P<value>"[^"]*"|\S+)'
)


def _coerce_console_value(value: str) -> object:
    """Give console fields the same types the JSON renderer would produce."""

    if value == "True":
        return True
    if value == "False":
        return False
    if value == "None":
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


def _parse_console_log_record(line: str) -> dict[str, object] | None:
    head = _CONSOLE_LOG_HEAD_RE.search(line)
    if head is None:
        return None
    record: dict[str, object] = {
        "event": head.group("event"),
        "level": head.group("level"),
    }
    for match in _CONSOLE_LOG_FIELD_RE.finditer(head.group("fields")):
        raw = match.group("value")
        if len(raw) >= 2 and raw[:1] == '"' and raw[-1:] == '"':
            raw = raw[1:-1]
        record[match.group("key")] = _coerce_console_value(raw)
    return record


def _parse_log_record(line: str) -> dict[str, object]:
    start = line.find("{")
    if start >= 0:
        try:
            value = json.loads(line[start:])
        except json.JSONDecodeError:
            value = None
        if isinstance(value, dict):
            return value
    console = _parse_console_log_record(line)
    if console is not None:
        return console
    return {"event": line}


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


def _parse_bool_default(value: str | None, default: bool) -> bool:
    """Parse a boolean that older SQL output may not have emitted yet."""

    if value is None or not str(value).strip():
        return default
    return _parse_bool(value)


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


# Only decision-relevant, account-stable features take part in the durable
# signal fingerprint.  Rolling ratio features (``notional_5m_vs_30m``) are
# recomputed per run from that run's own local market-state window, so two
# accounts on the *same* configuration disagree there by a rounding-level
# amount and would report a divergence forever.  A whitelist rather than a
# list of exceptions keeps a future feature from quietly reintroducing that
# noise.  The keys mirror ``_features`` in
# ``strategies/order_flow_impulse/runtime.py``.
_SIGNAL_FINGERPRINT_FEATURE_KEYS: tuple[str, ...] = (
    "direction",
    "impulse_start",
    "impulse_end",
    "impulse_start_price",
    "impulse_end_price",
    "impulse_return_pct",
    "breakout_level",
    "breakout_distance_pct",
    "impulse_trade_count",
    "impulse_trade_notional",
    "aggressive_buy_notional",
    "aggressive_sell_notional",
    "aggressive_imbalance",
    "baseline_notional",
    "notional_intensity",
    "liquidation_count",
    "liquidation_notional",
)


# A reduce-only signal reports how much to *close*, which is a function of how
# much is held -- and accounts whose fills differed hold different amounts.
# batch_id is account-local by construction: no two accounts ever share one.
# Both therefore describe the position, not the decision, and would report the
# fill difference again through the signal table.
_POSITION_DERIVED_FEATURE_KEYS: tuple[str, ...] = ("quantity", "batch_id")
_REDUCE_ONLY_SIGNAL_KIND = "reduce_only_candidate"

# Features that carry a number.  They are stored as JSON *strings*, and two
# correct accounts do not always render the same value with the same trailing
# zeros: one writes 0.1469 as "0.1469000" and the other as
# "0.146900000000000000" (Decimal(float) expands to the full IEEE value).
# The numbers are equal, so comparing the raw text reports a divergence that
# is not there.  trim_scale drops trailing zeros while keeping the significant
# digits, and leaves integers alone.
_SIGNAL_FINGERPRINT_NUMERIC_KEYS: frozenset[str] = frozenset(
    {
        "impulse_start_price",
        "impulse_end_price",
        "impulse_return_pct",
        "breakout_level",
        "breakout_distance_pct",
        "impulse_trade_count",
        "impulse_trade_notional",
        "aggressive_buy_notional",
        "aggressive_sell_notional",
        "aggressive_imbalance",
        "baseline_notional",
        "notional_intensity",
        "liquidation_count",
        "liquidation_notional",
    }
)


def _fingerprint_feature_value_sql(key: str) -> str:
    """Render one feature for the fingerprint, normalising numeric ones.

    Numeric features are stored as JSON strings and the same value can carry
    different trailing zeros between two accounts.  Those are equal numbers, so
    the raw text must not be compared.  ``->>`` is used (not ``->``) because
    the JSON string still carries its quotes and would not cast to numeric.
    """

    if key in _SIGNAL_FINGERPRINT_NUMERIC_KEYS:
        return (
            f"trim_scale((features->>{_sql_literal(key)})::numeric)"
        )
    return f"features->{_sql_literal(key)}"


def _fingerprint_features_sql() -> str:
    """Render the ``features`` subset that feeds the durable signal fingerprint.

    ``jsonb`` normalises key order, so the rendered text is deterministic for a
    given set of values even though the stored column's key order is not.  A
    reduce-only signal drops the keys that describe the position it came from.
    Numeric features go through ``trim_scale`` so that equal values written with
    different trailing zeros compare equal.
    """

    pairs = ", ".join(
        f"{_sql_literal(key)}, {_fingerprint_feature_value_sql(key)}"
        for key in _SIGNAL_FINGERPRINT_FEATURE_KEYS
    )
    built = f"jsonb_build_object({pairs})"
    dropped = "".join(
        f" - {_sql_literal(key)}" for key in _POSITION_DERIVED_FEATURE_KEYS
    )
    return (
        f"(CASE WHEN signal_kind = {_sql_literal(_REDUCE_ONLY_SIGNAL_KIND)} "
        f"THEN {built}{dropped} ELSE {built} END)::text"
    )


def _fingerprint_reference_prices_sql() -> str:
    """Render ``reference_prices`` for the durable fingerprint.

    ``desired_notional`` is quantity times price, so it follows the position
    exactly as ``quantity`` does and is dropped for reduce-only signals.
    Numeric values are normalized through ``trim_scale`` so that equal values
    with different trailing zeros compare equal.
    """

    filtered = (
        f"CASE WHEN signal_kind = {_sql_literal(_REDUCE_ONLY_SIGNAL_KIND)} "
        "THEN reference_prices - 'desired_notional' "
        "ELSE reference_prices END"
    )
    return (
        f"(SELECT COALESCE(jsonb_object_agg(k, "
        "CASE WHEN v ~ '^-?[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?$' "
        "THEN trim_scale(v::numeric)::text ELSE v END), '{}'::jsonb) "
        f"FROM jsonb_each_text({filtered}) t(k, v))::text"
    )


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
    differences = details.get("differences")
    if isinstance(differences, Sequence) and differences:
        first_diff = differences[0]
        if isinstance(first_diff, Mapping):
            if first_diff.get("symbol"):
                return str(first_diff["symbol"])
            qd = first_diff.get("quantity_differences")
            if (
                isinstance(qd, Sequence)
                and qd
                and isinstance(qd[0], Mapping)
                and qd[0].get("symbol")
            ):
                return str(qd[0]["symbol"])
    if details.get("symbol"):
        return str(details["symbol"])
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
    if base_name == "container_memory_pressure":
        curr_mb = details.get("memory_current_mb")
        limit_mb = details.get("memory_limit_mb")
        if (
            isinstance(curr_mb, (int, float))
            and isinstance(limit_mb, (int, float))
            and limit_mb > 0
            and (curr_mb / limit_mb) < 0.6
        ):
            return "仅低频冷页置换入 Swap，物理内存充足，核心查询无延迟影响。"
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
            return f"已触发定向重启（第 {attempt} 次），等待健康检查与行情连接恢复。"
        restart_attempts = details.get("restart_attempts")
        if restart_attempts:
            return "已触发过自动重启，正在等待冷却或健康检查恢复。"
    if base_name == "live_heartbeat_auto_restarted":
        attempt = details.get("attempt")
        attempt_str = f"（第 {attempt} 次）" if attempt else ""
        return f"已执行定向重启{attempt_str}，等待健康检查与行情连接恢复。"
    if base_name == "live_heartbeat_restart_failed":
        return "自愈重启执行异常；紧急检查 Compose 权限、端口占用或数据库连接状态。"
    if base_name == "live_heartbeat_restart_suppressed":
        return "连续自愈失败已触发熔断保护；必须立即登录主机排查崩溃根因后手动恢复。"
    if base_name == "live_position_intent_divergence":
        differences = details.get("differences")
        if isinstance(differences, Sequence) and differences:
            zero_accounts: list[str] = []
            for diff in differences:
                if isinstance(diff, Mapping):
                    accounts = diff.get("accounts")
                    if isinstance(accounts, Sequence):
                        for acc in accounts:
                            if isinstance(acc, Mapping) and acc.get("order_count") == 0:
                                label = str(acc.get("account_label", ""))
                                if label and label not in zero_accounts:
                                    zero_accounts.append(label)
            if zero_accounts:
                zero_str = f"（如 {', '.join(zero_accounts)}）"
                return (
                    f"检测到单边未下单，请优先排查未下单账户{zero_str}的可用保证金、"
                    "持仓上限、对账状态或风控阻断原因；确认前暂停扩大仓位。"
                )
        return (
            "同配置账户下单参数分歧，请核对各账户委托方向、"
            "数量与价格；排查完毕前暂停扩仓。"
        )
    if base_name == "live_position_divergence":
        return (
            "核对各账户交易所实际持仓快照，确认是否存在漏平仓；确认仓位前暂停扩仓。"
        )
    if base_name == "live_signal_divergence":
        return (
            "暂停扩仓；核对各账户策略配置差异、K 线数据新鲜度及近期事件循环日志。"
        )
    if base_name == "container_memory_pressure":
        curr_mb = details.get("memory_current_mb")
        limit_mb = details.get("memory_limit_mb")
        if (
            isinstance(curr_mb, (int, float))
            and isinstance(limit_mb, (int, float))
            and limit_mb > 0
            and (curr_mb / limit_mb) < 0.6
        ):
            return "物理内存充足，属于系统冷页置换入 Swap，无需重启服务，建议继续观察。"
        return "请检查该容器近期查询占用与堆缓存，必要时调大容器内存限额。"
    if base_name == "live_market_state_delay":
        delay = details.get("delay_ms")
        delay_str = f"（当前 {delay:.0f}ms）" if isinstance(delay, (int, float)) else ""
        return f"行情延迟过高{delay_str}，检查交易所网络往返延迟（RTT）及数据库写入排队。"
    if base_name == "live_unknown_orders":
        return "严禁重发相同订单；立即按 client_order_id 查交易所确认真实状态并对齐账目。"
    return _ALERT_ACTIONS.get(
        base_name,
        "已记录告警，建议结合技术详情检查相关服务。",
    )


# The notification title is capped, and a truncated Chinese label loses its
# meaning -- "实时状态 checkpoint 已过" says nothing.  The scope is dropped
# first instead: it is repeated on the body's first line, so nothing is lost.
_SERVERCHAN_TITLE_LIMIT = 32


def _serverchan_title(severity: str, scope: str | None, label: str) -> str:
    """Build the push title, preferring a complete label over the scope."""

    head = " | ".join(["CML", severity])
    if scope:
        with_scope = f"{head} | {scope} | {label}"
        if len(with_scope) <= _SERVERCHAN_TITLE_LIMIT:
            return with_scope
        compact = f"CML[{severity}] {scope} | {label}"
        if len(compact) <= _SERVERCHAN_TITLE_LIMIT:
            return compact
        compact2 = f"[{severity}] {scope} | {label}"
        if len(compact2) <= _SERVERCHAN_TITLE_LIMIT:
            return compact2
    return f"{head} | {label}"[:_SERVERCHAN_TITLE_LIMIT]


def _trim_decimal(val: object) -> str:
    """Trim trailing zeros from decimal strings or numbers."""
    if val is None:
        return "0"
    s = str(val).strip()
    s = re.sub(r'(\.\d*?[1-9])0+$', r'\1', s)
    s = re.sub(r'\.0+$', r'', s)
    return s


def _format_order_intent_items(raw_summary: str | None) -> list[str]:
    """Parse and normalize order intent items into clean Chinese strings."""
    if not raw_summary:
        return []
    items = [x.strip() for x in re.split(r'[\x1e,]', raw_summary) if x.strip()]
    formatted: list[str] = []
    for item in items:
        # Strip trailing zeros from decimals (e.g. 684.000000000000000000 -> 684, 0.146040000000000000 -> 0.14604)
        normalized = re.sub(r'(\.\d*?[1-9])0+(?=[^\d]|$)', r'\1', item)
        normalized = re.sub(r'\.0+(?=[^\d]|$)', r'', normalized)
        if re.search(r'（(限价|市价)）$', normalized):
            formatted.append(normalized)
            continue
        m = re.match(
            r'^(BUY|SELL|买入|卖出)\s+(LIMIT|MARKET|限价|市价)?\s*(.*?)$',
            normalized,
            re.IGNORECASE,
        )
        if m:
            side_raw, type_raw, rest = m.groups()
            side = "买入" if side_raw.upper() in ("BUY", "买入") else "卖出"
            order_type = (
                "限价"
                if (type_raw and type_raw.upper() in ("LIMIT", "限价"))
                else "市价"
            )
            if "close" in rest.lower() or "平仓" in rest:
                formatted.append(f"{side} 平仓（{order_type}）")
            elif "@" in rest:
                parts = rest.split("@", 1)
                formatted.append(
                    f"{side} {parts[0].strip()} @ {parts[1].strip()}（{order_type}）"
                )
            else:
                formatted.append(f"{side} {rest.strip()}（{order_type}）")
        else:
            formatted.append(normalized)
    return formatted


def _format_alert_human_details(
    alert_name: str, details: Mapping[str, object]
) -> list[str]:
    """Format human-readable markdown summary lines for ServerChan push."""

    base_name, _scope = _split_alert_name(alert_name)
    lines: list[str] = []

    # 1. Intent divergence
    if base_name == "live_position_intent_divergence":
        differences = details.get("differences")
        if isinstance(differences, Sequence):
            for diff in differences:
                if not isinstance(diff, Mapping):
                    continue
                sym = diff.get("symbol", "")
                cfg_hash = str(diff.get("strategy_config_hash", ""))
                short_cfg = cfg_hash[:8] if cfg_hash else "未知"
                lines.append(f"- **分叉标的**：`{sym}`（策略配置: `{short_cfg}`）")
                accs = diff.get("accounts")
                if isinstance(accs, Sequence) and accs:
                    lines.append("")
                    lines.append("| 账户 | 委托状态 | 委托详情 |")
                    lines.append("| :--- | :---: | :--- |")
                    for acc in accs:
                        if not isinstance(acc, Mapping):
                            continue
                        acc_lbl = acc.get("account_label", "")
                        cnt = acc.get("order_count", 0)
                        summary = acc.get("intent_summary")
                        if cnt == 0:
                            lines.append(f"| `{acc_lbl}` | **未下单** (0 笔) | *(无委托)* |")
                        else:
                            order_items = _format_order_intent_items(summary)
                            order_str = "<br>".join(order_items) if order_items else "未知"
                            lines.append(
                                f"| `{acc_lbl}` | 已下单 (**{cnt}** 笔) | {order_str} |"
                            )
                    lines.append("")

    # 2. Position divergence
    elif base_name == "live_position_divergence":
        differences = details.get("differences")
        if isinstance(differences, Sequence):
            for diff in differences:
                if not isinstance(diff, Mapping):
                    continue
                accs = diff.get("accounts")
                cfg_hash = str(diff.get("strategy_config_hash", ""))
                short_cfg = cfg_hash[:8] if cfg_hash else "未知"
                qds = diff.get("quantity_differences")
                if isinstance(qds, Sequence):
                    for qd in qds:
                        if not isinstance(qd, Mapping):
                            continue
                        sym = qd.get("symbol", "")
                        side = str(qd.get("position_side", "")).upper()
                        side_label = (
                            "多头 (LONG)"
                            if side == "LONG"
                            else ("空头 (SHORT)" if side == "SHORT" else side)
                        )
                        left_q = _trim_decimal(qd.get("left_quantity", "0"))
                        right_q = _trim_decimal(qd.get("right_quantity", "0"))
                        left_acc = (
                            accs[0]
                            if isinstance(accs, Sequence) and len(accs) > 0
                            else "账户1"
                        )
                        right_acc = (
                            accs[1]
                            if isinstance(accs, Sequence) and len(accs) > 1
                            else "账户2"
                        )
                        lines.append(
                            f"- **分叉标的**：`{sym}`（方向: {side_label}，策略配置: `{short_cfg}`）"
                        )
                        lines.append("")
                        lines.append("| 账户 | 方向 | 实际持仓 |")
                        lines.append("| :--- | :---: | :---: |")
                        lines.append(f"| `{left_acc}` | {side} | **{left_q}** |")
                        lines.append(f"| `{right_acc}` | {side} | **{right_q}** |")
                        lines.append("")

    # 3. Signal divergence
    elif base_name == "live_signal_divergence":
        differences = details.get("differences")
        if isinstance(differences, Sequence):
            for diff in differences:
                if not isinstance(diff, Mapping):
                    continue
                sym = diff.get("symbol", "")
                raw_bucket = diff.get("bucket_start", "")
                formatted_bucket = (
                    _format_alert_time(raw_bucket) if raw_bucket else "未知"
                )
                cfg_hash = str(diff.get("strategy_config_hash", ""))
                short_cfg = cfg_hash[:8] if cfg_hash else "未知"
                lines.append(
                    f"- **分叉标的**：`{sym}`（时间桶: `{formatted_bucket}`，策略配置: `{short_cfg}`）"
                )
                accs = diff.get("accounts")
                if isinstance(accs, Sequence) and accs:
                    lines.append("")
                    lines.append("| 账户 | 信号 / 候选 | 决策指纹 |")
                    lines.append("| :--- | :---: | :---: |")
                    for acc in accs:
                        if not isinstance(acc, Mapping):
                            continue
                        acc_lbl = acc.get("account_label", "")
                        sig_cnt = acc.get("signal_count", 0)
                        cand_cnt = acc.get("candidate_count", 0)
                        fp = acc.get("fingerprint")
                        fp_str = f"`{str(fp)[:8]}`" if fp else "-"
                        lines.append(
                            f"| `{acc_lbl}` | **{sig_cnt}** / {cand_cnt} | {fp_str} |"
                        )
                    lines.append("")

    # 4. Container memory pressure (swap)
    elif base_name == "container_memory_pressure":
        curr_mb = details.get("memory_current_mb")
        limit_mb = details.get("memory_limit_mb")
        swap_mb = details.get("memory_swap_current_mb")
        swap_growth = details.get("memory_swap_growth_mb")
        peak_mb = details.get("memory_peak_mb")
        if (
            isinstance(curr_mb, (int, float))
            and isinstance(limit_mb, (int, float))
            and limit_mb > 0
        ):
            pct = (curr_mb / limit_mb) * 100
            peak_str = (
                f"，峰值 {peak_mb:.1f} MB"
                if isinstance(peak_mb, (int, float))
                else ""
            )
            lines.append(
                f"- **物理内存用量**：`{curr_mb:.1f} MB` / `{limit_mb:.1f} MB`"
                f"（占比 **{pct:.1f}%**{peak_str}）"
            )
        if isinstance(swap_mb, (int, float)):
            growth_str = (
                f"（本次新增: `+{swap_growth:.1f} MB`）"
                if isinstance(swap_growth, (int, float)) and swap_growth > 0
                else ""
            )
            lines.append(
                f"- **Swap 换出情况**：当前换出 `{swap_mb:.1f} MB` {growth_str}"
            )

    # 5. Container memory high / growth / rss_growth
    elif base_name in (
        "container_memory_high",
        "container_memory_growth",
        "rss_growth",
    ):
        svc = details.get("service") or details.get("account_label") or ""
        curr_mb = details.get("memory_current_mb") or details.get("rss_mb")
        limit_mb = details.get("memory_limit_mb")
        growth_mb = details.get("memory_growth_mb") or details.get("growth_mb")
        if svc:
            lines.append(f"- **影响服务**：`{svc}`")
        if (
            isinstance(curr_mb, (int, float))
            and isinstance(limit_mb, (int, float))
            and limit_mb > 0
        ):
            pct = (curr_mb / limit_mb) * 100
            lines.append(
                f"- **内存用量**：`{curr_mb:.1f} MB` / `{limit_mb:.1f} MB`"
                f"（占比 **{pct:.1f}%**）"
            )
        elif isinstance(curr_mb, (int, float)):
            lines.append(f"- **内存用量**：`{curr_mb:.1f} MB`")
        if isinstance(growth_mb, (int, float)):
            lines.append(f"- **增长幅度**：窗口内持续增长 `+{growth_mb:.1f} MB`")

    # 6. Heartbeat & Live session
    elif base_name in (
        "live_heartbeat_stale",
        "live_heartbeat_auto_restarted",
        "live_heartbeat_restart_failed",
        "live_heartbeat_restart_suppressed",
    ):
        acc = details.get("account_label", "")
        age = details.get("heartbeat_age_seconds")
        attempt = details.get("attempt")
        if acc:
            lines.append(f"- **责任账户**：`{acc}`")
        if isinstance(age, (int, float)):
            lines.append(f"- **心跳中断时长**：已失联 **{age:.0f} 秒**")
        if attempt:
            lines.append(f"- **自愈进度**：已执行定向自动重启（第 **{attempt}** 次）")

    # 7. Market delay & stale
    elif base_name == "live_market_state_delay":
        acc = details.get("account_label", "")
        delay = details.get("delay_ms")
        warn_th = details.get("warning_threshold_ms")
        if acc:
            lines.append(f"- **受影响账户**：`{acc}`")
        if isinstance(delay, (int, float)):
            warn_str = (
                f"（警戒阈值: {warn_th:.0f} ms）"
                if isinstance(warn_th, (int, float))
                else ""
            )
            lines.append(f"- **行情滞后延迟**：**{delay:.0f} ms**{warn_str}")

    # 8. Unknown orders
    elif base_name == "live_unknown_orders":
        acc = details.get("account_label", "")
        cnt = details.get("unknown_order_count")
        oldest = details.get("oldest_unknown_order_age_seconds")
        if acc:
            lines.append(f"- **异常账户**：`{acc}`")
        if isinstance(cnt, (int, float)):
            lines.append(f"- **在途失步订单**：共 **{cnt}** 笔")
        if isinstance(oldest, (int, float)):
            lines.append(f"- **最长挂起时间**：**{oldest:.0f} 秒**")

    # 9. Container unhealthy / missing / oom
    elif base_name in (
        "container_unhealthy",
        "container_missing",
        "container_oom_killed",
    ):
        svc = details.get("service", "")
        cid = str(details.get("container_id", ""))
        health = details.get("health")
        restart_cnt = details.get("restart_count")
        if svc:
            lines.append(
                f"- **异常服务容器**：`{svc}`"
                + (f" (`{cid[:12]}`)" if cid else "")
            )
        if health:
            lines.append(f"- **健康状态**：`{health}`")
        if restart_cnt is not None:
            lines.append(f"- **已重启次数**：**{restart_cnt}** 次")

    # 10. Account lifecycle & reconciliation
    elif base_name == "live_account_lifecycle_not_ready":
        acc = details.get("account_label", "")
        state = details.get("state", "")
        age_human = details.get("age_human", "")
        thresh_human = details.get("threshold_human", "")
        if acc:
            lines.append(f"- **责任账户**：`{acc}`")
        if state:
            lines.append(f"- **生命周期状态**：`{state}`")
        if age_human:
            thresh_str = f"（安全阈值: {thresh_human}）" if thresh_human else ""
            lines.append(f"- **异常停滞时间**：已停滞 **{age_human}**{thresh_str}")

    elif base_name == "live_account_reconciliation_stale":
        acc = details.get("account_label", "")
        status = details.get("status", "")
        age_human = details.get("age_human", "")
        thresh_human = details.get("threshold_human", "")
        if acc:
            lines.append(f"- **责任账户**：`{acc}`")
        if status:
            lines.append(f"- **对账状态**：`{status}`")
        if age_human:
            thresh_str = f"（安全阈值: {thresh_human}）" if thresh_human else ""
            lines.append(f"- **对账停滞时长**：距上次对账已 **{age_human}**{thresh_str}")

    # 11. Market state stale & checkpoint stale
    elif base_name == "live_market_state_stale":
        acc = details.get("account_label", "")
        age_human = details.get("age_human", "")
        thresh_human = details.get("threshold_human", "")
        if acc:
            lines.append(f"- **责任账户**：`{acc}`")
        if age_human:
            thresh_str = f"（安全阈值: {thresh_human}）" if thresh_human else ""
            lines.append(f"- **行情停滞时长**：已中断 **{age_human}**{thresh_str}")

    elif base_name == "live_checkpoint_stale":
        acc = details.get("account_label", "")
        age_human = details.get("age_human", "")
        thresh_human = details.get("threshold_human", "")
        if acc:
            lines.append(f"- **责任账户**：`{acc}`")
        if age_human:
            thresh_str = f"（安全阈值: {thresh_human}）" if thresh_human else ""
            lines.append(f"- **状态停滞时长**：距上次写入已 **{age_human}**{thresh_str}")

    # 12. Live session not ready
    elif base_name == "live_session_not_ready":
        acc = details.get("account_label", "")
        s_ready = details.get("session_state_ready")
        l_active = details.get("lease_active")
        c_present = details.get("checkpoint_present")
        if acc:
            lines.append(f"- **责任账户**：`{acc}`")
        if s_ready is not None:
            lines.append(f"- **会话状态**：{'就绪' if s_ready else '未就绪（异常）'}")
        if l_active is not None:
            lines.append(f"- **分布式租约**：{'有效' if l_active else '失效（异常）'}")
        if c_present is not None:
            lines.append(f"- **策略检查点**：{'存在' if c_present else '缺失（异常）'}")

    # 13. Telemetry & legacy order conflict & market tasks
    elif base_name == "telemetry_persist_failure":
        failures = details.get("failure_count")
        if failures:
            lines.append(f"- **落库失败批次**：**{failures}** 次")

    elif base_name == "live_legacy_order_identity_conflict":
        conflicts = details.get("conflict_count")
        if conflicts:
            lines.append(f"- **冲突记录数**：**{conflicts}** 笔")

    elif base_name == "market_task_not_alive":
        group_ids = details.get("group_ids")
        if isinstance(group_ids, Sequence) and group_ids:
            tasks_str = ", ".join(f"`{g}`" for g in group_ids)
            lines.append(f"- **异常连接任务**：{tasks_str}")

    # 14. Operational errors & database failures
    elif base_name in (
        "database_check_failed",
        "live_consistency_check_failed",
        "ops_monitor_failed",
    ):
        acc = details.get("account_label")
        err_type = details.get("error_type")
        err_msg = str(details.get("error") or "")
        if acc:
            lines.append(f"- **责任账户**：`{acc}`")
        if err_type:
            lines.append(f"- **异常类型**：`{err_type}`")
        if err_msg:
            clean_err = " ".join(err_msg.split())[:120]
            lines.append(f"- **异常摘要**：`{clean_err}`")

    # 15. Database advisory warnings
    elif base_name == "database_parallel_maintenance_enabled":
        workers = details.get("max_parallel_maintenance_workers")
        if workers is not None:
            lines.append(f"- **当前工作进程数**：`{workers}`（建议配置为 0 或 1）")

    return lines


def _alert_conclusion(
    alert_name: str, details: Mapping[str, object]
) -> str | None:
    """Provide a one-line executive takeaway for the alert header."""

    base_name, _scope = _split_alert_name(alert_name)
    if base_name == "container_memory_pressure":
        curr_mb = details.get("memory_current_mb")
        limit_mb = details.get("memory_limit_mb")
        if (
            isinstance(curr_mb, (int, float))
            and isinstance(limit_mb, (int, float))
            and limit_mb > 0
        ):
            pct = (curr_mb / limit_mb) * 100
            if pct < 60:
                return (
                    f"物理内存充足（仅占 {pct:.1f}%），系 Linux 内核置换低频冷页入 Swap，"
                    "**服务运行正常，无需人工干预**。"
                )
            return (
                f"物理内存占用偏高（{pct:.1f}%）且持续换出，**建议关注内存增长趋势与慢查询**。"
            )
    if base_name == "container_memory_high":
        curr_mb = details.get("memory_current_mb")
        limit_mb = details.get("memory_limit_mb")
        if (
            isinstance(curr_mb, (int, float))
            and isinstance(limit_mb, (int, float))
            and limit_mb > 0
        ):
            pct = (curr_mb / limit_mb) * 100
            return f"容器内存占用已达 {pct:.1f}%，**接近限额，存在触发 OOM 崩溃风险**。"
        return "容器内存占用已达到警戒水位，**存在触发 OOM 崩溃风险**。"
    if base_name in ("container_memory_growth", "rss_growth"):
        growth_mb = details.get("memory_growth_mb") or details.get("growth_mb")
        if isinstance(growth_mb, (int, float)):
            return (
                f"容器内存在监控窗口内持续净增长 +{growth_mb:.1f} MB，"
                "**需排查内存泄漏或缓存积压**。"
            )
        return "容器内存呈现持续增长趋势，**需排查内存泄漏或缓存积压**。"
    if base_name == "live_position_intent_divergence":
        differences = details.get("differences")
        if isinstance(differences, Sequence) and differences:
            for diff in differences:
                if isinstance(diff, Mapping):
                    accounts = diff.get("accounts")
                    if isinstance(accounts, Sequence):
                        for acc in accounts:
                            if isinstance(acc, Mapping) and acc.get("order_count") == 0:
                                return "检测到单边漏单（主/从账户下单意图分叉），**执行与风控路径已失步**。"
        return "同配置账户向交易所下达了不同的订单参数，**下单意图已分叉**。"
    if base_name == "live_position_divergence":
        return "可比账户在交易所的实际持仓数量不一致，**存在单边未平仓或对账失步风险**。"
    if base_name == "live_signal_divergence":
        return "同配置账户信号指纹不一致，**策略计算已失步**。"
    if base_name in ("container_oom_killed",):
        return "容器超出内存限制配额，**已被系统内核强制终止**。"
    if base_name in ("container_missing",):
        return "核心服务容器未运行或已异常退出，**相关功能已中断**。"
    if base_name in ("container_unhealthy",):
        return "容器健康检查持续失败，**服务可能处于假死或死锁状态**。"
    if base_name == "live_market_state_delay":
        delay = details.get("delay_ms")
        delay_str = f"（当前 {delay:.0f}ms）" if isinstance(delay, (int, float)) else ""
        return f"行情接收严重滞后{delay_str}，**存在信号失效与成交滑点风险**。"
    if base_name == "live_market_state_stale":
        return "行情数据推进中断，**策略已暂停基于实时 K 线的交易计算**。"
    if base_name == "live_checkpoint_stale":
        return "策略持久化状态已过期，**若发生异常退出可能丢失最新运行时状态**。"
    if base_name == "live_session_not_ready":
        return "实时交易会话、租约或状态未就绪，**策略无法进入安全交易状态**。"
    if base_name == "live_account_lifecycle_not_ready":
        state = details.get("state", "")
        state_str = f"（状态: {state}）" if state else ""
        return f"账户进程生命周期未就绪{state_str}，**交易执行已挂起**。"
    if base_name == "live_account_reconciliation_stale":
        status = details.get("status", "")
        status_str = f"（状态: {status}）" if status else ""
        return f"交易所对账快照已失步{status_str}，**持仓和订单真实性暂无法核实**。"
    if base_name == "live_unknown_orders":
        cnt = details.get("unknown_order_count", 0)
        cnt_str = f"（共 {cnt} 笔）" if cnt else ""
        return f"发现未确认在途订单{cnt_str}，**本地与交易所订单失步，严禁盲目重发**。"
    if base_name == "market_task_not_alive":
        return "行情 WebSocket 连接任务挂死，**部分币种实时行情已中断**。"
    if base_name == "telemetry_persist_failure":
        return "数据库遥测批次批量落库失败，**监控与运行诊断数据存在丢失风险**。"
    if base_name == "live_legacy_order_identity_conflict":
        return "检测到本地订单 ID 重复关联交易所订单，**订单生命周期冲突**。"
    if base_name == "live_heartbeat_stale":
        age = details.get("heartbeat_age_seconds")
        age_str = f"（已失联 {age:.0f} 秒）" if isinstance(age, (int, float)) else ""
        return f"策略主循环心跳停滞{age_str}，**交易与行情推进可能已挂死**。"
    if base_name == "live_heartbeat_auto_restarted":
        attempt = details.get("attempt")
        attempt_str = f"（第 {attempt} 次）" if attempt else ""
        return f"策略心跳超时，**已自动执行定向重启自愈{attempt_str}**。"
    if base_name == "live_heartbeat_restart_failed":
        return "策略自动重启自愈执行失败，**需要紧急人工介入排查**。"
    if base_name == "live_heartbeat_restart_suppressed":
        return "策略连续自动重启已达上限，**自愈保护熔断，交易已挂起**。"
    if base_name == "live_crash_log_archive_failed":
        return "策略崩溃日志转储失败，**重启前现场日志可能未完整留存**。"
    if base_name == "database_check_failed":
        return "PostgreSQL 状态检查查询失败，**暂无法确认数据库读写是否健康**。"
    if base_name == "database_query_stats_unavailable":
        return "pg_stat_statements 扩展未载入，**慢查询与 SQL 分析受限（不影响交易）**。"
    if base_name == "database_io_timing_disabled":
        return "PostgreSQL I/O 耗时跟踪未开启，**数据库磁盘 I/O 延迟定位能力受限**。"
    if base_name == "database_parallel_maintenance_enabled":
        return "数据库并行维护工作进程超过护栏，**高并发时可能争用交易资源**。"
    if base_name == "live_consistency_check_failed":
        return "跨账户一致性校验查询超时或失败，**多账户运行状态暂无法比对**。"
    if base_name == "ops_monitor_failed":
        return "运维监控自身主循环发生未捕获异常，**请检查监控进程日志**。"
    return None


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
        icon = "🚨" if severity == "严重" else ("⚠️" if severity == "警告" else "ℹ️")
        title = _serverchan_title(severity, scope, label)
        conclusion = _alert_conclusion(alert_name, details)
        body = [
            f"## {icon} [{severity}] {scope + '：' if scope else ''}{label}",
        ]
        if conclusion:
            body.append(f"> **诊断结论**：{conclusion}\n")
        body.append(
            f"- **发生时间**："
            f"{_format_alert_time(payload.get('observed_at'))}（北京时间）"
        )
        human_lines = _format_alert_human_details(alert_name, details)
        if human_lines:
            body.extend(human_lines)
        if not conclusion:
            impact = _alert_impact(alert_name, details)
            if impact:
                body.append(f"- **影响**：{impact}")
        body.append(f"- **处置建议**：{_alert_action(alert_name, details)}")
        body.append(f"- **事件编号**：`{alert_name}`")
    else:
        title = _serverchan_title("恢复", scope, label)
        body = [
            f"## 🟢 [恢复] {scope + '：' if scope else ''}{label}",
            "- **恢复时间**："
            f"{_format_alert_time(payload.get('observed_at'))}（北京时间）",
            f"- **持续时间**：{_format_duration(payload.get('duration_seconds'))}",
            "- **当前状态**：监控已恢复，后续将继续观察。",
            f"- **原告警编号**：`{alert_name}`",
        ]
    return {
        "title": " ".join(title.split()),
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
