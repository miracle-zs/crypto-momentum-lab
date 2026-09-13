from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

import crypto_momentum_lab.live_rollout.runtime_cache as runtime_cache
from crypto_momentum_lab.live_rollout.runtime_cache import (
    LiveRuntimeCacheMaintenance,
)

NOW = datetime(2026, 7, 3, 23, 59, tzinfo=UTC)


class _Strategy:
    buffered_symbol_count = 3
    buffered_state_count = 12

    def __init__(self) -> None:
        self.protected: frozenset[str] | None = None
        self.prune_calls = 0

    def cache_protected_symbols(self) -> frozenset[str]:
        return frozenset({"cooldown"})

    def prune_inactive_symbols(self, **kwargs: Any) -> tuple[str, ...]:
        self.prune_calls += 1
        self.protected = frozenset(kwargs["protected_symbols"])
        return ("STALE",)


class _Telemetry:
    sample_series_count = 7

    def __init__(self) -> None:
        self.protected: frozenset[str] | None = None
        self.prune_calls = 0

    def prune_inactive_symbols(self, **kwargs: Any) -> int:
        self.prune_calls += 1
        self.protected = frozenset(kwargs["protected_symbols"])
        return 1


def test_runtime_cache_maintains_protection_set_and_interval() -> None:
    strategy = _Strategy()
    telemetry = _Telemetry()
    maintenance = LiveRuntimeCacheMaintenance(
        run_id="run-1",
        strategy=strategy,
        telemetry=telemetry,
        pending_entry_symbols=lambda: {"pendingusdt"},
    )

    maintenance.prune(now=NOW, current_symbol="BTCUSDT")
    assert strategy.prune_calls == 0

    maintenance.update_managed_symbols(
        position_symbols={"BTCUSDT"},
        order_symbols={"ETHUSDT"},
    )
    maintenance.prune(
        now=NOW,
        current_symbol="solusdt",
        active_symbols={"adausdt"},
    )

    expected = {
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "ADAUSDT",
        "PENDINGUSDT",
        "COOLDOWN",
    }
    assert strategy.protected == frozenset(expected)
    assert telemetry.protected == frozenset(expected)
    assert strategy.prune_calls == 1
    assert telemetry.prune_calls == 1

    maintenance.prune(
        now=NOW + timedelta(seconds=30),
        current_symbol="solusdt",
    )
    assert strategy.prune_calls == 1

    maintenance.prune(
        now=NOW + timedelta(minutes=1),
        current_symbol="solusdt",
    )
    assert strategy.prune_calls == 2
    assert telemetry.prune_calls == 2


def test_runtime_cache_logs_memory_and_cache_snapshot() -> None:
    records: list[tuple[str, dict[str, object]]] = []

    class FakeLog:
        def info(self, event: str, **fields: object) -> None:
            records.append((event, fields))

    strategy = _Strategy()
    telemetry = _Telemetry()
    maintenance = LiveRuntimeCacheMaintenance(
        run_id="run-1",
        strategy=strategy,
        telemetry=telemetry,
        pending_entry_symbols=lambda: (),
    )
    maintenance.update_managed_symbols(position_symbols=(), order_symbols=())
    with (
        patch.object(runtime_cache, "log", FakeLog()),
        patch.object(runtime_cache, "current_rss_bytes", lambda: 1000),
        patch.object(
            runtime_cache,
            "cgroup_memory_snapshot",
            lambda: {
                "cgroup_memory_current_bytes": 1100,
                "cgroup_memory_limit_bytes": 2200,
            },
        ),
        patch.object(
            runtime_cache,
            "tracemalloc_memory_snapshot",
            lambda: {
                "tracemalloc_enabled": True,
                "tracemalloc_current_bytes": 200,
                "tracemalloc_peak_bytes": 300,
            },
        ),
    ):
        maintenance.prune(now=NOW, current_symbol="BTCUSDT")

    event, fields = records[0]
    assert event == "live_runtime_memory_snapshot"
    assert fields["rss_bytes"] == 1000
    assert fields["cgroup_memory_current_bytes"] == 1100
    assert fields["cgroup_memory_limit_bytes"] == 2200
    assert fields["tracemalloc_current_bytes"] == 200
    assert fields["tracemalloc_peak_bytes"] == 300
    assert fields["buffered_symbol_count"] == 3
    assert fields["buffered_state_count"] == 12
    assert fields["telemetry_sample_series_count"] == 7
