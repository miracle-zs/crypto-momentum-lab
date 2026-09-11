from datetime import UTC, datetime, timedelta
from typing import Any

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
