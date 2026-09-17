"""Cold-cache protection and pruning for live runtime derived state."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable
from datetime import datetime, timedelta

import structlog

from crypto_momentum_lab.health.memory import (
    cgroup_memory_snapshot,
    current_rss_bytes,
    tracemalloc_memory_snapshot,
)

log = structlog.get_logger()

_CACHE_MAINTENANCE_INTERVAL = timedelta(minutes=1)
_STRATEGY_CACHE_INACTIVE_AFTER = timedelta(minutes=15)


class LiveRuntimeCacheMaintenance:
    """Protect active symbols while pruning cold strategy/telemetry state."""

    def __init__(
        self,
        *,
        run_id: str,
        strategy: object,
        telemetry: object | None,
        pending_entry_symbols: Callable[[], Iterable[str]],
        volume_metrics_provider: Callable[[], dict[str, object]] | None = None,
    ) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        self._run_id = run_id
        self._strategy = strategy
        self._telemetry = telemetry
        self._pending_entry_symbols = pending_entry_symbols
        self._volume_metrics_provider = volume_metrics_provider
        self._managed_position_symbols: frozenset[str] = frozenset()
        self._managed_order_symbols: frozenset[str] = frozenset()
        self._known = False
        self._last_maintenance_at: datetime | None = None

    def update_managed_symbols(
        self,
        *,
        position_symbols: Collection[str],
        order_symbols: Collection[str],
    ) -> None:
        self._managed_position_symbols = frozenset(
            _normalize_symbols(position_symbols)
        )
        self._managed_order_symbols = frozenset(
            _normalize_symbols(order_symbols)
        )
        self._known = True

    def prune(
        self,
        *,
        now: datetime,
        current_symbol: str,
        active_symbols: Collection[str] | None = None,
    ) -> None:
        """Prune at most once per interval without blocking the decision loop."""

        if not self._known:
            return
        previous = self._last_maintenance_at
        if (
            previous is not None
            and now - previous < _CACHE_MAINTENANCE_INTERVAL
        ):
            return

        protected = set(self._managed_position_symbols)
        protected.update(self._managed_order_symbols)
        protected.update(_normalize_symbols((current_symbol,)))
        if active_symbols is not None:
            protected.update(_normalize_symbols(active_symbols))
        protected.update(_normalize_symbols(self._pending_entry_symbols()))

        strategy_protected = getattr(
            self._strategy,
            "cache_protected_symbols",
            None,
        )
        if callable(strategy_protected):
            protected.update(_normalize_symbols(strategy_protected()))
        protected_symbols = frozenset(protected)

        strategy_prune = getattr(
            self._strategy,
            "prune_inactive_symbols",
            None,
        )
        evicted_strategy_symbols: tuple[str, ...] = ()
        if callable(strategy_prune):
            evicted_strategy_symbols = strategy_prune(
                now=now,
                protected_symbols=protected_symbols,
                inactive_after=_STRATEGY_CACHE_INACTIVE_AFTER,
            )

        evicted_telemetry_series = 0
        self._last_maintenance_at = now
        volume_metrics: dict[str, object] = {}
        if self._volume_metrics_provider is not None:
            try:
                volume_metrics = self._volume_metrics_provider() or {}
            except Exception:
                volume_metrics = {}

        log.info(
            "live_runtime_memory_snapshot",
            run_id=self._run_id,
            rss_bytes=current_rss_bytes(),
            **cgroup_memory_snapshot(),
            **tracemalloc_memory_snapshot(),
            protected_symbol_count=len(protected_symbols),
            evicted_strategy_symbols=len(evicted_strategy_symbols),
            buffered_symbol_count=getattr(
                self._strategy,
                "buffered_symbol_count",
                None,
            ),
            buffered_state_count=getattr(
                self._strategy,
                "buffered_state_count",
                None,
            ),
            volume_cached_symbols=volume_metrics.get("cached_symbol_count"),
            volume_total_snapshots=volume_metrics.get("total_snapshot_count"),
            volume_oldest_age_seconds=volume_metrics.get("oldest_snapshot_age_seconds"),
            volume_hit_count=volume_metrics.get("lookup_hit_count"),
            volume_miss_count=volume_metrics.get("lookup_miss_count"),
        )
        if evicted_strategy_symbols:
            log.info(
                "live_runtime_cache_pruned",
                run_id=self._run_id,
                protected_symbol_count=len(protected_symbols),
                evicted_strategy_symbols=len(evicted_strategy_symbols),
                buffered_symbol_count=getattr(
                    self._strategy,
                    "buffered_symbol_count",
                    None,
                ),
                buffered_state_count=getattr(
                    self._strategy,
                    "buffered_state_count",
                    None,
                ),
            )


def _normalize_symbols(symbols: Iterable[str]) -> set[str]:
    return {
        symbol.strip().upper()
        for symbol in symbols
        if symbol.strip()
    }


__all__ = ["LiveRuntimeCacheMaintenance"]
