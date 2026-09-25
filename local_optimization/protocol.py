"""Optimization protocol schema, parameter domain, and deterministic hashing.

Every experiment run is bound to an immutable OptimizationProtocol.
Any change in parameters, constraints, capital, costs, or window rules
generates a distinct protocol_id (SHA-256).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

CANONICAL_DIMS_8D = [
    "impulse_window_buckets",
    "confirmation_buckets",
    "min_return_pct",
    "min_imbalance",
    "min_intensity",
    "min_volume_ratio",
    "cooldown_buckets",
    "max_open_positions",
]


def canonicalize_candidate_params(
    params: dict[str, Any],
    default_max_open_positions: int | None = None,
) -> dict[str, Any]:
    """Normalize candidate parameter dictionary to canonical 8D schema.

    Translates legacy bar/bps field names, normalizes float precisions,
    and enforces integer types to guarantee deterministic hashing.
    """
    key_mapping = {
        "impulse_window_bars": "impulse_window_buckets",
        "confirmation_window_bars": "confirmation_buckets",
        "min_directional_return_bps": "min_return_pct",
        "imbalance_threshold": "min_imbalance",
        "min_notional_intensity": "min_intensity",
        "symbol_cooldown_bars": "cooldown_buckets",
        "concurrency_slots": "max_open_positions",
    }
    canon: dict[str, Any] = {}
    for k, v in params.items():
        canonical_k = key_mapping.get(k, k)
        if k == "min_directional_return_bps" and canonical_k == "min_return_pct":
            try:
                val_f = float(v)
                canon[canonical_k] = (
                    round(val_f / 100.0, 4) if val_f >= 5.0 else round(val_f, 4)
                )
            except (ValueError, TypeError):
                canon[canonical_k] = v
        elif canonical_k in {
            "impulse_window_buckets",
            "confirmation_buckets",
            "cooldown_buckets",
            "max_open_positions",
        }:
            try:
                canon[canonical_k] = int(v)
            except (ValueError, TypeError):
                canon[canonical_k] = v
        elif canonical_k in {"min_return_pct", "min_imbalance"}:
            try:
                canon[canonical_k] = round(float(v), 4)
            except (ValueError, TypeError):
                canon[canonical_k] = v
        elif canonical_k in {"min_intensity", "min_volume_ratio"}:
            try:
                canon[canonical_k] = round(float(v), 2)
            except (ValueError, TypeError):
                canon[canonical_k] = v
        else:
            canon[canonical_k] = v

    if default_max_open_positions is not None and "max_open_positions" not in canon:
        canon["max_open_positions"] = int(default_max_open_positions)

    return canon


@dataclass(frozen=True)
class ParameterCandidate:
    """A specific parameter assignment."""

    params: dict[str, Any]
    parameter_id: str

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> ParameterCandidate:
        # Standardize keys and values into a canonical JSON string for hashing
        canon_params = canonicalize_candidate_params(params)
        canon = json.dumps(
            canon_params, sort_keys=True, separators=(",", ":"), default=str
        )
        param_id = hashlib.sha256(canon.encode("utf-8")).hexdigest()[:16]
        return cls(params=canon_params, parameter_id=param_id)


@dataclass(frozen=True)
class OptimizationProtocol:
    """Standardized experiment protocol."""

    scenario_family: str
    protocol_version: str = "v1.0"
    initial_equity: float = 1000.0
    entry_notional_usdt: float = 100.0
    leverage: float = 5.0
    fee_rate_bps: float = 4.0
    max_initial_margin_usdt: float | None = 280.0
    max_allowed_mdd_pct: float | None = 0.20
    max_allowed_ui: float | None = 0.10
    min_trades: int = 30
    near_optimal_delta_usdt: float = 30.0
    near_optimal_delta_log_growth: float = 0.005
    window_rule: str = "fixed_start"  # fixed_start | rolling_30d | rolling_90d
    sizing_mode: str = "fixed"  # fixed | daily_ratio | risk_adaptive
    sizing_fraction_f: float = 0.10
    margin_ratio_cap: float | None = 0.28
    selection_strategy: str = "ui_min"  # ui_min | calmar_stability | compounding
    parameter_grid: dict[str, list[Any]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def protocol_id(self) -> str:
        """Deterministic SHA-256 hash identifying this exact protocol configuration."""
        data = asdict(self)
        canon = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:24]

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["protocol_id"] = self.protocol_id
        return d

    def save_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, sort_keys=True)


def default_orderflow_protocol(
    scenario_family: str = "margin280-free-cooldown",
    max_initial_margin_usdt: float | None = 280.0,
    sizing_mode: str = "fixed",
    sizing_fraction_f: float = 0.10,
    margin_ratio_cap: float | None = 0.28,
    selection_strategy: str = "ui_min",
) -> OptimizationProtocol:
    """Default 8-dimensional order-flow protocol matching production baselines."""
    return OptimizationProtocol(
        scenario_family=scenario_family,
        initial_equity=1000.0,
        entry_notional_usdt=100.0,
        leverage=5.0,
        max_initial_margin_usdt=max_initial_margin_usdt,
        sizing_mode=sizing_mode,
        sizing_fraction_f=sizing_fraction_f,
        margin_ratio_cap=margin_ratio_cap,
        selection_strategy=selection_strategy,
        parameter_grid={
            "impulse_window_buckets": [2, 3, 4, 5],
            "confirmation_buckets": [1, 2, 3],
            "min_return_pct": [0.25, 0.50, 0.75, 1.00, 1.50],
            "min_imbalance": [0.30, 0.40, 0.50, 0.60],
            "min_intensity": [1.0, 1.5, 2.0, 3.0, 4.0],
            "min_volume_ratio": [0.0, 1.25, 1.5, 1.75, 2.0],
            "cooldown_buckets": [0, 2, 4, 8],
            "max_open_positions": [1, 2, 3, 4],
        },
    )
