"""Adaptive two-stage coarse-to-fine grid generator for local parameter optimization.

Reduces the 7-dimensional search space from 25,200 combinations to ~1,500 - 2,500
by:
1. Stage 1 (Coarse Grid): Stride-sampling each dimension's values to quickly
   identify profitable, margin-compliant convex basins.
2. Stage 2 (Focal Expansion): Locally interpolating 1-step neighbors around the
   top-performing coarse candidates to achieve fine-grained precision where it
   matters, bypassing unviable parameter dead zones.
"""

from __future__ import annotations

import itertools
from collections.abc import Sequence
from typing import Any

from local_optimization.protocol import ParameterCandidate

DEFAULT_GRID_VALUES: dict[str, list[Any]] = {
    "impulse_window_buckets": [1, 2, 3, 4],
    "confirmation_buckets": [1, 2, 3],
    "min_return_pct": [0.3, 0.5, 0.8, 1.2, 1.5, 2.0],
    "min_imbalance": [0.15, 0.20, 0.30, 0.40, 0.50],
    "min_intensity": [1.0, 1.5, 2.5, 4.0, 6.0],
    "min_volume_ratio": [0.0, 1.0, 1.5, 2.0],
    "cooldown_buckets": [0],
}

DIMS = list(DEFAULT_GRID_VALUES.keys())


def generate_coarse_grid(
    grid_values: dict[str, list[Any]] | None = None,
    *,
    stride: int = 2,
) -> list[dict[str, Any]]:
    """Sample coarse grid values with boundary preservation.

    For each dimension, picks every `stride`-th index, always guaranteeing that
    both the minimum (index 0) and maximum (last index) are included.
    """
    values = grid_values or DEFAULT_GRID_VALUES
    coarse_values: dict[str, list[Any]] = {}

    for dim, vals in values.items():
        if len(vals) <= 2:
            coarse_values[dim] = list(vals)
            continue
        # Pick stride indices, plus ensure last index is included
        indices = list(range(0, len(vals), stride))
        last_idx = len(vals) - 1
        if last_idx not in indices:
            indices.append(last_idx)
        indices.sort()
        coarse_values[dim] = [vals[i] for i in indices]

    # Generate Cartesian product
    keys = list(coarse_values.keys())
    product_iter = itertools.product(*(coarse_values[k] for k in keys))
    return [dict(zip(keys, item, strict=True)) for item in product_iter]


def focal_expansion(
    evaluated_candidates: Sequence[dict[str, Any]],
    grid_values: dict[str, list[Any]] | None = None,
    *,
    top_n: int = 30,
    margin_cap_usdt: float | None = 280.0,
    min_net_pnl: float = 0.0,
) -> list[dict[str, Any]]:
    """Expand the 1-step neighborhood around top coarse candidates in the full grid.

    Parameters:
    - evaluated_candidates: list of dicts with parameter keys and 'full_net_pnl_usdt'
      (or 'net_pnl') and 'initial_margin_peak_usdt' (or 'peak_initial_margin_usdt').
    - top_n: maximum number of top candidate seeds to expand around.
    - margin_cap_usdt: margin constraint threshold.
    - min_net_pnl: minimum profit required for a seed candidate.

    Returns:
    - list of newly discovered neighboring parameter dicts (excluding already
      evaluated seeds).
    """
    values = grid_values or DEFAULT_GRID_VALUES
    val_to_idx = {d: {v: i for i, v in enumerate(values[d])} for d in DIMS}

    # Filter feasible and profitable candidates
    viable = []
    for item in evaluated_candidates:
        pnl = float(item.get("full_net_pnl_usdt") or item.get("net_pnl") or 0.0)
        margin = float(
            item.get("initial_margin_peak_usdt")
            or item.get("peak_initial_margin_usdt")
            or 0.0
        )
        if pnl <= min_net_pnl:
            continue
        if margin_cap_usdt is not None and margin > margin_cap_usdt + 1e-9:
            continue
        viable.append((pnl, item))

    # Sort descending by profit and take top_n
    viable.sort(key=lambda x: x[0], reverse=True)
    seeds = [item for _, item in viable[:top_n]]

    already_evaluated_keys = {
        tuple(item[d] for d in DIMS) for item in evaluated_candidates
    }
    expanded_keys: set[tuple[Any, ...]] = set()

    for seed in seeds:
        seed_key = tuple(seed[d] for d in DIMS)
        for i, d in enumerate(DIMS):
            vals = values[d]
            curr_val = seed_key[i]
            v_idx = val_to_idx[d].get(curr_val)
            if v_idx is None:
                continue
            for step in (-1, 1):
                next_idx = v_idx + step
                if 0 <= next_idx < len(vals):
                    n_key = list(seed_key)
                    n_key[i] = vals[next_idx]
                    t_key = tuple(n_key)
                    if t_key not in already_evaluated_keys:
                        expanded_keys.add(t_key)

    return [dict(zip(DIMS, k, strict=True)) for k in sorted(expanded_keys)]


def candidate_to_param_tuple(candidate: ParameterCandidate) -> tuple[Any, ...]:
    """Convert ParameterCandidate to canonical tuple matching DIMS."""
    return tuple(candidate.params[d] for d in DIMS)
