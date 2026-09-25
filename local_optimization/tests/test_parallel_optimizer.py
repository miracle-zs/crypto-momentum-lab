"""Tests for multi-process parallelization and adaptive grid generation."""

from __future__ import annotations

import itertools
from typing import Any

import pandas as pd
import pytest

from local_optimization.adaptive_grid import (
    DEFAULT_GRID_VALUES,
    DIMS,
    focal_expansion,
    generate_coarse_grid,
)
from local_optimization.run_two_stage_grid_optimization import (
    compute_all_neighborhood_stabilities,
)


@pytest.fixture
def sample_grid_df() -> tuple[pd.DataFrame, dict[str, list[Any]]]:
    """Generate a controlled test grid with known neighborhood relationships."""
    grid_values: dict[str, list[Any]] = {
        "impulse_window_buckets": [1, 2, 3],
        "confirmation_buckets": [1, 2],
        "min_return_pct": [0.5, 1.0, 1.5],
        "min_imbalance": [0.2, 0.4],
        "min_intensity": [1.0, 2.5],
        "min_volume_ratio": [0.0, 1.5],
        "cooldown_buckets": [0, 2],
    }
    keys = list(grid_values.keys())
    combos = list(itertools.product(*(grid_values[k] for k in keys)))
    # Total combinations = 3 * 2 * 3 * 2 * 2 * 2 * 2 = 576 rows
    rows = []
    for i, combo in enumerate(combos):
        d = dict(zip(keys, combo, strict=True))
        # PnL varies predictably, some positive, some negative
        pnl = float(50.0 + (i % 20) * 5.0 - (i % 7) * 10.0)
        margin = float(150.0 + (i % 10) * 15.0)
        d["full_net_pnl_usdt"] = pnl
        d["initial_margin_peak_usdt"] = margin
        d["full_max_drawdown_usdt"] = 25.0
        d["full_n_closed"] = 45
        rows.append(d)

    df = pd.DataFrame(rows)
    return df, grid_values


def test_parallel_stability_parity_across_workers(
    sample_grid_df: tuple[pd.DataFrame, dict[str, list[Any]]],
) -> None:
    """Verify that 1, 2, and 4-worker calculations are 100% numerically identical."""
    df, grid_values = sample_grid_df

    # 1. Run sequential (workers=1)
    stab_seq_280 = compute_all_neighborhood_stabilities(
        df, grid_values, cap=280.0, workers=1
    )
    stab_seq_none = compute_all_neighborhood_stabilities(
        df, grid_values, cap=None, workers=1
    )

    # 2. Run parallel (workers=2)
    stab_p2_280 = compute_all_neighborhood_stabilities(
        df, grid_values, cap=280.0, workers=2
    )
    stab_p2_none = compute_all_neighborhood_stabilities(
        df, grid_values, cap=None, workers=2
    )

    # 3. Run parallel (workers=4)
    stab_p4_280 = compute_all_neighborhood_stabilities(
        df, grid_values, cap=280.0, workers=4
    )
    stab_p4_none = compute_all_neighborhood_stabilities(
        df, grid_values, cap=None, workers=4
    )

    # Assert exact key and value equivalence
    assert (
        set(stab_seq_280.keys()) == set(stab_p2_280.keys()) == set(stab_p4_280.keys())
    )
    assert (
        set(stab_seq_none.keys())
        == set(stab_p2_none.keys())
        == set(stab_p4_none.keys())
    )

    for k in stab_seq_280:
        assert pytest.approx(stab_seq_280[k], abs=1e-9) == stab_p2_280[k]
        assert pytest.approx(stab_seq_280[k], abs=1e-9) == stab_p4_280[k]

    for k in stab_seq_none:
        assert pytest.approx(stab_seq_none[k], abs=1e-9) == stab_p2_none[k]
        assert pytest.approx(stab_seq_none[k], abs=1e-9) == stab_p4_none[k]


def test_coarse_grid_sampling_boundaries() -> None:
    """Verify coarse grid generator preserves boundaries and shrinks candidate count."""
    coarse = generate_coarse_grid(DEFAULT_GRID_VALUES, stride=2)
    assert len(coarse) > 0
    # Original product is 25,200; coarse with stride 2 should be ~1,000 - 2,500
    assert len(coarse) < 5000

    # Ensure min and max bounds for each dimension are present in coarse grid
    for d, vals in DEFAULT_GRID_VALUES.items():
        coarse_vals = {item[d] for item in coarse}
        assert vals[0] in coarse_vals
        assert vals[-1] in coarse_vals


def test_focal_expansion_around_top_candidates() -> None:
    """Verify focal expansion explores local 1-step neighbors around best candidates."""
    # Create two seed candidates
    seeds = [
        {
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": 0.5,
            "min_imbalance": 0.30,
            "min_intensity": 4.0,
            "min_volume_ratio": 1.5,
            "cooldown_buckets": 0,
            "net_pnl": 150.0,
            "peak_initial_margin_usdt": 220.0,
        },
        {
            "impulse_window_buckets": 3,
            "confirmation_buckets": 2,
            "min_return_pct": 1.2,
            "min_imbalance": 0.40,
            "min_intensity": 2.5,
            "min_volume_ratio": 1.0,
            "cooldown_buckets": 1,
            "net_pnl": 80.0,
            "peak_initial_margin_usdt": 250.0,
        },
    ]

    expanded = focal_expansion(
        seeds,
        DEFAULT_GRID_VALUES,
        top_n=2,
        margin_cap_usdt=280.0,
        min_net_pnl=0.0,
    )

    assert len(expanded) > 0
    # Each seed has up to 14 neighbors in 7 dimensions
    assert len(expanded) <= 28

    # Ensure none of the seeds themselves are re-expanded
    seed_keys = {tuple(s[d] for d in DIMS) for s in seeds}
    expanded_keys = {tuple(e[d] for d in DIMS) for e in expanded}
    assert seed_keys.isdisjoint(expanded_keys)
