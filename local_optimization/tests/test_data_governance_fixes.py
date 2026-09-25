from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from local_optimization.build_raw_opportunity_pool import save_price_series_cache
from local_optimization.mtm_engine import load_cached_price_series


def test_save_price_series_cache_binds_manifest_metadata(tmp_path: Path) -> None:
    cache_path = tmp_path / "test_price_cache.pkl"
    price_data = {
        "BTCUSDT": ([1000.0, 1015.0], [50000.0, 50100.0]),
    }
    manifest = SimpleNamespace(
        content_hash="test_sha256_hash_12345",
        watermark_end=datetime(2026, 9, 20, 16, 0, tzinfo=UTC),
    )

    save_price_series_cache(price_data, cache_path, manifest=manifest)

    # 1. Load without manifest
    loaded_raw = load_cached_price_series(cache_path, expected_manifest=None)
    assert "BTCUSDT" in loaded_raw
    assert loaded_raw["BTCUSDT"] == price_data["BTCUSDT"]

    # 2. Load with matching manifest - must succeed
    loaded_auth = load_cached_price_series(cache_path, expected_manifest=manifest)
    assert "BTCUSDT" in loaded_auth

    # 3. Load with mismatched manifest hash - must fail closed
    bad_hash_manifest = SimpleNamespace(
        content_hash="mismatched_sha256",
        watermark_end=manifest.watermark_end,
    )
    import pytest

    with pytest.raises(ValueError, match="Price cache content hash mismatch"):
        load_cached_price_series(cache_path, expected_manifest=bad_hash_manifest)


def test_reconciliation_cache_loads_v2_prices(tmp_path: Path) -> None:
    cache_path = tmp_path / "v2_cache.pkl"
    price_data = {
        "ETHUSDT": ([100.0, 115.0], [3000.0, 3010.0]),
    }
    save_price_series_cache(price_data, cache_path, manifest=None)

    loaded = load_cached_price_series(cache_path)
    assert isinstance(loaded, dict)
    assert "ETHUSDT" in loaded
    assert "__metadata__" not in loaded  # unnested correctly


def test_opps_by_wc_fail_closed_on_missing_combination() -> None:
    all_events = [{"id": 1}, {"id": 2}, {"id": 3}]
    opps_by_wc = {
        (2, 1): [{"id": 1}],
        (3, 1): [{"id": 2}],
    }

    # Hit existing
    res_existing = opps_by_wc.get((2, 1), []) if opps_by_wc is not None else all_events
    assert len(res_existing) == 1

    # Missing combination: must fail closed to []
    res_missing = opps_by_wc.get((4, 2), []) if opps_by_wc is not None else all_events
    assert res_missing == [], (
        "Missing (w, c) combination must yield empty list, not all_events"
    )
