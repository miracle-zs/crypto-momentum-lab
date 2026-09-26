"""Unit tests for DatasetCatalog and reproducible DatasetManifests.

Tests:
1. Dataset manifest building with interval and holes coverage proof;
2. Verified ordered stream streaming from open_dataset;
3. Cryptographic manifest hash tampering detection (ManifestIntegrityError);
4. Missing revision fails closed with UnreproducibleError.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.market.market_book import (
    DatasetCatalog,
    InMemoryMarketBookRepository,
    ManifestIntegrityError,
    MarketBook,
    UnreproducibleError,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.revision_models import (
    DatasetManifest,
    MarketVisibilityMode,
)


def _make_state(
    *,
    symbol: str,
    bucket_start: datetime,
    close_price: Decimal = Decimal("65000.00"),
) -> MarketState15s:
    bucket_end = bucket_start + timedelta(seconds=15)
    return MarketState15s(
        schema_version=1,
        environment="live",
        exchange="binance",
        symbol=symbol,
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        open_price=Decimal("64990.00"),
        high_price=Decimal("65010.00"),
        low_price=Decimal("64980.00"),
        close_price=close_price,
        trade_count=100,
        trade_notional=Decimal("1000000.00"),
        aggressive_buy_notional=Decimal("500000.00"),
        aggressive_sell_notional=Decimal("500000.00"),
        last_bid_price=Decimal("64999.00"),
        last_ask_price=Decimal("65001.00"),
        spread=Decimal("2.00"),
        midpoint=Decimal("65000.00"),
        liquidation_count=0,
        liquidation_notional=Decimal("0.00"),
        mark_price=Decimal("65000.00"),
        closed_kline_count=0,
        source_event_count=100,
        first_received_at=bucket_start + timedelta(milliseconds=100),
        last_received_at=bucket_end - timedelta(milliseconds=50),
        data_complete=True,
        missing_agg_trade_count=0,
        is_backfill=False,
    )


def test_dataset_manifest_coverage_and_holes_proof() -> None:
    repo = InMemoryMarketBookRepository()
    book = MarketBook(repo)
    catalog = DatasetCatalog(book, repo)

    t0 = datetime(2026, 9, 25, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(seconds=15)
    t2 = t1 + timedelta(seconds=15)
    t3 = t2 + timedelta(seconds=15)

    # Publish BTCUSDT at t0, t1, t2 (3 buckets)
    # Publish ETHUSDT only at t0, t2 (missing t1 -> 1 hole)
    book.publish(_make_state(symbol="BTCUSDT", bucket_start=t0))
    book.publish(_make_state(symbol="BTCUSDT", bucket_start=t1))
    book.publish(_make_state(symbol="BTCUSDT", bucket_start=t2))

    book.publish(_make_state(symbol="ETHUSDT", bucket_start=t0))
    book.publish(_make_state(symbol="ETHUSDT", bucket_start=t2))

    manifest = catalog.build_dataset(
        manifest_id="ds_test_01",
        scope="live",
        symbols=("BTCUSDT", "ETHUSDT"),
        interval="15s",
        start_time=t0,
        end_time=t3,  # 3 intervals * 2 symbols = 6 expected buckets
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
    )

    assert manifest.manifest_id == "ds_test_01"
    assert len(manifest.revision_refs) == 5
    assert len(manifest.holes) == 1
    assert manifest.holes[0] == (t1, t2)
    # 5 / 6 coverage
    assert manifest.coverage_ratio == Decimal("5") / Decimal("6")
    assert manifest.manifest_hash != ""


def test_open_dataset_verified_stream() -> None:
    repo = InMemoryMarketBookRepository()
    book = MarketBook(repo)
    catalog = DatasetCatalog(book, repo)

    t0 = datetime(2026, 9, 25, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(seconds=15)

    book.publish(_make_state(symbol="BTCUSDT", bucket_start=t0))
    book.publish(_make_state(symbol="ETHUSDT", bucket_start=t0))
    book.publish(_make_state(symbol="BTCUSDT", bucket_start=t1))
    book.publish(_make_state(symbol="ETHUSDT", bucket_start=t1))

    catalog.build_dataset(
        manifest_id="ds_test_02",
        scope="live",
        symbols=("BTCUSDT", "ETHUSDT"),
        interval="15s",
        start_time=t0,
        end_time=t1 + timedelta(seconds=15),
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
    )

    stream = catalog.open_dataset("ds_test_02")
    assert len(stream) == 4
    # Verification: ordered by bucket_start then symbol
    assert (stream[0].ref.bucket_start, stream[0].ref.symbol) == (t0, "BTCUSDT")
    assert (stream[1].ref.bucket_start, stream[1].ref.symbol) == (t0, "ETHUSDT")
    assert (stream[2].ref.bucket_start, stream[2].ref.symbol) == (t1, "BTCUSDT")
    assert (stream[3].ref.bucket_start, stream[3].ref.symbol) == (t1, "ETHUSDT")


def test_manifest_tampering_integrity_check() -> None:
    repo = InMemoryMarketBookRepository()
    book = MarketBook(repo)
    catalog = DatasetCatalog(book, repo)

    t0 = datetime(2026, 9, 25, 0, 0, 0, tzinfo=UTC)
    book.publish(_make_state(symbol="BTCUSDT", bucket_start=t0))

    manifest = catalog.build_dataset(
        manifest_id="ds_tampered",
        scope="live",
        symbols=("BTCUSDT",),
        interval="15s",
        start_time=t0,
        end_time=t0 + timedelta(seconds=15),
    )

    # Tamper with manifest_hash
    tampered = DatasetManifest(
        manifest_id=manifest.manifest_id,
        scope=manifest.scope,
        symbols=manifest.symbols,
        interval=manifest.interval,
        start_time=manifest.start_time,
        end_time=manifest.end_time,
        visibility_mode=manifest.visibility_mode,
        revision_refs=manifest.revision_refs,
        schema_version=manifest.schema_version,
        feature_algorithm_version=manifest.feature_algorithm_version,
        manifest_hash="bad_hash_forgery",
        created_at=manifest.created_at,
        coverage_ratio=manifest.coverage_ratio,
        holes=manifest.holes,
    )
    repo.save_manifest(tampered)

    with pytest.raises(ManifestIntegrityError, match="integrity violated"):
        catalog.open_dataset("ds_tampered")


def test_open_dataset_missing_revision_raises_unreproducible() -> None:
    repo = InMemoryMarketBookRepository()
    book = MarketBook(repo)
    catalog = DatasetCatalog(book, repo)

    t0 = datetime(2026, 9, 25, 0, 0, 0, tzinfo=UTC)
    ref = book.publish(_make_state(symbol="BTCUSDT", bucket_start=t0))

    catalog.build_dataset(
        manifest_id="ds_missing_rev",
        scope="live",
        symbols=("BTCUSDT",),
        interval="15s",
        start_time=t0,
        end_time=t0 + timedelta(seconds=15),
    )

    # Simulate revision being pruned/deleted illegally from storage
    repo.envelopes.pop(ref.revision_id)

    with pytest.raises(UnreproducibleError, match="cannot be reproduced"):
        catalog.open_dataset("ds_missing_rev")


def test_dataset_catalog_batch_range_fetch_and_verify() -> None:
    repo = InMemoryMarketBookRepository()
    book = MarketBook(repo)
    catalog = DatasetCatalog(book, repo)

    t0 = datetime(2026, 9, 25, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(seconds=15)
    t2 = t1 + timedelta(seconds=15)

    book.publish(
        _make_state(symbol="BTCUSDT", bucket_start=t0),
        is_canonical=True,
    )
    book.publish(
        _make_state(symbol="ETHUSDT", bucket_start=t0),
        is_canonical=True,
    )
    book.publish(
        _make_state(symbol="BTCUSDT", bucket_start=t1),
        is_canonical=True,
    )
    book.publish(
        _make_state(symbol="ETHUSDT", bucket_start=t1),
        is_canonical=True,
    )

    # Test get_canonical_refs_in_range
    range_refs = repo.get_canonical_refs_in_range(
        "live",
        ("BTCUSDT", "ETHUSDT"),
        "15s",
        t0,
        t2,
    )
    assert len(range_refs) == 4
    assert ("BTCUSDT", t0) in range_refs
    assert ("ETHUSDT", t1) in range_refs

    # Test build_dataset with batch fetcher
    manifest = catalog.build_dataset(
        manifest_id="ds_batch_test",
        scope="live",
        symbols=("BTCUSDT", "ETHUSDT"),
        interval="15s",
        start_time=t0,
        end_time=t2,
        visibility_mode=MarketVisibilityMode.CANONICAL,
    )
    assert manifest.manifest_id == "ds_batch_test"
    assert len(manifest.revision_refs) == 4
    assert len(manifest.holes) == 0
    assert manifest.coverage_ratio == Decimal("1.0")

    # Test verify_manifest
    verified = catalog.verify_manifest("ds_batch_test")
    assert verified["status"] == "VERIFIED_REPRODUCIBLE"
    assert verified["verified"] is True
    assert verified["revisions_count"] == 4
    assert Decimal(verified["coverage_ratio"]) == Decimal("1.0")


def test_dataset_catalog_verify_manifest_integrity_violation() -> None:
    repo = InMemoryMarketBookRepository()
    book = MarketBook(repo)
    catalog = DatasetCatalog(book, repo)

    t0 = datetime(2026, 9, 25, 0, 0, 0, tzinfo=UTC)
    book.publish(
        _make_state(symbol="BTCUSDT", bucket_start=t0),
        is_canonical=True,
    )

    manifest = catalog.build_dataset(
        manifest_id="ds_tampered_verify",
        scope="live",
        symbols=("BTCUSDT",),
        interval="15s",
        start_time=t0,
        end_time=t0 + timedelta(seconds=15),
    )

    # Tamper with stored hash
    tampered = DatasetManifest(
        manifest_id=manifest.manifest_id,
        scope=manifest.scope,
        symbols=manifest.symbols,
        interval=manifest.interval,
        start_time=manifest.start_time,
        end_time=manifest.end_time,
        visibility_mode=manifest.visibility_mode,
        revision_refs=manifest.revision_refs,
        schema_version=manifest.schema_version,
        feature_algorithm_version=manifest.feature_algorithm_version,
        manifest_hash="tampered_hash_123",
        created_at=manifest.created_at,
        coverage_ratio=manifest.coverage_ratio,
        holes=manifest.holes,
    )
    repo.save_manifest(tampered)

    res = catalog.verify_manifest("ds_tampered_verify")
    assert res["status"] == "INTEGRITY_VIOLATION"
    assert res["verified"] is False

    # Non-existent manifest
    missing = catalog.verify_manifest("ds_nonexistent")
    assert missing["status"] == "NOT_FOUND"
    assert missing["verified"] is False


def test_dataset_catalog_list_and_distinct_dates() -> None:
    from crypto_momentum_lab.tools.catalog_dataset import catalog_auto_daily

    repo = InMemoryMarketBookRepository()
    book = MarketBook(repo)
    catalog = DatasetCatalog(book, repo)

    d1 = datetime(2026, 9, 24, 10, 0, 0, tzinfo=UTC)
    d2 = datetime(2026, 9, 25, 11, 0, 0, tzinfo=UTC)

    book.publish(
        _make_state(symbol="BTCUSDT", bucket_start=d1),
        is_canonical=True,
    )
    book.publish(
        _make_state(symbol="ETHUSDT", bucket_start=d1),
        is_canonical=True,
    )
    book.publish(
        _make_state(symbol="SOLUSDT", bucket_start=d2),
        is_canonical=True,
    )

    dates_syms = repo.get_distinct_dates_and_symbols("live", "15s")
    assert len(dates_syms) == 2
    assert dates_syms[0][0] == d1.date()
    assert dates_syms[0][1] == ("BTCUSDT", "ETHUSDT")
    assert dates_syms[1][0] == d2.date()
    assert dates_syms[1][1] == ("SOLUSDT",)

    # Test auto_daily tool logic
    results = catalog_auto_daily(
        catalog,
        repo,  # type: ignore[arg-type]
        scope="live",
        interval="15s",
        save=True,
    )
    assert len(results) == 2
    assert results[0]["symbols_count"] == 2
    assert results[1]["symbols_count"] == 1

    # Test list_manifests
    listed = catalog.list_manifests(scope="live")
    assert len(listed) == 2

