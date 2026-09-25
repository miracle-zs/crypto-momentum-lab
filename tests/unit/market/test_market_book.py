"""Unit tests for MarketBook R2 Dual-Revision Authority.

Tests:
1. Deterministic hashing of MarketState15s;
2. Publication idempotency;
3. Revision conflict rejection on payload mismatch;
4. Dual-revision coexistence (decision_visible vs canonical) for the same bucket;
5. Canonical pointer movement and immutable old revision readability.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.market.market_book import (
    InMemoryMarketBookRepository,
    MarketBook,
    RevisionConflictError,
    RevisionNotFoundError,
    compute_market_state_hash,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.revision_models import MarketVisibilityMode


def _create_sample_state(
    *,
    symbol: str = "BTCUSDT",
    bucket_start: datetime,
    close_price: Decimal = Decimal("65000.00"),
    missing_count: int = 0,
    data_complete: bool = True,
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
        data_complete=data_complete,
        missing_agg_trade_count=missing_count,
        is_backfill=not data_complete,
    )


def test_deterministic_market_state_hash() -> None:
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    s1 = _create_sample_state(bucket_start=t0, close_price=Decimal("65000.00"))
    s2 = _create_sample_state(bucket_start=t0, close_price=Decimal("65000.00"))
    s3 = _create_sample_state(bucket_start=t0, close_price=Decimal("65050.00"))

    h1 = compute_market_state_hash(s1)
    h2 = compute_market_state_hash(s2)
    h3 = compute_market_state_hash(s3)

    assert h1 == h2, "Identical states must produce identical hashes"
    assert h1 != h3, "Different close prices must produce different hashes"


def test_market_book_publish_idempotency() -> None:
    repo = InMemoryMarketBookRepository()
    book = MarketBook(repo)
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    s1 = _create_sample_state(bucket_start=t0)

    ref1 = book.publish(s1, source_epoch="ep1")
    ref2 = book.publish(s1, source_epoch="ep1")

    assert ref1.revision_id == ref2.revision_id
    assert ref1.content_hash == ref2.content_hash
    assert len(repo.envelopes) == 1


def test_market_book_conflict_rejection() -> None:
    repo = InMemoryMarketBookRepository()
    book = MarketBook(repo)
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    s1 = _create_sample_state(bucket_start=t0, close_price=Decimal("65000.00"))

    ref1 = book.publish(s1)
    env1 = repo.load_envelope(ref1.revision_id)
    assert env1 is not None

    # Simulate corrupted repository entry with same revision_id but differing hash
    from crypto_momentum_lab.domain.market.revision_models import (
        MarketEnvelope,
        MarketRevisionRef,
    )

    tampered_ref = MarketRevisionRef(
        scope=ref1.scope,
        symbol=ref1.symbol,
        interval=ref1.interval,
        bucket_start=ref1.bucket_start,
        bucket_end=ref1.bucket_end,
        revision_id=ref1.revision_id,
        content_hash="tampered_hash_12345",
        published_at=ref1.published_at,
        source_epoch=ref1.source_epoch,
        visibility_mode=ref1.visibility_mode,
    )
    repo.save_envelope(MarketEnvelope(ref=tampered_ref, state=s1))

    with pytest.raises(RevisionConflictError, match="Revision collision"):
        book.publish(s1)


def test_dual_revision_coexistence_and_pointers() -> None:
    repo = InMemoryMarketBookRepository()
    book = MarketBook(repo)
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)

    # 1. v1: Real-time observed (missing ticks, close=65000)
    s1 = _create_sample_state(
        bucket_start=t0,
        close_price=Decimal("65000.00"),
        missing_count=2,
        data_complete=False,
    )
    ref1 = book.publish(
        s1,
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
        is_canonical=False,
    )

    # Initially canonical pointer is None because v1 was published as non-canonical
    assert book.get_canonical_ref("live", "BTCUSDT", "15s", t0) is None
    assert book.get_decision_visible_ref("live", "BTCUSDT", "15s", t0) == ref1

    # 2. v2: Canonical repaired (complete, close=65020)
    s2 = _create_sample_state(
        bucket_start=t0,
        close_price=Decimal("65020.00"),
        missing_count=0,
        data_complete=True,
    )
    ref2 = book.publish(
        s2,
        visibility_mode=MarketVisibilityMode.CANONICAL,
        is_canonical=True,
    )

    assert ref1.revision_id != ref2.revision_id
    assert ref1.content_hash != ref2.content_hash

    # Pointer check: canonical points to v2, decision_visible resolves v1
    assert book.get_canonical_ref("live", "BTCUSDT", "15s", t0) == ref2
    assert book.get_decision_visible_ref("live", "BTCUSDT", "15s", t0) == ref1

    # Both envelopes are immutably readable
    env1 = book.read(ref1)
    env2 = book.read(ref2)
    assert env1.state.close_price == Decimal("65000.00")
    assert env2.state.close_price == Decimal("65020.00")
    assert env1.data_complete is False
    assert env2.data_complete is True


def test_read_missing_revision_raises() -> None:
    book = MarketBook()
    with pytest.raises(RevisionNotFoundError, match="not found"):
        book.read("non_existent_revision_123")


def test_decision_visible_ref_strict_isolation_and_no_leakage() -> None:
    """Regression test: get_decision_visible_ref must never leak canonical or future revisions."""
    repo = InMemoryMarketBookRepository()
    book = MarketBook(repo)
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)

    # 1. Bucket t0 has only canonical revision published at t0 + 15s
    s_canon = _create_sample_state(bucket_start=t0)
    book.publish(
        s_canon,
        visibility_mode=MarketVisibilityMode.CANONICAL,
        is_canonical=True,
    )
    # Decision visible ref must be None at decision_time = t0 + 15s because only canonical exists
    assert (
        book.get_decision_visible_ref(
            "live", "BTCUSDT", "15s", t0, decision_time=t0 + timedelta(seconds=15)
        )
        is None
    )

    # 2. Bucket t0 gets a decision-visible revision, but published at t0 + 16s (delayed)
    s_dv = _create_sample_state(bucket_start=t0, close_price=Decimal("65010.00"))
    ref_dv = book.publish(
        s_dv,
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
        is_canonical=False,
        published_at=t0 + timedelta(seconds=16),
    )

    # If decision_time is t0 + 15s, ref_dv was published in the future (> decision_time), so it must NOT be visible!
    assert (
        book.get_decision_visible_ref(
            "live", "BTCUSDT", "15s", t0, decision_time=t0 + timedelta(seconds=15)
        )
        is None
    )

    # If decision_time is t0 + 17s (after published_at), it IS visible
    assert (
        book.get_decision_visible_ref(
            "live", "BTCUSDT", "15s", t0, decision_time=t0 + timedelta(seconds=17)
        )
        == ref_dv
    )


def test_postgres_market_book_repository_fails_closed_on_missing_revision() -> None:
    """Regression test: PostgresMarketBookRepository must raise UnreproducibleError when revision is missing."""
    from unittest.mock import MagicMock

    from crypto_momentum_lab.domain.market.market_book import UnreproducibleError
    from crypto_momentum_lab.persistence.postgres.market_book_repository import (
        PostgresMarketBookRepository,
    )
    from crypto_momentum_lab.persistence.postgres.models import (
        DatasetManifestRow,
        DecisionTraceRow,
    )

    mock_session = MagicMock()
    mock_session.__enter__.return_value = mock_session
    mock_session_factory = MagicMock(return_value=mock_session)

    repo = PostgresMarketBookRepository(mock_session_factory)

    # 1. DecisionTraceRow references a missing revision
    mock_trace_row = DecisionTraceRow(
        decision_id="dec_missing_rev_1",
        strategy_name="orderflow_impulse",
        account_label="primary",
        decision_time=datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC),
        intent_produced=True,
        intent_id="intent_1",
        rejection_reason=None,
        evaluated_revision_ids=["rev_exists", "rev_missing_999"],
        trace_payload={},
        created_at=datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC),
    )
    mock_session.get.return_value = mock_trace_row
    # Simulate DB returning empty list of revisions
    mock_session.execute.return_value.scalars.return_value.all.return_value = []

    with pytest.raises(
        UnreproducibleError, match="missing revision rev_exists"
    ):
        repo.load_decision_trace("dec_missing_rev_1")

    # 2. DatasetManifestRow references a missing revision
    mock_manifest_row = DatasetManifestRow(
        manifest_id="mf_missing_rev_1",
        scope="live",
        symbols="BTCUSDT",
        interval="15s",
        start_time=datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC),
        end_time=datetime(2026, 9, 25, 12, 15, 0, tzinfo=UTC),
        visibility_mode="canonical",
        schema_version=1,
        feature_algorithm_version="v1",
        manifest_hash="hash_123",
        coverage_ratio=Decimal("1.0"),
        revision_ids=["rev_missing_888"],
        holes=[],
        created_at=datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC),
    )
    mock_session.get.return_value = mock_manifest_row

    with pytest.raises(
        UnreproducibleError, match="missing revision rev_missing_888"
    ):
        repo.load_manifest("mf_missing_rev_1")


