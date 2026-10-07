from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from crypto_momentum_lab.domain.market.market_book import compute_market_state_hash
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.state_codec import market_state_to_payload
from crypto_momentum_lab.persistence.postgres.market_book_repository import (
    PostgresMarketBookRepository,
)
from crypto_momentum_lab.persistence.postgres.market_revision_archive import (
    ZstdMarketRevisionPayloadArchive,
)
from crypto_momentum_lab.persistence.postgres.models import MarketRevisionRefRow
from crypto_momentum_lab.tools.archive_market_revision_payloads import (
    _eligible_partitions,
    archive_market_revision_payloads,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def archive_sessions(async_database_url):
    engine = create_engine(
        async_database_url.replace("postgresql+asyncpg://", "postgresql+psycopg://")
    )
    schema = f"archive_test_{uuid4().hex}"
    with engine.connect() as connection, connection.begin():
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        connection = connection.execution_options(schema_translate_map={None: schema})
        MarketRevisionRefRow.__table__.create(connection)
        yield sessionmaker(
            connection,
            expire_on_commit=False,
            join_transaction_mode="create_savepoint",
        )
        # Roll back the outer transaction, including the isolated schema.
        connection.rollback()
    engine.dispose()


def _row(revision_id, scope, bucket_start):
    return MarketRevisionRefRow(
        revision_id=revision_id,
        scope=scope,
        symbol="BTCUSDT",
        interval="15s",
        bucket_start=bucket_start,
        bucket_end=bucket_start + timedelta(seconds=15),
        content_hash="a" * 64,
        published_at=bucket_start,
        source_epoch="test",
        visibility_mode="canonical",
        is_canonical=True,
        payload={"schema_version": 1},
        lineage={},
    )


def test_archive_discovery_preserves_window_scope_order_and_cutoff(archive_sessions):
    start = datetime(2026, 10, 1, tzinfo=UTC)
    with archive_sessions.begin() as session:
        session.add_all(
            [
                _row("b-first", "b", start),
                _row("a-later", "a", start + timedelta(minutes=2)),
                _row("a-same-window", "a", start + timedelta(minutes=3)),
                _row("next-window", "a", start + timedelta(minutes=15)),
                _row("sparse-window", "b", start + timedelta(hours=2)),
                _row("cutoff", "a", start + timedelta(hours=3)),
            ]
        )
        archived = _row("already-archived", "a", start - timedelta(days=1))
        archived.payload = None  # Historical JSON null with a valid pointer.
        archived.payload_archive_path = "old.jsonl.zst"
        archived.payload_archive_sha256 = "b" * 64
        session.add(archived)
    cutoff = start + timedelta(hours=3)
    with archive_sessions() as session:
        partitions = _eligible_partitions(session, cutoff=cutoff, limit=20)
        assert [(p.scope, p.start) for p in partitions] == [
            ("a", start),
            ("b", start),
            ("a", start + timedelta(minutes=15)),
            ("b", start + timedelta(hours=2)),
        ]
        assert _eligible_partitions(session, cutoff=cutoff, limit=1) == partitions[:1]
        assert _eligible_partitions(session, cutoff=cutoff, limit=2) == partitions[:2]
        assert _eligible_partitions(session, cutoff=cutoff, limit=0) == []
        assert _eligible_partitions(session, cutoff=start, limit=20) == []


def test_archive_writes_sql_null_and_replays_from_verified_file(
    archive_sessions, tmp_path
):
    start = datetime(2026, 10, 1, tzinfo=UTC)
    state = MarketState15s(
        schema_version=1,
        environment="live",
        exchange="binance",
        symbol="BTCUSDT",
        bucket_start=start,
        bucket_end=start + timedelta(seconds=15),
        open_price=Decimal("100"),
        high_price=Decimal("100"),
        low_price=Decimal("100"),
        close_price=Decimal("100"),
        trade_count=1,
        trade_notional=Decimal("100"),
        aggressive_buy_notional=Decimal("100"),
        aggressive_sell_notional=Decimal("0"),
        last_bid_price=Decimal("99"),
        last_ask_price=Decimal("101"),
        spread=Decimal("2"),
        midpoint=Decimal("100"),
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=Decimal("100"),
        closed_kline_count=0,
        source_event_count=1,
        first_received_at=start,
        last_received_at=start,
        data_complete=True,
        missing_agg_trade_count=0,
        is_backfill=False,
    )
    with archive_sessions.begin() as session:
        row = _row("replay", "research", start)
        row.payload = market_state_to_payload(state)
        row.content_hash = compute_market_state_hash(state)
        session.add(row)
    result = archive_market_revision_payloads(
        session_factory=archive_sessions,
        archive_root=tmp_path,
        cutoff=start + timedelta(days=1),
        max_chunks=1,
        batch_size=1,
        apply=True,
    )
    assert result["archived_rows"] == 1
    with archive_sessions() as session:
        assert session.query(MarketRevisionRefRow).filter(
            MarketRevisionRefRow.payload.is_(None),
            MarketRevisionRefRow.payload_archive_path.is_not(None),
        ).count() == 1
        assert _eligible_partitions(
            session, cutoff=start + timedelta(days=1), limit=20
        ) == []
    repository = PostgresMarketBookRepository(
        archive_sessions,
        payload_archive=ZstdMarketRevisionPayloadArchive(tmp_path),
    )
    envelope = repository.load_envelope("replay")
    assert envelope is not None
    assert envelope.state == state
