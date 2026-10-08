from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import text

from crypto_momentum_lab.domain.market.market_book import DatasetCatalog, MarketBook
from crypto_momentum_lab.persistence.postgres.market_book_repository import (
    PostgresMarketBookRepository,
)
from crypto_momentum_lab.persistence.postgres.market_revision_archive import (
    ZstdMarketRevisionPayloadArchive,
)
from crypto_momentum_lab.persistence.postgres.market_revision_metadata_archive import (
    SqliteMarketRevisionMetadataArchive,
)
from crypto_momentum_lab.persistence.postgres.models import (
    DatasetManifestRow,
    DecisionTraceRow,
    MarketRevisionRefRow,
)
from crypto_momentum_lab.tools.archive_market_revision_metadata import archive_batch
from crypto_momentum_lab.tools.archive_market_revision_payloads import (
    archive_market_revision_payloads,
)
from tests.integration.persistence.test_market_revision_archiver import (
    archive_sessions,  # noqa: F401
)
from tests.unit.decision.test_decision_engine import _make_market_envelope

pytestmark = pytest.mark.integration


@pytest.fixture
def metadata_sessions(archive_sessions):  # noqa: F811 - imported pytest fixture
    with archive_sessions.begin() as session:
        connection = session.connection()
        schema = connection.get_execution_options()["schema_translate_map"][None]
        session.execute(text(f'SET LOCAL search_path TO "{schema}"'))
        DecisionTraceRow.__table__.create(connection)
        DatasetManifestRow.__table__.create(connection)
    return archive_sessions


def setup_archive(factory, root):
    start = datetime(2026, 9, 25, tzinfo=UTC)
    _, envelope = _make_market_envelope("BTCUSDT", start, Decimal("65000"))
    hot = PostgresMarketBookRepository(factory)
    hot.save_envelope(envelope)
    hot.set_canonical_ref(envelope.ref.scope, "BTCUSDT", "15s", start, envelope.ref)
    archive_market_revision_payloads(
        session_factory=factory,
        archive_root=root,
        cutoff=start + timedelta(days=1),
        max_chunks=1,
        batch_size=10,
        apply=True,
    )
    catalog = SqliteMarketRevisionMetadataArchive(root)
    payloads = ZstdMarketRevisionPayloadArchive(root)
    repo = PostgresMarketBookRepository(
        factory, payload_archive=payloads, metadata_archive=catalog
    )
    return start, envelope, catalog, payloads, repo


def test_cold_metadata_retains_envelope_and_canonical_lookup(
    metadata_sessions, tmp_path
):
    start, envelope, catalog, payloads, repo = setup_archive(
        metadata_sessions, tmp_path
    )
    with metadata_sessions.begin() as session:
        assert archive_batch(
            session,
            catalog=catalog,
            payload_archive=payloads,
            cutoff=start + timedelta(days=1),
            apply=True,
        ) == (1, 1)
    with metadata_sessions() as session:
        assert session.get(MarketRevisionRefRow, envelope.ref.revision_id) is None
    assert repo.load_envelope(envelope.ref.revision_id) == envelope
    assert (
        repo.get_canonical_ref(envelope.ref.scope, "BTCUSDT", "15s", start)
        == envelope.ref
    )
    assert repo.get_revisions_for_bucket(
        envelope.ref.scope, "BTCUSDT", "15s", start
    ) == (envelope.ref,)
    assert repo.get_canonical_refs_in_range(
        envelope.ref.scope, ("BTCUSDT",), "15s", start, start + timedelta(seconds=15)
    ) == {("BTCUSDT", start): envelope.ref}
    assert repo.get_distinct_dates_and_symbols(envelope.ref.scope) == [
        (start.date(), ("BTCUSDT",))
    ]


def test_full_trace_reference_prevents_hot_metadata_removal(
    metadata_sessions, tmp_path
):
    start, envelope, catalog, payloads, repo = setup_archive(
        metadata_sessions, tmp_path
    )
    with metadata_sessions.begin() as session:
        session.add(
            DecisionTraceRow(
                decision_id="protected",
                strategy_name="test",
                account_label="primary",
                decision_time=start,
                intent_produced=False,
                intent_id=None,
                rejection_reason="no_candidate",
                evaluated_revision_ids=[envelope.ref.revision_id],
                trace_payload={},
                created_at=start,
            )
        )
    with metadata_sessions.begin() as session:
        assert archive_batch(
            session,
            catalog=catalog,
            payload_archive=payloads,
            cutoff=start + timedelta(days=1),
            apply=True,
        ) == (0, 0)
    assert catalog.get(envelope.ref.revision_id) is None


def test_bad_payload_archive_keeps_hot_metadata(metadata_sessions, tmp_path):
    start, envelope, catalog, payloads, repo = setup_archive(
        metadata_sessions, tmp_path
    )
    with metadata_sessions() as session:
        row = session.get(MarketRevisionRefRow, envelope.ref.revision_id)
        path = tmp_path / row.payload_archive_path
    path.write_bytes(path.read_bytes() + b"corrupt")
    with pytest.raises(Exception, match="SHA-256"):
        with metadata_sessions.begin() as session:
            archive_batch(
                session,
                catalog=catalog,
                payload_archive=payloads,
                cutoff=start + timedelta(days=1),
                apply=True,
            )
    with metadata_sessions() as session:
        assert session.get(MarketRevisionRefRow, envelope.ref.revision_id) is not None
    assert catalog.get(envelope.ref.revision_id) is None


def test_dataset_manifest_hash_and_replay_survive_metadata_transfer(
    metadata_sessions, tmp_path
):
    from crypto_momentum_lab.domain.market.revision_models import MarketVisibilityMode

    start, envelope, cold, payloads, repo = setup_archive(metadata_sessions, tmp_path)
    catalog = DatasetCatalog(MarketBook(repo), repo)
    manifest = catalog.build_dataset(
        manifest_id="before",
        scope=envelope.ref.scope,
        symbols=("BTCUSDT",),
        start_time=start,
        end_time=start + timedelta(seconds=15),
        visibility_mode=MarketVisibilityMode.CANONICAL,
    )
    repo.save_manifest(manifest)
    assert repo.verify_manifest("before")["verified"]
    with metadata_sessions.begin() as session:
        assert archive_batch(
            session,
            catalog=cold,
            payload_archive=payloads,
            cutoff=start + timedelta(days=1),
            apply=True,
        ) == (1, 1)
    assert repo.load_manifest("before") == manifest
    assert repo.verify_manifest("before")["verified"]
    assert catalog.open_dataset("before") == (envelope,)
    after = catalog.build_dataset(
        manifest_id="after",
        scope=envelope.ref.scope,
        symbols=("BTCUSDT",),
        start_time=start,
        end_time=start + timedelta(seconds=15),
        visibility_mode=MarketVisibilityMode.CANONICAL,
    )
    assert after.manifest_hash == manifest.manifest_hash


def test_hot_canonical_override_and_cold_target_promotion(metadata_sessions, tmp_path):
    start, envelope, cold, payloads, repo = setup_archive(metadata_sessions, tmp_path)
    with metadata_sessions.begin() as session:
        archive_batch(
            session,
            catalog=cold,
            payload_archive=payloads,
            cutoff=start + timedelta(days=1),
            apply=True,
        )
    _, new = _make_market_envelope("BTCUSDT", start, Decimal("65001"))
    new = replace(new, ref=replace(new.ref, revision_id="new-canonical"))
    repo.save_envelope(new)
    repo.set_canonical_ref(
        new.ref.scope, new.ref.symbol, new.ref.interval, start, new.ref
    )
    assert (
        repo.get_canonical_ref(new.ref.scope, new.ref.symbol, new.ref.interval, start)
        == new.ref
    )
    assert (
        repo.get_canonical_refs_in_range(
            new.ref.scope, ("BTCUSDT",), "15s", start, start + timedelta(seconds=15)
        )[("BTCUSDT", start)]
        == new.ref
    )
    repo.set_canonical_ref(envelope.ref.scope, "BTCUSDT", "15s", start, envelope.ref)
    assert (
        repo.get_canonical_ref(envelope.ref.scope, "BTCUSDT", "15s", start)
        == envelope.ref
    )


def test_historical_full_decision_promotes_and_protects_cold_identity(
    metadata_sessions, tmp_path
):
    from crypto_momentum_lab.domain.market.revision_models import DecisionTrace

    start, envelope, cold, payloads, repo = setup_archive(metadata_sessions, tmp_path)
    with metadata_sessions.begin() as session:
        archive_batch(
            session,
            catalog=cold,
            payload_archive=payloads,
            cutoff=start + timedelta(days=1),
            apply=True,
        )
    trace = DecisionTrace(
        decision_id="historical",
        strategy_name="test",
        account_label="primary",
        decision_time=start,
        evaluated_market_refs=(envelope.ref,),
        intent_produced=False,
        rejection_reason="no_candidate",
        trace_payload={},
    )
    repo.save_decision_trace(trace)
    assert repo.load_decision_trace("historical").evaluated_market_refs == (
        envelope.ref,
    )
    assert (
        repo.get_canonical_ref(envelope.ref.scope, "BTCUSDT", "15s", start)
        == envelope.ref
    )
    with metadata_sessions.begin() as session:
        assert archive_batch(
            session,
            catalog=cold,
            payload_archive=payloads,
            cutoff=start + timedelta(days=1),
            apply=True,
        ) == (0, 0)


def test_key_share_writer_blocks_collection_and_committed_trace_protects_row(
    async_database_url, tmp_path
):
    from uuid import uuid4

    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(
        async_database_url.replace("postgresql+asyncpg://", "postgresql+psycopg://")
    )
    schema = "metadata_concurrent_" + uuid4().hex
    with engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        connection = connection.execution_options(schema_translate_map={None: schema})
        MarketRevisionRefRow.__table__.create(connection)
        DecisionTraceRow.__table__.create(connection)
    scoped = engine.execution_options(
        schema_translate_map={None: schema},
        isolation_level="READ COMMITTED",
    )
    factory = sessionmaker(scoped, expire_on_commit=False)
    try:
        start, envelope, catalog, payloads, repo = setup_archive(factory, tmp_path)
        with factory() as writer:
            writer.scalar(
                select(MarketRevisionRefRow)
                .where(MarketRevisionRefRow.revision_id == envelope.ref.revision_id)
                .with_for_update(read=True, key_share=True)
            )
            with factory.begin() as collector:
                collector.execute(text(f'SET LOCAL search_path TO "{schema}"'))
                assert archive_batch(
                    collector,
                    catalog=catalog,
                    payload_archive=payloads,
                    cutoff=start + timedelta(days=1),
                    apply=True,
                ) == (0, 0)
            writer.add(
                DecisionTraceRow(
                    decision_id="concurrent",
                    strategy_name="test",
                    account_label="primary",
                    decision_time=start,
                    intent_produced=True,
                    intent_id=None,
                    rejection_reason=None,
                    evaluated_revision_ids=[envelope.ref.revision_id],
                    trace_payload={},
                    created_at=start,
                )
            )
            writer.commit()
        with factory.begin() as collector:
            collector.execute(text(f'SET LOCAL search_path TO "{schema}"'))
            assert archive_batch(
                collector,
                catalog=catalog,
                payload_archive=payloads,
                cutoff=start + timedelta(days=1),
                apply=True,
            ) == (0, 0)
        assert repo.load_envelope(envelope.ref.revision_id) == envelope
    finally:
        with engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        engine.dispose()


def test_cold_metadata_migration_creates_valid_concurrent_indexes(async_database_url):
    import importlib.util
    from pathlib import Path
    from uuid import uuid4

    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy import create_engine

    engine = create_engine(
        async_database_url.replace("postgresql+asyncpg://", "postgresql+psycopg://")
    )
    schema = "metadata_migration_" + uuid4().hex
    spec = importlib.util.spec_from_file_location(
        "cold_migration",
        Path("alembic/versions/20261009_0056_cold_market_revision_metadata.py"),
    )
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    try:
        with engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            mapped = connection.execution_options(schema_translate_map={None: schema})
            MarketRevisionRefRow.__table__.create(mapped)
            DecisionTraceRow.__table__.create(mapped)
            connection.execute(
                text(f'DROP INDEX "{schema}".ix_market_revision_refs_cold_metadata')
            )
            connection.execute(
                text(f'DROP INDEX "{schema}".ix_decision_traces_full_revision_ids')
            )
        with engine.connect() as connection:
            connection.execute(text(f'SET search_path TO "{schema}"'))
            connection.commit()
            context = MigrationContext.configure(connection)
            with Operations.context(context), context.begin_transaction():
                migration.upgrade()
            valid = connection.execute(
                text(
                    "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
                    "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema "
                    "AND c.relname IN ('ix_market_revision_refs_cold_metadata',"
                    "'ix_decision_traces_full_revision_ids') AND i.indisvalid"
                ),
                {"schema": schema},
            ).scalar_one()
            assert valid == 2
    finally:
        with engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        engine.dispose()
