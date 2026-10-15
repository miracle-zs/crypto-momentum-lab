import importlib.util
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
import sqlalchemy as sa

MIGRATION_PATH = (
    Path(__file__).parents[4]
    / "alembic"
    / "versions"
    / "20261015_0066_compact_execution_trade_identities.py"
)
SPEC = importlib.util.spec_from_file_location(
    "compact_execution_trade_identities",
    MIGRATION_PATH,
)
assert SPEC is not None and SPEC.loader is not None
MIGRATION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MIGRATION)


def _identity_database() -> tuple[sa.Engine, sa.Connection, sa.Table]:
    engine = sa.create_engine("sqlite:///:memory:")
    connection = engine.connect()
    table = sa.Table(
        "execution_trade_identities",
        sa.MetaData(),
        sa.Column("environment", sa.String, primary_key=True),
        sa.Column("account_label", sa.String, primary_key=True),
        sa.Column("symbol", sa.String, primary_key=True),
        sa.Column("position_side", sa.String, primary_key=True),
        sa.Column("trade_id", sa.String, primary_key=True),
        sa.Column("order_id", sa.String, nullable=False),
        sa.Column("quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("price", sa.Numeric(38, 18), nullable=False),
        sa.Column("side", sa.String, nullable=False),
        sa.Column("payload_digest", sa.String, nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
    )
    table.metadata.create_all(engine)
    return engine, connection, table


def _row_params(identity: tuple[str, ...], digest: str) -> dict[str, object]:
    return {
        "environment": identity[0],
        "account_label": identity[1],
        "symbol": identity[2],
        "position_side": identity[3],
        "trade_id": identity[4],
        "order_id": identity[5],
        "quantity": Decimal(identity[6]),
        "price": Decimal(identity[7]),
        "side": identity[8],
        "first_seen_at": datetime.fromisoformat(identity[9]),
        "payload_digest": digest,
    }


def test_migration_repairs_only_the_verified_legacy_trade_digests() -> None:
    engine, connection, table = _identity_database()
    connection.execute(
        table.insert(),
        [
            _row_params(identity, identity[10])
            for identity in MIGRATION._LEGACY_WS_DIGEST_REPAIRS
        ],
    )

    MIGRATION._repair_legacy_ws_trade_digests(connection)

    actual = connection.execute(
        sa.text(
            "SELECT account_label, trade_id, payload_digest "
            "FROM execution_trade_identities ORDER BY account_label, trade_id"
        )
    ).all()
    expected = sorted(
        (identity[1], identity[4], identity[11])
        for identity in MIGRATION._LEGACY_WS_DIGEST_REPAIRS
    )
    assert actual == expected
    connection.close()
    engine.dispose()


def test_migration_stops_if_a_known_trade_has_an_unexpected_digest() -> None:
    engine, connection, table = _identity_database()
    account = MIGRATION._LEGACY_WS_DIGEST_REPAIRS[0]
    connection.execute(
        table.insert(),
        _row_params(account, "unexpected-digest"),
    )

    with pytest.raises(RuntimeError, match="legacy trade digest changed"):
        MIGRATION._repair_legacy_ws_trade_digests(connection)
    connection.close()
    engine.dispose()
