import importlib.util
from pathlib import Path

import sqlalchemy as sa

from crypto_momentum_lab.next_gen.db_models import (
    LiveOrderRow,
    LivePositionBatchRow,
)

MIGRATION_PATH = (
    Path(__file__).parents[3]
    / "alembic"
    / "versions"
    / "20261018_0067_next_gen_execution_tables.py"
)
SPEC = importlib.util.spec_from_file_location(
    "next_gen_execution_tables_0067",
    MIGRATION_PATH,
)
assert SPEC is not None and SPEC.loader is not None
migration_0067 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(migration_0067)


def test_live_position_batch_row_model_structure() -> None:
    table = LivePositionBatchRow.__table__
    assert table.name == "live_position_batches"

    # Primary key
    pk_cols = [col.name for col in table.primary_key.columns]
    assert pk_cols == ["environment", "account_label", "batch_id"]

    # Check constraints
    # (SQLAlchemy Base applies NAMING_CONVENTION prefix ck_<table>_<name>)
    ck_names = {
        ck.name for ck in table.constraints if isinstance(ck, sa.CheckConstraint)
    }
    assert any("live_batches_quantity_non_negative" in name for name in ck_names)
    assert any("live_batches_exiting_qty_non_negative" in name for name in ck_names)
    assert any("live_batches_status_check" in name for name in ck_names)

    # Indexes
    idx_names = {idx.name for idx in table.indexes}
    assert "idx_live_batches_acc_sym_status" in idx_names


def test_live_order_row_model_structure() -> None:
    table = LiveOrderRow.__table__
    assert table.name == "live_orders"

    # Primary key
    pk_cols = [col.name for col in table.primary_key.columns]
    assert pk_cols == ["environment", "account_label", "client_order_id"]

    # Check constraints
    ck_names = {
        ck.name for ck in table.constraints if isinstance(ck, sa.CheckConstraint)
    }
    assert any("live_orders_client_id_len_check" in name for name in ck_names)
    assert any("live_orders_status_check" in name for name in ck_names)

    # Indexes
    idx_names = {idx.name for idx in table.indexes}
    assert "idx_live_orders_acc_sym_status" in idx_names
    assert "idx_live_orders_acc_batch" in idx_names

    # Foreign Keys: must NOT have physical foreign key to avoid insert deadlock
    # on the first entry order before batch row is created.
    fk_constraints = [
        c for c in table.constraints if isinstance(c, sa.ForeignKeyConstraint)
    ]
    assert len(fk_constraints) == 0


def test_migration_0067_revision_chain() -> None:
    assert migration_0067.revision == "20261018_0067"
    assert migration_0067.down_revision == "20261015_0066"
