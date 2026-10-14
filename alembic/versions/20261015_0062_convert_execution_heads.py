"""Convert persisted execution heads to the current-position model.

Revision ID: 20261015_0062
Revises: 20261014_0061
"""

import json

import sqlalchemy as sa

from alembic import op
from crypto_momentum_lab.persistence.postgres.execution_head_migration import (
    convert_legacy_execution_head,
)

revision = "20261015_0062"
down_revision = "20261014_0061"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout='5s'")
    connection = op.get_bind()
    connection.execute(
        sa.text("LOCK TABLE execution_book_heads IN ACCESS EXCLUSIVE MODE")
    )

    unsupported = connection.execute(
        sa.text(
            """
            SELECT count(*)
            FROM execution_book_heads
            WHERE state_payload->>'schema_version' IS NULL
               OR state_payload->>'schema_version' NOT IN ('1', '4')
            """
        )
    ).scalar_one()
    if unsupported:
        raise RuntimeError(
            f"execution_book_heads contains {unsupported} unsupported schema versions"
        )

    legacy_rows = connection.execute(
        sa.text(
            """
            SELECT environment, account_label, symbol, position_side, state_payload
            FROM execution_book_heads
            WHERE state_payload->>'schema_version' = '1'
            """
        )
    ).mappings()
    for row in legacy_rows:
        converted = convert_legacy_execution_head(row["state_payload"])
        key = converted["position_key"]
        if key != {
            "environment": row["environment"],
            "account_label": row["account_label"],
            "symbol": row["symbol"],
            "position_side": row["position_side"],
        }:
            raise RuntimeError("execution head row identity does not match payload")
        result = connection.execute(
            sa.text(
                """
                UPDATE execution_book_heads
                SET state_payload = CAST(:payload AS jsonb)
                WHERE environment = :environment
                  AND account_label = :account_label
                  AND symbol = :symbol
                  AND position_side = :position_side
                """
            ),
            {
                "payload": json.dumps(converted, separators=(",", ":")),
                "environment": row["environment"],
                "account_label": row["account_label"],
                "symbol": row["symbol"],
                "position_side": row["position_side"],
            },
        )
        if result.rowcount != 1:
            raise RuntimeError("execution head conversion did not update one row")

    remaining = connection.execute(
        sa.text(
            """
            SELECT count(*) FROM execution_book_heads
            WHERE state_payload->>'schema_version' IS NULL
               OR state_payload->>'schema_version' <> '4'
            """
        )
    ).scalar_one()
    if remaining:
        raise RuntimeError(f"{remaining} execution heads remain outside schema 4")


def downgrade() -> None:
    raise RuntimeError(
        "Current execution heads cannot be downgraded to the legacy model"
    )
