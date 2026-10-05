"""Normalize historical execution-head empty collections.

Revision ID: 20261005_0049
Revises: 20261005_0048
Create Date: 2026-10-05
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20261005_0049"
down_revision: str | None = "20261005_0048"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Execution heads written before recovery-command and external-recovery
    # tracking existed omit these keys. Both represent sets, so absence has one
    # unambiguous durable meaning: an empty set. Reject any other malformed
    # representation instead of guessing at execution state.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM execution_book_heads
                WHERE (
                    state_payload ? 'recovery_command_ids'
                    AND jsonb_typeof(state_payload -> 'recovery_command_ids')
                        NOT IN ('array', 'null')
                )
                OR (
                    state_payload ? 'external_recovery_ids'
                    AND jsonb_typeof(state_payload -> 'external_recovery_ids')
                        NOT IN ('array', 'null')
                )
            ) THEN
                RAISE EXCEPTION
                    'execution_book_heads has malformed recovery identity collections';
            END IF;
        END $$;
        """
    )
    op.execute(
        """
        UPDATE execution_book_heads
        SET state_payload = jsonb_set(
            jsonb_set(
                state_payload,
                '{recovery_command_ids}',
                CASE
                    WHEN jsonb_typeof(state_payload -> 'recovery_command_ids') = 'array'
                        THEN state_payload -> 'recovery_command_ids'
                    ELSE '[]'::jsonb
                END,
                true
            ),
            '{external_recovery_ids}',
            CASE
                WHEN jsonb_typeof(state_payload -> 'external_recovery_ids') = 'array'
                    THEN state_payload -> 'external_recovery_ids'
                ELSE '[]'::jsonb
            END,
            true
        )
        WHERE NOT (state_payload ? 'recovery_command_ids')
           OR jsonb_typeof(state_payload -> 'recovery_command_ids') = 'null'
           OR NOT (state_payload ? 'external_recovery_ids')
           OR jsonb_typeof(state_payload -> 'external_recovery_ids') = 'null';
        """
    )


def downgrade() -> None:
    # Older runtimes ignore the added JSON keys. Preserve the normalized
    # execution history rather than discarding a durable migration.
    pass
