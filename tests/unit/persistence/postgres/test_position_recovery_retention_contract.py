"""Retention contract for superseded position-recovery state.

The pruner deletes production history, so its safety rules are asserted
directly against the statements it executes: active references (the newest
checkpoint per scope, every execution-head binding) are never candidates, only
covered state copies are deleted, every pass is bounded, and deletion requires
an explicit ``--apply``.
"""

import inspect
import pathlib
import re

from crypto_momentum_lab.tools import prune_position_recovery_history as pruner

SOURCE = pathlib.Path(inspect.getfile(pruner)).read_text()


def _function_body(name: str) -> str:
    source = inspect.getsource(getattr(pruner, name))
    return source


def test_protected_checkpoints_keep_newest_per_scope_and_head_bindings() -> None:
    body = _function_body("_prepare_protected_checkpoints")

    # Newest checkpoint per scope is retained (DISTINCT ON ... ORDER BY cut).
    assert "DISTINCT ON (" in body
    assert "event_cut DESC" in body
    # Every checkpoint currently bound by an execution head is retained.
    assert "execution_book_heads" in body
    assert "recovery_checkpoint" in body and "checkpoint_id" in body


def test_checkpoint_deletion_rechecks_active_head_bindings() -> None:
    materialize = _function_body("_materialize_checkpoint_candidates")
    delete = _function_body("_delete_checkpoint_batches")

    # Candidate materialization excludes protected and head-bound checkpoints.
    assert "protected_recovery_checkpoints" in materialize
    assert "execution_book_heads" in materialize
    assert "NOT EXISTS" in materialize
    # The delete re-checks the head binding under the row lock, so a head that
    # adopts the checkpoint between materialization and deletion is safe.
    assert "execution_book_heads" in delete
    assert "NOT EXISTS" in delete
    assert "position_recovery_checkpoint_candidates" in delete


def test_only_covered_state_copies_are_eligible_for_deletion() -> None:
    assert set(pruner._COMPACTABLE_KINDS) == {
        "facts_state",
        "snapshot",
        "coverage",
        "fill_load_provenance",
    }
    body = _function_body("_materialize_state_event_candidates")

    # Eligibility is a covered cut: retained checkpoint cut and revision.
    assert "retained_recovery_checkpoint_cuts" in body
    assert "events.occurred_at <= checkpoints.event_cut" in body
    assert "events.source_revision <= checkpoints.source_revision" in body
    # One old row per scope and kind survives as retained evidence.
    assert "occurred_order_rank > 1" in body
    assert "revision_order_rank > 1" in body
    # Immutable kinds are never in the eligible set.
    assert "event_kind = ANY($2::text[])" in body
    for immutable in ("fill", "boundary", "conflict", "integrity"):
        assert immutable not in pruner._COMPACTABLE_KINDS


def test_every_deletion_pass_is_bounded_and_locked() -> None:
    delete_checkpoints = _function_body("_delete_checkpoint_batches")
    delete_events = _function_body("_delete_state_event_batches")

    for body in (delete_checkpoints, delete_events):
        assert "LIMIT $1" in body
        assert "FOR UPDATE SKIP LOCKED" in body
        assert "batch_size" in body

    run = _function_body("_run")
    assert "pg_try_advisory_lock" in run
    assert "pg_advisory_unlock" in run
    assert "statement_timeout" in run


def test_dry_run_is_the_default_operation() -> None:
    main_source = inspect.getsource(pruner.main)

    # The caller must choose an operation explicitly; nothing deletes by default.
    assert 'operation = parser.add_mutually_exclusive_group(required=True)' in (
        main_source
    )
    assert '"--dry-run", action="store_true"' in main_source
    assert '"--apply", action="store_true"' in main_source
    run = _function_body("_run")
    # Both destructive steps are guarded by "not dry_run".
    assert run.count("if args.dry_run:") >= 3
    assert run.count("await _delete_checkpoint_batches(") == 1
    assert run.count("await _delete_state_event_batches(") == 1


def test_limits_are_positive_cli_options() -> None:
    main_source = inspect.getsource(pruner.main)

    for option in (
        "--minimum-age-hours",
        "--max-checkpoints",
        "--max-events",
        "--batch-size",
    ):
        assert option in main_source
    # No unbounded default: each option carries a numeric default.
    assert len(re.findall(r'default=\d+', main_source)) >= 4
