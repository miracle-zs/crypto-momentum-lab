from dataclasses import replace
from unittest.mock import patch

from crypto_momentum_lab.domain.execution.recovery_models import AccountFacts
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    _fact_event_specs,
    _facts_from_rows,
)
from tests.unit.persistence.postgres.test_checkpoint_snapshot_recovery import (
    _checkpoint,
    _row,
)


def facts_at_checkpoint():
    checkpoint = _checkpoint()
    return AccountFacts(
        position_key=checkpoint.key,
        stream_scope=checkpoint.stream_scope,
        recovery_checkpoint=checkpoint,
        has_synthetic_fills=checkpoint.has_synthetic_fills,
        has_late_events=checkpoint.has_late_events,
        integrity_issues=checkpoint.integrity_issues,
    )


def test_current_checkpoint_covers_identical_state_without_duplicate_event():
    facts = facts_at_checkpoint()
    assert "facts_state" not in {
        spec[0]
        for spec in _fact_event_specs(
            facts, revision=facts.recovery_checkpoint.source_revision
        )
    }


def test_changed_flags_after_checkpoint_still_append_state():
    facts = replace(facts_at_checkpoint(), has_late_events=True)
    assert "facts_state" in {
        spec[0]
        for spec in _fact_event_specs(
            facts, revision=facts.recovery_checkpoint.source_revision
        )
    }


def test_older_checkpoint_cannot_suppress_current_state():
    facts = facts_at_checkpoint()
    assert "facts_state" in {
        spec[0]
        for spec in _fact_event_specs(
            facts, revision=facts.recovery_checkpoint.source_revision + 1
        )
    }


def test_checkpoint_state_restores_without_journal_copy():
    checkpoint = replace(
        _checkpoint(), has_late_events=True, integrity_issues=("source uncertain",)
    )
    facts, _, _, _ = _facts_from_rows(
        scope=checkpoint.stream_scope,
        rows=[],
        checkpoint=checkpoint,
    )
    assert facts.has_late_events is True
    assert facts.integrity_issues == checkpoint.integrity_issues


def test_covered_state_does_not_rehash_entire_fact_history():
    facts = facts_at_checkpoint()
    with patch.object(AccountFacts, "compute_facts_hash", side_effect=AssertionError):
        _fact_event_specs(facts, revision=facts.recovery_checkpoint.source_revision)


def test_old_prefix_state_cannot_override_new_checkpoint_flags():
    checkpoint = replace(_checkpoint(), has_synthetic_fills=True)
    old = _row(
        checkpoint,
        "facts_state",
        "old-state",
        checkpoint.event_cut,
        {
            "schema_version": 1,
            "has_synthetic_fills": False,
            "has_late_events": False,
            "integrity_issues": [],
        },
    )
    old.source_revision = checkpoint.source_revision - 1
    facts, _, _, _ = _facts_from_rows(
        scope=checkpoint.stream_scope,
        rows=[old],
        checkpoint=checkpoint,
    )
    assert facts.has_synthetic_fills is True


def test_state_event_after_checkpoint_still_overrides_flags():
    checkpoint = _checkpoint()
    newer = _row(
        checkpoint,
        "facts_state",
        "new-state",
        checkpoint.event_cut,
        {
            "schema_version": 1,
            "has_synthetic_fills": True,
            "has_late_events": False,
            "integrity_issues": [],
        },
    )
    facts, _, _, _ = _facts_from_rows(
        scope=checkpoint.stream_scope,
        rows=[newer],
        checkpoint=checkpoint,
    )
    assert facts.has_synthetic_fills is True
