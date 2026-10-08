import pytest

from crypto_momentum_lab.persistence.postgres.decision_trace_storage import (
    share_policy_states,
)
from deploy.ops.archive_table import validate_decision_archive_row
from tests.unit.decision.test_decision_state_storage import large_payload


def test_archive_requires_exact_shared_state_and_market_identities_before_pruning():
    payload, states = share_policy_states(large_payload())
    row = {
        "trace_payload": payload,
        "archived_policy_states": states,
        "evaluated_revision_ids": ["revision"],
        "archived_market_refs": [{"revision_id": "revision"}],
    }
    validate_decision_archive_row(row)
    with pytest.raises(ValueError, match="referenced policy state"):
        validate_decision_archive_row({**row, "archived_policy_states": {}})
    with pytest.raises(ValueError, match="market identities"):
        validate_decision_archive_row({**row, "archived_market_refs": []})
    digest = next(iter(states))
    with pytest.raises(ValueError, match="digest mismatch"):
        validate_decision_archive_row(
            {**row, "archived_policy_states": {digest: {"wrong": True}}}
        )
