import json

import pytest

from crypto_momentum_lab.domain.market.market_book import UnreproducibleError
from crypto_momentum_lab.persistence.postgres.decision_trace_storage import (
    compact_trace_for_hot_storage,
    expand_trace_payload,
)


def large_payload():
    state = {
        "serialization_version": 1,
        "sizing_state": {
            f"COIN{i}USDT": {
                "symbol": f"COIN{i}USDT",
                "quantized_quantity": str(i + 1),
                "sizing_timestamp": "2026-10-08T14:00:00+00:00",
                "features": {"reason": "sizing" * 50},
            }
            for i in range(143)
        },
    }
    return {
        "trace_schema_version": 1,
        "prior_policy_state": state,
        "next_policy_state": state,
        "input_hash": "input",
        "frame_digest": "frame",
    }


def test_unchanged_large_no_candidate_state_is_lossless_and_small():
    payload = large_payload()
    stored = compact_trace_for_hot_storage(
        intent_produced=False, rejection_reason="no_candidate", trace_payload=payload
    )
    assert len(json.dumps(stored)) < len(json.dumps(payload)) / 10
    assert stored.get("evidence_level") != "summary"
    assert expand_trace_payload(stored) == payload
    assert "prior_policy_state" in payload  # never mutate the domain trace


def test_changed_state_and_order_output_remain_exact():
    payload = large_payload()
    payload["next_policy_state"] = {"serialization_version": 1, "policy_version": 2}
    payload["output_intent"] = {"candidate_id": "order-1"}
    stored = compact_trace_for_hot_storage(
        intent_produced=True, rejection_reason=None, trace_payload=payload
    )
    assert expand_trace_payload(stored) == payload


def test_state_codec_rejects_corruption():
    stored = compact_trace_for_hot_storage(
        intent_produced=False,
        rejection_reason="no_candidate",
        trace_payload=large_payload(),
    )
    stored["compressed_policy_states"]["sha256"] = "0" * 64
    with pytest.raises(UnreproducibleError):
        expand_trace_payload(stored)
