import base64
import hashlib
import json
import zlib

import pytest

from crypto_momentum_lab.domain.market.market_book import UnreproducibleError
from crypto_momentum_lab.persistence.postgres.decision_trace_storage import (
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

def _legacy_compressed_payload(payload):
    states = {"prior": payload["prior_policy_state"]}
    if payload["next_policy_state"] != payload["prior_policy_state"]:
        states["next"] = payload["next_policy_state"]
    raw = json.dumps(
        states, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    stored = dict(payload)
    stored.pop("prior_policy_state")
    stored.pop("next_policy_state")
    stored["compressed_policy_states"] = {
        "codec": "zlib-base64-v1",
        "sha256": hashlib.sha256(raw).hexdigest(),
        "data": base64.b64encode(zlib.compress(raw, level=6)).decode("ascii"),
    }
    return stored

def test_legacy_compressed_state_decodes_without_mutating_source():
    payload = large_payload()
    stored = _legacy_compressed_payload(payload)

    assert expand_trace_payload(stored) == payload
    assert "prior_policy_state" in payload

def test_legacy_compressed_distinct_states_and_output_decode_exactly():
    payload = large_payload()
    payload["next_policy_state"] = {"serialization_version": 1, "policy_version": 2}
    payload["output_intent"] = {"candidate_id": "order-1"}

    assert expand_trace_payload(_legacy_compressed_payload(payload)) == payload

def test_state_codec_rejects_corruption():
    stored = _legacy_compressed_payload(large_payload())
    stored["compressed_policy_states"]["sha256"] = "0" * 64

    with pytest.raises(UnreproducibleError):
        expand_trace_payload(stored)
