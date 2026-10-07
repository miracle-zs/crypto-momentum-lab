from __future__ import annotations

from crypto_momentum_lab.tools.compact_normal_hold_evidence import (
    build_revision_purge_candidate_sql,
    build_summary_payload,
)


def test_build_summary_payload_preserves_identity_and_digests() -> None:
    original = {
        "input_hash": "input-1",
        "frame_digest": "frame-1",
        "market_state": {"symbols": ["BTCUSDT"] * 100},
    }
    refs = [
        {
            "scope": "live",
            "symbol": "BTCUSDT",
            "interval": "15s",
            "bucket_start": "2026-10-05T00:00:00+00:00",
            "bucket_end": "2026-10-05T00:00:15+00:00",
            "revision_id": "revision-1",
            "content_hash": "content-1",
            "published_at": "2026-10-05T00:00:15+00:00",
            "source_epoch": "epoch-1",
            "visibility_mode": "decision_visible",
            "observed_at": "",
        }
    ]

    summary = build_summary_payload(original, refs)

    assert summary == {
        "evidence_level": "summary",
        "summary_schema_version": 1,
        "frame_digest": "frame-1",
        "input_hash": "input-1",
        "original_payload_sha256": (
            "65031c321aea879fa67a021cf10f07ff6e065acd4393e256bc66f0ae8dadea64"
        ),
        "outcome": "holding_position_no_exit",
        "market_refs": refs,
    }


def test_revision_purge_query_keeps_live_full_evidence_and_honours_age_cutoff() -> None:
    sql = build_revision_purge_candidate_sql(
        batch_size_parameter="$2",
        older_than_parameter="$1",
    )

    assert "revisions.published_at < $1" in sql
    assert "LIMIT $2" in sql
    assert "protected.revision_id IS NULL" in sql
    assert "FROM decision_traces AS traces" in sql
    assert "traces.evaluated_revision_ids ? revisions.revision_id" in sql
    assert "FROM dataset_manifests AS manifests" in sql
    assert "manifests.revision_ids ? revisions.revision_id" in sql


def test_revision_purge_count_query_can_omit_the_batch_limit() -> None:
    sql = build_revision_purge_candidate_sql(
        batch_size_parameter=None,
        older_than_parameter=None,
    )

    assert "LIMIT" not in sql
