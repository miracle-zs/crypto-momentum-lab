"""CLI and programmatic tool to reproduce and audit historic DecisionTraces.

Obeys Astra Architecture Blueprint Section 8.3:
- Loads pinned DecisionTrace and exact MarketRevisionRefs from Postgres;
- Verifies input hashes, frame digests, and semantic decision outputs;
- Replays decision and certifies 100% reproducibility in an independent process.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import io
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import zstandard
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.config import resolve_database_url
from crypto_momentum_lab.domain.decision.trace_audit import verify_decision_trace
from crypto_momentum_lab.domain.market.market_book import UnreproducibleError
from crypto_momentum_lab.domain.market.revision_models import DecisionTrace
from crypto_momentum_lab.persistence.postgres.decision_trace_repository import (
    PostgresDecisionTraceRepository,
)
from crypto_momentum_lab.persistence.postgres.decision_trace_storage import (
    load_summary_market_refs,
    resolve_policy_states,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


async def audit_decision_trace(
    decision_id: str,
    database_url: str | None = None,
    trace_override: Any | None = None,
) -> dict[str, Any]:
    """Audits and verifies exact reproducibility of a DecisionTrace."""
    if trace_override is not None:
        trace = trace_override
    else:
        url = resolve_database_url(
            database_url,
            "CML_OBSERVABILITY_DATABASE_URL",
            "CML_DATABASE_URL",
        )
        if not url:
            raise ValueError(
                "Database URL must be provided or configured via CML_DATABASE_URL"
            )
        engine = create_async_database_engine(url, pool_size=1, max_overflow=0)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        repo = PostgresDecisionTraceRepository(session_factory)

        try:
            trace = await repo.load_decision_trace(decision_id)
        finally:
            await engine.dispose()
    return verify_decision_trace(trace, decision_id=decision_id)


def load_archived_decision_trace(
    manifest_path: Path, decision_id: str
) -> DecisionTrace | None:
    """Read verified cold evidence without restoring or writing to PostgreSQL."""
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("table") != "decision_traces" or manifest.get("format") != "jsonl":
        raise UnreproducibleError("Not a decision evidence archive manifest")
    name = manifest.get("file")
    if not isinstance(name, str) or Path(name).name != name:
        raise UnreproducibleError("Invalid decision archive file path")
    archive = manifest_path.parent / name
    with archive.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    if digest != manifest.get("sha256"):
        raise UnreproducibleError("Decision archive SHA-256 mismatch")
    with (
        archive.open("rb") as source,
        zstandard.ZstdDecompressor().stream_reader(source) as stream,
    ):
        for line in io.TextIOWrapper(stream, encoding="utf-8"):
            row = json.loads(line)
            if row["decision_id"] != decision_id:
                continue
            payload = resolve_policy_states(
                row["trace_payload"], row.get("archived_policy_states", {})
            )
            if payload.get("evidence_level") == "summary":
                refs = load_summary_market_refs(payload, decision_id=decision_id)
            else:
                raw_by_id = {r["revision_id"]: r for r in row["archived_market_refs"]}
                refs_payload = []
                for revision_id in row["evaluated_revision_ids"]:
                    raw = raw_by_id.get(revision_id)
                    if raw is None:
                        raise UnreproducibleError(
                            "Cold decision archive lacks a market reference"
                        )
                    refs_payload.append(
                        {
                            **raw,
                            "observed_at": raw.get("lineage", {}).get(
                                "observed_at", ""
                            ),
                        }
                    )
                refs = load_summary_market_refs(
                    {"market_refs": refs_payload}, decision_id=decision_id
                )
            return DecisionTrace(
                decision_id=decision_id,
                strategy_name=row["strategy_name"],
                account_label=row["account_label"],
                decision_time=datetime.fromisoformat(row["decision_time"]),
                evaluated_market_refs=tuple(refs),
                intent_produced=row["intent_produced"],
                intent_id=row["intent_id"],
                rejection_reason=row["rejection_reason"],
                input_hash=str(payload.get("input_hash", "")),
                frame_digest=str(payload.get("frame_digest", "")),
                trace_payload=payload,
            )
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit and reproduce a DecisionTrace.")
    parser.add_argument("decision_id", help="The decision_id of the trace to audit")
    parser.add_argument("--db-url", default=None, help="PostgreSQL connection URL")
    parser.add_argument(
        "--archive-manifest", type=Path, help="Verified cold decision JSONL manifest"
    )
    args = parser.parse_args()

    if args.archive_manifest:
        trace = load_archived_decision_trace(args.archive_manifest, args.decision_id)
        result = verify_decision_trace(trace, decision_id=args.decision_id)
    else:
        result = asyncio.run(
            audit_decision_trace(args.decision_id, database_url=args.db_url)
        )
    print(json.dumps(result, indent=2))
    if not result.get("reproduced", False):
        sys.exit(1)


if __name__ == "__main__":
    main()
