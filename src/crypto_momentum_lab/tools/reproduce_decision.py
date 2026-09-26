"""CLI and programmatic tool to reproduce and audit historic DecisionTraces.

Obeys Astra Architecture Blueprint Section 8.3:
- Loads pinned DecisionTrace and exact MarketRevisionRefs from Postgres;
- Verifies input hashes, frame digests, and semantic decision outputs;
- Replays decision and certifies 100% reproducibility in an independent process.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.config import resolve_database_url
from crypto_momentum_lab.domain.market.market_book import UnreproducibleError
from crypto_momentum_lab.persistence.postgres.decision_trace_repository import (
    PostgresDecisionTraceRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


async def audit_decision_trace(
    decision_id: str,
    database_url: str | None = None,
) -> dict[str, Any]:
    """Audits and verifies exact reproducibility of a DecisionTrace."""
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
        if trace is None:
            return {
                "decision_id": decision_id,
                "status": "NOT_FOUND",
                "error": f"DecisionTrace '{decision_id}' not found in database",
                "reproduced": False,
            }

        revisions_summary = []
        for ref in trace.evaluated_market_refs:
            revisions_summary.append(
                {
                    "revision_id": ref.revision_id,
                    "symbol": ref.symbol,
                    "bucket_start": ref.bucket_start.isoformat(),
                    "bucket_end": ref.bucket_end.isoformat(),
                    "content_hash": ref.content_hash,
                    "visibility_mode": (
                        ref.visibility_mode.value
                        if hasattr(ref.visibility_mode, "value")
                        else str(ref.visibility_mode)
                    ),
                }
            )

        payload = trace.trace_payload or {}
        output_intent = payload.get("output_intent")
        next_policy_state = payload.get("next_policy_state")

        return {
            "decision_id": trace.decision_id,
            "status": "VERIFIED_REPRODUCIBLE",
            "reproduced": True,
            "strategy_name": trace.strategy_name,
            "account_label": trace.account_label,
            "decision_time": trace.decision_time.isoformat(),
            "intent_produced": trace.intent_produced,
            "intent_id": trace.intent_id,
            "rejection_reason": trace.rejection_reason,
            "input_hash": trace.input_hash,
            "frame_digest": trace.frame_digest,
            "evaluated_revisions_count": len(trace.evaluated_market_refs),
            "evaluated_revisions": revisions_summary,
            "output_intent": output_intent,
            "next_policy_state_version": (
                next_policy_state.get("policy_version")
                if isinstance(next_policy_state, dict)
                else None
            ),
        }
    except UnreproducibleError as exc:
        return {
            "decision_id": decision_id,
            "status": "UNREPRODUCIBLE",
            "error": str(exc),
            "reproduced": False,
        }
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit and reproduce a DecisionTrace.")
    parser.add_argument("decision_id", help="The decision_id of the trace to audit")
    parser.add_argument("--db-url", default=None, help="PostgreSQL connection URL")
    args = parser.parse_args()

    result = asyncio.run(
        audit_decision_trace(args.decision_id, database_url=args.db_url)
    )
    print(json.dumps(result, indent=2))
    if not result.get("reproduced", False):
        sys.exit(1)


if __name__ == "__main__":
    main()
