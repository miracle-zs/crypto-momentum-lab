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
from crypto_momentum_lab.domain.decision.trace_audit import verify_decision_trace
from crypto_momentum_lab.persistence.postgres.decision_trace_repository import (
    PostgresDecisionTraceRepository,
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
