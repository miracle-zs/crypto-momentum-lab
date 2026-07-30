"""Container readiness checks backed by the service's durable heartbeat."""

import argparse
import os
from datetime import UTC, datetime

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

_DEFAULT_MAX_AGE_SECONDS = 180.0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--service",
        choices=("market-data", "paper"),
        required=True,
    )
    parser.add_argument(
        "--max-age-seconds",
        type=float,
        default=_DEFAULT_MAX_AGE_SECONDS,
    )
    args = parser.parse_args()
    database_url = os.environ.get("CML_DATABASE_URL")
    if not database_url or args.max_age_seconds <= 0:
        return 1

    engine = create_engine(_sync_database_url(database_url), pool_pre_ping=True)
    try:
        with engine.connect() as connection:
            if args.service == "market-data":
                return 0 if _market_data_ready(
                    connection,
                    max_age_seconds=args.max_age_seconds,
                ) else 1
            configured_run_ids = os.environ.get("CML_HEALTHCHECK_RUN_IDS")
            if configured_run_ids:
                run_ids = tuple(
                    item.strip()
                    for item in configured_run_ids.split(",")
                    if item.strip()
                )
            else:
                run_id = os.environ.get("CML_HEALTHCHECK_RUN_ID")
                run_ids = () if not run_id else (run_id,)
            if not run_ids:
                return 1
            return 0 if all(
                _paper_ready(
                    connection,
                    run_id=run_id,
                    max_age_seconds=args.max_age_seconds,
                )
                for run_id in run_ids
            ) else 1
    except SQLAlchemyError:
        return 1
    finally:
        engine.dispose()


def _market_data_ready(
    connection: Connection,
    *,
    max_age_seconds: float,
) -> bool:
    process = connection.execute(
        text(
            "SELECT state, occurred_at "
            "FROM market_data_process_states "
            "ORDER BY occurred_at DESC, state_id DESC LIMIT 1"
        )
    ).mappings().first()
    if process is None or process["state"] not in {"ready", "degraded"}:
        return False
    latest_state_at = connection.execute(
        text(
            "SELECT bucket_end "
            "FROM runtime_market_states_15s "
            "ORDER BY bucket_start DESC LIMIT 1"
        )
    ).scalar_one_or_none()
    return _fresh(latest_state_at, max_age_seconds=max_age_seconds)


def _paper_ready(
    connection: Connection,
    *,
    run_id: str,
    max_age_seconds: float,
) -> bool:
    checkpoint_at = connection.execute(
        text(
            "SELECT saved_at FROM strategy_runtime_checkpoints "
            "WHERE run_id = :run_id"
        ),
        {"run_id": run_id},
    ).scalar_one_or_none()
    return _fresh(checkpoint_at, max_age_seconds=max_age_seconds)


def _fresh(observed_at: datetime | None, *, max_age_seconds: float) -> bool:
    if observed_at is None:
        return False
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        observed_at = observed_at.replace(tzinfo=UTC)
    age_seconds = (datetime.now(UTC) - observed_at.astimezone(UTC)).total_seconds()
    return age_seconds <= max_age_seconds


def _sync_database_url(database_url: str) -> str:
    return database_url.replace(
        "postgresql+asyncpg://",
        "postgresql+psycopg://",
        1,
    )


if __name__ == "__main__":
    raise SystemExit(main())
