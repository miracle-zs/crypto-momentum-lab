"""Container readiness checks backed by the service's durable heartbeat."""

import argparse
import os
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

_DEFAULT_MAX_AGE_SECONDS = 180.0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--service",
        choices=("market-data", "paper", "execution-account", "live"),
        required=True,
    )
    parser.add_argument(
        "--max-age-seconds",
        type=float,
        default=_DEFAULT_MAX_AGE_SECONDS,
    )
    parser.add_argument(
        "--ignore-age",
        action="store_true",
        help="only require a ready heartbeat; do not apply an age cutoff",
    )
    parser.add_argument("--market-environment", default="research")
    parser.add_argument("--account-label", default="primary")
    parser.add_argument("--session-id", default="")
    parser.add_argument("--lease-owner", default="live-worker")
    args = parser.parse_args()
    database_url = os.environ.get("CML_DATABASE_URL")
    if not database_url or (
        not args.ignore_age and args.max_age_seconds <= 0
    ):
        return 1
    max_age_seconds = None if args.ignore_age else args.max_age_seconds

    engine = create_engine(_sync_database_url(database_url), pool_pre_ping=True)
    try:
        with engine.connect() as connection:
            if args.service == "market-data":
                return 0 if _market_data_ready(
                    connection,
                    max_age_seconds=max_age_seconds,
                    not_before=_process_started_at(),
                    environment=args.market_environment,
                ) else 1
            if args.service == "execution-account":
                return 0 if _execution_account_ready(
                    connection,
                    account_label=args.account_label,
                    max_age_seconds=max_age_seconds,
                ) else 1
            if args.service == "live":
                session_id = args.session_id or os.environ.get(
                    "CML_HEALTHCHECK_RUN_ID", ""
                )
                if not session_id.strip():
                    return 1
                return 0 if _live_ready(
                    connection,
                    account_label=args.account_label,
                    session_id=session_id,
                    lease_owner=args.lease_owner,
                    max_age_seconds=max_age_seconds,
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
                    max_age_seconds=max_age_seconds,
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
    max_age_seconds: float | None,
    not_before: datetime | None = None,
    environment: str = "research",
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
    occurred_at = process["occurred_at"]
    if not_before is not None and (
        occurred_at is None
        or _as_utc(occurred_at) < not_before.astimezone(UTC)
    ):
        return False
    latest_state_at = connection.execute(
        text(
            "SELECT bucket_end "
            "FROM runtime_market_states_15s "
            "WHERE environment = :environment "
            "ORDER BY bucket_start DESC LIMIT 1"
        ),
        {"environment": environment},
    ).scalar_one_or_none()
    return _fresh(latest_state_at, max_age_seconds=max_age_seconds)


def _paper_ready(
    connection: Connection,
    *,
    run_id: str,
    max_age_seconds: float | None,
) -> bool:
    checkpoint_at = connection.execute(
        text(
            "SELECT saved_at FROM strategy_runtime_checkpoints "
            "WHERE run_id = :run_id"
        ),
        {"run_id": run_id},
    ).scalar_one_or_none()
    return _fresh(checkpoint_at, max_age_seconds=max_age_seconds)


def _live_ready(
    connection: Connection,
    *,
    account_label: str,
    session_id: str,
    lease_owner: str,
    max_age_seconds: float | None,
) -> bool:
    transition = connection.execute(
        text(
            "SELECT state FROM live_session_transitions "
            "WHERE session_id = :session_id "
            "ORDER BY occurred_at DESC LIMIT 1"
        ),
        {"session_id": session_id},
    ).mappings().first()
    if transition is None or transition["state"] not in {"live_enabled", "draining"}:
        return False
    lease = connection.execute(
        text(
            "SELECT lease_id FROM trading_leases "
            "WHERE environment = 'live' "
            "AND account_label = :account_label "
            "AND owner = :lease_owner "
            "AND state = 'active' "
            "AND expires_at > :now "
            "ORDER BY expires_at DESC LIMIT 1"
        ),
        {
            "account_label": account_label,
            "lease_owner": lease_owner,
            "now": datetime.now(UTC),
        },
    ).mappings().first()
    if lease is None:
        return False
    checkpoint_at = connection.execute(
        text(
            "SELECT saved_at FROM strategy_runtime_checkpoints "
            "WHERE run_id = :run_id"
        ),
        {"run_id": session_id},
    ).scalar_one_or_none()
    return _fresh(checkpoint_at, max_age_seconds=max_age_seconds)


def _execution_account_ready(
    connection: Connection,
    *,
    account_label: str,
    max_age_seconds: float | None,
) -> bool:
    process = connection.execute(
        text(
            "SELECT state, occurred_at "
            "FROM execution_account_process_states "
            "WHERE environment = 'live' AND account_label = :account_label "
            "ORDER BY occurred_at DESC, state_id DESC LIMIT 1"
        ),
        {"account_label": account_label},
    ).mappings().first()
    return bool(
        process is not None
        and process["state"] == "ready_readonly"
        and _fresh(process["occurred_at"], max_age_seconds=max_age_seconds)
    )


def _fresh(observed_at: datetime | None, *, max_age_seconds: float | None) -> bool:
    if observed_at is None:
        return False
    if max_age_seconds is None:
        return True
    age_seconds = (datetime.now(UTC) - _as_utc(observed_at)).total_seconds()
    return age_seconds <= max_age_seconds


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _process_started_at(
    *,
    pid: int = 1,
    proc_root: Path = Path("/proc"),
    clock_ticks_per_second: int | None = None,
) -> datetime | None:
    try:
        process_stat = (proc_root / str(pid) / "stat").read_text()
        fields_after_name = process_stat[process_stat.rfind(")") + 2 :].split()
        start_ticks = int(fields_after_name[19])
        boot_time = next(
            int(line.split()[1])
            for line in (proc_root / "stat").read_text().splitlines()
            if line.startswith("btime ")
        )
        ticks_per_second = (
            os.sysconf("SC_CLK_TCK")
            if clock_ticks_per_second is None
            else clock_ticks_per_second
        )
        return datetime.fromtimestamp(
            boot_time + start_ticks / ticks_per_second,
            tz=UTC,
        )
    except (IndexError, OSError, StopIteration, ValueError):
        return None


def _sync_database_url(database_url: str) -> str:
    return database_url.replace(
        "postgresql+asyncpg://",
        "postgresql+psycopg://",
        1,
    )


if __name__ == "__main__":
    raise SystemExit(main())
