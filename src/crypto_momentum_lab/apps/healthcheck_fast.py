"""Low-overhead container readiness checks backed by PostgreSQL."""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import psycopg

_DEFAULT_MAX_AGE_SECONDS = 180.0


class _Cursor(Protocol):
    def execute(
        self,
        statement: str,
        params: tuple[object, ...] = (),
    ) -> object: ...

    def fetchone(self) -> tuple[object, ...] | None: ...


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

    try:
        with psycopg.connect(
            _sync_database_url(database_url),
            autocommit=True,
            connect_timeout=2,
            options="-c statement_timeout=1000",
        ) as connection:
            with connection.cursor() as cursor:
                return _check_service(
                    cursor,
                    service=args.service,
                    max_age_seconds=max_age_seconds,
                    market_environment=args.market_environment,
                    account_label=args.account_label,
                    session_id=(
                        args.session_id
                        or os.environ.get("CML_HEALTHCHECK_RUN_ID", "")
                    ),
                    lease_owner=args.lease_owner,
                )
    except (OSError, ValueError, psycopg.Error):
        return 1


def _check_service(
    cursor: _Cursor,
    *,
    service: str,
    max_age_seconds: float | None,
    market_environment: str,
    account_label: str,
    session_id: str,
    lease_owner: str,
) -> int:
    if service == "market-data":
        return _exit_code(
            _market_data_ready(
                cursor,
                max_age_seconds=max_age_seconds,
                not_before=_process_started_at(),
                environment=market_environment,
            )
        )
    if service == "execution-account":
        return _exit_code(
            _execution_account_ready(
                cursor,
                account_label=account_label,
                max_age_seconds=max_age_seconds,
            )
        )
    if service == "live":
        if not session_id.strip():
            return 1
        return _exit_code(
            _live_ready(
                cursor,
                account_label=account_label,
                session_id=session_id,
                lease_owner=lease_owner,
                max_age_seconds=max_age_seconds,
                not_before=_process_started_at(),
            )
        )

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
    return _exit_code(
        all(
            _paper_ready(
                cursor,
                run_id=run_id,
                max_age_seconds=max_age_seconds,
            )
            for run_id in run_ids
        )
    )


def _exit_code(ready: bool) -> int:
    return 0 if ready else 1


def _market_data_ready(
    cursor: _Cursor,
    *,
    max_age_seconds: float | None,
    not_before: datetime | None,
    environment: str,
) -> bool:
    process = _fetchone(
        cursor,
        """
        SELECT state, occurred_at
        FROM market_data_process_states
        ORDER BY occurred_at DESC, state_id DESC
        LIMIT 1
        """,
    )
    if process is None or process[0] not in {"ready", "degraded"}:
        return False
    occurred_at = _datetime_or_none(process[1])
    if not_before is not None and (
        occurred_at is None
        or _as_utc(occurred_at) < not_before.astimezone(UTC)
    ):
        return False
    latest_state = _fetchone(
        cursor,
        """
        SELECT bucket_end
        FROM runtime_market_states_15s
        WHERE environment = %s
        ORDER BY bucket_start DESC
        LIMIT 1
        """,
        (environment,),
    )
    return latest_state is not None and _fresh(
        _datetime_or_none(latest_state[0]),
        max_age_seconds=max_age_seconds,
    )


def _paper_ready(
    cursor: _Cursor,
    *,
    run_id: str,
    max_age_seconds: float | None,
) -> bool:
    checkpoint = _fetchone(
        cursor,
        """
        SELECT saved_at
        FROM strategy_runtime_checkpoints
        WHERE run_id = %s
        """,
        (run_id,),
    )
    return checkpoint is not None and _fresh(
        _datetime_or_none(checkpoint[0]),
        max_age_seconds=max_age_seconds,
    )


def _live_ready(
    cursor: _Cursor,
    *,
    account_label: str,
    session_id: str,
    lease_owner: str,
    max_age_seconds: float | None,
    not_before: datetime | None,
) -> bool:
    transition = _fetchone(
        cursor,
        """
        SELECT state, occurred_at
        FROM live_session_transitions
        WHERE session_id = %s
        ORDER BY occurred_at DESC
        LIMIT 1
        """,
        (session_id,),
    )
    if transition is None or transition[0] not in {"live_enabled", "draining"}:
        return False
    occurred_at = _datetime_or_none(transition[1])
    if not_before is not None and (
        occurred_at is None
        or _as_utc(occurred_at) < not_before.astimezone(UTC)
    ):
        return False
    lease = _fetchone(
        cursor,
        """
        SELECT lease_id
        FROM trading_leases
        WHERE environment = 'live'
          AND account_label = %s
          AND owner = %s
          AND state = 'active'
          AND expires_at > %s
        ORDER BY expires_at DESC
        LIMIT 1
        """,
        (account_label, lease_owner, datetime.now(UTC)),
    )
    if lease is None:
        return False
    checkpoint = _fetchone(
        cursor,
        """
        SELECT saved_at
        FROM strategy_runtime_checkpoints
        WHERE run_id = %s
        """,
        (session_id,),
    )
    return checkpoint is not None and _fresh(
        _datetime_or_none(checkpoint[0]),
        max_age_seconds=max_age_seconds,
    )


def _execution_account_ready(
    cursor: _Cursor,
    *,
    account_label: str,
    max_age_seconds: float | None,
) -> bool:
    process = _fetchone(
        cursor,
        """
        SELECT state, occurred_at
        FROM execution_account_process_states
        WHERE environment = 'live' AND account_label = %s
        ORDER BY occurred_at DESC, state_id DESC
        LIMIT 1
        """,
        (account_label,),
    )
    return bool(
        process is not None
        and process[0] == "ready_readonly"
        and _fresh(
            _datetime_or_none(process[1]),
            max_age_seconds=max_age_seconds,
        )
    )


def _fetchone(
    cursor: _Cursor,
    statement: str,
    params: tuple[object, ...] = (),
) -> tuple[object, ...] | None:
    cursor.execute(statement, params)
    return cursor.fetchone()


def _fresh(
    observed_at: datetime | None,
    *,
    max_age_seconds: float | None,
) -> bool:
    if observed_at is None:
        return False
    if max_age_seconds is None:
        return True
    age_seconds = (datetime.now(UTC) - _as_utc(observed_at)).total_seconds()
    return age_seconds <= max_age_seconds


def _datetime_or_none(value: object) -> datetime | None:
    return value if isinstance(value, datetime) else None


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
    for prefix in ("postgresql+asyncpg://", "postgresql+psycopg://"):
        if database_url.startswith(prefix):
            return "postgresql://" + database_url[len(prefix) :]
    return database_url


if __name__ == "__main__":
    raise SystemExit(main())
