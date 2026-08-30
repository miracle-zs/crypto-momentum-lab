from datetime import UTC, datetime, timedelta

from crypto_momentum_lab.apps import healthcheck_fast


class _Cursor:
    def __init__(self, *rows) -> None:
        self._rows = iter(rows)

    def execute(self, statement, params=()):
        del statement, params

    def fetchone(self):
        return next(self._rows)


def test_market_data_readiness_preserves_process_and_state_checks() -> None:
    now = datetime.now(UTC)
    cursor = _Cursor(
        ("ready", now),
        (now,),
    )

    assert healthcheck_fast._market_data_ready(
        cursor,
        max_age_seconds=30,
        not_before=now - timedelta(seconds=1),
        environment="research",
    )


def test_execution_account_can_skip_age_cutoff() -> None:
    cursor = _Cursor(("ready_readonly", datetime(2020, 1, 1, tzinfo=UTC)))

    assert healthcheck_fast._execution_account_ready(
        cursor,
        account_label="primary",
        max_age_seconds=None,
    )


def test_live_readiness_requires_lease_and_checkpoint() -> None:
    now = datetime.now(UTC)
    cursor = _Cursor(
        ("live_enabled", now),
        ("lease-1",),
        (now,),
    )

    assert healthcheck_fast._live_ready(
        cursor,
        account_label="primary",
        session_id="live-primary-v1",
        lease_owner="live-worker",
        max_age_seconds=30,
        not_before=now - timedelta(seconds=1),
    )


def test_sync_database_url_removes_async_driver_suffix() -> None:
    assert healthcheck_fast._sync_database_url(
        "postgresql+asyncpg://cml:secret@postgres:5432/cml"
    ) == "postgresql://cml:secret@postgres:5432/cml"
