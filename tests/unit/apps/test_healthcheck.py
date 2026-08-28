from datetime import UTC, datetime
from pathlib import Path

from crypto_momentum_lab.apps import healthcheck


class _Result:
    def __init__(self, value):
        self._value = value

    def mappings(self):
        return self

    def first(self):
        return self._value

    def scalar_one_or_none(self):
        return self._value


class _Connection:
    def __init__(self, *results) -> None:
        self._results = iter(results)

    def execute(self, statement, params=None):
        del statement, params
        return _Result(next(self._results))


def test_market_data_readiness_rejects_heartbeat_from_previous_process() -> None:
    now = datetime.now(UTC)
    connection = _Connection(
        {"state": "ready", "occurred_at": now},
        now,
    )

    assert not healthcheck._market_data_ready(
        connection,
        max_age_seconds=180,
        not_before=datetime.max.replace(tzinfo=UTC),
    )


def test_process_started_at_reads_linux_proc_stat(tmp_path: Path) -> None:
    process_root = tmp_path / "1"
    process_root.mkdir()
    fields = ["S", *("0" for _ in range(18)), "250"]
    (process_root / "stat").write_text(f"1 (market data) {' '.join(fields)}\n")
    (tmp_path / "stat").write_text("cpu 0 0 0 0\nbtime 1000\n")

    assert healthcheck._process_started_at(
        proc_root=tmp_path,
        clock_ticks_per_second=100,
    ) == datetime.fromtimestamp(1002.5, tz=UTC)


def test_execution_account_readiness_requires_fresh_ready_state() -> None:
    now = datetime.now(UTC)

    assert healthcheck._execution_account_ready(
        _Connection({"state": "ready_readonly", "occurred_at": now}),
        account_label="primary",
        max_age_seconds=30,
    )
    assert not healthcheck._execution_account_ready(
        _Connection({"state": "degraded", "occurred_at": now}),
        account_label="primary",
        max_age_seconds=30,
    )


def test_execution_account_readiness_can_skip_age_cutoff() -> None:
    old = datetime(2020, 1, 1, tzinfo=UTC)

    assert healthcheck._execution_account_ready(
        _Connection({"state": "ready_readonly", "occurred_at": old}),
        account_label="primary",
        max_age_seconds=None,
    )


def test_live_readiness_rejects_checkpoint_without_active_lease() -> None:
    connection = _Connection(
        {"state": "live_enabled"},
        None,
    )

    assert not healthcheck._live_ready(
        connection,
        account_label="primary",
        session_id="live-primary-v1",
        lease_owner="live-worker",
        max_age_seconds=None,
    )


def test_live_readiness_rejects_latest_halted_session() -> None:
    connection = _Connection(
        {"state": "halted"},
        {"lease_id": "lease-1"},
        datetime.now(UTC),
    )

    assert not healthcheck._live_ready(
        connection,
        account_label="primary",
        session_id="live-primary-v1",
        lease_owner="live-worker",
        max_age_seconds=None,
    )
