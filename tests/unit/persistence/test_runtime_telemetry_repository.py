from datetime import UTC, datetime

from sqlalchemy.dialects import postgresql

from crypto_momentum_lab.persistence.postgres.runtime_telemetry_repository import (
    PostgresRuntimeTelemetryRepository,
)


class _AsyncContext:
    def __init__(self, value):
        self._value = value

    async def __aenter__(self):
        return self._value

    async def __aexit__(self, *_args):
        return None


class _Session:
    def __init__(self) -> None:
        self.statements: list[object] = []

    def begin(self) -> _AsyncContext:
        return _AsyncContext(self)

    async def execute(self, statement) -> None:
        self.statements.append(statement)


class _SessionFactory:
    def __init__(self, session: _Session) -> None:
        self._session = session

    def __call__(self) -> _AsyncContext:
        return _AsyncContext(self._session)


async def test_runtime_telemetry_uses_non_durable_observability_commit() -> None:
    session = _Session()
    repository = PostgresRuntimeTelemetryRepository(_SessionFactory(session))

    await repository.save_runtime_events(
        (
            {
                "event_id": "event-1",
                "run_id": "run-1",
                "event_type": "candidate_accepted",
                "occurred_at": datetime(2026, 8, 23, tzinfo=UTC),
                "symbol": "BTCUSDT",
                "bucket_start": None,
                "details": {"lane": "entry"},
            },
        )
    )

    assert session.statements[0].text == "SET LOCAL synchronous_commit = OFF"
    compiled = str(
        session.statements[1].compile(dialect=postgresql.dialect())
    )
    assert "ON CONFLICT (event_id) DO NOTHING" in compiled
