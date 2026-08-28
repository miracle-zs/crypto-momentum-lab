from crypto_momentum_lab.persistence.postgres import session


def test_dashboard_engine_bounds_query_memory_risk(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_create_async_engine(database_url: str, **kwargs: object):
        captured["database_url"] = database_url
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(session, "create_async_engine", fake_create_async_engine)

    result = session.create_dashboard_database_engine(
        "postgresql+asyncpg://dashboard"
    )

    assert result is not None
    assert captured["database_url"] == "postgresql+asyncpg://dashboard"
    assert captured["pool_size"] == 2
    assert captured["max_overflow"] == 0
    assert captured["pool_timeout"] == 3
    assert captured["pool_recycle"] == 900
    assert captured["connect_args"] == {"command_timeout": 10}


def test_execution_engine_uses_a_bounded_dedicated_pool(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_create_async_engine(database_url: str, **kwargs: object):
        captured["database_url"] = database_url
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(session, "create_async_engine", fake_create_async_engine)

    result = session.create_execution_database_engine("postgresql+asyncpg://exec")

    assert result is not None
    assert captured["database_url"] == "postgresql+asyncpg://exec"
    assert captured["pool_size"] == 4
    assert captured["max_overflow"] == 0
    assert captured["pool_timeout"] == 3
    assert captured["connect_args"] == {"command_timeout": 5}


def test_observability_engine_has_a_small_best_effort_pool(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_create_async_engine(database_url: str, **kwargs: object):
        captured["database_url"] = database_url
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(session, "create_async_engine", fake_create_async_engine)

    result = session.create_observability_database_engine(
        "postgresql+asyncpg://observability"
    )

    assert result is not None
    assert captured["database_url"] == "postgresql+asyncpg://observability"
    assert captured["pool_size"] == 1
    assert captured["max_overflow"] == 0
    assert captured["pool_timeout"] == 1
    assert captured["connect_args"] == {"command_timeout": 1}


def test_checkpoint_engine_isolated_from_best_effort_telemetry(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_create_async_engine(database_url: str, **kwargs: object):
        captured["database_url"] = database_url
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(session, "create_async_engine", fake_create_async_engine)

    result = session.create_checkpoint_database_engine(
        "postgresql+asyncpg://checkpoint"
    )

    assert result is not None
    assert captured["database_url"] == "postgresql+asyncpg://checkpoint"
    assert captured["pool_size"] == 1
    assert captured["max_overflow"] == 0
    assert captured["pool_timeout"] == 2
    assert captured["connect_args"] == {"command_timeout": 10}


def test_market_engine_has_a_bounded_read_pool(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_create_async_engine(database_url: str, **kwargs: object):
        captured["database_url"] = database_url
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(session, "create_async_engine", fake_create_async_engine)

    result = session.create_market_database_engine(
        "postgresql+asyncpg://market"
    )

    assert result is not None
    assert captured["database_url"] == "postgresql+asyncpg://market"
    assert captured["pool_size"] == 2
    assert captured["max_overflow"] == 0
    assert captured["pool_timeout"] == 2
    assert captured["connect_args"] == {"command_timeout": 5}
