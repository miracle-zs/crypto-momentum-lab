from crypto_momentum_lab.config import resolve_database_url


def test_explicit_database_url_has_highest_priority(monkeypatch) -> None:
    monkeypatch.setenv("CML_MARKET_DATABASE_URL", "postgresql://market")
    monkeypatch.setenv("CML_DATABASE_URL", "postgresql://shared")

    assert resolve_database_url(
        "postgresql://explicit",
        "CML_MARKET_DATABASE_URL",
        "CML_DATABASE_URL",
    ) == "postgresql://explicit"


def test_database_url_uses_environment_variables_in_declared_order(
    monkeypatch,
) -> None:
    monkeypatch.setenv("CML_DATABASE_URL", "postgresql://shared")

    assert resolve_database_url(
        None,
        "CML_MARKET_DATABASE_URL",
        "CML_DATABASE_URL",
    ) == "postgresql://shared"


def test_database_url_returns_none_when_no_source_is_configured(monkeypatch) -> None:
    monkeypatch.delenv("CML_MARKET_DATABASE_URL", raising=False)
    monkeypatch.delenv("CML_DATABASE_URL", raising=False)

    assert (
        resolve_database_url(None, "CML_MARKET_DATABASE_URL", "CML_DATABASE_URL")
        is None
    )
