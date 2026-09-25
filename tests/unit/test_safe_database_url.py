import pytest
from tests.conftest import assert_safe_test_database_url


def test_safe_test_database_url_accepts_test_database_names() -> None:
    assert_safe_test_database_url("postgresql+asyncpg://cml:cml@localhost:54329/cml_test")
    assert_safe_test_database_url("postgresql+asyncpg://cml:cml@127.0.0.1:54329/cml_test")
    assert_safe_test_database_url("postgresql+asyncpg://cml:cml@localhost:5432/cml_test")
    assert_safe_test_database_url("postgresql+asyncpg://cml:cml@127.0.0.1:5432/cml_review_20260925")


def test_safe_test_database_url_rejects_remote_hosts() -> None:
    with pytest.raises(RuntimeError, match="non-local database host"):
        assert_safe_test_database_url("postgresql+asyncpg://root:secret@43.167.191.253:5432/cml_test")

    with pytest.raises(RuntimeError, match="non-local database host"):
        assert_safe_test_database_url("postgresql+asyncpg://user:secret@db.internal.corp:5432/test_db")


def test_safe_test_database_url_rejects_production_names() -> None:
    with pytest.raises(RuntimeError, match="production keyword"):
        assert_safe_test_database_url("postgresql+asyncpg://cml:cml@localhost:54329/prod_db")

    with pytest.raises(RuntimeError, match="production keyword"):
        assert_safe_test_database_url("postgresql+asyncpg://cml:cml@localhost:5432/live_database")


def test_safe_test_database_url_rejects_unrecognized_default_port_db() -> None:
    with pytest.raises(RuntimeError, match="not recognized as a disposable test database"):
        assert_safe_test_database_url("postgresql+asyncpg://cml:cml@localhost:5432/cml")
    with pytest.raises(RuntimeError, match="not recognized as a disposable test database"):
        assert_safe_test_database_url("postgresql+asyncpg://cml:cml@localhost:54329/cml")
