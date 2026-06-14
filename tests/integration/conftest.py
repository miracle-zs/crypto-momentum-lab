import os

import pytest


@pytest.fixture(scope="session")
def database_url() -> str:
    return os.environ.get(
        "CML_TEST_DATABASE_URL",
        "postgresql+psycopg://cml:cml@localhost:54329/cml",
    )
