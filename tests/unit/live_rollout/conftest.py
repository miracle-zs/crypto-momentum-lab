import pytest


@pytest.fixture(autouse=True)
async def close_daemon_test_coordinators():
    from tests.unit.live_rollout.test_daemon import _created_coordinators

    try:
        yield
    finally:
        while _created_coordinators:
            await _created_coordinators.pop().aclose()
