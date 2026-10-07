from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from crypto_momentum_lab.live_rollout.runtime_startup import (
    LiveRuntimeStartupStatus,
    validate_live_runtime_endpoints,
)


def _config(**market_overrides: str) -> object:
    market = {
        "market_state_source": "hub",
        "market_state_hub_url": "ws://market-state",
        "market_quote_hub_url": "ws://market-quote",
        "market_quote_volume_hub_url": "ws://quote-volume",
        "account_event_hub_url": "ws://account-events",
    }
    market.update(market_overrides)
    return SimpleNamespace(market=SimpleNamespace(**market))


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"market_state_source": "invalid"}, "market_state_source"),
        ({"market_state_hub_url": ""}, "market_state_hub_url"),
        ({"market_quote_hub_url": ""}, "market_quote_hub_url"),
        ({"market_quote_volume_hub_url": ""}, "market_quote_volume_hub_url"),
        ({"account_event_hub_url": ""}, "account_event_hub_url"),
    ],
)
def test_validate_live_runtime_endpoints_fails_before_resource_assembly(
    overrides: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        validate_live_runtime_endpoints(_config(**overrides))  # type: ignore[arg-type]


def test_startup_status_publishes_readiness_with_health_heartbeat() -> None:
    health = Mock()
    readiness = Mock()
    status = LiveRuntimeStartupStatus(health)
    status.readiness = readiness

    status.mark_ready()

    health.heartbeat.assert_called_once_with(database_ok=True)
    readiness.publish.assert_called_once_with()
