from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.execution_account.binance.user_data_models import (
    BinanceUserDataEvent,
)
from crypto_momentum_lab.execution_account.user_data_models import UserDataStateError
from crypto_momentum_lab.execution_account.user_data_sequence import (
    validate_exchange_update_watermark,
)


def event(update_id, previous_update_id):
    timestamp = datetime(2026, 10, 1, tzinfo=UTC)
    return BinanceUserDataEvent(
        event_type="ACCOUNT_UPDATE",
        event_at=timestamp,
        received_at=timestamp,
        payload={},
        event_id="0" * 64,
        exchange_update_id=update_id,
        exchange_previous_update_id=previous_update_id,
    )


@pytest.mark.parametrize(
    "update_id,previous_update_id,last",
    [
        (None, 999, 10),
        (0, 999, None),
        (11, 10, 10),
        (11, None, 10),
        (100, 10, 10),
        (0, None, None),
    ],
)
def test_optional_and_first_sequence_or_valid_advancement_is_accepted(
    update_id, previous_update_id, last
):
    observation = event(update_id, previous_update_id)
    assert validate_exchange_update_watermark(observation, last) is None
    assert observation.exchange_update_id == update_id
    assert observation.exchange_previous_update_id == previous_update_id


@pytest.mark.parametrize(
    "update_id,previous_update_id,last,error",
    [
        (10, 10, 10, "watermark moved backwards"),
        (9, 10, 10, "watermark moved backwards"),
        (10, None, 10, "watermark moved backwards"),
        (20, 9, 10, "sequence is not contiguous"),
        (9, 9, 10, "sequence is not contiguous"),
    ],
)
def test_duplicate_regression_and_continuity_failure_keep_error_precedence(
    update_id, previous_update_id, last, error
):
    with pytest.raises(UserDataStateError, match=error):
        validate_exchange_update_watermark(event(update_id, previous_update_id), last)
