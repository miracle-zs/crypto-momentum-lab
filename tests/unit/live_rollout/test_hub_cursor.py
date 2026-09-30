"""Hub cursor contracts independent of live runtime assembly."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.domain.strategy import StrategyCheckpoint
from crypto_momentum_lab.live_rollout.hub_cursor import (
    LiveHubCursorState,
    hub_cursor_for_startup,
    hub_cursor_from_checkpoint_payload,
)
from crypto_momentum_lab.market_data.hub import MarketStateBatch


def checkpoint(raw):
    return StrategyCheckpoint(
        last_processed_at_by_symbol={},
        warmup_buckets_by_symbol={},
        cooldown_buckets_remaining_by_symbol={},
        payload={"market_state_hub_cursor": raw},
    )


@pytest.mark.parametrize(
    "raw",
    [
        {"stream_id": "", "sequence": 0},
        {"stream_id": "  ", "sequence": 0},
        {"stream_id": None, "sequence": 0},
        {"stream_id": 42, "sequence": 0},
        {"stream_id": "s", "sequence": True},
        {"stream_id": "s", "sequence": -1},
        {"stream_id": "s", "sequence": 1.5},
        {"stream_id": "s", "sequence": None},
    ],
)
def test_invalid_resume_cursor_is_ignored_and_explicit_restore_rejects(raw):
    assert (
        hub_cursor_for_startup(checkpoint(raw), requires_market_recovery=False) is None
    )
    cursor = LiveHubCursorState()
    cursor.restore({"stream_id": "existing", "sequence": 5})
    with pytest.raises(ValueError):
        cursor.restore(raw)
    assert cursor.snapshot() == {"stream_id": "existing", "sequence": 5}


@pytest.mark.parametrize("raw", [None, [], "bad", 12])
def test_non_mapping_checkpoint_cursor_is_absent(raw):
    assert hub_cursor_from_checkpoint_payload(checkpoint(raw)) is None
    assert (
        hub_cursor_for_startup(checkpoint(raw), requires_market_recovery=False) is None
    )


def test_zero_cursor_restore_and_snapshots_do_not_alias_state():
    cursor = LiveHubCursorState()
    raw = {"stream_id": "s", "sequence": 0}
    cursor.restore(raw)
    raw["sequence"] = 12
    snapshot = cursor.snapshot()
    assert snapshot == {"stream_id": "s", "sequence": 0}
    snapshot["sequence"] = 13
    assert cursor.snapshot() == {"stream_id": "s", "sequence": 0}
    assert hub_cursor_for_startup(None, requires_market_recovery=False) is None


def batch(sequence, *, stream_id="s", symbol="TESTUSDT"):
    at = datetime(2026, 9, 30, tzinfo=UTC) + timedelta(seconds=sequence * 15)
    state = SimpleNamespace(symbol=symbol, bucket_start=at)
    return MarketStateBatch(
        sequence=sequence,
        stream_id=stream_id,
        published_at=at,
        environment="live",
        states=(state,),
    ), state


def test_unknown_or_duplicate_acknowledgement_does_not_advance_cursor():
    cursor = LiveHubCursorState()
    observed, state = batch(7)
    cursor.acknowledge_state(state)
    assert cursor.snapshot() is None
    cursor.observe_batch(observed)
    cursor.acknowledge_state(state)
    cursor.acknowledge_state(state)
    assert cursor.snapshot() == {"stream_id": "s", "sequence": 7}


def test_completed_older_batch_does_not_regress_same_stream_cursor():
    cursor = LiveHubCursorState()
    cursor.restore({"stream_id": "s", "sequence": 9})
    observed, state = batch(7)
    cursor.observe_batch(observed)
    cursor.acknowledge_state(state)
    assert cursor.snapshot() == {"stream_id": "s", "sequence": 9}


def test_completed_new_stream_can_replace_old_stream_sequence():
    cursor = LiveHubCursorState()
    cursor.restore({"stream_id": "old", "sequence": 90})
    observed, state = batch(1, stream_id="new")
    cursor.observe_batch(observed)
    assert cursor.snapshot() == {"stream_id": "old", "sequence": 90}
    cursor.acknowledge_state(state)
    assert cursor.snapshot() == {"stream_id": "new", "sequence": 1}


def test_batch_without_stream_does_not_publish_cursor_or_symbol_entries():
    cursor = LiveHubCursorState()
    observed, state = batch(7, stream_id=None)
    cursor.observe_batch(observed)
    cursor.acknowledge_state(state)
    assert cursor.snapshot() is None


def test_empty_batch_keeps_existing_cursor_until_state_acknowledgement():
    cursor = LiveHubCursorState()
    cursor.restore({"stream_id": "s", "sequence": 1})
    cursor.observe_batch(
        MarketStateBatch(
            sequence=2,
            stream_id="s",
            published_at=datetime(2026, 9, 30, tzinfo=UTC),
            environment="live",
            states=(),
        )
    )
    assert cursor.snapshot() == {"stream_id": "s", "sequence": 1}
