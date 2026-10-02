from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.persistence.postgres import fill_recovery_sources


@pytest.mark.parametrize("verified", [True, False])
async def test_checkpoint_verification_controls_historical_fallback_queries(
    monkeypatch, verified
):
    head = SimpleNamespace(
        symbol="BTCUSDT",
        position_side="LONG",
        stream_id="hub",
        stream_epoch="old",
        state_payload={"recovery_checkpoint": {"checkpoint_id": "checkpoint"}},
    )
    checkpoint = SimpleNamespace(symbol="BTCUSDT", position_side="LONG")
    anchor = object() if verified else None
    monkeypatch.setattr(
        fill_recovery_sources, "_checkpoint_anchor", Mock(return_value=anchor)
    )
    # Extra queries fail when the validated checkpoint already supplies the
    # source. Unverified checkpoints must still attempt both durable fallbacks.
    rows = [[head], [checkpoint]] + ([] if verified else [[], []])
    session = AsyncMock()
    session.scalars.side_effect = [Mock(all=Mock(return_value=x)) for x in rows]
    manager = AsyncMock()
    manager.__aenter__.return_value = session
    sources = await fill_recovery_sources.load_fill_recovery_sources(
        Mock(return_value=manager), environment="live", account_label="primary"
    )
    assert sources == {("BTCUSDT", "LONG"): anchor}
    assert session.scalars.await_count == (2 if verified else 4)
