"""Empty-heartbeat eligibility for strategy-output durability."""

from __future__ import annotations

from crypto_momentum_lab.live_rollout.market_loop import (
    _empty_heartbeat_eligible,
)


def test_unconfigured_entry_pool_keeps_every_symbol_eligible() -> None:
    assert _empty_heartbeat_eligible(
        "ETHUSDT",
        entry_symbols=None,
        open_position_symbols=frozenset(),
    )


def test_entry_pool_member_is_eligible() -> None:
    assert _empty_heartbeat_eligible(
        "BTCUSDT",
        entry_symbols=frozenset({"BTCUSDT", "ETHUSDT"}),
        open_position_symbols=frozenset(),
    )


def test_open_position_symbol_is_eligible_outside_entry_pool() -> None:
    assert _empty_heartbeat_eligible(
        "SOLUSDT",
        entry_symbols=frozenset({"BTCUSDT"}),
        open_position_symbols=frozenset({"SOLUSDT"}),
    )


def test_unrelated_symbol_is_not_eligible() -> None:
    assert not _empty_heartbeat_eligible(
        "DOGEUSDT",
        entry_symbols=frozenset({"BTCUSDT"}),
        open_position_symbols=frozenset({"SOLUSDT"}),
    )
