from types import SimpleNamespace

from crypto_momentum_lab.live_rollout import market_loop
from crypto_momentum_lab.live_rollout.market_loop import LiveMarketLoop


def test_unmanaged_position_warning_is_emitted_on_change_not_each_market_event(
    monkeypatch,
):
    warnings = []
    infos = []
    monkeypatch.setattr(
        market_loop,
        "log",
        SimpleNamespace(
            warning=lambda event, **kwargs: warnings.append((event, kwargs)),
            info=lambda event, **kwargs: infos.append((event, kwargs)),
        ),
    )
    loop = object.__new__(LiveMarketLoop)
    loop._run_id = "run-1"
    loop._previous_unmanaged_position_symbols = frozenset()

    loop._report_unmanaged_position_changes(frozenset({"1000RATSUSDT"}))
    loop._report_unmanaged_position_changes(frozenset({"1000RATSUSDT"}))
    loop._report_unmanaged_position_changes(frozenset())

    assert warnings == [
        (
            "live_unmanaged_positions_observed",
            {
                "run_id": "run-1",
                "symbols": ["1000RATSUSDT"],
                "current_symbols": ["1000RATSUSDT"],
            },
        )
    ]
    assert infos == [
        (
            "live_unmanaged_positions_resolved",
            {"run_id": "run-1", "symbols": ["1000RATSUSDT"]},
        )
    ]
