from crypto_momentum_lab.domain.execution import ExecutionRunMode


def test_run_mode_accepts_shadow_and_live_as_distinct_modes() -> None:
    assert ExecutionRunMode.SHADOW.value == "shadow"
    assert ExecutionRunMode.LIVE.value == "live"
    assert ExecutionRunMode.SHADOW is not ExecutionRunMode.LIVE
