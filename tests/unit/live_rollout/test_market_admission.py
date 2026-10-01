from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.live_rollout.market_admission import LiveMarketStateAdmission


@pytest.mark.parametrize("enabled", [False, True])
def test_invalidation_uses_only_explicit_callback(enabled: bool) -> None:
    calls = []

    def unexpected():
        pytest.fail("must not discover provider invalidation methods")

    admission = LiveMarketStateAdmission(
        context_provider=SimpleNamespace(
            invalidate=unexpected
        ),
        context_generation=lambda: 0,
        apply_context=SimpleNamespace(),
        telemetry=None,
        clock=lambda: datetime.now(tz=UTC),
        invalidate_context=(lambda: calls.append("invalidate")) if enabled else None,
    )
    admission.invalidate_context_cache()
    assert calls == (["invalidate"] if enabled else [])
