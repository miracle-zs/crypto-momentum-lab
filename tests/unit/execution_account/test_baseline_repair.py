from datetime import UTC, datetime, timedelta

import pytest

from crypto_momentum_lab.execution_account.baseline_repair import BaselineRepairAttempt


@pytest.mark.parametrize(
    "events,connection,recovery,age,live,commit",
    [
        (7, 2, False, 0, True, True),
        (8, 2, False, 0, True, False),
        (7, None, False, 0, False, False),
        (7, 3, False, 0, False, False),
        (7, 2, True, 0, True, False),
        (7, 2, False, 180, True, True),
        (7, 2, False, 181, False, False),
        (7, 2, False, -1, False, False),
    ],
)
def test_repair_evidence_distinguishes_serving_live_from_replacing_baseline(
    events, connection, recovery, age, live, commit
):
    cut = datetime(2026, 10, 1, tzinfo=UTC)
    attempt = BaselineRepairAttempt(
        event_generation=7, stream_token=2, baseline_observed_at=cut
    )
    now = cut + timedelta(seconds=age)
    assert attempt.can_serve_live(stream_token=connection, now=now) is live
    assert (
        attempt.can_commit(
            event_generation=events,
            stream_token=connection,
            recovery_required=recovery,
            candidate_observed_at=cut,
            now=now,
        )
        is commit
    )


def test_halt_result_is_preserved_only_when_scan_continuity_is_intact():
    cut = datetime(2026, 10, 1, tzinfo=UTC)
    attempt = BaselineRepairAttempt(7, 2, cut)
    assert attempt.can_commit(
        event_generation=7,
        stream_token=2,
        recovery_required=False,
        candidate_observed_at=None,
        now=cut,
    )
    assert not attempt.can_commit(
        event_generation=8,
        stream_token=2,
        recovery_required=False,
        candidate_observed_at=None,
        now=cut,
    )
