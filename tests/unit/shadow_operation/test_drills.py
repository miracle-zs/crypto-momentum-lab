from datetime import UTC, datetime

from crypto_momentum_lab.shadow_operation.drills import run_shadow_drill


async def test_shadow_drill_records_passed_probe() -> None:
    async def probe() -> None:
        return None

    result = await run_shadow_drill(
        run_id="shadow-1",
        drill_name="stale_market_data",
        occurred_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        probe=probe,
    )

    assert result.outcome == "passed"


async def test_shadow_drill_records_failed_probe() -> None:
    async def probe() -> None:
        raise RuntimeError("expected failure")

    result = await run_shadow_drill(
        run_id="shadow-1",
        drill_name="database_temporary_failure",
        occurred_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        probe=probe,
    )

    assert result.outcome == "failed"
    assert result.details["error"] == "expected failure"
