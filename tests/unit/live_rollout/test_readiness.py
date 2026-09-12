import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from crypto_momentum_lab.health import LocalHealthWriter
from crypto_momentum_lab.live_rollout.readiness import (
    LiveReadinessPublisher,
    LiveWarmupStatus,
)


def _publisher(tmp_path):
    health = LocalHealthWriter.for_directory(tmp_path / "health")
    publisher = LiveReadinessPublisher(
        health=health,
        account_label="primary",
        session_id="live-primary-v1",
        strategy="orderflow_impulse",
        code_commit="a" * 40,
        migration_revision="20260911_0036",
        entry_universe_target_count=10,
        warmup_required_buckets=140,
    )
    return health, publisher


def test_readiness_publishes_warmup_and_entry_gate_state(tmp_path) -> None:
    health, publisher = _publisher(tmp_path)
    cutover = datetime(2026, 9, 12, 2, 0, tzinfo=UTC)

    publisher.update_warmup(
        LiveWarmupStatus(
            required_buckets=140,
            expected_symbols=frozenset({"BTCUSDT", "ETHUSDT"}),
            complete_symbols=frozenset({"BTCUSDT"}),
            cutover_at=cutover,
        )
    )
    publisher.update_entry_gate(
        entry_universe_count=2,
        entry_enabled=False,
        entry_enabled_reason="strategy_warmup_incomplete",
    )

    payload = json.loads(health.readiness_path.read_text())

    assert payload["entry_universe_count"] == 2
    assert payload["entry_universe_target_count"] == 10
    assert payload["warmup_required_buckets"] == 140
    assert payload["warmup_expected_symbols"] == 2
    assert payload["warmup_complete_symbols"] == 1
    assert payload["warmup_deferred_symbols"] == 1
    assert payload["entry_enabled"] is False
    assert payload["entry_enabled_reason"] == "strategy_warmup_incomplete"


def test_readiness_refreshes_progress_and_market_age(tmp_path) -> None:
    health, publisher = _publisher(tmp_path)
    publisher.set_expected_warmup_symbols({"BTCUSDT", "ETHUSDT"})

    class Strategy:
        def required_data(self):
            return SimpleNamespace(warmup_buckets=3)

        def checkpoint(self, *, include_market_state_buffers=True):
            return SimpleNamespace(
                warmup_buckets_by_symbol={"BTCUSDT": 3, "ETHUSDT": 2}
            )

    state = SimpleNamespace(
        bucket_start=datetime.now(tz=UTC) - timedelta(seconds=30),
        bucket_end=datetime.now(tz=UTC) - timedelta(seconds=15),
    )
    publisher.observe_market_state(
        state,
        strategy=Strategy(),
        entry_universe_count=2,
    )

    payload = json.loads(health.readiness_path.read_text())

    assert payload["warmup_complete_symbols"] == 1
    assert payload["warmup_deferred_symbols"] == 1
    assert payload["latest_market_state_age_seconds"] >= 0
