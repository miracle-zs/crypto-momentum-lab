import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
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
        entry_universe_target_count=10,
        warmup_required_buckets=140,
    )
    return health, publisher


def test_exit_stream_outage_is_published_even_when_entry_is_enabled(tmp_path):
    health, publisher = _publisher(tmp_path)
    publisher.update_entry_gate(
        entry_universe_count=2, entry_enabled=True, entry_enabled_reason="ready"
    )
    publisher.update_exit_stream(available=False)
    payload = json.loads(health.readiness_path.read_text())
    assert payload["tradeability"]["exit_gate_open"] is False
    assert (
        payload["tradeability"]["exit_gate_reason"]
        == "closed_candle_stream_unavailable"
    )
    publisher.update_exit_stream(available=True)
    assert (
        json.loads(health.readiness_path.read_text())["tradeability"]["exit_gate_open"]
        is True
    )


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
    assert payload["tradeability"]["entry_gate_open"] is False
    assert (
        payload["tradeability"]["entry_gate_reason"]
        == "strategy_warmup_incomplete"
    )
    assert "entry_enabled" not in payload
    assert "entry_enabled_reason" not in payload
    assert "stream_readiness" not in payload
    assert "unmanaged_risk_clear" not in payload["tradeability"]
    assert "halt_active" not in payload["tradeability"]


def test_mature_symbols_can_run_while_cold_symbols_keep_warming(tmp_path) -> None:
    _, publisher = _publisher(tmp_path)
    publisher.update_warmup(
        LiveWarmupStatus(
            required_buckets=140,
            expected_symbols=frozenset({"BTCUSDT", "ETHUSDT"}),
            complete_symbols=frozenset({"BTCUSDT"}),
            cutover_at=datetime(2026, 9, 12, 2, 0, tzinfo=UTC),
        )
    )

    assert publisher.has_warmup_ready_symbols is True


def test_no_mature_symbols_keeps_entries_blocked(tmp_path) -> None:
    _, publisher = _publisher(tmp_path)
    publisher.set_expected_warmup_symbols({"BTCUSDT", "ETHUSDT"})

    assert publisher.has_warmup_ready_symbols is False


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
    first_market_at = payload["latest_market_state_at"]

    # Subsequent bucket with same warmup must still update market age and time
    state2 = SimpleNamespace(
        bucket_start=state.bucket_start + timedelta(seconds=15),
        bucket_end=state.bucket_end + timedelta(seconds=15),
    )
    publisher.observe_market_state(
        state2,
        strategy=Strategy(),
        entry_universe_count=2,
    )
    payload2 = json.loads(health.readiness_path.read_text())
    assert payload2["latest_market_state_at"] != first_market_at
    assert payload2["warmup_complete_symbols"] == 1


def test_readiness_deduplicates_entry_gate_updates(tmp_path, monkeypatch) -> None:
    health, publisher = _publisher(tmp_path)
    call_count = 0
    original_write = health.write_readiness

    def counting_write(payload):
        nonlocal call_count
        call_count += 1
        return original_write(payload)

    monkeypatch.setattr(health, "write_readiness", counting_write)

    # First update with different state triggers publish
    publisher.update_entry_gate(
        entry_universe_count=5,
        entry_enabled=True,
        entry_enabled_reason="live_entry_prerequisites_ready",
    )
    assert call_count == 1

    # Identical update must NOT trigger publish
    publisher.update_entry_gate(
        entry_universe_count=5,
        entry_enabled=True,
        entry_enabled_reason="live_entry_prerequisites_ready",
    )
    assert call_count == 1

    # Changed update triggers publish
    publisher.update_entry_gate(
        entry_universe_count=5,
        entry_enabled=False,
        entry_enabled_reason="lease_heartbeat_degraded",
    )
    assert call_count == 2










def test_readiness_compute_dynamic_market_age_and_observed_at(
    tmp_path: Path,
) -> None:
    """Reader computes current age from latest_market_state_at
    rather than stale published age.
    """
    health, publisher = _publisher(tmp_path)

    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    state = SimpleNamespace(
        bucket_start=t0 - timedelta(seconds=15),
        bucket_end=t0,
    )
    publisher.observe_market_state(
        state,  # type: ignore[arg-type]
        strategy=SimpleNamespace(warmup_buckets_by_symbol={}),  # type: ignore[arg-type]
        entry_universe_count=10,
    )

    # 1. compute_market_state_age_seconds evaluated 5 seconds later
    t_reader = t0 + timedelta(seconds=5)
    age_5s = publisher.compute_market_state_age_seconds(now=t_reader)
    assert age_5s == 5.0

    # 2. compute_market_state_age_seconds evaluated 75 seconds later
    t_reader_stale = t0 + timedelta(seconds=75)
    age_75s = publisher.compute_market_state_age_seconds(now=t_reader_stale)
    assert age_75s == 75.0

    # 3. One publication timestamp is enough; market state keeps its own time.
    payload = json.loads(health.readiness_path.read_text())
    assert "observed_at" in payload
    assert "published_at" not in payload
    assert payload["latest_market_state_at"] == t0.isoformat()


def test_readiness_publishes_same_payload_to_existing_telemetry_and_file(tmp_path):
    from crypto_momentum_lab.live_rollout.telemetry import LiveRuntimeTelemetry
    health, publisher = _publisher(tmp_path)
    telemetry = LiveRuntimeTelemetry(run_id="live-primary-v1")
    publisher._on_publish = telemetry.runtime_readiness
    publisher.publish()
    event = telemetry.recent_events[-1]
    assert event.event_type == "runtime_readiness"
    assert json.loads((tmp_path / "health" / "readiness").read_text()) == dict(
        event.details
    )
    assert event.details["account_label"] == "primary"
    assert event.details["tradeability"]["entry_gate_open"] is False
    publisher.publish()
    assert len(telemetry.recent_events) == 1
    publisher._entry_enabled = True
    publisher._entry_enabled_reason = "ready"
    publisher.publish()
    assert len(telemetry.recent_events) == 2
    assert (
        telemetry.recent_events[-1].details["tradeability"]["entry_gate_open"]
        is True
    )


def test_failed_telemetry_sink_does_not_prevent_local_readiness(tmp_path):
    health, publisher = _publisher(tmp_path)
    def fail(payload):
        raise RuntimeError("writer unavailable")
    publisher._on_publish = fail
    publisher.publish()
    assert (tmp_path / "health" / "readiness").exists()


def test_readiness_publishes_order_observation_backpressure(tmp_path) -> None:
    health = LocalHealthWriter.for_directory(tmp_path / "health")
    health_payloads: list[dict] = []
    health.write_readiness = health_payloads.append  # type: ignore[method-assign]
    publisher = LiveReadinessPublisher(
        health=health,
        account_label="primary",
        session_id="live-primary-v1",
        strategy="orderflow_impulse",
        entry_universe_target_count=10,
        warmup_required_buckets=140,
        order_observation_metrics=lambda: {
            "capacity": 256,
            "depth": 3,
            "high_watermark": 17,
            "overflow_count": 1,
            "oldest_pending_age_seconds": 2.5,
        },
    )

    publisher.publish()

    observation = health_payloads[-1]["order_observation"]
    assert observation["depth"] == 3
    assert observation["high_watermark"] == 17
    assert observation["overflow_count"] == 1
    assert observation["oldest_pending_age_seconds"] == 2.5


def test_readiness_survives_a_failing_observation_metric(tmp_path) -> None:
    def _explode() -> dict:
        raise RuntimeError("metrics unavailable")

    payloads: list[object] = []
    health = LocalHealthWriter.for_directory(tmp_path / "health")
    publisher = LiveReadinessPublisher(
        health=health,
        on_publish=payloads.append,
        account_label="primary",
        session_id="live-primary-v1",
        strategy="orderflow_impulse",
        entry_universe_target_count=10,
        warmup_required_buckets=140,
        order_observation_metrics=_explode,
    )

    publisher.publish()

    # A failing metric must not block the readiness snapshot.
    assert payloads
    assert payloads[-1]["order_observation"] == {}  # type: ignore[index]
