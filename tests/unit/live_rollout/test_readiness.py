import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from crypto_momentum_lab.health import LocalHealthWriter
from crypto_momentum_lab.live_rollout.readiness import (
    LiveReadinessPublisher,
    LiveWarmupStatus,
    StreamReadinessSnapshot,
    TradeabilityAlertManager,
    TradeabilityMode,
    TradeabilitySnapshot,
)


def _publisher(tmp_path):
    health = LocalHealthWriter.for_directory(tmp_path / "health")
    publisher = LiveReadinessPublisher(
        health=health,
        account_label="primary",
        session_id="live-primary-v1",
        strategy="orderflow_impulse",
        code_commit="a" * 40,
        migration_revision="20260925_0043",
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


def test_tradeability_snapshot_mode_derivation() -> None:
    # 1. Fully tradeable
    snap1 = TradeabilitySnapshot.create(
        entry_gate_open=True,
        entry_gate_reason="live_entry_prerequisites_ready",
        exit_gate_open=True,
        unmanaged_risk_clear=True,
        halt_active=False,
    )
    assert snap1.mode is TradeabilityMode.FULLY_TRADEABLE

    # 2. Exit only when entry gate closed
    snap2 = TradeabilitySnapshot.create(
        entry_gate_open=False,
        entry_gate_reason="strategy_warmup_incomplete",
        exit_gate_open=True,
        unmanaged_risk_clear=True,
        halt_active=False,
    )
    assert snap2.mode is TradeabilityMode.EXIT_ONLY

    # 3. Halted when halt is active
    snap3 = TradeabilitySnapshot.create(
        entry_gate_open=True,
        entry_gate_reason="ready",
        exit_gate_open=True,
        unmanaged_risk_clear=True,
        halt_active=True,
    )
    assert snap3.mode is TradeabilityMode.HALTED

    # 4. Halted when exit gate is closed
    snap4 = TradeabilitySnapshot.create(
        entry_gate_open=True,
        entry_gate_reason="ready",
        exit_gate_open=False,
        unmanaged_risk_clear=True,
        halt_active=False,
    )
    assert snap4.mode is TradeabilityMode.HALTED

    # 5. Degraded when unmanaged risk present
    snap5 = TradeabilitySnapshot.create(
        entry_gate_open=True,
        entry_gate_reason="ready",
        exit_gate_open=True,
        unmanaged_risk_clear=False,
        halt_active=False,
    )
    assert snap5.mode is TradeabilityMode.DEGRADED


def test_stream_readiness_snapshot_aggregation() -> None:
    assert StreamReadinessSnapshot.from_streams({}).overall == "UNKNOWN"

    ready = StreamReadinessSnapshot.from_streams(
        {"account": "READY", "quote": "READY", "market": "READY"}
    )
    assert ready.overall == "READY"

    connecting = StreamReadinessSnapshot.from_streams(
        {"account": "CONNECTING", "quote": "READY"}
    )
    assert connecting.overall == "CONNECTING"

    recovering = StreamReadinessSnapshot.from_streams(
        {"account": "READY", "market": "RECOVERING"}
    )
    assert recovering.overall == "RECOVERING"

    disrupted = StreamReadinessSnapshot.from_streams(
        {"account": "READY", "quote": "DISRUPTED", "market": "RECOVERING"}
    )
    assert disrupted.overall == "DISRUPTED"


def test_tradeability_alert_manager_edge_triggered_and_heartbeat() -> None:
    current_time = 100.0

    def mock_clock() -> float:
        return current_time

    manager = TradeabilityAlertManager(
        fallback_heartbeat_seconds=30.0,
        clock=mock_clock,
    )

    # 1. Initial healthy state does not alert
    assert (
        manager.observe(
            mode=TradeabilityMode.FULLY_TRADEABLE,
            reason="ready",
        )
        is False
    )

    # 2. State transition to EXIT_ONLY triggers edge alert
    assert (
        manager.observe(
            mode=TradeabilityMode.EXIT_ONLY,
            reason="strategy_warmup_incomplete",
        )
        is True
    )
    assert manager.last_mode == "EXIT_ONLY"
    assert manager.last_reason == "strategy_warmup_incomplete"

    # 3. Duplicate steady-state call does NOT trigger alert
    assert (
        manager.observe(
            mode=TradeabilityMode.EXIT_ONLY,
            reason="strategy_warmup_incomplete",
        )
        is False
    )

    # 4. Reason transition (even in same mode) triggers edge alert
    assert (
        manager.observe(
            mode=TradeabilityMode.EXIT_ONLY,
            reason="lease_heartbeat_degraded",
        )
        is True
    )
    assert manager.last_reason == "lease_heartbeat_degraded"

    # 5. Severity transition triggers edge alert
    assert (
        manager.observe(
            mode=TradeabilityMode.EXIT_ONLY,
            reason="lease_heartbeat_degraded",
            severity="CRITICAL",
        )
        is True
    )
    assert manager.last_severity == "CRITICAL"

    # 6. Fallback heartbeat re-alerts after fallback interval
    current_time += 15.0  # only 15s elapsed
    assert (
        manager.observe(
            mode=TradeabilityMode.EXIT_ONLY,
            reason="lease_heartbeat_degraded",
            severity="CRITICAL",
        )
        is False
    )

    current_time += 20.0  # 35s elapsed (> 30s threshold)
    assert (
        manager.observe(
            mode=TradeabilityMode.EXIT_ONLY,
            reason="lease_heartbeat_degraded",
            severity="CRITICAL",
        )
        is True
    )

    # 7. Recovery to FULLY_TRADEABLE emits recovery alert
    assert (
        manager.observe(
            mode=TradeabilityMode.FULLY_TRADEABLE,
            reason="live_entry_prerequisites_ready",
        )
        is True
    )
    assert manager.last_mode == "FULLY_TRADEABLE"


def test_readiness_publisher_layered_tradeability_and_stream_readiness(
    tmp_path,
) -> None:
    health, publisher = _publisher(tmp_path)

    # Initial publish
    payload = json.loads(health.readiness_path.read_text())
    assert "tradeability" in payload
    assert payload["tradeability"]["mode"] == "EXIT_ONLY"
    assert payload["tradeability"]["entry_gate_open"] is False
    assert payload["tradeability"]["exit_gate_open"] is True
    assert payload["tradeability"]["unmanaged_risk_clear"] is True
    assert payload["tradeability"]["halt_active"] is False

    assert "stream_readiness" in payload
    assert payload["stream_readiness"]["overall"] == "UNKNOWN"

    # Update streams
    publisher.update_stream_readiness("account", "READY")
    publisher.update_stream_readiness("quote", "READY")
    publisher.update_stream_readiness("market_state", "READY")

    # Update tradeability to fully tradeable
    publisher.update_tradeability(
        entry_enabled=True,
        entry_reason="live_entry_prerequisites_ready",
    )

    payload2 = json.loads(health.readiness_path.read_text())
    assert payload2["tradeability"]["mode"] == "FULLY_TRADEABLE"
    assert payload2["tradeability"]["entry_gate_open"] is True
    assert payload2["tradeability"]["entry_gate_reason"] == (
        "live_entry_prerequisites_ready"
    )
    assert payload2["stream_readiness"]["overall"] == "READY"
    assert payload2["stream_readiness"]["streams"]["account"] == "READY"

    # Induce unmanaged risk -> mode becomes DEGRADED
    publisher.update_tradeability(unmanaged_risk_clear=False)
    payload3 = json.loads(health.readiness_path.read_text())
    assert payload3["tradeability"]["mode"] == "DEGRADED"
    assert payload3["tradeability"]["unmanaged_risk_clear"] is False


def test_readiness_compute_dynamic_market_age_and_published_at(
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

    # 3. Payload has published_at and latest_market_state_at timestamps
    payload = json.loads(health.readiness_path.read_text())
    assert "published_at" in payload
    assert payload["latest_market_state_at"] == t0.isoformat()

