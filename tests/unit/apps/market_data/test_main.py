
import asyncio
from collections.abc import Iterable
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from structlog.testing import capture_logs
from typer.testing import CliRunner

from crypto_momentum_lab.apps.market_data import main
from crypto_momentum_lab.domain.market.models import CaptureStream
from crypto_momentum_lab.domain.operational.retention_authority import (
    InMemoryRetentionRepository,
)
from crypto_momentum_lab.domain.universe.models import (
    MarketCandidate,
    MembershipStatus,
    RankEntry,
    RankingResult,
    RankingSide,
    TrackedMembership,
    UniverseSnapshot,
)
from crypto_momentum_lab.universe.scheduler import run_scheduler_loop

runner = CliRunner()


def test_retired_checkpoint_run_ids_are_explicit(monkeypatch) -> None:
    monkeypatch.setenv("CML_RETIRED_STRATEGY_RUN_IDS", "paper-retired, paper-retired-2")
    assert main.parse_retired_strategy_run_ids() == frozenset(
        {"paper-retired", "paper-retired-2"}
    )
    with pytest.raises(ValueError):
        main.parse_retired_strategy_run_ids("paper-*")
    with pytest.raises(ValueError):
        main.parse_retired_strategy_run_ids("paper-retired,")


async def test_market_retention_excludes_only_explicitly_retired_checkpoints(
    monkeypatch,
) -> None:
    old = datetime(2026, 9, 2, tzinfo=UTC)
    live = datetime(2026, 10, 7, tzinfo=UTC)
    position = datetime(2026, 10, 6, tzinfo=UTC)

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            pass

        async def scalars(self, _statement):
            return SimpleNamespace(all=lambda: [])

        async def scalar(self, statement):
            # Model the SQL predicate over two persisted checkpoints, not age.
            params = statement.compile().params
            retired = next(
                (set(v) for k, v in params.items() if k.startswith("run_id")), set()
            )
            return min(
                at
                for run, at in (("paper-retired", old), ("live-current", live))
                if run not in retired
            )

    monkeypatch.setenv("CML_RETIRED_STRATEGY_RUN_IDS", "paper-retired")
    monkeypatch.setattr(
        main.PostgresAccountRepository,
        "load_active_position_retention_watermark",
        AsyncMock(return_value=position),
        raising=False,
    )
    requirements = await main._resolve_market_data_consumer_requirements(Session)
    assert {r.consumer_id: r.min_required_watermark for r in requirements} == {
        "active_strategy_checkpoints": live,
        "active_position_market_states": position,
    }
    # An unclassified old run remains protected; absence of a heartbeat is not
    # an operator decision to retire it.
    monkeypatch.setenv("CML_RETIRED_STRATEGY_RUN_IDS", "")
    requirements = await main._resolve_market_data_consumer_requirements(Session)
    assert requirements[0].min_required_watermark == old


def test_market_database_url_prefers_the_market_plane(monkeypatch) -> None:
    monkeypatch.setenv(
        "CML_MARKET_DATABASE_URL",
        "postgresql+asyncpg://market",
    )

    assert main._market_database_url("postgresql+asyncpg://shared") == (
        "postgresql+asyncpg://market"
    )


def fixture_snapshot() -> UniverseSnapshot:
    at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    candidate = MarketCandidate(
        "BTCUSDT",
        Decimal("100"),
        Decimal("110"),
        at,
    )
    rank = RankEntry(
        "BTCUSDT",
        Decimal("0.1"),
        1,
        RankingSide.GAINER,
    )
    return UniverseSnapshot(
        snapshot_id=UUID("00000000-0000-0000-0000-000000000001"),
        observed_at=at,
        utc_day=at.date(),
        config_hash="a" * 64,
        activated=True,
        ranking=RankingResult(
            candidates=(candidate,),
            gainers=(rank,),
            losers=(),
            target_symbols=frozenset({"BTCUSDT"}),
            exclusions={},
        ),
        memberships=(
            TrackedMembership(
                "BTCUSDT",
                MembershipStatus.TARGET,
                RankingSide.GAINER,
            ),
        ),
    )


def fixture_tiered_snapshot() -> UniverseSnapshot:
    at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    gainers = tuple(
        RankEntry(
            f"S{rank:02d}USDT",
            Decimal("0.1") - Decimal(rank) / Decimal("1000"),
            rank,
            RankingSide.GAINER,
        )
        for rank in range(1, 41)
    )
    memberships = tuple(
        TrackedMembership(
            entry.symbol,
            (
                MembershipStatus.TARGET
                if entry.rank <= 20
                else MembershipStatus.EXTENDED
            ),
            RankingSide.GAINER,
        )
        for entry in gainers
    )
    return UniverseSnapshot(
        snapshot_id=UUID("00000000-0000-0000-0000-000000000002"),
        observed_at=at,
        utc_day=at.date(),
        config_hash="b" * 64,
        activated=True,
        ranking=RankingResult(
            candidates=(),
            gainers=gainers,
            losers=(),
            target_symbols=frozenset(entry.symbol for entry in gainers[:20]),
            exclusions={},
        ),
        memberships=memberships,
    )


def test_refresh_command_prints_snapshot_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[datetime] = []

    async def fake_refresh_once(config_path, observed_at):
        calls.append(observed_at)
        return fixture_snapshot()

    monkeypatch.setattr(main, "refresh_once", fake_refresh_once)
    result = runner.invoke(
        main.app,
        [
            "refresh-universe",
            "--config",
            "configs/environments/research.yaml",
            "--at",
            "2026-06-14T11:01:00Z",
        ],
    )

    assert result.exit_code == 0
    assert calls == [datetime(2026, 6, 14, 11, 1, tzinfo=UTC)]
    assert "target=1" in result.stdout
    assert "monitoring=1" in result.stdout
    assert "excluded=0" in result.stdout


def test_refresh_command_rejects_invalid_timestamp() -> None:
    result = runner.invoke(
        main.app,
        ["refresh-universe", "--at", "not-a-time"],
    )

    assert result.exit_code != 0


def test_parse_paper_exit_run_ids_normalizes_csv() -> None:
    assert main.parse_paper_exit_run_ids(" run-1,run-2, run-1, ,") == frozenset(
        {"run-1", "run-2"}
    )


def test_parse_live_position_account_labels_supports_multiple_accounts() -> None:
    assert main.parse_live_position_account_labels(
        " primary, account-2,primary "
    ) == frozenset({"primary", "account-2"})


def test_parse_live_position_account_labels_rejects_empty_tokens() -> None:
    with pytest.raises(ValueError, match="non-empty labels"):
        main.parse_live_position_account_labels("primary,,account-2")


async def test_protected_symbols_discover_live_accounts_with_positions() -> None:
    class FakePaperRepository:
        async def load_open_position_symbols(
            self,
            run_ids: frozenset[str],
        ) -> frozenset[str]:
            assert run_ids == frozenset({"paper-run"})
            return frozenset({"PAPERUSDT"})

    class FakeAccountRepository:
        def __init__(self) -> None:
            self.labels_calls: list[str] = []
            self.symbol_calls: list[str] = []

        async def load_active_position_account_labels(
            self,
            *,
            environment: str,
        ) -> frozenset[str]:
            self.labels_calls.append(environment)
            return frozenset({"primary", "account-2"})

        async def load_active_position_state(
            self,
            *,
            environment: str,
            account_label: str,
        ) -> SimpleNamespace:
            assert environment == "live"
            self.symbol_calls.append(account_label)
            return SimpleNamespace(symbols=frozenset({f"{account_label.upper()}USDT"}))

    accounts = FakeAccountRepository()

    symbols = await main._load_protected_symbols(
        paper_repository=FakePaperRepository(),
        account_repository=accounts,
        protected_run_ids=frozenset({"paper-run"}),
        configured_live_position_account_labels=frozenset({"primary"}),
    )

    assert symbols == frozenset({"PAPERUSDT", "PRIMARYUSDT", "ACCOUNT-2USDT"})
    assert accounts.labels_calls == ["live"]
    assert set(accounts.symbol_calls) == {"primary", "account-2"}


async def test_load_protected_symbols_succeeds_when_all_accounts_flat() -> None:
    class FakePaperRepository:
        async def load_open_position_symbols(
            self, protected_run_ids: Iterable[str]
        ) -> frozenset[str]:
            return frozenset()

    class FlatAccountRepository:
        async def load_active_position_account_labels(
            self, *, environment: str
        ) -> frozenset[str]:
            return frozenset({"primary", "account-2"})

        async def load_active_position_state(
            self, *, environment: str, account_label: str
        ) -> SimpleNamespace:
            return SimpleNamespace(symbols=frozenset())

    symbols = await main._load_protected_symbols(
        paper_repository=FakePaperRepository(),
        account_repository=FlatAccountRepository(),
        protected_run_ids=frozenset(),
        configured_live_position_account_labels=frozenset({"primary"}),
    )

    assert symbols == frozenset()


async def test_load_protected_symbols_propagates_account_exception() -> None:
    class FakePaperRepository:
        async def load_open_position_symbols(
            self, protected_run_ids: Iterable[str]
        ) -> frozenset[str]:
            return frozenset({"PAPERUSDT"})

    class FailingAccountRepository:
        async def load_active_position_account_labels(
            self, *, environment: str
        ) -> frozenset[str]:
            return frozenset({"primary", "account-2"})

        async def load_active_position_state(
            self, *, environment: str, account_label: str
        ) -> SimpleNamespace:
            if account_label == "account-2":
                raise RuntimeError("account state query failed")
            return SimpleNamespace(symbols=frozenset({"PRIMARYUSDT"}))

    with pytest.raises(RuntimeError, match="account state query failed"):
        await main._load_protected_symbols(
            paper_repository=FakePaperRepository(),
            account_repository=FailingAccountRepository(),
            protected_run_ids=frozenset({"paper-run"}),
            configured_live_position_account_labels=frozenset({"primary"}),
        )


async def test_load_protected_symbols_propagates_label_discovery_failure() -> None:
    class FakePaperRepository:
        async def load_open_position_symbols(
            self, protected_run_ids: Iterable[str]
        ) -> frozenset[str]:
            return frozenset({"PAPERUSDT"})

    class FailingDiscoveryAccountRepository:
        async def load_active_position_account_labels(
            self, *, environment: str
        ) -> frozenset[str]:
            raise RuntimeError("database timeout during discovery")

        async def load_active_position_state(
            self, *, environment: str, account_label: str
        ) -> SimpleNamespace:
            return SimpleNamespace(symbols=frozenset({"CONFIGUREDUSDT"}))

    with pytest.raises(RuntimeError, match="database timeout during discovery"):
        await main._load_protected_symbols(
            paper_repository=FakePaperRepository(),
            account_repository=FailingDiscoveryAccountRepository(),
            protected_run_ids=frozenset({"paper-run"}),
            configured_live_position_account_labels=frozenset({"primary"}),
        )


async def test_operational_retention_uses_bounded_batches() -> None:
    class RecordingRetention:
        def __init__(self) -> None:
            self.calls: list[tuple[str, int]] = []

        async def prune_contract_metadata(
            self,
            *,
            before: datetime,
            batch_size: int,
        ) -> int:
            del before
            self.calls.append(("contract", batch_size))
            return 0

        async def prune_runtime_market_states(
            self,
            *,
            before: datetime,
            batch_size: int,
        ) -> int:
            del before
            self.calls.append(("runtime", batch_size))
            return 0

        async def ensure_strategy_runtime_event_partitions(self) -> int:
            self.calls.append(("event_partitions", 0))
            return 0

    repository = RecordingRetention()

    await main.prune_operational_database_once(
        repository,
        authority=main.RetentionAuthority(InMemoryRetentionRepository()),
        now=datetime(2026, 6, 14, 11, 1, tzinfo=UTC),
    )

    assert repository.calls == [
        ("contract", 250),
        ("runtime", 250),
        ("event_partitions", 0),
    ]


async def test_retention_emits_prune_outcome_with_partitions() -> None:
    from crypto_momentum_lab.domain.operational.retention_authority import (
        InMemoryRetentionRepository,
        RetentionAuthority,
    )
    from crypto_momentum_lab.domain.operational.retention_models import (
        PruneReceiptStatus,
    )

    class PartitionedRetention:
        async def prune_contract_metadata(
            self,
            *,
            before: datetime,
            batch_size: int,
        ) -> int:
            del before, batch_size
            return 5

        async def prune_runtime_market_states(
            self,
            *,
            before: datetime,
            batch_size: int,
        ) -> int:
            del before, batch_size
            return 2

        async def ensure_strategy_runtime_event_partitions(self) -> int:
            return 0

    mem_repo = InMemoryRetentionRepository()
    authority = RetentionAuthority(mem_repo)
    repository = PartitionedRetention()
    await main.prune_operational_database_once(
        repository,  # type: ignore[arg-type]
        now=datetime(2026, 6, 14, 11, 1, tzinfo=UTC),
        authority=authority,
    )
    receipts = list(mem_repo.receipts.values())
    assert len(receipts) == 1
    assert receipts[0].status == PruneReceiptStatus.SUCCESS
    assert receipts[0].rows_deleted == 5
    assert receipts[0].partitions_dropped == 2


@pytest.mark.parametrize("external_days", [0, 45])
async def test_operational_retention_records_consumer_cutoff_before_execution(
    external_days: int,
) -> None:
    from crypto_momentum_lab.domain.operational.retention_contract import (
        RetentionConsumerRequirement,
    )
    from crypto_momentum_lab.domain.operational.retention_models import RecoverySpec

    now = datetime(2026, 10, 7, 11, tzinfo=UTC)
    watermark = datetime(2026, 9, 2, 13, tzinfo=UTC)
    requirement = RetentionConsumerRequirement(
        consumer_id="active_strategy_checkpoints",
        min_required_watermark=watermark,
        reason="checkpoint recovery baseline",
    )
    storage = InMemoryRetentionRepository()
    authority = main.RetentionAuthority(storage)
    if external_days:
        await authority.register_dependency_async(
            consumer_id="external_recovery",
            generation=1,
            recovery_spec=RecoverySpec(
                source_dataset="runtime_market_states_15s",
                earliest_needed_watermark=now - timedelta(days=external_days),
            ),
        )
    effective = min(watermark, now - timedelta(days=external_days))
    repository = SimpleNamespace(
        prune_contract_metadata=AsyncMock(return_value=0),
        prune_runtime_market_states=AsyncMock(return_value=0),
        ensure_strategy_runtime_event_partitions=AsyncMock(return_value=0),
    )
    await main.prune_operational_database_once(
        repository,
        authority=authority,
        now=now,
        consumer_requirements=(requirement,),
    )

    plan = next(iter(storage.plans.values()))
    receipt = next(iter(storage.receipts.values()))
    assert plan.requested_cutoff == now - timedelta(hours=12)
    assert plan.effective_cutoff == effective
    assert plan.is_constrained
    assert plan.binding_consumer_id is not None
    assert receipt.effective_cutoff == effective
    assert repository.prune_runtime_market_states.await_args.kwargs == {
        "before": effective,
        "batch_size": 250,
    }
    assert repository.prune_contract_metadata.await_args.kwargs == {
        "before": effective,
        "batch_size": 250,
    }


async def test_operational_retention_refreshes_its_own_protection_floor() -> None:
    from crypto_momentum_lab.domain.operational.retention_contract import (
        RetentionConsumerRequirement,
    )

    now = datetime(2026, 10, 7, 11, tzinfo=UTC)
    storage = InMemoryRetentionRepository()
    authority = main.RetentionAuthority(storage)
    repository = SimpleNamespace(
        prune_contract_metadata=AsyncMock(return_value=0),
        prune_runtime_market_states=AsyncMock(return_value=0),
        ensure_strategy_runtime_event_partitions=AsyncMock(return_value=0),
    )
    await main.prune_operational_database_once(
        repository,
        authority=authority,
        now=now,
        consumer_requirements=(
            RetentionConsumerRequirement(
                consumer_id="checkpoint",
                min_required_watermark=now - timedelta(days=30),
                reason="recovery",
            ),
        ),
    )
    await main.prune_operational_database_once(
        repository, authority=authority, now=now + timedelta(minutes=5)
    )
    assert len(storage.dependencies) == 1
    expected = now + timedelta(minutes=5) - timedelta(hours=12)
    assert (
        repository.prune_runtime_market_states.await_args.kwargs["before"] == expected
    )
    assert list(storage.receipts.values())[-1].effective_cutoff == expected


async def test_operational_retention_surfaces_failed_receipt() -> None:
    storage = InMemoryRetentionRepository()
    repository = SimpleNamespace(
        prune_contract_metadata=AsyncMock(side_effect=RuntimeError("database timeout")),
        prune_runtime_market_states=AsyncMock(return_value=0),
        ensure_strategy_runtime_event_partitions=AsyncMock(return_value=0),
    )
    with pytest.raises(RuntimeError, match="EXECUTION_FAILED.*database timeout"):
        await main.prune_operational_database_once(
            repository, authority=main.RetentionAuthority(storage)
        )
    repository.prune_runtime_market_states.assert_not_awaited()
    repository.ensure_strategy_runtime_event_partitions.assert_not_awaited()


def test_run_market_data_uses_combined_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: list[Path] = []

    async def fake_run(
        config_path: Path,
        *,
        stop_requested: asyncio.Event | None = None,
    ) -> None:
        del stop_requested
        called.append(config_path)

    monkeypatch.setattr(main, "run_market_data", fake_run)
    result = runner.invoke(
        main.app,
        [
            "run-market-data",
            "--config",
            "configs/environments/research.yaml",
        ],
    )

    assert result.exit_code == 0
    assert called == [Path("configs/environments/research.yaml")]


async def test_run_market_data_until_stopped_cancels_and_awaits_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    cleaned_up = asyncio.Event()

    async def fake_run(
        config_path: Path,
        *,
        stop_requested: asyncio.Event | None = None,
    ) -> None:
        del config_path
        assert stop_requested is not None
        started.set()
        try:
            await stop_requested.wait()
        finally:
            cleaned_up.set()

    monkeypatch.setattr(main, "run_market_data", fake_run)
    stop_requested = asyncio.Event()
    task = asyncio.create_task(
        main.run_market_data_until_stopped(Path("server.yaml"), stop_requested)
    )
    await started.wait()

    stop_requested.set()
    await asyncio.wait_for(task, timeout=1)

    assert cleaned_up.is_set()

async def test_run_market_data_keeps_consumer_alive_while_capture_stops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeHealth:
        def __init__(self) -> None:
            self.heartbeats: list[bool] = []
            self.readiness: list[dict[str, object]] = []
            self.stopped_called = False

        def heartbeat(self, *, database_ok: bool = False) -> None:
            self.heartbeats.append(database_ok)

        def write_readiness(self, payload: dict[str, object]) -> None:
            self.readiness.append(payload)

        def stopped(self) -> None:
            self.stopped_called = True

    class FakeCapture:
        def __init__(self) -> None:
            self.run_started = asyncio.Event()
            self.run_finished = asyncio.Event()
            self.run_cancelled = False
            self.stop_called = False

        async def start(self, **kwargs) -> None:
            del kwargs

        async def run(self) -> None:
            self.run_started.set()
            try:
                await self.run_finished.wait()
            except asyncio.CancelledError:
                self.run_cancelled = True
                raise

        async def stop(self) -> None:
            self.stop_called = True
            self.run_finished.set()

        def metrics_snapshot(self):
            return SimpleNamespace(
                queue_events=0,
                queue_bytes=0,
                monitoring_symbols=1,
            )

    class FakeUniverse:
        async def refresh(self, *, observed_at: datetime) -> UniverseSnapshot:
            del observed_at
            return fixture_snapshot()

    class FakeStateHub:
        def __init__(self) -> None:
            self.started = False
            self.stopped = False

        async def start(self) -> None:
            self.started = True

        async def stop(self) -> None:
            self.stopped = True

    class FakePublisher:
        def __init__(self) -> None:
            self.started = False
            self.stopped = False
            self.metrics = SimpleNamespace(latest_watermark_at=None)

        def lateness_metrics_snapshot(self) -> dict[str, object]:
            return {}

        async def start(self) -> None:
            self.started = True

        async def stop(self) -> None:
            self.stopped = True

    async def block_forever(*args, **kwargs) -> None:
        del args, kwargs
        await asyncio.Event().wait()

    capture = FakeCapture()
    state_hub = FakeStateHub()
    publisher = FakePublisher()
    runtime = SimpleNamespace(
        quote_hub=SimpleNamespace(start=AsyncMock(), stop=AsyncMock()),
        quote_volume_publisher=None,
        daily_open_prefetcher=None,
        operational_retention=None,
        maintenance_session_factory=None,
        maintenance_capture_repository=None,
        capture=capture,
        connection_pool=SimpleNamespace(
            metrics_snapshot=lambda: SimpleNamespace(
                active_connections=1,
                ready_connections=1,
                reconnect_count=0,
                ack_mismatch_count=0,
                control_commands_sent=1,
                received_messages=1,
            )
        ),
        state_hub=state_hub,
        initial_symbols=frozenset({"BTCUSDT"}),
        enabled_streams=(CaptureStream.AGG_TRADE,),
        universe=FakeUniverse(),
        universe_activation_minute=1,
        universe_refresh_interval_minutes=15,
        runtime_state_publisher=publisher,
        subscription_observer=object(),
        capture_repository=object(),
        archive_root=Path("raw"),
        archive_retention_days=7,
        archive_retention_interval_seconds=3600,
        capture_shutdown_timeout_seconds=30,
    )

    durable_state_callbacks = []

    @asynccontextmanager
    async def fake_runtime(
        config_path: Path,
        *,
        on_durable_state_persisted=None,
        startup_timer=None,
    ):
        assert startup_timer is not None
        del config_path
        assert on_durable_state_persisted is not None
        durable_state_callbacks.append(on_durable_state_persisted)
        yield runtime

    health = FakeHealth()
    monkeypatch.setattr(
        main.LocalHealthWriter,
        "from_environment",
        lambda: health,
    )
    monkeypatch.setattr(main, "build_market_data_runtime", fake_runtime)
    monkeypatch.setattr(main, "run_scheduler_loop", block_forever)
    monkeypatch.setattr(main, "monitor_market_data_freshness", block_forever)
    monkeypatch.setattr(main, "monitor_market_data_health", block_forever)
    monkeypatch.setattr(
        main,
        "reconcile_paper_exit_subscriptions",
        block_forever,
    )
    monkeypatch.setattr(main, "run_raw_archive_retention_loop", block_forever)
    stop_requested = asyncio.Event()
    task = asyncio.create_task(
        main.run_market_data(Path("server.yaml"), stop_requested=stop_requested)
    )
    await capture.run_started.wait()

    # Building the runtime and scheduling capture must not make Docker see a
    # ready market-data process. Only a completed durable state can do that.
    assert health.heartbeats == []
    assert health.readiness == []
    durable_state_callbacks[0](datetime(2026, 10, 6, 1, 2, tzinfo=UTC))
    assert health.heartbeats == [True]
    assert health.readiness == [
        {
            "service": "market-data",
            "startup_ready": True,
            "durable_state_watermark": "2026-10-06T01:02:00+00:00",
        }
    ]

    stop_requested.set()
    await asyncio.wait_for(task, timeout=1)

    assert capture.stop_called is True
    assert capture.run_finished.is_set()
    assert capture.run_cancelled is False
    assert state_hub.started is True
    assert state_hub.stopped is True
    assert publisher.started is True
    assert publisher.stopped is True


async def test_scheduler_propagates_cancellation_cleanly() -> None:
    class FakeService:
        async def refresh(self, *, observed_at: datetime) -> UniverseSnapshot:
            raise AssertionError("refresh must not run after cancellation")

    async def cancelled_sleep(seconds: float) -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await run_scheduler_loop(
            FakeService(),
            activation_minute=1,
            clock=lambda: datetime(2026, 6, 14, 10, 30, tzinfo=UTC),
            sleeper=cancelled_sleep,
        )


async def test_scheduler_retries_failed_refresh_without_losing_schedule() -> None:
    refresh_calls: list[datetime] = []
    sleep_calls: list[float] = []

    class FlakyService:
        async def refresh(self, *, observed_at: datetime) -> UniverseSnapshot:
            refresh_calls.append(observed_at)
            if len(refresh_calls) == 1:
                raise RuntimeError("temporary database failure")
            raise asyncio.CancelledError

    async def record_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        if len(sleep_calls) >= 4:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await run_scheduler_loop(
            FlakyService(),
            activation_minute=1,
            retry_delay_seconds=0.25,
            clock=lambda: datetime(2026, 6, 14, 10, 30, tzinfo=UTC),
            sleeper=record_sleep,
        )

    assert refresh_calls == [
        datetime(2026, 6, 14, 11, 1, tzinfo=UTC),
        datetime(2026, 6, 14, 11, 1, tzinfo=UTC),
    ]
    assert 0.25 in sleep_calls


async def test_paper_exit_reconcile_retries_transient_failure() -> None:
    calls = 0
    sleep_calls: list[float] = []

    class FlakyObserver:
        async def refresh_protected_symbols(self) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("temporary database failure")
            raise asyncio.CancelledError

    async def record_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        if len(sleep_calls) >= 4:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await main.reconcile_paper_exit_subscriptions(
            FlakyObserver(),
            interval_seconds=0.1,
            retry_delay_seconds=0.25,
            sleeper=record_sleep,
        )

    assert calls == 2
    assert 0.25 in sleep_calls


async def test_capture_observer_applies_membership_symbols() -> None:
    class FakeCapture:
        def __init__(self) -> None:
            self.calls = []

        async def apply_symbols(self, symbols, *, streams, generation) -> None:
            self.calls.append((symbols, streams, generation))

    capture = FakeCapture()
    changed_symbols = []
    observer = main.CaptureUniverseObserver(
        capture,
        streams=(CaptureStream.AGG_TRADE,),
        initial_generation=1,
        on_symbols_changed=changed_symbols.append,
    )
    snapshot = fixture_snapshot()

    await observer.snapshot_updated(snapshot)

    assert capture.calls == [
        (
            frozenset({"BTCUSDT"}),
            (CaptureStream.AGG_TRADE,),
            2,
        )
    ]
    assert changed_symbols == [frozenset({"BTCUSDT"})]


async def test_capture_observer_limits_trade_streams_to_top_gainer_rank() -> None:
    class FakeCapture:
        def __init__(self) -> None:
            self.calls = []

        async def apply_symbols(self, symbols, *, streams, generation) -> None:
            self.calls.append((symbols, streams, generation))

    capture = FakeCapture()
    changed: list[frozenset[str]] = []
    observer = main.CaptureUniverseObserver(
        capture,
        streams=(CaptureStream.AGG_TRADE, CaptureStream.FORCE_ORDER),
        initial_generation=1,
        full_stream_max_gainer_rank=30,
        on_symbols_changed=changed.append,
    )

    await observer.snapshot_updated(fixture_tiered_snapshot())

    applied = capture.calls[0][0]
    assert len(applied) == 30
    assert applied == frozenset(f"S{rank:02d}USDT" for rank in range(1, 31))
    assert "S31USDT" not in applied
    assert "S40USDT" not in applied
    assert changed == [applied]


async def test_capture_observer_tier_zero_keeps_all_monitoring_symbols() -> None:
    class FakeCapture:
        def __init__(self) -> None:
            self.calls = []

        async def apply_symbols(self, symbols, *, streams, generation) -> None:
            self.calls.append(symbols)

    capture = FakeCapture()
    observer = main.CaptureUniverseObserver(
        capture,
        streams=(CaptureStream.AGG_TRADE,),
        initial_generation=1,
        full_stream_max_gainer_rank=0,
    )

    await observer.snapshot_updated(fixture_tiered_snapshot())

    assert capture.calls[0] == frozenset(f"S{rank:02d}USDT" for rank in range(1, 41))


async def test_capture_observer_tier_promotes_when_rank_improves() -> None:
    class FakeCapture:
        def __init__(self) -> None:
            self.calls = []

        async def apply_symbols(self, symbols, *, streams, generation) -> None:
            self.calls.append(symbols)

    capture = FakeCapture()
    observer = main.CaptureUniverseObserver(
        capture,
        streams=(CaptureStream.AGG_TRADE,),
        initial_generation=1,
        full_stream_max_gainer_rank=30,
    )
    first = fixture_tiered_snapshot()
    await observer.snapshot_updated(first)
    assert "S35USDT" not in capture.calls[-1]

    promoted = replace(
        first,
        ranking=replace(
            first.ranking,
            gainers=tuple(
                replace(entry, rank=12) if entry.symbol == "S35USDT" else entry
                for entry in first.ranking.gainers
            ),
        ),
    )
    await observer.snapshot_updated(promoted)
    assert "S35USDT" in capture.calls[-1]


async def test_capture_observer_backfills_when_t1_promotes_into_must_warm() -> None:
    class FakeCapture:
        async def apply_symbols(self, symbols, *, streams, generation) -> None:
            return None

    backfilled: list[frozenset[str]] = []

    async def on_promoted(symbols: frozenset[str]) -> None:
        backfilled.append(symbols)

    observer = main.CaptureUniverseObserver(
        FakeCapture(),
        streams=(CaptureStream.AGG_TRADE,),
        initial_generation=1,
        full_stream_max_gainer_rank=30,
        must_warm_max_gainer_rank=20,
        on_trade_symbols_promoted=on_promoted,
    )
    first = fixture_tiered_snapshot()
    # Move S15 out of the must-warm band so the next refresh is a real T1→T0.
    first = replace(
        first,
        ranking=replace(
            first.ranking,
            gainers=tuple(
                replace(entry, rank=25) if entry.symbol == "S15USDT" else entry
                for entry in first.ranking.gainers
            ),
        ),
    )
    # First apply is startup: no promotion callback.
    await observer.snapshot_updated(first)
    assert backfilled == []

    # S15 was already in the trade tier (rank 25).  Crossing into the
    # must-warm band (rank <= 20) after only a few minutes must still
    # REST-backfill even though the subscription set is unchanged.
    promoted = replace(
        first,
        observed_at=first.observed_at + timedelta(minutes=5),
        ranking=replace(
            first.ranking,
            gainers=tuple(
                replace(entry, rank=5) if entry.symbol == "S15USDT" else entry
                for entry in first.ranking.gainers
            ),
        ),
    )
    await observer.snapshot_updated(promoted)
    await asyncio.sleep(0)
    assert backfilled == [frozenset({"S15USDT"})]

    # After the optimistic warm mark, the next refresh must not re-fetch.
    await observer.snapshot_updated(
        replace(
            promoted,
            observed_at=promoted.observed_at + timedelta(minutes=5),
        )
    )
    await asyncio.sleep(0)
    assert backfilled == [frozenset({"S15USDT"})]


async def test_capture_observer_skips_backfill_after_full_trade_tier_residence() -> (
    None
):
    class FakeCapture:
        async def apply_symbols(self, symbols, *, streams, generation) -> None:
            return None

    backfilled: list[frozenset[str]] = []

    async def on_promoted(symbols: frozenset[str]) -> None:
        backfilled.append(symbols)

    observer = main.CaptureUniverseObserver(
        FakeCapture(),
        streams=(CaptureStream.AGG_TRADE,),
        initial_generation=1,
        full_stream_max_gainer_rank=30,
        must_warm_max_gainer_rank=20,
        on_trade_symbols_promoted=on_promoted,
    )
    first = fixture_tiered_snapshot()
    first = replace(
        first,
        ranking=replace(
            first.ranking,
            gainers=tuple(
                replace(entry, rank=25) if entry.symbol == "S15USDT" else entry
                for entry in first.ranking.gainers
            ),
        ),
    )
    await observer.snapshot_updated(first)

    # 40 minutes later S15 has a full local window; T1→T0 needs no REST.
    promoted = replace(
        first,
        observed_at=first.observed_at + timedelta(minutes=40),
        ranking=replace(
            first.ranking,
            gainers=tuple(
                replace(entry, rank=5) if entry.symbol == "S15USDT" else entry
                for entry in first.ranking.gainers
            ),
        ),
    )
    await observer.snapshot_updated(promoted)
    await asyncio.sleep(0)
    assert backfilled == []


async def test_capture_observer_keeps_open_position_symbols_subscribed() -> None:
    class FakeCapture:
        def __init__(self) -> None:
            self.calls = []

        async def apply_symbols(self, symbols, *, streams, generation) -> None:
            self.calls.append((symbols, streams, generation))

    protected_symbols = frozenset({"OLDUSDT"})

    async def load_protected_symbols() -> frozenset[str]:
        return protected_symbols

    capture = FakeCapture()
    observer = main.CaptureUniverseObserver(
        capture,
        streams=(CaptureStream.AGG_TRADE,),
        initial_generation=3,
        protected_symbol_loader=load_protected_symbols,
    )

    await observer.snapshot_updated(fixture_snapshot())
    await observer.refresh_protected_symbols()

    assert capture.calls == [
        (
            frozenset({"BTCUSDT", "OLDUSDT"}),
            (CaptureStream.AGG_TRADE,),
            4,
        )
    ]

    protected_symbols = frozenset()
    await observer.refresh_protected_symbols()

    assert capture.calls[-1] == (
        frozenset({"BTCUSDT"}),
        (CaptureStream.AGG_TRADE,),
        5,
    )


async def test_logging_refresh_service_times_out_stalled_refresh() -> None:
    class StalledRefreshService:
        async def refresh(self, *, observed_at: datetime) -> UniverseSnapshot:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    service = main.LoggingRefreshService(
        StalledRefreshService(),
        timeout_seconds=0.001,
    )

    with pytest.raises(TimeoutError):
        await service.refresh(observed_at=datetime(2026, 7, 28, 0, 0, tzinfo=UTC))


async def test_market_data_watchdog_rejects_missing_startup_data() -> None:
    now = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    clock_values = iter((now, now + timedelta(seconds=121)))

    async def no_sleep(_: float) -> None:
        return None

    with pytest.raises(main.MarketDataStaleError, match="no market data"):
        await main.monitor_market_data_freshness(
            latest_observed_at=lambda: None,
            startup_grace_seconds=120,
            stale_after_seconds=120,
            check_interval_seconds=1,
            clock=lambda: next(clock_values),
            sleeper=no_sleep,
        )


async def test_market_data_watchdog_rejects_stale_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    records: list[tuple[str, dict[str, object]]] = []

    class FakeLog:
        def error(self, event: str, **fields: object) -> None:
            records.append((event, fields))

    monkeypatch.setattr(main, "log", FakeLog())

    async def no_sleep(_: float) -> None:
        return None

    with pytest.raises(main.MarketDataStaleError, match="market data stale"):
        await main.monitor_market_data_freshness(
            latest_observed_at=lambda: now - timedelta(seconds=121),
            startup_grace_seconds=120,
            stale_after_seconds=120,
            check_interval_seconds=1,
            clock=lambda: now,
            sleeper=no_sleep,
            diagnostic_snapshot=lambda: {"incident_marker": "stale-feed"},
        )

    assert records == [
        (
            "market_data_freshness_violation",
            {
                "reason": "market_data_stale",
                "age_seconds": 121.0,
                "observed_at": (now - timedelta(seconds=121)).isoformat(),
                "diagnostics": {"incident_marker": "stale-feed"},
            },
        )
    ]


async def test_capture_observer_reports_membership_churn() -> None:
    """Entering/leaving the monitored set must be observable.

    Downstream a symbol looks identical whether it never had a bucket or
    simply was not subscribed, so the churn has to be recorded where it is
    actually known.
    """

    class FakeCapture:
        def __init__(self) -> None:
            self.calls = []

        async def apply_symbols(self, symbols, *, streams, generation) -> None:
            self.calls.append((symbols, streams, generation))

    first = fixture_snapshot()
    second = replace(
        first,
        memberships=(
            *first.memberships,
            TrackedMembership(
                "ETHUSDT",
                MembershipStatus.TARGET,
                RankingSide.GAINER,
            ),
        ),
    )

    capture = FakeCapture()
    observer = main.CaptureUniverseObserver(
        capture,
        streams=(CaptureStream.AGG_TRADE,),
        initial_generation=1,
    )

    with capture_logs() as logs:
        await observer.snapshot_updated(first)
        await observer.snapshot_updated(second)

    churn = [entry for entry in logs if entry["event"] == "capture_symbols_changed"]
    # The first snapshot establishes the baseline; only the second reports churn.
    assert len(churn) == 1
    assert churn[0]["added"] == 1
    assert churn[0]["removed"] == 0
    assert churn[0]["added_symbols"] == ["ETHUSDT"]
    assert churn[0]["removed_symbols"] == []


async def test_capture_observer_reports_symbols_leaving_the_set() -> None:
    """A departing symbol is reported too -- that is the other half of churn."""

    class FakeCapture:
        def __init__(self) -> None:
            self.calls = []

        async def apply_symbols(self, symbols, *, streams, generation) -> None:
            self.calls.append((symbols, streams, generation))

    first = fixture_snapshot()
    second = replace(first, memberships=())

    capture = FakeCapture()
    observer = main.CaptureUniverseObserver(
        capture,
        streams=(CaptureStream.AGG_TRADE,),
        initial_generation=1,
    )

    with capture_logs() as logs:
        await observer.snapshot_updated(first)
        await observer.snapshot_updated(second)

    churn = [entry for entry in logs if entry["event"] == "capture_symbols_changed"]
    assert len(churn) == 1
    assert churn[0]["added"] == 0
    assert churn[0]["removed"] == 1
    assert churn[0]["removed_symbols"] == ["BTCUSDT"]


async def test_capture_observer_keeps_recently_removed_symbols_for_prewarm() -> None:
    """A short ranking dip must not break the rolling strategy history."""

    class FakeCapture:
        def __init__(self) -> None:
            self.calls = []

        async def apply_symbols(self, symbols, *, streams, generation) -> None:
            self.calls.append((symbols, streams, generation))

    first = fixture_snapshot()
    left_at = first.observed_at + timedelta(minutes=5)
    left = replace(
        first,
        observed_at=left_at,
        memberships=(),
    )
    expired = replace(
        left,
        observed_at=left_at + timedelta(minutes=41),
    )

    capture = FakeCapture()
    observer = main.CaptureUniverseObserver(
        capture,
        streams=(CaptureStream.AGG_TRADE,),
        initial_generation=1,
        prewarm_retention_minutes=40,
        max_prewarm_symbols=1,
    )

    await observer.snapshot_updated(first)
    await observer.snapshot_updated(left)

    # The symbol remains subscribed during the prewarm grace period.
    assert capture.calls[-1][0] == frozenset({"BTCUSDT"})
    assert len(capture.calls) == 1

    await observer.snapshot_updated(expired)

    assert capture.calls[-1][0] == frozenset()
    assert len(capture.calls) == 2


async def test_capture_observer_trade_tier_retains_falling_symbols() -> None:
    class FakeCapture:
        def __init__(self) -> None:
            self.calls = []

        async def apply_symbols(self, symbols, *, streams, generation) -> None:
            self.calls.append((symbols, streams, generation))

    capture = FakeCapture()
    observer = main.CaptureUniverseObserver(
        capture,
        streams=(CaptureStream.AGG_TRADE,),
        initial_generation=1,
        full_stream_max_gainer_rank=30,
        prewarm_retention_minutes=40,
        max_prewarm_symbols=1,
    )

    first = fixture_tiered_snapshot()
    await observer.snapshot_updated(first)
    assert "S25USDT" in capture.calls[-1][0]
    assert "S35USDT" not in capture.calls[-1][0]

    # S25 drops to rank 35 (in extended universe, > 30).
    # Since it was in the trade tier, it must be retained in prewarm.
    second = replace(
        first,
        observed_at=first.observed_at + timedelta(minutes=5),
        ranking=replace(
            first.ranking,
            gainers=tuple(
                replace(entry, rank=35) if entry.symbol == "S25USDT" else entry
                for entry in first.ranking.gainers
            ),
        ),
    )
    await observer.snapshot_updated(second)
    assert "S25USDT" in capture.calls[-1][0]

    # After 41 minutes (> 40m retention), S25 should expire and be dropped.
    expired = replace(
        second,
        observed_at=second.observed_at + timedelta(minutes=41),
    )
    await observer.snapshot_updated(expired)
    assert "S25USDT" not in capture.calls[-1][0]


async def test_capture_observer_bounds_prewarm_to_nearest_ranked_demotions() -> None:
    class FakeCapture:
        def __init__(self) -> None:
            self.calls = []

        async def apply_symbols(self, symbols, *, streams, generation) -> None:
            self.calls.append((symbols, streams, generation))

    capture = FakeCapture()
    observer = main.CaptureUniverseObserver(
        capture,
        streams=(CaptureStream.AGG_TRADE,),
        initial_generation=1,
        full_stream_max_gainer_rank=30,
        prewarm_retention_minutes=40,
        max_prewarm_symbols=2,
    )
    first = fixture_tiered_snapshot()
    await observer.snapshot_updated(first)

    replacement_rank = {
        "S28USDT": 31,
        "S29USDT": 32,
        "S30USDT": 33,
        "S31USDT": 28,
        "S32USDT": 29,
        "S33USDT": 30,
    }
    second = replace(
        first,
        observed_at=first.observed_at + timedelta(minutes=1),
        ranking=replace(
            first.ranking,
            gainers=tuple(
                replace(entry, rank=replacement_rank.get(entry.symbol, entry.rank))
                for entry in first.ranking.gainers
            ),
        ),
    )
    await observer.snapshot_updated(second)

    symbols = capture.calls[-1][0]
    assert {"S28USDT", "S29USDT"} <= symbols
    assert "S30USDT" not in symbols
    assert len(symbols) == 32


async def test_capture_observer_watch_only_symbols_do_not_gain_trade_stream_on_exit(
) -> None:
    class FakeCapture:
        def __init__(self) -> None:
            self.calls = []

        async def apply_symbols(self, symbols, *, streams, generation) -> None:
            self.calls.append((symbols, streams, generation))

    capture = FakeCapture()
    observer = main.CaptureUniverseObserver(
        capture,
        streams=(CaptureStream.AGG_TRADE,),
        initial_generation=1,
        full_stream_max_gainer_rank=30,
        prewarm_retention_minutes=40,
        max_prewarm_symbols=1,
    )

    first = fixture_tiered_snapshot()
    await observer.snapshot_updated(first)
    assert "S35USDT" not in capture.calls[-1][0]

    # S35 was in watch-only (rank 35). It leaves the universe entirely.
    second = replace(
        first,
        observed_at=first.observed_at + timedelta(minutes=5),
        memberships=tuple(m for m in first.memberships if m.symbol != "S35USDT"),
        ranking=replace(
            first.ranking,
            gainers=tuple(g for g in first.ranking.gainers if g.symbol != "S35USDT"),
        ),
    )
    await observer.snapshot_updated(second)

    # S35 was NEVER in the trade tier; leaving the universe must NOT give it
    # an aggTrade stream.
    assert "S35USDT" not in capture.calls[-1][0]
