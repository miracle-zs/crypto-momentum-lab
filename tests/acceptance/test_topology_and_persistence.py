"""SYSTEM.md section 8 acceptance: target topology and lean persistence.

This file closes the two acceptance items that SYSTEM.md 8.1 still lists as
uncovered ("目标单进程装配与唯一派发者" and "目标单进程装配与精简持久化增长").
Every case drives the real ``ExecutionBook``, the real PostgreSQL repositories
and the real trading-lease repository against the local test database.  Only
the external boundaries are substitutable (exchange network, clock); nothing
here mocks the target engine or re-implements decision, settlement or recovery
logic.

Row "目标单进程装配与唯一派发者" (T3)
    Q5/T3 require account synchronisation and trading execution to be assembled
    as ONE process per account with the ``read``/``trade`` credential roles
    preserved inside it, and exactly one effective writer per account.  The
    credential-isolation, four-account and single-order-dispatcher half of this
    row is already proven by ``tests/smoke/test_server_deployment_manifest.py``
    and is deliberately not repeated.  Added here:

    * per-account process assembly, derived from the merged deployment
      descriptor by account label and credential role only - never by service
      name (EXPECTED FAIL today: every account is still assembled as a read-only
      synchroniser plus a separate trade writer);
    * durable account-level writer exclusivity of the real trading lease
      repository: while one writer holds the active lease a competing writer is
      refused, an expired lease is no longer effective, and after hand-over
      exactly one active writer exists for the account.

    NOT EXECUTABLE: "切换不得双跑旧新写入者" during cut-over.  No ownership
    token or control plane can observe whether two writers are simultaneously in
    effect.  SYSTEM.md 8.1 records the minimal missing contract: input
    ``{environment, account_label, desired_writer_id, schema/format_version}``
    -> output ``{allowed, blockers[]}`` enumerating open positions,
    active/unresolved orders, unknown submissions and unsettled
    reservations/settlement, plus a durable dispatch-ownership token acquired
    before the first write.  The only existing surfaces are entry-stop
    (``LiveEntryControlGate``/``DRAINING``) and the single-position
    incompatible-format refusal in ``ExecutionBook.restore``; neither can show
    that two writers are both live, so no assertion is fabricated for it.

Row "目标单进程装配与精简持久化增长" - lean persistence half (Q7)
    Necessary execution evidence survives a restart in a brand new
    process-level ``ExecutionBook.restore``: the accepted command, the real
    fill, the reservation, the active batch and the recoverable current state.
    Repeated meaningless observations must not grow durable facts without
    bound; measured as real row counts before and after a repeated-observation
    burst.  Re-observing the same envelope identity is a complete no-op, and
    re-observing an already-recorded snapshot/coverage/fill datum is expected to
    add no durable state row either (EXPECTED FAIL today: each redelivery
    appends one durable journal state row - SYSTEM.md 4 requires the target
    writer to stop duplicating internal journal/checkpoint/policy snapshots).
    The dedup receipt index legitimately keeps one row per distinct envelope
    identity and is not asserted against.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
import yaml
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
)
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_action_models import (
    Accepted,
    ExecutionRequest,
)
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
    Duplicate,
    EvidenceConflict,
)
from crypto_momentum_lab.domain.execution.order_state import ExitAllocation
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    CoverageEvidence,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    AccountFacts,
)
from crypto_momentum_lab.domain.execution.snapshot_encoding import (
    stable_snapshot_anchor_id,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitPolicyMode,
    TradeCommandType,
)
from crypto_momentum_lab.domain.risk import TradingLease, TradingLeaseState
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
)
from crypto_momentum_lab.persistence.postgres.command_repository import (
    PostgresCommandRepository,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresExecutionUnitOfWork,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
    ExecutionEvidenceReceiptRow,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExecutionCommandRow,
    PositionReservationRow,
    TradingLeaseRow,
)
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
)
from crypto_momentum_lab.persistence.postgres.position_reservation_repository import (
    AsyncPostgresPositionReservationRepository,
)
from crypto_momentum_lab.persistence.postgres.risk_repository import (
    LeaseAlreadyHeldError,
    PostgresRiskRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)

pytestmark = pytest.mark.contract

NOW = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)

# The four live accounts are business identities, not service names; they are
# checked against the deployment descriptor so renaming a service cannot
# silently drop an account from the assertion.
LIVE_ACCOUNTS = frozenset({"primary", "account-2", "account-3", "account-4"})

CREDENTIAL_ROLES = ("READ", "TRADE")

BURST = 50


# --------------------------------------------------------------------------- #
# (A) Target per-account assembly and one effective writer
# --------------------------------------------------------------------------- #
def _merged_live_services() -> dict[str, dict[str, object]]:
    base = yaml.safe_load(Path("compose.server.yaml").read_text(encoding="utf-8"))[
        "services"
    ]
    overlay = yaml.safe_load(
        Path("compose.live.accounts.yaml").read_text(encoding="utf-8")
    )["services"]
    assert not set(base) & set(overlay), "overlay must not redefine an account"
    return {
        name: body
        for name, body in (base | overlay).items()
        if "live" in body.get("profiles", [])
    }


def _service_account(body: dict[str, object]) -> str | None:
    command = body.get("command", [])
    if "--account-label" in command:
        raw = command[command.index("--account-label") + 1]
    else:
        raw = body.get("environment", {}).get("CML_ACCOUNT_LABEL")
    if raw is None:
        return None
    if raw.startswith("${"):
        raw = raw.split(":-", 1)[1].removesuffix("}")
    return raw


def _service_roles(body: dict[str, object]) -> frozenset[str]:
    environment = body.get("environment", {})
    return frozenset(
        role for role in CREDENTIAL_ROLES if f"BINANCE_{role}_API_KEY" in environment
    )


def test_each_account_is_assembled_as_one_process_holding_both_roles() -> None:
    """Q5/T3: one process per account, holding the read and trade roles.

    An account is 'one process' when the merged deployment descriptor gives it
    exactly one live service, and that service is the one that both
    synchronises account facts (read credentials) and executes trades (trade
    credentials).  No service name is treated as the target architecture: the
    account label and the credential roles are the only inputs.
    """
    by_account: dict[str, list[tuple[str, frozenset[str]]]] = {
        account: [] for account in LIVE_ACCOUNTS
    }
    for name, body in _merged_live_services().items():
        account = _service_account(body)
        roles = _service_roles(body)
        if account in by_account and roles:
            by_account[account].append((name, roles))

    violations: dict[str, object] = {}
    for account in sorted(LIVE_ACCOUNTS):
        processes = by_account[account]
        if not processes:
            violations[account] = "no live process carries its credentials"
            continue
        if len(processes) != 1:
            violations[account] = {
                "live_processes": len(processes),
                "roles": {name: sorted(roles) for name, roles in processes},
            }
            continue
        roles = processes[0][1]
        if roles != frozenset(CREDENTIAL_ROLES):
            violations[account] = {"live_processes": 1, "roles": sorted(roles)}

    assert not violations, (
        "the target topology is one process per account that both synchronises "
        "account facts and executes trades; observed assembly: "
        f"{violations}"
    )


def _lease(
    account: str, owner: str, *, acquired_at: datetime, ttl: int
) -> TradingLease:
    return TradingLease(
        lease_id=f"lease-{uuid4().hex}",
        environment="live",
        account_label=account,
        strategy_name="orderflow_impulse",
        owner=owner,
        code_generation="acceptance",
        state=TradingLeaseState.ACTIVE,
        acquired_at=acquired_at,
        expires_at=acquired_at + timedelta(seconds=ttl),
    )


async def test_each_account_has_at_most_one_effective_writer_lease(
    async_database_url: str,
) -> None:
    """Exactly one effective writer per account; an expired lease is not one."""
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    repository = PostgresRiskRepository(factory)
    try:
        async with factory() as session, session.begin():
            await session.execute(
                delete(TradingLeaseRow).where(
                    TradingLeaseRow.environment == "live",
                    TradingLeaseRow.account_label.in_(sorted(LIVE_ACCOUNTS)),
                )
            )
        for account in sorted(LIVE_ACCOUNTS):
            first = _lease(account, "writer-a", acquired_at=NOW, ttl=60)
            await repository.acquire_lease(first)
            assert (await repository.load_active_lease("live", account, NOW)) == first

            competing = _lease(
                account, "writer-b", acquired_at=NOW + timedelta(seconds=1), ttl=60
            )
            with pytest.raises(LeaseAlreadyHeldError):
                await repository.acquire_lease(competing)

            # The first writer's lease expires unrenewed: the successor takes
            # over, and the old lease is no longer active, so the account never
            # has two effective writers at once.
            takeover_at = first.expires_at + timedelta(seconds=1)
            successor = _lease(account, "writer-b", acquired_at=takeover_at, ttl=60)
            await repository.acquire_lease(successor)

            evaluated_at = takeover_at + timedelta(seconds=1)
            active = await repository.load_active_lease("live", account, evaluated_at)
            assert active is not None
            assert active.owner == "writer-b"
            assert active.lease_id == successor.lease_id
            async with factory() as session:
                effective = int(
                    await session.scalar(
                        select(func.count())
                        .select_from(TradingLeaseRow)
                        .where(
                            TradingLeaseRow.environment == "live",
                            TradingLeaseRow.account_label == account,
                            TradingLeaseRow.state == TradingLeaseState.ACTIVE.value,
                            TradingLeaseRow.expires_at > evaluated_at,
                        )
                    )
                    or 0
                )
            assert effective == 1, (
                f"{account} has {effective} effective writers after hand-over"
            )
    finally:
        await engine.dispose()


# --------------------------------------------------------------------------- #
# (B) Lean persistence: recoverable evidence, bounded meaningless observation
# --------------------------------------------------------------------------- #
def _book(
    factory: async_sessionmaker,
) -> tuple[ExecutionBook, AsyncPostgresPositionReservationRepository]:
    commands = PostgresCommandRepository(factory)
    reservations = AsyncPostgresPositionReservationRepository(
        factory, strategy_name="topology-acceptance"
    )
    uow = AsyncPostgresExecutionUnitOfWork(
        factory,
        journal_store=PostgresAccountJournalStore(),
        command_repository=commands,
        reservation_repository=reservations,
    )
    return (
        ExecutionBook(
            command_repository=commands,
            reservation_repository=reservations,
            execution_unit_of_work=uow,
        ),
        reservations,
    )


def _scope(key: PositionKey) -> ExecutionScope:
    return ExecutionScope(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side,
    )


def _fill(
    key: PositionKey,
    *,
    trade_id: str,
    order_id: str,
    quantity: Decimal,
    at: datetime,
) -> AccountFillEvent:
    return AccountFillEvent(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        trade_id=trade_id,
        order_id=order_id,
        side="BUY",
        price=Decimal("100"),
        quantity=quantity,
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=at,
        raw_payload={"positionSide": key.position_side.value},
    )


def _fill_evidence(
    key: PositionKey,
    *,
    trade_id: str,
    order_id: str,
    quantity: Decimal,
    at: datetime,
) -> ExecutionEvidence:
    return ExecutionEvidence(
        evidence_id=f"fill-{trade_id}",
        scope=_scope(key),
        observed_at=at,
        fill=_fill(key, trade_id=trade_id, order_id=order_id, quantity=quantity, at=at),
        stream_id="account-events",
        stream_epoch="epoch-1",
        sequence=1,
    )


def _snapshot(key: PositionKey, at: datetime, quantity: str) -> AccountPositionSnapshot:
    amount = Decimal(quantity)
    return AccountPositionSnapshot(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
        position_amt=amount,
        entry_price=Decimal("100"),
        mark_price=Decimal("100"),
        unrealized_pnl=Decimal("0"),
        notional=abs(amount) * Decimal("100"),
        leverage=2,
        margin_type="cross",
        observed_at=at,
        raw_payload={
            "positionSide": key.position_side.value,
            "include_flat": amount == 0,
        },
    )


def _coverage(
    scope: AccountFactStreamScope,
    *,
    load_id: str,
    scan_origin: datetime,
    anchor_id: str,
    anchor_cut: datetime,
    anchor_kind: str,
    checked_through: datetime,
) -> CoverageEvidence:
    provenance = AccountFillLoadProvenance(
        stream_scope=scope,
        load_id=load_id,
        scan_origin_from_id=None,
        scan_origin_start_time_ms=int(scan_origin.timestamp() * 1000),
        request_from_id=None,
        next_from_id=None,
        page_count=1,
        page_exhausted=True,
        truncated=False,
        checked_through=checked_through,
        observed_at=checked_through,
        source_anchor_id=anchor_id,
        source_anchor_event_cut=anchor_cut,
        source_anchor_kind=anchor_kind,
    )
    endpoint_checkpoint_id = (
        anchor_id
        if anchor_kind == "zero_snapshot" and checked_through == anchor_cut
        else f"source-cut-{load_id}"
    )
    return CoverageEvidence(
        fill_cursor_id=load_id,
        fill_load_start=provenance.origin_start_at,
        fill_checked_through=checked_through,
        checkpoint_id=endpoint_checkpoint_id,
        checkpoint_event_cut=checked_through,
        stream_scope=scope,
        evidence_observed_at=checked_through,
        page_exhausted=True,
        not_truncated=True,
        load_provenance=provenance,
    )


def _scoped_evidence(
    key: PositionKey,
    *,
    evidence_id: str,
    scope: AccountFactStreamScope,
    at: datetime,
    sequence: int,
    proof: CoverageEvidence,
    snapshot: AccountPositionSnapshot | None = None,
    fill: AccountFillEvent | None = None,
) -> ExecutionEvidence:
    return ExecutionEvidence(
        evidence_id=evidence_id,
        scope=_scope(key),
        observed_at=at,
        snapshot=snapshot,
        fill=fill,
        coverage_evidence=proof,
        fill_load_provenance=proof.load_provenance,
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        sequence=sequence,
    )


def _exit_request(
    key: PositionKey,
    *,
    command_id: str,
    allocation: ExitAllocation,
    view_token: str,
) -> ExecutionRequest:
    return ExecutionRequest(
        request_id=command_id,
        scope=_scope(key),
        strategy_name="orderflow_impulse",
        run_id="run-1",
        decision_ref=command_id,
        expected_view_token=view_token,
        action=TradeCommandType.EXIT,
        requested_quantity=allocation.allocated_quantity,
        side=StrategySide.LONG,
        order_type="MARKET",
        reduce_only=True,
        target_batch_ids=(allocation.batch_id,),
        batch_quantities={allocation.batch_id: allocation.allocated_quantity},
        exit_policy_mode=ExitPolicyMode.TARGET_BATCHES_ONLY,
        created_at=NOW,
    )


async def _durable_row_counts(
    factory: async_sessionmaker, account: str
) -> dict[str, int]:
    """Real row counts for one account across its durable execution tables."""
    async with factory() as session:
        counts = {}
        for table, model in (
            ("execution_book_heads", ExecutionBookHeadRow),
            ("execution_evidence_receipts", ExecutionEvidenceReceiptRow),
            ("position_fact_journal_events", PositionFactJournalEventRow),
            ("position_reservations", PositionReservationRow),
        ):
            counts[table] = int(
                await session.scalar(
                    select(func.count())
                    .select_from(model)
                    .where(model.account_label == account)
                )
                or 0
            )
        counts["execution_commands"] = int(
            await session.scalar(
                select(func.count())
                .select_from(ExecutionCommandRow)
                .where(
                    ExecutionCommandRow.details["scope"]["account_label"].astext
                    == account
                )
            )
            or 0
        )
        return counts


async def test_restart_recovers_command_fill_reservation_batch_and_state(
    async_database_url: str,
) -> None:
    """Necessary execution evidence survives a new process-level Book restore."""
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"persist-recovery-{uuid4().hex}"
    key = PositionKey("live", account, "BTCUSDT")
    scope = _scope(key)
    command_id = f"exit-{uuid4().hex[:20]}"
    opened_at = NOW - timedelta(minutes=10)
    try:
        # Independent business example: 3 contracts filled at 100, then a
        # reduce-only exit accepted for 2 of them.
        book, _reservations = _book(factory)
        await book.restore(account_label=account)
        applied = await book.observe(
            _fill_evidence(
                key,
                trade_id=f"trade-{command_id}",
                order_id=f"order-{command_id}",
                quantity=Decimal("3"),
                at=opened_at,
            )
        )
        assert isinstance(applied, Applied)
        before = await book.read(scope)
        assert before.total_quantity == Decimal("3")
        batch = before.batches[0]
        assert batch.quantity == Decimal("3")
        assert batch.entry_price == Decimal("100")

        allocation = ExitAllocation(
            batch_id=batch.batch_id,
            allocated_quantity=Decimal("2"),
            entry_price=batch.entry_price,
        )
        accepted = await book.act(
            _exit_request(
                key,
                command_id=command_id,
                allocation=allocation,
                view_token=before.projection_version,
            )
        )
        assert isinstance(accepted, Accepted), accepted

        # Process restart: a brand new Book rebuilds from PostgreSQL only.
        restarted, reservations = _book(factory)
        await restarted.restore(account_label=account)
        after = await restarted.read(scope)

        # Active batch and cost basis.
        assert after.total_quantity == Decimal("3")
        assert after.projection_version == before.projection_version
        assert len(after.batches) == 1
        assert after.batches[0].batch_id == batch.batch_id
        assert after.batches[0].quantity == Decimal("3")
        assert after.batches[0].entry_price == Decimal("100")

        # The accepted command is recoverable with its reduce-only quantity.
        outbox = restarted.get_outbox(command_id)
        assert outbox is not None, "the accepted command did not survive restart"
        assert outbox.state is DispatchState.PREPARED
        assert outbox.command.reduce_only is True
        assert outbox.command.requested_quantity == Decimal("2")

        # The reservation is recovered and still guards the real position.
        reservation_ids = {
            reservation.reservation_id for reservation in accepted.receipt.reservations
        }
        assert len(reservation_ids) == 1
        active = restarted.get_active_reservations(key)
        assert len(active) == 1
        assert active[0].reservation_id in reservation_ids
        assert active[0].command_id == command_id
        assert active[0].active_quantity == Decimal("2")
        durable = await reservations.load_active_reservations(key)
        assert {item.reservation_id for item in durable} == reservation_ids
        assert sum((item.active_quantity for item in durable), Decimal("0")) == Decimal(
            "2"
        )

        # The real fill identity is recovered: re-observing it changes nothing.
        replay = await restarted.observe(
            _fill_evidence(
                key,
                trade_id=f"trade-{command_id}",
                order_id=f"order-{command_id}",
                quantity=Decimal("3"),
                at=opened_at,
            )
        )
        assert isinstance(replay, Duplicate)
        assert (await restarted.read(scope)).total_quantity == Decimal("3")
    finally:
        await engine.dispose()


async def _seed_known_datum(
    factory: async_sessionmaker,
    *,
    account: str,
) -> tuple[ExecutionBook, PositionKey, ExecutionEvidence, ExecutionEvidence]:
    """Seed one real position datum (one fill, one later snapshot).

    The observable business example is a flat anchor followed by a real 3-lot
    fill at 100; both envelopes carry complete load coverage.  The second
    envelope is the datum that the bursts below re-observe.
    """
    key = PositionKey("live", account, "BTCUSDT")
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-events", stream_epoch="epoch-1"
    )
    book, _reservations = _book(factory)
    await book.restore(account_label=account)
    anchor_at = NOW - timedelta(minutes=20)
    anchor_id = stable_snapshot_anchor_id(_snapshot(key, anchor_at, "0"))
    flat = _scoped_evidence(
        key,
        evidence_id="flat-anchor",
        scope=scope,
        at=anchor_at,
        sequence=1,
        snapshot=_snapshot(key, anchor_at, "0"),
        proof=_coverage(
            scope,
            load_id="scan-1",
            scan_origin=anchor_at,
            anchor_id=anchor_id,
            anchor_cut=anchor_at,
            anchor_kind="zero_snapshot",
            checked_through=anchor_at,
        ),
    )
    assert isinstance(await book.observe(flat), Applied)
    cut = anchor_at + timedelta(minutes=5)
    opened = _scoped_evidence(
        key,
        evidence_id="position-cut",
        scope=scope,
        at=cut,
        sequence=2,
        snapshot=_snapshot(key, cut, "3"),
        fill=_fill(
            key,
            trade_id="trade-open",
            order_id="order-open",
            quantity=Decimal("3"),
            at=cut,
        ),
        proof=_coverage(
            scope,
            load_id="scan-2",
            scan_origin=anchor_at,
            anchor_id=anchor_id,
            anchor_cut=anchor_at,
            anchor_kind="zero_snapshot",
            checked_through=cut,
        ),
    )
    assert isinstance(await book.observe(opened), Applied)
    return book, key, flat, opened


async def _assert_position_is_recoverable(
    factory: async_sessionmaker, *, account: str, key: PositionKey
) -> None:
    """A fresh process-level restore still yields the real business position."""
    restarted, _reservations = _book(factory)
    await restarted.restore(account_label=account)
    view = await restarted.read(_scope(key))
    assert view.total_quantity == Decimal("3")
    assert len(view.batches) == 1
    assert view.batches[0].quantity == Decimal("3")
    assert view.batches[0].entry_price == Decimal("100")


async def test_duplicate_evidence_identity_is_an_idempotent_no_op(
    async_database_url: str,
) -> None:
    """Re-observing the same evidence identity grows no durable row at all."""
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"persist-duplicate-{uuid4().hex}"
    try:
        book, key, flat, opened = await _seed_known_datum(factory, account=account)
        before = await _durable_row_counts(factory, account)
        assert before["execution_book_heads"] == 1
        assert before["execution_evidence_receipts"] == 2
        assert before["position_fact_journal_events"] > 0

        for _ in range(BURST):
            assert isinstance(await book.observe(opened), Duplicate)
            assert isinstance(await book.observe(flat), Duplicate)

        after = await _durable_row_counts(factory, account)
        assert after == before, (
            "re-observing two already-accepted evidence identities "
            f"{2 * BURST} times changed durable rows: {before} -> {after}"
        )

        # And the required evidence is still recoverable after the burst.
        await _assert_position_is_recoverable(factory, account=account, key=key)
    finally:
        await engine.dispose()


async def test_redelivered_known_datum_appends_no_durable_state_row(
    async_database_url: str,
) -> None:
    """A redelivered snapshot/coverage/fill datum carries no new business fact.

    The envelope identity is fresh, but the datum (snapshot at the same
    observed_at, same coverage proof, same real trade) was already accepted, so
    SYSTEM.md 5 requires an idempotent no-op.  Only the dedup receipt index -
    one row per distinct envelope identity, retained as the identity basis -
    may grow.
    """
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"persist-redelivery-{uuid4().hex}"
    try:
        book, key, _flat, opened = await _seed_known_datum(factory, account=account)
        before = await _durable_row_counts(factory, account)

        for index in range(BURST):
            await book.observe(
                replace(
                    opened,
                    evidence_id=f"redelivery-{index}",
                    sequence=100 + index,
                )
            )

        after = await _durable_row_counts(factory, account)

        # Position, command and reservation authority are unchanged.
        assert after["execution_book_heads"] == before["execution_book_heads"]
        assert after["position_reservations"] == before["position_reservations"]
        assert after["execution_commands"] == before["execution_commands"]

        # The evidence accepted before the burst is still recoverable after it.
        await _assert_position_is_recoverable(factory, account=account, key=key)

        # No durable position-state row may be appended for a known datum.
        appended = (
            after["position_fact_journal_events"]
            - before["position_fact_journal_events"]
        )
        assert (
            after["position_fact_journal_events"]
            == before["position_fact_journal_events"]
        ), (
            f"re-observing the same snapshot/coverage/fill datum {BURST} times "
            f"appended {appended} durable journal state rows; the datum carries "
            "no new business fact and SYSTEM.md 4 requires the writer to stop "
            "duplicating internal journal/checkpoint/policy snapshots"
        )
    finally:
        await engine.dispose()


async def test_redelivered_known_datum_does_not_advance_head_revision(
    async_database_url: str,
) -> None:
    """A redelivered known datum returns Duplicate and does not advance head
    revision.
    """
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"persist-redelivery-head-{uuid4().hex}"
    key = PositionKey("live", account, "BTCUSDT")
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-events", stream_epoch="epoch-1"
    )
    try:
        book, key, _flat, opened = await _seed_known_datum(factory, account=account)
        async with factory() as session:
            head_row = await session.get(
                ExecutionBookHeadRow,
                (
                    key.environment,
                    key.account_label,
                    key.symbol,
                    key.position_side.value,
                ),
            )
            assert head_row is not None
            assert head_row.revision == 2

        # Re-observe known datum with fresh evidence_id and higher sequence
        result = await book.observe(
            replace(
                opened,
                evidence_id="fresh-redelivery",
                sequence=100,
            )
        )
        assert isinstance(result, Duplicate)

        # Head revision in DB must still be 2
        async with factory() as session:
            head_row = await session.get(
                ExecutionBookHeadRow,
                (
                    key.environment,
                    key.account_label,
                    key.symbol,
                    key.position_side.value,
                ),
            )
            assert head_row is not None
            assert head_row.revision == 2

        # After restart, the restored book knows about sequence 100 and rejects <= 100
        restarted, _ = _book(factory)
        await restarted.restore(account_label=account)
        # Sequence <= 100 must conflict / be rejected
        conflict_result = await restarted.observe(
            replace(
                opened,
                evidence_id="stale-seq",
                sequence=50,
            )
        )
        assert isinstance(conflict_result, EvidenceConflict)

        # Same evidence identity with different payload must conflict
        payload_conflict = await restarted.observe(
            replace(
                opened,
                evidence_id="fresh-redelivery",
                sequence=100,
                snapshot=replace(opened.snapshot, position_amt=Decimal("99")),
            )
        )
        assert isinstance(payload_conflict, EvidenceConflict)

        # New valid fill/coverage advances head revision
        cut_new = opened.observed_at + timedelta(minutes=5)
        new_evidence = _scoped_evidence(
            key,
            evidence_id="new-fill-datum",
            scope=scope,
            at=cut_new,
            sequence=101,
            fill=_fill(
                key,
                trade_id="trade-open-2",
                order_id="order-open-2",
                quantity=Decimal("1"),
                at=cut_new,
            ),
            proof=_coverage(
                scope,
                load_id="scan-3",
                scan_origin=opened.observed_at,
                anchor_id="anchor-2",
                anchor_cut=opened.observed_at,
                anchor_kind="zero_snapshot",
                checked_through=cut_new,
            ),
        )
        applied_result = await restarted.observe(new_evidence)
        assert isinstance(applied_result, Applied)
        async with factory() as session:
            head_row = await session.get(
                ExecutionBookHeadRow,
                (
                    key.environment,
                    key.account_label,
                    key.symbol,
                    key.position_side.value,
                ),
            )
            assert head_row is not None
            assert head_row.revision == 3
    finally:
        await engine.dispose()


async def test_checkpoint_bounded_suffix_and_known_prefix_deduplication(
    async_database_url: str,
) -> None:
    """A restored checkpoint folds historical fills; known prefix trade deduplicates."""
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"persist-bounded-suffix-{uuid4().hex}"
    try:
        _seeded_book, key, _flat, opened = await _seed_known_datum(
            factory, account=account
        )

        restarted, _reservations = _book(factory)
        await restarted.restore(account_label=account)

        journal = restarted._journals.get(key.canonical_id)
        assert journal is not None
        assert journal.read_cut().recovery_checkpoint is not None, (
            "seed failed to create a verified checkpoint folding the opened trade"
        )
        assert opened.fill is not None
        assert not any(
            fill.trade_id == opened.fill.trade_id for fill in journal.read_cut().fills
        )

        scope = _scope(key)
        view_before = await restarted.read(scope)
        assert view_before.total_quantity == Decimal("3")
        assert len(view_before.batches) == 1
        assert view_before.batches[0].quantity == Decimal("3")
        assert view_before.batches[0].entry_price == Decimal("100")

        async with factory() as session:
            head_before = await session.scalar(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.account_label == account,
                    ExecutionBookHeadRow.symbol == key.symbol,
                    ExecutionBookHeadRow.position_side == key.position_side.value,
                )
            )
        assert head_before is not None
        initial_revision = head_before.revision

        prefix_bare = replace(
            opened,
            evidence_id="prefix-bare",
            sequence=100,
            snapshot=None,
            coverage_evidence=None,
            fill_load_provenance=None,
        )
        result_bare = await restarted.observe(prefix_bare)
        assert isinstance(result_bare, Duplicate)

        view_after_bare = await restarted.read(scope)
        assert view_after_bare.total_quantity == Decimal("3")
        assert view_after_bare.batches == view_before.batches
        assert view_after_bare.projection_version == view_before.projection_version

        async with factory() as session:
            head_bare = await session.scalar(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.account_label == account,
                    ExecutionBookHeadRow.symbol == key.symbol,
                    ExecutionBookHeadRow.position_side == key.position_side.value,
                )
            )
        assert head_bare is not None
        assert head_bare.revision == initial_revision

        retry_same = await restarted.observe(prefix_bare)
        assert isinstance(retry_same, Duplicate)

        conflict_evidence = replace(
            prefix_bare,
            evidence_id="prefix-bare-conflict",
            sequence=101,
            fill=replace(opened.fill, price=Decimal("101")),
        )
        result_conflict = await restarted.observe(conflict_evidence)
        assert isinstance(result_conflict, EvidenceConflict)

        async with factory() as session:
            head_conflict = await session.scalar(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.account_label == account,
                    ExecutionBookHeadRow.symbol == key.symbol,
                    ExecutionBookHeadRow.position_side == key.position_side.value,
                )
            )
        assert head_conflict is not None
        assert head_conflict.revision == initial_revision

        fresh_trade_at = opened.observed_at + timedelta(minutes=5)
        new_fill_evidence = ExecutionEvidence(
            evidence_id="fresh-fill-after-checkpoint",
            scope=scope,
            observed_at=fresh_trade_at,
            fill=AccountFillEvent(
                environment=key.environment,
                account_label=key.account_label,
                symbol=key.symbol,
                trade_id=f"trade-fresh-{uuid4().hex[:8]}",
                order_id=f"order-fresh-{uuid4().hex[:8]}",
                side="BUY",
                price=Decimal("100"),
                quantity=Decimal("1"),
                realized_pnl=Decimal("0"),
                fee=Decimal("0"),
                fee_asset="USDT",
                trade_at=fresh_trade_at,
                raw_payload={"positionSide": key.position_side.value},
            ),
            stream_id=opened.stream_id,
            stream_epoch=opened.stream_epoch,
            sequence=101,
        )
        result_fresh = await restarted.observe(new_fill_evidence)
        assert isinstance(result_fresh, Applied)

        view_fresh = await restarted.read(scope)
        assert view_fresh.total_quantity == Decimal("4")
    finally:
        await engine.dispose()


async def test_full_replay_after_restart_deduplicates_without_advancing_head(
    async_database_url: str,
) -> None:
    """Fresh full envelope replay after restart must Duplicate and keep
    revision stable.
    """
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"persist-full-replay-{uuid4().hex}"
    try:
        _seeded_book, key, _flat, opened = await _seed_known_datum(
            factory, account=account
        )

        restarted, _reservations = _book(factory)
        await restarted.restore(account_label=account)

        async with factory() as session:
            head_before = await session.scalar(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.account_label == account,
                    ExecutionBookHeadRow.symbol == key.symbol,
                    ExecutionBookHeadRow.position_side == key.position_side.value,
                )
            )
        assert head_before is not None
        initial_revision = head_before.revision

        scope = _scope(key)
        view_before = await restarted.read(scope)

        full_replay = replace(
            opened,
            evidence_id="full-replay",
            sequence=100,
        )
        result = await restarted.observe(full_replay)
        assert isinstance(result, Duplicate)

        async with factory() as session:
            head_after = await session.scalar(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.account_label == account,
                    ExecutionBookHeadRow.symbol == key.symbol,
                    ExecutionBookHeadRow.position_side == key.position_side.value,
                )
            )
        assert head_after is not None
        assert head_after.revision == initial_revision

        view_after = await restarted.read(scope)
        assert view_after.total_quantity == view_before.total_quantity
        assert view_after.batches == view_before.batches
        assert view_after.projection_version == view_before.projection_version

        conflict_replay = replace(
            opened,
            evidence_id="prefix-snapshot-conflict",
            sequence=101,
            snapshot=replace(opened.snapshot, position_amt=Decimal("99")),
        )
        conflict_result = await restarted.observe(conflict_replay)
        assert isinstance(conflict_result, EvidenceConflict)

        async with factory() as session:
            head_after_conflict = await session.scalar(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.account_label == account,
                    ExecutionBookHeadRow.symbol == key.symbol,
                    ExecutionBookHeadRow.position_side == key.position_side.value,
                )
            )
        assert head_after_conflict is not None
        assert head_after_conflict.revision == initial_revision

        view_after_conflict = await restarted.read(scope)
        assert view_after_conflict.total_quantity == view_before.total_quantity
        assert view_after_conflict.batches == view_before.batches
        assert view_after_conflict.projection_version == view_before.projection_version
    finally:
        await engine.dispose()


async def test_checkpoint_known_prefix_with_fresh_coverage_advances_head(
    async_database_url: str,
) -> None:
    """Restored checkpoint given known prefix fill & snapshot plus fresh coverage
    scan advances head revision and checked_through while keeping business state.
    """
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"persist-fresh-cov-{uuid4().hex}"
    try:
        _seeded_book, key, _flat, opened = await _seed_known_datum(
            factory, account=account
        )
        restarted, _reservations = _book(factory)
        await restarted.restore(account_label=account)

        scope = _scope(key)
        view_before = await restarted.read(scope)
        assert view_before.total_quantity == Decimal("3")

        journal = restarted._journals.get(key.canonical_id)
        assert journal is not None
        cut = journal.read_cut()
        assert cut.recovery_checkpoint is not None
        ckpt = cut.recovery_checkpoint

        async with factory() as session:
            head_before = await session.scalar(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.account_label == account,
                    ExecutionBookHeadRow.symbol == key.symbol,
                    ExecutionBookHeadRow.position_side == key.position_side.value,
                )
            )
        assert head_before is not None
        initial_rev = head_before.revision

        stream_scope = ckpt.stream_scope
        adv_checked_through = ckpt.event_cut + timedelta(minutes=10)
        fresh_coverage_evidence = _scoped_evidence(
            key,
            evidence_id="fresh-coverage-after-checkpoint",
            scope=stream_scope,
            at=adv_checked_through,
            sequence=100,
            fill=opened.fill,
            snapshot=opened.snapshot,
            proof=_coverage(
                stream_scope,
                load_id="scan-adv-1",
                scan_origin=ckpt.event_cut,
                anchor_id=ckpt.checkpoint_id,
                anchor_cut=ckpt.event_cut,
                anchor_kind="recovery_checkpoint",
                checked_through=adv_checked_through,
            ),
        )

        observe_result = await restarted.observe(fresh_coverage_evidence)
        assert isinstance(observe_result, Applied)

        async with factory() as session:
            head_after = await session.scalar(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.account_label == account,
                    ExecutionBookHeadRow.symbol == key.symbol,
                    ExecutionBookHeadRow.position_side == key.position_side.value,
                )
            )
        assert head_after is not None
        assert head_after.revision == initial_rev + 1

        view_after = await restarted.read(scope)
        assert view_after.total_quantity == Decimal("3")
        assert view_after.batches == view_before.batches

        restarted_again, _ = _book(factory)
        await restarted_again.restore(account_label=account)
        journal_again = restarted_again._journals.get(key.canonical_id)
        assert journal_again is not None
        cut_again = journal_again.read_cut()
        assert cut_again.coverage is not None
        assert cut_again.coverage.end_at == adv_checked_through
        assert cut_again.fill_load_provenance is not None
        assert cut_again.fill_load_provenance.checked_through == adv_checked_through
        assert restarted_again._last_sequences[key.canonical_id] == 100
        view_again = await restarted_again.read(scope)
        assert view_again.total_quantity == Decimal("3")
        assert view_again.batches == view_before.batches
    finally:
        await engine.dispose()


async def test_headless_uncheckpointed_recovery_from_raw_journal(
    async_database_url: str,
) -> None:
    """Old format / uncheckpointed stream with 2 real fills and no HeadRow/checkpoint
    restores correct quantity, weighted cost basis and batches; duplicate replay
    does not double-count, and altered payload triggers EvidenceConflict.
    """
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"persist-headless-{uuid4().hex}"
    key = PositionKey("live", account, "BTCUSDT")
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="headless-events", stream_epoch="epoch-0"
    )
    store = PostgresAccountJournalStore()
    t1 = NOW - timedelta(minutes=15)
    t2 = NOW - timedelta(minutes=10)

    f1 = AccountFillEvent(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        trade_id="headless-trade-1",
        order_id="headless-order-1",
        side="BUY",
        price=Decimal("100"),
        quantity=Decimal("2"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=t1,
        raw_payload={"positionSide": key.position_side.value},
    )
    f2 = AccountFillEvent(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        trade_id="headless-trade-2",
        order_id="headless-order-2",
        side="BUY",
        price=Decimal("130"),
        quantity=Decimal("1"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=t2,
        raw_payload={"positionSide": key.position_side.value},
    )
    facts = AccountFacts(
        position_key=key,
        stream_scope=scope,
        fills=(f1, f2),
    )

    try:
        async with factory() as session, session.begin():
            await store.persist_facts_in_session(
                session, scope=scope, facts=facts, revision=2
            )

        # Confirm no ExecutionBookHeadRow and no PositionRecoveryCheckpoint exist
        async with factory() as session:
            head_row = await session.scalar(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.account_label == account,
                )
            )
            assert head_row is None

        book, _reservations = _book(factory)
        from crypto_momentum_lab.domain.execution.ports import DecisionCommitConflict

        # Headless stream with existing responsibilities must explicitly block recovery
        with pytest.raises(DecisionCommitConflict, match="recovery blocked"):
            await book.restore(account_label=account)
    finally:
        await engine.dispose()
