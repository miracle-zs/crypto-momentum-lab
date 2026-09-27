from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution import (
    Accepted,
    AlreadyAccepted,
    Applied,
    Blocked,
    CommandConflict,
    DispatchState,
    Duplicate,
    ExchangeOrderEvent,
    ExchangeOrderState,
    ExecutionBook,
    ExecutionEvidence,
    ExecutionRequest,
    ExecutionScope,
    FactCoverageInterval,
    FactCoverageStatus,
    FuturesPositionSide,
    PositionHealthStatus,
    StaleView,
    TradeCommandType,
)


def _dt(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 9, 25, hour, minute, second, tzinfo=UTC)


def _scope() -> ExecutionScope:
    return ExecutionScope(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.LONG,
    )


@pytest.mark.asyncio
async def test_execution_book_read_deterministic_view() -> None:
    book = ExecutionBook()
    scope = _scope()

    view1 = await book.read(scope)
    view2 = await book.read(scope)

    # Calling read multiple times without state changes returns exact same token
    assert view1.projection_version == view2.projection_version
    assert view1.health_status == PositionHealthStatus.READY
    assert view1.is_ready_for_trade is False  # unconfirmed coverage


@pytest.mark.asyncio
async def test_execution_book_act_stale_view_rejected() -> None:
    book = ExecutionBook()
    scope = _scope()

    req = ExecutionRequest(
        request_id="req-1",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-1",
        expected_view_token="pv_BTCUSDT_wrong_token",
        action=TradeCommandType.ENTRY,
        requested_quantity=Decimal("1.0"),
    )

    result = await book.act(req)
    assert isinstance(result, StaleView)
    assert result.expected_token == "pv_BTCUSDT_wrong_token"


@pytest.mark.asyncio
async def test_execution_book_act_blocked_when_not_ready() -> None:
    book = ExecutionBook()
    scope = _scope()
    view = await book.read(scope)

    req = ExecutionRequest(
        request_id="req-1",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.ENTRY,
        requested_quantity=Decimal("1.0"),
    )

    # Not ready because coverage is unconfirmed
    result = await book.act(req)
    assert isinstance(result, Blocked)
    assert "not ready for trade" in result.reason


@pytest.mark.asyncio
async def test_execution_book_act_and_idempotency_workflow() -> None:
    book = ExecutionBook()
    scope = _scope()
    t0 = _dt(10, 0)

    # Ingest confirmed coverage, fill, and snapshot via observe
    f1 = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t1",
        order_id="ord_1",
        side="BUY",
        price=Decimal("50000"),
        quantity=Decimal("1.0"),
        realized_pnl=Decimal("0"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG", "is_system": True},
    )
    snap = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("1.0"),
        entry_price=Decimal("50000"),
        mark_price=Decimal("50000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("50000"),
        leverage=5,
        margin_type="cross",
        observed_at=t0,
        raw_payload={},
    )

    key = scope.to_position_key()
    journal = book._ensure_journal(key)
    journal.set_coverage(
        FactCoverageInterval(
            start_at=t0,
            end_at=t0,
            status=FactCoverageStatus.CONFIRMED,
        )
    )

    ev_fill = ExecutionEvidence(
        evidence_id="ev_f1",
        scope=scope,
        observed_at=t0,
        fill=f1,
        snapshot=snap,
    )
    applied = await book.observe(ev_fill)
    assert isinstance(applied, Applied)

    # Now read authoritative view
    view = await book.read(scope)
    assert view.health_status == PositionHealthStatus.READY
    assert view.is_ready_for_trade is True

    # Submit exit request
    req = ExecutionRequest(
        request_id="req-exit-1",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-exit-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("1.0"),
    )

    act_result = await book.act(req)
    assert isinstance(act_result, Accepted)
    assert act_result.receipt.request_id == "req-exit-1"
    assert len(act_result.receipt.reservations) == 1
    assert act_result.receipt.reservations[0].reserved_quantity == Decimal("1.0")

    # Idempotent re-submission returns AlreadyAccepted with same receipt
    act_repeat = await book.act(req)
    assert isinstance(act_repeat, AlreadyAccepted)
    assert act_repeat.receipt.request_id == "req-exit-1"

    # Conflicting submission with different requested quantity returns CommandConflict
    req_conflict = ExecutionRequest(
        request_id="req-exit-1",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-exit-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("0.5"),
    )
    conflict_result = await book.act(req_conflict)
    assert isinstance(conflict_result, CommandConflict)


@pytest.mark.asyncio
async def test_execution_book_observe_idempotency_and_settlement() -> None:
    book = ExecutionBook()
    scope = _scope()
    t0 = _dt(10, 0)

    f1 = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t1",
        order_id="ord_1",
        side="BUY",
        price=Decimal("50000"),
        quantity=Decimal("1.0"),
        realized_pnl=Decimal("0"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG", "is_system": True},
    )
    ev = ExecutionEvidence(
        evidence_id="ev_1",
        scope=scope,
        observed_at=t0,
        fill=f1,
    )

    res1 = await book.observe(ev)
    assert isinstance(res1, Applied)

    # Re-observing same evidence_id returns Duplicate
    res2 = await book.observe(ev)
    assert isinstance(res2, Duplicate)
    assert res2.evidence_id == "ev_1"


@pytest.mark.asyncio
async def test_execution_book_fails_closed_when_acceptance_persistence_fails() -> None:
    class FailingCommandRepository:
        async def upsert_execution_command(self, **kwargs: object) -> None:
            raise RuntimeError("database unavailable")

    book = ExecutionBook(command_repository=FailingCommandRepository())
    scope = _scope()
    t0 = _dt(10, 0)
    flat = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("0"),
        entry_price=Decimal("0"),
        mark_price=Decimal("50000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("0"),
        leverage=5,
        margin_type="cross",
        observed_at=t0,
        raw_payload={},
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="flat-for-fail-closed",
            scope=scope,
            observed_at=t0,
            snapshot=flat,
        )
    )
    view = await book.read(scope)
    req = ExecutionRequest(
        request_id="req-persist-failure",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.ENTRY,
        requested_quantity=Decimal("1.0"),
    )

    result = await book.act(req)

    assert isinstance(result, Blocked)
    assert "database unavailable" in " ".join(result.diagnostics)
    assert book.get_outbox(req.request_id) is None
    assert book.list_outbox() == ()


@pytest.mark.asyncio
async def test_execution_book_persists_reservation_link_with_first_outbox_write() -> (
    None
):
    class RecordingCommandRepository:
        def __init__(self) -> None:
            self.writes: list[dict[str, object]] = []

        async def upsert_execution_command(self, **kwargs: object) -> None:
            self.writes.append(kwargs)

    command_repo = RecordingCommandRepository()
    book = ExecutionBook(command_repository=command_repo)
    scope = _scope()
    t0 = _dt(10, 0)
    fill = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="outbox-entry-fill",
        order_id="entry-order",
        side="BUY",
        price=Decimal("50000"),
        quantity=Decimal("2.0"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG"},
    )
    snapshot = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("2.0"),
        entry_price=Decimal("50000"),
        mark_price=Decimal("50000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("100000"),
        leverage=5,
        margin_type="cross",
        observed_at=t0,
        raw_payload={},
    )
    journal = book._ensure_journal(scope.to_position_key())
    journal.set_coverage(
        FactCoverageInterval(
            start_at=t0, end_at=t0, status=FactCoverageStatus.CONFIRMED
        )
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="seed-outbox-position",
            scope=scope,
            observed_at=t0,
            fill=fill,
            snapshot=snapshot,
        )
    )
    view = await book.read(scope)
    req = ExecutionRequest(
        request_id="req-with-reservation-link",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-exit",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("1.0"),
    )

    result = await book.act(req)

    assert isinstance(result, Accepted)
    assert len(command_repo.writes) == 1
    details = command_repo.writes[0]["details"]
    assert isinstance(details, dict)
    assert details["reservations"] == [
        reservation.reservation_id for reservation in result.receipt.reservations
    ]


@pytest.mark.asyncio
async def test_execution_book_restore_rejects_incomplete_active_command() -> None:
    class LegacyCommandRepository:
        async def load_active_execution_commands(self, **kwargs: object):
            return (
                {
                    "command_id": "legacy-command",
                    "status": "prepared",
                    "details": {"scope": {"account_label": "primary"}},
                },
            )

        async def load_seen_event_ids(self) -> tuple[str, ...]:
            return ()

        async def load_seen_fill_trade_ids(self) -> tuple[str, ...]:
            return ()

        async def load_execution_order_watermarks(self, **kwargs: object):
            return ()

    book = ExecutionBook(command_repository=LegacyCommandRepository())

    with pytest.raises(RuntimeError, match="restore active execution commands"):
        await book.restore(account_label="primary")

    assert book._persistence_failed is True


@pytest.mark.asyncio
async def test_linked_settlement_ignores_late_duplicate() -> None:
    from crypto_momentum_lab.domain.execution.trade_command import (
        PositionReservation,
        TradeCommand,
    )
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

    book = ExecutionBook()
    scope = _scope()
    key = scope.to_position_key()

    def register(command_id: str, reservation_id: str, quantity: str) -> None:
        command = TradeCommand(
            command_id=command_id,
            position_key=key,
            command_type=TradeCommandType.EXIT,
            side=StrategySide.LONG,
            order_type=EntryType.MARKET,
            requested_quantity=Decimal(quantity),
            reduce_only=True,
        )
        reservation = PositionReservation(
            reservation_id=reservation_id,
            command_id=command_id,
            position_key=key,
            batch_id=f"batch-{command_id}",
            reserved_quantity=Decimal(quantity),
            created_at=_dt(10, 0),
        )
        book.coordinator.register_reservation(reservation)
        book.register_prepared_command(command, scope, [reservation_id])

    register("order-a", "reservation-a", "4")
    register("order-b", "reservation-b", "5")

    def evidence(
        evidence_id: str,
        order_id: str,
        quantity: str,
        state: ExchangeOrderState,
    ) -> ExecutionEvidence:
        cumulative = Decimal(quantity)
        return ExecutionEvidence(
            evidence_id=evidence_id,
            scope=scope,
            observed_at=_dt(10, 1),
            fill=AccountFillEvent(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                trade_id=f"{order_id}-cum-{quantity}",
                order_id=order_id,
                side="SELL",
                price=Decimal("100"),
                quantity=cumulative,
                realized_pnl=Decimal("0"),
                fee=Decimal("0"),
                fee_asset="USDT",
                trade_at=_dt(10, 1),
                raw_payload={
                    "is_cumulative": True,
                    "cum_qty": quantity,
                    "cum_quote": str(cumulative * Decimal("100")),
                    "reduce_only": True,
                },
            ),
            order_event=ExchangeOrderEvent(
                event_id=evidence_id,
                client_order_id=order_id,
                state=state,
                occurred_at=_dt(10, 1),
                exchange_order_id=f"exchange-{order_id}",
                details={"is_reduce_only": True},
            ),
        )

    await book.observe(
        evidence(
            "fill-a-2",
            "order-a",
            "2",
            ExchangeOrderState.PARTIALLY_FILLED,
        )
    )
    await book.observe(
        evidence(
            "fill-b-3",
            "order-b",
            "3",
            ExchangeOrderState.PARTIALLY_FILLED,
        )
    )
    reservations = {r.reservation_id: r for r in book.get_active_reservations(key)}
    assert reservations["reservation-a"].consumed_quantity == Decimal("2")
    assert reservations["reservation-b"].consumed_quantity == Decimal("3")

    await book.observe(
        ExecutionEvidence(
            evidence_id="cancel-b",
            scope=scope,
            observed_at=_dt(10, 2),
            order_event=ExchangeOrderEvent(
                event_id="cancel-b",
                client_order_id="order-b",
                state=ExchangeOrderState.CANCELED,
                occurred_at=_dt(10, 2),
                exchange_order_id="exchange-order-b",
                details={},
            ),
        )
    )
    duplicate_late_report = await book.observe(
        evidence(
            "late-b-same-watermark",
            "order-b",
            "3",
            ExchangeOrderState.PARTIALLY_FILLED,
        )
    )
    assert isinstance(duplicate_late_report, Applied)
    assert book.get_outbox("order-b").state is DispatchState.TERMINAL
    active = {r.reservation_id: r for r in book.get_active_reservations(key)}
    assert active["reservation-a"].active_quantity == Decimal("2")
    assert "reservation-b" not in active

    late_additional_fill = await book.observe(
        evidence(
            "late-b-new-fill",
            "order-b",
            "4",
            ExchangeOrderState.PARTIALLY_FILLED,
        )
    )
    assert isinstance(late_additional_fill, Applied)
    assert late_additional_fill.recovery_required is True
    assert book.get_active_reservations(key)[0].reservation_id == "reservation-a"


@pytest.mark.asyncio
async def test_duplicate_trade_id_does_not_consume_twice() -> None:
    from crypto_momentum_lab.domain.execution.trade_command import (
        PositionReservation,
        TradeCommand,
    )
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

    book = ExecutionBook()
    scope = _scope()
    key = scope.to_position_key()
    reservation = PositionReservation(
        reservation_id="reservation-duplicate-trade",
        command_id="order-duplicate-trade",
        position_key=key,
        batch_id="batch-duplicate-trade",
        reserved_quantity=Decimal("5"),
    )
    book.coordinator.register_reservation(reservation)
    book.register_prepared_command(
        TradeCommand(
            command_id="order-duplicate-trade",
            position_key=key,
            command_type=TradeCommandType.EXIT,
            side=StrategySide.LONG,
            order_type=EntryType.MARKET,
            requested_quantity=Decimal("5"),
            reduce_only=True,
        ),
        scope,
        [reservation.reservation_id],
    )

    def event(evidence_id: str) -> ExecutionEvidence:
        return ExecutionEvidence(
            evidence_id=evidence_id,
            scope=scope,
            observed_at=_dt(10, 0),
            fill=AccountFillEvent(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                trade_id="exchange-trade-1",
                order_id="order-duplicate-trade",
                side="SELL",
                price=Decimal("100"),
                quantity=Decimal("2"),
                realized_pnl=Decimal("0"),
                fee=Decimal("0"),
                fee_asset="USDT",
                trade_at=_dt(10, 0),
                raw_payload={"reduce_only": True},
            ),
        )

    first = await book.observe(event("trade-event-1"))
    duplicate = await book.observe(event("trade-event-2"))

    assert isinstance(first, Applied)
    assert first.consumed_quantity == Decimal("2")
    assert isinstance(duplicate, Applied)
    assert duplicate.consumed_quantity == Decimal("0")
    assert book.get_active_reservations(key)[0].consumed_quantity == Decimal("2")


@pytest.mark.asyncio
async def test_restore_cumulative_quantity_and_quote_watermarks() -> None:
    from copy import deepcopy

    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        InMemoryPositionReservationRepository,
    )
    from crypto_momentum_lab.domain.execution.trade_command import (
        PositionReservation,
        TradeCommand,
    )
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

    class PersistedCommandRepository:
        def __init__(self) -> None:
            self.rows: dict[str, dict[str, object]] = {}

        async def upsert_execution_command(self, **values: object) -> None:
            command_id = str(values["command_id"])
            self.rows[command_id] = dict(values)

        async def load_active_execution_commands(
            self, account_label: str | None = None
        ) -> tuple[dict[str, object], ...]:
            active = []
            for row in self.rows.values():
                details = row["details"]
                assert isinstance(details, dict)
                scope_data = details["scope"]
                assert isinstance(scope_data, dict)
                if row["status"] in ("terminal", "rejected"):
                    continue
                if (
                    account_label is not None
                    and scope_data["account_label"] != account_label
                ):
                    continue
                active.append(row)
            return tuple(active)

        async def load_execution_order_watermarks(
            self, account_label: str | None = None
        ) -> tuple[dict[str, object], ...]:
            watermarks = []
            for row in self.rows.values():
                details = row["details"]
                assert isinstance(details, dict)
                scope_data = details["scope"]
                assert isinstance(scope_data, dict)
                if (
                    account_label is not None
                    and scope_data["account_label"] != account_label
                ):
                    continue
                watermarks.append(
                    {
                        "scope": scope_data,
                        "client_order_id": row["client_order_id"],
                        "cumulative_filled_quantity": details[
                            "cumulative_filled_quantity"
                        ],
                        "cumulative_filled_quote": details["cumulative_filled_quote"],
                        "status": row["status"],
                    }
                )
            return tuple(watermarks)

        async def load_seen_event_ids(self) -> tuple[str, ...]:
            return ()

        async def load_seen_fill_trade_ids(self) -> tuple[str, ...]:
            return ()

    scope = _scope()
    key = scope.to_position_key()
    command_repo = PersistedCommandRepository()
    reservation_repo = InMemoryPositionReservationRepository()
    order_command = TradeCommand(
        command_id="order-restart-cumulative",
        position_key=key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("10"),
        reduce_only=True,
        created_at=_dt(10, 0),
    )
    reservation = PositionReservation(
        reservation_id="reservation-restart-cumulative",
        command_id=order_command.command_id,
        position_key=key,
        batch_id="batch-restart",
        reserved_quantity=Decimal("10"),
        created_at=_dt(10, 0),
    )
    reservation_repo.save_reservation(reservation)

    first_book = ExecutionBook(
        command_repository=command_repo,
        reservation_repository=reservation_repo,
    )
    first_book.coordinator.register_reservation(reservation)
    first_entry = first_book.register_prepared_command(
        order_command, scope, [reservation.reservation_id]
    )
    await first_book._persist_outbox_state(first_entry)
    await first_book.mark_dispatching(order_command.command_id)
    await first_book.mark_acknowledged(order_command.command_id, "exchange-order-1")

    terminal_command = TradeCommand(
        command_id="order-restart-terminal",
        position_key=key,
        command_type=TradeCommandType.ENTRY,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("1"),
        reduce_only=False,
        created_at=_dt(9, 0),
    )
    terminal_entry = first_book.register_prepared_command(terminal_command, scope)
    terminal_key = first_book._order_watermark_key(key, terminal_command.command_id)
    first_book._order_cumulative_fills[terminal_key] = Decimal("2")
    first_book._order_cumulative_quotes[terminal_key] = Decimal("190")
    await first_book._persist_outbox_state(terminal_entry)
    await first_book.mark_terminal(terminal_command.command_id)

    def cumulative_evidence(
        evidence_id: str,
        quantity: str,
        quote: str,
        price: str,
    ) -> ExecutionEvidence:
        cumulative_quantity = Decimal(quantity)
        return ExecutionEvidence(
            evidence_id=evidence_id,
            scope=scope,
            observed_at=_dt(10, 1),
            fill=AccountFillEvent(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                trade_id=f"cum-{quantity}",
                order_id=order_command.command_id,
                side="SELL",
                price=Decimal(price),
                quantity=cumulative_quantity,
                realized_pnl=Decimal("0"),
                fee=Decimal("0"),
                fee_asset="USDT",
                trade_at=_dt(10, 1),
                raw_payload={
                    "is_cumulative": True,
                    "cum_qty": quantity,
                    "cum_quote": quote,
                    "reduce_only": True,
                },
            ),
        )

    first_result = await first_book.observe(
        cumulative_evidence("cumulative-3", "3", "300", "100")
    )
    assert isinstance(first_result, Applied)
    assert first_result.consumed_quantity == Decimal("3")

    restored_book = ExecutionBook(
        command_repository=command_repo,
        reservation_repository=reservation_repo,
    )
    await restored_book.restore(account_label="primary")
    assert restored_book._order_cumulative_fills[
        restored_book._order_watermark_key(key, order_command.command_id)
    ] == Decimal("3")
    assert restored_book._order_cumulative_quotes[
        restored_book._order_watermark_key(key, order_command.command_id)
    ] == Decimal("300")
    assert restored_book._order_cumulative_fills[
        restored_book._order_watermark_key(key, terminal_command.command_id)
    ] == Decimal("2")

    second_result = await restored_book.observe(
        cumulative_evidence("cumulative-5-after-restart", "5", "700", "140")
    )
    assert isinstance(second_result, Applied)
    assert second_result.consumed_quantity == Decimal("2")
    fills = restored_book._ensure_journal(key).read_cut().fills
    incremental_fill = next(fill for fill in fills if fill.trade_id == "cum-5")
    assert incremental_fill.quantity == Decimal("2")
    assert incremental_fill.price == Decimal("200")
    restored_reservation = restored_book.get_active_reservations(key)[0]
    assert restored_reservation.consumed_quantity == Decimal("5")
    assert restored_reservation.active_quantity == Decimal("5")

    invalid_command_repo = deepcopy(command_repo)
    active_details = invalid_command_repo.rows[order_command.command_id]["details"]
    assert isinstance(active_details, dict)
    active_details["cumulative_filled_quote"] = "0"
    invalid_book = ExecutionBook(command_repository=invalid_command_repo)
    with pytest.raises(RuntimeError, match="cumulative fill watermarks"):
        await invalid_book.restore(account_label="primary")


@pytest.mark.parametrize(
    "resolution_state",
    (
        ExchangeOrderState.CANCELED,
        ExchangeOrderState.FILLED,
        ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
    ),
)
@pytest.mark.asyncio
async def test_restored_dispatch_latch_requires_durable_resolution(
    resolution_state: ExchangeOrderState,
) -> None:
    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        InMemoryPositionReservationRepository,
    )
    from crypto_momentum_lab.domain.execution.trade_command import (
        PositionReservation,
        TradeCommand,
    )
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

    class CommandRepository:
        def __init__(self) -> None:
            self.rows: dict[str, dict[str, object]] = {}

        async def upsert_execution_command(self, **values: object) -> None:
            self.rows[str(values["command_id"])] = dict(values)

        async def load_active_execution_commands(
            self, account_label: str | None = None
        ) -> tuple[dict[str, object], ...]:
            active = []
            for row in self.rows.values():
                if row["status"] in ("terminal", "rejected"):
                    continue
                details = row["details"]
                assert isinstance(details, dict)
                scope_data = details["scope"]
                assert isinstance(scope_data, dict)
                if (
                    account_label is None
                    or scope_data["account_label"] == account_label
                ):
                    active.append(row)
            return tuple(active)

        async def load_execution_order_watermarks(
            self, account_label: str | None = None
        ) -> tuple[dict[str, object], ...]:
            watermarks = []
            for row in self.rows.values():
                details = row["details"]
                assert isinstance(details, dict)
                scope_data = details["scope"]
                assert isinstance(scope_data, dict)
                if (
                    account_label is None
                    or scope_data["account_label"] == account_label
                ):
                    watermarks.append(
                        {
                            "scope": scope_data,
                            "client_order_id": row["client_order_id"],
                            "cumulative_filled_quantity": details[
                                "cumulative_filled_quantity"
                            ],
                            "cumulative_filled_quote": details[
                                "cumulative_filled_quote"
                            ],
                        }
                    )
            return tuple(watermarks)

        async def load_seen_event_ids(self) -> tuple[str, ...]:
            return ()

        async def load_seen_fill_trade_ids(self) -> tuple[str, ...]:
            return ()

    scope = _scope()
    key = scope.to_position_key()
    command_id = f"restored-{resolution_state.value}"
    command_repo = CommandRepository()
    reservation_repo = InMemoryPositionReservationRepository()
    command = TradeCommand(
        command_id=command_id,
        position_key=key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("1"),
        reduce_only=True,
        created_at=_dt(10, 0),
    )
    reservation = PositionReservation(
        reservation_id=f"reservation-{command_id}",
        command_id=command_id,
        position_key=key,
        batch_id="batch-restored",
        reserved_quantity=Decimal("1"),
        created_at=_dt(10, 0),
    )
    reservation_repo.save_reservation(reservation)
    first_book = ExecutionBook(
        command_repository=command_repo,
        reservation_repository=reservation_repo,
    )
    first_book.coordinator.register_reservation(reservation)
    prepared = first_book.register_prepared_command(
        command, scope, [reservation.reservation_id]
    )
    await first_book._persist_outbox_state(prepared)
    await first_book.mark_dispatching(command_id)

    book = ExecutionBook(
        command_repository=command_repo,
        reservation_repository=reservation_repo,
    )
    await book.restore(account_label="primary")
    restored = book.get_outbox(command_id)
    assert restored is not None
    assert restored.state is DispatchState.UNKNOWN
    assert command_id in book._dispatch_reconciliation_required_commands

    fill = None
    if resolution_state is ExchangeOrderState.FILLED:
        fill = AccountFillEvent(
            environment="live",
            account_label="primary",
            symbol="BTCUSDT",
            trade_id=f"fill-{command_id}",
            order_id=command_id,
            side="SELL",
            price=Decimal("100"),
            quantity=Decimal("1"),
            realized_pnl=Decimal("0"),
            fee=Decimal("0"),
            fee_asset="USDT",
            trade_at=_dt(10, 1),
            raw_payload={
                "is_cumulative": True,
                "cum_qty": "1",
                "cum_quote": "100",
                "reduce_only": True,
            },
        )
    observed = await book.observe(
        ExecutionEvidence(
            evidence_id=f"resolution-{resolution_state.value}",
            scope=scope,
            observed_at=_dt(10, 1),
            fill=fill,
            order_event=ExchangeOrderEvent(
                event_id=f"event-{resolution_state.value}",
                client_order_id=command_id,
                state=resolution_state,
                occurred_at=_dt(10, 1),
                exchange_order_id="exchange-1",
                details={"account_label": "primary", "symbol": "BTCUSDT"},
            ),
        )
    )
    assert isinstance(observed, Applied)

    if resolution_state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
        assert command_id in book._dispatch_reconciliation_required_commands
        blocked = await book.act(
            ExecutionRequest(
                request_id=f"new-entry-{command_id}",
                scope=scope,
                strategy_name="trend_v1",
                strategy_version="1.0.0",
                run_id="run-after-restore",
                decision_ref="decision-after-restore",
                expected_view_token="*",
                action=TradeCommandType.ENTRY,
                requested_quantity=Decimal("1"),
            )
        )
        assert isinstance(blocked, Blocked)
        assert "reconciliation" in blocked.reason
        assert command_id in blocked.diagnostics
        return

    assert command_id not in book._dispatch_reconciliation_required_commands
    assert command_id not in book._recovery_required_commands

    timestamp = _dt(10, 2)
    journal = book._ensure_journal(key)
    journal.set_coverage(
        FactCoverageInterval(
            start_at=timestamp,
            end_at=timestamp,
            status=FactCoverageStatus.CONFIRMED,
        )
    )
    journal.record_snapshot(
        AccountPositionSnapshot(
            environment="live",
            account_label="primary",
            symbol="BTCUSDT",
            position_side="LONG",
            position_amt=Decimal("0"),
            entry_price=Decimal("0"),
            mark_price=Decimal("100"),
            unrealized_pnl=Decimal("0"),
            notional=Decimal("0"),
            leverage=5,
            margin_type="cross",
            observed_at=timestamp,
            raw_payload={},
        )
    )
    view = await book.read(scope)
    accepted = await book.act(
        ExecutionRequest(
            request_id=f"new-entry-{command_id}",
            scope=scope,
            strategy_name="trend_v1",
            strategy_version="1.0.0",
            run_id="run-after-restore",
            decision_ref="decision-after-restore",
            expected_view_token=view.projection_version,
            action=TradeCommandType.ENTRY,
            requested_quantity=Decimal("1"),
        )
    )
    assert isinstance(accepted, Accepted)


@pytest.mark.asyncio
async def test_execution_book_outbox_lifecycle_and_transitions() -> None:
    """Outbox state transitions:
    PREPARED -> DISPATCHING -> ACKNOWLEDGED / UNKNOWN / REJECTED.
    """
    book = ExecutionBook()
    scope = _scope()
    t0 = _dt(10, 0)
    key = scope.to_position_key()

    # Ingest position so view is trade-ready
    f_entry = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_entry",
        order_id="ord_entry",
        side="BUY",
        price=Decimal("50000"),
        quantity=Decimal("2.0"),
        realized_pnl=Decimal("0"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG"},
    )
    journal = book._ensure_journal(key)
    journal.set_coverage(
        FactCoverageInterval(
            start_at=t0,
            end_at=t0,
            status=FactCoverageStatus.CONFIRMED,
        )
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="ev_init", scope=scope, observed_at=t0, fill=f_entry
        )
    )

    view = await book.read(scope)
    req = ExecutionRequest(
        request_id="req-outbox-1",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("2.0"),
    )

    act_res = await book.act(req)
    assert isinstance(act_res, Accepted)
    cmd_id = req.request_id

    # 1. Outbox entry is PREPARED upon act acceptance
    outbox = book.get_outbox(cmd_id)
    assert outbox is not None
    assert outbox.state == DispatchState.PREPARED
    assert outbox.attempt_count == 0

    # 2. mark_dispatching transitions to DISPATCHING
    dispatching = await book.mark_dispatching(cmd_id)
    assert dispatching.state == DispatchState.DISPATCHING
    assert dispatching.attempt_count == 1

    # 3. mark_unknown preserves active reservations!
    unknown = await book.mark_unknown(cmd_id, reason="Gateway timeout 504")
    assert unknown.state == DispatchState.UNKNOWN
    assert unknown.last_error == "Gateway timeout 504"
    active_res = book._coordinator.get_active_reservations(key)
    assert len(active_res) == 1
    assert active_res[0].active_quantity == Decimal("2.0")

    # 4. mark_acknowledged transitions to ACKNOWLEDGED
    acked = await book.mark_acknowledged(cmd_id, external_order_id="binance_ord_999")
    assert acked.state == DispatchState.ACKNOWLEDGED
    assert acked.external_order_id == "binance_ord_999"

    # 5. mark_terminal releases active reservations
    terminal = await book.mark_terminal(cmd_id, reason="Fully settled")
    assert terminal.state == DispatchState.TERMINAL
    active_after_term = book._coordinator.get_active_reservations(key)
    assert len(active_after_term) == 0


@pytest.mark.asyncio
async def test_execution_book_cumulative_fills_and_reservation_settlement() -> None:
    """Cumulative fill reports 3 -> 3 -> 5 only consume 5 total, never 11."""
    book = ExecutionBook()
    scope = _scope()
    t0 = _dt(10, 0)
    key = scope.to_position_key()

    # Open 5 BTC position
    f_open = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_open",
        order_id="ord_open",
        side="BUY",
        price=Decimal("60000"),
        quantity=Decimal("5.0"),
        realized_pnl=Decimal("0"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG"},
    )
    journal = book._ensure_journal(key)
    journal.set_coverage(
        FactCoverageInterval(
            start_at=t0,
            end_at=t0,
            status=FactCoverageStatus.CONFIRMED,
        )
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="ev_open", scope=scope, observed_at=t0, fill=f_open
        )
    )

    view = await book.read(scope)
    req = ExecutionRequest(
        request_id="req-exit-cum",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-cum",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("5.0"),
    )

    act_res = await book.act(req)
    assert isinstance(act_res, Accepted)
    res_id = act_res.receipt.reservations[0].reservation_id
    r0 = book._coordinator.get_reservation(res_id)
    assert r0 is not None
    assert r0.reserved_quantity == Decimal("5.0")
    assert r0.consumed_quantity == Decimal("0")
    assert r0.active_quantity == Decimal("5.0")

    # Report 1: cumulative filled = 3.0
    f_cum1 = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_fill_1",
        order_id="req-exit-cum",
        side="SELL",
        price=Decimal("61000"),
        quantity=Decimal("3.0"),
        realized_pnl=Decimal("100"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG", "is_cumulative": True, "cum_qty": "3.0"},
    )
    app1 = await book.observe(
        ExecutionEvidence(evidence_id="ev_c1", scope=scope, observed_at=t0, fill=f_cum1)
    )
    assert isinstance(app1, Applied)
    assert app1.consumed_quantity == Decimal("3.0")

    r1 = book._coordinator.get_reservation(res_id)
    assert r1 is not None
    assert r1.consumed_quantity == Decimal("3.0")
    assert r1.active_quantity == Decimal("2.0")

    # Report 2: duplicate cumulative filled = 3.0 (must consume 0 delta)
    f_cum2 = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_fill_2",
        order_id="req-exit-cum",
        side="SELL",
        price=Decimal("61000"),
        quantity=Decimal("3.0"),
        realized_pnl=Decimal("100"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG", "is_cumulative": True, "cum_qty": "3.0"},
    )
    app2 = await book.observe(
        ExecutionEvidence(evidence_id="ev_c2", scope=scope, observed_at=t0, fill=f_cum2)
    )
    assert isinstance(app2, Applied)
    assert app2.consumed_quantity == Decimal("0")

    r2 = book._coordinator.get_reservation(res_id)
    assert r2 is not None
    assert r2.consumed_quantity == Decimal("3.0")
    assert r2.active_quantity == Decimal("2.0")

    # Report 3: cumulative filled = 5.0 (delta = 2.0, must consume remaining 2.0)
    f_cum3 = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_fill_3",
        order_id="req-exit-cum",
        side="SELL",
        price=Decimal("61000"),
        quantity=Decimal("5.0"),
        realized_pnl=Decimal("150"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG", "is_cumulative": True, "cum_qty": "5.0"},
    )
    app3 = await book.observe(
        ExecutionEvidence(evidence_id="ev_c3", scope=scope, observed_at=t0, fill=f_cum3)
    )
    assert isinstance(app3, Applied)
    assert app3.consumed_quantity == Decimal("2.0")

    r3 = book._coordinator.get_reservation(res_id)
    assert r3 is not None
    # Crucial invariant: total consumed is 5.0, not 11.0!
    assert r3.consumed_quantity == Decimal("5.0")
    assert r3.active_quantity == Decimal("0")
    assert r3.reserved_quantity == (
        r3.active_quantity + r3.consumed_quantity + r3.released_quantity
    )


@pytest.mark.asyncio
async def test_execution_book_partial_fill_and_order_cancel_event() -> None:
    """When order partially filled then canceled, remaining reservation is released."""
    book = ExecutionBook()
    scope = _scope()
    t0 = _dt(10, 0)
    key = scope.to_position_key()

    # Open 2.0 BTC position
    f_open = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_open",
        order_id="ord_open",
        side="BUY",
        price=Decimal("60000"),
        quantity=Decimal("2.0"),
        realized_pnl=Decimal("0"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG"},
    )
    journal = book._ensure_journal(key)
    journal.set_coverage(
        FactCoverageInterval(
            start_at=t0,
            end_at=t0,
            status=FactCoverageStatus.CONFIRMED,
        )
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="ev_open", scope=scope, observed_at=t0, fill=f_open
        )
    )

    view = await book.read(scope)
    req = ExecutionRequest(
        request_id="cmd-exit-partial",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-part",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("2.0"),
    )

    act_res = await book.act(req)
    assert isinstance(act_res, Accepted)
    res_id = act_res.receipt.reservations[0].reservation_id

    # 1. Partial fill of 0.8 BTC
    f_part = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_part",
        order_id="cmd-exit-partial",
        side="SELL",
        price=Decimal("61000"),
        quantity=Decimal("0.8"),
        realized_pnl=Decimal("50"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG"},
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="ev_part", scope=scope, observed_at=t0, fill=f_part
        )
    )

    r_part = book._coordinator.get_reservation(res_id)
    assert r_part is not None
    assert r_part.consumed_quantity == Decimal("0.8")
    assert r_part.active_quantity == Decimal("1.2")

    # 2. Exchange order cancel event arrives
    ev_cancel = ExchangeOrderEvent(
        event_id="ev_cancel_1",
        client_order_id="cmd-exit-partial",
        exchange_order_id="binance_ord_cancel",
        state=ExchangeOrderState.CANCELED,
        occurred_at=t0,
        details={},
    )
    app_cancel = await book.observe(
        ExecutionEvidence(
            evidence_id="ev_cancel_evidence",
            scope=scope,
            observed_at=t0,
            order_event=ev_cancel,
        )
    )
    assert isinstance(app_cancel, Applied)
    assert app_cancel.released_quantity == Decimal("1.2")

    # 3. Invariant: reserved (2.0) == active (0) + consumed (0.8) + released (1.2)
    r_final = book._coordinator.get_reservation(res_id)
    assert r_final is not None
    assert r_final.active_quantity == Decimal("0")
    assert r_final.consumed_quantity == Decimal("0.8")
    assert r_final.released_quantity == Decimal("1.2")
    assert r_final.reserved_quantity == (
        r_final.active_quantity + r_final.consumed_quantity + r_final.released_quantity
    )

    # 4. Outbox is marked TERMINAL
    outbox = book.get_outbox("cmd-exit-partial")
    assert outbox is not None
    assert outbox.state == DispatchState.TERMINAL
