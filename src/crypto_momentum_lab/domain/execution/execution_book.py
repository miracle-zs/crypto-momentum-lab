import inspect
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

import structlog

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.account_journal import (
    AccountJournal,
)
from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ExecutionCoordinator,
    ExecutionReadinessError,
    ReservationConflictError,
    VersionConflictError,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    ExitAllocation,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.execution.position_book import (
    PositionBook,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    ExitOrderSubmissionFact,
    FactCoverageInterval,
    FreshnessRequirement,
    PositionKey,
    PositionView,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocationPlan,
    ExitAllocator,
    ExitPolicyMode,
    PositionReservation,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

log = structlog.get_logger(__name__)


async def _maybe_await(val: Any) -> Any:
    if inspect.isawaitable(val):
        return await val
    return val


def _required_text(values: Mapping[str, Any], field_name: str) -> str:
    value = values.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"execution command {field_name} is missing or invalid")
    return value


class DispatchState(StrEnum):
    """Authoritative lifecycle states for outbound trade commands."""

    PREPARED = "prepared"
    DISPATCHING = "dispatching"
    ACKNOWLEDGED = "acknowledged"
    REJECTED = "rejected"
    UNKNOWN = "unknown"
    TERMINAL = "terminal"


@dataclass(frozen=True, slots=True)
class ExecutionScope:
    environment: str
    account_label: str
    symbol: str
    position_side: FuturesPositionSide = FuturesPositionSide.BOTH

    def to_position_key(self) -> PositionKey:
        return PositionKey(
            environment=self.environment,
            account_label=self.account_label,
            symbol=self.symbol,
            position_side=self.position_side,
        )


@dataclass(frozen=True, slots=True)
class OutboxEntry:
    """Immutable dispatch record tracking external order submission attempt."""

    command_id: str
    request_id: str
    scope: ExecutionScope
    command: TradeCommand
    state: DispatchState = DispatchState.PREPARED
    attempt_count: int = 0
    external_order_id: str | None = None
    last_error: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True, slots=True)
class ExecutionRequest:
    request_id: str
    scope: ExecutionScope
    strategy_name: str
    strategy_version: str
    run_id: str
    decision_ref: str
    expected_view_token: str
    action: TradeCommandType
    requested_quantity: Decimal
    order_type: str = "MARKET"
    limit_price: Decimal | None = None
    reduce_only: bool = False
    target_batch_ids: tuple[str, ...] = ()
    batch_quantities: Mapping[str, Decimal] | None = None
    exit_policy_mode: ExitPolicyMode = ExitPolicyMode.CONSOLIDATE_ELIGIBLE
    expected_projection_version: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.request_id.strip():
            raise ValueError("request_id must not be empty")
        if self.requested_quantity <= 0:
            raise ValueError("requested_quantity must be positive")
        if not self.expected_view_token.strip():
            raise ValueError("expected_view_token must not be empty")


@dataclass(frozen=True, slots=True)
class ExecutionReceipt:
    request_id: str
    scope: ExecutionScope
    command: TradeCommand
    reservations: tuple[PositionReservation, ...]
    committed_at: datetime
    view_token: str
    outbox_entry: OutboxEntry | None = None


@dataclass(frozen=True, slots=True)
class Accepted:
    receipt: ExecutionReceipt


@dataclass(frozen=True, slots=True)
class AlreadyAccepted:
    receipt: ExecutionReceipt


@dataclass(frozen=True, slots=True)
class StaleView:
    expected_token: str
    current_token: str
    reason: str


@dataclass(frozen=True, slots=True)
class Blocked:
    reason: str
    diagnostics: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CommandConflict:
    request_id: str
    reason: str


ExecutionActResult = Accepted | AlreadyAccepted | StaleView | Blocked | CommandConflict


@dataclass(frozen=True, slots=True)
class ExecutionEvidence:
    evidence_id: str
    scope: ExecutionScope
    observed_at: datetime
    fill: AccountFillEvent | None = None
    snapshot: AccountPositionSnapshot | None = None
    boundary: ExitOrderSubmissionFact | None = None
    order_event: ExchangeOrderEvent | None = None
    coverage: FactCoverageInterval | None = None


@dataclass(frozen=True, slots=True)
class Applied:
    evidence_id: str
    updated_view_token: str
    consumed_quantity: Decimal = Decimal("0")
    released_quantity: Decimal = Decimal("0")
    recovery_required: bool = False
    diagnostics: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Duplicate:
    evidence_id: str
    view_token: str


@dataclass(frozen=True, slots=True)
class EvidenceConflict:
    evidence_id: str
    reason: str


ExecutionObserveResult = Applied | Duplicate | EvidenceConflict


class ExecutionBook:
    """Authoritative account execution service coordinating PositionBook, Journal,
    and Reservations.

    Implements Section 7 of RFC 2026-09-25:
    - read(scope, requirement) -> PositionView
    - act(request) -> Accepted | AlreadyAccepted | StaleView | Blocked | CommandConflict
    - observe(evidence) -> Applied | Duplicate | EvidenceConflict
    """

    def __init__(
        self,
        *,
        books_by_key: dict[str, PositionBook] | None = None,
        journals_by_key: dict[str, AccountJournal] | None = None,
        coordinator: ExecutionCoordinator | None = None,
        reservation_repository: Any | None = None,
        command_repository: Any | None = None,
    ) -> None:
        self._books: dict[str, PositionBook] = books_by_key or {}
        self._journals: dict[str, AccountJournal] = journals_by_key or {}
        self._coordinator = coordinator or ExecutionCoordinator(
            repository=reservation_repository
        )
        self._reservation_repo = reservation_repository
        self._command_repo = command_repository
        self._requests_by_id: dict[str, ExecutionRequest] = {}
        self._receipts_by_id: dict[str, ExecutionReceipt] = {}
        self._seen_evidence_ids: set[str] = set()
        self._seen_trade_ids: set[str] = set()
        self._outbox_by_command_id: dict[str, OutboxEntry] = {}
        self._command_reservations: dict[str, list[str]] = {}
        self._order_cumulative_fills: dict[str, Decimal] = {}
        self._order_cumulative_quotes: dict[str, Decimal] = {}
        self._persistence_failed = False
        self._recovery_required_commands: set[str] = set()
        self._dispatch_reconciliation_required_commands: set[str] = set()

    @property
    def coordinator(self) -> ExecutionCoordinator:
        return self._coordinator

    @property
    def has_command_repository(self) -> bool:
        return self._command_repo is not None

    async def _persist_outbox_state(self, entry: OutboxEntry) -> None:
        if self._command_repo is None:
            return
        upserter = getattr(self._command_repo, "upsert_execution_command", None)
        if not callable(upserter):
            raise RuntimeError(
                "command repository does not implement upsert_execution_command"
            )
        watermark_key = self._order_watermark_key(
            entry.scope.to_position_key(), entry.command.command_id
        )
        details = {
            "scope": {
                "environment": entry.scope.environment,
                "account_label": entry.scope.account_label,
                "symbol": entry.scope.symbol,
                "position_side": (
                    entry.scope.position_side.value
                    if hasattr(entry.scope.position_side, "value")
                    else str(entry.scope.position_side)
                ),
            },
            "request_id": entry.request_id,
            "attempt_count": entry.attempt_count,
            "external_order_id": entry.external_order_id,
            "last_error": entry.last_error,
            "quantity": str(entry.command.requested_quantity),
            "side": (
                entry.command.side.value
                if hasattr(entry.command.side, "value")
                else str(entry.command.side)
            ),
            "order_type": (
                entry.command.order_type.value
                if hasattr(entry.command.order_type, "value")
                else str(entry.command.order_type)
            ),
            "limit_price": (
                str(entry.command.limit_price)
                if entry.command.limit_price is not None
                else None
            ),
            "reduce_only": entry.command.reduce_only,
            "expected_projection_version": entry.command.expected_projection_version,
            "reservations": self._command_reservations.get(entry.command_id, []),
            "cumulative_filled_quantity": str(
                self._order_cumulative_fills.get(watermark_key, Decimal("0"))
            ),
            "cumulative_filled_quote": str(
                self._order_cumulative_quotes.get(watermark_key, Decimal("0"))
            ),
        }
        try:
            await _maybe_await(
                upserter(
                    command_id=entry.command_id,
                    client_order_id=entry.command.command_id,
                    command=(
                        entry.command.command_type.value
                        if hasattr(entry.command.command_type, "value")
                        else str(entry.command.command_type)
                    ),
                    status=entry.state.value,
                    requested_at=entry.created_at,
                    details=details,
                )
            )
        except Exception as err:
            self._persistence_failed = True
            log.error(
                "persist_outbox_state_failed",
                command_id=entry.command_id,
                error=str(err),
            )
            raise

    async def drain(self, timeout_seconds: float = 5.0) -> None:
        """Lifecycle hook retained for callers; all persistence is awaited inline."""
        del timeout_seconds

    async def restore(self, account_label: str | None = None) -> None:
        """Restores in-flight outbox commands, deduplication, and reservations."""
        self._persistence_failed = True
        if self._command_repo is not None:
            loader = getattr(self._command_repo, "load_active_execution_commands", None)
            if callable(loader):
                try:
                    import inspect

                    sig = inspect.signature(loader)
                    if "account_label" in sig.parameters:
                        active_cmds = await _maybe_await(
                            loader(account_label=account_label)
                        )
                    else:
                        active_cmds = await _maybe_await(loader())
                    for cmd_data in active_cmds:
                        if not isinstance(cmd_data, Mapping):
                            raise TypeError("execution command row must be a mapping")
                        cid = _required_text(cmd_data, "command_id")
                        client_order_id = _required_text(cmd_data, "client_order_id")
                        if cid != client_order_id:
                            raise ValueError(
                                "execution command_id must match client_order_id"
                            )
                        status_str = _required_text(cmd_data, "status")
                        disp_state = DispatchState(status_str)
                        dtls = cmd_data.get("details")
                        if not isinstance(dtls, Mapping):
                            raise TypeError(
                                "execution command details must be a mapping"
                            )
                        scope_data = dtls.get("scope")
                        if not isinstance(scope_data, Mapping):
                            raise TypeError("execution command scope must be a mapping")
                        environment = _required_text(scope_data, "environment")
                        acc = _required_text(scope_data, "account_label")
                        symbol = _required_text(scope_data, "symbol")
                        position_side = FuturesPositionSide(
                            _required_text(scope_data, "position_side")
                        )
                        if account_label is not None and acc != account_label:
                            continue
                        scope = ExecutionScope(
                            environment=environment,
                            account_label=acc,
                            symbol=symbol,
                            position_side=position_side,
                        )
                        try:
                            side = StrategySide(_required_text(dtls, "side"))
                            order_type = EntryType(
                                _required_text(dtls, "order_type").lower()
                            )
                            command_type = TradeCommandType(
                                _required_text(cmd_data, "command").lower()
                            )
                            quantity = Decimal(_required_text(dtls, "quantity"))
                            if not quantity.is_finite() or quantity <= Decimal("0"):
                                raise ValueError(
                                    "execution command quantity must be positive"
                                )
                            if "reduce_only" not in dtls or not isinstance(
                                dtls["reduce_only"], bool
                            ):
                                raise ValueError(
                                    "execution command reduce_only must be persisted "
                                    "as bool"
                                )
                            raw_res_ids = dtls.get("reservations")
                            if not isinstance(raw_res_ids, (list, tuple)) or any(
                                not isinstance(res_id, str) or not res_id
                                for res_id in raw_res_ids
                            ):
                                raise ValueError(
                                    "execution command reservation links are missing "
                                    "or invalid"
                                )
                            request_id = _required_text(dtls, "request_id")
                            requested_at = cmd_data.get("requested_at")
                            if (
                                not isinstance(requested_at, datetime)
                                or requested_at.tzinfo is None
                            ):
                                raise ValueError(
                                    "execution command requested_at must be "
                                    "timezone-aware"
                                )
                            attempt_count = dtls.get("attempt_count")
                            if not isinstance(attempt_count, int) or attempt_count < 0:
                                raise ValueError(
                                    "execution command attempt_count is missing "
                                    "or invalid"
                                )
                            limit_price_val = dtls.get("limit_price")
                            limit_price = (
                                Decimal(str(limit_price_val))
                                if limit_price_val is not None
                                else None
                            )
                        except (KeyError, ValueError, TypeError) as parse_err:
                            log.warning(
                                "skipping_unparseable_active_execution_command",
                                command_id=cid,
                                error=str(parse_err),
                            )
                            continue

                        cmd = TradeCommand(
                            command_id=cid,
                            position_key=scope.to_position_key(),
                            command_type=command_type,
                            side=side,
                            order_type=order_type,
                            requested_quantity=quantity,
                            limit_price=limit_price,
                            reduce_only=dtls["reduce_only"],
                            expected_projection_version=dtls.get(
                                "expected_projection_version"
                            ),
                            created_at=requested_at,
                        )
                        entry = OutboxEntry(
                            command_id=cid,
                            request_id=request_id,
                            scope=scope,
                            command=cmd,
                            state=disp_state,
                            attempt_count=attempt_count,
                            external_order_id=dtls.get("external_order_id"),
                            last_error=dtls.get("last_error"),
                            created_at=requested_at,
                            updated_at=requested_at,
                        )
                        self._outbox_by_command_id[cid] = entry
                        self._command_reservations[cid] = list(raw_res_ids)
                        if disp_state == DispatchState.UNKNOWN:
                            self._dispatch_reconciliation_required_commands.add(cid)
                        elif disp_state == DispatchState.DISPATCHING:
                            # A process may have stopped after the network write
                            # but before recording its response. Never redispatch.
                            unknown = replace(
                                entry,
                                state=DispatchState.UNKNOWN,
                                last_error="restored dispatch requires reconciliation",
                                updated_at=datetime.now(UTC),
                            )
                            self._outbox_by_command_id[cid] = unknown
                            self._dispatch_reconciliation_required_commands.add(cid)
                            await self._persist_outbox_state(unknown)
                except Exception as err:
                    log.error("restore_active_commands_failed", error=str(err))
                    raise RuntimeError(
                        "Failed to restore active execution commands"
                    ) from err
            else:
                raise RuntimeError(
                    "command repository does not implement active command restore"
                )

            ev_loader = getattr(self._command_repo, "load_seen_event_ids", None)
            if callable(ev_loader):
                try:
                    seen_events = await _maybe_await(ev_loader())
                    self._seen_evidence_ids.update(seen_events)
                except Exception as err:
                    raise RuntimeError(
                        "Failed to restore execution event identities"
                    ) from err
            else:
                raise RuntimeError(
                    "command repository does not implement event identity restore"
                )

            fill_loader = getattr(self._command_repo, "load_seen_fill_trade_ids", None)
            if callable(fill_loader):
                try:
                    seen_trades = await _maybe_await(fill_loader())
                    self._seen_trade_ids.update(seen_trades)
                except Exception as err:
                    raise RuntimeError("Failed to restore fill identities") from err
            else:
                raise RuntimeError(
                    "command repository does not implement fill identity restore"
                )

            watermark_loader = getattr(
                self._command_repo, "load_execution_order_watermarks", None
            )
            if not callable(watermark_loader):
                raise RuntimeError(
                    "command repository does not implement cumulative fill "
                    "watermark restore"
                )
            try:
                import inspect

                sig = inspect.signature(watermark_loader)
                if "account_label" in sig.parameters:
                    watermark_rows = await _maybe_await(
                        watermark_loader(account_label=account_label)
                    )
                else:
                    watermark_rows = await _maybe_await(watermark_loader())
                for row in watermark_rows:
                    scope_data = row["scope"]
                    scope = ExecutionScope(
                        environment=scope_data["environment"],
                        account_label=scope_data["account_label"],
                        symbol=scope_data["symbol"],
                        position_side=FuturesPositionSide(scope_data["position_side"]),
                    )
                    if (
                        account_label is not None
                        and scope.account_label != account_label
                    ):
                        continue
                    order_id = _required_text(row, "client_order_id")
                    quantity = Decimal(str(row["cumulative_filled_quantity"]))
                    if not quantity.is_finite() or quantity < Decimal("0"):
                        raise ValueError("cumulative fill watermark cannot be negative")
                    quote = Decimal(str(row["cumulative_filled_quote"]))
                    if not quote.is_finite() or quote < Decimal("0"):
                        raise ValueError(
                            "cumulative quote watermark cannot be negative"
                        )
                    if quantity == Decimal("0") and quote != Decimal("0"):
                        raise ValueError(
                            "zero-quantity order cannot have cumulative quote"
                        )
                    if quantity > Decimal("0") and quote <= Decimal("0"):
                        raise ValueError(
                            "positive cumulative quantity requires positive quote"
                        )
                    key = self._order_watermark_key(scope.to_position_key(), order_id)
                    self._order_cumulative_fills[key] = max(
                        self._order_cumulative_fills.get(key, Decimal("0")),
                        quantity,
                    )
                    self._order_cumulative_quotes[key] = max(
                        self._order_cumulative_quotes.get(key, Decimal("0")),
                        quote,
                    )
            except Exception as err:
                raise RuntimeError(
                    "Failed to restore cumulative fill watermarks"
                ) from err

        if self._reservation_repo is not None:
            res_loader = getattr(
                self._reservation_repo, "load_active_reservations", None
            )
            if callable(res_loader):
                try:
                    active_res = await _maybe_await(res_loader())
                    for r in active_res:
                        if (
                            account_label is not None
                            and r.position_key.account_label != account_label
                        ):
                            continue
                        self._coordinator.register_reservation(r)
                except Exception as err:
                    raise RuntimeError("Failed to restore active reservations") from err
            else:
                raise RuntimeError(
                    "reservation repository does not implement active restore"
                )
        self._persistence_failed = False

    def _ensure_book(self, key: PositionKey) -> PositionBook:
        canon = key.canonical_id
        if canon not in self._books:
            if canon not in self._journals:
                self._journals[canon] = AccountJournal(key)
            self._books[canon] = PositionBook(self._journals[canon])
        return self._books[canon]

    def _ensure_journal(self, key: PositionKey) -> AccountJournal:
        canon = key.canonical_id
        if canon not in self._journals:
            self._journals[canon] = AccountJournal(key)
        return self._journals[canon]

    @staticmethod
    def _order_watermark_key(key: PositionKey, order_id: str) -> str:
        return f"{key.canonical_id}\x1f{order_id}"

    def _find_active_reservations_for_command(
        self, command_id: str
    ) -> list[PositionReservation]:
        res_ids = self._command_reservations.get(command_id, [])
        active: list[PositionReservation] = []
        for r_id in res_ids:
            r = self._coordinator.get_reservation(r_id)
            if r is not None and r.active_quantity > Decimal("0"):
                active.append(r)
        return active

    def get_active_reservations(
        self, key: PositionKey | None = None
    ) -> tuple[PositionReservation, ...]:
        """Returns active reservations tracked by the domain coordinator."""
        if hasattr(self._coordinator, "get_active_reservations"):
            if key is not None:
                return self._coordinator.get_active_reservations(key)
            active = [
                r
                for r in getattr(self._coordinator, "_reservations_by_id", {}).values()
                if r.active_quantity > Decimal("0")
            ]
            active.sort(key=lambda r: (r.created_at, r.reservation_id))
            return tuple(active)
        return ()

    async def read(
        self,
        scope: ExecutionScope,
        requirement: FreshnessRequirement | None = None,
        now: datetime | None = None,
    ) -> PositionView:
        """Projects authoritative point-in-time PositionView for scope."""
        key = scope.to_position_key()
        book = self._ensure_book(key)
        return book.get_view(requirement=requirement, now=now)

    async def act(
        self,
        request: ExecutionRequest,
    ) -> ExecutionActResult:
        """Accepts a trade request, enforcing CAS view token, capacity, and outbox."""
        if self._persistence_failed:
            return Blocked(
                reason=(
                    "Execution persistence failed; restore is required before trading"
                )
            )
        if self._recovery_required_commands:
            return Blocked(
                reason="Execution reservation settlement requires recovery",
                diagnostics=tuple(sorted(self._recovery_required_commands)),
            )
        if self._dispatch_reconciliation_required_commands:
            return Blocked(
                reason="Execution command reconciliation is required",
                diagnostics=tuple(
                    sorted(self._dispatch_reconciliation_required_commands)
                ),
            )
        key = request.scope.to_position_key()
        book = self._ensure_book(key)
        view = book.get_view()
        effective_view_token = view.projection_version

        # 1. Idempotency verification
        if request.request_id in self._requests_by_id:
            existing_req = self._requests_by_id[request.request_id]
            if existing_req == request:
                return AlreadyAccepted(self._receipts_by_id[request.request_id])
            return CommandConflict(
                request_id=request.request_id,
                reason="Conflicting payload for identical request_id",
            )
        restored_entry = self._outbox_by_command_id.get(request.request_id)
        if restored_entry is not None:
            return Blocked(
                reason="Execution command already exists and requires reconciliation",
                diagnostics=(restored_entry.state.value,),
            )

        # 2. View token CAS validation
        if (
            request.expected_view_token not in ("*", "pv_initial")
            and request.expected_view_token != view.projection_version
        ):
            return StaleView(
                expected_token=request.expected_view_token,
                current_token=view.projection_version,
                reason=(
                    f"Expected view token {request.expected_view_token} does not match "
                    f"current projection {view.projection_version}"
                ),
            )

        # 3. Trade readiness check
        if not view.is_ready_for_trade and not request.target_batch_ids:
            return Blocked(
                reason=(
                    f"PositionView is not ready for trade (status={view.health_status})"
                ),
                diagnostics=view.diagnostics,
            )

        # 4. Command building and reservation calculation
        episode = getattr(view, "active_episode", None)
        if episode is not None and getattr(episode, "side", None) is not None:
            side = episode.side
        elif key.position_side == FuturesPositionSide.SHORT:
            side = StrategySide.SHORT
        else:
            side = StrategySide.LONG

        order_type = (
            EntryType(request.order_type.lower())
            if isinstance(request.order_type, str)
            else request.order_type
        )

        reservations: tuple[PositionReservation, ...] = ()
        if request.action == TradeCommandType.EXIT:
            alloc_plan = ExitAllocator.plan_exit(
                view,
                target_batch_ids=(request.target_batch_ids or None),
                requested_quantity=request.requested_quantity,
                policy=request.exit_policy_mode,
                reason=f"exit_{request.decision_ref}",
            )

            if alloc_plan is None or alloc_plan.total_allocated_quantity <= Decimal(
                "0"
            ):
                if request.target_batch_ids:
                    if request.batch_quantities:
                        allocations = tuple(
                            ExitAllocation(
                                batch_id=bid,
                                allocated_quantity=request.batch_quantities.get(
                                    bid,
                                    request.requested_quantity
                                    / len(request.target_batch_ids),
                                ),
                            )
                            for bid in request.target_batch_ids
                        )
                    else:
                        qty_per_batch = request.requested_quantity / len(
                            request.target_batch_ids
                        )
                        allocations = tuple(
                            ExitAllocation(
                                batch_id=bid,
                                allocated_quantity=qty_per_batch,
                            )
                            for bid in request.target_batch_ids
                        )
                    alloc_plan = ExitAllocationPlan(
                        position_key=key,
                        allocations=allocations,
                        total_allocated_quantity=request.requested_quantity,
                        policy=request.exit_policy_mode,
                        reason=f"exit_{request.decision_ref}",
                        projection_version=effective_view_token,
                    )
                else:
                    return Blocked(
                        reason=(
                            "Insufficient active batch capacity for "
                            "requested exit quantity"
                        ),
                        diagnostics=(
                            f"Requested: {request.requested_quantity}, "
                            f"Total active: {view.total_quantity}",
                        ),
                    )

            command = TradeCommand(
                command_id=request.request_id,
                position_key=key,
                command_type=TradeCommandType.EXIT,
                side=side,
                order_type=order_type,
                requested_quantity=alloc_plan.total_allocated_quantity,
                limit_price=request.limit_price,
                reduce_only=True,
                expected_projection_version=effective_view_token,
                allocation_plan=alloc_plan,
                created_at=request.created_at,
            )

            if view.batches:
                try:
                    reservations = self._coordinator.reserve_exit(command, view)
                except (
                    ReservationConflictError,
                    VersionConflictError,
                    ExecutionReadinessError,
                ) as err:
                    return Blocked(
                        reason=str(err),
                        diagnostics=(type(err).__name__,),
                    )
            else:
                res_list: list[PositionReservation] = []
                for idx, alloc in enumerate(alloc_plan.allocations):
                    res_id = (
                        f"res_{command.command_id}"
                        if len(alloc_plan.allocations) == 1
                        else f"res_{command.command_id}_{idx}"
                    )
                    r = PositionReservation(
                        reservation_id=res_id,
                        command_id=command.command_id,
                        position_key=key,
                        batch_id=alloc.batch_id,
                        reserved_quantity=alloc.allocated_quantity,
                        created_at=command.created_at,
                    )
                    self._coordinator.register_reservation(r)
                    res_list.append(r)
                reservations = tuple(res_list)

            batch_quantities_dict = (
                {
                    alloc.batch_id: alloc.allocated_quantity
                    for alloc in alloc_plan.allocations
                }
                if alloc_plan
                else None
            )

            if self._reservation_repo is not None:
                loader = getattr(self._reservation_repo, "load_reservation", None)
                saver = getattr(self._reservation_repo, "save_reservations", None)
                single_saver = getattr(self._reservation_repo, "save_reservation", None)

                to_save: list[PositionReservation] = []
                for res in reservations:
                    if callable(loader):
                        try:
                            existing = await _maybe_await(loader(res.reservation_id))
                            if existing is not None:
                                if (
                                    existing.batch_id != res.batch_id
                                    or existing.reserved_quantity
                                    != res.reserved_quantity
                                    or existing.position_key != res.position_key
                                ):
                                    return CommandConflict(
                                        request_id=command.command_id,
                                        reason=(
                                            f"Reservation {res.reservation_id} already "
                                            f"exists with different parameters "
                                            f"(batch_id={existing.batch_id}, "
                                            f"quantity={existing.reserved_quantity}) "
                                            f"that does not match requested "
                                            f"(batch_id={res.batch_id}, "
                                            f"quantity={res.reserved_quantity})"
                                        ),
                                    )
                                continue
                        except ReservationConflictError:
                            raise
                        except Exception:
                            pass
                    to_save.append(res)

                if to_save:
                    expected_ver = (
                        request.expected_projection_version
                        if request.expected_projection_version is not None
                        else (
                            None
                            if request.expected_view_token in ("*", "pv_initial")
                            else request.expected_view_token
                        )
                    )
                    try:
                        if callable(saver):
                            await _maybe_await(
                                saver(
                                    tuple(to_save),
                                    expected_projection_version=expected_ver,
                                    batch_quantities=batch_quantities_dict,
                                )
                            )
                        elif callable(single_saver):
                            for res in to_save:
                                await _maybe_await(
                                    single_saver(
                                        res,
                                        expected_projection_version=expected_ver,
                                    )
                                )
                    except Exception as save_err:
                        return CommandConflict(
                            request_id=command.command_id,
                            reason=(
                                f"Reservation already exists or save conflict: "
                                f"{save_err}"
                            ),
                        )
        else:
            command = TradeCommand(
                command_id=request.request_id,
                position_key=key,
                command_type=request.action,
                side=side,
                order_type=order_type,
                requested_quantity=request.requested_quantity,
                limit_price=request.limit_price,
                reduce_only=request.reduce_only,
                expected_projection_version=effective_view_token,
                created_at=request.created_at,
            )

        committed_at = datetime.now(UTC)
        outbox = OutboxEntry(
            command_id=command.command_id,
            request_id=request.request_id,
            scope=request.scope,
            command=command,
            state=DispatchState.PREPARED,
            created_at=committed_at,
            updated_at=committed_at,
        )
        if reservations:
            self._command_reservations[command.command_id] = [
                r.reservation_id for r in reservations
            ]
        self._outbox_by_command_id[command.command_id] = outbox
        try:
            await self._persist_outbox_state(outbox)
        except Exception as persist_err:
            self._outbox_by_command_id.pop(command.command_id, None)
            self._command_reservations.pop(command.command_id, None)
            rollback_errors: list[str] = []
            for reservation in reservations:
                current = self._coordinator.get_reservation(reservation.reservation_id)
                if current is None or current.active_quantity <= Decimal("0"):
                    continue
                try:
                    released = self._coordinator.release_reservation(
                        current.reservation_id, current.active_quantity
                    )
                    if self._reservation_repo is not None:
                        updater = getattr(
                            self._reservation_repo, "update_reservation", None
                        )
                        if callable(updater):
                            await _maybe_await(
                                updater(
                                    released,
                                    release_reason="outbox_acceptance_failed",
                                )
                            )
                except Exception as rollback_err:
                    rollback_errors.append(str(rollback_err))
            diagnostics = [f"outbox persistence failed: {persist_err}"]
            if rollback_errors:
                diagnostics.append(
                    "reservation rollback failed: " + "; ".join(rollback_errors)
                )
            return Blocked(
                reason="Execution command was not durably accepted",
                diagnostics=tuple(diagnostics),
            )

        receipt = ExecutionReceipt(
            request_id=request.request_id,
            scope=request.scope,
            command=command,
            reservations=reservations,
            committed_at=committed_at,
            view_token=effective_view_token,
            outbox_entry=outbox,
        )
        self._requests_by_id[request.request_id] = request
        self._receipts_by_id[request.request_id] = receipt
        return Accepted(receipt)

    def register_prepared_command(
        self,
        command: TradeCommand,
        scope: ExecutionScope,
        reservation_ids: list[str] | tuple[str, ...] = (),
    ) -> OutboxEntry:
        """Registers a prepared command into the outbox and links reservations."""
        entry = OutboxEntry(
            command_id=command.command_id,
            request_id=command.command_id,
            scope=scope,
            command=command,
            state=DispatchState.PREPARED,
            created_at=command.created_at,
            updated_at=command.created_at,
        )
        self._outbox_by_command_id[command.command_id] = entry
        if reservation_ids:
            self._command_reservations[command.command_id] = list(reservation_ids)
        return entry

    def get_outbox(self, command_id: str) -> OutboxEntry | None:
        """Returns the outbox record for command_id if found."""
        return self._outbox_by_command_id.get(command_id)

    def list_outbox(
        self,
        scope: ExecutionScope | None = None,
        state: DispatchState | None = None,
    ) -> tuple[OutboxEntry, ...]:
        """Queries outbox records filtered by scope and dispatch state."""
        entries: list[OutboxEntry] = list(self._outbox_by_command_id.values())
        if scope is not None:
            entries = [
                e
                for e in entries
                if e.scope.to_position_key().canonical_id
                == scope.to_position_key().canonical_id
            ]
        if state is not None:
            entries = [e for e in entries if e.state == state]
        return tuple(entries)

    async def mark_dispatching(
        self, command_id: str, dispatched_at: datetime | None = None
    ) -> OutboxEntry:
        """Transition PREPARED to DISPATCHING; UNKNOWN requires reconciliation."""
        entry = self._outbox_by_command_id.get(command_id)
        if entry is None:
            raise KeyError(f"Outbox entry {command_id} not found")
        if entry.state != DispatchState.PREPARED:
            raise ValueError(
                f"Cannot dispatch outbox entry in state {entry.state.value}"
            )
        now = dispatched_at or datetime.now(UTC)
        updated = replace(
            entry,
            state=DispatchState.DISPATCHING,
            attempt_count=entry.attempt_count + 1,
            updated_at=now,
        )
        await self._persist_transition(entry, updated)
        return self._outbox_by_command_id[command_id]

    async def mark_acknowledged(
        self,
        command_id: str,
        external_order_id: str,
        acknowledged_at: datetime | None = None,
    ) -> OutboxEntry:
        """Transitions outbox to ACKNOWLEDGED with external exchange order ID."""
        entry = self._outbox_by_command_id.get(command_id)
        if entry is None:
            raise KeyError(f"Outbox entry {command_id} not found")
        now = acknowledged_at or datetime.now(UTC)
        updated = replace(
            entry,
            state=DispatchState.ACKNOWLEDGED,
            external_order_id=external_order_id,
            updated_at=now,
        )
        await self._persist_transition(entry, updated)
        return self._outbox_by_command_id[command_id]

    async def mark_unknown(
        self,
        command_id: str,
        reason: str,
        unknown_at: datetime | None = None,
    ) -> OutboxEntry:
        """Transitions outbox to UNKNOWN while preserving active reservations."""
        entry = self._outbox_by_command_id.get(command_id)
        if entry is None:
            raise KeyError(f"Outbox entry {command_id} not found")
        if entry.state in (DispatchState.TERMINAL, DispatchState.REJECTED):
            return entry
        now = unknown_at or datetime.now(UTC)
        updated = replace(
            entry,
            state=DispatchState.UNKNOWN,
            last_error=reason,
            updated_at=now,
        )
        self._dispatch_reconciliation_required_commands.add(command_id)
        try:
            await self._persist_transition(entry, updated)
        except Exception:
            # Once a submit may have reached the exchange, a failed durable
            # UNKNOWN write must still seal this process against resubmission.
            self._outbox_by_command_id[command_id] = updated
            self._persistence_failed = True
            raise
        return self._outbox_by_command_id[command_id]

    async def mark_rejected(
        self,
        command_id: str,
        reason: str,
        rejected_at: datetime | None = None,
    ) -> OutboxEntry:
        """Transitions outbox to REJECTED and releases all active reservations."""
        entry = self._outbox_by_command_id.get(command_id)
        if entry is None:
            raise KeyError(f"Outbox entry {command_id} not found")
        now = rejected_at or datetime.now(UTC)
        updated = replace(
            entry,
            state=DispatchState.REJECTED,
            last_error=reason,
            updated_at=now,
        )
        await self._persist_transition(entry, updated)
        await self._release_command_reservations(command_id, reason="command_rejected")
        return self._outbox_by_command_id[command_id]

    async def mark_terminal(
        self,
        command_id: str,
        reason: str = "",
        terminal_at: datetime | None = None,
    ) -> OutboxEntry:
        """Transitions outbox to TERMINAL and releases remaining reservations."""
        entry = self._outbox_by_command_id.get(command_id)
        if entry is None:
            raise KeyError(f"Outbox entry {command_id} not found")
        now = terminal_at or datetime.now(UTC)
        updated = replace(
            entry,
            state=DispatchState.TERMINAL,
            last_error=reason if reason else entry.last_error,
            updated_at=now,
        )
        await self._persist_transition(entry, updated)
        await self._release_command_reservations(
            command_id, reason=reason or "command_terminal"
        )
        return self._outbox_by_command_id[command_id]

    async def _persist_transition(
        self,
        previous: OutboxEntry,
        updated: OutboxEntry,
    ) -> None:
        await self._persist_outbox_state(updated)
        self._outbox_by_command_id[updated.command_id] = updated

    async def _release_command_reservations(
        self,
        command_id: str,
        *,
        reason: str,
    ) -> Decimal:
        released_total = Decimal("0")
        for reservation in self._find_active_reservations_for_command(command_id):
            released = reservation.release(reservation.active_quantity)
            await self._persist_reservation_update(released, release_reason=reason)
            released_total += released.released_quantity - reservation.released_quantity
        return released_total

    async def _persist_reservation_update(
        self,
        reservation: PositionReservation,
        *,
        release_reason: str | None = None,
    ) -> None:
        if self._reservation_repo is not None:
            updater = getattr(self._reservation_repo, "update_reservation", None)
            if not callable(updater):
                self._persistence_failed = True
                raise RuntimeError(
                    "reservation repository does not implement update_reservation"
                )
            try:
                if release_reason is None:
                    await _maybe_await(updater(reservation))
                else:
                    await _maybe_await(
                        updater(reservation, release_reason=release_reason)
                    )
            except Exception:
                self._persistence_failed = True
                self._recovery_required_commands.add(reservation.command_id)
                raise
        self._coordinator.update_reservation(reservation)

    async def observe(
        self,
        evidence: ExecutionEvidence,
    ) -> ExecutionObserveResult:
        """Idempotently ingests exchange evidence and settles allocations."""
        if evidence.evidence_id in self._seen_evidence_ids:
            key = evidence.scope.to_position_key()
            book = self._ensure_book(key)
            return Duplicate(
                evidence_id=evidence.evidence_id,
                view_token=book.get_view().projection_version,
            )

        key = evidence.scope.to_position_key()
        journal = self._ensure_journal(key)
        book = self._ensure_book(key)

        consumed_qty = Decimal("0")
        released_qty = Decimal("0")
        settlement_recovery_required = False
        diagnostics: tuple[str, ...] = ()
        pending_watermark: tuple[str, Decimal, Decimal] | None = None
        dispatch_reconciled_command_id: str | None = None

        # 1. Process Fill
        if evidence.fill is not None:
            fill = evidence.fill
            trade_id = fill.trade_id
            order_id = fill.order_id
            raw_payload = fill.raw_payload if isinstance(fill.raw_payload, dict) else {}
            is_cumulative = bool(
                raw_payload.get("is_cumulative") or "cum_qty" in raw_payload
            )
            delta_qty = fill.quantity
            applied_fill = fill
            if is_cumulative:
                cumulative_qty = Decimal(str(raw_payload.get("cum_qty", fill.quantity)))
                cumulative_quote = Decimal(
                    str(raw_payload.get("cum_quote", cumulative_qty * fill.price))
                )
                if (
                    not cumulative_qty.is_finite()
                    or not cumulative_quote.is_finite()
                    or cumulative_qty < Decimal("0")
                    or cumulative_quote < Decimal("0")
                ):
                    return EvidenceConflict(
                        evidence_id=evidence.evidence_id,
                        reason="Cumulative fill quantity or quote is invalid",
                    )
                watermark_key = self._order_watermark_key(key, order_id)
                previous_cumulative = self._order_cumulative_fills.get(
                    watermark_key, Decimal("0")
                )
                previous_quote = self._order_cumulative_quotes.get(
                    watermark_key, Decimal("0")
                )
                delta_qty = cumulative_qty - previous_cumulative
                if delta_qty < Decimal("0"):
                    # An older exchange report is harmless: it must not rewind
                    # the high-water mark or change the current position view.
                    delta_qty = Decimal("0")
                elif delta_qty == Decimal("0"):
                    if cumulative_quote != previous_quote:
                        return EvidenceConflict(
                            evidence_id=evidence.evidence_id,
                            reason=(
                                "Cumulative quote changed without a quantity change"
                            ),
                        )
                else:
                    delta_quote = cumulative_quote - previous_quote
                    if delta_quote <= Decimal("0"):
                        return EvidenceConflict(
                            evidence_id=evidence.evidence_id,
                            reason=(
                                "Cumulative quote did not increase with cumulative "
                                "quantity"
                            ),
                        )
                    if trade_id in self._seen_trade_ids:
                        return EvidenceConflict(
                            evidence_id=evidence.evidence_id,
                            reason=(
                                f"Cumulative fill identity {trade_id} was reused with "
                                "a higher cumulative quantity"
                            ),
                        )
                    applied_fill = replace(
                        fill,
                        quantity=delta_qty,
                        price=delta_quote / delta_qty,
                    )
                    pending_watermark = (
                        watermark_key,
                        cumulative_qty,
                        cumulative_quote,
                    )

            existing_trade = next(
                (
                    prior
                    for prior in journal.read_cut().fills
                    if prior.trade_id == trade_id
                ),
                None,
            )
            if not is_cumulative and existing_trade is not None:
                if (
                    existing_trade.quantity != fill.quantity
                    or existing_trade.price != fill.price
                    or existing_trade.side.upper() != fill.side.upper()
                    or existing_trade.symbol != fill.symbol
                ):
                    return EvidenceConflict(
                        evidence_id=evidence.evidence_id,
                        reason=(
                            f"Fill {trade_id} conflicts with existing journal records"
                        ),
                    )
                # A repeated exchange trade can arrive with a new transport
                # evidence ID. The trade ID, rather than the evidence ID, owns
                # fill quantity and reservation settlement.
                delta_qty = Decimal("0")
            elif not is_cumulative and trade_id in self._seen_trade_ids:
                # Restore currently reloads trade identities without replaying
                # all historical fills into the journal. In that case we know
                # this ID was consumed but cannot prove the payload matches.
                return EvidenceConflict(
                    evidence_id=evidence.evidence_id,
                    reason=(
                        f"Fill {trade_id} was already seen but its journal facts "
                        "are unavailable; recovery is required"
                    ),
                )

            is_new_trade = trade_id not in self._seen_trade_ids
            if is_new_trade:
                if delta_qty > Decimal("0"):
                    accepted = journal.append_fill(applied_fill)
                    if not accepted and journal.has_conflicts:
                        return EvidenceConflict(
                            evidence_id=evidence.evidence_id,
                            reason=(
                                f"Fill {fill.trade_id} conflicted with "
                                "existing journal records"
                            ),
                        )
                    self._seen_trade_ids.add(trade_id)
                elif not is_cumulative:
                    self._seen_trade_ids.add(trade_id)

            active_episode = book.get_view().active_episode
            is_exit_fill = (
                (
                    fill.side.upper() == "SELL"
                    and key.position_side == FuturesPositionSide.LONG
                )
                or (
                    fill.side.upper() == "BUY"
                    and key.position_side == FuturesPositionSide.SHORT
                )
                or bool(self._find_active_reservations_for_command(order_id))
                or bool(raw_payload.get("reduce_only"))
                or (
                    evidence.order_event is not None
                    and bool((evidence.order_event.details or {}).get("is_reduce_only"))
                )
                or (
                    active_episode is not None
                    and (
                        (
                            active_episode.side == StrategySide.LONG
                            and fill.side.upper() == "SELL"
                        )
                        or (
                            active_episode.side == StrategySide.SHORT
                            and fill.side.upper() == "BUY"
                        )
                    )
                )
            )
            if is_exit_fill and delta_qty > Decimal("0"):
                linked_reservations = self._find_active_reservations_for_command(
                    order_id
                )
                if not linked_reservations:
                    self._recovery_required_commands.add(order_id)
                    diagnostics = (
                        f"No active reservation is linked to filled command {order_id}",
                    )
                    settlement_recovery_required = True
                else:
                    remaining = delta_qty
                    for reservation in linked_reservations:
                        if remaining <= Decimal("0"):
                            break
                        consume_amt = min(remaining, reservation.active_quantity)
                        if consume_amt > Decimal("0"):
                            updated_res = reservation.consume(consume_amt)
                            await self._persist_reservation_update(updated_res)
                            consumed_qty += consume_amt
                            remaining -= consume_amt
                    if remaining > Decimal("0"):
                        self._recovery_required_commands.add(order_id)
                        reported_quantity = (
                            cumulative_qty if is_cumulative else delta_qty
                        )
                        diagnostics = (
                            f"Cumulative fill {reported_quantity} exceeds linked "
                            f"active reservations by {remaining}",
                        )
                        settlement_recovery_required = True

        # 2. Process Snapshot
        if evidence.snapshot is not None:
            journal.record_snapshot(evidence.snapshot)

        # 2b. Process Coverage
        if getattr(evidence, "coverage", None) is not None:
            journal.set_coverage(evidence.coverage)

        # 3. Process Boundary
        if evidence.boundary is not None:
            journal.record_boundary(evidence.boundary)

        # 4. Process Order Event
        if evidence.order_event is not None:
            ev_state = evidence.order_event.state
            cmd_id = evidence.order_event.client_order_id
            outbox = self._outbox_by_command_id.get(cmd_id)
            already_terminal = outbox is not None and outbox.state in (
                DispatchState.TERMINAL,
                DispatchState.REJECTED,
            )

            if outbox is not None and not already_terminal:
                if ev_state in (
                    ExchangeOrderState.ACKNOWLEDGED,
                    ExchangeOrderState.SUBMITTED,
                ) and outbox.state in (
                    DispatchState.PREPARED,
                    DispatchState.DISPATCHING,
                    DispatchState.UNKNOWN,
                ):
                    await self._persist_transition(
                        outbox,
                        replace(
                            outbox,
                            state=DispatchState.ACKNOWLEDGED,
                            updated_at=evidence.observed_at,
                        ),
                    )
                elif ev_state in (
                    ExchangeOrderState.CANCELED,
                    ExchangeOrderState.EXPIRED,
                    ExchangeOrderState.REJECTED,
                    ExchangeOrderState.ABSENT_RECONCILED,
                    ExchangeOrderState.FILLED,
                ):
                    target_state = (
                        DispatchState.REJECTED
                        if ev_state == ExchangeOrderState.REJECTED
                        else DispatchState.TERMINAL
                    )
                    updated = replace(
                        outbox,
                        state=target_state,
                        last_error=(
                            f"Order {ev_state.value}"
                            if ev_state != ExchangeOrderState.FILLED
                            else outbox.last_error
                        ),
                        updated_at=evidence.observed_at,
                    )
                    await self._persist_transition(outbox, updated)
                    released_qty += await self._release_command_reservations(
                        cmd_id,
                        reason=f"order_finished_{ev_state.value.lower()}",
                    )
                    dispatch_reconciled_command_id = cmd_id
                elif ev_state == ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
                    self._dispatch_reconciliation_required_commands.add(cmd_id)
                    await self._persist_transition(
                        outbox,
                        replace(
                            outbox,
                            state=DispatchState.UNKNOWN,
                            last_error="Pending reconciliation",
                            updated_at=evidence.observed_at,
                        ),
                    )

        if pending_watermark is not None:
            watermark_key, cumulative_qty, cumulative_quote = pending_watermark
            self._order_cumulative_fills[watermark_key] = cumulative_qty
            self._order_cumulative_quotes[watermark_key] = cumulative_quote
            cmd_id = (
                evidence.order_event.client_order_id
                if evidence.order_event is not None
                else evidence.fill.order_id
                if evidence.fill is not None
                else ""
            )
            current_entry = self._outbox_by_command_id.get(cmd_id)
            if current_entry is not None:
                await self._persist_outbox_state(current_entry)
            else:
                self._recovery_required_commands.add(cmd_id)
                settlement_recovery_required = True
                diagnostics = (
                    f"No outbox command exists for cumulative fill {cmd_id}",
                )

        if dispatch_reconciled_command_id is not None:
            self._dispatch_reconciliation_required_commands.discard(
                dispatch_reconciled_command_id
            )

        self._seen_evidence_ids.add(evidence.evidence_id)
        updated_view = book.get_view(now=evidence.observed_at)

        return Applied(
            evidence_id=evidence.evidence_id,
            updated_view_token=updated_view.projection_version,
            consumed_quantity=consumed_qty,
            released_quantity=released_qty,
            recovery_required=settlement_recovery_required,
            diagnostics=diagnostics,
        )


__all__ = [
    "Accepted",
    "AlreadyAccepted",
    "Blocked",
    "CommandConflict",
    "DispatchState",
    "Duplicate",
    "EvidenceConflict",
    "ExecutionActResult",
    "ExecutionBook",
    "ExecutionEvidence",
    "ExecutionObserveResult",
    "ExecutionReceipt",
    "ExecutionRequest",
    "ExecutionScope",
    "OutboxEntry",
    "StaleView",
]
