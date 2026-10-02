"""Unmanaged-position repair use case; transactions and ORM live in adapters."""

from collections.abc import Callable
from decimal import Decimal

import structlog

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.ports import DecisionCommitConflict
from crypto_momentum_lab.domain.execution.position_context_ports import (
    PositionContextBook,
    PositionRepairBook,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.position_repair import (
    build_position_repair,
)
from crypto_momentum_lab.domain.execution.position_repair_models import (
    PositionRepairBlocked,
    PositionRepairRequest,
    PositionRepairUnitOfWork,
)
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext

log = structlog.get_logger(__name__)


async def auto_heal_unmanaged_position(
    *,
    request: PositionRepairRequest,
    uow: PositionRepairUnitOfWork,
    book: PositionRepairBook,
    is_current: Callable[[], bool] | None = None,
) -> bool:
    for attempt in range(3):
        try:
            async with uow.transaction(request.key) as tx:
                loaded = await tx.load_repair_facts(request)
                if is_current is not None and not is_current():
                    raise PositionRepairBlocked(
                        "repair context advanced during fact load"
                    )
                repair = build_position_repair(request, loaded)
                receipt = await tx.persist_repair(repair)
            break
        except PositionRepairBlocked as error:
            log.warning(
                "position_repair_blocked",
                symbol=request.key.symbol,
                account_label=request.key.account_label,
                reason=str(error),
            )
            return False
        except DecisionCommitConflict:
            if attempt == 2:
                raise
            # Re-enter the shared lock, re-read and recompute. Never replay a plan.
    # The durable commit has returned. The Book validates/reloads under its own
    # mutation lock before publishing any candidate or clearing unmanaged status.
    reloaded = await book.reload_position(
        request.key,
        expected_scope=request.scope,
        expected_quantity=request.expected_quantity,
    )
    if (
        reloaded is None
        or reloaded.stream_scope != receipt.scope
        or reloaded.total_quantity != request.expected_quantity
        or not reloaded.is_ready_for_trade
    ):
        raise PositionRepairBlocked(
            "durable repair committed but Book reload is not ready"
        )
    log.info(
        "unmanaged_position_auto_healed_success"
        if receipt.changed
        else "position_repair_reloaded",
        account_label=request.key.account_label,
        symbol=request.key.symbol,
        new_facts=repair.new_facts,
        head_revision=receipt.head_revision,
        projection_version=reloaded.projection_version,
    )
    return True


class LiveUnmanagedPositionRepair:
    """Coalesce current exposure for the existing account repair worker."""

    def __init__(
        self,
        *,
        account_label: str,
        run_id: str,
        book: PositionContextBook,
        uow: PositionRepairUnitOfWork,
        context_is_current: Callable[[LiveDaemonRuntimeContext], bool],
        invalidate_context: Callable[[], None],
        request_recovery: Callable[[], None],
    ) -> None:
        self._account = account_label
        self._run_id = run_id
        self._book = book
        self._uow = uow
        self._is_current = context_is_current
        self._invalidate = invalidate_context
        self._request_recovery = request_recovery
        self._pending: LiveDaemonRuntimeContext | None = None
        self._latest: LiveDaemonRuntimeContext | None = None

    def request(self, context: LiveDaemonRuntimeContext) -> None:
        if not self._is_current(context):
            return
        self._latest = context
        self._pending = context if context.unmanaged_position_symbols else None
        if self._pending is not None:
            self._request_recovery()

    def _repair_is_current(
        self, request: PositionRepairRequest, entry_price: Decimal
    ) -> bool:
        context = self._latest
        if (
            context is None
            or not self._is_current(context)
            or context.account_snapshot is None
            or request.key.symbol not in context.unmanaged_position_symbols
            or self._book.get_active_stream("live", self._account)
            != (request.scope.stream_id, request.scope.stream_epoch)
        ):
            return False
        # A new mark price/context version is not a new position. The normal
        # transaction lock and CAS still validate the latest durable facts.
        return any(
            position.symbol == request.key.symbol
            and position.position_side.upper() == request.key.position_side.value
            and abs(position.position_amt) == request.expected_quantity
            and position.entry_price == entry_price
            and position.observed_at >= request.observed_at
            for position in context.account_snapshot.positions
        )

    async def repair_pending(self) -> None:
        context = self._pending
        self._pending = None
        if context is None or context.account_snapshot is None:
            return
        if not self._is_current(context):
            return
        stream = self._book.get_active_stream("live", self._account)
        if stream is None:
            self._pending = context
            return
        repaired = False
        for position in context.account_snapshot.positions:
            if not self._is_current(context):
                break
            if (
                position.symbol not in context.unmanaged_position_symbols
                or position.position_amt == 0
            ):
                continue
            try:
                key = PositionKey(
                    "live",
                    self._account,
                    position.symbol,
                    FuturesPositionSide(position.position_side.upper()),
                )
                request = PositionRepairRequest(
                    key=key,
                    run_id=self._run_id,
                    scope=AccountFactStreamScope.for_position_key(
                        key, stream_id=stream[0], stream_epoch=stream[1]
                    ),
                    expected_quantity=abs(position.position_amt),
                    observed_at=position.observed_at,
                )
                repaired = (
                    await auto_heal_unmanaged_position(
                        request=request,
                        uow=self._uow,
                        book=self._book,
                        is_current=lambda: self._repair_is_current(
                            request, position.entry_price
                        ),
                    )
                    or repaired
                )
            except Exception as error:
                log.exception(
                    "auto_heal_unmanaged_position_failed",
                    account_label=self._account,
                    symbol=position.symbol,
                    error_type=type(error).__name__,
                )
        if repaired:
            self._invalidate()
        elif self._pending is None and self._is_current(context):
            # Retry on the existing worker's next periodic pass, without a spin loop.
            self._pending = context
