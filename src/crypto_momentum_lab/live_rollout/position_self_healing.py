"""Unmanaged-position repair use case; transactions and ORM live in adapters."""

import structlog

from crypto_momentum_lab.domain.execution.ports import DecisionCommitConflict
from crypto_momentum_lab.domain.execution.position_context_ports import (
    PositionRepairBook,
)
from crypto_momentum_lab.domain.execution.position_repair import (
    build_position_repair,
)
from crypto_momentum_lab.domain.execution.position_repair_models import (
    PositionRepairBlocked,
    PositionRepairRequest,
    PositionRepairUnitOfWork,
)

log = structlog.get_logger(__name__)


async def auto_heal_unmanaged_position(
    *,
    request: PositionRepairRequest,
    uow: PositionRepairUnitOfWork,
    book: PositionRepairBook,
) -> bool:
    for attempt in range(3):
        try:
            async with uow.transaction(request.key) as tx:
                loaded = await tx.load_repair_facts(request)
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
