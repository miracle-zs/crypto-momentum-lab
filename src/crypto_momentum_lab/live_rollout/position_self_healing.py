"""Unmanaged-position repair use case; transactions and ORM live in adapters."""

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from functools import partial
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_context_ports import (
    PositionContextBook,
    PositionRepairBook,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.position_repair_models import (
    PositionRepairBlocked,
    PositionRepairRequest,
    PositionRepairUnitOfWork,
)
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext

log = structlog.get_logger(__name__)

DEFAULT_REPAIR_RETRY_SECONDS = 30.0


class _PositionFacts(Protocol):
    """Minimal account-position shape needed to identify a divergence."""

    @property
    def symbol(self) -> str: ...

    @property
    def position_side(self) -> str: ...

    @property
    def position_amt(self) -> Decimal: ...

    @property
    def entry_price(self) -> Decimal: ...

    @property
    def observed_at(self) -> datetime: ...


@dataclass(slots=True)
class _RepairRetry:
    """Evidence version and deadline for retrying one unresolved position."""

    evidence: tuple[object, ...]
    retry_at: float


async def auto_heal_unmanaged_position(
    *,
    request: PositionRepairRequest,
    uow: PositionRepairUnitOfWork,
    book: PositionRepairBook,
    is_current: Callable[[], bool] | None = None,
) -> bool:
    try:
        published = await book.repair_position(request, uow=uow, is_current=is_current)
    except PositionRepairBlocked as error:
        reason = str(error)
        log.warning("position_repair_blocked", symbol=request.key.symbol,
                    account_label=request.key.account_label, reason=reason)
        return False
    receipt, reloaded = published.receipt, published.view
    log.info(
        "unmanaged_position_auto_healed_success"
        if receipt.changed
        else "position_repair_reloaded",
        account_label=request.key.account_label,
        symbol=request.key.symbol,
        new_facts=published.new_facts,
        head_revision=receipt.head_revision,
        projection_version=reloaded.projection_version,
    )
    return True


class LiveUnmanagedPositionRepair:
    """Coalesce current exposure for the existing account repair worker.

    Requests are merged per position key and material evidence. The same
    divergence is retried only after the recovery worker's interval; a changed
    quantity, entry price or stream can be repaired immediately.
    """

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
        clock: Callable[[], float] = time.monotonic,
        retry_interval_seconds: float = DEFAULT_REPAIR_RETRY_SECONDS,
    ) -> None:
        if retry_interval_seconds <= 0:
            raise ValueError("retry_interval_seconds must be positive")
        self._account = account_label
        self._run_id = run_id
        self._book = book
        self._uow = uow
        self._is_current = context_is_current
        self._invalidate = invalidate_context
        self._request_recovery = request_recovery
        self._clock = clock
        self._retry_interval_seconds = retry_interval_seconds
        self._retries: dict[PositionKey, _RepairRetry] = {}
        self._pending: LiveDaemonRuntimeContext | None = None
        self._latest: LiveDaemonRuntimeContext | None = None

    def request(self, context: LiveDaemonRuntimeContext) -> None:
        if not self._is_current(context):
            return
        self._latest = context
        if context.account_snapshot is not None:
            self._forget_keys_outside(context)
        if not context.unmanaged_position_symbols:
            # Every divergent position is resolved/reloaded.
            self._pending = None
            return
        if context.account_snapshot is None:
            return
        if self._needs_repair(context):
            # New evidence (or the retry deadline) for at least one position.
            self._pending = context
            self._request_recovery()
            return
        # Only the snapshot timestamp advanced: the divergence and its
        # evidence are unchanged, so leave the existing retry deadline alone
        # instead of attempting the same repair again.
        self._pending = context

    def _evidence(
        self,
        position: _PositionFacts,
        stream: tuple[str, str],
    ) -> tuple[object, ...]:
        """Material divergence evidence: quantity, price and stream, no clock."""
        return (
            position.symbol,
            str(position.position_side).upper(),
            abs(position.position_amt),
            position.entry_price,
            stream,
        )

    def _needs_attempt(
        self, key: PositionKey, evidence: tuple[object, ...], now: float
    ) -> bool:
        record = self._retries.get(key)
        if record is None or record.evidence != evidence:
            return True
        return now >= record.retry_at

    def _needs_repair(self, context: LiveDaemonRuntimeContext) -> bool:
        """Whether a divergent key has new evidence or an expired deadline.

        A newer snapshot timestamp with the same quantity, entry price and
        stream is the same divergence: it must not re-run the repair before the
        backoff deadline, because the evidence has not changed.
        """
        if context.account_snapshot is None:
            return False
        stream = self._book.get_active_stream("live", self._account)
        if stream is None:
            return True
        now = self._clock()
        for position in context.account_snapshot.positions:
            if (
                position.symbol not in context.unmanaged_position_symbols
                or position.position_amt == 0
            ):
                continue
            key = self._key_for(position)
            if self._needs_attempt(key, self._evidence(position, stream), now):
                return True
        return False

    def _key_for(self, position: _PositionFacts) -> PositionKey:
        return PositionKey(
            "live",
            self._account,
            position.symbol,
            FuturesPositionSide(str(position.position_side).upper()),
        )

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

    async def repair_pending(self) -> bool:
        """Return whether the worker must retain a retry for unfinished repair."""
        context = self._pending
        self._pending = None
        if context is None or context.account_snapshot is None:
            return False
        if not self._is_current(context):
            return False
        stream = self._book.get_active_stream("live", self._account)
        if stream is None:
            self._pending = context
            return True
        now = self._clock()
        self._forget_keys_outside(context)
        repaired = False
        remaining = False
        for position in context.account_snapshot.positions:
            if not self._is_current(context):
                break
            if (
                position.symbol not in context.unmanaged_position_symbols
                or position.position_amt == 0
            ):
                continue
            key = self._key_for(position)
            evidence = self._evidence(position, stream)
            if not self._needs_attempt(key, evidence, now):
                remaining = True
                continue
            try:
                # Record before I/O: a blocked or failing attempt must not be
                # retried until its deadline, and never twice concurrently.
                self._retries[key] = _RepairRetry(
                    evidence=evidence,
                    retry_at=now + self._retry_interval_seconds,
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
                healed = await auto_heal_unmanaged_position(
                    request=request,
                    uow=self._uow,
                    book=self._book,
                    is_current=partial(
                        self._repair_is_current,
                        request,
                        position.entry_price,
                    ),
                )
            except Exception as error:
                log.exception(
                    "auto_heal_unmanaged_position_failed",
                    account_label=self._account,
                    symbol=position.symbol,
                    error_type=type(error).__name__,
                )
                remaining = True
                continue
            if healed:
                repaired = True
            else:
                remaining = True
        if repaired:
            self._invalidate()
        if remaining and self._is_current(context):
            # Retry on the existing worker's next periodic pass (its own
            # deadline), not on every new context observation.
            self._pending = context
        return self._pending is not None

    def _forget_keys_outside(self, context: LiveDaemonRuntimeContext) -> None:
        """Drop attempt records for positions that are no longer divergent."""
        if context.account_snapshot is None:
            return
        divergent = {
            self._key_for(position)
            for position in context.account_snapshot.positions
            if position.symbol in context.unmanaged_position_symbols
            and position.position_amt != 0
        }
        for key in tuple(self._retries):
            if key not in divergent:
                self._retries.pop(key, None)
