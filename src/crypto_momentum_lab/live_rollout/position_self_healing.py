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
class _RepairAttempt:
    """Per-position repair record: evidence version, failure class, backoff."""

    evidence: tuple[object, ...]
    attempts: int
    last_failure: str | None
    next_attempt_at: float
    deferral_logged: bool = False


async def auto_heal_unmanaged_position(
    *,
    request: PositionRepairRequest,
    uow: PositionRepairUnitOfWork,
    book: PositionRepairBook,
    is_current: Callable[[], bool] | None = None,
    on_blocked: Callable[[str], None] | None = None,
) -> bool:
    try:
        published = await book.repair_position(request, uow=uow, is_current=is_current)
    except PositionRepairBlocked as error:
        reason = str(error)
        log.warning("position_repair_blocked", symbol=request.key.symbol,
                    account_label=request.key.account_label, reason=reason)
        if on_blocked is not None:
            on_blocked(reason)
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

    Requests are merged per position key and evidence version: repeating the
    same unmanaged observation (same quantity, entry price, snapshot time and
    stream epoch) never re-runs the repair before its backoff deadline, so a
    divergence that the evidence does not explain cannot spin the worker or
    flood the log. Each attempt records its failure classification and the next
    attempt time, exposed through :meth:`recovery_status`.
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
        self._attempts: dict[PositionKey, _RepairAttempt] = {}
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
        for position in context.account_snapshot.positions:
            if (
                position.symbol in context.unmanaged_position_symbols
                and position.position_amt != 0
            ):
                self._log_deferral(self._key_for(position))

    def recovery_status(self) -> tuple[dict[str, object], ...]:
        """Per-position attempt classification and next deadline, for health."""
        now = self._clock()
        return tuple(
            {
                "symbol": key.symbol,
                "position_side": key.position_side.value,
                "attempts": record.attempts,
                "last_failure": record.last_failure,
                "next_attempt_in_seconds": max(
                    0.0, record.next_attempt_at - now
                ),
            }
            for key, record in sorted(
                self._attempts.items(), key=lambda item: item[0].canonical_id
            )
        )

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
        record = self._attempts.get(key)
        if record is None or record.evidence != evidence:
            return True
        return now >= record.next_attempt_at

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

    def _log_deferral(self, key: PositionKey) -> None:
        """Report one deferral per unchanged divergence, then stay quiet."""
        record = self._attempts.get(key)
        if record is None or record.deferral_logged:
            return
        record.deferral_logged = True
        log.info(
            "position_repair_deferred_unchanged_evidence",
            account_label=self._account,
            symbol=key.symbol,
            attempts=record.attempts,
            last_failure=record.last_failure,
            retry_in_seconds=round(
                max(0.0, record.next_attempt_at - self._clock()), 3
            ),
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
            try:
                # Record before I/O: a blocked or failing attempt must not be
                # retried until its deadline, and never twice concurrently.
                previous_attempts = (
                    self._attempts[key].attempts if key in self._attempts else 0
                )
                record = _RepairAttempt(
                    evidence=evidence,
                    attempts=previous_attempts + 1,
                    last_failure=None,
                    next_attempt_at=now + self._retry_interval_seconds,
                )
                self._attempts[key] = record
                request = PositionRepairRequest(
                    key=key,
                    run_id=self._run_id,
                    scope=AccountFactStreamScope.for_position_key(
                        key, stream_id=stream[0], stream_epoch=stream[1]
                    ),
                    expected_quantity=abs(position.position_amt),
                    observed_at=position.observed_at,
                )
                failure: list[str] = []

                def _blocked(reason: str, _sink: list[str] = failure) -> None:
                    _sink.append(reason)

                healed = await auto_heal_unmanaged_position(
                    request=request,
                    uow=self._uow,
                    book=self._book,
                    is_current=partial(
                        self._repair_is_current,
                        request,
                        position.entry_price,
                    ),
                    on_blocked=_blocked,
                )
            except Exception as error:
                log.exception(
                    "auto_heal_unmanaged_position_failed",
                    account_label=self._account,
                    symbol=position.symbol,
                    error_type=type(error).__name__,
                )
                if key in self._attempts:
                    self._attempts[key].last_failure = (
                        f"error:{type(error).__name__}"
                    )
                remaining = True
                continue
            if healed:
                repaired = True
            else:
                if key in self._attempts:
                    self._attempts[key].last_failure = (
                        failure[0] if failure else "repair_not_published"
                    )
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
        for key in tuple(self._attempts):
            if key not in divergent:
                self._attempts.pop(key, None)
