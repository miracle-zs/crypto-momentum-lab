"""Recover a live lease using durable session state and the unchanged live gate."""

from __future__ import annotations

from datetime import timedelta
from typing import Protocol
from uuid import uuid4

import structlog

import crypto_momentum_lab.live_rollout.session_state as session_state
from crypto_momentum_lab.domain.live_rollout import LiveSessionState
from crypto_momentum_lab.domain.risk import TradingLease, TradingLeaseState
from crypto_momentum_lab.live_rollout.gates import LiveGateContext, evaluate_live_gate

log = structlog.get_logger(__name__)


class LiveLeaseAcquirer(Protocol):
    async def acquire_lease(self, lease: TradingLease) -> None: ...


def should_auto_reacquire_live_lease(
    *,
    lease_present: bool,
    session_was_live_enabled: bool,
    draining: bool,
    gate_reasons: tuple[str, ...],
) -> bool:
    """Allow recovery only for an already-enabled, non-draining session."""

    return (
        not lease_present
        and session_was_live_enabled
        and not draining
        and gate_reasons == ("missing_active_lease",)
    )


async def maybe_auto_reacquire_live_lease(
    *,
    session_state_reader: session_state.LiveSessionStateReader,
    risk_repository: LiveLeaseAcquirer,
    gate_context: LiveGateContext,
    session_id: str,
    draining: bool,
    lease_ttl_seconds: int,
) -> TradingLease | None:
    """Recover one lost lease without bypassing the live gate."""

    gate = evaluate_live_gate(gate_context)
    if gate.approved or gate_context.active_lease is not None:
        return gate_context.active_lease
    if (
        await session_state_reader.load_latest_operating_state(session_id)
        != LiveSessionState.LIVE_ENABLED.value
    ):
        return None
    if not should_auto_reacquire_live_lease(
        lease_present=False,
        session_was_live_enabled=True,
        draining=draining,
        gate_reasons=gate.reasons,
    ):
        return None
    now = gate_context.now
    lease = TradingLease(
        lease_id=f"lease-{uuid4()}",
        environment="live",
        account_label=gate_context.account_label,
        strategy_name=gate_context.strategy_name,
        owner=gate_context.required_lease_owner,
        code_generation=gate_context.git_commit_hash,
        state=TradingLeaseState.ACTIVE,
        acquired_at=now,
        expires_at=now + timedelta(seconds=lease_ttl_seconds),
    )
    await risk_repository.acquire_lease(lease)
    log.info(
        "live_lease_auto_reacquired",
        account_label=lease.account_label,
        session_id=session_id,
        lease_id=lease.lease_id,
        lease_expires_at=lease.expires_at.isoformat(),
    )
    return lease
