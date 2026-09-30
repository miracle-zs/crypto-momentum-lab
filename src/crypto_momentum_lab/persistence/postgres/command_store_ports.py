"""Outbox writes performed within an execution-owned SQL session."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Protocol

from crypto_momentum_lab.domain.market.models import JsonValue

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class ExecutionCommandStore(Protocol):
    async def upsert_execution_command_in_session(
        self,
        session: AsyncSession,
        *,
        command_id: str,
        client_order_id: str | None,
        command: str,
        status: str,
        requested_at: datetime,
        details: dict[str, JsonValue],
    ) -> None: ...
