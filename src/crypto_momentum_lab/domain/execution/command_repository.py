"""Awaited command persistence capabilities used by ExecutionBook."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Protocol

from crypto_momentum_lab.domain.market.models import JsonValue


class CommandRepository(Protocol):
    async def upsert_execution_command(
        self,
        *,
        command_id: str,
        client_order_id: str | None,
        command: str,
        status: str,
        requested_at: datetime,
        details: dict[str, JsonValue],
    ) -> None: ...
    async def load_active_execution_commands(
        self,
        *,
        account_label: str | None,
    ) -> Sequence[Mapping[str, object]]: ...
    async def load_seen_event_ids(self) -> Sequence[str]: ...
    async def load_seen_fill_trade_ids(self) -> Sequence[str]: ...
    async def load_execution_order_watermarks(
        self,
        *,
        account_label: str | None,
    ) -> Sequence[Mapping[str, object]]: ...
