"""Explicit compatibility adapter for legacy sync/async command repositories.

Runtime Postgres implements the awaited port directly. Legacy callers opt into
this adapter; ExecutionBook never inspects method availability or signatures.
"""

import inspect
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import cast

from crypto_momentum_lab.domain.market.models import JsonValue


class LegacyCommandRepositoryAdapter:
    def __init__(self, repository: object) -> None:
        self._repository = repository

    async def _call(
        self,
        name: str,
        *,
        account_label: str | None = None,
        scoped: bool = False,
        **kwargs: object,
    ) -> object:
        method = getattr(self._repository, name, None)
        if not callable(method):
            raise RuntimeError(f"legacy command repository does not implement {name}")
        if scoped and "account_label" in inspect.signature(method).parameters:
            kwargs["account_label"] = account_label
        result = method(**kwargs)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def upsert_execution_command(
        self,
        *,
        command_id: str,
        client_order_id: str | None,
        command: str,
        status: str,
        requested_at: datetime,
        details: dict[str, JsonValue],
    ) -> None:
        await self._call(
            "upsert_execution_command",
            command_id=command_id,
            client_order_id=client_order_id,
            command=command,
            status=status,
            requested_at=requested_at,
            details=details,
        )

    async def load_active_execution_commands(
        self,
        *,
        account_label: str | None,
    ) -> Sequence[Mapping[str, object]]:
        return cast(
            Sequence[Mapping[str, object]],
            await self._call(
                "load_active_execution_commands",
                account_label=account_label,
                scoped=True,
            ),
        )

    async def load_seen_event_ids(self) -> Sequence[str]:
        return cast(Sequence[str], await self._call("load_seen_event_ids"))

    async def load_seen_fill_trade_ids(self) -> Sequence[str]:
        return cast(Sequence[str], await self._call("load_seen_fill_trade_ids"))

    async def load_execution_order_watermarks(
        self,
        *,
        account_label: str | None,
    ) -> Sequence[Mapping[str, object]]:
        return cast(
            Sequence[Mapping[str, object]],
            await self._call(
                "load_execution_order_watermarks",
                account_label=account_label,
                scoped=True,
            ),
        )
