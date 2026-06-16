from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Protocol

from crypto_momentum_lab.domain.market.models import (
    ArchiveManifest,
    DurableArchiveAcknowledgement,
    MarketDataState,
    QualityEvent,
    RawEnvelope,
)


class RawArchive(Protocol):
    async def append(
        self,
        envelope: RawEnvelope,
    ) -> DurableArchiveAcknowledgement: ...

    async def close(self) -> None: ...


class CaptureRepository(Protocol):
    async def save_manifest(self, manifest: ArchiveManifest) -> None: ...

    async def save_quality_event(self, event: QualityEvent) -> None: ...

    async def save_process_state(
        self,
        *,
        state: MarketDataState,
        occurred_at: datetime,
        reason: str | None,
    ) -> None: ...


ArchiveAcknowledgementSink = Callable[
    [DurableArchiveAcknowledgement],
    Awaitable[None],
]
ArchiveManifestSink = Callable[[ArchiveManifest], Awaitable[None]]
