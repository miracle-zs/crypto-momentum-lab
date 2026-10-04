from typing import Protocol

from crypto_momentum_lab.domain.market.models import (
    DurableArchiveAcknowledgement,
    RawEnvelope,
)


class RawArchive(Protocol):
    async def append(
        self,
        envelope: RawEnvelope,
    ) -> DurableArchiveAcknowledgement: ...

    async def close(self) -> None: ...
