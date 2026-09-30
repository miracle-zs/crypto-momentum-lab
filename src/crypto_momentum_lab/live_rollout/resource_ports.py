"""Cleanup capabilities consumed by the live resource lifecycle owner."""

from typing import Protocol


class AsyncStoppable(Protocol):
    async def stop(self) -> None: ...


class AsyncClosable(Protocol):
    async def aclose(self) -> None: ...


class SyncClosable(Protocol):
    def close(self) -> None: ...


class AsyncDisposable(Protocol):
    async def dispose(self) -> None: ...


class HealthStopMarker(Protocol):
    def stopped(self) -> None: ...
