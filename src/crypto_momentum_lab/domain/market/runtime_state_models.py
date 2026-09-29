"""Contracts shared by market-state producers and persistence adapters."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RuntimeStateSequenceRange:
    minimum: int | None = None
    maximum: int | None = None
