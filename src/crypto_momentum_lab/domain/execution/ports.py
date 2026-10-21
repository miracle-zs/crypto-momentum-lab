"""Shared conflict type used by durable decision commits."""


class DecisionCommitConflict(RuntimeError):
    """A durable decision commit conflicts with the current stored state."""
