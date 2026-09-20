"""Domain authorization contracts and invariants for live rollout commands."""

from __future__ import annotations

from crypto_momentum_lab.domain.live_rollout.models import RollbackCommand

EMERGENCY_FLATTEN_CONFIRMATION = "EMERGENCY FLATTEN LIVE ACCOUNT"
CANCEL_ALL_OPEN_ENTRIES_CONFIRMATION = "CANCEL ALL OPEN LIVE ENTRIES"
RELEASE_LEASE_CONFIRMATION = "RELEASE LIVE TRADING LEASE"

EMERGENCY_FLATTEN_COMMAND = "emergency_flatten"
CANCEL_ALL_OPEN_ENTRIES_COMMAND = "cancel_all_open_entries"


def require_authorized_command(
    command: RollbackCommand | None,
    *,
    command_type: str,
    confirmation_text: str,
) -> RollbackCommand:
    """Validate that an operator command is authorized, unexpired, and executable."""
    if command is None:
        raise PermissionError("persisted operator command is required")
    if command.command_type != command_type:
        raise PermissionError("operator command type mismatch")
    if command.confirmation_text != confirmation_text:
        raise PermissionError("operator command confirmation mismatch")
    if command.status not in {"requested", "executing"}:
        raise PermissionError("operator command is not executable")
    return command


__all__ = [
    "CANCEL_ALL_OPEN_ENTRIES_COMMAND",
    "CANCEL_ALL_OPEN_ENTRIES_CONFIRMATION",
    "EMERGENCY_FLATTEN_COMMAND",
    "EMERGENCY_FLATTEN_CONFIRMATION",
    "RELEASE_LEASE_CONFIRMATION",
    "require_authorized_command",
]
