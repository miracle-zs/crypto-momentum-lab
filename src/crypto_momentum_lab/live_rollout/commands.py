from crypto_momentum_lab.domain.live_rollout import RollbackCommand

CANCEL_ALL_CONFIRMATION = "CANCEL ALL OPEN ORDERS"
EMERGENCY_FLATTEN_CONFIRMATION = "EMERGENCY FLATTEN LIVE ACCOUNT"
RELEASE_LEASE_CONFIRMATION = "RELEASE LIVE TRADING LEASE"


def require_authorized_command(
    command: RollbackCommand | None,
    *,
    command_type: str,
    confirmation_text: str,
) -> RollbackCommand:
    if command is None:
        raise PermissionError("persisted operator command is required")
    if command.command_type != command_type:
        raise PermissionError("operator command type mismatch")
    if command.confirmation_text != confirmation_text:
        raise PermissionError("operator command confirmation mismatch")
    if command.status not in {"requested", "executing"}:
        raise PermissionError("operator command is not executable")
    return command
