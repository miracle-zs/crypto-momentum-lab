"""Compatibility behavior for account clients used by synchronization."""

from inspect import signature

from crypto_momentum_lab.domain.account.models import AccountPositionSnapshot
from crypto_momentum_lab.execution_account.sync_ports import ReadOnlyAccountClient


async def fetch_positions_for_reconciliation(
    client: ReadOnlyAccountClient,
) -> tuple[AccountPositionSnapshot, ...]:
    fetch_positions = client.fetch_positions
    try:
        supports_explicit_flat_rows = (
            "include_flat" in signature(fetch_positions).parameters
        )
    except (TypeError, ValueError):
        supports_explicit_flat_rows = False
    if supports_explicit_flat_rows:
        return await fetch_positions(include_flat=True)
    # Adapters without V2's explicit flat rows remain usable for paper/tests,
    # but cannot establish a live zero-position anchor.
    return await fetch_positions()
