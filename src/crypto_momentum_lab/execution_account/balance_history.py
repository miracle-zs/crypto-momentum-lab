"""Sparse balance history selection without observation cache ownership."""

from collections.abc import Mapping
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import AccountBalanceSnapshot

type BalanceValue = tuple[Decimal, Decimal, Decimal]


def balance_value(balance: AccountBalanceSnapshot) -> BalanceValue:
    return (
        balance.wallet_balance,
        balance.available_balance,
        balance.unrealized_pnl,
    )


def _balance_has_value(balance: AccountBalanceSnapshot) -> bool:
    return _balance_value_is_nonzero(balance_value(balance))


def _balance_value_is_nonzero(value: BalanceValue | None) -> bool:
    return value is not None and any(item != 0 for item in value)


def select_balance_history(
    balances: tuple[AccountBalanceSnapshot, ...],
    previous: Mapping[str, BalanceValue],
) -> tuple[AccountBalanceSnapshot, ...]:
    """Keep nonzero values and the zero transition after a nonzero observation."""
    return tuple(
        balance
        for balance in balances
        if _balance_has_value(balance)
        or _balance_value_is_nonzero(previous.get(balance.asset))
    )
