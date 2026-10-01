"""Durable WS receipts: local journal order is not an exchange-wide sequence."""

from dataclasses import dataclass

from crypto_momentum_lab.execution_account.binance.user_data_models import (
    BinanceUserDataEvent,
)


@dataclass(frozen=True, slots=True)
class AccountEventJournalEntry:
    sequence: int
    environment: str
    account_label: str
    receiver_session_id: str
    stream_token: int | None
    event: BinanceUserDataEvent
