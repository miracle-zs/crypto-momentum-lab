"""A persisted baseline plus local journal prefix, never a WS reconnect proof."""

from dataclasses import dataclass
from typing import Literal, cast

from pydantic import TypeAdapter

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.execution_account.snapshot_models import AccountSnapshot


@dataclass(frozen=True, slots=True)
class AccountBaselineCheckpoint:
    schema_version: Literal[1]
    baseline_id: str
    journal_sequence: int
    receiver_session_id: str
    stream_token: int
    snapshot: AccountSnapshot

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise ValueError("unsupported checkpoint schema")
        if not self.baseline_id.strip() or not self.receiver_session_id.strip():
            raise ValueError("checkpoint identity must not be empty")
        if (
            type(self.journal_sequence) is not int
            or type(self.stream_token) is not int
            or self.journal_sequence < 0
            or self.stream_token < 1
        ):
            raise ValueError("invalid checkpoint journal or connection cursor")
        scope = (self.snapshot.config.environment, self.snapshot.config.account_label)
        scopes = [
            (row.environment, row.account_label) for row in self.snapshot.balances
        ]
        scopes.extend(
            (row.environment, row.account_label) for row in self.snapshot.positions
        )
        scopes.extend(
            (row.environment, row.account_label) for row in self.snapshot.open_orders
        )
        if any(row_scope != scope for row_scope in scopes):
            raise ValueError("checkpoint snapshot crosses account scope")


_checkpoint_codec = TypeAdapter(AccountBaselineCheckpoint)


def encode_baseline_checkpoint(
    checkpoint: AccountBaselineCheckpoint,
) -> dict[str, JsonValue]:
    return cast(
        dict[str, JsonValue], _checkpoint_codec.dump_python(checkpoint, mode="json")
    )


def decode_baseline_checkpoint(payload: object) -> AccountBaselineCheckpoint:
    if not isinstance(payload, dict) or any(
        type(payload.get(key)) is not int
        for key in ("schema_version", "journal_sequence", "stream_token")
    ):
        raise ValueError("invalid checkpoint evidence types")
    return _checkpoint_codec.validate_python(payload)
