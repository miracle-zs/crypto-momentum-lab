from dataclasses import replace

import pytest

from crypto_momentum_lab.execution_account.baseline_checkpoint import (
    AccountBaselineCheckpoint,
    decode_baseline_checkpoint,
    encode_baseline_checkpoint,
)
from crypto_momentum_lab.execution_account.snapshot_models import AccountSnapshot
from tests.unit.execution_account.test_sync import FakeClient


async def test_checkpoint_preserves_complete_snapshot_and_rejects_unknown_versions():
    client = FakeClient()
    snapshot = AccountSnapshot(
        config=client.config,
        balances=await client.fetch_balances(),
        positions=(),
        open_orders=(),
    )
    checkpoint = AccountBaselineCheckpoint(
        1, "baseline-1", 17, "receiver-a", 2, snapshot
    )
    payload = encode_baseline_checkpoint(checkpoint)
    decoded = decode_baseline_checkpoint(payload)
    assert decoded == checkpoint
    assert [row.asset for row in decoded.snapshot.balances] == ["USDT", "BNB"]
    payload["schema_version"] = 2
    with pytest.raises(ValueError):
        decode_baseline_checkpoint(payload)
    payload["schema_version"] = 1
    payload["journal_sequence"] = True
    with pytest.raises(ValueError, match="evidence types"):
        decode_baseline_checkpoint(payload)
    with pytest.raises(ValueError, match="crosses account scope"):
        replace(
            checkpoint,
            snapshot=replace(
                snapshot,
                balances=(replace(snapshot.balances[0], account_label="other"),),
            ),
        )
    with pytest.raises(ValueError, match="cursor"):
        replace(checkpoint, journal_sequence=-1)
