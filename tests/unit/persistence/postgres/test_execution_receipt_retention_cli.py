from argparse import Namespace
from pathlib import Path
from unittest.mock import Mock

import pytest

from crypto_momentum_lab.tools import archive_execution_receipts


@pytest.mark.parametrize(
    "key,value",
    [
        ("minimum_age_hours", 0),
        ("batch_size", 0),
        ("batch_size", 1001),
        ("max_scopes", 0),
        ("max_scopes", 1001),
        ("max_runtime_seconds", 0),
    ],
)
async def test_invalid_retention_budget_does_not_connect_to_database(
    key, value, monkeypatch
):
    values = dict(
        minimum_age_hours=72,
        batch_size=500,
        max_scopes=50,
        max_runtime_seconds=45,
        archive_root=Path("unused"),
        apply=False,
    )
    values[key] = value
    engine = Mock()
    monkeypatch.setattr(
        archive_execution_receipts, "create_maintenance_database_engine", engine
    )
    with pytest.raises(ValueError):
        await archive_execution_receipts.run(Namespace(**values))
    engine.assert_not_called()
