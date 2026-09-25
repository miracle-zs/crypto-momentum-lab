from unittest.mock import MagicMock
import pytest
import sqlalchemy as sa

from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeOrderRow,
    RuntimeMarketState15sRow,
    UniverseEntryRow,
)


def test_models_have_correct_indexes() -> None:
    # 1. ix_runtime_market_states_15s_symbol_time must NOT be present (F12)
    rms_indexes = {
        arg.name
        for arg in RuntimeMarketState15sRow.__table_args__
        if isinstance(arg, sa.Index)
    }
    assert "ix_runtime_market_states_15s_symbol_time" not in rms_indexes
    assert "ix_runtime_market_states_15s_polling" in rms_indexes

    # 2. ix_universe_entries_price_time must be present (F12)
    ue_indexes = {
        arg.name
        for arg in UniverseEntryRow.__table_args__
        if isinstance(arg, sa.Index)
    }
    assert "ix_universe_entries_price_time" in ue_indexes

    # 3. ix_exchange_orders_unresolved_partial must be present (F12)
    eo_indexes = {
        arg.name
        for arg in ExchangeOrderRow.__table_args__
        if isinstance(arg, sa.Index)
    }
    assert "ix_exchange_orders_unresolved_partial" in eo_indexes


def test_0037_downgrade_guards_against_partitioned_table(monkeypatch: pytest.MonkeyPatch) -> None:
    """F13: downgrade() in 0037 must raise RuntimeError when strategy_runtime_events is partitioned."""
    import importlib.util
    from pathlib import Path

    migration_path = (
        Path(__file__).resolve().parents[3]
        / "alembic"
        / "versions"
        / "20260918_0037_strategy_runtime_events_composite_pk.py"
    )
    spec = importlib.util.spec_from_file_location("migration_0037", migration_path)
    assert spec is not None and spec.loader is not None
    migration_0037 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration_0037)

    mock_bind = MagicMock()
    mock_bind.dialect.name = "postgresql"
    mock_bind.execute.return_value.scalar.return_value = "p"  # Partitioned table

    monkeypatch.setattr(migration_0037.op, "get_bind", lambda: mock_bind)

    with pytest.raises(
        RuntimeError,
        match="Cannot downgrade primary key on partitioned table 'strategy_runtime_events'",
    ):
        migration_0037.downgrade()
