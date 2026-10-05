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
        arg.name for arg in UniverseEntryRow.__table_args__ if isinstance(arg, sa.Index)
    }
    assert "ix_universe_entries_price_time" in ue_indexes

    # 3. ix_exchange_orders_unresolved_partial must be present (F12)
    eo_indexes = {
        arg.name for arg in ExchangeOrderRow.__table_args__ if isinstance(arg, sa.Index)
    }
    assert "ix_exchange_orders_unresolved_partial" in eo_indexes
