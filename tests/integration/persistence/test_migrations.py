from sqlalchemy import inspect

from crypto_momentum_lab.persistence.postgres.session import create_sync_engine


def test_initial_migration_creates_universe_tables(database_url: str) -> None:
    engine = create_sync_engine(database_url)
    try:
        table_names = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()

    assert {
        "account_balance_snapshots",
        "account_config_snapshots",
        "account_fill_events",
        "account_open_orders",
        "account_position_snapshots",
        "account_reconciliation_runs",
        "contract_metadata",
        "daily_open_prices",
        "execution_account_process_states",
        "market_data_process_states",
        "market_data_quality_events",
        "order_intent_candidates",
        "paper_fills",
        "raw_archive_manifests",
        "runtime_market_states_15s",
        "strategy_checkpoints",
        "strategy_runs",
        "strategy_signals",
        "universe_snapshots",
        "universe_entries",
        "monitoring_memberships",
    } <= table_names
