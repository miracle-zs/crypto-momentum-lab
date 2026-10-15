from sqlalchemy import inspect, text

from crypto_momentum_lab.persistence.postgres.session import create_sync_engine


def test_initial_migration_creates_universe_tables(database_url: str) -> None:
    engine = create_sync_engine(database_url)
    try:
        table_names = set(inspect(engine).get_table_names())
        with engine.connect() as connection:
            revision = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
    finally:
        engine.dispose()

    assert {
        "account_balance_snapshots",
        "account_config_snapshots",
        "account_fill_events",
        "account_fill_reconciliation_cursors",
        "account_open_orders",
        "account_position_snapshots",
        "account_reconciliation_runs",
        "contract_metadata",
        "daily_open_prices",
        "execution_account_process_states",
        "execution_commands",
        "execution_reconciliation_events",
        "exchange_fills",
        "exchange_order_events",
        "exchange_orders",
        "exit_episode_reservations",
        "market_data_process_states",
        "market_data_quality_events",
        "live_operator_approvals",
        "live_exposure_claims",
        "live_rollback_commands",
        "live_strategy_signals",
        "live_session_transitions",
        "order_intent_candidates",
        "order_intent_claims",
        "order_intents",
        "paper_fills",
        "raw_archive_manifests",
        "risk_config_snapshots",
        "risk_evaluations",
        "risk_halts",
        "risk_rejections",
        "runtime_market_states_15s",
        "runtime_market_state_gaps",
        "strategy_checkpoints",
        "strategy_runs",
        "strategy_signals",
        "strategy_live_states",
        "universe_snapshots",
        "universe_entries",
        "monitoring_memberships",
    } <= table_names
    assert "trading_leases" not in table_names
    retired_tables = {
        "account_reconciliation_heads": "20261015_0063",
        "account_user_data_journal": "20261015_0064",
        "position_fact_journal_events": "20261015_0065",
        "position_recovery_checkpoints": "20261015_0065",
    }
    for table, drop_revision in retired_tables.items():
        assert (table not in table_names) == (revision >= drop_revision)

    columns = {column["name"] for column in inspect(engine).get_columns(
        "execution_trade_identities"
    )}
    indexes = {
        index["name"]
        for index in inspect(engine).get_indexes("execution_trade_identities")
    }
    if revision >= "20261015_0066":
        assert columns == {
            "environment",
            "account_label",
            "symbol",
            "position_side",
            "trade_id",
            "payload_digest",
        }
        assert "ix_execution_trade_identities_order" not in indexes
    else:
        assert {
            "order_id",
            "quantity",
            "price",
            "side",
            "first_seen_at",
        } <= columns
        assert "ix_execution_trade_identities_order" in indexes
