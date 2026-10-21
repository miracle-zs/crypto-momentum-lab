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
        "exchange_fills",
        "exchange_order_events",
        "exchange_orders",
        "market_data_process_states",
        "market_data_quality_events",
        "live_strategy_signals",
        "live_session_transitions",
        "order_intent_candidates",
        "order_intents",
        "paper_fills",
        "raw_archive_manifests",
        "risk_config_snapshots",
        "risk_halts",
        "runtime_market_states_15s",
        "runtime_market_state_gaps",
        "strategy_checkpoints",
        "strategy_runs",
        "strategy_signals",
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
        "execution_account_process_states": "20261021_0068",
        "execution_commands": "20261021_0068",
        "execution_reconciliation_events": "20261021_0068",
        "live_operator_approvals": "20261021_0068",
        "live_exposure_claims": "20261021_0068",
        "live_rollback_commands": "20261021_0068",
        "strategy_live_states": "20261021_0068",
        "exit_episode_reservations": "20261021_0068",
        "order_intent_claims": "20261021_0068",
        "risk_evaluations": "20261021_0068",
        "risk_rejections": "20261021_0068",
        "execution_evidence_receipts": "20261021_0068",
        "execution_retired_streams": "20261021_0068",
        "execution_order_watermarks": "20261021_0068",
        "execution_trade_identities": "20261021_0068",
        "durable_policy_states": "20261021_0068",
        "durable_policy_commits": "20261021_0068",
        "durable_decision_exits": "20261021_0068",
        "position_reservations": "20261021_0068",
        "execution_book_heads": "20261021_0068",
    }
    for table, drop_revision in retired_tables.items():
        assert (table not in table_names) == (revision >= drop_revision)

    if revision < "20261021_0068":
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

    if revision >= "20261018_0067":
        assert {"live_position_batches", "live_orders"} <= table_names

