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
        "execution_commands",
        "execution_reconciliation_events",
        "exchange_fills",
        "exchange_order_events",
        "exchange_orders",
        "market_data_process_states",
        "market_data_quality_events",
        "live_operator_approvals",
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
        "shadow_decision_metrics",
        "shadow_drill_results",
        "shadow_order_plans",
        "shadow_sessions",
        "shadow_suppression_events",
        "strategy_checkpoints",
        "strategy_runs",
        "strategy_signals",
        "strategy_live_states",
        "trading_leases",
        "universe_snapshots",
        "universe_entries",
        "monitoring_memberships",
    } <= table_names
