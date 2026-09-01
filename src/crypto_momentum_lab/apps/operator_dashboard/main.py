import argparse
import os

import uvicorn

from crypto_momentum_lab.operator_dashboard.api import create_dashboard_app
from crypto_momentum_lab.operator_dashboard.queries import (
    parse_common_equity_start_at,
    parse_live_cash_flow_adjustments,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the operator dashboard")
    parser.add_argument("--database-url", default=os.environ.get("CML_DATABASE_URL"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--username",
        default=os.environ.get("CML_DASHBOARD_USERNAME"),
    )
    parser.add_argument(
        "--password",
        default=os.environ.get("CML_DASHBOARD_PASSWORD"),
    )
    args = parser.parse_args()
    if not args.database_url:
        parser.error("--database-url or CML_DATABASE_URL is required")
    uvicorn.run(
        create_dashboard_app(
            database_url=args.database_url,
            auth_username=args.username,
            auth_password=args.password,
            paper_run_ids=parse_paper_run_ids(),
            live_cash_flow_adjustments=parse_live_cash_flow_adjustments(),
            common_equity_start_at=parse_common_equity_start_at(),
        ),
        host=args.host,
        port=args.port,
    )


def parse_paper_run_ids(value: str | None = None) -> frozenset[str] | None:
    raw_value = (
        os.environ.get("CML_PAPER_ACCOUNT_RUN_IDS", "")
        if value is None
        else value
    )
    run_ids = frozenset(
        item.strip() for item in raw_value.split(",") if item.strip()
    )
    return run_ids or None


if __name__ == "__main__":
    main()
