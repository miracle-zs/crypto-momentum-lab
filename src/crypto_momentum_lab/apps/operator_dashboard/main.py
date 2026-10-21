import argparse
import os
from pathlib import Path

import uvicorn

from crypto_momentum_lab.config import resolve_database_url
from crypto_momentum_lab.operator_dashboard.api import create_dashboard_app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the operator dashboard")
    parser.add_argument("--database-url")
    parser.add_argument(
        "--runtime-manifest", type=Path, default=Path("/app/deploy/live-runtime.yaml")
    )
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
    database_url = resolve_database_url(args.database_url, "CML_DATABASE_URL")
    if not database_url:
        parser.error("--database-url or CML_DATABASE_URL is required")
    uvicorn.run(
        create_dashboard_app(
            database_url=database_url,
            runtime_manifest=args.runtime_manifest,
            auth_username=args.username,
            auth_password=args.password,
            research_collector_root=Path(
                os.environ.get("CML_RESEARCH_COLLECTOR_ROOT", "/app/research-data")
            ),
        ),
        host=args.host,
        port=args.port,
    )
if __name__ == "__main__":
    main()
