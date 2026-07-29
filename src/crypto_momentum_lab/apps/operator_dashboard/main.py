import argparse
import os

import uvicorn

from crypto_momentum_lab.operator_dashboard.api import create_dashboard_app


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
        ),
        host=args.host,
        port=args.port,
    )


if __name__ == "__main__":
    main()
