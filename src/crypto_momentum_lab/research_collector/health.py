"""Small durable-progress probe, also executable directly with ``python -S``.

Only the collector measures capacity. Probes read its atomic snapshot and never
walk the research directory, import application dependencies, or query Postgres.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path


class CollectorHealthStore:
    def __init__(self, root: Path, environment: str) -> None:
        self.path = root / "health" / f"{environment}.json"

    def reset(self) -> None:
        self.path.unlink(missing_ok=True)

    def save(self, payload: dict[str, object]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(dir=self.path.parent, prefix=".health-")
        try:
            with os.fdopen(fd, "w") as stream:
                json.dump(payload, stream, sort_keys=True)
            os.replace(name, self.path)
        finally:
            Path(name).unlink(missing_ok=True)


def check_health(
    root: Path,
    environment: str,
    max_age_seconds: int = 120,
) -> tuple[bool, dict[str, object]]:
    try:
        payload = json.loads(CollectorHealthStore(root, environment).path.read_text())
        if not isinstance(payload, dict):
            raise ValueError("health snapshot must be an object")
        now = datetime.now(UTC)
        ages = [
            (now - datetime.fromisoformat(payload[field])).total_seconds()
            for field in ("updated_at", "capacity_updated_at")
        ]
        stale = any(
            age < 0 or (max_age_seconds > 0 and age > max_age_seconds) for age in ages
        )
        ready = (
            payload.get("environment") == environment
            and payload.get("ready") is True
            and payload.get("capacity_state") in ("healthy", "warning")
            and not stale
        )
        return ready, {**payload, "stale": stale}
    except (OSError, ValueError, TypeError, KeyError) as error:
        return False, {"ready": False, "error": str(error)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/app/research-data"))
    parser.add_argument("--environment", default="research")
    parser.add_argument("--max-age-seconds", type=int, default=120)
    args = parser.parse_args()
    if args.max_age_seconds < 0:
        parser.error("max-age-seconds must be nonnegative")
    ready, payload = check_health(args.root, args.environment, args.max_age_seconds)
    print(json.dumps(payload, sort_keys=True))
    return 0 if ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
