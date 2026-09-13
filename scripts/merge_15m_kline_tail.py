"""Merge a short 15m kline tail into a replay-compatible base archive."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
from datetime import UTC, datetime
from pathlib import Path


def read_rows(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        return {
            (row["symbol"], row["candle_start"]): row
            for row in csv.DictReader(handle)
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--tail", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    base = read_rows(args.base)
    tail = read_rows(args.tail)
    overlap = set(base) & set(tail)
    replaced = sum(base[key] != tail[key] for key in overlap)
    merged = dict(base)
    merged.update(tail)
    fieldnames = [
        "symbol",
        "candle_start",
        "candle_end",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "close_ms",
    ]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".part")
    with gzip.open(temporary, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(merged[key] for key in sorted(merged))
    temporary.replace(args.output)
    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "base": str(args.base),
        "tail": str(args.tail),
        "base_rows": len(base),
        "tail_rows": len(tail),
        "overlap_rows": len(overlap),
        "replaced_rows": replaced,
        "merged_rows": len(merged),
    }
    args.output.with_suffix(args.output.suffix + ".manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(manifest, ensure_ascii=False))


if __name__ == "__main__":
    main()
