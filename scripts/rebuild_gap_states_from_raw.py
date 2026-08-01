#!/usr/bin/env python3
"""Rebuild missing 15-second states from retained raw archive events."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL, make_url
from sqlalchemy.orm import Session

from crypto_momentum_lab.domain.market.models import NormalizedMarketEvent, RawEnvelope
from crypto_momentum_lab.market_data.aggregation.state_15s import (
    aggregate_market_states_15s,
)
from crypto_momentum_lab.market_data.normalization import normalize_binance_envelope
from crypto_momentum_lab.persistence.postgres.models import RuntimeMarketState15sRow
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    runtime_state_row,
)
from crypto_momentum_lab.persistence.raw_files.reader import iter_archive_file


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.environ.get("CML_DATABASE_URL"))
    parser.add_argument("--database-name", required=True)
    parser.add_argument("--expected-database", required=True)
    parser.add_argument("--raw-root", type=Path, default=Path("/app/data/raw"))
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("--database-url or CML_DATABASE_URL is required")
    start = _parse_timestamp(args.start)
    end = _parse_timestamp(args.end)
    if end <= start:
        raise SystemExit("--end must be after --start")

    url = make_url(args.database_url).set(database=args.database_name)
    engine = create_engine(_sync_url(url))
    try:
        with engine.connect() as connection:
            database = connection.exec_driver_sql(
                "SELECT current_database()"
            ).scalar_one()
        if database != args.expected_database:
            raise SystemExit(
                f"database guard failed: expected {args.expected_database!r}, "
                f"connected to {database!r}"
            )

        envelopes, raw_counts, path_count = _load_gap_envelopes(
            root=args.raw_root,
            start=start,
            end=end,
        )
        normalized = tuple(
            sorted(
                (normalize_binance_envelope(envelope) for envelope in envelopes),
                key=_event_sort_key,
            )
        )
        states = aggregate_market_states_15s(normalized)
        existing_count = _existing_state_count(
            engine,
            start=start,
            end=end,
        )
        report = {
            "database": database,
            "mode": "apply" if args.apply else "dry-run",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "archive_files": path_count,
            "unique_raw_events": dict(sorted(raw_counts.items())),
            "normalized_events": len(normalized),
            "rebuilt_states": len(states),
            "rebuilt_symbols": len({state.symbol for state in states}),
            "rebuilt_buckets": len({state.bucket_start for state in states}),
            "states_with_executable_quote": sum(
                state.last_bid_price is not None and state.last_ask_price is not None
                for state in states
            ),
            "existing_states_in_window": existing_count,
        }
        if args.apply:
            report["inserted_states"] = _insert_states(
                engine,
                states=states,
                envelopes=envelopes,
            )
        print(json.dumps(report, indent=2, sort_keys=True))
    finally:
        engine.dispose()


def _load_gap_envelopes(
    *,
    root: Path,
    start: datetime,
    end: datetime,
) -> tuple[tuple[RawEnvelope, ...], Counter[str], int]:
    date_roots = tuple(
        root / "exchange=binance-usdm" / f"date={day.isoformat()}"
        for day in {start.date(), (end - timedelta(microseconds=1)).date()}
    )
    paths = tuple(
        sorted(
            path
            for date_root in date_roots
            for path in date_root.glob("stream=*/symbol=*/hour=*/*.jsonl.zst")
            if _hour_may_overlap(path, start=start, end=end)
        )
    )
    unique: dict[tuple[str, str, str], RawEnvelope] = {}
    counts: Counter[str] = Counter()
    for path in paths:
        for envelope in iter_archive_file(path):
            event_at = envelope.exchange_event_at or envelope.received_at
            if not start <= event_at < end:
                continue
            key = _event_key(envelope)
            if key in unique:
                continue
            unique[key] = envelope
            counts[envelope.stream.value] += 1
    envelopes = tuple(
        sorted(
            unique.values(),
            key=lambda item: (
                item.exchange_event_at or item.received_at,
                item.received_at,
                item.local_sequence,
            ),
        )
    )
    return envelopes, counts, len(paths)


def _hour_may_overlap(path: Path, *, start: datetime, end: datetime) -> bool:
    hour_component = next(
        (part for part in path.parts if part.startswith("hour=")),
        None,
    )
    date_component = next(
        (part for part in path.parts if part.startswith("date=")),
        None,
    )
    if hour_component is None or date_component is None:
        return False
    hour_start = datetime.fromisoformat(
        f"{date_component.removeprefix('date=')}T"
        f"{hour_component.removeprefix('hour=')}:00:00+00:00"
    )
    return hour_start < end and hour_start + timedelta(hours=1) > start


def _event_key(envelope: RawEnvelope) -> tuple[str, str, str]:
    sequence = envelope.exchange_sequence
    if sequence is None:
        encoded = json.dumps(
            envelope.raw_payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        sequence = hashlib.sha256(encoded).hexdigest()
    return envelope.stream.value, envelope.symbol or "", sequence


def _event_sort_key(event: NormalizedMarketEvent) -> tuple[datetime, datetime, int]:
    return event.event_at, event.received_at, event.source_local_sequence


def _existing_state_count(
    engine: object,
    *,
    start: datetime,
    end: datetime,
) -> int:
    with Session(engine) as session:
        return session.scalar(
            select(func.count()).select_from(RuntimeMarketState15sRow).where(
                RuntimeMarketState15sRow.environment == "research",
                RuntimeMarketState15sRow.bucket_start >= start,
                RuntimeMarketState15sRow.bucket_start < end,
            )
        ) or 0


def _insert_states(
    engine: object,
    *,
    states: tuple[object, ...],
    envelopes: tuple[RawEnvelope, ...],
) -> int:
    sequences: dict[tuple[str, datetime], list[int]] = {}
    for envelope in envelopes:
        event_at = envelope.exchange_event_at or envelope.received_at
        bucket_start = event_at.astimezone(UTC).replace(
            second=(event_at.second // 15) * 15,
            microsecond=0,
        )
        sequences.setdefault((envelope.symbol or "", bucket_start), []).append(
            envelope.local_sequence
        )
    rows = []
    for state in states:
        sequence_values = sequences.get((state.symbol, state.bucket_start), [])
        rows.append(
            runtime_state_row(
                state,
                source_watermark_at=state.bucket_end + timedelta(seconds=3),
                input_sequence_min=min(sequence_values, default=None),
                input_sequence_max=max(sequence_values, default=None),
                closure_reason="raw_archive_backfill",
            )
        )
    inserted = 0
    with engine.begin() as connection:
        for offset in range(0, len(rows), 500):
            statement = (
                insert(RuntimeMarketState15sRow)
                .on_conflict_do_nothing(
                    index_elements=["environment", "symbol", "bucket_start"]
                )
                .returning(RuntimeMarketState15sRow.environment)
            )
            inserted += len(
                connection.execute(statement, rows[offset : offset + 500]).all()
            )
    return inserted


def _sync_url(value: str | URL) -> URL:
    url = make_url(value)
    if not url.drivername.startswith("postgresql"):
        raise SystemExit("database URL must use PostgreSQL")
    return url.set(drivername="postgresql+psycopg")


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SystemExit("timestamp must include a UTC offset")
    return parsed.astimezone(UTC)


if __name__ == "__main__":
    main()
