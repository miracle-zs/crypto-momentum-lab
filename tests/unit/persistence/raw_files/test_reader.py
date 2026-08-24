import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import zstandard

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    RawEnvelope,
)
from crypto_momentum_lab.persistence.raw_files.archive import serialize_envelope
from crypto_momentum_lab.persistence.raw_files.reader import (
    deserialize_envelope_row,
    replay_envelopes,
)


def test_deserialize_envelope_row_round_trips_archive_json() -> None:
    row = json.dumps(
        {
            "schema_version": 1,
            "exchange": "binance-usdm",
            "environment": "research",
            "route": "market",
            "stream": "aggTrade",
            "symbol": "BTCUSDT",
            "exchange_event_at": "2026-06-15T02:00:00+00:00",
            "received_at": "2026-06-15T02:00:01+00:00",
            "received_monotonic_ns": 100,
            "connection_session_id": str(UUID(int=1)),
            "local_sequence": 7,
            "exchange_sequence": "42",
            "subscription_generation": 3,
            "raw_payload": {"e": "aggTrade", "s": "BTCUSDT", "a": 42},
        }
    )

    envelope = deserialize_envelope_row(row)

    assert envelope.schema_version == 1
    assert envelope.route is CaptureRoute.MARKET
    assert envelope.stream is CaptureStream.AGG_TRADE
    assert envelope.symbol == "BTCUSDT"
    assert envelope.connection_session_id == UUID(int=1)
    assert envelope.local_sequence == 7
    assert envelope.raw_payload == {"e": "aggTrade", "s": "BTCUSDT", "a": 42}


def test_replay_envelopes_sorts_by_local_receive_order(
    tmp_path: Path,
    raw_envelope: RawEnvelope,
) -> None:
    later = replace(
        raw_envelope,
        received_at=datetime(2026, 6, 15, 2, 0, 2, tzinfo=UTC),
        received_monotonic_ns=200,
        connection_session_id=UUID(int=2),
        local_sequence=1,
    )
    earlier = replace(
        raw_envelope,
        received_at=datetime(2026, 6, 15, 2, 0, 1, tzinfo=UTC),
        received_monotonic_ns=100,
        connection_session_id=UUID(int=1),
        local_sequence=9,
    )
    first_path = tmp_path / "first.jsonl.zst"
    second_path = tmp_path / "second.jsonl.zst"
    _write_zstd_rows(first_path, (serialize_envelope(later).decode(),))
    _write_zstd_rows(second_path, (serialize_envelope(earlier).decode(),))

    replayed = replay_envelopes((first_path, second_path))

    assert [item.connection_session_id for item in replayed] == [
        UUID(int=1),
        UUID(int=2),
    ]
    assert [item.local_sequence for item in replayed] == [9, 1]


def test_archive_round_trip_preserves_recovery_provenance(
    raw_envelope: RawEnvelope,
) -> None:
    recovered = replace(raw_envelope, recovered=True)

    decoded = deserialize_envelope_row(serialize_envelope(recovered).decode())

    assert decoded == recovered


def _write_zstd_rows(path: Path, rows: tuple[str, ...]) -> None:
    with zstandard.open(path, "wt", encoding="utf-8") as stream:
        for row in rows:
            stream.write(row)
