import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.config.models import CaptureConfig
from crypto_momentum_lab.domain.market.models import (
    CaptureStream,
    MarketDataState,
)
from crypto_momentum_lab.persistence.postgres.models import (
    MarketDataProcessStateRow,
    RawArchiveManifestRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)

pytestmark = pytest.mark.live


async def test_live_capture_manifests_are_durable() -> None:
    capture = _load_capture_config(_environment_config_path())
    database_url = _database_url()
    manifests, states = await _load_capture_state(database_url)

    assert manifests, "no raw archive manifests found"
    required_streams = {
        CaptureStream(item)
        for item in capture.enabled_streams
        if CaptureStream(item) is not CaptureStream.FORCE_ORDER
    }
    manifested_streams = {
        CaptureStream(row.stream) for row in manifests if row.row_count > 0
    }
    assert required_streams.issubset(manifested_streams)

    archive_root = capture.archive.root
    for manifest in manifests:
        path = archive_root / manifest.relative_path
        assert path.is_file(), f"missing archive file: {path}"
        assert manifest.row_count > 0
        assert manifest.compressed_bytes == path.stat().st_size
        assert manifest.sha256 == _sha256(path)

    observed_states = [MarketDataState(row.state) for row in states]
    assert observed_states, "no market data process state rows found"
    assert MarketDataState.HALTED not in observed_states
    latest = observed_states[-1]
    if latest is MarketDataState.STOPPED:
        assert any(
            state in {MarketDataState.READY, MarketDataState.DEGRADED}
            for state in observed_states
        )
    else:
        assert latest in {MarketDataState.READY, MarketDataState.DEGRADED}

    assert not tuple(archive_root.rglob("*.tmp"))
    assert _oldest_pending_manifest_seconds(archive_root) <= (
        capture.archive.pending_manifest_max_age_seconds
    )


def _environment_config_path() -> Path:
    return Path(
        os.environ.get(
            "CML_ENVIRONMENT_CONFIG",
            "configs/environments/research.yaml",
        )
    )


def _database_url() -> str:
    return os.environ.get(
        "CML_TEST_ASYNC_DATABASE_URL",
        os.environ.get("CML_DATABASE_URL", ""),
    )


def _load_capture_config(environment_path: Path) -> CaptureConfig:
    environment = _read_yaml(environment_path)
    capture_path = Path(str(environment["capture_config"]))
    return CaptureConfig.model_validate(_read_yaml(capture_path))


def _read_yaml(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return loaded


async def _load_capture_state(
    database_url: str,
) -> tuple[
    tuple[RawArchiveManifestRow, ...],
    tuple[MarketDataProcessStateRow, ...],
]:
    if not database_url:
        raise AssertionError(
            "set CML_TEST_ASYNC_DATABASE_URL or CML_DATABASE_URL for live smoke"
        )
    engine = create_async_database_engine(database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            manifests = (
                await session.execute(select(RawArchiveManifestRow))
            ).scalars()
            states = (
                await session.execute(
                    select(MarketDataProcessStateRow).order_by(
                        MarketDataProcessStateRow.occurred_at,
                        MarketDataProcessStateRow.state_id,
                    )
                )
            ).scalars()
            return tuple(manifests), tuple(states)
    finally:
        await engine.dispose()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _oldest_pending_manifest_seconds(archive_root: Path) -> float:
    pending_dir = archive_root / ".pending-manifests"
    if not pending_dir.exists():
        return 0.0
    entries = tuple(path for path in pending_dir.glob("*.json") if path.is_file())
    if not entries:
        return 0.0
    oldest = min(path.stat().st_mtime for path in entries)
    return max(0.0, datetime.now(UTC).timestamp() - oldest)
