from pathlib import Path

from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    field_validator,
    model_validator,
)


class UniverseConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    top_count: int = Field(gt=0)
    retention_rank: int = Field(gt=0)
    ranking_depth: int = Field(default=30, gt=0)
    extended_gainer_count: int = Field(default=0, ge=0)
    retention_hours: int = Field(gt=0)
    activation_minute: int = Field(ge=0, le=59)
    refresh_interval_minutes: int = Field(default=60, gt=0, le=60)

    @model_validator(mode="after")
    def validate_retention_rank(self) -> "UniverseConfig":
        if self.retention_rank < self.top_count:
            raise ValueError("retention_rank must be >= top_count")
        if self.ranking_depth < max(
            self.top_count,
            self.retention_rank,
            self.extended_gainer_count,
        ):
            raise ValueError(
                "ranking_depth must be >= top_count, retention_rank, "
                "and extended_gainer_count"
            )
        return self


class ArchiveConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    root: Path
    streams: tuple[str, ...] | None = None
    zstd_level: int = Field(ge=1, le=19)
    rotation_uncompressed_bytes: int = Field(gt=0)
    max_open_writers: int = Field(gt=0)
    group_commit_max_events: int = Field(gt=0)
    group_commit_max_milliseconds: int = Field(gt=0)
    warning_free_bytes: int = Field(gt=0)
    halt_free_bytes: int = Field(gt=0)
    recovery_free_bytes: int = Field(gt=0)
    disk_check_interval_seconds: float = Field(gt=0)
    pending_manifest_max_age_seconds: float = Field(gt=0)
    retention_days: int = Field(default=7, gt=0)
    retention_check_interval_seconds: float = Field(default=3600, gt=0)

    @model_validator(mode="after")
    def validate_disk_thresholds(self) -> "ArchiveConfig":
        if self.warning_free_bytes <= self.halt_free_bytes:
            raise ValueError(
                "warning_free_bytes must be greater than halt_free_bytes"
            )
        if self.recovery_free_bytes <= self.halt_free_bytes:
            raise ValueError(
                "recovery_free_bytes must be greater than halt_free_bytes"
            )
        return self


class CaptureConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    market_websocket_url: AnyUrl
    public_websocket_url: AnyUrl
    enabled_streams: tuple[str, ...]
    max_subscriptions_per_connection: int = Field(gt=0, le=100)
    ingress_queue_max_events: int = Field(default=4096, gt=0, le=100000)
    book_ticker_max_subscriptions_per_connection: int | None = Field(
        default=None,
        gt=0,
        le=100,
    )
    book_ticker_use_all_stream: bool = False
    book_ticker_coalescing_interval_seconds: float = Field(
        default=1.0,
        gt=0,
        le=15,
    )
    control_messages_per_second: float = Field(gt=0, le=5)
    control_ack_timeout_seconds: float = Field(default=10.0, gt=0)
    connection_lifetime_seconds: float = Field(gt=0, lt=86400)
    open_timeout_seconds: float = Field(gt=0)
    ping_interval_seconds: float = Field(gt=0)
    ping_timeout_seconds: float = Field(gt=0)
    silence_timeout_seconds: float = Field(gt=0)
    # Realtime consumers and the durable audit path intentionally use
    # different lateness budgets.
    realtime_closure_delay_seconds: float = Field(
        default=1.0,
        gt=0,
        le=30,
    )
    durable_closure_delay_seconds: float = Field(
        default=3.0,
        gt=0,
        le=30,
    )
    queue_max_events: int = Field(gt=0)
    queue_max_bytes: int = Field(gt=0)
    shutdown_timeout_seconds: float = Field(gt=0)
    archive: ArchiveConfig

    @field_validator("market_websocket_url", "public_websocket_url")
    @classmethod
    def validate_websocket_url(cls, value: AnyUrl) -> AnyUrl:
        if value.scheme not in {"ws", "wss"}:
            raise ValueError("websocket URLs must use ws or wss")
        return value

    @model_validator(mode="before")
    @classmethod
    def normalize_legacy_closure_delay(cls, value: object) -> object:
        if not isinstance(value, dict) or "closure_delay_seconds" not in value:
            return value
        normalized = dict(value)
        legacy_delay = normalized.pop("closure_delay_seconds")
        normalized.setdefault("realtime_closure_delay_seconds", legacy_delay)
        normalized.setdefault("durable_closure_delay_seconds", legacy_delay)
        return normalized

    @property
    def closure_delay_seconds(self) -> float:
        """Compatibility alias for the effective realtime delay."""
        return self.realtime_closure_delay_seconds

    @model_validator(mode="after")
    def validate_archive_streams(self) -> "CaptureConfig":
        if (
            self.durable_closure_delay_seconds
            < self.realtime_closure_delay_seconds
        ):
            raise ValueError(
                "durable_closure_delay_seconds must be >= "
                "realtime_closure_delay_seconds"
            )
        if self.archive.streams is None:
            return self
        unknown = set(self.archive.streams) - set(self.enabled_streams)
        if unknown:
            raise ValueError(
                "archive streams must be enabled capture streams: "
                + ", ".join(sorted(unknown))
            )
        return self


class EnvironmentFile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    environment: str
    binance_base_url: HttpUrl
    universe_config: Path
    capture_config: Path


class RuntimeConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    environment: str
    database_url: str
    binance_base_url: HttpUrl
    universe: UniverseConfig
    capture: CaptureConfig
