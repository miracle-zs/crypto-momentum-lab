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
    retention_hours: int = Field(gt=0)
    activation_minute: int = Field(ge=0, le=59)

    @model_validator(mode="after")
    def validate_retention_rank(self) -> "UniverseConfig":
        if self.retention_rank < self.top_count:
            raise ValueError("retention_rank must be >= top_count")
        return self


class ArchiveConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    root: Path
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
    control_messages_per_second: float = Field(gt=0, le=5)
    connection_lifetime_seconds: float = Field(gt=0, lt=86400)
    open_timeout_seconds: float = Field(gt=0)
    ping_interval_seconds: float = Field(gt=0)
    ping_timeout_seconds: float = Field(gt=0)
    silence_timeout_seconds: float = Field(gt=0)
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
