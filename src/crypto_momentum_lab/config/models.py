from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


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


class EnvironmentFile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    environment: str
    binance_base_url: HttpUrl
    universe_config: Path


class RuntimeConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    environment: str
    database_url: str
    binance_base_url: HttpUrl
    universe: UniverseConfig
