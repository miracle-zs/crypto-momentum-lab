"""Domain models for operational runtime configuration snapshots and verification."""

from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


def compute_content_hash(content: str | bytes | dict[str, Any] | list[Any]) -> str:
    """Compute deterministic SHA-256 hex digest for arbitrary configuration content."""
    if isinstance(content, (dict, list)):
        encoded = json.dumps(content, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    elif isinstance(content, str):
        encoded = content.encode("utf-8")
    elif isinstance(content, bytes):
        encoded = content
    else:
        raise TypeError(f"Unsupported content type for hashing: {type(content)!r}")
    return hashlib.sha256(encoded).hexdigest()


def compute_trading_rules_hash(rules: Any) -> str:
    """Compute deterministic SHA-256 hash for symbol trading rules."""
    if not isinstance(rules, dict):
        return compute_content_hash(str(rules))
    serialized: dict[str, Any] = {}
    for symbol, rule in sorted(rules.items()):
        if hasattr(rule, "tick_size"):
            serialized[str(symbol)] = {
                "tick_size": str(rule.tick_size),
                "step_size": str(rule.step_size),
                "min_quantity": str(rule.min_quantity),
                "max_quantity": str(rule.max_quantity),
                "min_notional": str(rule.min_notional),
            }
        elif isinstance(rule, dict):
            serialized[str(symbol)] = {k: str(v) for k, v in sorted(rule.items())}
        else:
            serialized[str(symbol)] = str(rule)
    return compute_content_hash(serialized)


@dataclass(frozen=True, slots=True)
class RuntimeMetadataSnapshot:
    """
    Immutable snapshot of effective operational runtime metadata and configuration
    hashes.
    """

    environment: str
    account_label: str
    git_commit: str
    code_generation: str
    python_version: str
    strategy_config_hash: str
    risk_config_hash: str
    trading_rules_hash: str
    started_at: datetime

    def __post_init__(self) -> None:
        if not self.environment.strip():
            raise ValueError("environment must not be empty")
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
        if not self.git_commit.strip():
            raise ValueError("git_commit must not be empty")
        if not self.code_generation.strip():
            raise ValueError("code_generation must not be empty")
        if not self.python_version.strip():
            raise ValueError("python_version must not be empty")
        if not self.strategy_config_hash.strip():
            raise ValueError("strategy_config_hash must not be empty")
        if not self.risk_config_hash.strip():
            raise ValueError("risk_config_hash must not be empty")
        if not self.trading_rules_hash.strip():
            raise ValueError("trading_rules_hash must not be empty")
        if self.started_at.tzinfo is None:
            raise ValueError("started_at must be timezone-aware")

    def to_dict(self) -> dict[str, str]:
        """Serialize metadata snapshot to a deterministic string dictionary."""
        return {
            "environment": self.environment,
            "account_label": self.account_label,
            "git_commit": self.git_commit,
            "code_generation": self.code_generation,
            "python_version": self.python_version,
            "strategy_config_hash": self.strategy_config_hash,
            "risk_config_hash": self.risk_config_hash,
            "trading_rules_hash": self.trading_rules_hash,
            "started_at": self.started_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RuntimeMetadataSnapshot:
        """Construct a snapshot from a serialized dictionary."""
        started_at = data["started_at"]
        if isinstance(started_at, str):
            started_at = datetime.fromisoformat(started_at)
        return cls(
            environment=str(data["environment"]),
            account_label=str(data["account_label"]),
            git_commit=str(data["git_commit"]),
            code_generation=str(data["code_generation"]),
            python_version=str(data["python_version"]),
            strategy_config_hash=str(data["strategy_config_hash"]),
            risk_config_hash=str(data["risk_config_hash"]),
            trading_rules_hash=str(data["trading_rules_hash"]),
            started_at=started_at,
        )

    @classmethod
    def create(
        cls,
        *,
        environment: str,
        account_label: str,
        git_commit: str,
        code_generation: str | None = None,
        python_version: str | None = None,
        strategy_config: str | bytes | dict[str, Any] | None = None,
        risk_config: str | bytes | dict[str, Any] | None = None,
        trading_rules: str | bytes | dict[str, Any] | None = None,
        strategy_config_hash: str | None = None,
        risk_config_hash: str | None = None,
        trading_rules_hash: str | None = None,
        started_at: datetime | None = None,
    ) -> RuntimeMetadataSnapshot:
        """
        Factory method to construct snapshot, computing hashes if configs are
        provided.
        """
        if python_version is None:
            python_version = sys.version.split()[0]
        if code_generation is None:
            code_generation = git_commit

        if strategy_config_hash is None:
            if strategy_config is None:
                raise ValueError(
                    "Either strategy_config or strategy_config_hash must be provided"
                )
            strategy_config_hash = compute_content_hash(strategy_config)

        if risk_config_hash is None:
            if risk_config is None:
                raise ValueError(
                    "Either risk_config or risk_config_hash must be provided"
                )
            risk_config_hash = compute_content_hash(risk_config)

        if trading_rules_hash is None:
            if trading_rules is None:
                raise ValueError(
                    "Either trading_rules or trading_rules_hash must be provided"
                )
            trading_rules_hash = compute_content_hash(trading_rules)

        if started_at is None:
            started_at = datetime.now(UTC)

        return cls(
            environment=environment,
            account_label=account_label,
            git_commit=git_commit,
            code_generation=code_generation,
            python_version=python_version,
            strategy_config_hash=strategy_config_hash,
            risk_config_hash=risk_config_hash,
            trading_rules_hash=trading_rules_hash,
            started_at=started_at,
        )
