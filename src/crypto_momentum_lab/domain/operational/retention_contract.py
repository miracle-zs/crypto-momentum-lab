"""Domain models and pure services for retention watermark safety gating."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass(frozen=True, slots=True)
class RetentionConsumerRequirement:
    """A requirement from an active consumer defining minimum data history needed."""

    consumer_id: str
    min_required_watermark: datetime
    reason: str

    def __post_init__(self) -> None:
        if not self.consumer_id.strip():
            raise ValueError("consumer_id must not be empty")
        if self.min_required_watermark.tzinfo is None:
            raise ValueError("min_required_watermark must be timezone-aware")


@dataclass(frozen=True, slots=True)
class RetentionGatingEvaluation:
    """Evaluation result establishing safe cutoff boundary for table pruning."""

    requested_cutoff: datetime
    effective_cutoff: datetime
    is_constrained: bool
    binding_constraint: RetentionConsumerRequirement | None
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if self.requested_cutoff.tzinfo is None:
            raise ValueError("requested_cutoff must be timezone-aware")
        if self.effective_cutoff.tzinfo is None:
            raise ValueError("effective_cutoff must be timezone-aware")
        if self.effective_cutoff > self.requested_cutoff:
            raise ValueError(
                f"effective_cutoff {self.effective_cutoff} must never be newer "
                f"than requested_cutoff {self.requested_cutoff}"
            )


class RetentionWatermarkEvaluator:
    """Pure domain service enforcing consumer safe watermarks before table deletion."""

    @classmethod
    def evaluate_cutoff(
        cls,
        *,
        requested_cutoff: datetime,
        requirements: tuple[RetentionConsumerRequirement, ...] = (),
        current_time: datetime | None = None,
    ) -> RetentionGatingEvaluation:
        """Calculate effective cutoff time bounded by active consumer requirements.

        Guarantees:
        - effective_cutoff <= requested_cutoff;
        - If any active consumer requires history prior to requested_cutoff,
          effective_cutoff is pulled back to that requirement's watermark;
        - Never silently deletes active facts needed for episode replay or open orders.
        """
        if requested_cutoff.tzinfo is None:
            raise ValueError("requested_cutoff must be timezone-aware")

        eval_time = current_time or datetime.now(UTC)
        if eval_time.tzinfo is None:
            raise ValueError("current_time must be timezone-aware")

        if not requirements:
            return RetentionGatingEvaluation(
                requested_cutoff=requested_cutoff,
                effective_cutoff=requested_cutoff,
                is_constrained=False,
                binding_constraint=None,
                evaluated_at=eval_time,
            )

        binding = min(requirements, key=lambda r: r.min_required_watermark)

        if binding.min_required_watermark < requested_cutoff:
            return RetentionGatingEvaluation(
                requested_cutoff=requested_cutoff,
                effective_cutoff=binding.min_required_watermark,
                is_constrained=True,
                binding_constraint=binding,
                evaluated_at=eval_time,
            )

        return RetentionGatingEvaluation(
            requested_cutoff=requested_cutoff,
            effective_cutoff=requested_cutoff,
            is_constrained=False,
            binding_constraint=None,
            evaluated_at=eval_time,
        )


__all__ = [
    "RetentionConsumerRequirement",
    "RetentionGatingEvaluation",
    "RetentionWatermarkEvaluator",
]
