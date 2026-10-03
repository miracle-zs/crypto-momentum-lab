"""Immutable execution observation outcomes, independent of Book."""

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum


@dataclass(frozen=True, slots=True)
class Applied:
    evidence_id: str
    updated_view_token: str
    consumed_quantity: Decimal = Decimal("0")
    released_quantity: Decimal = Decimal("0")
    recovery_required: bool = False
    diagnostics: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Duplicate:
    evidence_id: str
    view_token: str


@dataclass(frozen=True, slots=True)
class EvidenceConflict:
    evidence_id: str
    reason: str


class EvidencePendingReason(StrEnum):
    STREAM_RECOVERY_PROOF_REQUIRED = "stream_recovery_proof_required"
    PARENT_CHECKPOINT_UNAVAILABLE = "parent_checkpoint_unavailable"


@dataclass(frozen=True, slots=True)
class WaitingForEvidence:
    """Observation deferred without accepting its facts or declaring a conflict."""

    evidence_id: str
    reason: EvidencePendingReason


ExecutionObserveResult = Applied | Duplicate | WaitingForEvidence | EvidenceConflict
