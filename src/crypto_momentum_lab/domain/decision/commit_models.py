"""Storage-independent values for atomic policy decisions and exit recovery."""

from dataclasses import dataclass
from datetime import datetime

from crypto_momentum_lab.domain.decision.decision_engine import PolicyState
from crypto_momentum_lab.domain.execution.trade_command import TradeCommand
from crypto_momentum_lab.domain.market.revision_models import DecisionTrace
from crypto_momentum_lab.domain.operational.retention_models import ConsumerDependency


@dataclass(frozen=True, slots=True)
class DecisionCommit:
    """Complete, typed input for an atomic live decision commit."""

    trace: DecisionTrace
    policy_key: str
    expected_policy_revision: int
    expected_prior_digest: str
    prior_policy_state: PolicyState
    next_policy_state: PolicyState
    dependencies: tuple[ConsumerDependency, ...] = ()
    accepted_exit: TradeCommand | None = None

    def __post_init__(self) -> None:
        if not self.policy_key.strip():
            raise ValueError("policy_key must not be empty")
        if self.expected_policy_revision < 0:
            raise ValueError("expected_policy_revision must be non-negative")
        if not self.expected_prior_digest.strip():
            raise ValueError("expected_prior_digest must not be empty")
        if not self.trace.decision_id.strip():
            raise ValueError("decision trace id must not be empty")


@dataclass(frozen=True, slots=True)
class DecisionCommitReceipt:
    decision_id: str
    policy_key: str
    prior_state_digest: str
    next_state_digest: str
    policy_revision: int
    durable_at: datetime
    pending_exit_id: str | None = None
    is_replay: bool = False


@dataclass(frozen=True, slots=True)
class DurablePolicySnapshot:
    state: PolicyState
    state_digest: str
    revision: int
    last_decision_id: str
