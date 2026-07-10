from dataclasses import dataclass

from crypto_momentum_lab.domain.strategy import OrderIntentCandidate


@dataclass(frozen=True, slots=True)
class FlatReconciliationState:
    local_position_count: int
    exchange_position_count: int
    local_open_order_count: int
    exchange_open_order_count: int
    reconciliation_mismatch_count: int

    @property
    def confirmed_flat(self) -> bool:
        return all(
            value == 0
            for value in (
                self.local_position_count,
                self.exchange_position_count,
                self.local_open_order_count,
                self.exchange_open_order_count,
                self.reconciliation_mismatch_count,
            )
        )


def draining_allows_intent(intent: OrderIntentCandidate) -> bool:
    return intent.reduce_only


def require_flat_before_lease_release(state: FlatReconciliationState) -> None:
    if not state.confirmed_flat:
        raise RuntimeError(
            "cannot release trading lease before local and exchange state are flat"
        )
