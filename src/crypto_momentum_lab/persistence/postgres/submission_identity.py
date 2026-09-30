"""Durable order identity comparison shared by submission and adoption."""

from collections.abc import Mapping

from crypto_momentum_lab.persistence.postgres.models import ExchangeOrderRow


def _same_order_identity(
    existing_order: ExchangeOrderRow,
    expected_values: Mapping[str, object],
) -> bool:
    """Keep idempotency scoped to the exact durable order identity."""

    return all(
        getattr(existing_order, field_name) == expected_values[field_name]
        for field_name in (
            "intent_id",
            "run_id",
            "symbol",
            "side",
            "order_type",
            "quantity",
            "price",
            "time_in_force",
            "expires_at",
            "reduce_only",
            "position_side",
            "created_at",
        )
    )
