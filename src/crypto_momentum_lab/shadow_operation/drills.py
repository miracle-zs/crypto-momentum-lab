from collections.abc import Awaitable, Callable
from datetime import datetime
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.shadow_operation.models import ShadowDrillResult

SUPPORTED_SHADOW_DRILLS = (
    "market_data_reconnect",
    "account_stream_reconnect",
    "database_temporary_failure",
    "process_restart_with_active_lease",
    "strategy_halt",
    "stale_market_data",
    "risk_daily_loss_halt",
    "order_submission_ambiguity",
)


async def run_shadow_drill(
    *,
    run_id: str,
    drill_name: str,
    occurred_at: datetime,
    probe: Callable[[], Awaitable[None]],
) -> ShadowDrillResult:
    if drill_name not in SUPPORTED_SHADOW_DRILLS:
        raise ValueError(f"unsupported shadow drill: {drill_name}")
    details: dict[str, JsonValue]
    try:
        await probe()
    except Exception as exc:
        outcome = "failed"
        details = {"error": str(exc)}
    else:
        outcome = "passed"
        details = {}
    return ShadowDrillResult(
        drill_result_id=str(
            uuid5(
                NAMESPACE_URL,
                f"shadow-drill:{run_id}:{drill_name}:{occurred_at.isoformat()}",
            )
        ),
        run_id=run_id,
        drill_name=drill_name,
        outcome=outcome,
        occurred_at=occurred_at,
        details=details,
    )
