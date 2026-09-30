"""Shadow preflight warnings consume completed-session evidence only."""

from typing import Protocol

import structlog

log = structlog.get_logger(__name__)


class CompletedShadowSessionReader(Protocol):
    async def has_matching_completed_session(
        self, *, strategy_name: str, strategy_config_hash: str
    ) -> bool: ...


async def warn_if_shadow_preflight_missing(
    reader: CompletedShadowSessionReader,
    *,
    strategy_name: str,
    strategy_config_hash: str,
    account_label: str,
    session_id: str,
    acknowledged: bool = False,
) -> None:
    if await reader.has_matching_completed_session(
        strategy_name=strategy_name,
        strategy_config_hash=strategy_config_hash,
    ):
        return
    details = {
        "account_label": account_label,
        "session_id": session_id,
        "strategy_name": strategy_name,
        "strategy_config_hash": strategy_config_hash,
    }
    if acknowledged:
        log.info("live_shadow_preflight_missing_acknowledged", **details)
    else:
        log.warning("live_shadow_preflight_missing", **details)
