from datetime import datetime, timedelta

from crypto_momentum_lab.domain.universe.models import (
    MembershipStatus,
    RankingResult,
    RankingSide,
    TrackedMembership,
)


def _target_side(result: RankingResult, symbol: str) -> RankingSide:
    gainer = next((entry for entry in result.gainers if entry.symbol == symbol), None)
    loser = next((entry for entry in result.losers if entry.symbol == symbol), None)
    if gainer is not None and loser is not None:
        return RankingSide.GAINER if gainer.utc_day_return >= 0 else RankingSide.LOSER
    if gainer is not None:
        return RankingSide.GAINER
    return RankingSide.LOSER


def _rank_for_side(
    result: RankingResult,
    symbol: str,
    side: RankingSide,
) -> int | None:
    entries = result.gainers if side is RankingSide.GAINER else result.losers
    entry = next((item for item in entries if item.symbol == symbol), None)
    return None if entry is None else entry.rank


def build_monitoring_memberships(
    result: RankingResult,
    *,
    previous: dict[str, TrackedMembership],
    forced_symbols: frozenset[str],
    observed_at: datetime,
    retention_rank: int,
    retention_duration: timedelta,
    extended_gainer_count: int = 0,
) -> dict[str, TrackedMembership]:
    if extended_gainer_count < 0:
        raise ValueError("extended_gainer_count must be non-negative")
    memberships: dict[str, TrackedMembership] = {}

    for symbol in sorted(result.target_symbols):
        memberships[symbol] = TrackedMembership(
            symbol=symbol,
            status=MembershipStatus.TARGET,
            side=_target_side(result, symbol),
            left_target_at=None,
        )

    for symbol, old in sorted(previous.items()):
        if symbol in memberships or old.side is None:
            continue
        left_target_at = old.left_target_at or observed_at
        rank = _rank_for_side(result, symbol, old.side)
        if (
            rank is not None
            and rank <= retention_rank
            and observed_at - left_target_at < retention_duration
        ):
            memberships[symbol] = TrackedMembership(
                symbol=symbol,
                status=MembershipStatus.RETAINED,
                side=old.side,
                left_target_at=left_target_at,
            )

    for entry in result.gainers:
        if (
            entry.rank <= extended_gainer_count
            and entry.utc_day_return > 0
            and entry.symbol not in memberships
        ):
            memberships[entry.symbol] = TrackedMembership(
                symbol=entry.symbol,
                status=MembershipStatus.EXTENDED,
                side=RankingSide.GAINER,
                left_target_at=None,
            )

    for symbol in sorted(forced_symbols):
        if symbol not in memberships:
            previous_membership = previous.get(symbol)
            memberships[symbol] = TrackedMembership(
                symbol=symbol,
                status=MembershipStatus.FORCED,
                side=(
                    None if previous_membership is None else previous_membership.side
                ),
                left_target_at=(
                    None
                    if previous_membership is None
                    else previous_membership.left_target_at
                ),
            )

    return memberships
