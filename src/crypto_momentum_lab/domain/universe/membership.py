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


def build_monitoring_memberships(
    result: RankingResult,
    *,
    forced_symbols: frozenset[str],
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
        if symbol in memberships:
            continue
        memberships[symbol] = TrackedMembership(
            symbol=symbol,
            status=MembershipStatus.FORCED,
            side=None,
            left_target_at=None,
        )

    return memberships
