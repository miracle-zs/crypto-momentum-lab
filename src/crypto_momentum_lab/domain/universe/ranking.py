from decimal import Decimal

from crypto_momentum_lab.domain.universe.models import (
    MarketCandidate,
    RankEntry,
    RankingResult,
    RankingSide,
)


def rank_utc_day_returns(
    candidates: list[MarketCandidate],
    *,
    top_count: int,
    ranking_depth: int,
    loser_target_count: int | None = None,
) -> RankingResult:
    effective_loser_target_count = (
        top_count if loser_target_count is None else loser_target_count
    )
    if effective_loser_target_count < 0:
        raise ValueError("loser_target_count must be non-negative")
    if ranking_depth < max(top_count, effective_loser_target_count):
        raise ValueError("ranking_depth must be >= top_count")

    valid: list[tuple[MarketCandidate, Decimal]] = []
    exclusions: dict[str, str] = {}

    for candidate in sorted(candidates, key=lambda item: item.symbol):
        if candidate.open_price is None:
            exclusions[candidate.symbol] = "missing_open_price"
            continue
        if candidate.current_price is None:
            exclusions[candidate.symbol] = "missing_current_price"
            continue
        if candidate.open_price <= 0:
            exclusions[candidate.symbol] = "non_positive_open_price"
            continue
        if candidate.current_price <= 0:
            exclusions[candidate.symbol] = "non_positive_current_price"
            continue
        day_return = candidate.current_price / candidate.open_price - Decimal(1)
        valid.append((candidate, day_return))

    descending = sorted(valid, key=lambda item: (-item[1], item[0].symbol))
    ascending = sorted(valid, key=lambda item: (item[1], item[0].symbol))

    gainers = tuple(
        RankEntry(
            symbol=candidate.symbol,
            utc_day_return=day_return,
            rank=index,
            side=RankingSide.GAINER,
        )
        for index, (candidate, day_return) in enumerate(
            descending[:ranking_depth],
            start=1,
        )
    )
    losers = tuple(
        RankEntry(
            symbol=candidate.symbol,
            utc_day_return=day_return,
            rank=index,
            side=RankingSide.LOSER,
        )
        for index, (candidate, day_return) in enumerate(
            ascending[:ranking_depth],
            start=1,
        )
    )
    target_symbols = frozenset(
        entry.symbol
        for entry in (
            *gainers[:top_count],
            *losers[:effective_loser_target_count],
        )
    )
    return RankingResult(
        candidates=tuple(sorted(candidates, key=lambda item: item.symbol)),
        gainers=gainers,
        losers=losers,
        target_symbols=target_symbols,
        exclusions=exclusions,
    )
