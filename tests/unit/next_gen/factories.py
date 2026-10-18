from decimal import Decimal

from crypto_momentum_lab.next_gen.symbol_rules import SymbolFilter, SymbolRulesRegistry


def symbol_rules():
    return SymbolRulesRegistry(
        {
            symbol: SymbolFilter(
                symbol, Decimal(tick), Decimal(step), Decimal(step), Decimal("5")
            )
            for symbol, tick, step in [
                ("BTCUSDT", "0.1", "0.001"),
                ("ETHUSDT", "0.01", "0.001"),
                ("SOLUSDT", "0.01", "0.001"),
                ("DOGEUSDT", "0.00001", "1"),
            ]
        }
    )


def risk_config(environment="live", account_label="a1"):
    from datetime import UTC, datetime

    from crypto_momentum_lab.domain.risk.models import RiskConfigSnapshot

    return RiskConfigSnapshot(
        environment,
        account_label,
        Decimal("1000000"),
        Decimal("2000000"),
        None,
        100,
        True,
        datetime.now(UTC),
    )
