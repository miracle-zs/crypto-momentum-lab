from decimal import Decimal

from crypto_momentum_lab.domain.strategy.sizing import SymbolLotRules


def btc_lot_rules() -> SymbolLotRules:
    """Fixed test rules; not exchange metadata or a production fallback."""
    return SymbolLotRules(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        step_size=Decimal("0.001"),
        min_quantity=Decimal("0.001"),
        max_quantity=Decimal("1000.00"),
        min_notional=Decimal("5.00"),
    )
