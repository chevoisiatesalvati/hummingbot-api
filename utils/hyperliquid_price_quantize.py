"""Hyperliquid order price tick alignment for pip-installed hummingbot connectors."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import Any


def quantize_hyperliquid_order_price(connector: Any, trading_pair: str, price: Decimal) -> Decimal:
    """Align price to Hyperliquid rules: max 5 significant figures, then tick size.

    Tick-only rounding can produce prices like 68013.8 (6 sig figs) which HL rejects
    with "Price must be divisible by tick size".
    """
    # HL allows at most 5 significant figures on limitPx
    price = Decimal(str(float(f"{price:.5g}")))
    trading_rule = connector._trading_rules.get(trading_pair)
    if trading_rule is not None and trading_rule.min_price_increment:
        tick = trading_rule.min_price_increment
        price = (price / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick
    return price


def patch_hyperliquid_quantize_order_price(connector: Any) -> bool:
    """Override hummingbot's .5g price quantize for Hyperliquid connectors."""
    name = (getattr(connector, "name", None) or "").lower()
    if "hyperliquid" not in name:
        return False

    def quantize_order_price(trading_pair: str, price: Decimal) -> Decimal:
        return quantize_hyperliquid_order_price(connector, trading_pair, price)

    connector.quantize_order_price = quantize_order_price
    return True
