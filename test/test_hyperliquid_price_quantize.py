"""Tests for Hyperliquid price quantize patch (hummingbot-api)."""

from decimal import Decimal
from unittest.mock import MagicMock

from utils.hyperliquid_price_quantize import patch_hyperliquid_quantize_order_price, quantize_hyperliquid_order_price


def test_quantize_hyperliquid_order_price_uses_tick():
    rule = MagicMock()
    rule.min_price_increment = Decimal("0.00001")
    connector = MagicMock()
    connector._trading_rules = {"ARB-USD": rule}
    result = quantize_hyperliquid_order_price(connector, "ARB-USD", Decimal("0.088027"))
    assert result == Decimal("0.08803")


def test_quantize_btc_market_slippage_respects_five_sig_figs():
    """BTC mid*1.05 must not become 68013.8 (HL rejects 6 sig figs)."""
    rule = MagicMock()
    rule.min_price_increment = Decimal("0.1")
    connector = MagicMock()
    connector._trading_rules = {"BTC-USD": rule}
    result = quantize_hyperliquid_order_price(connector, "BTC-USD", Decimal("68013.75"))
    assert result == Decimal("68014")


def test_patch_hyperliquid_quantize_order_price():
    connector = MagicMock()
    connector.name = "hyperliquid_perpetual"
    rule = MagicMock()
    rule.min_price_increment = Decimal("0.00001")
    connector._trading_rules = {"ARB-USD": rule}
    assert patch_hyperliquid_quantize_order_price(connector) is True
    assert connector.quantize_order_price("ARB-USD", Decimal("0.088027")) == Decimal("0.08803")
