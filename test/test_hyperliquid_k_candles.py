"""Tests for Hyperliquid k-scaled perpetual candle helpers."""

from unittest.mock import MagicMock

from utils.hyperliquid_candles import (
    compute_candles_ready_timeout,
    is_k_scaled_hl_base,
    parse_hl_candle_price,
    patch_hyperliquid_perpetual_candles,
    trading_pair_to_hl_candle_coin,
)


def test_is_k_scaled_hl_base():
    assert is_k_scaled_hl_base("kBONK") is True
    assert is_k_scaled_hl_base("kPEPE") is True
    assert is_k_scaled_hl_base("kSHIB") is True
    assert is_k_scaled_hl_base("BTC") is False
    assert is_k_scaled_hl_base("PEPE") is False


def test_trading_pair_to_hl_candle_coin():
    assert trading_pair_to_hl_candle_coin("kBONK-USD") == "kBONK"
    assert trading_pair_to_hl_candle_coin("kPEPE-USD") == "kPEPE"
    assert trading_pair_to_hl_candle_coin("BTC-USD") == "BTC"
    assert trading_pair_to_hl_candle_coin("XYZ:AAPL-USD") == "xyz:AAPL"


def test_parse_hl_candle_price_preserves_sub_cent():
    assert parse_hl_candle_price("0.003791") == 0.003791
    assert parse_hl_candle_price("0.002827") == 0.002827
    assert parse_hl_candle_price(0.003791) == 0.003791


def test_compute_candles_ready_timeout_scales_for_k_pairs():
    base = 30
    k_timeout = compute_candles_ready_timeout(
        "hyperliquid_perpetual", 500, base, "kBONK-USD"
    )
    btc_timeout = compute_candles_ready_timeout(
        "hyperliquid_perpetual", 500, base, "BTC-USD"
    )
    assert k_timeout >= 75.0
    assert btc_timeout >= 70.0
    assert k_timeout >= btc_timeout
    assert compute_candles_ready_timeout("binance", 500, base, "BTC-USDT") == 30.0


def test_patch_hyperliquid_perpetual_candles_normalizes_ohlc():
    feed = MagicMock()
    feed.name = "hyperliquid_perpetual_kBONK-USD"
    feed._trading_pair = "kBONK-USD"
    feed._base_asset = "kBONK"
    feed._parse_rest_candles = MagicMock(
        return_value=[
            [1_784_068_500.0, "0.003791", "0.003811", "0.003791", "0.003803", "7007106.0", 0.0, 28.0, 0.0, 0.0]
        ]
    )
    feed._parse_websocket_message = MagicMock(
        return_value={
            "timestamp": 1_784_068_560.0,
            "open": "0.003792",
            "high": "0.003792",
            "low": "0.003792",
            "close": "0.003792",
            "volume": "5220.0",
            "quote_asset_volume": 0.0,
            "n_trades": 1.0,
            "taker_buy_base_volume": 0.0,
            "taker_buy_quote_volume": 0.0,
        }
    )

    assert patch_hyperliquid_perpetual_candles(feed) is True

    rest_rows = feed._parse_rest_candles([{}])
    assert rest_rows[0][1:5] == [0.003791, 0.003811, 0.003791, 0.003803]

    ws_row = feed._parse_websocket_message({})
    assert ws_row["close"] == 0.003792
