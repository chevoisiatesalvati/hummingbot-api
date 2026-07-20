"""Hyperliquid perpetual candle helpers and patches for hummingbot-api."""

from __future__ import annotations

import re
from typing import Any, List, Optional

_K_SCALED_BASE_RE = re.compile(r"^k[A-Z]")


def is_k_scaled_hl_base(base_asset: str) -> bool:
    """True for Hyperliquid 1000x quoted perps (kPEPE, kBONK, kSHIB, ...)."""
    return bool(_K_SCALED_BASE_RE.match(base_asset))


def trading_pair_to_hl_candle_coin(trading_pair: str) -> str:
    """Map hummingbot trading pair to Hyperliquid candleSnapshot / WS coin name."""
    base = trading_pair.split("-")[0]
    if ":" in base:
        deployer, coin = base.split(":", 1)
        return f"{deployer.lower()}:{coin}"
    return base


def parse_hl_candle_price(value: Any) -> float:
    """Parse HL OHLC fields without truncating sub-cent prices."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return 0.0
    return float(text)


def compute_candles_ready_timeout(
    connector: str,
    max_records: int,
    base_timeout: int,
    trading_pair: str = "",
) -> float:
    """Scale HL perp candle warmup timeout with buffer size (k-pairs backfill slowly)."""
    if connector != "hyperliquid_perpetual":
        return float(base_timeout)

    base_asset = trading_pair.split("-")[0] if trading_pair else ""
    per_record_s = 0.12
    if is_k_scaled_hl_base(base_asset):
        per_record_s = 0.15
    estimated = max_records * per_record_s + 10.0
    return float(min(max(base_timeout, estimated), 120.0))


def patch_hyperliquid_perpetual_candles(feed: Any) -> bool:
    """Ensure HL perp candle OHLC are parsed as floats and coin mapping is correct."""
    name = getattr(feed, "name", "") or ""
    if not name.startswith("hyperliquid_perpetual_"):
        return False

    trading_pair = getattr(feed, "_trading_pair", "")
    expected_coin = trading_pair_to_hl_candle_coin(trading_pair)
    if getattr(feed, "_base_asset", None) != expected_coin:
        feed._base_asset = expected_coin

    original_parse_rest = feed._parse_rest_candles
    original_parse_ws = feed._parse_websocket_message

    def _parse_rest_candles(data: dict, end_time: Optional[int] = None) -> List[List[float]]:
        rows = original_parse_rest(data, end_time)
        parsed: List[List[float]] = []
        for row in rows:
            parsed.append(
                [
                    float(row[0]),
                    parse_hl_candle_price(row[1]),
                    parse_hl_candle_price(row[2]),
                    parse_hl_candle_price(row[3]),
                    parse_hl_candle_price(row[4]),
                    parse_hl_candle_price(row[5]),
                    float(row[6]),
                    float(row[7]),
                    float(row[8]),
                    float(row[9]),
                ]
            )
        return parsed

    def _parse_websocket_message(data: dict):
        parsed = original_parse_ws(data)
        if isinstance(parsed, dict):
            for key in ("open", "high", "low", "close", "volume"):
                if key in parsed:
                    parsed[key] = parse_hl_candle_price(parsed[key])
        return parsed

    feed._parse_rest_candles = _parse_rest_candles
    feed._parse_websocket_message = _parse_websocket_message

    return True
