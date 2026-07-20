"""Tests for Hyperliquid fill-based PnL repair on terminated position executors."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from services.executor_service import ExecutorService


def _svc() -> ExecutorService:
    return object.__new__(ExecutorService)


def test_may_need_repair_when_real_open_close_oids_and_nonzero_wrong_pnl():
    """MARKET close with mid-price PnL must still be eligible for HL fill repair."""
    svc = _svc()
    record = SimpleNamespace(
        executor_type="position_executor",
        status="TERMINATED",
        connector_name="hyperliquid_perpetual",
        net_pnl_quote=9.36,
        close_type="TAKE_PROFIT",
        final_state=(
            '{"order_ids":["0x635d14dbe34725b5bda59c8fe235db6b",'
            '"0x5852e057aeb29900318152470df9c366"],'
            '"close_price":0.003332}'
        ),
    )
    assert svc._executor_may_need_hl_pnl_repair(record) is True


def test_may_need_repair_skips_non_hl():
    svc = _svc()
    record = SimpleNamespace(
        executor_type="position_executor",
        status="TERMINATED",
        connector_name="binance_perpetual",
        net_pnl_quote=9.36,
        close_type="TAKE_PROFIT",
        final_state='{"order_ids":["a","b"]}',
    )
    assert svc._executor_may_need_hl_pnl_repair(record) is False


def test_compute_pnl_matched_vwap_equal_size_short():
    """Equal-size short without closedPnl uses matched VWAP (legacy mid-price repair)."""
    open_fills = [{"px": "0.003417", "sz": "110152", "fee": "0"}]
    close_fills = [{"px": "0.003176", "sz": "110152", "fee": "0.15"}]
    net_pnl, net_pct, filled = ExecutorService._compute_realized_pnl_from_hl_fills(
        open_fills, close_fills, is_buy=False
    )
    assert float(net_pnl) == 26.396632
    # pct on matched close notional
    close_notional = 0.003176 * 110152
    assert abs(float(net_pct) - (26.396632 / close_notional)) < 1e-9
    assert abs(float(filled) - 726.232136) < 1e-6


def test_compute_pnl_prefers_close_closed_pnl_over_quote_diff():
    """Flip/partial: open_quote-close_quote would be ~197; closedPnl path is ~4.05."""
    open_fills = [
        {"px": "0.003777", "sz": "102247", "fee": "0.168188", "closedPnl": "12.5"}
    ]
    close_fills = [
        {
            "px": "0.003696",
            "sz": "50954",
            "fee": "0.08",
            "closedPnl": "4.127274",
        }
    ]
    # Naive quote-diff (old bug):
    naive = (0.003777 * 102247) - (0.003696 * 50954) - 0.248188
    assert abs(naive - 197.612747) < 1e-6

    net_pnl, net_pct, filled = ExecutorService._compute_realized_pnl_from_hl_fills(
        open_fills, close_fills, is_buy=False
    )
    # size_mismatch: closedPnl(close) - close_fee only (open fee is flip pollution)
    expected = 4.127274 - 0.08
    assert abs(float(net_pnl) - expected) < 1e-6
    assert abs(float(net_pnl) - 4.05) < 0.02
    assert abs(float(net_pnl) - 197.612747) > 1.0

    breakdown = ExecutorService._hl_pnl_breakdown_from_fills(
        open_fills, close_fills, is_buy=False
    )
    assert breakdown is not None
    assert breakdown["method"] == "closed_pnl"
    assert breakdown["size_mismatch"] is True
    assert breakdown["proposed_entry"] is None  # do not trust flip open VWAP
    assert float(breakdown["hl_closed_pnl"]) == 4.127274
    assert abs(float(breakdown["hl_net_pnl"]) - expected) < 1e-6
    assert abs(float(breakdown["proposed_fees"]) - 0.08) < 1e-9


def test_matched_vwap_fallback_on_size_mismatch_without_closed_pnl():
    """Without closedPnl, matched base prevents phantom quote-diff PnL."""
    open_fills = [{"px": "0.003777", "sz": "102247", "fee": "0.168188"}]
    close_fills = [{"px": "0.003696", "sz": "50954", "fee": "0.08"}]
    net_pnl, _, _ = ExecutorService._compute_realized_pnl_from_hl_fills(
        open_fills, close_fills, is_buy=False
    )
    # (0.003777 - 0.003696) * 50954 - fees
    expected = (0.003777 - 0.003696) * 50954 - 0.248188
    assert abs(float(net_pnl) - expected) < 1e-6
    assert abs(float(net_pnl) - 197.61) > 1.0


def test_vwap_from_close_fills():
    fills = [{"px": "0.003176", "sz": "110152", "fee": "0.15"}]
    assert ExecutorService._vwap_from_hl_fills(fills) == Decimal("0.003176")


def test_fills_for_oids_merges_client_and_exchange_keys():
    by_oid = {
        "0xabc": [{"px": "1", "sz": "1", "tid": "t1", "time": 1, "side": "B"}],
        "12345": [{"px": "2", "sz": "2", "tid": "t2", "time": 2, "side": "A"}],
    }
    rows = ExecutorService._fills_for_oids(by_oid, "0xabc", "12345")
    assert len(rows) == 2
