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


def test_compute_pnl_from_hl_fills_matches_kbonk_short():
    """HL truth: entry ~0.003417, close 0.003176, pnl ~26.40 after fees."""
    open_fills = [{"px": "0.003417", "sz": "110152", "fee": "0"}]
    close_fills = [{"px": "0.003176", "sz": "110152", "fee": "0.15"}]
    net_pnl, net_pct, filled = ExecutorService._compute_realized_pnl_from_hl_fills(
        open_fills, close_fills, is_buy=False
    )
    assert float(net_pnl) == 26.396632
    assert abs(float(net_pct) - (26.396632 / 376.389384)) < 1e-9
    assert float(filled) == 726.232384


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
