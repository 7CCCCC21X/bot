"""Unit tests for the pure helper functions in predict_monitor_bot.

These cover the parsing / normalization / diffing logic that has no I/O, so
they run fast and without a Telegram token or network. Import-time env vars
are set in conftest.py.
"""
import time

import predict_monitor_bot as m


# ---------------------------------------------------------------- retry-after
def test_parse_retry_after_numeric():
    assert m._parse_retry_after("12") == 12.0
    assert m._parse_retry_after("0") == 0.0


def test_parse_retry_after_none_and_invalid():
    assert m._parse_retry_after(None) is None
    assert m._parse_retry_after("not-a-date") is None


def test_parse_retry_after_http_date_in_future():
    # An HTTP-date a minute out should parse to a small positive delay.
    from email.utils import format_datetime
    from datetime import datetime, timezone, timedelta

    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    val = m._parse_retry_after(format_datetime(future))
    assert val is not None
    assert 50 <= val <= 65


# ------------------------------------------------------------ market resolution
def test_market_resolution_unresolved():
    assert m.market_resolution({"status": "open"}) == (False, None)
    assert m.market_resolution(None) == (False, None)


def test_market_resolution_resolved_variants():
    assert m.market_resolution({"resolved": True, "winningOutcome": 1}) == (True, 1)
    assert m.market_resolution({"status": "resolved", "resolvedOutcome": 0}) == (True, 0)
    assert m.market_resolution({"state": "settled", "winningOutcomeIndex": "2"}) == (True, 2)


def test_market_resolution_closed_without_winner():
    assert m.market_resolution({"status": "closed"}) == (True, None)


# ----------------------------------------------------------- market cache TTL
def test_market_cache_fresh_respects_ttl(monkeypatch):
    monkeypatch.setattr(m, "MARKET_CACHE_TTL_S", 100)
    m.market_cache.clear()
    m._market_cache_ts.clear()

    m._store_market("mkt-unresolved", {"status": "open"})
    assert m._market_cache_fresh("mkt-unresolved") is True

    # Age the entry past the TTL.
    m._market_cache_ts["mkt-unresolved"] = time.time() - 200
    assert m._market_cache_fresh("mkt-unresolved") is False


def test_market_cache_resolved_is_permanently_fresh(monkeypatch):
    monkeypatch.setattr(m, "MARKET_CACHE_TTL_S", 1)
    m.market_cache.clear()
    m._market_cache_ts.clear()

    m._store_market("mkt-done", {"status": "resolved", "resolvedOutcome": 1})
    # Even with the timestamp far in the past, a resolved market stays fresh.
    m._market_cache_ts["mkt-done"] = 0
    assert m._market_cache_fresh("mkt-done") is True


def test_store_market_evicts_over_capacity(monkeypatch):
    monkeypatch.setattr(m, "MARKET_CACHE_MAX", 3)
    m.market_cache.clear()
    m._market_cache_ts.clear()
    for i in range(5):
        m._store_market(f"m{i}", {"status": "open"})
        # Force monotonically increasing timestamps for deterministic eviction.
        m._market_cache_ts[f"m{i}"] = 1000 + i
    assert len(m.market_cache) <= 3
    # Oldest entries should have been evicted, newest kept.
    assert "m4" in m.market_cache
    assert "m0" not in m.market_cache


# ----------------------------------------------------------------- normalizers
def test_safe_float():
    assert m._safe_float("3.5") == 3.5
    assert m._safe_float(None) is None
    assert m._safe_float("abc") is None


def test_norm_amount_to_shares_scales_wei():
    # Values above 1e9 are treated as 18-decimal wei.
    assert m._norm_amount_to_shares(str(5 * 10**18)) == 5.0
    assert m._norm_amount_to_shares("12.5") == 12.5
    assert m._norm_amount_to_shares(None) is None


def test_norm_price_to_cents():
    # 18-decimal probability fraction → cents.
    assert abs(m._norm_price_to_cents(2 * 10**15) - 0.2) < 1e-6
    # decimal-string probability → cents.
    assert abs(m._norm_price_to_cents("0.5") - 50.0) < 1e-6


def test_fmt_shares_scale_adaptive():
    assert m._fmt_shares(76421.9982) == "76,422"
    assert m._fmt_shares(None) == "?"
    assert m._fmt_shares(5.5556) == "5.56"


# -------------------------------------------------------------------- pos keys
def test_pos_key_and_size():
    pos = {"marketId": "abc", "outcomeIndex": 1, "shares": "10"}
    assert m.pos_key(pos) == "abc_1"
    assert m.pos_size(pos) == 10.0


def test_pos_key_from_nested_objects():
    pos = {"market": {"id": "xyz"}, "outcome": {"index": 0}}
    assert m.pos_key(pos) == "xyz_0"


# ----------------------------------------------------------------- diff logic
def _pos(market, outcome, shares):
    return {"marketId": market, "outcomeIndex": outcome, "shares": shares}


def test_diff_positions_added():
    added, changed, closed = m.diff_positions({}, [_pos("a", 0, 5)])
    assert len(added) == 1 and not changed and not closed


def test_diff_positions_changed_and_closed():
    old = {"a_0": 5.0, "b_1": 3.0}
    new = [_pos("a", 0, 8)]  # a changed 5→8, b_1 disappeared
    added, changed, closed = m.diff_positions(old, new)
    assert not added
    assert len(changed) == 1
    pos, prev = changed[0]
    assert prev == 5.0
    assert closed == ["b_1"]


def test_diff_positions_threshold_suppresses_small_change():
    old = {"a_0": 100.0}
    new = [_pos("a", 0, 105)]  # 5% change
    # 10% threshold should suppress it.
    added, changed, closed = m.diff_positions(old, new, threshold_pct=10.0)
    assert not changed
    # 1% threshold should let it through.
    _, changed2, _ = m.diff_positions(old, new, threshold_pct=1.0)
    assert len(changed2) == 1


def test_diff_positions_legacy_hash_snapshot_silent():
    # Legacy snapshot value is a string hash → no change notification.
    old = {"a_0": "deadbeefhash"}
    new = [_pos("a", 0, 8)]
    added, changed, closed = m.diff_positions(old, new)
    assert not added and not changed and not closed


# ------------------------------------------------------------- consolidate fills
def test_consolidate_fills_merges_same_tx():
    matches = [
        {"transactionHash": "0xtx1", "amountFilled": "5"},
        {"transactionHash": "0xtx1", "amountFilled": "7"},
        {"transactionHash": "0xtx2", "amountFilled": "3"},
    ]
    out = m.consolidate_fills(matches)
    assert len(out) == 2
    # tx1 merges to 12 (stored as a float); tx2 is a single fill so its
    # original value is preserved untouched. Compare numerically.
    by_amount = sorted(float(x["amountFilled"]) for x in out)
    assert by_amount == [3.0, 12.0]
    merged = [x for x in out if x.get("_merged_count")]
    assert len(merged) == 1 and merged[0]["amountFilled"] == 12.0


# ------------------------------------------------------------------- percentile
def test_percentile_small_sample():
    vals = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert m._percentile([], 0.5) is None
    assert m._percentile(vals, 0.95) == 5.0  # max for small n
    assert m._percentile(vals, 0.5) == 3.0   # median


# ------------------------------------------------------------ dust interval parse
def test_parse_dust_interval_raw():
    assert m._parse_dust_interval_raw("off") == -1
    assert m._parse_dust_interval_raw("default") == 0
    assert m._parse_dust_interval_raw("5m") == 300
    assert m._parse_dust_interval_raw("2h") == 7200
    assert m._parse_dust_interval_raw("90s") == 90
    # Below 30s positive is rejected.
    assert m._parse_dust_interval_raw("10") is None
    assert m._parse_dust_interval_raw("garbage") is None


def test_fmt_addr():
    assert m.fmt_addr("0x1234567890abcdef") == "0x1234…cdef"
