#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import time
from datetime import datetime


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def main():
    ap = argparse.ArgumentParser(description="Show buy/sell stats from flow.db")
    ap.add_argument("--db", default="flow.db", help="SQLite DB path")
    ap.add_argument("--minutes", type=int, default=60, help="Lookback window in minutes (default 60)")
    ap.add_argument("--interval", type=int, default=300, help="Aggregation interval seconds (default 300 = 5min)")
    ap.add_argument("--pair", default=None, help="Optional: filter by pair address (0x...)")
    ap.add_argument("--show-buckets", action="store_true", help="Also print per-bucket rows")
    args = ap.parse_args()

    now = int(time.time())
    since = now - args.minutes * 60

    con = sqlite3.connect(args.db)

    pair_filter_sql = ""
    pair_params = ()
    if args.pair:
        pair_filter_sql = " AND pair = ? "
        pair_params = (args.pair,)

    # --- 1) Summary from swaps table (ground truth for window) ---
    # BUY adds to buy_token, SELL adds to sell_token, net = buy - sell
    row = con.execute(
        f"""
        SELECT
          pair,
          MIN(block_time) as first_ts,
          MAX(block_time) as last_ts,
          SUM(CASE WHEN side='BUY' THEN token_amount ELSE 0 END) as buy_token,
          SUM(CASE WHEN side='SELL' THEN token_amount ELSE 0 END) as sell_token,
          SUM(CASE WHEN side='BUY' THEN quote_amount ELSE 0 END) as buy_quote,
          SUM(CASE WHEN side='SELL' THEN quote_amount ELSE 0 END) as sell_quote,
          COUNT(*) as trades,
          MAX(token_symbol) as token_symbol,
          MAX(quote_symbol) as quote_symbol
        FROM swaps
        WHERE block_time >= ?
        {pair_filter_sql}
        GROUP BY pair
        ORDER BY (buy_token - sell_token) DESC
        """,
        (since, *pair_params),
    ).fetchall()

    if not row:
        print(f"Keine Daten in den letzten {args.minutes} Minuten. (db={args.db})")
        return

    print(f"\nZeitraum: {fmt_ts(since)}  →  {fmt_ts(now)}  (last {args.minutes} min)")
    print("-" * 110)
    print(f"{'PAIR':42}  {'SYMBOL':12}  {'TRADES':>6}  {'BUY':>14}  {'SELL':>14}  {'NET':>14}  {'QUOTE NET':>14}")
    print("-" * 110)

    for (pair, first_ts, last_ts, buy_t, sell_t, buy_q, sell_q, trades, tok_sym, q_sym) in row:
        buy_t = float(buy_t or 0)
        sell_t = float(sell_t or 0)
        net_t = buy_t - sell_t

        buy_q = float(buy_q or 0)
        sell_q = float(sell_q or 0)
        net_q = buy_q - sell_q

        symbol = f"{tok_sym}/{q_sym}"
        print(f"{pair:42}  {symbol:12}  {trades:6d}  {buy_t:14.6f}  {sell_t:14.6f}  {net_t:14.6f}  {net_q:14.6f}")

    print("-" * 110)

    # --- 2) Optional: show buckets from agg table for the interval ---
    if args.show_buckets:
        interval = args.interval
        bucket_since = (since // interval) * interval

        buckets = con.execute(
            f"""
            SELECT pair, bucket_start, buy_token, sell_token, net_token, trades
            FROM agg
            WHERE interval_s = ?
              AND bucket_start >= ?
            {pair_filter_sql}
            ORDER BY pair, bucket_start DESC
            """,
            (interval, bucket_since, *pair_params),
        ).fetchall()

        print(f"\nBuckets (interval={interval}s) seit {fmt_ts(bucket_since)}")
        print("-" * 90)
        print(f"{'PAIR':42}  {'BUCKET':19}  {'BUY':>12}  {'SELL':>12}  {'NET':>12}  {'TR':>4}")
        print("-" * 90)
        for pair, bts, buy_t, sell_t, net_t, tr in buckets:
            print(f"{pair:42}  {fmt_ts(int(bts)):19}  {float(buy_t):12.6f}  {float(sell_t):12.6f}  {float(net_t):12.6f}  {int(tr):4d}")

        print("-" * 90)


if __name__ == "__main__":
    main()
