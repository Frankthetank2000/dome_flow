#!/usr/bin/env python3
"""
flow_monitor.py
Automatisches Buy/Sell Monitoring via PancakeSwap V2 Pair Swap-Events (BSC),
Speicherung in SQLite + Aggregation + Alerts.

BUY/SELL Definition relativ zum TARGET_TOKEN:
- BUY  => Pool gibt TARGET_TOKEN raus (token_out > 0)
- SELL => Pool bekommt TARGET_TOKEN rein (token_in > 0)
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
getcontext().prec = 60

# ------------------ Defaults (BSC / PancakeSwap V2) ------------------
PANCAKE_V2_FACTORY = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73"

# Common BSC quote tokens
WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
BUSD = "0xe9e7cea3dedca5984780bafc599bd69add087d56"
USDT = "0x55d398326f99059ff775485246999027b3197955"
USDC = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d"

DEFAULT_QUOTES = [WBNB, BUSD, USDT, USDC]

FACTORY_ABI = [
    {
        "name": "getPair",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "tokenA", "type": "address"}, {"name": "tokenB", "type": "address"}],
        "outputs": [{"name": "pair", "type": "address"}],
    }
]

PAIR_ABI = [
    {"name": "token0", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {"name": "token1", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {
        "anonymous": False,
        "type": "event",
        "name": "Swap",
        "inputs": [
            {"indexed": True, "name": "sender", "type": "address"},
            {"indexed": False, "name": "amount0In", "type": "uint256"},
            {"indexed": False, "name": "amount1In", "type": "uint256"},
            {"indexed": False, "name": "amount0Out", "type": "uint256"},
            {"indexed": False, "name": "amount1Out", "type": "uint256"},
            {"indexed": True, "name": "to", "type": "address"},
        ],
    },
]

ERC20_ABI = [
    {"name": "decimals", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint8"}]},
    {"name": "symbol", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "string"}]},
]

# Swap event signature topic0:
# keccak256("Swap(address,uint256,uint256,uint256,uint256,address)")
SWAP_TOPIC0 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"


@dataclass
class PairMeta:
    pair: str
    token0: str
    token1: str
    sym0: str
    sym1: str
    dec0: int
    dec1: int
    token_index: int  # 0 if token0 is target, 1 if token1 is target


def to_dec(x: int, d: int) -> Decimal:
    return Decimal(x) / (Decimal(10) ** Decimal(d))


def now_unix() -> int:
    return int(time.time())


def connect_w3(rpc_url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))
    if not w3.is_connected():
        raise SystemExit("RPC nicht erreichbar. RPC_URL prüfen.")

    # BSC braucht PoA/extraData Middleware
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    return w3


def checksum(w3: Web3, addr: str) -> str:
    return Web3.to_checksum_address(addr)


def find_pairs(w3: Web3, factory_addr: str, target_token: str, quotes: List[str]) -> List[str]:
    factory = w3.eth.contract(address=checksum(w3, factory_addr), abi=FACTORY_ABI)
    pairs = []
    for q in quotes:
        p = factory.functions.getPair(checksum(w3, target_token), checksum(w3, q)).call()
        if int(p, 16) != 0:
            pairs.append(Web3.to_checksum_address(p))
    return pairs


def load_pair_meta(w3: Web3, pair_addr: str, target_token: str) -> PairMeta:
    pair = w3.eth.contract(address=checksum(w3, pair_addr), abi=PAIR_ABI)
    t0 = Web3.to_checksum_address(pair.functions.token0().call())
    t1 = Web3.to_checksum_address(pair.functions.token1().call())

    tok0 = w3.eth.contract(address=t0, abi=ERC20_ABI)
    tok1 = w3.eth.contract(address=t1, abi=ERC20_ABI)

    d0 = int(tok0.functions.decimals().call())
    d1 = int(tok1.functions.decimals().call())

    # symbol() kann bei manchen Tokens failen -> fallback
    try:
        s0 = tok0.functions.symbol().call()
    except Exception:
        s0 = t0[:6] + "…" + t0[-4:]
    try:
        s1 = tok1.functions.symbol().call()
    except Exception:
        s1 = t1[:6] + "…" + t1[-4:]

    target = checksum(w3, target_token).lower()
    if t0.lower() == target:
        idx = 0
    elif t1.lower() == target:
        idx = 1
    else:
        raise SystemExit("Target Token ist nicht token0 oder token1 des Pairs. Falsches Pair?")

    return PairMeta(pair=checksum(w3, pair_addr), token0=t0, token1=t1, sym0=s0, sym1=s1, dec0=d0, dec1=d1, token_index=idx)


def init_db(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS swaps (
            chain TEXT NOT NULL,
            pair TEXT NOT NULL,
            tx_hash TEXT NOT NULL,
            log_index INTEGER NOT NULL,
            block_number INTEGER NOT NULL,
            block_time INTEGER NOT NULL,
            side TEXT NOT NULL,             -- BUY/SELL/MIXED
            token_symbol TEXT NOT NULL,
            quote_symbol TEXT NOT NULL,
            token_amount REAL NOT NULL,     -- Decimal stored as float (OK for analytics). Keep raw ints too if needed.
            quote_amount REAL NOT NULL,
            price REAL,                     -- quote per token
            raw_amount0_in TEXT,
            raw_amount1_in TEXT,
            raw_amount0_out TEXT,
            raw_amount1_out TEXT,
            PRIMARY KEY (tx_hash, log_index)
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS agg (
            pair TEXT NOT NULL,
            interval_s INTEGER NOT NULL,
            bucket_start INTEGER NOT NULL,  -- unix aligned
            buy_token REAL NOT NULL,
            sell_token REAL NOT NULL,
            net_token REAL NOT NULL,
            buy_quote REAL NOT NULL,
            sell_quote REAL NOT NULL,
            net_quote REAL NOT NULL,
            trades INTEGER NOT NULL,
            PRIMARY KEY (pair, interval_s, bucket_start)
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS state (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        );
        """
    )

    return con


def get_state(con: sqlite3.Connection, key: str) -> Optional[str]:
    row = con.execute("SELECT v FROM state WHERE k=?", (key,)).fetchone()
    return row[0] if row else None


def set_state(con: sqlite3.Connection, key: str, val: str) -> None:
    con.execute("INSERT INTO state(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, val))
    con.commit()


def classify_swap(meta: PairMeta, a0in: int, a1in: int, a0out: int, a1out: int) -> Tuple[str, Decimal, Decimal, Optional[Decimal], str, str]:
    """
    Returns:
      side, token_amt, quote_amt, price, token_symbol, quote_symbol
    """
    if meta.token_index == 0:
        token_in = a0in
        token_out = a0out
        quote_in = a1in
        quote_out = a1out
        token_dec, quote_dec = meta.dec0, meta.dec1
        token_sym, quote_sym = meta.sym0, meta.sym1
    else:
        token_in = a1in
        token_out = a1out
        quote_in = a0in
        quote_out = a0out
        token_dec, quote_dec = meta.dec1, meta.dec0
        token_sym, quote_sym = meta.sym1, meta.sym0

    if token_out > 0 and token_in == 0:
        side = "BUY"
        token_amt = to_dec(token_out, token_dec)
        quote_amt = to_dec(quote_in, quote_dec)
    elif token_in > 0 and token_out == 0:
        side = "SELL"
        token_amt = to_dec(token_in, token_dec)
        quote_amt = to_dec(quote_out, quote_dec)
    else:
        side = "MIXED"
        token_amt = to_dec(abs(token_out - token_in), token_dec)
        quote_amt = to_dec(abs(quote_in - quote_out), quote_dec)

    price = (quote_amt / token_amt) if token_amt != 0 else None
    return side, token_amt, quote_amt, price, token_sym, quote_sym


def fetch_swap_logs(w3: Web3, pair: str, from_block: int, to_block: int) -> List[dict]:
    """
    Robust get_logs with automatic range splitting when provider limits are hit.
    """
    def _get(a: int, b: int) -> List[dict]:
        return w3.eth.get_logs(
            {
                "fromBlock": a,
                "toBlock": b,
                "address": checksum(w3, pair),
                "topics": [SWAP_TOPIC0],
            }
        )

    # iterative stack-based splitting (avoids recursion depth issues)
    logs: List[dict] = []
    stack: List[Tuple[int, int]] = [(from_block, to_block)]

    while stack:
        a, b = stack.pop()
        if a > b:
            continue

        try:
            part = _get(a, b)
            logs.extend(part)
        except Exception as e:
            msg = str(e).lower()
            # Typical messages: "limit exceeded", "query returned more than ...", timeouts
            if ("limit exceeded" in msg) or ("more than" in msg) or ("timeout" in msg) or ("too many" in msg):
                if a == b:
                    # Even a single block fails -> give up for this block
                    # You could also sleep and retry.
                    continue
                mid = (a + b) // 2
                # split into two smaller ranges
                stack.append((mid + 1, b))
                stack.append((a, mid))
                # small backoff to be gentle to public RPC
                time.sleep(0.15)
            else:
                raise

    return logs

def block_timestamp(w3: Web3, block_number: int) -> int:
    blk = w3.eth.get_block(block_number)
    return int(blk["timestamp"])


def find_block_by_timestamp(w3: Web3, target_ts: int) -> int:
    """
    Binary search for a block with timestamp <= target_ts.
    This avoids guessing blocks/day and is good for "last N days".
    """
    latest = w3.eth.block_number
    lo, hi = 1, latest
    # quick bounds
    if block_timestamp(w3, 1) > target_ts:
        return 1
    if block_timestamp(w3, latest) <= target_ts:
        return latest

    while lo + 1 < hi:
        mid = (lo + hi) // 2
        ts = block_timestamp(w3, mid)
        if ts <= target_ts:
            lo = mid
        else:
            hi = mid
    return lo


def decode_swap_log(w3: Web3, meta: PairMeta, log: dict) -> Tuple[int, int, int, int, int]:
    """
    Decode amount0In/amount1In/amount0Out/amount1Out from log data.
    Easiest: use contract event ABI decoding.
    """
    pair = w3.eth.contract(address=meta.pair, abi=PAIR_ABI)
    ev = pair.events.Swap().process_log(log)
    a0in = int(ev["args"]["amount0In"])
    a1in = int(ev["args"]["amount1In"])
    a0out = int(ev["args"]["amount0Out"])
    a1out = int(ev["args"]["amount1Out"])
    return a0in, a1in, a0out, a1out, int(ev["blockNumber"])


def insert_swap(
    con: sqlite3.Connection,
    chain: str,
    meta: PairMeta,
    tx_hash: str,
    log_index: int,
    block_number: int,
    block_time: int,
    side: str,
    token_sym: str,
    quote_sym: str,
    token_amt: Decimal,
    quote_amt: Decimal,
    price: Optional[Decimal],
    a0in: int,
    a1in: int,
    a0out: int,
    a1out: int,
) -> bool:
    """
    Returns True if inserted (new), False if already exists.
    """
    try:
        con.execute(
            """
            INSERT INTO swaps(
              chain, pair, tx_hash, log_index, block_number, block_time, side,
              token_symbol, quote_symbol, token_amount, quote_amount, price,
              raw_amount0_in, raw_amount1_in, raw_amount0_out, raw_amount1_out
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                chain,
                meta.pair,
                tx_hash,
                log_index,
                block_number,
                block_time,
                side,
                token_sym,
                quote_sym,
                float(token_amt),
                float(quote_amt),
                float(price) if price is not None else None,
                str(a0in),
                str(a1in),
                str(a0out),
                str(a1out),
            ),
        )
        return True
    except sqlite3.IntegrityError:
        return False


def bucket_start(ts: int, interval_s: int) -> int:
    return (ts // interval_s) * interval_s


def update_agg(con: sqlite3.Connection, pair: str, interval_s: int, ts: int, side: str, token_amt: float, quote_amt: float) -> None:
    b = bucket_start(ts, interval_s)

    buy_token = token_amt if side == "BUY" else 0.0
    sell_token = token_amt if side == "SELL" else 0.0
    net_token = buy_token - sell_token

    buy_quote = quote_amt if side == "BUY" else 0.0
    sell_quote = quote_amt if side == "SELL" else 0.0
    net_quote = buy_quote - sell_quote

    con.execute(
        """
        INSERT INTO agg(pair, interval_s, bucket_start, buy_token, sell_token, net_token, buy_quote, sell_quote, net_quote, trades)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(pair, interval_s, bucket_start) DO UPDATE SET
          buy_token = buy_token + excluded.buy_token,
          sell_token = sell_token + excluded.sell_token,
          net_token = net_token + excluded.net_token,
          buy_quote = buy_quote + excluded.buy_quote,
          sell_quote = sell_quote + excluded.sell_quote,
          net_quote = net_quote + excluded.net_quote,
          trades = trades + 1
        """,
        (pair, interval_s, b, buy_token, sell_token, net_token, buy_quote, sell_quote, net_quote, 1),
    )


def check_alert(con: sqlite3.Connection, pair: str, interval_s: int, ts: int, threshold_net_buy_token: float) -> Optional[Tuple[int, float, int]]:
    """
    Returns (bucket_start, net_token, trades) if alert triggers for current bucket.
    """
    b = bucket_start(ts, interval_s)
    row = con.execute(
        "SELECT net_token, trades FROM agg WHERE pair=? AND interval_s=? AND bucket_start=?",
        (pair, interval_s, b),
    ).fetchone()
    if not row:
        return None
    net_token, trades = float(row[0]), int(row[1])
    if net_token >= threshold_net_buy_token:
        return (b, net_token, trades)
    return None


def run_backfill(
    w3: Web3,
    con: sqlite3.Connection,
    chain: str,
    metas: List[PairMeta],
    days: int,
    chunk_blocks: int,
    interval_s_list: List[int],
    alert_interval_s: int,
    alert_threshold: float,
) -> None:
    target_ts = now_unix() - days * 86400
    start_block = find_block_by_timestamp(w3, target_ts)
    latest = w3.eth.block_number
    print(f"[backfill] days={days} start_block≈{start_block} latest={latest}")

    # backfill per pair
    for meta in metas:
        print(f"\n[backfill] Pair {meta.pair} ({meta.sym0}/{meta.sym1})")
        from_block = start_block
        while from_block <= latest:
            to_block = min(from_block + chunk_blocks - 1, latest)
            logs = fetch_swap_logs(w3, meta.pair, from_block, to_block)
            if logs:
                # cache block timestamps per block number
                ts_cache: Dict[int, int] = {}
                new_count = 0
                for lg in logs:
                    tx = lg["transactionHash"].hex()
                    li = int(lg["logIndex"])
                    bn = int(lg["blockNumber"])
                    if bn not in ts_cache:
                        ts_cache[bn] = block_timestamp(w3, bn)
                    bt = ts_cache[bn]

                    a0in, a1in, a0out, a1out, _ = decode_swap_log(w3, meta, lg)
                    side, token_amt, quote_amt, price, token_sym, quote_sym = classify_swap(meta, a0in, a1in, a0out, a1out)

                    inserted = insert_swap(
                        con, chain, meta, tx, li, bn, bt, side, token_sym, quote_sym,
                        token_amt, quote_amt, price, a0in, a1in, a0out, a1out
                    )
                    if inserted:
                        new_count += 1
                        for itv in interval_s_list:
                            update_agg(con, meta.pair, itv, bt, side, float(token_amt), float(quote_amt))

                con.commit()
                print(f"  blocks {from_block}-{to_block}: logs={len(logs)} new={new_count}")
            else:
                # keep it quiet-ish
                pass

            from_block = to_block + 1


def run_monitor(
    w3: Web3,
    con: sqlite3.Connection,
    chain: str,
    metas: List[PairMeta],
    poll_seconds: int,
    interval_s_list: List[int],
    alert_interval_s: int,
    alert_threshold: float,
    chunk_blocks: int,
) -> None:
    # resume from last processed block if present
    k = "last_block"
    last = get_state(con, k)
    if last is not None:
        from_block = int(last)
    else:
        # start near latest
        latest = w3.eth.block_number
        from_block = max(latest - chunk_blocks, 1)

    print(f"[monitor] start from_block={from_block} poll={poll_seconds}s")

    while True:
        latest = w3.eth.block_number
        if from_block > latest:
            time.sleep(poll_seconds)
            continue

        to_block = min(from_block + chunk_blocks - 1, latest)

        any_new = False
        for meta in metas:
            logs = fetch_swap_logs(w3, meta.pair, from_block, to_block)
            if not logs:
                continue

            ts_cache: Dict[int, int] = {}
            new_count = 0
            for lg in logs:
                tx = lg["transactionHash"].hex()
                li = int(lg["logIndex"])
                bn = int(lg["blockNumber"])
                if bn not in ts_cache:
                    ts_cache[bn] = block_timestamp(w3, bn)
                bt = ts_cache[bn]

                a0in, a1in, a0out, a1out, _ = decode_swap_log(w3, meta, lg)
                side, token_amt, quote_amt, price, token_sym, quote_sym = classify_swap(meta, a0in, a1in, a0out, a1out)

                inserted = insert_swap(
                    con, chain, meta, tx, li, bn, bt, side, token_sym, quote_sym,
                    token_amt, quote_amt, price, a0in, a1in, a0out, a1out
                )
                if inserted:
                    any_new = True
                    new_count += 1
                    for itv in interval_s_list:
                        update_agg(con, meta.pair, itv, bt, side, float(token_amt), float(quote_amt))

                    # Alert check (only for target interval)
                    if alert_threshold > 0 and itv == alert_interval_s:
                        hit = check_alert(con, meta.pair, alert_interval_s, bt, alert_threshold)
                        if hit:
                            b, net_tok, trades = hit
                            # Keep it simple: print. (Webhook/Email can be added later.)
                            print(f"🚨 ALERT [{meta.sym0}/{meta.sym1}] bucket={b} net_buy_token={net_tok:.6f} trades={trades}")

            con.commit()
            print(f"[monitor] {meta.sym0}/{meta.sym1} blocks {from_block}-{to_block}: logs={len(logs)} new={new_count}")

        set_state(con, k, str(to_block + 1))
        from_block = to_block + 1

        if not any_new:
            time.sleep(poll_seconds)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rpc", required=True, help="BSC RPC URL (HTTP)")
    ap.add_argument("--token", required=True, help="Target token address (e.g. DOME token contract)")
    ap.add_argument("--quotes", nargs="*", default=DEFAULT_QUOTES, help="Quote token addresses")
    ap.add_argument("--factory", default=PANCAKE_V2_FACTORY, help="PancakeSwap V2 Factory address")
    ap.add_argument("--db", default="flow.db", help="SQLite DB path")
    ap.add_argument("--chain", default="bsc", help="Chain label stored in DB")
    ap.add_argument("--backfill-days", type=int, default=0, help="If >0, backfill last N days then exit (or continue with --monitor)")
    ap.add_argument("--monitor", action="store_true", help="Run continuous monitoring loop")
    ap.add_argument("--poll-seconds", type=int, default=10, help="Monitor poll interval")
    ap.add_argument("--chunk-blocks", type=int, default=2000, help="Block range per query")
    ap.add_argument("--agg", default="60,300", help="Aggregation intervals in seconds (comma-separated), e.g. 60,300")
    ap.add_argument("--alert-interval", type=int, default=300, help="Which interval bucket to alert on (seconds)")
    ap.add_argument("--alert-net-buy-token", type=float, default=0.0, help="Alert threshold for net buys (token units) in alert-interval bucket")
    return ap.parse_args()


def main():
    args = parse_args()

    w3 = connect_w3(args.rpc)
    target = checksum(w3, args.token)
    quotes = [checksum(w3, q) for q in args.quotes]
    factory = checksum(w3, args.factory)

    con = init_db(args.db)

    pairs = find_pairs(w3, factory, target, quotes)
    if not pairs:
        raise SystemExit("Keine Pairs gefunden. Entweder keine Liquidität oder falsche Quote-Liste.")

    metas = [load_pair_meta(w3, p, target) for p in pairs]

    print("Gefundene Pairs:")
    for m in metas:
        print(f"  {m.pair}  {m.sym0}/{m.sym1}  token0={m.token0} token1={m.token1}")

    interval_s_list = [int(x.strip()) for x in args.agg.split(",") if x.strip()]
    interval_s_list = sorted(set(interval_s_list))

    if args.backfill_days > 0:
        run_backfill(
            w3=w3,
            con=con,
            chain=args.chain,
            metas=metas,
            days=args.backfill_days,
            chunk_blocks=args.chunk_blocks,
            interval_s_list=interval_s_list,
            alert_interval_s=args.alert_interval,
            alert_threshold=args.alert_net_buy_token,
        )

    if args.monitor:
        run_monitor(
            w3=w3,
            con=con,
            chain=args.chain,
            metas=metas,
            poll_seconds=args.poll_seconds,
            interval_s_list=interval_s_list,
            alert_interval_s=args.alert_interval,
            alert_threshold=args.alert_net_buy_token,
            chunk_blocks=args.chunk_blocks,
        )

    if args.backfill_days == 0 and not args.monitor:
        print("Nichts zu tun: setze --backfill-days N und/oder --monitor.")


if __name__ == "__main__":
    main()