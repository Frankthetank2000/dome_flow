#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# venv aktivieren
source .venv/bin/activate

# --- CONFIG ---
RPC="https://bsc-dataseed.binance.org/"
TOKEN="0x475bFaa1848591ae0E6aB69600f48d828f61a80E"

# Backfill Zeitraum in Tagen
DAYS=1

# Optional: nur ein Pool scannen (schneller). USDT empfohlen.
USDT="0x55d398326f99059fF775485246999027B3197955"
QUOTES=("$USDT" "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56" "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")

# Performance/Provider Limits:
CHUNK_BLOCKS=300
DB="flow.db"
# --- /CONFIG ---

python3 -u flow_monitor.py \
  --rpc "$RPC" \
  --token "$TOKEN" \
  --quotes "${QUOTES[@]}" \
  --db "$DB" \
  --backfill-days "$DAYS" \
  --chunk-blocks "$CHUNK_BLOCKS"
