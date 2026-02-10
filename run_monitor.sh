#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# venv aktivieren
source .venv/bin/activate

# --- CONFIG ---
RPC="https://bsc-dataseed.binance.org/"
TOKEN="0x475bFaa1848591ae0E6aB69600f48d828f61a80E"

# Quote auswählen: USDT-Pool (oft am sinnvollsten für "Preis" & Flow)
USDT="0x55d398326f99059fF775485246999027B3197955"

# Alerts:
ALERT_INTERVAL=300        # 5 Minuten Bucket
ALERT_NET_BUY_TOKEN=1000  # Alarm ab Net-Buys >= 1000 Token im Bucket (0 = aus)

# Performance/Provider Limits:
CHUNK_BLOCKS=300
POLL_SECONDS=10
AGG="60,300"
DB="flow.db"
# --- /CONFIG ---

python3 -u flow_monitor.py \
  --rpc "$RPC" \
  --token "$TOKEN" \
  --quotes "$USDT" \
  --db "$DB" \
  --monitor \
  --agg "$AGG" \
  --chunk-blocks "$CHUNK_BLOCKS" \
  --poll-seconds "$POLL_SECONDS" \
  --alert-interval "$ALERT_INTERVAL" \
  --alert-net-buy-token "$ALERT_NET_BUY_TOKEN"
