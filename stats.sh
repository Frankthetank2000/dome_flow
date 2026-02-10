#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

source .venv/bin/activate

DB="flow.db"

# Default: letzte 60 Minuten, 5-Minuten-Buckets NICHT anzeigen
python3 -u stats.py --db "$DB" --minutes 60
