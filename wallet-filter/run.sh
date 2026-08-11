#!/usr/bin/env bash
# Filter wallets.csv down to wallets with 4+ transactions (in or out).
#
#   ./run.sh https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
#
# Results: out/passed.csv (keep) and out/filtered.csv (cut).
# Safe to re-run -- it resumes where it left off.
set -euo pipefail
cd "$(dirname "$0")"

export ALCHEMY_URL="${1:-${ALCHEMY_URL:-}}"
if [ -z "$ALCHEMY_URL" ]; then
    echo "usage: ./run.sh <alchemy-url>   (or set ALCHEMY_URL)" >&2
    exit 2
fi

python3 -c 'import requests' 2>/dev/null || python3 -m pip install --quiet requests

exec python3 filter_wallets.py wallets.csv --min-tx 4
