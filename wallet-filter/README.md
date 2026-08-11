# Wallet filter

Filters a list of Ethereum wallets by how many transactions they have on mainnet.
Built for the 7,913-row whitelist in `wallets.csv` (7,871 unique addresses after
de-duplication), with a default threshold of **4 transactions**.

## Quick start

```bash
pip install requests
export ALCHEMY_URL="https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY"
python3 filter_wallets.py wallets.csv --min-tx 4
```

Output lands in `./out`:

| file | contents |
| --- | --- |
| `passed.csv` | wallets with **>= 4** transactions — your filtered whitelist |
| `filtered.csv` | wallets with **< 4** transactions — the ones removed |
| `results.csv` | every wallet with its count and a `passed` flag |
| `errors.csv` | only written if some wallets could not be resolved |
| `checkpoint.jsonl` | resume state (safe to delete once you have the results) |

## How it counts

Two modes, because "transaction count" is genuinely ambiguous on Ethereum.

**`--mode nonce`** (default) uses `eth_getTransactionCount`, the account nonce:
the number of transactions the wallet has **sent**. It is exact, costs 26 CU per
wallet, and batches 100 addresses into one HTTP request — the whole list runs in
about 10 minutes on a 330 CU/s plan. A wallet that only ever *received* ETH has a
nonce of 0, which is usually what you want for whitelist screening: it means
nobody has ever transacted from that address.

**`--mode transfers`** uses `alchemy_getAssetTransfers` and counts transfers both
**in and out**. Use it if a wallet that received 20 transfers but sent 3 should
still qualify. It costs 150 CU per call and needs up to two calls per wallet
(~2 hours for this list), so only reach for it if inbound activity matters.

```bash
# inbound + outbound ETH transfers
python3 filter_wallets.py wallets.csv --min-tx 4 --mode transfers

# include token activity too
python3 filter_wallets.py wallets.csv --min-tx 4 --mode transfers \
  --categories external,internal,erc20,erc721,erc1155
```

In `transfers` mode counting stops as soon as a wallet reaches the threshold —
there is no reason to keep paying for pages once the answer is known. Those rows
show as `>=4` rather than a precise number. Pass `--exact-counts` for true totals
at considerably higher cost.

## Options

```
--min-tx N          keep wallets with at least N transactions (default: 4)
--mode MODE         nonce | transfers (default: nonce)
--url URL           RPC endpoint, or set ALCHEMY_URL
--out DIR           output directory (default: ./out)
--column NAME       CSV column holding the address (default: auto-detect)
--batch-size N      addresses per JSON-RPC batch, nonce mode (default: 100)
--workers N         concurrent requests (default: 5)
--categories LIST   transfer categories for transfers mode (default: external)
--exact-counts      transfers mode: count everything, do not stop at threshold
--cu-per-second N   your plan's throughput, for the time estimate (default: 330)
--no-resume         ignore an existing checkpoint and start over
--dry-run           parse the input and print an estimate without any network calls
```

Start with `--dry-run` to confirm the file parses and see the cost estimate:

```
$ python3 filter_wallets.py wallets.csv --dry-run
loaded 7871 unique addresses from wallets.csv
mode=nonce min-tx=4  ~204,646 CU, ~10.3 min at 330 CU/s
```

## Input format

Either a bare newline-delimited list of addresses or a CSV with a header. The
address column is auto-detected; use `--column` to name it explicitly. Addresses
are lowercased and de-duplicated, and rows that are not valid 20-byte hex
addresses are skipped and reported rather than silently dropped.

## Reliability

The list is large enough that a run will take minutes, so the tool is built to
survive interruptions:

- **Resumable.** Every answer is appended to `out/checkpoint.jsonl` as it comes
  in. Re-run the identical command after a crash, a `Ctrl-C`, or a dropped
  connection and it picks up where it stopped. A truncated final line from a
  hard kill is tolerated.
- **Rate limits.** A 429 throttles every worker (honouring `Retry-After`), then
  retries with exponential backoff. The live counter shows how many you have hit
  — if it climbs, lower `--workers`.
- **Partial failures.** A wallet that cannot be resolved after retries is written
  to `errors.csv` instead of being guessed at, and is *not* checkpointed, so
  re-running retries exactly those. A bad batch never takes down the run.
- **Auth failures** (401/403) stop immediately rather than burning retries on a
  bad key.

## Tests

```bash
pip install pytest && python3 -m pytest tests/ -q
```

27 tests covering CSV parsing, batch counting, out-of-order and partial batch
responses, early-exit transfer counting, 429/5xx retry behaviour, checkpoint
round-trips, and a full end-to-end run with resume — all against a fake RPC, so
no network or API credits are needed.

## Note on credentials

The endpoint URL contains your API key, so it is passed via `ALCHEMY_URL` and is
never stored in this repo. Anyone with that URL can spend your compute units —
rotate the key in the Alchemy dashboard if it has been shared anywhere public.
