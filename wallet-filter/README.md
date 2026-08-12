# Wallet filter

Filters a list of Ethereum wallets by how many transactions they have on mainnet,
counting activity **in both directions**. Built for the 7,913-row whitelist in
`wallets.csv` (7,871 unique addresses after de-duplication), with a default
threshold of **4 transactions**.

## Quick start

Windows (`cmd` or PowerShell):

```
pip install requests
python filter_wallets.py wallets.csv --min-tx 4 --url https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
```

macOS / Linux:

```bash
pip install requests
export ALCHEMY_URL="https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY"
python3 filter_wallets.py wallets.csv --min-tx 4
```

There are wrappers for both -- `run.bat <url>` on Windows, `./run.sh <url>`
elsewhere -- but they only do exactly the above.

Output lands in `./out`:

| file | contents |
| --- | --- |
| `passed.csv` | wallets with **>= 4** transactions — your filtered whitelist |
| `filtered.csv` | wallets with **< 4** transactions — the ones removed |
| `results.csv` | every wallet with its count and a `passed` flag |
| `errors.csv` | only written if some wallets could not be resolved |
| `checkpoint.jsonl`, `meta.json` | resume state (safe to delete once you have the results) |

## How it counts

The default counts **inbound and outbound** activity, and runs in two stages so
that the expensive half is only paid for where it is actually needed.

**Stage one — outbound.** The account nonce (`eth_getTransactionCount`) is
already the exact number of transactions a wallet has *sent*. It costs 26 CU and
batches 100 addresses per request, so the entire list is priced in ~10 minutes.
Every wallet that clears the threshold on outbound activity alone is finished
here and never costs a transfer lookup.

**Stage two — inbound.** Only the wallets still short of the threshold get an
`alchemy_getAssetTransfers` call for transfers *received*, at 150 CU each,
seeded with the nonce already known. Counting stops the moment the wallet
reaches the threshold.

On a representative run of this list, ~55% of wallets needed stage two, giving
roughly 860k CU (~45 min at 330 CU/s) against 2.36M CU (~2 hours) for asking
about transfers in both directions the naive way.

Note the deliberate asymmetry: the outbound side is *every* transaction sent
(including contract calls, approvals and failed transactions), while the inbound
side is asset transfers received. Outbound is therefore counted at least as
completely as `--mode transfers` would, for a sixth of the cost.

### The other two modes

```bash
# outbound only -- fastest, ~10 min for this list
python3 filter_wallets.py wallets.csv --min-tx 4 --mode nonce

# asset-transfer semantics on both sides -- slowest, ~2 hours
python3 filter_wallets.py wallets.csv --min-tx 4 --mode transfers
```

Use `nonce` if you only care that a wallet has actually transacted itself, and
`transfers` if you need identical semantics on both sides rather than the hybrid.

### Counting token activity too

By default only `external` (ETH) transfers count on the inbound side. To let
token activity qualify a wallet:

```bash
python3 filter_wallets.py wallets.csv --min-tx 4 \
  --categories external,internal,erc20,erc721,erc1155
```

Since counting stops at the threshold, a wallet that passes shows `>=4` rather
than a precise number — the exact total was never worth paying for. Pass
`--exact-counts` for true totals at considerably higher cost.

## Options

```
--min-tx N          keep wallets with at least N transactions (default: 4)
--mode MODE         both | nonce | transfers (default: both)
--url URL           RPC endpoint, or set ALCHEMY_URL
--out DIR           output directory (default: ./out)
--column NAME       CSV column holding the address (default: auto-detect)
--batch-size N      addresses per JSON-RPC batch, outbound stage (default: 100)
--workers N         concurrent requests (default: 5)
--categories LIST   transfer categories counted inbound (default: external)
--exact-counts      count everything, do not stop at the threshold
--cu-per-second N   your plan's throughput, for the time estimate (default: 330)
--no-resume         ignore an existing checkpoint and start over
--dry-run           parse the input and print an estimate without any network calls
```

Start with `--dry-run` to confirm the file parses and see the cost estimate:

```
$ python3 filter_wallets.py wallets.csv --dry-run
loaded 7871 unique addresses from wallets.csv
mode=both min-tx=4  204,646-1,385,296 CU, 10-70 min at 330 CU/s
```

The range is real: the low end is every wallet passing on outbound activity
alone, the high end is every wallet needing an inbound lookup. Your run lands
somewhere between, and the progress line shows which stage it is in.

## Input format

Either a bare newline-delimited list of addresses or a CSV with a header. The
address column is auto-detected; use `--column` to name it explicitly. Addresses
are lowercased and de-duplicated, and rows that are not valid 20-byte hex
addresses are skipped and reported rather than silently dropped.

## Reliability

The list is large enough that a run will take minutes, so the tool is built to
survive interruptions:

- **Resumable, including between stages.** Every answer is appended to
  `out/checkpoint.jsonl` as it comes in, and stage-one results are recorded even
  when the wallet still needs an inbound lookup. Re-run the identical command
  after a crash, a `Ctrl-C`, or a dropped connection and it resumes exactly
  where it stopped — an interruption during stage two does not re-fetch a single
  nonce. A truncated final line from a hard kill is tolerated.
- **Resume is refused when it would be wrong.** The threshold, mode and
  categories are recorded in `out/meta.json`. Since counts stop early at the
  threshold, a checkpoint built for `--min-tx 4` cannot answer `--min-tx 10`, so
  changing any of them stops the run and tells you to use `--no-resume` or a
  separate `--out` rather than silently mixing incompatible answers. Changing
  the input list is fine: results are scoped to the wallets in the file you pass.
- **Rate limits.** A 429 throttles every worker (honouring `Retry-After`), then
  retries with exponential backoff. The live counter shows how many you have hit
  — if it climbs, lower `--workers`.
- **Partial failures.** A wallet that cannot be resolved after retries is written
  to `errors.csv` instead of being guessed at, and is *not* checkpointed, so
  re-running retries exactly those. A bad batch never takes down the run.
- **Auth failures** (401/403) stop the whole run immediately rather than burning
  retries on a bad key — and, more importantly, rather than marking all 7,871
  wallets as errored and handing you an empty whitelist.

## Tests

```bash
pip install pytest && python3 -m pytest tests/ -q
```

46 tests covering CSV parsing, batch counting, out-of-order and partial batch
responses, two-stage in+out counting and its skip-the-inbound-lookup shortcut,
early-exit transfer counting, 429/5xx retry behaviour, checkpoint round-trips,
resume-mismatch guards, and full end-to-end runs that resume mid-stage — all
against a fake RPC, so no network or API credits are needed.

## Note on credentials

The endpoint URL contains your API key, so it is passed via `ALCHEMY_URL` and is
never stored in this repo. Anyone with that URL can spend your compute units —
rotate the key in the Alchemy dashboard if it has been shared anywhere public.
