# Running the wallet filter on GitHub Actions

Free, no host to pay for, no container to keep alive. GitHub's runners allow up
to 6 hours per job; a full ~8,000 wallet run takes about an hour on a free-tier
Alchemy key.

## Setup, once

1. **Settings → Secrets and variables → Actions → New repository secret**
   - Name: `ALCHEMY_URL`
   - Value: `https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY`
2. Commit the wallet list you want to filter. `wallet-filter/wallets.csv` is
   already there; add others by dragging them into the repo on github.com.

## Each run

1. **Actions** tab → **Filter wallets** → **Run workflow**
2. Set the minimum transactions, what to count, and your plan's CU/s
3. Start it, then close the tab — it keeps running

When it finishes, the run page shows the split at the top, and
**Artifacts → wallet-filter-results** has `passed.csv`, `filtered.csv`,
`results.csv`, and `errors.csv` if anything failed.

## Resuming

Progress is cached between runs. If a run is cancelled, times out, or ends with
unresolved wallets, just run the workflow again with the same settings: every
wallet already answered is skipped and only the rest are queried.

Changing the wallet list, the threshold, the mode or the categories starts a
fresh cache, because a count capped at ">= 4" cannot answer a threshold of 10.

## Cost

Public repository: unlimited minutes. Private repository: 2,000 minutes a month
on the free plan, so roughly 30 full runs. Only one run happens at a time —
the Alchemy budget is per key, so parallel runs would just throttle each other.
