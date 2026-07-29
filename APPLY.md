# Applying the automint work locally

Two signed commits on branch `claude/vibrant-cray-8jozjc`, on top of your
existing `1540243 Initial commit`:

```
f1f3727  Add OpenSea Drops watcher as the primary mint path
4ddecca  Add automint: free public NFT mint watcher and executor
```

42 files. 96 tests. Typecheck clean.

## Option A — the bundle (recommended)

Preserves the exact commits **and their signatures**, so they stay Verified on
GitHub. Use this unless it fails.

From inside your existing clone of `my-practce-repo`:

```bash
git fetch /path/to/automint.bundle claude/vibrant-cray-8jozjc:claude/vibrant-cray-8jozjc
git checkout claude/vibrant-cray-8jozjc
git push -u origin claude/vibrant-cray-8jozjc
```

Or, to inspect it standalone before touching your repo:

```bash
git clone --branch claude/vibrant-cray-8jozjc automint.bundle automint-review
```

Verify before trusting it:

```bash
git bundle verify automint.bundle     # expect "records a complete history"
git rev-parse claude/vibrant-cray-8jozjc   # expect f1f3727...
```

## Option B — the patches

Use only if the bundle route fails. **This re-authors the commits, so they will
push as Unverified** — the signature does not survive `git am`.

```bash
git checkout -b claude/vibrant-cray-8jozjc main
git am 0001-Add-automint-free-public-NFT-mint-watcher-and-execut.patch
git am 0002-Add-OpenSea-Drops-watcher-as-the-primary-mint-path.patch
```

If a patch conflicts, `git am --abort` and use the bundle instead.

## Then

```bash
npm install
npm run typecheck     # expect no output
npm test              # expect 96 passed
cp .env.example .env  # add your burner key + OpenSea API key
```

`dryRun` is on by default. Read the README's "What this protects you from, and
what it doesn't" section before turning it off.

## First thing to actually verify

The OpenSea Drops endpoint paths could not be checked against a live API from
the machine this was written on — `api.opensea.io` was blocked. So do this first:

```bash
npx tsx src/cli.ts drops
```

If it lists drops, the paths are right. If it prints nothing, every path and
field name is isolated in `src/opensea/drops.ts` (`DROP_ENDPOINTS` plus the
normalisers) — that one file is the only thing that needs fixing.
