#!/usr/bin/env python3
"""Filter Ethereum wallets by on-chain transaction count.

Reads a list of addresses, asks an Ethereum JSON-RPC endpoint (Alchemy) how many
transactions each one has, and splits the list into wallets that meet a minimum
and wallets that do not.

Three ways of counting are supported:

  both       Inbound *and* outbound activity. This is the default. It runs in
             two stages: first the account nonce for everyone (cheap and
             batched), which is the exact number of transactions the wallet has
             sent; then, only for the wallets that are still short of the
             threshold, one alchemy_getAssetTransfers call for inbound
             transfers. Wallets that already clear the bar on outbound activity
             never cost a transfer lookup at all.

  nonce      eth_getTransactionCount only -- outbound transactions, nothing
             inbound. Exact, cheap (26 CU), batchable 100 at a time.

  transfers  alchemy_getAssetTransfers in both directions. Same asset-transfer
             semantics on each side, at 150 CU per call and up to two calls per
             wallet, so it is by far the slowest option.

Usage:

    export ALCHEMY_URL="https://eth-mainnet.g.alchemy.com/v2/<key>"
    python3 filter_wallets.py wallets.csv --min-tx 4

Results land in ./out: passed.csv, filtered.csv, results.csv, errors.csv.
Progress is checkpointed, so re-running the same command resumes instead of
re-querying wallets that already have an answer.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Iterator, Sequence

import requests

ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

# Compute-unit cost per RPC method, used only to estimate runtime up front.
CU_NONCE = 26
CU_TRANSFERS = 150

TRANSFER_CATEGORIES = ("external", "internal", "erc20", "erc721", "erc1155", "specialnft")

# alchemy_getAssetTransfers refuses maxCount above 1000.
MAX_TRANSFER_PAGE = 1000


class RpcError(RuntimeError):
    """An RPC call failed in a way that is not worth retrying."""


class FatalRpcError(RpcError):
    """The endpoint itself is unusable (bad key, bad URL) -- stop the whole run.

    Distinct from RpcError so that a bad credential does not quietly mark every
    wallet in the list as errored.
    """


# --------------------------------------------------------------------------
# input
# --------------------------------------------------------------------------


def load_addresses(path: Path, column: str | None = None) -> tuple[list[str], list[str]]:
    """Read addresses from a CSV or newline-delimited file.

    Handles a header row or no header, one column or many, and quoted fields.
    Returns (unique lowercased addresses in first-seen order, malformed rows).
    """
    seen: set[str] = set()
    ordered: list[str] = []
    malformed: list[str] = []

    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.reader(handle))

    if not rows:
        return [], []

    col_index = 0
    start = 0
    header = [cell.strip() for cell in rows[0]]

    if column is not None:
        if column not in header:
            raise SystemExit(f"column {column!r} not found in header: {header}")
        col_index = header.index(column)
        start = 1
    elif not any(ADDRESS_RE.match(cell.strip()) for cell in header):
        # No address anywhere in row 0, so it is a header. Pick the column whose
        # first data row looks like an address, falling back to a named column.
        start = 1
        probe = rows[1] if len(rows) > 1 else []
        col_index = next(
            (i for i, cell in enumerate(probe) if ADDRESS_RE.match(cell.strip())),
            next((i for i, name in enumerate(header) if "address" in name.lower() or "wallet" in name.lower()), 0),
        )
    else:
        col_index = next(i for i, cell in enumerate(header) if ADDRESS_RE.match(cell.strip()))

    for row in rows[start:]:
        if not row or col_index >= len(row):
            continue
        raw = row[col_index].strip()
        if not raw:
            continue
        if not ADDRESS_RE.match(raw):
            malformed.append(raw)
            continue
        addr = raw.lower()
        if addr not in seen:
            seen.add(addr)
            ordered.append(addr)

    return ordered, malformed


def chunked(items: Sequence[str], size: int) -> Iterator[list[str]]:
    for i in range(0, len(items), size):
        yield list(items[i : i + size])


# --------------------------------------------------------------------------
# RPC transport
# --------------------------------------------------------------------------


@dataclass
class Result:
    address: str
    count: int
    capped: bool = False  # count stopped early at the threshold; true count may be higher
    error: str | None = None
    partial: bool = False  # "both" mode: outbound is known, inbound still to come


class AlchemyClient:
    """Thin JSON-RPC client with per-thread sessions, retries and 429 handling."""

    def __init__(
        self,
        url: str,
        timeout: float = 30.0,
        max_retries: int = 6,
        session_factory: Callable[[], requests.Session] | None = None,
    ) -> None:
        self.url = url
        self.timeout = timeout
        self.max_retries = max_retries
        self._session_factory = session_factory or requests.Session
        self._local = threading.local()
        self._throttle_until = 0.0
        self._lock = threading.Lock()
        self.rate_limit_hits = 0

    @property
    def session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = self._session_factory()
            self._local.session = session
        return session

    def _wait_for_throttle(self) -> None:
        while True:
            with self._lock:
                delay = self._throttle_until - time.monotonic()
            if delay <= 0:
                return
            time.sleep(min(delay, 5.0))

    def _throttle(self, seconds: float) -> None:
        with self._lock:
            self.rate_limit_hits += 1
            self._throttle_until = max(self._throttle_until, time.monotonic() + seconds)

    def call(self, payload: list[dict] | dict) -> list[dict] | dict:
        """POST a single request or a batch, retrying transient failures."""
        last_error: Exception | None = None

        for attempt in range(self.max_retries):
            self._wait_for_throttle()
            try:
                response = self.session.post(self.url, json=payload, timeout=self.timeout)
            except requests.RequestException as exc:
                last_error = exc
                time.sleep(min(2**attempt, 30))
                continue

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else min(2**attempt, 30)
                self._throttle(delay)
                last_error = RuntimeError("429 rate limited")
                continue

            if response.status_code in (500, 502, 503, 504):
                last_error = RuntimeError(f"HTTP {response.status_code}")
                time.sleep(min(2**attempt, 30))
                continue

            if response.status_code in (401, 403):
                raise FatalRpcError(f"HTTP {response.status_code} from RPC endpoint -- check the API key / URL")

            if response.status_code != 200:
                raise RpcError(f"HTTP {response.status_code}: {response.text[:200]}")

            try:
                return response.json()
            except ValueError as exc:
                last_error = exc
                time.sleep(min(2**attempt, 30))
                continue

        raise RpcError(f"giving up after {self.max_retries} attempts: {last_error}")


# --------------------------------------------------------------------------
# counting strategies
# --------------------------------------------------------------------------


def count_nonces(client: AlchemyClient, addresses: Sequence[str], block: str = "latest") -> list[Result]:
    """Outgoing transaction count for a batch of addresses, one HTTP round trip."""
    payload = [
        {"jsonrpc": "2.0", "id": i, "method": "eth_getTransactionCount", "params": [addr, block]}
        for i, addr in enumerate(addresses)
    ]

    try:
        raw = client.call(payload)
    except FatalRpcError:
        raise
    except RpcError as exc:
        return [Result(addr, -1, error=str(exc)) for addr in addresses]

    # A batch reply may come back out of order, so index it by id.
    if isinstance(raw, dict):
        raw = [raw]
    by_id = {item.get("id"): item for item in raw if isinstance(item, dict)}

    results: list[Result] = []
    for i, addr in enumerate(addresses):
        item = by_id.get(i)
        if item is None:
            results.append(Result(addr, -1, error="missing from batch response"))
        elif "error" in item and item["error"]:
            results.append(Result(addr, -1, error=str(item["error"])[:200]))
        else:
            try:
                results.append(Result(addr, int(item["result"], 16)))
            except (KeyError, TypeError, ValueError) as exc:
                results.append(Result(addr, -1, error=f"bad result: {exc}"))
    return results


def _transfer_page(
    client: AlchemyClient,
    address: str,
    direction: str,
    categories: Sequence[str],
    max_count: int,
    page_key: str | None,
) -> tuple[int, str | None]:
    params: dict = {
        "fromBlock": "0x0",
        "toBlock": "latest",
        "category": list(categories),
        "withMetadata": False,
        "excludeZeroValue": False,
        "maxCount": hex(min(max_count, MAX_TRANSFER_PAGE)),
        direction: address,
    }
    if page_key:
        params["pageKey"] = page_key

    raw = client.call({"jsonrpc": "2.0", "id": 1, "method": "alchemy_getAssetTransfers", "params": [params]})
    if isinstance(raw, list):
        raw = raw[0]
    if raw.get("error"):
        raise RpcError(str(raw["error"])[:200])
    result = raw.get("result") or {}
    return len(result.get("transfers") or []), result.get("pageKey")


def count_transfers(
    client: AlchemyClient,
    address: str,
    threshold: int,
    categories: Sequence[str],
    exact: bool = False,
    directions: Sequence[str] = ("fromAddress", "toAddress"),
    start: int = 0,
) -> Result:
    """Count transfers for a wallet, in either or both directions.

    `start` seeds the total with a count already known by other means -- "both"
    mode passes the nonce here and then only asks about the inbound side.

    By default counting stops as soon as the threshold is reached -- we only need
    to know whether the wallet clears the bar, and stopping early saves a lot of
    compute units. Pass exact=True to page through everything.
    """
    total = start
    if not exact and total >= threshold:
        return Result(address, total, capped=True)

    try:
        for direction in directions:
            page_key: str | None = None
            while True:
                remaining = MAX_TRANSFER_PAGE if exact else max(threshold - total, 1)
                found, page_key = _transfer_page(client, address, direction, categories, remaining, page_key)
                total += found
                if not exact and total >= threshold:
                    return Result(address, total, capped=True)
                if not page_key:
                    break
    except FatalRpcError:
        raise
    except RpcError as exc:
        return Result(address, -1, error=str(exc))

    return Result(address, total)


# --------------------------------------------------------------------------
# checkpointing
# --------------------------------------------------------------------------


def load_checkpoint(path: Path) -> dict[str, Result]:
    """Read previously resolved wallets. Errors are not kept, so they get retried.

    Partial records -- "both" mode wallets whose outbound count is known but
    whose inbound lookup had not happened yet -- are kept and returned with
    partial=True, so an interrupted run resumes at stage two rather than
    re-fetching every nonce. A later record for the same address wins, which is
    how a stage-two answer supersedes its stage-one partial.
    """
    done: dict[str, Result] = {}
    if not path.exists():
        return done

    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                if record.get("error"):
                    done.pop(record["address"], None)
                    continue
                done[record["address"]] = Result(
                    record["address"],
                    int(record["count"]),
                    bool(record.get("capped", False)),
                    partial=bool(record.get("partial", False)),
                )
            except (ValueError, KeyError, TypeError):
                continue  # truncated final line from an interrupted run
    return done


def run_signature(cfg: Config) -> dict:
    """The settings a checkpoint's counts depend on.

    Resuming across a change to any of these would mix incompatible answers --
    a count capped at ">= 4" says nothing about a threshold of 10, and a nonce
    is not an inbound transfer count.
    """
    return {
        "mode": cfg.mode,
        "min_tx": cfg.threshold,
        "categories": list(cfg.categories),
        "exact_counts": cfg.exact,
    }


def describe_signature_mismatch(meta_path: Path, cfg: Config) -> str | None:
    """Human-readable description of how an existing checkpoint differs, if it does."""
    if not meta_path.exists():
        return None
    try:
        previous = json.loads(meta_path.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return None

    current = run_signature(cfg)
    changed = [key for key, value in current.items() if key in previous and previous[key] != value]
    if not changed:
        return None
    return ", ".join(f"{key}={previous[key]!r} (now {current[key]!r})" for key in changed)


class CheckpointWriter:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = path.open("a", encoding="utf-8")
        self._lock = threading.Lock()

    def write(self, results: Iterable[Result]) -> None:
        with self._lock:
            for result in results:
                self._handle.write(
                    json.dumps(
                        {
                            "address": result.address,
                            "count": result.count,
                            "capped": result.capped,
                            "error": result.error,
                            "partial": result.partial,
                        }
                    )
                    + "\n"
                )
            self._handle.flush()

    def close(self) -> None:
        with self._lock:
            self._handle.close()


# --------------------------------------------------------------------------
# driver
# --------------------------------------------------------------------------


@dataclass
class Config:
    mode: str = "both"
    threshold: int = 4
    categories: Sequence[str] = ("external",)
    batch_size: int = 100
    workers: int = 5
    exact: bool = False


ProgressFn = Callable[[str, int, int], None]


def _execute(
    units: Sequence[list[str]],
    work: Callable[[list[str]], list[Result]],
    workers: int,
    checkpoint: CheckpointWriter | None,
    on_progress: ProgressFn | None,
    stage: str,
) -> list[Result]:
    """Run one stage's units across a thread pool, checkpointing as answers land."""
    results: list[Result] = []
    if not units:
        return results

    done = 0
    total = sum(len(unit) for unit in units)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(work, unit): unit for unit in units}
        for future in as_completed(futures):
            unit = futures[future]
            try:
                batch_results = future.result()
            except RpcError:
                raise
            except Exception as exc:  # noqa: BLE001 - one bad batch must not kill the run
                batch_results = [Result(addr, -1, error=repr(exc)) for addr in unit]

            results.extend(batch_results)
            if checkpoint:
                checkpoint.write(batch_results)
            done += len(unit)
            if on_progress:
                on_progress(stage, done, total)

    return results


def run(
    client: AlchemyClient,
    addresses: Sequence[str],
    cfg: Config,
    checkpoint: CheckpointWriter | None = None,
    on_progress: ProgressFn | None = None,
    resume_partials: Sequence[Result] = (),
) -> list[Result]:
    """Resolve a transaction count for every address.

    In "both" mode this is two stages: batched nonces for everyone, then an
    inbound transfer lookup for only those wallets the nonce alone did not carry
    over the threshold. `resume_partials` are wallets from an earlier run whose
    stage one is already done, so they skip straight to stage two.
    """
    results: list[Result] = []
    pending_inbound: list[Result] = list(resume_partials)

    if cfg.mode == "transfers":
        return _execute(
            [[addr] for addr in addresses],
            lambda unit: [count_transfers(client, unit[0], cfg.threshold, cfg.categories, cfg.exact)],
            cfg.workers,
            checkpoint,
            on_progress,
            "transfers",
        )

    # Stage one: the nonce, which is the exact outbound transaction count.
    def outbound(batch: list[str]) -> list[Result]:
        counted = count_nonces(client, batch)
        if cfg.mode == "both":
            for result in counted:
                # Wallets already over the bar are final; the rest need inbound.
                result.partial = result.error is None and result.count < cfg.threshold
        return counted

    stage_one = _execute(
        list(chunked(addresses, cfg.batch_size)),
        outbound,
        cfg.workers,
        checkpoint,
        on_progress,
        "outbound",
    )

    results.extend(r for r in stage_one if not r.partial)
    pending_inbound.extend(r for r in stage_one if r.partial)

    if cfg.mode == "nonce" or not pending_inbound:
        return results + pending_inbound

    # Stage two: inbound transfers, only for wallets still short of the threshold.
    outbound_counts = {r.address: r.count for r in pending_inbound}

    def inbound(unit: list[str]) -> list[Result]:
        address = unit[0]
        return [
            count_transfers(
                client,
                address,
                cfg.threshold,
                cfg.categories,
                cfg.exact,
                directions=("toAddress",),
                start=outbound_counts[address],
            )
        ]

    results.extend(
        _execute(
            [[addr] for addr in outbound_counts],
            inbound,
            cfg.workers,
            checkpoint,
            on_progress,
            "inbound",
        )
    )
    return results


def write_outputs(out_dir: Path, results: Sequence[Result], threshold: int) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    ordered = sorted(results, key=lambda r: (r.error is not None, -r.count, r.address))

    passed = [r for r in ordered if r.error is None and r.count >= threshold]
    filtered = [r for r in ordered if r.error is None and r.count < threshold]
    errored = [r for r in ordered if r.error is not None]

    with (out_dir / "passed.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["address", "tx_count"])
        for r in passed:
            writer.writerow([r.address, f">={r.count}" if r.capped else r.count])

    with (out_dir / "filtered.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["address", "tx_count"])
        for r in filtered:
            writer.writerow([r.address, r.count])

    with (out_dir / "results.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["address", "tx_count", "passed"])
        for r in ordered:
            if r.error is not None:
                continue
            writer.writerow([r.address, f">={r.count}" if r.capped else r.count, r.count >= threshold])

    if errored:
        with (out_dir / "errors.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["address", "error"])
            for r in errored:
                writer.writerow([r.address, r.error])

    return {"passed": len(passed), "filtered": len(filtered), "errors": len(errored)}


def estimate(count: int, mode: str, cu_per_second: int) -> tuple[int, int, float, float]:
    """Best- and worst-case compute units, and the matching times in seconds.

    The range is real rather than decorative: in "both" mode the cost depends on
    how many wallets clear the threshold on outbound activity alone (no transfer
    lookup at all) versus how many need the inbound stage.
    """
    if mode == "nonce":
        cu_low = cu_high = count * CU_NONCE
    elif mode == "both":
        cu_low = count * CU_NONCE  # everyone passes on the nonce alone
        cu_high = count * (CU_NONCE + CU_TRANSFERS)  # everyone needs the inbound lookup
    else:
        cu_low = count * CU_TRANSFERS  # outbound alone answers it
        cu_high = count * CU_TRANSFERS * 2  # both directions for every wallet

    if not cu_per_second:
        return cu_low, cu_high, 0.0, 0.0
    return cu_low, cu_high, cu_low / cu_per_second, cu_high / cu_per_second


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Filter Ethereum wallets by transaction count.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("input", type=Path, help="CSV or newline-delimited list of addresses")
    parser.add_argument("--min-tx", type=int, default=4, help="keep wallets with at least this many txs (default: 4)")
    parser.add_argument(
        "--mode",
        choices=("both", "nonce", "transfers"),
        default="both",
        help=(
            "both = inbound + outbound, nonce for the outbound half (default); "
            "nonce = outbound only; transfers = asset transfers both ways, slowest"
        ),
    )
    parser.add_argument("--url", default=os.environ.get("ALCHEMY_URL"), help="RPC URL (or set ALCHEMY_URL)")
    parser.add_argument("--out", type=Path, default=Path("out"), help="output directory (default: ./out)")
    parser.add_argument("--column", help="CSV column holding the address (default: auto-detect)")
    parser.add_argument("--batch-size", type=int, default=100, help="addresses per JSON-RPC batch (nonce mode)")
    parser.add_argument("--workers", type=int, default=5, help="concurrent requests (default: 5)")
    parser.add_argument(
        "--categories",
        default="external",
        help="comma-separated transfer categories counted on the inbound side (default: external)",
    )
    parser.add_argument(
        "--exact-counts",
        action="store_true",
        help="count every transfer instead of stopping at the threshold (much slower)",
    )
    parser.add_argument("--cu-per-second", type=int, default=330, help="your Alchemy CU/s, for the time estimate")
    parser.add_argument("--no-resume", action="store_true", help="ignore any existing checkpoint and start over")
    parser.add_argument("--dry-run", action="store_true", help="parse the input and print an estimate, no network")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.min_tx < 1:
        print("--min-tx must be at least 1", file=sys.stderr)
        return 2
    if not args.input.exists():
        print(f"input file not found: {args.input}", file=sys.stderr)
        return 2

    addresses, malformed = load_addresses(args.input, args.column)
    categories = [c.strip() for c in args.categories.split(",") if c.strip()]
    unknown = [c for c in categories if c not in TRANSFER_CATEGORIES]
    if unknown:
        print(f"unknown transfer categories: {', '.join(unknown)}", file=sys.stderr)
        return 2

    print(f"loaded {len(addresses)} unique addresses from {args.input}")
    if malformed:
        print(f"  skipped {len(malformed)} malformed rows (e.g. {malformed[0]!r})")

    cu_low, cu_high, sec_low, sec_high = estimate(len(addresses), args.mode, args.cu_per_second)
    if cu_low == cu_high:
        cost = f"~{cu_low:,} CU, ~{sec_low / 60:.1f} min"
    else:
        cost = f"{cu_low:,}-{cu_high:,} CU, {sec_low / 60:.0f}-{sec_high / 60:.0f} min"
    print(f"mode={args.mode} min-tx={args.min_tx}  {cost} at {args.cu_per_second} CU/s")

    if args.dry_run:
        return 0

    if not args.url:
        print("no RPC URL: pass --url or set ALCHEMY_URL", file=sys.stderr)
        return 2

    args.out.mkdir(parents=True, exist_ok=True)
    checkpoint_path = args.out / "checkpoint.jsonl"
    meta_path = args.out / "meta.json"
    if args.no_resume:
        # Both, or the stale signature would still block the fresh run.
        checkpoint_path.unlink(missing_ok=True)
        meta_path.unlink(missing_ok=True)

    cfg = Config(
        mode=args.mode,
        threshold=args.min_tx,
        categories=categories,
        batch_size=args.batch_size,
        workers=args.workers,
        exact=args.exact_counts,
    )

    stale = describe_signature_mismatch(meta_path, cfg)
    if stale:
        print(f"checkpoint in {args.out} was built with {stale}", file=sys.stderr)
        print("its counts do not answer this question -- re-run with --no-resume or a different --out", file=sys.stderr)
        return 2
    meta_path.write_text(json.dumps(run_signature(cfg), indent=2), encoding="utf-8")

    cached = load_checkpoint(checkpoint_path)
    known = set(addresses)
    # Partials have their outbound half done and resume at the inbound stage.
    partials = [r for a, r in cached.items() if r.partial and a in known]
    # Scoped to the current input, so swapping the list does not leak old wallets
    # from the checkpoint into the results.
    finished = {a: r for a, r in cached.items() if not r.partial and a in known}
    pending = [addr for addr in addresses if addr not in cached]

    if cached:
        detail = f", {len(partials)} needing only the inbound stage" if partials else ""
        print(f"resuming: {len(cached)} already done{detail}, {len(pending)} to go")
    client = AlchemyClient(args.url)
    writer = CheckpointWriter(checkpoint_path)
    started = time.monotonic()

    def progress(stage: str, done: int, total: int) -> None:
        elapsed = time.monotonic() - started
        rate = done / elapsed if elapsed > 0 else 0
        eta = (total - done) / rate if rate > 0 else 0
        sys.stderr.write(
            f"\r  {stage}: {done}/{total}  {rate:.0f}/s  eta {eta / 60:.1f}m  429s:{client.rate_limit_hits}   "
        )
        sys.stderr.flush()

    try:
        fresh = run(client, pending, cfg, writer, progress, resume_partials=partials)
    except RpcError as exc:
        sys.stderr.write("\n")
        print(f"fatal: {exc}", file=sys.stderr)
        print(f"progress is saved in {checkpoint_path} -- fix the problem and re-run to resume", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        sys.stderr.write("\ninterrupted -- re-run the same command to resume\n")
        return 130
    finally:
        writer.close()

    sys.stderr.write("\n")

    results = list(finished.values()) + fresh
    counts = write_outputs(args.out, results, args.min_tx)

    total = counts["passed"] + counts["filtered"]
    share = counts["passed"] / total * 100 if total else 0.0
    print(f"\npassed   (>= {args.min_tx} tx): {counts['passed']:,}  ({share:.1f}%)")
    print(f"filtered (<  {args.min_tx} tx): {counts['filtered']:,}")
    if counts["errors"]:
        print(f"errors:                {counts['errors']:,}  -- see {args.out / 'errors.csv'}, re-run to retry")
    print(f"\nwrote {args.out}/passed.csv, filtered.csv, results.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
