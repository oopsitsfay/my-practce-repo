#!/usr/bin/env python3
"""Filter Ethereum wallets by on-chain transaction count.

Reads a list of addresses, asks an Ethereum JSON-RPC endpoint (Alchemy) how many
transactions each one has, and splits the list into wallets that meet a minimum
and wallets that do not.

Two ways of counting are supported:

  nonce      eth_getTransactionCount -- the account nonce, i.e. the number of
             transactions the wallet has *sent*. Exact, cheap (26 CU), and
             batchable 100 at a time. This is the default.

  transfers  alchemy_getAssetTransfers -- transfers in *and* out of the wallet.
             Counts a wallet that only ever received funds. Costs 150 CU per
             call and needs up to two calls per wallet, so it is much slower.

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
                raise RpcError(f"HTTP {response.status_code} from RPC endpoint -- check the API key / URL")

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
) -> Result:
    """Transfers in and out of a wallet.

    By default counting stops as soon as the threshold is reached -- we only need
    to know whether the wallet clears the bar, and stopping early saves a lot of
    compute units. Pass exact=True to page through everything.
    """
    total = 0
    try:
        for direction in ("fromAddress", "toAddress"):
            page_key: str | None = None
            while True:
                remaining = MAX_TRANSFER_PAGE if exact else max(threshold - total, 1)
                found, page_key = _transfer_page(client, address, direction, categories, remaining, page_key)
                total += found
                if not exact and total >= threshold:
                    return Result(address, total, capped=True)
                if not page_key:
                    break
    except RpcError as exc:
        return Result(address, -1, error=str(exc))

    return Result(address, total)


# --------------------------------------------------------------------------
# checkpointing
# --------------------------------------------------------------------------


def load_checkpoint(path: Path) -> dict[str, Result]:
    """Read previously resolved wallets. Errors are not kept, so they get retried."""
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
                    continue
                done[record["address"]] = Result(
                    record["address"], int(record["count"]), bool(record.get("capped", False))
                )
            except (ValueError, KeyError, TypeError):
                continue  # truncated final line from an interrupted run
    return done


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


def run(
    client: AlchemyClient,
    addresses: Sequence[str],
    mode: str,
    threshold: int,
    categories: Sequence[str],
    batch_size: int,
    workers: int,
    checkpoint: CheckpointWriter | None,
    exact: bool = False,
    on_progress: Callable[[int, int], None] | None = None,
) -> list[Result]:
    """Query every address, in parallel, writing to the checkpoint as we go."""
    results: list[Result] = []
    done = 0
    total = len(addresses)

    if mode == "nonce":
        units: list[list[str]] = list(chunked(addresses, batch_size))
        work: Callable[[list[str]], list[Result]] = lambda batch: count_nonces(client, batch)
    else:
        units = [[addr] for addr in addresses]
        work = lambda batch: [count_transfers(client, batch[0], threshold, categories, exact)]

    if not units:
        return []

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
                on_progress(done, total)

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


def estimate(count: int, mode: str, batch_size: int, cu_per_second: int) -> tuple[int, float]:
    """Total compute units and a rough wall-clock estimate in seconds."""
    if mode == "nonce":
        cu = count * CU_NONCE
        requests_needed = -(-count // batch_size)
    else:
        cu = count * CU_TRANSFERS * 2  # worst case: both directions
        requests_needed = count * 2
    seconds = cu / cu_per_second if cu_per_second else 0.0
    return cu, max(seconds, requests_needed * 0.02)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Filter Ethereum wallets by transaction count.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("input", type=Path, help="CSV or newline-delimited list of addresses")
    parser.add_argument("--min-tx", type=int, default=4, help="keep wallets with at least this many txs (default: 4)")
    parser.add_argument(
        "--mode",
        choices=("nonce", "transfers"),
        default="nonce",
        help="nonce = outgoing txs, exact and fast (default); transfers = in+out, slower",
    )
    parser.add_argument("--url", default=os.environ.get("ALCHEMY_URL"), help="RPC URL (or set ALCHEMY_URL)")
    parser.add_argument("--out", type=Path, default=Path("out"), help="output directory (default: ./out)")
    parser.add_argument("--column", help="CSV column holding the address (default: auto-detect)")
    parser.add_argument("--batch-size", type=int, default=100, help="addresses per JSON-RPC batch (nonce mode)")
    parser.add_argument("--workers", type=int, default=5, help="concurrent requests (default: 5)")
    parser.add_argument(
        "--categories",
        default="external",
        help="comma-separated transfer categories for --mode transfers (default: external)",
    )
    parser.add_argument(
        "--exact-counts",
        action="store_true",
        help="transfers mode: count every transfer instead of stopping at the threshold (much slower)",
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

    cu, seconds = estimate(len(addresses), args.mode, args.batch_size, args.cu_per_second)
    print(f"mode={args.mode} min-tx={args.min_tx}  ~{cu:,} CU, ~{seconds / 60:.1f} min at {args.cu_per_second} CU/s")

    if args.dry_run:
        return 0

    if not args.url:
        print("no RPC URL: pass --url or set ALCHEMY_URL", file=sys.stderr)
        return 2

    args.out.mkdir(parents=True, exist_ok=True)
    checkpoint_path = args.out / "checkpoint.jsonl"
    if args.no_resume and checkpoint_path.exists():
        checkpoint_path.unlink()

    cached = load_checkpoint(checkpoint_path)
    pending = [addr for addr in addresses if addr not in cached]
    if cached:
        print(f"resuming: {len(cached)} already done, {len(pending)} to go")

    client = AlchemyClient(args.url)
    writer = CheckpointWriter(checkpoint_path)
    started = time.monotonic()

    def progress(done: int, total: int) -> None:
        elapsed = time.monotonic() - started
        rate = done / elapsed if elapsed > 0 else 0
        eta = (total - done) / rate if rate > 0 else 0
        sys.stderr.write(f"\r  {done}/{total}  {rate:.0f}/s  eta {eta / 60:.1f}m  429s:{client.rate_limit_hits}   ")
        sys.stderr.flush()

    try:
        fresh = run(
            client,
            pending,
            args.mode,
            args.min_tx,
            categories,
            args.batch_size,
            args.workers,
            writer,
            args.exact_counts,
            progress,
        )
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

    results = list(cached.values()) + fresh
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
