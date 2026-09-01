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
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Callable, Iterable, Iterator, Sequence

import requests

ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

# Compute-unit cost per RPC method, used only to estimate runtime up front.
CU_NONCE = 26
CU_TRANSFERS = 150
# Both are plain state reads, priced the same as a nonce, and batch the same way.
CU_BALANCE = 26
CU_CODE = 26

TRANSFER_CATEGORIES = ("external", "internal", "erc20", "erc721", "erc1155", "specialnft")

# alchemy_getAssetTransfers refuses maxCount above 1000.
MAX_TRANSFER_PAGE = 1000

# Batching is billed per wallet, so a batch of N nonces costs N * CU_NONCE in a
# single request. Exceed the plan's per-second budget with one request and it is
# rejected outright -- retries cannot help, because it never fits. So the batch
# is sized to the budget rather than picked by hand.
MAX_BATCH = 100


def safe_batch_size(cu_per_second: float) -> int:
    """Largest batch whose compute-unit cost still fits one second of budget."""
    if cu_per_second <= 0:
        return MAX_BATCH
    return max(1, min(MAX_BATCH, int(cu_per_second // CU_NONCE)))


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
    # Set when a screening criterion settled the wallet without counting it.
    # Only one is ever populated; both None means "decide on the tx count".
    rejected_by: str | None = None  # name of the criterion that excluded it
    kept_by: str | None = None  # name of the criterion that kept it outright

    @property
    def counted(self) -> bool:
        """False for wallets screened out before any counting happened."""
        return self.count >= 0

    def verdict(self, threshold: int) -> bool:
        """Did this wallet make the final cut?"""
        if self.error is not None:
            return False
        if self.rejected_by is not None:
            return False
        if self.kept_by is not None:
            return True
        return self.count >= threshold

    def reason(self, threshold: int) -> str:
        """Why the wallet landed where it did, for the output CSVs."""
        if self.rejected_by is not None:
            return self.rejected_by
        if self.kept_by is not None:
            return self.kept_by
        return "min_tx" if self.count < threshold else "tx_count"


class CuLimiter:
    """Token bucket over Alchemy compute units.

    Alchemy bills per compute unit and throttles on CU/second, so the honest way
    to stay inside the budget is to pace requests against it rather than fire at
    will and treat 429s as the brake. A sustained 429 storm is far more expensive
    than waiting: throughput collapses and wallets start failing outright.

    The rate halves on every 429 and drifts back up on success, so an unknown or
    lower plan tier converges to whatever the endpoint actually allows.
    """

    # Depart a little under the stated budget. Running exactly at the ceiling
    # leaves no room for clock skew or the endpoint's own accounting.
    HEADROOM = 0.9

    def __init__(self, cu_per_second: float) -> None:
        self.configured = float(cu_per_second)
        self.rate = self.configured
        self.floor = max(self.configured * 0.05, 10.0)
        self.next_free = time.monotonic()
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self.configured > 0

    def acquire(self, cost: float) -> None:
        """Block until this request's turn to depart.

        Requests are scheduled onto a timeline rather than drawn from a bucket.
        A bucket lets every worker fire at once the moment it has tokens, which
        is exactly the burst the endpoint rejects; spacing departures by
        cost/rate means instantaneous demand never exceeds the budget, however
        many workers there are.
        """
        if not self.enabled:
            return
        with self._lock:
            now = time.monotonic()
            start = max(now, self.next_free)
            self.next_free = start + cost / (self.rate * self.HEADROOM)
        wait = start - time.monotonic()
        if wait > 0:
            time.sleep(wait)

    def penalize(self) -> None:
        """Back off after a 429."""
        if not self.enabled:
            return
        with self._lock:
            self.rate = max(self.rate / 2, self.floor)

    def recover(self) -> None:
        """Drift back toward the configured rate after a success."""
        if not self.enabled:
            return
        with self._lock:
            if self.rate < self.configured:
                self.rate = min(self.configured, self.rate * 1.05)


class AlchemyClient:
    """Thin JSON-RPC client with per-thread sessions, CU pacing and retries."""

    def __init__(
        self,
        url: str,
        timeout: float = 30.0,
        max_retries: int = 8,
        cu_per_second: float = 0.0,
        session_factory: Callable[[], requests.Session] | None = None,
    ) -> None:
        self.url = url
        self.timeout = timeout
        self.max_retries = max_retries
        self.limiter = CuLimiter(cu_per_second)
        self._session_factory = session_factory or requests.Session
        self._local = threading.local()
        self._throttle_until = 0.0
        self._lock = threading.Lock()
        self.rate_limit_hits = 0
        self.successes = 0
        self._consecutive_rate_limits = 0

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

    # Enough consecutive rejections with nothing ever succeeding to conclude the
    # requests themselves do not fit, rather than the endpoint being busy.
    DOA_RATE_LIMITS = 8

    def _throttle(self, seconds: float) -> None:
        with self._lock:
            self.rate_limit_hits += 1
            self._consecutive_rate_limits += 1
            self._throttle_until = max(self._throttle_until, time.monotonic() + seconds)
            doomed = self.successes == 0 and self._consecutive_rate_limits >= self.DOA_RATE_LIMITS

        if doomed:
            # Nothing has ever got through, so waiting longer will not help: the
            # per-request cost is above the plan's ceiling and always will be.
            raise FatalRpcError(
                f"every request has been rate limited ({self.rate_limit_hits} in a row, none succeeded). "
                "The endpoint's real budget is lower than the CU/s this run was told to expect -- "
                "lower it (the free tier is 330) so requests are smaller and properly paced."
            )

    def call(self, payload: list[dict] | dict, cost: float = 0.0) -> list[dict] | dict:
        """POST a single request or a batch, retrying transient failures.

        `cost` is the request's compute-unit price, used to pace against the plan.
        """
        last_error: Exception | None = None

        for attempt in range(self.max_retries):
            self.limiter.acquire(cost)
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
                if self.successes == 0:
                    # Nothing has worked yet, so this looks like requests that do
                    # not fit rather than a busy endpoint. Backing off for half a
                    # minute only delays telling the user something is wrong.
                    delay = min(delay, 2.0)
                self.limiter.penalize()  # pace slower from here on, not just this once
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
                parsed = response.json()
            except ValueError as exc:
                last_error = exc
                time.sleep(min(2**attempt, 30))
                continue

            with self._lock:
                self.successes += 1
                self._consecutive_rate_limits = 0
            self.limiter.recover()
            return parsed

        raise RpcError(f"giving up after {self.max_retries} attempts: {last_error}")


# --------------------------------------------------------------------------
# screening criteria
# --------------------------------------------------------------------------


def format_eth(wei: int) -> str:
    """Render wei as a plain ETH string, without trailing zeros or float error."""
    whole, frac = divmod(int(wei), 10**18)
    if not frac:
        return str(whole)
    return f"{whole}.{frac:018d}".rstrip("0")


@dataclass
class Screen:
    """Criteria applied besides the transaction count.

    Ordered by what they cost. The lists are free and settle a wallet with no
    network at all; balance and code are one batched state read each. Anything a
    cheap criterion rejects never reaches the 150 CU inbound lookup, which is the
    same cheapest-first principle the counting stages already use.
    """

    denylist: frozenset[str] = frozenset()
    allowlist: frozenset[str] = frozenset()
    min_balance_wei: int = 0
    exclude_contracts: bool = False

    @property
    def needs_balance(self) -> bool:
        return self.min_balance_wei > 0

    @property
    def needs_code(self) -> bool:
        return self.exclude_contracts

    @property
    def active(self) -> bool:
        return bool(self.denylist or self.allowlist) or self.needs_balance or self.needs_code

    @property
    def cu_per_address(self) -> int:
        """Compute units the on-chain half of the screen costs per wallet."""
        return (CU_BALANCE if self.needs_balance else 0) + (CU_CODE if self.needs_code else 0)

    def describe(self) -> str:
        parts = []
        if self.denylist:
            parts.append(f"denylist={len(self.denylist)}")
        if self.allowlist:
            parts.append(f"allowlist={len(self.allowlist)}")
        if self.needs_balance:
            parts.append(f"min-balance={format_eth(self.min_balance_wei)} ETH")
        if self.exclude_contracts:
            parts.append("exclude-contracts")
        return " ".join(parts)


def load_address_list(path: Path) -> frozenset[str]:
    """Read a deny/allow list: one address per line, `#` comments allowed.

    Malformed lines are ignored rather than fatal -- these files are usually
    pasted together by hand, and one bad row should not stop a run.
    """
    addresses: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if ADDRESS_RE.match(line):
            addresses.add(line.lower())
    return frozenset(addresses)


def screen_local(addresses: Sequence[str], screen: Screen) -> tuple[list[Result], list[str]]:
    """Apply the free criteria. Returns (settled, still-to-check).

    The denylist wins over the allowlist: an address on both is a contradiction,
    and refusing is the safer way to resolve it.
    """
    settled: list[Result] = []
    remaining: list[str] = []

    for address in addresses:
        key = address.lower()
        if key in screen.denylist:
            settled.append(Result(address, -1, rejected_by="denylist"))
        elif key in screen.allowlist:
            settled.append(Result(address, -1, kept_by="allowlist"))
        else:
            remaining.append(address)

    return settled, remaining


def screen_onchain(client: AlchemyClient, addresses: Sequence[str], screen: Screen) -> list[Result]:
    """Balance and/or contract-code checks for a batch, in one round trip.

    Returns a Result only for wallets the screen *rejects*; anything that passes
    is left for the counting stages to decide, so nothing here can accidentally
    admit a wallet that has not met the transaction threshold.
    """
    if not (screen.needs_balance or screen.needs_code):
        return []

    payload: list[dict] = []
    slots: list[tuple[int, str, str]] = []  # (id, address, which check)
    for address in addresses:
        if screen.needs_balance:
            slots.append((len(payload), address, "balance"))
            payload.append(
                {"jsonrpc": "2.0", "id": len(payload), "method": "eth_getBalance", "params": [address, "latest"]}
            )
        if screen.needs_code:
            slots.append((len(payload), address, "code"))
            payload.append(
                {"jsonrpc": "2.0", "id": len(payload), "method": "eth_getCode", "params": [address, "latest"]}
            )

    try:
        raw = client.call(payload, cost=len(addresses) * screen.cu_per_address)
    except FatalRpcError:
        raise
    except RpcError as exc:
        return [Result(addr, -1, error=str(exc)) for addr in addresses]

    if isinstance(raw, dict):
        raw = [raw]
    by_id = {item.get("id"): item for item in raw if isinstance(item, dict)}

    rejected: dict[str, Result] = {}
    for request_id, address, which in slots:
        if address in rejected:
            continue  # already excluded by the other check in this batch
        item = by_id.get(request_id)
        if item is None or (item.get("error") if isinstance(item, dict) else None):
            detail = "missing from batch response" if item is None else str(item["error"])[:200]
            rejected[address] = Result(address, -1, error=detail)
            continue

        value = item.get("result")
        if which == "balance":
            try:
                balance = int(value, 16)
            except (TypeError, ValueError):
                rejected[address] = Result(address, -1, error=f"bad balance: {value!r}")
                continue
            if balance < screen.min_balance_wei:
                rejected[address] = Result(address, -1, rejected_by="min_balance")
        else:
            if not isinstance(value, str):
                rejected[address] = Result(address, -1, error=f"bad code: {value!r}")
                continue
            # Anything other than empty bytecode means this is a contract, not an EOA.
            if value not in ("0x", "0x0", ""):
                rejected[address] = Result(address, -1, rejected_by="contract")

    return list(rejected.values())


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
        raw = client.call(payload, cost=len(addresses) * CU_NONCE)
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

    raw = client.call(
        {"jsonrpc": "2.0", "id": 1, "method": "alchemy_getAssetTransfers", "params": [params]},
        cost=CU_TRANSFERS,
    )
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
                    rejected_by=record.get("rejected_by"),
                    kept_by=record.get("kept_by"),
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
        # A checkpoint also carries screening verdicts, so a changed screen makes
        # those stale in exactly the same way a changed threshold does.
        "screen": cfg.screen.describe(),
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
                            "rejected_by": result.rejected_by,
                            "kept_by": result.kept_by,
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
    screen: Screen = field(default_factory=Screen)


ProgressFn = Callable[[str, int, int], None]


class Cancelled(RuntimeError):
    """The caller asked the run to stop."""


def _execute(
    units: Sequence[list[str]],
    work: Callable[[list[str]], list[Result]],
    workers: int,
    checkpoint: CheckpointWriter | None,
    on_progress: ProgressFn | None,
    stage: str,
    should_stop: Callable[[], bool] | None = None,
) -> list[Result]:
    """Run one stage's units across a thread pool, checkpointing as answers land."""
    results: list[Result] = []
    if not units:
        return results

    done = 0
    total = sum(len(unit) for unit in units)

    def guarded(unit: list[str]) -> list[Result]:
        # Checked per unit rather than mid-request, so a stop lands on a clean
        # boundary and everything already answered stays in the checkpoint.
        if should_stop and should_stop():
            raise Cancelled()
        return work(unit)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(guarded, unit): unit for unit in units}
        for future in as_completed(futures):
            unit = futures[future]
            try:
                batch_results = future.result()
            except (RpcError, Cancelled):
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
    should_stop: Callable[[], bool] | None = None,
) -> list[Result]:
    """Resolve a transaction count for every address.

    In "both" mode this is two stages: batched nonces for everyone, then an
    inbound transfer lookup for only those wallets the nonce alone did not carry
    over the threshold. `resume_partials` are wallets from an earlier run whose
    stage one is already done, so they skip straight to stage two.
    """
    results: list[Result] = []
    pending_inbound: list[Result] = list(resume_partials)

    # Screening runs first and cheapest-first, so a wallet excluded by a list or
    # a 26 CU state read never reaches the counting stages at all.
    if cfg.screen.active:
        settled, addresses = screen_local(addresses, cfg.screen)
        if settled:
            results.extend(settled)
            if checkpoint:
                checkpoint.write(settled)
            if on_progress:
                on_progress("screen", len(settled), len(settled))

        if addresses and (cfg.screen.needs_balance or cfg.screen.needs_code):
            screened = _execute(
                list(chunked(addresses, cfg.batch_size)),
                lambda unit: screen_onchain(client, unit, cfg.screen),
                cfg.workers,
                checkpoint,
                on_progress,
                "screen",
                should_stop,
            )
            results.extend(screened)
            excluded = {r.address for r in screened}
            addresses = [addr for addr in addresses if addr not in excluded]

        # Wallets kept outright by the allowlist must not also be counted.
        skip = {r.address for r in results}
        pending_inbound = [r for r in pending_inbound if r.address not in skip]

    if cfg.mode == "transfers":
        # `results` already holds anything the screen settled, so it is carried
        # through rather than returned away.
        return results + _execute(
            [[addr] for addr in addresses],
            lambda unit: [count_transfers(client, unit[0], cfg.threshold, cfg.categories, cfg.exact)],
            cfg.workers,
            checkpoint,
            on_progress,
            "transfers",
            should_stop,
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
        should_stop,
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
            should_stop,
        )
    )
    return results


class OutputLocked(RuntimeError):
    """An output file could not be written because something else holds it open.

    On Windows an open Excel window locks the file outright. The run itself is
    finished and checkpointed at this point, so this must read as "close the
    file and re-run", not as a lost run.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        super().__init__(str(path))


def write_outputs(out_dir: Path, results: Sequence[Result], threshold: int) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    ordered = sorted(results, key=lambda r: (r.error is not None, -r.count, r.address))

    passed = [r for r in ordered if r.error is None and r.verdict(threshold)]
    filtered = [r for r in ordered if r.error is None and not r.verdict(threshold)]
    errored = [r for r in ordered if r.error is not None]

    def write_csv(name: str, header: list[str], rows: Iterable[list]) -> None:
        path = out_dir / name
        try:
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(header)
                writer.writerows(rows)
        except PermissionError as exc:
            raise OutputLocked(path) from exc

    def count_cell(r: Result) -> str | int:
        # Screened-out wallets were never counted, so a number would be a lie.
        if not r.counted:
            return ""
        return f">={r.count}" if r.capped else r.count

    write_csv(
        "passed.csv",
        ["address", "tx_count", "reason"],
        ([r.address, count_cell(r), r.reason(threshold)] for r in passed),
    )
    write_csv(
        "filtered.csv",
        ["address", "tx_count", "reason"],
        ([r.address, count_cell(r), r.reason(threshold)] for r in filtered),
    )
    write_csv(
        "results.csv",
        ["address", "tx_count", "passed", "reason"],
        (
            [r.address, count_cell(r), r.verdict(threshold), r.reason(threshold)]
            for r in ordered
            if r.error is None
        ),
    )
    if errored:
        write_csv("errors.csv", ["address", "error"], ([r.address, r.error] for r in errored))

    # Machine-readable outcome, for callers that are not a terminal -- CI job
    # summaries, the web app, anything wanting the split without parsing stdout.
    breakdown: dict[str, int] = {}
    for r in filtered:
        key = r.reason(threshold)
        breakdown[key] = breakdown.get(key, 0) + 1

    summary = {
        "passed": len(passed),
        "filtered": len(filtered),
        "errors": len(errored),
        "min_tx": threshold,
        "total": len(passed) + len(filtered),
        # Which criterion removed each wallet, so a run can be explained without
        # re-reading the CSVs.
        "filtered_by": breakdown,
    }
    try:
        (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    except PermissionError as exc:
        raise OutputLocked(out_dir / "summary.json") from exc

    return {"passed": len(passed), "filtered": len(filtered), "errors": len(errored)}


def estimate(
    count: int, mode: str, cu_per_second: int, screen: Screen | None = None
) -> tuple[int, int, float, float]:
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

    if screen is not None and screen.cu_per_address:
        # The screen is paid up front for everyone, but each wallet it rejects
        # saves the whole counting cost -- so it raises the floor and lowers the
        # ceiling at the same time.
        cu_low += count * screen.cu_per_address
        cu_high += count * screen.cu_per_address

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
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="addresses per JSON-RPC batch (default: derived from --cu-per-second)",
    )
    parser.add_argument("--workers", type=int, default=3, help="concurrent requests (default: 3)")
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
    parser.add_argument(
        "--cu-per-second",
        type=int,
        default=330,
        help="your plan's compute units per second -- requests are paced to it (default: 330, the free tier; 0 = no pacing)",
    )
    screen_group = parser.add_argument_group(
        "screening",
        "Extra criteria applied besides the transaction count. Cheapest first: the "
        "lists cost nothing, balance and code are one batched state read each, and "
        "anything rejected never pays for a transfer lookup.",
    )
    screen_group.add_argument(
        "--denylist",
        type=Path,
        help="file of addresses to always reject (one per line, # comments allowed)",
    )
    screen_group.add_argument(
        "--allowlist",
        type=Path,
        help="file of addresses to always keep, without counting them",
    )
    screen_group.add_argument(
        "--min-balance",
        default="0",
        help="reject wallets holding less than this much ETH (e.g. 0.01); default 0 = no check",
    )
    screen_group.add_argument(
        "--exclude-contracts",
        action="store_true",
        help="reject addresses that have contract code (keep externally-owned accounts only)",
    )
    parser.add_argument("--no-resume", action="store_true", help="ignore any existing checkpoint and start over")
    parser.add_argument("--no-retry", action="store_true", help="skip the automatic retry pass over failed wallets")
    parser.add_argument("--dry-run", action="store_true", help="parse the input and print an estimate, no network")
    return parser


def parse_eth_amount(raw: str) -> int:
    """Turn an ETH amount like "0.01" into wei, exactly.

    Decimal string handling rather than float maths, so 0.1 means 0.1 and not
    0.09999999999999999.
    """
    text = str(raw).strip()
    if not text:
        return 0
    if not re.fullmatch(r"\d*(\.\d*)?", text) or text in (".", ""):
        raise ValueError(f"--min-balance must be a plain ETH amount, got {raw!r}")
    whole, _, frac = text.partition(".")
    frac = (frac + "0" * 18)[:18]
    return int(whole or "0") * 10**18 + int(frac or "0")


def build_screen(args: argparse.Namespace) -> Screen:
    """Assemble the screening criteria from parsed CLI arguments."""

    def read_list(path: Path | None, label: str) -> frozenset[str]:
        if path is None:
            return frozenset()
        if not path.exists():
            raise OSError(f"{label} file not found: {path}")
        return load_address_list(path)

    return Screen(
        denylist=read_list(getattr(args, "denylist", None), "--denylist"),
        allowlist=read_list(getattr(args, "allowlist", None), "--allowlist"),
        min_balance_wei=parse_eth_amount(getattr(args, "min_balance", "0") or "0"),
        exclude_contracts=bool(getattr(args, "exclude_contracts", False)),
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.min_tx < 1:
        print("--min-tx must be at least 1", file=sys.stderr)
        return 2
    if not args.input.exists():
        print(f"input file not found: {args.input}", file=sys.stderr)
        return 2

    try:
        screen = build_screen(args)
    except (OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
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

    cu_low, cu_high, sec_low, sec_high = estimate(len(addresses), args.mode, args.cu_per_second, screen)
    if cu_low == cu_high:
        cost = f"~{cu_low:,} CU, ~{sec_low / 60:.1f} min"
    else:
        cost = f"{cu_low:,}-{cu_high:,} CU, {sec_low / 60:.0f}-{sec_high / 60:.0f} min"
    batching = args.batch_size if args.batch_size else safe_batch_size(args.cu_per_second)
    print(f"mode={args.mode} min-tx={args.min_tx}  {cost} at {args.cu_per_second} CU/s, batches of {batching}")
    if screen.active:
        print(f"screening: {screen.describe()}")

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

    batch_size = args.batch_size if args.batch_size else safe_batch_size(args.cu_per_second)
    if args.batch_size and args.cu_per_second and args.batch_size * CU_NONCE > args.cu_per_second:
        print(
            f"warning: a batch of {args.batch_size} costs {args.batch_size * CU_NONCE:,} CU, over the "
            f"{args.cu_per_second:,} CU/s budget -- those requests will be rejected, not merely slowed",
            file=sys.stderr,
        )

    cfg = Config(
        mode=args.mode,
        threshold=args.min_tx,
        categories=categories,
        batch_size=batch_size,
        workers=args.workers,
        exact=args.exact_counts,
        screen=screen,
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
    client = AlchemyClient(args.url, cu_per_second=args.cu_per_second)
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

        # A rate-limit storm can leave a lot of wallets unresolved. Retry them
        # single-threaded before reporting, since a run that answers two thirds
        # of the list is not an answer at all.
        failed = [r.address for r in fresh if r.error]
        if failed and not args.no_retry:
            sys.stderr.write(f"\n  retrying {len(failed):,} wallets that failed, more slowly\n")
            retried = run(client, failed, replace(cfg, workers=1), writer, progress)
            fresh = [r for r in fresh if not r.error] + retried
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
    try:
        counts = write_outputs(args.out, results, args.min_tx)
    except OutputLocked as locked:
        print(f"\ncannot write {locked.path}: another program has it open.", file=sys.stderr)
        print("On Windows that is usually Excel. Close the file and re-run -- every wallet is", file=sys.stderr)
        print("already answered in the checkpoint, so it will finish instantly without querying", file=sys.stderr)
        print(f"anything. Or write elsewhere with --out {args.out}2", file=sys.stderr)
        return 1

    total = counts["passed"] + counts["filtered"]
    share = counts["passed"] / total * 100 if total else 0.0
    print(f"\npassed   (>= {args.min_tx} tx): {counts['passed']:,}  ({share:.1f}%)")
    print(f"filtered (<  {args.min_tx} tx): {counts['filtered']:,}")
    if counts["errors"]:
        print(f"errors:                {counts['errors']:,}  -- see {args.out / 'errors.csv'}")
        print(f"\nWARNING: {counts['errors']:,} of {len(addresses):,} wallets could not be resolved.")
        print("The split above only covers the rest, so do not use it yet -- re-run to retry just those.")
        if client.rate_limit_hits:
            print(f"Hit {client.rate_limit_hits:,} rate limits; try --cu-per-second {max(args.cu_per_second // 2, 25)}")
    print(f"\nwrote {args.out}/passed.csv, filtered.csv, results.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
