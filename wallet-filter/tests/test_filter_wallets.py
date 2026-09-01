"""Tests for filter_wallets, using a fake RPC server instead of the network."""

from __future__ import annotations

import csv
import json
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import filter_wallets as fw  # noqa: E402


def addr(n: int) -> str:
    return "0x" + f"{n:040x}"


class FakeResponse:
    def __init__(self, payload, status_code=200, headers=None):
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {}
        self.text = json.dumps(payload) if payload is not None else ""

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


class FakeSession:
    """Answers eth_getTransactionCount and alchemy_getAssetTransfers from a dict."""

    def __init__(self, nonces=None, transfers=None, script=None, balances=None, codes=None):
        self.nonces = nonces or {}
        self.transfers = transfers or {}
        self.balances = balances or {}  # address -> wei
        self.codes = codes or {}  # address -> bytecode hex ("0x" for an EOA)
        self.script = list(script or [])  # queued canned responses, popped first
        self.calls = []

    def _batch_result(self, item):
        """Answer one entry of a JSON-RPC batch according to its method."""
        method = item.get("method")
        address = item["params"][0]
        if method == "eth_getBalance":
            return hex(self.balances.get(address, 0))
        if method == "eth_getCode":
            return self.codes.get(address, "0x")
        return hex(self.nonces.get(address, 0))

    def post(self, url, json=None, timeout=None):  # noqa: A002
        self.calls.append(json)
        if self.script:
            canned = self.script.pop(0)
            if isinstance(canned, Exception):
                raise canned
            return canned

        if isinstance(json, list):
            return FakeResponse(
                [
                    {"jsonrpc": "2.0", "id": item["id"], "result": self._batch_result(item)}
                    for item in json
                ]
            )

        params = json["params"][0]
        direction = "fromAddress" if "fromAddress" in params else "toAddress"
        target = params[direction]
        available = self.transfers.get((target, direction), 0)
        wanted = int(params["maxCount"], 16)
        served = min(available, wanted)
        return FakeResponse({"jsonrpc": "2.0", "id": 1, "result": {"transfers": [{}] * served, "pageKey": None}})


def client_for(session: FakeSession) -> fw.AlchemyClient:
    return fw.AlchemyClient("http://fake", session_factory=lambda: session, max_retries=3)


# ---------------------------------------------------------------- input parsing


def test_loads_bare_address_list(tmp_path):
    path = tmp_path / "w.csv"
    path.write_text(f"{addr(1)}\n{addr(2)}\n")
    addresses, malformed = fw.load_addresses(path)
    assert addresses == [addr(1), addr(2)]
    assert malformed == []


def test_header_and_multiple_columns(tmp_path):
    path = tmp_path / "w.csv"
    path.write_text(f"name,wallet,note\nalice,{addr(1)},hi\nbob,{addr(2)},yo\n")
    addresses, _ = fw.load_addresses(path)
    assert addresses == [addr(1), addr(2)]


def test_explicit_column(tmp_path):
    path = tmp_path / "w.csv"
    path.write_text(f"a,b\n{addr(1)},{addr(2)}\n")
    addresses, _ = fw.load_addresses(path, column="b")
    assert addresses == [addr(2)]


def test_dedupes_case_insensitively_and_keeps_order(tmp_path):
    path = tmp_path / "w.csv"
    path.write_text(f"{addr(2)}\n{addr(2).upper().replace('0X', '0x')}\n{addr(1)}\n")
    addresses, _ = fw.load_addresses(path)
    assert addresses == [addr(2), addr(1)]


def test_reports_malformed_rows(tmp_path):
    path = tmp_path / "w.csv"
    path.write_text(f"{addr(1)}\nnot-an-address\n0x123\n\n")
    addresses, malformed = fw.load_addresses(path)
    assert addresses == [addr(1)]
    assert malformed == ["not-an-address", "0x123"]


def test_handles_bom_and_crlf(tmp_path):
    path = tmp_path / "w.csv"
    path.write_bytes(f"﻿{addr(1)}\r\n{addr(2)}\r\n".encode())
    addresses, malformed = fw.load_addresses(path)
    assert addresses == [addr(1), addr(2)]
    assert malformed == []


# ---------------------------------------------------------------- nonce mode


def test_nonce_batch_counts():
    session = FakeSession(nonces={addr(1): 7, addr(2): 0, addr(3): 4})
    results = fw.count_nonces(client_for(session), [addr(1), addr(2), addr(3)])
    assert {r.address: r.count for r in results} == {addr(1): 7, addr(2): 0, addr(3): 4}
    assert len(session.calls) == 1  # one round trip for the whole batch


def test_nonce_batch_response_out_of_order():
    session = FakeSession(
        script=[
            FakeResponse(
                [
                    {"id": 2, "result": "0x9"},
                    {"id": 0, "result": "0x1"},
                    {"id": 1, "result": "0x0"},
                ]
            )
        ]
    )
    results = fw.count_nonces(client_for(session), [addr(1), addr(2), addr(3)])
    assert {r.address: r.count for r in results} == {addr(1): 1, addr(2): 0, addr(3): 9}


def test_per_address_rpc_error_is_isolated():
    session = FakeSession(script=[FakeResponse([{"id": 0, "result": "0x2"}, {"id": 1, "error": {"message": "boom"}}])])
    results = fw.count_nonces(client_for(session), [addr(1), addr(2)])
    by_addr = {r.address: r for r in results}
    assert by_addr[addr(1)].count == 2
    assert by_addr[addr(2)].error and by_addr[addr(2)].count == -1


def test_missing_entry_in_batch_response_is_an_error():
    session = FakeSession(script=[FakeResponse([{"id": 0, "result": "0x2"}])])
    results = fw.count_nonces(client_for(session), [addr(1), addr(2)])
    assert {r.address: r.error is not None for r in results} == {addr(1): False, addr(2): True}


# ---------------------------------------------------------------- transfers mode


def test_transfers_seeded_start_counts_toward_threshold():
    session = FakeSession(transfers={(addr(1), "toAddress"): 2})
    result = fw.count_transfers(
        client_for(session), addr(1), threshold=4, categories=["external"], directions=("toAddress",), start=1
    )
    assert result.count == 3  # 1 outbound seeded + 2 inbound


def test_transfers_skips_the_call_when_start_already_clears_threshold():
    session = FakeSession(transfers={(addr(1), "toAddress"): 99})
    result = fw.count_transfers(
        client_for(session), addr(1), threshold=4, categories=["external"], directions=("toAddress",), start=4
    )
    assert result.count == 4 and result.capped
    assert session.calls == []  # no compute units spent at all


def test_transfers_counts_both_directions():
    session = FakeSession(transfers={(addr(1), "fromAddress"): 1, (addr(1), "toAddress"): 2})
    result = fw.count_transfers(client_for(session), addr(1), threshold=4, categories=["external"])
    assert result.count == 3
    assert not result.capped


def test_transfers_stops_early_at_threshold():
    session = FakeSession(transfers={(addr(1), "fromAddress"): 50, (addr(1), "toAddress"): 50})
    result = fw.count_transfers(client_for(session), addr(1), threshold=4, categories=["external"])
    assert result.count == 4 and result.capped
    assert len(session.calls) == 1  # never had to ask about the inbound side


def test_transfers_error_is_recorded():
    session = FakeSession(script=[FakeResponse({"id": 1, "error": {"message": "bad request"}})])
    result = fw.count_transfers(client_for(session), addr(1), threshold=4, categories=["external"])
    assert result.count == -1 and "bad request" in result.error


# ---------------------------------------------------------------- transport


def test_retries_on_429_then_succeeds(monkeypatch):
    monkeypatch.setattr(fw.time, "sleep", lambda _s: None)
    session = FakeSession(
        script=[
            FakeResponse(None, status_code=429, headers={"Retry-After": "0"}),
            FakeResponse([{"id": 0, "result": "0x5"}]),
        ]
    )
    client = client_for(session)
    results = fw.count_nonces(client, [addr(1)])
    assert results[0].count == 5
    assert client.rate_limit_hits == 1


def test_retries_on_5xx(monkeypatch):
    monkeypatch.setattr(fw.time, "sleep", lambda _s: None)
    session = FakeSession(script=[FakeResponse(None, status_code=503), FakeResponse([{"id": 0, "result": "0x3"}])])
    assert fw.count_nonces(client_for(session), [addr(1)])[0].count == 3


def test_auth_failure_is_fatal_not_retried():
    session = FakeSession(script=[FakeResponse(None, status_code=401)])
    with pytest.raises(fw.FatalRpcError, match="API key"):
        client_for(session).call([{"id": 0}])
    assert len(session.calls) == 1


def test_auth_failure_propagates_rather_than_marking_every_wallet_errored():
    # A bad key must not quietly produce a list where nothing passed.
    session = FakeSession(script=[FakeResponse(None, status_code=401)])
    with pytest.raises(fw.FatalRpcError):
        fw.count_nonces(client_for(session), [addr(1), addr(2)])

    session = FakeSession(script=[FakeResponse(None, status_code=401)])
    with pytest.raises(fw.FatalRpcError):
        fw.count_transfers(client_for(session), addr(1), threshold=4, categories=["external"])


def test_gives_up_after_max_retries(monkeypatch):
    monkeypatch.setattr(fw.time, "sleep", lambda _s: None)
    session = FakeSession(script=[FakeResponse(None, status_code=503)] * 3)
    results = fw.count_nonces(client_for(session), [addr(1)])
    assert results[0].error and results[0].count == -1


# ---------------------------------------------------------------- both mode


def both_cfg(**kw):
    return fw.Config(mode="both", threshold=4, categories=["external"], batch_size=100, workers=2, **kw)


def test_both_adds_inbound_to_outbound():
    # 1 sent + 3 received = 4, which clears a threshold that outbound alone misses.
    session = FakeSession(nonces={addr(1): 1}, transfers={(addr(1), "toAddress"): 3})
    results = fw.run(client_for(session), [addr(1)], both_cfg())
    assert [(r.address, r.count) for r in results] == [(addr(1), 4)]


def test_both_still_fails_a_wallet_that_is_short_on_each_side():
    session = FakeSession(nonces={addr(1): 1}, transfers={(addr(1), "toAddress"): 1})
    results = fw.run(client_for(session), [addr(1)], both_cfg())
    assert results[0].count == 2 and not results[0].capped


def test_both_counts_a_receive_only_wallet():
    # Nonce 0, but plenty of inbound -- the case that motivated in+out counting.
    session = FakeSession(nonces={addr(1): 0}, transfers={(addr(1), "toAddress"): 9})
    results = fw.run(client_for(session), [addr(1)], both_cfg())
    assert results[0].count >= 4


def test_both_skips_the_inbound_lookup_when_the_nonce_already_passes():
    session = FakeSession(nonces={addr(1): 4, addr(2): 99}, transfers={})
    results = fw.run(client_for(session), [addr(1), addr(2)], both_cfg())
    assert all(r.count >= 4 for r in results)
    assert len(session.calls) == 1  # one batched nonce call, zero transfer calls


def test_both_only_pays_for_transfers_on_the_wallets_that_need_them():
    addresses = [addr(i) for i in range(1, 11)]
    passing = set(addresses[:6])  # six clear the bar on outbound alone
    nonces = {a: (9 if a in passing else 0) for a in addresses}
    session = FakeSession(nonces=nonces, transfers={})
    fw.run(client_for(session), addresses, both_cfg())
    transfer_calls = [c for c in session.calls if isinstance(c, dict)]
    assert len(transfer_calls) == len(addresses) - len(passing) == 4


def test_both_reports_progress_for_each_stage():
    session = FakeSession(nonces={addr(1): 0}, transfers={(addr(1), "toAddress"): 1})
    stages = []
    fw.run(client_for(session), [addr(1)], both_cfg(), on_progress=lambda s, d, t: stages.append(s))
    assert stages == ["outbound", "inbound"]


def test_both_keeps_stage_one_errors_out_of_stage_two():
    session = FakeSession(script=[FakeResponse([{"id": 0, "error": {"message": "boom"}}])])
    results = fw.run(client_for(session), [addr(1)], both_cfg())
    assert results[0].error and len(session.calls) == 1


def test_nonce_mode_never_runs_the_inbound_stage():
    session = FakeSession(nonces={addr(1): 0}, transfers={(addr(1), "toAddress"): 50})
    cfg = fw.Config(mode="nonce", threshold=4, categories=["external"], batch_size=100, workers=2)
    results = fw.run(client_for(session), [addr(1)], cfg)
    assert results[0].count == 0
    assert len(session.calls) == 1


def test_both_resumes_partials_without_refetching_nonces():
    session = FakeSession(nonces={addr(1): 1}, transfers={(addr(1), "toAddress"): 3})
    partial = fw.Result(addr(1), 1, partial=True)
    results = fw.run(client_for(session), [], both_cfg(), resume_partials=[partial])
    assert results[0].count == 4
    assert all(isinstance(c, dict) for c in session.calls)  # transfer calls only, no batch


# ---------------------------------------------------------------- CU pacing


def test_limiter_paces_to_the_budget(monkeypatch):
    slept = []
    monkeypatch.setattr(fw.time, "sleep", lambda s: slept.append(s))
    limiter = fw.CuLimiter(100)

    limiter.acquire(100)  # first request departs immediately
    assert slept == []
    limiter.acquire(100)  # the next waits roughly a second's worth of budget
    assert slept and 1.0 <= slept[-1] <= 1.2  # 1/HEADROOM


def test_limiter_spaces_concurrent_workers_instead_of_bursting():
    # The failure this guards against: several workers each firing a
    # near-budget request at the same instant, which the endpoint rejects.
    limiter = fw.CuLimiter(330)
    start = time.monotonic()
    results = []

    def worker():
        limiter.acquire(312)  # a 12-wallet nonce batch
        results.append(time.monotonic() - start)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    departures = sorted(results)
    gaps = [b - a for a, b in zip(departures, departures[1:])]
    assert all(gap > 0.5 for gap in gaps), f"requests bunched up: {gaps}"


def test_limiter_disabled_at_zero(monkeypatch):
    monkeypatch.setattr(fw.time, "sleep", lambda s: pytest.fail("should not sleep"))
    limiter = fw.CuLimiter(0)
    assert not limiter.enabled
    for _ in range(100):
        limiter.acquire(10_000)


def test_limiter_halves_on_429_and_drifts_back_up():
    limiter = fw.CuLimiter(320)
    limiter.penalize()
    limiter.penalize()
    assert limiter.rate == 80  # 320 -> 160 -> 80

    for _ in range(200):
        limiter.recover()
    assert limiter.rate == 320  # never overshoots the configured rate


def test_limiter_penalty_has_a_floor():
    limiter = fw.CuLimiter(330)
    for _ in range(50):
        limiter.penalize()
    assert limiter.rate == limiter.floor > 0  # throttled, never stalled


def test_client_prices_requests_by_compute_unit(monkeypatch):
    monkeypatch.setattr(fw.time, "sleep", lambda _s: None)
    session = FakeSession(nonces={addr(i): 1 for i in range(1, 6)})
    client = fw.AlchemyClient("http://fake", cu_per_second=1000, session_factory=lambda: session)

    costs = []
    real_acquire = client.limiter.acquire
    client.limiter.acquire = lambda cost: (costs.append(cost), real_acquire(cost))[1]

    fw.count_nonces(client, [addr(i) for i in range(1, 6)])
    assert costs == [5 * fw.CU_NONCE]  # batch priced by how many wallets it carries

    costs.clear()
    fw.count_transfers(client, addr(1), threshold=4, categories=["external"], directions=("toAddress",))
    assert costs == [fw.CU_TRANSFERS]


def test_endless_rate_limits_with_no_success_fail_fast(monkeypatch):
    # The real-world case: the stated plan is higher than the key's actual plan,
    # so every request is too expensive to ever fit. Grinding through retries
    # forever is worse than saying so.
    monkeypatch.setattr(fw.time, "sleep", lambda _s: None)
    monkeypatch.setattr(fw.AlchemyClient, "_wait_for_throttle", lambda self: None)
    session = FakeSession(script=[FakeResponse(None, status_code=429)] * 200)
    client = fw.AlchemyClient("http://fake", cu_per_second=0, max_retries=100, session_factory=lambda: session)

    with pytest.raises(fw.FatalRpcError, match="lower it"):
        client.call([{"id": 0}], cost=2600)
    assert len(session.calls) < 20  # gave up early rather than burning 100 attempts


def test_rate_limits_after_a_success_do_not_fail_fast(monkeypatch):
    # A busy endpoint that is still making progress must not be killed off.
    monkeypatch.setattr(fw.time, "sleep", lambda _s: None)
    monkeypatch.setattr(fw.AlchemyClient, "_wait_for_throttle", lambda self: None)
    session = FakeSession(
        script=[FakeResponse([{"id": 0, "result": "0x1"}])] + [FakeResponse(None, status_code=429)] * 40
    )
    client = fw.AlchemyClient("http://fake", cu_per_second=0, max_retries=30, session_factory=lambda: session)

    client.call([{"id": 0}], cost=26)  # one success on the board
    with pytest.raises(fw.RpcError) as caught:
        client.call([{"id": 0}], cost=26)
    assert not isinstance(caught.value, fw.FatalRpcError)  # exhausted retries, not declared doomed


def test_a_429_slows_the_client_down_for_later_calls(monkeypatch):
    monkeypatch.setattr(fw.time, "sleep", lambda _s: None)
    session = FakeSession(
        script=[
            FakeResponse(None, status_code=429, headers={"Retry-After": "0"}),
            FakeResponse([{"id": 0, "result": "0x5"}]),
        ]
    )
    client = fw.AlchemyClient("http://fake", cu_per_second=330, session_factory=lambda: session)
    before = client.limiter.rate
    fw.count_nonces(client, [addr(1)])
    assert client.limiter.rate < before  # paced slower from here on, not just retried


def test_batch_size_fits_the_budget():
    # A batch costs len * 26 CU in one request. Over the per-second budget and the
    # endpoint rejects it outright, which no amount of retrying can fix.
    for cu in (330, 660, 3000):
        assert fw.safe_batch_size(cu) * fw.CU_NONCE <= cu

    assert fw.safe_batch_size(330) == 12  # free tier
    assert fw.safe_batch_size(0) == fw.MAX_BATCH  # pacing off, batch freely
    assert fw.safe_batch_size(1) == 1  # never zero
    assert fw.safe_batch_size(10**9) == fw.MAX_BATCH  # capped


def test_oversized_manual_batch_is_flagged(tmp_path, monkeypatch, capsys):
    source = tmp_path / "wallets.csv"
    source.write_text(f"{addr(1)}\n")
    session = FakeSession(nonces={addr(1): 9})
    monkeypatch.setattr(fw.requests, "Session", lambda: session)

    args = [str(source), "--url", "http://fake", "--out", str(tmp_path / "out"), "--batch-size", "100"]
    assert fw.main(args) == 0
    assert "will be rejected" in capsys.readouterr().err


# ---------------------------------------------------------------- checkpointing


def test_checkpoint_roundtrip_skips_errors(tmp_path):
    path = tmp_path / "checkpoint.jsonl"
    writer = fw.CheckpointWriter(path)
    writer.write([fw.Result(addr(1), 5), fw.Result(addr(2), -1, error="boom")])
    writer.close()

    done = fw.load_checkpoint(path)
    assert set(done) == {addr(1)}  # the failed one is retried on resume
    assert done[addr(1)].count == 5


def test_checkpoint_preserves_partial_flag(tmp_path):
    path = tmp_path / "checkpoint.jsonl"
    writer = fw.CheckpointWriter(path)
    writer.write([fw.Result(addr(1), 2, partial=True), fw.Result(addr(2), 9)])
    writer.close()

    done = fw.load_checkpoint(path)
    assert done[addr(1)].partial and done[addr(1)].count == 2
    assert not done[addr(2)].partial


def test_later_checkpoint_record_supersedes_its_partial(tmp_path):
    path = tmp_path / "checkpoint.jsonl"
    writer = fw.CheckpointWriter(path)
    writer.write([fw.Result(addr(1), 2, partial=True)])
    writer.write([fw.Result(addr(1), 5)])  # stage two answer for the same wallet
    writer.close()

    done = fw.load_checkpoint(path)
    assert done[addr(1)].count == 5 and not done[addr(1)].partial


def test_checkpoint_tolerates_truncated_last_line(tmp_path):
    path = tmp_path / "checkpoint.jsonl"
    path.write_text(json.dumps({"address": addr(1), "count": 5}) + '\n{"address": "0x00')
    assert set(fw.load_checkpoint(path)) == {addr(1)}


# ---------------------------------------------------------------- outputs


def test_write_outputs_splits_on_threshold(tmp_path):
    results = [
        fw.Result(addr(1), 9),
        fw.Result(addr(2), 4),
        fw.Result(addr(3), 3),
        fw.Result(addr(4), 0),
        fw.Result(addr(5), -1, error="boom"),
    ]
    counts = fw.write_outputs(tmp_path, results, threshold=4)
    assert counts == {"passed": 2, "filtered": 2, "errors": 1}

    passed = list(csv.DictReader((tmp_path / "passed.csv").open()))
    assert [row["address"] for row in passed] == [addr(1), addr(2)]

    filtered = list(csv.DictReader((tmp_path / "filtered.csv").open()))
    assert [row["address"] for row in filtered] == [addr(3), addr(4)]

    assert [row["address"] for row in csv.DictReader((tmp_path / "errors.csv").open())] == [addr(5)]


def test_capped_counts_render_with_a_prefix(tmp_path):
    fw.write_outputs(tmp_path, [fw.Result(addr(1), 4, capped=True)], threshold=4)
    assert list(csv.DictReader((tmp_path / "passed.csv").open()))[0]["tx_count"] == ">=4"


def test_locked_output_reports_which_file(tmp_path, monkeypatch):
    # Windows: Excel holding passed.csv open makes it unwritable. The run is
    # finished and checkpointed by then, so this must be a clear message.
    real_open = Path.open

    def deny_passed(self, *args, **kwargs):
        if self.name == "passed.csv" and "w" in (args[0] if args else kwargs.get("mode", "r")):
            raise PermissionError(13, "Permission denied")
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", deny_passed)
    with pytest.raises(fw.OutputLocked) as caught:
        fw.write_outputs(tmp_path, [fw.Result(addr(1), 9)], threshold=4)
    assert caught.value.path.name == "passed.csv"


def test_locked_output_exits_cleanly_instead_of_a_traceback(tmp_path, monkeypatch, capsys):
    source = tmp_path / "wallets.csv"
    source.write_text(f"{addr(1)}\n")
    out = tmp_path / "out"
    session = FakeSession(nonces={addr(1): 9})
    monkeypatch.setattr(fw.requests, "Session", lambda: session)
    monkeypatch.setattr(fw, "write_outputs", lambda *a, **k: (_ for _ in ()).throw(fw.OutputLocked(out / "passed.csv")))

    args = [str(source), "--url", "http://fake", "--cu-per-second", "0", "--out", str(out)]
    assert fw.main(args) == 1
    err = capsys.readouterr().err
    assert "passed.csv" in err and "Excel" in err and "checkpoint" in err


def test_summary_json_matches_the_csvs(tmp_path):
    results = [fw.Result(addr(1), 9), fw.Result(addr(2), 1), fw.Result(addr(3), -1, error="boom")]
    fw.write_outputs(tmp_path, results, threshold=4)

    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary == {
        "passed": 1,
        "filtered": 1,
        "errors": 1,
        "min_tx": 4,
        "total": 2,
        "filtered_by": {"min_tx": 1},
    }


def test_ci_summary_rendering(tmp_path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "summarize", Path(__file__).resolve().parents[2] / ".github" / "scripts" / "summarize.py"
    )
    summarize = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(summarize)

    fw.write_outputs(tmp_path, [fw.Result(addr(1), 9), fw.Result(addr(2), 1)], threshold=4)
    clean = summarize.render(tmp_path / "summary.json")
    assert "**1** (50.0%)" in clean and "Unresolved" not in clean

    fw.write_outputs(tmp_path, [fw.Result(addr(1), 9), fw.Result(addr(2), -1, error="x")], threshold=4)
    with_errors = summarize.render(tmp_path / "summary.json")
    assert "Unresolved" in with_errors and "Re-run this workflow" in with_errors

    # A run that died before writing anything must not render a fake split.
    missing = summarize.render(tmp_path / "nope.json")
    assert "did not finish" in missing and "resume" in missing


def test_no_errors_file_when_clean(tmp_path):
    fw.write_outputs(tmp_path, [fw.Result(addr(1), 9)], threshold=4)
    assert not (tmp_path / "errors.csv").exists()


# ---------------------------------------------------------------- end to end


def test_full_run_resumes_from_checkpoint(tmp_path, monkeypatch):
    addresses = [addr(i) for i in range(1, 251)]
    nonces = {a: (i % 8) for i, a in enumerate(addresses)}
    source = tmp_path / "wallets.csv"
    source.write_text("\n".join(addresses) + "\n")

    out = tmp_path / "out"
    session = FakeSession(nonces=nonces)
    monkeypatch.setattr(fw.requests, "Session", lambda: session)

    args = [str(source), "--min-tx", "4", "--mode", "nonce", "--url", "http://fake", "--cu-per-second", "0", "--out", str(out)]
    assert fw.main(args) == 0

    expected_pass = sum(1 for a in addresses if nonces[a] >= 4)
    assert len(list(csv.DictReader((out / "passed.csv").open()))) == expected_pass
    assert len(list(csv.DictReader((out / "filtered.csv").open()))) == len(addresses) - expected_pass

    # Second run: everything is in the checkpoint, so no further RPC calls.
    before = len(session.calls)
    assert fw.main(args) == 0
    assert len(session.calls) == before
    assert len(list(csv.DictReader((out / "passed.csv").open()))) == expected_pass


def test_full_both_mode_run_resumes_mid_stage(tmp_path, monkeypatch):
    addresses = [addr(i) for i in range(1, 121)]
    # A third clear the bar on outbound alone; the rest need inbound to decide.
    nonces = {a: (9 if i % 3 == 0 else 1) for i, a in enumerate(addresses)}
    inbound = {(a, "toAddress"): (5 if i % 2 == 0 else 0) for i, a in enumerate(addresses)}
    source = tmp_path / "wallets.csv"
    source.write_text("\n".join(addresses) + "\n")

    out = tmp_path / "out"
    session = FakeSession(nonces=nonces, transfers=inbound)
    monkeypatch.setattr(fw.requests, "Session", lambda: session)
    args = [str(source), "--min-tx", "4", "--url", "http://fake", "--cu-per-second", "0", "--out", str(out), "--workers", "2"]

    assert fw.main(args) == 0
    expected = {a for i, a in enumerate(addresses) if nonces[a] + inbound[(a, "toAddress")] >= 4}
    assert {row["address"] for row in csv.DictReader((out / "passed.csv").open())} == expected

    # Truncate the checkpoint to a point where stage one is done but stage two
    # is only half finished, then resume and expect the identical answer.
    lines = (out / "checkpoint.jsonl").read_text().splitlines()
    partial_count = sum(1 for line in lines if json.loads(line).get("partial"))
    keep = [line for line in lines if json.loads(line).get("partial")]
    keep += [line for line in lines if not json.loads(line).get("partial")][: partial_count // 2]
    (out / "checkpoint.jsonl").write_text("\n".join(keep) + "\n")

    before = len(session.calls)
    assert fw.main(args) == 0
    assert {row["address"] for row in csv.DictReader((out / "passed.csv").open())} == expected
    # It resumed at stage two: no new batched nonce request was issued.
    assert not any(isinstance(c, list) for c in session.calls[before:])


def test_resume_refuses_a_checkpoint_built_for_another_threshold(tmp_path, monkeypatch, capsys):
    source = tmp_path / "wallets.csv"
    source.write_text(f"{addr(1)}\n")
    out = tmp_path / "out"
    session = FakeSession(nonces={addr(1): 9})
    monkeypatch.setattr(fw.requests, "Session", lambda: session)

    assert fw.main([str(source), "--min-tx", "4", "--url", "http://fake", "--cu-per-second", "0", "--out", str(out)]) == 0
    assert fw.main([str(source), "--min-tx", "10", "--url", "http://fake", "--cu-per-second", "0", "--out", str(out)]) == 2
    err = capsys.readouterr().err
    assert "min_tx=4" in err and "--no-resume" in err


def test_resume_refuses_a_checkpoint_built_in_another_mode(tmp_path, monkeypatch, capsys):
    source = tmp_path / "wallets.csv"
    source.write_text(f"{addr(1)}\n")
    out = tmp_path / "out"
    session = FakeSession(nonces={addr(1): 9})
    monkeypatch.setattr(fw.requests, "Session", lambda: session)

    assert fw.main([str(source), "--mode", "nonce", "--url", "http://fake", "--cu-per-second", "0", "--out", str(out)]) == 0
    assert fw.main([str(source), "--mode", "transfers", "--url", "http://fake", "--cu-per-second", "0", "--out", str(out)]) == 2
    assert "mode='nonce'" in capsys.readouterr().err


def test_no_resume_clears_a_mismatched_checkpoint(tmp_path, monkeypatch):
    source = tmp_path / "wallets.csv"
    source.write_text(f"{addr(1)}\n")
    out = tmp_path / "out"
    session = FakeSession(nonces={addr(1): 9}, transfers={(addr(1), "toAddress"): 0})
    monkeypatch.setattr(fw.requests, "Session", lambda: session)

    assert fw.main([str(source), "--min-tx", "4", "--url", "http://fake", "--cu-per-second", "0", "--out", str(out)]) == 0
    assert fw.main([str(source), "--min-tx", "10", "--url", "http://fake", "--cu-per-second", "0", "--out", str(out), "--no-resume"]) == 0


def test_changing_the_input_list_does_not_leak_old_wallets(tmp_path, monkeypatch):
    out = tmp_path / "out"
    session = FakeSession(nonces={addr(1): 9, addr(2): 9})
    monkeypatch.setattr(fw.requests, "Session", lambda: session)

    first = tmp_path / "a.csv"
    first.write_text(f"{addr(1)}\n")
    assert fw.main([str(first), "--url", "http://fake", "--cu-per-second", "0", "--out", str(out)]) == 0

    second = tmp_path / "b.csv"
    second.write_text(f"{addr(2)}\n")
    assert fw.main([str(second), "--url", "http://fake", "--cu-per-second", "0", "--out", str(out)]) == 0

    assert {row["address"] for row in csv.DictReader((out / "results.csv").open())} == {addr(2)}


def test_failed_wallets_are_retried_before_reporting(tmp_path, monkeypatch):
    monkeypatch.setattr(fw.time, "sleep", lambda _s: None)
    source = tmp_path / "wallets.csv"
    source.write_text(f"{addr(1)}\n")
    out = tmp_path / "out"

    # Exhaust every retry inside the client, so the wallet lands as an error;
    # the run-level retry pass should then pick it up and resolve it.
    session = FakeSession(
        nonces={addr(1): 9},
        script=[FakeResponse(None, status_code=503)] * 8,
    )
    monkeypatch.setattr(fw.requests, "Session", lambda: session)

    assert fw.main([str(source), "--url", "http://fake", "--cu-per-second", "0", "--out", str(out)]) == 0
    assert not (out / "errors.csv").exists()
    assert [row["address"] for row in csv.DictReader((out / "passed.csv").open())] == [addr(1)]


def test_no_retry_flag_leaves_the_failure_reported(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(fw.time, "sleep", lambda _s: None)
    source = tmp_path / "wallets.csv"
    source.write_text(f"{addr(1)}\n")
    out = tmp_path / "out"
    session = FakeSession(nonces={addr(1): 9}, script=[FakeResponse(None, status_code=503)] * 8)
    monkeypatch.setattr(fw.requests, "Session", lambda: session)

    args = [str(source), "--url", "http://fake", "--cu-per-second", "0", "--out", str(out), "--no-retry"]
    assert fw.main(args) == 0
    assert (out / "errors.csv").exists()
    # The split must not be presented as usable when wallets are missing from it.
    assert "WARNING" in capsys.readouterr().out


def test_dry_run_needs_no_url(tmp_path, capsys):
    source = tmp_path / "wallets.csv"
    source.write_text(f"{addr(1)}\n{addr(2)}\n")
    assert fw.main([str(source), "--dry-run"]) == 0
    assert "2 unique addresses" in capsys.readouterr().out


def test_missing_url_is_an_error(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("ALCHEMY_URL", raising=False)
    source = tmp_path / "wallets.csv"
    source.write_text(f"{addr(1)}\n")
    assert fw.main([str(source)]) == 2
    assert "no RPC URL" in capsys.readouterr().err


def test_rejects_unknown_category(tmp_path, capsys):
    source = tmp_path / "wallets.csv"
    source.write_text(f"{addr(1)}\n")
    assert fw.main([str(source), "--categories", "bogus", "--url", "http://fake", "--cu-per-second", "0"]) == 2
    assert "unknown transfer categories" in capsys.readouterr().err


def test_estimate_ranks_the_modes_by_cost():
    nonce_low, nonce_high, _, _ = fw.estimate(7871, "nonce", 330)
    both_low, both_high, _, _ = fw.estimate(7871, "both", 330)
    _, transfers_high, _, _ = fw.estimate(7871, "transfers", 330)

    assert nonce_low == nonce_high == 7871 * 26  # exact, no range
    assert both_low == nonce_low  # best case: nobody needs a transfer lookup
    assert both_high < transfers_high  # worst case still beats pure transfers


# ------------------------------------------------------------------- screening


ETH = 10**18


def write_list(path: Path, addresses) -> Path:
    path.write_text("\n".join(addresses) + "\n")
    return path


def test_parses_eth_amounts_exactly():
    assert fw.parse_eth_amount("0") == 0
    assert fw.parse_eth_amount("1") == ETH
    assert fw.parse_eth_amount("0.01") == ETH // 100
    # The case float maths would get wrong.
    assert fw.parse_eth_amount("0.1") == 10**17
    assert fw.parse_eth_amount("1.5") == 15 * 10**17


def test_rejects_nonsense_eth_amounts():
    for bad in ("abc", "1.2.3", "-1", "1e18", "."):
        with pytest.raises(ValueError):
            fw.parse_eth_amount(bad)


def test_address_list_ignores_comments_and_junk(tmp_path):
    path = tmp_path / "deny.txt"
    path.write_text(f"# a comment\n{addr(1)}\n\nnot-an-address\n{addr(2)}  # trailing\n")
    assert fw.load_address_list(path) == {addr(1), addr(2)}


def test_address_list_is_case_insensitive(tmp_path):
    path = tmp_path / "deny.txt"
    path.write_text("0x" + "A" * 40 + "\n")
    assert fw.load_address_list(path) == {"0x" + "a" * 40}


def test_denylist_rejects_without_any_network():
    screen = fw.Screen(denylist=frozenset({addr(1)}))
    settled, remaining = fw.screen_local([addr(1), addr(2)], screen)

    assert [(r.address, r.rejected_by) for r in settled] == [(addr(1), "denylist")]
    assert remaining == [addr(2)]


def test_allowlist_keeps_without_counting():
    screen = fw.Screen(allowlist=frozenset({addr(1)}))
    settled, remaining = fw.screen_local([addr(1), addr(2)], screen)

    assert [(r.address, r.kept_by) for r in settled] == [(addr(1), "allowlist")]
    assert settled[0].verdict(threshold=99) is True  # kept regardless of the bar
    assert remaining == [addr(2)]


def test_denylist_beats_allowlist():
    screen = fw.Screen(denylist=frozenset({addr(1)}), allowlist=frozenset({addr(1)}))
    settled, _ = fw.screen_local([addr(1)], screen)
    assert settled[0].rejected_by == "denylist"


def test_screen_local_is_case_insensitive():
    screen = fw.Screen(denylist=frozenset({addr(1)}))
    settled, remaining = fw.screen_local([addr(1).upper().replace("0X", "0x")], screen)
    assert settled and settled[0].rejected_by == "denylist"
    assert remaining == []


def test_min_balance_rejects_poor_wallets():
    session = FakeSession(balances={addr(1): 5 * ETH, addr(2): 0})
    screen = fw.Screen(min_balance_wei=ETH)

    rejected = fw.screen_onchain(client_for(session), [addr(1), addr(2)], screen)

    # Only the failing wallet comes back; the rich one is left to be counted.
    assert [(r.address, r.rejected_by) for r in rejected] == [(addr(2), "min_balance")]


def test_min_balance_boundary_is_inclusive():
    session = FakeSession(balances={addr(1): ETH})
    rejected = fw.screen_onchain(client_for(session), [addr(1)], fw.Screen(min_balance_wei=ETH))
    assert rejected == []


def test_exclude_contracts_rejects_addresses_with_code():
    session = FakeSession(codes={addr(1): "0x", addr(2): "0x60806040"})
    screen = fw.Screen(exclude_contracts=True)

    rejected = fw.screen_onchain(client_for(session), [addr(1), addr(2)], screen)

    assert [(r.address, r.rejected_by) for r in rejected] == [(addr(2), "contract")]


def test_screen_onchain_batches_both_checks_in_one_request():
    session = FakeSession(balances={addr(1): 5 * ETH}, codes={addr(1): "0x"})
    screen = fw.Screen(min_balance_wei=ETH, exclude_contracts=True)

    fw.screen_onchain(client_for(session), [addr(1), addr(2)], screen)

    assert len(session.calls) == 1  # one round trip, not one per check
    methods = sorted(item["method"] for item in session.calls[0])
    assert methods == ["eth_getBalance", "eth_getBalance", "eth_getCode", "eth_getCode"]


def test_screen_onchain_is_a_no_op_without_onchain_criteria():
    session = FakeSession()
    assert fw.screen_onchain(client_for(session), [addr(1)], fw.Screen()) == []
    assert session.calls == []


def test_screened_wallets_never_reach_the_counting_stages():
    # addr(1) is denied, addr(2) has no balance, addr(3) survives to be counted.
    session = FakeSession(nonces={addr(3): 9}, balances={addr(2): 0, addr(3): 5 * ETH})
    cfg = fw.Config(
        mode="nonce",
        threshold=4,
        screen=fw.Screen(denylist=frozenset({addr(1)}), min_balance_wei=ETH),
    )

    results = fw.run(client_for(session), [addr(1), addr(2), addr(3)], cfg)
    by_address = {r.address: r for r in results}

    assert by_address[addr(1)].rejected_by == "denylist"
    assert by_address[addr(2)].rejected_by == "min_balance"
    assert by_address[addr(3)].count == 9

    # The nonce batch asked about the survivor only.
    nonce_batches = [c for c in session.calls if any(i["method"] == "eth_getTransactionCount" for i in c)]
    asked = {i["params"][0] for batch in nonce_batches for i in batch}
    assert asked == {addr(3)}


def test_screening_results_survive_transfers_mode():
    session = FakeSession(transfers={(addr(2), "fromAddress"): 9, (addr(2), "toAddress"): 0})
    cfg = fw.Config(mode="transfers", threshold=1, screen=fw.Screen(denylist=frozenset({addr(1)})))

    results = fw.run(client_for(session), [addr(1), addr(2)], cfg)

    assert {r.address for r in results} == {addr(1), addr(2)}
    assert next(r for r in results if r.address == addr(1)).rejected_by == "denylist"


def test_outputs_carry_the_rejection_reason(tmp_path):
    results = [
        fw.Result(addr(1), 9),
        fw.Result(addr(2), 1),
        fw.Result(addr(3), -1, rejected_by="denylist"),
        fw.Result(addr(4), -1, kept_by="allowlist"),
    ]
    fw.write_outputs(tmp_path, results, threshold=4)

    passed = {row["address"]: row for row in csv.DictReader((tmp_path / "passed.csv").open())}
    filtered = {row["address"]: row for row in csv.DictReader((tmp_path / "filtered.csv").open())}

    assert set(passed) == {addr(1), addr(4)}
    assert set(filtered) == {addr(2), addr(3)}
    assert filtered[addr(3)]["reason"] == "denylist"
    assert filtered[addr(2)]["reason"] == "min_tx"
    assert passed[addr(4)]["reason"] == "allowlist"
    # Never counted, so the count cell stays blank rather than claiming a number.
    assert passed[addr(4)]["tx_count"] == ""
    assert filtered[addr(3)]["tx_count"] == ""


def test_summary_breaks_down_why_wallets_were_filtered(tmp_path):
    results = [
        fw.Result(addr(1), 1),
        fw.Result(addr(2), -1, rejected_by="denylist"),
        fw.Result(addr(3), -1, rejected_by="contract"),
        fw.Result(addr(4), -1, rejected_by="contract"),
    ]
    fw.write_outputs(tmp_path, results, threshold=4)

    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["filtered_by"] == {"min_tx": 1, "denylist": 1, "contract": 2}


def test_checkpoint_round_trips_screening_verdicts(tmp_path):
    path = tmp_path / "checkpoint.jsonl"
    writer = fw.CheckpointWriter(path)
    writer.write([fw.Result(addr(1), -1, rejected_by="denylist"), fw.Result(addr(2), -1, kept_by="allowlist")])
    writer.close()

    restored = fw.load_checkpoint(path)
    assert restored[addr(1)].rejected_by == "denylist"
    assert restored[addr(2)].kept_by == "allowlist"


def test_changing_the_screen_invalidates_a_checkpoint(tmp_path):
    meta = tmp_path / "meta.json"
    base = fw.Config(screen=fw.Screen(min_balance_wei=ETH))
    meta.write_text(json.dumps(fw.run_signature(base)))

    assert fw.describe_signature_mismatch(meta, base) is None
    changed = fw.Config(screen=fw.Screen(min_balance_wei=2 * ETH))
    assert "screen" in (fw.describe_signature_mismatch(meta, changed) or "")


def test_estimate_prices_the_screen():
    plain = fw.estimate(100, "nonce", 0)[0]
    screened = fw.estimate(100, "nonce", 0, fw.Screen(min_balance_wei=1, exclude_contracts=True))[0]
    assert screened == plain + 100 * (fw.CU_BALANCE + fw.CU_CODE)


def test_build_screen_reports_a_missing_list(tmp_path):
    parser = fw.build_parser()
    args = parser.parse_args([str(tmp_path / "in.csv"), "--denylist", str(tmp_path / "nope.txt")])
    with pytest.raises(OSError):
        fw.build_screen(args)


def test_cli_wires_screening_through(tmp_path, monkeypatch):
    wallets = tmp_path / "w.csv"
    wallets.write_text(f"{addr(1)}\n{addr(2)}\n{addr(3)}\n")
    deny = write_list(tmp_path / "deny.txt", [addr(1)])
    out = tmp_path / "out"

    session = FakeSession(nonces={addr(2): 9, addr(3): 0}, balances={addr(2): 5 * ETH, addr(3): 5 * ETH})
    monkeypatch.setattr(fw.requests, "Session", lambda: session)

    code = fw.main(
        [
            str(wallets), "--url", "http://fake", "--out", str(out),
            "--min-tx", "4", "--mode", "nonce", "--cu-per-second", "0",
            "--denylist", str(deny), "--min-balance", "0.5",
        ]
    )

    assert code == 0
    rows = {r["address"]: r for r in csv.DictReader((out / "results.csv").open())}
    assert rows[addr(1)]["reason"] == "denylist"
    assert rows[addr(2)]["passed"] == "True"
    assert rows[addr(3)]["reason"] == "min_tx"


def test_formats_eth_without_float_error():
    assert fw.format_eth(0) == "0"
    assert fw.format_eth(ETH) == "1"
    assert fw.format_eth(10**17) == "0.1"
    assert fw.format_eth(ETH // 100) == "0.01"
    assert fw.format_eth(15 * 10**17) == "1.5"


def test_eth_amount_round_trips_through_formatting():
    for text in ("0.01", "0.1", "1", "1.5", "12.345"):
        assert fw.format_eth(fw.parse_eth_amount(text)) == text
