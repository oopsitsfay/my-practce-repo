"""Tests for filter_wallets, using a fake RPC server instead of the network."""

from __future__ import annotations

import csv
import json
import sys
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

    def __init__(self, nonces=None, transfers=None, script=None):
        self.nonces = nonces or {}
        self.transfers = transfers or {}
        self.script = list(script or [])  # queued canned responses, popped first
        self.calls = []

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
                    {"jsonrpc": "2.0", "id": item["id"], "result": hex(self.nonces.get(item["params"][0], 0))}
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
    with pytest.raises(fw.RpcError, match="API key"):
        client_for(session).call([{"id": 0}])
    assert len(session.calls) == 1


def test_gives_up_after_max_retries(monkeypatch):
    monkeypatch.setattr(fw.time, "sleep", lambda _s: None)
    session = FakeSession(script=[FakeResponse(None, status_code=503)] * 3)
    results = fw.count_nonces(client_for(session), [addr(1)])
    assert results[0].error and results[0].count == -1


# ---------------------------------------------------------------- checkpointing


def test_checkpoint_roundtrip_skips_errors(tmp_path):
    path = tmp_path / "checkpoint.jsonl"
    writer = fw.CheckpointWriter(path)
    writer.write([fw.Result(addr(1), 5), fw.Result(addr(2), -1, error="boom")])
    writer.close()

    done = fw.load_checkpoint(path)
    assert set(done) == {addr(1)}  # the failed one is retried on resume
    assert done[addr(1)].count == 5


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

    assert fw.main([str(source), "--min-tx", "4", "--url", "http://fake", "--out", str(out)]) == 0

    expected_pass = sum(1 for a in addresses if nonces[a] >= 4)
    assert len(list(csv.DictReader((out / "passed.csv").open()))) == expected_pass
    assert len(list(csv.DictReader((out / "filtered.csv").open()))) == len(addresses) - expected_pass

    # Second run: everything is in the checkpoint, so no further RPC calls.
    before = len(session.calls)
    assert fw.main([str(source), "--min-tx", "4", "--url", "http://fake", "--out", str(out)]) == 0
    assert len(session.calls) == before
    assert len(list(csv.DictReader((out / "passed.csv").open()))) == expected_pass


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
    assert fw.main([str(source), "--categories", "bogus", "--url", "http://fake"]) == 2
    assert "unknown transfer categories" in capsys.readouterr().err


def test_estimate_scales_with_mode():
    cu_nonce, _ = fw.estimate(7871, "nonce", 100, 330)
    cu_transfers, _ = fw.estimate(7871, "transfers", 100, 330)
    assert cu_nonce == 7871 * 26
    assert cu_transfers > cu_nonce
