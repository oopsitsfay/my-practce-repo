"""Tests for the Vercel control panel, against a fake GitHub API."""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE / "api"))


def addr(n: int) -> str:
    return "0x" + f"{n:040x}"


class FakeResponse:
    def __init__(self, payload=None, status_code=200, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = json.dumps(payload) if payload is not None else ""

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class FakeGitHub:
    """Records calls and answers them from a small canned world."""

    def __init__(self):
        self.calls = []
        self.runs = []
        self.results = {}
        self.contents_status = 201
        self.dispatch_status = 204
        self.artifacts = [{"id": 77, "expired": False}]

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs.get("json")))

        if "/actions/workflows/" in url and url.endswith("/runs"):
            return FakeResponse({"workflow_runs": self.runs})
        if url.endswith("/dispatches"):
            return FakeResponse(None, self.dispatch_status)
        if "/contents/results" in url:
            if not self.results:
                return FakeResponse(None, 404)
            return FakeResponse(
                [{"name": f"{rid}.json", "download_url": f"https://raw/{rid}"} for rid in self.results]
            )
        if "/contents/" in url:
            return FakeResponse({"content": {}}, self.contents_status)
        if url.endswith("/artifacts"):
            return FakeResponse({"artifacts": self.artifacts})
        if "/artifacts/" in url and url.endswith("/zip"):
            return FakeResponse(None, 302, {"Location": "https://signed.example/zip"})
        if url.endswith("/cancel"):
            return FakeResponse(None, 202)
        return FakeResponse(None, 404)

    def get(self, url, **kwargs):
        # Only used for fetching a published summary blob by download_url.
        rid = url.rsplit("/", 1)[-1]
        return FakeResponse(self.results[int(rid)])


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "tok")
    monkeypatch.setenv("GITHUB_REPO", "someone/repo")
    monkeypatch.setenv("APP_PASSWORD", "hunter2")
    monkeypatch.setenv("SESSION_SECRET", "test-secret")

    import index as panel

    importlib.reload(panel)
    fake = FakeGitHub()
    monkeypatch.setattr(panel.requests, "request", fake.request)
    monkeypatch.setattr(panel.requests, "get", fake.get)

    # The session cookie is Secure (Vercel is always HTTPS), so the test client
    # must speak https or it silently drops the cookie.
    test_client = TestClient(panel.app, base_url="https://testserver")
    test_client._gh = fake
    test_client._panel = panel
    return test_client


def sign_in(client):
    assert client.post("/api/login", data={"password": "hunter2"}).status_code == 200


def upload(client, rows, **fields):
    body = ("\n".join(rows) + "\n").encode()
    data = {"min_tx": "4", "mode": "both", "categories": "external", "cu_per_second": "330"}
    data.update({k: str(v) for k, v in fields.items()})
    return client.post("/api/runs", files={"file": ("wallets.csv", body, "text/csv")}, data=data)


# ---------------------------------------------------------------- auth


def test_health_needs_no_session(client):
    body = client.get("/api/health").json()
    assert body["ok"] and body["configured"] and body["repo"] == "someone/repo"


def test_api_requires_a_session(client):
    assert client.get("/api/runs").status_code == 401
    assert upload(client, [addr(1)]).status_code == 401


def test_wrong_password_rejected(client):
    assert client.post("/api/login", data={"password": "nope"}).status_code == 401
    assert client.get("/api/runs").status_code == 401


def test_sign_in_then_out(client):
    sign_in(client)
    assert client.get("/api/runs").status_code == 200
    client.post("/api/logout")
    assert client.get("/api/runs").status_code == 401


def test_session_cookie_is_signed_not_guessable(client):
    # Serverless functions share no memory, so the cookie itself carries proof.
    client.cookies.set("wf_panel", "9999999999.deadbeef")
    assert client.get("/api/runs").status_code == 401


def test_session_token_survives_a_new_process(client, monkeypatch):
    sign_in(client)
    token = client.cookies.get("wf_panel")

    import index as panel

    importlib.reload(panel)  # a cold start, as Vercel would do
    monkeypatch.setattr(panel.requests, "request", client._gh.request)
    fresh = TestClient(panel.app, base_url="https://testserver")
    fresh.cookies.set("wf_panel", token)
    assert fresh.get("/api/runs").status_code == 200


# ---------------------------------------------------------------- starting runs


def test_upload_commits_then_dispatches(client):
    sign_in(client)
    res = upload(client, [addr(1), addr(2)], min_tx=6, mode="nonce")
    assert res.status_code == 202
    assert res.json()["addresses"] == 2

    methods = [(m, u.split("/repos/someone/repo")[-1]) for m, u, _ in client._gh.calls]
    assert any(m == "PUT" and p.startswith("/contents/uploads/") for m, p in methods)
    assert any(m == "POST" and p.endswith("/dispatches") for m, p in methods)

    dispatch = [body for m, u, body in client._gh.calls if u.endswith("/dispatches")][0]
    assert dispatch["inputs"]["min_tx"] == "6"
    assert dispatch["inputs"]["mode"] == "nonce"
    assert dispatch["inputs"]["wallets_file"].startswith("uploads/")


def test_rejects_a_file_with_no_addresses(client):
    sign_in(client)
    res = upload(client, ["name,note", "alice,hi"])
    assert res.status_code == 400 and "no valid" in res.json()["detail"]
    # Nothing was committed and nothing was started.
    assert not any(u.endswith("/dispatches") for _, u, _ in client._gh.calls)


def test_rejects_bad_settings(client):
    sign_in(client)
    assert upload(client, [addr(1)], min_tx=0).status_code == 400
    assert upload(client, [addr(1)], mode="sideways").status_code == 400


def test_upload_filename_is_sanitised(client):
    sign_in(client)
    body = (addr(1) + "\n").encode()
    client.post(
        "/api/runs",
        files={"file": ("../../etc/pass wd.csv", body, "text/csv")},
        data={"min_tx": "4", "mode": "both", "categories": "external", "cu_per_second": "330"},
    )
    path = [u for m, u, _ in client._gh.calls if m == "PUT"][0]
    assert "/contents/uploads/" in path and ".." not in path and " " not in path


def test_a_failed_dispatch_is_reported(client):
    sign_in(client)
    client._gh.dispatch_status = 422
    res = upload(client, [addr(1)])
    assert res.status_code == 502 and "could not start" in res.json()["detail"]


# ---------------------------------------------------------------- listing


def test_runs_carry_their_published_summary(client):
    sign_in(client)
    client._gh.runs = [
        {
            "id": 42,
            "display_title": "uploads/wallets.csv — min 4 tx, both",
            "run_number": 7,
            "status": "completed",
            "conclusion": "success",
            "created_at": "2026-08-12T10:00:00Z",
            "updated_at": "2026-08-12T11:00:00Z",
            "html_url": "https://github.com/x/y/actions/runs/42",
        }
    ]
    client._gh.results = {42: {"run_id": 42, "passed": 5704, "filtered": 2167, "errors": 0, "total": 7871, "min_tx": 4}}

    run = client.get("/api/runs").json()["runs"][0]
    assert run["status"] == "completed" and run["conclusion"] == "success"
    assert run["summary"]["passed"] == 5704 and run["summary"]["filtered"] == 2167


def test_a_run_without_a_summary_still_lists(client):
    sign_in(client)
    client._gh.runs = [
        {
            "id": 43,
            "display_title": "running one",
            "run_number": 8,
            "status": "in_progress",
            "conclusion": None,
            "created_at": "2026-08-12T10:00:00Z",
            "updated_at": "2026-08-12T10:05:00Z",
            "html_url": "https://example/43",
        }
    ]
    run = client.get("/api/runs").json()["runs"][0]
    assert run["summary"] is None and run["status"] == "in_progress"


# ---------------------------------------------------------------- downloads


def test_download_hands_back_the_signed_url(client):
    sign_in(client)
    res = client.get("/api/runs/42/download", follow_redirects=False)
    assert res.status_code == 302
    assert res.headers["location"] == "https://signed.example/zip"


def test_download_with_no_artifact_is_404(client):
    sign_in(client)
    client._gh.artifacts = []
    assert client.get("/api/runs/42/download", follow_redirects=False).status_code == 404


def test_expired_artifact_says_so(client):
    sign_in(client)
    client._gh.artifacts = [{"id": 77, "expired": True}]
    res = client.get("/api/runs/42/download", follow_redirects=False)
    assert res.status_code == 410 and "expired" in res.json()["detail"]


def test_download_needs_a_session(client):
    assert client.get("/api/runs/42/download", follow_redirects=False).status_code == 401


# ---------------------------------------------------------------- config errors


def test_missing_github_config_is_a_clear_error(monkeypatch):
    monkeypatch.setenv("APP_PASSWORD", "hunter2")
    monkeypatch.setenv("SESSION_SECRET", "s")
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_REPO", raising=False)

    import index as panel

    importlib.reload(panel)
    c = TestClient(panel.app, base_url="https://testserver", raise_server_exceptions=False)
    assert c.get("/api/health").json()["configured"] is False
    c.post("/api/login", data={"password": "hunter2"})
    assert c.get("/api/runs").status_code == 500


def test_a_rejected_token_is_reported_not_swallowed(client, monkeypatch):
    sign_in(client)
    monkeypatch.setattr(client._panel.requests, "request", lambda *a, **k: FakeResponse(None, 403))
    res = client.get("/api/runs")
    assert res.status_code == 502 and "token" in res.json()["detail"]
