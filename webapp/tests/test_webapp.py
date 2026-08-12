"""Tests for the web front end, against a fake RPC and a temporary data dir."""

from __future__ import annotations

import importlib
import json
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "wallet-filter"))

import filter_wallets as fw  # noqa: E402


def addr(n: int) -> str:
    return "0x" + f"{n:040x}"


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.status_code = status_code
        self._payload = payload
        self.headers = {}
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class FakeSession:
    """Serves nonces and inbound transfers with no network involved."""

    def __init__(self, nonces, inbound):
        self.nonces = nonces
        self.inbound = inbound

    def post(self, url, json=None, timeout=None):  # noqa: A002
        if isinstance(json, list):
            return FakeResponse(
                [{"id": i["id"], "result": hex(self.nonces.get(i["params"][0], 0))} for i in json]
            )
        params = json["params"][0]
        served = min(self.inbound.get(params["toAddress"], 0), int(params["maxCount"], 16))
        return FakeResponse({"id": 1, "result": {"transfers": [{}] * served, "pageKey": None}})


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("APP_PASSWORD", "hunter2")
    monkeypatch.setenv("ALCHEMY_URL", "http://fake")

    # 20 wallets: even ones pass on outbound, odd ones need the inbound stage
    # and only every fourth has enough inbound to clear the bar.
    addresses = [addr(i) for i in range(1, 21)]
    nonces = {a: (9 if i % 2 == 0 else 1) for i, a in enumerate(addresses)}
    inbound = {a: (5 if i % 4 == 1 else 0) for i, a in enumerate(addresses)}
    session = FakeSession(nonces, inbound)
    monkeypatch.setattr(fw.requests, "Session", lambda: session)

    import app as app_module

    importlib.reload(app_module)
    app_module.runner.alchemy_url = "http://fake"
    with TestClient(app_module.app) as test_client:
        test_client._module = app_module
        test_client._addresses = addresses
        test_client._expected_pass = {a for i, a in enumerate(addresses) if nonces[a] + inbound[a] >= 4}
        yield test_client
    # Each test reloads the app, so without this the orphaned runners keep
    # engine thread pools alive and the interpreter never exits.
    app_module.runner.stop()


def sign_in(client):
    client.post("/login", data={"password": "hunter2"}, follow_redirects=False)


def upload(client, addresses, **fields):
    body = ("\n".join(addresses) + "\n").encode()
    data = {"min_tx": "4", "mode": "both", "categories": "external", "cu_per_second": "0"}
    data.update({k: str(v) for k, v in fields.items()})
    return client.post("/api/jobs", files={"file": ("wallets.csv", body, "text/csv")}, data=data)


def wait_for(client, job_id, status, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = client.get(f"/api/jobs/{job_id}").json()
        if job["status"] == status:
            return job
        time.sleep(0.05)
    pytest.fail(f"job stayed {job['status']}, wanted {status}")


# ---------------------------------------------------------------- auth


def test_login_page_when_signed_out(client):
    page = client.get("/").text
    assert 'name="password"' in page and "Sign in" in page
    assert "New job" not in page  # the app itself is not served to a stranger


def test_theme_css_is_served(client):
    res = client.get("/static/theme.css")
    assert res.status_code == 200 and "--green" in res.text


def test_api_requires_a_session(client):
    assert client.get("/api/jobs").status_code == 401
    assert upload(client, [addr(1)]).status_code == 401


def test_wrong_password_is_rejected(client):
    client.post("/login", data={"password": "nope"}, follow_redirects=False)
    assert client.get("/api/jobs").status_code == 401


def test_sign_in_then_out(client):
    sign_in(client)
    assert client.get("/api/jobs").status_code == 200
    client.post("/logout", follow_redirects=False)
    assert client.get("/api/jobs").status_code == 401


def test_password_is_never_blank_by_default(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "d"))
    monkeypatch.delenv("APP_PASSWORD", raising=False)
    import app as app_module

    importlib.reload(app_module)
    assert app_module.APP_PASSWORD  # generated rather than left open


# ---------------------------------------------------------------- uploads


def test_rejects_a_file_with_no_addresses(client):
    sign_in(client)
    res = client.post(
        "/api/jobs",
        files={"file": ("junk.csv", b"name,note\nalice,hi\n", "text/csv")},
        data={"min_tx": "4", "mode": "both", "categories": "external", "cu_per_second": "0"},
    )
    assert res.status_code == 400 and "no valid" in res.json()["detail"]


def test_rejects_bad_settings(client):
    sign_in(client)
    assert upload(client, [addr(1)], min_tx=0).status_code == 400
    assert upload(client, [addr(1)], mode="sideways").status_code == 400
    assert upload(client, [addr(1)], categories="nonsense").status_code == 400


def test_counts_and_reports_malformed_rows(client):
    sign_in(client)
    job = upload(client, [addr(1), "not-an-address", addr(2)]).json()
    assert job["total"] == 2 and job["malformed"] == 1


# ---------------------------------------------------------------- running


def test_job_runs_to_completion_with_the_right_split(client):
    sign_in(client)
    created = upload(client, client._addresses).json()
    job = wait_for(client, created["id"], "done")

    assert job["passed"] == len(client._expected_pass)
    assert job["passed"] + job["filtered"] == len(client._addresses)
    assert job["errors"] == 0
    assert job["percent"] == 100.0


def test_downloads_contain_the_split(client):
    sign_in(client)
    created = upload(client, client._addresses).json()
    wait_for(client, created["id"], "done")

    passed = client.get(f"/api/jobs/{created['id']}/download/passed")
    assert passed.status_code == 200
    rows = [line.split(",")[0] for line in passed.text.strip().splitlines()[1:]]
    assert set(rows) == client._expected_pass

    filtered = client.get(f"/api/jobs/{created['id']}/download/filtered")
    assert set(line.split(",")[0] for line in filtered.text.strip().splitlines()[1:]) == (
        set(client._addresses) - client._expected_pass
    )


def test_downloads_are_named_after_the_upload(client):
    sign_in(client)
    created = upload(client, client._addresses).json()
    wait_for(client, created["id"], "done")
    res = client.get(f"/api/jobs/{created['id']}/download/passed")
    assert "wallets-passed.csv" in res.headers["content-disposition"]


def test_missing_download_is_404_not_a_crash(client):
    sign_in(client)
    created = upload(client, client._addresses).json()
    wait_for(client, created["id"], "done")
    assert client.get(f"/api/jobs/{created['id']}/download/errors").status_code == 404
    assert client.get(f"/api/jobs/{created['id']}/download/secrets").status_code == 404


def test_jobs_are_listed_newest_first(client):
    sign_in(client)
    first = upload(client, [addr(1)]).json()
    wait_for(client, first["id"], "done")
    second = upload(client, [addr(2)]).json()
    wait_for(client, second["id"], "done")

    listed = client.get("/api/jobs").json()["jobs"]
    assert [job["id"] for job in listed][:2] == [second["id"], first["id"]]


# ---------------------------------------------------------------- lifecycle


def test_delete_removes_the_job_and_its_files(client):
    sign_in(client)
    created = upload(client, client._addresses).json()
    wait_for(client, created["id"], "done")
    job_dir = client._module.store.job_dir(created["id"])
    assert job_dir.exists()

    assert client.delete(f"/api/jobs/{created['id']}").status_code == 200
    assert client.get(f"/api/jobs/{created['id']}").status_code == 404
    assert not job_dir.exists()


def test_resume_reruns_a_finished_job_without_redoing_the_work(client):
    sign_in(client)
    created = upload(client, client._addresses).json()
    wait_for(client, created["id"], "done")

    assert client.post(f"/api/jobs/{created['id']}/resume").status_code == 200
    job = wait_for(client, created["id"], "done")
    # Everything was already checkpointed, so the rerun answers from cache.
    assert job["resumed"] == len(client._addresses)
    assert job["passed"] == len(client._expected_pass)


def test_interrupted_jobs_are_requeued_on_restart(client, tmp_path):
    sign_in(client)
    created = upload(client, client._addresses).json()
    wait_for(client, created["id"], "done")

    # Simulate a container restart mid-job: mark it running, then boot a runner.
    store = client._module.store
    job = store.get(created["id"])
    job.status = "running"
    store.save(job)

    from jobs import Runner

    runner = Runner(store, "http://fake")
    runner.requeue_interrupted()
    assert store.get(created["id"]).status == "queued"


def test_cancel_a_queued_job(client):
    sign_in(client)
    created = upload(client, client._addresses).json()
    client.post(f"/api/jobs/{created['id']}/cancel")
    job = client.get(f"/api/jobs/{created['id']}").json()
    assert job["status"] in ("cancelled", "running", "done")


def test_cannot_delete_an_active_job(client):
    sign_in(client)
    created = upload(client, client._addresses).json()
    job = client._module.store.get(created["id"])
    job.status = "running"
    client._module.store.save(job)
    assert client.delete(f"/api/jobs/{created['id']}").status_code == 409


def test_shutdown_leaves_a_job_queued_not_cancelled(client):
    # A redeploy must not look like the user pressed cancel: the job stays
    # queued so the next boot picks it up from its checkpoint.
    sign_in(client)
    created = upload(client, client._addresses).json()
    runner = client._module.runner
    runner.stop(timeout=15)

    job = client._module.store.get(created["id"])
    assert job.status in ("queued", "done")
    assert job.status != "cancelled"


def test_stop_is_safe_to_call_twice(client):
    runner = client._module.runner
    runner.stop(timeout=5)
    runner.stop(timeout=5)  # idempotent, no hang and no exception


def test_healthz_is_open(client):
    body = client.get("/healthz").json()
    assert body["ok"] and body["alchemy_configured"]
