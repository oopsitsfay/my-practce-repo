"""Vercel control panel for the wallet filter.

Vercel cannot run the job itself -- every request is a serverless function that
is killed long before an hour-long run finishes, and there is no disk to keep a
checkpoint on. So this is a control panel, not a worker:

    this page  ->  commits your CSV and triggers the GitHub Actions workflow
    GitHub     ->  does the actual work, up to six hours, free

Every request here takes a second or two, comfortably inside Vercel's limits.

Environment variables (Vercel project settings):

    GITHUB_TOKEN    fine-grained PAT with Contents: read/write and Actions:
                    read/write on the repository
    GITHUB_REPO     owner/repo, e.g. oopsitsfay/my-practce-repo
    APP_PASSWORD    the single sign-in password
    SESSION_SECRET  optional; random per deploy if unset, which just means
                    everyone is signed out again after a redeploy
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import time
from typing import Any

import requests
from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse, RedirectResponse

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
GITHUB_REPO = os.environ.get("GITHUB_REPO", "").strip()
WORKFLOW_FILE = os.environ.get("WORKFLOW_FILE", "filter-wallets.yml").strip()
BRANCH = os.environ.get("GITHUB_BRANCH", "main").strip()
APP_PASSWORD = os.environ.get("APP_PASSWORD", "").strip()
SESSION_SECRET = (os.environ.get("SESSION_SECRET") or secrets.token_urlsafe(32)).encode()

SESSION_COOKIE = "wf_panel"
SESSION_MAX_AGE = 30 * 24 * 3600
ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
MAX_UPLOAD_BYTES = 4 * 1024 * 1024  # the Contents API is not for large blobs

app = FastAPI(title="Wallet filter panel")


# ---------------------------------------------------------------- sessions


def make_token() -> str:
    """A signed cookie, since serverless functions share no memory."""
    issued = str(int(time.time()))
    signature = hmac.new(SESSION_SECRET, issued.encode(), hashlib.sha256).hexdigest()
    return f"{issued}.{signature}"


def valid_token(token: str | None) -> bool:
    if not token or "." not in token:
        return False
    issued, _, signature = token.partition(".")
    expected = hmac.new(SESSION_SECRET, issued.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return False
    try:
        return time.time() - int(issued) < SESSION_MAX_AGE
    except ValueError:
        return False


def require_session(request: Request) -> None:
    if not valid_token(request.cookies.get(SESSION_COOKIE)):
        raise HTTPException(401, "not signed in")


# ---------------------------------------------------------------- github


def gh(method: str, path: str, **kwargs: Any) -> requests.Response:
    if not GITHUB_TOKEN or not GITHUB_REPO:
        raise HTTPException(500, "GITHUB_TOKEN and GITHUB_REPO are not configured on this deployment")
    response = requests.request(
        method,
        f"https://api.github.com{path}",
        headers={
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        timeout=20,
        **kwargs,
    )
    if response.status_code in (401, 403):
        raise HTTPException(502, "GitHub rejected the token -- check GITHUB_TOKEN and its repository permissions")
    return response


def repo_path(path: str) -> str:
    return f"/repos/{GITHUB_REPO}{path}"


def load_summaries() -> dict[int, dict]:
    """Every published run summary, keyed by run id.

    The workflow commits these, so the outcome of a run is one API call away
    instead of downloading and unzipping an artifact.
    """
    response = gh("GET", repo_path("/contents/results"), params={"ref": BRANCH})
    if response.status_code != 200:
        return {}

    summaries: dict[int, dict] = {}
    for entry in response.json():
        if not entry["name"].endswith(".json"):
            continue
        try:
            body = requests.get(entry["download_url"], timeout=15).json()
            summaries[int(body["run_id"])] = body
        except (requests.RequestException, ValueError, KeyError):
            continue
    return summaries


# ---------------------------------------------------------------- routes


@app.get("/api/health")
def health() -> dict:
    return {
        "ok": True,
        "configured": bool(GITHUB_TOKEN and GITHUB_REPO and APP_PASSWORD),
        "repo": GITHUB_REPO or None,
    }


@app.post("/api/login")
def login(password: str = Form(...)) -> JSONResponse:
    if not APP_PASSWORD:
        raise HTTPException(500, "APP_PASSWORD is not set on this deployment")
    if not secrets.compare_digest(password, APP_PASSWORD):
        raise HTTPException(401, "wrong password")

    response = JSONResponse({"ok": True})
    response.set_cookie(
        SESSION_COOKIE, make_token(), httponly=True, secure=True, samesite="lax", max_age=SESSION_MAX_AGE
    )
    return response


@app.post("/api/logout")
def logout() -> JSONResponse:
    response = JSONResponse({"ok": True})
    response.delete_cookie(SESSION_COOKIE)
    return response


@app.get("/api/runs", dependencies=[Depends(require_session)])
def list_runs() -> dict:
    response = gh(
        "GET",
        repo_path(f"/actions/workflows/{WORKFLOW_FILE}/runs"),
        params={"per_page": 20, "branch": BRANCH},
    )
    if response.status_code == 404:
        raise HTTPException(404, f"no workflow named {WORKFLOW_FILE} on {BRANCH}")
    if response.status_code != 200:
        raise HTTPException(502, f"GitHub returned {response.status_code} listing runs")

    summaries = load_summaries()
    runs = []
    for run in response.json().get("workflow_runs", []):
        summary = summaries.get(run["id"])
        runs.append(
            {
                "id": run["id"],
                "name": run.get("display_title") or run.get("name") or "run",
                "number": run.get("run_number"),
                # queued | in_progress | completed, with conclusion once completed
                "status": run.get("status"),
                "conclusion": run.get("conclusion"),
                "created_at": run.get("created_at"),
                "updated_at": run.get("updated_at"),
                "html_url": run.get("html_url"),
                "summary": summary,
            }
        )
    return {"runs": runs, "repo": GITHUB_REPO}


@app.post("/api/runs", dependencies=[Depends(require_session)])
async def start_run(
    file: UploadFile = File(...),
    min_tx: int = Form(4),
    mode: str = Form("both"),
    categories: str = Form("external"),
    cu_per_second: int = Form(330),
) -> JSONResponse:
    if min_tx < 1:
        raise HTTPException(400, "minimum transactions must be at least 1")
    if mode not in ("both", "nonce", "transfers"):
        raise HTTPException(400, "unknown mode")

    payload = await file.read()
    if len(payload) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, "file is larger than 4 MB, which is more than the GitHub contents API should carry")

    # Validate here rather than letting the workflow fail an hour later.
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(400, "that file is not text") from None
    found = sum(1 for line in text.splitlines() if ADDRESS_RE.match(line.split(",")[0].strip()))
    if not found:
        raise HTTPException(400, "no valid Ethereum addresses found in that file")

    stem = re.sub(r"[^A-Za-z0-9._-]", "-", (file.filename or "wallets.csv").rsplit("/", 1)[-1])
    path = f"uploads/{int(time.time())}-{stem}"

    committed = gh(
        "PUT",
        repo_path(f"/contents/{path}"),
        json={
            "message": f"Add wallet list {stem}",
            "content": base64.b64encode(payload).decode(),
            "branch": BRANCH,
        },
    )
    if committed.status_code not in (200, 201):
        raise HTTPException(502, f"could not commit the wallet list: GitHub returned {committed.status_code}")

    dispatched = gh(
        "POST",
        repo_path(f"/actions/workflows/{WORKFLOW_FILE}/dispatches"),
        json={
            "ref": BRANCH,
            "inputs": {
                "wallets_file": path,
                "min_tx": str(min_tx),
                "mode": mode,
                "categories": categories,
                "cu_per_second": str(cu_per_second),
            },
        },
    )
    if dispatched.status_code != 204:
        raise HTTPException(502, f"could not start the workflow: GitHub returned {dispatched.status_code}")

    return JSONResponse({"ok": True, "wallets_file": path, "addresses": found}, status_code=202)


@app.post("/api/runs/{run_id}/cancel", dependencies=[Depends(require_session)])
def cancel_run(run_id: int) -> dict:
    response = gh("POST", repo_path(f"/actions/runs/{run_id}/cancel"))
    if response.status_code not in (202, 409):
        raise HTTPException(502, f"GitHub returned {response.status_code} cancelling the run")
    return {"ok": True}


@app.get("/api/runs/{run_id}/download", dependencies=[Depends(require_session)])
def download(run_id: int) -> RedirectResponse:
    """Hand back a short-lived signed URL for the run's artifact zip."""
    listing = gh("GET", repo_path(f"/actions/runs/{run_id}/artifacts"))
    if listing.status_code != 200:
        raise HTTPException(502, f"GitHub returned {listing.status_code} listing artifacts")

    artifacts = listing.json().get("artifacts", [])
    if not artifacts:
        raise HTTPException(404, "this run has no results to download yet")
    if artifacts[0].get("expired"):
        raise HTTPException(410, "the results for this run have expired -- run it again")

    # GitHub answers with a 302 to a pre-signed URL the browser can use directly.
    signed = gh("GET", repo_path(f"/actions/artifacts/{artifacts[0]['id']}/zip"), allow_redirects=False)
    location = signed.headers.get("Location")
    if not location:
        raise HTTPException(502, "GitHub did not return a download link")
    return RedirectResponse(location, status_code=302)
