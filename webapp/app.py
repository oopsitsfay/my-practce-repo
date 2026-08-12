"""Web front end for the wallet filter.

Upload a CSV, the job runs in the background, download the filtered list when
it finishes. Single shared password -- this is a private tool, not a service.

    ALCHEMY_URL=https://eth-mainnet.g.alchemy.com/v2/KEY APP_PASSWORD=secret \
        uvicorn app:app --host 0.0.0.0 --port 7860
"""

from __future__ import annotations

import os
import secrets
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, Response, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "wallet-filter"))

import filter_wallets as fw  # noqa: E402

from jobs import ACTIVE, DOWNLOADS, JobStore, Runner, new_job  # noqa: E402

HERE = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", HERE / "data"))
ALCHEMY_URL = os.environ.get("ALCHEMY_URL", "").strip()
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", 25 * 1024 * 1024))
SESSION_COOKIE = "wf_session"

# An accidentally public instance would let anyone spend your compute units, so
# never fall back to "no password" -- mint one and print it instead.
APP_PASSWORD = os.environ.get("APP_PASSWORD", "").strip()
if not APP_PASSWORD:
    APP_PASSWORD = secrets.token_urlsafe(12)
    print(f"\n  APP_PASSWORD was not set. Using this one for this run:\n\n      {APP_PASSWORD}\n", flush=True)

store = JobStore(DATA_DIR)
runner = Runner(store, ALCHEMY_URL or None)
sessions: set[str] = set()

@asynccontextmanager
async def lifespan(_: FastAPI):
    runner.start()
    if not ALCHEMY_URL:
        print("  WARNING: ALCHEMY_URL is not set -- jobs will fail until it is.", flush=True)
    try:
        yield
    finally:
        # Let the running job stop on a clean boundary and stay queued, so a
        # redeploy resumes it instead of losing the container mid-run.
        runner.stop()


app = FastAPI(title="Wallet filter", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")


# ------------------------------------------------------------------ auth


def require_session(request: Request) -> None:
    if request.cookies.get(SESSION_COOKIE) not in sessions:
        raise HTTPException(status_code=401, detail="not signed in")


@app.post("/login")
def login(response: Response, password: str = Form(...)) -> RedirectResponse:
    if not secrets.compare_digest(password, APP_PASSWORD):
        return RedirectResponse("/?error=1", status_code=303)
    token = secrets.token_urlsafe(32)
    sessions.add(token)
    redirect = RedirectResponse("/", status_code=303)
    redirect.set_cookie(SESSION_COOKIE, token, httponly=True, samesite="lax", max_age=30 * 24 * 3600)
    return redirect


@app.post("/logout")
def logout(request: Request) -> RedirectResponse:
    sessions.discard(request.cookies.get(SESSION_COOKIE, ""))
    redirect = RedirectResponse("/", status_code=303)
    redirect.delete_cookie(SESSION_COOKIE)
    return redirect


# ------------------------------------------------------------------ pages


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    signed_in = request.cookies.get(SESSION_COOKIE) in sessions
    page = "index.html" if signed_in else "login.html"
    return HTMLResponse((HERE / "static" / page).read_text(encoding="utf-8"))


@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True, "alchemy_configured": bool(ALCHEMY_URL)}


# ------------------------------------------------------------------ jobs api


@app.get("/api/jobs", dependencies=[Depends(require_session)])
def list_jobs() -> dict:
    jobs = store.list()
    return {
        "jobs": [job_json(job) for job in jobs],
        "queued": sum(1 for job in jobs if job.status in ACTIVE),
        "alchemy_configured": bool(ALCHEMY_URL),
    }


@app.get("/api/jobs/{job_id}", dependencies=[Depends(require_session)])
def get_job(job_id: str) -> dict:
    job = store.get(job_id)
    if not job:
        raise HTTPException(404, "no such job")
    return job_json(job)


@app.post("/api/jobs", dependencies=[Depends(require_session)])
async def create_job(
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
    wanted = [c.strip() for c in categories.split(",") if c.strip()]
    unknown = [c for c in wanted if c not in fw.TRANSFER_CATEGORIES]
    if unknown:
        raise HTTPException(400, f"unknown transfer categories: {', '.join(unknown)}")

    payload = await file.read()
    if len(payload) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, f"file is larger than {MAX_UPLOAD_BYTES // 1024 // 1024} MB")

    job = new_job(file.filename or "wallets.csv", min_tx, mode, ",".join(wanted), cu_per_second)
    work_dir = store.job_dir(job.id)
    work_dir.mkdir(parents=True, exist_ok=True)
    (work_dir / "input.csv").write_bytes(payload)

    # Parse before queuing so a bad file is rejected while the user is watching,
    # rather than failing silently an hour later.
    addresses, malformed = fw.load_addresses(work_dir / "input.csv")
    if not addresses:
        store.delete(job.id)
        raise HTTPException(400, "no valid Ethereum addresses found in that file")
    job.total = len(addresses)
    job.malformed = len(malformed)

    runner.submit(job)
    return JSONResponse(job_json(job), status_code=201)


@app.post("/api/jobs/{job_id}/cancel", dependencies=[Depends(require_session)])
def cancel_job(job_id: str) -> dict:
    if not store.get(job_id):
        raise HTTPException(404, "no such job")
    runner.cancel(job_id)
    return {"ok": True}


@app.post("/api/jobs/{job_id}/resume", dependencies=[Depends(require_session)])
def resume_job(job_id: str) -> dict:
    """Re-queue a finished job. Its checkpoint means only unresolved wallets are retried."""
    job = store.get(job_id)
    if not job:
        raise HTTPException(404, "no such job")
    if job.status in ACTIVE:
        raise HTTPException(409, "that job is already queued")
    runner._cancelled.discard(job_id)  # a previous cancel must not stop the new attempt
    job.status = "queued"
    job.message = ""
    job.finished_at = None
    runner.submit(job)
    return job_json(job)


@app.delete("/api/jobs/{job_id}", dependencies=[Depends(require_session)])
def delete_job(job_id: str) -> dict:
    job = store.get(job_id)
    if not job:
        raise HTTPException(404, "no such job")
    if job.status in ACTIVE:
        raise HTTPException(409, "cancel the job before deleting it")
    store.delete(job_id)
    return {"ok": True}


@app.get("/api/jobs/{job_id}/download/{which}", dependencies=[Depends(require_session)])
def download(job_id: str, which: str) -> FileResponse:
    if which not in DOWNLOADS:
        raise HTTPException(404, "no such file")
    job = store.get(job_id)
    if not job:
        raise HTTPException(404, "no such job")
    path = store.job_dir(job_id) / "out" / DOWNLOADS[which]
    if not path.exists():
        raise HTTPException(404, "not generated for this job")
    stem = Path(job.name).stem
    return FileResponse(path, media_type="text/csv", filename=f"{stem}-{which}.csv")


def job_json(job) -> dict:
    return {
        "id": job.id,
        "name": job.name,
        "status": job.status,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "min_tx": job.min_tx,
        "mode": job.mode,
        "categories": job.categories,
        "cu_per_second": job.cu_per_second,
        "total": job.total,
        "malformed": job.malformed,
        "stage": job.stage,
        "stage_done": job.stage_done,
        "stage_total": job.stage_total,
        "resumed": job.resumed,
        "percent": job.percent,
        "passed": job.passed,
        "filtered": job.filtered,
        "errors": job.errors,
        "rate_limit_hits": job.rate_limit_hits,
        "message": job.message,
        "running": job.id == runner.current,
    }
