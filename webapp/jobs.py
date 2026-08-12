"""Job store and background runner for the wallet filter.

Jobs are long -- an hour is normal on a free-tier key -- so nothing here runs
inside a web request. A job is written to disk, queued, and worked by a single
background thread; the browser polls for progress.

Only one job runs at a time, deliberately. The Alchemy budget is per key, not
per job, so two jobs in parallel do not go twice as fast: they halve each
other's throughput and start colliding with the rate limit. Queuing is the
honest behaviour.
"""

from __future__ import annotations

import json
import queue
import shutil
import sqlite3
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "wallet-filter"))

import filter_wallets as fw  # noqa: E402

QUEUED, RUNNING, DONE, FAILED, CANCELLED = "queued", "running", "done", "failed", "cancelled"
ACTIVE = (QUEUED, RUNNING)

DOWNLOADS = {
    "passed": "passed.csv",
    "filtered": "filtered.csv",
    "results": "results.csv",
    "errors": "errors.csv",
}


@dataclass
class Job:
    id: str
    name: str
    status: str = QUEUED
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None

    # settings
    min_tx: int = 4
    mode: str = "both"
    categories: str = "external"
    cu_per_second: int = 330

    # input
    total: int = 0
    malformed: int = 0

    # progress
    stage: str = ""
    stage_done: int = 0
    stage_total: int = 0
    resumed: int = 0

    # outcome
    passed: int = 0
    filtered: int = 0
    errors: int = 0
    rate_limit_hits: int = 0
    message: str = ""

    @property
    def percent(self) -> float:
        if self.status == DONE:
            return 100.0
        if not self.stage_total:
            return 0.0
        # Two stages, so outbound fills the first half and inbound the second.
        # Rough by design: how many wallets need stage two is not known up front.
        share = self.stage_done / self.stage_total
        if self.mode != "both":
            return round(share * 100, 1)
        base = 0.0 if self.stage in ("", "outbound") else 50.0
        return round(min(base + share * 50, 99.9), 1)


class JobStore:
    """SQLite-backed job records. One row per job, the job itself as JSON."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(self.root / "jobs.db", check_same_thread=False)
        self._db.execute("CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, created REAL, data TEXT)")
        self._db.commit()

    def job_dir(self, job_id: str) -> Path:
        return self.root / "jobs" / job_id

    def save(self, job: Job) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO jobs (id, created, data) VALUES (?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET data = excluded.data",
                (job.id, job.created_at, json.dumps(asdict(job))),
            )
            self._db.commit()

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            row = self._db.execute("SELECT data FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return Job(**json.loads(row[0])) if row else None

    def list(self, limit: int = 50) -> list[Job]:
        with self._lock:
            rows = self._db.execute("SELECT data FROM jobs ORDER BY created DESC LIMIT ?", (limit,)).fetchall()
        return [Job(**json.loads(row[0])) for row in rows]

    def delete(self, job_id: str) -> None:
        with self._lock:
            self._db.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
            self._db.commit()
        shutil.rmtree(self.job_dir(job_id), ignore_errors=True)


class Runner:
    """Single worker thread pulling jobs off a queue."""

    def __init__(self, store: JobStore, alchemy_url: str | None) -> None:
        self.store = store
        self.alchemy_url = alchemy_url
        self._queue: queue.Queue[str | None] = queue.Queue()
        self._cancelled: set[str] = set()
        self._current: str | None = None
        self._lock = threading.Lock()
        self._shutdown = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="job-runner")

    # ---------------------------------------------------------------- control

    def start(self) -> None:
        self.requeue_interrupted()
        self._thread.start()

    def submit(self, job: Job) -> None:
        self.store.save(job)
        self._queue.put(job.id)

    def stop(self, timeout: float = 10.0) -> None:
        """Stop the worker, letting the running job checkpoint and bail out.

        Worker threads inside the engine's pools are not daemons, so without an
        explicit stop the process cannot exit while a job is in flight.
        """
        self._shutdown.set()
        self._queue.put(None)
        if self._thread.is_alive():
            self._thread.join(timeout)

    def cancel(self, job_id: str) -> None:
        with self._lock:
            self._cancelled.add(job_id)
        job = self.store.get(job_id)
        # A job still waiting in the queue is cancelled here and skipped later;
        # a running one notices the flag at its next batch boundary.
        if job and job.status == QUEUED:
            job.status = CANCELLED
            job.finished_at = time.time()
            job.message = "cancelled before it started"
            self.store.save(job)

    def requeue_interrupted(self) -> None:
        """Re-queue anything left mid-flight by a restart.

        Free hosting restarts containers without warning. The engine checkpoints
        every answer, so a resumed job continues rather than starting over.
        """
        for job in self.store.list(limit=500):
            if job.status in ACTIVE:
                job.status = QUEUED
                job.message = "requeued after a restart"
                self.store.save(job)
                self._queue.put(job.id)

    @property
    def current(self) -> str | None:
        return self._current

    def _stop_reason(self, job_id: str) -> str | None:
        """Why this job should stop, if it should.

        A shutdown and a user cancellation both stop the work, but they mean
        opposite things afterwards: a redeploy must leave the job queued so it
        resumes, while a cancellation is a decision to be remembered.
        """
        if self._shutdown.is_set():
            return "shutdown"
        with self._lock:
            return "cancelled" if job_id in self._cancelled else None

    def _should_stop(self, job_id: str) -> bool:
        return self._stop_reason(job_id) is not None

    # ---------------------------------------------------------------- worker

    def _loop(self) -> None:
        while True:
            job_id = self._queue.get()
            if job_id is None or self._shutdown.is_set():
                self._queue.task_done()
                return
            try:
                self._run_one(job_id)
            except Exception as exc:  # noqa: BLE001 - the worker must outlive any single job
                job = self.store.get(job_id)
                if job:
                    job.status = FAILED
                    job.message = f"unexpected error: {exc!r}"
                    job.finished_at = time.time()
                    self.store.save(job)
            finally:
                self._current = None
                self._queue.task_done()

    def _run_one(self, job_id: str) -> None:
        job = self.store.get(job_id)
        if not job or job.status == CANCELLED:
            return
        reason = self._stop_reason(job_id)
        if reason == "shutdown":
            return  # left queued, so the next boot picks it up untouched
        if reason:
            job.status = CANCELLED
            job.finished_at = time.time()
            self.store.save(job)
            return

        if not self.alchemy_url:
            job.status = FAILED
            job.message = "no ALCHEMY_URL configured on the server"
            job.finished_at = time.time()
            self.store.save(job)
            return

        self._current = job_id
        job.status = RUNNING
        job.started_at = job.started_at or time.time()
        job.message = ""
        self.store.save(job)

        work_dir = self.store.job_dir(job_id)
        out_dir = work_dir / "out"
        out_dir.mkdir(parents=True, exist_ok=True)

        addresses, malformed = fw.load_addresses(work_dir / "input.csv")
        job.total = len(addresses)
        job.malformed = len(malformed)
        self.store.save(job)

        categories = [c.strip() for c in job.categories.split(",") if c.strip()]
        cfg = fw.Config(
            mode=job.mode,
            threshold=job.min_tx,
            categories=categories,
            batch_size=fw.safe_batch_size(job.cu_per_second),
            workers=3,
        )

        checkpoint_path = out_dir / "checkpoint.jsonl"
        cached = fw.load_checkpoint(checkpoint_path)
        known = set(addresses)
        partials = [r for a, r in cached.items() if r.partial and a in known]
        finished = {a: r for a, r in cached.items() if not r.partial and a in known}
        pending = [a for a in addresses if a not in cached]
        job.resumed = len(cached)
        self.store.save(job)

        client = fw.AlchemyClient(self.alchemy_url, cu_per_second=job.cu_per_second)
        writer = fw.CheckpointWriter(checkpoint_path)
        last_saved = 0.0

        # A job being rate limited completes no batches, so progress callbacks
        # stop firing and the UI would show a frozen 0% with no explanation.
        # This publishes the rate-limit count regardless of progress.
        beat_stop = threading.Event()

        def heartbeat() -> None:
            while not beat_stop.wait(3.0):
                if client.rate_limit_hits != job.rate_limit_hits:
                    job.rate_limit_hits = client.rate_limit_hits
                    self.store.save(job)

        beat = threading.Thread(target=heartbeat, daemon=True, name=f"beat-{job_id}")
        beat.start()

        def on_progress(stage: str, done: int, total: int) -> None:
            nonlocal last_saved
            job.stage, job.stage_done, job.stage_total = stage, done, total
            job.rate_limit_hits = client.rate_limit_hits
            # Throttled so a fast stage does not turn into a write per batch.
            if time.time() - last_saved > 1.0 or done == total:
                self.store.save(job)
                last_saved = time.time()

        try:
            results = fw.run(
                client,
                pending,
                cfg,
                writer,
                on_progress,
                resume_partials=partials,
                should_stop=lambda: self._should_stop(job_id),
            )
            failed = [r.address for r in results if r.error]
            if failed:
                job.stage = "retrying"
                self.store.save(job)
                retried = fw.run(
                    client,
                    failed,
                    fw.replace(cfg, workers=1),
                    writer,
                    on_progress,
                    should_stop=lambda: self._should_stop(job_id),
                )
                results = [r for r in results if not r.error] + retried
        except fw.Cancelled:
            if self._stop_reason(job_id) == "shutdown":
                job.status = QUEUED
                job.message = "paused by a restart -- it will continue automatically"
            else:
                job.status = CANCELLED
                job.finished_at = time.time()
                job.message = "cancelled -- progress is saved, resume by running it again"
            self.store.save(job)
            return
        except fw.RpcError as exc:
            job.status = FAILED
            job.finished_at = time.time()
            job.message = str(exc)
            self.store.save(job)
            return
        finally:
            beat_stop.set()
            writer.close()

        counts = fw.write_outputs(out_dir, list(finished.values()) + results, job.min_tx)
        job.passed = counts["passed"]
        job.filtered = counts["filtered"]
        job.errors = counts["errors"]
        job.rate_limit_hits = client.rate_limit_hits
        job.status = DONE
        job.finished_at = time.time()
        job.stage = ""
        job.message = (
            f"{counts['errors']:,} wallets could not be resolved -- run it again to retry just those"
            if counts["errors"]
            else ""
        )
        self.store.save(job)


def new_job(name: str, min_tx: int, mode: str, categories: str, cu_per_second: int) -> Job:
    return Job(
        id=uuid.uuid4().hex[:12],
        name=name,
        min_tx=min_tx,
        mode=mode,
        categories=categories,
        cu_per_second=cu_per_second,
    )
