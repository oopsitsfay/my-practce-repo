# Wallet filter — web app

Upload a wallet list in the browser, the job runs in the background, download the
filtered CSV when it finishes. Same engine as the CLI in `../wallet-filter`, so
counting, pacing and checkpointing behave identically.

## Why it is built this way

**Jobs run in the background, not in the request.** A batch of ~8,000 wallets
takes 45–60 minutes on a free-tier Alchemy key. That is far past any HTTP
timeout, so uploading only queues the job; the browser polls for progress.

**One job at a time.** The Alchemy budget is per key, not per job. Two jobs in
parallel do not go twice as fast — they halve each other's throughput and start
colliding with the rate limit. Queuing is the honest behaviour, so the UI says
so rather than pretending otherwise.

**Restarts are survivable.** Free hosting recycles containers without warning.
Every answer is checkpointed as it arrives, and anything left mid-flight is
requeued on boot, so a restart costs the current batch rather than the run.

## Running it locally

```bash
pip install -r requirements.txt

export ALCHEMY_URL="https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY"
export APP_PASSWORD="pick-something"
uvicorn app:app --host 0.0.0.0 --port 7860
```

Then open <http://localhost:7860>. If `APP_PASSWORD` is unset a random one is
generated and printed to the log — the app never runs without a password, since
an open instance would let anyone spend your compute units.

| Variable | Meaning |
| --- | --- |
| `ALCHEMY_URL` | Your endpoint, key included. Required. |
| `APP_PASSWORD` | The single sign-in password. Generated if unset. |
| `DATA_DIR` | Where jobs and results live. Defaults to `./data`. |
| `PORT` | Listen port. Defaults to 7860. |
| `MAX_UPLOAD_BYTES` | Upload cap. Defaults to 25 MB (~700k addresses). |

## Deploying free, on Hugging Face Spaces

Free CPU, no credit card, and — unlike most free tiers — it does not sleep after
15 minutes of inactivity, which matters when a job runs for an hour.

1. Create a Space at <https://huggingface.co/new-space>. Pick **Docker → Blank**,
   and set it **Private**.
2. Add this repository's contents to the Space (push this repo to it, or upload
   `Dockerfile`, `webapp/` and `wallet-filter/filter_wallets.py`).
3. Put this at the top of the Space's own `README.md` — Spaces reads it as
   configuration:

   ```yaml
   ---
   title: Wallet Filter
   sdk: docker
   app_port: 7860
   pinned: false
   ---
   ```

4. In **Settings → Variables and secrets**, add two *secrets*:
   `ALCHEMY_URL` and `APP_PASSWORD`.
5. The Space builds and starts itself. Open it, sign in with `APP_PASSWORD`.

**The one real limitation:** free Spaces have ephemeral storage, so a rebuild
wipes finished jobs. Download results you care about rather than treating the
Space as an archive. Persistent storage is a paid add-on, and any of the
migration targets below solve it too.

## Moving somewhere else later

It is a plain Docker container with one writable directory, so switching hosts
is configuration rather than a rewrite:

- **Railway / Render** (~$5/mo) — connect the repo, set the two environment
  variables, add a volume mounted at `DATA_DIR` for persistence.
- **Fly.io** — `fly launch` reads the Dockerfile; attach a volume for `DATA_DIR`.
- **Any VPS** — `docker build -t wallet-filter . && docker run -d -p 80:7860
  -e ALCHEMY_URL=... -e APP_PASSWORD=... -v /srv/wf:/app/webapp/data wallet-filter`.

Nothing in the app assumes a particular host. The only stateful thing is
`DATA_DIR`.

## What is in a job

Each job gets `DATA_DIR/jobs/<id>/` holding the uploaded `input.csv` and an
`out/` directory with `passed.csv`, `filtered.csv`, `results.csv`, an
`errors.csv` when some wallets could not be resolved, and the `checkpoint.jsonl`
that makes resuming work. Job metadata lives in `DATA_DIR/jobs.db` (SQLite).

**Resume** re-queues a finished job; because the checkpoint is intact, only
unresolved wallets are retried. That is the button to press when a run finishes
with errors. **Cancel** stops at the next clean batch boundary and keeps
everything already answered.

## API

Every route except `/healthz` needs the session cookie from `POST /login`.

```
POST   /api/jobs                       upload + queue      → job
GET    /api/jobs                       list jobs
GET    /api/jobs/{id}                  one job with progress
POST   /api/jobs/{id}/cancel           stop at the next boundary
POST   /api/jobs/{id}/resume           re-queue, retrying only what is unresolved
DELETE /api/jobs/{id}                  remove the job and its files
GET    /api/jobs/{id}/download/{which} passed | filtered | results | errors
GET    /healthz                        liveness, no auth
```

## Tests

```bash
pip install pytest
python3 -m pytest tests/ -q
```

21 tests covering auth, upload validation, a full job through the queue with the
correct split, downloads, resume, delete, and requeue-after-restart — all against
a fake RPC, so no network and no compute units.
