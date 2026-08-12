# Wallet filter — Vercel control panel

A web page at your own URL that uploads a wallet list, starts the GitHub Actions
run, and hands you the results. Free on both halves.

## Why the work happens on GitHub, not on Vercel

Vercel has no long-running processes: every request is a serverless function
killed well under a minute on the free plan, and there is no disk between
requests for a checkpoint to live on. A wallet run takes about an hour. So
Vercel does the part it is good at and GitHub does the part it is good at:

```
this panel  ──▶  commits your CSV, triggers the workflow   (a second per request)
GitHub      ──▶  runs the filter, up to six hours          (free)
this panel  ◀──  reads status and the published summary
```

The one thing you give up compared with running the app in `../webapp` on a
paid host: progress is coarse. GitHub reports *queued / running / completed*,
not "3,412 of 7,871". Everything else — upload, settings, history, downloads —
is the same.

## Deploying

You need a **GitHub token** so the panel can act on your repository.

1. Go to <https://github.com/settings/personal-access-tokens/new> (fine-grained
   token).
   - **Repository access** → *Only select repositories* → pick your repo
   - **Permissions → Repository permissions**:
     - *Contents*: **Read and write** (to upload wallet lists)
     - *Actions*: **Read and write** (to start runs and fetch results)
   - Generate it and copy the `github_pat_…` value
2. Go to <https://vercel.com/new> and import this repository.
   - **Root Directory** → set to `panel`
   - **Environment Variables** → add:

     | Name | Value |
     | --- | --- |
     | `GITHUB_TOKEN` | the `github_pat_…` token from step 1 |
     | `GITHUB_REPO` | `owner/repo`, e.g. `oopsitsfay/my-practce-repo` |
     | `APP_PASSWORD` | a password you choose, to sign in to the panel |
     | `SESSION_SECRET` | any long random string |
3. Deploy. Open the URL, sign in with `APP_PASSWORD`.

`ALCHEMY_URL` is **not** needed here — it stays a GitHub Actions secret, since
that is where the filter actually runs. The panel never sees your Alchemy key.

Set `SESSION_SECRET` rather than leaving it out: without it a new random secret
is generated per cold start, which signs you out at unpredictable moments.

## Using it

Pick a CSV, set the threshold and mode, **Start run**. The panel uploads the
file to `uploads/` in your repository and triggers the workflow. The run appears
in the list within a few seconds; when it finishes you get the split and a
**results.zip** download.

Re-uploading the same list resumes rather than restarting — the workflow caches
progress, so only wallets without an answer are queried again.

## Environment variables

| Name | Required | Meaning |
| --- | --- | --- |
| `GITHUB_TOKEN` | yes | Fine-grained PAT with Contents and Actions read/write |
| `GITHUB_REPO` | yes | `owner/repo` holding the workflow |
| `APP_PASSWORD` | yes | Single sign-in password for the panel |
| `SESSION_SECRET` | recommended | Signs the session cookie; random per cold start if unset |
| `GITHUB_BRANCH` | no | Defaults to `main` |
| `WORKFLOW_FILE` | no | Defaults to `filter-wallets.yml` |

## Notes on how it works

**Sessions are a signed cookie, not server memory.** Serverless functions share
no state, so the cookie carries an HMAC of its issue time and every function
verifies it independently.

**Run outcomes come from the repository, not from artifacts.** The workflow
commits `results/<run_id>.json` after each run. Artifacts can only be fetched as
a zip, which is a lot of work for six numbers; a committed summary is one API
call and leaves a browsable history.

**Downloads are a redirect.** The panel asks GitHub for the artifact, GitHub
answers with a short-lived signed URL, and the panel passes that to your browser
— so the zip never travels through the function.

## Tests

```bash
pip install pytest fastapi requests python-multipart httpx
python3 -m pytest tests/ -q
```

19 tests covering sign-in, the signed cookie surviving a cold start, upload
validation, commit-then-dispatch with the right inputs, filename sanitising,
run listing with published summaries, the download redirect, expired artifacts,
and clear errors when the token or configuration is wrong — all against a fake
GitHub API, so no network and no repository writes.
