# Spec 10 — Job History

**Status:** Accepted — implemented
**Depends on:** `specs/01-data-model.md` (the `job_logs` table), `specs/02-api-core.md` (the HTTP contract)
**Related decisions:** DECISIONS.md §7

## 1. Scope

`job_logs` has been written since part 1. Every state transition — from the API and from the worker alike — goes through one helper, `record_transition`, which writes the audit row and emits the structured log line together (spec 08 §3). Nothing reads it back.

This adds the read path: `GET /jobs/{job_id}/logs`.

**Why it is missing until now is worth stating.** `job_logs` was built for the operator's question *why is this job in the state it is in*, and until this spec the only way to answer it was `psql`. The row on `jobs` carries the **latest** error and nothing else, so a job that failed twice and then succeeded presents as a plain success, and a job that was reaped three times presents as a job on attempt four with no explanation of the other three. The history exists; it simply had no door.

## 2. `GET /jobs/{job_id}/logs`

Query: `limit` (1–100, default 20), `offset` (≥ 0) — the same bounds as `GET /jobs`, enforced the same way.

```json
{
  "items": [
    {"level": "info",    "message": "Job created with status pending",
     "meta": {"payload_bytes": 84, "payload_sha256": "3f2a9c1d4b7e0a55"},
     "created_at": "2026-08-05T09:00:00.120Z"},
    {"level": "info",    "message": "Claimed by hostA-7-0",
     "meta": {"priority": 5, "queued_seconds": 0.4, "from_hint": true},
     "created_at": "2026-08-05T09:00:00.550Z"},
    {"level": "warning", "message": "Attempt 1 failed; retrying",
     "meta": {"status": "pending", "error_type": "WebhookFailed"},
     "created_at": "2026-08-05T09:00:02.900Z"}
  ],
  "limit": 20, "offset": 0, "has_more": false
}
```

| Response | Condition |
|---|---|
| `200` | The job exists; its history is returned, oldest first |
| `404` | No such job |
| `422` | The path segment is not a UUID, or `limit`/`offset` are out of range |

### Oldest first — the opposite of `GET /jobs`, deliberately

The job list is newest-first because it is a feed: an operator wants what just happened. A single job's history is a **timeline**, read start to finish to reconstruct a sequence, so it is ordered `created_at ASC, id ASC`. The `id` tie-break matters: `job_logs.id` is a `BIGSERIAL`, so two rows written inside the same transaction share a `created_at` from `now()` and only the sequence separates them. Ordering on the timestamp alone would let the claim and its failure appear in either order — a timeline that is occasionally, invisibly, wrong.

This is served by `ix_job_logs_job` on `(job_id, created_at)`, created in the first migration. **No schema change.**

### `404`, not an empty list

An unknown job and a job with no history are different answers, and a client that cannot tell them apart will misreport one as the other. The service reuses the existing `JobService.get`, so the not-found path is the one already covered by `JobNotFoundError` → `404` — one behaviour, not a second implementation of it.

### A sub-resource, not a field on `GET /jobs/{job_id}`

Embedding the history in the job response was rejected. History is unbounded: a job reaped repeatedly, or one manually retried several times, accumulates rows indefinitely, while `GET /jobs/{job_id}` is the endpoint a client polls in a loop while waiting for a result. Embedding would make the most frequently fetched response in the system grow without limit, and would force a `job_logs` scan on every poll for a caller that almost always only wants `status`.

A sub-resource keeps the cost where the interest is, and pagination is then available for free rather than needing a nested limit.

### `has_more`, not `total`

Identical to `GET /jobs` (spec 02 §3), for the identical reason: `limit + 1` rows are fetched and the extra is discarded. No `COUNT(*)`.

## 3. What is exposed, and what is not

`meta` is returned as stored. This is safe by construction rather than by filtering — the keys that are ever written are:

| Key | Written by |
|---|---|
| `payload_bytes`, `payload_sha256` | submission (`payload_fingerprint`, spec 02 §7) |
| `priority`, `queued_seconds`, `from_hint` | `job.claimed` |
| `status` | every transition that changes one |
| `error_type` | `job.failed_attempt`, `job.failed` |
| `dead_letter_reason` | `job.dead_lettered` |
| `worker_id` | claim, release, reap |

**Payload contents are never among them.** Submission already records a fingerprint and a size instead of the payload itself, precisely so that logs carry no user data (spec 02 §7), and `error_type` is the exception class name rather than its message. The same rule that keeps the log stream clean keeps this endpoint clean, and a test pins it here as `E2E-28b` pins it there.

Stored error *messages* are not in `job_logs` at all — they live in `jobs.error`, which `JobResponse` already exposes and which is truncated at write time.

## 4. Layering

Unchanged from spec 02 §2, and worth restating because this is the first endpoint added since the worker existed:

```
GET /jobs/{id}/logs → JobService.get_logs → JobRepository.list_logs → PostgreSQL
```

`list_logs` is a read-only `SELECT` sitting beside `add_log`. It touches nothing the worker depends on: no claim, no lease, no ownership predicate, no write of any kind.

## 5. Acceptance criteria

- A job that has been created, claimed, failed and retried returns those events in that order, oldest first.
- Two rows written in one transaction come back in the order they were written.
- An unknown job id returns `404`; a `limit` of 0 or 101 returns `422`.
- Paging with `limit=2` twice returns four distinct rows and reports `has_more` correctly at each step.
- No response from this endpoint contains any value from a job's `payload`.
- The endpoint performs no write: a job's `status`, `attempts` and `updated_at` are unchanged by reading its history.
