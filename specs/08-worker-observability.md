# Spec 08 — Worker Observability

**Status:** Accepted — implemented
**Depends on:** `specs/03-worker-runtime.md`, `specs/07-redis-dispatch.md`
**Extends:** `specs/02-api-core.md` §7

## 1. Scope

What an operator can see once workers exist, and specifically: **how they diagnose a queue that is stuck or slow, at 3am, without reading the code.** That question is the design brief for this document, not a nice-to-have on top of it.

## 2. `GET /health`

```json
{
  "status": "ok",
  "version": "0.1.0",
  "uptime_seconds": 412,
  "database": "ok",
  "redis": "ok",
  "queue": {
    "scheduled": 2, "pending": 17, "processing": 4,
    "completed": 340, "failed": 3, "cancelled": 1,
    "oldest_pending_seconds": 12,
    "ready_hints": 17
  },
  "workers": {
    "count": 4,
    "ids": ["hostA-7-0", "hostA-7-1", "hostB-9-0", "hostB-9-1"]
  }
}
```

There is no heartbeat age here. The keys carry a TTL of twice the heartbeat interval, so a key being present *is* the freshness guarantee — reporting an age as well would mean storing a timestamp in every key to answer a question the key's existence already answers.

`503` when the database is unreachable. Redis being unreachable is **not** a `503` — the system is still processing jobs, and taking the service out of a load balancer for a degraded cache would turn a latency problem into an outage.

### `oldest_pending_seconds` is the field that matters

Queue depth alone cannot distinguish load from failure. Depth and age together can:

| Depth | Oldest pending | Reading |
|---|---|---|
| high | low | busy, keeping up |
| high | high | **not keeping up** — workers too few, or too slow |
| low | high | **stuck** — a job nothing will claim, or no workers at all |
| low | low | healthy |

`SELECT min(created_at) FROM jobs WHERE status = 'pending'` — an index-only scan over the partial claim index, which contains only pending rows.

### `workers`, and the difference between zero and unknown

Populated from the Redis liveness keys (spec 07 §7). When Redis is unreachable the field is `null` and `"redis": "error"` — never `count: 0`.

**"I cannot see the workers" and "there are no workers" are different incidents** with different responses, and reporting the second when the first is true sends an operator to restart healthy workers during a Redis outage.

`ready_hints` is `ZCARD` of the dispatch set. Comparing it to `pending` is a direct read on dispatch health: they should track, and a large gap means announcements are failing and everything is arriving via the slower fallback path.

## 3. Log events

Every state transition already writes a `job_logs` row and emits a structured line through one helper (spec 02 §7). Part 2 adds worker context to that helper — `worker_id`, `attempt`, and `duration_ms` on terminal events — and the following events:

| Event | Level | Fields beyond the standard set |
|---|---|---|
| `job.claimed` | info | `worker_id`, `attempt`, `priority`, `queued_seconds` |
| `job.completed` | info | `worker_id`, `attempt`, `duration_ms` |
| `job.failed_attempt` | warning | `worker_id`, `attempt`, `error_type`, `retry_in_seconds` |
| `job.failed` | error | `worker_id`, `attempt`, `error_type` |
| `job.lease_lost` | warning | `worker_id`, `attempt` — this worker was displaced mid-execution |
| `job.reaped` | warning | `worker_id` of the previous holder, `expired_for_seconds` |
| `job.promoted` | info | how late the promotion was against `scheduled_at` |
| `worker.started` / `worker.stopping` / `worker.stopped` | info | `worker_id`, `slots`, `in_flight` |
| `worker.forced_release` | warning | `job_id` — shutdown grace expired with work in flight |

`queued_seconds` on `job.claimed` is queue latency measured per job; it is the value to aggregate when asking whether the queue is keeping up, and it needs no extra storage because `created_at` is already on the row.

`job.lease_lost` and `job.reaped` are the two events that indicate the system corrected itself. Neither is an error — recovery is the system working — but both are `warning` because a healthy deployment produces almost none, and a sudden run of them means workers are dying, hanging, or the lease is too short for the work.

Payloads are never logged (spec 02 §6). Job ids, types and statuses are.

## 4. Diagnosing a stuck queue

The sequence this is designed to support, in order:

1. **`GET /health`.** Depth and `oldest_pending_seconds` classify the problem per the table above; `workers.count` says whether anything is alive to do the work.
2. **`GET /jobs?status=processing`.** Anything here with an old `started_at` is a job that is hung rather than slow — its worker is alive enough to hold the lease but is not finishing.
3. **`GET /jobs?status=failed`.** A cluster of failures with the same `error.type` is a downstream dependency, not a queue problem.
4. **`job_logs` for one job id.** The full transition history: every claim, every failed attempt with its error, every reap. This is what answers "why is *this* job in this state", which is the question that actually gets asked.
5. **The log stream, filtered by `worker_id`.** A single worker misbehaving is visible here and nowhere else.

Everything in that sequence is reachable through the API or the log stream. None of it needs a database session, and none of it needs the code.

## 5. Acceptance criteria

- `/health` reports queue depth, `oldest_pending_seconds`, worker count and ids, and `redis` status.
- With Redis stopped, `/health` returns `200` with `"redis": "error"` and `workers: null` — not `count: 0`, and not `503`.
- With the database stopped, `/health` returns `503`.
- `oldest_pending_seconds` is `null` when nothing is pending, and tracks the oldest pending job otherwise.
- A job that is claimed, fails, retries and completes produces one `job_logs` row per transition, in order, each with the `worker_id` that made it.
- Every log line emitted by the worker is valid JSON carrying `service` and `worker_id`.
- Killing a worker mid-job produces a `job.reaped` warning naming the dead worker.
