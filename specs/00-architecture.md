# Spec 00 — System Architecture

**Status:** Accepted
**Supersedes:** —
**Related decisions:** DECISIONS.md §0, §1, §3

## 1. Scope

Defines the component boundaries of the job queue service, which component owns which piece of state, and the exact flow of a job from submission to terminal state. Every later spec builds on the invariants declared here (§6) and may not violate them.

## 2. Stack

| Concern | Choice |
|---|---|
| Language | Python 3.11+ |
| API | FastAPI (Pydantic payload validation, generated OpenAPI) |
| State of record | PostgreSQL |
| Dispatch queue | Redis (sorted set) |
| Packaging | Docker Compose — API, worker(s), Postgres, Redis |

The worker does not import FastAPI. Claiming, execution, retry and recovery live in a core layer that both the API process and the worker process depend on; the web framework is a detail of the API layer only.

## 3. Core decision

**PostgreSQL is the single source of truth. Redis is a dispatch hint.**

Redis holds the set of jobs that are ready to run and is the fast path workers pull from. It never decides anything: every state transition is an conditional `UPDATE` against Postgres, and a worker only owns a job if that `UPDATE` reports a row was changed.

The consequence that matters: **the system is correct with Redis stopped.** Redis loss degrades latency, never correctness, and the system self-heals when it returns. This is verifiable — see §8.

## 4. Components

```
                        ┌──────────────┐
                        │    CLIENT    │
                        └──────┬───────┘
                               │ HTTP
                               ▼
        ┌──────────────────────────────────────────────┐
        │                API — FastAPI                  │
        │   Pydantic validation → INSERT → commit       │
        └────────┬─────────────────────────┬───────────┘
                 │ ① INSERT + COMMIT       │ ② ZADD  (only after commit)
                 ▼                         ▼
   ┌──────────────────────────┐   ┌─────────────────────────┐
   │       PostgreSQL          │   │         Redis           │
   │  ═══ SOURCE OF TRUTH ═══  │   │  ═══ DISPATCH HINT ═══  │
   │                           │   │                         │
   │  jobs                     │   │  ZSET  jobs:ready       │
   │   status, priority,       │   │    member = job_id      │
   │   attempts, scheduled_at, │   │    score  = priority|ts │
   │   worker_id, lease_until, │   │                         │
   │   result, error, progress │   │  queue depth / stats    │
   │  job_logs                 │   │                         │
   └────▲──────────────▲───────┘   └───────────┬─────────────┘
        │              │                       │
        │ ④ conditional│ ⑤ fallback            │ ③ BZPOPMIN
        │    UPDATE    │  SKIP LOCKED scan     │   → job_id
        │              │                       │
   ┌────┴──────────────┴───────────────────────┴─────────────┐
   │                      WORKER  × N                         │
   │   claim → execute → heartbeat → conditional write        │
   └──────────────────────────────────────────────────────────┘

   ┌──────────────────────────────────────────────────────────┐
   │  REAPER   (expired lease → back to pending)              │
   └──────────────────────────────────────────────────────────┘
```

Arrow ④ is the only arrow that decides anything. Everything reaching a worker through Redis is a hint that may be stale, duplicated, or missing.

## 5. Flows

### 5.1 Submission

1. API validates the payload against the schema registered for that job type. An invalid payload is rejected here and never persisted.
2. `INSERT` into `jobs` with `status='pending'` (or `'scheduled'` when a future `scheduled_at` is supplied) → **commit**.
3. **After the commit**, `ZADD jobs:ready <score> <job_id>`.
4. Respond to the client.

Ordering is deliberate: pushing to Redis before the commit lets a worker pop the id and query Postgres before the row is visible, get zero rows, and drop the hint. Not a correctness bug, but a latency bug that is hard to trace.

### 5.2 Claim — hot path

```
BZPOPMIN jobs:ready 5     →  job_id
```

```sql
UPDATE jobs
SET status='processing', worker_id=$me,
    lease_until=now() + interval '60 seconds',
    started_at=now(), attempts=attempts+1
WHERE id=$job_id AND status='pending'
RETURNING *;
```

Zero rows means the hint was stale — the job was cancelled, already claimed, or no longer eligible. The worker discards it and continues. **This is normal operation, not an error condition.** The `AND status='pending'` predicate is the entire defence: if two workers ever hold the same id, only one observes `pending`.

### 5.3 Claim — fallback path

When `BZPOPMIN` returns empty at its timeout, the worker asks Postgres directly:

```sql
UPDATE jobs
SET status='processing', worker_id=$me,
    lease_until=now() + interval '60 seconds',
    started_at=now(), attempts=attempts+1
WHERE id = (
  SELECT id FROM jobs
  WHERE status='pending'
    AND (scheduled_at IS NULL OR scheduled_at <= now())
  ORDER BY priority DESC, created_at ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING *;
```

`FOR UPDATE SKIP LOCKED` makes concurrent workers step over rows another worker has locked instead of queueing behind them, so N workers claim N distinct jobs without contention.

This path is **not** an emergency branch — it is exercised on every run, because four categories of job reach a worker only through it:

| Source | Why no `ZADD` exists |
|---|---|
| Scheduled job becoming due | No process is present at the moment the time arrives |
| Retry whose backoff elapsed | Same |
| Job released by the reaper | Covered here regardless of whether the reaper re-pushes |
| A `ZADD` that failed, or Redis data loss | The hint simply does not exist |

Because the guaranteeing path is also a continuously exercised path, there is no untested branch holding up correctness.

### 5.4 Completion

```sql
UPDATE jobs SET status='completed', result=$1, completed_at=now()
WHERE id=$job AND worker_id=$me AND status='processing';
```

`AND worker_id=$me` is required. If the lease expired and the reaper reassigned the job, this reports zero rows and **the worker discards its own result** — it is no longer the owner. Without the predicate, a slow worker would overwrite the result of the worker that legitimately took over.

### 5.5 Failure and retry

- `attempts < max_attempts` → `status='pending'`, `scheduled_at = now() + backoff`. **No `ZADD`** — the job is not yet due; the fallback path picks it up when it is.
- `attempts >= max_attempts` → `status='failed'`, error persisted.

Exact backoff timings: spec 05.

### 5.6 Cancellation racing a claim

```sql
UPDATE jobs SET status='cancelled'
WHERE id=$1 AND status IN ('pending','scheduled');
```

Cancellation and claim contend for the same row lock. Postgres serialises them; whichever arrives second sees a status its predicate does not match and reports zero rows. There is no timing window to reason about. A stale id may remain in the Redis set — a worker will pop it, get zero rows, and drop it, so the hint set cleans itself.

### 5.7 Worker crash mid-job

1. The worker extends `lease_until` on an interval while executing (heartbeat).
2. It dies; extensions stop.
3. The reaper returns the job:

```sql
UPDATE jobs SET status='pending', worker_id=NULL
WHERE status='processing' AND lease_until < now();
```

If the worker was merely slow rather than dead, its completion write (§5.4) reports zero rows and it yields. Full mechanism and timings: spec 06.

### 5.8 Redis unavailable

`BZPOPMIN` raises; the worker catches it and proceeds directly to the fallback path. All jobs continue to be processed with latency rising from milliseconds to seconds. When Redis returns, new `ZADD`s repopulate it and throughput recovers without operator action.

## 6. Invariants

Later specs and implementations must preserve all of these. They are the contract the test suite asserts against.

1. **A job transitions state only via a conditional `UPDATE` whose `WHERE` clause names the status it expects.** No read-then-write.
2. **A worker owns a job only if its claim `UPDATE` reported a changed row.** Zero rows means no ownership, always.
3. **A worker writes a result only while it still owns the job** (`worker_id` predicate).
4. **Redis is never read as authority.** Any id from Redis is revalidated against Postgres before use.
5. **Nothing is pushed to Redis before the corresponding Postgres transaction commits.**
6. **Every job reachable through Redis is also reachable through the fallback query.** Redis may only make delivery faster, never possible.

## 7. What this does not guarantee

Stated deliberately; these are design limits, not oversights.

- **Delivery is at-least-once, not exactly-once.** A worker stalled past its lease (long GC pause, network partition) can have its job reclaimed and rerun. Its own writes are rejected by invariants 2–3, but any side effect it already performed has happened. Exactly-once execution is not achievable at this layer — it requires idempotent job handlers. Handlers are written accordingly.
- **Priority ordering is per-claim, not global.** With N workers, each claims the highest-priority job *available* to it, not the globally highest. This is inherent to any concurrent queue. Consequence for testing: **priority-ordering tests must run a single worker**, or they are flaky by construction.
- **Low-priority starvation is possible.** A sustained stream of high-priority work can starve low-priority jobs. Aging is not implemented; see DECISIONS.md §5.
- **Throughput is bounded by Postgres write rate**, since every claim is a write. Ample at the scale in question; the scaling path is documented in DECISIONS.md §1.
- **Scheduled jobs fire no earlier than their time, but may fire up to one fallback interval late.** Sub-second scheduling accuracy would require a dynamically computed wait.
- **No backpressure.** Submission is unbounded.
- **Redis sorted-set scores are float64.** The priority/timestamp encoding must stay within 2^53 or FIFO ordering degrades silently. Bounds are asserted in tests; see spec 07.

## 8. Acceptance criteria

- `docker compose up` starts API, worker, Postgres and Redis; health endpoint responds.
- A job submitted via the API reaches `completed` with a result readable through the API.
- With Redis stopped, submitted jobs still reach `completed`; with Redis restarted, no manual intervention is needed.
- Concurrent workers against a seeded backlog produce zero jobs executed twice and zero jobs left in `processing`.

## 9. Alternatives considered

**Redis as the authoritative queue (Postgres for state only).** Higher throughput ceiling and instant queue-depth reads. Rejected: it makes submission a dual write across two systems with no shared transaction, so a failure between them either loses a job silently or produces a queue entry with no backing row. Repairing that properly means an outbox plus a relay — which reconstructs the Postgres-driven path anyway, with an extra moving part. It also makes cancellation of an already-queued job awkward (entries cannot be efficiently removed from the middle of a queue, so cancelled jobs must be tombstoned and skipped), and it places the least durable component in charge of work that has not run yet.

**Redis Streams with consumer groups.** The only Redis structure with built-in at-least-once delivery, a pending-entries list, and `XAUTOCLAIM` for recovering messages from dead consumers — effectively free crash recovery. Rejected because streams are strictly time-ordered and offer **no priority support**; recovering priority requires one stream per level read in order, which forfeits the clean blocking read that made streams attractive.

**Redis lists with `BRPOP` across priority keys.** `BRPOP` checks keys in argument order, giving blocking priority dequeue in one command. Rejected: only discrete levels with strict precedence (no aging), and the reliable variant `BLMOVE` accepts a single source key — so reliability and multi-key priority cannot be had together.

**An existing framework (Celery, RQ, Dramatiq, arq).** The likely production choice. Rejected here because job pickup, crash recovery and retry strategy are precisely what this exercise asks to design and document; delegating them would leave nothing to evaluate.

**Postgres alone with `LISTEN/NOTIFY` as the wakeup channel.** Clean, and removes a component entirely. Rejected: the assignment requires a queue/cache technology for the job queue, and `LISTEN/NOTIFY` is fire-and-forget with the same reliability profile as Redis pub/sub, so it trades a required component for no reliability gain.
