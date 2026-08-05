# Job Queue Service

A distributed background job processing system: an HTTP API for submitting and inspecting jobs, a PostgreSQL-backed job store, and worker processes that claim and execute them.

Submit a job over HTTP and a worker picks it up, runs it, and records the result — correctly with several workers running at once, when a handler fails, and when a worker is killed mid-job.

A job that fails is retried with backoff; a job that *cannot run* is dead-lettered and will not be handed back out.

```bash
docker compose up --build
```

PostgreSQL, Redis, the API and **two worker processes running two slots each** — four jobs in flight at once. Migrations apply on start; the service listens on <http://localhost:8000>, with interactive documentation at <http://localhost:8000/docs>.

---

## What is implemented, and where

Every requirement in the assignment, with the code that satisfies it and the test that proves it. Test IDs are the actual test names in the suite (`pytest -k w2_03`), and the full matrix is [`specs/TEST_PLAN.md`](specs/TEST_PLAN.md).

### Must have

| Requirement | Implementation | Test |
|---|---|---|
| Python 3.11+, relational database | 3.12, PostgreSQL 16, SQLAlchemy async | — |
| Queue/cache technology | Redis sorted set as a dispatch hint · [`app/dispatch/`](app/dispatch/) | `W2-18`, `W4-03` |
| Separate worker process | [`app/worker/`](app/worker/) — `python -m app.worker`, no FastAPI import | `W3-01` |
| Submit a job | `POST /jobs` · [`app/api/routes/jobs.py`](app/api/routes/jobs.py) | `E2E-01` |
| Get status, result or error | `GET /jobs/{id}` | `E2E-14` |
| List with filters | `GET /jobs?status=&job_type=` | `E2E-17`, `E2E-18` |
| Cancel a job | `POST /jobs/{id}/cancel` — conditional `UPDATE` | `E2E-20…23` |
| Retry a failed job | `POST /jobs/{id}/retry` — attempts reset | `H-01`, `E2E-29` |
| Health with queue statistics | `GET /health` · [`routes/health.py`](app/api/routes/health.py) | `E2E-24`, `W3-04` |
| Retry with backoff, 3 attempts | [`app/services/backoff.py`](app/services/backoff.py) — 30 s, 2 min, equal jitter | `W1-01…03`, `W2-12` |
| Priority-based processing | `ORDER BY priority DESC, created_at ASC` in the claim | `W2-01`, `W3-02` |
| Containerised, both services | [`docker-compose.yml`](docker-compose.yml) | — |
| Job submission and retrieval | | `E2E-01`, `E2E-14`, `L2-01` |
| Job completion flow | | `W3-01`, `W1-08` |
| Job failure and retry | | `W2-12`, `W2-13`, `W1-09` |
| Cancellation | | `E2E-20…23`, `L2-09…11` |
| Idempotency | | `E2E-11…13`, `L2-13…15`, `L2-22` |
| Priority ordering | | `W3-02`, `W2-01` |

### Should have

| Requirement | Implementation | Test |
|---|---|---|
| Scheduled jobs | `scheduled` status + promoter sweep · [`worker/maintenance.py`](app/worker/maintenance.py) | `W2-11`, `E2E-02` |
| Worker crash recovery | lease + heartbeat + reaper · [`specs/06`](specs/06-crash-recovery.md) | `W2-07…10`, `W4-02` |
| Structured JSON logging with job context | [`app/core/logging.py`](app/core/logging.py), one helper per transition | `E2E-28`, `L2-20` |
| Graceful shutdown | `SIGTERM` → finish in hand, then release leases | `W2-15`, `W2-16` |
| Health endpoint with queue stats | depth, age, worker liveness, dead letters | `E2E-24`, `W3-05` |

### Nice to have

| Requirement | Implementation | Test |
|---|---|---|
| Multiple concurrent workers | N processes × M slots; `SKIP LOCKED` claiming | `W2-03`, `W2-04`, `W4-01`, `W4-03` |
| Progress tracking for batch jobs | `report_progress` through `JobContext` | `L1-34`, `W2-14`, `W3-06` |
| Job timeout enforcement | `asyncio.wait_for(handler, BaseJob.timeout_seconds)` | `H-06`, `H-07` |
| Dead letter for poison messages | `dead_letter_reason` + refusal to retry | `H-04`, `H-08…12` |

### Beyond the assignment

| | Implementation | Test |
|---|---|---|
| Job history over HTTP | `GET /jobs/{id}/logs` · [`specs/10`](specs/10-job-history.md) | `E2E-34…37`, `L2-21` |
| Payload validation as a security boundary | per-type Pydantic schemas, SSRF rules, body size cap | `L1-20…26b`, `E2E-07`, `E2E-08` |
| Request correlation | `X-Request-ID` on every response and log line | `E2E-25` |

Two requirements are met by a property rather than by code, so they are stated rather than linked. **Idempotency keys are retained for at least 24 hours** because nothing ever expires them — there is no TTL and no retention sweep, which `L2-22` pins against the day one is added. **No polling hot-loop**: an idle slot blocks in Redis `BZPOPMIN` rather than spinning, and a busy one never waits at all.

## Submitting a test job

```bash
curl -sS -X POST http://localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{
        "job_type": "email",
        "priority": 7,
        "idempotency_key": "demo-1",
        "payload": {"to": "user@example.com", "subject": "Hello", "body": "Hi there"}
      }'
```

```json
{
  "id": "f2b953c5-516d-45c1-873c-89550c7dbb80",
  "job_type": "email",
  "status": "pending",
  "priority": 7,
  "attempts": 0,
  "max_attempts": 3,
  "progress": 0,
  "payload": {"to": "user@example.com", "subject": "Hello", "body": "Hi there", "cc": []},
  "result": null,
  "error": null,
  "idempotency_key": "demo-1",
  "scheduled_at": null,
  "created_at": "2026-08-04T08:25:33.771534Z",
  "started_at": null,
  "completed_at": null
}
```

Read it back a couple of seconds later and a worker has run it:

```bash
curl -sS http://localhost:8000/jobs/<id>
```

```json
{
  "status": "completed",
  "attempts": 1,
  "result": {"message_id": "msg-ea6e0b2a78344330"},
  "started_at": "2026-08-04T18:45:31.832816Z",
  "completed_at": "2026-08-04T18:45:33.729669Z"
}
```

Repeat that first request verbatim and the answer is `200`, not `201`, with the same id — nothing was created. Cancel the same job twice and the second attempt is `409`. And a webhook aimed at the cloud metadata endpoint never reaches the queue at all:

```bash
curl -sS -X POST http://localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"job_type":"webhook","payload":{"url":"http://169.254.169.254/latest/meta-data/"}}'
```

Everything else — listing, cancelling, retrying, submitting each job type — is easiest from <http://localhost:8000/docs>; nothing needs to be installed locally to run any of it. For a scripted walkthrough of the behaviours the design actually turns on, including crash recovery and a Redis outage, follow [`DEMO.md`](DEMO.md).

## API

| Method | Path | Behaviour |
|---|---|---|
| `POST` | `/jobs` | Submit. `201` on create; **`200`** when an `idempotency_key` matched an existing job |
| `GET` | `/jobs/{id}` | Fetch one job. `404` if unknown |
| `GET` | `/jobs/{id}/logs` | The job's history, oldest first, paged. `404` if unknown |
| `GET` | `/jobs` | List, filtered by `status` and `job_type`, paged by `limit` (1–100) and `offset` |
| `POST` | `/jobs/{id}/cancel` | Cancel a `pending` or `scheduled` job. `409` in any other state, `404` if unknown |
| `POST` | `/jobs/{id}/retry` | Requeue a `failed` job with its attempts reset. `409` if it never failed **or was dead-lettered** |
| `GET` | `/health` | Queue depth and age, worker liveness, dependency status. `503` only if the **database** is unreachable |

Errors share one shape, and never carry a traceback, SQL, or driver text:

```json
{"error": {"code": "job_not_cancellable",
           "message": "Job is completed and can no longer be cancelled",
           "request_id": "e7c0d5301d4f461ca59b2c8a521ab7cd",
           "details": []}}
```

Every response carries an `X-Request-ID` header, echoing an inbound one when it is well-formed, and the same id appears on every log line produced while handling that request.

## Architecture

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
   │       PostgreSQL          │   │          Redis          │
   │  ═══ SOURCE OF TRUTH ═══  │   │  ═══ DISPATCH HINT ═══  │
   └────▲──────────────▲───────┘   └───────────┬─────────────┘
        │ ④ conditional│ ⑤ fallback            │ ③ BZPOPMIN
        │    UPDATE    │  SKIP LOCKED scan     │
   ┌────┴──────────────┴───────────────────────┴─────────────┐
   │        WORKER × N processes, × M slots each              │
   │        + reaper and promoter, on an interval             │
   └──────────────────────────────────────────────────────────┘
```

Arrow ④ is the only arrow that decides anything. Everything reaching a worker through Redis is a hint that may be stale, duplicated or missing.

**PostgreSQL is the single source of truth; Redis is a dispatch hint.** Redis holds the set of ready jobs and is the fast path workers pull from, but it decides nothing: every state transition is a conditional `UPDATE` against Postgres, and a worker owns a job only if that update reports a changed row. The consequence that matters is that **the system stays correct with Redis stopped** — losing it raises latency, never correctness, and it self-heals when it returns.

Keeping the queue and the state in one system collapses five separate problems into one mechanism: exactly-once claiming, priority ordering, scheduled execution, cancellation and idempotency all become predicates over the same row, rather than coordination protocols across two stores.

### Layering

```
HTTP → app/api/ → app/services/ → app/db/ → PostgreSQL
                        │
                        └── app/jobs/   (pure: no DB, no HTTP)
```

Each layer may only call the one below it. `api/` translates HTTP to commands and domain errors to status codes — no SQL, no business rules. `services/` holds the use cases and the transaction boundary — no HTTP types, no `HTTPException`. `db/repository.py` is the only module in the codebase that constructs SQL. `jobs/` knows about payload shapes and how to execute work, and nothing else.

### Two hierarchies, deliberately separate

Every job type inherits from an abstract `BaseJob`, but the `jobs` table is flat — one row shape for every type, no ORM polymorphism. The claim query has to find the highest-priority eligible job across all types from a single index; joined-table inheritance would put a JOIN in that hot path, and the per-type difference is entirely the shape of the JSONB `payload` anyway.

The table describes *a job*. The class describes *what the job does*.

### How a job is claimed, and why only one worker gets it

```sql
UPDATE jobs SET status='processing', worker_id=:worker,
                lease_until=now() + :lease, attempts=attempts+1
WHERE id = (SELECT id FROM jobs
            WHERE status='pending' AND (scheduled_at IS NULL OR scheduled_at <= now())
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED LIMIT 1)
RETURNING *;
```

One statement, so there is no window between choosing a row and taking it. `SKIP LOCKED` makes a worker step over a row another worker has locked instead of queueing behind it, so N workers claim N distinct jobs with no contention. **The guarantee is a property of the engine, not of the order our code happens to run in.**

Priority, scheduling and exactly-once pickup are all this one query — three requirements the assignment lists separately, served by one mechanism.

Every write after the claim repeats `worker_id` *and* `attempts` in its `WHERE` clause. `attempts` increments on every claim, so it is a fencing token: a worker displaced by the reaper finds its own completion write matches zero rows, and discards its result rather than overwriting the work of whoever took over. `worker_id` alone would not be enough — two slots in one process would share it.

## Job types

| Type | Payload | Result |
|---|---|---|
| `email` | `to`, `subject`, `body`, optional `cc` | `message_id` |
| `webhook` | `url`, optional `method`, `headers`, `body` | `status_code`, `response_ms` |
| `report` | `report_type`, `date_from`, `date_to`, `format` | `file_url`, `row_count` |
| `batch` | `items` (1–1000), `operation` | `total`, `succeeded`, `failed`, `errors` |

Each type declares its own schema, defaults and behaviour in one module under [`app/jobs/`](app/jobs/). Adding a type is a new file plus a `@register` decorator — no change to the API, service or persistence layers. There is a test that proves exactly that.

## Operating it

### Diagnosing a queue

```json
{
  "database": "ok",
  "redis": "ok",
  "queue": {"pending": 17, "processing": 4, "completed": 340, "failed": 3,
            "oldest_pending_seconds": 12, "ready_hints": 17},
  "workers": {"count": 4, "ids": ["hostA-7-0", "hostA-7-1", "hostB-9-0", "hostB-9-1"]}
}
```

Depth alone cannot separate load from failure; **depth with age can**:

| Depth | Oldest pending | Reading |
|---|---|---|
| high | low | busy, keeping up |
| high | high | not keeping up — too few workers, or too slow |
| low | high | stuck — a job nothing will claim, or no workers at all |

`workers` is `null` rather than `count: 0` when Redis cannot be reached. "I cannot see the workers" and "there are no workers" are different incidents, and an operator told the second when the first is true will restart healthy workers during a cache outage.

`queue.dead_lettered` should be zero. It counts failures that no retry can help.

### Why is *this* job in the state it is in

```bash
curl -sS 'http://localhost:8000/jobs/<id>/logs'
```

One row per transition, oldest first, written by whichever component made the transition — the API, a worker, or the reaper. The job row carries only the *latest* error, so this is where a job that failed twice and then succeeded, or one that was reaped three times, explains itself:

```
info     | Job created with status pending
info     | Claimed by hostA-7-1
warning  | Lease expired; returned to the queue
info     | Claimed by hostA-7-0
info     | Job completed
```

Payload contents never appear here — submission records a size and a digest instead, and a test pins it.

### Failed, versus cannot run

```bash
curl -sS 'http://localhost:8000/jobs?dead_lettered=true'
```

A job that exhausts its attempts on ordinary handler exceptions is `failed`, and `POST /jobs/{id}/retry` will put it back in the queue with its attempts reset — a webhook that got three 500s did its work, the service it called was down, and retrying once that recovers is the right move.

A job is **dead-lettered** only when retrying cannot help: its stored payload no longer parses, it exceeded its time budget on every attempt, or every attempt ended with the worker dying rather than reporting anything. Those are refused with `409`, which is the point — an operator draining an incident by retrying everything that failed must not re-arm a job that takes a worker down with it.

| `dead_letter_reason` | Meaning |
|---|---|
| `unprocessable_payload` | the payload no longer matches its schema — fix the payload and submit again |
| `timeout_loop` | never finished inside its budget — the work is too big, or the budget too small |
| `worker_crash_loop` | killed every worker that picked it up |

### Watching it survive things

```bash
docker compose stop redis
```

Jobs keep completing. `/health` reports `"redis": "error"` and `workers: null`, and still returns `200` — the database is a hard dependency and Redis is not, so a degraded cache must not pull a working service out of a load balancer. Start Redis again and it recovers with no intervention.

```bash
docker compose kill -s SIGKILL worker
```

Whatever those workers were holding stays in `processing` for one lease duration — that is the window in which nothing can tell a dead worker from a slow one — and is then returned to the queue by the reaper and finished by someone else. Watch it with:

```bash
watch -n2 "curl -sS localhost:8000/health | jq .queue"
```

```bash
docker compose up -d --scale worker=4
```

Concurrency has two dimensions: `WORKER_CONCURRENCY` slots inside each process, and however many processes are run.

## Running the tests

```bash
docker compose up -d db-test redis
pip install -e ".[dev]"
pytest --cov=app --cov-report=term-missing
```

488 tests, 100 % coverage, in about 70 seconds. The first run after starting `db-test` is slower — a cold PostgreSQL and the migration to head.

```bash
pytest tests/unit -q                          # no I/O at all, runs in under a second
pytest tests/integration -q                   # real PostgreSQL, no HTTP
pytest tests/e2e -q                           # full ASGI stack plus a worker
pytest tests/load -q                          # a backlog drained by four slots
```

```bash
pytest tests/unit/test_handlers.py::test_l1_34_batch_progress_is_monotonic_and_ends_at_100 -q
```

Static checks:

```bash
ruff check . && ruff format --check . && mypy app
```

Four things about the suite are deliberate:

- **Job logic is tested with no infrastructure.** Handlers receive a `JobContext` protocol — no session, no repository — so unit tests pass a fake and assert on what the handler did. This is how worker logic is verified independently of the API.
- **Tests run against real PostgreSQL, never SQLite.** SQLite has no `JSONB`, different partial-index semantics, and no `FOR UPDATE SKIP LOCKED`. A suite passing against it would assert nothing about the mechanisms this service depends on. `db-test` exists for that, on port 5433.
- **Nothing waits on the wall clock.** Time, sleeping and randomness are injected. A test asserts that an email job sleeps between one and three seconds without the suite spending three seconds proving it, and the webhook's 20 % failure rate can be forced either way.
- **The concurrency tests use real connections.** They cannot share the rolled-back session the rest of the suite uses, because one transaction would serialise exactly the concurrency they exist to exercise. Ten workers race for one job; ten cancels race ten claims; four slots drain two hundred, with the handler recording every execution so "exactly once" is asserted against what actually ran.

Redis is optional for the suite — most worker tests run against the no-Redis fallback, which is the path that guarantees correctness. `tests/integration/test_redis_dispatch.py` and `tests/load/test_redis_throughput.py` need a server and skip without one. The second is worth running deliberately (`pytest tests/load/test_redis_throughput.py`): it is the only test that puts the *hint* path under concurrent load, and it asserts on the `from_hint` flag recorded in `job_logs` rather than on the system still working — because the system works identically when the dispatch is broken, which is how a worker wired to the wrong dispatch once passed the entire suite.

The scenario matrix, with an ID per case, is in [`specs/TEST_PLAN.md`](specs/TEST_PLAN.md); those IDs are the test names in the suite. [`DEMO.md`](DEMO.md) is the manual counterpart, run against a live stack.

## Project layout

```
app/
  core/        config, clock, structured logging, enums, domain errors
  jobs/        BaseJob, JobContext, registry, and one module per job type
  db/          models, repository (all SQL), session management
  dispatch/    the Redis hint, and the null implementation it degrades to
  services/    use cases — JobService (API), ExecutionService (worker)
  api/         routes, schemas, middleware, error handlers
  worker/      slots, context, maintenance, lifecycle, entry point
  migrations/  Alembic, hand-written
tests/
  unit/        no I/O
  integration/ real PostgreSQL, no HTTP
  e2e/         full ASGI stack plus a worker
  load/        a backlog drained by concurrent slots
specs/         the specifications the code was written against
```

## Configuration

Read from the environment; Docker Compose sets what it needs. See [`.env.example`](.env.example).

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://jobs:jobs@localhost:5432/jobs` | Primary database |
| `TEST_DATABASE_URL` | `…@localhost:5433/jobs_test` | Used by the test suite |
| `REDIS_URL` | `redis://localhost:6379/0` | Dispatch hint and worker liveness |
| `TEST_REDIS_URL` | `redis://localhost:6379/15` | A separate logical database for the suite |
| `LOG_LEVEL` | `INFO` | |
| `WORKER_CONCURRENCY` | `2` | Slots per worker process |
| `WORKER_LEASE_SECONDS` | `60` | How long a claim holds a job before the reaper may take it |
| `WORKER_HEARTBEAT_SECONDS` | `20` | Lease extension interval — a third of the lease, so one missed extension is survivable |
| `WORKER_POLL_INTERVAL_SECONDS` | `5` | How long an idle slot blocks on Redis before falling back to a scan |
| `MAINTENANCE_INTERVAL_SECONDS` | `5` | Reaper and promoter sweep interval — the upper bound on recovery latency after a lease expires |
| `MAINTENANCE_BATCH_SIZE` | `100` | Rows one sweep may touch, so a large backlog cannot become one long transaction |
| `SHUTDOWN_GRACE_SECONDS` | `30` | Time in-flight jobs get to finish before their leases are force-released |
| `MAX_REQUEST_BODY_BYTES` | `65536` | Bodies above this are rejected before parsing |
| `MAX_SCHEDULE_HORIZON_DAYS` | `90` | Furthest a job may be scheduled ahead |
| `MAX_PAGE_SIZE` | `100` | Upper bound on `limit` |

The container's `stop_grace_period` must exceed `SHUTDOWN_GRACE_SECONDS`, or Docker sends `SIGKILL` part-way through the wind-down and the in-flight leases are never released. `docker-compose.yml` sets 40 s against 30 s.

Migrations run automatically in the container. Manually:

```bash
DATABASE_URL=postgresql+asyncpg://jobs:jobs@localhost:5432/jobs alembic upgrade head
```

## Design documents

The specifications were written and reviewed before the code, and the code was written against them.

| Document | Contents |
|---|---|
| [`specs/00-architecture.md`](specs/00-architecture.md) | Components, data flows, system invariants, rejected alternatives |
| [`specs/01-data-model.md`](specs/01-data-model.md) | Schema, constraints, indexes, state machine, idempotency |
| [`specs/02-api-core.md`](specs/02-api-core.md) | HTTP contract, job class hierarchy, validation, security model |
| [`specs/03-worker-runtime.md`](specs/03-worker-runtime.md) | The worker process, slots, execution, `DbJobContext` |
| [`specs/04-claiming.md`](specs/04-claiming.md) | Exactly-once pickup, priority, scheduling — one query |
| [`specs/05-retry-and-failure.md`](specs/05-retry-and-failure.md) | Backoff, jitter, what is and is not retried |
| [`specs/06-crash-recovery.md`](specs/06-crash-recovery.md) | Leases, heartbeats, the reaper, graceful shutdown |
| [`specs/07-redis-dispatch.md`](specs/07-redis-dispatch.md) | Score encoding, announcement ordering, degradation |
| [`specs/08-worker-observability.md`](specs/08-worker-observability.md) | Health fields, log events, diagnosing a stuck queue |
| [`specs/09-hardening.md`](specs/09-hardening.md) | Manual retry, job timeout, dead-letter classification |
| [`specs/10-job-history.md`](specs/10-job-history.md) | Reading a job's audit trail, and what is deliberately not exposed |
| [`specs/TEST_PLAN.md`](specs/TEST_PLAN.md) | Test levels, determinism rules, numbered scenario matrix |
| [`DECISIONS.md`](DECISIONS.md) | Each design decision, its reasoning, and what it cost |
| [`AI_USAGE.md`](AI_USAGE.md) | How AI tools were used, and where their output was wrong |
| [`DEMO.md`](DEMO.md) | A manual walkthrough against the running system |

## Known limitations

Stated deliberately; the reasoning for each is in [`DECISIONS.md`](DECISIONS.md) §5.

- Delivery is **at-least-once**, not exactly-once. A worker stalled past its lease can have its job reclaimed and rerun; its writes are rejected, but side effects it already performed have happened. Handlers must be idempotent.
- **No aging**, so a sustained stream of high-priority work can starve low-priority jobs.
- **No authentication.** Anyone who can reach the API can read any job whose id they know. Random UUIDs are obscurity, not authorization.
- **SSRF protection is incomplete by construction.** A hostname that resolves publicly at submission can resolve inward by execution time; closing that gap requires pinning the resolved address in the HTTP client at request time.
- **No retention policy**, so terminal jobs — and their idempotency keys — accumulate without bound.
