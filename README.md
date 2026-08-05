# Job Queue Service

A distributed background job processing system: an HTTP API for submitting and inspecting jobs, a PostgreSQL-backed job store, and worker processes that claim and execute them.

Submit a job over HTTP and a worker picks it up, runs it, and records the result — correctly with several workers running at once, when a handler fails, and when a worker is killed mid-job.

A job that fails is retried with backoff; a job that *cannot run* is dead-lettered and will not be handed back out.

---

## Running the project

```bash
docker compose up --build
```

That brings up PostgreSQL, Redis, the API, and **two worker processes running two slots each** — four jobs can be in flight at once. Migrations are applied automatically on start, and the service listens on <http://localhost:8000>. Interactive documentation is at <http://localhost:8000/docs> — the fastest way to submit a job without leaving the browser.

Nothing needs to be installed locally to run the service.

```bash
docker compose up -d --scale worker=4
```

Concurrency has two dimensions: `WORKER_CONCURRENCY` slots inside each process, and however many processes are run.

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

List and cancel:

```bash
curl -sS 'http://localhost:8000/jobs?status=pending&limit=20'
```

```bash
curl -sS -X POST http://localhost:8000/jobs/<id>/cancel
```

A few behaviours worth trying, because they are the ones the design turns on:

```bash
# Repeat the first request verbatim: 200, not 201, and the same id. Nothing was created.
```

```bash
# Cancel the same job twice: the second returns 409 and the job is unchanged.
```

```bash
# A webhook aimed at the cloud metadata endpoint is refused with 422.
curl -sS -X POST http://localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"job_type":"webhook","payload":{"url":"http://169.254.169.254/latest/meta-data/"}}'
```

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

## API

| Method | Path | Behaviour |
|---|---|---|
| `POST` | `/jobs` | Submit. `201` on create; **`200`** when an `idempotency_key` matched an existing job |
| `GET` | `/jobs/{id}` | Fetch one job. `404` if unknown |
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

### Job types

| Type | Payload | Result |
|---|---|---|
| `email` | `to`, `subject`, `body`, optional `cc` | `message_id` |
| `webhook` | `url`, optional `method`, `headers`, `body` | `status_code`, `response_ms` |
| `report` | `report_type`, `date_from`, `date_to`, `format` | `file_url`, `row_count` |
| `batch` | `items` (1–1000), `operation` | `total`, `succeeded`, `failed`, `errors` |

Each type declares its own schema, defaults and behaviour in one module under [`app/jobs/`](app/jobs/). Adding a type is a new file plus a `@register` decorator — no change to the API, service or persistence layers. There is a test that proves exactly that.

## Running the tests

```bash
docker compose up -d db-test redis
pip install -e ".[dev]"
pytest --cov=app --cov-report=term-missing
```

464 tests, 100 % coverage, in about two minutes.

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
- **The concurrency tests use real connections.** They cannot share the rolled-back session the rest of the suite uses, because one transaction would serialise exactly the concurrency they exist to exercise. Ten workers race for one job; four slots drain two hundred, with the handler recording every execution so "exactly once" is asserted against what actually ran.

Redis is optional for the suite — the worker tests run against the no-Redis fallback, which is the path that guarantees correctness. Only `tests/integration/test_redis_dispatch.py` needs a server, and it skips without one.

The scenario matrix, with an ID per case, is in [`specs/TEST_PLAN.md`](specs/TEST_PLAN.md); those IDs are the test names in the suite.

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
| `LOG_LEVEL` | `INFO` | |
| `WORKER_CONCURRENCY` | `2` | Slots per worker process |
| `WORKER_LEASE_SECONDS` | `60` | How long a claim holds a job before the reaper may take it |
| `WORKER_HEARTBEAT_SECONDS` | `20` | Lease extension interval — a third of the lease, so one missed extension is survivable |
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
| [`specs/TEST_PLAN.md`](specs/TEST_PLAN.md) | Test levels, determinism rules, numbered scenario matrix |
| [`DECISIONS.md`](DECISIONS.md) | Each design decision, its reasoning, and what it cost |
| [`AI_USAGE.md`](AI_USAGE.md) | How AI tools were used, and where their output was wrong |

## Known limitations

Stated deliberately; the reasoning for each is in [`DECISIONS.md`](DECISIONS.md) §5.

- Delivery is **at-least-once**, not exactly-once. A worker stalled past its lease can have its job reclaimed and rerun; its writes are rejected, but side effects it already performed have happened. Handlers must be idempotent.
- **No aging**, so a sustained stream of high-priority work can starve low-priority jobs.
- **No authentication.** Anyone who can reach the API can read any job whose id they know. Random UUIDs are obscurity, not authorization.
- **SSRF protection is incomplete by construction.** A hostname that resolves publicly at submission can resolve inward by execution time; closing that gap requires pinning the resolved address in the HTTP client at request time.
- **No retention policy**, so terminal jobs — and their idempotency keys — accumulate without bound.
