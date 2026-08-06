# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Feature-complete against the assignment. Data model, job class hierarchy, the API, the worker (claiming, retry with backoff, lease/heartbeat/reaper, graceful shutdown, Redis dispatch, worker liveness), manual retry, job timeout enforcement, dead-letter routing and job history. 493 tests, 100 % coverage.

Deliberately not built, and recorded as such in `DECISIONS.md` §5: priority aging, job retention, backpressure and rate limiting, authentication.

## What this repository is

A distributed background **job queue service** in Python, built as a take-home assignment for a Mid/Senior Backend role and submitted as a Git repository.

Four components: **API service** (submit/query) → **queue** (pending jobs) → **worker process(es)** (pull and execute) → **relational DB** (state and results). The worker must be a separate process; no in-request processing.

## Working method: spec before code (SDD)

This is not optional here. The specs are part of the deliverable, and a spec that no longer describes the code is a defect in the same way a failing test is.

For every feature: write `specs/<nn>-<feature>.md` first, get it approved, then implement strictly to it. A spec covers the requirement, the design decision and its alternatives, edge cases, and acceptance criteria. If the implementation diverges, update the spec in the same change — a spec that no longer describes the code is worse than no spec.

Specs feed `DECISIONS.md` directly; write once, don't duplicate prose between them.

## Constraints that are not negotiable

- Python 3.11+; relational DB for persistence; a queue/cache technology for the queue.
- `docker-compose up` must bring up both the API and the worker.
- Retry with exponential backoff, 3 attempts (immediate → ~30s → ~2min → permanent `FAILED`).
- Priority ordering, job cancellation, idempotency keys retained ≥24h.
- Six required test scenarios: submission+retrieval, completion flow, failure+retry, cancellation, idempotency, priority ordering. Worker logic must be tested independently of the API.

## Job state machine

`SCHEDULED` → (time arrives) → `PENDING` → `PROCESSING` → `COMPLETED` | `FAILED`
`PENDING` → `CANCELLED`; `FAILED` → (manual retry) → `PENDING`

Every state transition emits a structured JSON log line carrying job context.

## The hard parts, and how they are judged

Concurrency correctness is the core of the evaluation, not a detail:

- **Exactly-once pickup.** With multiple workers running, a job must be claimed by exactly one. Whatever mechanism is chosen (atomic dequeue, `SELECT ... FOR UPDATE SKIP LOCKED`, lease-based claiming), it must hold regardless of which concurrent path returns first — assume an interleaving is deliberately adversarial and test it under real parallel workers, not with mocks that serialize.
- **Crash recovery.** A worker dying mid-job must not leave the job in `PROCESSING` forever. Requires a lease/heartbeat with expiry plus a reaper, and the reaper must not resurrect a job a live worker still holds.
- **No polling hot-loops.** Dequeue efficiency under a large backlog is explicitly graded; busy-waiting is a finding, not a style preference.
- **Poison messages.** Malformed payloads must not crash a worker repeatedly — validate payloads at the boundary and route repeat offenders to a dead-letter path.

## Required deliverables

`README.md` (how to run, how to test, example job submission, architecture overview) · `DECISIONS.md` (job pickup strategy, crash recovery incl. the actual mid-job failure flow, priority queue implementation, backoff timings, and one honest "what I'd do differently") · `AI_USAGE.md` (tools used, where AI helped, **where AI was wrong — especially on concurrency**, where it didn't help) · `docker-compose.yml` · `Dockerfile`.

Both markdown files are graded as evidence of iterative, traceable development. Honesty scores better than polish.

## Code standards specific to this repo

Every line must be explainable in a live follow-up interview. Prefer the smallest solution that satisfies the spec: no speculative abstractions, no unused config knobs, no defensive layers guarding conditions that cannot occur. Dead or unexplainable code is a scoring liability, so delete rather than keep "just in case".

Test coverage is measured. Cover failure paths — lease expiry and reclaim, retry exhaustion, cancellation racing pickup, duplicate idempotency keys — not just the happy path.

## Commands

Run everything:

```bash
docker compose up --build
```

The test suite needs the `db-test` container (real PostgreSQL — see below). Redis is optional: only `tests/integration/test_redis_dispatch.py` needs it, and it skips without one.

```bash
docker compose up -d db-test redis && pytest --cov=app --cov-report=term-missing
```

A worker on its own:

```bash
python -m app.worker
```

A single test, or one level:

```bash
pytest tests/unit/test_handlers.py::test_l1_34_batch_progress_is_monotonic_and_ends_at_100 -q
```

```bash
pytest tests/unit -q
```

Lint, format and types — all three must be clean:

```bash
ruff check . && ruff format --check . && mypy app
```

Migrations run automatically in the `api` container. Manually:

```bash
DATABASE_URL=postgresql+asyncpg://jobs:jobs@localhost:5432/jobs alembic upgrade head
```

## Architecture notes

Layering, enforced by review: **`api/` → `services/` → `db/` → PostgreSQL**, with `jobs/` hanging off the service as a pure, dependency-free branch. Each layer may only call the one below it. `api/` holds no SQL and no business rules; `services/` holds no HTTP types and raises domain errors instead of `HTTPException`; `db/repository.py` is the only module in the codebase that constructs SQL.

**The worker obeys the same rule: `worker/` → `services/ExecutionService` → `db/`.** It never touches the repository. This is not tidiness — without it, the retry-versus-give-up decision, the backoff calculation, the shape of a persisted error and the transition logging all end up inside the slot loop, and none of them can be tested without starting a worker. `slot.py` is orchestration only: claim, run, hand the outcome over.

**`attempts` is the fencing token.** Every write after a claim carries `WHERE id=… AND worker_id=… AND attempts=… AND status='processing'`. `worker_id` alone is insufficient because slots inside one process would share it: slot A stalls, the reaper releases, slot B in the same process reclaims with the same `worker_id`, and A's late write matches and overwrites a legitimate result. `attempts` increments on every claim, so a value read at claim time can never describe a later claim. A write matching zero rows is normal — it means ownership was lost, and the caller discards its result.

**One clock, and it is the database's.** `lease_until`, retry deadlines, `started_at` and `completed_at` are all computed in SQL as `now() + interval`. These values are *compared* by the database (`scheduled_at <= now()`, `lease_until < now()`), so a value produced by an application clock would be wrong by exactly the skew between the machines, silently. The injected `Clock` is only for values the database never compares.

**Do not add `announce` calls to the maintenance sweeps.** Reaped and promoted jobs reach a worker through the fallback claim within one poll interval. Announcing them would mean carrying priority and creation time out of a batch `UPDATE ... RETURNING` purely to rebuild a Redis score, for a few seconds on jobs that are already late.

**Returning a job to the queue is always two statements.** Both the reaper and the shutdown release must split on `attempts < max_attempts`: a job requeued with its attempts spent makes the next claim compute `attempts + 1` past the limit and violate `ck_jobs_attempts` — and since the claim picks by priority and age, that row is then selected first by every worker, so one bad row stops the whole fleet. The exhausted job is failed instead, and **not** dead-lettered: a deploy landing on it is not the job's fault, so it stays retryable. If a third path to `pending` is ever added, it needs the same split.

**Redis never raises.** Every `Dispatch` method logs and degrades. Callers have a working fallback, so error handling at the call sites would be dead code. The corollary is that a broken dispatch is invisible unless something asserts on it — which is how a socket read timeout equal to the `BZPOPMIN` block went unnoticed: every idle poll raised, logged "Redis failed", and degraded to the fallback that makes it all still work. `from_url` derives the read budget from the longest block its caller will ask for; do not let it inherit a library default.

**Degrading is not the same as returning early.** `next_hint` is the only pacing the slot loop has — `run_forever` has no sleep in its idle path, which is why `NullDispatch.next_hint` sleeps for the timeout it is handed. A refused connection fails in a round trip rather than in the interval the caller asked to wait, so `RedisDispatch.next_hint` must wait out the remainder of the block before answering `None`. Without it a Redis outage becomes a hot loop of claim queries against the one database the outage did not take away: measured on the running stack at 49 idle cycles in twenty seconds where the poll interval intends 8. Any future method the slot loop awaits for pacing carries the same obligation.

**A slot loop must survive its own errors.** Nothing awaits a slot task until shutdown, so an escaping exception retires that slot permanently while the process stays up and keeps announcing itself as live. `run_forever` reports, waits one poll interval, and continues; anything already claimed is recovered by the reaper.

**Dead-letter is a classification, not a status.** `dead_letter_reason` is NULL unless a failure means the job *cannot run* — an unparseable payload, a timeout on every attempt, a worker killed every time. A job that exhausted its attempts on ordinary handler exceptions gets NULL and stays retryable, because it did its work and the thing it called was down. Do not add a seventh status for this; the enum, the CHECK constraint, the transition table and every status filter would all have to change to express what `failed` already says.

**Two hierarchies, deliberately separate.** The `jobs` table is flat — one row shape for every job type, no ORM polymorphism — because the claim query in spec 04 must scan all types from one index. Type-specific behaviour lives in the `BaseJob` subclasses, and the per-type difference is entirely the shape of the JSONB `payload`. The table describes *a job*; the class describes *what the job does*.

**`JobContext` is the seam that makes the job logic testable.** Handlers receive a context protocol exposing only `report_progress`, `log` and `heartbeat` — no session, no repository. A handler physically cannot reach the database, so unit tests pass a fake and need no infrastructure. Sleeping and randomness are injected for the same reason: `tests/doubles.py` records the requested sleep durations instead of waiting, so a test can assert an email job sleeps 1–3 s without taking 3 s.

**Columns that look unused are not.** `worker_id`, `lease_until` and the `ix_jobs_claim` / `ix_jobs_lease` indexes exist for the worker in part 2. They were created in the first migration on purpose — cheaper than altering a table that later code already depends on.

**Tests run against real PostgreSQL, never SQLite.** SQLite has no JSONB, different partial-index semantics, and no `FOR UPDATE SKIP LOCKED`; a suite passing against it would assert nothing about the mechanisms being graded. `db-test` (port 5433, tmpfs) exists for this. Most tests share one transaction that is rolled back on teardown — but the concurrency tests must not, and use `committing_sessions` instead, because sharing a transaction would serialise the very concurrency they exist to exercise. The load tests use `pooled_sessions`: with `NullPool` every one of the thousands of short units of work a worker opens is a fresh connection, and the test ends up measuring the fixture rather than the queue.

**`NullDispatch` is not only a test double.** It is what the API and worker fall back to when Redis is unreachable, and the whole worker suite runs against it — which is how the PostgreSQL fallback claim, the path that actually guarantees correctness, stays exercised. The corollary bit once: a worker wired with `NullDispatch` by mistake passes every test, because that is precisely what it is designed to do. `tests/integration/test_worker_runtime.py` asserts the entry point builds a real dispatch.

**Coverage needs `concurrency = ["thread", "greenlet"]`** (already set in `pyproject.toml`). SQLAlchemy's async layer runs inside greenlets; without it, every line after an `await session.execute(...)` is falsely reported as uncovered.
