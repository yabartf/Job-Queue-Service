# Spec 01 — Data Model

**Status:** Accepted — implemented
**Depends on:** `specs/00-architecture.md`
**Related decisions:** DECISIONS.md §1

## 1. Scope

The persistent schema: tables, columns, constraints, indexes, and the job state machine they enforce. Covers columns used only by later specs (leases, attempts) so the schema is created once rather than migrated repeatedly.

Out of scope: how jobs are claimed (spec 04), retried (spec 05), or recovered (spec 06). This spec defines the state those mechanisms will move through, not the mechanisms.

## 2. Shape decision: one table, no ORM inheritance

All four job types live in a single `jobs` table. There is no joined-table or single-table SQLAlchemy polymorphism.

**Why:** the claim query (spec 04) must find the highest-priority eligible job across *all* types from one index. Joined-table inheritance would put a JOIN in that hot path; single-table inheritance adds a discriminator and mapper overhead while every type shares an identical column set. The difference between job types is entirely the shape of `payload`, and `payload` is JSONB.

Type-specific behaviour lives in the `BaseJob` class hierarchy (spec 02 §4), not in the schema. The table describes *a job*; the class describes *what the job does*.

## 3. State machine

```
                    ┌─────────────┐
         ┌──────────│  SCHEDULED  │
         │          └──────┬──────┘
         │                 │ scheduled_at reached
         ▼                 ▼
    ┌─────────┐       ┌──────────────┐
    │ PENDING │──────▶│  PROCESSING  │
    └────┬────┘       └──────┬───────┘
         │                   │
    cancel                ┌──┴────────────┬──────────────┐
         │                ▼               ▼              ▼
         ▼          ┌──────────┐   ┌──────────┐   ┌──────────┐
   ┌───────────┐    │COMPLETED │   │  FAILED  │   │ PENDING  │
   │ CANCELLED │    └──────────┘   └────┬─────┘   │ (retry / │
   └───────────┘                        │         │  reaped) │
                                  manual retry    └──────────┘
                                        │
                                        ▼
                                    ┌─────────┐
                                    │ PENDING │
                                    └─────────┘
```

### Allowed transitions

| From | To | Trigger | Spec |
|---|---|---|---|
| — | `pending` | submit, no future `scheduled_at` | 02 |
| — | `scheduled` | submit with future `scheduled_at` | 02 |
| `scheduled` | `pending` | scheduled time reached | 04 |
| `scheduled` | `cancelled` | cancel | 02 |
| `pending` | `processing` | worker claim | 04 |
| `pending` | `cancelled` | cancel | 02 |
| `processing` | `completed` | handler returned a result | 03 |
| `processing` | `pending` | attempt failed, retries remain | 05 |
| `processing` | `failed` | attempts exhausted | 05 |
| `processing` | `pending` | lease expired, reaper released | 06 |
| `failed` | `pending` | manual retry | 05 |

`completed` and `cancelled` are terminal. **`processing` → `cancelled` is not permitted** — the assignment scopes cancellation to pending and scheduled jobs, and cancelling work already in flight would require cooperative interruption of a running handler.

Every transition is performed by a conditional `UPDATE` naming the expected source status (architecture invariant 1). No transition is ever performed as read-then-write.

### Attempt counting

`attempts` is incremented **at claim time**, not at failure. A job that has never been claimed has `attempts = 0`; a job currently on its first execution has `attempts = 1`. With `max_attempts = 3` a job executes at most three times.

Incrementing at claim rather than at failure means a worker that dies without writing anything still consumes an attempt. That is deliberate: a job that reliably kills its worker must not be retried forever (queue-poisoning defence, part 3).

**Manual retry resets `attempts` to 0.** Without the reset, retrying a job that already exhausted its attempts would fail again immediately. The reset is recorded in `job_logs`.

## 4. Table: `jobs`

```sql
CREATE TABLE jobs (
    id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type         TEXT         NOT NULL,
    payload          JSONB        NOT NULL,
    status           TEXT         NOT NULL,
    priority         SMALLINT     NOT NULL DEFAULT 5,
    attempts         SMALLINT     NOT NULL DEFAULT 0,
    max_attempts     SMALLINT     NOT NULL DEFAULT 3,
    progress         SMALLINT     NOT NULL DEFAULT 0,
    result           JSONB,
    error            JSONB,
    idempotency_key  TEXT,
    scheduled_at     TIMESTAMPTZ,
    worker_id        TEXT,
    lease_until      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

### Column notes

**`id` — UUID, not a sequence.** Job ids are returned to clients and appear in URLs; a monotonic integer would leak submission volume and allow trivial enumeration of other clients' jobs. Cost: random UUIDs fragment the primary-key B-tree more than sequential keys. Accepted at this scale; UUIDv7 would recover locality if it mattered.

**`payload` — JSONB, not TEXT.** JSONB is parsed and validated as JSON by the driver, so nothing type-specific has to be string-concatenated anywhere. It also allows indexing into payload fields later without a migration.

**`priority` — SMALLINT bounded to 0–9.** Two separate reasons, both real:
1. An unbounded integer lets any client submit `priority = 2^31-1` and jump the entire queue permanently. Bounding the range makes priority a coarse class, not a weapon.
2. Spec 07 encodes priority and timestamp into a single Redis sorted-set score, and Redis scores are float64. A bounded priority scale is what keeps that encoding inside the 2^53 exact-integer range; an unbounded one would break FIFO ordering silently.

Higher value = more urgent, matching the assignment. Default 5 leaves room in both directions.

**`error` — JSONB `{type, message, attempt, at}`.** Structured, so failures can be filtered and aggregated. **Never contains a traceback or driver text** (spec 02 §6). Holds the most recent failure only; the full history is in `job_logs`.

**`progress` — SMALLINT 0–100.** Meaningful for batch jobs; left at 0 for the others rather than nullable, so consumers never branch on NULL.

**`worker_id`, `lease_until` — created now, used in specs 04 and 06.** Adding them up front costs one column definition; adding them later costs a migration against a table other code already depends on.

**Timestamps are all `TIMESTAMPTZ`.** `TIMESTAMP` without a zone in a system that schedules future work is a defect waiting to happen. All application code works in UTC; the API requires timezone-aware input (spec 02 §5).

**Enums as `TEXT` + `CHECK`, not native PostgreSQL `ENUM`.** Altering a native enum type inside a migration is awkward and partially non-transactional. A `CHECK` against a text column gives the same guarantee and edits like any other constraint. The authoritative list lives in `app/core/enums.py` as a `StrEnum`; the constraint mirrors it.

### Constraints

```sql
ALTER TABLE jobs
  ADD CONSTRAINT ck_jobs_status CHECK (
      status IN ('scheduled','pending','processing','completed','failed','cancelled')),
  ADD CONSTRAINT ck_jobs_type CHECK (
      job_type IN ('email','webhook','report','batch')),
  ADD CONSTRAINT ck_jobs_priority     CHECK (priority BETWEEN 0 AND 9),
  ADD CONSTRAINT ck_jobs_max_attempts CHECK (max_attempts BETWEEN 1 AND 10),
  ADD CONSTRAINT ck_jobs_attempts     CHECK (attempts >= 0 AND attempts <= max_attempts),
  ADD CONSTRAINT ck_jobs_progress     CHECK (progress BETWEEN 0 AND 100),
  ADD CONSTRAINT ck_jobs_idem_len     CHECK (idempotency_key IS NULL
                                             OR char_length(idempotency_key) <= 255),

  -- state invariants
  ADD CONSTRAINT ck_jobs_result_when_completed CHECK (
      result IS NULL OR status = 'completed'),
  ADD CONSTRAINT ck_jobs_scheduled_has_time CHECK (
      status <> 'scheduled' OR scheduled_at IS NOT NULL),
  ADD CONSTRAINT ck_jobs_processing_has_lease CHECK (
      status <> 'processing' OR (worker_id IS NOT NULL AND lease_until IS NOT NULL)),
  ADD CONSTRAINT ck_jobs_completed_after_started CHECK (
      completed_at IS NULL OR started_at IS NOT NULL);
```

These push correctness into the database rather than trusting the application. A bug in claim, retry or recovery logic cannot produce a row that is internally impossible — it raises instead, loudly, at the moment of the bad write. `ck_jobs_processing_has_lease` in particular makes it impossible to mark a job `processing` without also recording who holds it and until when, which is exactly the failure that leaves jobs stuck forever.

`error` is deliberately *not* constrained to a status: a job that failed an attempt and is awaiting retry is `pending` and still carries its last error.

### Indexes

```sql
CREATE UNIQUE INDEX ux_jobs_idempotency ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX ix_jobs_claim ON jobs (priority DESC, created_at)
    WHERE status = 'pending';

CREATE INDEX ix_jobs_scheduled ON jobs (scheduled_at)
    WHERE status = 'scheduled';

CREATE INDEX ix_jobs_lease ON jobs (lease_until)
    WHERE status = 'processing';

CREATE INDEX ix_jobs_status_created ON jobs (status, created_at DESC);
CREATE INDEX ix_jobs_created        ON jobs (created_at DESC);
```

Every index is **partial** where it can be. `ix_jobs_claim` contains only pending rows — over the life of the system the overwhelming majority of rows are `completed`, and excluding them keeps the hot index small enough to stay cached.

**Index count is itself a trade-off.** Every index is write amplification on the claim path, which is the highest-frequency write in the system. The set above is the minimum that serves a distinct access pattern:

| Index | Serves |
|---|---|
| `ux_jobs_idempotency` | duplicate prevention (§6) |
| `ix_jobs_claim` | the claim query, spec 04 |
| `ix_jobs_scheduled` | scheduled maturation, spec 04 |
| `ix_jobs_lease` | reaper sweep, spec 06 |
| `ix_jobs_status_created` | list filtered by status |
| `ix_jobs_created` | unfiltered list, and `/health` |

**There is deliberately no index on `job_type`.** It has four distinct values; a B-tree over it is too low-selectivity for the planner to prefer over a scan, so it would cost write throughput and buy nothing. Listing filtered by type alone resolves through `ix_jobs_created` with a filter. Documented rather than silently omitted.

### `updated_at` maintenance

```sql
CREATE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_jobs_updated_at BEFORE UPDATE ON jobs
FOR EACH ROW EXECUTE FUNCTION set_updated_at();
```

A trigger rather than SQLAlchemy's `onupdate`, because the claim, completion and reaper statements in later specs are issued as direct `UPDATE`s that never pass through the ORM's mapper. An ORM-level hook would silently not fire for exactly the writes whose timing matters most.

## 5. Table: `job_logs`

```sql
CREATE TABLE job_logs (
    id         BIGSERIAL    PRIMARY KEY,
    job_id     UUID         NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    level      TEXT         NOT NULL,
    message    TEXT         NOT NULL,
    meta       JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
ALTER TABLE job_logs ADD CONSTRAINT ck_job_logs_level
    CHECK (level IN ('info','warning','error'));
CREATE INDEX ix_job_logs_job ON job_logs (job_id, created_at);
```

An append-only audit trail: one row per state transition, plus anything a handler chooses to record. This is what an operator reads when asked why a specific job is in the state it is in.

`BIGSERIAL` rather than UUID — these rows are never addressed externally, and a sequential key keeps insert locality.

⚠️ The column is `meta`, not `metadata`. `metadata` is reserved on SQLAlchemy declarative classes and collides with the mapper.

**Every state transition writes a `job_logs` row and emits a structured log line through one shared helper** (spec 02 §7), so the audit trail and the log stream cannot drift apart.

## 6. Idempotency

Enforced by `ux_jobs_idempotency`, a **partial unique index** — partial so that the many jobs submitted without a key do not collide on NULL.

Submission:

```sql
INSERT INTO jobs (...) VALUES (...)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING *;
-- zero rows returned → SELECT the existing row by key → respond 200 instead of 201
```

**Correct under concurrent submission by construction.** Two simultaneous requests with the same key: one inserts; the other blocks on the index until the first commits, then returns no row and reads the winner. There is no check-then-write window in application code, so there is no race to reason about.

**Decisions:**

- **Scope is global, not per job type.** The assignment states "same idempotency key → return the existing job" without qualification. A per-type scope would let one key produce four different jobs, which is a surprising reading.
- **Keys are retained for the life of the job row, which is unbounded — and that is a decision, not an omission.** The requirement is a floor ("at least 24 hours"), not a window, so the safe direction to miss it in is upwards. Expiring a key means a client that retries a submission after the window gets a *second job* instead of the original, silently, and a duplicate charge or a duplicate email is a worse failure than a large table. Nothing in the system expires a key, and nothing should without the design work in §8. `L2-22` pins the floor by ageing a key past 25 hours and asserting the replay still matches — so the day a retention policy takes the key with it, a test fails rather than a client double-pays.
- **Same key, different payload → return the existing job**, as instructed, and emit a `warning` log plus a `job_logs` entry. Returning 409 would arguably serve the client better by surfacing their bug, but it contradicts the stated requirement; the alternative is recorded rather than silently chosen.

## 7. Migrations

Alembic, one migration for this spec. It creates both tables, all constraints, all indexes, and the trigger. Written by hand rather than autogenerated — autogenerate does not emit partial indexes, `CHECK` constraints or triggers reliably, and a migration that silently omits `ux_jobs_idempotency` would remove the only thing enforcing duplicate prevention.

The migration is verified by a test that runs `upgrade head` then `downgrade base` against a real database.

## 8. Deferred

### Retention — known, unsolved, and needing its own spec

The partial indexes keep the hot paths fast as terminal rows accumulate, but **nothing removes them**: the `jobs` table, its `job_logs` children and every idempotency key ever submitted grow without bound. On a real deployment this eventually needs a policy, and this project does not have one. It is the largest thing knowingly left unbuilt in the data model.

It is deferred rather than added because retention is not a sweep — it is a set of product decisions this project has no basis to make (how long is a completed job interesting? is an audit trail a compliance artefact?), and getting it wrong deletes data. What follows is not a plan; it is the set of constraints any future implementation inherits, recorded now while the reasons are fresh:

1. **A 24-hour floor, enforced in configuration rather than documented.** The retention window is the idempotency window (§6), so a value below 24 hours breaks a stated requirement of the system. It must be impossible to configure, not merely discouraged.
2. **The submit path stops being safe.** `JobService.submit` reads the existing row after `ON CONFLICT DO NOTHING` returns nothing, and its `if existing is None` branch is currently unreachable — marked `pragma: no cover` — precisely because rows are never deleted. Under any expiry it becomes reachable: the insert conflicts, the sweep commits, the read finds nothing, and a valid submission fails. The fix is to retry the insert once, since a key that has just disappeared is a key the insert is now entitled to take. Whoever adds retention owns that branch and a test for it.
3. **`failed` is not terminal here.** `TERMINAL_STATUSES` deliberately excludes it, because a failed job can be manually retried (spec 09 §2). Expiring the key of a failed job leaves one key describing two jobs, one of which is still re-armable.
4. **Deleting a job deletes its history.** `job_logs` cascades. `GET /jobs/{id}/logs` is the answer to "why is this job in the state it is in" (spec 10), so a retention policy is also a decision to stop being able to answer that question after N days.

Until that spec exists, the property in §6 holds and is tested. The limitation is recorded here, in `DECISIONS.md` §5, and in the README's known limitations, so that it is found rather than discovered.

### Smaller

- Payload-field indexing (e.g. GIN on `payload`). Nothing needs it yet.
- Keyset pagination for listing. Offset pagination is specified in spec 02; the switch is mechanical if listings grow.

## 9. Acceptance criteria

- `alembic upgrade head` on an empty database produces the schema above; `downgrade base` removes it cleanly.
- Inserting a row that violates any `CHECK` raises `IntegrityError` — one test per constraint.
- Two concurrent inserts with the same `idempotency_key` result in exactly one row.
- `EXPLAIN` on the listing query with a status filter uses `ix_jobs_status_created`.
- Updating any column changes `updated_at`, including via a raw `UPDATE` that bypasses the ORM.
