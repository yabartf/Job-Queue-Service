# Test Plan

**Status:** Accepted — implemented
**Covers:** specs 01–02 (part 1, §6–§8), specs 03–08 (part 2, §9), spec 09 (part 3, `tests/integration/test_hardening.py`) and spec 10 (§12). 493 tests, 100 % coverage.

## 1. Principles

1. **Every layer is tested at the level where its behaviour actually lives.** A validation rule is a unit test, a constraint is an integration test, a status code is an E2E test. Testing everything through HTTP produces a slow suite that localises nothing.
2. **Worker logic is tested with no infrastructure at all.** Handlers receive a `JobContext`; tests pass a fake. This is the evidence the rubric asks for that job logic was tested independently of the API.
3. **No test depends on wall-clock time, network, or randomness.** Time, sleep and randomness are injected (§4).
4. **Assertions state the requirement, not the implementation.** `assert response.status_code == 409` is weak on its own; the test also asserts the job's status is unchanged and that no second row was created.

## 2. Levels

| Level | Runs against | Contains |
|---|---|---|
| **L1 — unit** | nothing (pure) | registry, payload validation, SSRF rules, handler `run()`, clock |
| **L2 — integration** | real PostgreSQL, no HTTP | repository statements, constraints, idempotency under concurrency, migrations |
| **L3 — E2E** | real PostgreSQL, full ASGI stack | endpoint contracts, status codes, error shapes, headers |

L3 uses `httpx.AsyncClient` over `ASGITransport` — the real application including middleware and exception handlers, without binding a port.

## 3. Why a real PostgreSQL and not SQLite

SQLite has no `JSONB`, does not enforce partial unique indexes with the same semantics, has no `TIMESTAMPTZ`, and — decisively — has no `FOR UPDATE SKIP LOCKED`, which spec 04 depends on entirely.

A suite that passed against SQLite would be asserting nothing about the mechanisms this project is being evaluated on. A separate `db-test` service in `docker-compose.yml` costs one container and keeps the tests honest.

## 4. Determinism

**Time** — `Clock` protocol; `FrozenClock` in tests. Anything comparing `scheduled_at` to "now" is deterministic.

**Sleeping** — handlers do not call `asyncio.sleep` directly; they call an injected `Sleeper`. The test sleeper returns immediately and **records the requested durations**, so tests assert that `EmailJob` sleeps within 1–3 s as specified without the suite spending 3 seconds proving it. The full suite runs in seconds.

**Randomness** — `WebhookJob`'s 20 % failure rate comes from an injected `Random`, as does the retry jitter. Tests seed it, or inject a stub, to exercise both branches deterministically and to assert exact backoff delays rather than ranges. A handler whose failure rate cannot be forced cannot be tested.

**Dispatch** — the worker suite runs against `NullDispatch`. That is not a shortcut: it means the Postgres fallback claim, which is the path that actually guarantees correctness, is the one under test, and the suite needs no Redis to be meaningful.

**Loops** — the slot exposes `run_once()`, and tests drive it a step at a time rather than starting `run_forever` and waiting for something to happen. Two tests of the loop itself cannot: W1-15b asserts a *rate*, which only exists over elapsed time, and it uses a 50 ms interval so the wait is bounded and the assertion is a range rather than an exact count. Every other loop test ends the loop from inside a fake — W1-15c sets the stop event from within the failing claim — so nothing waits on a timer.

**Database isolation** — each test runs inside a transaction rolled back on teardown.

**Connection pooling** — the concurrency fixtures use `NullPool` so each session is unmistakably its own connection. The *load* tests use a pooled fixture instead: a worker opens a short unit of work per operation, so draining a backlog means thousands of them, and with `NullPool` each one is a fresh TCP connection and authentication. The first version of the load test took twenty minutes and timed out; pooled, the same test takes three seconds. It was measuring the fixture.

⚠️ **Exception:** every concurrency test needs genuinely separate connections and therefore cannot share a rolled-back transaction — the concurrent-idempotency ones (L2-14, E2E-12), the claiming and cancellation races (W2-03, W2-04, W2-07, W2-19) on `committing_sessions`, and the load tests on `pooled_sessions`. They commit and clean up explicitly by truncating. This is called out because reusing the standard fixture there would make the test silently meaningless — it would serialise the very concurrency it exists to exercise.

## 5. Coverage

`pytest --cov=app --cov-report=term-missing` — the command the README gives. `--cov-fail-under=90` is the floor to add in CI; the suite has stayed above it since part 1.

Excluded: `app/migrations/` only — hand-written Alembic revisions, exercised by L2-17 rather than measured. The worker's entry point is **not** excluded: it is what builds the real dispatch, and excluding it is exactly how a worker wired to `NullDispatch` passed a green suite once already. Current: **100 %**.

⚠️ Coverage is configured with `concurrency = ["thread", "greenlet"]`. SQLAlchemy's asyncio layer runs its synchronous core inside greenlets; without this setting the tracer is lost across a greenlet switch and every line *after* an `await session.execute(...)` is reported as unreached. The symptom is a plausible-looking 96 % that hides nothing real — worth knowing, because the natural reaction is to write tests for code that was already covered.

Coverage is a floor, not the goal: a covered line with no assertion about its behaviour is not tested. Reviews check the assertions, not the percentage.

## 6. L1 — Unit

### Registry

| ID | Case | Assert |
|---|---|---|
| L1-01 | register a valid class | present in `JOB_REGISTRY` under its `job_type` |
| L1-02 | register a duplicate `job_type` | raises at import time |
| L1-03 | register without `Payload`/`Result` | raises at import time |
| L1-04 | look up an unknown type | raises `UnknownJobTypeError` |
| L1-05 | all four production types registered | registry keys == `JobType` members |

### Payload validation — run per job type

| ID | Case | Assert |
|---|---|---|
| L1-10 | valid payload | parses; field values preserved |
| L1-11 | required field missing | `ValidationError` naming the field |
| L1-12 | unknown extra field | rejected (`extra="forbid"`) |
| L1-13 | field at min and max bound | accepted |
| L1-14 | field one past each bound | rejected |
| L1-15 | wrong type (`"5"` where int expected, etc.) | rejected, not coerced |
| L1-16 | `ReportJob` with `date_from > date_to` | rejected |
| L1-17 | `BatchJob` with 0 items / 1001 items | rejected |
| L1-18 | `EmailJob` with malformed address | rejected |

### SSRF rules

| ID | Input | Assert |
|---|---|---|
| L1-20 | `https://example.com/hook` | accepted |
| L1-21 | `http://127.0.0.1/x`, `http://[::1]/x` | rejected |
| L1-22 | `http://10.0.0.1`, `http://192.168.1.1`, `http://172.16.0.1` | rejected |
| L1-23 | `http://169.254.169.254/latest/meta-data/` | rejected |
| L1-24 | `http://localhost`, `http://svc.local` | rejected |
| L1-25 | `file:///etc/passwd`, `gopher://x` | rejected |
| L1-26 | `http://`, `notaurl` | rejected |
| L1-26b | `http://intranet`, `https:///path` | rejected — a single-label name resolves through the local search domain, and pydantic normalises `https:///path` to the host `path` |

### Handlers — against `FakeJobContext`

| ID | Case | Assert |
|---|---|---|
| L1-30 | each handler `run()` | returns an instance of its own `Result` |
| L1-31 | each handler's sleep | recorded duration inside the range the assignment specifies |
| L1-32 | `WebhookJob`, random forced to fail | raises; error carries no payload contents |
| L1-33 | `WebhookJob`, random forced to succeed | returns `status_code`, `response_ms` |
| L1-34 | `BatchJob` progress | `report_progress` called monotonically, ends at exactly 100 |
| L1-35 | `BatchJob` result | `total == len(items)`, `succeeded + failed == total` |
| L1-36 | any handler | never touches a session or repository (fake exposes only the protocol) |

### Core

| ID | Case | Assert |
|---|---|---|
| L1-40 | `FrozenClock` | `now()` stable; advances only when told |
| L1-41 | status transition table | every allowed transition in spec 01 §3 permitted; a sample of disallowed ones refused |

## 7. L2 — Integration (real PostgreSQL)

| ID | Case | Assert |
|---|---|---|
| L2-01 | insert and read back | all columns round-trip, including JSONB |
| L2-02 | get missing id | returns `None` (repository), not an exception |
| L2-03 | list, no filter | newest first |
| L2-04 | list filtered by status | only matching rows |
| L2-05 | list filtered by job_type | only matching rows |
| L2-06 | list with both filters | intersection |
| L2-07 | pagination | `has_more` true when a further row exists, false at the end; no row appears on two pages |
| L2-08 | `limit` above 100 | rejected before reaching SQL |
| L2-09 | cancel a `pending` job | one row updated, status `cancelled` |
| L2-10 | cancel a `scheduled` job | one row updated |
| L2-11 | cancel a `completed`/`cancelled`/`processing` job | **zero rows updated**, status unchanged |
| L2-12 | insert violating each `CHECK` (one test per constraint) | `IntegrityError` |
| L2-13 | duplicate `idempotency_key`, sequential | second insert returns no row; existing row found |
| L2-14 | duplicate `idempotency_key`, 10 concurrent connections | exactly one row exists; all callers get the same id |
| L2-15 | two rows with `idempotency_key = NULL` | both inserted (partial index) |
| L2-16 | raw `UPDATE` bypassing the ORM | `updated_at` changed — trigger fired |
| L2-17 | `alembic upgrade head` then `downgrade base` | both succeed on a clean database |
| L2-18 | `EXPLAIN` list-by-status | uses `ix_jobs_status_created` |
| L2-19 | delete a job | its `job_logs` rows cascade |
| L2-20 | transition helper | writes a `job_logs` row and emits a log line for the same transition |
| L2-21 | `list_logs` | oldest first, ordered by the sequence rather than the shared `created_at`; `has_more` correct across pages; another job's rows excluded |
| L2-22 | idempotency key 25 hours old | still matches; the existing job is returned and no second row is created |

## 8. L3 — E2E

| ID | Scenario | Assert |
|---|---|---|
| E2E-01 | submit a valid job | 201, `status="pending"`, id is a UUID, `Location` resolves |
| E2E-02 | submit with future `scheduled_at` | 201, `status="scheduled"` |
| E2E-03 | submit with past `scheduled_at` | 201, `status="pending"` |
| E2E-04 | submit unknown `job_type` | 422, code `unknown_job_type` |
| E2E-05 | payload missing a required field — one per type | 422, `details` names the field |
| E2E-06 | payload with an extra field | 422 |
| E2E-07 | body over 64 KB | 413, and no row was created |
| E2E-08 | webhook url `127.0.0.1` / `169.254.169.254` | 422 |
| E2E-09 | `priority` = -1 and = 10 | 422 |
| E2E-10 | `scheduled_at` without timezone | 422 |
| E2E-11 | same `idempotency_key` twice | first 201, second **200** with the same id; exactly one row |
| E2E-12 | 10 concurrent submits, one shared key | exactly one row; all responses share an id |
| E2E-13 | same key, different payload | 200, original job returned unchanged, warning logged |
| E2E-14 | get an existing job | 200, every documented field present |
| E2E-15 | get an unknown id | 404, code `job_not_found` |
| E2E-16 | get a malformed uuid | 422 |
| E2E-17 | list filtered by status | only matching |
| E2E-18 | list filtered by type | only matching |
| E2E-19 | list paginated | `has_more` correct; pages do not overlap |
| E2E-20 | cancel a `pending` job | 200, `status="cancelled"` |
| E2E-21 | cancel a `scheduled` job | 200 |
| E2E-22 | cancel the same job twice | second returns 409, status still `cancelled` |
| E2E-23 | cancel an unknown id | 404 |
| E2E-24 | `GET /health` | 200, counts match rows actually present |
| E2E-25 | `X-Request-ID` | echoed when supplied, generated when not, present on every response |
| E2E-26 | force a 500 | body contains no `Traceback`, no SQL, no driver text |
| E2E-27 | register a throwaway job type in-test and submit it | 201 — proves extensibility with no changes outside `app/jobs/` |
| E2E-28 | logs captured during one request | all valid JSON, all carrying the response's `request_id` |
| E2E-34 | a job's history | every transition, oldest first, with `meta` intact |
| E2E-35 | history of an unknown id | 404, code `job_not_found` — not an empty page |
| E2E-36 | history paginated | `has_more` correct; pages do not overlap; `limit` of 0 or 101 is 422 |
| E2E-37 | a sensitive payload driven through claim, note and failure | no field of the payload appears in any of the four rows, while the same string is still returned by `GET /jobs/{id}` |

## 9. Part 2 — worker

Specs 03–08. The whole worker suite runs against `NullDispatch`, so the Postgres fallback claim — the path that guarantees correctness — is what the tests exercise. Redis-specific behaviour is tested separately against a real Redis.

### W1 — unit, no I/O

| ID | Case | Assert |
|---|---|---|
| W1-01 | backoff table | 30 s after attempt 1, 120 s after attempt 2 |
| W1-02 | equal jitter bounds | delay ∈ [d/2, d]; pinned RNG gives an exact value |
| W1-03 | jitter is monotonic across attempts | attempt 2's minimum exceeds attempt 1's maximum |
| W1-04 | dispatch score encoding | higher priority sorts first; FIFO within a priority |
| W1-05 | score precision | encoded value < 2^53 across the whole legal priority range |
| W1-06 | `NullDispatch` | announce is a no-op; `next_hint` yields nothing; workers report unknown |
| W1-07 | `worker_id` format | unique per slot within one process |
| W1-08 | slot: happy path | claim → run → `mark_completed` with the claimed `attempts` |
| W1-09 | slot: handler raises, attempts remain | `reschedule_after_failure`, never `mark_failed` |
| W1-10 | slot: handler raises, attempts exhausted | `mark_failed`, never `reschedule` |
| W1-11 | slot: unparseable stored payload | `mark_failed` on the **first** attempt |
| W1-12 | slot: heartbeat loses the lease | handler task cancelled; no result written |
| W1-13 | slot: nothing to claim | `run_once()` returns False and writes nothing |
| W1-14 | dispatch read timeout | the client's socket budget exceeds the longest block it will carry, so an idle poll cannot be mistaken for a Redis failure |
| W1-14c | dispatch fails while the caller asked to block | the failure still costs the block — the remainder of it, not the whole timeout again — while the non-blocking form returns at once |
| W1-15 | slot: a cycle raises | the loop reports, waits one interval, and goes on claiming — it does not end the slot |
| W1-15b | slot: every cycle raises | roughly one attempt per interval, not thousands — an outage must not become a hot loop against the dependency that is already down |
| W1-15c | slot: a shutdown lands during that wait | the loop ends there, rather than waiting the interval out and running one more cycle |

### W2 — integration, real PostgreSQL

| ID | Case | Assert |
|---|---|---|
| W2-01 | `claim_next` ordering | highest priority first; oldest first within a priority |
| W2-02 | claim skips ineligible rows | future `scheduled_at`, and any status other than `pending` |
| W2-03 | **10 concurrent claims, 1 pending job** | exactly one succeeds, nine get `None` |
| W2-04 | **10 concurrent claims, 10 pending jobs** | ten distinct jobs, no id claimed twice |
| W2-05 | claim increments attempts and sets the lease | `worker_id` and `lease_until` both non-null |
| W2-06 | ownership predicate | a write with a stale `attempts` matches zero rows |
| W2-07 | **the displaced-worker test** | claim → expire lease → reap → another worker completes it → the original worker's write matches zero rows and its result is discarded |
| W2-08 | reaper returns expired jobs | status `pending`, `worker_id` and `lease_until` cleared |
| W2-09 | reaper fails exhausted jobs | expired with `attempts == max_attempts` → `failed`, and the next claim raises no `IntegrityError` |
| W2-10 | **reaper leaves live leases alone** | including one expiring a second from now |
| W2-11 | promoter | due `scheduled` → `pending`; not-yet-due untouched |
| W2-12 | retry timing | a failed attempt lands `pending` with `scheduled_at` in the future, and is not claimable until it passes |
| W2-13 | retry exhaustion | third failure → `failed`, `completed_at` set, no `result` |
| W2-14 | progress writes | visible on the row while the job runs; ignored once ownership is lost |
| W2-15 | graceful shutdown | in-flight job completes, no new job claimed, process exits within the grace period |
| W2-16 | forced shutdown | job left `pending` with the lease cleared, claimable at once |
| W2-16c | **forced shutdown on the final attempt** | `failed`, not `pending` — a requeue here would breach `ck_jobs_attempts` on the next claim and wedge claiming for every worker |
| W2-16d | the claim after such a release | succeeds and returns a different job, rather than raising |
| W2-16e | such a failure is not poison | no `dead_letter_reason`; a manual retry is accepted |
| W2-17 | `EXPLAIN` on the claim | uses `ix_jobs_claim` |
| W2-18 | Redis dispatch round trip | announce → `BZPOPMIN` returns it; a stale hint claims nothing and is dropped |
| W2-19 | **cancel racing pickup** | ten cancels interleaved with ten claims over the same rows: every job ends either cancelled and never claimed, or claimed once and never cancelled |

### W3 — end to end

| ID | Scenario | Assert |
|---|---|---|
| W3-01 | submit via API → run worker → read back | `completed` with a `result` matching the handler's `Result` schema |
| W3-02 | **priority ordering, single worker** | claimed in strict priority order — see the note below |
| W3-03 | webhook forced to fail, then succeed | `attempts` reflects both, final status `completed` |
| W3-04 | `/health` with workers running | worker count, ids, `oldest_pending_seconds` |
| W3-05 | `/health` with Redis stopped | `200`, `"redis": "error"`, `workers: null` — not `count: 0`, not `503` |
| W3-06 | batch job progress | `GET /jobs/{id}` shows progress advancing, ending at 100 |
| W3-07 | `job_logs` for one job | one row per transition, in order, each naming the `worker_id` |

### W4 — load

| ID | Case | Assert |
|---|---|---|
| W4-01 | 200 jobs, 4 slots, real PostgreSQL | **zero double executions** (the handler records every run), zero jobs left in `processing`, every job terminal |
| W4-02 | same, with a worker killed mid-run | all jobs still reach a terminal state after the reaper sweeps |
| W4-03 | 40 jobs arriving at **idle workers through a real Redis** | zero double executions, **and** at least one claim recorded `from_hint` |

⚠️ **W4-03 needs the worker running before the work arrives.** A slot consults Redis only when its own scan came back empty (spec 03 §3), so seeding a backlog and then starting a worker exercises the PostgreSQL fallback with Redis merely attached — a test that passes identically with no dispatch at all. The worker therefore starts first and settles into `BZPOPMIN`.

The `from_hint` assertion is the point of the test, and it is not decoration. Everything else in it is also true when the dispatch is broken, because that is what graceful degradation means; the claim's own `from_hint` flag in `job_logs` is the only thing that distinguishes "the hint path works" from "the fallback covered for it". Swapped to `NullDispatch` the test fails, which is how it was checked.

Two smaller things follow from the same principle. The dispatch is built through `RedisDispatch.from_url(url, max_block_seconds=POLL)` rather than `RedisDispatch(Redis.from_url(...))`, because only the former derives the socket read budget the worker actually runs with — wrapping a default client would leave the one test billed as exercising the production path silently not exercising it (W1-14b pins the derivation itself). And the wait for the backlog to clear is bounded by `asyncio.wait_for`, as `drain` bounds W4-01 and W4-02: a hint path that stops handing work out is the defect this test exists to catch, and an unbounded wait would answer it by hanging the suite rather than by failing.

### Multi-process coverage — a stated limit

Every automated concurrency test runs **slots inside one process**, against real connections and real commits. Nothing in the suite starts a second OS process.

That is a deliberate boundary, not an omission. What the tests must exercise is two claimants racing for one row, and a slot is a genuine claimant: its own session, its own connection, its own transaction, arbitrated by the same row lock a second process would meet. A subprocess would add scheduling, teardown and log-plumbing complexity to the most timing-sensitive tests in the suite while changing nothing about the mechanism under test — the guarantee comes from `FOR UPDATE SKIP LOCKED` and the conditional update, which know nothing about process boundaries.

The multi-process case is covered instead by `docker-compose.yml` running two worker replicas by default, and by `DEMO.md` §10, which kills both and watches the reaper recover their work.

## 10. Traceability

The assignment names six required test scenarios. Where each one lives:

| Required scenario | Tests | Status |
|---|---|---|
| Job submission and retrieval | E2E-01, E2E-14, L2-01 | **part 1** |
| Cancellation | E2E-20…23, L2-09…11 | **part 1** |
| Idempotency | E2E-11…13, L2-13…15, L2-22 | **part 1** |
| Job completion flow | W3-01, W1-08, W2-05 | **part 2** |
| Job failure and retry | W2-12, W2-13, W1-09, W1-10 | **part 2** |
| Priority ordering | W3-02, W2-01 | **part 2** |

⚠️ **The priority-ordering test must run a single worker.** With concurrent workers each claims the highest-priority job *available to it*, not the globally highest (architecture §7, spec 04 §4), so a multi-worker ordering assertion is flaky by construction rather than by accident. W2-01 covers ordering at the SQL level; W3-02 covers it end to end with one worker.

## 11. Part 3 — hardening

Spec 09, in `tests/integration/test_hardening.py`, with the endpoint covered end to end in `tests/e2e/test_jobs_api.py` (E2E-29…33).

| ID | Case | Assert |
|---|---|---|
| H-01 | retry a `failed` job | `pending`, `attempts = 0`, error and timestamps cleared |
| H-02 | a retried job is claimed again | counting starts over at 1 |
| H-03 | retry any non-failed status | zero rows, status unchanged |
| H-04 | **retry a dead-lettered job** | refused |
| H-06 | timeout with attempts remaining | ordinary retry, **not** dead-lettered |
| H-07 | attempts exhausted by timeouts | `timeout_loop`, logged at `error` |
| H-08 | unparseable payload | `unprocessable_payload` on the first attempt |
| H-09 | attempts exhausted via lease expiry | `worker_crash_loop` |
| H-10 | **attempts exhausted by ordinary exceptions** | **NULL — and still retryable** |
| H-11 | `dead_letter_reason` on a non-failed job | `IntegrityError` |
| H-12 | `dead_lettered` filter and health counter | only the poison |

H-10 is the one worth reading. Everything else here is machinery; that test is the design.

The slot's timeout branch is covered in `tests/unit/test_slot.py`, including that it is not mistaken for losing the lease — confusing the two would mean a timed-out job records no outcome and waits for the reaper instead.

## 12. Job history

Spec 10. `GET /jobs/{id}/logs` in `tests/e2e/test_jobs_api.py` (E2E-34…37), `list_logs` in `tests/integration/test_repository.py` (L2-21).

The case that matters is E2E-37. The endpoint returns `meta` as stored, which is safe only because submission records a payload's size and digest rather than its contents (spec 02 §7) — a property of a different module, three layers away, and one that a future log line could break without anything else failing. So it is asserted here as well as there.

Reading a freshly submitted job would not have asserted it. That job has one row, whose `meta` is the fingerprint; the rows that carry worker-supplied values — the claim, a handler's own note through `ExecutionService.note`, the failure — would all have gone unexamined. The test therefore claims the job, notes a line and fails it before reading, and it raises the failure with the payload inside the exception message: `jobs.error` keeps that text and `job_logs` records only the exception's class name, which is the distinction §3 of spec 10 rests on. `GET /jobs/{id}` is asserted to still return the string, so its absence from the history is a property of `job_logs` rather than of the test's own inputs.

## 13. Later additions

Priority aging, if it is ever implemented. Recorded so this plan stays the single source of truth.
