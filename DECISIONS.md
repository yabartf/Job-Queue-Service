# Design Decisions

Each decision was written when it was made, and each one has a spec in `specs/` that was reviewed before the code. This file is the summary; the specs carry the detail.

493 tests, 100 % coverage. Verified end to end against the real stack: jobs run to completion, the system keeps processing with Redis stopped, and `SIGKILL` on both workers strands four jobs that the reaper returns and another worker finishes.

---

## 0. Stack

**Chosen:** Python 3.12, FastAPI, PostgreSQL 16, Redis.

**Why FastAPI:** job payloads are untrusted input that gets persisted and later executed in another process, so validation is a security concern here, not a convenience. Pydantic lets each job type declare its own schema, enforced at the boundary, and gives an OpenAPI page for free.

**Why PostgreSQL:** the job table is the state of record, and everything this assignment asks for — priority, scheduling, cancellation, idempotency — is naturally a predicate over that same table.

**Trade-off:** Django would have brought an admin and ORM, and a much larger surface than a queue service needs. Flask would work, but I would be hand-rolling the validation that is being graded.

The worker is a plain process and does not import FastAPI. The web framework is a detail of the API layer only.

---

## 1. Job Pickup Strategy

**Chosen:** PostgreSQL is the single source of truth. Redis is a dispatch hint that decides nothing.

Redis holds a sorted set of ready jobs and is the fast path workers block on. A worker pops an id and then attempts a conditional update:

```sql
UPDATE jobs SET status='processing', worker_id=$me,
                lease_until=now() + interval '60 seconds', attempts=attempts+1
WHERE id=$job AND status='pending'
RETURNING *;
```

Zero rows means the hint was stale — cancelled or already taken — and the worker drops it. When Redis has nothing, the worker claims directly from Postgres with `FOR UPDATE SKIP LOCKED`, so concurrent workers step over locked rows instead of queueing behind them.

**Why:** two workers can never both hold a job, because ownership is decided by one conditional write rather than by ordering or coordination. `AND status='pending'` is the whole mechanism.

Keeping the queue and the state in one system also removes a class of problem: submission is one transaction, not a write to two stores with no shared commit.

**The property I care about most:** the system is correct with Redis stopped. Every job reachable through Redis is reachable through the fallback, so losing Redis costs latency and never work.

**Trade-offs:** throughput is bounded by the Postgres write rate, since every claim is a write. Claim latency is an indexed scan rather than an O(log n) pop. At a scale that demanded more, the path is to make Redis authoritative and add an outbox — accepting a relay process and a reconciliation sweep as the price. I did not pay that for this workload.

### `attempts` is the fencing token

Every write after a claim repeats ownership in its `WHERE` clause: `id`, `worker_id`, `attempts`, `status='processing'`.

`worker_id` alone is not enough, and the reason is specific to running several slots in one process:

1. Slot A claims job J as `w1`, then stalls.
2. The lease expires and the reaper returns J to `pending`.
3. Slot B — same process, same `w1` — claims J.
4. Slot A wakes and writes its result. It matches, and overwrites B's work.

`attempts` closes it: it increments on every claim, so a value read at claim time can never describe a later one. A is holding `1`, the row now says `2`, A's write matches nothing and A discards its own result. No new column, no migration — `attempts` already existed and already incremented at the right moment.

**A write matching zero rows is not an error.** It means ownership was lost. Discard and continue.

### One clock, and it is the database's

`lease_until`, retry deadlines, `started_at` and `completed_at` are all computed in SQL as `now() + interval`, never in Python.

These values are *compared* by the database: the claim tests `scheduled_at <= now()`, the reaper tests `lease_until < now()`. Mixing a worker's clock into one side of that comparison would make every lease and every retry fire early or late by exactly the skew between the machines, silently.

I got this wrong first: `fail()` originally computed `run_at` in Python. The integration tests caught it because they run against a frozen clock, which is simply a very large skew.

Full flow and rejected alternatives: `specs/00-architecture.md`, `specs/04-claiming.md`.

---

## 2. Worker Crash Recovery

**Chosen:** a lease with a heartbeat, a background reaper, and every write conditioned on still owning the job.

A claim writes `worker_id` and `lease_until = now() + 60s` in the same statement that sets `processing`. The worker extends the lease while it runs. The reaper returns jobs whose lease has expired.

**Why not detect a lost connection:** a worker can be alive but unreachable, or stalled in a long GC pause, and a connection can drop while the process runs on. A lease makes liveness something the worker must actively assert, so every failure mode looks the same: the lease stops being extended.

The schema enforces it rather than trusting the code — `ck_jobs_processing_has_lease` makes it impossible to mark a job `processing` without recording who holds it and until when.

**What happens if a worker crashes mid-job:**

1. It stops extending `lease_until`.
2. Within one reaper interval the lease expires and the job returns to `pending`, with `attempts` already spent — so a job that reliably kills workers cannot loop forever.
3. Another worker claims it and runs it again.

**The case that actually matters** is the one that looks like a crash and is not. A worker stalled past its lease is reclaimed while still running, so for a period two workers hold the same job. The ownership predicate resolves it: the stalled worker's write matches zero rows and it discards its result. Without that predicate it would overwrite the work of whoever legitimately took over — silent corruption no happy-path test would catch.

**Losing the lease cancels the work.** When a heartbeat matches zero rows, the slot cancels the running handler instead of letting it finish. The result would be rejected anyway; this bounds the window where two workers execute the same job to about one heartbeat.

**Returning a job to the queue always takes two statements.** A job whose lease expires with `attempts == max_attempts` cannot go back to `pending`: the next claim would compute `attempts + 1` past the limit and violate `ck_jobs_attempts`. Since the claim picks by priority and age, that one row would then be selected first by every worker in the fleet. Exhausted jobs are failed instead.

I had this right in the reaper and wrong in the graceful-shutdown release, which did the same thing to the same column with one statement. One badly timed deploy would have stopped the queue. Both paths now split the same way, and the shutdown case is **not** dead-lettered — nothing about the job caused it.

**The reaper runs in every worker process, with no leader election.** Both sweeps are `LIMIT`-bounded conditional updates using `SKIP LOCKED`, so concurrent sweepers take disjoint sets. A dedicated reaper process would be another deployable and another thing to monitor.

**What this does not give:** delivery is at-least-once. The stalled worker's writes are rejected, but a side effect it already performed has happened. See §5.

Detail: `specs/06-crash-recovery.md`.

---

## 3. Priority Queue Implementation

**Chosen:** ordering is a predicate on the claim query, not a separate data structure.

```sql
ORDER BY priority DESC, created_at ASC
FOR UPDATE SKIP LOCKED LIMIT 1
```

served by a partial index on `(priority DESC, created_at) WHERE status = 'pending'`. Redis mirrors the same ordering in a sorted set, but Postgres remains the authority.

**Why:** every alternative forces a trade-off this one avoids.

| Option | Why not |
|---|---|
| Redis lists, `BRPOP` across priority keys | discrete levels with strict precedence, no aging possible; and the reliable variant `BLMOVE` takes one source key, so reliability and multi-key priority cannot be combined |
| Redis Streams | at-least-once and `XAUTOCLAIM` are attractive, but streams are strictly time-ordered with no priority; one stream per level gives back the clean blocking read |
| RabbitMQ | 255 native levels, but another broker — and priority stops applying once messages are prefetched |

Ordering in SQL keeps the scale continuous and the tie-break explicit.

**Two details that are decisions, not defaults:**

`priority` is a `SMALLINT` constrained to **0–9**. An unbounded integer would let any client submit `2^31-1` and pre-empt the queue permanently. The bound also keeps the Redis score inside float64's exact-integer range.

Under concurrent workers, ordering is **per-claim, not global**: each worker takes the highest-priority job *available to it*, because `SKIP LOCKED` steps over what another worker holds. That is inherent to any concurrent queue, and it means a priority-ordering test must run a single worker or it is flaky by construction.

**Scheduling shares the query, with one deliberate asymmetry.** A job submitted for a future time waits in `scheduled` and is promoted by a sweep; a job waiting out a retry backoff stays `pending` with a future `scheduled_at` and is held back by the claim predicate alone. `status` is client-facing semantics — a job serving a backoff is mid-lifecycle, and calling it `scheduled` would misrepresent it. The split also keeps the hot path on its partial index.

Detail: `specs/04-claiming.md`.

---

## 4. Retry Backoff Strategy

**Chosen:** retries reuse the scheduling mechanism. A failed attempt with retries left sets `status='pending'` and `scheduled_at = now() + backoff`, and the claim query's existing predicate does the rest. No delayed queue, no second code path.

**Timing:**

| Attempt | Nominal | Actual wait, with jitter |
|---|---|---|
| 1 | immediate | immediate |
| 2 | 30 s | **15–30 s** |
| 3 | 2 min | **60–120 s** |
| after 3 | — | `FAILED`, permanently |

The third column is what a stopwatch measures. No retry here waits exactly 30 seconds, by design.

**Jitter: equal, not full.** The wait is `delay/2 + uniform(0, delay/2)`. A downstream outage fails every in-flight job at roughly the same moment; without jitter they all retry at the same instant, and again after the same delay, turning recovery into a self-inflicted load spike. I first specified full jitter (`uniform(0, delay)`), which spreads better, and changed it because it can retry after two seconds — and a correct implementation that appears to ignore the stated timing is a bad trade when equal jitter preserves the property that matters.

**`attempts` increments at claim time, not at failure.** A worker that dies without writing anything still consumes an attempt. Otherwise a payload that reliably kills its worker would be reclaimed and retried forever, taking down every worker in turn — the queue-poisoning case the assignment names.

**A payload that no longer parses is failed immediately.** It passed validation at submission, so a parse failure at execution means the schema moved underneath a stored job. Retrying would fail identically twice more and occupy a worker each time.

### Dead-letter routing: a column, not a status

`dead_letter_reason` is NULL unless the failure means the job **cannot run**.

| How the job ended | Reason |
|---|---|
| stored payload no longer parses | `unprocessable_payload` |
| attempts exhausted, last failure a timeout | `timeout_loop` |
| attempts exhausted through lease expiry | `worker_crash_loop` |
| **attempts exhausted on ordinary exceptions** | **NULL** |

That last row is the design. A webhook that returned 500 three times did its work and the service it called was down; retrying once that recovers is right. What belongs in the dead-letter set is work no retry can help.

**Why not a seventh status:** `failed` already is the terminal failure state. Being dead-lettered describes *why* a job failed, so a status would have meant editing the enum, the CHECK constraint, the transition table and every status filter to say what `failed` already says.

**What it buys:** `POST /jobs/{id}/retry` refuses dead-lettered jobs. An operator draining an incident by retrying everything that failed must not re-arm a job that takes a worker down with it.

**Trade-off:** a reviewer looking for something shaped like a queue will find a filter and a counter, not a separate store. The jobs never left the table, and moving them would mean a second place to keep consistent.

### Job timeout

`BaseJob.timeout_seconds` — 60 by default, 120 for reports, 300 for batches — enforced with `asyncio.wait_for`, so a wedged job occupies a slot for a bounded time instead of until its lease expires. Worth admitting how this was found: the attribute existed from part 1 and **nothing read it**. Dead configuration is the one kind of dead code no tooling reports.

Detail: `specs/05-retry-and-failure.md`, `specs/09-hardening.md`.

---

## 5. One Thing I Would Do Differently With More Time

**Exactly-once side effects.**

Everything above guarantees a job's *state* is written once. Delivery is at-least-once, so a job's *side effects* can happen twice: a worker stalled past its lease has already sent the email by the time its write is rejected. I documented that rather than hiding it, but documenting a limitation is not handling it.

With more time I would push idempotency into the handlers, where it belongs: give each execution a deterministic operation key derived from the job id and attempt, and require handlers with external side effects to present it downstream. That turns "we might send twice" into "the downstream deduplicates", which is the only place the problem can actually be solved. It changes the `BaseJob` contract, which is why it needs time rather than a patch.

**Other things I knowingly left out:**

- **No aging, so low priority can starve.** Two earlier drafts of this file called aging "a one-line change to the `ORDER BY`" — true of the diff, false of the cost. A sort key containing `now()` is not a stored value, so no index can return it in order: measured with `EXPLAIN`, it turns an index scan into a sort over every eligible row, on the most frequent query in the system. The approaches that keep the index — a promotion sweep, a lottery claim every Nth time, a stored effective priority — are compared in `specs/04-claiming.md` §4. Starvation is at least visible: `oldest_pending_seconds` climbing while throughput stays healthy is its signature.
- **Unbounded table growth.** Nothing removes terminal rows. Keeping idempotency keys forever is deliberate — the requirement is a floor of 24 hours, and an expiring key would mean a client retrying later quietly gets a second job — but a retention policy is still needed, and it is constrained: a 24-hour floor that must be enforced in config, a currently unreachable branch in `JobService.submit` that any expiry makes reachable, and the fact that deleting a job deletes its audit trail. Written out in `specs/01-data-model.md` §8.
- **No backpressure and no rate limiting.** Submission is unbounded. For a queue the fitting mechanism is a depth ceiling, not requests per second — a limiter does nothing about a client that accumulates a million jobs slowly. Rate limiting is also awkward here: with no authentication there is no principal to limit, and a shared-state limiter would have to choose between failing open during the incident it exists for and failing closed, which contradicts §7's decision that Redis being down is not a `503`. In production this belongs at the edge.
- **No authentication.** The assignment specifies none, so I built none — but the consequence is explicit: anyone who can reach the API can read any job whose id they know. Random UUIDs are obscurity, not authorization.
- **SSRF protection is incomplete by construction.** The validator rejects private ranges, the metadata endpoint and single-label names, but a hostname that resolves publicly at submission can resolve to `127.0.0.1` by execution time. Closing it means pinning the resolved address in the HTTP client. The webhook here is simulated, so the residual risk is nil today.

---

## 6. Worker Concurrency Model

**Chosen:** each worker process runs N independent **slots** — separate coroutines, each with its own claim → execute → write loop — and several processes run side by side. Compose runs 2 × 2.

**Why slots:** the service is async end to end and a job spends nearly all its time awaiting I/O, so one job per process leaves an event loop idle. Slots also make concurrency testable in-process: the exactly-once and load tests run four real claim loops against a real database without spawning subprocesses.

**Why not a pre-fetched batch:** a slot claims only when it is free, so the number of jobs in `processing` never exceeds the number being worked on. A worker that pre-fetched would hold leases on jobs it had not started, and every one would have to survive a crash.

**Trade-off:** slots share a process, so they share its failure — one `SIGKILL` loses N jobs rather than one. Acceptable precisely because the lease mechanism treats losing N the same as losing one.

---

## 7. Observability Decisions

**Redis being down is not a `503`.** The database is a hard dependency; Redis is not. Jobs keep flowing through the Postgres fallback, so a `503` would pull a working service out of a load balancer and turn a latency problem into an outage.

**"No workers" and "cannot see the workers" are reported differently.** Worker liveness comes from Redis keys with a TTL. When Redis is unreachable the field is `null`, never `count: 0` — otherwise an operator restarts healthy workers in the middle of a cache outage.

**`oldest_pending_seconds` is the field worth adding.** Depth alone cannot separate load from failure; depth and age together can. High depth with low age is a busy system keeping up. Low depth with high age is a stuck one.

**An audit trail nobody can read is not observability.** `job_logs` was written on every transition from part 1, and for two parts the only way to read it was `psql`. `GET /jobs/{id}/logs` closes that. The job row carries only the *latest* error, so without the history a job that failed twice and then succeeded looks like an ordinary success.

**Recovery events are logged at `warning`, not `info`.** A job being reaped is the system working correctly, so `error` would be wrong — but a healthy deployment produces almost none, and a sudden run means workers are dying or the lease is too short.

**A warning that fires constantly is worse than no warning.** The first Redis dispatch had `next_hint` blocking for the poll interval while redis-py's socket read timeout was the same 5 seconds. The socket always won, so every idle poll logged the same line that means "Redis is unreachable" — twice every five seconds, against a healthy server. Nothing broke, because the fallback carries the work, which is exactly why a green suite never noticed. The client's read budget is now derived from the longest block it will carry, and a test pins the relationship.

Detail: `specs/08-worker-observability.md`.
