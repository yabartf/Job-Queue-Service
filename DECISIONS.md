# Design Decisions

Each decision below was written when it was made, not reconstructed afterwards, and each one is backed by a specification in `specs/` that was reviewed before the corresponding code was written.

Some decisions are settled but not yet built. Rather than leave those sections empty, each carries an explicit **Status** line, so it is always clear which parts of this document describe running code and which describe a committed design. Nothing here is aspirational: every "designed" item has a spec, a state machine entry, and — where it touches the schema — columns and indexes already migrated.

| Part | Contents | State |
|---|---|---|
| 1 | Data model, job class hierarchy, submit / get / list / cancel / health | **Implemented** |
| 2 | Worker runtime, claiming, retry, crash recovery, Redis dispatch | **Implemented** |
| 3 | Manual retry endpoint, dead-letter routing, job timeout enforcement | **Implemented** |

464 tests, 100 % line coverage. Verified end to end against the real stack: a submitted job runs to completion, the system keeps processing with Redis stopped, and `SIGKILL` on both workers strands four jobs which the reaper returns and another worker finishes — with no intervention.

---

## 0. Stack

**Approach chosen:** Python 3.11+, FastAPI for the API layer, PostgreSQL for persistence.

**Why:**

- **FastAPI** — payload validation is a first-class concern in this system: job payloads are untrusted input that gets persisted, handed to a worker in another process, and executed. Pydantic lets each job type declare an explicit schema that is enforced at the API boundary, so a malformed payload is rejected before it can ever reach a worker. The generated OpenAPI page also makes the service trivially explorable without reading the README.
- **PostgreSQL** — the job table is the system's state of record, and the queue semantics this assignment requires (priority ordering, scheduled execution, cancellation, idempotency) are all naturally expressed as predicates over that same table.

**Trade-offs:** Django would have supplied an admin and ORM out of the box, but brings a large surface area that a queue service does not need. Flask would work, but the payload validation and API documentation that Pydantic/FastAPI provide for free would have to be written by hand — in a system where validation is a graded security concern, that is the wrong thing to hand-roll.

**Note on layering:** the worker is a plain Python process and does not import FastAPI. The web framework is a detail of the API layer only; job execution, claiming, and retry logic live in a core layer that both the API and the worker depend on.

---

## 1. Job Pickup Strategy

**Approach chosen:** PostgreSQL is the single source of truth; Redis is a dispatch hint.

Redis holds a sorted set of ready jobs and is the fast path workers block on. It decides nothing. A worker pops a job id from Redis and then attempts a conditional update in Postgres:

```sql
UPDATE jobs SET status='processing', worker_id=$me, lease_until=now()+interval '60 seconds', attempts=attempts+1
WHERE id=$job_id AND status='pending'
RETURNING *;
```

Zero rows means the hint was stale — the job was cancelled or already claimed — and the worker discards it. When Redis has nothing, the worker falls back to claiming directly from Postgres with `FOR UPDATE SKIP LOCKED`, which lets concurrent workers step over rows another worker has locked rather than queue behind them.

**Why:** Two workers can never both hold a job, because ownership is decided by a single conditional write rather than by ordering or coordination. The `AND status='pending'` predicate is the whole mechanism: if two workers race, only one observes the expected status.

Keeping the queue and the state in one system also removes an entire class of problem. Submission is one transaction, not a write to two systems with no shared commit. Cancellation, idempotency, scheduling and retry backoff all become predicates over the same row rather than separate coordination protocols against a second store.

The property I care about most is that **the system is correct with Redis stopped**. Every job reachable through Redis is also reachable through the fallback query, so losing Redis raises latency and never loses or duplicates work — and the fallback is not an untested emergency branch, because scheduled jobs, elapsed retry backoffs and reaper-released jobs arrive through it on every run.

**Trade-offs:** Throughput is bounded by the Postgres write rate, since every claim is a write — ample here, but a Redis-authoritative queue would have a far higher ceiling. Claim latency also depends on an indexed scan rather than an O(log n) pop, and the jobs table becomes hot enough at scale to need archival of completed rows. If throughput demanded it, the path forward is to make Redis authoritative for dispatch and add an outbox to keep submission a single transaction — accepting a relay process and a reconciliation sweep as the cost. I did not pay that cost for a workload this size.

### `attempts` is the fencing token

Claiming establishes ownership, and **every write after it repeats that ownership in its `WHERE` clause**:

```sql
WHERE id = :id AND worker_id = :worker AND attempts = :claimed_attempts
  AND status = 'processing'
```

`worker_id` alone is not enough, and the reason is specific to running several slots inside one process:

1. Slot A claims job J as `w1`, then stalls.
2. The lease expires and the reaper returns J to `pending`.
3. Slot B — same process, therefore the same `w1` — claims J.
4. Slot A wakes and writes its result.

Step 4 matches, because `worker_id` is still `w1`, and slot A overwrites the result of the worker that legitimately took over. Silent data loss, reachable only under a specific interleaving, invisible to any single-worker test.

`attempts` closes it: it increments on every claim, so a value read at claim time can never describe a later claim of the same job. Slot A remembers `1`, the row now holds `2`, A's write matches nothing, and A discards its own result. This is a complete fencing token **with no new column and no migration** — `attempts` already existed and was already incremented at exactly the right moment. `worker_id` is additionally made unique per slot, which is redundant with the above but free, and makes every log line directly attributable.

**A write matching zero rows is not an error.** It means ownership was lost. The correct response is to discard and continue — never to retry the write, never to raise.

### One clock, and it is the database's

Every deadline in the system — `lease_until`, a retry's `scheduled_at`, `started_at`, `completed_at` — is computed **in SQL** as `now() + interval`, never as an instant calculated in Python and sent over.

The reason is that these values are *compared* by the database. The claim predicate tests `scheduled_at <= now()`; the reaper tests `lease_until < now()`. If one side of that comparison came from a worker host's clock and the other from the database's, every lease and every retry would fire early or late by exactly the skew between the two machines — and nothing anywhere would report it. It would look like a queue that is occasionally, inexplicably, a few seconds wrong.

I did not get this right first time. `ExecutionService.fail` originally computed `run_at = clock.now() + delay` and passed a timestamp down. The integration tests caught it immediately for an unusual reason: they run against a *frozen* clock, which is simply a very large skew, and a job scheduled two hours in the past was instantly claimable. A production skew of a few hundred milliseconds would have produced the same bug with none of the visibility.

The injected `Clock` remains, but only for values that are never compared against the database — deciding whether a submitted `scheduled_at` is in the future, and nothing else.

Full flow, invariants and rejected alternatives: `specs/00-architecture.md`, `specs/04-claiming.md`.

---

## 2. Worker Crash Recovery

**Status:** implemented — `app/worker/runtime.py`, `app/worker/maintenance.py`, `specs/06-crash-recovery.md`. `worker_id`, `lease_until` and `ix_jobs_lease` were migrated in part 1, so this needed no schema change at all.

**Approach chosen:** a lease with a heartbeat, plus a background reaper — and, critically, **every write conditioned on still owning the job**.

A worker claiming a job writes `worker_id` and `lease_until = now() + 60s` in the same statement that sets the status to `processing`. While executing, it extends `lease_until` periodically. A reaper returns jobs whose lease has expired:

```sql
UPDATE jobs SET status='pending', worker_id=NULL
WHERE status='processing' AND lease_until < now();
```

**Why:** the alternative — treating a lost connection as the signal — does not work. A worker can be alive but unreachable, or stalled in a long garbage-collection pause, and a connection can drop while the process keeps running. A lease makes liveness something the worker must actively assert, so the failure mode is uniform: whatever went wrong, the lease stops being extended and the job returns to the queue.

The schema enforces the invariant rather than trusting the code. `ck_jobs_processing_has_lease` makes it impossible to mark a job `processing` without also recording who holds it and until when — which is precisely the write that, if it ever went missing, would strand a job in `processing` forever with nothing to detect it.

**What happens if a worker crashes mid-job:**

1. It stops extending `lease_until`.
2. Within one reaper interval the lease expires and the job returns to `pending`, with `attempts` already incremented — it consumed one of its attempts, so a job that reliably kills workers cannot loop forever.
3. Another worker claims it and runs it again.

The case that actually matters is the one that looks like a crash but is not. A worker stalled past its lease is reclaimed while still running, so for a period **two workers hold the same job**. That is resolved by making the completion write conditional on ownership:

```sql
UPDATE jobs SET status='completed', result=$1, completed_at=now()
WHERE id=$job AND worker_id=$me AND status='processing';
```

If the stalled worker wakes and tries to write, this matches zero rows and it discards its own result. Without the `worker_id` predicate it would overwrite the result of the worker that legitimately took over — a silent corruption that no test of the happy path would ever catch.

**Losing the lease cancels the work.** When a heartbeat matches zero rows, the worker no longer owns the job, and the slot cancels the running handler rather than letting it finish. This is worth doing even though the result would be rejected anyway: it bounds the window in which two workers are executing the same job to roughly one heartbeat interval instead of however long the handler had left. It turns "the job runs twice, in full" into "the job runs twice, and the second one stops early" — two emails versus one email and a partial attempt.

**The reaper needs two statements, and the second one is easy to miss.** A job whose lease expires with `attempts == max_attempts` cannot be returned to the queue: the next claim would compute `attempts + 1 > max_attempts` and violate `ck_jobs_attempts`, raising an `IntegrityError` in production. Expired-and-exhausted jobs are marked `failed`; the rest go back to `pending`. This is a case a constraint written in part 1 forces you to handle correctly, rather than letting the counter drift quietly.

**The reaper runs in every worker process, with no leader election.** Both sweeps are `LIMIT`-bounded conditional updates using `SKIP LOCKED`, so concurrent sweepers take disjoint sets and never contend. A dedicated reaper process would be another deployable, another thing to monitor, and another single point of failure — for a query that is safe to run everywhere.

**Shutdown releases leases explicitly.** When the grace period expires with work still in flight, the worker clears `worker_id` and `lease_until` on its own jobs instead of letting the lease lapse. Without this, a deploy rolling twenty workers leaves every in-flight job invisible for a full lease duration before the reaper notices — a minute of unexplained latency on every release. Releasing on the way out makes those jobs claimable immediately and turns a routine deploy into a non-event.

**And it needs the same second statement the reaper does — which I got wrong first.** The release above was written as a single unconditional requeue, and a review of the finished code caught what that means for a job interrupted on its *final* attempt: it returns to `pending` with `attempts == max_attempts`, and the next claim breaches `ck_jobs_attempts`. Since the claim selects by priority and age, that one row is picked first by every worker in the fleet, and the reaper cannot clear it because the job is `pending`, not `processing`. One badly timed deploy would stop the queue. The release now mirrors the reaper exactly: `attempts < max_attempts` goes back to the queue, and an exhausted one is failed with a `WorkerShutdown` error. It is **not** dead-lettered — nothing about the job caused it, so it stays retryable.

The interesting part is not the fix but why the constraint was documented for one path and not the other. Both are "return a job to the queue"; only one of them had been thought of as such.

**What this does not give:** delivery is at-least-once, not exactly-once. The stalled worker's *writes* are rejected, but any side effect it already performed has happened — the email went out twice. Exactly-once execution is not achievable at this layer; it requires idempotent handlers. See §5.

---

## 3. Priority Queue Implementation

**Status:** implemented — `JobRepository.claim_next`, `specs/04-claiming.md`. `priority` is bounded and `ix_jobs_claim` was migrated in part 1.

**Approach chosen:** ordering is a predicate on the claim query, not a separate data structure:

```sql
ORDER BY priority DESC, created_at ASC
FOR UPDATE SKIP LOCKED LIMIT 1
```

served by a partial index on `(priority DESC, created_at) WHERE status = 'pending'`. Redis mirrors the same ordering in a sorted set whose score encodes priority and submission time together, so the fast path preserves it, but Postgres remains the authority.

**Why:** every other candidate forces a trade-off this one avoids.

- **Redis lists with `BRPOP` across priority keys** gives blocking priority dequeue in a single command, but only in discrete levels with strict precedence — no aging is possible — and the reliable variant `BLMOVE` accepts one source key, so reliability and multi-key priority cannot be had together.
- **Redis Streams** has built-in at-least-once delivery and `XAUTOCLAIM` for recovering from dead consumers, which is tempting for §2. But streams are strictly time-ordered and support no priority at all; recovering it needs one stream per level, which gives back the clean blocking read that made streams attractive.
- **RabbitMQ** supports 255 priority levels natively, but adds a broker, and its priority applies only to messages still sitting in the queue — with a high prefetch, priority quietly stops working.

Ordering in SQL keeps the scale continuous, makes the tie-break explicit, and leaves room for aging (`ORDER BY priority + age_bonus`) as a one-line change rather than a redesign.

**Two details that are decisions, not defaults:**

`priority` is a `SMALLINT` constrained to **0–9**. An unbounded integer would let any client submit `priority = 2^31-1` and permanently pre-empt the queue — a denial of service dressed as a valid request. The bound also keeps the Redis score inside float64's exact-integer range; an unbounded scale would break FIFO ordering within a priority silently, with no error anywhere.

Under concurrent workers, ordering is **per-claim, not global**: each worker takes the highest-priority job *available* to it, not the globally highest, because `SKIP LOCKED` steps over what another worker already holds. This is inherent to any concurrent queue. Its practical consequence is a testing one — a priority-ordering test must run a single worker, or it is flaky by construction rather than by accident.

### Scheduling shares the same query, with one deliberate asymmetry

A job submitted for a future time waits in `scheduled` and is moved to `pending` by a promoter sweep. A job waiting out a retry backoff stays `pending` with a future `scheduled_at` and is held back by the claim predicate alone.

Two mechanisms for one idea looks inconsistent, and the inconsistency is considered. `status` is **client-facing semantics**: `scheduled` says "this job has not entered the queue yet", while a job serving a retry backoff is mid-lifecycle and already queued — presenting it as `scheduled` would misrepresent it to whoever is watching. Both are gated by the same `scheduled_at <= now()` comparison, so there is no duplicated timing logic, only an honest external state.

The split also protects the hot path. Had the claim query used `status IN ('pending','scheduled')` it could not use the partial index built for `status = 'pending'`, and the most frequent query in the system would have given up its index to serve a case the promoter handles for free.

---

## 4. Retry Backoff Strategy

**Status:** implemented — `ExecutionService.fail`, `app/services/backoff.py`, `specs/05-retry-and-failure.md`.

**Approach chosen:** retries reuse the scheduling mechanism rather than adding a second one. A failed attempt with retries remaining sets `status='pending'` and `scheduled_at = now() + backoff`, and the claim query's existing `scheduled_at <= now()` predicate does the rest. No delayed queue, no promoter process, no second code path to keep correct.

**Timing:**

| Attempt | Delay before it runs |
|---|---|
| 1 | immediate |
| 2 | 30 seconds |
| 3 | 2 minutes |
| after 3 | `FAILED`, permanently |

**Jitter: equal, not full.** The actual wait is `delay/2 + uniform(0, delay/2)` — 15–30 s before attempt 2, 60–120 s before attempt 3.

Jitter is not decoration. A downstream outage fails every in-flight job at roughly the same moment; without it, all of them retry at the same instant and again 90 seconds later, and the retry storm becomes a self-inflicted load spike on a service that is already unhealthy. Worse, the synchronisation persists across every subsequent round.

I originally specified **full jitter** (`uniform(0, delay)`), which spreads better and is the usual recommendation for pure contention. I changed it, and the reason is worth stating plainly: full jitter can retry after two seconds, while the requirement is written as a concrete duration — "Attempt 2: after delay, e.g. 30 seconds". A correct implementation that appears not to honour the stated timing is a bad trade when the property that actually matters — removing the synchronisation — is fully preserved by equal jitter. The cost is a narrower spread, and across three attempts that is not a meaningful loss.

The `Random` instance is injected, so tests pin the jitter and assert exact delays rather than ranges.

**`attempts` is incremented at claim time, not at failure.** This is deliberate and slightly counter-intuitive: a worker that dies without writing anything still consumes an attempt. If attempts were only counted on a recorded failure, a payload that reliably kills its worker would be reclaimed by the reaper and retried forever, taking down every worker in turn — the queue-poisoning case the assignment calls out. It also means the two ways a job can be retried — a handler that raised, and a lease that expired — consume attempts identically, so `max_attempts` bounds executions regardless of how the previous ones ended.

**A payload that no longer parses is failed immediately, not retried.** It passed validation at submission, so a parse failure at execution means the schema changed underneath a stored job; retrying would fail identically twice more and occupy a worker each time. Straight to `failed` on the first attempt. This is the cheapest defence against a poison payload — one attempt instead of three — and it is why "retry everything" is not the right default even though everything else *is* retried. Distinguishing transient from permanent failures generally needs knowledge the queue does not have, and guessing wrong in the permanent direction loses work silently; a payload that cannot be parsed is the one case where the queue does know.

**Manual retry resets `attempts` to 0**, and the reset is recorded in `job_logs`. Without it, retrying a job that had exhausted its attempts would fail again immediately and the endpoint would be useless. `POST /jobs/{id}/retry` also clears the previous error, progress and timestamps: the job is starting a fresh life, and its old one belongs in `job_logs` where it will not be mistaken for the current state.

### Dead-letter routing: a column, not a status

A failed job carries `dead_letter_reason`, which is NULL unless the failure means the job **cannot run** — as opposed to merely having failed.

**Why not a seventh status.** `failed` already is the terminal failure state. Being dead-lettered describes *why* a job failed, not a state it passes through, so expressing it as a status would have meant editing the enum, the CHECK constraint, the transition table and every status filter to say something the status already said. A nullable column and one constraint (`dead_letter_reason IS NULL OR status = 'failed'`) carry the same information without touching the state machine.

**What counts as poison, and what deliberately does not:**

| How the job ended | Reason |
|---|---|
| stored payload no longer parses | `unprocessable_payload` |
| attempts exhausted and the last failure was a timeout | `timeout_loop` |
| attempts exhausted through lease expiry — it killed its workers | `worker_crash_loop` |
| **attempts exhausted through ordinary handler exceptions** | **NULL** |

That last row is the whole design. A webhook that returned 500 three times is `failed` and is **not** poison: it did its work, the service it called was down, and retrying it once that recovers is exactly right. What belongs in the dead-letter set is work no retry can help.

**What it buys.** `POST /jobs/{id}/retry` refuses dead-lettered jobs. An operator draining an incident by retrying everything that failed must not re-arm a job that takes a worker down with it — and the queue is the only thing positioned to know the difference. Without the distinction the dead-letter queue is a second name for `failed`, and refusing on it would protect nothing.

**Trade-off:** a reviewer looking for something that reads like a *queue* will find a filter (`GET /jobs?dead_lettered=true`) and a counter in `/health`, not a separate store. That is what it is — the jobs never left the table, and moving them somewhere else would mean a second place to keep consistent for no gain in what an operator can actually do.

### Job timeout

`BaseJob.timeout_seconds` — 60 by default, 120 for reports, 300 for batches — is enforced by wrapping the handler in `asyncio.wait_for`. A wedged job now occupies a slot for a bounded time instead of until its lease expires.

It is worth admitting how this was found: the attribute had existed since the job hierarchy was written and **nothing read it**. It was dead configuration, which the repository's own standard forbids, and it survived two parts of the project because nothing fails when a knob is merely ignored.

A timeout is retryable — one slow run says nothing about the next. Exceeding the budget on *every* attempt is what earns `timeout_loop`.

---

## 5. One Thing I Would Do Differently With More Time

**The honest answer: exactly-once side effects.**

Everything above guarantees that a job's *state* is written exactly once — the conditional updates see to that. But delivery is at-least-once, so a job's *side effects* can happen twice: a worker stalled past its lease has already sent the email by the time its write is rejected. I documented this rather than hid it, but documenting a limitation is not the same as handling it.

With more time I would push idempotency down into the handlers, where it actually belongs: give each execution a deterministic operation key derived from the job id and attempt, and require handlers performing external side effects to present that key to the downstream system. That turns "we might send twice" into "the downstream deduplicates", which is the only place the problem can genuinely be solved. It changes the `BaseJob` contract, so it is not a patch — which is exactly why it needs time rather than a quick fix.

**Other things I knowingly left out**, smallest first:

- **No aging, so low priority can starve.** A sustained stream of high-priority work means priority-0 jobs never run. The fix is a one-line change to the `ORDER BY`; what it really needs is a decision about *how* fast a job should age, and that is a product question I had no basis to answer.
- **Unbounded table growth.** The partial indexes keep the hot paths fast as terminal rows accumulate, but nothing removes them. A real deployment needs a retention policy, which also resolves how long idempotency keys are kept — currently "forever", which satisfies the 24-hour requirement by accident rather than by design.
- **No backpressure.** Submission is unbounded; a client can enqueue a million jobs and nothing pushes back.
- **No authentication.** The assignment specifies none, so I built none — but the consequence is explicit: anyone who can reach the API can read any job whose id they know. Random UUIDs make ids impractical to guess, and that is obscurity, not authorization. I would rather state that plainly than let a reviewer assume it was considered.
- **SSRF protection is incomplete by construction.** The webhook URL validator rejects private ranges, the cloud metadata endpoint, and single-label names, but a hostname that resolves publicly at submission can resolve to `127.0.0.1` by the time the request is made. A queue's submit-to-execute gap is exactly where DNS rebinding is exploitable. Closing it requires resolving and pinning the address in the HTTP client at request time. The webhook here is simulated and issues no real request, so the residual risk is nil today — but the moment it becomes real, this is the first thing to fix.

---

## 6. Worker Concurrency Model

**Approach chosen:** each worker process runs N independent **slots** — separate coroutines, each with its own claim → execute → write loop — and several such processes run side by side. `docker-compose.yml` runs 2 processes × 2 slots.

**Why slots and not one job per process:** the service is async end to end and a job spends nearly all its wall time awaiting I/O, so a process handling one job at a time leaves an event loop idle. Slots are also what makes concurrency testable in-process: the exactly-once and load tests run four real claim loops against a real database without spawning subprocesses, which is the difference between a test that exercises the mechanism and one that mocks it.

**Why independent loops and not a pre-fetched batch:** a slot claims only when it is free, so the number of jobs held in `processing` never exceeds the number actually being worked on. A worker that pre-fetched would hold leases on jobs it has not started, and every one of those would have to survive a crash — turning a simple invariant into a batch-recovery problem for a throughput gain the workload does not need.

**Trade-off:** slots share a process, so they share its failure. One `SIGKILL` loses N jobs rather than one. That is acceptable precisely because the lease mechanism treats losing N jobs the same as losing one — they all come back — and it is why the recovery path, not the claim path, is where the design effort went.

---

## 7. Observability Decisions

**Redis being down is not a `503`.** The database is a hard dependency; Redis is not. Jobs continue to flow through the Postgres fallback claim when Redis is unreachable, so returning `503` would pull a working service out of a load balancer and turn a latency problem into an outage.

**"No workers" and "cannot see the workers" are reported differently.** Worker liveness comes from Redis keys with a TTL. When Redis is unreachable the field is `null`, never `count: 0`. They are different incidents with different responses, and reporting the second when the first is true sends an operator to restart healthy workers in the middle of a Redis outage.

**`oldest_pending_seconds` is the field worth adding.** Queue depth alone cannot tell load apart from failure; depth and age together can. High depth with low age is a busy system keeping up. Low depth with high age is a stuck one. It costs an index-only scan over the partial claim index, and it is the single number that turns "the queue looks wrong" into a diagnosis.

**Recovery events are logged at `warning`, not `info`.** A job being reaped or a lease being lost is the system working correctly, so `error` would be wrong — but a healthy deployment produces almost none of them, and a sudden run means workers are dying, hanging, or the lease is too short for the work. Logging them at `info` would bury exactly the signal that matters.

**A warning that fires constantly is worse than no warning.** That principle was violated in the first version of the Redis dispatch and the log stream said so plainly. `next_hint` blocks on `BZPOPMIN` for the poll interval, and redis-py's default socket read timeout is 5 seconds — the same value. The socket always won the race, so every idle poll raised instead of returning empty, and each one logged `dispatch.next_hint_failed`: the identical line emitted when Redis is genuinely unreachable, twice every five seconds, against a healthy server. Nothing broke, because the fallback claim carries the work — which is exactly why 464 passing tests and a Docker smoke test never noticed. The client's read budget is now derived from the longest block it will carry rather than inherited from a library default, and the relationship is pinned by a test instead of by two numbers that happened to be equal.

**A slot that dies must not do so quietly.** `Slot.run_forever` let exceptions escape, and nothing awaits a slot task until shutdown, so any transient database error retired that slot permanently while the process stayed up and kept announcing itself as live. The loop now reports, waits one poll interval, and carries on; a job already claimed is recovered by the reaper on the ordinary path. The maintenance sweep had this guard from the start — the loop that actually does the work did not.
