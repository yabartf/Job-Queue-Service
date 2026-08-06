# Spec 04 — Claiming: Exactly-Once Pickup, Priority and Scheduling

**Status:** Accepted — implemented
**Depends on:** `specs/01-data-model.md`, `specs/03-worker-runtime.md`
**Related decisions:** DECISIONS.md §1, §3

## 1. Scope

Three requirements the assignment lists separately — a job is picked up by exactly one worker, higher priority runs first, and scheduled jobs do not run early — are **one SQL statement**. Specifying them apart would invite three mechanisms where one is needed, so they are specified together.

Also here: the ownership token that every write after the claim depends on, because it is established by the claim.

## 2. The claim

```sql
UPDATE jobs
SET status     = 'processing',
    worker_id  = :worker,
    lease_until= now() + :lease,
    started_at = now(),
    attempts   = attempts + 1
WHERE id = (
  SELECT id FROM jobs
  WHERE status = 'pending'
    AND (scheduled_at IS NULL OR scheduled_at <= now())
  ORDER BY priority DESC, created_at ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING *;
```

One statement, therefore one implicit transaction: the row is selected, locked, and mutated with no window in between. There is no application-level lock, no advisory lock, and no `BEGIN` to get wrong.

`started_at` is set on every claim and means **when the current attempt started**, not when the job first ran. That is the more useful value when diagnosing a job that is taking too long; the original submission time is `created_at`.

## 3. Exactly-once pickup

`FOR UPDATE` locks the selected row until the transaction ends. Without `SKIP LOCKED`, a second worker reaching the same row *waits*; when the lock releases it re-evaluates the `WHERE` clause under `READ COMMITTED`, sees `status = 'processing'`, and returns nothing. Correct, but every worker queues behind the same row and the queue degrades to serial — the contention the performance criterion penalises.

`SKIP LOCKED` makes a worker step over a locked row and take the next candidate instead. N workers claim N distinct jobs, with no waiting and no coordination.

The guarantee does not rest on timing. Two workers cannot hold the same job because the row lock is held for the whole statement, and any worker arriving second finds a row whose status no longer matches its predicate. **This is a property of the engine, not of the order in which our code happens to run.**

What `SKIP LOCKED` does *not* do is protect a job whose worker dies after claiming it: the lock disappears with the connection, the row stays `processing`, and nothing will claim it because it is no longer `pending`. That is a separate mechanism — spec 06. Conflating the two is the most common mistake in this design.

## 4. Priority and scheduling in the same predicate

**Priority** is `ORDER BY priority DESC, created_at ASC` — highest first, oldest first within a level — served by `ix_jobs_claim` on `(priority DESC, created_at) WHERE status = 'pending'`.

**Scheduling** is the `scheduled_at IS NULL OR scheduled_at <= now()` filter. No separate delayed queue and no promoter is needed for a job that is already `pending`.

### Two routes to a future execution time, deliberately

| Case | Status while waiting | What makes it runnable |
|---|---|---|
| Submitted with a future `scheduled_at` | `scheduled` | the **promoter** flips it to `pending` when due |
| Failed, awaiting a retry | `pending` with a future `scheduled_at` | the claim predicate alone |

They differ because `status` is **client-facing semantics**. `scheduled` says "this job has not entered the queue yet"; a job waiting out a retry backoff is mid-lifecycle and already queued, and showing it as `scheduled` would misrepresent it. Both are gated by the same `scheduled_at <= now()` comparison, so there is no duplicated timing logic — only an honest external state.

The split also keeps the hot path fast. Had the claim query used `status IN ('pending','scheduled')` it could not use the partial index built for `status = 'pending'`, and the most frequent query in the system would have lost its index to serve a case the promoter handles for free.

### The promoter

```sql
UPDATE jobs SET status = 'pending'
WHERE id IN (
  SELECT id FROM jobs
  WHERE status = 'scheduled' AND scheduled_at <= now()
  ORDER BY scheduled_at
  FOR UPDATE SKIP LOCKED
  LIMIT :batch
)
RETURNING id;
```

Runs in the maintenance task (spec 03 §6) and uses `ix_jobs_scheduled`. `SKIP LOCKED` again, so N workers sweeping concurrently is harmless rather than contended.

Promoted jobs are **not** announced to Redis; they reach a worker through the fallback claim within one poll interval. See spec 07 §5 for why that trade goes this way.

### Known consequences

**Ordering is per-claim, not global.** With N workers, each takes the highest-priority job *available to it* — `SKIP LOCKED` steps over what another worker already holds. This is inherent to any concurrent queue and is not a defect. Its practical consequence is a testing rule: **a priority-ordering test must run a single worker**, or it is flaky by construction rather than by accident.

**Low priority can starve, and aging is not a one-line fix.** A sustained stream of high-priority work means priority-0 jobs never run. Not implemented — DECISIONS.md §5 — and the reason it is not is worth stating precisely, because the obvious fix looks free and is not.

Writing the age into the sort key does work as a diff:

```sql
ORDER BY priority + LEAST(EXTRACT(epoch FROM now() - created_at) / 600, 5) DESC, created_at
```

and it costs the index. Measured with `EXPLAIN` against a real database:

| Sort key | Plan |
|---|---|
| `priority DESC, created_at` | `Index Scan using ix_jobs_claim` — stops at the first eligible row |
| the expression above | `Sort` over the whole eligible set, feeding from `ix_jobs_status_created` |

A sort key that depends on `now()` is not a stored value, so no index can supply it in order. Every claim, from every worker, would read all eligible rows, evaluate the expression per row and sort — O(n log n) where it is now O(log n), on the most frequent query in the system and precisely under the large backlog where dequeue efficiency is being judged. `W2-17` is what fails first: it asserts the plan uses `ix_jobs_claim`.

Three approaches keep the index, and each pays somewhere else:

1. **A promotion sweep** raises `priority` on old rows by `UPDATE` in the maintenance pass. The index is untouched; the cost is write amplification and that the `priority` a client reads back is no longer the one it submitted.
2. **Lottery** — every Nth claim ignores priority and takes the oldest eligible job through `ix_jobs_created`. Starvation gets a ceiling of N claims rather than a gradient, for a few lines in `claim_next`.
3. **A stored effective priority** recomputed by the sweep, so the ordering stays over a materialised column.

Choosing between them needs a decision about how fast a job should age, which is a product question this project has no basis to answer. What it does not need is the impression that the answer is one line.

**Retry backoffs sit inside the claim index.** A `pending` job with a future `scheduled_at` is in `ix_jobs_claim` but not yet eligible, so the index scan walks over it. With a large backlog of waiting retries this costs a few extra index entries per claim. It cannot be indexed away — `now()` is not immutable, so no partial index can express "due" — and at this scale it is not worth a second mechanism.

## 5. Ownership: `attempts` is the fencing token

Every write after the claim carries the same predicate:

```sql
WHERE id = :id AND worker_id = :worker AND attempts = :claimed_attempts
  AND status = 'processing'
```

**`worker_id` alone is not sufficient**, and the reason is specific to running several slots inside one process. Suppose `worker_id` identified the process:

1. Slot A claims job J as `w1` and stalls.
2. The lease expires; the reaper returns J to `pending`.
3. Slot B — same process, same `w1` — claims J.
4. Slot A wakes and writes its result.

Step 4's predicate matches, because `worker_id` is still `w1`. Slot A overwrites the result of the worker that legitimately took over. Silent data loss, reachable only under a specific interleaving, and invisible to any single-worker test.

`attempts` closes it. It increments on every claim, so a value observed at claim time can never describe a later claim of the same job. Slot A remembers `attempts = 1`; after the reclaim the row holds `2`; A's write matches nothing and A discards its own result.

This gives a full fencing token **with no new column and no migration** — `attempts` already exists and is already incremented at exactly the right moment. `worker_id` is additionally made unique per slot (`{host}-{pid}-{slot}`), which is redundant with the above but costs nothing and makes logs directly attributable.

**A write that matches zero rows is not an error.** It means ownership was lost, and the correct response is to discard the result and continue — never to retry the write, and never to raise.

## 6. The hint path

When Redis supplies a job id (spec 07), the worker claims that specific job:

```sql
UPDATE jobs SET status='processing', worker_id=:worker, lease_until=now() + :lease,
                started_at=now(), attempts=attempts+1
WHERE id = :id AND status = 'pending'
  AND (scheduled_at IS NULL OR scheduled_at <= now())
RETURNING *;
```

Zero rows means the hint was stale — cancelled, already claimed, or not yet due — and the worker drops it and continues. Stale hints are expected steady-state behaviour, not an error condition: cancellation deliberately leaves entries in the Redis set to be cleaned up exactly this way (spec 02 §3).

A slot tries the hint path first and falls back to §2 when Redis yields nothing. Both end in a conditional `UPDATE` against Postgres, so the guarantee is identical either way.

## 7. Invariants

Extends the list in `specs/00-architecture.md` §6.

7. **A worker holds a job only while `(worker_id, attempts)` still match what its claim returned.**
8. **`attempts` is incremented only by a claim**, never by a failure or a retry decision.
9. **Zero rows from any ownership-predicated write means ownership was lost** — discard, do not raise, do not retry.

## 8. Acceptance criteria

- Ten concurrent workers against a single pending job: exactly one claim succeeds, nine return `None`.
- A seeded backlog drained by four slots: every job executed exactly once, none left in `processing`.
- With one worker, jobs are claimed in strict `priority DESC, created_at ASC` order.
- A job with a future `scheduled_at` is never claimed before its time, in either status.
- The promoter moves a due `scheduled` job to `pending`; a not-yet-due one is untouched.
- After a reclaim, a write from the previous holder matches zero rows and its result is discarded.
- `EXPLAIN` shows the claim query using `ix_jobs_claim`.
