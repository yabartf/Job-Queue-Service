# Spec 06 — Crash Recovery and Graceful Shutdown

**Status:** Accepted — implemented
**Depends on:** `specs/03-worker-runtime.md`, `specs/04-claiming.md`
**Related decisions:** DECISIONS.md §2

## 1. Scope

Keeping a job from being stranded when the worker holding it stops — whether it died, hung, or was asked to stop politely. The claim guarantees a job is picked up once (spec 04); this document is what happens when the worker that picked it up does not finish.

## 2. Why a lease

A dropped connection is not a usable liveness signal. A worker can be alive but unreachable, or stalled in a long garbage-collection pause with its connection intact, or dead with its connection lingering. Any scheme keyed on connection state gets at least one of those wrong.

A lease inverts it: liveness is something the worker must **actively assert**, and every failure mode collapses into one observable — the lease stopped being extended. There is nothing to detect and no distinction to draw between kinds of death.

The schema enforces this rather than trusting the code. `ck_jobs_processing_has_lease` makes it impossible to write `status = 'processing'` without also writing `worker_id` and `lease_until`, which is exactly the omission that would strand a job forever with nothing able to notice.

## 3. Lease and heartbeat

The claim sets `lease_until = now() + 60s`. While the handler runs, a background task extends it every 20 seconds:

```sql
UPDATE jobs SET lease_until = now() + :lease
WHERE id = :id AND worker_id = :worker AND attempts = :attempts
  AND status = 'processing';
```

The interval is a third of the lease, so a single missed extension — a slow query, a brief network stall — does not expire it.

### Losing the lease cancels the work

If the extension matches zero rows, this worker no longer owns the job: the reaper released it and someone else has it. The slot **cancels the handler task** rather than letting it run to completion.

This is worth doing even though the result would be rejected anyway. Abandoning immediately bounds the window in which two workers are executing the same job to roughly one heartbeat interval, instead of however long the handler had left. It converts "the job might run twice, in full" into "the job might run twice, and the second one stops early" — which is the difference between two emails and one email plus a partial attempt.

`ctx.heartbeat()` called by a handler does the same extension, rate-limited to once per interval. It exists because a handler that occupies the event loop prevents the background task from running at all; an explicit `await` is both a lease extension and a yield point.

## 4. The reaper

Runs in the maintenance task of every worker process, on an interval. It needs **two statements**, and the second one is easy to miss.

**Expired and out of attempts → terminal failure.** A job with `attempts = max_attempts` cannot be returned to the queue: the next claim would compute `attempts + 1 > max_attempts` and violate `ck_jobs_attempts`, raising an `IntegrityError` in production. It is marked `failed` with an error recording that its lease expired.

**Expired with attempts remaining → back to the queue.**

```sql
UPDATE jobs SET status='pending', worker_id=NULL, lease_until=NULL
WHERE id IN (
  SELECT id FROM jobs
  WHERE status='processing' AND lease_until < now() AND attempts < max_attempts
  ORDER BY lease_until
  FOR UPDATE SKIP LOCKED
  LIMIT :batch
)
RETURNING id;
```

Released ids are **not** announced to Redis. They are picked up by the fallback claim within one poll interval, and re-announcing them would mean carrying each job's priority and creation time out of a batch `UPDATE ... RETURNING` purely to rebuild a dispatch score — for a few seconds on jobs that are already late. See spec 07 §5.

Both statements are bounded by `LIMIT` and use `SKIP LOCKED`, so several worker processes sweeping at once is harmless: each takes a disjoint set and no two contend.

**A live lease is never touched.** The `lease_until < now()` predicate is evaluated by the database against its own clock in the same statement that performs the update, so there is no interval during which a decision to reap could be based on a stale read.

## 5. What happens when a worker dies mid-job

1. It stops extending `lease_until`.
2. Within one lease duration plus one sweep interval, the reaper returns the job to `pending` — or fails it, if its attempts were exhausted.
3. Another worker claims it. `attempts` was already incremented by the original claim, so the dead attempt was counted.
4. The job runs again.

The case that actually matters is the one that looks identical from outside but is not: **a worker that is merely slow.** It is reclaimed while still running, so for a period two workers hold the same job. Every write it subsequently attempts carries `worker_id` and `attempts` (spec 04 §5), so it matches zero rows, and the worker discards its own result rather than overwriting the legitimate one.

**What this does not give:** delivery is at-least-once. Writes from the displaced worker are rejected, but side effects it already performed have happened. Exactly-once execution requires idempotent handlers and cannot be provided at this layer — DECISIONS.md §5.

## 6. Graceful shutdown

On `SIGTERM` or `SIGINT`:

1. Set the stop event. **Slots stop claiming immediately** — no new work is taken.
2. In-flight jobs continue, with heartbeats still extending their leases.
3. Wait up to `SHUTDOWN_GRACE_SECONDS` for slots to finish.
4. Stop the maintenance task and close the dispatch client.
5. Exit.

A second signal during shutdown skips straight to step 6.

### When the grace period is not enough

Any job still running is cancelled, and **its lease is explicitly released**:

```sql
UPDATE jobs SET status='pending', worker_id=NULL, lease_until=NULL
WHERE id=:id AND worker_id=:worker AND attempts=:attempts AND status='processing';
```

Doing this rather than letting the lease lapse matters operationally. A deploy that rolls twenty workers would otherwise leave every in-flight job invisible for a full lease duration before the reaper notices — a minute of unexplained latency on every release. Releasing on the way out makes the job claimable immediately, and turns a routine deploy into a non-event.

The released attempt is still consumed, which is correct: the job did start, and a job that repeatedly gets caught by shutdowns should not retry indefinitely.

The container's `stop_grace_period` must exceed `SHUTDOWN_GRACE_SECONDS`, or Docker sends `SIGKILL` mid-cleanup and the explicit release never runs. `docker-compose.yml` sets 40 s against a 30 s grace.

## 7. Acceptance criteria

- A claimed job whose lease is expired by hand is returned to `pending` by the reaper.
- A job with a live lease is left alone by the reaper, including when its lease is seconds from expiry.
- An expired job with no attempts remaining becomes `failed`, not `pending`, and no subsequent claim raises `IntegrityError`.
- After a reclaim, the previous holder's completion write matches zero rows and its result is discarded.
- A heartbeat that matches zero rows cancels the running handler.
- `SIGTERM` during execution: the running job completes, no new job is claimed, the process exits within the grace period.
- A job still running when the grace period expires is left `pending` with `worker_id` and `lease_until` cleared, and is claimable at once.
- `SIGKILL` on a worker leaves its job recoverable by the reaper with no manual intervention.
