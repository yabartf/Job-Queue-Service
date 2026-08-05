# Spec 05 — Retry and Failure

**Status:** Accepted — implemented
**Depends on:** `specs/03-worker-runtime.md`, `specs/04-claiming.md`
**Related decisions:** DECISIONS.md §4

## 1. Scope

What happens when a handler raises: how the error is recorded, when the job is retried, how long it waits, and when it stops.

## 2. Outcome of a failed attempt

```
handler raises
   ├── attempts < max_attempts  →  status='pending', scheduled_at = now() + backoff
   └── attempts >= max_attempts →  status='failed',  completed_at = now()
```

Both are ownership-predicated writes (spec 04 §5). A worker that has lost the job records nothing.

Retries reuse the scheduling mechanism rather than introducing a second one: a job awaiting a retry is `pending` with a future `scheduled_at`, and the claim predicate already refuses to pick it up early. No delayed queue, no timer, no second code path that can disagree with the first.

## 3. Backoff

```
base = min(30 × 4^(failed_attempt - 1), 3600)
```

| Attempt that just failed | Base wait |
|---|---|
| 1 | 30 s |
| 2 | 120 s |
| 3 | 480 s — only reachable with `max_attempts > 3` |
| ≥ 5 | capped at 1 hour |

With the default `max_attempts = 3` a job runs at most three times and only the first two rows apply, matching the assignment.

The growth factor is **4, not the more usual 2**, so that the two delays the assignment specifies — 30 seconds then 2 minutes — come out of a single formula. A factor of 2 would give 30 s and 60 s, which would mean hard-coding a two-entry lookup table for the specified attempts and bolting an exponential onto the tail for `max_attempts` up to 10. One formula that happens to hit the required values is better than a table that agrees with them by construction.

### Jitter

The wait is **equal jitter**: `delay/2 + uniform(0, delay/2)`. Attempt 2 therefore runs 15–30 s after attempt 1, and attempt 3 runs 60–120 s after attempt 2.

Jitter is not decoration. A downstream outage fails every in-flight job at roughly the same moment; without jitter all of them retry at the same instant, and again 90 seconds later. The retry storm becomes a self-inflicted load spike on a service that is already unhealthy, and the synchronisation persists across every subsequent round.

**Why equal jitter and not full jitter.** Full jitter — `uniform(0, delay)` — spreads better and is the usual recommendation for pure contention. It was rejected here because it can retry after two seconds, and the requirement is stated as a concrete duration ("Attempt 2: after delay, e.g. 30 seconds"). Equal jitter keeps the delay recognisably the specified one while still removing the synchronisation, which is the property that actually matters. The trade-off is a narrower spread, and at three attempts that is not a meaningful loss.

The `Random` instance is injected, so tests pin the jitter and assert exact delays rather than ranges.

## 4. `attempts`

Incremented **by the claim**, never by the failure (spec 04 §7, invariant 8). A job on its first execution has `attempts = 1`.

The consequence is deliberate: a worker that dies without recording anything still consumed an attempt. If attempts were only counted on a recorded failure, a payload that reliably kills its worker would be reclaimed by the reaper and retried forever, taking down every worker in turn. That is the queue-poisoning case the assignment names, and counting at claim time is what bounds it.

It also means the two ways a job can be retried — a handler that raised, and a lease that expired — consume attempts identically, so `max_attempts` is a true bound on executions regardless of how the previous ones ended.

## 5. What is not retried

**A payload that no longer parses.** It passed validation at submission, so a parse failure at execution means the schema changed underneath a stored job. Retrying would fail identically twice more and occupy a worker each time. The job goes straight to `failed` with an error naming the field, on its first attempt.

**A cancelled job.** Cancellation is only possible before a claim (spec 01 §3), so this cannot arise after execution begins — but the ownership predicate would reject the write anyway.

Everything else — anything a handler raises — is retryable. Distinguishing "transient" from "permanent" failures generally requires knowledge the queue does not have, and guessing wrong in the permanent direction loses work silently. Attempt limits are the backstop instead.

## 6. Error shape

```json
{"type": "JobExecutionError",
 "message": "Webhook endpoint returned an error response",
 "attempt": 2}
```

Stored in `jobs.error`, replaced on each failure; the full history accumulates in `job_logs`. **Never contains a traceback**, and handler messages must not embed payload contents (spec 02 §6) — this value is returned through the API. The message is truncated at 500 characters, because it is client-visible and an exception can carry an arbitrarily long one.

The exception type name is recorded as a string rather than the class, so failures can be grouped and counted without the API layer needing to know what exception types exist.

**No timestamp inside the error.** `jobs.updated_at` and `job_logs.created_at` already record when the failure happened, and both are written by the database. Embedding a time taken from an application clock beside them would give one event two answers that disagree by whatever the skew between the machines is.

### One clock, and it is the database's

The retry deadline is written as `now() + interval` **in SQL**, not as an instant computed in Python. The claim predicate compares `scheduled_at` against the database's `now()`, so the value it is compared with has to be produced by the same clock — otherwise every retry fires early or late by exactly the skew between the worker host and the database, and nothing in the system would report it. The same rule governs `lease_until` (spec 06 §3).

## 7. Manual retry

`POST /jobs/{id}/retry` is part 3. Its decision is already fixed: **it resets `attempts` to 0** and records the reset in `job_logs`. Without the reset, retrying a job that had exhausted its attempts would fail again immediately and the endpoint would be useless. `FAILED → PENDING` is already in the state machine (spec 01 §3).

## 8. Acceptance criteria

- A handler that raises with attempts remaining leaves the job `pending` with `scheduled_at` in the future and `error` populated.
- The job is not claimable before that time, and is claimable after it.
- A third failure leaves the job `failed` with `completed_at` set and no `result`.
- With the jitter source pinned, the delays are exactly 30 s and 120 s; with it at its extremes, 15 s and 60 s.
- A stored payload that no longer parses is `failed` after one attempt.
- A lease that expires consumes an attempt, and a job whose attempts are exhausted when its lease expires becomes `failed` rather than returning to the queue.
- No error written to `jobs.error` or returned by the API contains the substring `Traceback`.
