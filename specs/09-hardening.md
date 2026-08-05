# Spec 09 — Manual Retry, Job Timeout and Dead-Letter Routing

**Status:** Draft — awaiting approval
**Depends on:** `specs/05-retry-and-failure.md`, `specs/06-crash-recovery.md`
**Related decisions:** DECISIONS.md §4, §5

## 1. Scope

Three things that share one theme: what happens to work that will not succeed on its own. A human putting a failed job back in the queue, a job that runs forever, and telling apart a job that failed from a job that *cannot run*.

## 2. `POST /jobs/{job_id}/retry`

The last endpoint the assignment lists. It takes a job that failed permanently and puts it back in the queue.

```sql
UPDATE jobs
SET status='pending', attempts=0, error=NULL, scheduled_at=NULL,
    started_at=NULL, completed_at=NULL, worker_id=NULL, lease_until=NULL,
    progress=0
WHERE id = :id AND status = 'failed' AND dead_letter_reason IS NULL
RETURNING *;
```

Same shape as cancellation (spec 02 §3): the conditional `UPDATE` is the authority, and the row is read afterwards only to explain why nothing matched. Reading first would reintroduce a race with the worker.

| Response | Condition |
|---|---|
| `200` | was `failed` and not dead-lettered; now `pending` |
| `409` | exists but cannot be retried — wrong status, or dead-lettered |
| `404` | no such job |

**`attempts` resets to 0.** Without it the job would be claimed once, find its attempts already spent, and fail again immediately — an endpoint that does nothing. Decided in DECISIONS.md §4 before this was built.

**`error`, `progress` and the three timestamps are cleared.** The job is starting a fresh life; its previous one is in `job_logs`, which is where history belongs and where it is not mistaken for the current state. `started_at` and `completed_at` are cleared together so `ck_jobs_completed_after_started` cannot be violated.

**A dead-lettered job is refused.** This is the protection the dead-letter classification exists to provide — see §4.

Announced to Redis after the commit, exactly as submission is (spec 07 §5).

## 3. Job timeout

`BaseJob.timeout_seconds` has existed since spec 02 — 60 by default, 120 for reports, 300 for batches — and until now **nothing read it.** That made it dead configuration, which the repository's own standard forbids. This enforces it.

The slot wraps the handler:

```python
result = await asyncio.wait_for(run_task, timeout=type(handler).timeout_seconds)
```

A job that exceeds its budget is failed with `JobTimeoutError` and the handler is cancelled, so a wedged job occupies a slot for a bounded time rather than until its lease expires.

### The interaction with the heartbeat

Two different things cancel the same task, and they must not be confused:

| Cause | What the slot sees | Meaning |
|---|---|---|
| `wait_for` budget elapsed | `TimeoutError` | the job ran too long — record a failure |
| heartbeat lost the lease | `CancelledError`, `_lease_lost` set | the job belongs to someone else — write nothing |
| worker shutting down | `CancelledError`, `_lease_lost` clear | re-raise; the worker releases the lease |

The `_lease_lost` flag introduced in spec 06 §3 already separates the second from the third. The timeout branch must be added without swallowing either.

**A timeout is retryable.** A job that ran long once may be fine next time — a slow downstream, a large batch, a busy host. What is *not* fine is a job that exceeds its budget on every attempt, and that is handled by §4 rather than by refusing the first retry.

⚠️ On Python 3.11+ `asyncio.TimeoutError` and the builtin `TimeoutError` are the same class, so a handler that raises `TimeoutError` itself is indistinguishable from one the worker timed out. Treating both as a timeout is the correct outcome anyway.

## 4. Dead-letter routing

### A column, not a status

`dead_letter_reason TEXT NULL`, with `CHECK (dead_letter_reason IS NULL OR status = 'failed')`.

**There is no seventh status.** `failed` remains the single terminal failure state. Being dead-lettered is a *classification of a failure*, not a state a job moves through — so the state machine, the enum, the transition table and every status filter stay exactly as they were. A new status would have meant editing all of them to express something the job's status already says.

The constraint keeps the two consistent: a dead letter is always a failed job, enforced by the database rather than by remembering.

### What is poison, and what is merely broken

| How the job ended | `dead_letter_reason` |
|---|---|
| stored payload no longer parses, or its type is not registered | `unprocessable_payload` |
| attempts exhausted and the last failure was a timeout | `timeout_loop` |
| attempts exhausted through lease expiry — it killed its workers | `worker_crash_loop` |
| **attempts exhausted through ordinary handler exceptions** | **NULL** |

That last row carries the whole idea. A webhook that returned 500 three times is `failed`, and it is **not** poison: it did its job, the thing it was calling was down, and retrying it once that recovers is exactly the right move. What belongs in the dead-letter set is work that *cannot* run — a payload that will not parse, a job that kills whatever picks it up, a job that has never once finished inside its budget.

Without that distinction the dead-letter queue is just a second name for `failed`, and the endpoint that consults it protects nothing.

### What it is for

Refusing `POST /jobs/{id}/retry`. An operator draining an incident by retrying everything that failed must not re-arm a job that takes a worker down with it, and the queue is the only thing positioned to know the difference.

### Exposure

- `dead_letter_reason` on `JobResponse` — the reason, so the operator knows what to fix
- `GET /jobs?dead_lettered=true` — the queue as a list
- `queue.dead_lettered` in `/health` — a number that should be zero and is worth alerting on
- `job.dead_lettered` log event at `error`

**No dedicated index.** The set is small by construction, and a query filtered on it narrows through `ix_jobs_status_created` first. Another index would cost write throughput on the claim path — the same reasoning already recorded for `job_type` in spec 01 §4.

## 5. Acceptance criteria

- Retrying a `failed` job returns `200`, leaves it `pending` with `attempts = 0` and no `error`, and a worker then runs it to completion.
- Retrying a `pending`, `processing`, `completed` or `cancelled` job returns `409` and changes nothing.
- Retrying a dead-lettered job returns `409`.
- Retrying an unknown id returns `404`.
- A handler that exceeds its budget is failed rather than left running, and the slot goes on to claim another job.
- A timeout with attempts remaining is retried normally and is **not** dead-lettered.
- Attempts exhausted by timeouts sets `timeout_loop`; by lease expiry sets `worker_crash_loop`; by an unparseable payload sets `unprocessable_payload` on the first attempt.
- **Attempts exhausted by ordinary handler exceptions leaves `dead_letter_reason` NULL.**
- The database rejects a `dead_letter_reason` on any job that is not `failed`.
- `GET /jobs?dead_lettered=true` returns exactly the dead-lettered jobs, and `/health` counts them.
