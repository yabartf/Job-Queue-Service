# Spec 03 — Worker Runtime

**Status:** Accepted — implemented
**Depends on:** `specs/00-architecture.md`, `specs/02-api-core.md`
**Related:** claiming in spec 04, failure handling in spec 05, recovery in spec 06

## 1. Scope

The worker process: how it is structured, how it executes a job, and what it does with the outcome. Claiming, retry timing, lease management and Redis are each specified separately; this document is the runtime that calls into them.

## 2. Process model

One `python -m app.worker` process runs **N independent slots**, each a coroutine executing its own claim → execute → write loop. Slots share the process's engine and dispatch client and nothing else — no shared job state, no coordination between them.

```
Worker process
├── slot 0 ──┐
├── slot 1 ──┼── each: claim → execute → write result → repeat
├── ...      ┘
└── maintenance ── reaper + promoter, on an interval
```

Concurrency therefore has two dimensions: `WORKER_CONCURRENCY` slots per process, and however many processes are run. `docker-compose.yml` runs 2 processes × 2 slots.

**Why slots rather than one job per process:** the service is async throughout and a job spends nearly all its time awaiting I/O, so a process running one job at a time leaves an event loop idle. Slots are also what makes the exactly-once behaviour testable in-process, without spawning subprocesses in the test suite.

**Why independent loops rather than a shared queue of claimed jobs:** a slot that claims only when it is free means the number of jobs held in `processing` never exceeds the number of slots actually working on them. A worker that pre-fetched a batch would hold leases on jobs it has not started, and every one of those would have to survive a crash — turning a simple invariant into a batch-recovery problem.

### Worker identity

`worker_id = "{hostname}-{pid}-{slot}"`, unique per slot rather than per process. Per-process identity would be a correctness bug, not a cosmetic one — see spec 04 §5.

## 3. The slot loop

```python
async def run_once(self) -> bool:
    """Claim and run at most one job. Returns whether work was done."""
    job = await self.claim()  # hint path, then fallback (spec 04)
    if job is None:
        return False
    await self.execute(job)
    return True


async def run_forever(self, stop: asyncio.Event) -> None:
    hint = None
    while not stop.is_set():
        try:
            claimed = await self.run_once(hint)
        except Exception:
            log.exception("slot.cycle_failed")
            hint = None
            if await sleep_unless_stopped(stop, poll_interval):
                return
            continue
        if claimed:
            hint = None
            continue
        hint = await self.dispatch.next_hint(timeout=poll_interval)  # blocks
```

A hint arrives as an argument to the next `run_once` rather than opening a second execution path: there is one way a job gets run, and the hint only changes which row is tried first.

`run_once` exists as a separate method for one reason: **almost every test drives it directly.** A loop that can only be started and stopped forces every test to reason about timing; a loop with a single-step entry point does not. `run_forever` adds the wait — and the only place in the worker that decides a failure is survivable.

When `run_once` returns `True` the loop immediately tries again rather than waiting. A backlog is drained continuously; the wait happens only when the queue is genuinely empty.

### Why the loop cannot let an exception out

Nothing awaits a slot task until shutdown: `Worker.run` creates them and then parks on the stop event (§7). An exception escaping `run_once` would therefore end that slot **permanently and in silence** — the process stays up, the liveness key keeps being refreshed, `/health` keeps reporting a live worker, and it never claims again. With `WORKER_CONCURRENCY=1` the process becomes a convincing zombie; the only symptom is `oldest_pending_seconds` climbing on an endpoint nobody is watching at 3am.

The causes are ordinary and transient: a dropped connection, a failover, a schema not yet migrated when the container started. So the loop reports and waits out one poll interval rather than retrying instantly — a dependency that is down stays down for longer than one iteration, and an immediate retry would turn an outage into a hot loop against the thing already struggling.

A job that was already claimed when the failure hit stays `processing` and is recovered by the reaper on the ordinary path (spec 06 §4). Nothing here tries to be cleverer than that.

The maintenance sweep has had the same guard since it was written (§7). The slot loop is where it matters more.

## 4. Executing a job

1. Look up the handler class for `job.job_type` (`app/jobs/registry.py`).
2. Parse `job.payload` against the handler's `Payload` model.
3. Construct the handler with a `DbJobContext` bound to this job and attempt.
4. Start the heartbeat task (spec 06).
5. `await handler.run()` as a cancellable task.
6. Validate the returned object against the handler's `Result` model, then write the outcome.

**A payload that fails to parse here is a terminal failure, not a retryable one.** It passed validation at submission, so a parse failure now means the schema changed under a stored job. Retrying would fail identically and burn attempts against a worker each time; the job is marked `failed` immediately with an error naming the schema mismatch. This is the first line of defence against a poison payload, and it costs one attempt rather than three.

Each execution runs in its own session and transaction. The claim commits before the handler starts, so a job is durably `processing` before any work happens — otherwise a crash mid-execution would leave it `pending` with a lease that was never persisted, and it would be silently re-run with no attempt consumed.

## 5. `DbJobContext`

The concrete `JobContext` (spec 02 §4) that handlers already depend on. Nothing in `app/jobs/` changes to accommodate it — that was the point of the protocol.

| Method | Behaviour |
|---|---|
| `report_progress(pct)` | conditional `UPDATE` of `progress`; ignored if ownership is lost |
| `log(level, message, **fields)` | writes a `job_logs` row and emits a structured log line, through the same helper the service uses |
| `heartbeat()` | extends the lease, rate-limited to once per heartbeat interval |

Every write goes through the ownership predicate (spec 04 §5). A context whose job has been reclaimed silently stops writing rather than raising — the handler is about to be cancelled anyway, and an exception surfacing from `report_progress` would be reported as a job failure, which it is not.

**`heartbeat()` is deliberately redundant with the automatic heartbeat task.** The automatic one is the mechanism; the explicit one exists because a handler that occupies the event loop for a long stretch prevents the timer from running at all, and `await ctx.heartbeat()` is both a lease extension and a yield point. Rate-limiting stops a chatty handler from turning it into write amplification.

## 6. Maintenance task

One additional coroutine per process runs the reaper and the promoter on an interval (spec 06 §4, spec 04 §4). It is deliberately part of the worker rather than a separate service: it must run wherever workers run, and a dedicated process would be another thing to deploy and another thing to notice had died.

Running it in every worker means N processes each sweep. That is harmless — both sweeps are conditional updates that simply match nothing when another process got there first — and it removes the need for leader election.

## 7. Configuration

| Setting | Default | Meaning |
|---|---|---|
| `WORKER_CONCURRENCY` | 2 | slots per process |
| `WORKER_LEASE_SECONDS` | 60 | lease duration on claim |
| `WORKER_HEARTBEAT_SECONDS` | 20 | lease extension interval |
| `WORKER_POLL_INTERVAL_SECONDS` | 5 | blocking wait when the queue is empty |
| `MAINTENANCE_INTERVAL_SECONDS` | 5 | reaper and promoter sweep |
| `SHUTDOWN_GRACE_SECONDS` | 30 | time allowed to finish in-flight work |

The heartbeat interval must stay comfortably below a third of the lease, so a single missed extension does not expire it.

## 8. Testability

- `run_once()` is the entry point for unit and integration tests; no test waits on `run_forever`.
- The slot takes its repository, dispatch, clock and sleeper by constructor injection, so unit tests use fakes and integration tests use the real ones against the test database.
- Handler sleeps are already injected (spec 02 §4), so a worker test that runs a batch job completes instantly.
- `NullDispatch` lets the entire worker run with no Redis, which is how the fallback claim path is exercised in tests.

## 9. Acceptance criteria

- `python -m app.worker` starts, claims and completes jobs, and exits cleanly on `SIGTERM`.
- A job submitted through the API reaches `completed` with a result readable through the API.
- A handler that raises is recorded as a failure and retried or failed per spec 05.
- A job whose stored payload no longer parses is marked `failed` on its first attempt, not its third.
- A batch job's progress is visible through `GET /jobs/{id}` while it runs.
- Running the worker with `NullDispatch` produces identical outcomes, only with higher latency.
