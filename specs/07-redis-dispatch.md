# Spec 07 — Redis Dispatch

**Status:** Accepted — implemented
**Depends on:** `specs/00-architecture.md`, `specs/04-claiming.md`
**Related decisions:** DECISIONS.md §1

## 1. Scope

What Redis holds, who writes to it, and — the part that matters most — what happens when it is not there.

## 2. Role

Redis holds the set of jobs that are ready to run and is the fast path workers pull from. **It decides nothing.** Every id it hands out is revalidated against Postgres by a conditional `UPDATE` before any work begins (spec 04 §6).

The property this buys, and the one the design is optimised for: **the system is correct with Redis stopped.** Losing it raises latency from milliseconds to one poll interval; it never loses a job, never duplicates one, and needs no operator action when Redis returns.

That holds because of architecture invariant 6 — every job reachable through Redis is also reachable through the Postgres fallback claim. And the fallback is not a dormant emergency branch: scheduled jobs becoming due, retry backoffs elapsing, and jobs released by the reaper all arrive through it on every single run.

## 3. Interface

A `Dispatch` protocol with two implementations. `RedisDispatch` talks to Redis; `NullDispatch` does nothing and reports nothing.

`NullDispatch` is not a test-only convenience — it is the configuration the service falls back to when Redis is unreachable at startup, and it is what the entire worker test suite runs against, which is how the fallback claim path stays exercised rather than assumed.

| Method | Redis | Purpose |
|---|---|---|
| `announce(job_id, priority, created_at)` | `ZADD jobs:ready` | publish a dispatch hint |
| `next_hint(timeout)` | `BZPOPMIN jobs:ready` | blocking wait for work |
| `heartbeat_worker(worker_id, ttl)` | `SET worker:{id} … EX ttl` | liveness for `/health` |
| `active_workers()` | `SCAN MATCH worker:*` | who is alive |
| `ready_depth()` | `ZCARD jobs:ready` | queue depth, instantly |

`SCAN` rather than `KEYS`: `KEYS` blocks the server for the duration of the scan, and a health endpoint must never be the thing that stalls Redis.

## 4. Score encoding

```
score = (MAX_PRIORITY - priority) * 10^13 + created_at_ms
```

`BZPOPMIN` pops the lowest score, so inverting priority makes the highest-priority job pop first, and adding the submission timestamp orders oldest-first within a priority level. One key, one command, both orderings — matching the SQL in spec 04 §4 so the two paths agree.

**The precision constraint is real and silent.** Redis sorted-set scores are float64, which represents integers exactly only up to 2^53 ≈ 9.007 × 10^15. With priority bounded to 0–9 (spec 01 §4) the first term reaches 9 × 10^13, and a 2026 millisecond timestamp is about 1.79 × 10^12, giving roughly 9.2 × 10^13 — two orders of magnitude of headroom.

Exceeding it would not raise; scores would round and FIFO ordering within a priority would degrade with no error anywhere. The bounds are therefore asserted in tests:

- the priority scale must satisfy `(MAX_PRIORITY + 1) × 10^13 < 2^53`
- the encoding holds while `created_at_ms < 10^13`, i.e. until the year 2286

## 5. Announcing

**Strictly after the transaction commits.** `JobService.submit` commits, then announces.

The reverse order is not a correctness bug — the conditional claim would simply match nothing — but it is a latency bug that is unpleasant to diagnose: a worker pops the id, queries a row that is not yet visible, discards the hint as stale, and the job waits for the fallback poll instead. Intermittent, load-dependent, and invisible in the data afterwards.

A failed `announce` is logged and swallowed. The job is already durably `pending`; the fallback will find it. Failing the client's request because a cache write failed would be the wrong trade.

**Submission is the only thing that announces.** Jobs the reaper releases and jobs the promoter makes due are *not* re-announced: they reach a worker through the fallback claim, within one poll interval. Announcing them would mean carrying each job's priority and creation time out of a batch `UPDATE ... RETURNING` purely to reconstruct a score, in exchange for a few seconds on jobs that are by definition already late. The queue's correctness does not change either way — invariant 6 holds regardless — so the simpler side of that trade is the right one.

### The blocking wait needs a socket that outlives it

`next_hint(timeout)` asks Redis to hold the connection open for up to `timeout` seconds. The client has its own read deadline, and **redis-py defaults it to 5 seconds** — the same order as a sensible poll interval, and in fact exactly `WORKER_POLL_INTERVAL_SECONDS`.

When the two are equal the socket always wins. The pop raises `redis.exceptions.TimeoutError` instead of returning empty, so a perfectly idle queue is reported as a Redis failure — logged with the same event that means the server is gone, once per slot per interval, forever. The signal that should have announced a real outage is on permanently, and redis-py discards a connection after each read timeout, so every idle poll also costs a fresh TCP connection and handshake.

Nothing breaks: `next_hint` degrades to "no hint" and the fallback claim carries the work, which is why the system looks healthy while doing this. The client's read budget is therefore **derived** from the longest block its caller will request rather than inherited from a library default:

```
socket_timeout = max_block_seconds + BLOCK_TIMEOUT_MARGIN_SECONDS
```

The worker passes its poll interval; the API passes nothing, because it only announces and reads depth and never blocks. Tests pin the relationship, not the numbers.

### A failed hint still has to cost the block

`next_hint` is the **only pacing the slot loop has** — `run_forever` has no sleep in its idle path, which is why `NullDispatch.next_hint` sleeps for the timeout it is handed rather than returning at once.

A refused connection fails in a round trip. The failure branch therefore cannot simply log and return `None`: doing so hands control straight back to a loop with nothing else to wait on, and a Redis outage becomes a hot loop of claim queries against the one database the outage did not take away. Measured on the running stack: **49 idle cycles in twenty seconds where the poll interval intends 8**, per two slots.

So the failure branch waits out **the remainder** of the block before answering — the remainder rather than the whole timeout, because a read that timed out has already spent it, and sleeping the full amount again would double the idle interval. Re-measured after the fix: 10.

The rule this generalises to: **degrading is not the same as returning early.** Any future method the slot loop awaits for pacing carries the same obligation, and W1-14c is what holds it.

## 6. Stale entries

Entries are never removed on cancellation, and this is deliberate. A cancelled job stays in the sorted set until a worker pops it, attempts the conditional claim, matches zero rows, and drops it. **The set cleans itself as a side effect of normal operation**, which is cheaper and simpler than keeping two stores transactionally aligned — the exact coupling this design exists to avoid.

The same mechanism absorbs duplicate announcements: `ZADD` on an existing member updates its score rather than adding a second entry, so a job announced twice is popped once.

## 7. Worker liveness

Each slot writes `worker:{worker_id}` with a TTL of twice the heartbeat interval, refreshed on every heartbeat. `/health` lists the surviving keys.

Redis is the right store for this precisely because it is the non-authoritative one: worker liveness is ephemeral observability data with a natural expiry, nothing depends on it for correctness, and expressing "gone if not refreshed" as a TTL is free. Putting it in Postgres would mean a table, a migration, and a cleanup sweep for data that is worthless sixty seconds later.

If Redis is down, `/health` reports worker status as unknown rather than as zero. **"I cannot see the workers" and "there are no workers" are different incidents**, and an operator must not be told the second when the first is true.

## 8. Degradation

| Failure | Effect |
|---|---|
| Redis unreachable at startup | worker and API run with `NullDispatch`; jobs flow through the fallback claim |
| Redis dies while running | `next_hint` raises, is logged, waits out the rest of the block it was asked for, and the slot proceeds to the fallback claim at its ordinary pace |
| Redis data lost entirely | nothing is lost; the set repopulates from new submissions and maintenance sweeps |
| Redis recovers | new announcements resume; latency returns to normal with no intervention |

No path through any of these loses, duplicates, or strands a job.

## 9. Acceptance criteria

- A submitted job is announced only after its row is visible to another connection.
- `BZPOPMIN` returns the highest-priority job, and the oldest within that priority.
- A cancelled job left in the set is popped once, claimed by nobody, and disappears.
- An idle blocking poll returns empty and logs **nothing**; a client whose read deadline only matches its block logs a failure, and that difference is asserted.
- **With Redis stopped, submitted jobs still reach `completed`**; restarting it requires no intervention.
- With Redis stopped, an idle slot claims at its poll interval and not faster: a failed hint costs the remainder of the block it was asked for, while the non-blocking form still returns at once.
- The whole worker test suite passes against `NullDispatch`, exercising only the fallback path.
- Encoded scores stay below 2^53 across the full legal priority range.
- With Redis down, `/health` reports worker status as unknown, not as zero workers.
