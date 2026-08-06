# Manual walkthrough

One path, end to end, against the running system. About 20 minutes.

This is not a replacement for `specs/TEST_PLAN.md` (493 automated tests) — it is what you run by hand before submitting, or in an interview when someone says "show me it works".

**Shell:** bash. **Prerequisite:** `docker compose up --build` is running.

---

## 0 · Setup

```bash
docker compose up -d --build
curl -sS localhost:8000/health | python -m json.tool
```

**Expect:** `status: "ok"`, `database: "ok"`, `redis: "ok"`, and `workers.count` of **4** (two processes × two slots). If it is 0, the workers did not start or Redis is unreachable.

Shorthands used throughout:

```bash
sub() { curl -sS -X POST localhost:8000/jobs -H 'Content-Type: application/json' -d "$1"; }
jid() { python -c "import sys,json;print(json.load(sys.stdin)['id'])"; }
show() { curl -sS "localhost:8000/jobs/$1" | python -m json.tool; }
hist() { curl -sS "localhost:8000/jobs/$1/logs" | python -c "import sys,json;[print(f\"{e['level']:8} | {e['message']}\") for e in json.load(sys.stdin)['items']]"; }
psql() { docker compose exec -T db psql -U jobs -d jobs -c "$1"; }
```

- [ ] passed

---

## 1 · Submission and retrieval

```bash
ID=$(sub '{"job_type":"email","payload":{"to":"a@example.com","subject":"Hello","body":"Message body"}}' | jid)
echo $ID
show $ID
```

**Expect:** `201` on submission, a `Location` header, and status `pending` or `processing` on retrieval. `attempts` is 0 before pickup and 1 after.

- [ ] passed

---

## 2 · Completion, end to end

```bash
ID=$(sub '{"job_type":"batch","payload":{"items":["a","b","c","d"],"operation":"index"},"priority":8}' | jid)
sleep 5
show $ID
```

**Expect:** `status: "completed"` · `progress: 100` · a `result` containing `{"total": 4, "succeeded": …, "failed": …}` · `started_at` and `completed_at` both set · `worker_id` and `lease_until` cleared (they are not in the response — check the database if you want to see it).

The history says the same thing in sequence:

```bash
hist $ID
```

**Expect:** `Job created with status pending` → `Claimed by …` → `Job completed`.

- [ ] passed

---

## 3 · Failure and retry with backoff

A webhook job fails 20 % of the time by design. Ten submissions make at least one failure statistically certain.

```bash
for i in $(seq 1 10); do
  sub '{"job_type":"webhook","payload":{"url":"https://example.com/hook"}}' > /dev/null
done
sleep 8
psql "SELECT attempts, status, scheduled_at > now() AS waiting, error->>'type' AS err
      FROM jobs WHERE job_type='webhook' AND attempts > 0 ORDER BY attempts DESC LIMIT 5;"
```

**Expect:** at least one row with `attempts >= 1`, status `pending`, `waiting = t` and `err = JobExecutionError` — the job went back into the queue **held back**, rather than failing outright.

The delay itself:

```bash
psql "SELECT attempts, round(extract(epoch FROM scheduled_at - updated_at)) AS delay_seconds
      FROM jobs WHERE status='pending' AND attempts=1 AND scheduled_at IS NOT NULL LIMIT 3;"
```

**Expect:** `delay_seconds` between **15 and 30** for the first retry. The second is 60–120. The range rather than an exact value is equal jitter: without it, a downstream outage fails every in-flight job at the same instant and they all retry at the same instant too.

- [ ] passed

---

## 4 · Cancellation

```bash
# An hour out — inside the 90-day scheduling horizon. A fixed far-future date is
# rejected with 422, and that is correct: max_schedule_horizon_days is a rule of
# the system, not a malfunction.
LATER=$(python -c "import datetime;print((datetime.datetime.now(datetime.UTC)+datetime.timedelta(hours=1)).isoformat())")
ID=$(sub "{\"job_type\":\"report\",\"payload\":{\"report_type\":\"sales\",\"date_from\":\"2026-01-01\",\"date_to\":\"2026-06-30\"},\"scheduled_at\":\"$LATER\"}" | jid)
curl -sS -X POST "localhost:8000/jobs/$ID/cancel" | python -m json.tool
curl -sS -o /dev/null -w "%{http_code}\n" -X POST "localhost:8000/jobs/$ID/cancel"
```

**Expect:** the first returns `200` with `status: "cancelled"`. The second returns **`409`** — a cancelled job cannot be cancelled again.

Now cancelling a job that is already running. It has to be a job **long enough** to catch mid-flight — an email job finishes in 1–3 seconds and then you get a 409 for the wrong reason:

```bash
ITEMS=$(python -c "import json;print(json.dumps([f'i{n}' for n in range(400)]))")
ID=$(sub "{\"job_type\":\"batch\",\"payload\":{\"items\":$ITEMS,\"operation\":\"index\"},\"priority\":9}" | jid)
sleep 3
curl -sS "localhost:8000/jobs/$ID" | python -c "import sys,json;print('status now:',json.load(sys.stdin)['status'])"
curl -sS -X POST "localhost:8000/jobs/$ID/cancel" | python -c "import sys,json;print(json.load(sys.stdin)['error']['message'])"
```

**Expect:** the status is `processing`, and the message is `Job is processing and can no longer be cancelled` — not `completed`. Work already in flight is not interrupted; that is a documented decision, not an oversight.

- [ ] passed

---

## 5 · Idempotency

```bash
BODY='{"job_type":"email","payload":{"to":"dup@example.com","subject":"s","body":"b"},"idempotency_key":"manual-test-001"}'
curl -sS -o /dev/null -w "first:  %{http_code}\n" -X POST localhost:8000/jobs -H 'Content-Type: application/json' -d "$BODY"
curl -sS -o /dev/null -w "second: %{http_code}\n" -X POST localhost:8000/jobs -H 'Content-Type: application/json' -d "$BODY"
psql "SELECT count(*) FROM jobs WHERE idempotency_key='manual-test-001';"
```

**Expect:** `201` then **`200`** — not 201, not 409 — and a count of **1**. The 200 is what lets a client that timed out learn whether its original request landed.

Keys are never expired or swept, so the assignment's "retained for at least 24 hours" holds by construction. `L2-22` pins it.

- [ ] passed

---

## 6 · Priority ordering

This needs one worker with one slot. Otherwise every job is claimed at once and there is nothing to measure.

```bash
docker compose stop worker
psql "TRUNCATE jobs, job_logs CASCADE;"

for p in 0 9 3 7 1; do
  sub "{\"job_type\":\"email\",\"payload\":{\"to\":\"p$p@example.com\",\"subject\":\"p$p\",\"body\":\"b\"},\"priority\":$p}" > /dev/null
done

docker compose run -d --name pri-worker -e WORKER_CONCURRENCY=1 worker
sleep 22
docker rm -f pri-worker
```

```bash
psql "SELECT priority, to_char(started_at,'HH24:MI:SS.MS') AS started
      FROM jobs WHERE payload->>'subject' LIKE 'p%' ORDER BY started_at;"
docker compose start worker
```

**Expect:** the priority column descends — **9, 7, 3, 1, 0**.

Two traps, which are why the test is built this way. `TRUNCATE` rather than `DELETE ... WHERE status='pending'`, or jobs finished during earlier sections show up in the result; and `HH24:MI:SS` rather than seconds alone, or rows from different minutes look out of order while `ORDER BY` is in fact right.

- [ ] passed

---

## 7 · Future scheduling

```bash
FUTURE=$(python -c "import datetime;print((datetime.datetime.now(datetime.UTC)+datetime.timedelta(seconds=20)).isoformat())")
ID=$(sub "{\"job_type\":\"email\",\"payload\":{\"to\":\"later@example.com\",\"subject\":\"s\",\"body\":\"b\"},\"scheduled_at\":\"$FUTURE\"}" | jid)
show $ID | grep status
sleep 30
show $ID | grep status
```

**Expect:** `scheduled` at first, then `completed` once the time has passed. Promotion happens in the maintenance sweep, every 5 seconds.

- [ ] passed

---

## 8 · Manual retry of a failed job

Fabricate a job that failed permanently — what three failed attempts would have left behind:

```bash
ID=$(psql "INSERT INTO jobs (job_type, payload, status, attempts, max_attempts, error, started_at, completed_at)
           VALUES ('email','{\"to\":\"r@example.com\",\"subject\":\"s\",\"body\":\"b\",\"cc\":[]}','failed',3,3,
                   '{\"type\":\"JobExecutionError\",\"message\":\"downstream was down\"}', now(), now())
           RETURNING id;" | sed -n 3p | tr -d ' ')
curl -sS -X POST "localhost:8000/jobs/$ID/retry" | python -m json.tool
sleep 6
show $ID
```

**Expect:** the retry returns `200` with `status: "pending"` and **`attempts: 0`**, and `error` / `started_at` / `completed_at` cleared. A few seconds later, `completed`.

Without the attempts reset the endpoint would be useless: the job would be claimed once, find its attempts spent, and fail immediately.

```bash
hist $ID
```

**Expect:** `Job requeued by manual retry; attempts reset` sits between the old life and the new one. The previous failure is in the history, not in the current state — which is exactly why the state was cleared.

- [ ] passed

---

## 9 · Poison message → dead letter

A payload rejected at submission never reaches the queue, so simulate the real case: **a schema that moved underneath a stored job.**

```bash
ID=$(psql "INSERT INTO jobs (job_type, payload, status)
           VALUES ('email','{\"nonsense\": true}','pending') RETURNING id;" | sed -n 3p | tr -d ' ')
sleep 6
show $ID
curl -sS -o /dev/null -w "retry: %{http_code}\n" -X POST "localhost:8000/jobs/$ID/retry"
curl -sS 'localhost:8000/jobs?dead_lettered=true' | python -m json.tool | grep -c '"id"'
```

**Expect:**

- `status: "failed"` with **`dead_letter_reason: "unprocessable_payload"`** on the **first attempt** — no point spending two more workers to discover the same thing.
- The retry comes back **`409`**. That refusal is what the dead-letter classification is actually for.
- The job appears under the `dead_lettered=true` filter.
- In `/health`, `queue.dead_lettered` has risen to 1.

**And the converse** — a job that failed through an ordinary exception (section 8) is `failed` with `dead_letter_reason: null` and **can** be retried. That distinction is the whole design.

- [ ] passed

---

## 10 · Crash recovery

```bash
ITEMS=$(python -c "import json;print(json.dumps([f'item-{i}' for i in range(600)]))")
ID=$(sub "{\"job_type\":\"batch\",\"payload\":{\"items\":$ITEMS,\"operation\":\"transform\"},\"priority\":9}" | jid)
sleep 3
psql "SELECT status, attempts, worker_id FROM jobs WHERE id='$ID';"

docker kill jobqueueservice-worker-1 jobqueueservice-worker-2
psql "SELECT status, attempts, lease_until > now() AS lease_alive FROM jobs WHERE id='$ID';"
```

**Expect at this point:** `processing`, `attempts=1`, `lease_alive = t` — the job is stranded, holding a lease nobody is extending.

```bash
docker compose start worker
sleep 110    # 60s lease + up to 5s sweep + ~30s to run the 600 items again
show $ID | grep -E 'status|attempts|progress'
```

**Expect:** `completed` with **`attempts: 2`** and `progress: 100`. The lease expired, the reaper returned the job to the queue, and another worker ran it from the start. The dead attempt was counted, which is why a job that kills workers cannot loop forever.

Do not shorten the wait: 600 items is 30 seconds of work, and after recovery it starts over. Measuring too early shows `processing` with `attempts: 2` — meaning recovery worked and the run is still going.

**The best evidence is the job's own history:**

```bash
hist $ID
```

```
info     | Job created with status pending
info     | Claimed by 3de43e9f6f78-1-0          ← the worker that was killed
warning  | Lease expired; returned to the queue
info     | Claimed by 3de43e9f6f78-1-0          ← reclaimed after the restart
info     | Job completed
```

Every one of those rows was written by a different part of the system — the API, a worker process that no longer exists, the reaper, and the worker that finished the job — and they read as one sequence.

The two claim lines often carry the **same** id, which is not a mistake: a worker's identity is its hostname, pid and slot, and a restarted container gets its hostname back and starts at pid 1 again. Identity is per slot, not per lifetime — which is exactly why `worker_id` alone cannot prove ownership and `attempts` is the fencing token (DECISIONS.md §1).

- [ ] passed

---

## 11 · Graceful shutdown

```bash
ID=$(sub '{"job_type":"report","payload":{"report_type":"users","date_from":"2026-01-01","date_to":"2026-03-31"},"priority":9}' | jid)
sleep 2
docker compose stop worker
show $ID | grep -E 'status|attempts'
docker compose start worker
```

**Expect:** `completed` with `attempts: 1` — the worker took `SIGTERM` and finished what it was holding before exiting, instead of leaving it to the reaper.

- [ ] passed

---

## 12 · Redis goes down, the system carries on

```bash
docker compose stop redis
curl -sS localhost:8000/health | python -c "import sys,json;j=json.load(sys.stdin);print('redis:',j['redis'],'| workers:',j['workers'])"

ID=$(sub '{"job_type":"email","payload":{"to":"noredis@example.com","subject":"s","body":"b"}}' | jid)
sleep 12
show $ID | grep status

docker compose start redis
```

**Expect:**

- `/health` returns **`200`** — not 503 — with `redis: "error"`, `workers: null` and `ready_hints: null`. `null` rather than `count: 0`: "I cannot see the workers" is not "there are no workers", and an operator given the second answer will go and restart healthy workers in the middle of a cache outage.
- The job reaches `completed` anyway, through the PostgreSQL fallback claim. Only slower.
- After Redis comes back, everything recovers with no intervention.

**Do not be alarmed if 2 workers appear instead of 4 immediately afterwards.** The liveness keys went with Redis, and each slot rewrites its own only on its next heartbeat — up to 20 seconds. Wait and look again:

```bash
sleep 22 && curl -sS localhost:8000/health | python -c "import sys,json;print(json.load(sys.stdin)['workers'])"
```

- [ ] passed

---

## 13 · Validation and the security layer

```bash
# unknown type
curl -sS -o /dev/null -w "%{http_code} unknown type\n" -X POST localhost:8000/jobs \
  -H 'Content-Type: application/json' -d '{"job_type":"teleport","payload":{}}'

# payload missing a field
curl -sS -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"job_type":"email","payload":{"to":"a@b.com"}}' | python -m json.tool

# extra field
curl -sS -o /dev/null -w "%{http_code} extra field\n" -X POST localhost:8000/jobs \
  -H 'Content-Type: application/json' -d '{"job_type":"email","payload":{"to":"a@b.com","subject":"s","body":"b","admin":true}}'

# priority out of range
curl -sS -o /dev/null -w "%{http_code} priority=99\n" -X POST localhost:8000/jobs \
  -H 'Content-Type: application/json' -d '{"job_type":"email","payload":{"to":"a@b.com","subject":"s","body":"b"},"priority":99}'

# SSRF — loopback, private network, cloud metadata, and finally a legitimate URL as a control
for u in "http://127.0.0.1/x" "http://[::1]/x" "http://169.254.169.254/latest/meta-data" \
         "http://intranet/x" "http://db.internal/x" "https://hooks.example.com/ok"; do
  curl -sS -o /dev/null -w "%{http_code}  $u\n" -X POST localhost:8000/jobs \
    -H 'Content-Type: application/json' -d "{\"job_type\":\"webhook\",\"payload\":{\"url\":\"$u\"}}"
done

# a body over 64 KB — rejected before anything tries to parse it
python -c "import json;print(json.dumps({'job_type':'batch','payload':{'items':['x'*400 for _ in range(500)],'operation':'index'}}))" > big.json
curl -sS -o /dev/null -w "%{http_code} body of %{size_upload} bytes\n" -X POST localhost:8000/jobs \
  -H 'Content-Type: application/json' --data-binary @big.json
rm big.json
```

**Expect:** every validation check returns `422`; in the SSRF block, **the first five are `422` and the last is `201`**; the oversized body is `413`. The control line is not decoration — without it, a broken endpoint that rejects *everything* would pass this section with distinction.

The missing-payload response carries `details` naming `payload.subject` and `payload.body` — an error that tells the client what to fix.

Note: `http://intranet` is blocked because a single-label name resolves through the local search domain to an internal host. That rule was in no specification — it came out of the tests.

- [ ] passed

---

## 14 · Final snapshot

```bash
curl -sS localhost:8000/health | python -m json.tool
```

**What to read:**

| Field | What it tells you |
|---|---|
| `queue.pending` + `oldest_pending_seconds` | Depth alone cannot separate load from failure. High depth with low age = a busy system keeping up. Low depth with high age = a stuck one. |
| `queue.ready_hints` | Should track `pending`. A large gap means announcements are failing and everything is arriving through the fallback. |
| `queue.dead_lettered` | Should be 0. If it is not, there is work no retry can help. |
| `workers.count` | 4 in a normal run. `null` means no visibility into Redis. |

```bash
docker logs --since 120s jobqueueservice-worker-1 2>&1 | grep -c 'next_hint_failed'   # expect 0
docker logs --since 120s jobqueueservice-worker-1 2>&1 | grep -c 'slot.cycle_failed'  # expect 0
docker logs jobqueueservice-worker-1 2>&1 | grep -vc '^{'                             # expect 0
```

**Expect:** zero on all three.

- `next_hint_failed` against a live Redis was the bug where the block and the socket timeout were both 5 seconds. **The `--since` window is mandatory** — section 12 disconnected Redis deliberately and left genuine warnings in the cumulative log. If they show up outside that window, something is wrong.
- `slot.cycle_failed` is a slot loop that fell over and recovered. One is a transient event; a steady stream means a dependency below is unwell.
- The third line checks that **all** log output is valid JSON. A single line of free text breaks any log pipeline.

- [ ] passed

---

## Cleanup

```bash
psql "TRUNCATE jobs, job_logs CASCADE;"
docker compose restart worker
```

---

## What this walkthrough does not cover, and why

| | |
|---|---|
| **Exactly-once pickup under concurrency** | Needs ten workers competing for the same row in the same millisecond. That cannot be timed by hand — `tests/integration/test_claiming.py` and `tests/load/` do it with real connections and real commits, including through the Redis dispatch path. |
| **A displaced worker writing a stale result** | Needs a worker frozen precisely between lease expiry and its write. That is `test_w2_07`. |
| **Exact attempt exhaustion** | Depends on the 20 % coin flip; in the suite it is deterministic with an injected RNG. |

What it does do: all fourteen sections above fail if any of those mechanisms is broken. They do not prove the mechanism — they rule it out.
