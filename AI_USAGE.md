# AI Tool Usage

## Tools I Used

Claude Code (Opus), for the whole cycle: drafting the specs, implementing against them, and writing the tests.

I worked spec first. For each part of the system I wrote a spec in `specs/`, reviewed it, and only then implemented. The test IDs in `specs/TEST_PLAN.md` are the actual test names in the suite, so any claim in a spec can be traced to the test that holds it.

## What Helped Most

**Exhaustive test matrices.** A case per CHECK constraint, a payload-validation matrix across all four job types, and about thirty SSRF inputs — loopback, private ranges, link-local, IPv4-mapped IPv6, malformed URLs. This is the work where my own attention fades after the three obvious cases. The constraint matrix passed first time and confirmed the schema; the SSRF matrix found two real holes in code I already thought was finished.

**Comparing queue technologies before committing.** Redis lists with `BRPOP`, sorted sets, Redis Streams with consumer groups, RabbitMQ priority queues — mechanics and limits of each, laid out side by side. That turned a day of reading into an hour. The choice was still mine; what I got was the map.

## What I Had to Fix

**A concurrency test that proved nothing.** The idempotency-under-concurrency test is one of the six the assignment requires. The obvious way to write it is to reuse the shared database fixture — a session inside a transaction that is rolled back afterwards. That is wrong, and quietly: ten "concurrent" submissions on one connection inside one transaction run in sequence. The test goes green while asserting nothing about concurrency. It needed a separate fixture with independent connections and real commits. A test that passes and is vacuous is worse than no test, because it also removes the pressure to write a real one.

**A completion write with no ownership check.** The obvious way to finish a job is `UPDATE jobs SET status='completed' WHERE id=$1`. That is a data-loss bug. If the worker stalled long enough for its lease to expire, the reaper has already handed the job to someone else, and the stalled worker then overwrites a legitimate result with its own stale one. The write has to be conditional on still owning the job, and the worker has to discard its result when it matches zero rows. Nothing in the happy path suggests this, and no single-worker test will ever catch it. It is the most important line in the design, and it is in `DECISIONS.md` §1 and §2 for that reason.

Three others, briefly: a retry deadline computed from the application clock while the claim query compares against the database's, so every retry would fire early or late by the clock skew; `ON CONFLICT DO NOTHING` that could not infer a partial unique index, failing only on requests that carried an idempotency key; and a worker whose entry point was wired to the no-Redis fallback, which passed every test because that fallback is designed to behave identically.

## What AI Struggled With

**Library behaviour it assumed instead of verifying.** Pydantic returns IPv6 hosts bracketed, so `url.host` for `http://[::1]/x` is the string `"[::1]"`, which `ipaddress.ip_address()` refuses. Loopback over IPv6 walked straight through an SSRF check that looked airtight. The same matrix showed that pydantic normalises `https:///path` into the host `path`, which is why the validator now rejects single-label hostnames — a rule that was in no spec I had written.

**Anything that depends on context it cannot see.** Every "which of these two correct designs belongs here" question came back as a balanced summary of both. Making Postgres the source of truth and demoting Redis to a hint, bounding priority to 0–9 so a client cannot submit `2^31-1` and pre-empt the queue forever, answering an idempotent replay with `200` rather than `201` — none of those were suggested to me.

## On Reviewing the Output

The rule was that anything I could not explain does not ship: no config knobs nothing reads, no defensive branches for conditions that cannot occur, no layers added "for later". `ruff` and `mypy --strict` run clean, but they only catch style and types.

Worth recording about the review itself: my last pass over the finished repository produced eleven findings, three of which mattered. Three others did not survive checking — a plausible-looking SSRF bypass that pydantic already normalises away, and two crashes that could not actually happen. A confident review is a source of hypotheses, not conclusions.
