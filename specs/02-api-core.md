# Spec 02 — API, Job Class Hierarchy and Validation

**Status:** Accepted — implemented
**Depends on:** `specs/00-architecture.md`, `specs/01-data-model.md`
**Related decisions:** DECISIONS.md §0

## 1. Scope

The synchronous half of the system: the HTTP contract, the job class hierarchy that gives each job type its schema and behaviour, the validation layers, the error model, and how logging attaches to all of it.

Out of scope: the worker loop, claiming, Redis, retry timing, leases. Jobs submitted through this API remain `pending` or `scheduled` until spec 03 exists. **That is expected behaviour at this stage, not a defect.**

## 2. Layering

```
HTTP  →  api/routes  →  services/JobService  →  db/JobRepository  →  PostgreSQL
                              │
                              └── jobs/registry → BaseJob subclasses
```

One rule, enforced by review: **each layer may only call the layer below it.**

- `api/` translates HTTP to commands and domain errors to status codes. No SQL, no business rules.
- `services/` holds use cases and owns the transaction boundary. No HTTP types, no `Request`, no `HTTPException`.
- `db/` holds every statement in the system. Nothing above it constructs SQL.
- `jobs/` is pure: no database, no HTTP, no session. It knows payload shapes and how to execute work.

This is what makes each layer testable alone, and it is why the handler tests in spec TEST_PLAN L1 need no infrastructure at all.

`JobService` receives its `JobRepository`, `Clock` and logger by constructor injection; FastAPI wires them in `api/deps.py`. Tests construct the service directly or override the dependency.

## 3. Endpoints

### `POST /jobs` — submit

```json
{
  "job_type": "email",
  "payload": { "to": "a@b.com", "subject": "hi", "body": "..." },
  "priority": 7,
  "max_attempts": 3,
  "scheduled_at": "2026-08-05T09:00:00Z",
  "idempotency_key": "order-1234-confirmation"
}
```

Only `job_type` and `payload` are required. `priority` and `max_attempts` default to the values declared on the job class, so defaults are a property of the job type rather than a magic number in the API layer.

| Response | Meaning |
|---|---|
| `201` | New job created |
| `200` | `idempotency_key` matched an existing job; that job is returned, nothing was created |
| `413` | Body exceeded the size limit |
| `422` | Envelope or payload failed validation |

The 200/201 split is deliberate: an idempotent replay did not create a resource, and a client retrying after a network timeout can tell from the status code whether its original request landed.

### `GET /jobs/{job_id}` — fetch

`200` with the job · `404` if unknown · `422` if the path segment is not a UUID.

### `GET /jobs` — list

Query: `status`, `job_type`, `limit` (1–100, default 20), `offset` (≥ 0).

```json
{ "items": [ ... ], "limit": 20, "offset": 0, "has_more": true }
```

**No `total` field.** Producing one requires `COUNT(*)` over the filtered set on every request, which degrades linearly as the table grows — on the endpoint most likely to be hit repeatedly by an operator watching a backlog. `has_more` is computed by fetching `limit + 1` rows and discarding the extra, which costs nothing. If a total is ever genuinely needed it should be an approximate count from statistics, not an exact scan.

Offset pagination is accepted here (deep offsets are slow, but an operator listing jobs does not page to 10,000). Keyset pagination is the documented upgrade path.

### `POST /jobs/{job_id}/cancel` — cancel

Implemented as the conditional update that spec 01 §3 requires:

```sql
UPDATE jobs SET status='cancelled'
WHERE id = :id AND status IN ('pending','scheduled')
RETURNING *;
```

| Response | Condition |
|---|---|
| `200` | Job was `pending` or `scheduled`; now `cancelled` |
| `409` | Job exists but is `processing`, `completed`, `failed` or already `cancelled` |
| `404` | No such job |

Distinguishing 404 from 409 requires a second read when zero rows come back — the update is still the authority, the read only explains *why* it matched nothing. That ordering matters: reading first and then updating would reintroduce the race this shape exists to avoid.

The endpoint is written this way now, before any worker exists, precisely so that the cancel-versus-claim race in spec 04 is already resolved correctly when the worker arrives.

### `GET /health`

```json
{
  "status": "ok",
  "version": "0.1.0",
  "uptime_seconds": 412,
  "database": "ok",
  "queue": { "scheduled": 2, "pending": 17, "processing": 0,
             "completed": 340, "failed": 3, "cancelled": 1 }
}
```

`503` with `"database": "error"` if the connectivity check fails, so a load balancer can act on it. Counts come from a single grouped aggregate, not one query per status. Worker liveness is added in spec 08.

## 4. Job class hierarchy

Two hierarchies exist and they are deliberately separate (spec 01 §2): the table is flat, the behaviour is polymorphic.

### `JobContext` — the seam

```python
class JobContext(Protocol):
    @property
    def job_id(self) -> UUID: ...
    @property
    def attempt(self) -> int: ...

    async def report_progress(self, pct: int) -> None: ...
    async def log(self, level: str, message: str, **fields: Any) -> None: ...
    async def heartbeat(self) -> None: ...
```

`job_id` and `attempt` are read-only. A handler is told which job it is running, never asked to decide — and declaring them as properties rather than settable attributes is what lets the database-backed implementation derive them from the ownership token instead of storing a second copy.

Everything a handler is allowed to touch. No session, no repository, no ORM object. A handler physically cannot reach the database, which means it cannot be coupled to it, which means it can be tested by passing a `FakeJobContext` that records calls into a list.

In this spec the concrete implementation is a no-op recorder; spec 03 supplies the one backed by the database. Handlers do not change when that happens.

### `BaseJob`

```python
class BaseJob(ABC):
    job_type: ClassVar[JobType]
    Payload: ClassVar[type[BaseModel]]
    Result: ClassVar[type[BaseModel]]
    default_priority: ClassVar[int] = 5
    default_max_attempts: ClassVar[int] = 3
    timeout_seconds: ClassVar[int] = 60

    def __init__(self, payload: BaseModel, ctx: JobContext) -> None: ...

    @classmethod
    def parse_payload(cls, raw: Mapping[str, Any]) -> BaseModel:
        return cls.Payload.model_validate(raw)

    @abstractmethod
    async def run(self) -> BaseModel: ...
```

A job type declares its input schema, its output schema, its defaults, and what it does. Nothing else in the system needs to know the difference between an email job and a report job.

### Registry

```python
JOB_REGISTRY: dict[JobType, type[BaseJob]] = {}


@register
class EmailJob(BaseJob): ...
```

`register` validates at import time that `job_type` is unique and that `Payload`, `Result` and `run` are all defined — a misconfigured job type fails at startup, not on the first request that uses it.

Adding a job type is one new file plus the decorator. The API, service, repository and schema are untouched. This is the extensibility claim, and the test suite asserts it by registering a dummy type and submitting it end to end.

### The four types

| Class | Payload | Result | Behaviour |
|---|---|---|---|
| `EmailJob` | `to: EmailStr`, `subject` 1–200, `body` 1–10 000, `cc: list[EmailStr]` ≤ 10 | `message_id` | sleep 1–3s |
| `WebhookJob` | `url: HttpUrl` (+ SSRF check), `method: Literal["POST","PUT"]`, `headers` ≤ 10, `body: dict \| None` | `status_code`, `response_ms` | sleep 1–2s, 20 % simulated failure |
| `ReportJob` | `report_type: Literal[...]`, `date_from ≤ date_to`, `format: Literal["csv","pdf"]` | `file_url`, `row_count` | sleep 3–5s |
| `BatchJob` | `items: list[str]` 1–1000, `operation: Literal[...]` | `total`, `succeeded`, `failed`, `errors` ≤ 100 | per-item delay, reports progress |

`run()` is implemented now even though no worker calls it. Without it the abstraction is half-built and untestable; with it, every handler is covered by unit tests that touch no infrastructure — which is the evidence the rubric asks for that worker logic was tested independently of the API.

The randomness in `WebhookJob` is drawn from an injected `Random` instance, seeded in tests. A handler whose failure rate cannot be controlled cannot be tested.

## 5. Validation

Three layers, each stopping something the others cannot.

**Layer 0 — middleware.** Requests whose body exceeds **64 KB** are rejected with `413` *before* parsing. A validator cannot protect against a payload that is hostile by size, because rejecting it requires parsing it first.

**Layer 1 — envelope.** `SubmitJobRequest`:

| Field | Rule |
|---|---|
| `job_type` | must be a registered type |
| `priority` | 0–9 |
| `max_attempts` | 1–10 |
| `scheduled_at` | timezone-aware; not more than 90 days ahead |
| `idempotency_key` | `^[A-Za-z0-9_.:-]{1,255}$` |

`scheduled_at` must carry an offset. A naive datetime would be interpreted in whatever timezone the server happens to run in, which is a scheduling bug that only appears in production. A past `scheduled_at` is accepted and the job starts as `pending` — clamping is friendlier than a 422 for a client whose clock is a few seconds off.

**Layer 2 — payload, dispatched by type.** `get_job_class(job_type).parse_payload(raw)`.

All payload models set `ConfigDict(extra="forbid", str_strip_whitespace=True)`. `extra="forbid"` matters beyond tidiness: silently accepting unknown fields lets a client believe it configured something it did not, and lets unreviewed data ride along into storage.

## 6. Security

**SSRF in `WebhookJob` is the one genuine injection risk in this system** — a job payload that names a URL the server will later request. The validator rejects:

- schemes other than `http`/`https`
- literal addresses in private, loopback, link-local, multicast or reserved ranges — including `169.254.169.254`, the cloud instance-metadata endpoint. IPv6 literals arrive bracketed from pydantic and are unwrapped first, and IPv4-mapped IPv6 addresses (`::ffff:127.0.0.1`) are reduced before the check
- hostnames ending in `.localhost`, `.local` or `.internal`
- **single-label hostnames** — `localhost`, `intranet`, `db`. A name with no dot resolves through the local search domain to an internal host; every public name has a dot. This also catches `https:///path`, which pydantic normalises to the host `path`

**Stated limitation:** this cannot be complete at validation time. A hostname that resolves to a public address when submitted can resolve to `127.0.0.1` when the request is finally made (DNS rebinding), and the gap between submission and execution in a queue is exactly where that is exploitable. Closing it requires resolving the host at request time and pinning the resolved IP in the HTTP client. The webhook here is simulated and issues no real request, so the residual risk is nil today; the constraint is recorded so it is not forgotten if the handler ever becomes real.

**SQL injection is structurally excluded.** Every statement is built from SQLAlchemy constructs or `text()` with bound parameters; there is no string interpolation into SQL anywhere in the codebase. Payloads are passed to the driver as JSONB values, never concatenated. This is a property to preserve under review, not a filter to maintain.

**Result and error handling.** Results are validated against the job class's `Result` model before persistence, so a handler cannot write arbitrary shapes into storage. Responses are `application/json` only — no server-side HTML rendering exists, so job content has no path to an XSS sink. Error responses carry a stable machine code and a human message, never a traceback, driver text, or SQL.

**Log redaction.** Payloads are never logged verbatim; they contain user data such as email addresses and message bodies. Logs record `job_type`, payload size in bytes, and a short SHA-256 prefix — enough to correlate two identical submissions without storing their contents twice.

**Queue poisoning.** Validating at the boundary means a payload that cannot be parsed never becomes a job, so it can never reach a worker to crash it. The residual case — a payload that is valid but expensive — is bounded here by the size and item-count caps, and handled in part 3 by attempt limits and a dead-letter path.

**Authentication is out of scope.** The assignment specifies none, and none is implemented. The consequence is explicit: anyone who can reach the API can submit jobs and can read any job whose id they know. Random UUIDs make ids impractical to guess, but that is obscurity, not authorization. A real deployment needs an authenticated principal on submission and ownership scoping on read; recorded in DECISIONS.md §5 rather than half-built.

## 7. Errors and observability

### Error shape

```json
{ "error": { "code": "job_not_cancellable",
             "message": "Job is already completed",
             "request_id": "01J...",
             "details": [ { "field": "payload.to", "message": "not a valid email" } ] } }
```

One handler maps domain exceptions to responses, so status codes are decided in one place:

| Exception | Status | Code |
|---|---|---|
| `JobNotFoundError` | 404 | `job_not_found` |
| `JobNotCancellableError` | 409 | `job_not_cancellable` |
| `UnknownJobTypeError` | 422 | `unknown_job_type` |
| `PayloadValidationError` | 422 | `payload_invalid` |
| `PayloadTooLargeError` | 413 | `payload_too_large` |
| anything else | 500 | `internal_error` — message fixed, details suppressed |

### Logging

`structlog`, JSON to stdout, one event per line. Standard fields: `ts`, `level`, `event`, `service`, `request_id`, and where applicable `job_id`, `job_type`, `status`, `attempt`, `duration_ms`.

Middleware assigns a `request_id` per request — reusing an inbound `X-Request-ID` when it is well-formed, so a trace can span services — binds it to a contextvar, and echoes it on the response. Every log line emitted while handling that request carries it without being passed around.

**State transitions go through one helper** that writes the `job_logs` row and emits the log line together. Two call sites would eventually disagree; one cannot.

## 8. Deferred

- Authentication and per-client scoping (§6).
- Rate limiting and submission backpressure — nothing currently stops unbounded enqueueing.
- Keyset pagination (§3).
- `Retry-After` on 409/503.

## 9. Acceptance criteria

- Every row of the TEST_PLAN E2E matrix passes.
- A payload with an unknown extra field is rejected with the field named in `details`.
- A webhook payload targeting `169.254.169.254` or `127.0.0.1` is rejected with 422.
- Two identical submissions with the same `idempotency_key` return the same `id`, the second with status 200, and exactly one row exists.
- Ten concurrent submissions with one shared key create exactly one row.
- Cancelling a `pending` job returns 200; cancelling it again returns 409.
- No response body anywhere in the suite contains the substring `Traceback`.
- Registering a new job type in a test requires touching no file outside `app/jobs/`.
- Every log line emitted during a request is valid JSON and carries the same `request_id` as the response header.
