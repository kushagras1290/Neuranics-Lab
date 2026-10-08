# MorphX Offline First Device Simulator

This repository implements the Neuranics Lab software-engineering assignment as two independent Python processes:

- `morphx-agent` simulates a diagnostic device, acquires one synthetic test every five seconds by default, commits it to a local SQLite outbox, and synchronizes in the background.
- `morphx-server` exposes an HTTP API, stores accepted records in a separate SQLite database, deduplicates retries, and returns records in per-device acquisition order.

Acquisition never waits for the network, and SQLite work runs in worker threads so it never stalls the event loop either. If the server is stopped, the agent continues allocating durable per-device sequence numbers and recording tests. When connectivity returns, due outbox records are retried automatically. Delivery is at least once; the server gives it exactly-once storage effect by treating `event_id` as an idempotency key and rejecting conflicting reuse.

## Architecture

```text
┌──────────────────────── MorphX device process ────────────────────────┐
│ synthetic generator ── atomic SQLite transaction ── local outbox     │
│                                      │                                │
│                                      └── background HTTP synchronizer │
└───────────────────────────────────────────────────┬───────────────────┘
                                                    │ POST /v1/measurements
                                                    │ retry with stable event_id
┌──────────────────── central server process ───────▼───────────────────┐
│ FastAPI validation ── idempotency/sequence checks ── server SQLite   │
│                                                           │           │
│                           ordered, cursor-based retrieval ─┘           │
└───────────────────────────────────────────────────────────────────────┘
```

The databases are intentionally separate. `agent.db` represents storage on the device and `server.db` represents central storage. Both use WAL mode, parameterized SQL, uniqueness constraints, explicit transactions for critical writes, and restart-safe files.

### Module layout

| Module | Responsibility |
|---|---|
| `models.py` | Wire schema, units, shared identifier patterns, canonical serialization |
| `errors.py` | Exception hierarchy rooted at `MorphXError` |
| `config.py` | Environment + CLI parsing, validated at startup (fails closed) |
| `generator.py` | Synthetic measurement values |
| `agent_store.py` | Device SQLite: atomic sequence allocation, outbox, quarantine, migration |
| `sync.py` | Response classification, backoff, pass halting, outbox updates |
| `supervision.py` | Failure budgets, task supervision, cooperative shutdown |
| `agent.py` | Device process: fixed-rate acquisition loop, wiring, entry point |
| `server_store.py` | Central SQLite: idempotent ingest, filtered retrieval, device summary |
| `server.py` | FastAPI routes, auth dependency, request-ID middleware, entry point |
| `sqlite_utils.py` | Connection defaults and the `BEGIN IMMEDIATE` transaction helper |
| `logging_utils.py` | One JSON object per log line, including structured extras |

## Design Decisions

- **Python + asyncio for the agent.** Acquisition and synchronization are two independent tasks in one process. Network I/O is non-blocking, and blocking SQLite calls are moved to worker threads with `asyncio.to_thread`, so a slow disk or a black-holed server cannot delay the acquisition schedule.
- **FastAPI + Pydantic for the server.** The wire schema is declared once in `models.py` and enforced on both sides (`extra="forbid"`, literal units, bounded values, timezone-aware timestamps). FastAPI turns those models into request validation and OpenAPI docs at `/docs` without hand-written glue.
- **SQLite on both sides.** It is an embedded, transactional, crash-safe store with no extra service to operate, which is exactly what a device needs. `synchronous=FULL` plus WAL means a committed measurement survives power loss. On the server it keeps the assignment self-contained; the schema relies only on unique constraints and `BEGIN IMMEDIATE`, both of which map directly to PostgreSQL if central load ever requires it.
- **Transactional outbox instead of send-then-store.** The sequence bump and the outbox insert commit together, so a crash at any point leaves either both or neither. The `event_id` is minted before the first send and never changes, which makes retries idempotent.
- **Fixed-rate cadence.** Deadlines are `start + n × interval`, so time spent persisting does not accumulate as drift. If the device stalls past a whole interval the missed ticks are skipped and logged rather than burst-acquired.

## Failure Handling

| Situation | Behaviour |
|---|---|
| Server unreachable or timing out | Record retried with capped exponential backoff + jitter; the pass stops after the first transport failure, so one timeout per pass rather than one per pending record |
| `429`, `502`, `503`, `504` | Same as above; a numeric `Retry-After` is honoured up to `MORPHX_RETRY_MAX_SECONDS` |
| `500` or a malformed / mismatched acknowledgement | That record backs off; later records in the pass continue, so one poisoned record cannot block the queue |
| `400`, `409`, `413`, `422` | Permanent rejection. The record is **quarantined**: kept in the outbox with `quarantined_at` and the server's error code (`EVENT_ID_CONFLICT`, `SEQUENCE_CONFLICT`, ...), never retried, and logged at `ERROR` |
| `401`, `403`, `404`, `405` | Configuration problem (key or URL). Nothing is quarantined; the pass stops and waits `MORPHX_RETRY_MAX_SECONDS` before trying again, logged at `ERROR` |
| SQLite `OperationalError` (locked, disk full) | The loop logs and retries next iteration; after 5 consecutive failures the task gives up |
| Any task crashes or gives up | Supervisor cancels the sibling task, flushes once, and the process exits with status 1 so Docker (`restart: unless-stopped`) or systemd restarts it |
| `SIGTERM` / `SIGINT` | Tasks get 1 s to finish an in-flight step, then a final sync bounded by `MORPHX_SHUTDOWN_SYNC_SECONDS` (default 3 s), well inside Docker's 10 s stop window. Unsent data stays in the outbox |
| Agent killed with `SIGKILL` | Nothing to do: every acquired record was committed before it was logged. On restart the sequence continues and the backlog drains |

A quarantined `SEQUENCE_CONFLICT` usually means the agent database was deleted while the device kept its `MORPHX_DEVICE_ID`, so sequences restarted at 1. Recover by restoring the original `agent.db` or by assigning a new device ID.

## Requirements Traceability

| Assignment requirement | Implementation |
|---|---|
| Generate a record every five seconds | `MORPHX_INTERVAL_SECONDS=5`; `--interval` overrides it |
| WBC, RBC, and haemoglobin with units | Strict Pydantic wire schema with `10^3/uL`, `10^6/uL`, and `g/dL` literals |
| Stable identity across retries/restarts | UUID `event_id` is stored in the outbox before any send attempt |
| Per-device sequence survives restarts | Sequence allocation and outbox insert share one `BEGIN IMMEDIATE` transaction |
| Acquisition survives outages | Acquisition and synchronization are independent, supervised asyncio tasks; network I/O never runs in the acquisition path and SQLite runs off the event loop |
| Automatic recovery | Due records retry with capped exponential backoff and jitter; permanent rejections are quarantined instead of retried forever |
| Central persistence | Server SQLite database on a persistent file/volume |
| Ordered retrieval | `GET /v1/devices/{device_id}/measurements` orders by sequence, supports a cursor and an optional `measured_at` window; `GET /v1/devices` lists known devices |
| Separate receipt time | Server stores immutable `received_at` independently of device `measured_at` |
| Separate Linux-compatible processes | Two console entry points and two Docker Compose services |

## Quick Start with uv

Prerequisites: Python 3.12-3.14 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync --locked --extra dev
```

Start the server in terminal 1:

```bash
uv run morphx-server
```

Start the device in terminal 2:

```bash
uv run morphx-agent
```

Retrieve the first 100 records in acquisition order:

```bash
curl "http://127.0.0.1:8000/v1/devices/MORPHX_SIM_001/measurements?after_sequence=0&limit=100"
```

Interactive API documentation is available at `http://127.0.0.1:8000/docs`.

## Demonstrate Outage Recovery

1. Start both processes and wait for several synchronized records.
2. Stop only `morphx-server`. Leave the agent running for 15-20 seconds. Its logs continue to show `measurement acquired and persisted` while synchronization is deferred.
3. Restart `morphx-server` with the same database path. The agent automatically sends the backlog.
4. Query the device endpoint and verify that sequences are continuous and ordered.

The same scenario, plus a hard kill of the agent and a mid-run server restart, is automated with real subprocesses, HTTP, and SQLite files:

```bash
uv run python scripts/verify_outage_recovery.py
```

It runs four phases against one pair of databases: server down, server started, agent `kill -9` and restart, server stop and restart. It then checks that server sequences are contiguous from 1, that no `event_id` is duplicated or re-minted, and that every event the agent marked synced exists centrally. It prints one JSON line and exits non-zero on failure. The script uses temporary databases and ports and leaves project data untouched.

## Docker Compose

```bash
docker compose up --build
```

Compose creates distinct named volumes for device and server state. Stop and restart the stack to verify persistence:

```bash
docker compose down
docker compose up
```

Do not add `-v` to `docker compose down` if you want to retain the databases.

## Configuration

Copy `.env.example` to `.env` for local overrides. Command-line values take precedence for `--device-id`, `--db-path`, `--server-url`, `--interval`, `--host`, and `--port`.

| Variable | Default | Purpose |
|---|---:|---|
| `MORPHX_DEVICE_ID` | `MORPHX_SIM_001` | Stable device identity |
| `MORPHX_AGENT_DB_PATH` | `./data/agent.db` | Durable device state and outbox |
| `MORPHX_SERVER_URL` | `http://127.0.0.1:8000` | Central server base URL |
| `MORPHX_INTERVAL_SECONDS` | `5` | Acquisition interval |
| `MORPHX_SYNC_POLL_SECONDS` | `1` | Maximum idle wait between sync passes |
| `MORPHX_SYNC_BATCH_SIZE` | `100` | Maximum due records per pass |
| `MORPHX_REQUEST_TIMEOUT_SECONDS` | `5` | HTTP timeout |
| `MORPHX_RETRY_BASE_SECONDS` | `1` | First retry delay |
| `MORPHX_RETRY_MAX_SECONDS` | `60` | Retry-delay cap, also the wait after an auth/URL failure |
| `MORPHX_SHUTDOWN_SYNC_SECONDS` | `3` | Upper bound on the final flush at shutdown |
| `MORPHX_RANDOM_SEED` | unset | Optional seed for reproducible synthetic values |
| `MORPHX_SERVER_DB_PATH` | `./data/server.db` | Durable central database |
| `MORPHX_SERVER_HOST` | `127.0.0.1` | Listen address; Compose passes `--host 0.0.0.0` inside the container |
| `MORPHX_SERVER_PORT` | `8000` | Listen port |
| `MORPHX_API_KEY` | unset | Optional shared API key; set identically on both processes |
| `MORPHX_LOG_LEVEL` | `INFO` | Structured JSON log level |

Changing `MORPHX_DEVICE_ID` while reusing an existing agent database fails closed. This prevents one physical state file from silently producing records for multiple logical devices.

## HTTP API

### Ingest

`POST /v1/measurements`

- `201 Created`: first durable insert
- `200 OK`: byte-equivalent retry of an existing `event_id`
- `409 Conflict`: `detail.code` is `EVENT_ID_CONFLICT` (same `event_id`, different content) or `SEQUENCE_CONFLICT` (the device sequence belongs to another event)
- `422 Unprocessable Entity`: invalid schema, timestamp, identifier, unit, or numeric bound

### Retrieve

`GET /v1/devices/{device_id}/measurements?after_sequence=0&limit=100`

Results are ordered by ascending `sequence`. If `next_after_sequence` is non-null, pass it as the next request's `after_sequence` cursor. Optional `measured_from` and `measured_to` (inclusive, ISO 8601 with a timezone) restrict results to a measurement-time window and combine with the cursor.

`GET /v1/devices` returns one summary per device: record count, first and last sequence, and the latest `measured_at` / `received_at`. A gap between `measurement_count` and `last_sequence - first_sequence + 1` means records are still in flight or quarantined on the device.

### Health

- `GET /health/live` confirms that the HTTP process is running.
- `GET /health/ready` confirms that SQLite is queryable.

## Verification

```bash
uv run ruff check .
uv run mypy src
uv run pytest
uv export --extra dev --no-emit-project --no-hashes --format requirements-txt > requirements-audit.txt
uv run pip-audit -r requirements-audit.txt
docker compose config
docker build -t morphx-simulator:local .
```

The test suite covers:

- schema rejection
- restart-safe sequence allocation
- transaction rollback
- schema migration of an existing device database
- device-identity protection
- server restart persistence
- duplicate retries and idempotency conflict codes
- ordered cursor pagination and time-window filtering
- optional authentication
- offline-to-online synchronization
- response classification (quarantine, halt, `Retry-After`)
- storage-fault budgets
- task supervision and cooperative shutdown
- the bounded final flush
- fixed-rate cadence
- both process entry points

## Operational and Security Notes

- No secret is stored in source control. When `MORPHX_API_KEY` is configured, comparisons use a timing-safe function. Production deployment should inject it from a secret manager.
- The server binds to `127.0.0.1` by default and logs a warning if it is bound to any other address without an API key.
- No CORS policy is enabled because this is a device-to-server API, not a browser API.
- Network requests have explicit timeouts and bounded connection pools. Retry delay is capped, and errors are sanitized before local persistence.
- Logs contain operational identifiers and sequence numbers, not measurement payloads or API keys.
- The container runs as a non-root user and writes only to `/data`.
- SQLite is appropriate for this single-server assignment. A horizontally scaled deployment should replace central SQLite with PostgreSQL while retaining the same unique constraints and transaction semantics.
- TLS termination, key rotation, rate limiting, metrics export, backup automation, and central database replication belong at the deployment layer and are not simulated here.
- Synced outbox rows are retained as a local audit trail. A production retention policy should prune acknowledged rows only after operational requirements are defined.

## Limitations and Unfinished Work

**Time spent:** about 12 hours in total, more than the suggested 6 to 8 hours.

These were left out deliberately to keep the submission focused on the offline-first guarantees the assignment asks for:

- **No operator tooling for quarantined records.** They are visible in `agent.db` (`quarantined_at`, `last_error`) and in `ERROR` logs, but there is no command to inspect, re-queue, or discard them.
- **No device health endpoint or metrics.** The agent has no HTTP surface, so backlog size and quarantine count are only reported in the startup log line and per-record logs. A production device would export them, for example as Prometheus gauges or a heartbeat to the server.
- **One record per request.** Sync sends records individually in sequence order. A batch ingest endpoint would cut round-trips for large backlogs but complicates partial-failure handling.
- **Single shared API key.** No per-device credentials, rotation, or mTLS. TLS termination is expected at the deployment layer.
- **Central SQLite.** It is fine for one server process. Multiple replicas need PostgreSQL; the unique constraints and transaction pattern carry over unchanged.
- **No outbox retention.** Synced rows are never pruned, so `agent.db` grows without bound over the device's lifetime.
- **Retrieval is per device and sequence-ordered.** There is no cross-device query, and time filtering is a scan within the device's rows; an index on `(device_id, measured_at)` would be the next step for large volumes.

All measurements are synthetic. This project is a software reliability exercise, not a medical device implementation and not evidence of clinical, regulatory, privacy, or diagnostic validation.
