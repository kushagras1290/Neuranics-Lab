# MorphX Offline First Device Simulator

This repository implements the Neuranics Lab software-engineering assignment as two independent Python processes:

- `morphx-agent` simulates a diagnostic device, acquires one synthetic test every five seconds by default, commits it to a local SQLite outbox, and synchronizes in the background.
- `morphx-server` exposes an HTTP API, stores accepted records in a separate SQLite database, deduplicates retries, and returns records in per-device acquisition order.

Acquisition never waits for the network. If the server is stopped, the agent continues allocating durable per-device sequence numbers and recording tests. When connectivity returns, due outbox records are retried automatically. Delivery is at least once; the server gives it exactly-once storage effect by treating `event_id` as an idempotency key and rejecting conflicting reuse.

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

## Requirements Traceability

| Assignment requirement | Implementation |
|---|---|
| Generate a record every five seconds | `MORPHX_INTERVAL_SECONDS=5`; `--interval` overrides it |
| WBC, RBC, and haemoglobin with units | Strict Pydantic wire schema with `10^3/uL`, `10^6/uL`, and `g/dL` literals |
| Stable identity across retries/restarts | UUID `event_id` is stored in the outbox before any send attempt |
| Per-device sequence survives restarts | Sequence allocation and outbox insert share one `BEGIN IMMEDIATE` transaction |
| Acquisition survives outages | Acquisition and synchronization are independent asyncio tasks; network I/O never runs in the acquisition path |
| Automatic recovery | Due records retry with capped exponential backoff and jitter |
| Central persistence | Server SQLite database on a persistent file/volume |
| Ordered retrieval | `GET /v1/devices/{device_id}/measurements` orders by sequence and supports a cursor |
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

The same scenario is automated with real subprocesses and HTTP:

```bash
uv run python scripts/verify_outage_recovery.py
```

The script uses temporary databases and ports and leaves project data untouched.

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
| `MORPHX_RETRY_MAX_SECONDS` | `60` | Retry-delay cap |
| `MORPHX_SERVER_DB_PATH` | `./data/server.db` | Durable central database |
| `MORPHX_SERVER_HOST` | `0.0.0.0` | Listen address |
| `MORPHX_SERVER_PORT` | `8000` | Listen port |
| `MORPHX_API_KEY` | unset | Optional shared API key; set identically on both processes |
| `MORPHX_LOG_LEVEL` | `INFO` | Structured JSON log level |

Changing `MORPHX_DEVICE_ID` while reusing an existing agent database fails closed. This prevents one physical state file from silently producing records for multiple logical devices.

## HTTP API

### Ingest

`POST /v1/measurements`

- `201 Created`: first durable insert
- `200 OK`: byte-equivalent retry of an existing `event_id`
- `409 Conflict`: the same `event_id` has different content, or a device sequence belongs to another event
- `422 Unprocessable Entity`: invalid schema, timestamp, identifier, unit, or numeric bound

### Retrieve

`GET /v1/devices/{device_id}/measurements?after_sequence=0&limit=100`

Results are ordered by ascending `sequence`. If `next_after_sequence` is non-null, pass it as the next request's `after_sequence` cursor.

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

The test suite covers schema rejection, restart-safe sequence allocation, transaction rollback, device-identity protection, server restart persistence, duplicate retries, idempotency conflicts, ordered cursor pagination, optional authentication, and offline-to-online synchronization.

## Operational and Security Notes

- No secret is stored in source control. When `MORPHX_API_KEY` is configured, comparisons use a timing-safe function. Production deployment should inject it from a secret manager.
- No CORS policy is enabled because this is a device-to-server API, not a browser API.
- Network requests have explicit timeouts and bounded connection pools. Retry delay is capped, and errors are sanitized before local persistence.
- Logs contain operational identifiers and sequence numbers, not measurement payloads or API keys.
- The container runs as a non-root user and writes only to `/data`.
- SQLite is appropriate for this single-server assignment. A horizontally scaled deployment should replace central SQLite with PostgreSQL while retaining the same unique constraints and transaction semantics.
- TLS termination, key rotation, rate limiting, metrics export, backup automation, and central database replication belong at the deployment layer and are not simulated here.
- Synced outbox rows are retained as a local audit trail. A production retention policy should prune acknowledged rows only after operational requirements are defined.

All measurements are synthetic. This project is a software reliability exercise, not a medical device implementation and not evidence of clinical, regulatory, privacy, or diagnostic validation.
