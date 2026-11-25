# Pulse

Pulse is a local, demo-grade API uptime monitor implemented as a single Go process backed by PostgreSQL. It periodically runs outbound HTTP/HTTPS health checks against registered endpoints, tracks monitor health state transitions, stores check history in PostgreSQL, and exposes an unauthenticated localhost-only REST API for managing monitors and querying availability and latency statistics.

> **Local / Demo Only**: Pulse is designed strictly for local development and demonstration. It runs as a single process without authentication, alerting, or distributed coordination.

---

## Architecture

```mermaid
flowchart TD
    subgraph Process["Pulse (Single Go Process)"]
        API["REST API Server (net/http)"]
        Scheduler["In-Process Scheduler (1s ticker)"]
        InFlight["In-Flight Set (sync.Mutex)"]
        Queue["Bounded Work Channel (cap: 100)"]
        Workers["Worker Pool (5 workers)"]
        Checker["Safe HTTP Checker (Basic SSRF Protection)"]
    end

    DB[("PostgreSQL\n(monitors, check_results)")]
    Targets["Target HTTP/HTTPS Endpoints"]

    API -->|"CRUD, History, Stats, /readyz"| DB
    Scheduler -->|"Poll due monitors\n(next_check_at <= current UTC time)"| DB
    Scheduler -->|"Deduplicate"| InFlight
    Scheduler -->|"Non-blocking send"| Queue
    Queue -->|"Dequeue"| Workers
    Workers -->|"HTTP GET"| Checker
    Checker -->|"Outbound request"| Targets
    Workers -->|"Tx: insert check_results + update monitor"| DB
    Workers -->|"Release marker"| InFlight
```

### Database Schema

PostgreSQL stores exactly two tables (`monitors` and `check_results`), initialized idempotently from [`internal/db/schema.sql`](internal/db/schema.sql):
- On first container database creation, Docker Compose mounts `schema.sql` into `/docker-entrypoint-initdb.d/01-schema.sql`.
- On every application startup, `cmd/pulse` executes the embedded `schema.sql` (`CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`) without dropping or altering existing data.

---

## Features

- **Monitor Management**: Create, list, inspect, and delete HTTP/HTTPS endpoint monitors (`DELETE` cascades to `check_results`).
- **In-Process Scheduling & Bounded Concurrency**: 1-second polling ticker, in-memory `InFlightSet` deduplication, bounded work channel (default capacity `100`), and a fixed worker pool (default `5` workers).
- **Health State Machine**: Tracks `UNKNOWN`, `UP`, and `DOWN` transitions using consecutive failure thresholds.
- **Transactional Result Persistence**: Inserts check observations and advances `next_check_at` atomically in one PostgreSQL transaction.
- **Check History & Sliding-Window Stats**: Paginated check history and uptime/latency stats over configurable hour windows (1–720 hours) with strict zero-observation `null` handling.
- **Basic Outbound SSRF Protections**: Restricts schemes to `http`/`https`, rejects embedded URL credentials, disables redirects and environment proxies, validates all DNS answers inside the dialer (rejecting loopback, RFC 1918/4193 private, link-local, multicast, unspecified, broadcast, CGNAT, and zoned IPv6 addresses, as well as mixed public/private answer sets), dials the validated IP directly while preserving `Host` and TLS SNI, and discards response bodies.
- **Health Probes & Graceful Shutdown**: `/healthz`, `/readyz`, and `SIGINT`/`SIGTERM` handling that drains active checks within a bounded timeout and closes the database pool cleanly.

---

## Quick Start (Docker Compose)

No real secrets are required to run the local demo. `docker-compose.yml` uses development-only demo credentials (`pulse`/`pulse`), binds PostgreSQL to `127.0.0.1:5432`, runs the Go app listening on `0.0.0.0:8080` inside the container, and publishes the API exclusively on `127.0.0.1:8080` on the host.

### Start

```bash
docker compose up --build -d
```

Wait for readiness:

```bash
curl -s http://127.0.0.1:8080/healthz
curl -s http://127.0.0.1:8080/readyz
```

View logs:

```bash
docker compose logs -f app
```

### Shutdown

Stop containers while preserving the named PostgreSQL volume (`pulse_pgdata`):

```bash
docker compose down
```

Stop containers and also remove the named PostgreSQL volume:

```bash
docker compose down -v
```

---

## Running Locally Without App Container

You can run only PostgreSQL in Docker Compose and run the Go application directly on your host. Because Go does not automatically load `.env` files, export the variables into your shell before running the application:

```bash
docker compose up -d db
cp .env.example .env
set -a
source .env
set +a
go run ./cmd/pulse
```

Stop the process with `Ctrl+C` (`SIGINT`) or `SIGTERM` for graceful shutdown.

---

## Configuration Defaults

Configuration is loaded from environment variables in [`internal/config/config.go`](internal/config/config.go). See [`.env.example`](.env.example).

| Variable | Default | Valid Range / Rules | Notes |
|---|---|---|---|
| `APP_ADDR` | `127.0.0.1:8080` | Valid `host:port` | `docker-compose.yml` sets `0.0.0.0:8080` inside container and publishes `127.0.0.1:8080:8080` |
| `DATABASE_URL` | `postgres://pulse:pulse@localhost:5432/pulse?sslmode=disable` | `postgres://` or `postgresql://` with host | `docker-compose.yml` sets `@db:5432` for the `app` service; `pulse`/`pulse` are development-only |
| `WORKER_COUNT` | `5` | `1` to `64` | Fixed worker pool size |
| `QUEUE_CAPACITY` | `100` | `1` to `10000` | Bounded in-memory work channel capacity |
| `SCHEDULER_POLL_INTERVAL` | `1s` | `100ms` to `1h` | Ticker interval for querying due monitors |
| `DEFAULT_MONITOR_INTERVAL` | `60s` | `30s` to `24h` | Must be `>= DEFAULT_CHECK_TIMEOUT` |
| `DEFAULT_CHECK_TIMEOUT` | `5s` | `1s` to `30s` | Must be `<= DEFAULT_MONITOR_INTERVAL` |

---

## API Routes & Curl Examples

Base URL: `http://127.0.0.1:8080`

| Method | Path | Description |
|---|---|---|
| `GET` | `/healthz` | Liveness probe (`200 OK`) |
| `GET` | `/readyz` | PostgreSQL readiness probe (`200 OK` or `503 Service Unavailable`) |
| `POST` | `/api/v1/monitors` | Create a monitor (`201 Created`) |
| `GET` | `/api/v1/monitors` | List all monitors ordered by `created_at DESC` (`200 OK`) |
| `GET` | `/api/v1/monitors/{id}` | Get monitor details by UUID (`200 OK` / `404 Not Found`) |
| `GET` | `/api/v1/monitors/{id}/checks` | Get recent check results (`?limit=50`, range `1`–`200`) |
| `GET` | `/api/v1/monitors/{id}/stats` | Get availability & latency stats (`?hours=24`, range `1`–`720`) |
| `DELETE` | `/api/v1/monitors/{id}` | Delete a monitor and cascade delete its checks (`204 No Content`) |

### Example `curl` Flow

```bash
# 1. Liveness and readiness probes
curl -s http://127.0.0.1:8080/healthz
curl -s http://127.0.0.1:8080/readyz

# 2. Create a monitor (immediately due on the next scheduler tick)
curl -s -X POST http://127.0.0.1:8080/api/v1/monitors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Example Site",
    "url": "https://example.com",
    "interval_seconds": 30,
    "timeout_seconds": 5
  }'

# 3. List monitors
curl -s http://127.0.0.1:8080/api/v1/monitors

# 4. Get a specific monitor by ID
MONITOR_ID="<uuid-from-create-response>"
curl -s "http://127.0.0.1:8080/api/v1/monitors/${MONITOR_ID}"

# 5. Inspect recent check results (default limit=50, max=200)
curl -s "http://127.0.0.1:8080/api/v1/monitors/${MONITOR_ID}/checks?limit=10"

# 6. Inspect sliding-window stats (default hours=24, max=720)
curl -s "http://127.0.0.1:8080/api/v1/monitors/${MONITOR_ID}/stats?hours=24"

# 7. Delete the monitor (cascade deletes check_results)
curl -i -X DELETE "http://127.0.0.1:8080/api/v1/monitors/${MONITOR_ID}"
```

### Monitor Creation Rules & Stats Zero-Observation Rule

- `name`: required, non-empty, max 255 characters.
- `url`: required, valid `http://` or `https://` absolute URL without embedded credentials (`user:pass@`).
- `interval_seconds`: optional, defaults to `60`, valid range `30`–`86400`.
- `timeout_seconds`: optional, defaults to `5`, valid range `1`–`30`, must not exceed `interval_seconds`.
- **Zero-observation rule**: When `total_checks == 0` in the requested stats window, both `availability` and `average_latency_ms` are `null` (never `100%` or `0ms`). When `total_checks > 0` and `successful_checks == 0`, `availability` is `0` and `average_latency_ms` is `null`.

---

## Health Policy

- **Initial status**: Newly created monitors start with `current_status = "UNKNOWN"`, `consecutive_failures = 0`, `last_checked_at = null`, `last_success_at = null`, and `next_check_at = created_at`.
- **Healthy check (HTTP `200`–`399`)**: `success = true`. Immediately sets `current_status = "UP"`, resets `consecutive_failures = 0`, and updates `last_success_at`.
- **Unhealthy check (HTTP `400`–`599`, network/DNS/TLS/timeout error, or SSRF rejection)**: `success = false`. Increments `consecutive_failures`.
- **Transition to `DOWN`**: Occurs strictly upon reaching 3 consecutive failures (`consecutive_failures >= 3`). While `consecutive_failures` is `1` or `2`, a monitor in `UNKNOWN` remains `UNKNOWN` and a monitor in `UP` remains `UP`.

---

## Scheduling Behavior

- **1-Second Poller**: Every `SCHEDULER_POLL_INTERVAL` (`1s`), the scheduler queries up to 100 due monitors using a parameterized query against the supplied current UTC time (`next_check_at <= $1`, ordered by `next_check_at ASC, id ASC`).
- **In-Flight Deduplication**: Before enqueueing, each monitor ID is checked against an in-memory `InFlightSet` (`sync.Mutex`-guarded `map[uuid.UUID]struct{}`). Monitors already queued or executing are skipped.
- **Bounded Queue & Saturation**: Tasks are sent with a non-blocking `select` into a buffered channel of capacity `QUEUE_CAPACITY` (`100`). If the channel is full, the monitor's in-flight marker is immediately removed and `next_check_at` is left unchanged in PostgreSQL so it remains due on the next tick.
- **Post-Commit Schedule Advancement**: A worker executes the check, opens a PostgreSQL transaction, inserts the row into `check_results`, updates the monitor state, and advances `next_check_at` to `completed_at + interval_seconds`. If persistence fails or rolls back, `next_check_at` is unchanged so the monitor remains due for retry.
- **Restart Behavior**: Checks do not run while Pulse is stopped. On restart, any overdue monitor (`next_check_at <= current UTC time`) is checked once; missed intervals are never backfilled or replayed.

---

## Verification & Test Commands

Run formatting, static analysis, unit tests, race detector, repeated concurrency test, and Compose validation:

```bash
# Formatting check (expects empty output)
gofmt -l .

# Static analysis
go vet ./...

# Unit + live DB integration tests (requires reachable PostgreSQL; without DB, live DB tests skip)
go test ./...

# Repeated worker concurrency limit test
go test -count=10 -run TestWorkerPoolRespectsConcurrencyLimit ./internal/scheduler

# Race detector
go test -race ./...

# Validate Docker Compose configuration
docker compose config
```

To run the live PostgreSQL integration tests explicitly against the Compose database:

```bash
docker compose up -d db
DATABASE_URL=postgres://pulse:pulse@localhost:5432/pulse?sslmode=disable go test -v ./internal/db ./internal/store
```

### Worker Concurrency Test Explained

`TestWorkerPoolRespectsConcurrencyLimit` in [`internal/scheduler/pool_test.go`](internal/scheduler/pool_test.go) verifies the worker pool's concurrency cap deterministically:

1. It creates a pool with **5 workers** (queue capacity 100) and submits **25 synthetic tasks** (more tasks than workers).
2. Each task signals when it starts on a channel, increments an atomic counter of currently active tasks, records the peak simultaneous count (`maxActive`), and blocks on a shared release channel.
3. The test waits until 5 tasks have started and verifies that **exactly 5** are simultaneously active and no 6th task has started while those 5 are blocked.
4. It then unblocks all tasks, waits for all 25 tasks to finish, and asserts that all 25 completed and peak simultaneous execution equaled 5 (never exceeding the cap).
5. **It is not a throughput benchmark**—it uses deterministic channel synchronization rather than timing or load measurements to prove the concurrency bound.

---

## Limitations & Scope Boundaries

- **Single process / single instance only**: There is no clustering, leader election, or multi-instance coordination.
- **In-memory queue**: The bounded work channel lives in memory only; queued tasks are not persisted as job rows and do not survive process restarts.
- **Checks pause while Pulse is stopped**: No checks execute while the process is down, and missed intervals are never replayed or backfilled on startup.
- **No authentication or authorization**: The REST API has no auth layer and is intended strictly for localhost binding (`127.0.0.1:8080`).
- **Local / demo only**: Not hardened for multi-tenant or public-internet deployment. Default credentials (`pulse`/`pulse`) are for local development only.
- **Basic SSRF protections are not a production security guarantee**: Dial-path DNS/IP validation, redirect blocking, and proxy disabling reduce accidental access to non-public networks in a local demo, but are not a substitute for network-level egress firewalls or production isolation.
- **Simple availability estimate is not an SLA**: Availability (`(successful_checks / total_checks) * 100`) is a straightforward ratio over observed checks in the selected window, not a formal SLA/SLO calculation.
- **No business-level API assertions**: Health checks evaluate only HTTP status codes (`200`–`399`) and transport success; response payloads are drained/discarded and never parsed or asserted against schema/content rules.
- **No alerting or UI**: Pulse provides a JSON REST API only—no web dashboard, webhooks, email, Slack, or incident workflows.
