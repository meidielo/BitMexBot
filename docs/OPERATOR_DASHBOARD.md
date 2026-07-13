# Secure Operator Dashboard

## Purpose and safety boundary

The operator dashboard is an authenticated, read-only view of sanitized bot
telemetry. It does not start or stop the bot, submit or cancel orders, change
risk settings, show exchange identifiers, read logs, or connect to BitMEX.

The boundary is deliberately one-way:

```text
private bot host
  trades_v2.db + bounded JSON status files
                  |
                  v
        operator_snapshot.py
                  |
                  v
  data/operator_dashboard/operator_status.json
                  |
                  v  dedicated directory mounted read-only
        dashboard container
                  |
                  v
       Coolify HTTPS endpoint
```

Only the dedicated `data/operator_dashboard` directory is shared with the web
container. Never mount the repository, `data`, `logs`, `.env`, a database, or
the Docker socket into the dashboard. Mount the directory rather than the
individual JSON file because the exporter uses atomic file replacement.

The dashboard image contains only `dashboard.py` and its web dependencies.
The `.dockerignore` allowlist prevents trading code and local data from being
sent to the image builder. The Python base image is pinned to a multi-platform
digest; update that digest deliberately when refreshing the base image.

## Sanitized inputs

`operator_snapshot.py` opens `data/trades_v2.db` with SQLite `mode=ro` and
`PRAGMA query_only=ON`. It validates the v4 schema and fixed allowlisted fields.
It never exports decision keys, client order IDs, exchange order IDs, halt
text, raw exceptions, balances, credentials, or logs.

The optional status inputs are strict, bounded JSON objects:

```json
{
  "schema_version": 1,
  "environment": "testnet",
  "status": "RUNNING",
  "generated_at_utc": "2026-07-14T01:00:00Z",
  "detail_code": "main_loop"
}
```

The runtime publishes that as `data/runner_status.json`. Accepted producer
states are `STARTING`, `RUNNING`, `WAITING`, `PAUSED`, `MANUAL_HALT`, `FAILED`,
and `STOPPED`. The exporter ignores `detail_code` so no free-form text reaches
the web process.

```json
{
  "schema_version": 1,
  "environment": "testnet",
  "state": "healthy",
  "generated_at_utc": "2026-07-14T01:00:00Z",
  "fresh": true
}
```

The independent watchdog publishes that as `data/watchdog_status.json`.
Its producer states are `starting`, `synchronizing`, `healthy`, `stale`, and
`failed`. A current file claiming `healthy` is still treated as stale when its
own `fresh` field is false.

Missing, stale, oversized, malformed, non-finite, or unrecognized status files
produce `UNKNOWN`, `STALE`, or `UNAVAILABLE`. They never produce a healthy
state. Seeing those fail-closed states is expected whenever either private
producer has not started or has stopped publishing.

Daily loss is accepted only from the existing `data/daily_loss.json` contract:

```json
{"date":"2026-07-14","loss_usd":0.0,"source":"trades_v2.db"}
```

Promotion evidence is evaluated by the existing fail-closed promotion
evaluator, but the snapshot exports only its stage and blocker count. Even a
complete result is labelled `CANARY_REVIEW_ONLY`; `production_enabled` and
`actions_enabled` are always false.

## Run the private exporter

One-shot publication:

```powershell
python operator_snapshot.py
```

Continuous publication every 15 seconds:

```powershell
python operator_snapshot.py --poll-seconds 15
```

Paths and freshness thresholds are explicit CLI options:

```powershell
python operator_snapshot.py `
  --ledger data/trades_v2.db `
  --daily-loss data/daily_loss.json `
  --heartbeat data/runner_status.json `
  --watchdog data/watchdog_status.json `
  --promotion-evidence data/promotion_evidence.json `
  --output data/operator_dashboard/operator_status.json `
  --heartbeat-max-age-seconds 120 `
  --watchdog-max-age-seconds 180 `
  --poll-seconds 15
```

Publication writes and flushes a temporary file, then atomically replaces the
previous snapshot. A failed write leaves the previous complete snapshot in
place. The web app independently validates and allowlists the file again.

## Authentication

The application requires HTTP Basic authentication on `/` and
`/api/v1/status`. HTTP Basic does not encrypt credentials itself. Use Coolify
HTTPS when a domain is available. Without a public HTTPS domain, keep the
service private to the remote host's Tailscale network and prefer Tailscale
Serve HTTPS rather than exposing a public HTTP port.

Generate a Werkzeug password hash interactively so the plaintext password is
not placed in shell history:

```powershell
python -c "from getpass import getpass; from werkzeug.security import generate_password_hash; print(generate_password_hash(getpass('Dashboard password: ')))"
```

Set these as Coolify secrets or runtime environment variables:

```text
DASH_USER=<unique operator username>
DASH_PASSWORD_HASH=<the full Werkzeug scrypt or PBKDF2 hash>
DASH_SNAPSHOT_PATH=/snapshot/operator_status.json
DASH_SNAPSHOT_MAX_AGE_SECONDS=120
DASH_SNAPSHOT_MAX_BYTES=262144
```

Do not commit the username, hash, or password. Preserve every `$` character in
the hash exactly when adding it to Coolify. Use a unique high-entropy password.
Restrict the planned remote deployment to Tailscale peers if a public HTTPS
domain is unavailable, and add reverse-proxy MFA if it becomes internet
reachable.

## Coolify deployment

1. Create an application using the repository and select
   `Dockerfile.dashboard` as the Dockerfile.
2. Configure container port `8080` and an HTTPS domain when available. If there
   is no public domain, bind the route only to the remote host's Tailscale path,
   preferably through Tailscale Serve HTTPS. Do not publish a direct public
   host port that bypasses Coolify or Tailscale access controls.
3. Bind mount only the host directory containing the sanitized snapshot to
   `/snapshot` as read-only. On a same-host deployment, that is the resolved
   `data/operator_dashboard` directory, not the parent `data` directory.
   The exporter publishes the secret-free snapshot as mode `0644` so the
   container's non-root UID can read it. Keep the dedicated parent directory
   free of any other files.
4. Add the authentication variables above as secrets.
5. Configure the container with a read-only root filesystem, all Linux
   capabilities dropped, `no-new-privileges`, and a small writable `tmpfs` at
   `/tmp` for Gunicorn worker files. The image disables Gunicorn's control
   socket because the process has no writable home directory.
6. Keep the Docker health check on `/readyz`; use `/healthz` only as a process
   liveness diagnostic.

`/healthz` proves only that the Flask/Gunicorn process can answer. `/readyz`
returns 200 only when authentication is configured and the sanitized snapshot
is structurally valid, fresh, and not unavailable. Both endpoints return only
a one-word status and require no credentials. They reveal no paths, ages,
errors, or operational data.

Official Coolify references:

- [Dockerfile applications](https://coolify.io/docs/applications/index)
- [Persistent storage](https://coolify.io/docs/knowledge-base/persistent-storage)
- [Health checks](https://coolify.io/docs/knowledge-base/health-checks)
- [Traefik custom middleware](https://coolify.io/docs/knowledge-base/proxy/traefik/custom-middlewares)

If Coolify and the bot are on different hosts, do not expose SQLite and do not
add an unauthenticated upload endpoint. Transfer only the sanitized snapshot
directory through a one-way authenticated mechanism, then mount the receiving
directory read-only.

## HTTP surface

| Route | Authentication | Response |
| --- | --- | --- |
| `GET /` | HTTP Basic | Server-rendered operator view |
| `GET /api/v1/status` | HTTP Basic | Allowlisted snapshot JSON |
| `GET /healthz` | None | Process status only |
| `GET /readyz` | None | Generic readiness status only |

All non-GET methods return 405. Control, configuration, key, order, position,
log, file, webhook, and raw database routes do not exist. Responses use
`Cache-Control: no-store`, a hash-based Content Security Policy with no
scripts, HSTS, frame denial, MIME sniffing denial, a restrictive permissions
policy, and no CORS opt-in.

## Verification

Run the focused test suites:

```powershell
python -m unittest -v test_operator_snapshot.py test_dashboard.py
```

Before deployment, also run the repository verification and machine quality
gate documented in the project README. In a Coolify staging deployment verify:

- unauthenticated `/` and `/api/v1/status` return 401;
- valid credentials return the sanitized view;
- HTTP redirects to HTTPS;
- no direct container port is reachable;
- a stale snapshot makes `/readyz` return 503 while `/healthz` stays 200;
- the dashboard remains useful when the private exporter is stopped;
- the mounted snapshot directory and container root filesystem reject writes;
- the built image contains no `.env`, database, logs, or trading modules.
