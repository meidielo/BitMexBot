# Remote Coolify Host Deployment

This deployment runs only on the remote Linux host. The local Windows PC is a
source and verification workstation. The stack remains authenticated BitMEX
Testnet-only and does not add a mainnet client or a production switch.

## Runtime boundary

`compose.remote.yml` defines six isolated services:

- `secure-dns`: strict DNS-over-TLS to authenticated Cloudflare and Quad9
  resolvers. There is no plaintext DNS fallback.
- `ledger-init`: a networkless one-shot migration step that runs before the
  credential-bearing services.
- `runner`: the Testnet-only decision and execution process.
- `watchdog`: an independent authenticated, read-only BitMEX Testnet WebSocket
  observer with no order or cancellation methods. It is an explicit Compose
  profile and remains off until a separate read-only key exists.
- `snapshot`: a one-way exporter that reads private runtime state and writes a
  bounded, sanitized JSON document.
- `dashboard`: a non-root Gunicorn process that receives only the sanitized
  snapshot volume. It receives no BitMEX credentials, database, logs, source
  tree, or Docker socket.

The images use digest-pinned base images. Containers use read-only root filesystems,
dropped capabilities, `no-new-privileges`, bounded temporary filesystems, and
named persistent volumes. The resolver keeps only `NET_BIND_SERVICE`, which is
required for its non-root process to listen on port 53.

## Required remote secrets

Copy the matching templates from `deploy/runner.env.example`,
`deploy/watchdog.env.example`, and `deploy/dashboard.env.example`. Rename them
to the paths below, replace every placeholder, and keep them untracked.

Create `/home/meidie/BitMexBot-safe/.env.runner` with mode `0600`:

```text
BITMEX_TESTNET=true
BITMEX_TESTNET_API_KEY=<dedicated Testnet key>
BITMEX_TESTNET_API_SECRET=<dedicated Testnet secret>
```

The runner key must belong to a dedicated Testnet account, have only the
permissions needed for Testnet orders, have no withdrawal capability, and be
IP-restricted where the exchange supports it. The generic legacy variable
names are not accepted by the hardened clients.

Create `.env.watchdog` with mode `0600` and a different read-only Testnet API key:

```text
BITMEX_TESTNET=true
BITMEX_TESTNET_API_KEY=<dedicated read-only Testnet key>
BITMEX_TESTNET_API_SECRET=<dedicated read-only Testnet secret>
```

Keeping these files separate prevents the observer container from receiving
the runner's order authority. A second key must be created in the BitMEX
Testnet account UI. Do not reuse the runner key for the observer.

Start the base stack without the watchdog profile when that second key is not
yet available. The dashboard remains usable but reports a degraded watchdog
state. After the key is created, add `.env.watchdog` and enable the profile with
`docker compose -f compose.remote.yml --profile watchdog up -d`.

Create `.env.dashboard` with mode `0600`:

```text
DASH_USER=<operator username>
DASH_PASSWORD_HASH=<Werkzeug scrypt or PBKDF2 hash>
DASH_SNAPSHOT_MAX_AGE_SECONDS=120
DASH_SNAPSHOT_MAX_BYTES=262144
```

Never place the plaintext dashboard password or either exchange secret in the
repository, Compose file, image, or command output.

## Preflight and cutover

From the fresh remote checkout:

```bash
docker compose -f compose.remote.yml config --quiet
docker compose -f compose.remote.yml build --pull
```

The one-shot, network-disabled `ledger-init` service creates or migrates the v2
ledger before the runner can start. It is safe to run again on later deploys.

Before starting the new runner, use a read-only private exchange query through
the strict resolver to prove that the dedicated account is flat and has no open
orders. Stop the legacy runner and legacy dashboard before starting the new
stack. Never allow both runner processes to hold the same Testnet credentials
concurrently.

Start and inspect the hardened stack:

```bash
docker compose -f compose.remote.yml up -d
docker compose -f compose.remote.yml ps
docker compose -f compose.remote.yml logs --tail=100 secure-dns runner watchdog snapshot dashboard
```

Do not print either environment file or use `docker inspect` output that would
dump environment values into terminal history.

## Private HTTPS dashboard

The Compose stack publishes no host port. The dashboard has the fixed address
`10.254.54.10:8080` on an internal Docker bridge, with no default external
route. The host can reach that bridge, but the public internet and LAN cannot.
On the current remote host, publish it only to authenticated Tailscale peers
with Tailscale Serve HTTPS:

```bash
tailscale serve --bg --https=8443 http://10.254.54.10:8080
tailscale serve status
```

The resulting private URL is:

```text
https://meidie-b550m-phantom-gaming-4.tail2df761.ts.net:8443/
```

`/` and `/api/v1/status` require dashboard credentials. `/healthz` proves only
that the web process answers. `/readyz` proves that auth is configured and the
sanitized snapshot is current and structurally available. Neither health route
contains private operational data.

## Verification

Expected checks after deployment:

```bash
docker compose -f compose.remote.yml ps
curl -sS -o /dev/null -w '%{http_code}\n' http://10.254.54.10:8080/healthz
curl -sS -o /dev/null -w '%{http_code}\n' http://10.254.54.10:8080/
curl -sS -o /dev/null -w '%{http_code}\n' http://10.254.54.10:8080/readyz
```

The expected statuses are `200`, `401`, and `200` respectively once all
services are healthy. An unavailable ledger or stale telemetry must make
`/readyz` return `503` instead of producing a false-ready dashboard.

Verify inside the runner network that `testnet.bitmex.com` resolves to BitMEX
addresses and that HTTPS certificate verification succeeds. Do not disable TLS
verification to work around the remote ISP's intercepted DNS response.

## Safe rollback

If the new runner or watchdog cannot prove safety, stop both execution services
first:

```bash
docker compose -f compose.remote.yml stop runner watchdog
```

Keep the account flat and investigate. Do not automatically restart the legacy
runner because its resolver path is not trustworthy on this host. The dashboard
and sanitized snapshot can remain up while diagnostics run. Persistent v2 data
must be backed up before any volume replacement or migration.

This deployment is a safer Testnet engineering environment. It is not evidence
of a profitable strategy and is not permission to use real funds.
