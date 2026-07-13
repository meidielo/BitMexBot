# Testnet WebSocket Watchdog

`exchange_watchdog.py` is an independent, read-only observer for the BitMEX
Testnet account used by this research project. It does not share the trading
loop's CCXT client. It connects directly to the raw Testnet WebSocket endpoint,
authenticates during the upgrade request, and subscribes only to these private
tables:

- `order`
- `position`
- `execution`
- `margin`

The watchdog cannot submit, amend, cancel, or close orders. It does not call a
dead-man switch. No authenticated production endpoint exists in this module.

## Credential boundary

Startup fails unless all three conditions hold:

1. `BITMEX_TESTNET` is exactly `true`.
2. `BITMEX_TESTNET_API_KEY` is non-empty.
3. `BITMEX_TESTNET_API_SECRET` is non-empty.

Generic credential variable names are not accepted. Use a dedicated BitMEX API
key with the default read-only permissions. Do not grant order, order-cancel,
or withdrawal permissions to the watchdog key. IP-lock the key when the host
has a stable egress address.

Authentication uses `api-key`, `api-expires`, and `api-signature` headers on
the WebSocket upgrade. The signature is HMAC-SHA256 over
`GET/realtime<expires>`. Credentials never enter the URL, status file, alert
payload, or error output. Every upgrade redirect is rejected before the client
can reuse those custom authentication headers at another origin.

## Running

From the repository root, with the Testnet variables available to the process:

```powershell
py -3.12 exchange_watchdog.py
```

The process is intentionally long-running. A service manager should restart it
if it exits. Only one instance should publish the status file for a given host.

## State processing

BitMEX table messages are applied in this order:

- `partial` establishes the table keys and atomically replaces its snapshot.
- `insert` requires a new complete row.
- `update` requires an existing keyed row.
- `delete` requires an existing keyed row.

Unknown tables, actions, keys, rows, or control messages fail closed. Deltas
before a `partial` snapshot also fail closed. Each reconnect starts with empty
state and must receive successful subscription acknowledgements and a partial
snapshot for all four tables within the synchronization deadline.

The in-memory tables deliberately discard fields that are not needed for the
summary. Order state retains only the symbol, status, remaining contracts, and
the private key needed to apply deltas. Position state retains only the symbol,
current contracts, and its private keys. Execution and margin state retain only
their private keys. Private keys are never serialized.

An idle connection is challenged with the documented raw `ping` command. A
missing bounded `pong`, a malformed frame, failed authentication, failed
subscription, closed transport, or stale connection publishes a failed status
and triggers a reconnect. Reconnect delay doubles from one second and is capped
at 30 seconds.

## Sanitized status

Status is written atomically to:

```text
data/watchdog_status.json
```

The file is schema-versioned and contains exactly:

```json
{
  "schema_version": 1,
  "environment": "testnet",
  "state": "healthy",
  "generated_at_utc": "2026-07-14T00:00:00+00:00",
  "last_message_at_utc": "2026-07-14T00:00:00+00:00",
  "freshness_seconds": 0.0,
  "fresh": true,
  "subscriptions": {
    "order": true,
    "position": true,
    "execution": true,
    "margin": true
  },
  "position_contracts": 0,
  "open_order_count": 0,
  "last_event_type": "synchronized"
}
```

Allowed states are `starting`, `synchronizing`, `healthy`, `stale`, and
`failed`. Before a table snapshot is available, its derived count is `null`.
The position contract quantity is signed. Unknown non-terminal order statuses
are counted as open, while a terminal order with remaining quantity is treated
as a protocol failure.

The file never contains API credentials, account IDs, order IDs, execution
IDs, prices, balances, fees, raw messages, or exchange error bodies. Publication
uses a temporary file in the same directory, flushes it, and replaces the old
document atomically. A publication failure stops the process because monitoring
without a trustworthy status surface is not acceptable.

## Optional alert webhook

Set `WATCHDOG_ALERT_WEBHOOK_URL` to an HTTPS endpoint to receive failure event
notifications. User information and URL fragments are rejected. Redirects are
not followed, the total request timeout is five seconds, and repeated identical
events are limited to one alert per five minutes.

Alert bodies contain only the schema version, fixed source and environment,
UTC generation time, and one allowlisted event type such as `auth_error`,
`subscription_error`, `protocol_error`, `transport_error`, or `stale`. They do
not include the local status snapshot or exception text.

Treat a webhook URL containing a provider token as a secret. Configure it in
the host secret manager and never commit it.

## Verification

Run the focused tests with:

```powershell
py -3.12 -m unittest test_exchange_watchdog -v
```

The tests cover the Testnet gate, authentication signature, all four delta
actions, atomic in-memory behavior, malformed messages, staleness, status
redaction, atomic file replacement, and alert redaction.

## Operational limits

This watchdog improves independent visibility. It does not make the bot ready
for real funds and it does not replace REST reconciliation, operator paging,
host monitoring, tested recovery drills, or complete exit accounting. A
`healthy` watchdog only proves that the subscribed Testnet connection is fresh
and internally consistent enough to summarize.

Official protocol references:

- [BitMEX WebSocket API](https://www.bitmex.com/app/wsAPI)
- [BitMEX API key usage](https://www.bitmex.com/app/apiKeysUsage)
- [BitMEX Testnet API endpoints](https://support.bitmex.com/hc/en-gb/articles/6205448296605-Does-BitMEX-Have-An-API)
