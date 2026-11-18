# monitorctl

Control CLI for [service-watch](https://github.com/houseme/service-watch):
queries and drives the [monitor-agent](https://github.com/houseme/service-watch/tree/main/crates/monitor-agent)
HTTP control plane, plus a local hardware snapshot tool.

## Usage

```text
monitorctl [--url <URL>] [--token <TOKEN>] [--insecure] [--client-cert <PATH> --client-key <PATH>] <COMMAND>
```

`--url` defaults to `http://127.0.0.1:9170` (the agent's control-plane address).

## mTLS client certificate (F38)

When the agent is configured with `[control] client_ca_cert` (on top of TLS), the
TLS handshake requires a client certificate issued by that CA. Pass it with the
global `--client-cert <PATH>` and `--client-key <PATH>` flags (both or neither;
PEM, PKCS#8/#1/SEC1 keys accepted via `reqwest::Identity::from_pem`). Orthogonal
to `--insecure` (server self-signed) and `--token` (Bearer) — all three can
stack.

## Access token (F23)

When the agent is configured with `[control] token`, every endpoint except
`GET /health` requires `Authorization: Bearer <token>`. Pass the token with the
global `--token <TOKEN>` flag or the `SERVICE_WATCH_TOKEN` environment variable
(the flag wins when both are set; blank values are treated as unset; prefer the
environment variable in production — command-line arguments are visible in
process listings and shell history). All
requests then carry the header; an agent without a configured token ignores it.
A wrong or missing token yields HTTP 401 and monitorctl exits with code 2,
printing a hint. The token value is never written to logs.

## Commands

| Command | Description |
|---|---|
| `status [--json]` | Status table (or raw JSON) of all targets and checks |
| `watch [--interval <secs>] [--json] [--exit-on-error]` | Continuous polling view; `--json` emits JSON Lines; Ctrl-C exits 0 |
| `check <target>` | Probe a target immediately |
| `restart <target>` | Graceful stop + start of a supervised target |
| `reset <target>` | Clear Fatal latch and restart budget (after fixing the root cause) |
| `metrics [--prom]` | JSON metrics (or Prometheus format) |
| `silence <target> --duration <secs> [--reason <text>]` | Suppress Alert/Escalate/Flap notifications for a target (monotonic deadline; Resolve and Fatal-latch alerts always pass) |
| `unsilence <target>` | End a silence early |
| `journal [--tail <n>] [--target <name>] [--type <type>] [--since <epoch>] [--json]` | Tail query over the journal (incl. rotated gen-1); table or JSON Lines |
| `config [--json]` | Effective-config view (secrets shown as `***redacted***`, not re-ingestible) |
| `dump-snapshot [--config <path>]` | One local hardware snapshot, no agent required |

`check` / `restart` / `reset` return 202 when the command is queued — confirm
the outcome via `status` and the journal. `restart` on a Fatal target returns 409.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Config error (`dump-snapshot --config <path>` load/validation failure) |
| 2 | Runtime error (incl. 409 / non-2xx responses) |
| 3 | Target not found (reserved) |
| 4 | Agent unreachable |

Logs are silent unless `RUST_LOG` is set (output goes to stderr).

## Features

| Feature | Default | Effect |
|---|---|---|
| `collect-deep-hardware` | no | Deep hardware sensors in `dump-snapshot` |

## Examples

```bash
monitorctl status
monitorctl watch --interval 2
monitorctl metrics --prom
monitorctl --url http://127.0.0.1:9270 status
monitorctl --token "$(cat /etc/service-watch/control-token)" status
SERVICE_WATCH_TOKEN=secret monitorctl status
```

Operational semantics and troubleshooting: [RUNBOOK.md](https://github.com/houseme/service-watch/blob/main/docs/RUNBOOK.md).
Licensed under Apache-2.0.
