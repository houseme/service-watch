# monitorctl

Control CLI for [service-watch](https://github.com/houseme/service-watch):
queries and drives the [monitor-agent](https://github.com/houseme/service-watch/tree/main/crates/monitor-agent)
HTTP control plane, plus a local hardware snapshot tool.

## Usage

```text
monitorctl [--url <URL>] <COMMAND>
```

`--url` defaults to `http://127.0.0.1:9170` (the agent's control-plane address).

## Commands

| Command | Description |
|---|---|
| `status [--json]` | Status table (or raw JSON) of all targets and checks |
| `watch [--interval <secs>] [--json] [--exit-on-error]` | Continuous polling view; `--json` emits JSON Lines; Ctrl-C exits 0 |
| `check <target>` | Probe a target immediately |
| `restart <target>` | Graceful stop + start of a supervised target |
| `reset <target>` | Clear Fatal latch and restart budget (after fixing the root cause) |
| `metrics [--prom]` | JSON metrics (or Prometheus format) |
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
```

Operational semantics and troubleshooting: [RUNBOOK.md](https://github.com/houseme/service-watch/blob/main/docs/RUNBOOK.md).
Licensed under Apache-2.0.
