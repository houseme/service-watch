# monitor-agent

The service-watch monitoring agent — a single foreground binary that runs the
collect → evaluate → alert → recover loop and serves the local HTTP control
plane. It is meant to be kept alive by your system service manager
(systemd / launchd / SCM), never daemonized: on exit it leaves supervised child
processes running and adopts or reconciles them on the next start.

## CLI

```text
monitor-agent [FLAGS]
```

| Flag | Description |
|---|---|
| `--config <PATH>` | Config file (TOML). Defaults to `./service-watch.toml`; falls back to built-in defaults if absent |
| `--check-config <PATH>` | Validate config offline and exit (`config ok`, exit code 0 on success) |
| `--dump-snapshot` | Collect one system snapshot, print pretty JSON, exit (no runtime started) |
| `--log-json` | Structured JSON logs (equivalent to `[log].json = true`) |
| `--service-run`, `--service-name`, `--log-file` | Windows SCM form (hidden; requires the `windows-service` feature, used by the service installer) |

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Clean exit |
| 1 | Config error |
| 2 | Runtime / critical persistence error |
| 130 | SIGINT/SIGTERM graceful shutdown (treated as success by the systemd unit) |

## Control plane (default `127.0.0.1:9170`)

| Endpoint | Purpose |
|---|---|
| `GET /health` | Liveness (HTTP 200) |
| `GET /ready` | Readiness JSON (scheduler started) |
| `GET /status` | Current target + health state |
| `GET /metrics` | JSON metrics (`?format=prometheus` for Prometheus) |
| `GET /metrics/history?range=300` | Historical metrics |
| `POST /v1/restart/{target}` | Queue graceful restart of a supervised target (409 if Fatal) |
| `POST /v1/check/{target}` | Force bound checks to run now |
| `POST /v1/reset/{target}` | Clear Fatal latch + restart budget (202; outcome via journal) |
| `POST /v1/reload` | Hot-reload check/service/watch topology |
| `GET /` | Read-only HTML status page |

Operate it with [monitorctl](https://github.com/houseme/service-watch/tree/main/crates/monitorctl).

## Features

| Feature | Default | Effect |
|---|---|---|
| `collect-sysinfo`, `probe-http`, `notify-webhook`, `control-http` | yes | Passthrough to [monitor-core](https://github.com/houseme/service-watch/tree/main/crates/monitor-core); disable to cut reqwest/axum |
| `collect-deep-hardware` | no | Deep hardware sensors (Linux hwmon / macOS AppleSMC) |
| `windows-service` | no | Windows Service (SCM-aware) form for the agent itself — Windows only |

## Quick start

```bash
cargo build --release -p monitor-agent -p monitorctl
./target/release/monitor-agent --check-config config/config.example.toml
./target/release/monitor-agent --config service-watch.toml
```

Configuration schema: [`config/config.example.toml`](https://github.com/houseme/service-watch/blob/main/config/config.example.toml).
Installation per platform: [deployment guides](https://github.com/houseme/service-watch/tree/main/docs).
Semantics: [DESIGN.md](https://github.com/houseme/service-watch/blob/main/DESIGN.md). Licensed under Apache-2.0.
