# service-watch

Cross-platform service and hardware monitoring tool built with Rust 2024. Core logic lives in `monitor-core`; `monitor-agent` runs the scheduling loop and the control plane; `monitorctl` provides the query and operations CLI.

- CPU, memory, disk, network, temperature and process metrics collection — missing data is surfaced via `Option` with machine-readable degradation reasons.
- HTTP, TCP, process-metrics and process-liveness checks, with incident deduplication, escalation, recovery notification, flap suppression and webhook retries.
- supervise-style process recovery: consecutive confirmations, startup grace, backoff, sliding restart budget, Fatal latch and dependency startup ordering.
- Control plane, read-only web status page, Prometheus exposition, historical metrics, terminal watch, and hot reload of the target topology.
- Optional `collect-deep-hardware` deep collection providing Linux hwmon and macOS AppleSMC fan and voltage readings.

## Quick start

```bash
cargo build --release -p monitor-agent -p monitorctl
./target/release/monitor-agent --check-config config/config.example.toml
./target/release/monitor-agent --dump-snapshot
```

Create `service-watch.toml` and replace `command` with the actual executable path:

```toml
[agent]
data_dir = "./data"

[[service]]
name = "my-app"
command = "/usr/local/bin/my-app"

[[check]]
name = "my-app"
kind = "http"
url = "http://127.0.0.1:8080/healthz"
interval = "10s"
timeout = "5s"
```

When a check shares a name with a service, its health verdict feeds that service's recovery decisions; independently named checks provide alerting only. The full configuration covers webhooks, dependencies, restart policies and attach targets — adjust the paths in the example to your machine. [Full configuration reference](config/config.example.toml)

```bash
./target/release/monitor-agent --config service-watch.toml

# in another terminal
./target/release/monitorctl status
./target/release/monitorctl watch --interval 2
./target/release/monitorctl metrics --prom
./target/release/monitorctl restart my-app
./target/release/monitorctl reset my-app
```

Open the [local status page](http://127.0.0.1:9170/) in a browser. A Fatal target requires handling the fault first and then a `reset`. `restart`, `check` and `reset` returning 202 means the command was queued; verify the outcome via `status` and the journal.

## Scope & boundaries

- Attach targets are monitored only. Supervised child processes survive agent exit; after a restart the agent reconciles orphans from journal identities plus fresh collection evidence.
- `SIGHUP` and `POST /v1/reload` apply check, service and watch topology changes; the other config sections require a restart. Redefining a service waits for the old instance to be reaped before starting the new one.
- JSON metrics at `/metrics`, Prometheus format via `?format=prometheus`, history at `/metrics/history?range=300`. The `[metrics]` periodic push is implemented: with `enabled = true` and a `url` configured, a MetricsReport JSON is POSTed every `interval` (single attempt, failures overwritten next round, consecutive-failure edges write a journal audit).
- Webhook failure events are written to a spool; replayed records are deleted on confirmed success and expire after 7 days. In-memory events not yet flushed before a force kill still have a loss window.
- The control plane listens on loopback by default; the HTTP interface relies on the local trust boundary. An optional `[control] token` enables Bearer authentication on every endpoint except `GET /health` (monitorctl: `--token` flag or `SERVICE_WATCH_TOKEN`).

## Platforms & optional builds

| Capability | Linux | macOS | Windows |
|---|---|---|---|
| Basic hardware & process collection | sysinfo | sysinfo | sysinfo |
| Temperature | hwmon-dependent | model-dependent | explicit degradation |
| Fans, voltage | hwmon, read-only | AppleSMC, read-only, model-dependent | explicit degradation |
| supervise recovery | process group | process group | Job Object |
| system daemon | systemd | launchd | SCM, requires `windows-service` |

```bash
cargo build --release -p monitor-agent -p monitorctl --features collect-deep-hardware
cargo run -p monitor-agent --features collect-deep-hardware -- --dump-snapshot

# Build the Windows SCM service entry point
cargo build --release -p monitor-agent --features windows-service
```

Deep collection is off by default. Missing sensors, permission denials, faults and invalid values all degrade with machine-readable reasons. Linux voltage keeps the driver-reported chip-pin semantics and converts to volts. macOS uses common SMC keys filtered by type, with a ±100 V plausible voltage range; different machines expose different sensors.

The default Windows HTTP/TLS build uses rustls with aws-lc-rs and requires NASM and CMake. Locally verified toolchain: stable rustc (1.99.1 first verified 2026-11-12, 1.100.0 re-verified 2026-11-18); treat the verification records as the source of truth. Version and platform verification results are in the review records.

## Documentation

| Document | Contents |
|---|---|
| [docs/USAGE.md](docs/USAGE.md) | Bilingual (English / 中文) usage guide: build, run, configure, operate |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | Full deployment & operations guide: configuration, three-platform installation, daily ops, upgrades and troubleshooting |
| [ROADMAP.md](ROADMAP.md) | Feature specs, acceptance criteria, progress and backlog |
| [docs/REVIEW-2026-11-12.md](docs/REVIEW-2026-11-12.md) | Multi-expert review, fix and verification records |
| [DESIGN.md](DESIGN.md) | Configuration, ownership, state machines and platform contracts |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | Hot reload, Fatal, journal and delivery incident handling |
| [docs/deploy/](docs/deploy/README.md) | systemd, launchd, Windows deployment and performance scripts |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development & verification commands |
| [AGENT.md](AGENT.md) | Engineering hard constraints |
| [EXECUTION.md](EXECUTION.md) | Phased execution plan |
| [BENCHMARK.md](BENCHMARK.md) | Selection rationale |

All workspace crates use Rust 2024 and Apache-2.0, and omit `rust-version` from their manifests. Safe recovery uses process identity verification, and the agent lifecycle is independent of the monitored services.

## License

[Apache-2.0](LICENSE)
