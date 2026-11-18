# monitor-core

Cross-platform service & hardware monitoring library. The core of
[service-watch](https://github.com/houseme/service-watch): every capability
line lives here, and the [monitor-agent](https://github.com/houseme/service-watch/tree/main/crates/monitor-agent)
binary is the reference consumer.

## Module map

| Module | Responsibility |
|---|---|
| `config` | TOML config parsing + validation (schema, constraint checks) |
| `model` | Snapshot / alert / control data types shared across the stack |
| `collect` | CPU, memory, disk, network, temperature and process metrics; missing data is surfaced as `Option` + machine-readable degradation reasons, never fabricated |
| `eval` | Check evaluation, incident lifecycle (dedup, escalation, recovery, flap suppression) |
| `alert` | Webhook delivery with exponential-backoff retries, dead-man switch, replay spool |
| `recover` | Supervise state machine: confirmation windows, startup grace, backoff, sliding restart budget, Fatal latch, `(pid, start_time)` identity checks, orphan reconciliation |
| `control` | HTTP control plane (`/`, `/health`, `/ready`, `/status`, `/metrics`, `/metrics/history`, `/v1/restart|check|reset/{target}`, `/v1/reload`, `/v1/silence/{target}` (POST/DELETE), `/v1/journal`, `/v1/config`) + read-only status page; optional TLS/mTLS |

## Feature flags

| Feature | Default | Effect |
|---|---|---|
| `collect-sysinfo` | yes | System metrics via the `sysinfo` crate |
| `probe-http` | yes | HTTP health probes (pulls `http-client`) |
| `notify-webhook` | yes | Webhook alert delivery (pulls `http-client`) |
| `control-http` | yes | Axum-based control plane |
| `collect-deep-hardware` | no | Deep sensors: Linux hwmon, macOS AppleSMC (fans, voltages); Windows degrades to machine-readable `unsupported` |
| `http-client` | no | Shared reqwest/rustls client (enabled by `probe-http` / `notify-webhook`) |
| `scm-target` | no | Native Windows Service targets (start/stop/status via SCM); dependency is only compiled on Windows |

A fully reduced build works too: `--no-default-features --features collect-sysinfo`
compiles without any C-toolchain dependency (no reqwest/axum).

## Usage

```toml
[dependencies]
monitor-core = { version = "0.3", default-features = false, features = ["collect-sysinfo", "probe-http"] }
```

See [`config/config.example.toml`](https://github.com/houseme/service-watch/blob/main/config/config.example.toml)
for the full configuration schema, [DESIGN.md](https://github.com/houseme/service-watch/blob/main/DESIGN.md)
for the state-machine and recovery semantics, and the [deployment guides](https://github.com/houseme/service-watch/tree/main/docs)
for platform installation.

## Constraints

- Rust edition 2024; no MSRV declared (workspace rule: members must not set `rust-version`).
- Linux / macOS / Windows supported; platform-specific code is `#[cfg]`-gated.
- Licensed under Apache-2.0.
