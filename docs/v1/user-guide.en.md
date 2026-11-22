# service-watch User Guide (English)

> Applies to the main branch (v0.3.0, including all v0.4–v0.8 features). The semantic authority is [DESIGN.md](../../DESIGN.md); deployment details live in [docs/DEPLOYMENT.md](../DEPLOYMENT.md); incident handling lives in [docs/RUNBOOK.md](../RUNBOOK.md). Chinese version: [user-guide.zh.md](user-guide.zh.md).
>
> This guide targets operators and users of service-watch and is organized by function and usage: concepts, build & install, quick start, configuration reference, alerting, metrics, the control plane, recovery semantics, deployment, operations, and troubleshooting.

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Core Concepts](#2-core-concepts)
3. [Build & Install](#3-build--install)
4. [Quick Start](#4-quick-start)
5. [Configuration Reference](#5-configuration-reference)
6. [Targets: check / service / watch](#6-targets-check--service--watch)
7. [Alerts & Notifications](#7-alerts--notifications)
8. [Noise Control: Silence & Maintenance Windows](#8-noise-control-silence--maintenance-windows)
9. [Metrics & Observability](#9-metrics--observability)
10. [Control Plane & monitorctl](#10-control-plane--monitorctl)
11. [Recovery Semantics: Process Revival & Fatal](#11-recovery-semantics-process-revival--fatal)
12. [Hot Reload](#12-hot-reload)
13. [Running as a System Service](#13-running-as-a-system-service)
14. [Daily Operations](#14-daily-operations)
15. [Troubleshooting](#15-troubleshooting)
16. [Upgrade, Rollback & Uninstall](#16-upgrade-rollback--uninstall)
17. [Appendix](#17-appendix)

---

## 1. Introduction

service-watch is a **cross-platform service and hardware monitoring tool** written in Rust 2024, covering three capability lines:

- **Hardware monitoring**: CPU, memory, disk, network, temperature, and process metric collection; unavailable data is surfaced with machine-readable degradation reasons — never faked as 0.
- **Anomaly alerting**: HTTP / TCP / process-metrics / process-liveness checks with incident deduplication, escalation, recovery notification, flap suppression, webhook retries, and persistent replay of failed deliveries.
- **Recovery**: supervise-style process revival — consecutive confirmations, startup grace, exponential backoff, a sliding restart budget, a Fatal terminal latch, and dependency-ordered startup.

| Component | Role |
|---|---|
| `monitor-agent` | Daemon: runs the `collect → evaluate → alert → recover` loop, serves a local HTTP control plane, and supervises managed processes |
| `monitorctl` | Operator CLI: queries and operates the control plane; also provides a local hardware snapshot tool |
| `monitor-core` | Library: all core logic — config, collect, eval, alert, recover, control plane |

**Design boundaries (important)**:

- External observer: the agent is independent of the monitored services' lifecycle; the agent exiting never kills the monitored services, and the services are not responsible for reviving the agent.
- Attach (`watch`) targets are **monitored only — never killed or restarted**; only `[[service]]` supervised targets are revived by the agent.
- The agent has no daemonize mode; it runs in the foreground under systemd / launchd / Windows SCM.
- Supervised targets must not daemonize (double-forking makes the agent misread an instant exit and respawn repeatedly) — run them as foreground processes.
- The control plane listens on loopback `127.0.0.1:9170` by default and relies on the local trust boundary; for remote use, stack TLS / Bearer token / mTLS trust layers (see §10.4).

---

## 2. Core Concepts

With these concepts in place, the configuration and operations in later chapters become straightforward.

### 2.1 Three Kinds of Targets

| Target kind | Config section | Agent behavior |
|---|---|---|
| **check** | `[[check]]` | Periodic probe (http / tcp / process) producing health states and alerts; does not manage processes |
| **service** | `[[service]]` | A process spawned (and revived) by the agent: monitoring + alerting + **automatic crash revival**, governed by the restart policy |
| **watch** | `[[watch]]` | "Adopted" monitoring of an existing process: read-only, never killed/restarted; identified by `pid` or `pidfile` |

**Same-name binding rule**: when a check shares a `name` with a service, that check's health verdict feeds the service's recovery decisions (sustained health failure can trigger revival); independently named checks provide alerting only.

### 2.2 Check State Machine & Incidents

Each check maintains an independent state machine:

```text
Ok ──1 failure──▶ Warning ──failure_threshold consecutive failures──▶ Critical
 ▲                  │                │
 └────success_threshold consecutive successes──────────────────────┘
```

- `incident_key = hash(target, check)` — the same target + check always maps to the same key; a Warning → Critical escalation is **the same incident** escalating, never a second incident.
- Incident lifecycle: `open(Warning) → escalate(Critical) → resolve(Ok)`; event categories `Alert / Escalate / Resolve / Flap`.
- All timing uses a monotonic clock; host wall-clock jumps never cause "permanent suppression" or "alert storms".

### 2.3 Recovery State Machine & the Fatal Latch

Supervised targets advance through `running → suspect → recovering → restarting → starting → crashloop → fatal` (full transition table in §11). Key points:

- A health failure triggers recovery only after `confirmations` (default 2) consecutive observations; failures inside `startup_grace` don't enter the state machine.
- Reaching `max_restarts` (default 5) automatic restarts within the sliding `restart_window` (default 10m) → **the Fatal latch**.
- Fatal is terminal: it **never auto-recovers** (the window sliding past doesn't help, nor does an agent restart); the only exit is a manual `monitorctl reset` (audited).

### 2.4 The journal (Event Log)

`{data_dir}/journal.jsonl` is an append-only JSONL event log recording nine event types: `incident / restart / fatal / reset / reset_targets / process_identity / spawn_intent / spawn_outcome / audit`. It is the fact source of the recovery domain: after a restart the agent replays the journal to restore Fatal latches, budgets, and process identities for orphan reconciliation. The journal supports built-in single-generation rotation (default 10 MiB → `journal.1.jsonl`) and tail queries via `monitorctl journal` (see §9.4, §14).

### 2.5 The Control Plane

The agent embeds an HTTP control plane (default `127.0.0.1:9170`): the status page, status/metric queries, operation commands (restart/check/reset/silence), journal queries, and the hot-reload endpoint. `monitorctl` is its thin client.

---

## 3. Build & Install

### 3.1 Building from Source

Requires a Rust stable toolchain.

```bash
cargo build --release -p monitor-agent -p monitorctl
# artifacts: ./target/release/monitor-agent and ./target/release/monitorctl
```

### 3.2 Optional Features

| Feature | Effect |
|---|---|
| `collect-deep-hardware` | Deep sensors: Linux hwmon / macOS AppleSMC fan speeds and voltages (off by default) |
| `windows-service` | Windows SCM service entry point (`--service-run`, Windows only) |

```bash
# Deep hardware collection (usable by both agent and monitorctl)
cargo build --release -p monitor-agent -p monitorctl --features collect-deep-hardware

# Windows SCM service form
cargo build --release -p monitor-agent --features windows-service
```

**Windows build note**: the default HTTP/TLS stack is rustls + aws-lc-rs, which requires NASM and CMake (`choco install -y nasm cmake`).

### 3.3 Platform Capability Matrix

| Capability | Linux | macOS | Windows |
|---|---|---|---|
| CPU/memory/disk/network/process | sysinfo | sysinfo | sysinfo |
| Temperature | hwmon-dependent | available by default (SMC/HID, plausibility-filtered) | usually empty, machine-readable degradation |
| Fans/voltage (deep collection) | hwmon read-only | AppleSMC read-only, model-dependent | machine-readable unsupported |
| supervise isolation | process group | process group | Job Object |
| Graceful stop | `killpg(SIGTERM)` | same | TerminateProcess baseline; CTRL_BREAK attempted only for `console=true` |
| Force kill | `killpg(SIGKILL)` | same | `TerminateJobObject` |
| System daemon | systemd | launchd | SCM (`windows-service` feature) |

### 3.4 Release Artifacts

GitHub Releases provide Linux x86_64 and Windows x86_64 assets (tag-triggered builds); macOS assets resume from the next tag after v0.3.0. You can also build yourself following the platform deployment docs.

---

## 4. Quick Start

### 4.1 Five-Minute Start (standalone HTTP monitoring, no process management)

```bash
# 1) Validate the example config offline (exit 0 = ok)
./target/release/monitor-agent --check-config config/config.example.toml

# 2) One-shot hardware snapshot (no runtime started) — verifies the collection layer
./target/release/monitor-agent --dump-snapshot

# 3) Create a minimal config service-watch.toml
```

```toml
[agent]
data_dir = "./data"

[control]
enabled = true
listen = "127.0.0.1:9170"

[alert]
webhook_url = ""              # set a real address to enable delivery; empty = journal/logs only
webhook_format = "json"       # json | slack
failure_threshold = 3
success_threshold = 2
renotify_interval = "15m"

[[check]]
name = "external-api"
kind = "http"
url = "http://127.0.0.1:8080/healthz"
expected_status = "200-299"
interval = "10s"
timeout = "5s"
```

```bash
# 4) Start in the foreground
./target/release/monitor-agent --config service-watch.toml

# 5) Verify from another terminal
curl -fsS http://127.0.0.1:9170/health    # 200 = process alive (shallow)
curl -fsS http://127.0.0.1:9170/ready     # 200 = config loaded + scheduler started
./target/release/monitorctl status
./target/release/monitorctl metrics --prom
```

Open [http://127.0.0.1:9170/](http://127.0.0.1:9170/) in a browser for the read-only status page (with sparklines of key metrics).

### 4.2 Adding Process Revival (service + same-name check)

```toml
[[service]]
name = "my-app"
command = "/srv/my-app/bin/my-app"   # must run in the foreground; daemonizing targets forbidden
args = ["--port", "8080"]
workdir = "/srv/my-app"
env = { RUST_LOG = "info" }

[service.restart]
min_uptime = "10s"
startup_grace = "30s"
stop_grace = "15s"
backoff_initial = "1s"
backoff_max = "60s"
max_restarts = 5
restart_window = "10m"
confirmations = 2
hung_action = "restart"

[[check]]
name = "my-app"                      # same name as the service → feeds recovery decisions
kind = "http"
url = "http://127.0.0.1:8080/healthz"
interval = "10s"
timeout = "5s"
```

Common operations:

```bash
./target/release/monitorctl restart my-app   # manual restart (202 = queued)
./target/release/monitorctl check my-app     # make the bound check due immediately
./target/release/monitorctl reset my-app     # after Fatal: fix the root cause first, then reset
```

### 4.3 Monitoring Existing Processes (watch, never touched)

```toml
[[watch]]
name = "existing-app"
pidfile = "/run/existing-app.pid"    # or pid = 1234

[[check]]
name = "existing-app"
kind = "process"
process = "existing-app"             # "pid:1234" also works
cpu_threshold = 90.0
mem_threshold_mb = 1024
interval = "10s"
timeout = "5s"
```

---

## 5. Configuration Reference

### 5.1 Loading & Validation

- The default config path is `./service-watch.toml`; override with `--config <path>`.
- **Unknown keys are rejected** (`deny_unknown_fields`) — typos fail fast at `--check-config`.
- Durations accept `"10s"` / `"5m"`-style strings.
- Full annotated template: [config/config.example.toml](../../config/config.example.toml) (maps 1:1 to DESIGN.md §4; a unit test guarantees it parses).

```bash
./target/release/monitor-agent --check-config service-watch.toml   # prints config ok, exit 0
```

### 5.2 Section Overview

| Section | Purpose | Key fields |
|---|---|---|
| `[agent]` | Data location | `data_dir` (journal/spool root, default `./data`), `journal_max_size` (rotation threshold, default 10 MiB, `0` = disable) |
| `[log]` | Logging | `json` = structured JSON logs |
| `[control]` | HTTP control plane | `enabled`, `listen` (loopback by default), `token` (optional Bearer), `tls_cert/tls_key` (optional HTTPS), `client_ca_cert` (optional mTLS) |
| `[collect]` | Collection cadence | `interval` (default 5s) |
| `[alert]` | webhook alert delivery | see §7 |
| `[metrics]` | periodic push / history | see §9.3, §9.5 |
| `[[check]]` | check targets (repeated) | see §6 |
| `[[service]]` | supervised targets (repeated) | see §6 |
| `[[watch]]` | attach targets (repeated) | see §6 |
| `[[alert.routes]]` | per-target alert routing (repeated) | see §7.4 |
| `[[maintenance]]` | recurring maintenance windows (repeated) | see §8.2 |

### 5.3 Defaults

| Parameter | Default | Parameter | Default |
|---|---|---|---|
| `agent.journal_max_size` | 10 MiB (`0` = rotation off) | `alert.failure_threshold` | 3 |
| `alert.timeout` | 10s | `alert.success_threshold` | 2 |
| `alert.retries` | 3 | `alert.renotify_interval` | 15m |
| `alert.flap_window` | 30m | `alert.flap_threshold` | 5 |
| `restart.min_uptime` | 10s | `restart.backoff_initial` | 1s |
| `restart.backoff_max` | 60s | `restart.max_restarts` | 5 |
| `restart.restart_window` | 10m | `restart.stop_grace` | 15s |
| `restart.startup_grace` | 30s | `restart.confirmations` | 2 |
| `activate.timeout` | 10s | `activate.verify_for` | 30s |
| `control.listen` | `127.0.0.1:9170` | `metrics.interval` | 60s |
| `maintenance.duration` | 1h (validated floor 60s) | `maintenance.days` | unset = every day |

### 5.4 Main Validation Rules (violations are config errors, exit 1)

1. Every check: `timeout < interval`; check / collect / `[metrics]` (when enabled) intervals ≥ 100ms.
2. http check: `url` valid with http/https scheme; `expected_status` shaped like `"200-299"`.
3. process check: `process` non-empty; at least one of cpu/mem thresholds; CPU threshold finite and non-negative.
4. supervise: `command` non-empty; `backoff_initial ≤ backoff_max`; `startup_grace ≥ min_uptime`; `depends_on` references must exist, no self-reference, no cycles.
5. `hung_action = "activate"` requires `[service.activate]`; the hook must set exactly one of command/url.
6. check/service/watch names unique and non-empty within their own space; `data_dir` non-empty and creatable; alert thresholds ≥ 1; `alert.retries ≤ 100`; webhook/deadman URLs http/https.
7. `[[watch]] pidfile` must be a non-empty path.
8. `[control] tls_cert/tls_key` come in pairs; `client_ca_cert` requires TLS to be configured.
9. `[[alert.routes]]`: `match` non-blank, url scheme valid; `webhook_secret` / route `secret` / `[metrics] secret` must not be pure whitespace.
10. `[alert] headers` / route `headers`: names must be RFC 7230 tokens; values non-empty, no CR/LF/control characters (visible ASCII + TAB only); header names differing only by case are rejected within and across tables.
11. `[[maintenance]]`: `targets` non-empty, `start_utc` two-digit `HH:MM` (UTC), `duration` ≥ 60s, `days` only `mon tue wed thu fri sat sun`.

---

## 6. Targets: check / service / watch

### 6.1 check

```toml
[[check]]
name = "api-health"                     # globally unique
kind = "http"                           # http | tcp | process
url = "http://127.0.0.1:8080/healthz"   # required for http
expected_status = "200-299"             # optional, default 200-299 inclusive; probes follow redirects (≤10 hops), the final status decides
interval = "10s"                        # must be > timeout
timeout = "5s"
```

Three `kind` values:

- **http**: `url` + `expected_status`. Point `url` at the final endpoint when you need strict verdicts.
- **tcp**: `addr = "127.0.0.1:5432"`, connect-based verdict.
- **process**: thresholds `cpu_threshold` / `mem_threshold_mb` on a named process (`process = "my-app"` — a configured service/watch name — or `pid:1234`), at least one threshold.

**In-flight probe cap of 1**: if the previous probe hasn't finished, this round is skipped and counted in `probe_skipped`; `timeout < interval` is enforced by validation, so probes never pile up. After host sleep/wake, the first probe round is discarded and recorded as degraded.

### 6.2 service

```toml
[[service]]
name = "my-app"
command = "/usr/local/bin/my-app"       # required; foreground process form
args = ["--port", "8080"]
workdir = "/srv/my-app"
env = { RUST_LOG = "info" }
console = false                         # Windows: true = CREATE_NEW_PROCESS_GROUP, allows CTRL_BREAK attempt
depends_on = ["db"]                     # optional: start only after all deps are Running with live processes

[service.restart]                       # every field optional; defaults in §5.3
min_uptime = "10s"                      # dying sooner = crash-loop suspect (fast_exit)
backoff_initial = "1s"
backoff_max = "60s"                     # exponential ×2 cap
max_restarts = 5                        # max automatic restarts within the sliding restart_window
restart_window = "10m"
stop_grace = "15s"                      # graceful → force-kill escalation deadline
startup_grace = "30s"                   # warm-up: failures inside don't enter the state machine
confirmations = 2                       # consecutive observations before health-triggered recovery
hung_action = "restart"                 # restart | activate

[service.activate]                      # required when hung_action = "activate"
command = "/usr/local/bin/kick-my-app"  # or url = "http://..."; exactly one of the two
timeout = "10s"
verify_for = "30s"                      # observation window; still failing → Restart
```

Key points:

- **Dependency orchestration** (`depends_on`): a service cold-starts only after all dependencies are Running with live process evidence; a Fatal dependency → dependents never auto-start (no cascading failure; manual intervention). A control-plane restart of a service first stops its (transitive) dependencies in reverse topological order and cascades them back once it is Running.
- **Hung handling** (`hung_action`): `restart` performs the full stop→spawn cycle; `activate` runs the activation hook (command or HTTP) first — health recovering within `verify_for` → Running, otherwise → Restart and count against the budget.

### 6.3 watch

```toml
[[watch]]
name = "systemd-nginx"
pidfile = "/run/nginx.pid"              # or pid = 1234
```

Monitoring and alerting only; never killed or restarted — process management stays with the original manager (e.g. systemd).

---

## 7. Alerts & Notifications

### 7.1 Thresholds & Notification Rules

| Rule | Default | Meaning |
|---|---|---|
| First notification is never suppressed | — | an incident's first alert always ships (no blind spot when an incident recurs right after Resolve) |
| `failure_threshold` | 3 | consecutive failures → Warning (alert) → Critical (escalate) |
| `success_threshold` | 2 | consecutive successes → Ok (resolve); Warning → Ok also emits Resolve |
| `renotify_interval` | 15m | repeat interval for same-severity notifications while an incident stays open; **Escalate (Warning→Critical) always ships** |
| `flap_window` / `flap_threshold` | 30m / 5 | reaching the threshold inside the window → one Flap summary only (with count); suppression exits automatically |

Resolve semantics: references the original incident and **always ships** (provided that incident had notified); an incident that never notified emits no Resolve (no noise). Webhook receivers should close alert panels on `category=resolve`, not by guessing from severity.

### 7.2 webhook Delivery

```toml
[alert]
webhook_url = "https://hooks.example.com/alert"   # empty = no delivery; events still hit journal/logs
webhook_format = "json"                            # json (raw AlertEvent) | slack (Slack/Discord-compatible {"text":...})
timeout = "10s"                                    # per-delivery timeout
retries = 3                                        # exponential backoff: 1s start ×2, cap 10s; max 100
deadman_url = ""                                   # optional heartbeat POST ("agent alive but deaf" is covered externally)
```

- Final failure logs an ERROR and increments `alert_delivery_failures`.
- The bounded queue (256) drops the oldest on overflow + `alerts_dropped` counter (timeliness over completeness).
- **Persistence of failed deliveries (spool)**: failed events are written to `{data_dir}/spool/pending.jsonl`, replayed after a confirmed success, then deleted; they expire after 7 days. Note: in-memory events not yet flushed before a force kill still have a loss window.
- `deadman_url` POSTs a heartbeat periodically; failures only log. Recommended in production together with external heartbeat monitoring.

### 7.3 HMAC Signing (F29/F34)

```toml
[alert]
webhook_secret = "change-me"       # global signing key; unset = unsigned
[[alert.routes]]
match = "edge"
url = "https://hooks.example.com/edge"
secret = "edge-only"               # route-level overrides global

[metrics]
secret = "push-secret"             # metrics push uses the same scheme
```

When configured, every delivery/push carries `X-Service-Watch-Signature: sha256=<hex hmac-sha256(body)>`. The signature covers the exact bytes sent. Receiver-side verification (always constant-time):

```bash
# one-line openssl verify ($body = raw received bytes, $sig = header hex minus the "sha256=" prefix)
printf '%s' "$body" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print $2}' | grep -qx "$sig"
```

```python
# python (hmac.verify / compare_digest are constant-time)
import hmac, hashlib
hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest() == sig.removeprefix("sha256=")
```

Note: HTTP/2 and strict receivers see the lowercase wire name `x-service-watch-signature` (RFC 9110 names are case-insensitive). Keys and signature values never enter logs.

### 7.4 Per-Target Routing (F26)

```toml
[[alert.routes]]
match = "edge"                        # target-name prefix, first match in order; or "*" for all
url = "https://hooks.example.com/edge"
format = "slack"                      # default json
```

- First match in configuration order wins; no match falls back to `webhook_url` (the default channel is the fallback).
- Routing does not change delivery semantics (timeout/backoff/queue/spool are handled uniformly).
- **Spool replay re-routes by the config the agent started with** — after changing routes and restarting, old backlogs are delivered under the new configuration.

### 7.5 Custom Delivery Headers (F37)

```toml
[alert]
headers = { X-Auth-Token = "global-secret" }          # global table; the default channel uses the global table

[[alert.routes]]
match = "edge"
url = "https://hooks.example.com/edge"
headers = { X-Auth-Token = "route-secret", X-Team = "edge" }  # same-name keys override global; other keys coexist
```

- Every delivery (slack/json and spool replay alike) carries all of them; they coexist independently with the signature header.
- **Do not customize the reserved header** `X-Service-Watch-Signature` (you'd end up with two same-name headers; most receivers take the first or reject); likewise leave `content-type`/`host` alone.
- Header values may be sensitive tokens: they go into request headers only, never into logs/audits.

---

## 8. Noise Control: Silence & Maintenance Windows

Both mechanisms **suppress notifications only**: `Alert / Escalate / Flap` are not delivered; `Resolve` (recovery notices) and Fatal-latch synthetic alerts **always pass through**; journal Incident events are still written (auditable after the fact); the check state machine, recovery FSM, and restart budgets are completely unaffected.

### 8.1 Target-Level Silence (F27 — one-shot, runtime operation)

```bash
# Silence a target for 30 minutes before a deploy, with an audit reason
monitorctl silence my-app --duration 1800 --reason "planned deploy v2.3.0"

# Deploy finished — lift early (audited; the remaining window is discarded)
monitorctl unsilence my-app

# Query silence audits (Audit events have no target field — don't pass --target)
monitorctl journal --type audit --tail 20
```

Semantics and caveats:

- Auto-expires (monotonic-clock deadline, immune to wall-clock jumps); expiry writes one audit entry.
- **Silence does not survive an agent restart** (in-memory only; better noisy than blind). For silence across restarts, re-apply after the restart.
- Re-silencing the same target overwrites (the window restarts at the new command time).
- `--duration 0` or > 7 days (604800s), and unknown targets: the CLI still shows `accepted (202)`; the actual rejection is visible in the journal audit.
- The first suppression hit inside the window writes one visibility audit; no per-hit records afterwards (anti-spam).
- After lifting, the next notification of an ongoing incident is delayed by at most one `renotify_interval` (default 15m).

### 8.2 Recurring Maintenance Windows (F31 — periodic, declared in config)

```toml
[[maintenance]]
name = "nightly-backup"        # optional; unset = audit reports the targets
targets = ["db-*"]             # target prefixes, or ["*"] = all
start_utc = "18:30"            # UTC "HH:MM" (convert your local time first)
duration = "30m"               # ≥ 60s; may cross midnight
days = ["mon", "wed", "fri"]   # optional; unset = every day
```

- **All times are UTC**; DST is not supported (a DST switch just shifts the window 1h relative to local clocks).
- Crossing midnight is legal: `start_utc = "23:00"` + `duration = "3h"` ends at 02:00 next day; the overflow belongs to the **opening day's** weekday.
- Window open/close each write one journal audit; after an agent restart windows re-enter automatically from the UTC clock (unlike silence — maintenance windows are long-term intent declared in config).
- **Not hot-reloadable**: restart the agent to apply changes.
- Stacks with silence: either matching suppresses (no priority between them).

---

## 9. Metrics & Observability

### 9.1 HTTP Metric Endpoints

| Endpoint | Content |
|---|---|
| `GET /metrics` | JSON `MetricsReport`; `?format=prometheus` (or `Accept: text/plain`) yields Prometheus text with the `service_watch_` prefix |
| `GET /metrics/history?range=<secs>` | History window; default 300s, max 7200s; > 600 points are evenly downsampled (first/last kept) |
| `GET /status` | Per-target health + recovery state machine position (structure in §10.2) |
| `GET /ready` | 200 once config is loaded and the scheduler started; **degraded collection does not fail ready** |
| `GET /health` | Shallow liveness, always 200, **always auth-exempt** |
| `GET /` | Read-only HTML status page (in-page JS polls `/status` + `/metrics` every 2s, with sparklines of key metrics) |

### 9.2 Key Metrics (patrol baseline)

| Group | Fields | Meaning |
|---|---|---|
| Alert pipeline | `alerts_sent` / `alert_delivery_failures` / `alerts_dropped` | notifications sent / final delivery failures / queue-overflow drops |
| journal | `journal_dropped` / `journal_write_failures` / `journal_write_successes` / `journal_critical_failures` | regular-event drops / write failures / successes / critical-ack failures |
| Probes | `probe_executed` / `probe_skipped` | executed / skipped due to the in-flight cap (skipped growing = interval too tight or target too slow) |
| Recovery | `restarts_total` / `state_transitions_total` / `fatal_targets` and the `recovery_*` state gauges | recovery activity and Fatal count |
| Collection | `degradations_current` / `degradations_events` / `last_collect_at` | current degradations / degradation edges / last collection time |
| Overview | `targets_total` / `checks_{ok,warning,critical}` / `services_{total,running}` / `uptime_secs` | direct dashboard inputs |

**Patrol baseline suggestions**: `alert_delivery_failures` growing, `alerts_dropped` growing, `probe_skipped` growing steadily, `recovery_fatal > 0`, `degradations_events` spiking, `last_collect_at` stalled.

Prometheus text uses the same field names as JSON (prefix `service_watch_`); the one exception: `last_collect_at` is exported as `service_watch_last_collect_timestamp_seconds` (before the first collection the whole line is omitted — no data rather than 0).

### 9.3 Periodic Metrics Push (`[metrics]`)

```toml
[metrics]
enabled = true
url = "http://collector:9090/ingest"   # POST MetricsReport JSON
interval = "60s"                       # timeout < interval (enforced)
timeout = "10s"
# secret = "change-me"                 # optional HMAC signing (F34)
```

Semantics: single attempt, failure abandoned (the next round carries fresh values anyway); consecutive-failure edges write journal audits (sticky edges, no spam; the recovery round is audited too). Pull-based scraping via `GET /metrics?format=prometheus` is preferred for scrapers (with auth enabled, scrapers must carry the token as well).

### 9.4 journal Queries & Structure

The journal lives at `{data_dir}/journal.jsonl` with nine event types (field-level examples in [RUNBOOK §2.1](../RUNBOOK.md)):

```jsonc
{"type":"incident","at":{...},"incident":1234567890,"target":"my-app","check":"api-health",
 "category":"alert","severity":"warning","reason":"HTTP probe failed: connect timeout"}
{"type":"restart","at":{...},"target":"my-app","fast_exit":true}
{"type":"fatal","at":{...},"target":"my-app","reason":"restart budget exhausted: 5 restarts in 10m window"}
{"type":"reset","at":{...},"target":"my-app","source":"control"}
{"type":"audit","at":{...},"message":"..."}
{"type":"reset_targets","at":{...},"targets":["my-app"],"source":"reload"}
{"type":"process_identity","at":{...},"target":"my-app","identity":{"pid":1234,"start_time":1731...},"verified":true}
{"type":"spawn_intent","at":{...},"target":"my-app","config_digest":"a1b2c3d4e5f60708"}
{"type":"spawn_outcome","at":{...},"target":"my-app","identity":{...},"pid_confirmed":true}
```

Query paths:

```bash
# via the control plane (remotely, with auth; merges the previous generation journal.1.jsonl)
monitorctl journal --tail 50
monitorctl journal --target my-app --type fatal --tail 20 --json

# local full queries with jq
J=/var/lib/service-watch/journal.jsonl
jq -r 'select(.type=="fatal") | "\(.at.secs_since_epoch | todate)\t\(.target)\t\(.reason)"' "$J"
jq -r 'select(.type=="restart") | .target' "$J" | sort | uniq -c | sort -rn | head -10
```

### 9.5 Metrics History Persistence (F39)

```toml
[metrics]
# history_path = "./data/metrics-history.jsonl"   # unset = in-memory ring buffer only
```

When configured, each published metric sample is appended as JSONL; one line equals one `/metrics/history` sample object `{"at":..., "report":{...}}`. Analysis via jq:

```bash
jq -c '.report | {probe_executed, alerts_dropped, recovery_fatal}' ./data/metrics-history.jsonl | tail -20
jq -c 'select(.report.recovery_fatal > 0)' ./data/metrics-history.jsonl
```

Key points: restarts **append, never truncate** (don't point at a path your boot scripts wipe); built-in single-generation rotation over 10 MiB (`metrics-history.1.jsonl`; rotation logs via tracing only); roughly 80–90 MB/day at full load — rotation is expected, not an anomaly; external rotation tools: copytruncate compatible, rename-style ineffective, **never delete the main file**; if the file can't be opened the feature shuts down for this run while the agent keeps running.

---

## 10. Control Plane & monitorctl

### 10.1 Endpoint Reference

Default `http://127.0.0.1:9170`.

| Endpoint | Method | Behavior |
|---|---|---|
| `/health` | GET | shallow liveness, always 200, always auth-exempt |
| `/ready` | GET | config loaded + scheduler started; 503 until then |
| `/status` | GET | full status report |
| `/metrics`, `/metrics/history` | GET | metrics (§9.1) |
| `/` | GET | read-only status page |
| `/v1/restart/{target}` | POST | queue a graceful restart; **409 for Fatal targets** (prompting a reset first) |
| `/v1/check/{target}` | POST | make the bound checks due immediately |
| `/v1/reset/{target}` | POST | clear the Fatal latch + restart budget (audited) |
| `/v1/silence/{target}` | POST / DELETE | silence a target / lift early (body `{"duration":<secs>,"reason":"<optional>"}`) |
| `/v1/journal?tail=&target=&type=&since=` | GET | journal tail query (tail default 200, max 1000) |
| `/v1/config` | GET | effective-config view (token/signing keys/header values as `***redacted***`, **not re-feedable**) |
| `/v1/reload` | POST | hot-reload topology (optional body `{"path":"..."}`), see §12 |

**Queue-and-ack semantics**: `restart / check / reset / silence / unsilence / reload` return **202 = queued** (command channel capacity 16; full → 503); admission **does not validate target existence** — a typo'd name is still accepted, and invalid targets are rejected eval-side with a journal audit — always confirm the outcome via `status` and the journal.

### 10.2 `/status` Structure

```jsonc
{"started_at": {...}, "ready": true,
 "targets": [{
   "name": "my-app",
   "mode": "supervise",              // supervise | attach
   "health": "ok",                   // ok | warning | critical (aggregated over checks)
   "recovery": "running",            // null | running | suspect | recovering | force_kill
                                     //      | restarting | starting | crashloop | fatal
   "checks": ["api-health"],
   "identity": {"pid": 4242, "start_time": 1731...},
   "incident": null,                 // incident level (ok|warning|critical)
   "restarts_in_window": 0,
   "last_transition": {...}
 }]}
```

### 10.3 monitorctl Commands

```bash
monitorctl [--url <URL>] [--token <TOKEN>] [--insecure]
           [--client-cert <PATH> --client-key <PATH>] <COMMAND>
```

| Operation | Command |
|---|---|
| Status table / raw JSON | `monitorctl status` / `monitorctl status --json` |
| Continuous watch (full-screen Δ history) | `monitorctl watch --interval 2` |
| JSON Lines watch / exit on error | `monitorctl watch --interval 2 --json` / `monitorctl watch --exit-on-error` |
| Probe now | `monitorctl check my-app` |
| Restart a supervised target | `monitorctl restart my-app` |
| Clear Fatal and the budget | `monitorctl reset my-app` |
| Silence / lift | `monitorctl silence my-app --duration 1800 --reason "planned deploy"` / `monitorctl unsilence my-app` |
| journal query | `monitorctl journal --tail 50` / `monitorctl journal --target my-app --type fatal --tail 20 --json` |
| JSON / Prometheus metrics | `monitorctl metrics` / `monitorctl metrics --prom` |
| Effective config (table/JSON) | `monitorctl config` / `monitorctl config --json` |
| Local hardware snapshot (no agent) | `monitorctl dump-snapshot` |
| Point at another control plane | `monitorctl --url http://127.0.0.1:9270 status` |
| Carry a token | `--token <TOKEN>` or the `SERVICE_WATCH_TOKEN` env var (flag wins) |

**monitorctl exit codes**: `0` success; `1` config error (dump-snapshot); `2` runtime error (incl. 409/non-2xx); `3` target-not-found (reserved; rarely returned — invalid targets are rejected agent-side); `4` agent unreachable. Script patrols should branch on exit codes, never on message text. monitorctl logs stay silent unless `RUST_LOG` is set (stderr).

### 10.4 Transport Security Trio (independent, stackable)

1. **HTTPS (F32)**: set `[control] tls_cert` + `tls_key` (PEM paths, both required) → the same `listen` address becomes pure HTTPS. Plain HTTP to a TLS port only gets a connection error. Certificate load failures exit at startup (exit 2). **Rotation = restart**.

   ```toml
   [control]
   listen = "127.0.0.1:9170"
   tls_cert = "/etc/service-watch/tls/cert.pem"   # leaf first, then the intermediate chain
   tls_key  = "/etc/service-watch/tls/key.pem"    # permission 0600
   ```

   Generating a self-signed test certificate:

   ```bash
   openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
     -keyout key.pem -out cert.pem -days 365 \
     -subj "/CN=service-watch" -addext "subjectAltName=IP:127.0.0.1,DNS:localhost"
   ```

   monitorctl's `--url https://...` validates against the system trust store; install your private CA into the system trust store, or use `--insecure` for throwaway tests (**test scenarios only** — disabling validation makes MITM undetectable).

2. **Bearer token (F23)**: with `[control] token = "..."`, every endpoint except `GET /health` requires `Authorization: Bearer <token>`; missing or wrong → 401 (failure reasons are not distinguished; constant-time comparison; the token never enters logs). Without a token configured the agent ignores any Authorization header.

3. **mTLS (F38)**: with `[control] client_ca_cert` (TLS must also be configured), the TLS handshake requires a client certificate issued by that CA; monitorctl uses the pair `--client-cert` + `--client-key`:

   ```bash
   monitorctl --url https://127.0.0.1:9170 \
     --client-cert client-cert.pem --client-key client-key.pem \
     --token "$SERVICE_WATCH_TOKEN" status
   ```

   An expired client certificate shows up as monitorctl exit 4; check `openssl x509 -enddate` first.

**Typical combinations**: public-internet remote ops = TLS + Bearer + mTLS all on; local deployment = all off. A token is a control-plane credential, not a security boundary — keeping `listen` on loopback plus tight firewall rules remains the outermost line of defense.

**Auth and the status page**: the in-page JS cannot attach custom headers — with auth enabled, opening `/` directly in a browser returns 401; serve the page through a reverse proxy that injects the header, or keep the token for API callers only and open the firewall for browser sources.

---

## 11. Recovery Semantics: Process Revival & Fatal

### 11.1 Trigger Contract

| Verdict | Evidence | Path |
|---|---|---|
| Certain death | own-child exit event; or an attach target's liveness missing `(pid, start_time)` for `confirmations` consecutive rounds | straight → Recovering |
| Health failure | check Critical while the process is alive | → Suspect; `confirmations` consecutive confirmations → Recovering |
| Hung | Critical, alive, and all failures are timeouts | same; Recovering prefers `hung_action` |

While recovery is executing, alert escalation for that target is suppressed (avoiding a "just restarted, judged hung again" loop); the Starting grace window is purely time-based (`last_spawn + startup_grace`).

### 11.2 Recovery State Machine (Full Transition Table)

```text
Running      --certain death--------------------▶ Recovering
Running      --health failures at threshold------▶ Suspect
Suspect      --confirmed failure again-----------▶ Recovering
Suspect      --confirmed recovery (successes)----▶ Running
Recovering   --activate: hook ok + healthy in verify window--▶ Running
Recovering   --activate: timeout/verify failed---▶ stop → Restarting (budget-counted)
Recovering   --restart: graceful ok--------------▶ Restarting
Recovering   --graceful timeout------------------▶ ForceKill --ok--▶ Restarting
ForceKill    --failure (e.g. Unix D state)-------▶ Fatal
Restarting   --spawn ok after backoff------------▶ Starting (startup_grace)
Restarting   --spawn failed (binary/port/workdir)▶ CrashLoop (budget-counted)
Starting     --alive > min_uptime + first healthy--▶ Running
Starting     --exited with uptime < min_uptime----▶ CrashLoop (budget-counted)
Starting     --failing past startup_grace+2×interval--▶ Suspect
CrashLoop    --retrying, budget not exhausted-----▶ Restarting (exponential backoff)
CrashLoop    --restart budget exhausted-----------▶ Fatal + Critical alert
Fatal        --only monitorctl reset (audited)----▶ Running (counters reset)
(any state)  --agent restart--------------------▶ journal replay + orphan reconciliation
```

- Backoff: `backoff_initial × 2^n`, capped at `backoff_max`; reset once Running persists past `min_uptime`.
- **Every automatic restart counts against the budget** (including spawn failures and slow deaths) — preventing "die once every 11 minutes" from evading the budget.
- Every kill/restart action is gated on `(pid, start_time)` identity verification; bare PIDs are never a kill justification (PID-reuse protection).

### 11.3 Crash Safety & Orphan Reconciliation

- Before every spawn a `spawn_intent` is journaled; the result is journaled as `spawn_outcome`; critical events wait for `sync_data` acknowledgment (2s timeout or write error → PersistenceFailed shutdown, exit 2).
- After an agent restart: replay restores Fatal/budget/identities; a confirmed still-alive old identity → read-only attach (during takeover, stop/kill/activate/restart are forbidden); a confirmed-gone identity → supervise resumes under the current config and budget.
- Supervised children and attach targets **survive agent exit**; the Fatal latch persists across agent restarts.
- One `data_dir` belongs to exactly one agent instance.

### 11.4 Fatal Handling Procedure (never skip a step)

```bash
# 1) Detect: recovery=fatal in status (usually with a Critical alert)
monitorctl status

# 2) Read the journal: Fatal reason and restart timeline
jq -c 'select(.type=="fatal" or ((.type=="restart") and .target=="<target>"))' \
   /var/lib/service-watch/journal.jsonl | tail -30

# 3) Locate the root cause: missing dependency / bad binary or config / port taken / OOM / path permissions
#    restarts dominated by fast_exit=true → crash loop; read the target's own logs

# 4) Fix the root cause — reset only clears the latch; without a fix it just Fatals again

# 5) Clear the latch (audited; clears Fatal + restart budget)
monitorctl reset <target>

# 6) Verify immediately
monitorctl check <target>
monitorctl status        # expect recovery: starting → running
```

**Never hand-edit the journal to "wash away" a Fatal** (it breaks replay order semantics); archive the lesson with a whole-file `journal.jsonl.bak-*` backup instead.

---

## 12. Hot Reload

`SIGHUP` (Unix) and `POST /v1/reload` apply **check / service / watch topology changes** (add/remove/redefine):

```bash
# validate the actual startup config offline first
monitor-agent --check-config /etc/service-watch/config.toml

# submit a reload (default re-reads the startup path; or specify explicitly)
curl -fsS -X POST http://127.0.0.1:9170/v1/reload
curl -fsS -X POST http://127.0.0.1:9170/v1/reload \
  -H 'Content-Type: application/json' \
  --data '{"path":"/etc/service-watch/config.toml"}'

# Windows PowerShell
Invoke-WebRequest -Method Post -Uri http://127.0.0.1:9170/v1/reload
```

Scope and semantics:

- **Hot-applied**: adding/removing/redefining checks/services/watches (matched by name); check parameter changes keep state-machine watermarks; redefining a service stops and reaps the old instance before starting the new definition; deletes/redefinitions persist a Reset first, clearing the old definition's Fatal and budget.
- **Requires restart**: `[agent] / [log] / [control] / [collect] / [alert] / [metrics] / [[maintenance]]` section changes (reload records an audit).
- Parse or validation failure **rejects the whole file**; the old config stays effective; after 202, confirm via logs and journal audits.
- Reload prunes silence entries of removed targets (re-adding the same name won't resurrect old silences).

---

## 13. Running as a System Service

Full per-platform walkthroughs: [docs/DEPLOYMENT.md](../DEPLOYMENT.md) and [docs/deploy/](../deploy/README.md); summary here.

### 13.1 Paths & Directories

| Item | Linux | macOS | Windows |
|---|---|---|---|
| Service manager | systemd | launchd | SCM |
| agent path | `/usr/local/bin/monitor-agent` | `/usr/local/bin/monitor-agent` | `C:\service-watch\monitor-agent.exe` |
| config path | `/etc/service-watch/config.toml` | `/etc/service-watch/config.toml` | `C:\service-watch\config.toml` |
| data dir | `/var/lib/service-watch` | `/var/lib/service-watch` | `C:\service-watch\data` |
| logs | journald | `/var/log/service-watch/` | `C:\service-watch\data\agent.log` (service mode) |
| run account | template `svcwatch` | template root (UserName configurable) | LocalSystem (dedicated account possible) |

Each agent instance uses its own data dir and control port; the run account needs read/write on the data dir and read on the config, target binaries, and pidfiles.

### 13.2 Linux (systemd)

```bash
getent passwd svcwatch || sudo useradd --system --user-group \
  --home-dir /var/lib/service-watch --shell /usr/sbin/nologin svcwatch

sudo install -m 0755 target/release/monitor-agent /usr/local/bin/monitor-agent
sudo install -m 0755 target/release/monitorctl /usr/local/bin/monitorctl
sudo install -d -m 0750 -o root -g svcwatch /etc/service-watch
sudo install -d -m 0750 -o svcwatch -g svcwatch /var/lib/service-watch
sudo install -m 0640 -o root -g svcwatch service-watch.toml /etc/service-watch/config.toml

sudo -u svcwatch /usr/local/bin/monitor-agent --check-config /etc/service-watch/config.toml
sudo install -m 0644 docs/deploy/systemd/service-watch.service /etc/systemd/system/service-watch.service
sudo systemctl daemon-reload && sudo systemctl enable --now service-watch
curl -fsS http://127.0.0.1:9170/ready && monitorctl status
```

Template highlights: `ProtectSystem=strict` hardening (add `ReadWritePaths` via `systemctl edit` when targets need extra write paths); `KillMode=process` keeps supervised children; `SuccessExitStatus=130` treats signal shutdown as success.

### 13.3 macOS (launchd)

```bash
sudo install -m 0755 target/release/monitor-agent /usr/local/bin/monitor-agent
sudo install -m 0755 target/release/monitorctl /usr/local/bin/monitorctl
sudo mkdir -p /etc/service-watch /var/lib/service-watch /var/log/service-watch
sudo install -m 0640 -o root -g wheel service-watch.toml /etc/service-watch/config.toml

plutil -lint docs/deploy/launchd/com.houseme.service-watch.plist
sudo install -m 0644 -o root -g wheel \
  docs/deploy/launchd/com.houseme.service-watch.plist \
  /Library/LaunchDaemons/com.houseme.service-watch.plist
sudo launchctl bootstrap system /Library/LaunchDaemons/com.houseme.service-watch.plist
sudo launchctl print system/com.houseme.service-watch
```

Stop with `sudo launchctl bootout system/com.houseme.service-watch`; logs in `/var/log/service-watch/`.

### 13.4 Windows (SCM)

Admin PowerShell; the agent must be built with `--features windows-service`:

```powershell
New-Item -ItemType Directory -Force -Path C:\service-watch\data | Out-Null
Copy-Item target\release\monitor-agent.exe C:\service-watch\monitor-agent.exe
Copy-Item target\release\monitorctl.exe C:\service-watch\monitorctl.exe
Copy-Item service-watch.toml C:\service-watch\config.toml

& C:\service-watch\monitor-agent.exe --check-config C:\service-watch\config.toml
.\docs\deploy\windows\install-service.ps1     # InstallDir/ConfigPath/DataDir/ServiceName parameters
Get-Service service-watch
Invoke-RestMethod http://127.0.0.1:9170/ready
```

`Stop-Service` / `Start-Service` go through the SCM handler; the script configures failure actions and `failureflag 1`. SCM manages the agent itself; existing Windows services can be monitored via watch, or driven through SCM APIs with `[[service]] windows_service = "<service name>"` (feature `scm-target`).

### 13.5 agent Exit Codes

`0` clean; `1` config error; `2` runtime/critical-persistence error; `130` SIGINT/SIGTERM graceful shutdown (treated as success by the systemd unit).

---

## 14. Daily Operations

### 14.1 Quick Reference

| I want to… | Command |
|---|---|
| See overall status | `monitorctl status` (JSON: `--json`) |
| See metric counters | `monitorctl metrics` (`--prom`) |
| Revive manually | `monitorctl restart <target>` |
| Verify right after a fix | `monitorctl check <target>` |
| Clear Fatal | `monitorctl reset <target>` |
| Quiet a deploy | `monitorctl silence <t> --duration 1800 --reason "deploy"` |
| Local collection self-test | `monitorctl dump-snapshot` |
| Event timeline | `jq -c . <data_dir>/journal.jsonl \| less` or `monitorctl journal --tail 50` |
| Liveness / readiness | `curl -fsS http://127.0.0.1:9170/health` / `.../ready` |
| Verify effective config | `monitorctl config` (`--json`) |

### 14.2 Agent Restarts & Orphan Takeover

When the agent exits, supervised children keep running; on restart it reads journal identities: a confirmed still-alive old identity → shown as attach with read-only monitoring; a confirmed-gone identity → supervise resumes under the current config and budget. The Fatal latch persists across restarts; lifting it still takes a reset. During takeover, process operations belong to the original external manager.

Post-restart expectations (F25):

- Still-open incidents are restored into the engine and **each re-notifies once** (`reason` prefixed with `restored from journal:`) — a feature, telling operators "an unclosed fault existed at restart".
- `/metrics` `checks_*` and target `health` start fresh and return to true levels within a few intervals as live probes land; don't read the first post-restart intervals as "recovered" — trust the `incident` field and notifications.

### 14.3 Capacity & Performance Budget

- Agent CPU acceptance line: < 2% of one core (5s collection interval; cost drops roughly linearly with larger intervals).
- Memory: the 24h soak gate passed (2026-11-17): RSS steady-state growth 176KB, tens-of-MB residency.
- The journal records events only — quiet systems barely grow; ~150–300B per line. 100 checks × 10 incidents/day ≈ 2–5 MB/day.
- Quick acceptance: `bash docs/deploy/perf-check.sh ./target/release/monitor-agent 60` (isolated port 19190 + temp data dir); 24h re-verification: `bash docs/deploy/soak-24h.sh`.
- Log rotation recipes (logrotate copytruncate / newsyslog limitations and the copy+truncate alternative / Windows scheduled tasks) in [RUNBOOK §6.3](../RUNBOOK.md).

---

## 15. Troubleshooting

### 15.1 Symptom Decision Tree (summary)

```text
[A] agent not listening (monitorctl exit 4)
├─ Is the daemon alive? systemctl status / launchctl print / sc query
│  ├─ Won't start: read journalctl / agent.err.log
│  │   ├─ "config error:" + exit 1       → fix config with --check-config
│  │   ├─ Start limit hit                → systemd reset-failed, then start
│  │   └─ Address already in use (exit 2)→ find the 9170 occupant; change [control].listen
│  └─ Alive but unreachable:
│      ├─ [control].enabled = false? / listen mismatched with --url?
│      ├─ HTTPS/mTLS failure             → verify trust chain and --client-cert/--client-key; check expiry
│      └─ HTTP 401                       → add --token or set SERVICE_WATCH_TOKEN

[B] All checks Critical
├─ Is the target actually down? curl/nc directly; if down → supervise revives, attach only alerts (by design)
└─ Alive but judged dead: network path (proxy/DNS/firewall); check probe_skipped for a too-tight interval;
   first round after sleep is discarded → wait a round

[C] Supervised target keeps restarting
├─ status: recovery=crashloop? restarts_in_window nearing max_restarts?
├─ fast_exit distribution of journal restart events: mostly true → crash loop, read the target's own logs;
│  mostly false → dies after running a while (resources/upstream/killed externally)
├─ spawn failures (binary missing/port taken/workdir permissions) → journal audit + ERROR
└─ Budget exhausted → Fatal → §11.4 procedure

[D] webhook not arriving
├─ alert_delivery_failures growing → read delivery error logs (DNS/certs/egress/endpoint 4xx 5xx)
├─ alerts_dropped growing → endpoint too slow; fix latency first (events are still in the journal)
└─ both 0 but no notifications → suppression (renotify/flap/silence/maintenance); is the incident in the journal?

[E] Agent self-health
├─ journal write failures → data_dir permissions, disk space; critical-ack failure exits 2
├─ degradations_events spiking → collector init / temperature anomalies; read the degraded fields
└─ memory/CPU anomalies → correlate uptime_secs with restart counts
```

### 15.2 FAQ Quick Table

| Symptom | What to check |
|---|---|
| agent exits at startup | run check-config as the actual service account; read service logs and the exit code |
| port 9170 taken | find the occupant; change control.listen and sync monitorctl --url |
| monitorctl exit 4 | agent running? control port? HTTPS/mTLS trust chain |
| config ok but the app won't start | verify command/args/workdir/account permissions; **daemonizing targets are forbidden** (double-fork reads as instant death and respawns forever) |
| target can't write files under systemd | directory ownership, ReadWritePaths, inherited sandbox settings |
| journal write failures | disk space, data-dir permissions; critical-ack timeout shuts the agent down — inspect the journal first after recovery |
| status stuck in attach | old process identity, collection permissions, liveness evidence |
| no recovery after Fatal | fix the actual fault first, then reset and watch |
| webhook not arriving | URL/format/network + alert_delivery_failures; inspect spool and journal |
| empty temperature/fans/voltage | read `hardware.degradations` and reasons; check the model's interfaces, permissions, and build features |
| browser can't open the status page with auth on | in-page JS can't attach the token; inject via a reverse proxy or open the network for browsers |
| "restored from journal" notifications after a restart | feature: recovery re-notification for incidents still open at restart (F25) |

---

## 16. Upgrade, Rollback & Uninstall

Before upgrading, record the version, status, and config; stop the agent; **back up the whole data directory (journal + spool), the config, and the old binaries**:

```bash
monitor-agent --version
monitorctl status --json > status-before.json
sudo systemctl stop service-watch
sudo tar -czf service-watch-backup.tgz -C / var/lib/service-watch etc/service-watch
```

(macOS stop: `sudo launchctl bootout system/com.houseme.service-watch`; Windows: `Stop-Service service-watch`.)

After replacing binaries, re-validate the config and start the service; then confirm `/ready`, target states, journal error counters, and alert delivery. On Unix you can write a temp file in the same directory and `mv` it into place, keeping the old binary for rollback.

Rollback: verify config and journal schema compatibility with the target version first; restoring a historical journal rolls budgets, Fatal, and identity state back — reconcile against the target's current PID and start time.

Uninstall: stop and disable the system service → handle any still-running app processes → archive config, journal, spool, and logs per your retention policy.

---

## 17. Appendix

### 17.1 monitor-agent Command Line

| Command | Purpose |
|---|---|
| `monitor-agent [--config <path>] [--log-json]` | main loop (default reads `./service-watch.toml`; top-level flags, no `run` subcommand) |
| `monitor-agent --dump-snapshot [--config <path>]` | collect once, print JSON, exit |
| `monitor-agent --check-config <path>` | offline config validation |
| `monitor-agent --version` | version |
| `monitor-agent --service-run` (Windows, `windows-service` feature) | SCM service entry |
| `--log-file <path>` | service-mode file log |

### 17.2 Unified Exit Codes

| Code | Meaning (agent / monitorctl) |
|---|---|
| 0 | success |
| 1 | config error |
| 2 | runtime error (agent: incl. critical persistence; monitorctl: incl. 409/non-2xx) |
| 3 | target not found (monitorctl reserved; rarely returned) |
| 4 | agent unreachable (monitorctl) |
| 130 | SIGINT/SIGTERM graceful shutdown (agent) |

### 17.3 Data Files

| File | Content |
|---|---|
| `{data_dir}/journal.jsonl` | event log (append-only JSONL; recovery-domain fact source) |
| `{data_dir}/journal.1.jsonl` | the previous generation of built-in journal rotation (archive/delete freely) |
| `{data_dir}/spool/pending.jsonl` | failed-delivery replay queue (deleted on confirmed success; 7-day expiry) |
| the file at `[metrics] history_path` | metrics history JSONL (`.1` = rotated previous generation) |

### 17.4 Document Map

| Document | Contents |
|---|---|
| [DESIGN.md](../../DESIGN.md) | semantic authority: config schema, ownership, state machines, platform contracts |
| [docs/DEPLOYMENT.md](../DEPLOYMENT.md) | full deployment & operations walkthrough |
| [docs/RUNBOOK.md](../RUNBOOK.md) | ops manual: journal/jq, alert semantics, decision trees, capacity, rotation |
| [docs/DEPLOY-LINUX/-MACOS/-WINDOWS.md](../DEPLOYMENT.md) | per-platform installation |
| [docs/deploy/](../deploy/README.md) | systemd/launchd/Windows templates and performance scripts |
| [ROADMAP.md](../../ROADMAP.md) | feature specs, acceptance, progress |
| [CHANGELOG.md](../../CHANGELOG.md) | change log |
| [developer-guide.en.md](developer-guide.en.md) | developer guide (docs v1) |

---

*Compiled from the main branch implementation as of 2026-11-22; where this document and [DESIGN.md](../../DESIGN.md) disagree, DESIGN.md wins.*
