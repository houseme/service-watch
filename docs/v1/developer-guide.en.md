# service-watch Developer Guide (English)

> For engineers developing and maintaining service-watch. The semantic authority is [DESIGN.md](../../DESIGN.md), hard constraints live in [AGENT.md](../../AGENT.md), scope and progress in [ROADMAP.md](../../ROADMAP.md); the user-facing view is [user-guide.en.md](user-guide.en.md). Chinese version: [developer-guide.zh.md](developer-guide.zh.md).
>
> Content is organized in the order you need it to read the code: design principles → workspace → architecture & concurrency → module map → subsystems → config/storage/platform layers → features & build → testing → workflow → extension points.

---

## Table of Contents

1. [Overview & Design Principles](#1-overview--design-principles)
2. [Workspace & Dependencies](#2-workspace--dependencies)
3. [Architecture & Concurrency Model](#3-architecture--concurrency-model)
4. [monitor-core Module Map](#4-monitor-core-module-map)
5. [Data Model](#5-data-model)
6. [Collection Layer (collect)](#6-collection-layer-collect)
7. [Check & Alert Engine (eval + alert)](#7-check--alert-engine-eval--alert)
8. [Recovery Engine (recover)](#8-recovery-engine-recover)
9. [Control Plane (control)](#9-control-plane-control)
10. [Configuration System (config)](#10-configuration-system-config)
11. [Storage & Persistence](#11-storage--persistence)
12. [Platform Differences](#12-platform-differences)
13. [Feature Flags & Build Matrix](#13-feature-flags--build-matrix)
14. [Testing Strategy](#14-testing-strategy)
15. [Workflow & Engineering Discipline](#15-workflow--engineering-discipline)
16. [Extension Points: How to Add X](#16-extension-points-how-to-add-x)
17. [Document Map](#17-document-map)

---

## 1. Overview & Design Principles

service-watch is a cross-platform (Rust 2024) service and hardware monitoring tool: one `monitor-core` library plus two thin binaries (`monitor-agent`, `monitorctl`) covering collection, check/alerting, process recovery, and a control plane.

Five design principles (DESIGN §1 — the highest standard in code review):

1. **External observer**: the agent is independent of the monitored services' lifecycle. The Windows Job Object does **not** set `KILL_ON_JOB_CLOSE`; Unix spawns use `process_group(0)` + `kill_on_drop(false)`. The agent dying never kills the targets.
2. **Explicit recovery policy**: `min_uptime` / `backoff` / `restart_budget{max,window}` / `stop_grace` / `startup_grace` are all explicitly configurable; Fatal is a terminal latch, cleared only by a manual `reset`.
3. **Observable degradation, no faked data**: unavailable fields are always `Option` + a machine-readable `Degradation` enum; the CPU first sample and implausible temperatures go through degradation paths — filling 0 is forbidden.
4. **Zero-lock single writer**: all state machines are owned exclusively by the single eval task; cross-task communication passes immutable messages only; shared mutable state via `Arc<Mutex>` is categorically not allowed (DESIGN §2.2).
5. **Deterministically testable**: all duration judgments take an injected monotonic clock (`Instant` as a parameter); sysinfo/network/processes are all mockable behind trait boundaries.

Precedence when documents conflict: `AGENT.md` (hard constraints) > `DESIGN.md` (implementation details) > `ROADMAP.md` (scope & progress) > `EXECUTION.md` (execution plan).

---

## 2. Workspace & Dependencies

### 2.1 Workspace

```text
service-watch/
├── Cargo.toml              # [workspace] resolver = "3", no default-members; version = 0.3.0
├── crates/
│   ├── monitor-core/       # lib: all business logic
│   ├── monitor-agent/      # bin: thin entry (CLI/logging/signals/SCM)
│   └── monitorctl/         # bin: control-plane client + local dump-snapshot
├── config/config.example.toml   # maps 1:1 to the schema; a unit test guarantees it parses
├── docs/                   # deployment/ops/review docs + the v1 user & developer guides
└── .github/workflows/      # ci.yml / release.yml / windows-scm.yml
```

`[profile.release]`: `lto = "thin"`, `strip = "symbols"`.

### 2.2 Key Dependencies (workspace.dependencies; verification dates in Cargo.toml comments)

| Dependency | Version | Purpose |
|---|---|---|
| tokio | 1.54.1 | only the needed features (rt-multi-thread/time/sync/macros/signal/process/net/io-util/fs); `full` forbidden |
| serde / serde_json / toml | 1.0.229 / 1.0.154 / 1.2.2 | config & serialization |
| thiserror | 2.0.21 | `CoreError` |
| tracing / tracing-subscriber | 0.1.44 / 0.3.23 | env-filter + json |
| sysinfo | 0.40.4 | hardware/process collection (declares MSRV 1.95, enforced by the resolver) |
| reqwest | 0.13.5 | `default-features = false`, features `json` + `rustls` (default-tls disabled) |
| axum / axum-server / rustls / rustls-pemfile | 0.8.9 / 0.9.2 / 0.23.46 / 2.2.0 | control plane & TLS |
| clap | 4.6.9 | derive CLI |
| hmac / sha2 | 0.13.0 / 0.11.1 | HMAC-SHA256 signing (pure Rust; doesn't break the cross-compile line) |
| libc / windows-sys / windows-service | 0.2.191 / 0.61.2 / 0.8.1 | platform FFI & SCM |
| rcgen | 0.14.10 | dev-dependency, self-signed certs in tests |

**Hard constraints** (AGENT.md; CI fails on violation): every crate uses `edition = "2024"` and `license = "Apache-2.0"`; **no Cargo.toml may contain a `rust-version` field** (self-check: `cargo metadata --format-version 1 | jq -e '[.packages[] | select(.source == null) | select(.rust_version != null)] | length == 0'`); dependency versions cited in docs must carry a verification date.

### 2.3 Binary Entry Points

- **monitor-agent** (`crates/monitor-agent/src/main.rs`): top-level clap flags (no `run` subcommand) — `--config` (default `./service-watch.toml`; when the file is absent an internal minimal default config is used), `--log-json`, `--dump-snapshot`, `--check-config`, and the hidden `--service-run` / `--service-name` / `--log-file` (Windows SCM service form, implemented in `service.rs`). `StopReason` → exit code mapping: `Clean`→0, `Signal`→130, `PersistenceFailed`/`EvalExited`/Err→2.
- **monitorctl** (`crates/monitorctl/src/main.rs`): globals `--url` (default `http://127.0.0.1:9170`) / `--token` (flag wins over `SERVICE_WATCH_TOKEN`) / `--insecure` / `--client-cert` + `--client-key` (pairs); 11 subcommands: `status / restart / check / reset / metrics / dump-snapshot / watch / silence / unsilence / journal / config`. HTTP timeout 5s; target path segments percent-encoded; table rendering strips control characters and clips overwide columns; logging silent by default (initialized only when `RUST_LOG` is set).

---

## 3. Architecture & Concurrency Model

### 3.1 Components & Data Flow

```text
┌──────────────────────────── monitor-agent (bin) ────────────────────────────┐
│  main: clap → tracing init → monitor_core::runtime::run(config)             │
└──────────────────────────────────┬──────────────────────────────────────────┘
┌──────────────────────────────────▼──────────────────────────────────────────┐
│  [collect blocking thread] --watch--> [eval single-writer] --mpsc--> [alert]│
│   owns sysinfo::System       state machines ×N    bounded queue + retries    │
│                                     │                                       │
│                             [recover inside eval]                           │
│                     RecoveryFsm × supervise targets                         │
│                     spawn/stop via ProcessManager (platform impls)          │
│  [control HTTP task] <--watch/snapshot--┘   [journal writer] <--bounded mpsc│
└─────────────────────────────────────────────────────────────────────────────┘
```

Data flows one way: `collect → eval → alert / recover`; recover never queries the collector backwards — process liveness comes from two channels: own-child exit events + liveness probes (adopted targets).

### 3.2 Concurrency & Ownership (mandatory, DESIGN §2.2)

| Component | Shape |
|---|---|
| collect | dedicated OS thread owning `sysinfo::System` (refresh is blocking; never call it bare in async context); snapshots broadcast via `tokio::sync::watch` |
| eval | one tokio task owning all state (check state machines + incident table + recovery FSMs + silence table); main loop `select`s on: `sleep(nearest due)` + `control_rx.recv()` + `snapshot_rx.changed()`, with a 1s `MAX_TICK` fallback; probe concurrency via `JoinSet`, results return to the same task to drive the state machines |
| alert | an independent task consumes `mpsc::channel(256)`; dispatch bounds concurrency with a `Semaphore`; each delivery runs in its own task (panic isolation); the dispatcher task is supervised by the runtime and rebuilt up to 3 times after panics (give-up writes a journal audit) |
| journal | an independent blocking writer task consumes `mpsc::channel(1024)`, append-only; critical events `send().await` waiting for `sync_data` acknowledgment (2s total budget including enqueue) |
| control | an independent axum task; reads snapshots via watch, submits commands via the command channel (capacity 16); full → 503 |
| metrics history | an independent writer task consumes a clone of the metrics watch (zero eval changes; a bypass) |

**Backpressure policy**: alert queue full → drop the oldest + WARN audit (timeliness over completeness; completeness is covered by the spool); eval command channel full → control returns 503.

### 3.3 runtime Assembly & Shutdown (`runtime.rs`)

Assembly order (all early-failure points; any failure exits with code 2 before `data_dir` is even created): TLS material loading → port bind → config validation → data_dir/journal assembly → task startup. A Bearer token without TLS warns once at assembly (plaintext transport notice).

Shutdown sequence: set shutdown → abort eval/control → drain alerts within a 500ms window (remainder counted as dropped + journal audit) → abort metrics pusher → abort history writer → flush journal within a 100ms window.

`request_shutdown()` (F15, in-process shutdown): static `REQUESTED` + `Notify`, with no permit-loss window — **never guard the select branch with `if REQUESTED`** (a disabled branch never registers a waiter, so notify just stores a permit and nothing wakes; the round-17 P0 lesson; regression in `tests/inprocess_shutdown.rs`).

---

## 4. monitor-core Module Map

| Module | Files | Responsibility |
|---|---|---|
| `config` | `src/config.rs` | `ConfigRaw` (serde, deny_unknown_fields + serde defaults) → `Config::validate()` cross-validation; defaults; redacted view |
| `model` | `src/model/{ids,snapshot,alert,control}.rs` | `ProcessId` newtype, `ProcessIdentity{pid, start_time}`; `HardwareSnapshot`/`ProcessSnapshot`/`Degradation`; `AlertEvent`; `ControlCommand`/`StatusReport`/`MetricsReport`/`MetricsSample` (cap 7200) |
| `collect` | `src/collect/{mod,sysinfo_collector}.rs` + `deep/{mod,hwmon,smc}.rs` | the synchronous `Collector` trait; sysinfo 0.40 implementation; deep-collection feature (hwmon/AppleSMC); `StubCollector` degradation |
| `eval` | `src/eval/{mod,check,state,incident,maintenance,reload}.rs` | single-writer scheduler main loop; probe execution; check state machine; incident denoising; maintenance windows; hot-reload diff/apply; silence table; metrics ring buffer; crash-safe spawn transactions |
| `alert` | `src/alert/{mod,webhook,signature,spool,metrics_push}.rs` | AlertQueue (drop-oldest 256); RoutingNotifier; webhook/slack payloads; shared HMAC signing layer; spool; periodic metrics push; deadman |
| `recover` | `src/recover/{mod,policy,journal,reconcile}.rs` + `process/{mod,unix,windows}.rs` | the full `RecoveryFsm` transition table; `RestartPolicy`/`Backoff`/`BudgetWindow`; journal write/rotation/replay; orphan reconciliation; platform process management |
| `control` | `src/control/mod.rs` (+ `status_page.html`) | axum routes, auth middleware, TLS/mTLS assembly, read-only status page |
| `runtime` | `src/runtime.rs` | assembly, task orchestration, shutdown sequence, `StopReason` |
| `metrics_history` | `src/metrics_history.rs` | the F39 history persistence writer |
| `error` | `src/error.rs` | `CoreError`: Config/Io/Collect/Alert/Recover/Control/Json/Toml/Http |

---

## 5. Data Model

### 5.1 Identity (PID-reuse protection)

```rust
pub struct ProcessId(pub u32);                                   // newtype; sysinfo::Pid never leaks
pub struct ProcessIdentity { pub pid: ProcessId, pub start_time: u64 }  // epoch seconds
```

Every liveness verdict, stop, and takeover action must verify `(pid, start_time)` first; a bare PID is never a kill justification (mandatory, DESIGN §8.3).

### 5.2 Snapshots & Degradation

```rust
pub struct HardwareSnapshot {
    pub timestamp: SystemTime,           // event timestamp (display; judgments always use monotonic clocks)
    pub cpu_usage_percent: Option<f32>,  // first sample None + Degradation, never a faked 0
    pub per_core_usage: Vec<Option<f32>>,
    pub memory_total_bytes: u64,
    pub memory_used_bytes: u64,
    pub disks: Vec<DiskUsage>,
    pub networks: Vec<NetIo>,
    pub temperatures: Vec<Temperature>,  // empty Vec + Degradation = platform has no data
    pub degradations: Vec<Degradation>,
}
pub struct Temperature { pub label: String, pub celsius: Option<f32> }

#[derive(Serialize)]
#[serde(tag = "kind", content = "detail")]
pub enum Degradation {
    TemperatureUnavailable { platform: &'static str },
    TemperatureImplausible { label: String, raw: f32 },
    CpuFirstSample { note: &'static str },
    CollectorInit { source: String },
    // deep collection adds DeepHardwareUnavailable / SensorRead with a machine-readable
    // SensorUnavailableReason: Unsupported/NoSensors/PermissionDenied/Io/InvalidData/Disabled/Fault
}
```

### 5.3 Alert Event

```rust
pub struct AlertEvent {
    pub incident: u64,        // = incident_key = hash(target_name, check_name)
    pub category: Category,   // Alert | Escalate | Resolve | Flap
    pub severity: Severity,   // Warning | Critical
    pub target: String,
    pub check: String,        // Fatal synthetic alerts use RECOVER_CHECK = "__recover__"
    pub reason: String,
    pub observed: Option<String>,
    pub first_seen: SystemTime,
    pub last_seen: SystemTime,
    pub occurrences: u64,
}
```

The journal and webhooks share this structure (serde-serialized).

---

## 6. Collection Layer (collect)

- `Collector` is a **synchronous trait** (implementations own `sysinfo::System`, driven single-threaded by a dedicated blocking thread); `create_collector()` degrades to `StubCollector` on init failure (carrying `Degradation::CollectorInit`).
- **sysinfo 0.40 API anchors** (DESIGN §6.1): `System::new() -> Result`, `global_cpu_usage()`, `Component::temperature() -> Option<f32>`, `refresh_processes(ProcessesToUpdate::Some(&pids), true) -> usize`, `Process::start_time() -> u64`. `refresh_all` is forbidden.
- **CPU first sample**: double sampling spaced by `MINIMUM_CPU_UPDATE_INTERVAL` (200ms on Apple platforms) at construction; never sleeps afterwards; unavailable first-tick data → `None` + `Degradation::CpuFirstSample`.
- **Partial refresh protocol (mandatory)**: each round eval supplies **all** watched PIDs; one `refresh_processes(..., remove_dead_processes=true)` call; death = `(pid, start_time)` no longer present; a missing PID ≠ confirmed death (needs `confirmations` consecutive rounds).
- **Temperature plausibility filter**: values outside -20..=150 ℃ are dropped + `Degradation::TemperatureImplausible` (macOS demonstrably returns -9201 ℃ garbage); labels containing known noise sources (`tdev`/`als-temp`) are skipped.
- **Deep collection** (`collect-deep-hardware`): `DeepCollector` wraps the base collector. Linux reads `/sys/class/hwmon` `fanN_input` (RPM) / `inN_input` (mV→V) honoring enable/fault attributes; macOS reads fixed AppleSMC candidate keys read-only via IOKit, decodes by SMC type, with a ±100 V voltage plausibility bound; each sensor keeps `Option<f32>`, with machine-readable degradation for faults/disabled/permission/invalid values; Windows returns an unsupported degradation. 0 RPM is a valid reading.

---

## 7. Check & Alert Engine (eval + alert)

### 7.1 The eval Single Writer (`eval/mod.rs`)

The main loop `select`s on three sources: sleep to the nearest due time / control commands / snapshot changes; a 1s `MAX_TICK` fallback. Each tick: drive due probes (in-flight cap 1, skips counted as `probe_skipped`) → advance check state machines → incident denoising produces AlertEvents → advance recovery FSMs → consume control commands → ring-buffer metrics (F18, `METRICS_HISTORY_CAP = 7200` ≈ 2h@1s).

### 7.2 Check State Machine (`eval/state.rs`)

`Ok --1 failure--> Warning --failure_threshold consecutive--> Critical`; `success_threshold` consecutive successes return to Ok. Pure logic with an injected monotonic `Instant` — time is never read internally. Failures inside `startup_grace` never enter the machine; the first probe round after host sleep is discarded as degraded.

### 7.3 Probes (`eval/check.rs`)

`ProbeResult::{Unavailable, Ok, Fail}`; three kinds: TCP connect / HTTP GET + status range (default 200-299, follows redirects ≤10 hops, the 302 chain's final status decides — the opposite of webhook delivery, which forbids redirects) / process CPU+memory thresholds (identity-verified). Callers wrap every probe in `tokio::time::timeout`; timeout = failure and the future is dropped.

### 7.4 Incident Denoising (`eval/incident.rs`)

- `incident_key = hash(target, check)`, excluding severity/reason — an escalation never becomes a "second incident".
- Five rules: ① the first notification is never suppressed; ② repeat same-severity notifications while open are gated by `renotify_interval`; ③ Escalate always ships; ④ Resolve ships only if that incident had notified; ⑤ open/close ≥ `flap_threshold` within `flap_window` → one Flap summary only (Flap doesn't count as notified, preventing resolve-without-alert); the transitions ring is capped at 1024.
- **F25 cross-restart restore**: journal replay maintains the open-incident set (Alert/Escalate open + refresh, Resolve closes + orphan-resolve counting, Flap leaves the gate alone; the Fatal synthetic incident belongs to the recovery domain and is closed by Reset); at startup restored incidents re-notify once with `reason = "restored from journal: <original reason>"`; the notification window starts at restore time; flap counters are not restored (fresh session window); check state machines are not back-filled (fresh start, preserving the conservative `confirmations` contract); keys are recomputed at runtime, never trusted from the journal.
- **F27 silence (eval-side)**: `SILENCE_MAX_SECS = 604800` (7 days); deadlines use a monotonic `Instant`; while active, Alert/Escalate/Flap **never enter the AlertQueue**; Resolve and Fatal synthetic alerts (`check = RECOVER_CHECK`) are always exempt; create/remove/expiry/first-suppressed-hit each write one Audit; in-memory only — forgotten on restart; reload prunes silences of removed targets.

### 7.5 Maintenance Windows (`eval/maintenance.rs`)

A UTC pure-function calendar (epoch seconds → weekday index + minutes-of-day; epoch 0 anchors to Thursday); `contains` handles midnight crossing and days filtering (overflow belongs to the opening day); the predicate is a pure function of injected epoch seconds, recomputed every tick with no persistent state (auto re-entry after restart); the window table is a construction-time snapshot, not hot-reloaded.

### 7.6 Hot Reload (`eval/reload.rs`)

A pure diff function: applies only target topology (checks/services/watches add/remove/redefine, matched by name + serde JSON Value comparison); changes to a service's command/args/workdir/env/windows_service = delete+add (FSM watermarks not kept), while soft fields (restart/activate/console) update in place (watermarks kept); non-topology sections are detected in a fixed order `agent → log → control → collect → alert → metrics → maintenance` and only audited as `section X changed but requires restart`; parse/validation failures reject the whole file. Reload reads and validates files inside `spawn_blocking` (never freezes the single writer). The config view (F33) is pushed through a watch after a successful reload.

### 7.7 Alert Dispatch (`alert/mod.rs`)

- `AlertQueue`: bounded drop-oldest 256; overflow drops the oldest + `alerts_dropped` + WARN; `counters()` exposes the three counters (published with the eval report to all three outlets).
- Dispatch: `retries + 1` attempts per delivery, exponential backoff 1s×2^n capped at 10s, each attempt bounded by `alert.timeout`; each attempt runs in its own task (panic isolation); total failure → `alert_delivery_failures` + ERROR audit → spool.
- **RoutingNotifier (F26)**: assembled when routes are non-empty; `*` wildcards, otherwise target-prefix `starts_with`; first match in order; no match falls back to the default channel; route-level secret/headers override global (`effective_route_secret` / `effective_route_headers` pure functions); a route that fails to build is WARN-skipped; empty `webhook_url` with no match → NoopNotifier (events still hit the journal, `alerts_sent` keeps counting).
- **spool (F12)**: appends to `{data_dir}/spool/pending.jsonl` (flush + sync_data); after a confirmed success, atomically rewritten via the `pending.jsonl.next` rename (transactions hold a mutex permit); replayed at startup and every 60s, max 16 per round; entries expire after 7 days; replay uses the same notifier instance the dispatcher holds (re-routed/signed/headed under the current config).
- **webhook (F29)**: payload `json` (raw AlertEvent) or `slack` (`{"text": "[Severity] Target/Check: reason (xN)"}`); **redirects disabled** (`Policy::none()`, preventing POST→GET degradation); non-2xx = failure; custom headers (F37) are all appended, invalid name/value dropped with WARN at construction.
- **Shared HMAC signing layer (`alert/signature.rs`)**: header `X-Service-Watch-Signature`, value `sha256=<lowercase hex>`; signs the exact body bytes sent (`serde_json::to_vec` first, then sign); no key = no signature header; keys/signatures never enter logs; webhook signing is gated by `notify-webhook`, metrics-push signing by `http-client`.
- **metrics push (`alert/metrics_push.rs`, F34)**: POSTs the latest MetricsReport JSON every `interval`; single attempt, no retry (next round overwrites); redirects disabled; the first consecutive failure / every 10 consecutive failures / the recovery round write journal audits (sticky edges).
- **deadman**: an independent task POSTs `{}` every 30s; failures only WARN; the first beat fires immediately; started once by the runtime.

---

## 8. Recovery Engine (recover)

### 8.1 RecoveryFsm (`recover/mod.rs`)

One FSM per supervised target. States: `Running / Suspect / Recovering / ForceKill / Restarting / Starting / CrashLoop / Fatal`.

Event entry points: `on_child_exit` / `on_liveness_gone` / `on_liveness_ok` / `on_health` / `on_tick` / `on_spawn_result` / `on_scm_spawn_ok` (dedicated entry for SCM targets without process identity) / `on_stop_result` / `on_activate_result` / `reset` / `restore` / `update_policy` (hot updates preserve watermarks).

Actions: `RecoverAction::{ScheduleSpawn, SignalStop, ForceKill, RunActivate, MarkFatal, JournalRestart, Audit}`.

**Budget semantics (critical)**: every automatic restart counts (slow deaths, spawn failures, the Restart branch of health hangs); the Activate branch doesn't (a successful activation is not a restart); a child_exit arriving during ForceKill must be finalized (a P0 regression lesson). Fatal clears only via `reset`. The full transition table: DESIGN §8.2 / user guide §11.2.

### 8.2 Policy Components (`recover/policy.rs`)

- `RestartPolicy`: solidified from `RestartCfg`.
- `Backoff`: `initial × 2^(attempt-1)` capped at `max`; reset once Running persists past `min_uptime`.
- `BudgetWindow`: a sliding-window ring of restart timestamps.

### 8.3 journal (`recover/journal.rs`)

- Writing: bounded channel 1024 → an exclusive blocking writer; critical events (Restart/Fatal/Reset/ResetTargets/ProcessIdentity/SpawnIntent/SpawnOutcome) `send().await` waiting for `sync_data` ack (2s total budget including enqueue); failure → `JournalCounters.fail()` → runtime `StopReason::PersistenceFailed` shutdown; ordinary audits `try_send`, overflow counted and dropped (ERROR on the first and then every 64th).
- **Rotation (F24)**: after writing, the measured size over `journal_max_size` (default 10 MiB, 0 = disabled) renames the current file to `journal.1.jsonl` (one generation only), recreates the current file + a rotation audit; rename failure degrades to keep-writing + WARN; on open, a missing trailing `\n` is repaired (crash half-line delimitation).
- **Replay**: applies `journal.1 → journal.jsonl` in line order to one state; bad/half lines isolated; produces `JournalState { restarts, fatal, last_reset, processes, pending_spawns, open_incidents, orphan_resolves }`; `config_digest` = FNV-1a-64 over the hot-field set (name+command/args/workdir/env/windows_service), stable across rustc versions.

### 8.4 Orphan Reconciliation (`recover/reconcile.rs`)

`OrphanTracker` states `Pending / Attached(identity) / Gone`; only fresh snapshots that explicitly requested the candidate identity and have a usable collector participate; an unverified identity with the same pid but a different start_time persisting 600s (`RECONCILE_MISMATCH_TIMEOUT`) is adjudicated Gone (PID-reuse fallback). During takeover (attach presentation) stop/force-kill/activate/control-plane restart are forbidden.

### 8.5 Process Management (`recover/process/`)

- `ProcessManager`: owned exclusively by eval; spawn/stop/poll-exit.
- **Unix** (`unix.rs`): `process_group(0)` + `kill_on_drop(false)` + all stdio null; stop = `killpg(SIGTERM)`, force = `killpg(SIGKILL)` (ESRCH counts as success); `try_wait` reaping prevents zombies; `pending_kills` registers group members still alive after the leader reaps, to SIGKILL at deadline (a seconds-scale pgid-reuse mis-kill window exists — recorded as-is).
- **Windows** (`windows.rs`): `AssignProcessToJobObject` immediately after spawn (Job without KILL_ON_JOB_CLOSE; tokio Child doesn't expose the main-thread handle, so a microsecond-scale escape window exists between spawn and assign — recorded as a deviation); `console = true` attempts `GenerateConsoleCtrlEvent(CTRL_BREAK)` first, escalating to `TerminateJobObject` on grace timeout.
- **SCM targets** (`scm-target` feature + the `windows_service` field): spawn = `Start` (idempotent); stop = `Stop` (idempotent); liveness = `QueryStatus == Running` (only SERVICE_STOPPED is death evidence); force_kill = Stop again (honest degradation); no `(pid, start_time)` identity — the FSM enters via `on_scm_spawn_ok`; the `ScmOps` trait accepts test fakes; the agent only stops services it started in this session.

---

## 9. Control Plane (control)

### 9.1 Routing & Auth

axum 0.8 with two sub-routers: `public` holds only `GET /health` (never authenticated); everything else goes into `protected` behind the Bearer middleware (F23). 401s return a uniform JSON body + `WWW-Authenticate: Bearer` without distinguishing failure reasons; constant-time comparison `ct_eq` (hand-written, length differences counted); token values never enter any log/audit. TLS (F32) and mTLS (F38) load at assembly; bad material exits with code 2; the rustls provider prefers the process-level `CryptoProvider::get_default()` and otherwise builds a local aws-lc provider — **never `install_default`**; ALPN `h2`/`http/1.1` is set manually.

### 9.2 Endpoint Reference

| Endpoint | Notes |
|---|---|
| `GET /health` | 200 empty body; auth-exempt |
| `GET /` | F22 read-only status page (`include_str!("status_page.html")`, inline CSS/JS polling /status + /metrics every 2s, SVG sparklines `spark-*`, all interpolation escapeHtml) |
| `GET /ready` | 200 `{"ready":true}` / 503 `{"ready":false}` |
| `GET /status` | StatusReport (the eval's latest report; readable even if the sender is gone) |
| `GET /metrics` | JSON by default; `?format=prometheus` or `Accept: text/plain` → Prometheus text (`render_prometheus` is table-driven, prefix `service_watch_`; `last_collect_at` → `service_watch_last_collect_timestamp_seconds`) |
| `GET /metrics/history?range=` | F18 ring-buffer window snapshot; range default 300, max 7200, invalid 400; > 600 points evenly downsampled keeping first/last |
| `POST /v1/restart/{target}` | Fatal (judged via the status watch) → 409; otherwise 202 |
| `POST /v1/check/{target}` | 202 (bound checks due immediately) |
| `POST /v1/reset/{target}` | 202 (clears Fatal + budget) |
| `POST /v1/reload` | F14; optional body `{"path": "..."}`; invalid body 400 |
| `POST/DELETE /v1/silence/{target}` | F27; POST body `{"duration": <secs>, "reason": <optional>}`; missing duration/non-JSON → 400; semantic rejections happen eval-side |
| `GET /v1/journal` | F28/F35; `tail` (1..=1000, default 200) / `target` exact / `type` serde tag / `since` epoch secs; response `{"truncated": bool, "lines": [...]}`; gen1 merged first; 512KB bounded tail read per file (spawn_blocking) |
| `GET /v1/config` | F33; `Config::redacted()` JSON (token/webhook_secret/route secrets/metrics.secret/delivery header **values** all `***redacted***`) |

Command channel (`ControlCommand`, serde tag `"op"` lowercase): `Restart / Check / Reset / Reload{path} / Silence{target,duration_secs,reason} / Unsilence`; capacity 16, full → 503 "command queue full", shutting down → 503 "agent shutting down".

---

## 10. Configuration System (config)

Two layers: the parsing layer `ConfigRaw` (serde, `deny_unknown_fields` + `#[serde(default)]`; exception: `CheckCfg` can't validate strictly because `kind` uses `#[serde(flatten)]`, but unknown kind values are still rejected) → the validation layer `Config::validate()` cross-validation; either failing is a config error with exit 1. Runtime sees only `Config`.

Duration format `"500ms"|"5s"|"10m"|"1h"`, composable `"1h30m"`. The full schema and defaults: DESIGN §4 and [config/config.example.toml](../../config/config.example.toml) (unit test `parses_full_example` guarantees the example always parses).

`validate()` highlights (source `config.rs` L723-1395): durations positive and within monotonic-clock range; check `timeout < interval`; header names RFC 7230 tokens / values visible ASCII + TAB only (case-fold collisions rejected within and across tables); TLS comes in pairs; mTLS requires TLS; maintenance `HH:MM`/duration/days validity; names unique within each of the three spaces; `depends_on` cycle-checked with a three-color DFS; `history_path` collision-guarded (normalized) against `journal.jsonl` / `journal.1.jsonl` / `spool/pending.jsonl` / `spool/pending.jsonl.next`.

`Config::redacted()`: token / webhook_secret / route secrets / metrics.secret / global and route delivery header **values** become `***redacted***`; everything else (including TLS paths) verbatim; consumed by `GET /v1/config`; the placeholder must never be fed back into a config file.

---

## 11. Storage & Persistence

| File | Module | Notes |
|---|---|---|
| `journal.jsonl` + `journal.1.jsonl` | `recover/journal.rs` | recovery-domain fact source; critical events acknowledged; single-generation rotation; half-line delimitation; two-generation ordered replay |
| `spool/pending.jsonl` (+ `.next`) | `alert/spool.rs` | delivery replay; rename atomic transaction + mutex permit; 7-day expiry |
| `[metrics] history_path` (+ `.1`) | `metrics_history.rs` | observability domain; line = `{"at","report"}` (isomorphic to `/metrics/history` samples); appends across restarts; 10 MiB single-generation rotation (gen1 path reuses `journal::gen1_path` as the single fact source); no per-line fsync; rotation logs via tracing only, never a journal audit |
| in-memory ring buffer | `model::control` | `METRICS_HISTORY_CAP = 7200`; distributed via the metrics watch |

**Domain isolation principle**: the journal is the recovery domain (tied to shutdown/exit codes); metrics history is an observability outlet (open failure → ERROR + disabled for this run, the agent keeps running) — the two never drag each other down.

---

## 12. Platform Differences

| Aspect | Unix | Windows |
|---|---|---|
| spawn isolation | `process_group(0)` | Job Object (no KILL_ON_JOB_CLOSE; microsecond pre-assign race) |
| graceful stop | `killpg(SIGTERM)` | CTRL_BREAK attempt for `console=true`, Terminate baseline otherwise |
| force kill | `killpg(SIGKILL)` | `TerminateJobObject` |
| hot-reload trigger | SIGHUP + endpoint | endpoint only |
| deep collection | Linux hwmon / macOS AppleSMC (`#[cfg(target_os)]`) | unsupported degradation |
| service form | none (hand to systemd/launchd) | `windows-service` feature + `--service-run` (SCM dispatcher + control handler; `request_shutdown` called before reporting StopPending; 30s wait-hint fallback; file logging without ANSI) |
| SCM targets | — | `scm-target` feature + the `windows_service` field |
| shutdown signals | SIGINT/SIGTERM/SIGHUP → exit 130 | ctrl_c + REQUEST_NOTIFY |

Platform conditional compilation: `#[cfg(unix)]` / `#[cfg(windows)]` / `#[cfg(target_os = "macos")]` / `#[cfg(any(target_os = "linux", test))]` (hwmon visible to tests).

---

## 13. Feature Flags & Build Matrix

monitor-core features:

```toml
default = ["collect-sysinfo", "probe-http", "notify-webhook", "control-http"]
collect-sysinfo        = ["dep:sysinfo"]
collect-deep-hardware  = []                # pure gate; implies collect-sysinfo semantics
http-client            = ["dep:reqwest", "dep:hmac", "dep:sha2"]
probe-http             = ["http-client"]
notify-webhook         = ["http-client"]
control-http           = ["dep:axum", "dep:axum-server", "dep:rustls-pemfile", "dep:rustls"]
scm-target             = ["dep:windows-service"]   # only effective under cfg(windows); honest error otherwise
```

- monitor-agent features mirror core (passthrough with `default-features = false` so `--no-default-features` truly removes reqwest/axum); it adds `windows-service`.
- monitorctl depends only on core's `collect-sysinfo` plus the optional `collect-deep-hardware` passthrough.
- **Two independent cross-compile acceptance lines** (DESIGN §6.5): ① local/CI `cargo check --target x86_64-pc-windows-msvc` (feature combos without C dependencies; reqwest/ring is out of local Windows cross scope); ② full Windows verification is owned by the CI `windows-latest` runner (NASM + CMake required).

---

## 14. Testing Strategy

1. **Deterministic unit tests**: state machines/denoising/budgets are driven by injected monotonic `Instant`s (callers pass parameters; machines never read clocks); the scheduler's tokio time supports `start_paused`. Coverage hard target: at least one unit test per transition edge in DESIGN §7.1/§7.2/§8.2.
2. **Boundary mocks**: `Collector` / `Notifier` / `ProcessHandle` / `ScmOps` are traits with fake implementations (zero sysinfo/network/process dependencies).
3. **Integration tests** (`crates/monitor-core/tests/`, 13 files):

| File | Coverage |
|---|---|
| `alert_routing.rs` | F26 routing hit/fallback with dual webhook stubs + F29 signature verification (incl. spool replay end-to-end) + F37 headers |
| `http_probe.rs` | HTTP probe end-to-end (axum stub + real scheduler): slow response → alert → recovery |
| `incident_restore.rs` | F25 incident cross-restart restore & re-notification |
| `inprocess_shutdown.rs` | F15 `request_shutdown` shutdown regression (the round-17 P0 deadlock root cause) |
| `maintenance_window.rs` | F31 maintenance windows end-to-end (suppression / Resolve pass-through / audit edges) |
| `metrics_history.rs` | F39 history persistence (line format + single-generation rotation) |
| `metrics_push.rs` | F34 push signing (with secret verified / None sends no header) |
| `scm_target.rs` | SCM target runtime contract (injected fake ScmOps) |
| `silence_notification.rs` | F27 silence end-to-end (suppression / pass-through / audit edges) |
| `status_page.rs` | F30 status page SVG sparklines + history data-source contract |
| `storage_fault.rs` | storage fault drills (disk full / slow-disk timeout / power-cut half-line isolation / repaired restart) |
| `supervise_smoke.rs` | real-process smoke (`#![cfg(unix)]`): cold-start identity, kill -9 revival, crash loop → Fatal |
| `tls_control.rs` | F32 HTTPS + F38 mTLS (rcgen self-signed; `--insecure` equivalent client / default distrust / plaintext failure / bad PEM / handshake accept & reject) |

Many more unit tests are embedded in source files (config validation matrix, FSM transition tables, journal rotation/replay, control endpoints, monitorctl rendering pure functions). The full-suite baseline is 369+ (see the per-round verification records in CHANGELOG).

4. **CI matrix**: three platforms fmt + clippy (-D warnings) + test; Windows cross-check on the two lines; `windows-scm.yml` real-machine drill (install → RUNNING → /health → spawn transaction → graceful stop → Job Object orphan assertion → hot reload → error-exit self-healing).

---

## 15. Workflow & Engineering Discipline

### 15.1 Before Every Commit

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                 # fully deterministic, no real sleeps
cargo metadata --format-version 1 | jq -e '[.packages[] | select(.source == null) | select(.rust_version != null)] | length == 0'
```

Optional feature verification:

```bash
cargo test --workspace --features collect-deep-hardware
cargo clippy --workspace --all-targets --features collect-deep-hardware -- -D warnings
cargo check --workspace --no-default-features --features collect-sysinfo
cargo check --workspace --no-default-features --features collect-sysinfo,control-http
cargo check -p monitor-agent --target x86_64-pc-windows-msvc --no-default-features --features collect-deep-hardware,windows-service
```

### 15.2 Code Discipline (CONTRIBUTING / AGENT.md)

- **Single-writer principle**: state machines change only inside the eval task; cross-task communication passes immutable messages.
- **Observable degradation**: unavailable data is `Option` + `Degradation`; faking 0 is forbidden.
- **Monotonic clocks**: duration judgments use `Instant`; `SystemTime` is for event timestamps only.
- **Identity checks first**: kills/restarts are gated on `(pid, start_time)` verification; attach targets are never touched.
- **No `unwrap()`/`panic!` on the run path** (poisoned locks recover via `into_inner()`); errors unify under `CoreError`.
- **Log discipline**: recovery/alert/degradation events must be `INFO`+ with structured fields (`target`/`check`/`incident`/`degraded`/`pid`/`attempt`); no hot-path DEBUG spam.
- **Secret discipline**: tokens/signing keys/header values/private key material never enter tracing fields, logs, audits, or error messages; assembly logs report booleans (e.g. `signed = true/false`) or counts only.
- **edition 2024 notes**: prefer `libc`/`windows-sys` for platform FFI; hand-written FFI uses `unsafe extern` blocks; `unsafe_op_in_unsafe_fn` is warn-by-default; avoid the identifier `gen`; dyn scenarios use `#[async_trait]` uniformly (e.g. `Notifier`).

### 15.3 PR Checklist

- [ ] `cargo fmt --check` / `clippy -D warnings` / `cargo test --workspace` all green
- [ ] New behavior has deterministic tests (injected time parameters, no real sleeps)
- [ ] Semantic changes update DESIGN.md (state machines/defaults/schema) and ROADMAP.md (status/deviation table)
- [ ] Cross-platform impact: platform branches have degradation notes (DESIGN §6.4 matrix)
- [ ] Alert/recovery semantic changes: add tests against the §7.2 five rules and the §8.2 transition table
- [ ] CHANGELOG.md gains an entry

### 15.4 Key Historical Lessons (read before coding)

- **Never guard select branches with a precondition**: an `if REQUESTED` guard disables the branch so `notified()` never registers a waiter; notify permits go unwoken (round-17 P0; pinned by `inprocess_shutdown.rs`).
- **The eval report is the reverse base for merged_metrics**: `merged_metrics` uses the eval report as the base and overrides only the three alert fields — new MetricsReport fields pass through automatically, eradicating the "missed field" defect class (round 18).
- **Critical journal events must await acknowledgment**: a `try_send` path can trip the critical-failure latch under congestion while the in-memory side has already taken effect (round 19 P3).
- **Wrap slow-disk operations in spawn_blocking**: startup replay and reload config reads must never freeze the eval single writer.
- **Header value validation follows the http crate byte rules** (non-ASCII visibility issue, round-14 P2).
- **reqwest 0.13 renamed a feature**: `rustls-tls` → `rustls` (README/DESIGN use the new name).
- **Windows service shutdown order**: `request_shutdown()` must be called before reporting StopPending (live SCM debugging lesson).

---

## 16. Extension Points: How to Add X

- **A new check kind**: add a `CheckKind` variant + probe implementation in `eval/check.rs` (returning `ProbeResult`); add fields/validation in `config.rs`'s `CheckCfg` flatten layer; sync `config.example.toml`; state machine and denoising come for free.
- **A new notification outlet**: implement `#[async_trait] Notifier` (dyn-compatible); wrap it into the dispatch chain at runtime assembly; keep timeout/backoff/queue/spool semantics in the dispatch layer, uniformly.
- **A new journal event**: add a `JournalEvent` variant (serde tag snake_case) → choose the write path (critical/acknowledged vs ordinary/try_send) → add the replay apply branch → extend `JournalState` accordingly.
- **A new config field**: ConfigRaw → default → validate cross-check → sync the example → DESIGN §4 schema/defaults table → decide the reload scope (topology or restart-required).
- **A new platform capability**: put implementations under `#[cfg(target_os = ...)]` or target-specific dependencies; you must also provide that platform's compile check and a degradation note (into the DESIGN §6.4 matrix).
- **A new control endpoint**: mount it on the protected Router (auth applies automatically); add a `ControlCommand` variant (consumed eval-side); add a monitorctl subcommand with exit-code semantics; check `Config::redacted()` for any new sensitive field.

---

## 17. Document Map

| Document | Contents |
|---|---|
| [AGENT.md](../../AGENT.md) | engineering hard constraints (highest precedence) |
| [DESIGN.md](../../DESIGN.md) | the authoritative implementation spec (schema/state machines/concurrency/platform contracts) |
| [ROADMAP.md](../../ROADMAP.md) | feature specs, acceptance criteria, progress, known-deviation table |
| [EXECUTION.md](../../EXECUTION.md) | phased execution plan |
| [BENCHMARK.md](../../BENCHMARK.md) | peer-project comparison and selection rationale |
| [CONTRIBUTING.md](../../CONTRIBUTING.md) | build/test commands and the PR checklist |
| [CHANGELOG.md](../../CHANGELOG.md) | releases and review-fix records |
| [docs/RUNBOOK.md](../RUNBOOK.md) | ops manual (consult while troubleshooting) |
| [docs/DEPLOYMENT.md](../DEPLOYMENT.md) | deployment manual |
| [user-guide.zh.md](user-guide.zh.md) / [user-guide.en.md](user-guide.en.md) | user guides |
| [developer-guide.zh.md](developer-guide.zh.md) | this document, Chinese |

---

*Compiled from the main branch source (v0.3.0) as of 2026-11-22; where implementation details disagree with the code, the code and [DESIGN.md](../../DESIGN.md) win.*
