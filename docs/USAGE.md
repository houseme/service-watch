# service-watch Usage Guide / 使用指南

> Bilingual guide (English / 中文对照). Applies to main branch (0.3.0, incl. v0.4-v0.8); semantics follow
> [DESIGN.md](../DESIGN.md). For platform installation, see
> [docs/DEPLOYMENT.md](DEPLOYMENT.md); for incident handling, see
> [docs/RUNBOOK.md](RUNBOOK.md).
>
> 双语对照使用指南 (英文 / 中文)。适用 main 分支(0.3.0,含 v0.4-v0.8),语义以 [DESIGN.md](../DESIGN.md)
> 为准;分平台安装见 [docs/DEPLOYMENT.md](DEPLOYMENT.md),故障处置见
> [docs/RUNBOOK.md](RUNBOOK.md)。

---

## 1. Overview / 概述

**EN**: service-watch is a cross-platform service & hardware monitor written in Rust 2024. One agent binary runs the loop `collect → evaluate → alert → recover`, serves a local HTTP control plane, and keeps supervised processes alive. `monitorctl` is the operator CLI. Core logic lives in the `monitor-core` library.

**中文**:service-watch 是用 Rust 2024 编写的跨平台服务与硬件监控工具。单个 agent 二进制运行 `collect → evaluate → alert → recover` 循环，提供本机 HTTP 控制平面，并监管被托管进程的存活;`monitorctl` 是运维 CLI;核心逻辑位于 `monitor-core` 库。

| Component / 组件 | Role / 职责 |
|---|---|
| `monitor-agent` | Daemon: collect metrics, run checks, alert, supervise & recover processes / 守护进程：采集、检查、告警、监管与恢复 |
| `monitorctl` | Operator CLI against the control plane + local snapshot tool / 面向控制平面的运维 CLI + 本机快照工具 |
| `monitor-core` | Library: config, collect, eval, alert, recover, control / 库：配置、采集、评估、告警、恢复、控制平面 |

---

## 2. Build & install / 构建与安装

**EN**:

```bash
cargo build --release -p monitor-agent -p monitorctl
# artifacts: ./target/release/monitor-agent, ./target/release/monitorctl
```

Optional features:

| Feature | Effect |
|---|---|
| `collect-deep-hardware` | Deep sensors: Linux hwmon / macOS AppleSMC fans & voltage |
| `windows-service` | Windows SCM service entry point (Windows only) |

```bash
cargo build --release -p monitor-agent -p monitorctl --features collect-deep-hardware
cargo build --release -p monitor-agent --features windows-service   # Windows SCM
```

Notes: the default Windows HTTP/TLS stack is rustls + aws-lc-rs, which needs NASM and CMake. There is no daemonize mode — run the agent under systemd / launchd / SCM.

**中文**:构建产物为 `./target/release/monitor-agent` 与 `./target/release/monitorctl`。可选 feature:`collect-deep-hardware`(深度传感器:Linux hwmon / macOS AppleSMC 风扇与电压) 与 `windows-service`(Windows SCM 服务入口，仅 Windows)。注意:Windows 默认 HTTP/TLS 栈为 rustls + aws-lc-rs，需要 NASM 和 CMake。agent 无 daemonize 模式，必须交给 systemd / launchd / SCM 托管。

---

## 3. Quick start / 快速开始

**EN**:

```bash
# 1. validate a config offline (exit 0 = ok)
./target/release/monitor-agent --check-config config/config.example.toml

# 2. one-shot hardware snapshot (no runtime started)
./target/release/monitor-agent --dump-snapshot

# 3. create service-watch.toml (see §4) and run
./target/release/monitor-agent --config service-watch.toml

# 4. in another terminal
./target/release/monitorctl status
```

Then open the status page at `http://127.0.0.1:9170/`.

**中文**:① 用 `--check-config` 离线校验配置 (退出码 0 = 通过);② `--dump-snapshot` 采集一次硬件快照即退出 (不启动运行时);③ 创建 `service-watch.toml`(见 §4) 后用 `--config` 启动;④ 另开终端用 `monitorctl status` 查看。浏览器打开状态页 `http://127.0.0.1:9170/`。

Minimal config / 最小配置：

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

**EN**: a check with the same `name` as a service feeds that service's recovery decisions; independently named checks are alert-only.

**中文**:检查与 service 同 `name` 时，其健康结论参与该服务的恢复判定;独立命名的检查只提供告警。

---

## 4. Configuration / 配置说明

**EN**: full annotated reference: [config/config.example.toml](../config/config.example.toml) (maps 1:1 to DESIGN.md §4). Unknown keys are rejected (`deny_unknown_fields`), so typos fail fast at `--check-config`. Durations accept `10s` / `5m` style strings.

**中文**:完整带注释参考：[config/config.example.toml](../config/config.example.toml)(与 DESIGN.md §4 一一对应)。未知键一律拒绝 (`deny_unknown_fields`),写错键名在 `--check-config` 即报错。时长字段接受 `10s` / `5m` 形式。

| Section / 段 | Purpose / 用途 | Key fields / 关键字段 |
|---|---|---|
| `[agent]` | Data location / 数据目录 | `data_dir`(journal root) |
| `[log]` | Logging / 日志 | `json` = structured JSON logs / 结构化 JSON 日志 |
| `[control]` | HTTP control plane / 控制平面 | `enabled`, `listen`(loopback by default / 默认仅回环) |
| `[collect]` | Hardware & process snapshot cadence / 采集节奏 | `interval`(default 5s) |
| `[alert]` | Webhook delivery / 告警投递 | see §6 / 见 §6 |
| `[metrics]` | Periodic metrics push / 指标周期推送 | see §7 / 见 §7 |
| `[[check]]` | Monitoring-only targets / 仅监控目标 | see §5 / 见 §5 |
| `[[service]]` | Supervised targets / 监管目标 | see §5 / 见 §5 |
| `[[watch]]` | Attach targets (never killed) / attach 目标 (永不杀) | `pid` or `pidfile` |

---

## 5. Checks, services, watches / 检查、监管与 attach 目标

**EN** — three target kinds:

- `[[check]]` — monitoring + alerting only. `kind = http`(with `url`, `expected_status` default `200-299`) / `tcp`(with `addr`) / `process`(thresholds on CPU/mem of a named or `pid:NNNN` process). Constraint: `interval > timeout` (enforced).
- `[[service]]` — supervised targets the agent (re)starts: `command`(no daemonizing targets), `args`, `workdir`, `env`, `depends_on`(startup ordering, references must exist, no cycles). Restart policy lives in `[service.restart]`: `min_uptime`, `backoff_initial`→`backoff_max`, `max_restarts` within `restart_window`(sliding), `stop_grace`(graceful→force-kill deadline), `startup_grace`(failures during grace don't enter the state machine), `confirmations`, `hung_action = restart | activate`. With `activate`, `[service.activate]` defines the kick hook (`command` XOR `url`, `timeout`, `verify_for` observation window).
- `[[watch]]` — attach targets: monitor only, never killed/restarted; identify by `pid` or `pidfile`(must be a non-empty path).

**中文**——三类目标：

- `[[check]]`——仅监控 + 告警。`kind = http`(`url`、`expected_status` 默认 `200-299`)/ `tcp`(`addr`)/ `process`(对具名或 `pid:NNNN` 进程设 CPU/内存阈值)。强制约束:`interval > timeout`。
- `[[service]]`——由 agent 拉活的监管目标：`command`(禁止 daemonize 型目标)、`args`、`workdir`、`env`、`depends_on`(启动编排，引用必须存在、禁止自依赖与环)。重启策略在 `[service.restart]`:`min_uptime`(crash loop 嫌疑线)、`backoff_initial`→`backoff_max`、滑动窗口内 `max_restarts`/`restart_window`、`stop_grace`(graceful→强杀升级时限)、`startup_grace`(预热期失败不入状态机)、`confirmations`、`hung_action = restart | activate`。选 `activate` 时在 `[service.activate]` 配置激活钩子 (`command` 与 `url` 二选一、`timeout`、`verify_for` 观察期)。
- `[[watch]]`——attach 目标：只监控，永不杀/重启;以 `pid` 或 `pidfile` 标识 (不允许空路径)。

---

## 6. Alerts & notifications / 告警与通知

**EN**:

- Thresholds: `failure_threshold` consecutive failures → Critical; `success_threshold` consecutive successes → Resolve. A warning trail precedes Critical.
- Delivery: `webhook_url` + `webhook_format = json | slack`, per-delivery `timeout`(10s default) with `retries` exponential-backoff attempts(1s, ×2, capped 10s; `retries` must be ≤ 100). Final failure → ERROR log + `alert_delivery_failures` counter.
- `renotify_interval`: repeat notifications while the same incident stays open(first notification is never cooled down).
- Flap suppression: ≥ `flap_threshold` open/close transitions within `flap_window` → summarized, not notified.
- `deadman_url`: optional heartbeat POST("agent alive but deaf" is covered by external heartbeat monitoring).
- Persistence: failed deliveries are spooled to `{data_dir}/spool/pending.jsonl`, replayed after a confirmed success, deleted then, expire after 7 days.
- The bounded queue (256) drops the oldest on overflow (alert timeliness over completeness).

**中文**:

- 阈值：连续 `failure_threshold` 次失败 → Critical;连续 `success_threshold` 次成功 → Resolve;Critical 前有 warning 级 trail。
- 投递：`webhook_url` + `webhook_format = json | slack`,单次投递 `timeout`(默认 10s),失败按 `retries` 次指数退避重试 (1s 起步 ×2，封顶 10s;`retries` 上限 100)。最终失败记 ERROR + `alert_delivery_failures` 计数。
- `renotify_interval`:同一 incident 持续 open 期间按间隔重复通知 (首条通知永不冷却)。
- flap 抑制：`flap_window` 内 open/close 达到 `flap_threshold` 次 → 记摘要，不再逐次通知。
- `deadman_url`:可选心跳 POST("agent 活着但失聪"由外部心跳监控兜底)。
- 持久化：投递失败事件写入 `{data_dir}/spool/pending.jsonl`,收到成功确认后重放并删除，7 天过期。
- 有界队列 (256) 满时丢最旧 (告警时效优先于完整性)。

---

## 7. Metrics & observability / 指标与可观测性

**EN**:

| Source | Content |
|---|---|
| `GET /metrics` | JSON `MetricsReport`; `?format=prometheus` (or `Accept: text/plain`) gives Prometheus text (prefix `service_watch_`) |
| `GET /metrics/history?range=<secs>` | History window; default 300s, max 7200s; >600 points are downsampled keeping first/last |
| `GET /status` | Per-target health + recovery state machine position |
| `GET /ready` | 503 until config loaded + scheduler started; degraded collection does NOT fail ready |
| `GET /` | Read-only HTML status page (JS polls `/status` + `/metrics` every 2s) |
| `[metrics]` push | With `enabled = true` + `url`: POST the MetricsReport JSON every `interval` — single attempt, no retry, a failure is overwritten next round, consecutive-failure edges write a journal audit |

Journal events land in `{data_dir}/journal.jsonl` (append-only JSONL): `restart`, `fatal`, `reset`, `reset_targets`, `process_identity`, `spawn_intent`, `spawn_outcome`, `incident`(alert/escalate/flap/resolve), `audit`.

**中文**:

| 来源 | 内容 |
|---|---|
| `GET /metrics` | JSON `MetricsReport`;`?format=prometheus`(或 `Accept: text/plain`) 输出 Prometheus 文本 (前缀 `service_watch_`) |
| `GET /metrics/history?range=<secs>` | 历史窗口;缺省 300s，上限 7200s;超过 600 点降采样保留首尾 |
| `GET /status` | 各目标健康 + 恢复状态机位置 |
| `GET /ready` | 配置加载成功 + 调度启动前为 503;采集降级**不影响** ready |
| `GET /` | 只读 HTML 状态页 (页内 JS 每 2s 轮询 `/status` + `/metrics`) |
| `[metrics]` 推送 | `enabled = true` 且配置 `url` 后按 `interval` 周期 POST MetricsReport JSON——单次尝试无重试，失败下轮覆盖，连败边沿写 journal 审计 |

journal 事件写入 `{data_dir}/journal.jsonl`(append-only JSONL):`restart`、`fatal`、`reset`、`reset_targets`、`process_identity`、`spawn_intent`、`spawn_outcome`、`incident`(alert/escalate/flap/resolve)、`audit`。

---

## 8. Control plane & monitorctl / 控制平面与 monitorctl

**EN** — endpoints (default `127.0.0.1:9170`, loopback trust boundary):

| Endpoint | Behavior |
|---|---|
| `GET /health` | Liveness, always 200 |
| `GET /ready` / `GET /status` / `GET /metrics*` / `GET /` | See §7 |
| `POST /v1/restart/{target}` | Queue graceful restart; **409 if Fatal** |
| `POST /v1/check/{target}` | Force bound checks to run now |
| `POST /v1/reset/{target}` | Clear Fatal latch + restart budget (fix root cause first) |
| `POST /v1/reload` | Hot-reload topology (see §9) |

`restart`/`check`/`reset` are **queue-and-ack**: HTTP 202 means queued (queue capacity 16, full → 503); targets are not validated at admission, so confirm the outcome via `status` and the journal.

monitorctl: `monitorctl [--url <URL>] <COMMAND>` with `status [--json]`, `watch [--interval s] [--json] [--exit-on-error]`, `check|restart|reset <target>`, `metrics [--prom]`, `dump-snapshot [--config <path>]`. Exit codes: `0` success, `1` config error, `2` runtime error (incl. 409/non-2xx), `3` target-not-found (reserved; rarely returned — invalid targets are rejected agent-side), `4` agent unreachable. monitorctl logs stay silent unless `RUST_LOG` is set.

**中文**——端点 (默认 `127.0.0.1:9170`,本机信任边界):`GET /health` 恒 200;`restart/check/reset` 为**入队即 202**(命令通道容量 16，满则 503),入队**不校验目标存在性**,请以 `status` 与 journal 确认实际效果;Fatal 目标 `restart` 直接 **409**(先修复故障再 `reset`)。

monitorctl:`monitorctl [--url <URL>] <COMMAND>`,子命令 `status [--json]`、`watch [--interval s] [--json] [--exit-on-error]`、`check|restart|reset <target>`、`metrics [--prom]`、`dump-snapshot [--config <path>]`。退出码：`0` 成功;`1` 配置错误;`2` 运行错误 (含 409/非 2xx);`3` 目标不存在 (保留码，基本不出现——无效目标由 agent 侧拒绝并记审计);`4` agent 不可达。monitorctl 自身日志默认静默，设 `RUST_LOG` 才输出 (stderr)。

---

## 9. Hot reload / 配置热重载

**EN**: `SIGHUP` and `POST /v1/reload` apply **check / service / watch topology changes only** (add/remove/redefine targets). Every other config section — including `[control]` (listen, token) and `[alert]` / `[collect]` / `[metrics]` — requires an agent restart. Redefining a service waits for the old instance to be reaped before starting the new one.

**中文**:`SIGHUP` 与 `POST /v1/reload` 只应用 **check / service / watch 拓扑变更**(增删/重定义目标);其余所有配置段——含 `[control]`(listen、token) 与 `[alert]` / `[collect]` / `[metrics]`——都需要重启 agent 才生效。服务重定义会等待旧实例收割后再启动新实例。

---

## 10. Recovery semantics / 恢复语义

**EN**:

- Supervised processes run in a process group (POSIX) / Job Object (Windows). The agent never daemonizes targets.
- A health failure triggers recovery only after `confirmations` consecutive observations; failures inside `startup_grace` don't enter the state machine.
- Restart path: graceful stop → `stop_grace` deadline → force kill; restarts back off `backoff_initial`→`backoff_max`; `max_restarts` within the sliding `restart_window` and crash-loop suspects (`min_uptime`) escalate to the **Fatal latch** — cleared only by `reset`(manual or monitorctl), never by time.
- Crash safety: before each spawn a `spawn_intent` is journaled, the outcome as `spawn_outcome`; on restart the agent reconciles unknown instances from these records plus `(pid, start_time)` identity evidence — no double-spawn, no fail-open.
- Attach (`watch`) targets and supervised children survive agent exit; on the next start the agent reconciles orphans from journal identities plus fresh collection evidence.

**中文**:

- 监管进程以进程组 (POSIX) / Job Object (Windows) 运行;agent 不 daemonize 目标。
- 健康类失败需连续 `confirmations` 次确认才触发恢复;`startup_grace` 预热期内的失败不入状态机。
- 重启路径:graceful 停止 → `stop_grace` 时限 → 强杀;重启按 `backoff_initial`→`backoff_max` 退避;滑动窗口 `restart_window` 内达到 `max_restarts`、或 `min_uptime` 判定的 crash loop 嫌疑，升级为 **Fatal 闩锁**——只能 `reset`(人工或 monitorctl) 解除，时间不会自动恢复。
- 崩溃安全：每次 spawn 前先落盘 `spawn_intent`,结果落盘 `spawn_outcome`;重启后 agent 用这些记录加 `(pid, start_time)` 身份证据对账未知实例——不双拉、不 fail-open。
- attach(`watch`) 目标与被监管子进程在 agent 退出后继续存活;下次启动时 agent 依据 journal 身份与新鲜采集证据做孤儿对账。

---

## 11. Run as a system daemon / 以系统守护进程运行

**EN**: step-by-step unit/plist/SCM installation, hardening and log rotation per platform:

- Linux (systemd): [docs/DEPLOY-LINUX.md](DEPLOY-LINUX.md)
- macOS (launchd): [docs/DEPLOY-MACOS.md](DEPLOY-MACOS.md)
- Windows (SCM): [docs/DEPLOY-WINDOWS.md](DEPLOY-WINDOWS.md)
- Shared scripts (install, perf-check, soak-24h): [docs/deploy/README.md](deploy/README.md)

Agent exit codes: `0` clean, `1` config error, `2` runtime/critical persistence error, `130` SIGINT/SIGTERM graceful shutdown (treated as success by the systemd unit).

**中文**:分平台的 unit/plist/SCM 安装、加固与日志轮转步骤:Linux(systemd) 见 [DEPLOY-LINUX.md](DEPLOY-LINUX.md);macOS(launchd) 见 [DEPLOY-MACOS.md](DEPLOY-MACOS.md);Windows(SCM) 见 [DEPLOY-WINDOWS.md](DEPLOY-WINDOWS.md);公共脚本 (安装、perf-check、soak-24h) 见 [deploy/README.md](deploy/README.md)。agent 退出码：`0` 正常;`1` 配置错误;`2` 运行/关键持久化错误;`130` SIGINT/SIGTERM 优雅停机 (systemd unit 视为成功)。

---

## 12. Operations & troubleshooting / 运维与排障

**EN**:

- Baseline watch items: `alert_delivery_failures` growing, `alerts_dropped` growing, `probe_skipped` growing, `recovery_fatal > 0`, `degradations_events` spikes.
- Journal growth estimation and copy+truncate rotation recipes: RUNBOOK §6.
- Short synthetic load acceptance: `bash docs/deploy/perf-check.sh ./target/release/monitor-agent 60`; 24h soak gate passed 2026-11-17 (CPU 0.46% avg, RSS steady growth 176KB) — re-verify with `bash docs/deploy/soak-24h.sh`.
- Symptom-tree troubleshooting (control plane unreachable, checks all Critical, Fatal handling): [docs/RUNBOOK.md](RUNBOOK.md).

**中文**:

- 巡检基线：`alert_delivery_failures` 增长、`alerts_dropped` 增长、`probe_skipped` 持续增长、`recovery_fatal > 0`、`degradations_events` 突增。
- journal 增长估计与 copy+truncate 轮转配方:RUNBOOK §6。
- 本机合成负载快速验收：`bash docs/deploy/perf-check.sh ./target/release/monitor-agent 60`;24h 长跑完成门已于 2026-11-17 PASS(CPU 均值 0.46%、RSS 稳态增长 176KB),用 `bash docs/deploy/soak-24h.sh` 复验。
- 症状树排障 (控制面不可达、check 全面 Critical、Fatal 处置):[docs/RUNBOOK.md](RUNBOOK.md)。

---

## 13. Shipped v0.4/v0.5 features / v0.4/v0.5 已交付能力

**EN** (all backward compatible — unset = unchanged behavior; track [ROADMAP.md](../ROADMAP.md)):

| Feature | Behavior |
|---|---|
| `[control] token` (F23) | Optional Bearer token; every endpoint except `GET /health` requires `Authorization: Bearer <token>`; monitorctl takes `--token` or `SERVICE_WATCH_TOKEN` |
| `[agent] journal_max_size` (F24) | Journal size cap with single-generation rotation (`journal.1.jsonl`); replay reads gen-1 then current, so Fatal/budget survive rotation |
| `[[alert.routes]]` (F26) | Per-target webhook routing (first prefix match, `*` = all; fallback to `webhook_url`); spool replay re-routes by current config |
| F25 incident restore | Open incidents survive agent restart: restored into the engine, one re-notify per incident, resolve closes normally |
| F27 `POST/DELETE /v1/silence/{target}` | Suppress Alert/Escalate/Flap notifications for a target (monotonic deadline, auto-expire); Resolve and Fatal-latch alerts always pass; monitorctl `silence` / `unsilence` |
| F28 `GET /v1/journal` | Tail query over the current journal (`tail/target/type/since`, bounded); monitorctl `journal [--json]` |
| F29 HMAC signing | `[alert] webhook_secret` (+ route-level `secret`): deliveries carry `X-Service-Watch-Signature: sha256=<hex>` |

**中文**:以下能力已交付 (全部向后兼容——不配置则行为不变;进度见 [ROADMAP.md](../ROADMAP.md)):

| 能力 | 行为 |
|---|---|
| `[control] token`(F23) | 可选 Bearer 令牌;除 `GET /health` 外全部端点要求 `Authorization: Bearer <token>`;monitorctl 用 `--token` 或 `SERVICE_WATCH_TOKEN` |
| `[agent] journal_max_size`(F24) | journal 体积上限与单代轮转 (`journal.1.jsonl`);重放先上一代后当前，Fatal/预算跨轮转保持 |
| `[[alert.routes]]`(F26) | 按目标 webhook 路由 (前缀首匹配，`*` = 全部;回落 `webhook_url`);spool 重放按当前配置重新路由 |
| F25 incident 跨重启恢复 | open incident 重启后恢复进引擎并重通知一次，resolve 正常收口 |
| F27 `POST/DELETE /v1/silence/{target}` | 静默 target 的 Alert/Escalate/Flap 通知 (单调 deadline 自动过期);Resolve 与 Fatal 闩锁告警恒穿透;monitorctl `silence` / `unsilence` |
| F28 `GET /v1/journal` | journal 尾部查询 (`tail/target/type/since`,有界);monitorctl `journal [--json]` |
| F29 HMAC 签名 | `[alert] webhook_secret`(+ 路由级 `secret`):投递附 `X-Service-Watch-Signature: sha256=<hex>` |

---

## 14. Document map / 文档地图

| Document / 文档 | Contents / 内容 |
|---|---|
| [DESIGN.md](../DESIGN.md) | Authoritative semantics: config, ownership, state machines, platform contracts / 语义权威：配置、所有权、状态机、平台契约 |
| [docs/DEPLOYMENT.md](DEPLOYMENT.md) | Full deployment walkthrough / 完整部署与操作手册 |
| [docs/RUNBOOK.md](RUNBOOK.md) | Ops procedures & incident handling / 运维手册与故障处置 |
| [docs/DEPLOY-LINUX / -MACOS / -WINDOWS](DEPLOYMENT.md) | Per-platform installation / 三平台安装指南 |
| [ROADMAP.md](../ROADMAP.md) | Feature specs & progress / 功能规范与进度 |
| [CONTRIBUTING.md](../CONTRIBUTING.md) | Development & verification commands / 开发与验证命令 |

License: [Apache-2.0](../LICENSE)
