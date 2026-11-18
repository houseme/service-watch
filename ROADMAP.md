# ROADMAP.md

> 功能规范与交付追踪的唯一事实源。实现细节以 [DESIGN.md](DESIGN.md) 为准;硬性约束以 [AGENT.md](AGENT.md) 为准。
> 状态：`☐ 未开始` · `◐ 进行中` · `☑ 完成`。完成判定以各项"验收"为准，验收不可测的项不算完成。
> 版本引用核对日期:2026-11-10(sysinfo 0.40.1 / reqwest 0.13.5 / axum 0.8.9 / toml 1.2.1 / tokio 1.54 / rustc 1.99.1)。

---

## v0.1.0 — MVP:单机可用闭环

目标：在 macOS/Linux/Windows 上完成「采集 → 判定 → 告警 → 恢复 → 控制平面」全链路，所有状态机可确定性单测。

### F1 工作区骨架与硬约束 ☑

- [x] workspace 三 crate:`crates/{monitor-core, monitor-agent, monitorctl}`(统一 `crates/`,无 default-members)
- [x] 全部 `edition = "2024"`、`license = "Apache-2.0"`、**无 `rust-version`**、`resolver = "3"`
- [x] 重依赖 feature 化：`collect-sysinfo` / `http-client`(reqwest, default-features=false, rustls)/ `notify-webhook` / `probe-http` / `control-http`(axum)
- [x] monitor-core 的 tokio 只启用所需 feature，不使用 `full`
- [x] LICENSE(Apache-2.0 全文)/ .gitignore / rustfmt.toml / CI 工作流

**验收**:`cargo metadata` 工作区成员无任何 `rust_version`(jq 断言通过);`cargo check --workspace` 通过。
**注**:reqwest 0.13 已将 feature `rustls-tls` 更名为 `rustls`(2026-11-10 实测);约束守卫需 `select(.rust_version != null)`(cargo 恒定输出 null 键，`has()` 恒真)。

### F2 配置系统 ☑

- [x] 完整 TOML schema(见 DESIGN §4),serde 强类型 + `deny_unknown_fields`(除 check 的 flatten 层)+ `#[serde(default)]` 默认值总表
- [x] 交叉校验：每项 `timeout < interval`;URL scheme;`backoff_initial ≤ backoff_max`;`startup_grace ≥ min_uptime`;activate 钩子存在性;名称唯一
- [x] `config/config.example.toml` 与 schema 一一对应 (由单测保证:example 必须可解析)
- [x] `monitor-agent --check-config` 离线校验，退出码 0/1(E2E 实测)

**验收**:非法配置 (如 `timeout ≥ interval`) 被拒绝并给出可读错误;example 配置可通过 `--check-config`。

### F3 硬件与进程采集 ☑

- [x] `HardwareSnapshot`(CPU/内存/磁盘/网络/温度)+ `ProcessSnapshot`(按 watched PID 部分刷新)
- [x] sysinfo 0.40 API 锚点适配 (DESIGN §6.1):`System::new() -> Result`、`global_cpu_usage()`、`Component::temperature() -> Option<f32>`(另:0.40 中 Disks/Networks 已独立于 System，采集器持有三个子采集器)
- [x] CPU 首采语义：初始化时隔 `MINIMUM_CPU_UPDATE_INTERVAL` 双采样，首 tick 数据不可用时写 `degradations`,**不返回伪造 0**
- [x] 部分刷新协议：每轮传入**全部** watched PID;`(pid, start_time)` 不匹配视为 PID 复用排除
- [x] 温度可信过滤 (-20..=150 ℃ 之外丢弃并记 `Degradation`;NaN/±Inf 自动拒;`tdev`/`als-temp` 噪声 label 跳过),`Temperature.celsius: Option<f32>`
- [x] 机读降级枚举 `Degradation`(非自由文本)
- [x] 采集运行在专用阻塞线程，经 `watch` 通道广播 (E2E:--dump-snapshot 输出真实 CPU/内存数据)

**验收**:`monitor-agent --dump-snapshot` 输出合法 JSON,CPU/内存/磁盘字段非空;温度为 None 时必有对应 Degradation;agent 自身 CPU < 2%(5s 间隔)。

### F4 检查引擎 ☑

- [x] `CheckKind::{Liveness, HttpProbe, TcpProbe, ProcessMetrics}`(check.rs:Tcp 直连、Http 状态区间、Process 阈值 + 身份校验)
- [x] 状态机 `Ok → Warning → Critical`:1 次失败进 Warning，连续 `failure_threshold`(默认 3) 次升级 Critical(state.rs,5 单测)
- [x] `success_threshold`(默认 2) 次连续成功才回到 Ok(抗 flapping)
- [x] 所有探测 `tokio::time::timeout` 包裹;**在途探测上限 1**,上轮未完成则跳过本轮并计数 (调度器)
- [x] 配置校验保证 `timeout < interval`;全部时长判定用单调时钟 (状态机纯函数注入，调度用 tokio 时间)
- [x] 状态机确定性单测 (时间作为参数注入;等价 Clock 注入)

**验收**:注入时钟单测覆盖全部状态转移;`tests/http_probe.rs`(axum stub) 端到端验证：慢响应 (> timeout)→ Warning → Critical → Resolve 全链路 + 状态区间闭边界 (199/200/299/300)。

### F5 告警引擎 ☑

- [x] incident 模型：`incident_key = hash(target, check)`(不含 severity/reason);incident 生命周期 open → escalate → resolve
- [x] 告警事件 `AlertEvent{category: Alert|Escalate|Resolve|Flap, severity, ...}`;Resolve 必须引用原 incident
- [x] 去噪：同 incident 持续期间的重复通知受 `renotify_interval`(默认 15m) 抑制;**新 incident 的首条通知永不冷却**(杜绝 Resolved 后复发被吞)
- [x] flap 抑制：`flap_window`(默认 30m) 内 open/close ≥ `flap_threshold`(默认 5) 次 → 抑制并只发一条 Flap 摘要
- [x] `Notifier` trait(`#[async_trait]`,dyn 兼容)+ fan-out 分发器;`WebhookNotifier`(Slack 兼容 + 通用 JSON)
- [x] 投递语义：有界队列 (256) 满则丢最旧并记审计;每笔投递 timeout(默认 10s)+ 指数退避重试 (默认 3 次);最终失败记 error 日志 + 内部计数
- [x] deadman 心跳 (可选配置 `deadman_url`):周期 POST，失败仅记日志
- [x] Warning→Ok 同样产生 Resolve(与 EXECUTION 验收一致);从未发过通知的 incident 恢复时不发 Resolve

**验收**:单测覆盖:fingerprint 不随 severity 变化;Resolved 后立刻复发能再告警;flap 进入抑制;队列满丢最旧有审计日志。

### F6 进程恢复与孤儿接管 ☑

- [x] 两种模式：**supervise**(agent spawn，可重启) 与 **attach**(pid/pidfile，仅监控告警，不可重启)
- [x] 进程身份 = `(pid, start_time)` 二元组，liveness/kill/接管前一律校验 (防 PID 复用误杀)
- [x] 恢复状态机全转移表 (DESIGN §8.2):补齐 spawn 失败、ForceKill 失败、Suspect 自愈、Restarting 超时、Fatal 等全部边
- [x] 触发契约：确定性死亡 (子进程退出事件或 liveness 连续确认) 直接进 Recovering;健康失败走 Suspect 二次确认
- [x] `restart_budget = {max_restarts, window}` 滑动窗口 (ring buffer),**所有自动重启均计数**;耗尽 → Fatal 闩锁，仅控制平面 `reset` 解除
- [x] `startup_grace`(默认 30s):重启后预热期不计失败;Restarting→Running 判据 = 存活超 min_uptime 且健康首次成功
- [x] HungAction::{Restart, Activate};Activate 有独立 `timeout` + `verify_for` 事后验证，失败转 Restart 并计入 budget
- [x] Unix:`process_group(0)` spawn + killpg;SIGTERM→SIGKILL 升级;子进程 reaper 防僵尸
- [x] Windows:spawn 后立即 AssignProcessToJobObject(见偏差表:tokio Child 不暴露主线程句柄，CREATE_SUSPENDED 方案不可行，已评估竞态微秒级)(默认**不带** KILL_ON_JOB_CLOSE，保护观察者独立),停止 = 显式 TerminateJobObject;graceful 基线如实为 TerminateProcess + 审计
- [x] agent 重启状态恢复已接线 (journal 重放 → fsm.restore,Fatal 目标跳过冷启动 spawn);孤儿进程使用持久化身份对账，精确确认后以只读 attach 接管，候选身份在本轮被查询且单调采样时间足够新时才参与裁决，失去证据时继续等待

**验收**:kill -9 后 backoff 内拉活并计数;crash loop 耗尽 budget 后 Fatal 且**不自动恢复**;`monitorctl reset` 解除 Fatal;attach 模式永不执行杀/重启。

### F7 状态持久化 ☑

- [x] JSONL journal(data_dir 下，append-only):incident 转移、重启事件、Fatal 闩锁、Reset、审计 (6 单测含坏行跳过与 Reset 顺序语义)
- [x] agent 重启后恢复 budget 窗口与 Fatal 闩锁 (**已接线产品路径**:runtime replay → SchedulerHandles.replayed → eval fsm.restore;E2E 日志实测 `restored_restarts=1`;Fatal 目标跳过冷启动 spawn，不越权复活)
- [x] Fatal 目标重启后保持 Fatal(先探测再确认的 Fatal 部分);Warning 级 trail 不跨重启恢复 (事件已入 journal，重通知语义 v0.2)

**验收**:单测:journal 写入→重放还原 budget/Fatal;集成:杀掉 agent 再启动，Fatal 仍在。

### F8 控制平面 ☑

- [x] agent 内嵌 HTTP server(默认 `127.0.0.1:9170`,feature `control-http`)
- [x] `GET /health`(浅存活)/ `GET /ready`(配置加载成功 + 调度循环已启动;降级采集**不**影响 ready)/ `GET /status`(全 target 状态与 incident)/ `GET /metrics`(JSON 指标)
- [x] `POST /v1/restart/{target}` / `POST /v1/check/{target}` / `POST /v1/reset/{target}`(解除 Fatal+ 清 budget，写审计)
- [x] monitorctl 作为薄 HTTP 客户端：`status` / `restart` / `check` / `reset` / `metrics [--prom]`(F13)/ `dump-snapshot`(本地，不需 agent)

**验收**:`monitorctl status` 拿到运行态;`restart` 在 Fatal 态下被拒绝并提示先 `reset`;agent 不可达时退出码 4。

### F9 CLI 与退出码 ☑

- [x] `monitor-agent --config <path>`(顶层 flag 形态) / `--dump-snapshot` / `--check-config` / `--version`
- [x] SIGTERM/SIGINT 优雅停机 (退出码 **130**,2026-11-11 接线;注:supervise 子进程默认保留运行——观察者外置原则，孤儿由下次启动对账，见 F6):停探测 → flush journal → 退出
- [x] 退出码表 (DESIGN §9.3;实测 0/1/2/4/130):0 成功 / 1 配置错误 / 2 运行错误 / 3 目标不存在 / 4 agent 不可达 / 130 信号

**验收**:`--check-config` 对非法配置返回 1;kill -TERM 后进程在 grace 内退出 (退出码 130) 且子进程保留。

### F10 可观测性 ☑

- [x] tracing:`RUST_LOG` EnvFilter(默认 info),`--log-json` 结构化开关
- [x] 结构化字段约定：`target` / `check` / `incident` / `degraded`(DESIGN §10)

**验收**:JSON 模式下每行可被 jq 解析且含约定字段。

### F11 测试与 CI ☑

- [x] 状态机/去噪/budget/backoff 注入时钟单测;Collector/Notifier mock(79 lib 测试)
- [x] 集成测试：本地 HTTP stub + 临时 supervise 目标脚本 (`tests/http_probe.rs` / `tests/supervise_smoke.rs`,macOS/Linux)
- [x] CI:三平台 `cargo fmt --check` + `cargo clippy --workspace -- -D warnings` + `cargo test --workspace`;Windows/macOS 交叉 `cargo check`(仅默认无 C 依赖 feature 集)

**验收**:CI 全绿;本地 `cargo test --workspace` 通过。

---

## v0.2.x — 加固与运维化 ◐

- [x] **F12 告警 spool 落盘 (at-least-once)**:投递失败的事件持久化到 spool 目录 (`{data_dir}/spool/pending.jsonl`),恢复后重放 (启动重放 + 周期重放);上限与过期策略 (`SPOOL_MAX_AGE` 7d、批量 16/次)—— 集成测试断言 at-least-once 闭环
- [x] **F13 Prometheus /metrics 文本格式**:与 JSON 并存 (`?format=prometheus` / `Accept: text/plain`);eval 每轮发布真实运行指标 (探测执行/跳过、恢复状态分布、降级事件、健康分布、uptime)—— 2026-11-11 完成，E2E 实测
- [x] **F14 配置热重载**:SIGHUP(unix)+ `POST /v1/reload`(body 可选 path);增量生效语义 = 目标拓扑增删改 (check 水位保留、service 重定义删 + 增、解析失败整体拒绝绝不半应用、重名防御);非拓扑段变更要求重启 (记审计);diff 纯函数 12 单测 + apply 层 5 单测 + 真机 E2E(增删改/坏配置拒绝/删除收割)—— 2026-11-11
- [x] **F15 Windows Service 形态**:`windows-service` 0.8.1 feature + `--service-run` 入口 (SCM 感知);`sc stop` → SERVICE_CONTROL_STOP → `request_shutdown()` 优雅停机 (替代 TerminateProcess 基线);服务模式文件日志;windows target 交叉验证通过;标准构建行为不变 (两形态对比见 docs/deploy/windows/README)—— 2026-11-12
- [x] **F16 deploy 示例**:systemd unit / launchd plist / Windows 服务安装脚本 (agent 自身二级守护)—— 2026-11-11 落地 `docs/deploy/` 三平台守护配置 + `docs/RUNBOOK.md`
- [x] **F17 性能预算验证**:快速门 (perf-check.sh,CPU 0.37%)+ **24h 完成门 PASS**(2026-11-17:1412 采样，排除首采样 CPU 均值 0.46% < 2%,RSS 稳态增长 176KB < 20MB，零 ready 失败，SIGTERM 优雅退出 130)—— `soak-24h.sh` 教训落地：输出目录默认改持久路径 (第一次 /var/tmp 被清理器抹掉)

## v0.3.x — 扩展 ☑

- [x] **F18 历史指标环形缓冲**:`/metrics/history?range=<secs>`(容量 7200、>600 点降采样保留首尾、非法 400);历史经 MetricsReport 句柄搭载既有 metrics watch(偏差：未新增 SchedulerHandles/ControlShared 字段，零 runtime 装配改动)—— 2026-11-12,E2E 实测
- [x] **F19 monitorctl watch 模式**:纯客户端轮询 (无新增端点);ANSI 帧渲染 + 变化 diff(保留 5 条)+ JSON Lines + 不可达重试/退出码语义;11 单测 + 真机 E2E —— 2026-11-12
- [x] **F20 多服务编排**:**启动顺序**(depends_on:校验引用/自依赖/环;门控 = FSM Running **+ 运行中进程证据**;依赖 Fatal 不级联)+ **停止顺序**(控制面 restart 级联：逆拓扑先停被依赖方，回 Running 后拉回，Fatal 跳过，journal 审计)—— 2026-11-12,7 单测含真实进程级联闭环
- [x] **F21 深度硬件采集**。可选 `collect-deep-hardware` feature，Linux hwmon 只读风扇与电压，macOS AppleSMC 固定常见键，Windows 显式 unsupported。入口覆盖 runtime 与两个 CLI 的 dump-snapshot。读数使用 Option，权限、无效值、禁用和故障均机读降级。验收包括 fixture、SMC 解码、feature 组合和三平台编译，实机传感器覆盖范围另记评审报告。**Apple Silicon 实机证据（2026-11-18，本机 darwin arm64 / 18 核 / 128GB，`--features collect-deep-hardware --dump-snapshot` 实测）**：风扇 2×（SMC/F0Ac 1350 RPM、SMC/F1Ac 1460 RPM，单位 RPM）；电压 SMC/VD0R = 27.83 V（±100 V 可信域内），SMC/VBUS = null 且带机读降级 `SensorRead{sensor:voltage, reason:invalid_data}`（无效读数不伪造，如实降级）；温度 17 传感器（PMU tdie 系列 45–46 ℃、gas gauge battery 31 ℃、NAND CH0 34 ℃,单位 ℃）—— 类型/单位/降级契约全部符合 F21 规范。
- [x] **F22 Web 状态页**:`GET /` 内嵌只读页 (零外部依赖，JS 轮询 /status+/metrics,health 徽章/Fatal 高亮/escapeHtml 防 XSS/不可达保灰)—— 2026-11-12,E2E 实测 (200 text/html 10KB)

---

## v0.4.x — 可信边界与运维闭环 ◐ (2026-11-18 规划)

目标：补齐 0.x 阶段的信任边界、journal 生命周期与告警路由缺口。**全部默认行为向后兼容**（不配置新字段 = 行为不变）。
门禁：与 v0.3 相同 —— fmt / clippy -D warnings / 全测 / 两条 no-default 组合 + `scm-target` 交叉编译；文档（README / DESIGN / RUNBOOK / DEPLOYMENT / example 注释）随实现同步。

### F23 控制面可选访问令牌 ☑ (2026-11-18;第九轮验证：全端点覆盖/恒定时间比较/无绕过)

- [x] `[control] token = "..."`（可选，字符串）；配置后除 `GET /health` 外全部端点要求 `Authorization: Bearer <token>`，缺失或错误一律 **401**（含状态页 `GET /`）—— control/mod.rs 路由拆 public(/health)/protected 两子 Router，protected 统一叠 axum middleware；401 统一 JSON 体 + `WWW-Authenticate: Bearer`，不区分失败原因
- [x] `/health` 保持无鉴权（守护进程/负载均衡探活契约不变）；`/ready`、`/status`、`/metrics*`、`/v1/*` 全部受保护
- [x] monitorctl：`--token <TOKEN>` flag + `SERVICE_WATCH_TOKEN` 环境变量（flag 优先，空值视为未提供）；未配置 token 的 agent 忽略任何 Authorization 头（行为不变）
- [x] 恒定时间比较防时序侧信道（手写 `ct_eq`：XOR 累积 OR、长度差计入，零新依赖）；token 不落 journal/日志（失败审计只记方法与端点；401 体不回显 token —— 单测断言）
- [x] 文档：README 功能边界行、DEPLOYMENT §12 信任边界、RUNBOOK 防火墙行同步"可选 Bearer 令牌" —— DEPLOYMENT §12 / RUNBOOK §4+§5+§1.1 / DESIGN §9.2.1 / monitorctl README（含"生产建议用环境变量传 token"注记）/ 根 README "Scope & boundaries" 控制面行 —— 全部完成（2026-11-18）

**验收**：集成测试 —— 无 token 配置时带/不带头都 200；有 token 配置时无头 401、错头 401、对头 200、`/health` 仍 200；monitorctl 端到端（env + flag）拿到 status；错误 token 的审计记录不含 token 明文。—— 已覆盖（2026-11-18）：control/mod.rs 4 个 F23 单测（真 socket，覆盖上述全部状态码矩阵）+ monitorctl 2 个 F23 单测（resolve_token 优先级 + 手写 TCP stub 端到端带/不带 token）；鉴权失败日志仅记 `method`/`path` 字段（代码路径无 token 字符串，日志内容未做自动断言，如实记录）。

### F24 journal 体积上限与轮转 ☑

- [x] `[agent] journal_max_size`（字节，默认 10 MiB，`0` = 禁用轮转保持现状）；仅软性触发：**写事件后实测**当前文件尺寸超限 → 轮转（选写后实测而非写前预判：兼容外部 copytruncate 清零，journal 事件频率低一次 metadata 系统调用可忽略）
- [x] 轮转动作 = 当前 `journal.jsonl` 原子改名 `journal.1.jsonl`（覆盖上一代，**只保留一代**）→ 立即新建当前文件继续写 → 写一条 audit（含轮转前后字节数；Windows 语义：先释放句柄再 rename）
- [x] 重放顺序：`journal.1.jsonl` 存在时按 `journal.1 → journal.jsonl` 依次重放（Fatal 闩锁/预算/未决 SpawnIntent 跨轮转保持 —— "Fatal 跨重启保持" 契约不因轮转破洞）
- [x] 改名失败的降级路径：保留当前文件继续写 + WARN（不停机；体积上限是运维目标不是安全边界；不触发持久化故障闩锁）

**验收**：单测 —— 超限触发轮转、journal.1 生成且当前文件续写、audit 落盘、二次轮转覆盖旧 journal.1；集成级 —— Fatal 写入后轮转再重启仍 Fatal；轮转文件内的未决 SpawnIntent 仍阻塞同名冷启动；`journal_max_size = 0` 完全不轮转。**已实现（2026-11-18）**：以上全部有单测锁定（journal.rs 7 个 F24 测试，含轮转失败降级与轮转后坏行隔离不回归）；产品写入口为 `run_tracked_writer_with_limit`（runtime 传入 `cfg.agent.journal_max_size`），4 参 `run_tracked_writer` 保持兼容签名不轮转（eval 既有调用方零改动）。

### F25 incident 跨重启恢复与重通知 ☑ (2026-11-18;第九轮验证：主链路正确，`__recover__` 合成 incident 闭门修复 + 单测)

- [x] 重放时恢复**仍处于 open 的 incident**（journal 已有 alert/escalate、无配对 resolve 的记录），含 warning 级 trail —— journal 侧 open_incidents 表（主会话）+ eval 侧 `IncidentTracker::restore` 注入（trail 以已通知状态注入，key 运行期复算不信任持久化值）；恢复逻辑仅在表非空时生效（空表零行为变化）
- [x] 启动恢复的 open incident 以重放时刻为新通知窗口起点（延续"首条通知永不冷却"原则，恢复后必然至少可再告警一次）—— 每起恢复 incident 产出一条重通知（类别复用 `Alert` 不新增枚举，severity 用恢复值，`reason = "restored from journal: <原 reason>"`，走既有 AlertQueue + journal Incident 写路径），`last_notified` = 恢复时刻即 renotify 窗口起点
- [x] flap 计数器**不**跨重启恢复（新会话 fresh 窗口，避免旧抖动史误抑制新告警）—— 偏差表记录该取舍（见下偏差表新增行）
- [x] `/status` 与 `/metrics`（`checks_ok|warning|critical`）在重放后立即反映恢复的 incident；恢复后 resolve 到来正常发 Resolve 通知 —— **如实细化**：`/status` 的 `incident` 字段立即反映（引擎注入后即可查）；`/metrics` 的 `checks_*` 与 target `health` 来自检查状态机，**刻意 fresh 起步**、随 live 探测在数个 interval 内自然再升级（回填状态机会让启动首轮失败直接喂 Critical 给恢复 FSM，破坏 §8.1 保守确认契约 —— DESIGN §7.2.1 记录取舍）
- [x] 配对完整性：resolve 事件引用不存在的 incident（历史遗留）在重放时丢弃并计数 —— `JournalState.orphan_resolves`（journal.rs 主会话补齐，2026-11-18，F25 测试断言）

**验收**：单测 —— 重放 open warning/critical → incident 状态恢复 + 重通知事件产出 + resolve 正常收口；配对 resolve 丢弃；集成 —— 杀 agent 再启动，`/status` 立即呈现恢复的 critical，且新一轮 Critical 能产生新通知。—— 已覆盖（2026-11-18）：eval 4 个 F25 单测（注入重放表 → 引擎含 incident + 重通知产出 + renotify 窗口重起 + resolve 收口 + flap fresh + 空表零变化）+ journal `orphan_resolves` 断言；tests/incident_restore.rs 2 个集成测试走**真实 journal 写入 → replay → 注入新一轮 agent** 路径（恢复 critical 立即呈现于 /status、重通知、探测恢复后 Resolve 收口、再故障产生新 Alert+Escalate、/metrics checks_critical 随 live 探测再升级；空表回归）。

### F26 按目标告警路由 ☑

- [x] `[[alert.routes]]` 数组：`{ match = "<target 名称字面前缀>", url = "...", format = "slack|json" }`（`*` 整串匹配全部 target）；按配置顺序首条匹配 target 生效（`alert::RoutingNotifier`,每条 route 独立 `WebhookNotifier`）
- [x] 无匹配回落默认 `webhook_url`（routes 与默认并存：默认 = 兜底）；路由匹配纯函数（可单测）；空 routes 行为与现状完全一致
- [x] 投递语义与默认通道一致（超时/退避/队列/spool at-least-once）；spool 重放按**当前配置**重新路由（重放走分发任务持有的同一 notifier 实例 → RoutingNotifier 自动生效）
- [x] `[alert.routes]` 变更属非拓扑段（reload 拒绝、要求重启）—— 与 alert 段语义一致（`eval::reload::diff_sections` 对整段序列化比较，routes 字段变更自动落入既有"alert 段要求重启"路径，无需新增代码）

**验收**：单测 —— 顺序首匹配、通配、无匹配回落；集成 —— 双 webhook stub 各自收到对应 target 的告警、未匹配 target 落默认端点；spool 注入失败事件后重放按新配置路由。**已实现（2026-11-18）**：alert/mod.rs 5 个路由单测 + tests/alert_routing.rs 2 个集成测试（axum 双 stub + spool 重放路由锁定）。

---

### v0.4 遗留人工项（非代码，持续跟踪）

- [ ] **Windows 实机验收**（沿用 v0.2 条目）：安装、SCM stop、错误退出恢复、Job Object 演练；交叉编译只提供类型证据
- [ ] **硬件实机覆盖**（沿用 v0.2 条目）：Linux hwmon 设备与更多 Intel/Apple Silicon 机型读数证据 —— **Apple Silicon 部分已采集（2026-11-18 本机 darwin arm64 实测，见 F21 验收行补记）**;Linux hwmon 与 Intel 平台仍需真机

---

## v0.5.x — 运维降噪与可观测出口 ◐ (2026-11-18 规划)

目标：把「部署期静默」「事后查证」「投递可验证」「状态页可读」四件运维高频事做成产品能力。**全部默认向后兼容**（不配置新字段 = 行为不变）。
门禁：与 v0.4 相同 —— fmt / clippy -D warnings / 全测 / 两条 no-default 组合；文档随实现同步。

### F27 目标级告警静默 ☑ (2026-11-18)

- [x] `POST /v1/silence/{target}`：body `{"duration": <秒数>, "reason": "..."}`（duration 为 JSON 数字；字符串 → 400）→ 该 target 的 Alert/Escalate/Flap 通知在窗口内**静默**（事件照常写 journal，`suppressed` 语义在通知管道侧实现 —— eval emit_alert 入队点拦截）；`DELETE /v1/silence/{target}` 提前解除；到点自动过期（单调时钟 deadline，与墙钟无关；tick 惰性清理 + 到期 Audit，过期条目不残留）
- [x] **Resolve 永不静默**（恢复通知必须穿透，webhook 级集成锁定）；**Fatal 合成告警同等豁免**（P1 第十轮验证：无 trail/renotify 兜底，被吞即永久丢失 —— eval 单测锁定）；静默叠加目标可重复设置（后到覆盖，抑制可见性边随新窗口复位）
- [x] 静默建立/解除/过期各写一条 journal Audit（建立含 reason 与到期语义；静默期首次抑制命中另写一条可见性 Audit，期内不逐条记录防刷屏）；monitorctl `silence <target> --duration <s> --reason <text>` + `unsilence <target>`
- [x] 非法 duration/未知 target 由 eval 侧拒绝（入队 202 + WARN 审计，模式与既有命令端点一致；HTTP 端点仅做 body 语法校验 400）

**验收**：单测 —— deadline 到期自动解除、重复设置覆盖、Resolve 穿透、duration/未知 target 拒绝、Audit 边沿、Fatal 豁免（eval 8 例）；集成 —— 静默期间 critical 不出 webhook、解除后新 Critical 出 webhook、Audit 边沿落盘（tests/silence_notification.rs:真实 eval 循环 + run_dispatcher + axum webhook stub）。**已实现（2026-11-18）**：语义注记 —— 静默表为 eval 内存态，agent 重启不保留（journal 重放不含静默，宁多响不漏报）；静默与 F20/Fatal 的恢复行为无耦合（Fatal 照常闩锁；仅其闩锁通知豁免静默，DESIGN §7.3）。

### F28 journal 尾部查询（API + CLI）☑ (2026-11-18)

- [x] `GET /v1/journal?tail=<n>&target=<name>&type=<事件类型>&since=<epoch 秒>`：尾部有界扫描（512KB 上限，`spawn_blocking`），坏行跳过，`truncated` 标志；受 F23 鉴权保护；仅读当前文件（journal.1 不在范围，DESIGN §9.2 / RUNBOOK §2.2 注明）（agent 侧主会话落地，端点层集成测试本会话补齐）
- [x] monitorctl `journal [--tail <n>] [--target <name>] [--type <type>] [--since <epoch>] [--json]`：表格化输出（时间/target/type/摘要，摘要按类型取 reason/message）+ `--json` 原始行（JSON Lines）；`truncated` stderr 提示；agent 不可达退出码 4 / 401 复用既有 token 提示
- [x] agent 侧端点已由主会话落地（`ControlShared.journal_path` + `journal_tail_lines` 纯函数）

**验收**：单测 —— tail/截断/过滤（target/type/since）/坏行隔离（`journal_tail_lines_filters_and_truncates`）+ CLI 参数解析/表格渲染/URL 拼装/摘要选取（monitorctl 5 例）；集成 —— 真实 journal 文件经端点返回正确过滤结果（`journal_endpoint_serves_real_file_with_filters`:tail 截断 + target/type 过滤 + 非法参数 400）；monitorctl 端到端 = fetch 层 TCP stub 测试（truncated/行解码/401 分类）+ 渲染纯函数单测，真实 agent 的 stdout 打印未自动化（人工验证）。**已实现（2026-11-18）**。

### F29 webhook HMAC 签名 ☑ (2026-11-18)

- [x] `[alert] webhook_secret` + 路由级 `secret`（路由级覆盖全局，均 None 不签名）；`hmac` + `sha2` workspace 依赖（纯 Rust，notify-webhook feature 门控；字段与校验由主会话先行落地）
- [x] 每次投递附 `X-Service-Watch-Signature: sha256=<hex hmac-sha256(body)>`；RFC 4231 已知向量单测；集成测试 stub 端实算验签
- [x] 签名覆盖 slack 与 json 两种格式的最终 body；spool 重放同样签名

**验收**：单测 —— RFC 4231 向量、路由级覆盖全局、None 不加头；集成 —— 双端点（有 secret / 无 secret）分别验签与裸投递。**已实现（2026-11-18）**：webhook.rs 2 个单测（RFC 4231 TC2 hex 对照 RFC 原文与 openssl 双重核实）+ runtime.rs 路由覆盖单测 + tests/alert_routing.rs 3 个集成测试（验签 stub 服务端重算 HMAC：双格式验签通过/裸投递无头/错签按投递失败）；密钥与签名值不落日志。

### F30 状态页历史迷你图 ☑ (2026-11-18;人工目验待做)

- [x] status_page.html 内嵌零依赖 SVG 迷你图：`/metrics/history` 拉取后绘制 `checks_critical` 与 `recovery_fatal` 折线（escapeHtml 纪律延续 —— 折线坐标全部由 num() 防护后的数字计算，无文本插值；历史为空/接口失败不渲染，fetch 失败不阻塞既有 status/metrics 渲染）
- [x] 图形无写入口、无外部资源引用（与既有零依赖约束一致）

**验收**：集成 —— 状态页 200 且含 SVG 元素（tests/status_page.rs:`spark-*` 容器标记 + /metrics/history 样本契约）、历史非空时折线点数与 /metrics/history 一致；页面 JS 语法检查（node --check）**已跑（2026-11-18，通过）**。**人工目验已完成（2026-11-18，浏览器实测）**：真实 agent（无目标配置 → 全零序列正确贴基线；再配一个必然失败的 tcp check → `checks_critical` 折线清晰呈现 0→1 阶跃并保持在顶部，`recovery_fatal` 恒贴基线，critical 徽章高亮，`alerts_sent=2` 与端点数据一致）—— 截图证据存评审记录;meta 行同步修正为数据真实最大值（旧实现全零序列显示刻度值 "max 1" 有误导）。

---

## v0.6.x — 远程可信与运维节奏 ☑ (2026-11-18 规划并交付;第十一轮双专家验证收口)

目标：把控制面推到「可远程、可审计时点」的完成态。**全部默认向后兼容**（不配置新字段 = 行为不变）。
门禁：与 v0.5 相同 —— fmt / clippy -D warnings / 全测 / 两条 no-default 组合；文档随实现同步。

### F31 定期维护窗口 ☑

- [x] `[[maintenance]]`：`{ name?, targets, start_utc = "HH:MM", duration, days? }` —— 全部时刻按 **UTC** 计算（std 无时区库，如实限定 UTC、不支持 DST，文档明示）；`days` 缺省 = 每天；targets 前缀匹配同 `[[alert.routes]]`（`*` = 全部）（eval/maintenance.rs:`MaintenanceWindow::parse`/`matches_target`）
- [x] 窗口内匹配 target 的 Alert/Escalate/Flap 通知被抑制 —— **复用 F27 静默管线**（emit_alert 同一入队抑制点，F27/F31 任一命中即抑制；Resolve 与 Fatal 合成告警恒穿透；journal Incident 照常写）；窗口开启/关闭各写一条 journal Audit（每 tick 边沿判定 + per-window 状态位；有 name 报 name，无 name 报 targets；抑制命中不逐条审计 —— 开启 Audit 即可见性边沿）
- [x] 判定纯函数（epoch → days-of-week/分钟 of day，UTC 手工历法：`(days+3)%7`，epoch 0 = 周四）可单测；跨 midnight 窗口按「开启时刻起的 elapsed」判定（候选开启日区间 `[d×86400+start, +duration)`，越日界部分归属开启日，任意 duration 含 ≥24h 统一精确）与重启后的窗口重入正确（无持久状态，UTC 时钟自然重入）
- [x] 与 F27 silence 叠加语义：两者任一命中即抑制；文档写明优先级与叠加（DESIGN §7.3 F31 小节，无优先级，都是"抑制"）

**验收**：单测 —— HH:MM 解析、跨日界窗口、days 过滤、窗口开闭 Audit 边沿、Resolve/Fatal 穿透；集成 —— 窗口内 critical 不出 webhook、窗口外正常出。**第十一轮验证（2026-11-18，双专家）**：历法公式 1970→2100 全量对拍零误差、contains 算法 20 万随机配置对拍零失配;修复 P1 —— reload diff_sections 漏 maintenance 段（窗口变更 reload 零审计）已补列 + 单测;P3 —— 墙钟跳变下 Audit 边沿可成对丢失/重复（判定始终自洽，DESIGN 如实限定）。**已跑（2026-11-18，通过）**：maintenance.rs 单测 9 用例（weekday/minute 锚点 epoch 0 周四与 2026-11-18T12:34:56Z=周三、HH:MM 合法/拒绝、days 大小写与缺省全周、同日边界含/不含、跨 midnight、days 只过滤开启日、多日 duration、target 前缀/`*`/空 pattern、label 与 from_cfgs 防御）+ eval/mod.rs 3 用例（窗口内 Alert/Escalate/Flap 抑制且不逐条审计、Resolve/RECOVER_CHECK 穿透、target 前缀门控、opened/closed 审计边沿恰好一条且不重复）；集成 tests/maintenance_window.rs 2 用例（真实调度循环+webhook stub：窗口内 critical 不出 webhook+Resolve 穿透+opened 审计落盘+Incident 历史完整性；窗口外（days 注入非今天星期）alert/escalate 正常出且无维护审计）。

### F32 控制面 HTTPS ☑

- [x] `[control] tls_cert` / `tls_key`（PEM 路径，成对必填）→ 控制面以 HTTPS 监听同一地址（axum-server tls-rustls，随 control-http feature）；未配置 = 纯 HTTP 行为不变（`axum::serve` 现状路径零改动）
- [x] 证书/私钥加载失败 → 启动早退（bind 阶段，退出码 2）；私钥不落日志（错误仅含路径与解析原因）
- [x] monitorctl `--url https://...` 直接可用（reqwest rustls 同线）；新增 `--insecure`（跳过证书校验，仅自签/测试场景，文档警示）
- [x] 证书轮换 = 重启 agent（与 control 段语义一致）；DESIGN §9.2.2/RUNBOOK §1.2d/DEPLOYMENT/DEPLOY-*/ROADMAP 记录密钥准备与信任链提示

**验收**：单测/集成 —— 自签证书起服务，monitorctl HTTPS 请求可达、HTTP 明文不可达；`--insecure` 对自签可达；加载坏密钥启动失败退出码 2。**第十一轮验证（2026-11-18）**：tls_control 4 用例 + openssl 实机 e2e（--insecure status 退出 0/默认客户端拒信/明文不可达/坏 PEM exit 2 且 data_dir 未创建）;axum-server 0.9.2 provider 声明经 vendored 源码核实属实;修复 P3 —— TLS 加载改与 bind 同受 control.enabled 门控（控制面关闭时坏材料不阻止启动）;RUNBOOK 补证书链 leaf-first 说明。**已跑（2026-11-18，通过）**：tests/tls_control.rs 4 用例（rcgen 自签起服务 → 跳校验客户端 https /health+/status 200、默认客户端证书错误、明文 http 连接错误、坏 PEM load Err 且不回显内容、单边 tls_cert/tls_key → Config 错误、`run()` 早退且不创建 data_dir）；monitorctl 单测 `--insecure` 解析 + `build_client` 透传；实机 e2e：openssl 自签起 agent，`monitorctl --url https://127.0.0.1:9171 --insecure status` 退出 0、默认客户端退出 4（信任链拒信）、明文 http 退出 4、坏 PEM 启动退出码 2 且不创建 data_dir。

### F33 生效配置查看 ☑

- [x] `GET /v1/config`：`Config::redacted()` 视图（token/签名密钥以 `***redacted***` 占位，其余原样；TLS 路径原样）；受 F23 鉴权（protected 路由；watch 通道 `config_view`，runtime 构造初始值、控制平面只读透传）
- [x] monitorctl `config [--json]`：表格输出顶层段与维护窗口/路由条目，`--json` 原样（`***redacted***` 原样展示 + 尾部注脚注明含义与不可回灌）
- [x] 语义：反映**当前进程持有的配置**，**reload 成功后即新值**（eval reload 成功经 `config_view_tx` watch 推送新 redacted JSON —— 主会话修复第十轮偏差：快照不再滞留启动值；eval 单测 `reload_refreshes_config_view_watch` 锁定；`***redacted***` 占位不可回灌，DESIGN §9.2/RUNBOOK §4/DEPLOYMENT §7 注明）

**验收**：单测 —— redaction 覆盖三类敏感字段、非敏感原样；集成 —— 配 token + secret 的 agent 经端点取回占位值且无明文；monitorctl 端到端。**第十一轮验证（2026-11-18）修复 P2**：初版视图推整份新文件（reload 后把未生效的 control/alert/collect/maintenance 值展示成已生效）——改为**合并视图**（拓扑段随 reload 刷新，要求重启的段恒为启动值，Scheduler 持 startup_config_view）;eval 测试 `reload_refreshes_config_view_watch` 强化（占位符 + 明文不出现断言）。**已跑（2026-11-18，通过）**：config.rs 单测（主会话既有：redacted 覆盖 control.token/webhook_secret/路由 secret 三类 + 非敏感原样）；control 端点测试 `config_endpoint_returns_redacted_snapshot_without_secrets`（真实 Config+ 真实 axum server 经 reqwest：三类占位符透出、响应体不含任何明文、非敏感 url 原样；401 枚举测试补 /v1/config）；monitorctl 单测 4 用例（`config [--json]` 参数解析、render_config_table 对象段/列表逐条/空段/占位注脚/控制字符剥离、TCP stub 端到端 fetch→decode→render + 401 分类）。

### v0.6 遗留人工项（非代码，持续跟踪）

- [ ] **Windows 实机验收**（沿用）；**硬件实机覆盖**（沿用）；~~F30 状态页人工目验~~（✅ 2026-11-18 浏览器实测完成，见 F30 验收行）

---

---

## v0.7.x — 收尾攻坚 ◐ (2026-11-18 规划)

目标：清偿显式留白的 backlog，并把两个人工验收项中**可自动化的部分**搬进流水线。**全部默认向后兼容**。
门禁：与 v0.6 相同 —— fmt / clippy -D warnings / 全测 / 两条 no-default 组合；文档随实现同步。

### F34 指标推送 HMAC 签名 ☑ (2026-11-18;manifest 已落地，三 feature 组合全绿)

- [x] `[metrics] secret`(可选):配置后每次推送附 `X-Service-Watch-Signature: sha256=<hex hmac-sha256(body)>`(语义/头名/覆盖口径与 F29 webhook 签名一致;签名对象 = 实际发送 body 字节 —— 先 `serde_json::to_vec` 再签名发送，等价于 reqwest `.json()` 内部序列化，行为一致)
- [x] 签名 helper 下沉到 http-client 门控共享层 (**manifest 已落地并复核**:`alert::signature.rs` 门控 http-client,webhook.rs 委托共享实现、行为零变化，notify-webhook 线全绿;**http-client 单开线待 Cargo.toml**:hmac/sha2 目前仅随 notify-webhook 拉入，`--features ...,http-client`(不含 notify-webhook) 下 helper 无法链接 —— 需 `http-client = ["dep:reqwest", "dep:hmac", "dep:sha2"]` + `notify-webhook = ["http-client"]`;该组合已在 /tmp 草稿副本预验证;**manifest 已由主会话落地并复核**:`collect-sysinfo,http-client` 编译通过 + signature 单测 (RFC 4231 向量/None 不加头) 通过，全量 15 套件绿)
- [x] 密钥不落日志;None = 不签名 (行为不变 —— runtime 装配日志只报 `signed = <bool>`)

**验收**(2026-11-18 已跑;**第十二轮验证修复 [P1]**:redacted() 初版漏 metrics.secret,/v1/config 泄明文 —— 已补脱敏 + control/config 两处测试锁定):单测 —— RFC 4231 Test Case 2 向量随 helper (`alert::signature` 单测，http-client 门控)、None 不加头 (`signature_header_absent_without_secret`);集成 —— `tests/metrics_push.rs` 验签 stub 端实算验签 (有 secret 每次推送签名 = 服务端按收到的原始 body 重算 HMAC;None 推送照常且无签名头);附带清偿第十轮接受项 —— `tests/alert_routing.rs::spool_replay_delivers_with_valid_signature_and_confirms_removal` (spool 注入 + 有 secret notifier → 重放 → 验签通过 + spool 确认移除)。已跑 cargo fmt / clippy --workspace --all-targets -D warnings (零警告) / cargo test --workspace 全量 / combo `collect-sysinfo` 与 `collect-sysinfo,control-http`;`collect-sysinfo,http-client` 组合**已过 (manifest 落地后主会话复核，2026-11-18)**。

### F35 journal 查询覆盖轮转上一代 ☑ (2026-11-18;第十二轮验证修复 [P2]:gen1 读失败并入 truncated=true，防"看似完整却缺整代";[P3] gen1_path 双处实现去重为单一事实源)

- [x] `GET /v1/journal` 当 `journal.1.jsonl` 存在时一并尾读 (gen1 尾部在前 + 当前文件尾部在后 —— journal 追加写 = 时间顺序，合并后统一坏行隔离/过滤/tail 截断;每文件独立 512KB 有界读，合并至多 1MB);`truncated` = 任一文件字节截断 或 合并命中总数 > tail;单文件读失败只放弃该文件 (WARN),不毒化另一代;上一代 NotFound 行为与单文件时代一致;文档 (DESIGN §9.2 / RUNBOOK §1 表 + §2.2 / DEPLOYMENT §7) 从「仅当前文件」改为「含上一代 (仍不含更早代 —— F24 只保留一代)」
- [x] monitorctl 无需变更 (端点响应 schema `{"truncated","lines"}` 与行 schema 不变，gen1 行只是同 schema 的追加行;确认方式:monitorctl `JournalReply` 解析与 `fetch_journal_reply` 路径零改动 —— 端点对客户端透明)

**验收**(2026-11-18 已跑，通过)：单测 —— `journal_tail_lines_merges_gen1_before_current`(合并顺序 gen1→当前、双文件坏行隔离、target/since 跨两代过滤、tail 按合并命中数截断)、`journal_tail_lines_truncated_by_bytes_from_either_generation`(gen1 超 512KB → truncated 置位且当前文件数据完整)、`gen1_query_path_matches_rotation_naming`(查询侧上一代路径推导与 F24 轮转命名同规则);既有 `journal_tail_lines_filters_and_truncates` 锁定无 gen1 时行为与现状一致;端点 × F24 轮转联动 `journal_endpoint_reads_rotated_previous_generation`(产品 tracked writer 阈值触发真实轮转 → `GET /v1/journal` 查到上一代尾部哨兵事件，且 gen1 事件先于轮转 audit 与当前文件事件)。

### F36 Windows runner SCM 实机演练 ☑ (第十二轮验证修复 P1×2/P2×3;第十三轮扩展断言链:Job Object 孤儿存活/POST /v1/reload 热重载/强杀→SCM failure action 自愈，timeout 30min —— CI 首跑绿待 push 后验证) (2026-11-18;作业与文档已落地并通过本地校验 —— YAML 解析 / pwsh 解析器逐 step 语法 / 生成 config 过真 agent `--check-config` / 脚本参数 AST 核对;**「CI 首跑绿」待 push 后首次运行验证**)

- [x] CI 新增 windows-latest 作业 (`.github/workflows/windows-scm.yml`,独立文件、独立 job、timeout 20min，不改变 ci.yml 门禁语义):构建 monitor-agent(windows-service feature，默认 feature 带控制平面)→ 用 docs/deploy/windows/install-service.ps1 **真实安装服务**(`-InstallDir/-ConfigPath/-DataDir/-ServiceName` 参数化，人工用法零改动)→ 服务 RUNNING → `/health` 200 → `sc stop` → 轮询 STOPPED ≤60s 且进程优雅退出 (/health 拒连 + agent.log `reason=Clean`)→ journal/文件日志落盘断言(journal 按 spawn 事务行断言 —— 停机不写专门 journal 行，与实现语义如实对齐)→ 安装幂等断言 (已存在 fail-fast 守卫 + "先 delete 再 install"文档化重装路径含端口重绑)→ 卸载清理 (失败路径 dump 状态/日志/journal 尾部)
- [x] 演练即验收证据：把「Windows 实机验收」中的安装/启动/SCM stop/优雅退出自动化(交互式 Job Object console 演练仍属人工)

**验收**：CI 作业全绿 (**待 push 后 CI 首跑验证，未勾**);断言已覆盖安装幂等、SCM 状态迁移、优雅停机、日志与 journal 产出。

### v0.7 遗留人工项（非代码，持续跟踪）

- [ ] **Windows 实机验收**：交互式 Job Object/console 演练与真实目标服务场景仍需人工 (F36 已自动化安装/start/stop/优雅退出部分);**Linux hwmon / Intel 机型硬件证据**（Apple Silicon 证据已于 2026-11-18 本机采集，见 F21 验收行）

---

---

## v0.8.x — 可操作性增强 ☑ (2026-11-18 规划并交付;第十四轮双专家验证收口)

目标：补齐真实集成与排障场景的最后缺口。**全部默认向后兼容**。
门禁：与 v0.7 相同 —— fmt / clippy -D warnings / 全测 / 三条 no-default 组合；文档随实现同步。

### F37 webhook 自定义投递头 ☑ (2026-11-18;`alert.headers`/`routes[].headers` 字段与 RFC 7230/CR-LF/成对校验由主会话先期交付于 config.rs)

- [x] `[alert] headers`(全局)+ 路由级 `headers`：任意自定义头 (接收方 auth token、Grafana/Signoz 类集成);**合并语义 = 路由级同名键覆盖全局**,签名头 (F29) 独立叠加 —— `runtime::effective_route_headers` 纯函数 (全局为底,路由级覆盖,非同名共存);默认通道用全局表;notify 时全部叠加 (构造期一次性转 `HeaderName`/`HeaderValue`,HTTP 非法项 WARN 丢弃只含头名,兜底库直构;签名头总是附加不因同名自定义头丢弃,保留头 `X-Service-Watch-Signature` doc 级警示)
- [x] 注入面校验：header 名必须为 RFC 7230 token、值不得为空/含 CR/LF/控制字符(配置层拒绝 —— 主会话 config.rs + 坏配置测试)
- [x] slack/json 两格式与 spool 重放同路径生效 (同走 `Notifier::notify`,天然覆盖)

**验收** (2026-11-18 已跑,通过)：单测 —— 合并覆盖语义 (`route_headers_override_global_and_nonconflicting_coexist`)、构造期转换/非法项丢弃 (`custom_headers_collected_and_invalid_dropped`);集成 (tests/alert_routing.rs) —— `custom_headers_reach_both_channels_and_coexist_with_signature` (双 stub:路由级覆盖全局 + 非同名共存 + 签名头共存,json/slack 两格式各走一通道)、`spool_replay_delivers_with_custom_headers_and_signature` (重放带自定义头 + 验签通过)。

### F38 控制面 mTLS ☑ (2026-11-18;**实现侧偏差**:服务端 verifier 需 rustls crate 本体类型 (WebPkiClientVerifier/RootCertStore/ServerConfig,axum-server/reqwest/rustls-pemfile 均无公开 re-export),workspace + monitor-core(control-http feature) 新增 rustls 直接依赖 —— `default-features = false, features = ["std","tls12"]`,provider 经 builder_with_provider 显式传入不 install_default,aws-lc-rs 由 axum-server/reqwest feature 合并提供,不新增编译单元)

### F38 控制面 mTLS ☑ (2026-11-18;**实现侧偏差**:服务端 verifier 需 rustls crate 本体类型 (WebPkiClientVerifier/RootCertStore/ServerConfig,axum-server/reqwest/rustls-pemfile 均无公开 re-export),workspace + monitor-core(control-http feature) 新增 rustls 直接依赖 —— `default-features = false, features = ["std","tls12"]`,provider 经 builder_with_provider 显式传入不 install_default,aws-lc-rs 由 axum-server/reqwest feature 合并提供,不新增编译单元)

- [x] `[control] client_ca_cert`(可选，PEM;字段与单配校验由主会话交付于 config.rs):配置后 TLS 握手要求客户端证书 (由该 CA 签发),无证书/不可信证书握手失败 (连接错误,无 HTTP 层交互);必须与 tls_cert/tls_key 同配 (config 校验拒绝单配,`load_tls_config` 程序化兜底 `Err(Config)`)
- [x] rustls WebPkiClientVerifier (严格 webpki 校验,不 allow_untrusted/unauthenticated;`builder_with_provider` 显式传 provider) + axum-server `RustlsConfig::from_config`(该构造不代设 ALPN,手工补齐 h2/http/1.1);CA PEM 严格解析 (任一段失败即拒,空文件/无证书段 → 装配期早退,与 F32 同列,退出码 2);monitorctl `--client-cert/--client-key`(成对,单边启动错误) 经 `reqwest::Identity::from_pem` (0.13 rustls 后端唯一 PEM 入口;`from_pkcs8_pem` 仅 native-tls 提供) 全命令共用 (OnceLock 注入 build_client,cmd_* 签名零改动)
- [x] 文档：DESIGN §9.2.2 (mTLS 语义 + 信任链三件套 TLS+Bearer+mTLS 可叠加 —— 鉴权中间件仍在 TLS 层之后,401 语义不变,mTLS 失败到不了 HTTP 层);RUNBOOK §1.2e (client_ca_cert 配置 + openssl CA/客户端证书生成 + monitorctl 用法 + `--insecure` 与 mTLS 的正交语义);DEPLOYMENT control 段 + 速查表 mTLS 行

**验收** (2026-11-18 已跑,通过)：集成 (tests/tls_control.rs,7 测试) —— rcgen 自签 CA 同签服务端+客户端证书,mTLS 握手成功 (/health 与 /status 200)、无客户端证书握手失败、坏 CA 拒绝、坏 CA PEM/空 CA 装配期 Err (错误含路径不含内容)、`reqwest::Identity::from_pem` 对 rcgen 产物可用 (monitorctl 构建路径单元级);monitorctl 单测 26 过 (flag 解析/成对校验/坏文件错误路径);**二进制端到端** —— 真实 monitor-agent (tls+mTLS+token) + monitorctl:带证书+token status/metrics/journal 200 (exit 0)、无证书/坏 CA 证书 exit 4 (握手失败)、单边 flag exit 2 启动错误、agent 日志确认 `control plane mTLS enabled`。

### F39 指标历史持久化 ☑ (2026-11-18;`metrics.history_path` 字段与空路径校验由主会话先期交付于 config.rs;**实现侧偏差**:轮转不写 journal audit —— tracing::info 代替,journal 是恢复域、历史文件是观测出口,两域隔离;停机 = abort+await 收口 (与 metrics pusher 第十二轮同款),写入段无 await 不会截断半行,最后在途样本可能不落盘 —— 详见已知偏差表)

- [x] `[metrics] history_path`(可选):eval 每轮发布的样本以 JSONL 追加落盘 (一行 = /metrics/history 单个样本对象 `{"at","report"}`,MetricsSample serde 形态);复用 journal 单代轮转模式 (10 MiB → `.1` 保留一代,写后实测尺寸,gen1 路径复用 `recover::journal::gen1_path` 单一事实源;轮转失败 WARN 降级继续)
- [x] 独立 writer 任务 (`metrics_history` 顶层模块) 消费 metrics watch (eval 零改动、不阻塞单写者;watch 滞后时中间样本被最新值覆盖,首样本照写);重启追加不截断 (append+create);停机 abort+await 位于 metrics pusher 收口之后、journal drop 之前
- [x] 重放/查询不做 (纯数据出口，分析走 jq/外部工具)—— 文档明示 (DESIGN §10、RUNBOOK §1.2f、DEPLOYMENT metrics 段)

**验收** (2026-11-18 已跑,通过)：单测 (src/metrics_history.rs,4 测试) —— 轮转 (小阈值,`.1` 只保留一代且内容为最新样本)/续写/禁用轮转 (max_size=0)/重启追加 (两次会话写同一文件);**「坏行隔离」不适用** —— 单写者整行追加、无读回放路径 (打开时亦不做 journal 式半行定界,模块 doc 注明崩溃中途残行概率极低,分析侧容忍);集成 (tests/metrics_history.rs,2 测试,经 metrics watch 通道直接喂样本) —— 产品入口落盘逐行行格式与 `/metrics/history` 样本对象一致 (report 逐字段一致、at 单调)、小阈值轮转 `.1` 保留最新一代;`history_path` 消费无 feature 门控 (三 no-default 组合 check 通过)。

---

## 已知偏差记录 (实现与原设计的差异)

| 原设计 | 修订 | 依据 |
|---|---|---|
| fingerprint = hash(target+severity+reason) | hash(target, check),incident 模型 | 同一故障重复告警/被冷却吞掉 (SRE P0) |
| 进程不存在 → 直接重启 | 确定性死亡 (退出事件/连续 2 次 liveness 失败)→ 直接 Recovering;健康失败 → Suspect 二次确认 | 三文档矛盾，统一契约 |
| Windows graceful = CTRL_BREAK | TerminateProcess 为基线 + Job Object;CTRL_BREAK 仅显式启用 | CTRL_BREAK 对无 console 服务进程无效 (跨平台 P0) |
| `restart_budget: u32`(5 次/10 分钟) | `{max_restarts, window}` 滑动窗口，全量计数 | 单字段表达不了窗口;只计快退会漏 (P1) |
| reqwest 0.12 | 0.13,default-features=false + rustls | 0.12 永远解析不到 0.13;native-tls 跨平台不一致 |
| 首轮 CPU 显示 0% | 首采降级写 degraded_reasons | "不伪造数据"原则 |
| monitorctl 标注 optional | 必建，作为控制平面客户端 | 无控制平面则声明能力无法落地 |
| 收集器只上报"身份匹配"的 watched 进程 | 上报**观测身份**,PID 复用裁决移到消费端 (±2s 容差 + 采纳) | start_time=0 占位/本地钟跨秒失配会使严格过滤结构性失效 (评审 P0/P2) |
| eval 经 mpsc 桥接 AlertQueue | eval 直推 AlertQueue(drop-oldest) | 双层缓冲产生 drop-new 路径且不计数 (评审 P2) |
| renotify/flap 用墙钟判定 | 全部改单调时钟 (事件时间戳保留墙钟) | 墙钟回拨永久抑制重通知 (评审 P1) |
| budget 只计 fast-exit/spawn 失败 | 所有自动重启均计数 (含慢死亡) | "每 11 分钟死一次"永不触发 Fatal(评审 P1) |
| reqwest rustls 无工具链说明 | README/CI 记录 Windows 需 NASM+CMake(aws-lc-rs) | reqwest 0.13 无 ring 备选 (评审 P1) |
| F20 级联停止的 dependent 退出 = 确定性死亡 (计预算) | 计划内停止经 `on_planned_stop` 收口：不计预算、不排程，由依赖门控拉回;SCM dependent 经 liveness gone 同样门控 | 依赖反复 restart 会把 dependent 推向误 Fatal(第六轮评审 P3) |
| SCM 目标非 Running 一律视为死亡证据 | 三值投影：`Transitioning`(START_PENDING 等) 与查询失败不喂 FSM，仅 `Stopped` 是死亡证据;无响应启动由 SCM 自身超时 (~30s,1053) 兜底 | Start 发出后必然先经 START_PENDING，慢启动服务会被发 Stop 掐死直至 Fatal(第六轮评审 P1) |
| F25 跨重启恢复连 flap 计数器一并恢复 | flap transitions/抑制集不恢复(新会话 fresh 窗口)，仅恢复 open incident trail 与重通知 | 旧抖动史跨重启会误抑制新告警 (重启即失忆的抑制比"永被旧史压制"更安全;F25,2026-11-18) |
| F25 拓扑外遗留 incident (检查已从配置删除) 重放后不再恢复 | 仍恢复并重通知一次 (有测试锁定)；`/status` 不展示 (无对应 check 标签),trail 永不 Resolve | 已删除检查的重启时"幽灵告警"是一次性且可观察的，保守漏报比静默丢弃安全;若需静默，先 resolve 再删配置 (F25 验证记录，2026-11-18) |
| F24 journal 轮转与关键事件 2s 确认预算共享单写线程 | 慢盘环境轮转 (rename+ 重建+audit sync) 与下一关键事件确认共用预算，极端慢盘可能触发 PersistenceFailed 退出 2 | 单写者阻塞模型固有属性;本地 fs 轮转毫秒级，实际风险极低 (rotate_file doc 与 RUNBOOK 注明;第九轮验证 P3 存疑项) |
| F33 `/v1/config` 反映"当前进程持有的配置"(reload 后即新值) | 初版快照于启动时构造一次、reload 后不刷新;后改为 watch 通道 (`config_view_tx/rx`):eval reload 成功即推送新 redacted JSON，控制面透传;要求重启的段 (control/alert/collect/…) 恒为启动值 | 实现线偏差被第十轮验证指认后由主会话以 watch 通道修复 (2026-11-18);脱敏快照只读透传，明文密钥仍不出 agent |
| F39 原文「轮转写 audit 语义对齐 F24」「停机 flush 窗口与 journal 同口径」 | 轮转只写 `tracing::info` **不写 journal audit**(journal 是恢复域,历史文件是观测出口,两域隔离);停机 = abort+await 收口 (与 metrics pusher 第十二轮同款:位于 pusher 收口后、journal drop 前),写入段无 await 点不截断半行,最后在途样本可能不落盘 | 观测数据出口不需要恢复域的确认式持久化预算;abort 收口语义与在途 pusher 一致,历史文件为 append-only 观测物,尾部一条样本的丢失可接受 (F39,2026-11-18;DESIGN §10 与 RUNBOOK §1.2f 如实记录) |


## 2026-11-12 评审驱动交付

| 项目 | 功能规范与验收 | 状态 |
|---|---|---|
| R1 恢复证据与身份 | 采集不可用或指标缺失时保留健康水位，亲生 child 同 PID 采纳身份并同步 FSM，真实退出仍触发恢复 | 已实现，回归验证见评审记录 |
| R2 热重载一致性 | 删除和重定义先确认批量 Reset 落盘，旧 Child/Job 实际收割后启动新定义，软策略更新同步到 FSM 与 PM | 已实现，回归验证见评审记录 |
| R3 journal 故障处理 | 关键事件有界等待 sync_data 确认，失败停止当前恢复实例并退出 2，普通事件丢弃和写错误可观察，半尾行隔离 | 已实现，回归验证见评审记录 |
| R4 budget 重放一致性 | 每次自动恢复恰好产生一条预算事件，activate 失败遵守预算上限，reset 与窗口过滤语义跨重启保持 | 已实现，回归验证见评审记录 |
| R5 启动与停机所有权 | bind 和 journal 准备失败发生在任务启动前，分发与投递子任务随父任务取消，降级采集周期重试 | 已实现，回归验证见评审记录 |
| R6 告警 spool | 成功投递后才移除磁盘记录，失败或取消保留原件，队列并发 push 容量有界，坏 UTF8 与半尾行不吞后继事件 | 已实现，回归验证见评审记录 |
| R7 配置规范 | 缺省段和空段默认值一致，拒绝越出单调时钟范围的时长、非有限 CPU 阈值和无效钩子，明确 PID 与名称解析 | 已实现，回归验证见评审记录 |
| R8 平台交付 | Windows 服务 feature 进入 CI，结构化服务注册与非崩溃错误恢复动作，性能脚本只清理本次目标 | 已实现，Windows 实机验收待完成 |

### 后续验收与扩展

- [x] **F17 完成门**。固定版本与配置 24h 长跑完成 (1412 采样，RUN3 于 ~/.local/state/sw-soak-24h/20261116-233106/):CPU 均值 0.46%、RSS 稳态增长 176KB(无泄漏指征)、零 ready 失败、exit 130。前两次运行记录:RUN1 /var/tmp 被清理器抹掉 (931 采样，CPU 0.63%)、RUN2 /var/tmp 同因——教训驱动脚本默认目录改持久路径。短时脚本仍保留为快速回归门。—— 2026-11-17
- [ ] **Windows 实机验收**。执行安装、启动、SCM stop、错误退出恢复、Job Object 与 console 事件演练，记录服务状态及 journal。交叉编译只提供类型与 feature 编译证据。
- [x] **原生 Windows Service 目标恢复**。受 SCM 管理的目标类型 (`windows_service` 配置字段，与 command 二选一):启动幂等 (预查询)/ 停止 QueryStatus 确认 / force_kill 如实降级为 Stop / 无进程身份 (on_scm_spawn_ok → Starting,QueryStatus 即 liveness 证据)。三值状态投影 (Running/Stopped/Transitioning)—— 过渡态与查询失败不作死亡证据 (第六轮评审 P1 修复：此前 is_running 抹平三值，慢启动服务被 Stop 掐死直至 Fatal)。`scm-target` feature 门控默认关 (README §8);CI 覆盖交叉编译 + windows runner 测试 (第六轮评审补线)。实机验收并入上方「Windows 实机验收」行。—— 2026-11-13,7 集成测试
- [x] **崩溃启动事务**。`SpawnIntent`(spawn 前落盘确认，config_digest 绑定定义版本)+ `SpawnOutcome`;重启对账:intent 无 outcome 且 digest 匹配 → 跳过冷启动 + ERROR + Suspect(reset 解除);digest 不匹配照常冷启动。验收断言 (`spawn_intent_is_awaited_and_confirmed_before_process_spawn` 等)+ `JournalState.pending_spawns` 重放 —— 实现在树且测试通过 (第六轮重构时漏勾，2026-11-15 如实修正)
- [ ] **硬件实机覆盖**。补充 Linux 实际 hwmon 设备与更多 Intel、Apple Silicon 机型证据，保持读数类型、单位和降级一致。
- [x] **指标周期推送**。`[metrics]` 协议落地：每 interval 把最新 MetricsReport 以 JSON POST 到 url(单次尝试无重试 —— 指标可再生，失败下轮覆盖;重定向视为失败);失败 WARN + 连败边沿写 journal 审计 (首轮/恢复首轮/≥3 后每 10 连败采样);panic 任务边界隔离。—— 2026-11-13,2 集成测试 (真实 axum stub 闭环 + 连败审计边沿)
- [x] **存储故障演练**。`tests/storage_fault.rs` 5 演练全绿：磁盘满/不可写 (打开失败→启动早退→修复后重启)、慢盘 (sync_data 悬挂→关键事件 2s 预算耗尽返回 Err→恢复中止，精确计时)、断电半行 (撕裂行隔离 + 定界续写)、坏行隔离、fail 闩锁契约 (eval 中止);不可中断进程 (D 状态) 由 FSM 纯注入测试覆盖 (真实 D 状态 macOS 不可构造，如实记录)—— 2026-11-13
