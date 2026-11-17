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
- [x] **F21 深度硬件采集**。可选 `collect-deep-hardware` feature，Linux hwmon 只读风扇与电压，macOS AppleSMC 固定常见键，Windows 显式 unsupported。入口覆盖 runtime 与两个 CLI 的 dump-snapshot。读数使用 Option，权限、无效值、禁用和故障均机读降级。验收包括 fixture、SMC 解码、feature 组合和三平台编译，实机传感器覆盖范围另记评审报告。
- [x] **F22 Web 状态页**:`GET /` 内嵌只读页 (零外部依赖，JS 轮询 /status+/metrics,health 徽章/Fatal 高亮/escapeHtml 防 XSS/不可达保灰)—— 2026-11-12,E2E 实测 (200 text/html 10KB)

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
