# service-watch 开发者指南(中文)

> 面向参与 service-watch 开发与维护的工程师。语义权威为 [DESIGN.md](../../DESIGN.md),硬性约束为 [AGENT.md](../../AGENT.md),范围与进度以 [ROADMAP.md](../../ROADMAP.md) 为准;用户视角见 [user-guide.zh.md](user-guide.zh.md)。英文版:[developer-guide.en.md](developer-guide.en.md)。
>
> 内容按"读代码需要的顺序"组织:设计原则 → 工作区 → 架构与并发 → 模块地图 → 各子系统实现 → 配置/存储/平台层 → feature 与构建 → 测试 → 开发工作流 → 扩展点。

---

## 目录

1. [项目概览与设计原则](#1-项目概览与设计原则)
2. [工作区结构与依赖](#2-工作区结构与依赖)
3. [架构与并发模型](#3-架构与并发模型)
4. [monitor-core 模块地图](#4-monitor-core-模块地图)
5. [数据模型](#5-数据模型)
6. [采集层(collect)](#6-采集层collect)
7. [检查与告警引擎(eval + alert)](#7-检查与告警引擎eval--alert)
8. [恢复引擎(recover)](#8-恢复引擎recover)
9. [控制平面(control)](#9-控制平面control)
10. [配置系统(config)](#10-配置系统config)
11. [存储与持久化](#11-存储与持久化)
12. [平台差异层](#12-平台差异层)
13. [Feature Flags 与构建矩阵](#13-feature-flags-与构建矩阵)
14. [测试策略](#14-测试策略)
15. [开发工作流与工程纪律](#15-开发工作流与工程纪律)
16. [扩展点:如何新增 X](#16-扩展点如何新增-x)
17. [文档地图](#17-文档地图)

---

## 1. 项目概览与设计原则

service-watch 是跨平台(Rust 2024)服务与硬件监控工具:一个 `monitor-core` 库 + `monitor-agent` / `monitorctl` 两个薄入口二进制,覆盖采集、检查告警、进程恢复、控制平面四条链路。

五条设计原则(DESIGN §1,代码评审的最高准绳):

1. **观察者外置**:agent 独立于被监控服务的生命周期。Windows Job Object 默认**不带** `KILL_ON_JOB_CLOSE`;Unix spawn 用 `process_group(0)` + `kill_on_drop(false)`。agent 死亡不连带杀目标。
2. **恢复策略显式化**:`min_uptime` / `backoff` / `restart_budget{max,window}` / `stop_grace` / `startup_grace` 全部显式可配;Fatal 是终态闩锁,仅人工 `reset` 解除。
3. **降级可观测,不伪造数据**:不可用字段一律 `Option` + 机读 `Degradation` 枚举;CPU 首采、温度垃圾值均走降级路径,禁止填 0。
4. **零锁单写者**:全部状态机由单一 eval 任务独占;跨任务只传不可变消息;共享可变状态一律不允许 `Arc<Mutex>`(DESIGN §2.2)。
5. **可确定性测试**:所有时长判定走注入单调时钟(`Instant` 作参数);sysinfo/网络/进程全部在 trait 边界后可 mock。

规约裁决顺序(有冲突时):`AGENT.md`(硬约束)> `DESIGN.md`(实现细节)> `ROADMAP.md`(范围与进度)> `EXECUTION.md`(执行计划)。

---

## 2. 工作区结构与依赖

### 2.1 工作区

```text
service-watch/
├── Cargo.toml              # [workspace] resolver = "3",无 default-members;version = 0.3.0
├── crates/
│   ├── monitor-core/       # lib:全部业务逻辑
│   ├── monitor-agent/      # bin:薄入口(CLI/日志/信号/SCM)
│   └── monitorctl/         # bin:控制平面客户端 + 本地 dump-snapshot
├── config/config.example.toml   # 与 schema 一一对应,单测保证可解析
├── docs/                   # 部署/运维/评审文档 + v1 用户与开发者指南
└── .github/workflows/      # ci.yml / release.yml / windows-scm.yml
```

`[profile.release]`:`lto = "thin"`、`strip = "symbols"`。

### 2.2 关键依赖(workspace.dependencies,核对日期见 Cargo.toml 注释)

| 依赖 | 版本 | 用途 |
|---|---|---|
| tokio | 1.54.1 | 只启用所需 feature(rt-multi-thread/time/sync/macros/signal/process/net/io-util/fs),禁用 `full` |
| serde / serde_json / toml | 1.0.229 / 1.0.154 / 1.2.2 | 配置与序列化 |
| thiserror | 2.0.21 | `CoreError` |
| tracing / tracing-subscriber | 0.1.44 / 0.3.23 | env-filter + json |
| sysinfo | 0.40.4 | 硬件/进程采集(声明 MSRV 1.95,被解析器强制) |
| reqwest | 0.13.5 | `default-features = false`,features `json` + `rustls`(禁 default-tls) |
| axum / axum-server / rustls / rustls-pemfile | 0.8.9 / 0.9.2 / 0.23.46 / 2.2.0 | 控制平面与 TLS |
| clap | 4.6.9 | derive CLI |
| hmac / sha2 | 0.13.0 / 0.11.1 | HMAC-SHA256 签名(纯 Rust,不破坏交叉编译线) |
| libc / windows-sys / windows-service | 0.2.191 / 0.61.2 / 0.8.1 | 平台 FFI 与 SCM |
| rcgen | 0.14.10 | dev-dep,测试自签证书 |

**硬性约束**(AGENT.md,违反即 CI 失败):所有 crate `edition = "2024"`、`license = "Apache-2.0"`;**任何 Cargo.toml 禁止 `rust-version` 字段**(自检:`cargo metadata --format-version 1 | jq -e '[.packages[] | select(.source == null) | select(.rust_version != null)] | length == 0'`);文档引用依赖版本必须带核对日期。

### 2.3 二进制入口

- **monitor-agent**(`crates/monitor-agent/src/main.rs`):clap 顶层 flags(无 run 子命令)——`--config`(缺省 `./service-watch.toml`;文件不存在时用内置最小默认配置)、`--log-json`、`--dump-snapshot`、`--check-config`、隐藏的 `--service-run` / `--service-name` / `--log-file`(Windows SCM 服务形态,`service.rs` 承载)。`StopReason` → 退出码映射:`Clean`→0、`Signal`→130、`PersistenceFailed`/`EvalExited`/Err→2。
- **monitorctl**(`crates/monitorctl/src/main.rs`):全局 `--url`(默认 `http://127.0.0.1:9170`)/`--token`(flag 优先于 `SERVICE_WATCH_TOKEN`)/`--insecure`/`--client-cert` + `--client-key`(成对);11 个子命令 `status / restart / check / reset / metrics / dump-snapshot / watch / silence / unsilence / journal / config`。HTTP 超时 5s;target 路径段 percent-encode;表格渲染做控制字符剥离与超宽截断;默认日志静默(`RUST_LOG` 才初始化)。

---

## 3. 架构与并发模型

### 3.1 组件与数据流

```text
┌──────────────────────────── monitor-agent (bin) ────────────────────────────┐
│  main: clap → tracing init → monitor_core::runtime::run(config)             │
└──────────────────────────────────┬──────────────────────────────────────────┘
┌──────────────────────────────────▼──────────────────────────────────────────┐
│  [collect 阻塞线程] --watch--> [eval 单写者任务] --mpsc--> [alert 分发任务]   │
│   sysinfo System 独占        状态机×N(零锁)      有界队列 + 重试            │
│                                     │                                       │
│                              [recover 在 eval 内]                            │
│                     RecoveryFsm × supervise targets                         │
│                     spawn/stop 走 ProcessManager(平台实现)                  │
│  [control HTTP 任务] <--watch/快照--┘      [journal 写任务] <--有界 mpsc--   │
└─────────────────────────────────────────────────────────────────────────────┘
```

数据流单向:`collect → eval → alert / recover`;recover 不反向查询采集器——进程存活由「子进程退出事件(亲生)+ liveness 探测(收养)」双通道判定。

### 3.2 并发与所有权(强制,DESIGN §2.2)

| 组件 | 形态 |
|---|---|
| 采集 | 专用 OS 线程独占 `sysinfo::System`(refresh 是阻塞调用,禁在 async 上下文裸调);快照经 `tokio::sync::watch` 广播 |
| eval | 单一 tokio 任务独占全部状态(检查状态机 + incident 表 + 恢复 FSM + 静默表);主循环 `select`:`sleep(最近到期)` + `control_rx.recv()` + `snapshot_rx.changed()`,`MAX_TICK` 1s 兜底唤醒;探测并发用 `JoinSet`,结果回本任务推进状态机 |
| alert | 独立任务消费 `mpsc::channel(256)`;分发内部 `Semaphore` 限并发,单次投递独立任务执行(panic 隔离);分发任务 panic 由 runtime 监督重建(最多 3 次,give-up 写 journal 审计) |
| journal | 独立阻塞写任务,`mpsc::channel(1024)` 接收,append-only;关键事件 `send().await` 等待 `sync_data` 确认(含入队总预算 2s) |
| control | 独立 axum 任务;经 watch 读快照、经命令通道(容量 16)下发命令;满 → 503 |
| metrics history | 独立 writer 任务消费 metrics watch 克隆(eval 零改动,旁路) |

**背压策略**:alert 队列满 → 丢最旧 + WARN 审计(告警时效优先于完整性,完整性由 spool 补齐);eval 命令通道满 → control 返回 503。

### 3.3 runtime 装配与停机(`runtime.rs`)

装配顺序(全部早失败点,任一失败 main 退出码 2,且发生在 `data_dir` 创建之前):TLS 材料加载 → 端口 bind → 配置校验 → data_dir/journal 装配 → 各任务启动。配置了 Bearer token 但未配 TLS 时 WARN 一次(明文传输提示)。

停机序列:置 shutdown → abort eval/control → alert 排空 500ms 窗口(超时剩余计入 dropped + journal 审计)→ metrics pusher abort → history writer abort → journal flush 100ms 窗口。

`request_shutdown()`(F15,进程内停机):静态 `REQUESTED` + `Notify`,permit 无丢失窗口——**select 分支不得加 `if REQUESTED` 前置守卫**(分支被禁用时 `notified()` 未注册等待者,notify 只存 permit 不唤醒;第十七轮 P0 教训,`tests/inprocess_shutdown.rs` 回归咬合)。

---

## 4. monitor-core 模块地图

| 模块 | 文件 | 职责 |
|---|---|---|
| `config` | `src/config.rs` | `ConfigRaw`(serde,deny_unknown_fields + serde default)→ `Config::validate()` 交叉校验;默认值;redacted 视图 |
| `model` | `src/model/{ids,snapshot,alert,control}.rs` | `ProcessId` newtype、`ProcessIdentity{pid, start_time}`;`HardwareSnapshot`/`ProcessSnapshot`/`Degradation`;`AlertEvent`;`ControlCommand`/`StatusReport`/`MetricsReport`/`MetricsSample`(cap 7200) |
| `collect` | `src/collect/{mod,sysinfo_collector}.rs` + `deep/{mod,hwmon,smc}.rs` | `Collector` 同步 trait;sysinfo 0.40 实现;深度采集 feature(hwmon/AppleSMC);`StubCollector` 降级 |
| `eval` | `src/eval/{mod,check,state,incident,maintenance,reload}.rs` | 单写者调度器主循环;探测执行;检查状态机;incident 去噪;维护窗口;热重载 diff/apply;静默表;指标入环;崩溃启动事务 |
| `alert` | `src/alert/{mod,webhook,signature,spool,metrics_push}.rs` | AlertQueue(drop-oldest 256);RoutingNotifier;webhook/slack payload;HMAC 签名共享层;spool;指标周期推送;deadman |
| `recover` | `src/recover/{mod,policy,journal,reconcile}.rs` + `process/{mod,unix,windows}.rs` | `RecoveryFsm` 全转移表;`RestartPolicy`/`Backoff`/`BudgetWindow`;journal 写/轮转/重放;孤儿对账;进程管理平台层 |
| `control` | `src/control/mod.rs`(+ `status_page.html`) | axum 路由、鉴权中间件、TLS/mTLS 装配、只读状态页 |
| `runtime` | `src/runtime.rs` | 装配、任务编排、停机序列、`StopReason` |
| `metrics_history` | `src/metrics_history.rs` | F39 历史落盘 writer |
| `error` | `src/error.rs` | `CoreError`:Config/Io/Collect/Alert/Recover/Control/Json/Toml/Http |

---

## 5. 数据模型

### 5.1 身份(防 PID 复用)

```rust
pub struct ProcessId(pub u32);                                   // newtype,不泄漏 sysinfo::Pid
pub struct ProcessIdentity { pub pid: ProcessId, pub start_time: u64 }  // epoch 秒
```

一切存活判定、停止、接管动作前必须校验 `(pid, start_time)`;裸 PID 永远不作为杀进程依据(DESIGN §8.3 强制)。

### 5.2 快照与降级

```rust
pub struct HardwareSnapshot {
    pub timestamp: SystemTime,           // 事件时间戳(展示用;判定一律单调时钟)
    pub cpu_usage_percent: Option<f32>,  // 首采 None + Degradation,不伪造 0
    pub per_core_usage: Vec<Option<f32>>,
    pub memory_total_bytes: u64,
    pub memory_used_bytes: u64,
    pub disks: Vec<DiskUsage>,
    pub networks: Vec<NetIo>,
    pub temperatures: Vec<Temperature>,  // 空 Vec + Degradation = 平台无数据
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
    // 深度采集另有 DeepHardwareUnavailable / SensorRead(含机读 SensorUnavailableReason:
    // Unsupported/NoSensors/PermissionDenied/Io/InvalidData/Disabled/Fault)
}
```

### 5.3 告警事件

```rust
pub struct AlertEvent {
    pub incident: u64,        // = incident_key = hash(target_name, check_name)
    pub category: Category,   // Alert | Escalate | Resolve | Flap
    pub severity: Severity,   // Warning | Critical
    pub target: String,
    pub check: String,        // Fatal 合成告警固定为 RECOVER_CHECK = "__recover__"
    pub reason: String,
    pub observed: Option<String>,
    pub first_seen: SystemTime,
    pub last_seen: SystemTime,
    pub occurrences: u64,
}
```

journal 与 webhook 共用此结构(serde 序列化)。

---

## 6. 采集层(collect)

- `Collector` 是**同步 trait**(实现独占 `sysinfo::System`,由专用阻塞线程单线程驱动);`create_collector()` 初始化失败降级 `StubCollector`(携带 `Degradation::CollectorInit`)。
- **sysinfo 0.40 API 锚点**(DESIGN §6.1):`System::new() -> Result`、`global_cpu_usage()`、`Component::temperature() -> Option<f32>`、`refresh_processes(ProcessesToUpdate::Some(&pids), true) -> usize`、`Process::start_time() -> u64`。禁用 `refresh_all`。
- **CPU 首采**:构造期隔 `MINIMUM_CPU_UPDATE_INTERVAL`(Apple 平台 200ms)双采样;此后绝不睡眠;首 tick 数据不可用 → `None` + `Degradation::CpuFirstSample`。
- **部分刷新协议(强制)**:每轮由 eval 提供**全部** watched PID;一次 `refresh_processes(..., remove_dead_processes=true)`;死亡判定 = `(pid, start_time)` 不复存在;查不到 PID ≠ 死亡确认(需连续 `confirmations`)。
- **温度可信过滤**:-20..=150 ℃ 之外丢弃 + `Degradation::TemperatureImplausible`(macOS 实测有 -9201 ℃ 级垃圾值);label 含 `tdev`/`als-temp` 等噪声源跳过。
- **深度采集**(`collect-deep-hardware`):`DeepCollector` 包装基础采集器。Linux 只读 `/sys/class/hwmon` 的 `fanN_input`(RPM)/`inN_input`(mV→V),尊重 enable/fault 属性;macOS 经 IOKit 只读 AppleSMC 固定候选键,按 SMC 类型解码,电压可信界 ±100 V;每传感器保留 `Option<f32>`,故障/禁用/权限/无效值均机读降级;Windows 返回 unsupported 降级。0 RPM 是有效值。

---

## 7. 检查与告警引擎(eval + alert)

### 7.1 eval 单写者(`eval/mod.rs`)

主循环 `select` 三事件源:sleep 到最近到期时间 / 控制命令 / 快照变更;`MAX_TICK` 1s 兜底。每 tick:驱动到期探测(在途上限 1,跳过计 `probe_skipped`)→ 推进检查状态机 → incident 去噪产出 AlertEvent → 推进恢复 FSM → 消费控制命令 → 指标入环(F18,`METRICS_HISTORY_CAP = 7200` ≈ 2h@1s)。

### 7.2 检查状态机(`eval/state.rs`)

`Ok --1 次失败--> Warning --连续 failure_threshold--> Critical`;连续 `success_threshold` 成功回落 Ok。纯逻辑,单调 `Instant` 注入,时间从不内部读取。任意态下 `startup_grace` 期内失败不计入;宿主机休眠唤醒后首轮探测丢弃记 degraded。

### 7.3 探测(`eval/check.rs`)

`ProbeResult::{Unavailable, Ok, Fail}`;三种 kind:TCP connect / HTTP GET + 状态区间(默认 200-299,跟随重定向 ≤10 跳,302 链终点判 OK——与 webhook 投递禁重定向相反)/ process CPU+内存阈值(身份校验)。所有探测由调用方 `tokio::time::timeout` 包裹,超时=失败,future 被丢弃。

### 7.4 incident 去噪(`eval/incident.rs`)

- `incident_key = hash(target, check)`,不含 severity/reason——升级不产生"第二起事故"。
- 五条规则:① 首条通知永不抑制;② 持续期间同 severity 重通知受 `renotify_interval` 抑制;③ Escalate 总是发送;④ Resolve 仅当该 incident 曾通知过才发;⑤ `flap_window` 内 open/close ≥ `flap_threshold` → 只发一条 Flap 摘要(Flap 不计入 notified,防凭空 Resolve);transitions 环上限 1024。
- **F25 跨重启恢复**:journal 重放维护 open incident 集合(Alert/Escalate 开门刷新,Resolve 关门 + 孤儿 resolve 计数,Flap 不动门;Fatal 合成 incident 属恢复域,随 Reset 闭门);启动时恢复进引擎并重通知一次,`reason = "restored from journal: <原 reason>"`,通知窗口起点 = 恢复时刻;flap 计数器不恢复(新会话 fresh 窗口);检查状态机不回填(fresh 起步,保护 `confirmations` 保守契约);key 运行期复算,不信任 journal 持久化值。
- **F27 静默(eval 侧)**:`SILENCE_MAX_SECS = 604800`(7 天);deadline 用单调 `Instant`;窗口内 Alert/Escalate/Flap **不入 AlertQueue**;Resolve 与 Fatal 合成告警(`check = RECOVER_CHECK`)恒豁免;建立/解除/到期/首次抑制命中各写一条 Audit;内存态,重启失忆;reload 剪除被删 target 的静默。

### 7.5 维护窗口(`eval/maintenance.rs`)

UTC 纯函数历法(epoch 秒 → 星期索引 + 当日分钟,锚点 epoch 0 = 周四);`contains` 处理跨 midnight 与 days 过滤(越界部分归属开启日);判定为纯函数注入 epoch 秒,每 tick 重算,无持久状态(重启自动重入);窗口表为构建时快照,不热应用。

### 7.6 热重载(`eval/reload.rs`)

diff 纯函数:只应用目标拓扑(checks/services/watches 增删改,按 name 匹配 + serde JSON Value 比较);service 的 command/args/workdir/env/windows_service 变更 = 删+增(FSM 水位不保留),restart/activate/console 软字段原地更新(水位保留);非拓扑段按固定顺序检测 `agent → log → control → collect → alert → metrics → maintenance`,变更仅写审计 `section X changed but requires restart`;解析/校验失败整体拒绝。reload 在 `spawn_blocking` 中读文件+校验(不冻结单写者)。config_view(F33)reload 成功后经 watch 推送新 redacted JSON。

### 7.7 告警分发(`alert/mod.rs`)

- `AlertQueue`:drop-oldest 有界 256;满丢最旧 + `alerts_dropped` 计数 + WARN;`counters()` 暴露三计数(随 eval 报告发布到三出口)。
- 分发:每笔 `retries + 1` 次尝试,指数退避 1s×2^n 封顶 10s,单次受 `alert.timeout` 约束;单次投递独立任务(panic 隔离);全失败 → `alert_delivery_failures` + ERROR 审计 → spool 落盘。
- **RoutingNotifier(F26)**:routes 非空时装配;`*` 通配 / target 前缀 `starts_with`;顺序首条生效;无匹配回落默认通道;路由级 secret/headers 覆盖全局(`effective_route_secret` / `effective_route_headers` 纯函数);构建失败的 route WARN 跳过;`webhook_url` 空且无匹配 → NoopNotifier(事件仍入 journal,`alerts_sent` 照常计数)。
- **spool(F12)**:`{data_dir}/spool/pending.jsonl` 追加(含 flush + sync_data);成功确认后经 `pending.jsonl.next` rename 原子更新(事务持互斥许可);启动及每 60s 重放,每轮最多 16 条;超 7 天丢弃;重放走分发任务持有的同一 notifier 实例(按当前配置重新路由/签名/带头)。
- **webhook(F29)**:payload `json`(AlertEvent 原样)或 `slack`(`{"text": "[Severity] Target/Check: reason (xN)"}`);**禁用重定向**(`Policy::none()`,防 POST 降级 GET);非 2xx = 失败;自定义头(F37)全部叠加,非法名/值构造期 WARN 丢弃。
- **HMAC 签名共享层(`alert/signature.rs`)**:头名 `X-Service-Watch-Signature`,值 `sha256=<hex 小写>`;签名对象 = 实际发送的 body 字节(先 `serde_json::to_vec` 再签名发送);密钥未配置 = 无签名头;密钥/签名值不落日志;webhook 随 `notify-webhook` 门控、metrics push 签名随 `http-client` 门控。
- **metrics push(`alert/metrics_push.rs`,F34)**:每 `interval` POST 最新 MetricsReport JSON;单次尝试无重试(下轮覆盖);禁重定向;连败首轮/每 10 连败/恢复首轮写 journal Audit(粘性边沿)。
- **deadman**:独立任务每 30s POST `{}`,失败仅 WARN;首拍立即;runtime 单次启动。

---

## 8. 恢复引擎(recover)

### 8.1 RecoveryFsm(`recover/mod.rs`)

每个 supervise 目标独占一份 FSM。状态:`Running / Suspect / Recovering / ForceKill / Restarting / Starting / CrashLoop / Fatal`。

事件入口:`on_child_exit` / `on_liveness_gone` / `on_liveness_ok` / `on_health` / `on_tick` / `on_spawn_result` / `on_scm_spawn_ok`(SCM 目标无进程身份专用入口)/ `on_stop_result` / `on_activate_result` / `reset` / `restore` / `update_policy`(热更新保留水位)。

动作:`RecoverAction::{ScheduleSpawn, SignalStop, ForceKill, RunActivate, MarkFatal, JournalRestart, Audit}`。

**预算语义(关键)**:所有自动重启均计数(含慢死亡、spawn 失败、健康挂死的 Restart 分支);Activate 分支不计(激活成功 = 未重启);ForceKill 下收到 child_exit 必须收尾(P0 回归教训)。Fatal 仅 `reset` 解除。全转移表见 DESIGN §8.2 / 用户指南 §11.2。

### 8.2 策略组件(`recover/policy.rs`)

- `RestartPolicy`:从 `RestartCfg` 固化。
- `Backoff`:`initial × 2^(attempt-1)` 封顶 `max`;Running 持续 `min_uptime` 后 reset。
- `BudgetWindow`:滑动窗口 ring buffer 记录重启时间戳。

### 8.3 journal(`recover/journal.rs`)

- 写入:有界通道 1024 → 独占阻塞 writer;关键事件(Restart/Fatal/Reset/ResetTargets/ProcessIdentity/SpawnIntent/SpawnOutcome)`send().await` 等 `sync_data` 确认(含入队总预算 2s),失败 → `JournalCounters.fail()` → runtime `StopReason::PersistenceFailed` 停机;普通审计 `try_send`,满队列计数丢弃(首次 + 每 64 条 ERROR 一次)。
- **轮转(F24)**:写后实测尺寸超 `journal_max_size`(默认 10 MiB,0 = 禁用)→ 当前文件改名 `journal.1.jsonl`(只保留一代),重建当前文件 + 轮转 Audit;改名失败降级继续写 + WARN;打开时末字节非 `\n` 补一个(崩溃半行定界)。
- **重放**:`journal.1 → journal.jsonl` 按行顺序 apply 到同一状态;坏行/半行隔离;产出 `JournalState { restarts, fatal, last_reset, processes, pending_spawns, open_incidents, orphan_resolves }`;`config_digest` = FNV-1a-64 覆盖热字段集(name+command/args/workdir/env/windows_service),跨 rustc 版本稳定。

### 8.4 孤儿对账(`recover/reconcile.rs`)

`OrphanTracker` 状态 `Pending / Attached(identity) / Gone`;只有本轮明确请求过候选身份、且采集器可用的新快照才参与;未验证身份同 pid 不同 start_time 持续 600s(`RECONCILE_MISMATCH_TIMEOUT`)裁决 Gone(PID 复用兜底)。接管期间(attach 呈现)禁止 stop/force-kill/activate/控制面 restart。

### 8.5 进程管理(`recover/process/`)

- `ProcessManager`:eval 独占,spawn/stop/poll-exit。
- **Unix**(`unix.rs`):`process_group(0)` + `kill_on_drop(false)` + stdio 全 null;stop = `killpg(SIGTERM)`,force = `killpg(SIGKILL)`(ESRCH 视为成功);`try_wait` 兜底 reap 防僵尸;`pending_kills` 登记组长 reap 后仍存活的组员到期补 SIGKILL(存在秒级 pgid 复用误杀窗口,如实记录)。
- **Windows**(`windows.rs`):spawn 后立即 `AssignProcessToJobObject`(Job 不带 KILL_ON_JOB_CLOSE;tokio Child 不暴露主线程句柄,spawn→assign 间存在微秒级逃逸竞态,如实偏差);`console = true` 先尝试 `GenerateConsoleCtrlEvent(CTRL_BREAK)`,grace 超时升级 `TerminateJobObject`。
- **SCM 目标**(`scm-target` feature + `windows_service` 配置):spawn = `Start`(幂等);stop = `Stop`(幂等);liveness = `QueryStatus == Running`(仅 SERVICE_STOPPED 是死亡证据);force_kill = 再次 Stop(如实降级);无 `(pid, start_time)` 身份,FSM 走 `on_scm_spawn_ok`;`ScmOps` trait 可注入测试假实现;agent 只停本会话启动过的服务。

---

## 9. 控制平面(control)

### 9.1 路由与鉴权

axum 0.8,路由分两个子 Router:`public` 仅 `GET /health`(永不鉴权);其余全部进 `protected` 叠加 Bearer 中间件(F23)。401 统一 JSON 说明体 + `WWW-Authenticate: Bearer`,不区分失败原因;恒定时间比较 `ct_eq`(手写,长度差计入);token 值不进任何日志/审计。TLS(F32)与 mTLS(F38)在装配期加载,坏材料早退退出码 2;rustls provider:进程级 `CryptoProvider::get_default()` 优先,否则局部构造 aws-lc provider,**不 `install_default`**;手工补 ALPN `h2`/`http/1.1`。

### 9.2 端点清单

| 端点 | 说明 |
|---|---|
| `GET /health` | 200 空体;鉴权豁免 |
| `GET /` | F22 只读状态页(`include_str!("status_page.html")`,内联 CSS/JS,JS 每 2s fetch /status 与 /metrics,SVG 迷你图 `spark-*`,全部插值 escapeHtml) |
| `GET /ready` | 200 `{"ready":true}` / 503 `{"ready":false}` |
| `GET /status` | StatusReport(eval 最新汇报;sender 消失仍可读最后快照) |
| `GET /metrics` | 默认 JSON;`?format=prometheus` 或 `Accept: text/plain` → Prometheus 文本(`render_prometheus` 表驱动,前缀 `service_watch_`;`last_collect_at` → `service_watch_last_collect_timestamp_seconds`) |
| `GET /metrics/history?range=` | F18 环形缓冲窗口快照;range 缺省 300、上限 7200、非法 400;>600 点等距降采样保留首尾 |
| `POST /v1/restart/{target}` | Fatal(经 status watch 判定)→ 409;否则 202 |
| `POST /v1/check/{target}` | 202(绑定检查 next_due 立即到期) |
| `POST /v1/reset/{target}` | 202(解除 Fatal + 清预算) |
| `POST /v1/reload` | F14;body 可选 `{"path": "..."}`;非法 body 400 |
| `POST/DELETE /v1/silence/{target}` | F27;POST body `{"duration": <秒>, "reason": <可选>}`;缺 duration/非 JSON → 400;语义拒绝 eval 侧 |
| `GET /v1/journal` | F28/F35;`tail`(1..=1000,缺省 200)/`target` 精确/`type` serde tag/`since` epoch 秒;响应 `{"truncated": bool, "lines": [...]}`;gen1 在前合并;每文件 512KB 有界尾读(spawn_blocking) |
| `GET /v1/config` | F33;`Config::redacted()` JSON(token/webhook_secret/路由 secret/metrics.secret/投递头值一律 `***redacted***`) |

命令通道(`ControlCommand`,serde tag `"op"` lowercase):`Restart / Check / Reset / Reload{path} / Silence{target,duration_secs,reason} / Unsilence`;容量 16,满 → 503 "command queue full",停机中 → 503 "agent shutting down"。

---

## 10. 配置系统(config)

两层:解析层 `ConfigRaw`(serde,`deny_unknown_fields` + `#[serde(default)]`;例外:`CheckCfg` 因 `kind` 用 `#[serde(flatten)]` 无法严格校验,但未知 kind 值仍被拒绝)→ 校验层 `Config::validate()` 交叉校验;任一失败 = 配置错误退出码 1。运行时只见 `Config`。

时长格式 `"500ms"|"5s"|"10m"|"1h"`,可组合 `"1h30m"`。完整 schema 与默认值见 DESIGN §4 与 [config/config.example.toml](../../config/config.example.toml)(单测 `parses_full_example` 保证 example 始终可解析)。

`validate()` 要点(源码 `config.rs` L723-1395):时长正值且在单调钟范围内;check `timeout < interval`;header 名 RFC 7230 token / 值仅可见 ASCII + TAB(表内与跨表大小写折叠冲突均拒);TLS 成对;mTLS 必须建立在 TLS 上;maintenance `HH:MM` 与 duration/days 合法;name 三空间各自唯一;`depends_on` DFS 三色标记查环;`history_path` 与 `journal.jsonl` / `journal.1.jsonl` / `spool/pending.jsonl` / `spool/pending.jsonl.next` 规范化路径碰撞守卫。

`Config::redacted()`:token / webhook_secret / 路由级 secret / metrics.secret / 全局与路由投递头**值**以 `***redacted***` 占位,其余字段(含 TLS 路径)原样;`GET /v1/config` 消费;占位不可回灌配置。

---

## 11. 存储与持久化

| 文件 | 模块 | 要点 |
|---|---|---|
| `journal.jsonl` + `journal.1.jsonl` | `recover/journal.rs` | 恢复域事实源;关键事件确认制;单代轮转;半行定界;两代顺序重放 |
| `spool/pending.jsonl`(+ `.next` 中转) | `alert/spool.rs` | 投递补投;rename 原子事务 + 互斥许可;7 天过期 |
| `[metrics] history_path`(+ `.1`) | `metrics_history.rs` | 观测域;行 = `{"at","report"}`(与 `/metrics/history` 样本同构);append 跨重启追加;10 MiB 单代轮转(gen1 路径复用 `journal::gen1_path` 单一事实源);无逐行 fsync;轮转只写 tracing 不写 journal audit |
| 内存环形缓冲 | `model::control` | `METRICS_HISTORY_CAP = 7200`;随 metrics watch 分发 |

**域隔离原则**:journal 是恢复域(停机/退出码关联),metrics history 是观测出口(打开失败 ERROR 后本次停用,agent 照常启动)——两域互不拖累。

---

## 12. 平台差异层

| 面 | Unix | Windows |
|---|---|---|
| spawn 隔离 | `process_group(0)` | Job Object(无 KILL_ON_JOB_CLOSE;assign 前微秒级竞态) |
| graceful stop | `killpg(SIGTERM)` | `console=true` 尝试 CTRL_BREAK,否则 Terminate 基线 |
| 强杀 | `killpg(SIGKILL)` | `TerminateJobObject` |
| 热重载触发 | SIGHUP + 端点 | 仅端点 |
| 深度采集 | Linux hwmon / macOS AppleSMC(`#[cfg(target_os)]`) | unsupported 降级 |
| 服务形态 | 无(交 systemd/launchd) | `windows-service` feature + `--service-run`(SCM dispatcher + control handler;`request_shutdown` 先于 StopPending 报告调用;30s wait-hint 兜底;日志写文件禁 ANSI) |
| SCM 目标 | — | `scm-target` feature + `windows_service` 配置 |
| 停机信号 | SIGINT/SIGTERM/SIGHUP → 退出 130 | ctrl_c + REQUEST_NOTIFY |

平台条件编译:`#[cfg(unix)]` / `#[cfg(windows)]` / `#[cfg(target_os = "macos")]` / `#[cfg(any(target_os = "linux", test))]`(hwmon 测试可见)。

---

## 13. Feature Flags 与构建矩阵

monitor-core features:

```toml
default = ["collect-sysinfo", "probe-http", "notify-webhook", "control-http"]
collect-sysinfo        = ["dep:sysinfo"]
collect-deep-hardware  = []                # 纯门控,同时启用 collect-sysinfo 语义
http-client            = ["dep:reqwest", "dep:hmac", "dep:sha2"]
probe-http             = ["http-client"]
notify-webhook         = ["http-client"]
control-http           = ["dep:axum", "dep:axum-server", "dep:rustls-pemfile", "dep:rustls"]
scm-target             = ["dep:windows-service"]   # 仅 cfg(windows) 实际生效;非 Windows 诚实降级为错误
```

- monitor-agent 的 features 与 core 对齐(passthrough,`default-features = false` 使 `--no-default-features` 真正裁掉 reqwest/axum);另有 `windows-service`。
- monitorctl 仅依赖 core 的 `collect-sysinfo` + 可选 `collect-deep-hardware`。
- **交叉编译验收线两条**(DESIGN §6.5):① 本地/CI 交叉 `cargo check --target x86_64-pc-windows-msvc`(无 C 依赖 feature 组合;reqwest/ring 不在本地 Windows 交叉范围);② 完整 Windows 验证由 CI `windows-latest` runner 承担(需 NASM + CMake)。

---

## 14. 测试策略

1. **确定性单测**:状态机/去噪/预算全部以注入单调 `Instant` 驱动(调用方传参,状态机不读时钟);调度层 tokio 时间可 `start_paused`。覆盖硬指标:DESIGN §7.1/§7.2/§8.2 每条转移边至少一条单测。
2. **边界 mock**:`Collector` / `Notifier` / `ProcessHandle` / `ScmOps` 为 trait,测试用假实现(sysinfo/网络/进程零依赖)。
3. **集成测试**(`crates/monitor-core/tests/`,13 个文件):

| 文件 | 覆盖 |
|---|---|
| `alert_routing.rs` | F26 双 webhook stub 路由命中/回落 + F29 签名验签(含 spool 重放端到端)+ F37 头 |
| `http_probe.rs` | HTTP 探测端到端(axum stub + 真实调度器):慢响应→告警→恢复全链路 |
| `incident_restore.rs` | F25 incident 跨重启恢复与重通知 |
| `inprocess_shutdown.rs` | F15 `request_shutdown` 停机回归(第十七轮 P0 死锁根因) |
| `maintenance_window.rs` | F31 维护窗口端到端(抑制/Resolve 穿透/Audit 边沿) |
| `metrics_history.rs` | F39 历史落盘(行格式 + 单代轮转) |
| `metrics_push.rs` | F34 推送签名(有 secret 验签 / None 不带头) |
| `scm_target.rs` | SCM 目标运行时契约(注入假 ScmOps) |
| `silence_notification.rs` | F27 静默端到端(抑制/穿透/审计边沿) |
| `status_page.rs` | F30 状态页 SVG 迷你图 + history 数据源契约 |
| `storage_fault.rs` | 存储故障演练(磁盘满/慢盘超时/断电半行隔离/修复重启) |
| `supervise_smoke.rs` | 真进程冒烟(`#![cfg(unix)]`):冷启动身份、kill -9 拉活、crash loop → Fatal |
| `tls_control.rs` | F32 HTTPS + F38 mTLS(rcgen 自签;`--insecure` 等价客户端/默认拒信/明文失败/坏 PEM/握手接受与拒绝) |

另有大量单测内嵌各源文件(config 校验矩阵、FSM 全转移表、journal 轮转/重放、control 端点、monitorctl 渲染纯函数等)。全测基线 369+(见 CHANGELOG 各轮验证记录)。

4. **CI 矩阵**:三平台 fmt + clippy(-D warnings)+ test;Windows 交叉 check 两条线;`windows-scm.yml` 实机演练(安装→RUNNING→/health→spawn 事务→优雅停机→Job Object 孤儿断言→热重载→错误退出自愈)。

---

## 15. 开发工作流与工程纪律

### 15.1 每次提交前

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                 # 全部确定性,无真实 sleep
cargo metadata --format-version 1 | jq -e '[.packages[] | select(.source == null) | select(.rust_version != null)] | length == 0'
```

可选 feature 验证:

```bash
cargo test --workspace --features collect-deep-hardware
cargo clippy --workspace --all-targets --features collect-deep-hardware -- -D warnings
cargo check --workspace --no-default-features --features collect-sysinfo
cargo check --workspace --no-default-features --features collect-sysinfo,control-http
cargo check -p monitor-agent --target x86_64-pc-windows-msvc --no-default-features --features collect-deep-hardware,windows-service
```

### 15.2 代码纪律(CONTRIBUTING / AGENT.md)

- **单写者原则**:状态机只在 eval 任务内变动;跨任务只传不可变消息。
- **降级可观测**:不可用数据 `Option` + `Degradation`,禁止伪造 0。
- **单调时钟**:时长判定一律 `Instant`;`SystemTime` 仅作事件时间戳。
- **身份校验前置**:杀/重启必须以 `(pid, start_time)` 校验为前提;attach 目标永不干预。
- **运行路径禁 `unwrap()`/`panic!`**(锁中毒用 `into_inner()` 恢复);错误统一 `CoreError`。
- **日志纪律**:恢复/告警/降级三类事件必须 `INFO`+ 且含结构化字段(`target`/`check`/`incident`/`degraded`/`pid`/`attempt`);禁热路径 DEBUG 刷屏。
- **密钥纪律**:token/签名密钥/投递头值/私钥内容不进任何 tracing 字段、日志、审计与错误信息;装配日志只报布尔(如 `signed = true/false`)或数量。
- **edition 2024 注意**:平台 FFI 优先 `libc`/`windows-sys`;手写 FFI 用 `unsafe extern` 块;`unsafe_op_in_unsafe_fn` warn-by-default;标识符避开 `gen`;dyn 场景统一 `#[async_trait]`(如 `Notifier`)。

### 15.3 PR 检查清单

- [ ] `cargo fmt --check` / `clippy -D warnings` / `cargo test --workspace` 全绿
- [ ] 新行为有确定性测试(时间判定注入参数,不用真实 sleep)
- [ ] 语义变更同步更新 DESIGN.md(状态机/默认值/schema)与 ROADMAP.md(状态/偏差表)
- [ ] 跨平台影响:平台分支有降级说明(DESIGN §6.4 矩阵)
- [ ] 告警/恢复语义变更:对照 §7.2 五条规则与 §8.2 全转移表补测试
- [ ] CHANGELOG.md 增加条目

### 15.4 关键历史教训(写码前必读)

- **select 分支不加前置守卫**:`if REQUESTED` 守卫会禁用分支导致 `notified()` 未注册,notify permit 无人唤醒(第十七轮 P0,`inprocess_shutdown.rs` 咬合)。
- **eval 报告反向基座**:`merged_metrics` 以 eval 报告为基座只覆盖 alert 三字段——新增 MetricsReport 字段自动透传,根除"漏拷字段"缺陷类型(第十八轮)。
- **关键 journal 事件必须 await 确认**:`try_send` 路径在拥塞时会触发 critical 失败闩锁且内存侧清除已生效(第十九轮 P3)。
- **spawn_blocking 包裹慢盘操作**:启动重放与 reload 配置读取不得冻结 eval 单写者。
- **header 值校验对齐 http crate 字节规则**(非 ASCII 可见性问题,第十四轮 P2)。
- **reqwest 0.13 feature 更名**:`rustls-tls` → `rustls`(README/DESIGN 均按新名)。
- **Windows 服务停机顺序**:`request_shutdown()` 必须先于 StopPending 报告调用(SCM 实弹调试教训)。

---

## 16. 扩展点:如何新增 X

- **新增 check 类型**:`eval/check.rs` 加 `CheckKind` 变体与探测实现(返回 `ProbeResult`);`config.rs` 的 `CheckCfg` flatten 层加字段与校验;`config.example.toml` 同步;状态机/去噪自动复用。
- **新增通知出口**:实现 `#[async_trait] Notifier`(dyn 兼容);在 runtime 装配处包进 `RoutingNotifier` 分发链;保持超时/退避/队列/spool 语义由分发层统一处理。
- **新增 journal 事件**:`JournalEvent` 加变体(serde tag snake_case)→ 写入路径选择关键(确认制)或普通(try_send)→ 重放 apply 分支 → `JournalState` 相应字段。
- **新增配置字段**:ConfigRaw → 默认值 → validate 交叉校验 → example 同步 → DESIGN §4 schema/默认值表 → reload 范围判定(拓扑段 or 要求重启段)。
- **新增平台能力**:平台实现放 `#[cfg(target_os = ...)]` 或 target-specific dependency;必须同时提供该平台编译检查与降级说明(写进 DESIGN §6.4 矩阵)。
- **新增控制端点**:protected Router 挂路由(鉴权自动叠加);新命令加 `ControlCommand` 变体(eval 侧消费);`monitorctl` 加子命令与退出码语义;`Config::redacted()` 检查新敏感字段脱敏。

---

## 17. 文档地图

| 文档 | 内容 |
|---|---|
| [AGENT.md](../../AGENT.md) | 工程硬约束(裁决顺序最高) |
| [DESIGN.md](../../DESIGN.md) | 实现规约权威(schema/状态机/并发/平台契约) |
| [ROADMAP.md](../../ROADMAP.md) | 功能规范、验收标准、进度、已知偏差表 |
| [EXECUTION.md](../../EXECUTION.md) | 分阶段执行计划 |
| [BENCHMARK.md](../../BENCHMARK.md) | 同类项目对照与选型依据 |
| [CONTRIBUTING.md](../../CONTRIBUTING.md) | 构建测试命令与 PR 清单 |
| [CHANGELOG.md](../../CHANGELOG.md) | 发布与评审修复记录 |
| [docs/RUNBOOK.md](../RUNBOOK.md) | 运维手册(排障时查阅) |
| [docs/DEPLOYMENT.md](../DEPLOYMENT.md) | 部署手册 |
| [user-guide.zh.md](user-guide.zh.md) / [user-guide.en.md](user-guide.en.md) | 用户指南 |
| [developer-guide.en.md](developer-guide.en.md) | 本文档英文版 |

---

*本文档基于 2026-11-22 的 main 分支源码(v0.3.0)整理;实现细节如与代码冲突,以代码与 [DESIGN.md](../../DESIGN.md) 为准。*
