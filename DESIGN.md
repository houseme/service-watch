# DESIGN.md

> 实现规约 (权威)。默认值、schema、状态机转移以本文档为准;范围与进度见 [ROADMAP.md](ROADMAP.md);硬性约束见 [AGENT.md](AGENT.md);同类项目对照见 [BENCHMARK.md](BENCHMARK.md)。
> 版本引用核对日期:2026-11-10。

## 1. 设计原则

1. **观察者外置**:agent 独立于被监控服务的生命周期;被监控服务不负责拉起 agent,agent 死亡不连带杀被监控服务 (Windows Job Object 默认不带 `KILL_ON_JOB_CLOSE`,见 §8.6)。
2. **恢复策略显式化**:`min_uptime` / `backoff` / `restart_budget{max,window}` / `stop_grace` / `startup_grace` 全部显式可配;Fatal 是终态闩锁，仅人工解除。
3. **降级可观测，不伪造数据**:不可用字段用 `Option` + 机读 `Degradation` 枚举表达，不用 0 掩盖;CPU 首采、温度垃圾值均走降级路径。
4. **零锁单写者**:全部状态机由单一 eval 任务独占，跨任务只传不可变消息;共享可变状态一律不允许 `Arc<Mutex>`(§2.2)。
5. **可确定性测试**:所有时长判定走注入时钟;sysinfo/网络全部在 trait 边界后可 mock(§11)。

## 2. 总体架构与并发模型

### 2.1 组件图

```text
┌──────────────────────────── monitor-agent (bin) ────────────────────────────┐
│  main: clap → tracing init → monitor_core::run(config)                      │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────────────┐
│                            monitor-core (lib)                                │
│                                                                             │
│  [collect 线程 (阻塞)]  --watch-->  [eval 任务 (单写者)]  --mpsc-->  [alert 分发任务] │
│   sysinfo System 独占         状态机 × N(零锁)          有界队列 + 重试          │
│                                     │                                       │
│                                     ▼                                       │
│                              [recover(在 eval 内)]                           │
│                     RecoveryStateMachine × supervise targets                 │
│                     spawn/stop 走 ProcessHandle(平台实现)                    │
│                                     │                                       │
│  [control HTTP 任务] <──watch/快照──┘        [journal 写线程] <--append-- 异步 │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 并发与所有权模型 (强制)

| 组件 | 形态 | 说明 |
|---|---|---|
| 采集 | 专用 OS 线程独占 `sysinfo::System` | refresh 是阻塞调用，禁止在 async 上下文裸调;快照经 `tokio::sync::watch` 广播 |
| eval | 单一 tokio 任务独占全部状态 (检查状态机 + incident 表 + 恢复状态机) | 零锁;探测并发用 `JoinSet`,结果回本任务推进状态机 |
| alert | 独立任务消费 `tokio::sync::mpsc::channel(256)` | 队列满丢最旧 + 审计日志;分发内部 `Semaphore` 限并发，每笔投递带超时与重试 |
| journal | 独立写线程，`mpsc` 接收，append-only | eval 只发消息，不做文件 I/O |
| control | 独立 axum 任务 | 经 `watch` 读快照、经 eval 命令通道下发 restart/check/reset |

**背压策略 (显式)**:alert 队列满 → 丢最旧并记 `WARN` 审计 (告警时效优先于完整性，完整性由 v0.2 spool 补齐);eval 命令通道满 → control 返回 503。

### 2.3 数据流契约

单向：`collect → eval → alert / recover`。recover 不反向查询采集器：进程存活由「子进程退出事件 (亲生)+ liveness 探测 (收养)」双通道判定，身份以 `(pid, start_time)` 校验 (§8.3)。

## 3. 工作区与 Cargo 约束

```text
service-watch/
├── Cargo.toml            # [workspace] resolver = "3";无 default-members
├── crates/
│   ├── monitor-core/     # lib:全部可测试逻辑
│   ├── monitor-agent/    # bin:薄入口 (CLI/日志/信号)
│   └── monitorctl/       # bin:控制平面客户端 + 本地 dump-snapshot
```

### 3.1 monitor-core features 与依赖

```toml
[features]
default = ["collect-sysinfo", "probe-http", "notify-webhook", "control-http"]
collect-sysinfo = ["dep:sysinfo"]
collect-deep-hardware = ["collect-sysinfo"]
http-client     = ["dep:reqwest"]
probe-http      = ["http-client"]
notify-webhook  = ["http-client"]
control-http    = ["dep:axum"]

[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "time", "sync", "macros", "signal", "process", "net", "io-util"] }
serde  = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "1"
thiserror = "2"
tracing = "0.1"
url = { version = "2", features = ["serde"] }
async-trait = "0.1"
sysinfo = { version = "0.40", optional = true }          # 0.40.1 @ 2026-11-10
reqwest = { version = "0.13", default-features = false,  # 0.13.5;禁 default-tls
            features = ["json", "rustls"], optional = true }
axum = { version = "0.8", optional = true }              # 0.8.9
libc = { version = "0.2", optional = true }              # target.'cfg(unix)'
windows-sys = { version = "0.61", optional = true, features = [...] }  # target.'cfg(windows)'
```

约定：文档引用依赖版本一律带核对日期;以 `cargo add` 解析的最新可编译 stable 为准。**任何 Cargo.toml 禁止 `rust-version` 字段**(见 AGENT.md;注意依赖自身 MSRV 仍会被解析器强制，sysinfo 0.40.1 声明 1.95)。

### 3.2 edition 2024 注意事项

- 平台 FFI 优先用 `libc` / `windows-sys`,不手写 `extern`;必须手写时使用 `unsafe extern` 块。
- `unsafe_op_in_unsafe_fn` warn-by-default:unsafe 块显式化。
- 标识符避开 `gen`(2024 保留字)。
- `Notifier` 等 dyn 场景统一 `#[async_trait]`(原生 AFIT 非 dyn 兼容、future 无 `Send` 界)。

## 4. 配置系统 (权威 schema)

单一事实源。解析层 `ConfigRaw`(serde, `deny_unknown_fields` + `#[serde(default)]`)→ 校验层 `TryFrom<ConfigRaw> for Config`(交叉校验)→ 运行时只见 `Config`。

### 4.1 完整 schema(config/config.example.toml 与此一一对应)

```toml
[agent]
data_dir = "/var/lib/service-watch"     # journal/spool 目录;默认 "./data"

[log]
json = false                            # 结构化 JSON 输出

[control]                               # 控制平面 (feature control-http)
enabled = true
listen = "127.0.0.1:9170"               # 仅回环;绑定非回环需显式确认

[collect]
interval = "5s"                         # 全局采集间隔

[alert]
webhook_url = ""                        # 空 = 不投递，事件仍入 journal/日志
webhook_format = "json"                 # json | slack(Slack/Discord 兼容 {"text": ...})
timeout = "10s"                         # 单次投递超时
retries = 3                             # 指数退避重试次数 (1s 起步，×2，封顶 10s)
renotify_interval = "15m"               # 同 incident 持续期重复通知间隔
failure_threshold = 3                   # 连续失败 → Critical
success_threshold = 2                   # 连续成功 → Ok/Resolve
flap_window = "30m"                     # flap 检测窗口
flap_threshold = 5                      # 窗口内 open/close 次数达到即抑制
deadman_url = ""                        # 可选心跳 POST;失败仅记日志

[metrics]                               # 周期推送 (F 线 2026-11-13 已实现):
url = "http://127.0.0.1:9090/metrics"   #   POST MetricsReport JSON;失败即放弃 (下轮覆盖)
interval = "60s"                        #   连败边沿写 journal 审计;timeout < interval 校验
enabled = false
url = ""                                # 空 = 仅 /metrics 端点暴露
interval = "60s"

# ---- 检查目标 (可多个) ----
[[check]]
name = "api-health"                     # 全局唯一
kind = "http"                           # http | tcp | process
url = "http://127.0.0.1:8080/healthz"   # http 必填
expected_status = "200-299"             # http 可选，默认 200-299，含边界
# addr = "127.0.0.1:5432"               # tcp 必填
# process = "my-app"                    # process 必填：已配置 service/watch 名称或 "pid:1234"
# cpu_threshold = 90.0                  # process 可选
# mem_threshold_mb = 1024               # process 可选
interval = "10s"                        # 必须 > timeout
timeout = "5s"

# ---- 监管目标 (supervise，可多个;agent spawn 并可拉活) ----
[[service]]
name = "my-app"                         # supervise 必填
command = "/usr/local/bin/my-app"
args = ["--port", "8080"]
workdir = "/srv/my-app"
env = { RUST_LOG = "info" }
console = false                         # Windows:true = CREATE_NEW_PROCESS_GROUP，允许尝试 CTRL_BREAK
depends_on = []                         # F20 启动编排：依赖全部 Running 后才拉起 (禁止自引用/环)

[service.restart]                       # 默认值如下，可整体省略
min_uptime = "10s"
backoff_initial = "1s"
backoff_max = "60s"
max_restarts = 5                        # restart_window 内最大自动重启次数
restart_window = "10m"                  # 滑动窗口
stop_grace = "15s"
startup_grace = "30s"                   # 重启后预热期：失败不计入状态机
confirmations = 2                       # 健康类失败触发恢复的确认次数
hung_action = "restart"                 # restart | activate

[service.activate]                      # hung_action = "activate" 时必填
command = "/usr/local/bin/kick-my-app"  # 或 url = "http://..."
timeout = "10s"                         # 激活钩子超时
verify_for = "30s"                      # 激活后观察期，仍失败转 Restart

# ---- attach 目标 (仅监控告警，永不杀/重启) ----
[[watch]]
name = "systemd-nginx"
pidfile = "/run/nginx.pid"              # 或 pid = 1234
```

### 4.2 默认值总表

| 参数 | 默认值 | 参数 | 默认值 |
|---|---|---|---|
| collect.interval | 5s | alert.failure_threshold | 3 |
| alert.timeout | 10s | alert.success_threshold | 2 |
| alert.retries | 3 | alert.renotify_interval | 15m |
| alert.flap_window | 30m | alert.flap_threshold | 5 |
| restart.min_uptime | 10s | restart.backoff_initial | 1s |
| restart.backoff_max | 60s | restart.max_restarts | 5 |
| restart.restart_window | 10m | restart.stop_grace | 15s |
| restart.startup_grace | 30s | restart.confirmations | 2 |
| activate.timeout | 10s | activate.verify_for | 30s |
| control.listen | 127.0.0.1:9170 | metrics.interval | 60s |

### 4.3 校验规则 (违反即配置错误，退出码 1)

1. 每个 check 的 `timeout < interval`，check 与 collect 间隔均至少 100ms。
2. http check:`url` 合法且 scheme 为 http/https;`expected_status` 形如 `"200-299"`。
3. process check 的 `process` 非空，cpu/mem 阈值至少一个，CPU 阈值必须有限且非负。
4. supervise:`command` 非空;`backoff_initial ≤ backoff_max`;`startup_grace ≥ min_uptime`;check 名称引用存在。
5. `hung_action = "activate"` 时必须配置 `[service.activate]`，钩子的 command/url 必须且只能配置一个，command 非空，URL 使用 http/https。
6. check/service/watch 各自名称唯一且非空，`data_dir` 非空并可创建，告警阈值至少 1，webhook/deadman URL 使用 http/https。缺省 agent 与空 `[agent]` 均使用 `./data`。

## 5. 数据模型

```rust
pub struct ProcessId(pub u32);                          // newtype，不泄漏 sysinfo::Pid
pub struct ProcessIdentity { pub pid: ProcessId, pub start_time: u64 }  // epoch 秒

pub struct HardwareSnapshot {
    pub timestamp: SystemTime,           // 事件时间戳 (展示用;判定一律单调时钟)
    pub cpu_usage_percent: Option<f32>,  // 首采 None + Degradation，不伪造 0
    pub per_core_usage: Vec<Option<f32>>,
    pub memory_total_bytes: u64,
    pub memory_used_bytes: u64,
    pub disks: Vec<DiskUsage>,
    pub networks: Vec<NetIo>,
    pub temperatures: Vec<Temperature>,  // 空 Vec + Degradation 表示平台无数据
    pub degradations: Vec<Degradation>,
}
pub struct Temperature { pub label: String, pub celsius: Option<f32> }

pub struct ProcessSnapshot {
    pub identity: ProcessIdentity,
    pub cpu_usage_percent: Option<f32>,
    pub memory_bytes: Option<u64>,
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", content = "detail")]
pub enum Degradation {
    TemperatureUnavailable { platform: &'static str },
    TemperatureImplausible { label: String, raw: f32 },
    CpuFirstSample { note: &'static str },
    CollectorInit { source: String },
}
```

`AlertEvent`(serde 可序列化，journal 与 webhook 共用):

```rust
pub struct AlertEvent {
    pub incident: u64,        // = incident_key = hash(target_name, check_name)
    pub category: Category,   // Alert | Escalate | Resolve | Flap
    pub severity: Severity,   // Warning | Critical   (Resolved 事件带原 severity)
    pub target: String,
    pub check: String,
    pub reason: String,       // 人类可读;不参与去噪
    pub observed: Option<String>,
    pub first_seen: SystemTime,
    pub last_seen: SystemTime,
    pub occurrences: u64,
}
```

## 6. 采集层

### 6.1 sysinfo 0.40 API 锚点 (2026-11-10 实测核实)

| 用途 | API(0.40 签名) | 注意 |
|---|---|---|
| 构造 | `System::new() -> Result<System, Error>` | Err → Degradation::CollectorInit，不 panic |
| 全局 CPU | `system.global_cpu_usage() -> f32` | 旧 `cpu_usage()` 已删除 |
| 每核 CPU | `cpus().iter().map(Cpu::usage)` | 旧 `Cpu::cpu_usage` 已删除 |
| CPU 首采 | 需隔 `MINIMUM_CPU_UPDATE_INTERVAL`(Apple 平台 200ms) 两次 `refresh_cpu_usage` | 首采值无效 → `Option::None` + Degradation |
| 温度 | `Components::new_with_refreshed_list() -> Result`;`Component::temperature() -> Option<f32>` | NAN 语义已改为 Option |
| 进程刷新 | `refresh_processes(ProcessesToUpdate::Some(&pids), true) -> usize` | 见 6.2 协议 |
| 进程身份 | `Process::start_time() -> u64`(epoch 秒) | PID 复用防护的依据 |

### 6.2 部分刷新协议 (强制)

1. 每轮由 eval 提供当前**全部** watched PID 列表 (漏传者停留旧数据)。
2. 一次 `refresh_processes(ProcessesToUpdate::Some(&all_pids), true)`;返回值记入采集自监控。
3. 进程死亡判定 = 刷新后 map 中 `(pid, start_time)` 不复存在;**查不到 PID 不等于死亡确认**,需连续 `confirmations` 次。
4. 新 PID 会被插入 map(实测行为);对已消失 PID 依赖 `remove_dead_processes=true` 清理。

### 6.3 温度可信过滤

采集到的 `temperature()` 逐值过滤：**-20..150 ℃ 之外丢弃**并记 `Degradation::TemperatureImplausible`(实测 macOS 会返回 -9201 ℃ 级垃圾值);`label` 含 `tdev`/`als-temp` 等已知噪声源默认跳过。过滤只影响告警输入，原始值不保留。

### 6.4 跨平台矩阵 (修订版)

| 能力 | Linux | macOS | Windows |
|---|---|---|---|
| CPU/内存/磁盘/网络/进程 | sysinfo 完整 | sysinfo 完整 | sysinfo 完整 |
| 温度 | sysinfo components(hwmon) | **默认可用**(SMC/HID),经 6.3 过滤 | 常为空，依赖 OEM/WMI → Degradation |
| spawn 隔离 | `Command::process_group(0)` | 同左 | CREATE_SUSPENDED + Job Object(§8.6) |
| graceful stop | `killpg(SIGTERM)` | 同左 | TerminateProcess/Job 为**基线**;CTRL_BREAK 仅对显式 `console = true` 的 target 尝试 |
| 强杀 | `killpg(SIGKILL)` | 同左 | `TerminateJobObject` |
| 树清理 | 进程组天然覆盖 | 同左 | Job Object 覆盖 (KILL_ON_JOB_CLOSE 默认关) |

禁止伪造：任何平台取不到的温度/传感器一律 `None` + `Degradation`,不得填 0。

### 深度硬件采集 F21

`collect-deep-hardware` 是默认关闭的 feature，同时启用 `collect-sysinfo`。通过默认采集器入口包装基础采集，agent 与 monitorctl 的 dump-snapshot 均可使用。

- Linux 只读 `/sys/class/hwmon/hwmon*` 的 `fanN_input` 和 `inN_input`，风扇使用 RPM，电压从毫伏换算为伏。标签保留芯片和通道信息，板级分压比例需由部署方解释。其语义依据内核 hwmon ABI。[Linux hwmon 文档](https://docs.kernel.org/hwmon/sysfs-interface.html)
- macOS 只读 AppleSMC 常见风扇和电压键，按 SMC 数据类型解码，并校验响应大小、状态与数值范围。整数型电压键因单位语义未知而降级，电压采用 ±100 V 低压供电可信界，此范围属于实现选择。机型未暴露的传感器通过降级枚举报告。
- Windows 提供 unsupported 降级。关闭 feature 时 `fans`、`voltages` 为空。
- 每个传感器保留 `Option<f32>` 读数，故障、禁用、权限拒绝、无效值均报告原因。实测 0 RPM 是有效值。

### 6.5 交叉编译验收线 (两条独立)

1. 本地/CI 交叉 `cargo check --target x86_64-pc-windows-msvc`(及 aarch64/x86_64-apple-darwin)**仅默认 feature 中无 C 依赖的部分**;`reqwest`(ring) 不在本地 Windows 交叉范围 (实测缺 clang-cl 必失败)。
2. 完整 Windows 验证由 CI 的 `windows-latest` runner 承担 (原 test + check)。

## 7. 检查与告警模型

### 7.1 检查状态机 (每 check 独立)

```text
Ok ──1 次失败──▶ Warning ──连续 failure_threshold 次失败──▶ Critical
 ▲                  │                │
 └─连续 success_threshold 次成功──┴──────────────┘
(任意态)startup_grace 期内失败不计入;时间跳变/休眠唤醒后首轮探测丢弃并记 degraded
```

- Warning/Critical 进入即产生 `Alert`/`Escalate` 事件 (见 7.2);Critical → Ok 与 Warning → Ok 均产生 `Resolve`。
- **在途探测上限 1**:上轮未完成则跳过本轮 (scheduler 记 `skipped` 计数);`timeout < interval` 由配置校验保证。
- 探测一律 `tokio::time::timeout` 包裹;超时=失败，future 被丢弃，不悬挂。

### 7.2 incident 模型与去噪 (权威)

- `incident_key = hash(target_name, check_name)`——**不含 severity/reason**,同一目标同一检查永远同 key，升级不产生"第二起事故"。
- incident 生命周期：`open(Warning) → escalate(Critical) → resolve(Ok)`;事件 `category ∈ {Alert, Escalate, Resolve, Flap}`。
- 通知抑制规则：
  1. incident **首次通知永不抑制**(杜绝 Resolved 后 15 分钟内复发被冷却吞掉的盲区);
  2. 持续期间同 severity 重复通知受 `renotify_interval` 抑制;Escalate(Warning→Critical) 总是发送;
  3. `Resolve` 引用原 incident 且总是发送 (若该 incident 曾发出过通知);
  4. flap 抑制：`flap_window` 内 open/close ≥ `flap_threshold` → 进入抑制态，只发一条 `Flap` 摘要 (含计数),抑制期结束自动退出。

### 7.3 通知管道

- `#[async_trait] pub trait Notifier: Send + Sync { async fn notify(&self, event: &AlertEvent) -> Result<()>; }`(dyn 兼容)。
- 分发使用容量 256 的 AlertQueue，满时在同一临界区丢最旧并计数。每笔投递受 `alert.timeout` 限制，失败按 `retries` 指数退避重试。失败事件进入 spool，重放收到成功确认后原子移除，成功后崩溃可能重复投递。
- `WebhookNotifier`:Slack 兼容 payload 与通用 JSON 双格式;`[metrics]` 周期推送已实现 (`alert::metrics_push`,2026-11-13):每 interval POST MetricsReport JSON，失败即放弃 (下轮覆盖),连败边沿写 journal 审计;运行指标亦可经控制面读取。
- deadman(可选):周期心跳 POST 到 `deadman_url`,失败仅记日志——"agent 活着但失聪"由外部心跳监控兜底。

## 8. 恢复模型 (supervise)

### 8.1 触发契约 (统一三文档矛盾)

| 判定 | 依据 | 路径 |
|---|---|---|
| **确定性死亡** | 亲生进程退出事件 (reaper);或 attach 目标 liveness 连续 `confirmations` 次查无 `(pid,start_time)` | 直接 → Recovering(无需二次确认) |
| **健康类失败**(进程在但 probe 连续失败达阈值) | 检查状态机 Critical 且 liveness 存活 | → Suspect，连续 `confirmations` 次确认 → Recovering |
| **挂死 (Hung)** | Critical 且 liveness 存活且失败均为超时 | 同上;Recovering 优先 `hung_action` |

恢复执行期间该 target 的告警升级被抑制 (避免"刚重启就被判挂死"循环)—— 实现为：恢复执行态 (Recovering/ForceKill/Restarting/CrashLoop) 与 Starting 宽限期内，失败探测不计入状态机/去噪 (成功探测永不抑制，健康恢复必须能驱动 Starting→Running;Starting 窗口**仅按时间**计，`last_spawn + startup_grace`,2026-11-11 接线)。

### 8.2 恢复状态机 (全转移表)

```text
Running      --确定性死亡------------------------▶ Recovering
Running      --健康失败达阈值 (在)--------------▶ Suspect
Suspect      --再次确认失败----------------------▶ Recovering
Suspect      --确认恢复 (连续成功)--------------▶ Running
(实现决策 2026-11-11:原“超时未确认→Running”边不实现 —— 静默复活与恢复目的相悖;
 真挂死由后续确认继续推进，假阳性由确认恢复边闭合。见 ROADMAP 偏差表)
Recovering   --activate:钩子成功且 verify_for 内健康恢复--▶ Running
Recovering   --activate:超时/验证失败------------▶ stop → Restarting(计入 budget)
Recovering   --restart:graceful 成功-------------▶ Restarting
Recovering   --graceful 超时---------------------▶ ForceKill --成功--▶ Restarting
ForceKill    --失败 (Unix D 状态)---------------▶ Fatal
Restarting   --backoff 后 spawn 成功-------------▶ Starting(startup_grace)
Restarting   --spawn 失败 (二进制缺失/端口占用)---▶ CrashLoop(计入 budget)
Starting     --存活 > min_uptime 且健康首次成功---▶ Running
Starting     --进程退出且 uptime < min_uptime-----▶ CrashLoop(计入 budget)
Starting     --健康持续失败超过 startup_grace+2×interval --▶ Suspect
CrashLoop    --backoff 重试，窗口内计数未满--------▶ Restarting(指数退避)
CrashLoop    --restart_budget 窗口计数耗尽--------▶ Fatal + Critical 告警
Fatal        --仅 monitorctl reset(审计)---------▶ Running(重新计数)
(任意态)     --agent 重启------------------------▶ 从 journal 重放;孤儿对账 (§8.7)
```

- backoff:指数 `backoff_initial × 2^n`,封顶 `backoff_max`,每次 spawn 尝试递增，Running 持续 `min_uptime` 后重置。
- **启动编排 (F20)**:`depends_on` 中的依赖全部满足 (FSM Running **且确有运行中进程**) 后服务才被冷启动拉起;依赖 Fatal → 依赖方永不自动启动 (失败不级联，人工介入);reload 新增有依赖服务同样走门控。
- **停止顺序 (F20)**:控制面 restart X 时先停 X 的 (传递) 被依赖方 (逆拓扑序，叶子先停，journal 审计);X 回 Running 后按登记**级联拉回**(Fatal 闩锁的被依赖方跳过)。
- `restart_budget`:滑动窗口 ring buffer 记录重启时间戳，**所有自动重启 (含 spawn 失败重试) 均计数**;Fatal 是终态闩锁，窗口滑过**不**自动恢复。

### 8.3 进程身份与 PID 复用防护 (强制)

一切存活判定、停止、接管动作前必须校验 `(pid, start_time)`(sysinfo `Process::start_time()`,epoch 秒);不匹配视为 PID 已复用，原身份判定死亡。**裸 PID 永远不作为杀进程依据。**

### 8.4 父子契约 (parenting)

- supervise = agent 亲生:Unix `process_group(0)`(等价 setpgid,killpg 足够;需要会话隔离才 `pre_exec(setsid)`,unsafe 显式块);每子进程一个 reaper 任务 (`Child::wait`) 把退出事件回传 eval，防僵尸。
- attach = 收养：仅监控告警，无杀/重启能力;Windows 下可 `OpenProcess+WaitForSingleObject` 等待非亲生进程 (Unix 做不到，不对称如实记录)。
- **禁止 daemonize 目标**(双 fork 会让 agent 的 Child 立即退出 → 误判死亡 → 重复拉起);文档与配置校验均声明。

### 8.5 HungAction

- `Restart`:走 stop→spawn 全流程。
- `Activate`:执行激活钩子 (命令或 HTTP),自身带 `timeout`;成功后 `verify_for` 观察期内健康恢复 → Running;否则转 Restart 并计入 budget。

### 8.6 Windows 实现

- spawn 后立即 AssignProcessToJobObject。tokio Child 未提供主线程句柄，当前存在 spawn 到 assign 之间子孙逃逸的竞态窗口。
- Job 默认**不带** `KILL_ON_JOB_CLOSE`(agent 死不连带杀服务，守住原则 1);"agent 死则服务死"不做为选项提供 (v0.1)。
- 停止：基线 = `TerminateJobObject`/`TerminateProcess` + 审计日志;`console = true` 的 target 可尝试 `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT)`(要求 CREATE_NEW_PROCESS_GROUP，收到与否不强求，超时照常升级)。
- **SCM 目标监控 (F15 扩展，2026-11-13)**:`[[service]] windows_service = "<服务名>"`(与 command 互斥;feature `scm-target`)—— SCM QueryStatus 作为 liveness 证据，启停经 SCM API(幂等 start/stop;force_kill 如实降级为再次 Stop);目标无 (pid,start_time) 身份链，FSM 走 `on_scm_spawn_ok`;platform 差异经 `ScmOps` trait 抽象 (测试可注入 Fake)。
- **服务形态 (F15,2026-11-12)**:`--features windows-service` 构建 + `--service-run` 入口 —— SCM 感知 (`service_dispatcher` + control handler),`sc stop` 经 `runtime::request_shutdown()` 走与信号完全相同的优雅停机序列 (Clean);标准构建用于控制台运行，SCM 原生服务部署需要启用该 feature。服务模式日志写文件 (`--log-file`)。受控停止报告 `Stopped` 与成功码，eval 或持久化故障返回运行错误。安装脚本配置 failure actions 及 `failureflag 1`，覆盖非崩溃错误退出。F15 的范围是 agent 自身 SCM 集成。原生 Windows Service 目标的停止与恢复接口另列 roadmap。

### 8.7 状态恢复与孤儿对账 (agent 重启)

启动时重放 Fatal、预算和进程身份。Fatal 目标保持闩锁，已有候选进程仍参与只读对账。快照必须证明该身份在本轮 watched 查询中，且单调采集时间晚于本次启动。精确匹配转只读 attach，权限不足或采集失败继续等待。确认旧身份已消失后解除候选并恢复 supervise，仍遵守 Fatal 与依赖门控。

接管期间禁止 stop、force-kill、activate 和控制面 restart，状态以 attach 呈现。reload 删除、重定义和同名重加保留候选身份，防止重复启动。旧版 journal 无进程身份记录时无法自动发现历史遗留实例。OS spawn 成功到首次身份持久化确认之间的故障窗口仍需后续启动事务协议覆盖。

关键 Restart、Fatal、Reset 和进程身份记录通过有界 journal 通道发送，等待 `sync_data` 确认，2 秒超时或写入错误触发当前实例的 PersistenceFailed 停机。普通审计保持非阻塞并累计丢弃指标。启动先校验配置、预绑定控制端口、重放并确认 journal 可写，再启动调度。损坏尾行单独隔离，后续成功确认的记录保持可重放。

## 9. CLI、控制平面与退出码

### 9.1 monitor-agent

| 命令 | 说明 |
|---|---|
| `monitor-agent [--config <path>] [--log-json]` | 主循环 (默认读 `./service-watch.toml`;**顶层 flag 形态**,无 `run` 子命令) |
| `monitor-agent --dump-snapshot [--config <path>]` | 采集一次并输出 JSON 后退出 |
| `monitor-agent --check-config <path>` | 离线校验配置 |
| `monitor-agent --version` | 版本 |

### 9.2 monitorctl 与控制平面 API

| 子命令 | HTTP | 说明 |
|---|---|---|
| `monitorctl status [--json]` | `GET /status` | 全 target 状态/incident 摘要 (`--json` 原样输出 JSON) |
| `monitorctl restart <target>` | `POST /v1/restart/{target}` | Fatal 态被拒 (409),提示先 reset |
| `monitorctl check <target>` | `POST /v1/check/{target}` | 绑定检查立即到期 (下一轮调度立即执行) |
| `monitorctl reset <target>` | `POST /v1/reset/{target}` | 解除 Fatal + 清 budget(写审计) |
| `monitorctl metrics [--prom]` | `GET /metrics` | 运行指标 (默认 JSON;`--prom` 走 `?format=prometheus`) |
| `monitorctl dump-snapshot [--config <path>]` | (本地，不经 agent) | 单次采集 JSON(`--config` 仅做加载校验) |

Agent 端点：`GET /health`(浅存活)、`GET /ready`(配置加载成功 + 调度循环启动;**降级采集不影响 ready**)、`GET /status`、`GET /metrics`(默认 JSON;`?format=prometheus` 或 `Accept: text/plain` 返回 Prometheus 文本，指标前缀 `service_watch_`)、**`GET /metrics/history?range=<secs>`**(F18:缺省 300、上限 7200 ≈ 2h@1s tick、非法 400;>600 点等距降采样 ≤600 保留首尾;历史环形缓冲容量 7200，经 `MetricsReport.history`(serde skip 句柄) 搭载既有 metrics watch 分发，零 runtime 装配改动)、**`GET /`**(F22 只读状态页：内嵌 HTML,JS 轮询 /status+/metrics,health 徽章/恢复状态/Fatal 高亮，全部插值 escapeHtml，无写操作)。`POST /v1/restart/{target}` 对 Fatal 目标返回 **409**(经 status watch 判定;2026-11-11 接线)。`POST /v1/reload`(F14):body 可选 `{"path": "..."}`,缺省用启动路径;202 入队 (eval 单写者内应用)/ 400 非法 body / 503 通道满。**热重载范围 = 目标拓扑**(checks/services/watches 增删改，name 匹配;check 参数变更保留状态机水位;service 重定义 = 删 + 增 重启生效;解析/校验失败整体拒绝，旧配置原样生效);agent/log/control/collect/alert/metrics 段变更**要求重启**(记 journal 审计)。Unix 另支持 **SIGHUP** 触发 (Windows 仅端点)。

### 9.3 退出码

| 码 | 含义 | 码 | 含义 |
|---|---|---|---|
| 0 | 成功 | 3 | 目标不存在 |
| 1 | 配置错误 | 4 | agent 不可达 |
| 2 | 运行错误 | 130 | SIGINT/SIGTERM 退出 |

## 10. 可观测性

- tracing-subscriber:`RUST_LOG`(EnvFilter，默认 `info`)+ `--log-json`。
- 结构化字段约定：`target` / `check` / `incident`(u64)/ `degraded`(Degradation kind)/ `pid` / `attempt`。
- 内部计数 (`/metrics` 暴露，eval 每轮发布):告警管道 `alerts_sent` / `alert_delivery_failures` / `alerts_dropped`;探测 `probe_executed` / `probe_skipped`;恢复 `restarts_total` / `state_transitions_total` / `fatal_targets` + 恢复状态分布 gauge(`recovery_running` … `recovery_fatal`);采集 `degradations_current`(瞬时)/ `degradations_events`(无→有跳变)/ `last_collect_at`;概览 `targets_total` / `checks_{ok,warning,critical}` / `services_{total,running}` / `uptime_secs`。
- 日志纪律：恢复/告警/降级三类事件必须 `INFO`+ 可追溯 (含 target 与原因);禁止在热路径 `DEBUG` 刷屏。

## 11. 测试策略

1. **确定性单测**:检查/恢复状态机、incident 去噪、budget/backoff 全部以**时间参数注入**驱动 (调用方传单调 `Instant`,状态机自身从不读时钟)—— 等效于 Clock 注入且无 trait 开销;调度层的 tokio 时间在测试中可 `start_paused`。(实现偏差：无独立 Clock trait，见 ROADMAP 偏差表。)
2. **边界 mock**:Collector/Notifier/ProcessHandle 为 trait，测试用假实现 (sysinfo/网络/进程零依赖)。
3. **集成**:`tests/http_probe.rs`(axum stub:慢响应→Warning→Critical→Resolve 全链路 + 状态区间闭边界);`tests/supervise_smoke.rs`(Unix 真进程：冷启动身份、kill -9 拉活、crash loop→Fatal;Windows 由 `cfg(unix)` 排除)。
4. **CI 矩阵**:三平台 fmt + clippy(-D warnings)+ test;Windows/macOS 交叉 check 按 §6.5 两条线。
5. 覆盖硬指标:7.1/7.2/8.2 的每条转移边至少一条单测。

## 12. 关键取舍记录

| 取舍 | 选择 | 理由 |
|---|---|---|
| Notifier 并发形态 | `#[async_trait]` + dyn fan-out | AFIT 非 dyn 兼容;全项目统一，EXECUTION 旧签名作废 |
| 复用 shep/Malkuth | 不复用，只借鉴策略 | shep 不支持 Windows;需求要求统一 lib |
| 温度不可用填 0 | 禁止，`Option` + Degradation | 填 0 会被阈值规则误判 |
| Windows graceful 主路径 | TerminateProcess 基线 + Job Object | CTRL_BREAK 对无 console 服务进程无效 (实测结论) |
| 监控 agent 自身守护 | 外部 service manager + /health + deadman | 不做双向重启依赖 |
| rust-version | 禁写 (根约束) | 下游兼容性由 README 声明实测 MSRV + CI 最低工具链 job 兜底 |
