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
collect-deep-hardware = []
http-client     = ["dep:reqwest", "dep:hmac", "dep:sha2"]
probe-http      = ["http-client"]
notify-webhook  = ["http-client"]
control-http    = ["dep:axum", "dep:axum-server", "dep:rustls-pemfile", "dep:rustls"]

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
journal_max_size = 10485760             # journal 轮转阈值 (字节;默认 10 MiB;0 = 禁用轮转;只保留一代 journal.1.jsonl,F24)

[log]
json = false                            # 结构化 JSON 输出

[control]                               # 控制平面 (feature control-http)
enabled = true
listen = "127.0.0.1:9170"               # 仅回环;绑定非回环需显式确认
# token = "change-me"                   # 可选 Bearer 令牌 (F23;除 /health 外全部端点)
# tls_cert = "/etc/sw/tls.crt"          # 可选 HTTPS (F32;成对)
# tls_key = "/etc/sw/tls.key"
# client_ca_cert = "/etc/sw/ca.crt"     # 可选 mTLS (F38;必须与 tls 成对)

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
# webhook_secret = "change-me"          # 可选 HMAC 签名密钥 (F29);缺省 (不写) = 不签名;
# headers = { Authorization = "Bearer x" }  # 可选自定义投递头 (F37;token 名/可见 ASCII 值)
#                                       #   显式空白值被校验拒绝 (与 webhook_url 的 ""→None 语义不同)

# ---- 按目标告警路由 (F26，可多个) ----
# 顺序首匹配 target 名称前缀;`*` 通配全部;无匹配回落上方 webhook_url(默认 = 兜底)。
# [[alert.routes]]
# match = "edge"                         # 前缀或 * 通配 (非空白)
# url = "https://hooks.example.com/edge" # http/https
# format = "slack"                       # json | slack(缺省 json)
# secret = "edge-only"                   # 路由级签名密钥 (F29),缺省回落 webhook_secret

# ---- 定期维护窗口 (F31，可多个) ----
# 全部时刻按 UTC 计算 (不支持 DST);窗口内匹配 target 的 Alert/Escalate/Flap
# 通知被抑制，Resolve 与 Fatal 合成告警恒穿透 (§7.3)。跨 midnight 合法。
# [[maintenance]]
# name = "nightly"                       # 可选;缺省 = 审计报 targets
# targets = ["*"]                        # target 前缀或 * 通配 (同 [[alert.routes]]);非空
# start_utc = "18:30"                    # 窗口开始，UTC "HH:MM"(两位，HH<24/MM<60)
# duration = "30m"                       # ≥ 60s
# days = ["mon", "wed", "fri"]           # 可选;缺省 = 每天 (mon tue wed thu fri sat sun)

[metrics]                               # 周期推送 (2026-11-13 已实现):POST MetricsReport
enabled = false                         #   JSON;失败即放弃 (下轮覆盖);连败边沿写 journal 审计
url = ""                                # 空 = 仅 /metrics 端点暴露
interval = "60s"
timeout = "10s"                         # 单次推送超时;timeout < interval 校验
# secret = "change-me"                  # 可选推送签名 (F34)
# history_path = "./data/metrics.jsonl" # 可选历史持久化 (F39;10 MiB 单代轮转)

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
| agent.journal_max_size | 10 MiB (0 = 禁用轮转) | alert.failure_threshold | 3 |
| alert.timeout | 10s | alert.success_threshold | 2 |
| alert.retries | 3 | alert.renotify_interval | 15m |
| alert.flap_window | 30m | alert.flap_threshold | 5 |
| restart.min_uptime | 10s | restart.backoff_initial | 1s |
| restart.backoff_max | 60s | restart.max_restarts | 5 |
| restart.restart_window | 10m | restart.stop_grace | 15s |
| restart.startup_grace | 30s | restart.confirmations | 2 |
| activate.timeout | 10s | activate.verify_for | 30s |
| control.listen | 127.0.0.1:9170 | metrics.interval | 60s |
| maintenance.duration | 1h (校验下限 60s,F31) | maintenance.days | 缺省 = 每天 (F31) |

### 4.3 校验规则 (违反即配置错误，退出码 1)

1. 每个 check 的 `timeout < interval`，check、collect 与 `[metrics]`(enabled 时) 间隔均至少 100ms。
2. http check:`url` 合法且 scheme 为 http/https;`expected_status` 形如 `"200-299"`。探测默认**跟随重定向** (reqwest ≤10 跳):302 链最终落在区间内判 OK —— 需要严格判定时把 `url` 指向最终端点 (webhook 投递侧相反,禁重定向防 POST 降级 GET)。
3. process check 的 `process` 非空，cpu/mem 阈值至少一个，CPU 阈值必须有限且非负。
4. supervise:`command` 非空;`backoff_initial ≤ backoff_max`;`startup_grace ≥ min_uptime`;check 名称引用存在。
5. `hung_action = "activate"` 时必须配置 `[service.activate]`，钩子的 command/url 必须且只能配置一个，command 非空，URL 使用 http/https。
6. check/service/watch 各自名称唯一且非空，`data_dir` 非空并可创建，告警阈值至少 1，webhook/deadman URL 使用 http/https。缺省 agent 与空 `[agent]` 均使用 `./data`。`[[alert.routes]]` 的 `match` 非空白、`url` scheme 为 http/https (F26)。`webhook_secret` 与路由级 `secret` 不得为纯空白 (F29)。
8. F37 头名 RFC 7230 token、值仅可见 ASCII+TAB 且非空 (与 http crate 字节规则对齐);表内/跨表仅大小写不同的头名拒绝;F34 `[metrics] secret` 非空白;F38 `client_ca_cert` 必须与 `tls_cert/tls_key` 同配;F39 `history_path` 非空且不得与 journal/spool 系统文件相同;`alert.retries ≤ 100`;`[[watch]] pidfile` 非空路径。
7. `[[maintenance]]`(F31):`targets` 非空 (显式 `["*"]` 表示全部)、`start_utc` 为两位 `HH:MM`(HH < 24、MM < 60)、`duration` ≥ 60s、`days` 仅 `mon tue wed thu fri sat sun`(大小写不敏感);name 可空 (审计报 targets)。

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

#### 7.2.1 跨重启恢复 (F25)

journal 重放维护 open incident 集合 (Alert/Escalate 开门并刷新 severity/reason/last_event_at,Resolve 关门 (无配对幂等丢弃 + `orphan_resolves` 计数),Flap 摘要不动门;recover 域的 Reset/ResetTargets **不清**真实 check 的告警条目 —— 告警域与恢复预算域独立;**唯一例外**:Fatal 的合成 incident `(target, RECOVER_CHECK)` 属恢复域，随 Reset/ResetTargets 一并闭门，否则 reset 后每次重启都会重放虚假 "latched fatal" 重通知 —— 第九轮验证 P2,eval 侧 restore 另有 fatal 集合防御层)。启动时 (eval 冷启动后、调度开始前) 把重放出的仍 open 的 incident 恢复进引擎，并保证运维感知：

- **恢复重通知**:每起恢复的 open incident 产出一条通知，走既有 AlertQueue 链路，**类别复用 `Alert`(不新增枚举)**、severity 用恢复值、`reason = "restored from journal: <原 reason>"`;同时写 journal Incident 事件 (与运行期事件写路径一致，journal 门随刷新)。
- **新通知窗口起点**:trail 以「已通知」状态注入，`last_notified` = 恢复时刻 —— 重通知即新 renotify 窗口起点，延续「首条通知永不冷却」:恢复后必然至少可再告警一次。
- **flap 计数器不恢复**(取舍，偏差表已记):transitions 环与抑制集保持空 —— 新会话 fresh 窗口，旧抖动史不误抑制新告警;代价是旧抖动若持续到重启后，需要重新累计达到阈值才会再次抑制。
- **检查状态机不回填**(取舍):`/status` 的 incident 字段立即反映恢复 incident(引擎查询),但 health 与 `/metrics` 的 `checks_ok|warning|critical` 来自检查状态机，fresh 起步、随 live 探测在数个 interval 内自然再升级 (Warning 一轮、Critical `failure_threshold` 轮)。回填状态机会让启动首轮失败探测直接喂 Critical 给恢复 FSM，破坏「健康类失败需连续 `confirmations` 次确认」的保守契约 (§8.1),故不做。
- **key 复算口径**:恢复使用 `incident_key(target, check)` 运行期复算，不信任 journal 持久化值;occurrences 不可知，从 1 重新累计。
- 空重放表零行为变化 (向后兼容)。

### 7.3 通知管道

- `#[async_trait] pub trait Notifier: Send + Sync { async fn notify(&self, event: &AlertEvent) -> Result<()>; }`(dyn 兼容)。
- 分发使用容量 256 的 AlertQueue，满时在同一临界区丢最旧并计数。每笔投递受 `alert.timeout` 限制，失败按 `retries` 指数退避重试。失败事件进入 spool，重放收到成功确认后原子移除，成功后崩溃可能重复投递。
- **按目标路由 (F26,`RoutingNotifier`)**:`[[alert.routes]]` 非空时装配路由通知器 (默认通道 = `webhook_url` 作兜底)。匹配按**配置顺序首条生效**:pattern `*` 通配全部 target，否则 target 前缀匹配 (`starts_with`);无匹配走默认通道，空 routes 行为与现状完全一致。路由命中与否不影响投递语义 (超时/退避/队列/spool 由分发层统一处理);route 的 webhook 构建失败该路由跳过并回落默认。**spool 重放走分发任务持有的同一 notifier 实例 → 历史积压按当前启动配置重新路由**。`webhook_url` 为空且无匹配 = 事件不外投 (仍入 journal/日志),与现状「空 = 不投递」口径一致。
- `WebhookNotifier`:Slack 兼容 payload 与通用 JSON 双格式;`[metrics]` 周期推送已实现 (`alert::metrics_push`,2026-11-13):每 interval POST MetricsReport JSON，失败即放弃 (下轮覆盖),连败边沿写 journal 审计;运行指标亦可经控制面读取。**F34 推送签名**:`[metrics] secret` 配置后每次推送附验签头 (见下方「指标推送 HMAC 签名」)。
- **目标级静默 (F27,eval 侧实现)**:**范围注记 (第十六轮)** —— 抑制点在 eval 的 emit_alert 入队点;**spool 存量事件的重放**(投递失败落盘的历史) 不走该管线，不受后设的静默/维护窗口约束 (交付后仍会送达接收端)——运维在静默前应清空或接受存量重放;RUNBOOK §3.4 同口径。
- **目标级静默 (F27,eval 侧实现)**:`POST /v1/silence/{target}`(`{"duration": <秒>, "reason": "<可选>"}`)/ `DELETE` 提前解除 (§9.2)。语义与边界：
  - **只作用于通知管道**:窗口内该 target 的 `category ∈ {Alert, Escalate, Flap}` 事件**不入 AlertQueue**(通知不投递);**`Resolve` 永不静默** —— 恢复通知必须穿透;**Fatal 合成告警 (`check = RECOVER_CHECK`) 同等豁免**(P1 第十轮验证：它不经 tracker，无 trail/renotify 兜底，被吞后解除静默也不补发，「需人工介入」信号会永久丢失)。journal Incident 事件照常写 (历史完整性),检查状态机/恢复 FSM/renotify 窗口推进完全不受影响 —— 与 F20/Fatal 的**恢复行为**无任何耦合 (Fatal 照常闩锁，解除静默不解除 Fatal;豁免的只是 Fatal 闩锁那条**通知**)。
  - **披露 (取舍一致性)**:解除静默后，进行中 incident 的下一通知最多延迟一个 `renotify_interval`(默认 15m —— 抑制期间 tracker 照常推进，被吞通知已刷新 last_notified);静默期内打开并恢复的 incident 会先收到一条此前没有对应 Alert 的 Resolve(Alert 被吞而 Resolve 穿透 —— 与 flap「未通知不发 Resolve」口径不同，运维侧按"恢复即真相"处理)。
  - **deadline 用单调时钟** (`Instant + duration`),与墙钟回拨/跳变无关;到点由 eval 周期 tick 惰性清理 + 写一条到期 Audit，过期条目不残留;`emit_alert` 侧对 `until <= now` 的残留条目本就放行 (双保险)。静默表为 eval **内存态** —— journal 重放不恢复静默 (agent 重启后窗口失效，宁多响不漏报;运维需静默跨重启时应在重启后重设)。
  - **重复 Silence 后到覆盖** (新窗口整体替换);Unsilence 移除;`duration = 0` 或超 604800 (7 天)、未知 target 由 eval 侧拒绝 (202 入队 + WARN + journal 审计)。
  - **审计边沿**:建立/解除/过期各写一条 journal Audit (建立含 reason 与「单调 deadline 自动过期」语义);静默期首次抑制命中写一条可见性 Audit (`notifications silenced for target X until ...`),期内不再逐条记录 (防刷屏)。
- **定期维护窗口 (F31,eval 侧实现)**:`[[maintenance]]` —— `{name?, targets, start_utc = "HH:MM", duration, days?}`,`duration` ≥ 60s,`days` 缺省全周，`targets` 前缀匹配同 `[[alert.routes]]`(`*` = 全部)。语义与边界：
  - **只作用于通知管道，与 F27 静默叠加**:窗口内匹配 target 的 `category ∈ {Alert, Escalate, Flap}` 事件**不入 AlertQueue** —— 两个机制**无优先级，任一命中即抑制**(F27 用单调 deadline、F31 用 UTC 墙钟，时间语义各自独立);Audit 边沿各自独立写。**`Resolve` 与 Fatal 合成告警 (`check = RECOVER_CHECK`) 恒穿透** —— F27 的豁免对维护窗口同等生效。journal Incident 照常写 (历史完整性);检查状态机/恢复 FSM/renotify 推进完全不受影响。
  - **全部时刻按 UTC 计算**:std 无时区库，如实限定 UTC(运维把本地时刻换算成 UTC 后配置);**不支持 DST** —— 固定 UTC 偏移下语义确定，DST 切换仅表现为窗口相对本地钟平移 1h。跨 midnight 窗口 (start + duration 越日界) 按「**开启时刻起的 elapsed**」判定：候选开启日 `d`(星期命中) 的窗口区间为 `[d×86400+start, d×86400+start+duration)`,当前时刻落入任一区间即在窗口内 —— 次日零点后的部分天然归属**开启日**的星期 (days 过滤无歧义，无需两段区间法的额外约定),对任意 duration(含 ≥ 24h 的多日窗口) 统一精确。
  - **无持久状态，自然重入**:判定为纯函数注入 epoch 秒 (时间注入纪律，不在判定内读钟),每 tick(MAX_TICK=1s 粒度，配置校验 duration ≥ 60s 保证边沿不丢) 重算;agent 重启后按 UTC 时钟自动恢复生效 —— 与 F27 静默「重启即失忆」刻意不同：维护窗口由**配置**定义，属于长期意图而非临时操作。**如实限定**(P3 第十一轮验证):判定吃 UTC 墙钟 —— NTP 正向大步进可让一个完整窗口在两次 tick 之间被跳过 (开/关 Audit 成对丢失，抑制判定与当时钟自洽);回拨则同一窗口可能出现重复边沿对;进程级长停顿 (≥60s) 同理。无 panic/误抑制路径,仅审计边沿的墙钟自洽性受限。
  - **审计边沿**:窗口开启/关闭各写**恰好一条** journal Audit(每 tick 边沿判定 + per-window 状态位;有 name 报 name，无 name 报 targets;agent 启动即处于窗口内时首轮 tick 写一条 opened)。抑制命中**不逐条**写 Audit —— 开启 Audit 即可见性边沿，长窗口内不刷屏 (与 F27「首次抑制命中一条可见性 Audit」的粒度差异如实记录:F27 静默无开启审计，需要命中边沿;F31 有开启审计，命中边沿冗余)。
  - **热应用范围**:维护窗口表为构建时快照 (`cfg.maintenance`),不在拓扑热重载范围 —— 变更要求重启 (与 alert 段同口径，reload diff 的「要求重启」审计照常报)。
- **webhook HMAC 签名 (F29，可选)**:`webhook_secret`(全局) 或路由级 `secret` 配置后，每次投递附 `X-Service-Watch-Signature: sha256=<hex hmac-sha256(body)>` 头。语义与边界：
  - **签名对象 = 实际发送的 body 字节**:notify 内先 `serde_json::to_vec` 出最终字节再签名发送 (slack/json 两格式与 spool 重放同走 notify 路径，天然覆盖);接收方按收到的原始 body 重算 HMAC 即可验签。
  - **覆盖规则**:每条 route 用 `route.secret.or(webhook_secret)`(路由级覆盖全局),默认通道用 `webhook_secret`;两者皆 None 不签名 (裸投递，行为与 v0.4 一致，默认向后兼容)。
  - **feature 门控**:签名实现随 `http-client` feature 门控 (F34 下沉 `alert::signature` 共享层，webhook 与指标推送复用同一实现;hmac/sha2 纯 Rust 无 C 依赖，不破坏 §6.5 交叉编译线) —— webhook 本身仍随 `notify-webhook` 装配，产品装配在 feature 关闭时本就不构建 webhook，无独立降级路径。
  - **密钥纪律**:密钥值与签名值都不进任何 tracing 字段/审计;装配日志只报 `signed = true/false` 布尔。
  - **接收方验签提示**:应使用**恒定时间比较**(`hmac.verify()` 或等价),不要用普通 `==` 逐字节短路比较;见 RUNBOOK §3.4 示例。
- **指标推送 HMAC 签名 (F34，可选)**:`[metrics] secret` 配置后，每次推送附 `X-Service-Watch-Signature: sha256=<hex hmac-sha256(body)>` 头。与 F29 同口径，实现复用 [`alert::signature`] 共享 helper:
  - **签名对象 = 实际发送的 body 字节**:先 `serde_json::to_vec` 出最终字节再签名发送 (与 F29 同款口径;等价于 reqwest `.json()` 内部的序列化，行为一致)。
  - **`None` 不签名**(行为与未签名版本一致，默认向后兼容);密钥值与签名值都不进任何 tracing 字段/审计，装配日志只报 `signed = true/false` 布尔。
  - **feature 门控差异**:推送签名随 `http-client` feature(而非 webhook 的 `notify-webhook`)—— metrics_push 模块本身就在 http-client 门控内，签名 helper 与消费方同门控，`--features ...,http-client`(不含 notify-webhook) 组合下推送签名照常可用。
  - **序列化失败诚实失败**:`serde_json::to_vec` 失败按连败计数走既有失败路径 (不 panic、不跳过计次;理论路径，MetricsReport 序列化实际不触发)。
- **webhook 自定义投递头 (F37，可选)**:`[alert] headers`(全局 BTreeMap)+ 路由级 `headers`，接收方 auth token / Grafana、Signoz 类集成的通用注入口。语义与边界：
  - **合并语义 = 路由级同名键覆盖全局**：默认通道用全局表，每条 route 用「全局为底 + route.headers 同名键覆盖、非同名键共存」的合并表 (`runtime::effective_route_headers` 纯函数，BTreeMap 插入即覆盖)；notify 时**全部**叠加到请求 (slack/json 两格式与 spool 重放同走 notify 路径，天然全覆盖)。
  - **与签名头 (F29) 独立共存**：自定义头叠加在签名头之外，签名头**总是**附加、不因同名自定义头而丢弃；若自定义了保留头名 `X-Service-Watch-Signature`，reqwest 侧 append 会产生两个同名头 (多数接收端取第一个或拒绝) —— **不要自定义保留头** (doc 级约定，不代码强拒)。
  - **注入面校验 (config 层权威)**：头名必须为非空 RFC 7230 token、值非空且不含 CR/LF/控制字符，违反即配置错误 (退出码 1)；notifier 构造期另做一次 `HeaderName`/`HeaderValue` 转换兜底库直构 —— HTTP 非法项 WARN 丢弃 (日志只含头名；空值是合法 HTTP 头值，notifier 层不拦，空值拒绝属 config 策略)。
  - **密钥纪律**：头值可能是敏感 token —— 只进请求头，不进任何 tracing 字段/审计；构建失败日志只有 url/错误 (与 F29 同口径)，装配日志只报 headers 数量。
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

**体积上限与单代轮转 (F24)**:`agent.journal_max_size`(字节，默认 10 MiB,`0` = 完全禁用轮转) 是**软性**运维目标，不是安全边界——写事件后实测当前文件尺寸，超限即轮转：当前 `journal.jsonl` 原子改名 `journal.1.jsonl`(**只保留一代**,二次轮转覆盖;Windows 下必须先释放句柄再 rename),立即重建当前文件继续写，并向**新文件**写一条轮转 Audit(含轮转前后字节数)。单行超出阈值的一行溢出可接受;改名失败降级为保留当前文件继续写 + WARN(不停机、不丢事件)。重放顺序 `journal.1 → journal.jsonl` 依次 apply 到同一状态，Fatal 闩锁/预算窗口/未决 SpawnIntent 跨轮转保持;`max_size = 0` 时 journal.1 永不产生，重放逻辑天然兼容。阈值应显著大于单行长度，否则退化为逐事件轮转 (行为正确但历史只存活一代)。

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
| `monitorctl silence <target> --duration <s> [--reason <text>]` | `POST /v1/silence/{target}` | F27 目标级静默：窗口内 Alert/Escalate/Flap 不投递 (202 入队;时长 0/超 7d 与未知 target 由 eval 侧拒绝并审计) |
| `monitorctl unsilence <target>` | `DELETE /v1/silence/{target}` | F27 提前解除静默 (202 入队;无活动静默由 eval 侧拒绝并审计) |
| `monitorctl journal [--tail <n>] [--target <name>] [--type <type>] [--since <epoch s>] [--json]` | `GET /v1/journal` | F28 journal 尾部查询：默认表格 (时间/target/type/摘要),`--json` JSON Lines;`truncated` 时 stderr 提示 |
| `monitorctl metrics [--prom]` | `GET /metrics` | 运行指标 (默认 JSON;`--prom` 走 `?format=prometheus`) |
| `monitorctl config [--json]` | `GET /v1/config` | F33 生效配置查看：默认表格 (顶层段键值 + maintenance/routes 等列表逐行),`--json` 原样 pretty JSON;`***redacted***` 占位 (token/签名密钥/投递头值) 原样展示并注脚说明 —— **不可回灌配置** |
| `monitorctl dump-snapshot [--config <path>]` | (本地，不经 agent) | 单次采集 JSON(`--config` 仅做加载校验) |

Agent 端点：`GET /health`(浅存活)、`GET /ready`(配置加载成功 + 调度循环启动;**降级采集不影响 ready**)、`GET /status`、`GET /metrics`(默认 JSON;`?format=prometheus` 或 `Accept: text/plain` 返回 Prometheus 文本，指标前缀 `service_watch_`)、**`GET /metrics/history?range=<secs>`**(F18:缺省 300、上限 7200 ≈ 2h@1s tick、非法 400;>600 点等距降采样 ≤600 保留首尾;历史环形缓冲容量 7200，经 `MetricsReport.history`(serde skip 句柄) 搭载既有 metrics watch 分发，零 runtime 装配改动)、**`GET /`**(F22 只读状态页：内嵌 HTML,JS 轮询 /status+/metrics,health 徽章/恢复状态/Fatal 高亮，全部插值 escapeHtml，无写操作;F30 另拉 `/metrics/history?range=300` 绘 `checks_critical` 与 `recovery_fatal` 零依赖 SVG 迷你折线 —— 历史为空/接口失败不渲染且不影响既有渲染，折线坐标全部由防护后的数字计算)。`POST /v1/restart/{target}` 对 Fatal 目标返回 **409**(经 status watch 判定;2026-11-11 接线)。`POST /v1/reload`(F14):body 可选 `{"path": "..."}`,缺省用启动路径;202 入队 (eval 单写者内应用)/ 400 非法 body / 503 通道满。**热重载范围 = 目标拓扑**(checks/services/watches 增删改，name 匹配;check 参数变更保留状态机水位;service 重定义 = 删 + 增 重启生效;解析/校验失败整体拒绝，旧配置原样生效);agent/log/control/collect/alert/metrics 段变更**要求重启**(记 journal 审计)。Unix 另支持 **SIGHUP** 触发 (Windows 仅端点)。`POST|DELETE /v1/silence/{target}`(F27 目标级静默，语义见 §7.3):POST body JSON `{"duration": <秒,u64>, "reason": "<可选>"}`,必填缺 duration/非 JSON/负数 → 400;202 入队;`DELETE` 提前解除。时长合法性 (0 与超 604800) 与 target 存在性由 eval 侧拒绝 (202 入队 + WARN + journal 审计，与 restart/check 的入队后校验模式一致),重复 Silence 后到覆盖。`GET /v1/journal`(F28 journal 尾部查询):`?tail=<n>`(缺省 200，上限 1000，非法 400)`&target=<name>`(精确匹配)`&type=<事件类型>`(serde tag)`&since=<epoch 秒>`(含);尾部有界扫描 (`spawn_blocking`,坏行跳过),响应 `{"truncated": bool, "lines": [<journal JSON 对象>]}`。**F35:轮转上一代 `journal.1.jsonl` 存在时一并尾读** —— gen1 尾部在前、当前文件尾部在后 (追加写 = 时间顺序，合并后自然按时间排列),统一过滤/截断;每文件读入上限 512KB (合并至多 1MB),`truncated` = 任一文件被字节上限截断或合并命中总数 > tail;仍不含更早代 (F24 只保留一代，深查历史走 jq);单文件读失败只放弃该文件 (WARN),不毒化另一代。`GET /v1/config`(F33 生效配置查看):返回 `Config::redacted()` JSON **合并视图** —— token / webhook 签名密钥 (全局与路由级) 与投递头值 (F37,凭据常见注入口) 以 `***redacted***` 占位，**明文不出 agent**,其余字段 (含 TLS 路径) 原样;视图反映当前进程**持有并生效**的配置：**拓扑段 (check/service/watch) 随 reload 成功刷新**(eval 经 `config_view_tx` watch 推送，第十一轮偏差修复),**要求重启的段 (agent/log/control/collect/alert/metrics/maintenance) 恒为启动值**(这些段不热应用 —— 展示未生效的文件值会误导运维);占位值不可回灌 (写回配置会把字面量当作真实密钥);protected 路由，F23 鉴权生效。

#### 9.2.1 可选 Bearer 访问令牌 (F23)

`[control] token`(可选，`Option<String>`;空白值配置错误) 配置后，除 `GET /health` 外**全部端点**(含状态页 `GET /`、/ready、/status、/metrics*、/v1/*) 要求 `Authorization: Bearer <token>`;缺失/格式错误/值不匹配一律 **401**(统一 JSON 说明体 + `WWW-Authenticate: Bearer`,**不区分失败原因** —— 不泄漏「头缺失还是值错误」)。

- **豁免清单**:仅 `GET /health`(守护进程/负载均衡探活契约，鉴权会让 kill 探活误报失败)。实现为独立子路由 (不叠鉴权层),受保护端点统一叠加 axum middleware。
- **恒定时间比较**:手写 `ct_eq(a, b)`(零新依赖)——逐字节 XOR 累积 OR，长度差计入 (`len_a ^ len_b` 非零即判负),比较轮数只取决于较长切片，不因前缀命中提前返回。
- **token 不落盘**:token 值不进任何日志/审计;鉴权失败审计只记方法与端点。
- **向后兼容**:`token = None`(缺省) 时中间件直接放行并**忽略**任何 Authorization 头 —— 行为与 v0.3 完全一致。
- **客户端**:monitorctl 全局 `--token <TOKEN>` flag + `SERVICE_WATCH_TOKEN` 环境变量 (flag 优先，空值视为未提供),命中即所有请求携带头;未配置 token 的 agent 忽略该头。

#### 9.2.2 控制面 HTTPS (F32)

`[control] tls_cert` / `tls_key`(PEM 路径，**成对必填**,单边配置在 config 校验即拒绝) 配置后，控制面以**纯 HTTPS** 监听同一 `listen` 地址 (axum-server `tls-rustls`,随 control-http feature);未配置 = 纯 HTTP,`axum::serve` 现状路径零改动。

- **互斥语义**:同一端口二选一，无 HTTP/HTTPS 并存、无协议协商降级。明文 HTTP 请求打到 TLS 端口在 TLS 握手层直接收到 alert，客户端表现为连接错误 —— 如实文档化，不做兼容处理。
- **加载时机与失败路径**:证书/私钥由 `control::load_tls_config` 在 runtime 装配期加载 (与端口 bind 同列早失败点，先于 data_dir/journal 装配);失败 → `Err` → main 退出码 **2**。错误信息仅含路径与解析原因，**私钥内容不进错误/日志**(与 token 同纪律)。
- **rustls provider**:axum-server `tls-rustls` feature 优先复用进程级已装 `CryptoProvider`(reqwest 同线 aws-lc-rs),否则经 `builder_with_provider` 局部构造 aws-lc provider —— 不调用 `install_default`,无进程级重复安装 panic 面。ALPN 由 axum-server 置 `h2` + `http/1.1`。
- **停机**:TLS 接受循环与 HTTP 模式同口径 —— runtime `abort` 控制面任务 (control 模块 doc「停机与 409 语义」);abort 即释放监听端口，在途连接任务 (per-connection spawn) 自然收尾，不引入 `Handle::graceful_shutdown`,停机保持单一 abort 路径。
- **轮换 = 重启**:证书与私钥加载后驻留内存，不热轮换;换证书重启 agent(与 control 段其余字段「变更要求重启」一致)。
- **客户端**:monitorctl `--url https://...` 直接可用 (reqwest rustls，与 agent 端同线);全局 `--insecure` 跳过证书校验 (`danger_accept_invalid_certs`),**仅自签/测试场景**,生产走信任链 (自签 CA 安装见 RUNBOOK §1.2d)。

**F38 控制面 mTLS(可选客户端证书校验)**:`[control] client_ca_cert`(PEM 路径，config 校验**强制与 tls_cert/tls_key 同配**) 配置后，服务端在 TLS 握手层要求客户端证书 —— 证书须由该 CA(或其下级 CA) 签发且在有效期内，否则握手以 alert 失败 (客户端表现为连接错误，无 HTTP 层交互);`None` = 不请求客户端证书，F32 行为零改动。

- **实现**:rustls `WebPkiClientVerifier` 严格 webpki 校验 (不 `allow_untrusted`/不 `allow_unauthenticated` —— 后者会退化为「证书可选」),经 `builder_with_provider(roots, provider)` 复用与 HTTPS 同一 provider 选型 (进程级 `CryptoProvider::get_default()` 优先，否则局部构造 aws-lc provider，不 `install_default`);ServerConfig 以 `with_client_cert_verifier(..).with_single_cert(..)` 手工构建后交 `RustlsConfig::from_config`(该构造不代设 ALPN，手工补齐 `h2`/`http/1.1`)。CA PEM 解析**严格模式**(任一段失败即拒绝，不做宽容收集;空文件/无证书段 → 装配期 `Err`,与 F32 坏 PEM 同列早退，退出码 2)。
- **信任链三件套可叠加**:TLS(传输加密)、Bearer token(F23,HTTP 层)、mTLS(F38,TLS 握手层) 三者**独立配置、独立叠加** —— 鉴权中间件仍在 TLS 层之后，401 语义不变;mTLS 失败的请求根本到不了 HTTP 层 (不会产生 401/403)。典型组合:公网远程运维 = TLS + Bearer + mTLS 全开;本机部署 = 三者皆关 (现状)。
- **客户端**:monitorctl 全局 `--client-cert <PATH>` + `--client-key <PATH>`(**成对必填**,单边 → 启动错误) 经 `reqwest::Identity::from_pem`(reqwest 0.13 rustls 后端唯一 PEM 入口，单 buffer 收集证书段 + PKCS#8/#1/SEC1 私钥段) 构建客户端身份，全部子命令共用;与 `--token` 互不排斥。证书生成与运维细则见 RUNBOOK §1.2e。
- **轮换**:CA 与服务端证书/私钥同列加载驻留内存;轮换 (含增删客户端信任)= 重启 agent。

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
- **指标历史持久化 (F39,`[metrics] history_path` 可选)**:eval 每轮发布的样本以 JSONL **追加**落盘 —— 独立 writer 任务 (`metrics_history` 顶层模块) 消费 metrics watch 克隆，**eval 零改动、不阻塞单写者** (watch 旁路)。语义与边界：
  - **行格式 = `/metrics/history` 单样本对象** `{"at": <采样时刻>, "report": <MetricsReport>}`(`model::control::MetricsSample` 的 serde 形态),与该端点响应内的样本对象同构 —— 落盘文件直接按端点语义离线分析;`at` 由 writer 在消费时刻盖戳 (watch 通道不携带采样时刻，与 eval 入环**同轮**、通常亚毫秒偏差;journal 背压时 tick 体内的 await 可放大到毫秒至百毫秒级 —— 第十四轮验证如实化)。
  - **消费节奏与合并**：watch 每条变更写一行;writer 消费滞后时中间样本被最新值覆盖 (指标是瞬时快照，跳轮可接受，如实记录)。**首样本照写** (不去重):启动时 watch 当前值 (可能是全零默认报告) 也落一行 —— 重启追加不截断，历史跨重启连续。
  - **轮转 (复用 journal F24 单代模式)**:写**后**实测当前文件尺寸，超 10 MiB → 改名 `<stem>.1.jsonl` (只保留一代，二次轮转覆盖;gen1 路径复用 `recover::journal::gen1_path` 单一事实源);轮转失败 WARN 降级继续写当前文件 (体积上限是运维目标不是安全边界);轮转成功只写 `tracing::info`,**不写 journal audit** (journal 是恢复域，历史文件是观测出口，两域隔离)。
  - **写入与停机**：append+create 单写者整行写入，写入段无 await 点 —— 停机 abort 只落在 `changed().await`,不截断半行;最后在途样本可能不落盘 (观测数据可接受)。无逐行 fsync (可靠性 = OS 页缓存，崩溃丢尾部可容忍)。打开时做半行定界 (崩溃落在 write_all 中途的残行以补 `\n` 定界，爆炸半径压回 1 行 —— 第十四轮 P3)。打开失败/轮转降级重开失败 → ERROR 后 writer 退出 (持久化静默失效不可取;agent 不因此停机 —— 观测出口非恢复域)。`history_path` 消费不依赖 reqwest，无 feature 门控。
  - **不做重放/查询** (纯数据出口，分析走 jq/外部工具);外部 copytruncate 兼容 (O_APPEND + 实测尺寸),rename 式外部轮转无效、不得删除主文件 (RUNBOOK §1.2f)。
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
