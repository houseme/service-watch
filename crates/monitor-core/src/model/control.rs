//! 控制平面共享模型:agent ↔ monitorctl 契约 (DESIGN.md §9.2)。
//! 这些类型同时用于 axum 服务端序列化与 monitorctl 客户端反序列化，字段变更需两侧兼容。

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::SystemTime;

use super::ids::ProcessIdentity;

/// F18 历史指标环形缓冲容量 (约 2h @ 1s tick)。eval 入环上限与
/// `GET /metrics/history?range=` 的 range 上限共用本常量 —— 缓冲之外的数据
/// 本就不存在，range 超限收敛到上限即可。
pub const METRICS_HISTORY_CAP: usize = 7200;

/// F18 环形缓冲共享句柄:eval(单写者，DESIGN §2.2) 每轮 tick 入环，
/// control 侧只读快照 (`Arc` 直接共享，不走独立 watch —— 经
/// [`MetricsReport::history`] 搭载既有 metrics watch 分发，
/// `SchedulerHandles`/`ControlShared` 形状零改动)。
pub type MetricsHistoryRing = Arc<std::sync::Mutex<VecDeque<MetricsSample>>>;

/// F18 单个历史指标样本 (环形缓冲条目):采样时刻 + 当轮发布的指标报告。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSample {
    pub at: SystemTime,
    pub report: MetricsReport,
}

/// `GET /metrics/history` 响应：窗口内样本 + 计数 (降采样后计数)。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsHistory {
    pub samples: Vec<MetricsSample>,
    /// 与 `samples.len()` 一致 (经 [`MetricsHistory::new`] 构造保证)。
    pub count: usize,
}

impl MetricsHistory {
    /// 以样本集构造响应 (count 同步为样本数)。
    pub fn new(samples: Vec<MetricsSample>) -> Self {
        let count = samples.len();
        Self { samples, count }
    }
}

/// monitorctl → agent 命令 (经 `POST /v1/{restart,check,reset}/{target}` 与
/// `POST /v1/reload`;SIGHUP 由 runtime 转发为 `Reload`)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "op")]
pub enum ControlCommand {
    Restart {
        target: String,
    },
    Check {
        target: String,
    },
    /// 解除 Fatal 闩锁 + 清空重启预算 (写审计日志)。
    Reset {
        target: String,
    },
    /// F14 配置热重载:path 为 None 时由 eval 回填启动配置路径
    /// (`SchedulerHandles::config_path`)。仅应用目标拓扑 (checks/services/watches);
    /// 其余小节变更要求重启，不热应用 (见 `eval::reload`)。
    Reload {
        path: Option<String>,
    },
    /// F27 目标级静默 (`POST /v1/silence/{target}`):窗口内该 target 的
    /// Alert/Escalate/Flap 通知不投递 (Resolve 永不静默)。deadline 由 eval 侧
    /// 单调时钟计算 (与墙钟无关);`duration_secs = 0` 或超上限 (604800 = 7d)
    /// 由 eval 侧拒绝 (WARN + journal 审计)。重复设置后到覆盖。
    Silence {
        target: String,
        duration_secs: u64,
        reason: Option<String>,
    },
    /// F27 提前解除静默 (`DELETE /v1/silence/{target}`);无活动静默时由 eval
    /// 侧拒绝并审计。
    Unsilence {
        target: String,
    },
}

/// target 的形态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TargetMode {
    /// agent 亲生，可拉活。
    Supervise,
    /// 收养进程，只监控告警，永不杀/重启。
    Attach,
    /// 纯检查目标 (无进程身份)。
    Check,
}

/// 恢复状态机状态 (DESIGN.md §8.2 全转移表)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RecoveryState {
    Running,
    Suspect,
    Recovering,
    ForceKill,
    Restarting,
    Starting,
    CrashLoop,
    Fatal,
}

/// 检查健康级别 (DESIGN.md §7.1)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthLevel {
    Ok,
    Warning,
    Critical,
}

/// 单个 target 的运行态摘要。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetStatus {
    pub name: String,
    pub mode: TargetMode,
    pub health: HealthLevel,
    /// supervise 目标的恢复状态;其他模式为 None。
    pub recovery: Option<RecoveryState>,
    pub checks: Vec<String>,
    pub identity: Option<ProcessIdentity>,
    /// 当前未闭合 incident 的最高级别;无则为 Ok。
    pub incident: Option<HealthLevel>,
    /// 窗口内自动重启次数 (supervise)。
    pub restarts_in_window: u32,
    pub last_transition: Option<SystemTime>,
}

/// `GET /status` 响应。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusReport {
    pub started_at: SystemTime,
    pub ready: bool,
    pub targets: Vec<TargetStatus>,
}

/// `GET /metrics` 响应 (JSON，或 `?format=prometheus` 文本)。
///
/// 字段来源分三路合并 (control 层装配):
/// - alert 计数器 (sent/failures/dropped) 来自 [`crate::alert::AlertCounters`];
/// - 探测/恢复/运行概览来自 eval 每轮 tick 发布的 watch;
/// - `uptime_secs` 由 started_at 推导。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsReport {
    // ---- 告警管道 ----
    pub alerts_sent: u64,
    pub alert_delivery_failures: u64,
    pub alerts_dropped: u64,
    // ---- journal 持久化 ----
    pub journal_dropped: u64,
    pub journal_write_failures: u64,
    pub journal_write_successes: u64,
    pub journal_critical_failures: u64,
    // ---- 探测 ----
    pub probe_executed: u64,
    pub probe_skipped: u64,
    // ---- 恢复 ----
    pub restarts_total: u64,
    pub state_transitions_total: u64,
    pub fatal_targets: u64,
    // ---- 采集与降级 ----
    /// 当前快照中的降级条目数 (瞬时值，诚实反映"现在缺什么")。
    pub degradations_current: u64,
    /// 降级**事件**次数 (无降级 → 有降级的跳变计数;避免持续降级重复累计)。
    pub degradations_events: u64,
    pub last_collect_at: Option<SystemTime>,
    // ---- 运行概览 ----
    pub targets_total: u64,
    pub checks_ok: u64,
    pub checks_warning: u64,
    pub checks_critical: u64,
    pub services_total: u64,
    pub services_running: u64,
    pub recovery_running: u64,
    pub recovery_suspect: u64,
    pub recovery_recovering: u64,
    pub recovery_force_kill: u64,
    pub recovery_restarting: u64,
    pub recovery_starting: u64,
    pub recovery_crashloop: u64,
    pub recovery_fatal: u64,
    pub uptime_secs: u64,
    // ---- F18 历史指标环形缓冲 (仅进程内共享，不进 wire 契约)----
    /// 环形缓冲句柄 (eval 单写者;control 只读快照)。随本报告经既有 metrics
    /// watch 分发;`/metrics` JSON 与 Prometheus 文本不含本字段 (serde skip),
    /// `/metrics/history` 经 control 侧取快照渲染。
    #[serde(skip)]
    pub history: MetricsHistoryRing,
}
