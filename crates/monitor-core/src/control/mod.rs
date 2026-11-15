//! 控制平面 HTTP 服务(feature `control-http`,DESIGN.md §9.2,axum)。
//!
//! 端点:
//! - `GET /`        内嵌只读状态页(F22:`include_str!` 零外部依赖,内联
//!   CSS/JS;JS 每 2s fetch `/status` 与 `/metrics` 渲染;不提供 reload 入口)
//! - `GET /health`  浅存活(进程在即可,200 空体)
//! - `GET /ready`   配置加载成功 + 调度循环已启动(**降级采集不影响 ready**):
//!   ready → 200,否则 503;响应体 `{"ready":bool}`
//! - `GET /status`  StatusReport(经 watch 读 eval 最新汇报)
//! - `GET /metrics` 运行指标:**内容协商** —— `?format=prometheus` 或
//!   `Accept: text/plain` 返回 Prometheus 文本(DESIGN F13);默认 JSON。
//!   告警计数器来自 alert counters,探测/恢复/运行概览来自 eval 每轮发布
//! - `GET /metrics/history?range=<secs>`(F18 历史指标环形缓冲):窗口内样本
//!   JSON `{"samples":[{at, report}...], "count": N}`;range 缺省 300、上限
//!   [`METRICS_HISTORY_CAP`](超出收敛到上限);0/负/非数字 → 400;窗口内样本
//!   超过 600 点等距降采样(保留首尾)。与 `/metrics`(瞬时)并存,Prometheus
//!   文本端点不受影响
//! - `POST /v1/restart/{target}` / `POST /v1/check/{target}` / `POST /v1/reset/{target}`
//!   → `ControlCommand` 经 mpsc(16) 进 eval:入队成功 202;
//!   通道满(Full)→ 503;通道关闭(停机中)→ 503(DESIGN §2.2 背压策略)
//! - `POST /v1/reload`(F14 配置热重载):body 可选 JSON `{"path": "..."}`(缺省
//!   重读启动路径)→ `ControlCommand::Reload` 入队 202;非法 body → 400;
//!   满/关闭 → 503。仅应用目标拓扑(checks/services/watches);其余小节变更
//!   要求重启,不热应用(eval::reload 模块 doc)
//!
//! ## 停机与 409 语义(v0.1,如实记录)
//!
//! [`serve`] 是**无限 future**:内部 `axum::serve` 循环仅在 bind 失败或底层
//! IO 错误时返回;停机由 runtime `abort` 本任务完成(DESIGN §2.2:runtime
//! 负责装配与停机顺序),本函数不做优雅停机。
//!
//! 「Fatal 态 restart → 409」(2026-11-11 接线):经 status watch 读取该 target
//! 的恢复状态,Fatal → 409 + 提示 reset;非 Fatal 一律 202 入队(fire-and-forget
//! 语义不变,eval 侧保留防御性拒绝 + journal 审计)。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{mpsc, watch};

use crate::collect::CollectOutput;
use crate::error::Result;
use crate::model::control::{ControlCommand, MetricsReport, StatusReport};

// F18 历史指标端点专用导入(仅 control-http feature 参与编译)。
#[cfg(feature = "control-http")]
use crate::model::control::{METRICS_HISTORY_CAP, MetricsHistory, MetricsSample};
#[cfg(feature = "control-http")]
use std::time::Duration;

/// axum 相关导入(仅 control-http feature 参与编译)。
#[cfg(feature = "control-http")]
use axum::extract::{Path, State};
#[cfg(feature = "control-http")]
use axum::http::StatusCode;
#[cfg(feature = "control-http")]
use axum::routing::{get, post};
#[cfg(feature = "control-http")]
use axum::{Json, Router};

/// 控制平面共享状态(runtime 装配,只读快照 + 命令出口)。
pub struct ControlShared {
    pub started_at: SystemTime,
    pub ready: Arc<std::sync::atomic::AtomicBool>,
    pub status_rx: watch::Receiver<Arc<StatusReport>>,
    pub metrics_rx: watch::Receiver<Arc<MetricsReport>>,
    pub snapshot_rx: watch::Receiver<Arc<CollectOutput>>,
    /// eval 命令入口(容量小,满即 503 —— DESIGN §2.2 背压策略)。
    pub cmd_tx: mpsc::Sender<ControlCommand>,
    /// 供 `/metrics` 补充告警计数。
    pub alert_counters: Arc<crate::alert::AlertCounters>,
}

/// 启动控制平面服务(**无限 future**):绑定 `listen` 后进入 axum 服务循环,
/// 仅在 bind 失败或底层 IO 错误时返回。停机由 runtime abort 本任务完成
/// (见模块 doc「停机与 409 语义」)。
#[cfg(feature = "control-http")]
pub async fn serve(listen: SocketAddr, shared: Arc<ControlShared>) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    bind_and_serve(listener, shared).await
}

/// 绑定已完成的 serve 入口:runtime 先自行 bind 以提前暴露端口占用错误,
/// 再 spawn 本函数;单测对 127.0.0.1:0 临时端口直接调用。
#[cfg(feature = "control-http")]
pub(crate) async fn bind_and_serve(
    listener: tokio::net::TcpListener,
    shared: Arc<ControlShared>,
) -> Result<()> {
    let app = router(shared);
    axum::serve(listener, app)
        .await
        .map_err(crate::error::CoreError::from)
}

/// feature 关闭时的占位:控制平面不可用,调用即返回错误(不 panic)。
#[cfg(not(feature = "control-http"))]
pub async fn serve(_listen: SocketAddr, _shared: Arc<ControlShared>) -> Result<()> {
    Err(crate::error::CoreError::Control(
        "control-http feature is disabled".into(),
    ))
}

/// axum Router(axum 0.8 路由语法:`{target}`,非 `:target`)。
#[cfg(feature = "control-http")]
fn router(shared: Arc<ControlShared>) -> Router {
    Router::new()
        .route("/", get(status_page))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        .route("/metrics/history", get(metrics_history))
        .route("/v1/restart/{target}", post(restart))
        .route("/v1/check/{target}", post(check))
        .route("/v1/reset/{target}", post(reset))
        .route("/v1/reload", post(reload))
        .with_state(shared)
}

/// `GET /health`:浅存活 —— 进程在即 200 空体(不做任何依赖检查)。
#[cfg(feature = "control-http")]
async fn health() -> StatusCode {
    StatusCode::OK
}

/// 内嵌只读状态页(F22):`include_str!` 编译期嵌入,零外部依赖(内联
/// CSS/JS,无 CDN/字体/图标);页面自身只读 —— 不提供 reload 等写操作入口。
#[cfg(feature = "control-http")]
const STATUS_PAGE_HTML: &str = include_str!("status_page.html");

/// `GET /`(F22 Web 状态页):`Html` 以 `text/html; charset=utf-8` 返回。
/// 页内 JS 每 2s fetch `/status` 与 `/metrics` 渲染(见 status_page.html)。
#[cfg(feature = "control-http")]
async fn status_page() -> axum::response::Html<&'static str> {
    axum::response::Html(STATUS_PAGE_HTML)
}

/// `GET /ready`:ready → 200,否则 503;响应体 `{"ready":bool}`。
#[cfg(feature = "control-http")]
async fn ready(State(shared): State<Arc<ControlShared>>) -> (StatusCode, Json<serde_json::Value>) {
    if shared.ready.load(std::sync::atomic::Ordering::Relaxed) {
        (StatusCode::OK, Json(serde_json::json!({ "ready": true })))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "ready": false })),
        )
    }
}

/// `GET /status`:eval 最新汇报。初始值由 runtime 写入(空 targets);
/// sender 消失(eval 终止)后仍可读最后一份快照 —— 不 panic、不 5xx。
#[cfg(feature = "control-http")]
async fn status(State(shared): State<Arc<ControlShared>>) -> Json<StatusReport> {
    // 工作区 serde 未启用 `rc` feature:`Arc<StatusReport>` 不可序列化,
    // 克隆解 Arc(单次 HTTP 响应,开销可忽略)。
    let report = shared.status_rx.borrow().clone();
    Json((*report).clone())
}

/// 合并三路指标来源(alert 计数器 / eval watch / uptime 推导)。
#[cfg(feature = "control-http")]
fn merged_metrics(shared: &ControlShared) -> MetricsReport {
    let mut report = shared.alert_counters.to_metrics();
    let latest = shared.metrics_rx.borrow().clone();
    report.probe_executed = latest.probe_executed;
    report.probe_skipped = latest.probe_skipped;
    report.journal_dropped = latest.journal_dropped;
    report.journal_write_failures = latest.journal_write_failures;
    report.journal_write_successes = latest.journal_write_successes;
    report.journal_critical_failures = latest.journal_critical_failures;
    report.restarts_total = latest.restarts_total;
    report.state_transitions_total = latest.state_transitions_total;
    report.fatal_targets = latest.fatal_targets;
    report.degradations_current = latest.degradations_current;
    report.degradations_events = latest.degradations_events;
    report.last_collect_at = latest.last_collect_at;
    report.targets_total = latest.targets_total;
    report.checks_ok = latest.checks_ok;
    report.checks_warning = latest.checks_warning;
    report.checks_critical = latest.checks_critical;
    report.services_total = latest.services_total;
    report.services_running = latest.services_running;
    report.recovery_running = latest.recovery_running;
    report.recovery_suspect = latest.recovery_suspect;
    report.recovery_recovering = latest.recovery_recovering;
    report.recovery_force_kill = latest.recovery_force_kill;
    report.recovery_restarting = latest.recovery_restarting;
    report.recovery_starting = latest.recovery_starting;
    report.recovery_crashloop = latest.recovery_crashloop;
    report.recovery_fatal = latest.recovery_fatal;
    // F18:环形缓冲句柄随 eval 侧报告透传(serde skip,不进 /metrics JSON)。
    report.history = latest.history.clone();
    // uptime:eval 的单调源优先(墙钟回拨防护);watch 未发布(启动瞬间)才用墙钟。
    let wall_uptime = shared
        .started_at
        .elapsed()
        .map(|d| d.as_secs())
        .unwrap_or(0);
    report.uptime_secs = report.uptime_secs.max(wall_uptime);
    report
}

/// Prometheus 文本渲染(exposition format;指标前缀 `service_watch_`)。
/// 输出一组 HELP/TYPE/值行。
#[cfg(feature = "control-http")]
fn prom_line(out: &mut String, name: &str, help: &str, kind: &str, val: u64) {
    use std::fmt::Write as _;
    let _ = writeln!(out, "# HELP service_watch_{name} {help}");
    let _ = writeln!(out, "# TYPE service_watch_{name} {kind}");
    let _ = writeln!(out, "service_watch_{name} {val}");
}

#[cfg(feature = "control-http")]
fn render_prometheus(m: &MetricsReport) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(2048);
    prom_line(
        &mut out,
        "alerts_sent",
        "Alert events dispatched successfully.",
        "counter",
        m.alerts_sent,
    );
    prom_line(
        &mut out,
        "alert_delivery_failures",
        "Alert deliveries that exhausted retries.",
        "counter",
        m.alert_delivery_failures,
    );
    prom_line(
        &mut out,
        "alerts_dropped",
        "Alert events dropped (queue full / shutdown drain).",
        "counter",
        m.alerts_dropped,
    );
    for (name, help, value) in [
        (
            "journal_dropped",
            "Journal records rejected by the bounded writer queue.",
            m.journal_dropped,
        ),
        (
            "journal_write_failures",
            "Journal write or flush failures.",
            m.journal_write_failures,
        ),
        (
            "journal_write_successes",
            "Journal records successfully written.",
            m.journal_write_successes,
        ),
        (
            "journal_critical_failures",
            "Critical recovery records that failed persistence.",
            m.journal_critical_failures,
        ),
    ] {
        prom_line(&mut out, name, help, "counter", value);
    }
    prom_line(
        &mut out,
        "probe_executed",
        "Probes executed.",
        "counter",
        m.probe_executed,
    );
    prom_line(
        &mut out,
        "probe_skipped",
        "Probes skipped (previous still in flight).",
        "counter",
        m.probe_skipped,
    );
    prom_line(
        &mut out,
        "restarts_total",
        "Supervised process spawn attempts.",
        "counter",
        m.restarts_total,
    );
    prom_line(
        &mut out,
        "state_transitions_total",
        "Recovery state machine transitions.",
        "counter",
        m.state_transitions_total,
    );
    prom_line(
        &mut out,
        "fatal_targets",
        "Targets currently latched Fatal.",
        "gauge",
        m.fatal_targets,
    );
    prom_line(
        &mut out,
        "degradations_current",
        "Degradation entries in latest snapshot.",
        "gauge",
        m.degradations_current,
    );
    prom_line(
        &mut out,
        "degradations_events",
        "Degradation episodes (absent->present transitions).",
        "counter",
        m.degradations_events,
    );
    prom_line(
        &mut out,
        "targets_total",
        "Configured targets.",
        "gauge",
        m.targets_total,
    );
    prom_line(
        &mut out,
        "checks_ok",
        "Checks currently Ok.",
        "gauge",
        m.checks_ok,
    );
    prom_line(
        &mut out,
        "checks_warning",
        "Checks currently Warning.",
        "gauge",
        m.checks_warning,
    );
    prom_line(
        &mut out,
        "checks_critical",
        "Checks currently Critical.",
        "gauge",
        m.checks_critical,
    );
    prom_line(
        &mut out,
        "services_total",
        "Supervised services.",
        "gauge",
        m.services_total,
    );
    prom_line(
        &mut out,
        "services_running",
        "Supervised services with a live child.",
        "gauge",
        m.services_running,
    );
    for (name, val) in [
        ("recovery_running", m.recovery_running),
        ("recovery_suspect", m.recovery_suspect),
        ("recovery_recovering", m.recovery_recovering),
        ("recovery_force_kill", m.recovery_force_kill),
        ("recovery_restarting", m.recovery_restarting),
        ("recovery_starting", m.recovery_starting),
        ("recovery_crashloop", m.recovery_crashloop),
        ("recovery_fatal", m.recovery_fatal),
    ] {
        let _ = writeln!(out, "# TYPE service_watch_{name} gauge");
        let _ = writeln!(out, "service_watch_{name} {val}");
    }
    prom_line(
        &mut out,
        "uptime_secs",
        "Agent uptime in seconds.",
        "gauge",
        m.uptime_secs,
    );
    if let Some(t) = m.last_collect_at {
        let _ = writeln!(
            out,
            "# HELP service_watch_last_collect_timestamp_seconds Last successful collect (epoch)."
        );
        let _ = writeln!(
            out,
            "# TYPE service_watch_last_collect_timestamp_seconds gauge"
        );
        let _ = writeln!(
            out,
            "service_watch_last_collect_timestamp_seconds {}",
            t.duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
        );
    }
    out
}

/// `GET /metrics`:默认 JSON;`?format=prometheus` 或 `Accept: text/plain`
/// 返回 Prometheus 文本(DESIGN F13)。
#[cfg(feature = "control-http")]
async fn metrics(
    State(shared): State<Arc<ControlShared>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let merged = merged_metrics(&shared);
    // 协商优先级(评审 P2):显式 ?format 短路(可压过 Accept);无参数才看
    // Accept;Accept 忽略 q 值的误判由显式参数兜底。
    let wants_prom = match params.get("format").map(String::as_str) {
        Some("prometheus") => true,
        Some(_) => false,
        None => headers
            .get(axum::http::header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .map(|a| a.contains("text/plain"))
            .unwrap_or(false),
    };
    if wants_prom {
        (
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            render_prometheus(&merged),
        )
            .into_response()
    } else {
        Json(merged).into_response()
    }
}

// ---------------------------------------------------------------------------
// F18 历史指标环形缓冲(/metrics/history)
// ---------------------------------------------------------------------------

/// `?range=` 缺省窗口(秒)。
#[cfg(feature = "control-http")]
const METRICS_RANGE_DEFAULT_SECS: u64 = 300;

/// 单次响应的最大样本点数:窗口内超过本值时等距降采样。
#[cfg(feature = "control-http")]
const METRICS_HISTORY_MAX_POINTS: usize = 600;

/// `?range=<secs>` 解析(纯函数):缺省 300;上限 [`METRICS_HISTORY_CAP`]
/// (超出收敛到上限 —— 缓冲之外本就无数据,超限不算非法);0/负数/非数字
/// → `None`(端点回 400)。
#[cfg(feature = "control-http")]
fn parse_range_secs(raw: Option<&str>) -> Option<u64> {
    match raw {
        None => Some(METRICS_RANGE_DEFAULT_SECS),
        Some(s) => s
            .parse::<u64>()
            .ok()
            .filter(|&secs| secs > 0)
            .map(|secs| secs.min(METRICS_HISTORY_CAP as u64)),
    }
}

/// 窗口过滤(纯函数):保留 `at ≥ now - range` 的样本(时间升序入环,过滤
/// 保序)。cutoff 下溢(墙钟早于 epoch)时退化为全保留 —— 时钟异常时宁多勿缺。
#[cfg(feature = "control-http")]
fn window_samples(ring: &[MetricsSample], now: SystemTime, range: Duration) -> Vec<MetricsSample> {
    let cutoff = now.checked_sub(range).unwrap_or(SystemTime::UNIX_EPOCH);
    ring.iter().filter(|s| s.at >= cutoff).cloned().collect()
}

/// 等距降采样(纯函数):`len > max` 时按整数四舍五入等距抽取恰好 `max` 点,
/// **保留首尾**(最新样本必须在内 —— 运维视角最后一点即当前状态);否则
/// 原样克隆返回。`max <= 1` 且非空 → 仅保留最新一点。len > max 保证相邻
/// 抽取索引严格递增(round 单调 + 步距 > 1),无需去重。
#[cfg(feature = "control-http")]
fn downsample(samples: &[MetricsSample], max: usize) -> Vec<MetricsSample> {
    let len = samples.len();
    if len <= max {
        return samples.to_vec();
    }
    if max <= 1 {
        return samples.last().cloned().into_iter().collect();
    }
    let last = len - 1;
    let span = max - 1;
    (0..max)
        .map(|i| {
            let idx = (i * last + span / 2) / span;
            samples[idx].clone()
        })
        .collect()
}

/// `GET /metrics/history?range=<secs>`(F18):环形缓冲窗口快照,响应
/// `{"samples":[{at, report}...], "count": N}`。数据源为 eval 每轮发布、随
/// metrics watch 分发的环形缓冲句柄(单写者;此处只读,锁内仅做克隆)。
#[cfg(feature = "control-http")]
async fn metrics_history(
    State(shared): State<Arc<ControlShared>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(range_secs) = parse_range_secs(params.get("range").map(String::as_str)) else {
        return (
            StatusCode::BAD_REQUEST,
            "invalid range; expected integer seconds in 1..=7200",
        )
            .into_response();
    };
    let range = Duration::from_secs(range_secs);
    let latest = shared.metrics_rx.borrow().clone();
    // 快照(eval 单写者,毒化恢复与 eval 侧一致:历史可丢、不 panic)。
    let ring: Vec<MetricsSample> = match latest.history.lock() {
        Ok(guard) => guard.iter().cloned().collect(),
        Err(poisoned) => poisoned.into_inner().iter().cloned().collect(),
    };
    let windowed = window_samples(&ring, SystemTime::now(), range);
    let kept = downsample(&windowed, METRICS_HISTORY_MAX_POINTS);
    Json(MetricsHistory::new(kept)).into_response()
}

/// `POST /v1/restart/{target}`:Fatal(经 status watch 判定)→ 409 + 提示
/// reset;其余入队 202(Full/Closed → 503)。
#[cfg(feature = "control-http")]
async fn restart(
    State(shared): State<Arc<ControlShared>>,
    Path(target): Path<String>,
) -> (StatusCode, &'static str) {
    let is_fatal = shared.status_rx.borrow().targets.iter().any(|t| {
        t.name == target && t.recovery == Some(crate::model::control::RecoveryState::Fatal)
    });
    if is_fatal {
        return (
            StatusCode::CONFLICT,
            "target is latched Fatal; run `monitorctl reset <target>` after fixing the root cause",
        );
    }
    send_command(&shared, ControlCommand::Restart { target })
}

/// `POST /v1/check/{target}`:绑定检查的 next_due 立即到期(eval 侧处理)。
#[cfg(feature = "control-http")]
async fn check(
    State(shared): State<Arc<ControlShared>>,
    Path(target): Path<String>,
) -> (StatusCode, &'static str) {
    send_command(&shared, ControlCommand::Check { target })
}

/// `POST /v1/reset/{target}`:解除 Fatal 闩锁 + 清空预算(eval 侧写审计)。
#[cfg(feature = "control-http")]
async fn reset(
    State(shared): State<Arc<ControlShared>>,
    Path(target): Path<String>,
) -> (StatusCode, &'static str) {
    send_command(&shared, ControlCommand::Reset { target })
}

/// `POST /v1/reload` 请求体(可选):`{"path": "..."}` 指定配置文件;
/// 空 body / `{}` / `{"path": null}` → None(eval 侧回填启动配置路径)。
#[cfg(feature = "control-http")]
#[derive(Debug, Default, serde::Deserialize)]
struct ReloadBody {
    #[serde(default)]
    path: Option<String>,
}

/// `POST /v1/reload`(F14 配置热重载):触发 eval 重读配置并应用目标拓扑。
/// body 可选(空 / JSON `{"path": "..."}`);`?format` 无关。入队 202;
/// 非法 JSON body → 400;通道满/关闭 → 503(与其它命令端点一致)。
#[cfg(feature = "control-http")]
async fn reload(
    State(shared): State<Arc<ControlShared>>,
    body: String,
) -> (StatusCode, &'static str) {
    let path = if body.trim().is_empty() {
        None
    } else {
        match serde_json::from_str::<ReloadBody>(&body) {
            Ok(parsed) => parsed.path,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    "invalid reload body; expected {\"path\": \"...\"} or empty",
                );
            }
        }
    };
    send_command(&shared, ControlCommand::Reload { path })
}

/// 命令入队(DESIGN §2.2:通道满 → 503;关闭 = agent 停机中 → 503)。
#[cfg(feature = "control-http")]
fn send_command(shared: &ControlShared, cmd: ControlCommand) -> (StatusCode, &'static str) {
    match shared.cmd_tx.try_send(cmd) {
        Ok(()) => (StatusCode::ACCEPTED, "accepted"),
        Err(mpsc::error::TrySendError::Full(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, "command queue full")
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, "agent shutting down")
        }
    }
}

// ---------------------------------------------------------------------------
// 测试(真实 socket 集成:临时端口 bind_and_serve + reqwest 断言端点行为)
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "control-http", feature = "http-client"))]
mod tests {
    use super::*;
    use crate::collect::{Collector, StubCollector};
    use crate::model::control::{HealthLevel, RecoveryState, TargetMode, TargetStatus};
    use crate::model::ids::ProcessIdentity;
    use std::sync::atomic::Ordering;

    /// 构造测试用 ControlShared(cmd 通道容量可调;sender 侧立即 drop,
    /// 借以覆盖「sender 消失仍可读最后值」的防御路径)。metrics_tx 一并返回,
    /// 供直推 watch 模拟 eval 发布(F18 历史随 metrics 报告搭载)。
    #[allow(clippy::type_complexity)]
    fn shared_with(
        cmd_cap: usize,
    ) -> (
        Arc<ControlShared>,
        mpsc::Receiver<ControlCommand>,
        watch::Sender<Arc<StatusReport>>,
        watch::Sender<Arc<MetricsReport>>,
    ) {
        let (status_tx, status_rx) = watch::channel(Arc::new(StatusReport {
            started_at: SystemTime::now(),
            ready: false,
            targets: Vec::new(),
        }));
        let (metrics_tx, metrics_rx) = watch::channel(Arc::new(MetricsReport::default()));
        let initial = Arc::new(StubCollector::failed("test".into()).collect(&[]));
        let (_snapshot_tx, snapshot_rx) = watch::channel(initial);
        let (cmd_tx, cmd_rx) = mpsc::channel(cmd_cap);
        (
            Arc::new(ControlShared {
                started_at: SystemTime::now(),
                ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                status_rx,
                metrics_rx,
                snapshot_rx,
                cmd_tx,
                alert_counters: Arc::new(crate::alert::AlertCounters::default()),
            }),
            cmd_rx,
            status_tx,
            metrics_tx,
        )
    }

    /// 起服务于 127.0.0.1:0 临时端口,返回 base url(随测试 runtime 收尾)。
    async fn spawn_server(shared: Arc<ControlShared>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(bind_and_serve(listener, shared));
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn health_ready_status_metrics_report_expected_codes() {
        let (shared, _rx, _status_tx, _metrics_tx) = shared_with(16);
        let base = spawn_server(shared.clone()).await;

        // /health:200 空体
        let resp = reqwest::get(format!("{base}/health")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(resp.text().await.unwrap(), "");

        // /ready:ready = true → 200 {"ready":true}
        let resp = reqwest::get(format!("{base}/ready")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["ready"], serde_json::Value::Bool(true));

        // /ready:ready = false → 503 {"ready":false}
        shared.ready.store(false, Ordering::Relaxed);
        let resp = reqwest::get(format!("{base}/ready")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 503);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["ready"], serde_json::Value::Bool(false));

        // /status:初始空 targets,sender 已 drop 仍可读
        let resp = reqwest::get(format!("{base}/status")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let report: StatusReport = resp.json().await.unwrap();
        assert!(report.targets.is_empty());

        // /metrics:counters 侧字段合并(alerts_sent / alerts_dropped)
        shared.alert_counters.sent.store(7, Ordering::Relaxed);
        shared.alert_counters.dropped.store(2, Ordering::Relaxed);
        let resp = reqwest::get(format!("{base}/metrics")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let metrics: MetricsReport = resp.json().await.unwrap();
        assert_eq!(metrics.alerts_sent, 7);
        assert_eq!(metrics.alerts_dropped, 2);
        assert_eq!(metrics.alert_delivery_failures, 0);
        assert_eq!(metrics.probe_skipped, 0, "watch 侧默认值兜底");
    }

    #[tokio::test]
    async fn status_reflects_latest_eval_report() {
        let (shared, _rx, status_tx, _metrics_tx) = shared_with(16);
        let base = spawn_server(shared).await;

        let targets = vec![TargetStatus {
            name: "web-1".into(),
            mode: TargetMode::Supervise,
            health: HealthLevel::Warning,
            recovery: Some(RecoveryState::Running),
            checks: vec!["api-health".into()],
            identity: Some(ProcessIdentity::new(4242, 123)),
            incident: Some(HealthLevel::Warning),
            restarts_in_window: 3,
            last_transition: Some(SystemTime::now()),
        }];
        status_tx
            .send(Arc::new(StatusReport {
                started_at: SystemTime::now(),
                ready: true,
                targets,
            }))
            .unwrap();

        let resp = reqwest::get(format!("{base}/status")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let report: StatusReport = resp.json().await.unwrap();
        assert_eq!(report.targets.len(), 1);
        assert_eq!(report.targets[0].name, "web-1");
        assert_eq!(report.targets[0].mode, TargetMode::Supervise);
        assert_eq!(report.targets[0].restarts_in_window, 3);
    }

    #[tokio::test]
    async fn command_endpoints_return_202_and_enqueue() {
        let (shared, mut cmd_rx, _status_tx, _metrics_tx) = shared_with(16);
        let base = spawn_server(shared).await;
        let client = reqwest::Client::new();

        let cases = [
            (
                "/v1/restart/web-1",
                ControlCommand::Restart {
                    target: "web-1".into(),
                },
            ),
            (
                "/v1/check/api",
                ControlCommand::Check {
                    target: "api".into(),
                },
            ),
            (
                "/v1/reset/web-1",
                ControlCommand::Reset {
                    target: "web-1".into(),
                },
            ),
        ];
        for (path, expect_op) in cases {
            let resp = client.post(format!("{base}{path}")).send().await.unwrap();
            assert_eq!(resp.status().as_u16(), 202, "{path}");
            let got = cmd_rx.recv().await.expect("command queued");
            assert_eq!(got, expect_op, "{path}");
        }
    }

    /// `POST /v1/reload`(F14):空 body / `{"path": ...}` → 202 入队对应命令;
    /// 非法 JSON body → 400。
    #[tokio::test]
    async fn reload_endpoint_enqueues_reload_command() {
        let (shared, mut cmd_rx, _status_tx, _metrics_tx) = shared_with(16);
        let base = spawn_server(shared).await;
        let client = reqwest::Client::new();

        // 空 body → Reload { path: None }(缺省重读启动配置路径,eval 侧回填)
        let resp = client
            .post(format!("{base}/v1/reload"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 202);
        let got = cmd_rx.recv().await.expect("command queued");
        assert_eq!(got, ControlCommand::Reload { path: None });

        // `{}` 与 `{"path": null}` 同样等价于缺省路径
        for body in ["{}", r#"{"path": null}"#] {
            let resp = client
                .post(format!("{base}/v1/reload"))
                .body(body)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 202, "{body}");
            let got = cmd_rx.recv().await.expect("command queued");
            assert_eq!(got, ControlCommand::Reload { path: None }, "{body}");
        }

        // JSON body → Reload { path: Some(...) }
        let resp = client
            .post(format!("{base}/v1/reload"))
            .body(r#"{"path": "/tmp/other.toml"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 202);
        let got = cmd_rx.recv().await.expect("command queued");
        assert_eq!(
            got,
            ControlCommand::Reload {
                path: Some("/tmp/other.toml".into())
            }
        );

        // 非法 body → 400
        let resp = client
            .post(format!("{base}/v1/reload"))
            .body("not-json")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 400);
    }

    /// reload 命令同样受通道容量约束(满 → 503)。
    #[tokio::test]
    async fn reload_endpoint_returns_503_when_queue_full() {
        let (shared, _rx, _status_tx, _metrics_tx) = shared_with(1);
        let base = spawn_server(shared).await;
        let client = reqwest::Client::new();

        let resp = client
            .post(format!("{base}/v1/reload"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 202);
        let resp = client
            .post(format!("{base}/v1/reload"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 503);
    }

    #[tokio::test]
    async fn command_returns_503_when_queue_full() {
        let (shared, _rx, _status_tx, _metrics_tx) = shared_with(1);
        let base = spawn_server(shared).await;
        let client = reqwest::Client::new();

        // 容量 1:第一笔入队(无消费者,滞留通道),第二笔 Full → 503
        let resp = client
            .post(format!("{base}/v1/check/a"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 202);
        let resp = client
            .post(format!("{base}/v1/check/b"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 503);
    }

    #[tokio::test]
    async fn command_returns_503_when_channel_closed() {
        let (shared, rx, _status_tx, _metrics_tx) = shared_with(16);
        drop(rx); // eval 侧不存在(agent 停机语义)
        let base = spawn_server(shared).await;

        let resp = reqwest::Client::new()
            .post(format!("{base}/v1/check/a"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 503);
    }

    /// Prometheus 渲染:全部字段行存在 + 计数/类型注释正确(F13)。
    #[test]
    fn prometheus_render_covers_all_fields() {
        let m = MetricsReport {
            alerts_sent: 3,
            probe_executed: 10,
            probe_skipped: 2,
            restarts_total: 4,
            state_transitions_total: 7,
            fatal_targets: 1,
            degradations_current: 2,
            degradations_events: 5,
            last_collect_at: Some(std::time::SystemTime::UNIX_EPOCH),
            targets_total: 6,
            checks_critical: 1,
            recovery_fatal: 1,
            ..Default::default()
        };
        let text = render_prometheus(&m);
        for name in [
            "alerts_sent",
            "probe_executed",
            "probe_skipped",
            "restarts_total",
            "state_transitions_total",
            "fatal_targets",
            "degradations_current",
            "degradations_events",
            "targets_total",
            "checks_critical",
            "recovery_fatal",
            "uptime_secs",
            "last_collect_timestamp_seconds",
        ] {
            assert!(
                text.contains(&format!("service_watch_{name} ")),
                "缺指标 {name}:\n{text}"
            );
        }
        assert!(text.contains("# TYPE service_watch_alerts_sent counter"));
        assert!(text.contains("# TYPE service_watch_fatal_targets gauge"));
        assert!(text.contains("service_watch_alerts_sent 3"));
        assert!(
            text.starts_with("# HELP "),
            "exposition format 以 HELP 开头"
        );
    }

    // -----------------------------------------------------------------
    // F22 Web 状态页(GET /)
    // -----------------------------------------------------------------

    /// `GET /`:200 + text/html + 页面标题;XSS 转义函数必须在页内。
    #[tokio::test]
    async fn root_serves_embedded_status_page() {
        let (shared, _rx, _status_tx, _metrics_tx) = shared_with(16);
        let base = spawn_server(shared).await;

        let resp = reqwest::get(format!("{base}/")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let ct = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        assert!(
            ct.starts_with("text/html"),
            "Content-Type 应为 text/html:{ct}"
        );
        let body = resp.text().await.unwrap();
        assert!(body.contains("service-watch"), "页面标题");
        assert!(
            body.contains("escapeHtml"),
            "XSS 转义必须由页内 escapeHtml 承担"
        );
        assert!(body.contains("agent unreachable"), "刷新失败提示");
        assert!(
            !body.to_lowercase().contains("reload"),
            "只读页不提供 reload 入口"
        );
    }

    // -----------------------------------------------------------------
    // F18 历史指标环形缓冲(/metrics/history)
    // -----------------------------------------------------------------

    /// 初始(eval 未发布)为空;非法 range(0/负/非数字)→ 400。
    #[tokio::test]
    async fn metrics_history_starts_empty_and_rejects_invalid_range() {
        let (shared, _rx, _status_tx, _metrics_tx) = shared_with(16);
        let base = spawn_server(shared).await;

        let resp = reqwest::get(format!("{base}/metrics/history"))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let hist: MetricsHistory = resp.json().await.unwrap();
        assert!(hist.samples.is_empty(), "eval 尚未发布,应为空");
        assert_eq!(hist.count, 0);

        for bad in ["0", "-1", "abc", "1.5", ""] {
            let resp = reqwest::get(format!("{base}/metrics/history?range={bad}"))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 400, "range={bad:?} 应 400");
        }
    }

    /// eval 发布(直推 metrics watch 模拟)后非空;窗口外样本被过滤,
    /// 窗口内按入环顺序返回。
    #[tokio::test]
    async fn metrics_history_reflects_published_eval_metrics() {
        let (shared, _rx, _status_tx, metrics_tx) = shared_with(16);
        let base = spawn_server(shared).await;

        let now = SystemTime::now();
        let mut ring = std::collections::VecDeque::new();
        for i in 0..3u64 {
            ring.push_back(MetricsSample {
                at: now - Duration::from_secs(2 - i),
                report: MetricsReport {
                    probe_executed: 42,
                    ..Default::default()
                },
            });
        }
        // 窗口外老样本(epoch,远早于缺省 300s 窗口):必须被过滤。
        ring.push_front(MetricsSample {
            at: SystemTime::UNIX_EPOCH,
            report: MetricsReport::default(),
        });
        let report = MetricsReport {
            history: Arc::new(std::sync::Mutex::new(ring)),
            ..Default::default()
        };
        metrics_tx.send(Arc::new(report)).unwrap();

        let resp = reqwest::get(format!("{base}/metrics/history"))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let hist: MetricsHistory = resp.json().await.unwrap();
        assert_eq!(hist.count, 3, "窗口外样本被过滤,窗口内全保留");
        assert_eq!(hist.samples.len(), hist.count);
        assert_eq!(hist.samples[0].report.probe_executed, 42);
        assert!(
            hist.samples.windows(2).all(|w| w[0].at <= w[1].at),
            "按入环(时间升序)返回"
        );
    }

    /// 窗口内样本 > 600 点时端点侧降采样至恰好 600(保留首尾)。
    #[tokio::test]
    async fn metrics_history_downsamples_above_600_points() {
        let (shared, _rx, _status_tx, metrics_tx) = shared_with(16);
        let base = spawn_server(shared).await;

        let now = SystemTime::now();
        let mut ring = std::collections::VecDeque::new();
        let oldest = now - Duration::from_secs(699);
        for i in 0..700u64 {
            ring.push_back(MetricsSample {
                at: oldest + Duration::from_secs(i),
                report: MetricsReport::default(),
            });
        }
        let report = MetricsReport {
            history: Arc::new(std::sync::Mutex::new(ring)),
            ..Default::default()
        };
        metrics_tx.send(Arc::new(report)).unwrap();

        // range=7200:窗口覆盖全部 700 点 → 降采样至 600,首尾保留。
        let resp = reqwest::get(format!("{base}/metrics/history?range=7200"))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let hist: MetricsHistory = resp.json().await.unwrap();
        assert_eq!(hist.count, 600);
        assert_eq!(hist.samples[0].at, oldest, "首点保留");
        assert_eq!(
            hist.samples[599].at,
            oldest + Duration::from_secs(699),
            "最新点保留"
        );
        assert!(
            hist.samples.windows(2).all(|w| w[0].at < w[1].at),
            "严格递增"
        );
    }

    // -----------------------------------------------------------------
    // F18 纯函数:range 解析 / 窗口过滤 / 等距降采样
    // -----------------------------------------------------------------

    /// `/metrics` JSON 契约不得携带环形缓冲句柄(serde skip;monitorctl 兼容
    /// 与 Prometheus 文本不受 F18 影响)。
    #[tokio::test]
    async fn metrics_json_omits_history_field() {
        let (shared, _rx, _status_tx, _metrics_tx) = shared_with(16);
        let base = spawn_server(shared).await;

        let resp = reqwest::get(format!("{base}/metrics")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let body = resp.text().await.unwrap();
        assert!(
            !body.contains("history") && !body.contains("samples"),
            "/metrics JSON 不应出现 F18 内部字段:\n{body}"
        );
    }

    fn sample_at(secs: u64) -> MetricsSample {
        MetricsSample {
            at: SystemTime::UNIX_EPOCH + Duration::from_secs(secs),
            report: MetricsReport::default(),
        }
    }

    /// range 解析:缺省 300;0/负/非数字非法;超上限收敛到 7200。
    #[test]
    fn parse_range_secs_defaults_validates_and_clamps() {
        assert_eq!(parse_range_secs(None), Some(300));
        assert_eq!(parse_range_secs(Some("1")), Some(1));
        assert_eq!(parse_range_secs(Some("7200")), Some(7200));
        assert_eq!(
            parse_range_secs(Some("99999")),
            Some(7200),
            "超限收敛到上限"
        );
        assert_eq!(parse_range_secs(Some("0")), None);
        assert_eq!(parse_range_secs(Some("-1")), None);
        assert_eq!(parse_range_secs(Some("abc")), None);
        assert_eq!(parse_range_secs(Some("1.5")), None);
        assert_eq!(parse_range_secs(Some("")), None);
    }

    /// 窗口过滤:cutoff 之后的样本按序保留,更早的丢弃。
    #[test]
    fn window_samples_keeps_only_in_range() {
        let ring = vec![sample_at(0), sample_at(100), sample_at(200), sample_at(399)];
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(400);
        let kept = window_samples(&ring, now, Duration::from_secs(300));
        let ats: Vec<u64> = kept
            .iter()
            .map(|s| {
                s.at.duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            })
            .collect();
        assert_eq!(ats, [100, 200, 399], "cutoff=100 之前的样本被过滤,保序");
    }

    /// 降采样:<600 全保留;>600 恰抽 600 且首尾保留;max<=1 保留最新。
    #[test]
    fn downsample_keeps_all_below_limit_and_evens_above() {
        assert!(downsample(&[], 600).is_empty());

        let below: Vec<MetricsSample> = (0..600).map(sample_at).collect();
        let kept = downsample(&below, 600);
        assert_eq!(kept.len(), 600, "恰 600 点全保留");
        assert_eq!(kept[599].at, sample_at(599).at);

        let above: Vec<MetricsSample> = (0..1000).map(sample_at).collect();
        let kept = downsample(&above, 600);
        assert_eq!(kept.len(), 600, "超限等距抽取 600 点");
        assert_eq!(kept[0].at, sample_at(0).at, "首点保留");
        assert_eq!(kept[599].at, sample_at(999).at, "最新点保留");
        for w in kept.windows(2) {
            assert!(w[0].at < w[1].at, "抽取索引严格递增(无重复点)");
        }

        let one = downsample(&above, 1);
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].at, sample_at(999).at, "max<=1 保留最新一点");
    }
}
