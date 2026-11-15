//! monitorctl:控制平面客户端 + 本地 dump-snapshot(DESIGN §9.2/§9.3)。
//!
//! 子命令:`status [--json]` / `restart|check|reset <target>` / `metrics [--prom]` /
//! `dump-snapshot [--config <path>]` / `watch [--interval <secs>] [--json] [--exit-on-error]`。
//! `--url <addr>` 为全局参数(默认 http://127.0.0.1:9170)。
//!
//! 退出码(DESIGN §9.3):0 成功;2 运行错误(含 409/非 2xx 响应);3 目标不存在
//! (v0.1 agent 端点不校验 target,固定 202,本码预留);4 agent 不可达。
//! monitorctl 自身日志默认静默:仅当设置 RUST_LOG 才初始化 tracing(输出到 stderr)。
//!
//! `watch`(ROADMAP F19):客户端轮询 GET /status(不新增服务端端点),默认 ANSI 清屏
//! 渲染与 `status` 相同的表格 + 顶部时间戳/轮询计数 + 底部 `last poll`(保留最近 5 条
//! target/health/recovery 变化历史);`--json` 改为每轮输出一行紧凑 JSON(JSON Lines)。
//! Ctrl-C 退出 0;agent 不可达默认 WARN 后继续重试,`--exit-on-error` 时立即退出
//! (不可达 → 4,其余错误 → 2)。

use std::collections::VecDeque;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Parser, Subcommand};
use monitor_core::model::control::{StatusReport, TargetStatus};

const DEFAULT_BASE_URL: &str = "http://127.0.0.1:9170";
/// 单次 HTTP 请求超时(任务约定 5s)。
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Parser)]
#[command(name = "monitorctl", version, about = "service-watch control CLI")]
struct Cli {
    /// agent 控制平面基地址
    #[arg(long, global = true, value_name = "URL", default_value = DEFAULT_BASE_URL)]
    url: String,

    #[command(subcommand)]
    command: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// 查看 target 状态摘要(GET /status)
    Status {
        /// 原样输出 agent 返回的 JSON
        #[arg(long)]
        json: bool,
    },
    /// 重启 supervise 目标(POST /v1/restart/{target};409 = Fatal,先 reset)
    Restart { target: String },
    /// 立即触发一轮探测(POST /v1/check/{target})
    Check { target: String },
    /// 解除 Fatal 闩锁并清空重启预算(POST /v1/reset/{target})
    Reset { target: String },
    /// 查看运行指标(GET /metrics;--prom 输出 Prometheus 文本)
    Metrics {
        /// Prometheus exposition 文本(供抓取器/人工核对)
        #[arg(long)]
        prom: bool,
    },
    /// 本地单次采集输出 JSON(不经 agent;--config 仅做加载校验)
    DumpSnapshot {
        /// 配置文件路径(加载失败按配置错误退出 1)
        #[arg(long, value_name = "PATH")]
        config: Option<PathBuf>,
    },
    /// 实时跟随 status 变化(轮询 GET /status;Ctrl-C 退出)
    Watch {
        /// 轮询间隔秒数(≥1)
        #[arg(
            long,
            value_name = "SECS",
            default_value_t = 2,
            value_parser = clap::value_parser!(u64).range(1..)
        )]
        interval: u64,
        /// 每次 poll 输出一行紧凑 JSON(JSON Lines,可被 jq 消费)
        #[arg(long)]
        json: bool,
        /// agent 不可达/响应错误时立即退出(默认 WARN 后继续重试)
        #[arg(long)]
        exit_on_error: bool,
    },
}

#[tokio::main]
async fn main() {
    init_logging();
    let cli = Cli::parse();
    let code = match cli.command {
        Cmd::Status { json } => cmd_status(&cli.url, json).await,
        Cmd::Restart { target } => cmd_post(&cli.url, "restart", &target).await,
        Cmd::Check { target } => cmd_post(&cli.url, "check", &target).await,
        Cmd::Reset { target } => cmd_post(&cli.url, "reset", &target).await,
        Cmd::Metrics { prom } => cmd_metrics(&cli.url, prom).await,
        Cmd::DumpSnapshot { config } => cmd_dump_snapshot(config.as_deref()),
        Cmd::Watch {
            interval,
            json,
            exit_on_error,
        } => cmd_watch(&cli.url, interval, json, exit_on_error).await,
    };
    std::process::exit(code);
}

// ---------------------------------------------------------------------------
// 子命令实现
// ---------------------------------------------------------------------------

/// 路径段 percent-encode(评审 P2:target 含 `/ ? # 空格` 会改变请求路径;
/// 仅保留 RFC 3986 unreserved + `~`,其余转义,无新增依赖)。
fn percent_encode_path_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*b as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// `metrics`:GET /metrics;--prom 时请求 Prometheus 文本并原样输出,
/// 否则 pretty JSON(按关键字段摘要 + 完整 JSON 两种形态?—— 输出完整 JSON)。
async fn cmd_metrics(base: &str, prom: bool) -> i32 {
    let client = match build_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("client build failed: {e}");
            return 2;
        }
    };
    let url = format!(
        "{}/metrics{}",
        base.trim_end_matches('/'),
        if prom { "?format=prometheus" } else { "" }
    );
    match client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => match resp.text().await {
            Ok(text) => {
                if prom {
                    println!("{text}");
                } else {
                    match serde_json::from_str::<serde_json::Value>(&text) {
                        Ok(v) => println!("{}", serde_json::to_string_pretty(&v).unwrap_or(text)),
                        Err(_) => println!("{text}"),
                    }
                }
                0
            }
            Err(e) => {
                eprintln!("read body failed: {e}");
                2
            }
        },
        Ok(resp) => {
            eprintln!("agent /metrics: {}", resp.status());
            2
        }
        Err(e) => {
            eprintln!("agent unreachable ({url}): {e}");
            4
        }
    }
}

/// `status`:GET /status;人类可读表格,--json 原样输出。
async fn cmd_status(base: &str, json: bool) -> i32 {
    let client = match build_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("http client error: {e}");
            return 2;
        }
    };
    let body = match fetch_status_text(&client, base).await {
        Ok(text) => text,
        Err(e) => {
            eprintln!("{e}");
            return e.exit_code();
        }
    };

    if json {
        println!("{body}");
        return 0;
    }
    let report: StatusReport = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("decode /status response failed: {e}");
            return 2;
        }
    };
    println!("{}", render_status(&report));
    0
}

/// /status 拉取错误(status 与 watch 共用);`exit_code` 按 DESIGN §9.3 映射。
#[derive(Debug)]
enum FetchStatusError {
    /// 连接/请求失败 → 4
    Unreachable(String),
    /// agent 返回非 2xx → 2
    Http(u16),
    /// 读 body 失败 → 2
    Body(String),
    /// JSON 解码失败 → 2
    Decode(String),
}

impl FetchStatusError {
    fn exit_code(&self) -> i32 {
        match self {
            Self::Unreachable(_) => 4,
            _ => 2,
        }
    }
}

impl std::fmt::Display for FetchStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreachable(m) | Self::Body(m) | Self::Decode(m) => f.write_str(m),
            Self::Http(c) => write!(f, "agent returned HTTP {c}"),
        }
    }
}

/// GET `{base}/status`,成功返回响应文本;错误分类见 [`FetchStatusError`]。
async fn fetch_status_text(
    client: &reqwest::Client,
    base: &str,
) -> Result<String, FetchStatusError> {
    let url = format!("{}/status", base.trim_end_matches('/'));
    match client.get(&url).send().await {
        Ok(resp) => {
            let status = resp.status();
            match resp.text().await {
                Ok(text) if status.is_success() => Ok(text),
                Ok(_) => Err(FetchStatusError::Http(status.as_u16())),
                Err(e) => Err(FetchStatusError::Body(format!(
                    "read response body failed: {e}"
                ))),
            }
        }
        Err(e) => Err(FetchStatusError::Unreachable(format!(
            "agent unreachable ({url}): {e}"
        ))),
    }
}

/// `restart|check|reset`:POST /v1/{op}/{target};非 2xx → 2(409 附提示),连接失败 → 4。
async fn cmd_post(base: &str, op: &str, target: &str) -> i32 {
    let client = match build_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("http client error: {e}");
            return 2;
        }
    };
    let url = format!(
        "{}/v1/{op}/{}",
        base.trim_end_matches('/'),
        percent_encode_path_segment(target)
    );
    let resp = match client.post(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("agent unreachable ({url}): {e}");
            return 4;
        }
    };
    let status = resp.status().as_u16();
    if (200..300).contains(&status) {
        println!("{op} {target}: accepted ({status})");
        0
    } else if status == 409 {
        eprintln!(
            "{op} {target}: rejected ({status}); target is in fatal state — run `monitorctl reset {target}` first"
        );
        2
    } else {
        eprintln!("{op} {target}: agent returned HTTP {status}");
        2
    }
}

/// `dump-snapshot`:本地单次采集(阻塞调用,一次性 CLI 可接受),不经 agent。
fn cmd_dump_snapshot(config: Option<&Path>) -> i32 {
    if let Some(path) = config
        && let Err(e) = monitor_core::config::Config::from_path(path)
    {
        eprintln!("config error: {e}");
        return 1;
    }
    match monitor_core::runtime::collect_snapshot_json(&[]) {
        Ok(json) => {
            println!("{json}");
            0
        }
        Err(e) => {
            eprintln!("collect failed: {e}");
            2
        }
    }
}

// ---------------------------------------------------------------------------
// 输出与工具
// ---------------------------------------------------------------------------

/// /status 人类可读表格(name/mode/health/recovery/checks/restarts),
/// status 与 watch 共用;返回不带结尾换行的完整文本。
fn render_status(report: &StatusReport) -> String {
    let mut out = format!(
        "ready={} started={} targets={}",
        report.ready,
        fmt_time(report.started_at),
        report.targets.len()
    );
    if report.targets.is_empty() {
        out.push_str("\n(no targets reported)");
        return out;
    }
    out.push_str(&format!(
        "\n{:<22} {:<10} {:<8} {:<12} {:<26} {:>8}",
        "TARGET", "MODE", "HEALTH", "RECOVERY", "CHECKS", "RESTARTS"
    ));
    for t in &report.targets {
        out.push_str(&format!(
            "\n{:<22} {:<10} {:<8} {:<12} {:<26} {:>8}",
            clip(&t.name, 22),
            label(t.mode),
            label(t.health),
            t.recovery.map(label).unwrap_or_else(|| "-".to_string()),
            clip(&t.checks.join(","), 26),
            t.restarts_in_window
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// watch(ROADMAP F19):客户端轮询 GET /status,不新增服务端端点
// ---------------------------------------------------------------------------

/// 变化历史保留条数(最近 5 条)。
const WATCH_CHANGE_HISTORY: usize = 5;

/// `watch`:循环轮询 GET /status,实时跟随状态变化(ROADMAP F19)。
///
/// - 默认:每轮 ANSI 清屏(`\x1b[2J\x1b[H`)后渲染与 `status` 相同的表格
///   ([`render_status`]),顶部时间戳/轮询计数,表尾保留最近 [`WATCH_CHANGE_HISTORY`]
///   条 target/health/recovery 变化历史,底部 `last poll HH:MM:SS (n)`。
/// - `--json`:每轮输出一行紧凑 JSON(JSON Lines,可被 jq 消费);WARN 只走 stderr,
///   不污染管道。
/// - Ctrl-C → 0;错误默认 WARN 后继续重试,`--exit-on-error` 立即退出
///   (不可达 → 4,其余 → 2,DESIGN §9.3)。
async fn cmd_watch(base: &str, interval_secs: u64, json: bool, exit_on_error: bool) -> i32 {
    let client = match build_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("http client error: {e}");
            return 2;
        }
    };
    // tokio ≥1.4x:ctrl_c() 直接返回 Future(Output = io::Result<()>),pin 后供 select 复用。
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    let wait = Duration::from_secs(interval_secs);

    let mut poll: u64 = 0;
    let mut last_report: Option<StatusReport> = None;
    let mut changes: VecDeque<String> = VecDeque::new();
    loop {
        poll += 1;
        let outcome = tokio::select! {
            res = &mut ctrl_c => {
                // 注册失败实际不可达(仅 OS 级错误),仍按信号到达处理:退出 0。
                if let Err(e) = res {
                    eprintln!("watch: ctrl-c handler error: {e}");
                }
                return 0;
            }
            res = fetch_status_text(&client, base) => res,
        };
        match outcome {
            Ok(text) => match serde_json::from_str::<StatusReport>(&text) {
                Ok(report) => {
                    if json {
                        match serde_json::to_string(&report) {
                            Ok(line) => println!("{line}"),
                            Err(e) => {
                                eprintln!("watch WARN (poll #{poll}): serialize report failed: {e}")
                            }
                        }
                    } else {
                        let now = now_unix();
                        if let Some(prev) = &last_report {
                            for desc in status_diff(prev, &report) {
                                push_change(&mut changes, desc, now);
                            }
                        }
                        print!("\x1b[2J\x1b[H");
                        println!(
                            "{}",
                            render_watch_frame(
                                &report,
                                poll,
                                interval_secs,
                                now,
                                changes.make_contiguous()
                            )
                        );
                        let _ = std::io::stdout().flush();
                    }
                    last_report = Some(report);
                }
                Err(e) => {
                    let err =
                        FetchStatusError::Decode(format!("decode /status response failed: {e}"));
                    if let Some(code) = watch_error(exit_on_error, &err, poll, interval_secs) {
                        return code;
                    }
                }
            },
            Err(e) => {
                if let Some(code) = watch_error(exit_on_error, &e, poll, interval_secs) {
                    return code;
                }
            }
        }
        tokio::select! {
            res = &mut ctrl_c => {
                if let Err(e) = res {
                    eprintln!("watch: ctrl-c handler error: {e}");
                }
                return 0;
            }
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

/// watch 错误处置:--exit-on-error → 立即按 §9.3 退出(Some(code));
/// 默认 WARN 后继续重试(None)。
fn watch_error(
    exit_on_error: bool,
    err: &FetchStatusError,
    poll: u64,
    interval_secs: u64,
) -> Option<i32> {
    if exit_on_error {
        eprintln!("watch: {err} — exiting (--exit-on-error, poll #{poll})");
        Some(err.exit_code())
    } else {
        eprintln!("watch WARN (poll #{poll}): {err}; retrying in {interval_secs}s");
        None
    }
}

/// 追加一条带 `Δ HH:MM:SS` 时间戳的变化记录,保留最近 [`WATCH_CHANGE_HISTORY`] 条。
fn push_change(changes: &mut VecDeque<String>, desc: String, now_secs: u64) {
    changes.push_back(format!("Δ {} {desc}", fmt_hms(now_secs)));
    while changes.len() > WATCH_CHANGE_HISTORY {
        changes.pop_front();
    }
}

/// 对比两次 /status 快照,产出人类可读的变化行(纯函数,便于单测)。
///
/// 变化类别与顺序:先按新快照顺序报 health/recovery 变化与新增 target,
/// 再按旧快照顺序报删除 target。输出不含时间戳(调用方统一加 `Δ HH:MM:SS`)。
fn status_diff(old: &StatusReport, new: &StatusReport) -> Vec<String> {
    let old_map: std::collections::HashMap<&str, &TargetStatus> =
        old.targets.iter().map(|t| (t.name.as_str(), t)).collect();
    let new_names: std::collections::HashSet<&str> =
        new.targets.iter().map(|t| t.name.as_str()).collect();

    let mut out = Vec::new();
    for t in &new.targets {
        match old_map.get(t.name.as_str()) {
            None => out.push(format!(
                "{}: + added ({}, health={})",
                t.name,
                label(t.mode),
                label(t.health)
            )),
            Some(prev) => {
                if prev.health != t.health {
                    out.push(format!(
                        "{}: health {}→{}",
                        t.name,
                        label(prev.health),
                        label(t.health)
                    ));
                }
                match (prev.recovery, t.recovery) {
                    (Some(a), Some(b)) if a != b => {
                        out.push(format!("{}: recovery {}→{}", t.name, label(a), label(b)))
                    }
                    (None, Some(b)) => out.push(format!("{}: recovery -→{}", t.name, label(b))),
                    (Some(a), None) => out.push(format!("{}: recovery {}→-", t.name, label(a))),
                    _ => {}
                }
            }
        }
    }
    for t in &old.targets {
        if !new_names.contains(t.name.as_str()) {
            out.push(format!("{}: - removed", t.name));
        }
    }
    out
}

/// watch 单帧渲染:顶部时间戳/轮询计数 + status 表格 + 变化历史 + `last poll` 底栏。
fn render_watch_frame(
    report: &StatusReport,
    poll: u64,
    interval_secs: u64,
    now_secs: u64,
    changes: &[String],
) -> String {
    let mut out = format!(
        "watch {} poll #{poll} interval={interval_secs}s\n",
        fmt_hms(now_secs)
    );
    out.push_str(&render_status(report));
    if !changes.is_empty() {
        out.push_str("\nrecent changes:");
        for c in changes {
            out.push_str(&format!("\n  {c}"));
        }
    }
    out.push_str(&format!("\nlast poll {} ({poll})", fmt_hms(now_secs)));
    out
}

/// 当前墙钟 unix 秒(失败按 0;仅用于展示)。
fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// unix 秒 → UTC 当日 `HH:MM:SS`(不引入额外依赖,跨午夜按当日时间循环)。
fn fmt_hms(unix_secs: u64) -> String {
    let tod = unix_secs % 86_400;
    format!(
        "{:02}:{:02}:{:02}",
        tod / 3_600,
        (tod % 3_600) / 60,
        tod % 60
    )
}

/// 枚举 Debug 形态小写化(supervise / running / critical ...)。
fn label<T: std::fmt::Debug>(v: T) -> String {
    format!("{v:?}").to_lowercase()
}

/// 超宽单元格截断(表格对齐用)。
fn clip(s: &str, width: usize) -> String {
    if s.chars().count() <= width {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(width.saturating_sub(3)).collect();
        out.push_str("...");
        out
    }
}

/// 墙钟时间以 unix 秒展示(不引入额外依赖)。
fn fmt_time(t: std::time::SystemTime) -> String {
    t.duration_since(std::time::UNIX_EPOCH)
        .map(|d| format!("{}s", d.as_secs()))
        .unwrap_or_else(|_| "-".to_string())
}

fn build_client() -> std::result::Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder().timeout(HTTP_TIMEOUT).build()
}

/// monitorctl 默认静默:仅当设置 RUST_LOG 才初始化日志(输出到 stderr,不污染管道)。
fn init_logging() {
    if std::env::var_os("RUST_LOG").is_some() {
        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use monitor_core::model::control::{HealthLevel, RecoveryState, TargetMode};
    use std::time::UNIX_EPOCH;

    /// 测试用 TargetStatus 构造(identity/incident/last_transition 置空)。
    fn tgt(
        name: &str,
        mode: TargetMode,
        health: HealthLevel,
        recovery: Option<RecoveryState>,
    ) -> TargetStatus {
        TargetStatus {
            name: name.to_string(),
            mode,
            health,
            recovery,
            checks: vec!["http".to_string()],
            identity: None,
            incident: None,
            restarts_in_window: 0,
            last_transition: None,
        }
    }

    fn report(targets: Vec<TargetStatus>) -> StatusReport {
        StatusReport {
            started_at: UNIX_EPOCH,
            ready: true,
            targets,
        }
    }

    // ---- status_diff(纯函数)----

    #[test]
    fn status_diff_no_change_returns_empty() {
        let r = report(vec![
            tgt("web", TargetMode::Attach, HealthLevel::Ok, None),
            tgt(
                "db",
                TargetMode::Supervise,
                HealthLevel::Warning,
                Some(RecoveryState::Running),
            ),
        ]);
        assert!(status_diff(&r, &r).is_empty());
    }

    #[test]
    fn status_diff_health_change() {
        let old = report(vec![tgt(
            "c1",
            TargetMode::Check,
            HealthLevel::Warning,
            None,
        )]);
        let new = report(vec![tgt(
            "c1",
            TargetMode::Check,
            HealthLevel::Critical,
            None,
        )]);
        assert_eq!(status_diff(&old, &new), vec!["c1: health warning→critical"]);
    }

    #[test]
    fn status_diff_recovery_change() {
        // supervise:running → fatal(进 Fatal 闩锁)
        let old = report(vec![tgt(
            "db",
            TargetMode::Supervise,
            HealthLevel::Critical,
            Some(RecoveryState::Running),
        )]);
        let new = report(vec![tgt(
            "db",
            TargetMode::Supervise,
            HealthLevel::Critical,
            Some(RecoveryState::Fatal),
        )]);
        assert_eq!(status_diff(&old, &new), vec!["db: recovery running→fatal"]);

        // 非 supervise 拿到 recovery(异常侧路):None → Some / Some → None 也要报
        let old2 = report(vec![tgt("x", TargetMode::Attach, HealthLevel::Ok, None)]);
        let new2 = report(vec![tgt(
            "x",
            TargetMode::Attach,
            HealthLevel::Ok,
            Some(RecoveryState::Starting),
        )]);
        assert_eq!(status_diff(&old2, &new2), vec!["x: recovery -→starting"]);
        assert_eq!(status_diff(&new2, &old2), vec!["x: recovery starting→-"]);
    }

    #[test]
    fn status_diff_target_added_and_removed() {
        let old = report(vec![tgt("a", TargetMode::Check, HealthLevel::Ok, None)]);
        let new = report(vec![
            tgt(
                "b",
                TargetMode::Supervise,
                HealthLevel::Warning,
                Some(RecoveryState::Starting),
            ),
            tgt("a", TargetMode::Check, HealthLevel::Ok, None),
        ]);
        assert_eq!(
            status_diff(&old, &new),
            vec!["b: + added (supervise, health=warning)"]
        );
        assert_eq!(status_diff(&new, &old), vec!["b: - removed"]);
    }

    #[test]
    fn status_diff_combined_is_ordered_deterministic() {
        let old = report(vec![
            tgt("gone", TargetMode::Check, HealthLevel::Ok, None),
            tgt("c1", TargetMode::Check, HealthLevel::Ok, None),
        ]);
        let new = report(vec![
            tgt("c1", TargetMode::Check, HealthLevel::Critical, None),
            tgt("fresh", TargetMode::Check, HealthLevel::Ok, None),
        ]);
        assert_eq!(
            status_diff(&old, &new),
            vec![
                "c1: health ok→critical",
                "fresh: + added (check, health=ok)",
                "gone: - removed",
            ]
        );
        // 同输入重复调用结果一致
        assert_eq!(status_diff(&old, &new), status_diff(&old, &new));
    }

    // ---- render_status(渲染复用)----

    #[test]
    fn render_status_empty_targets() {
        let s = render_status(&report(vec![]));
        assert!(s.contains("ready=true"));
        assert!(s.contains("targets=0"));
        assert!(s.contains("(no targets reported)"));
        assert!(!s.contains("TARGET"));
    }

    #[test]
    fn render_status_fatal_badge_and_columns() {
        let r = report(vec![tgt(
            "db",
            TargetMode::Supervise,
            HealthLevel::Critical,
            Some(RecoveryState::Fatal),
        )]);
        let s = render_status(&r);
        for col in ["TARGET", "MODE", "HEALTH", "RECOVERY", "CHECKS", "RESTARTS"] {
            assert!(s.contains(col), "missing column header {col}");
        }
        assert!(s.contains("fatal"), "recovery Fatal 徽章缺失");
        assert!(s.contains("supervise"));
        assert!(s.contains("critical"));
        assert!(s.contains("http"));
    }

    // ---- watch 辅助 ----

    #[test]
    fn push_change_keeps_last_five() {
        let mut changes = VecDeque::new();
        for i in 0..7 {
            push_change(&mut changes, format!("t{i}: health ok→warning"), 3_600 + i);
        }
        assert_eq!(changes.len(), WATCH_CHANGE_HISTORY);
        assert!(changes.front().unwrap().contains("t2:"));
        assert!(changes.back().unwrap().contains("t6:"));
        assert!(changes.front().unwrap().starts_with("Δ 01:"));
    }

    #[test]
    fn render_watch_frame_header_history_footer() {
        let mut changes = VecDeque::new();
        push_change(
            &mut changes,
            "c1: health warning→critical".to_string(),
            45_296,
        );
        let s = render_watch_frame(&report(vec![]), 3, 2, 45_297, changes.make_contiguous());
        assert!(s.contains("watch 12:34:57 poll #3 interval=2s"));
        assert!(s.contains("(no targets reported)"));
        assert!(s.contains("recent changes:"));
        assert!(s.contains("Δ 12:34:56 c1: health warning→critical"));
        assert!(s.contains("last poll 12:34:57 (3)"));
        // 无变化历史时不出现 recent changes 块
        let plain = render_watch_frame(&report(vec![]), 1, 2, 0, &[]);
        assert!(!plain.contains("recent changes"));
        assert!(plain.contains("last poll 00:00:00 (1)"));
    }

    #[test]
    fn fmt_hms_formats_utc_time_of_day() {
        assert_eq!(fmt_hms(0), "00:00:00");
        assert_eq!(fmt_hms(45_296), "12:34:56");
        assert_eq!(fmt_hms(86_399), "23:59:59");
        assert_eq!(fmt_hms(86_400), "00:00:00");
    }

    #[test]
    fn fetch_error_exit_codes_follow_design_9_3() {
        use FetchStatusError as E;
        assert_eq!(E::Unreachable("x".into()).exit_code(), 4);
        assert_eq!(E::Http(500).exit_code(), 2);
        assert_eq!(E::Body("x".into()).exit_code(), 2);
        assert_eq!(E::Decode("x".into()).exit_code(), 2);
    }
}
