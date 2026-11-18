//! monitorctl:控制平面客户端 + 本地 dump-snapshot(DESIGN §9.2/§9.3)。
//!
//! 子命令:`status [--json]` / `restart|check|reset <target>` / `metrics [--prom]` /
//! `dump-snapshot [--config <path>]` / `watch [--interval <secs>] [--json] [--exit-on-error]` /
//! `silence <target> --duration <secs> [--reason <text>]` / `unsilence <target>` (F27) /
//! `journal [--tail <n>] [--target <name>] [--type <type>] [--since <secs>] [--json]` (F28) /
//! `config [--json]` (F33，生效配置查看，`***redacted***` 占位原样展示)。
//! `--url <addr>` 为全局参数 (默认 http://127.0.0.1:9170)。
//!
//! F23 访问令牌：全局 `--token <TOKEN>` 与 `SERVICE_WATCH_TOKEN` 环境变量
//! (flag 优先;空值视为未提供)。配置后所有请求携带 `Authorization: Bearer <token>`;
//! agent 未配置 token 时该头被忽略，行为不变。token 值不落日志。
//!
//! F32 TLS:`--url https://...` 经 reqwest rustls feature 直接可用 (与 agent
//! 端 axum-server 同为 rustls 线);全局 `--insecure` 跳过 TLS 证书校验
//! (`danger_accept_invalid_certs`),**仅自签/测试场景**使用 —— 生产部署应走
//! 信任链 (自签 CA 安装见 docs/RUNBOOK),明文校验关闭意味着中间人不可检测。
//!
//! F38 mTLS:agent 配置 `[control] client_ca_cert` 后握手要求客户端证书。
//! 全局 `--client-cert <PATH>` + `--client-key <PATH>`(成对必填，单边 →
//! 启动错误) 经 `reqwest::Identity::from_pem` 构建客户端身份，所有子命令
//! 共用 (经 build_client 注入);信任链三件套 TLS + Bearer + mTLS 可叠加 ——
//! `--token` 与 `--client-cert` 互不排斥 (见 DESIGN §9.2.2)。
//!
//! 退出码 (DESIGN §9.3):0 成功;2 运行错误 (含 409/非 2xx 响应);3 目标不存在
//! (v0.1 agent 端点不校验 target，固定 202，本码预留);4 agent 不可达。
//! monitorctl 自身日志默认静默：仅当设置 RUST_LOG 才初始化 tracing(输出到 stderr)。
//! watch 的轮询重试/内部警告经 `tracing::warn!` 输出 (受 RUST_LOG 门控);
//! 命令级致命错误 (不可达、解码失败等) 仍直接写 stderr，默认可见。
//!
//! `watch`(ROADMAP F19):客户端轮询 GET /status(不新增服务端端点),默认 ANSI 清屏
//! 渲染与 `status` 相同的表格 + 顶部时间戳/轮询计数 + 底部 `last poll`(保留最近 5 条
//! target/health/recovery 变化历史);`--json` 改为每轮输出一行紧凑 JSON(JSON Lines)。
//! Ctrl-C 退出 0;agent 不可达默认 WARN 后继续重试，`--exit-on-error` 时立即退出
//! (不可达 → 4，其余错误 → 2)。
//!
//! `silence/unsilence`(F27):薄客户端 —— duration 合法性 (0/超 7d) 与 target
//! 存在性由 agent 侧校验，202 入队后经 journal 审计确认拒绝原因。
//! `journal`(F28):GET /v1/journal 尾部查询，默认表格渲染 (时间/target/type/摘要),
//! `--json` 输出 JSON Lines;`truncated` 时 stderr 提示可缩小范围。

use std::collections::VecDeque;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Parser, Subcommand};
use monitor_core::model::control::{StatusReport, TargetStatus};

const DEFAULT_BASE_URL: &str = "http://127.0.0.1:9170";
/// 单次 HTTP 请求超时 (任务约定 5s)。
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Parser)]
#[command(name = "monitorctl", version, about = "service-watch control CLI")]
struct Cli {
    /// agent 控制平面基地址
    #[arg(long, global = true, value_name = "URL", default_value = DEFAULT_BASE_URL)]
    url: String,

    /// F23 Bearer 访问令牌 (优先于 SERVICE_WATCH_TOKEN 环境变量;空值视为未提供)。
    /// agent 未配置 token 时携带的头会被忽略。
    #[arg(long, global = true, value_name = "TOKEN")]
    token: Option<String>,

    /// F32:跳过 TLS 证书校验 (自签/测试场景;生产走信任链，见 docs/RUNBOOK)。
    /// 对 http:// URL 无影响。
    #[arg(long, global = true)]
    insecure: bool,

    /// F38:mTLS 客户端证书 PEM 路径 (与 --client-key 成对必填;对 http://
    /// URL 无影响)。agent 配置 client_ca_cert 时，控制面要求由其 CA 签发的
    /// 客户端证书 —— 缺失/不可信证书在 TLS 握手层被拒 (连接错误)。
    #[arg(long, global = true, value_name = "PATH")]
    client_cert: Option<PathBuf>,

    /// F38:mTLS 客户端私钥 PEM 路径 (与 --client-cert 成对必填;PKCS#8/
    /// PKCS#1/SEC1，密钥文件内容不落日志/错误输出)。
    #[arg(long, global = true, value_name = "PATH")]
    client_key: Option<PathBuf>,

    #[command(subcommand)]
    command: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// 查看 target 状态摘要 (GET /status)
    Status {
        /// 原样输出 agent 返回的 JSON
        #[arg(long)]
        json: bool,
    },
    /// 重启 supervise 目标 (POST /v1/restart/{target};409 = Fatal，先 reset)
    Restart { target: String },
    /// 立即触发一轮探测 (POST /v1/check/{target})
    Check { target: String },
    /// 解除 Fatal 闩锁并清空重启预算 (POST /v1/reset/{target})
    Reset { target: String },
    /// 查看运行指标 (GET /metrics;--prom 输出 Prometheus 文本)
    Metrics {
        /// Prometheus exposition 文本 (供抓取器/人工核对)
        #[arg(long)]
        prom: bool,
    },
    /// 本地单次采集输出 JSON(不经 agent;--config 仅做加载校验)
    DumpSnapshot {
        /// 配置文件路径 (加载失败按配置错误退出 1)
        #[arg(long, value_name = "PATH")]
        config: Option<PathBuf>,
    },
    /// 实时跟随 status 变化 (轮询 GET /status;Ctrl-C 退出)
    Watch {
        /// 轮询间隔秒数 (≥1)
        #[arg(
            long,
            value_name = "SECS",
            default_value_t = 2,
            value_parser = clap::value_parser!(u64).range(1..)
        )]
        interval: u64,
        /// 每次 poll 输出一行紧凑 JSON(JSON Lines，可被 jq 消费)
        #[arg(long)]
        json: bool,
        /// agent 不可达/响应错误时立即退出 (默认 WARN 后继续重试)
        #[arg(long)]
        exit_on_error: bool,
    },
    /// 静默目标通知 (POST /v1/silence/{target};F27)——窗口内 Alert/Escalate/Flap
    /// 不投递，Resolve 永不穿透;duration 合法性由 agent 侧校验 (journal 审计)
    Silence {
        target: String,
        /// 静默时长秒 (agent 侧拒绝 0 与超 7d/604800)
        #[arg(long, value_name = "SECS")]
        duration: u64,
        /// 审计原因 (journal;空/缺省 = 无原因)
        #[arg(long, value_name = "TEXT")]
        reason: Option<String>,
    },
    /// 提前解除静默 (DELETE /v1/silence/{target};F27)
    Unsilence { target: String },
    /// 查询 journal 尾部 (GET /v1/journal;F35,含轮转上一代 journal.1.jsonl)
    Journal {
        /// 尾部条数 (缺省 200，上限 1000)
        #[arg(long, value_name = "N")]
        tail: Option<usize>,
        /// 按 target 精确名过滤
        #[arg(long, value_name = "NAME")]
        target: Option<String>,
        /// 按事件类型过滤 (restart/fatal/audit/incident/...)
        #[arg(long = "type", value_name = "TYPE")]
        event_type: Option<String>,
        /// 仅显示 epoch 秒之后的事件 (含)
        #[arg(long, value_name = "SECS")]
        since: Option<u64>,
        /// 每行一条原始 JSON(JSON Lines，可被 jq 消费)
        #[arg(long)]
        json: bool,
    },
    /// 查看生效配置 (GET /v1/config;F33,脱敏快照:token/签名密钥以
    /// `***redacted***` 占位，明文不出 agent;占位不可回灌配置文件)
    Config {
        /// 原样输出 agent 返回的 JSON
        #[arg(long)]
        json: bool,
    },
}

#[tokio::main]
async fn main() {
    init_logging();
    let cli = Cli::parse();
    // F23:token 解析 (flag 优先，env 兜底;解析一次，全命令复用)。
    let env_token = std::env::var("SERVICE_WATCH_TOKEN").ok();
    let token = resolve_token(cli.token.as_deref(), env_token.as_deref());
    // F38:mTLS 客户端证书解析 (成对校验 + Identity 构建，启动期早失败 ——
    // 路径错误/不成对不发起任何请求，退出码 2 与「运行错误」同列)。
    let identity =
        match resolve_client_identity(cli.client_cert.as_deref(), cli.client_key.as_deref()) {
            Ok(id) => id,
            Err(msg) => {
                eprintln!("client cert/key error: {msg}");
                std::process::exit(2);
            }
        };
    // 注入 build_client 共享出口 (既有 cmd_* 签名与调用形态零改动;设置仅此
    // 一次，矛盾即程序错误)。
    CLIENT_IDENTITY
        .set(identity)
        .expect("client identity must be set exactly once at startup");
    let code = match cli.command {
        Cmd::Status { json } => cmd_status(&cli.url, json, token.as_deref(), cli.insecure).await,
        Cmd::Restart { target } => {
            cmd_post(&cli.url, "restart", &target, token.as_deref(), cli.insecure).await
        }
        Cmd::Check { target } => {
            cmd_post(&cli.url, "check", &target, token.as_deref(), cli.insecure).await
        }
        Cmd::Reset { target } => {
            cmd_post(&cli.url, "reset", &target, token.as_deref(), cli.insecure).await
        }
        Cmd::Metrics { prom } => cmd_metrics(&cli.url, prom, token.as_deref(), cli.insecure).await,
        Cmd::DumpSnapshot { config } => cmd_dump_snapshot(config.as_deref()),
        Cmd::Watch {
            interval,
            json,
            exit_on_error,
        } => {
            cmd_watch(
                &cli.url,
                interval,
                json,
                exit_on_error,
                token.as_deref(),
                cli.insecure,
            )
            .await
        }
        Cmd::Silence {
            target,
            duration,
            reason,
        } => {
            cmd_silence(
                &cli.url,
                &target,
                duration,
                reason.as_deref(),
                token.as_deref(),
                cli.insecure,
            )
            .await
        }
        Cmd::Unsilence { target } => {
            cmd_unsilence(&cli.url, &target, token.as_deref(), cli.insecure).await
        }
        Cmd::Journal {
            tail,
            target,
            event_type,
            since,
            json,
        } => {
            cmd_journal(
                &cli.url,
                tail,
                target.as_deref(),
                event_type.as_deref(),
                since,
                json,
                token.as_deref(),
                cli.insecure,
            )
            .await
        }
        Cmd::Config { json } => cmd_config(&cli.url, json, token.as_deref(), cli.insecure).await,
    };
    std::process::exit(code);
}

// ---------------------------------------------------------------------------
// F23 访问令牌解析
// ---------------------------------------------------------------------------

/// F23 token 解析 (纯函数，便于单测):flag 优先于环境变量;两者取「非空白」
/// 值 (空/全空白视为未提供);命中值 trim 后返回。返回 `None` = 不携带
/// Authorization 头。
fn resolve_token(flag: Option<&str>, env: Option<&str>) -> Option<String> {
    if let Some(t) = flag.map(str::trim).filter(|t| !t.is_empty()) {
        return Some(t.to_string());
    }
    env.map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_string)
}

/// 401 的人类可读提示 (F23:agent 配置了 token 而本端未带/带错时，引导排查
/// 而非只回显状态码)。不回显 token 值。
const AUTH_HINT: &str =
    "unauthorized: agent requires a bearer token; pass --token or set SERVICE_WATCH_TOKEN";

/// 为请求附加 Authorization 头 (F23):token 存在时 `Bearer <token>`。
fn with_auth(req: reqwest::RequestBuilder, token: Option<&str>) -> reqwest::RequestBuilder {
    match token {
        Some(t) => req.header(reqwest::header::AUTHORIZATION, format!("Bearer {t}")),
        None => req,
    }
}

// ---------------------------------------------------------------------------
// 子命令实现
// ---------------------------------------------------------------------------

/// 路径段 percent-encode(评审 P2:target 含 `/ ? # 空格` 会改变请求路径;
/// 仅保留 RFC 3986 unreserved + `~`,其余转义，无新增依赖)。
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

/// `metrics`:GET /metrics;--prom 时请求 Prometheus 文本并原样输出，
/// 否则 pretty JSON(按关键字段摘要 + 完整 JSON 两种形态？—— 输出完整 JSON)。
async fn cmd_metrics(base: &str, prom: bool, token: Option<&str>, insecure: bool) -> i32 {
    let client = match build_client(insecure) {
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
    match with_auth(client.get(&url), token).send().await {
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
        Ok(resp) if resp.status().as_u16() == 401 => {
            eprintln!("agent /metrics: 401 ({AUTH_HINT})");
            2
        }
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

/// `status`:GET /status;人类可读表格，--json 原样输出。
async fn cmd_status(base: &str, json: bool, token: Option<&str>, insecure: bool) -> i32 {
    let client = match build_client(insecure) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("http client error: {e}");
            return 2;
        }
    };
    let body = match fetch_status_text(&client, base, token).await {
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

/// /status 拉取错误 (status 与 watch 共用);`exit_code` 按 DESIGN §9.3 映射。
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
            // F23:401 附鉴权排查提示 (status 表格路径与 watch 重试路径共用)。
            Self::Http(401) => write!(f, "agent returned HTTP 401 ({AUTH_HINT})"),
            Self::Http(c) => write!(f, "agent returned HTTP {c}"),
        }
    }
}

/// GET `{base}/status`,成功返回响应文本;错误分类见 [`FetchStatusError`]。
async fn fetch_status_text(
    client: &reqwest::Client,
    base: &str,
    token: Option<&str>,
) -> Result<String, FetchStatusError> {
    let url = format!("{}/status", base.trim_end_matches('/'));
    match with_auth(client.get(&url), token).send().await {
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
async fn cmd_post(base: &str, op: &str, target: &str, token: Option<&str>, insecure: bool) -> i32 {
    let client = match build_client(insecure) {
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
    let resp = match with_auth(client.post(&url), token).send().await {
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
    } else if status == 401 {
        eprintln!("{op} {target}: agent returned HTTP 401 ({AUTH_HINT})");
        2
    } else {
        eprintln!("{op} {target}: agent returned HTTP {status}");
        2
    }
}

/// 命令类请求的响应处置 (silence/unsilence 与 cmd_post 同语义，共用本函数):
/// 2xx → `op label: accepted (status)` + 0;409 → 提示 + 2;401 → AUTH_HINT + 2;
/// 其余非 2xx → 2。
async fn report_op_response(op: &str, label: &str, resp: reqwest::Response) -> i32 {
    let status = resp.status().as_u16();
    if (200..300).contains(&status) {
        println!("{op} {label}: accepted ({status})");
        0
    } else if status == 409 {
        eprintln!("{op} {label}: rejected ({status})");
        2
    } else if status == 401 {
        eprintln!("{op} {label}: agent returned HTTP 401 ({AUTH_HINT})");
        2
    } else {
        eprintln!("{op} {label}: agent returned HTTP {status}");
        2
    }
}

/// `silence`(F27):POST /v1/silence/{target},JSON body `{duration, reason?}`。
/// 薄客户端:duration/target 合法性由 agent 侧校验 (拒绝走 WARN + journal 审计，
/// CLI 仍显示 accepted —— 与 restart/check 的入队语义一致)。
async fn cmd_silence(
    base: &str,
    target: &str,
    duration: u64,
    reason: Option<&str>,
    token: Option<&str>,
    insecure: bool,
) -> i32 {
    let client = match build_client(insecure) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("http client error: {e}");
            return 2;
        }
    };
    let url = format!(
        "{}/v1/silence/{}",
        base.trim_end_matches('/'),
        percent_encode_path_segment(target)
    );
    // reason 缺省时省略字段 (agent 侧 serde default → None)。
    let body = match reason {
        Some(r) => serde_json::json!({ "duration": duration, "reason": r }),
        None => serde_json::json!({ "duration": duration }),
    };
    match with_auth(client.post(&url).json(&body), token).send().await {
        Ok(resp) => report_op_response("silence", target, resp).await,
        Err(e) => {
            eprintln!("agent unreachable ({url}): {e}");
            4
        }
    }
}

/// `unsilence`(F27):DELETE /v1/silence/{target} 提前解除静默。
async fn cmd_unsilence(base: &str, target: &str, token: Option<&str>, insecure: bool) -> i32 {
    let client = match build_client(insecure) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("http client error: {e}");
            return 2;
        }
    };
    let url = format!(
        "{}/v1/silence/{}",
        base.trim_end_matches('/'),
        percent_encode_path_segment(target)
    );
    match with_auth(client.delete(&url), token).send().await {
        Ok(resp) => report_op_response("unsilence", target, resp).await,
        Err(e) => {
            eprintln!("agent unreachable ({url}): {e}");
            4
        }
    }
}

/// `GET /v1/journal` 响应 (F28):`{"truncated": bool, "lines": [...]}`。
/// 经 `serde_json::Value` 解出 (monitorctl 不直接依赖 serde derive)。
#[derive(Debug, Default)]
struct JournalReply {
    truncated: bool,
    lines: Vec<serde_json::Value>,
}

impl JournalReply {
    /// 解码响应文本：字段缺失/类型不符按缺省兜底 (空表 + 未截断)。
    fn parse(text: &str) -> Result<Self, serde_json::Error> {
        let v: serde_json::Value = serde_json::from_str(text)?;
        Ok(Self {
            truncated: v
                .get("truncated")
                .and_then(|t| t.as_bool())
                .unwrap_or(false),
            lines: v
                .get("lines")
                .and_then(|l| l.as_array())
                .cloned()
                .unwrap_or_default(),
        })
    }
}

/// /v1/journal 查询 URL 拼装 (纯函数，便于单测):target/type 值经
/// [`percent_encode_path_segment`] 转义 (复用其「仅 unreserved 字符」口径，
/// 防值中 `& = 空格` 改变查询语义)。
fn journal_query_url(
    base: &str,
    tail: Option<usize>,
    target: Option<&str>,
    event_type: Option<&str>,
    since: Option<u64>,
) -> String {
    let mut url = format!("{}/v1/journal", base.trim_end_matches('/'));
    let mut params: Vec<String> = Vec::new();
    if let Some(n) = tail {
        params.push(format!("tail={n}"));
    }
    if let Some(t) = target {
        params.push(format!("target={}", percent_encode_path_segment(t)));
    }
    if let Some(ty) = event_type {
        params.push(format!("type={}", percent_encode_path_segment(ty)));
    }
    if let Some(s) = since {
        params.push(format!("since={s}"));
    }
    if !params.is_empty() {
        url.push('?');
        url.push_str(&params.join("&"));
    }
    url
}

/// 拉取 /v1/journal 并解码 (journal 与错误分类复用 [`FetchStatusError`],
/// 退出码映射与 status 一致：不可达 4 / 其余 2)。
async fn fetch_journal_reply(
    client: &reqwest::Client,
    url: &str,
    token: Option<&str>,
) -> Result<JournalReply, FetchStatusError> {
    let resp = with_auth(client.get(url), token)
        .send()
        .await
        .map_err(|e| FetchStatusError::Unreachable(format!("agent unreachable ({url}): {e}")))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| FetchStatusError::Body(format!("read response body failed: {e}")))?;
    if !status.is_success() {
        return Err(FetchStatusError::Http(status.as_u16()));
    }
    JournalReply::parse(&text)
        .map_err(|e| FetchStatusError::Decode(format!("decode /v1/journal response failed: {e}")))
}

/// journal 行摘要 (纯函数):按事件类型取字段 —— `reason` (fatal/incident/
/// spawn_outcome 等) 优先，`message` (audit) 兜底，两者皆无 → `-`。
fn journal_summary(line: &serde_json::Value) -> String {
    let reason = line.get("reason").and_then(|v| v.as_str());
    let message = line.get("message").and_then(|v| v.as_str());
    match (reason, message) {
        (Some(r), _) => r.to_string(),
        (None, Some(m)) => m.to_string(),
        _ => "-".to_string(),
    }
}

/// 剥离 C0/C1 控制字符 (终端注入防护;P3 第十轮验证):journal reason/message
/// 源自探测错误串与运维自填 --reason,JSON 解码**不会**中和 `\u001b` 类转义
/// —— 表格路径按 chars 过滤 Cc (含 C0/DEL/C1);--json 路径经 serde 重序列化
/// 控制符已是合法 \uXX 转义，消费方按 JSON 解析，无需剥离。
fn strip_ctrl(s: &str) -> String {
    s.chars().filter(|c| !c.is_control()).collect()
}

/// journal 表格渲染 (纯函数):时间 (UTC 当日 HH:MM:SS)/target/type/摘要列，
/// 超宽经 [`clip`] 截断 —— 与 status/watch 渲染同一纪律 (纯文本 + 截断 +
/// [`strip_ctrl`] 控制字符剥离)。
fn render_journal_table(lines: &[serde_json::Value]) -> String {
    let mut out = format!(
        "{:<10} {:<20} {:<14} {:<40}",
        "TIME", "TARGET", "TYPE", "SUMMARY"
    );
    for line in lines {
        let at = line
            .get("at")
            .and_then(|v| v.get("secs_since_epoch"))
            .and_then(|v| v.as_u64());
        let time = at.map(fmt_hms).unwrap_or_else(|| "-".to_string());
        let target = line.get("target").and_then(|v| v.as_str()).unwrap_or("-");
        let ty = line.get("type").and_then(|v| v.as_str()).unwrap_or("-");
        out.push_str(&format!(
            "\n{:<10} {:<20} {:<14} {:<40}",
            time,
            clip(&strip_ctrl(target), 20),
            clip(&strip_ctrl(ty), 14),
            clip(&strip_ctrl(&journal_summary(line)), 40)
        ));
    }
    out
}

/// `journal`(F28):GET /v1/journal 尾部查询。默认表格输出
/// (时间/target/type/摘要);`--json` 每行一条原始 JSON (JSON Lines);
/// `truncated` 时 stderr 提示缩小范围;错误处置与 status 一致
/// (不可达 4 / 401 复用 token 提示 / 其余 2)。
/// F32 追加 `insecure` 透传后达 8 参 —— 薄客户端直传 CLI 解析结果，
/// 不再包一层参数对象 (与既有 cmd_* 签名风格一致)。
#[allow(clippy::too_many_arguments)]
async fn cmd_journal(
    base: &str,
    tail: Option<usize>,
    target: Option<&str>,
    event_type: Option<&str>,
    since: Option<u64>,
    json: bool,
    token: Option<&str>,
    insecure: bool,
) -> i32 {
    let client = match build_client(insecure) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("http client error: {e}");
            return 2;
        }
    };
    let url = journal_query_url(base, tail, target, event_type, since);
    let reply = match fetch_journal_reply(&client, &url, token).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{e}");
            return e.exit_code();
        }
    };
    if reply.truncated {
        eprintln!("输出被截断，可缩小范围 (--tail / --target / --type / --since)");
    }
    if json {
        for line in &reply.lines {
            match serde_json::to_string(line) {
                Ok(s) => println!("{s}"),
                Err(e) => eprintln!("serialize journal line failed: {e}"),
            }
        }
    } else {
        println!("{}", render_journal_table(&reply.lines));
    }
    0
}

// ---------------------------------------------------------------------------
// config(ROADMAP F33):GET /v1/config 生效配置查看
// ---------------------------------------------------------------------------

/// `***redacted***` 占位注脚 (F33):值原样展示，含义在此注明 ——
/// 占位**不可回灌** (写回配置文件会把字面量当作真实密钥)。
const REDACTED_NOTE: &str = "note: ***redacted*** = agent 已脱敏的敏感值 (control token / webhook 签名密钥),明文仅存于 agent 配置文件;该占位不可回灌配置\n";

/// 拉取 /v1/config 原始文本 (错误分类复用 [`FetchStatusError`],退出码映射
/// 与 status/journal 一致：不可达 4 / 401 复用 token 提示 / 其余 2)。
async fn fetch_config_text(
    client: &reqwest::Client,
    url: &str,
    token: Option<&str>,
) -> Result<String, FetchStatusError> {
    let resp = with_auth(client.get(url), token)
        .send()
        .await
        .map_err(|e| FetchStatusError::Unreachable(format!("agent unreachable ({url}): {e}")))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| FetchStatusError::Body(format!("read response body failed: {e}")))?;
    if !status.is_success() {
        return Err(FetchStatusError::Http(status.as_u16()));
    }
    Ok(text)
}

/// `config`(F33):GET /v1/config 生效配置查看。默认表格渲染
/// ([`render_config_table`]:顶层段键值 + maintenance/routes 等列表逐行，
/// `***redacted***` 占位原样展示并附注脚);`--json` 原样 pretty JSON。
async fn cmd_config(base: &str, json: bool, token: Option<&str>, insecure: bool) -> i32 {
    let client = match build_client(insecure) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("http client error: {e}");
            return 2;
        }
    };
    let url = format!("{}/v1/config", base.trim_end_matches('/'));
    let text = match fetch_config_text(&client, &url, token).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("{e}");
            return e.exit_code();
        }
    };
    let value: serde_json::Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("decode /v1/config response failed: {e}");
            return 2;
        }
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&value).unwrap_or(text));
    } else {
        println!("{}", render_config_table(&value));
    }
    0
}

/// 单值渲染 (纯函数):字符串直书 (`***redacted***` 占位原样展示),
/// 其余 (数字/布尔/null/数组/嵌套对象) 压一行 JSON。
fn fmt_config_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => strip_ctrl(s),
        other => serde_json::to_string(other).unwrap_or_else(|_| "-".into()),
    }
}

/// 快照是否含 `***redacted***` 占位 (递归扫描;命中即渲染尾部注脚)。
fn contains_redacted(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::String(s) => s.contains("***redacted***"),
        serde_json::Value::Array(items) => items.iter().any(contains_redacted),
        serde_json::Value::Object(map) => map.values().any(contains_redacted),
        _ => false,
    }
}

/// 生效配置表格渲染 (纯函数，F33):
/// - 顶层对象段:`section:` 头 + `  key = value` 逐行 (嵌套对象压一行);
/// - 顶层列表段 (maintenance/check/service/watch/alert.routes):
///   `section:` 头 + `  [i] k=v k=v ...` **逐条一行**;
/// - 空段标 `(empty)`;顶层标量直书;
/// - 快照含 `***redacted***` 时尾部追加 [`REDACTED_NOTE`] 注脚。
///
/// 非对象根 (异常响应) 退化为单行紧凑 JSON;字符串值经 [`strip_ctrl`]
/// 控制字符剥离 (与 journal/status 渲染同一纪律)。
fn render_config_table(v: &serde_json::Value) -> String {
    let Some(map) = v.as_object() else {
        return serde_json::to_string(v).unwrap_or_else(|_| "-".into());
    };
    let mut out = String::new();
    for (key, val) in map {
        match val {
            serde_json::Value::Object(inner) if !inner.is_empty() => {
                out.push_str(&format!("{key}:\n"));
                for (k, item) in inner {
                    out.push_str(&format!("  {k} = {}\n", fmt_config_value(item)));
                }
            }
            serde_json::Value::Array(items) if !items.is_empty() => {
                out.push_str(&format!("{key}:\n"));
                for (i, item) in items.iter().enumerate() {
                    match item.as_object() {
                        Some(entry) => {
                            let fields: Vec<String> = entry
                                .iter()
                                .map(|(k, val)| format!("{k}={}", fmt_config_value(val)))
                                .collect();
                            out.push_str(&format!("  [{i}] {}\n", fields.join(" ")));
                        }
                        None => out.push_str(&format!("  [{i}] {}\n", fmt_config_value(item))),
                    }
                }
            }
            other => {
                let rendered = match other {
                    // 空对象/空数组仍保留段头语义
                    serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                        format!("{key}: (empty)")
                    }
                    _ => format!("{key} = {}", fmt_config_value(other)),
                };
                out.push_str(&rendered);
                out.push('\n');
            }
        }
    }
    if contains_redacted(v) {
        out.push_str(REDACTED_NOTE);
    }
    out
}

/// `dump-snapshot`:本地单次采集 (阻塞调用，一次性 CLI 可接受),不经 agent。
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

/// /status 人类可读表格 (name/mode/health/recovery/checks/restarts),
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
// watch(ROADMAP F19):客户端轮询 GET /status，不新增服务端端点
// ---------------------------------------------------------------------------

/// 变化历史保留条数 (最近 5 条)。
const WATCH_CHANGE_HISTORY: usize = 5;

/// `watch`:循环轮询 GET /status，实时跟随状态变化 (ROADMAP F19)。
///
/// - 默认：每轮 ANSI 清屏 (`\x1b[2J\x1b[H`) 后渲染与 `status` 相同的表格
///   ([`render_status`]),顶部时间戳/轮询计数，表尾保留最近 [`WATCH_CHANGE_HISTORY`]
///   条 target/health/recovery 变化历史，底部 `last poll HH:MM:SS (n)`。
/// - `--json`:每轮输出一行紧凑 JSON(JSON Lines，可被 jq 消费);WARN 只走 stderr,
///   不污染管道。
/// - Ctrl-C → 0;错误默认 WARN 后继续重试，`--exit-on-error` 立即退出
///   (不可达 → 4，其余 → 2,DESIGN §9.3)。
async fn cmd_watch(
    base: &str,
    interval_secs: u64,
    json: bool,
    exit_on_error: bool,
    token: Option<&str>,
    insecure: bool,
) -> i32 {
    let client = match build_client(insecure) {
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
                // 注册失败实际不可达 (仅 OS 级错误),仍按信号到达处理：退出 0。
                if let Err(e) = res {
                    tracing::warn!(error = %e, "watch: ctrl-c handler error");
                }
                return 0;
            }
            res = fetch_status_text(&client, base, token) => res,
        };
        match outcome {
            Ok(text) => match serde_json::from_str::<StatusReport>(&text) {
                Ok(report) => {
                    if json {
                        match serde_json::to_string(&report) {
                            Ok(line) => println!("{line}"),
                            Err(e) => {
                                tracing::warn!(poll, error = %e, "watch: serialize report failed")
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
                    tracing::warn!(error = %e, "watch: ctrl-c handler error");
                }
                return 0;
            }
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

/// watch 错误处置:--exit-on-error → 立即按 §9.3 退出 (Some(code));
/// 默认 WARN 后继续重试 (None)。
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
        tracing::warn!(poll, error = %err, retry_in_secs = interval_secs, "watch: poll failed; retrying");
        None
    }
}

/// 追加一条带 `Δ HH:MM:SS` 时间戳的变化记录，保留最近 [`WATCH_CHANGE_HISTORY`] 条。
fn push_change(changes: &mut VecDeque<String>, desc: String, now_secs: u64) {
    changes.push_back(format!("Δ {} {desc}", fmt_hms(now_secs)));
    while changes.len() > WATCH_CHANGE_HISTORY {
        changes.pop_front();
    }
}

/// 对比两次 /status 快照，产出人类可读的变化行 (纯函数，便于单测)。
///
/// 变化类别与顺序：先按新快照顺序报 health/recovery 变化与新增 target,
/// 再按旧快照顺序报删除 target。输出不含时间戳 (调用方统一加 `Δ HH:MM:SS`)。
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

/// watch 单帧渲染：顶部时间戳/轮询计数 + status 表格 + 变化历史 + `last poll` 底栏。
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

/// 当前墙钟 unix 秒 (失败按 0;仅用于展示)。
fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// unix 秒 → UTC 当日 `HH:MM:SS`(不引入额外依赖，跨午夜按当日时间循环)。
fn fmt_hms(unix_secs: u64) -> String {
    let tod = unix_secs % 86_400;
    format!(
        "{:02}:{:02}:{:02}",
        tod / 3_600,
        (tod % 3_600) / 60,
        tod % 60
    )
}

/// 枚举 Debug 形态小写化 (supervise / running / critical ...)。
fn label<T: std::fmt::Debug>(v: T) -> String {
    format!("{v:?}").to_lowercase()
}

/// 超宽单元格截断 (表格对齐用)。
fn clip(s: &str, width: usize) -> String {
    if s.chars().count() <= width {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(width.saturating_sub(3)).collect();
        out.push_str("...");
        out
    }
}

/// 墙钟时间以 unix 秒展示 (不引入额外依赖)。
fn fmt_time(t: std::time::SystemTime) -> String {
    t.duration_since(std::time::UNIX_EPOCH)
        .map(|d| format!("{}s", d.as_secs()))
        .unwrap_or_else(|_| "-".to_string())
}

/// F38:mTLS 客户端身份的共享出口 (main 启动期一次性写入，build_client 读取;
/// `None` = 未配置客户端证书，行为与 F32 完全一致)。用 `OnceLock` 而非给
/// 全部 cmd_* 加参数:identity 在进程内全局唯一，透传只发生在 build_client
/// 一处 —— 与「flag 解析 → 启动错误 → 全命令复用」的 token 处理同构。
static CLIENT_IDENTITY: std::sync::OnceLock<Option<reqwest::Identity>> = std::sync::OnceLock::new();

/// F38 客户端身份解析 (成对 flag → `reqwest::Identity`;main 启动期调用):
/// - 双 `None` → `Ok(None)`(不带客户端证书，现状);
/// - 单边有值 → `Err`(提示成对，启动错误);
/// - 成对 → 读两文件，证书与私钥 PEM 拼接后
///   [`reqwest::Identity::from_pem`](reqwest 0.13 rustls 后端唯一 PEM 入口 ——
///   单 buffer 扫描全部 PEM 段，key 支持 PKCS#8/#1/SEC1;`from_pkcs8_pem`
///   仅 native-tls feature 提供，本 crate 未启用)。
///
/// 错误只含路径与解析原因 —— 证书/私钥内容不进错误输出 (与 agent 端
/// token/私钥同纪律)。
fn resolve_client_identity(
    cert: Option<&Path>,
    key: Option<&Path>,
) -> std::result::Result<Option<reqwest::Identity>, String> {
    match (cert, key) {
        (None, None) => Ok(None),
        (Some(_), None) | (None, Some(_)) => Err(
            "--client-cert / --client-key 必须成对提供 (mTLS 客户端证书 + 私钥);\
             仅提供一个无法构建客户端身份"
                .into(),
        ),
        (Some(cert), Some(key)) => {
            let cert_pem = std::fs::read(cert)
                .map_err(|e| format!("read client cert failed ({}): {e}", cert.display()))?;
            let key_pem = std::fs::read(key)
                .map_err(|e| format!("read client key failed ({}): {e}", key.display()))?;
            // 拼接顺序无关:from_pem 扫描整个 buffer 收集证书段与私钥段。
            let mut buf = cert_pem;
            buf.extend_from_slice(&key_pem);
            reqwest::Identity::from_pem(&buf)
                .map(Some)
                .map_err(|e| format!("build client identity failed: {e}"))
        }
    }
}

/// 构建共享 HTTP 客户端 (5s 超时)。F32:`insecure = true` 时跳过 TLS 证书
/// 校验 (`danger_accept_invalid_certs`),仅自签/测试场景 —— 生产部署应信任
/// agent 证书的 CA(自签 CA 安装见 docs/RUNBOOK);对 http:// URL 无影响。
/// `https://` 支持来自 reqwest rustls feature(与 agent 端 axum-server 同线)。
/// F38:`CLIENT_IDENTITY` 已配置时附带 mTLS 客户端证书 (agent 端
/// client_ca_cert 校验场景;远程运维/测试用)。
fn build_client(insecure: bool) -> std::result::Result<reqwest::Client, reqwest::Error> {
    let mut builder = reqwest::Client::builder().timeout(HTTP_TIMEOUT);
    if insecure {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(identity) = CLIENT_IDENTITY.get().and_then(|o| o.as_ref()) {
        builder = builder.identity(identity.clone());
    }
    builder.build()
}

/// monitorctl 默认静默：仅当设置 RUST_LOG 才初始化日志 (输出到 stderr，不污染管道)。
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

    /// 测试用 TargetStatus 构造 (identity/incident/last_transition 置空)。
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

    // ---- F23 访问令牌解析 ----

    #[test]
    fn resolve_token_flag_wins_over_env() {
        // flag 优先
        assert_eq!(
            resolve_token(Some("flag-tok"), Some("env-tok")).as_deref(),
            Some("flag-tok")
        );
        // 仅 env
        assert_eq!(
            resolve_token(None, Some("env-tok")).as_deref(),
            Some("env-tok")
        );
        // 都没有 → None(不带头)
        assert_eq!(resolve_token(None, None), None);
        // flag 空白视为未提供 → env 兜底
        assert_eq!(
            resolve_token(Some("   "), Some("env-tok")).as_deref(),
            Some("env-tok")
        );
        // env 空白视为未提供 → None
        assert_eq!(resolve_token(None, Some("  ")), None);
        // 命中值 trim
        assert_eq!(
            resolve_token(Some("  padded  "), None).as_deref(),
            Some("padded")
        );
    }

    /// 端到端 (F23):token 解析 → 请求携带 Authorization 头。手写最小 TCP stub
    /// (monitorctl 不依赖 axum):校验请求头后回 200 /status JSON，否则 401;
    /// 连续接受两笔连接 (带 token / 不带 token 各一)。
    #[tokio::test]
    async fn requests_carry_bearer_token_from_resolution() {
        // 模拟 env + flag 的最终产物
        let token = resolve_token(Some("cli-tok"), Some("env-tok")).unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = Vec::new();
                let mut byte = [0u8; 1];
                // 逐字节读到头尾空行 (测试流量小，可接受;避免额外依赖)。
                loop {
                    use tokio::io::AsyncReadExt;
                    match sock.read(&mut byte).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            buf.push(byte[0]);
                            if buf.ends_with(b"\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                let req = String::from_utf8_lossy(&buf).to_string();
                // HTTP/1.1 头名线上形态小写 (hyper 归一化) → 大小写不敏感判定。
                let authorized = req
                    .to_ascii_lowercase()
                    .contains("authorization: bearer cli-tok");
                let resp = if authorized {
                    // JSON 体 42 字节，Content-Length 必须精确 (reqwest 校验)
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 42\r\nConnection: close\r\n\r\n{\"started_at\":0,\"ready\":true,\"targets\":[]}"
                } else {
                    "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                };
                use tokio::io::AsyncWriteExt;
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });

        let client = build_client(false).unwrap();
        let base = format!("http://{addr}");
        let body = fetch_status_text(&client, &base, Some(&token))
            .await
            .expect("authorized request must succeed");
        assert!(body.contains("\"targets\":[]"));

        // 不带 token:stub 校验 Authorization 缺失 → 401 → Http(401)(提示文案)
        let err = fetch_status_text(&client, &base, None)
            .await
            .expect_err("unauthenticated request must fail");
        match err {
            FetchStatusError::Http(401) => {
                assert!(err.to_string().contains("SERVICE_WATCH_TOKEN"))
            }
            other => panic!("expected Http(401), got {other:?}"),
        }
        server.await.unwrap();
    }

    // ---- F32 --insecure / TLS 客户端 ----

    /// `--insecure` 全局 flag 解析 (子命令前后皆可，global = true);缺省 =
    /// 不跳过校验。build_client(true/false) 均可构建 —— 校验跳过的真实行为
    /// (自签可达/明文不可达) 由 monitor-core tests/tls_control.rs 的 TLS
    /// 集成测试覆盖 (同一 reqwest rustls 客户端配置)。
    #[test]
    fn parse_insecure_flag_and_client_build() {
        let cli = Cli::try_parse_from(["monitorctl", "--insecure", "status"]).unwrap();
        assert!(cli.insecure);
        let cli = Cli::try_parse_from(["monitorctl", "status", "--insecure"]).unwrap();
        assert!(cli.insecure);
        let cli = Cli::try_parse_from(["monitorctl", "status"]).unwrap();
        assert!(!cli.insecure, "缺省 = 正常证书校验");

        build_client(false).unwrap();
        build_client(true).unwrap();
    }

    // ---- F38 --client-cert/--client-key / mTLS 客户端 ----

    /// `--client-cert`/`--client-key` 全局 flag 解析 (global = true，子命令
    /// 前后皆可);缺省 = 不带客户端证书。build_client 在 CLIENT_IDENTITY
    /// 未设置时与 F32 行为一致 (OnceLock 默认 None)。
    #[test]
    fn parse_client_cert_flags() {
        let cli = Cli::try_parse_from([
            "monitorctl",
            "--client-cert",
            "/tmp/client-cert.pem",
            "--client-key",
            "/tmp/client-key.pem",
            "status",
        ])
        .unwrap();
        assert_eq!(
            cli.client_cert.as_deref(),
            Some(Path::new("/tmp/client-cert.pem"))
        );
        assert_eq!(
            cli.client_key.as_deref(),
            Some(Path::new("/tmp/client-key.pem"))
        );

        let cli =
            Cli::try_parse_from(["monitorctl", "status", "--client-key", "/tmp/k.pem"]).unwrap();
        assert_eq!(cli.client_key.as_deref(), Some(Path::new("/tmp/k.pem")));
        assert!(cli.client_cert.is_none(), "flag 可单边出现在 CLI 层");

        let cli = Cli::try_parse_from(["monitorctl", "status"]).unwrap();
        assert!(
            cli.client_cert.is_none() && cli.client_key.is_none(),
            "缺省不带"
        );

        // build_client 不依赖 CLIENT_IDENTITY 已设置 (未设置 = None = F32 行为)。
        build_client(false).unwrap();
    }

    /// F38 resolve_client_identity(纯函数级):
    /// - 双 None → Ok(None);
    /// - 单边 → Err 提示成对 (启动错误，退出码 2 的前置);
    /// - 坏文件 / 垃圾 PEM → Err 且不回显文件内容;
    /// - 真实 PEM 产物的成功路径由 monitor-core tests/tls_control.rs
    ///   `reqwest_identity_accepts_rcgen_pem` 覆盖 (同一 from_pem API;
    ///   monitorctl 无 rcgen dev 依赖)。
    #[test]
    fn resolve_client_identity_pairing_and_errors() {
        // 双 None = 不带客户端证书
        assert!(resolve_client_identity(None, None).unwrap().is_none());

        // 单边 → 成对提示
        let err = resolve_client_identity(Some(Path::new("/tmp/c.pem")), None)
            .expect_err("cert without key must be rejected");
        assert!(err.contains("成对"), "错误提示成对:{err}");
        let err = resolve_client_identity(None, Some(Path::new("/tmp/k.pem")))
            .expect_err("key without cert must be rejected");
        assert!(err.contains("成对"), "错误提示成对:{err}");

        // 文件不存在 → Err 带路径
        let dir = std::env::temp_dir().join(format!(
            "sw-ctl-ident-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let cert = dir.join("cert.pem");
        let key = dir.join("key.pem");
        // 非 base64 内容:PEM 段解析失败 (注意 from_pem 只做段解析，密钥
        // 有效性由 rustls 在连接期校验 —— 合法 base64 的伪内容不会在此拒绝)。
        let key_content =
            "-----BEGIN PRIVATE KEY-----\n!!! not base64 !!!\n-----END PRIVATE KEY-----\n";
        std::fs::write(
            &cert,
            "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        std::fs::write(&key, key_content).unwrap();
        let err = resolve_client_identity(Some(&cert), Some(&key))
            .expect_err("garbage key pem must fail");
        assert!(
            err.contains("key.pem") || err.contains("identity"),
            "错误带上下文:{err}"
        );
        assert!(!err.contains(key_content), "错误不得回显文件内容:{err}");

        // 缺文件 → Err 带缺失路径
        let err = resolve_client_identity(Some(&cert), Some(&dir.join("missing.pem")))
            .expect_err("missing key file must fail");
        assert!(err.contains("missing.pem"), "错误带缺失文件路径:{err}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- F27/F28 子命令参数解析 ----

    /// journal 子命令：全部过滤参数 + --json 正确解析;缺省全 None。
    #[test]
    fn parse_journal_flags() {
        let cli = Cli::try_parse_from([
            "monitorctl",
            "--url",
            "http://127.0.0.1:9270",
            "journal",
            "--tail",
            "5",
            "--target",
            "web-1",
            "--type",
            "fatal",
            "--since",
            "1000",
            "--json",
        ])
        .unwrap();
        let Cmd::Journal {
            tail,
            target,
            event_type,
            since,
            json,
        } = cli.command
        else {
            panic!("expected journal subcommand");
        };
        assert_eq!(tail, Some(5));
        assert_eq!(target.as_deref(), Some("web-1"));
        assert_eq!(
            event_type.as_deref(),
            Some("fatal"),
            "--type 映射到 event_type"
        );
        assert_eq!(since, Some(1000));
        assert!(json);
        assert_eq!(cli.url, "http://127.0.0.1:9270");

        // 缺省：全 None / 不带 --json
        let cli = Cli::try_parse_from(["monitorctl", "journal"]).unwrap();
        let Cmd::Journal {
            tail,
            target,
            event_type,
            since,
            json,
        } = cli.command
        else {
            panic!("expected journal subcommand");
        };
        assert!(tail.is_none() && target.is_none() && event_type.is_none() && since.is_none());
        assert!(!json);
    }

    /// silence/unsilence 子命令:target 位置参数 + --duration/--reason 解析。
    #[test]
    fn parse_silence_and_unsilence_args() {
        let cli = Cli::try_parse_from([
            "monitorctl",
            "silence",
            "web-1",
            "--duration",
            "3600",
            "--reason",
            "deploy window",
        ])
        .unwrap();
        let Cmd::Silence {
            target,
            duration,
            reason,
        } = cli.command
        else {
            panic!("expected silence subcommand");
        };
        assert_eq!(target, "web-1");
        assert_eq!(duration, 3600);
        assert_eq!(reason.as_deref(), Some("deploy window"));

        // 无 reason
        let cli = Cli::try_parse_from(["monitorctl", "silence", "db", "--duration", "60"]).unwrap();
        let Cmd::Silence {
            target,
            duration,
            reason,
            ..
        } = cli.command
        else {
            panic!("expected silence subcommand");
        };
        assert_eq!(target, "db");
        assert_eq!(duration, 60);
        assert!(reason.is_none());

        let cli = Cli::try_parse_from(["monitorctl", "unsilence", "web-1"]).unwrap();
        let Cmd::Unsilence { target } = cli.command else {
            panic!("expected unsilence subcommand");
        };
        assert_eq!(target, "web-1");
    }

    /// config 子命令 (F33):缺省表格 / `--json` 原样;全局参数 (--url) 照常解析。
    #[test]
    fn parse_config_flags() {
        let cli = Cli::try_parse_from([
            "monitorctl",
            "--url",
            "http://127.0.0.1:9270",
            "config",
            "--json",
        ])
        .unwrap();
        let Cmd::Config { json } = cli.command else {
            panic!("expected config subcommand");
        };
        assert!(json);
        assert_eq!(cli.url, "http://127.0.0.1:9270");

        // 缺省：表格输出
        let cli = Cli::try_parse_from(["monitorctl", "config"]).unwrap();
        let Cmd::Config { json } = cli.command else {
            panic!("expected config subcommand");
        };
        assert!(!json);
    }

    // ---- F33 config 表格渲染 (纯函数) ----

    /// 表格：对象段逐行 `key = value`;列表段逐条 `[i] k=v`;
    /// 空段 `(empty)`;含占位时尾部附注脚;占位值原样展示。
    #[test]
    fn render_config_table_sections_lists_and_note() {
        let v = serde_json::json!({
            "agent": { "data_dir": "./data", "journal_max_size": 10485760 },
            "control": { "enabled": true, "listen": "127.0.0.1:9170",
                         "token": "***redacted***" },
            "maintenance": [
                { "name": "nightly", "targets": ["*"], "start_utc": "18:30",
                  "duration": "30m", "days": ["mon", "fri"] }
            ],
            "alert": { "routes": [] },
            "check": [
                { "name": "api", "kind": "http", "url": "http://127.0.0.1:8080/healthz" }
            ]
        });
        let s = render_config_table(&v);
        // 对象段：键值逐行
        assert!(
            s.contains("agent:\n  data_dir = ./data"),
            "对象段渲染，got {s}"
        );
        assert!(s.contains("journal_max_size = 10485760"), "数值原样");
        assert!(s.contains("token = ***redacted***"), "占位值原样展示");
        // 列表段：逐条 [i] k=v (serde_json Map 按键排序迭代，断言按字段碎片)
        assert!(
            s.contains("maintenance:\n  [0] "),
            "maintenance 列表逐条，got {s}"
        );
        for field in [
            "name=nightly",
            "targets=[\"*\"]",
            "start_utc=18:30",
            "duration=30m",
            "days=[\"mon\",\"fri\"]",
        ] {
            assert!(s.contains(field), "maintenance 字段 {field} 渲染，got {s}");
        }
        assert!(
            s.contains("check:\n  [0] ") && s.contains("name=api"),
            "check 列表逐条"
        );
        // 空段
        assert!(s.contains("alert:\n  routes = []"), "空列表段保留语义");
        // 注脚
        assert!(s.contains(REDACTED_NOTE), "含占位时附注脚");

        // 无占位 → 无注脚
        let plain = serde_json::json!({ "agent": { "data_dir": "./data" } });
        assert!(
            !render_config_table(&plain).contains("note:"),
            "无占位无注脚"
        );

        // 非对象根：退化为紧凑 JSON
        let scalar = serde_json::json!(["x"]);
        assert_eq!(render_config_table(&scalar), "[\"x\"]");
    }

    /// 控制字符剥离与截断纪律同样适用于配置表格 (值来自配置文件，防御异常输入)。
    #[test]
    fn render_config_table_strips_ctrl_chars() {
        let v = serde_json::json!({
            "agent": { "data_dir": "./da\u{1b}ta" }
        });
        let s = render_config_table(&v);
        assert!(!s.contains('\u{1b}'), "控制字符必须剥离，got {s:?}");
        assert!(s.contains("./data"));
    }

    // ---- F28 journal 渲染与 URL 拼装 (纯函数) ----

    fn journal_line(
        type_: &str,
        secs: u64,
        target: Option<&str>,
        extra: serde_json::Value,
    ) -> serde_json::Value {
        let mut v = serde_json::json!({
            "type": type_,
            "at": { "secs_since_epoch": secs, "nanos_since_epoch": 0 },
        });
        if let Some(t) = target {
            v["target"] = serde_json::Value::String(t.into());
        }
        for (k, val) in extra.as_object().unwrap() {
            v[k] = val.clone();
        }
        v
    }

    /// 表格：时间/target/type/摘要四列;摘要按类型取 reason (fatal/incident)
    /// 或 message (audit);两者皆无 (restart) → `-`。
    #[test]
    fn render_journal_table_columns_and_summary() {
        let lines = vec![
            journal_line(
                "fatal",
                45_296,
                Some("app"),
                serde_json::json!({"reason": "budget exhausted"}),
            ),
            journal_line(
                "audit",
                45_297,
                None,
                serde_json::json!({"message": "silence set for app"}),
            ),
            journal_line(
                "restart",
                45_298,
                Some("app"),
                serde_json::json!({"fast_exit": true}),
            ),
        ];
        let s = render_journal_table(&lines);
        assert!(s.contains("TIME"), "表头");
        assert!(s.contains("TARGET"));
        assert!(s.contains("TYPE"));
        assert!(s.contains("SUMMARY"));
        assert!(s.contains("12:34:56"), "epoch 秒 → UTC HH:MM:SS");
        assert!(s.contains("app"));
        assert!(s.contains("budget exhausted"), "reason 优先");
        assert!(s.contains("silence set for app"), "audit 取 message");
        assert!(s.contains("restart"));
    }

    /// 超宽字段截断 (clip 纪律与 status 渲染一致)。
    #[test]
    fn render_journal_table_clips_wide_fields() {
        let long_reason = "x".repeat(120);
        let lines = vec![journal_line(
            "fatal",
            0,
            Some("very-long-target-name-exceeding-column"),
            serde_json::json!({ "reason": long_reason }),
        )];
        let s = render_journal_table(&lines);
        assert!(!s.contains(&"x".repeat(120)), "超长 reason 必须被截断");
        assert!(s.contains("..."), "截断以 ... 结尾");
    }

    /// URL 拼装：参数全参与缺省;值转义 (& 不得改变查询语义)。
    #[test]
    fn journal_query_url_builds_encoded_params() {
        assert_eq!(
            journal_query_url("http://a:9170/", None, None, None, None),
            "http://a:9170/v1/journal"
        );
        assert_eq!(
            journal_query_url(
                "http://a:9170",
                Some(5),
                Some("web 1&x=2"),
                Some("fatal"),
                Some(100)
            ),
            "http://a:9170/v1/journal?tail=5&target=web%201%26x%3D2&type=fatal&since=100"
        );
    }

    /// 端到端 (F28):手写 TCP stub 返回 `{"truncated":true,"lines":[...]}`,
    /// 客户端正确解码 truncated 与行集合;401 复用 token 提示分类。
    #[tokio::test]
    async fn fetch_journal_reply_decodes_stub_response() {
        let body = r#"{"truncated":true,"lines":[{"type":"fatal","at":{"secs_since_epoch":45296,"nanos_since_epoch":0},"target":"app","reason":"boom"}]}"#;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = Vec::new();
                let mut byte = [0u8; 1];
                loop {
                    use tokio::io::AsyncReadExt;
                    match sock.read(&mut byte).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            buf.push(byte[0]);
                            if buf.ends_with(b"\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                // 校验查询参数确实随请求发出 (journal_query_url → GET 行)。
                let req = String::from_utf8_lossy(&buf).to_string();
                let len = body.len();
                let resp = if req.starts_with("GET /v1/journal?tail=2&target=app HTTP/1.1") {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{body}"
                    )
                } else if req.starts_with("GET /v1/journal HTTP/1.1") {
                    "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_string()
                } else {
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
                };
                use tokio::io::AsyncWriteExt;
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });

        let client = build_client(false).unwrap();
        let base = format!("http://{addr}");
        let url = journal_query_url(&base, Some(2), Some("app"), None, None);
        let reply = fetch_journal_reply(&client, &url, None)
            .await
            .expect("stub reply must decode");
        assert!(reply.truncated);
        assert_eq!(reply.lines.len(), 1);
        assert_eq!(reply.lines[0]["target"], "app");
        assert_eq!(journal_summary(&reply.lines[0]), "boom");

        // 无查询参数路径 → 401 → Http(401)(AUTH_HINT 文案)
        let err = fetch_journal_reply(&client, &format!("{base}/v1/journal"), None)
            .await
            .expect_err("unauthenticated request must fail");
        match err {
            FetchStatusError::Http(401) => assert!(err.to_string().contains("SERVICE_WATCH_TOKEN")),
            other => panic!("expected Http(401), got {other:?}"),
        }
        server.await.unwrap();
    }

    /// 端到端 (F33):手写 TCP stub 返回 `GET /v1/config` 的脱敏快照 ——
    /// 拉取 (fetch_config_text) → 解码 → 表格渲染全链路;401 复用 token 提示
    /// 分类;响应体中的明文密钥只出现在 stub 侧 (客户端渲染为占位)。
    #[tokio::test]
    async fn fetch_config_text_and_render_stub_roundtrip() {
        let body = r#"{"control":{"token":"***redacted***","listen":"127.0.0.1:9170"},"maintenance":[{"name":"nightly","targets":["*"],"start_utc":"18:30","duration":"30m"}],"alert":{"webhook_secret":"***redacted***","routes":[]}}"#;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = Vec::new();
                let mut byte = [0u8; 1];
                loop {
                    use tokio::io::AsyncReadExt;
                    match sock.read(&mut byte).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            buf.push(byte[0]);
                            if buf.ends_with(b"\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                let req = String::from_utf8_lossy(&buf).to_string();
                let len = body.len();
                // F23 语义：带 Authorization 头 → 200，缺失 → 401 (头名线上形态
                // 小写化，大小写不敏感判定，与 F23 端到端测试同口径)。
                let resp = if req.starts_with("GET /v1/config HTTP/1.1")
                    && req.to_ascii_lowercase().contains("authorization:")
                {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{body}"
                    )
                } else {
                    "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_string()
                };
                use tokio::io::AsyncWriteExt;
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });

        let client = build_client(false).unwrap();
        let base = format!("http://{addr}");
        let text = fetch_config_text(&client, &format!("{base}/v1/config"), Some("tok"))
            .await
            .expect("stub config must fetch");
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        let rendered = render_config_table(&value);
        assert!(
            rendered.contains("token = ***redacted***"),
            "占位值原样渲染，got {rendered}"
        );
        assert!(rendered.contains("name=nightly"), "maintenance 列表逐条");
        assert!(rendered.contains(REDACTED_NOTE), "占位注脚");
        assert!(!rendered.contains("明文不应出现"));

        // 无 token 请求 401 路径 → Http(401)(AUTH_HINT 文案)
        let err = fetch_config_text(&client, &format!("{base}/v1/config"), None)
            .await
            .expect_err("second stub connection replies 401");
        match err {
            FetchStatusError::Http(401) => assert!(err.to_string().contains("SERVICE_WATCH_TOKEN")),
            other => panic!("expected Http(401), got {other:?}"),
        }
        server.await.unwrap();
    }
}
