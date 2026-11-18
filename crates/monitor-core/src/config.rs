//! 配置系统 (DESIGN.md §4，权威 schema)。
//!
//! 解析 + 校验两层:serde 直接反序列化到强类型结构 (`deny_unknown_fields`),
//! 随后 [`Config::validate`] 执行交叉校验 (§4.3)。违反任一规则即配置错误 (退出码 1)。

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::{CoreError, Result};

/// 人类可读时长解析/序列化 (`"500ms" | "5s" | "10m" | "1h"`,可组合如 `"1h30m"`)。
pub mod dur {
    use super::*;
    use serde::de::Error as _;

    pub fn parse(s: &str) -> std::result::Result<Duration, String> {
        let s = s.trim();
        if s.is_empty() {
            return Err("empty duration".into());
        }
        let mut total = Duration::ZERO;
        let mut num = String::new();
        let mut unit = String::new();
        let mut chars = s.chars().peekable();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() || c == '.' {
                if !unit.is_empty() {
                    total = total
                        .checked_add(parse_part(&num, &unit)?)
                        .ok_or("duration overflow")?;
                    num.clear();
                    unit.clear();
                }
                num.push(c);
            } else if c.is_ascii_alphabetic() {
                unit.push(c);
            } else {
                return Err(format!("invalid character {c:?} in duration"));
            }
            chars.next();
        }
        if !num.is_empty() {
            total = total
                .checked_add(parse_part(&num, &unit)?)
                .ok_or("duration overflow")?;
        }
        if total.is_zero() {
            return Err("duration must be > 0".into());
        }
        if std::time::Instant::now().checked_add(total).is_none() {
            return Err("duration exceeds monotonic clock range".into());
        }
        Ok(total)
    }

    fn parse_part(num: &str, unit: &str) -> std::result::Result<Duration, String> {
        let v: f64 = num.parse().map_err(|_| format!("invalid number {num:?}"))?;
        if v < 0.0 {
            return Err("duration must be positive".into());
        }
        // 巨大数值经乘法可溢出为 inf;from_secs_f64 对超域/inf 会 panic,
        // 这里统一以 is_finite/try_from 拦截为配置错误 (DESIGN §4.3:退出码 1，非 panic)。
        let secs = match unit {
            "ms" => v / 1000.0,
            "s" | "" => v,
            "m" => v * 60.0,
            "h" => v * 3600.0,
            other => return Err(format!("unknown duration unit {other:?} (use ms/s/m/h)")),
        };
        if !secs.is_finite() {
            return Err("duration overflow".into());
        }
        Duration::try_from_secs_f64(secs).map_err(|_| "duration value out of range".into())
    }

    pub fn serialize<S: serde::Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        let text = if d.as_secs() == 0 && d.subsec_nanos().is_multiple_of(1_000_000) {
            format!("{}ms", d.as_millis())
        } else if d.subsec_nanos() == 0 {
            let secs = d.as_secs();
            if secs > 0 && secs.is_multiple_of(3600) {
                format!("{}h", secs / 3600)
            } else if secs > 0 && secs.is_multiple_of(60) {
                format!("{}m", secs / 60)
            } else {
                format!("{secs}s")
            }
        } else {
            format!("{}s", d.as_secs_f64())
        };
        s.serialize_str(&text)
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let s = String::deserialize(d)?;
        parse(&s).map_err(D::Error::custom)
    }

    // ---- 各字段默认值 (DESIGN.md §4.2 默认值总表) ----
    pub fn default_collect_interval() -> Duration {
        Duration::from_secs(5)
    }
    pub fn default_check_interval() -> Duration {
        Duration::from_secs(10)
    }
    pub fn default_check_timeout() -> Duration {
        Duration::from_secs(5)
    }
    pub fn default_alert_timeout() -> Duration {
        Duration::from_secs(10)
    }
    pub fn default_renotify() -> Duration {
        Duration::from_secs(15 * 60)
    }
    pub fn default_flap_window() -> Duration {
        Duration::from_secs(30 * 60)
    }
    pub fn default_restart_window() -> Duration {
        Duration::from_secs(10 * 60)
    }
    pub fn default_min_uptime() -> Duration {
        Duration::from_secs(10)
    }
    pub fn default_backoff_initial() -> Duration {
        Duration::from_secs(1)
    }
    pub fn default_backoff_max() -> Duration {
        Duration::from_secs(60)
    }
    pub fn default_stop_grace() -> Duration {
        Duration::from_secs(15)
    }
    pub fn default_startup_grace() -> Duration {
        Duration::from_secs(30)
    }
    pub fn default_activate_timeout() -> Duration {
        Duration::from_secs(10)
    }
    pub fn default_verify_for() -> Duration {
        Duration::from_secs(30)
    }
    pub fn default_metrics_interval() -> Duration {
        Duration::from_secs(60)
    }
    pub fn default_failure_threshold() -> u32 {
        3
    }
    pub fn default_success_threshold() -> u32 {
        2
    }
    pub fn default_flap_threshold() -> u32 {
        5
    }
    pub fn default_retries() -> u32 {
        3
    }
    pub fn default_max_restarts() -> u32 {
        5
    }
    pub fn default_confirmations() -> u32 {
        2
    }
    pub fn default_listen() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 9170))
    }

    /// F31:维护窗口缺省时长 (1h)。
    pub fn default_maintenance_duration() -> Duration {
        Duration::from_secs(3600)
    }
}

// ---------------------------------------------------------------------------
// 顶层
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub agent: AgentCfg,
    #[serde(default)]
    pub log: LogCfg,
    #[serde(default)]
    pub control: ControlCfg,
    #[serde(default)]
    pub collect: CollectCfg,
    #[serde(default)]
    pub alert: AlertCfg,
    #[serde(default)]
    pub metrics: MetricsCfg,
    /// F31:定期维护窗口 (UTC;窗口内匹配 target 的 Alert/Escalate/Flap 通知
    /// 被抑制，Resolve/Fatal 合成告警恒穿透 —— 语义同 F27 静默)。
    #[serde(default, rename = "maintenance")]
    pub maintenance: Vec<MaintenanceCfg>,
    #[serde(default, rename = "check")]
    pub checks: Vec<CheckCfg>,
    #[serde(default, rename = "service")]
    pub services: Vec<ServiceCfg>,
    #[serde(default, rename = "watch")]
    pub watches: Vec<WatchCfg>,
}

/// F31:单条定期维护窗口。全部时刻按 **UTC** 计算 (std 无时区库，如实取 UTC
/// —— 运维换算本机时区后配置;不支持 DST)。`start_utc = "HH:MM"`,`days`
/// 缺省 = 每天;`targets` 前缀匹配同 `[[alert.routes]]`(`*` = 全部)。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceCfg {
    #[serde(default)]
    pub name: String,
    pub targets: Vec<String>,
    pub start_utc: String,
    #[serde(with = "dur", default = "dur::default_maintenance_duration")]
    pub duration: Duration,
    #[serde(default)]
    pub days: Vec<String>,
}

impl Default for MaintenanceCfg {
    fn default() -> Self {
        Self {
            name: String::new(),
            targets: Vec::new(),
            start_utc: "00:00".into(),
            duration: dur::default_maintenance_duration(),
            days: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCfg {
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    /// (F24, reserved) Journal rotation threshold in bytes; 0 disables rotation.
    /// Default 10 MiB. Parsed and validated only — rotation wiring lands with F24.
    #[serde(default = "default_journal_max_size")]
    pub journal_max_size: u64,
}

impl Default for AgentCfg {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            journal_max_size: default_journal_max_size(),
        }
    }
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

fn default_journal_max_size() -> u64 {
    10 * 1024 * 1024
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogCfg {
    #[serde(default)]
    pub json: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlCfg {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "dur::default_listen")]
    pub listen: SocketAddr,
    /// F23:可选 Bearer 访问令牌;None = 本机信任边界行为不变。配置后除
    /// `GET /health` 外全部端点要求 `Authorization: Bearer <token>`。
    #[serde(default)]
    pub token: Option<String>,
    /// F32:TLS 证书与私钥路径 (PEM);两者必须同时配置，配置后控制面以
    /// HTTPS 监听 (同一 listen 地址)。None = 纯 HTTP(现状不变)。
    #[serde(default)]
    pub tls_cert: Option<PathBuf>,
    #[serde(default)]
    pub tls_key: Option<PathBuf>,
    /// F38:可选 mTLS 客户端 CA(PEM)—— 配置后 TLS 握手要求客户端证书
    /// (由该 CA 签发);必须与 tls_cert/tls_key 同时配置。None = 不校验客户端
    /// 证书 (现状不变)。
    #[serde(default)]
    pub client_ca_cert: Option<PathBuf>,
}

impl Default for ControlCfg {
    fn default() -> Self {
        Self {
            enabled: true,
            listen: dur::default_listen(),
            token: None,
            tls_cert: None,
            tls_key: None,
            client_ca_cert: None,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectCfg {
    #[serde(with = "dur", default = "dur::default_collect_interval")]
    pub interval: Duration,
}

impl Default for CollectCfg {
    fn default() -> Self {
        Self {
            interval: dur::default_collect_interval(),
        }
    }
}

/// Webhook payload 格式 (DESIGN §7.3:Slack 兼容与通用 JSON 双格式)。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WebhookFormat {
    /// AlertEvent 原样 JSON。
    #[default]
    Json,
    /// `{"text": "<摘要>"}` — Slack/Discord webhook 兼容。
    Slack,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertCfg {
    #[serde(default, deserialize_with = "opt_url_from_str")]
    pub webhook_url: Option<url::Url>,
    #[serde(default)]
    pub webhook_format: WebhookFormat,
    #[serde(with = "dur", default = "dur::default_alert_timeout")]
    pub timeout: Duration,
    #[serde(default = "dur::default_retries")]
    pub retries: u32,
    #[serde(with = "dur", default = "dur::default_renotify")]
    pub renotify_interval: Duration,
    #[serde(default = "dur::default_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "dur::default_success_threshold")]
    pub success_threshold: u32,
    #[serde(with = "dur", default = "dur::default_flap_window")]
    pub flap_window: Duration,
    #[serde(default = "dur::default_flap_threshold")]
    pub flap_threshold: u32,
    #[serde(default, deserialize_with = "opt_url_from_str")]
    pub deadman_url: Option<url::Url>,
    /// F29:可选 HMAC-SHA256 签名密钥 —— 配置后每次投递附
    /// `X-Service-Watch-Signature: sha256=<hex>` 头 (接收方恒定时间验签)。
    #[serde(default)]
    pub webhook_secret: Option<String>,
    /// F37:全局自定义投递头 (接收方 auth token 等);路由级同名键覆盖。
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// F26:按目标路由 (顺序首匹配;无匹配回落 `webhook_url`)。
    #[serde(default)]
    pub routes: Vec<AlertRouteCfg>,
}

impl Default for AlertCfg {
    fn default() -> Self {
        Self {
            webhook_url: None,
            webhook_format: WebhookFormat::default(),
            timeout: dur::default_alert_timeout(),
            retries: dur::default_retries(),
            renotify_interval: dur::default_renotify(),
            failure_threshold: dur::default_failure_threshold(),
            success_threshold: dur::default_success_threshold(),
            flap_window: dur::default_flap_window(),
            flap_threshold: dur::default_flap_threshold(),
            deadman_url: None,
            webhook_secret: None,
            headers: BTreeMap::new(),
            routes: Vec::new(),
        }
    }
}

/// F26:单条告警路由。`match` 为 target 名称前缀，`*` 匹配全部 target。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertRouteCfg {
    pub r#match: String,
    #[serde(deserialize_with = "url_from_str")]
    pub url: url::Url,
    #[serde(default)]
    pub format: WebhookFormat,
    /// F29:路由级签名密钥;None = 回落 `alert.webhook_secret`(也为 None 则不签名)。
    #[serde(default)]
    pub secret: Option<String>,
    /// F37:路由级自定义投递头 —— 与全局 headers 合并，同名键覆盖全局。
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsCfg {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, deserialize_with = "opt_url_from_str")]
    pub url: Option<url::Url>,
    #[serde(with = "dur", default = "dur::default_metrics_interval")]
    pub interval: Duration,
    /// 单次推送超时 (默认 10s;失败即放弃，下轮覆盖)。
    #[serde(with = "dur", default = "dur::default_alert_timeout")]
    pub timeout: Duration,
    /// F34:可选 HMAC-SHA256 签名密钥 —— 配置后每次推送附
    /// `X-Service-Watch-Signature: sha256=<hex>` 头 (语义同 F29 webhook 签名)。
    #[serde(default)]
    pub secret: Option<String>,
    /// F39:可选指标历史持久化路径 (JSONL，每样本一行;复用 journal 的单代
    /// 轮转模式，达 10 MiB 轮转 .1 保留一代)。None = 仅内存环形缓冲 (现状)。
    #[serde(default)]
    pub history_path: Option<PathBuf>,
}

impl Default for MetricsCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            url: None,
            interval: dur::default_metrics_interval(),
            timeout: dur::default_alert_timeout(),
            secret: None,
            history_path: None,
        }
    }
}

/// `""` / 缺省 → `None`;其余按 URL 解析 (空字符串在示例配置中表示“关闭”)。
fn opt_url_from_str<'de, D>(d: D) -> std::result::Result<Option<url::Url>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(d)?;
    match s.as_deref() {
        None | Some("") => Ok(None),
        Some(s) => url::Url::parse(s)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

fn url_from_str<'de, D>(d: D) -> std::result::Result<url::Url, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = String::deserialize(d)?;
    url::Url::parse(&s).map_err(serde::de::Error::custom)
}

// ---------------------------------------------------------------------------
// 检查目标
// ---------------------------------------------------------------------------

/// 注意:`kind` 采用 `#[serde(flatten)]` + 内部 tagged enum(TOML 同层写法),
/// serde 不允许 flatten 与 `deny_unknown_fields` 共存，故本结构放弃未知字段严格校验
/// (其余顶层/子表全部保持 `deny_unknown_fields`;未知 kind 值仍会被拒绝)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckCfg {
    pub name: String,
    #[serde(flatten)]
    pub kind: CheckKindCfg,
    #[serde(with = "dur", default = "dur::default_check_interval")]
    pub interval: Duration,
    #[serde(with = "dur", default = "dur::default_check_timeout")]
    pub timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum CheckKindCfg {
    Http {
        url: url::Url,
        #[serde(default = "default_expected_status")]
        expected_status: StatusRange,
    },
    Tcp {
        addr: SocketAddr,
    },
    Process {
        /// 进程名或 `"pid:1234"`。
        process: String,
        #[serde(default)]
        cpu_threshold: Option<f32>,
        #[serde(default)]
        mem_threshold_mb: Option<u64>,
    },
}

/// 闭区间状态码范围，`"200-299"`(含边界)。serde 以字符串形式序列化。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusRange(pub u16, pub u16);

impl<'de> Deserialize<'de> for StatusRange {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl Serialize for StatusRange {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.serialize_as_string())
    }
}

impl Default for StatusRange {
    fn default() -> Self {
        Self(200, 299)
    }
}

fn default_expected_status() -> StatusRange {
    StatusRange::default()
}

impl std::str::FromStr for StatusRange {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (lo, hi) = s
            .split_once('-')
            .ok_or_else(|| format!("expected_status must be `LO-HI`, got {s:?}"))?;
        let lo: u16 = lo
            .trim()
            .parse()
            .map_err(|e| format!("bad lower bound: {e}"))?;
        let hi: u16 = hi
            .trim()
            .parse()
            .map_err(|e| format!("bad upper bound: {e}"))?;
        if lo > hi {
            return Err(format!("expected_status lower > upper: {s:?}"));
        }
        Ok(Self(lo, hi))
    }
}

impl StatusRange {
    /// `"LO-HI"` 形式 (日志与错误信息用)。
    pub fn serialize_as_string(&self) -> String {
        format!("{}-{}", self.0, self.1)
    }
}

// ---------------------------------------------------------------------------
// 监管目标 (supervise) 与 attach 目标 (watch)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceCfg {
    pub name: String,
    /// 普通 supervise 目标：可执行文件路径。与 [`Self::windows_service`] 二选一
    /// (校验强制;缺省空串以便 SCM 目标省略此字段)。
    #[serde(default)]
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub workdir: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Windows:以 CREATE_NEW_PROCESS_GROUP 启动，允许尝试 CTRL_BREAK(不保证生效)。
    #[serde(default)]
    pub console: bool,
    /// 原生 Windows Service 目标 (ROADMAP「原生 Windows Service 目标恢复」):
    /// SCM 服务名 (`sc create <name>` 的服务名，非显示名)。设置后启停完全由 SCM
    /// 管理:spawn = `Start`(已 Running 视为成功，幂等)、stop = `Stop`、
    /// liveness = `QueryStatus == Running`;没有 (pid, start_time) 进程身份。
    /// 与 command/args/workdir/env/console/activate 互斥 (校验强制);
    /// depends_on/restart 仍适用 (恢复策略与启动编排不变)。
    /// 非 Windows 平台或未启用 `scm-target` feature 时 spawn 诚实降级为错误。
    #[serde(default)]
    pub windows_service: Option<String>,
    /// 依赖的服务名 (全部达到 Running 后本服务才被拉起;F20 启动编排)。
    /// 引用必须存在、禁止自依赖与环 (校验强制)。
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub restart: RestartCfg,
    #[serde(default)]
    pub activate: Option<ActivateCfg>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestartCfg {
    #[serde(with = "dur", default = "dur::default_min_uptime")]
    pub min_uptime: Duration,
    #[serde(with = "dur", default = "dur::default_backoff_initial")]
    pub backoff_initial: Duration,
    #[serde(with = "dur", default = "dur::default_backoff_max")]
    pub backoff_max: Duration,
    #[serde(default = "dur::default_max_restarts")]
    pub max_restarts: u32,
    #[serde(with = "dur", default = "dur::default_restart_window")]
    pub restart_window: Duration,
    #[serde(with = "dur", default = "dur::default_stop_grace")]
    pub stop_grace: Duration,
    #[serde(with = "dur", default = "dur::default_startup_grace")]
    pub startup_grace: Duration,
    #[serde(default = "dur::default_confirmations")]
    pub confirmations: u32,
    #[serde(default)]
    pub hung_action: HungAction,
}

impl Default for RestartCfg {
    fn default() -> Self {
        Self {
            min_uptime: dur::default_min_uptime(),
            backoff_initial: dur::default_backoff_initial(),
            backoff_max: dur::default_backoff_max(),
            max_restarts: dur::default_max_restarts(),
            restart_window: dur::default_restart_window(),
            stop_grace: dur::default_stop_grace(),
            startup_grace: dur::default_startup_grace(),
            confirmations: dur::default_confirmations(),
            hung_action: HungAction::Restart,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HungAction {
    #[default]
    Restart,
    Activate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivateCfg {
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub url: Option<url::Url>,
    #[serde(with = "dur", default = "dur::default_activate_timeout")]
    pub timeout: Duration,
    #[serde(with = "dur", default = "dur::default_verify_for")]
    pub verify_for: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchCfg {
    pub name: String,
    #[serde(default)]
    pub pid: Option<u32>,
    #[serde(default)]
    pub pidfile: Option<PathBuf>,
}

// ---------------------------------------------------------------------------
// 解析入口与校验
// ---------------------------------------------------------------------------

impl Config {
    pub fn from_toml_str(s: &str) -> Result<Self> {
        let cfg: Config = toml::from_str(s)?;
        cfg.validate().map_err(|errs| {
            CoreError::Config(errs.iter().map(|e| format!("\n  - {e}")).collect())
        })?;
        Ok(cfg)
    }

    pub fn from_path(p: &Path) -> Result<Self> {
        let s = std::fs::read_to_string(p)?;
        Self::from_toml_str(&s)
    }

    /// DESIGN.md §4.3 交叉校验。返回全部违规项 (一次性报完，便于用户修正)。
    /// F33:生效配置视图 —— 敏感字段 (control.token / webhook_secret / 路由级
    /// secret) 以 `***redacted***` 占位，其余字段原样。供 `GET /v1/config` 与
    /// monitorctl 使用;序列化输出结构与 TOML schema 一致，占位值不会通过
    /// 二次校验 (仅供展示，不可回灌)。
    pub fn redacted(&self) -> Config {
        let mut cfg = self.clone();
        if cfg.control.token.is_some() {
            cfg.control.token = Some("***redacted***".into());
        }
        if cfg.alert.webhook_secret.is_some() {
            cfg.alert.webhook_secret = Some("***redacted***".into());
        }
        for route in &mut cfg.alert.routes {
            if route.secret.is_some() {
                route.secret = Some("***redacted***".into());
            }
        }
        if cfg.metrics.secret.is_some() {
            // F34:推送签名密钥与 webhook 同口径 —— 明文不出 /v1/config。
            cfg.metrics.secret = Some("***redacted***".into());
        }
        cfg
    }

    pub fn validate(&self) -> std::result::Result<(), Vec<String>> {
        let mut errs = Vec::new();
        let say = |errs: &mut Vec<String>, msg: String| errs.push(msg);

        // Config can also be built directly by library callers. Validate these
        // durations before any scheduler performs Instant + Duration.
        let now = std::time::Instant::now();
        let mut durations = vec![
            ("collect.interval", self.collect.interval),
            ("alert.timeout", self.alert.timeout),
            ("alert.renotify_interval", self.alert.renotify_interval),
            ("alert.flap_window", self.alert.flap_window),
            ("metrics.interval", self.metrics.interval),
        ];
        for check in &self.checks {
            durations.extend([
                ("check.interval", check.interval),
                ("check.timeout", check.timeout),
            ]);
        }
        for service in &self.services {
            let policy = &service.restart;
            durations.extend([
                ("restart.min_uptime", policy.min_uptime),
                ("restart.backoff_initial", policy.backoff_initial),
                ("restart.backoff_max", policy.backoff_max),
                ("restart.restart_window", policy.restart_window),
                ("restart.stop_grace", policy.stop_grace),
                ("restart.startup_grace", policy.startup_grace),
            ]);
            if let Some(activate) = &service.activate {
                durations.extend([
                    ("activate.timeout", activate.timeout),
                    ("activate.verify_for", activate.verify_for),
                ]);
            }
        }
        for (name, duration) in durations {
            if duration.is_zero() || now.checked_add(duration).is_none() {
                say(
                    &mut errs,
                    format!("{name}: duration must be positive and within monotonic clock range"),
                );
            }
        }

        for c in &self.checks {
            if c.name.trim().is_empty() {
                say(&mut errs, "check: name is blank".to_string());
            }
            if c.timeout >= c.interval {
                say(
                    &mut errs,
                    format!(
                        "check {:?}: timeout ({:?}) must be < interval ({:?})",
                        c.name, c.timeout, c.interval
                    ),
                );
            }
            // 下界 (第五轮评审 P3):过小的 interval 使探测/采集本身成为负载源
            // (sysinfo 全刷新 1ms 周期可拖垮宿主)。100ms = 探测可用下限。
            if c.interval < Duration::from_millis(100) {
                say(
                    &mut errs,
                    format!(
                        "check {:?}: interval ({:?}) must be >= 100ms",
                        c.name, c.interval
                    ),
                );
            }
            match &c.kind {
                CheckKindCfg::Http { url, .. } => {
                    if !matches!(url.scheme(), "http" | "https") {
                        say(
                            &mut errs,
                            format!("check {:?}: url scheme must be http/https", c.name),
                        );
                    }
                }
                CheckKindCfg::Process {
                    process,
                    cpu_threshold,
                    mem_threshold_mb,
                } => {
                    if process.trim().is_empty() {
                        say(&mut errs, format!("check {:?}: process is blank", c.name));
                    }
                    if let Some(pid) = process.strip_prefix("pid:")
                        && !pid.parse::<u32>().is_ok_and(|pid| pid > 0)
                    {
                        say(
                            &mut errs,
                            format!("check {:?}: process pid must be a positive u32", c.name),
                        );
                    }
                    if cpu_threshold.is_none() && mem_threshold_mb.is_none() {
                        say(
                            &mut errs,
                            format!(
                                "check {:?}: cpu_threshold / mem_threshold_mb 至少一个",
                                c.name
                            ),
                        );
                    }
                    if let Some(threshold) = cpu_threshold
                        && (!threshold.is_finite() || *threshold < 0.0)
                    {
                        say(
                            &mut errs,
                            format!("check {:?}: cpu_threshold must be finite and >= 0", c.name),
                        );
                    }
                }
                CheckKindCfg::Tcp { .. } => {}
            }
        }

        for s in &self.services {
            if s.name.trim().is_empty() {
                say(&mut errs, "service: name is blank".to_string());
            }
            // windows_service 目标形态 (ROADMAP「原生 Windows Service 目标恢复」):
            // command 二选一;SCM 管理启停，进程级启动参数一律无意义。
            let scm = s.windows_service.as_deref();
            if scm.is_some_and(|name| name.trim().is_empty()) {
                say(
                    &mut errs,
                    format!("service {:?}: windows_service is blank", s.name),
                );
            }
            match scm {
                Some(_) => {
                    if !s.command.trim().is_empty() {
                        say(
                            &mut errs,
                            format!(
                                "service {:?}: windows_service 与 command 必须且只能配置一个 (SCM 管理启停，不接受本地命令)",
                                s.name
                            ),
                        );
                    }
                    let mut clashes: Vec<&str> = Vec::new();
                    if !s.args.is_empty() {
                        clashes.push("args");
                    }
                    if s.workdir.is_some() {
                        clashes.push("workdir");
                    }
                    if !s.env.is_empty() {
                        clashes.push("env");
                    }
                    if s.console {
                        clashes.push("console");
                    }
                    if s.activate.is_some() {
                        clashes.push("activate");
                    }
                    if !clashes.is_empty() {
                        say(
                            &mut errs,
                            format!(
                                "service {:?}: windows_service 目标不支持 {}(启停由 SCM 管理;恢复策略用 [service.restart],编排用 depends_on)",
                                s.name,
                                clashes.join(" / ")
                            ),
                        );
                    }
                }
                None => {
                    if s.command.trim().is_empty() {
                        say(&mut errs, format!("service {:?}: command is blank", s.name));
                    }
                }
            }
            let r = &s.restart;
            if r.backoff_initial > r.backoff_max {
                say(
                    &mut errs,
                    format!("service {:?}: backoff_initial > backoff_max", s.name),
                );
            }
            if r.startup_grace < r.min_uptime {
                say(
                    &mut errs,
                    format!(
                        "service {:?}: startup_grace ({:?}) must be >= min_uptime ({:?})",
                        s.name, r.startup_grace, r.min_uptime
                    ),
                );
            }
            if r.confirmations == 0 {
                say(
                    &mut errs,
                    format!("service {:?}: confirmations must be >= 1", s.name),
                );
            }
            if r.max_restarts == 0 {
                say(
                    &mut errs,
                    format!("service {:?}: max_restarts must be >= 1", s.name),
                );
            }
            if r.hung_action == HungAction::Activate && s.activate.is_none() {
                say(
                    &mut errs,
                    format!(
                        "service {:?}: hung_action=activate 需要 [service.activate]",
                        s.name
                    ),
                );
            }
            if let Some(a) = &s.activate {
                if a.command.is_some() == a.url.is_some() {
                    say(
                        &mut errs,
                        format!(
                            "service {:?}: activate.command / activate.url 必须且只能配置一个",
                            s.name
                        ),
                    );
                }
                if a.command
                    .as_ref()
                    .is_some_and(|command| command.trim().is_empty())
                {
                    say(
                        &mut errs,
                        format!("service {:?}: activate.command is blank", s.name),
                    );
                }
                if let Some(url) = &a.url
                    && !matches!(url.scheme(), "http" | "https")
                {
                    say(
                        &mut errs,
                        format!(
                            "service {:?}: activate.url scheme must be http/https",
                            s.name
                        ),
                    );
                }
            }
        }

        for w in &self.watches {
            if w.name.trim().is_empty() {
                say(&mut errs, "watch: name is blank".into());
            }
            if w.pid.is_none() && w.pidfile.is_none() {
                say(
                    &mut errs,
                    format!("watch {:?}: pid / pidfile 至少一个", w.name),
                );
            }
            if w.pidfile.as_ref().is_some_and(|p| p.as_os_str().is_empty()) {
                // P3 (round 8): an empty path always fails to read — the watch
                // would never attach and would spam a WARN every cycle.
                say(
                    &mut errs,
                    format!("watch {:?}: pidfile must not be an empty path", w.name),
                );
            }
            if w.pid == Some(0) {
                say(
                    &mut errs,
                    format!("watch {:?}: pid must be positive", w.name),
                );
            }
        }

        // collect / alert 段 (第五轮评审 P3:此前零校验)
        if self.agent.data_dir.as_os_str().is_empty() {
            say(&mut errs, "agent: data_dir is blank".into());
        }
        if self
            .control
            .token
            .as_ref()
            .is_some_and(|t| t.trim().is_empty())
        {
            // F23: a blank token either locks everyone out or is a no-op —
            // both are configuration errors.
            say(&mut errs, "control: token must not be blank".into());
        }
        if self
            .alert
            .webhook_secret
            .as_ref()
            .is_some_and(|s| s.trim().is_empty())
        {
            // F29:空白密钥签出的签名毫无安全性，配置层直接拒绝。
            say(&mut errs, "alert: webhook_secret must not be blank".into());
        }
        if self
            .metrics
            .secret
            .as_ref()
            .is_some_and(|s| s.trim().is_empty())
        {
            // F34:推送签名密钥与 webhook 同口径 —— 空白即配置错误。
            say(&mut errs, "metrics: secret must not be blank".into());
        }
        // F37:投递头注入面 —— 名必须是合法 token(RFC 7230 tchar),值不得含
        // CR/LF/控制字符(header 注入),空名/空值拒绝。
        let is_tchar = |c: char| c.is_ascii_alphanumeric() || "!#$%&'*+-.^_`|~".contains(c);
        let mut check_headers = |where_: &str, hs: &BTreeMap<String, String>| {
            for (k, v) in hs {
                if k.is_empty() || !k.chars().all(is_tchar) {
                    say(
                        &mut errs,
                        format!("{where_}: header name {k:?} 必须是非空 RFC 7230 token"),
                    );
                }
                // P2(第十四轮验证):http crate 的 HeaderValue 字节规则是
                // `b >= 32 && b != 127 || b == TAB`(≥128 一律拒)—— Rust
                // is_control() 不覆盖 ≥ U+00A0 的非 ASCII(如 "café"),曾出现
                // "config 通过 → 构造期 WARN 丢弃 → 接收方缺头 401" 的排查黑洞。
                // 此处与 http crate 字节规则精确对齐:仅可见 ASCII + TAB。
                if v.is_empty() || !v.bytes().all(|b| (b >= 32 && b != 127) || b == b'\t') {
                    say(
                        &mut errs,
                        format!(
                            "{where_}: header {k:?} 的值不得为空,且仅可含可见 ASCII 字符与 TAB(header 值字节规则)"
                        ),
                    );
                }
            }
        };
        check_headers("alert.headers", &self.alert.headers);
        for route in &self.alert.routes {
            check_headers(
                &format!("alert: routes.headers (match={:?})", route.r#match),
                &route.headers,
            );
        }
        // F38:mTLS 必须建立在 TLS 之上
        if self.control.client_ca_cert.is_some() && self.control.tls_cert.is_none() {
            say(
                &mut errs,
                "control: client_ca_cert 必须与 tls_cert/tls_key 同时配置 (mTLS 建立在 HTTPS 之上)"
                    .into(),
            );
        }
        // F39:历史持久化路径非空
        if self
            .metrics
            .history_path
            .as_ref()
            .is_some_and(|p| p.as_os_str().is_empty())
        {
            say(&mut errs, "metrics: history_path 不能为空路径".into());
        }
        if self.alert.failure_threshold == 0
            || self.alert.success_threshold == 0
            || self.alert.flap_threshold == 0
        {
            say(
                &mut errs,
                "alert: failure_threshold / success_threshold / flap_threshold must be >= 1".into(),
            );
        }
        if self.alert.retries > 100 {
            // P3 (round 8): an unbounded order-of-magnitude slip would pin the
            // dispatch task on a single event's retries (100 x timeout + backoff
            // is on the order of 20 minutes — enough to expose the config error).
            say(
                &mut errs,
                format!(
                    "alert: retries ({}) must be <= 100 (an out-of-range magnitude pins the dispatch pipeline)",
                    self.alert.retries
                ),
            );
        }
        if let Some(url) = &self.alert.webhook_url
            && !matches!(url.scheme(), "http" | "https")
        {
            say(
                &mut errs,
                "alert: webhook_url scheme must be http/https".into(),
            );
        }
        for route in &self.alert.routes {
            if route.r#match.trim().is_empty() {
                say(&mut errs, "alert: routes.match must not be blank".into());
            }
            if !matches!(route.url.scheme(), "http" | "https") {
                say(
                    &mut errs,
                    format!(
                        "alert: routes.url scheme must be http/https (match={:?})",
                        route.r#match
                    ),
                );
            }
            if route.secret.as_ref().is_some_and(|s| s.trim().is_empty()) {
                say(
                    &mut errs,
                    format!(
                        "alert: routes.secret must not be blank (match={:?})",
                        route.r#match
                    ),
                );
            }
        }
        // F31:维护窗口校验 (UTC 手工解析;std 无时区库，如实限定 UTC)
        for m in &self.maintenance {
            if m.targets.is_empty() {
                say(
                    &mut errs,
                    format!(
                        "maintenance {:?}: targets must not be empty(显式 [\"*\"] 表示全部)",
                        m.name
                    ),
                );
            }
            let parts: Vec<&str> = m.start_utc.split(':').collect();
            let hhmm_ok = parts.len() == 2
                && parts
                    .iter()
                    .all(|p| p.len() == 2 && p.chars().all(|c| c.is_ascii_digit()))
                && parts[0].parse::<u8>().map(|h| h < 24).unwrap_or(false)
                && parts[1].parse::<u8>().map(|mi| mi < 60).unwrap_or(false);
            if !hhmm_ok {
                say(
                    &mut errs,
                    format!("maintenance {:?}: start_utc must be UTC \"HH:MM\"", m.name),
                );
            }
            if m.duration < Duration::from_secs(60) {
                say(
                    &mut errs,
                    format!(
                        "maintenance {:?}: duration must be >= 60s(过短窗口在采集/tick 粒度下形同虚设)",
                        m.name
                    ),
                );
            }
            const WEEKDAYS: [&str; 7] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
            for d in &m.days {
                let day = d.trim().to_ascii_lowercase();
                if !WEEKDAYS.contains(&day.as_str()) {
                    say(
                        &mut errs,
                        format!(
                            "maintenance {:?}: days 含非法值 {day:?}(合法：mon tue wed thu fri sat sun)",
                            m.name
                        ),
                    );
                }
            }
        }
        // F32:TLS 证书/私钥成对校验
        match (
            self.control.tls_cert.as_ref(),
            self.control.tls_key.as_ref(),
        ) {
            (Some(_), Some(_)) => {}
            (Some(_), None) | (None, Some(_)) => {
                say(
                    &mut errs,
                    "control: tls_cert / tls_key 必须同时配置 (或都不配 = 纯 HTTP)".into(),
                );
            }
            (None, None) => {}
        }
        if self.collect.interval < Duration::from_millis(100) {
            say(
                &mut errs,
                format!(
                    "collect: interval ({:?}) must be >= 100ms(sysinfo 全刷新周期过小会使采集本身成为负载源)",
                    self.collect.interval
                ),
            );
        }
        if let Some(u) = &self.alert.deadman_url
            && !matches!(u.scheme(), "http" | "https")
        {
            say(
                &mut errs,
                format!(
                    "alert: deadman_url scheme must be http/https(got {:?};非 HTTP scheme 每 30s 产生一次注定失败的 POST)",
                    u.scheme()
                ),
            );
        }

        // depends_on(F20 启动编排):引用存在、无自依赖、无环 (迭代 DFS 三色标记)
        {
            let names: std::collections::HashSet<&str> =
                self.services.iter().map(|s| s.name.as_str()).collect();
            for s in &self.services {
                for dep in &s.depends_on {
                    if dep == &s.name {
                        say(
                            &mut errs,
                            format!("service {:?}: depends_on 自引用", s.name),
                        );
                    } else if !names.contains(dep.as_str()) {
                        say(
                            &mut errs,
                            format!("service {:?}: depends_on 引用不存在的服务 {dep:?}", s.name),
                        );
                    }
                }
            }
            let mut color: std::collections::HashMap<&str, u8> = self
                .services
                .iter()
                .map(|s| (s.name.as_str(), 0u8))
                .collect();
            // 迭代 DFS:灰 (1)=栈中，黑 (2)=完成;回到灰节点即环
            for s in &self.services {
                let mut stack: Vec<(&str, usize)> = vec![(s.name.as_str(), 0)];
                while let Some((node, idx)) = stack.pop() {
                    if color.get(node).copied() == Some(2) {
                        continue;
                    }
                    let deps: Vec<&str> = self
                        .services
                        .iter()
                        .find(|sv| sv.name == node)
                        .map(|sv| sv.depends_on.iter().map(String::as_str).collect())
                        .unwrap_or_default();
                    if idx < deps.len() {
                        // 标灰 (node 在栈中)
                        if color.get(node).copied() == Some(0) {
                            color.insert(node, 1);
                        }
                        stack.push((node, idx + 1));
                        let next = deps[idx];
                        match color.get(next).copied() {
                            Some(1) => {
                                say(&mut errs, format!("service 依赖环：经过 {next:?}"));
                            }
                            Some(2) => {}
                            _ => stack.push((next, 0)),
                        }
                    } else {
                        color.insert(node, 2);
                    }
                }
            }
        }

        // [metrics] 推送 (ROADMAP「指标周期推送」)
        if self.metrics.enabled {
            match &self.metrics.url {
                None => say(&mut errs, "[metrics] enabled = true 需要 url".to_string()),
                Some(u) if !matches!(u.scheme(), "http" | "https") => {
                    say(
                        &mut errs,
                        "[metrics] url scheme 必须为 http/https".to_string(),
                    );
                }
                Some(_) => {}
            }
            if self.metrics.timeout >= self.metrics.interval {
                say(
                    &mut errs,
                    format!(
                        "[metrics] timeout ({:?}) must be < interval ({:?})",
                        self.metrics.timeout, self.metrics.interval
                    ),
                );
            }
        }

        // name 唯一 (三个空间独立)
        for (label, list) in [
            (
                "check",
                self.checks.iter().map(|c| &c.name).collect::<Vec<_>>(),
            ),
            (
                "service",
                self.services.iter().map(|s| &s.name).collect::<Vec<_>>(),
            ),
            (
                "watch",
                self.watches.iter().map(|w| &w.name).collect::<Vec<_>>(),
            ),
        ] {
            let mut seen = std::collections::HashSet::new();
            for n in list {
                if !seen.insert(n.clone()) {
                    say(&mut errs, format!("{label}: duplicate name {n:?}"));
                }
            }
        }

        if errs.is_empty() { Ok(()) } else { Err(errs) }
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
        [[check]]
        name = "c1"
        kind = "http"
        url = "http://127.0.0.1:8080/hz"
    "#;

    #[test]
    fn omitted_and_empty_sections_share_documented_defaults() {
        for source in ["", "[agent]\n[metrics]\n"] {
            let cfg = Config::from_toml_str(source).unwrap();
            assert_eq!(cfg.agent.data_dir, PathBuf::from("./data"));
            assert_eq!(cfg.metrics.interval, Duration::from_secs(60));
        }
        let cfg = Config::default();
        assert_eq!(cfg.agent.data_dir, PathBuf::from("./data"));
        assert_eq!(cfg.metrics.interval, Duration::from_secs(60));
    }

    #[test]
    fn rejects_invalid_notification_and_hook_config() {
        for source in [
            "[agent]\ndata_dir = ''",
            "[alert]\nwebhook_url = 'ftp://example.com/notify'",
            "[alert]\nfailure_threshold = 0",
            "[alert]\nsuccess_threshold = 0",
            "[alert]\nflap_threshold = 0",
            "[alert]\nretries = 101",
            "[[watch]]\nname='w'\npidfile=''",
            "[control]\ntoken = ' '",
            "[alert]\nwebhook_secret = ' '",
            "[metrics]\nenabled = true\nurl = 'http://ep/push'\nsecret = ' '",
            "[alert]\nheaders = { 'X-Auth' = '' }",
            "[alert]\nheaders = { 'Bad Header' = 'v' }",
            "[alert]\nheaders = { 'X-Inj' = \"line1\\nline2\" }",
            "[control]\nclient_ca_cert = 'ca.pem'",
            "[metrics]\nhistory_path = ''",
            "[[maintenance]]\ntargets = []\nstart_utc = '02:00'",
            "[[maintenance]]\ntargets = ['*']\nstart_utc = '24:00'",
            "[[maintenance]]\ntargets = ['*']\nstart_utc = '2:00'",
            "[[maintenance]]\ntargets = ['*']\nstart_utc = '02:00'\nduration = '30s'",
            "[[maintenance]]\ntargets = ['*']\nstart_utc = '02:00'\ndays = ['moon']",
            "[control]\ntls_cert = 'cert.pem'",
            "[alert]\n[[alert.routes]]\nmatch = 'edge'\nurl = 'ftp://ep/hook'",
            "[alert]\n[[alert.routes]]\nmatch = ' '\nurl = 'http://ep/hook'",
            "[alert]\n[[alert.routes]]\nmatch = 'edge'\nurl = 'http://ep/hook'\nsecret = ' '",
            "[[service]]\nname='app'\ncommand='app'\n[service.activate]\ncommand='hook'\nurl='http://localhost/hook'",
            "[[service]]\nname='app'\ncommand='app'\n[service.activate]\ncommand=' '",
            "[[service]]\nname='app'\ncommand='app'\n[service.activate]\nurl='ftp://localhost/hook'",
            "[[watch]]\nname=' '\npid=1",
        ] {
            assert!(Config::from_toml_str(source).is_err(), "{source}");
        }
    }

    #[test]
    fn parses_v04_optional_fields_with_defaults() {
        // Defaults = backward compatible (F23/F24/F26 all disabled or empty)
        let cfg = Config::from_toml_str("").unwrap();
        assert_eq!(cfg.agent.journal_max_size, 10 * 1024 * 1024);
        assert_eq!(cfg.control.token, None);
        assert!(cfg.alert.routes.is_empty());

        let cfg = Config::from_toml_str(
            "[control]\ntoken = 'secret'\n[agent]\njournal_max_size = 0\n\
             [alert]\n[[alert.routes]]\nmatch = 'edge-*'\nurl = 'http://ep/hook'\nformat = 'slack'",
        )
        .unwrap();
        assert_eq!(cfg.control.token.as_deref(), Some("secret"));
        assert_eq!(cfg.agent.journal_max_size, 0);
        assert_eq!(cfg.alert.routes.len(), 1);
        assert_eq!(cfg.alert.routes[0].r#match, "edge-*");
        assert!(matches!(cfg.alert.routes[0].format, WebhookFormat::Slack));
    }

    /// v0.6 基座:F31 维护窗口解析 / F32 TLS 成对字段 / F33 redaction。
    #[test]
    fn parses_v06_fields_and_redacts_secrets() {
        let cfg = Config::from_toml_str(
            "[control]\ntls_cert = 'c.pem'\ntls_key = 'k.pem'\ntoken = 'tok'\n\
             [alert]\nwebhook_secret = 'sec'\n[[alert.routes]]\nmatch = 'edge'\nurl = 'http://e/h'\nsecret = 'rs'\n\
             [metrics]\nenabled = true\nurl = 'http://p/ingest'\nsecret = 'push'\n\
             [[maintenance]]\nname = 'nightly'\ntargets = ['*']\nstart_utc = '18:30'\nduration = '30m'\ndays = ['mon', 'fri']",
        )
        .unwrap();
        assert_eq!(cfg.control.tls_cert, Some(PathBuf::from("c.pem")));
        assert_eq!(cfg.control.tls_key, Some(PathBuf::from("k.pem")));
        assert_eq!(cfg.maintenance.len(), 1);
        let m = &cfg.maintenance[0];
        assert_eq!(m.name, "nightly");
        assert_eq!(m.targets, vec!["*"]);
        assert_eq!(m.start_utc, "18:30");
        assert_eq!(m.duration, Duration::from_secs(1800));
        assert_eq!(m.days, vec!["mon", "fri"]);

        let r = cfg.redacted();
        assert_eq!(r.control.token.as_deref(), Some("***redacted***"));
        assert_eq!(r.alert.webhook_secret.as_deref(), Some("***redacted***"));
        assert_eq!(r.alert.routes[0].secret.as_deref(), Some("***redacted***"));
        assert_eq!(r.metrics.secret.as_deref(), Some("***redacted***"));
        assert!(
            !serde_json::to_string(&r).unwrap().contains("push"),
            "明文推送密钥不得出现在视图"
        );
        assert_eq!(
            r.control.tls_cert,
            Some(PathBuf::from("c.pem")),
            "非敏感字段 redaction 后原样"
        );

        // 缺省 (不配置) 零行为变化
        let empty = Config::from_toml_str("").unwrap();
        assert!(empty.maintenance.is_empty());
        assert_eq!(empty.control.tls_cert, None);
        assert_eq!(empty.control.tls_key, None);
    }

    #[test]
    fn rejects_nonfinite_and_negative_cpu_thresholds() {
        for threshold in ["nan", "inf", "-inf", "-1.0"] {
            let source = format!(
                "[[check]]\nname='cpu'\nkind='process'\nprocess='app'\ncpu_threshold={threshold}"
            );
            assert!(Config::from_toml_str(&source).is_err(), "{source}");
        }
    }

    #[test]
    fn rejects_invalid_explicit_process_ids() {
        for pid in ["0", "-1", "4294967296", "", "abc"] {
            let source = format!(
                "[[check]]\nname='cpu'\nkind='process'\nprocess='pid:{pid}'\nmem_threshold_mb=100"
            );
            assert!(Config::from_toml_str(&source).is_err(), "{source}");
        }
        assert!(Config::from_toml_str("[[watch]]\nname='app'\npid=0").is_err());
    }

    #[test]
    fn rejects_durations_outside_monotonic_clock_range() {
        assert!(dur::parse("18446744073709000000s").is_err());
        let mut cfg = Config::default();
        cfg.collect.interval = Duration::MAX;
        assert!(cfg.validate().is_err());
        cfg.collect.interval = Duration::ZERO;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn fractional_millisecond_durations_survive_config_serialization() {
        let mut cfg = Config::default();
        cfg.alert.timeout = Duration::from_micros(250);
        let encoded = toml::to_string(&cfg).unwrap();
        let decoded = Config::from_toml_str(&encoded).unwrap();
        assert_eq!(decoded.alert.timeout, cfg.alert.timeout);
    }

    #[test]
    fn parses_minimal_with_defaults() {
        let cfg = Config::from_toml_str(MINIMAL).unwrap();
        assert_eq!(cfg.checks.len(), 1);
        assert_eq!(cfg.collect.interval, Duration::from_secs(5));
        assert_eq!(cfg.alert.failure_threshold, 3);
        assert_eq!(cfg.control.listen.to_string(), "127.0.0.1:9170");
        match &cfg.checks[0].kind {
            CheckKindCfg::Http {
                expected_status, ..
            } => {
                assert_eq!(*expected_status, StatusRange(200, 299));
            }
            other => panic!("unexpected kind: {other:?}"),
        }
    }

    #[test]
    fn rejects_timeout_ge_interval() {
        let s = r#"
            [[check]]
            name = "c1"
            kind = "tcp"
            addr = "127.0.0.1:5432"
            interval = "5s"
            timeout = "5s"
        "#;
        let err = Config::from_toml_str(s).unwrap_err().to_string();
        assert!(err.contains("must be < interval"), "{err}");
    }

    /// interval 下界 (第五轮评审 P3):check/collect 过小 → 校验拒绝 (负载源防护)。
    #[test]
    fn rejects_intervals_below_floor() {
        let check_small = r#"
            [[check]]
            name = "c1"
            kind = "tcp"
            addr = "127.0.0.1:5432"
            interval = "50ms"
            timeout = "10ms"
        "#;
        let err = Config::from_toml_str(check_small).unwrap_err().to_string();
        assert!(err.contains("interval"), "{err}");
        assert!(err.contains("100ms"), "{err}");

        let collect_small = r#"
            [collect]
            interval = "1ms"
        "#;
        let err = Config::from_toml_str(collect_small)
            .unwrap_err()
            .to_string();
        assert!(err.contains("collect"), "{err}");
    }

    /// deadman_url scheme 校验 (第五轮评审 P3):非 http/https 拒绝。
    #[test]
    fn rejects_deadman_non_http_scheme() {
        let s = r#"
            [alert]
            deadman_url = "ftp://example.com/heartbeat"
        "#;
        let err = Config::from_toml_str(s).unwrap_err().to_string();
        assert!(err.contains("deadman_url"), "{err}");

        // 合法值与空值照常通过
        let ok = r#"
            [alert]
            deadman_url = "http://127.0.0.1:9/"
        "#;
        assert!(Config::from_toml_str(ok).is_ok());
    }

    #[test]
    fn rejects_unknown_fields() {
        let s = r#"
            [alert]
            webhook_urll = "http://x"
        "#;
        assert!(Config::from_toml_str(s).is_err());
    }

    #[test]
    fn rejects_activate_without_hook() {
        let s = r#"
            [[service]]
            name = "app"
            command = "/bin/true"
            [service.restart]
            hung_action = "activate"
        "#;
        let err = Config::from_toml_str(s).unwrap_err().to_string();
        assert!(err.contains("activate"), "{err}");
    }

    #[test]
    fn rejects_bad_backoff_ordering() {
        let s = r#"
            [[service]]
            name = "app"
            command = "/bin/true"
            [service.restart]
            backoff_initial = "60s"
            backoff_max = "1s"
        "#;
        assert!(Config::from_toml_str(s).is_err());
    }

    // ---- windows_service(SCM 目标) 校验：二选一与互斥 (ROADMAP 原生 Windows Service) ----

    /// 合法 SCM 目标:name + windows_service，依赖与恢复策略照常适用。
    #[test]
    fn accepts_scm_target_with_restart_and_deps() {
        let s = r#"
            [[service]]
            name = "wsvc"
            windows_service = "W3SVC"
            depends_on = ["db"]
            [service.restart]
            stop_grace = "30s"
            [[service]]
            name = "db"
            command = "/bin/db"
        "#;
        let cfg = Config::from_toml_str(s).unwrap();
        assert_eq!(cfg.services[0].windows_service.as_deref(), Some("W3SVC"));
        assert_eq!(cfg.services[0].command, "");
        assert_eq!(cfg.services[0].depends_on, vec!["db".to_string()]);
    }

    /// SCM 目标与 command/args/workdir/env/console/activate 互斥 (逐项)。
    #[test]
    fn rejects_scm_target_with_process_launch_fields() {
        let cases = [
            ("command = '/bin/app'", "command"),
            ("args = ['--x']", "args"),
            ("workdir = '/tmp'", "workdir"),
            ("[service.env]\nK = 'v'", "env"),
            ("console = true", "console"),
        ];
        for (field, label) in cases {
            let source = format!("[[service]]\nname = 'app'\nwindows_service = 'W3SVC'\n{field}");
            let err = Config::from_toml_str(&source).unwrap_err().to_string();
            assert!(err.contains("windows_service"), "{source}: {err}");
            assert!(err.contains(label), "{source}: {err}");
        }
        let activate = r#"
            [[service]]
            name = "app"
            windows_service = "W3SVC"
            [service.activate]
            command = "hook.cmd"
        "#;
        let err = Config::from_toml_str(activate).unwrap_err().to_string();
        assert!(err.contains("activate"), "{err}");
    }

    /// 二选一：两边都配置 / 两边都缺 / windows_service 空白，全部拒绝。
    #[test]
    fn rejects_scm_and_command_mutual_exclusion_violations() {
        let both = r#"
            [[service]]
            name = "app"
            command = "/bin/app"
            windows_service = "W3SVC"
        "#;
        let err = Config::from_toml_str(both).unwrap_err().to_string();
        assert!(err.contains("必须且只能配置一个"), "{err}");

        let neither = "[[service]]\nname = 'app'\n";
        assert!(Config::from_toml_str(neither).is_err());

        let blank = "[[service]]\nname = 'app'\nwindows_service = ' '\n";
        let err = Config::from_toml_str(blank).unwrap_err().to_string();
        assert!(err.contains("windows_service is blank"), "{err}");
    }

    #[test]
    fn rejects_duplicate_names() {
        let s = r#"
            [[check]]
            name = "c1"
            kind = "tcp"
            addr = "127.0.0.1:1"
            [[check]]
            name = "c1"
            kind = "tcp"
            addr = "127.0.0.1:2"
        "#;
        assert!(Config::from_toml_str(s).is_err());
    }

    #[test]
    fn parses_status_range_and_durations() {
        let r: StatusRange = "200-399".parse().unwrap();
        assert_eq!(r, StatusRange(200, 399));
        assert_eq!(dur::parse("90s").unwrap(), Duration::from_secs(90));
        assert_eq!(dur::parse("1h30m").unwrap(), Duration::from_secs(5400));
        assert_eq!(dur::parse("250ms").unwrap(), Duration::from_millis(250));
        assert!(dur::parse("5x").is_err());
        assert!(dur::parse("0s").is_err());
    }

    #[test]
    fn rejects_oversized_durations_without_panicking() {
        // 巨大数值必须返回 Err(配置错误),不得 panic(Duration::from_secs_f64 超域会 panic)
        assert!(dur::parse("99999999999999999999s").is_err());
        assert!(dur::parse("4000000000000000000h").is_err());
        assert!(dur::parse("1e400s").is_err(), "解析为 inf 的输入必须拒绝");
        assert!(std::panic::catch_unwind(|| dur::parse("99999999999999999999s")).is_ok());
    }

    #[test]
    fn accepts_dependency_chain() {
        let s = r#"
            [[service]]
            name = "db"
            command = "/bin/db"
            [[service]]
            name = "api"
            command = "/bin/api"
            depends_on = ["db"]
            [[service]]
            name = "worker"
            command = "/bin/worker"
            depends_on = ["api", "db"]
        "#;
        let cfg = Config::from_toml_str(s).unwrap();
        assert_eq!(
            cfg.services[2].depends_on,
            vec!["api".to_string(), "db".to_string()]
        );
    }

    #[test]
    fn rejects_missing_dependency_ref() {
        let s = r#"
            [[service]]
            name = "api"
            command = "/bin/api"
            depends_on = ["ghost"]
        "#;
        let err = Config::from_toml_str(s).unwrap_err().to_string();
        assert!(err.contains("不存在"), "{err}");
    }

    #[test]
    fn rejects_self_dependency() {
        let s = r#"
            [[service]]
            name = "api"
            command = "/bin/api"
            depends_on = ["api"]
        "#;
        assert!(
            Config::from_toml_str(s)
                .unwrap_err()
                .to_string()
                .contains("自引用")
        );
    }

    #[test]
    fn rejects_dependency_cycle() {
        let s = r#"
            [[service]]
            name = "a"
            command = "/bin/a"
            depends_on = ["b"]
            [[service]]
            name = "b"
            command = "/bin/b"
            depends_on = ["c"]
            [[service]]
            name = "c"
            command = "/bin/c"
            depends_on = ["a"]
        "#;
        let err = Config::from_toml_str(s).unwrap_err().to_string();
        assert!(err.contains("依赖环"), "{err}");
    }

    #[test]
    fn parses_full_example() {
        let example = include_str!("../../../config/config.example.toml");
        let cfg = Config::from_toml_str(example)
            .unwrap_or_else(|e| panic!("example config must parse: {e}"));
        assert_eq!(cfg.services.len(), 1);
        assert_eq!(cfg.checks.len(), 1);
    }
}
