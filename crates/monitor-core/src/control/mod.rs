//! 控制平面 HTTP 服务 (feature `control-http`,DESIGN.md §9.2,axum)。
//!
//! 端点：
//! - `GET /`        内嵌只读状态页 (F22:`include_str!` 零外部依赖，内联
//!   CSS/JS;JS 每 2s fetch `/status` 与 `/metrics` 渲染;不提供 reload 入口)
//! - `GET /health`  浅存活 (进程在即可，200 空体)
//! - `GET /ready`   配置加载成功 + 调度循环已启动 (**降级采集不影响 ready**):
//!   ready → 200，否则 503;响应体 `{"ready":bool}`
//! - `GET /status`  StatusReport(经 watch 读 eval 最新汇报)
//! - `GET /metrics` 运行指标:**内容协商** —— `?format=prometheus` 或
//!   `Accept: text/plain` 返回 Prometheus 文本 (DESIGN F13);默认 JSON。
//!   告警计数器来自 alert counters，探测/恢复/运行概览来自 eval 每轮发布
//! - `GET /metrics/history?range=<secs>`(F18 历史指标环形缓冲):窗口内样本
//!   JSON `{"samples":[{at, report}...], "count": N}`;range 缺省 300、上限
//!   [`METRICS_HISTORY_CAP`](超出收敛到上限);0/负/非数字 → 400;窗口内样本
//!   超过 600 点等距降采样 (保留首尾)。与 `/metrics`(瞬时) 并存，Prometheus
//!   文本端点不受影响
//! - `POST /v1/restart/{target}` / `POST /v1/check/{target}` / `POST /v1/reset/{target}`
//!   → `ControlCommand` 经 mpsc(16) 进 eval:入队成功 202;
//!   通道满 (Full)→ 503;通道关闭 (停机中)→ 503(DESIGN §2.2 背压策略)
//! - `POST /v1/reload`(F14 配置热重载):body 可选 JSON `{"path": "..."}`(缺省
//!   重读启动路径)→ `ControlCommand::Reload` 入队 202;非法 body → 400;
//!   满/关闭 → 503。仅应用目标拓扑 (checks/services/watches);其余小节变更
//!   要求重启，不热应用 (eval::reload 模块 doc)
//! - `POST /v1/silence/{target}`(F27 目标级静默):body JSON
//!   `{"duration": <秒，u64>, "reason": "<可选>"}` → `ControlCommand::Silence`
//!   入队 202;非法/缺 duration → 400;满/关闭 → 503。时长合法性 (0 与超 7d)
//!   与 target 存在性由 eval 侧拒绝 (202 入队 + WARN 审计模式，与 restart/check
//!   一致);`DELETE /v1/silence/{target}` → `ControlCommand::Unsilence` 入队
//!   202 (提前解除)。两端点均在 protected 路由 (F23 鉴权自动生效)
//! - `GET /v1/config`(F33 生效配置查看):返回 `Config::redacted()` JSON
//!   快照 ([`ControlShared::config_view`],runtime 构造) —— token/签名密钥
//!   以 `***redacted***` 占位，明文不出 agent;protected 路由 (F23 鉴权)
//!
//! ## 停机与 409 语义 (v0.1，如实记录)
//!
//! [`serve`] 是**无限 future**:内部 `axum::serve` 循环仅在 bind 失败或底层
//! IO 错误时返回;停机由 runtime `abort` 本任务完成 (DESIGN §2.2:runtime
//! 负责装配与停机顺序),本函数不做优雅停机。
//!
//! 「Fatal 态 restart → 409」(2026-11-11 接线):经 status watch 读取该 target
//! 的恢复状态，Fatal → 409 + 提示 reset;非 Fatal 一律 202 入队 (fire-and-forget
//! 语义不变，eval 侧保留防御性拒绝 + journal 审计)。
//!
//! ## F23 可选 Bearer 访问令牌
//!
//! `ControlShared.token = Some(..)` 时 (配置 `[control] token`),除 `GET /health`
//! 外**全部端点**(状态页 `GET /`、/ready、/status、/metrics*、/v1/*) 要求
//! `Authorization: Bearer <token>`;缺失/格式错误/值不匹配一律 **401**(JSON
//! 简短说明，不区分失败原因 —— 不泄漏「头缺失还是值错误」)。比较用手写
//! [`ct_eq`](恒定时间，长度差计入，零新依赖)。token 值不落任何日志/审计
//! (失败审计只记方法与端点)。`token = None` 时中间件直接放行并**忽略**任何
//! Authorization 头 —— 行为与未引入 F23 前完全一致 (向后兼容)。
//!
//! ## F32 控制面 HTTPS(可选)
//!
//! `[control] tls_cert` / `tls_key`(PEM 路径，成对必填，校验在 config) 配置后，
//! 控制面以**纯 HTTPS** 监听同一 `listen` 地址 (端口复用，无 HTTP/HTTPS 并存);
//! 未配置 = 纯 HTTP，现状路径零改动。明文 HTTP 请求打到 TLS 端口没有优雅
//! 降级 —— 服务端在 TLS 握手层直接回 alert，客户端表现为连接错误 (如实
//! 文档化，见 DESIGN §9.2)。
//!
//! - **加载时机**:[`load_tls_config`] 由 runtime 在装配期调用 (与端口 bind
//!   同列早失败点),失败 → `Err` → runtime 早退 → main 退出码 2;错误只含
//!   路径与解析原因，**私钥内容不进错误/日志**(与 token 同纪律)。
//! - **provider**:axum-server `tls-rustls` feature 优先复用进程级已装的
//!   `CryptoProvider`(reqwest 同线 aws-lc-rs),否则按自身 feature 经
//!   `builder_with_provider` 局部构造 aws-lc provider —— **不调用
//!   `install_default`**,无进程级重复安装 panic 面。
//! - **停机**:TLS 接受循环与 HTTP 模式同口径，由 runtime abort 本任务完成
//!   (见「停机与 409 语义」);abort 即释放监听端口，在途连接任务 (axum-server
//!   与 `axum::serve` 同为 per-connection spawn) 自然收尾 —— 不引入
//!   `Handle::graceful_shutdown`,停机保持单一 abort 路径。
//! - **轮换**:证书与私钥在加载时读入内存，轮换 = 重启 agent(与 control 段
//!   其余字段「变更要求重启」一致)。
//!
//! ## F38 控制面 mTLS(可选客户端证书校验)
//!
//! `[control] client_ca_cert`(PEM 路径,config 校验强制与 tls_cert/tls_key
//! 同配)配置后,服务端在 TLS 握手层**要求**客户端证书:证书须由该 CA(或其
//! 下级 CA)签发且在有效期内,否则握手以 alert 失败 —— 无证书/不可信证书
//! 的客户端表现为连接错误,不产生任何 HTTP 层交互。`None` = 不请求客户端
//! 证书,行为与 F32 完全一致(向后兼容)。
//!
//! - **实现**:`rustls::server::WebPkiClientVerifier`(严格 webpki 校验,
//!   不用 `allow_untrusted`/`allow_unauthenticated`) 经
//!   `builder_with_provider(roots, provider)` 复用与 F32 同一 provider 选型
//!   (进程级 `CryptoProvider::get_default()` 优先,否则局部构造 aws-lc
//!   provider,**不 install_default**);ServerConfig 以
//!   `with_client_cert_verifier(..).with_single_cert(..)` 手工构建后交
//!   `RustlsConfig::from_config`(axum-server 该构造**不代设 ALPN**,此处
//!   对齐其内部 helper 的 `h2`/`http/1.1` 序列手工补齐)。
//! - **加载时机**:与 F32 同列 —— [`load_tls_config`] 装配期调用,CA 文件
//!   缺失/解析失败/无有效证书 → `Err` → runtime 早退 → main 退出码 2;
//!   CA 证书解析**严格模式**(任一 PEM 段失败即拒绝,不做
//!   `add_parsable_certificates` 宽容收集)。错误只含路径与解析原因,证书/
//!   私钥内容不进错误/日志。
//! - **鉴权叠加语义**:mTLS 在 TLS 握手层,Bearer (F23) 在 HTTP 层 ——
//!   两者可独立叠加(信任链三件套 TLS + Bearer + mTLS,DESIGN §9.2.2);
//!   鉴权中间件仍在 TLS 层之后,401 语义不变,mTLS 失败根本到不了 HTTP 层。
//! - **停机/轮换**:与 F32 同口径(接受循环由 runtime abort;材料读入内存,
//!   轮换 = 重启 agent)。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{mpsc, watch};

use crate::collect::CollectOutput;
use crate::error::Result;
use crate::model::control::{ControlCommand, MetricsReport, StatusReport};

// F18 历史指标端点专用导入 (仅 control-http feature 参与编译)。
#[cfg(feature = "control-http")]
use crate::model::control::{METRICS_HISTORY_CAP, MetricsHistory, MetricsSample};
#[cfg(feature = "control-http")]
use std::time::Duration;

/// axum 相关导入 (仅 control-http feature 参与编译)。
#[cfg(feature = "control-http")]
use axum::extract::{Path, State};
#[cfg(feature = "control-http")]
use axum::http::StatusCode;
#[cfg(feature = "control-http")]
use axum::routing::{get, post};
#[cfg(feature = "control-http")]
use axum::{Json, Router};

/// 控制平面共享状态 (runtime 装配，只读快照 + 命令出口)。
pub struct ControlShared {
    pub started_at: SystemTime,
    pub ready: Arc<std::sync::atomic::AtomicBool>,
    pub status_rx: watch::Receiver<Arc<StatusReport>>,
    pub metrics_rx: watch::Receiver<Arc<MetricsReport>>,
    pub snapshot_rx: watch::Receiver<Arc<CollectOutput>>,
    /// eval 命令入口 (容量小，满即 503 —— DESIGN §2.2 背压策略)。
    pub cmd_tx: mpsc::Sender<ControlCommand>,
    /// 供 `/metrics` 补充告警计数。
    pub alert_counters: Arc<crate::alert::AlertCounters>,
    /// F23:可选 Bearer 访问令牌 (`[control] token`);`None` = 不鉴权
    /// (任何 Authorization 头被忽略，行为与 v0.3 完全一致)。
    pub token: Option<String>,
    /// F28:journal 当前文件路径 (`<data_dir>/journal.jsonl`),`GET /v1/journal`
    /// 尾部查询用。F35:轮转上一代 `journal.1.jsonl` 存在时一并尾读
    /// (gen1 行在前、当前文件行在后合并;更早代不存在 —— F24 只保留一代)。
    pub journal_path: std::path::PathBuf,
    /// F33:`GET /v1/config` 返回的生效配置视图 —— watch 接收端，初始值由
    /// runtime 构造 (`serde_json::json!(cfg.redacted())`,token/签名密钥以
    /// `***redacted***` 占位，其余原样);eval reload 成功后刷新新值。
    /// 控制平面只读透传，不持有明文密钥。
    pub config_view: watch::Receiver<Arc<serde_json::Value>>,
}

/// F32/F38:控制面 TLS 配置装配 (启动期早失败点):`cert`/`key` 皆 `None` →
/// `Ok(None)`(纯 HTTP，现状);成对有值 → `RustlsConfig::from_pem_file` 加载
/// PEM 证书/私钥，失败 → `Err`(runtime 在装配期 await 本函数，Err 早退 →
/// main 退出码 2，与端口占用同列)。单边有值 (程序化构造绕过 config 校验时)
/// → `Err(Config)`。错误信息仅含路径与解析原因 —— 文件内容 (尤其私钥) 不进
/// 错误/日志。provider 选型与停机语义见模块 doc「F32 控制面 HTTPS」。
///
/// F38:`client_ca = Some(ca)`(config 校验强制与 cert/key 同配)时以
/// [`build_mtls_config`] 构建**带客户端证书校验**的 ServerConfig(严格
/// WebPkiClientVerifier,见模块 doc「F38 控制面 mTLS」);`None` → F32 现状
/// 路径零改动。
#[cfg(feature = "control-http")]
pub async fn load_tls_config(
    cert: Option<&std::path::Path>,
    key: Option<&std::path::Path>,
    client_ca: Option<&std::path::Path>,
) -> Result<Option<axum_server::tls_rustls::RustlsConfig>> {
    match (cert, key) {
        (None, None) => {
            // 兜底 (config validate 已拒绝):client_ca 不得脱离 HTTPS 单配。
            if client_ca.is_some() {
                return Err(crate::error::CoreError::Config(
                    "control: client_ca_cert 必须与 tls_cert/tls_key 同时配置 (mTLS 建立在 HTTPS 之上)"
                        .into(),
                ));
            }
            Ok(None)
        }
        (Some(cert), Some(key)) => match client_ca {
            None => {
                // from_pem_file:IO 错误自带路径上下文 (fs-err),PEM 解析错误只
                // 描述格式问题，均不回显文件内容。
                axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
                    .await
                    .map(Some)
                    .map_err(|e| {
                        crate::error::CoreError::Control(format!("control tls load failed: {e}"))
                    })
            }
            Some(ca) => build_mtls_config(cert, key, ca).await.map(Some),
        },
        _ => Err(crate::error::CoreError::Config(
            "control: tls_cert / tls_key 必须同时配置 (或都不配 = 纯 HTTP)".into(),
        )),
    }
}

/// F38 mTLS provider 选型:与 axum-server 内部 helper 同口径 —— 进程级
/// `CryptoProvider::get_default()` 优先 (reqwest/axum-server 同线 aws-lc-rs),
/// 否则按 feature 合并结果局部构造 aws-lc provider。**不调用
/// `install_default`**,无进程级重复安装 panic 面 (模块 doc「F32 provider」)。
#[cfg(feature = "control-http")]
fn tls_crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    if let Some(provider) = rustls::crypto::CryptoProvider::get_default() {
        return provider.clone();
    }
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// F38:带客户端证书校验的 TLS 配置装配 (mTLS)。CA/server 证书与私钥的
/// 文件 IO + PEM 解析放 `spawn_blocking`(启动期一次性,避免阻塞 worker;
/// 与 axum-server `from_pem_file` 内部同做法)。失败 → `Err`(装配期早退,
/// 与 F32 同列);错误只含路径与解析原因,证书/私钥内容不进错误/日志。
#[cfg(feature = "control-http")]
async fn build_mtls_config(
    cert: &std::path::Path,
    key: &std::path::Path,
    ca: &std::path::Path,
) -> Result<axum_server::tls_rustls::RustlsConfig> {
    let cert = cert.to_path_buf();
    let key = key.to_path_buf();
    let ca = ca.to_path_buf();
    tokio::task::spawn_blocking(move || build_mtls_config_blocking(&cert, &key, &ca))
        .await
        .map_err(|e| {
            crate::error::CoreError::Control(format!("control tls load task failed: {e}"))
        })?
}

/// [`build_mtls_config`] 的阻塞实现 (spawn_blocking 内执行)。
///
/// CA 证书解析为**严格模式**:逐段 `RootCertStore::add`,证书段损坏
/// (证书段损坏;非证书段会被静默跳过) 即 `Err` —— 信任链配置错误宁可启动失败,不做
/// `add_parsable_certificates` 宽容收集 (静默丢根 = 拒信面扩大)。
#[cfg(feature = "control-http")]
fn build_mtls_config_blocking(
    cert: &std::path::Path,
    key: &std::path::Path,
    ca: &std::path::Path,
) -> Result<axum_server::tls_rustls::RustlsConfig> {
    use rustls::RootCertStore;
    use rustls::pki_types::CertificateDer;
    use rustls::server::{ServerConfig, WebPkiClientVerifier};

    // 客户端 CA 信任锚:文件缺失/坏 PEM/空文件全部装配期拒绝 (错误带路径,
    // 不回显内容)。
    let mut ca_reader = std::io::BufReader::new(std::fs::File::open(ca).map_err(|e| {
        crate::error::CoreError::Control(format!(
            "control client CA open failed ({}): {e}",
            ca.display()
        ))
    })?);
    let mut roots = RootCertStore::empty();
    for item in rustls_pemfile::certs(&mut ca_reader) {
        let der = item.map_err(|e| {
            crate::error::CoreError::Control(format!(
                "control client CA parse failed ({}): {e}",
                ca.display()
            ))
        })?;
        roots.add(der).map_err(|e| {
            crate::error::CoreError::Control(format!(
                "control client CA add failed ({}): {e}",
                ca.display()
            ))
        })?;
    }
    if roots.is_empty() {
        return Err(crate::error::CoreError::Control(format!(
            "control client CA contains no certificate ({})",
            ca.display()
        )));
    }

    // 服务端证书链 + 私钥 (rustls-pemfile 2;私钥支持 PKCS#1/#8/SEC1)。
    let mut cert_reader = std::io::BufReader::new(std::fs::File::open(cert).map_err(|e| {
        crate::error::CoreError::Control(format!(
            "control tls load failed ({}): {e}",
            cert.display()
        ))
    })?);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<std::io::Result<_>>()
        .map_err(|e| {
            crate::error::CoreError::Control(format!(
                "control tls load failed ({}): {e}",
                cert.display()
            ))
        })?;
    let mut key_reader = std::io::BufReader::new(std::fs::File::open(key).map_err(|e| {
        crate::error::CoreError::Control(format!(
            "control tls load failed ({}): {e}",
            key.display()
        ))
    })?);
    let key_der = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| {
            crate::error::CoreError::Control(format!(
                "control tls load failed ({}): {e}",
                key.display()
            ))
        })?
        .ok_or_else(|| {
            crate::error::CoreError::Control(format!(
                "control tls key contains no private key ({})",
                key.display()
            ))
        })?;

    // 严格客户端证书校验器:必须由 CA 链签发 (不 allow_untrusted /
    // 不 allow_unauthenticated —— 后者会退化为「证书可选」)。
    let provider = tls_crypto_provider();
    let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .map_err(|e| {
            crate::error::CoreError::Control(format!(
                "control client CA verifier build failed: {e}"
            ))
        })?;
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| {
            crate::error::CoreError::Control(format!(
                "control tls protocol versions unavailable: {e}"
            ))
        })?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key_der)
        .map_err(|e| {
            crate::error::CoreError::Control(format!(
                "control tls load failed ({} / {}): {e}",
                cert.display(),
                key.display()
            ))
        })?;
    // `RustlsConfig::from_config` 不代设 ALPN (其 doc 明示需手工设置):对齐
    // axum-server 内部 helper 的 ALPN_PROTOCOLS (h2 优先,回退 http/1.1),
    // 缺失会导致握手后无应用协议协商。
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    tracing::info!("control plane mTLS enabled (client certificate required)");
    Ok(axum_server::tls_rustls::RustlsConfig::from_config(
        Arc::new(config),
    ))
}

/// 启动控制平面服务 (**无限 future**):绑定 `listen` 后进入服务循环，
/// 仅在 bind 失败或底层 IO 错误时返回。停机由 runtime abort 本任务完成
/// (见模块 doc「停机与 409 语义」)。
///
/// F32:`tls = Some(config)`(经 [`load_tls_config`],皆 None → None) 时同一
/// 地址改以 HTTPS 监听;`None` = 纯 HTTP，行为与 v0.5 完全一致。
#[cfg(feature = "control-http")]
pub async fn serve(
    listen: SocketAddr,
    shared: Arc<ControlShared>,
    tls: Option<axum_server::tls_rustls::RustlsConfig>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    bind_and_serve(listener, shared, tls).await
}

/// 绑定已完成的 serve 入口:runtime 先自行 bind 以提前暴露端口占用错误，
/// 再 spawn 本函数;单测对 127.0.0.1:0 临时端口直接调用。
///
/// F32:HTTP(`tls = None`) 沿用 `axum::serve`(现状路径零改动);TLS 模式换
/// axum-server 复用**同一预 bind 监听器**(端口占用仍在装配期早退),TLS
/// 握手 + HTTP 由同一 Server 驱动，接受循环可被 runtime abort(停机语义见
/// 模块 doc「F32 控制面 HTTPS」)。
#[cfg(feature = "control-http")]
pub(crate) async fn bind_and_serve(
    listener: tokio::net::TcpListener,
    shared: Arc<ControlShared>,
    tls: Option<axum_server::tls_rustls::RustlsConfig>,
) -> Result<()> {
    // F23:鉴权开启提示 (只报状态，绝不打印 token 值)。
    if shared.token.is_some() {
        tracing::info!("control plane bearer token auth enabled; GET /health exempt");
    }
    let app = router(shared);
    match tls {
        None => axum::serve(listener, app)
            .await
            .map_err(crate::error::CoreError::from),
        Some(config) => {
            // TLS 模式：监听器转 std 交 axum-server(from_tcp_rustls 内部会
            // 重设 non-blocking)。serve 循环与 HTTP 模式同为可 abort 的接受
            // 循环;错误仅 bind/IO 类别，不含密钥内容。
            axum_server::from_tcp_rustls(listener.into_std()?, config)?
                .serve(app.into_make_service())
                .await
                .map_err(crate::error::CoreError::from)
        }
    }
}

/// feature 关闭时的占位：控制平面不可用，调用即返回错误 (不 panic)。
#[cfg(not(feature = "control-http"))]
pub async fn serve(_listen: SocketAddr, _shared: Arc<ControlShared>) -> Result<()> {
    Err(crate::error::CoreError::Control(
        "control-http feature is disabled".into(),
    ))
}

/// axum Router(axum 0.8 路由语法:`{target}`,非 `:target`)。
///
/// F23:路由拆成两个子 Router —— `public` 仅含 `GET /health`(守护进程/负载
/// 均衡探活契约，**永不鉴权**);其余全部端点进 `protected` 并叠加 Bearer 层
/// ([`auth_bearer`])。`token = None` 时层内直接放行，行为不变。
#[cfg(feature = "control-http")]
fn router(shared: Arc<ControlShared>) -> Router {
    let public = Router::new()
        .route("/health", get(health))
        .with_state(shared.clone());
    let protected = Router::new()
        .route("/", get(status_page))
        .route("/ready", get(ready))
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        .route("/metrics/history", get(metrics_history))
        .route("/v1/restart/{target}", post(restart))
        .route("/v1/check/{target}", post(check))
        .route("/v1/reset/{target}", post(reset))
        .route("/v1/reload", post(reload))
        .route("/v1/silence/{target}", post(silence).delete(unsilence))
        .route("/v1/journal", get(journal))
        .route("/v1/config", get(config_view))
        .layer(axum::middleware::from_fn_with_state(
            shared.clone(),
            auth_bearer,
        ))
        .with_state(shared);
    public.merge(protected)
}

// ---------------------------------------------------------------------------
// F23 Bearer 鉴权 (可选访问令牌)
// ---------------------------------------------------------------------------

/// 恒定时间字节比较 (F23，零依赖手写):逐字节 XOR 累积 OR,**长度差计入**
/// (`a.len() ^ b.len()` 非零即判负;比较轮数只取决于较长切片，不因前缀命中
/// 提前返回)。用于 token 全等判定，防时序侧信道 —— 拒绝耗时不随「错了几个
/// 字节 / 错在第几个字节」变化。
#[cfg(feature = "control-http")]
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = (a.len() ^ b.len()) as u64;
    for i in 0..a.len().max(b.len()) {
        let x = a.get(i).copied().unwrap_or(0) as u64;
        let y = b.get(i).copied().unwrap_or(0) as u64;
        diff |= x ^ y;
    }
    diff == 0
}

/// F23 鉴权判定 (纯函数，便于单测):
/// - `expected = None`(未配置)→ 恒放行 (调用方带不带任何 Authorization 头
///   行为一致 —— agent 未配置 token 时忽略任何 Authorization 头，F23 验收);
/// - `expected = Some(t)` → 头必须为 `Authorization: Bearer <token>`(scheme
///   大小写不敏感，RFC 7235;token 部分经 [`ct_eq`] 恒定时间比较)。
///   缺失 / 格式错误 / 值不匹配 → 拒绝。
///
/// 返回 `true` = 放行。
#[cfg(feature = "control-http")]
fn authorize(expected: Option<&str>, header: Option<&str>) -> bool {
    let Some(expected) = expected else {
        return true;
    };
    let Some(header) = header else {
        return false;
    };
    let rest = match header.split_once(' ') {
        Some((scheme, rest)) if scheme.eq_ignore_ascii_case("Bearer") => rest.trim(),
        _ => return false,
    };
    !rest.is_empty() && ct_eq(rest.as_bytes(), expected.as_bytes())
}

/// F23 Bearer 鉴权中间件 (仅叠加在受保护子路由;`/health` 豁免见 [`router`])。
/// 未配置 token 直接放行;配置后缺失/错误一律 401(统一 JSON 说明体，
/// `WWW-Authenticate: Bearer`)。失败审计只记方法与端点 —— token 值与任何
/// 传入头内容均不落日志 (F23「token 不进日志/审计」)。
#[cfg(feature = "control-http")]
async fn auth_bearer(
    State(shared): State<Arc<ControlShared>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(expected) = shared.token.as_deref() else {
        // 未配置 token:忽略任何 Authorization 头，行为与 v0.3 完全一致。
        return next.run(req).await;
    };
    let header = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    if authorize(Some(expected), header) {
        return next.run(req).await;
    }
    // 失败审计：只记端点与方法，不记 token 值。
    tracing::warn!(
        method = %req.method(),
        path = %req.uri().path(),
        "control request unauthorized"
    );
    (
        StatusCode::UNAUTHORIZED,
        [(axum::http::header::WWW_AUTHENTICATE, "Bearer")],
        Json(serde_json::json!({
            "error": "unauthorized: missing or invalid bearer token"
        })),
    )
        .into_response()
}

/// `GET /health`:浅存活 —— 进程在即 200 空体 (不做任何依赖检查)。
#[cfg(feature = "control-http")]
async fn health() -> StatusCode {
    StatusCode::OK
}

/// 内嵌只读状态页 (F22):`include_str!` 编译期嵌入，零外部依赖 (内联
/// CSS/JS,无 CDN/字体/图标);页面自身只读 —— 不提供 reload 等写操作入口。
#[cfg(feature = "control-http")]
const STATUS_PAGE_HTML: &str = include_str!("status_page.html");

/// `GET /`(F22 Web 状态页):`Html` 以 `text/html; charset=utf-8` 返回。
/// 页内 JS 每 2s fetch `/status` 与 `/metrics` 渲染 (见 status_page.html)。
#[cfg(feature = "control-http")]
async fn status_page() -> axum::response::Html<&'static str> {
    axum::response::Html(STATUS_PAGE_HTML)
}

/// `GET /ready`:ready → 200，否则 503;响应体 `{"ready":bool}`。
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

/// `GET /status`:eval 最新汇报。初始值由 runtime 写入 (空 targets);
/// sender 消失 (eval 终止) 后仍可读最后一份快照 —— 不 panic、不 5xx。
#[cfg(feature = "control-http")]
async fn status(State(shared): State<Arc<ControlShared>>) -> Json<StatusReport> {
    // 工作区 serde 未启用 `rc` feature:`Arc<StatusReport>` 不可序列化，
    // 克隆解 Arc(单次 HTTP 响应，开销可忽略)。
    let report = shared.status_rx.borrow().clone();
    Json((*report).clone())
}

/// 合并三路指标来源 (alert 计数器 / eval watch / uptime 推导)。
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
    // P2(第九轮验证):eval 侧的单调 uptime 必须在此拷入 —— 此前 copy 块漏了
    // 本字段，alert 计数器起底值恒 0，导致永远走下方墙钟回退，第八轮"单调
    // 优先"修复实际未生效。
    report.uptime_secs = latest.uptime_secs;
    // F18:环形缓冲句柄随 eval 侧报告透传 (serde skip，不进 /metrics JSON)。
    report.history = latest.history.clone();
    // uptime: prefer the eval-side monotonic source (guards against wall-clock
    // jumps); fall back to the wall clock only while eval has not published
    // yet (startup instant). P3 (round 8): the old max() let the inflated
    // post-jump wall value win forever — the opposite of the "monotonic
    // source first" intent (wall uptime is normally >= the monotonic value).
    if report.uptime_secs == 0 {
        report.uptime_secs = shared
            .started_at
            .elapsed()
            .map(|d| d.as_secs())
            .unwrap_or(0);
    }
    report
}

/// Prometheus 文本渲染 (exposition format;指标前缀 `service_watch_`)。
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
/// 返回 Prometheus 文本 (DESIGN F13)。
#[cfg(feature = "control-http")]
async fn metrics(
    State(shared): State<Arc<ControlShared>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let merged = merged_metrics(&shared);
    // 协商优先级 (评审 P2):显式 ?format 短路 (可压过 Accept);无参数才看
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
// F18 历史指标环形缓冲 (/metrics/history)
// ---------------------------------------------------------------------------

/// `?range=` 缺省窗口 (秒)。
#[cfg(feature = "control-http")]
const METRICS_RANGE_DEFAULT_SECS: u64 = 300;

/// 单次响应的最大样本点数：窗口内超过本值时等距降采样。
#[cfg(feature = "control-http")]
const METRICS_HISTORY_MAX_POINTS: usize = 600;

/// `?range=<secs>` 解析 (纯函数):缺省 300;上限 [`METRICS_HISTORY_CAP`]
/// (超出收敛到上限 —— 缓冲之外本就无数据，超限不算非法);0/负数/非数字
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

/// 窗口过滤 (纯函数):保留 `at ≥ now - range` 的样本 (时间升序入环，过滤
/// 保序)。cutoff 下溢 (墙钟早于 epoch) 时退化为全保留 —— 时钟异常时宁多勿缺。
#[cfg(feature = "control-http")]
fn window_samples(ring: &[MetricsSample], now: SystemTime, range: Duration) -> Vec<MetricsSample> {
    let cutoff = now.checked_sub(range).unwrap_or(SystemTime::UNIX_EPOCH);
    ring.iter().filter(|s| s.at >= cutoff).cloned().collect()
}

/// 等距降采样 (纯函数):`len > max` 时按整数四舍五入等距抽取恰好 `max` 点，
/// **保留首尾**(最新样本必须在内 —— 运维视角最后一点即当前状态);否则
/// 原样克隆返回。`max <= 1` 且非空 → 仅保留最新一点。len > max 保证相邻
/// 抽取索引严格递增 (round 单调 + 步距 > 1),无需去重。
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
/// metrics watch 分发的环形缓冲句柄 (单写者;此处只读，锁内仅做克隆)。
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
    // 快照 (eval 单写者，毒化恢复与 eval 侧一致：历史可丢、不 panic)。
    let ring: Vec<MetricsSample> = match latest.history.lock() {
        Ok(guard) => guard.iter().cloned().collect(),
        Err(poisoned) => poisoned.into_inner().iter().cloned().collect(),
    };
    let windowed = window_samples(&ring, SystemTime::now(), range);
    let kept = downsample(&windowed, METRICS_HISTORY_MAX_POINTS);
    Json(MetricsHistory::new(kept)).into_response()
}

/// `GET /v1/journal` (F28):journal 尾部查询。F35:轮转上一代
/// `journal.1.jsonl` 存在时一并尾读 (gen1 尾部在前、当前文件尾部在后合并;
/// 更早代不存在 —— F24 只保留一代);每文件单次读入有字节上限，尾部
/// 有界扫描在 `spawn_blocking` 中执行 (避免阻塞 worker)。
///
/// 查询参数:`tail` 条数 (默认 200，上限 1000)、`target` 精确名匹配、
/// `type` 事件类型 (serde tag，如 restart/fatal/audit)、`since` epoch 秒
/// (含)。命中行以原始 JSON 对象返回;`truncated = true` 表示读取被字节
/// 上限或条数上限截断 (可能少于 tail 条)。
#[cfg(feature = "control-http")]
async fn journal(
    State(shared): State<Arc<ControlShared>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let tail = match params.get("tail").map(String::as_str) {
        None => JOURNAL_TAIL_DEFAULT,
        Some(s) => match s.parse::<usize>() {
            Ok(n) if (1..=JOURNAL_TAIL_MAX).contains(&n) => n,
            _ => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("invalid tail; expected integer in 1..={JOURNAL_TAIL_MAX}"),
                )
                    .into_response();
            }
        },
    };
    let since = match params.get("since").map(String::as_str) {
        None => None,
        Some(s) => match s.parse::<u64>() {
            Ok(v) => Some(v),
            _ => {
                return (
                    StatusCode::BAD_REQUEST,
                    "invalid since; expected epoch seconds",
                )
                    .into_response();
            }
        },
    };
    let target = params.get("target").filter(|t| !t.is_empty()).cloned();
    let event_type = params.get("type").filter(|t| !t.is_empty()).cloned();

    let path = shared.journal_path.clone();
    let (lines, truncated) = tokio::task::spawn_blocking(move || {
        journal_tail_lines(&path, tail, target.as_deref(), event_type.as_deref(), since)
    })
    .await
    .unwrap_or_else(|_| (Vec::new(), false));
    Json(serde_json::json!({ "truncated": truncated, "lines": lines })).into_response()
}

/// `GET /v1/config` (F33 生效配置查看):返回 [`ControlShared::config_view`] ——
/// `Config::redacted()` JSON 视图 (启动快照;eval reload 成功后刷新)。token /
/// webhook 签名密钥 (全局与路由级) 以 `***redacted***` 占位，**明文不出
/// agent**;其余字段原样 (TLS 路径原样)。视图反映当前进程持有的配置：要求
/// 重启的段 (control/alert/collect/…) 恒为启动值，目标拓扑热重载后刷新。
/// 占位值不可回灌：写回配置文件会把字面量 `***redacted***` 当作真实密钥。
/// 受 F23 鉴权 (protected 路由)。
#[cfg(feature = "control-http")]
async fn config_view(State(shared): State<Arc<ControlShared>>) -> Json<serde_json::Value> {
    // Arc<ControlShared> 下拿不到 &mut receiver，用 borrow() (&self) 而非
    // borrow_and_update() —— 本端点只取最新值，mark-seen 语义无用;
    // Ref<Arc<Value>> → Value 深拷贝 (响应体需要 owned Value)。
    Json(shared.config_view.borrow().as_ref().clone())
}

/// `tail` 缺省条数 (仅 control-http 端点使用;feature 关闭时不存在，避免
/// monitorctl 等无 control-http 构建的 dead_code 警告)。
#[cfg(feature = "control-http")]
const JOURNAL_TAIL_DEFAULT: usize = 200;
#[cfg(feature = "control-http")]
const JOURNAL_TAIL_MAX: usize = 1000;
/// 尾部单次读入字节上限 (**每文件**):journal 单行 ~数百字节，512KB 覆盖
/// 1000 条 + 富余。F35 两代合并至多读入 1MB，仍是有界扫描。
#[cfg(feature = "control-http")]
const JOURNAL_TAIL_MAX_BYTES: u64 = 512 * 1024;

/// journal 尾部过滤纯函数 (可单测):F35 —— 轮转上一代 `journal.1.jsonl`
/// 存在时一并尾读，**gen1 行在前、当前文件行在后** (journal 追加写 = 时间
/// 顺序，合并后自然按时间排列) 统一做坏行隔离 / 过滤 / tail 截断。
/// `truncated` = 任一文件被 [`JOURNAL_TAIL_MAX_BYTES`] 字节上限截断 **或**
/// 合并后总命中数 > tail。上一代不存在 (NotFound) 时行为与单文件时代
/// 完全一致 (F35 向后兼容口径);更早代不存在 —— F24 只保留一代。
#[cfg(feature = "control-http")]
fn journal_tail_lines(
    path: &std::path::Path,
    tail: usize,
    target: Option<&str>,
    event_type: Option<&str>,
    since: Option<u64>,
) -> (Vec<serde_json::Value>, bool) {
    // 读入顺序 = 输出顺序：上一代在前、当前文件在后。
    let gen1 = gen1_query_path(path);
    let mut contents: Vec<String> = Vec::new();
    let mut truncated_by_bytes = false;
    for file in [&gen1, path] {
        let (content, truncated) = journal_tail_read(file);
        truncated_by_bytes |= truncated;
        contents.extend(content);
    }
    let mut matched = Vec::new();
    for content in &contents {
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                continue; // 坏行/半行跳过 (与重放侧隔离语义一致)
            };
            if let Some(t) = target
                && v.get("target").and_then(|x| x.as_str()) != Some(t)
            {
                continue;
            }
            if let Some(ty) = event_type
                && v.get("type").and_then(|x| x.as_str()) != Some(ty)
            {
                continue;
            }
            if let Some(s) = since {
                let at = v
                    .get("at")
                    .and_then(|x| x.get("secs_since_epoch"))
                    .and_then(|x| x.as_u64());
                if at.is_none_or(|secs| secs < s) {
                    continue;
                }
            }
            matched.push(v);
        }
    }
    let truncated = truncated_by_bytes || matched.len() > tail;
    if matched.len() > tail {
        matched.drain(..matched.len() - tail);
    }
    (matched, truncated)
}

/// 单文件有界尾读：从文件尾读 [`JOURNAL_TAIL_MAX_BYTES`],返回
/// (内容，是否被字节上限截断)。NotFound → `(None, false)` = 诚实空结果
/// (journal 未创建 / 无上一代);其他 IO 失败留服务端日志后按空处理 ——
/// F35 起单文件读失败只放弃该文件，不毒化另一代 (gen1 损坏不应掩盖
/// 当前代数据),也不把存储问题静默伪装成"没有数据" (P3 第十轮验证)。
#[cfg(feature = "control-http")]
fn journal_tail_read(path: &std::path::Path) -> (Option<String>, bool) {
    use std::io::{Read as _, Seek as _, SeekFrom};
    // ENOENT = 诚实空结果 (文件未创建)。其他 IO 失败：服务端 WARN + truncated
    // =true(P2 第十二轮验证 —— F35 合并场景下 gen1 读失败若仍返回 false，响应
    // "看起来完整"却缺整代历史;true = 保守口径"可能不完整",schema 零变化)。
    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return (None, false),
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "journal tail read failed");
            return (None, true);
        }
    };
    let len = match file.metadata() {
        Ok(m) => m.len(),
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "journal tail metadata failed");
            return (None, true);
        }
    };
    let start = len.saturating_sub(JOURNAL_TAIL_MAX_BYTES);
    let truncated_by_bytes = start > 0;
    if let Err(e) = file.seek(SeekFrom::Start(start)) {
        tracing::warn!(path = %path.display(), error = %e, "journal tail seek failed");
        return (None, true);
    }
    let mut buf = Vec::with_capacity((len - start).min(JOURNAL_TAIL_MAX_BYTES) as usize);
    if let Err(e) = file.read_to_end(&mut buf) {
        tracing::warn!(path = %path.display(), error = %e, "journal tail read_to_end failed");
        return (None, true);
    }
    (
        Some(String::from_utf8_lossy(&buf).into_owned()),
        truncated_by_bytes,
    )
}

/// F35:journal 查询侧的上一代路径推导 —— 委托 recover::journal::gen1_path
/// (第十一轮验证 P3 去重：命名规则单一事实源，writer/查询两侧不会漂移)。
#[cfg(feature = "control-http")]
fn gen1_query_path(path: &std::path::Path) -> std::path::PathBuf {
    crate::recover::journal::gen1_path(path)
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

/// `POST /v1/check/{target}`:绑定检查的 next_due 立即到期 (eval 侧处理)。
#[cfg(feature = "control-http")]
async fn check(
    State(shared): State<Arc<ControlShared>>,
    Path(target): Path<String>,
) -> (StatusCode, &'static str) {
    send_command(&shared, ControlCommand::Check { target })
}

/// `POST /v1/reset/{target}`:解除 Fatal 闩锁 + 清空预算 (eval 侧写审计)。
#[cfg(feature = "control-http")]
async fn reset(
    State(shared): State<Arc<ControlShared>>,
    Path(target): Path<String>,
) -> (StatusCode, &'static str) {
    send_command(&shared, ControlCommand::Reset { target })
}

/// `POST /v1/reload` 请求体 (可选):`{"path": "..."}` 指定配置文件;
/// 空 body / `{}` / `{"path": null}` → None(eval 侧回填启动配置路径)。
#[cfg(feature = "control-http")]
#[derive(Debug, Default, serde::Deserialize)]
struct ReloadBody {
    #[serde(default)]
    path: Option<String>,
}

/// `POST /v1/reload`(F14 配置热重载):触发 eval 重读配置并应用目标拓扑。
/// body 可选 (空 / JSON `{"path": "..."}`);`?format` 无关。入队 202;
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

/// `POST /v1/silence/{target}` 请求体 (F27):`{"duration": <秒>, "reason":
/// "<可选>"}`。duration 必填 (u64 秒);缺 duration / 非数字 / 非 JSON → 400。
/// `duration = 0` 语法合法 → 202 入队，语义拒绝由 eval 侧完成 (WARN + 审计，
/// 与 restart/check 的入队后校验模式一致)。
#[cfg(feature = "control-http")]
#[derive(Debug, serde::Deserialize)]
struct SilenceBody {
    duration: u64,
    #[serde(default)]
    reason: Option<String>,
}

/// `POST /v1/silence/{target}`(F27 目标级静默):该 target 的 Alert/Escalate/Flap
/// 通知在窗口内不投递 (Resolve 永不静默，eval 侧实现)。入队 202;非法 body →
/// 400;通道满/关闭 → 503 (与其它命令端点一致)。deadline 由 eval 侧单调时钟
/// 计算，与墙钟无关。
#[cfg(feature = "control-http")]
async fn silence(
    State(shared): State<Arc<ControlShared>>,
    Path(target): Path<String>,
    body: String,
) -> (StatusCode, &'static str) {
    let parsed = match serde_json::from_str::<SilenceBody>(&body) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                "invalid silence body; expected {\"duration\": <secs>, \"reason\": \"<optional>\"}",
            );
        }
    };
    send_command(
        &shared,
        ControlCommand::Silence {
            target,
            duration_secs: parsed.duration,
            reason: parsed.reason.filter(|r| !r.trim().is_empty()),
        },
    )
}

/// `DELETE /v1/silence/{target}`(F27 提前解除静默):入队 202;无活动静默时
/// 由 eval 侧拒绝并审计。通道满/关闭 → 503。
#[cfg(feature = "control-http")]
async fn unsilence(
    State(shared): State<Arc<ControlShared>>,
    Path(target): Path<String>,
) -> (StatusCode, &'static str) {
    send_command(&shared, ControlCommand::Unsilence { target })
}

/// 命令入队 (DESIGN §2.2:通道满 → 503;关闭 = agent 停机中 → 503)。
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
// 测试 (真实 socket 集成：临时端口 bind_and_serve + reqwest 断言端点行为)
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "control-http", feature = "http-client"))]
mod tests {
    use super::journal_tail_lines;

    /// F28:journal 尾部过滤纯函数 —— tail 截断/过滤/坏行隔离/truncated。
    #[test]
    fn journal_tail_lines_filters_and_truncates() {
        let dir = std::env::temp_dir().join(format!(
            "sw-jtail-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let mut body = String::new();
        for i in 0..10u64 {
            body.push_str(&format!(
                "{{\"type\":\"restart\",\"at\":{{\"secs_since_epoch\":{i},\"nanos_since_epoch\":0}},\"target\":\"app-{i}\",\"fast_exit\":false}}\n"
            ));
        }
        body.push_str("not-a-json-line\n");
        body.push('\n');
        std::fs::write(&path, &body).unwrap();

        // tail 截断：默认取最后 N 条命中
        let (lines, truncated) = journal_tail_lines(&path, 3, None, None, None);
        assert_eq!(lines.len(), 3, "tail=3 取最后 3 条");
        assert!(truncated, "命中数 (10) > tail → truncated");
        assert_eq!(lines[2]["target"], "app-9");

        // target 过滤
        let (lines, _) = journal_tail_lines(&path, 100, Some("app-3"), None, None);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["type"], "restart");

        // since 过滤 (含边界)
        let (lines, _) = journal_tail_lines(&path, 100, None, None, Some(8));
        assert_eq!(lines.len(), 2, "secs 8/9 两条");
        let (lines, _) = journal_tail_lines(&path, 100, None, None, Some(9));
        assert_eq!(lines.len(), 1);

        // 无过滤 = 全部好行，坏行/空行隔离
        let (lines, truncated) = journal_tail_lines(&path, 100, None, None, None);
        assert_eq!(lines.len(), 10, "坏行与空行不计入");
        assert!(!truncated, "命中数 ≤ tail 不截断");

        // 文件不存在
        let (lines, truncated) =
            journal_tail_lines(&dir.join("missing.jsonl"), 5, None, None, None);
        assert!(lines.is_empty() && !truncated);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- F35:journal 查询覆盖轮转上一代 ----

    /// F35:查询侧上一代路径推导与 recover::journal 轮转命名规则一致
    /// (`journal.jsonl` → 同目录 `journal.1.jsonl`;无扩展名/多级扩展名同规则)。
    #[test]
    fn gen1_query_path_matches_rotation_naming() {
        let dir = std::env::temp_dir();
        assert_eq!(
            gen1_query_path(&dir.join("journal.jsonl")),
            dir.join("journal.1.jsonl")
        );
        assert_eq!(
            gen1_query_path(&dir.join("journal")),
            dir.join("journal.1.jsonl")
        );
        assert_eq!(
            gen1_query_path(&dir.join("a.b.jsonl")),
            dir.join("a.b.1.jsonl")
        );
    }

    /// F35:gen1 + 当前文件合并 —— gen1 行在前、当前文件行在后 (时间顺序);
    /// 两文件坏行/空行各自隔离;过滤跨两代生效;tail 截断按合并后总命中数。
    /// 本测试目录无 gen1 时既有用例已锁定 (行为与单文件时代一致)。
    #[test]
    fn journal_tail_lines_merges_gen1_before_current() {
        let dir = std::env::temp_dir().join(format!(
            "sw-jtail-merge-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        // gen1:secs 0..4 + 一个坏行;当前文件:secs 5..9 + 一个坏行 + 空行。
        let mut gen1_body = String::new();
        for i in 0..5u64 {
            gen1_body.push_str(&format!(
                "{{\"type\":\"restart\",\"at\":{{\"secs_since_epoch\":{i},\"nanos_since_epoch\":0}},\"target\":\"app-{i}\",\"fast_exit\":false}}\n"
            ));
        }
        gen1_body.push_str("not-a-json-line\n");
        std::fs::write(dir.join("journal.1.jsonl"), &gen1_body).unwrap();

        let path = dir.join("journal.jsonl");
        let mut body = String::new();
        for i in 5..10u64 {
            body.push_str(&format!(
                "{{\"type\":\"restart\",\"at\":{{\"secs_since_epoch\":{i},\"nanos_since_epoch\":0}},\"target\":\"app-{i}\",\"fast_exit\":false}}\n"
            ));
        }
        body.push_str("garbage\n\n");
        std::fs::write(&path, &body).unwrap();

        // 合并顺序:gen1 的 app-0..4 在前，当前文件的 app-5..9 在后。
        let (lines, truncated) = journal_tail_lines(&path, 100, None, None, None);
        let seq: Vec<&str> = lines
            .iter()
            .map(|v| v["target"].as_str().unwrap())
            .collect();
        assert_eq!(
            seq,
            [
                "app-0", "app-1", "app-2", "app-3", "app-4", "app-5", "app-6", "app-7", "app-8",
                "app-9"
            ],
            "gen1 行在前、当前文件行在后"
        );
        assert!(!truncated, "合并命中 10 ≤ tail 不截断");

        // tail=3 → 合并序列的最后 3 条 (当前文件尾部;gen1 被窗口挤出)。
        let (lines, truncated) = journal_tail_lines(&path, 3, None, None, None);
        let seq: Vec<&str> = lines
            .iter()
            .map(|v| v["target"].as_str().unwrap())
            .collect();
        assert_eq!(seq, ["app-7", "app-8", "app-9"]);
        assert!(truncated, "合并命中 10 > tail → truncated");

        // target 过滤跨两代生效 (gen1 与当前文件各命中 1 条)。
        let (lines, _) = journal_tail_lines(&path, 100, Some("app-2"), None, None);
        assert_eq!(lines.len(), 1, "gen1 侧命中");
        assert_eq!(lines[0]["target"], "app-2");
        let (lines, _) = journal_tail_lines(&path, 100, Some("app-7"), None, None);
        assert_eq!(lines.len(), 1, "当前文件侧命中");

        // 坏行/空行按文件分别隔离，不计入合并命中 (since=3 → secs 3..9 七条)。
        let (lines, _) = journal_tail_lines(&path, 100, None, None, Some(3));
        assert_eq!(lines.len(), 7);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// F35:任一文件被字节上限截断 → truncated 置位，且只丢弃该文件超限
    /// 部分 (每文件独立 512KB 有界读，合并至多 1MB 仍有界),另一文件数据
    /// 完整返回。
    #[test]
    fn journal_tail_lines_truncated_by_bytes_from_either_generation() {
        let dir = std::env::temp_dir().join(format!(
            "sw-jtail-trunc-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        // gen1:单条 ~600KB JSON 行 —— 截断窗口从行中开始，首行成半行被隔离，
        // gen1 贡献 0 条;当前文件两条小行完整保留。
        let filler = "x".repeat(600 * 1024);
        let big = format!("{{\"type\":\"audit\",\"message\":\"{filler}\"}}\n");
        assert!(big.len() as u64 > JOURNAL_TAIL_MAX_BYTES);
        std::fs::write(dir.join("journal.1.jsonl"), &big).unwrap();

        let path = dir.join("journal.jsonl");
        let body = "{\"type\":\"restart\",\"at\":{\"secs_since_epoch\":9,\"nanos_since_epoch\":0},\"target\":\"cur-0\"}\n\
                    {\"type\":\"restart\",\"at\":{\"secs_since_epoch\":10,\"nanos_since_epoch\":0},\"target\":\"cur-1\"}\n";
        std::fs::write(&path, body).unwrap();

        let (lines, truncated) = journal_tail_lines(&path, 100, None, None, None);
        assert!(truncated, "gen1 超 512KB 字节上限 → truncated");
        let seq: Vec<&str> = lines
            .iter()
            .map(|v| v["target"].as_str().unwrap())
            .collect();
        assert_eq!(seq, ["cur-0", "cur-1"], "gen1 半行隔离，当前文件数据完整");

        let _ = std::fs::remove_dir_all(&dir);
    }

    use super::*;
    use crate::collect::{Collector, StubCollector};
    use crate::model::control::{HealthLevel, RecoveryState, TargetMode, TargetStatus};
    use crate::model::ids::ProcessIdentity;
    use std::sync::atomic::Ordering;

    /// 构造测试用 ControlShared(cmd 通道容量可调;sender 侧立即 drop,
    /// 借以覆盖「sender 消失仍可读最后值」的防御路径)。metrics_tx 一并返回，
    /// 供直推 watch 模拟 eval 发布 (F18 历史随 metrics 报告搭载)。
    /// F23:`token` 传 `Some` 即启用 Bearer 鉴权 (见 [`shared_with_config`])。
    /// F33:`config_view` 为 `/v1/config` 快照 (测试自定;生产由 runtime 构造)。
    #[allow(clippy::type_complexity)]
    fn shared_with_config(
        cmd_cap: usize,
        token: Option<&str>,
        config_view: serde_json::Value,
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
        // F33:测试快照经 watch 包装 (生产由 runtime 创建通道，eval reload 刷新)。
        let (_config_view_tx, config_view_rx) = watch::channel(Arc::new(config_view));
        (
            Arc::new(ControlShared {
                started_at: SystemTime::now(),
                ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                status_rx,
                metrics_rx,
                snapshot_rx,
                cmd_tx,
                alert_counters: Arc::new(crate::alert::AlertCounters::default()),
                token: token.map(str::to_string),
                journal_path: std::path::PathBuf::from("journal.jsonl"),
                config_view: config_view_rx,
            }),
            cmd_rx,
            status_tx,
            metrics_tx,
        )
    }

    /// F33 前的默认测试句柄:config_view 置空对象 (既有测试不关心该字段)。
    #[allow(clippy::type_complexity)]
    fn shared_with_token(
        cmd_cap: usize,
        token: Option<&str>,
    ) -> (
        Arc<ControlShared>,
        mpsc::Receiver<ControlCommand>,
        watch::Sender<Arc<StatusReport>>,
        watch::Sender<Arc<MetricsReport>>,
    ) {
        shared_with_config(cmd_cap, token, serde_json::json!({}))
    }

    /// 未配置 token 的默认测试句柄 (既有测试入口;F23 向后兼容语义)。
    #[allow(clippy::type_complexity)]
    fn shared_with(
        cmd_cap: usize,
    ) -> (
        Arc<ControlShared>,
        mpsc::Receiver<ControlCommand>,
        watch::Sender<Arc<StatusReport>>,
        watch::Sender<Arc<MetricsReport>>,
    ) {
        shared_with_token(cmd_cap, None)
    }

    /// 起服务于 127.0.0.1:0 临时端口，返回 base url(随测试 runtime 收尾)。
    /// F32:测试均为纯 HTTP 装配 (tls = None),TLS 行为在 tests/tls_control.rs。
    async fn spawn_server(shared: Arc<ControlShared>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(bind_and_serve(listener, shared, None));
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

        // /metrics:counters 侧字段合并 (alerts_sent / alerts_dropped)
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

        // 空 body → Reload { path: None }(缺省重读启动配置路径，eval 侧回填)
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

    /// reload 命令同样受通道容量约束 (满 → 503)。
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

    // -----------------------------------------------------------------
    // F27 目标级静默 (POST/DELETE /v1/silence/{target})
    // -----------------------------------------------------------------

    /// POST 合法 body → 202 入队 `Silence`(reason 空白过滤为 None，与
    /// 「可选文本」契约一致);DELETE → 202 入队 `Unsilence`。
    #[tokio::test]
    async fn silence_endpoints_enqueue_silence_and_unsilence() {
        let (shared, mut cmd_rx, _status_tx, _metrics_tx) = shared_with(16);
        let base = spawn_server(shared).await;
        let client = reqwest::Client::new();

        // 带 reason
        let resp = client
            .post(format!("{base}/v1/silence/web-1"))
            .body(r#"{"duration": 3600, "reason": "deploy window"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 202);
        let got = cmd_rx.recv().await.expect("command queued");
        assert_eq!(
            got,
            ControlCommand::Silence {
                target: "web-1".into(),
                duration_secs: 3600,
                reason: Some("deploy window".into()),
            }
        );

        // 无 reason 字段 / 空白 reason → None
        for body in [r#"{"duration": 60}"#, r#"{"duration": 60, "reason": "  "}"#] {
            let resp = client
                .post(format!("{base}/v1/silence/api"))
                .body(body)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 202, "{body}");
            let got = cmd_rx.recv().await.expect("command queued");
            assert_eq!(
                got,
                ControlCommand::Silence {
                    target: "api".into(),
                    duration_secs: 60,
                    reason: None,
                },
                "{body}"
            );
        }

        // 提前解除:DELETE → Unsilence
        let resp = client
            .delete(format!("{base}/v1/silence/web-1"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 202);
        let got = cmd_rx.recv().await.expect("command queued");
        assert_eq!(
            got,
            ControlCommand::Unsilence {
                target: "web-1".into(),
            }
        );
    }

    /// 非法 body → 400:非 JSON / 缺 duration / duration 非数字 / 负数。
    /// (duration=0 语法合法 → 202;语义拒绝在 eval 侧，WARN + 审计。)
    #[tokio::test]
    async fn silence_endpoint_rejects_invalid_body_with_400() {
        let (shared, mut cmd_rx, _status_tx, _metrics_tx) = shared_with(16);
        let base = spawn_server(shared).await;
        let client = reqwest::Client::new();

        for body in [
            "not-json",
            "{}",
            r#"{"reason": "no duration"}"#,
            r#"{"duration": "3600"}"#,
            r#"{"duration": -1}"#,
            r#"{"duration": 1.5}"#,
        ] {
            let resp = client
                .post(format!("{base}/v1/silence/web-1"))
                .body(body)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 400, "body={body} 应 400");
        }
        assert!(cmd_rx.try_recv().is_err(), "非法 body 不得产生入队命令");

        // duration=0:语法合法 → 202 入队 (eval 侧拒绝)
        let resp = client
            .post(format!("{base}/v1/silence/web-1"))
            .body(r#"{"duration": 0}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 202);
        let got = cmd_rx.recv().await.expect("command queued");
        assert_eq!(
            got,
            ControlCommand::Silence {
                target: "web-1".into(),
                duration_secs: 0,
                reason: None,
            }
        );
    }

    /// silence 命令同样受通道容量约束 (满 → 503)。
    #[tokio::test]
    async fn silence_endpoint_returns_503_when_queue_full() {
        let (shared, _rx, _status_tx, _metrics_tx) = shared_with(1);
        let base = spawn_server(shared).await;
        let client = reqwest::Client::new();

        let resp = client
            .post(format!("{base}/v1/silence/a"))
            .body(r#"{"duration": 60}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 202);
        let resp = client
            .post(format!("{base}/v1/silence/b"))
            .body(r#"{"duration": 60}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 503);
    }

    // -----------------------------------------------------------------
    // F28 journal 尾部查询端点 (GET /v1/journal):真实文件 + 过滤参数
    // -----------------------------------------------------------------

    /// 真实 journal 文件经端点返回正确过滤结果 (F28 集成):tail 截断 +
    /// truncated 标志 + target/type 过滤 (since 过滤由纯函数单测覆盖，
    /// 墙钟 now 依赖不便在端点层构造)。
    #[tokio::test]
    async fn journal_endpoint_serves_real_file_with_filters() {
        let dir = std::env::temp_dir().join(format!(
            "sw-jend-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let mut body = String::new();
        for i in 0..5u64 {
            body.push_str(&format!(
                "{{\"type\":\"fatal\",\"at\":{{\"secs_since_epoch\":{i},\"nanos_since_epoch\":0}},\"target\":\"app-{i}\",\"reason\":\"boom\"}}\n"
            ));
        }
        for i in 0..3u64 {
            body.push_str(&format!(
                "{{\"type\":\"audit\",\"at\":{{\"secs_since_epoch\":{i},\"nanos_since_epoch\":0}},\"message\":\"msg-{i}\"}}\n"
            ));
        }
        body.push_str("broken-line\n");
        std::fs::write(&path, &body).unwrap();

        let (shared, _rx, _status_tx, _metrics_tx) = shared_with(16);
        // 手工替换 journal_path 指向临时文件 (ControlShared 字段公开;此处为
        // Arc 唯一持有者，get_mut 直接改写)。
        let mut shared = shared;
        Arc::get_mut(&mut shared)
            .expect("no other holders")
            .journal_path = path;
        let base = spawn_server(shared).await;

        // 无参数：全部好行 (8 条),坏行隔离，不截断
        let resp = reqwest::get(format!("{base}/v1/journal")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(v["truncated"], serde_json::Value::Bool(false));
        assert_eq!(v["lines"].as_array().unwrap().len(), 8);

        // tail=3:取最后 3 条，truncated=true
        let resp = reqwest::get(format!("{base}/v1/journal?tail=3"))
            .await
            .unwrap();
        let v: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(v["truncated"], serde_json::Value::Bool(true));
        let lines = v["lines"].as_array().unwrap();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[2]["type"], "audit", "文件尾部是 audit 行");

        // target 过滤
        let resp = reqwest::get(format!("{base}/v1/journal?target=app-2"))
            .await
            .unwrap();
        let v: serde_json::Value = resp.json().await.unwrap();
        let lines = v["lines"].as_array().unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["target"], "app-2");
        assert_eq!(lines[0]["reason"], "boom");

        // type 过滤
        let resp = reqwest::get(format!("{base}/v1/journal?type=audit"))
            .await
            .unwrap();
        let v: serde_json::Value = resp.json().await.unwrap();
        let lines = v["lines"].as_array().unwrap();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0]["message"], "msg-0");

        // 非法参数 → 400
        for q in ["tail=0", "tail=abc", "tail=1001", "since=-1", "since=x"] {
            let resp = reqwest::get(format!("{base}/v1/journal?{q}"))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 400, "query={q} 应 400");
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// F35 × F24 轮转联动：用 recover::journal 产品 writer (`max_size` 触发
    /// 轮转，fs 模式与 journal 轮转测试一致) 真实产生 `journal.1.jsonl`,
    /// 随后 `GET /v1/journal` 能查到**上一代尾部事件**与当前文件事件，且
    /// gen1 侧事件出现在轮转 audit / 当前文件事件之前 (合并顺序 = 时间顺序)。
    #[tokio::test]
    async fn journal_endpoint_reads_rotated_previous_generation() {
        use crate::recover::journal::{
            JournalEvent, channel, open_writer, run_tracked_writer_with_limit,
        };

        let dir = std::env::temp_dir().join(format!(
            "sw-jend-rotate-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        // 产品 writer (阈值 2000 字节显著大于单行 ~150B 与轮转 audit ~400B):
        // 填充把文件推过阈值触发一次轮转 (journal.rs 轮转测试同款 fs 模式);
        // 轮转后当前文件 = 轮转 audit + 第二哨兵，远小于阈值，不再二次轮转
        // (二次轮转会覆盖 journal.1，破坏本用例前提)。
        let file = open_writer(&path).unwrap();
        let (sender, rx) = channel(16);
        let counters = sender.counters().clone();
        let writer = tokio::spawn(run_tracked_writer_with_limit(
            file,
            path.clone(),
            rx,
            counters,
            2000,
        ));
        let audit = |message: &str| JournalEvent::Audit {
            at: std::time::SystemTime::UNIX_EPOCH,
            message: message.to_string(),
        };
        let filler = |i: u64| format!("filler-{i:060}");
        // 第 1 条 = gen1 哨兵;16 条填充 (~2.3KB) 把文件推过阈值;轮转后
        // 第 2 哨兵落当前文件。
        sender
            .send(audit("rotation-query gen1 sentinel"))
            .await
            .unwrap();
        for i in 0..16u64 {
            sender.send(audit(&filler(i))).await.unwrap();
        }
        sender
            .send(audit("rotation-query current sentinel"))
            .await
            .unwrap();
        drop(sender);
        writer.await.unwrap().unwrap();

        let gen1 = dir.join("journal.1.jsonl");
        assert!(gen1.is_file(), "F24 轮转必须已产生上一代");
        let gen1_text = std::fs::read_to_string(&gen1).unwrap();
        assert!(gen1_text.contains("gen1 sentinel"), "哨兵 1 在上一代");
        assert!(!gen1_text.contains("current sentinel"), "哨兵 2 在当前文件");

        let (shared, _rx, _status_tx, _metrics_tx) = shared_with(16);
        let mut shared = shared;
        Arc::get_mut(&mut shared)
            .expect("no other holders")
            .journal_path = path;
        let base = spawn_server(shared).await;

        let resp = reqwest::get(format!("{base}/v1/journal?tail=1000"))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(v["truncated"], serde_json::Value::Bool(false));
        let lines = v["lines"].as_array().unwrap();
        let messages: Vec<&str> = lines.iter().filter_map(|l| l["message"].as_str()).collect();
        let gen1_idx = messages
            .iter()
            .position(|m| m.contains("gen1 sentinel"))
            .expect("端点必须查到上一代 (gen1) 尾部哨兵事件");
        let cur_idx = messages
            .iter()
            .position(|m| m.contains("current sentinel"))
            .expect("当前文件哨兵事件必须可见");
        assert!(
            gen1_idx < cur_idx,
            "gen1 事件必须排在当前文件事件之前 (合并顺序 = 时间顺序)"
        );
        // 两哨兵之间应恰有轮转 audit ("journal rotated: …") —— 证明合并读取
        // 横跨轮转边界且保持追加写顺序。
        assert!(
            messages[gen1_idx + 1..cur_idx]
                .iter()
                .any(|m| m.starts_with("journal rotated: ")),
            "轮转 audit 应落在两哨兵之间：{messages:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn command_returns_503_when_queue_full() {
        let (shared, _rx, _status_tx, _metrics_tx) = shared_with(1);
        let base = spawn_server(shared).await;
        let client = reqwest::Client::new();

        // 容量 1:第一笔入队 (无消费者，滞留通道),第二笔 Full → 503
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
        drop(rx); // eval 侧不存在 (agent 停机语义)
        let base = spawn_server(shared).await;

        let resp = reqwest::Client::new()
            .post(format!("{base}/v1/check/a"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 503);
    }

    /// Prometheus 渲染：全部字段行存在 + 计数/类型注释正确 (F13)。
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
    // F22 Web 状态页 (GET /)
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
    // F18 历史指标环形缓冲 (/metrics/history)
    // -----------------------------------------------------------------

    /// 初始 (eval 未发布) 为空;非法 range(0/负/非数字)→ 400。
    #[tokio::test]
    async fn metrics_history_starts_empty_and_rejects_invalid_range() {
        let (shared, _rx, _status_tx, _metrics_tx) = shared_with(16);
        let base = spawn_server(shared).await;

        let resp = reqwest::get(format!("{base}/metrics/history"))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let hist: MetricsHistory = resp.json().await.unwrap();
        assert!(hist.samples.is_empty(), "eval 尚未发布，应为空");
        assert_eq!(hist.count, 0);

        for bad in ["0", "-1", "abc", "1.5", ""] {
            let resp = reqwest::get(format!("{base}/metrics/history?range={bad}"))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 400, "range={bad:?} 应 400");
        }
    }

    /// eval 发布 (直推 metrics watch 模拟) 后非空;窗口外样本被过滤，
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
        // 窗口外老样本 (epoch，远早于缺省 300s 窗口):必须被过滤。
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
        assert_eq!(hist.count, 3, "窗口外样本被过滤，窗口内全保留");
        assert_eq!(hist.samples.len(), hist.count);
        assert_eq!(hist.samples[0].report.probe_executed, 42);
        assert!(
            hist.samples.windows(2).all(|w| w[0].at <= w[1].at),
            "按入环 (时间升序) 返回"
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

        // range=7200:窗口覆盖全部 700 点 → 降采样至 600，首尾保留。
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

    /// `/metrics` JSON 契约不得携带环形缓冲句柄 (serde skip;monitorctl 兼容
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

    /// range 解析：缺省 300;0/负/非数字非法;超上限收敛到 7200。
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

    /// 窗口过滤:cutoff 之后的样本按序保留，更早的丢弃。
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
        assert_eq!(ats, [100, 200, 399], "cutoff=100 之前的样本被过滤，保序");
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
            assert!(w[0].at < w[1].at, "抽取索引严格递增 (无重复点)");
        }

        let one = downsample(&above, 1);
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].at, sample_at(999).at, "max<=1 保留最新一点");
    }

    // -----------------------------------------------------------------
    // F23 可选 Bearer 访问令牌
    // -----------------------------------------------------------------

    /// ct_eq:等长相同/不同、长度不等 (双向)、空串、含 0 字节差异。
    #[test]
    fn ct_eq_matches_only_identical_slices() {
        assert!(ct_eq(b"secret", b"secret"));
        assert!(!ct_eq(b"secret", b"secreT"), "单字节差异必须判负");
        assert!(!ct_eq(b"secret", b"secre"), "右短判负");
        assert!(!ct_eq(b"secre", b"secret"), "左短判负");
        assert!(ct_eq(b"", b""));
        assert!(!ct_eq(b"", b"x"));
        assert!(!ct_eq(b"abc\0def", b"abc\0dee"), "含 0 字节差异判负");
        assert!(ct_eq(b"abc\0def", b"abc\0def"));
    }

    /// authorize 纯函数：未配置 → 恒放行 (含乱格式头);配置后按格式/值判定。
    #[test]
    fn authorize_decisions() {
        // 未配置 token:任何输入 (含乱格式头) 一律放行 —— 向后兼容语义。
        assert!(authorize(None, None));
        assert!(authorize(None, Some("garbage")));
        assert!(authorize(None, Some("Basic dXNlcjpwYXNz")));
        // 配置后：缺失 / 非 Bearer scheme / 空 token / 值不匹配 → 拒绝。
        assert!(!authorize(Some("t0k"), None), "缺头必须拒绝");
        assert!(!authorize(Some("t0k"), Some("t0k")), "无 scheme 必须拒绝");
        assert!(!authorize(Some("t0k"), Some("Basic t0k")));
        assert!(!authorize(Some("t0k"), Some("Bearer ")), "空 token 拒绝");
        assert!(!authorize(Some("t0k"), Some("Bearer wrong")));
        assert!(!authorize(Some("t0k"), Some("Bearer t0kx")), "超长判负");
        assert!(!authorize(Some("t0k"), Some("Bearer t0")), "过短判负");
        // 值匹配 → 放行;scheme 大小写不敏感;OWS 容忍。
        assert!(authorize(Some("t0k"), Some("Bearer t0k")));
        assert!(authorize(Some("t0k"), Some("bearer t0k")));
        assert!(authorize(Some("t0k"), Some("BEARER t0k")));
        assert!(authorize(Some("t0k"), Some("Bearer   t0k  ")));
    }

    /// 端到端 (router 层):未配置 token —— 带/不带 Authorization 头全部 200
    /// (携带的头被忽略，行为与 v0.3 完全一致)。
    #[tokio::test]
    async fn no_token_config_passes_with_or_without_auth_header() {
        let (shared, _rx, _status_tx, _metrics_tx) = shared_with_token(16, None);
        let base = spawn_server(shared).await;
        let client = reqwest::Client::new();

        // 不带头:200
        let resp = client.get(format!("{base}/status")).send().await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        // 带任意头 (格式乱/值乱):同样 200(未配置 token = 头被忽略)
        for hdr in ["Bearer wrong", "garbage", "Basic dXNlcg=="] {
            let resp = client
                .get(format!("{base}/status"))
                .header(reqwest::header::AUTHORIZATION, hdr)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200, "header={hdr:?}");
        }
    }

    /// 端到端 (router 层):配置 token —— 无头 401 / 错头 401 / 对头 200;
    /// `GET /health` 无头也 200(探活豁免);401 体不含 token 值。
    #[tokio::test]
    async fn token_config_enforces_bearer_and_exempts_health() {
        let (shared, mut cmd_rx, _status_tx, _metrics_tx) = shared_with_token(16, Some("s3cret"));
        let base = spawn_server(shared).await;
        let client = reqwest::Client::new();

        // 受保护端点：无头 → 401(状态页 GET / 同样受保护;F33 /v1/config 亦然)
        for path in [
            "/",
            "/ready",
            "/status",
            "/metrics",
            "/metrics/history",
            "/v1/config",
        ] {
            let resp = client.get(format!("{base}{path}")).send().await.unwrap();
            assert_eq!(resp.status().as_u16(), 401, "GET {path} 无头应 401");
        }
        // 错头 (scheme 错 / 值错)→ 401
        for hdr in ["Basic s3cret", "Bearer wrong", "Bearer s3cretx"] {
            let resp = client
                .get(format!("{base}/status"))
                .header(reqwest::header::AUTHORIZATION, hdr)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 401, "header={hdr:?} 应 401");
        }
        // 401 响应体是统一 JSON 说明，且**不回显**任何 token 片段
        let resp = client.get(format!("{base}/status")).send().await.unwrap();
        assert_eq!(resp.status().as_u16(), 401);
        assert_eq!(
            resp.headers()
                .get(reqwest::header::WWW_AUTHENTICATE)
                .and_then(|v| v.to_str().ok()),
            Some("Bearer")
        );
        let body = resp.text().await.unwrap();
        assert!(body.contains("unauthorized"), "401 体应含说明:{body}");
        assert!(!body.contains("s3cret"), "401 体不得回显 token:{body}");

        // 对头 → 受保护端点全部 200/202
        let auth = reqwest::header::AUTHORIZATION;
        for path in [
            "/",
            "/ready",
            "/status",
            "/metrics",
            "/metrics/history",
            "/v1/config",
        ] {
            let resp = client
                .get(format!("{base}{path}"))
                .header(auth.clone(), "Bearer s3cret")
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200, "GET {path} 对头应 200");
        }
        let resp = client
            .post(format!("{base}/v1/check/api"))
            .header(auth.clone(), "Bearer s3cret")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 202, "v1 写端点对头应 202");
        assert!(cmd_rx.try_recv().is_ok(), "命令应已入队");

        // /health 豁免：无头也 200(守护进程探活契约)
        let resp = client.get(format!("{base}/health")).send().await.unwrap();
        assert_eq!(resp.status().as_u16(), 200, "/health 必须免鉴权");
        // 对头同样 200(不破坏普通客户端)
        let resp = client
            .get(format!("{base}/health"))
            .header(auth, "Bearer s3cret")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
    }

    /// `GET /v1/config`(F33):返回 `Config::redacted()` 快照 —— token /
    /// webhook_secret / 路由级 secret 三类敏感字段以 `***redacted***` 占位
    /// 且**不含明文**;非敏感字段 (webhook url) 原样透出。
    #[tokio::test]
    async fn config_endpoint_returns_redacted_snapshot_without_secrets() {
        let cfg = crate::config::Config::from_toml_str(
            "[control]\ntoken = 'tok-plain'\n\n[alert]\nwebhook_secret = 'sec-plain'\n\
             [[alert.routes]]\nmatch = '*'\nurl = 'https://hooks.example.com/x'\n\
             secret = 'route-plain'\n\
             [metrics]\nenabled = true\nurl = 'https://ep/push'\nsecret = 'push-plain'\n",
        )
        .unwrap();
        let (shared, _rx, _st, _mt) =
            shared_with_config(16, None, serde_json::json!(cfg.redacted()));
        let base = spawn_server(shared).await;

        let resp = reqwest::get(format!("{base}/v1/config")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let body = resp.text().await.unwrap();
        let view: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            view["control"]["token"].as_str(),
            Some("***redacted***"),
            "control.token 以占位符透出"
        );
        assert_eq!(
            view["alert"]["webhook_secret"].as_str(),
            Some("***redacted***"),
            "webhook_secret 以占位符透出"
        );
        assert_eq!(
            view["alert"]["routes"][0]["secret"].as_str(),
            Some("***redacted***"),
            "路由级 secret 以占位符透出"
        );
        assert_eq!(
            view["metrics"]["secret"].as_str(),
            Some("***redacted***"),
            "metrics.secret 以占位符透出 (P1 第十二轮验证:F34 新增字段必须进 redaction)"
        );
        assert!(
            view["alert"]["routes"][0]["url"].as_str() == Some("https://hooks.example.com/x"),
            "非敏感字段原样"
        );
        for secret in ["tok-plain", "sec-plain", "route-plain", "push-plain"] {
            assert!(!body.contains(secret), "响应不得含明文 {secret},got {body}");
        }
        assert!(body.contains("***redacted***"), "占位符出现在响应中");
    }
}
