//! F32 控制面 HTTPS / F38 控制面 mTLS 集成测试 (ROADMAP F32/F38 / DESIGN §9.2):
//! - 测试内用 rcgen 生成自签证书 (纯 Rust，跨平台，不依赖 openssl CLI)→
//!   `load_tls_config` + `serve` 以 TLS 监听 127.0.0.1 随机端口;
//! - `--insecure` 等价客户端 (`danger_accept_invalid_certs`,与 monitorctl
//!   `build_client(true)` 同一 reqwest rustls 配置) 经 https 可达;
//! - 默认校验客户端对自签拒信 (信任链语义);
//! - 明文 http 请求同一端口失败 (配 TLS = 纯 HTTPS，无明文降级);
//! - 坏 PEM 文件 → `load_tls_config` Err(启动装配期早退路径，退出码 2 的
//!   前置);单边 tls_cert/tls_key → Config 错误;
//! - F38:rcgen 自签 CA 同签服务端与客户端证书 → 带 `reqwest::Identity`
//!   (monitorctl `--client-cert/--client-key` 的构建路径) 的客户端 200;
//!   无客户端证书 / 坏 CA 签发的客户端证书 → TLS 握手失败;坏 CA PEM →
//!   装配期 `Err`;`reqwest::Identity::from_pem` 对 rcgen 产物可用。
//!
//! control/mod.rs 不可写依赖 (并行开发),ControlShared 全字段 pub，测试自装配。

#![cfg(all(feature = "control-http", feature = "http-client"))]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use monitor_core::collect::{Collector, StubCollector};
use monitor_core::control::{ControlShared, load_tls_config, serve};
use monitor_core::model::control::{MetricsReport, StatusReport};
use tokio::sync::{mpsc, watch};

/// 自签证书生成 (测试专用):rcgen `generate_simple_self_signed`,PEM 落盘
/// 临时目录，返回 (cert, key) 路径。
fn write_self_signed(dir: &Path) -> (PathBuf, PathBuf) {
    let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()])
        .expect("rcgen self-signed generation");
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    (cert_path, key_path)
}

/// F38:自签测试 CA(rcgen `IsCa::Ca` + Unconstrained 约束),返回 (CA 证书
/// PEM,Issuer —— 供 [`issue_leaf`] 签发 leaf)。`params.self_signed(&key)`
/// 先借 key 签根证书,再 move key 进 Issuer(Issuer 只取 DN/key_id/key_usage,
/// 与证书对象解耦)。
fn test_ca() -> (String, rcgen::Issuer<'static, rcgen::KeyPair>) {
    let mut params =
        rcgen::CertificateParams::new(vec!["service-watch test ca".into()]).expect("ca params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let key = rcgen::KeyPair::generate().expect("ca keypair");
    let cert_pem = params.self_signed(&key).expect("self-signed CA").pem();
    let issuer = rcgen::Issuer::new(params, key);
    (cert_pem, issuer)
}

/// F38:用 CA 签发 leaf 证书并落盘,返回 (cert, key) 路径。`san` 可为 DNS 名
/// 或 IP(rcgen 按 `IpAddr` 解析自动选择 SAN 类型;服务端 leaf 传 `127.0.0.1`,
/// 客户端 leaf 传任意 DNS 名即可 —— 服务端校验客户端证书不比对 SAN)。
fn issue_leaf(
    dir: &Path,
    name: &str,
    san: &str,
    issuer: &rcgen::Issuer<'_, rcgen::KeyPair>,
) -> (PathBuf, PathBuf) {
    let params = rcgen::CertificateParams::new(vec![san.into()]).expect("leaf params");
    let key = rcgen::KeyPair::generate().expect("leaf keypair");
    let cert = params
        .signed_by(&key, issuer)
        .expect("CA-signed leaf certificate");
    let cert_path = dir.join(format!("{name}-cert.pem"));
    let key_path = dir.join(format!("{name}-key.pem"));
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    (cert_path, key_path)
}

/// 唯一临时目录 (pid + 纳秒防并行碰撞，与既有测试同口径)。
fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sw-tls-{tag}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 构造最小 ControlShared(与 control/mod.rs 单测同装配;token 未配置)。
fn shared_for_test() -> Arc<ControlShared> {
    let (_status_tx, status_rx) = watch::channel(Arc::new(StatusReport {
        started_at: SystemTime::now(),
        ready: true,
        targets: Vec::new(),
    }));
    let (_metrics_tx, metrics_rx) = watch::channel(Arc::new(MetricsReport::default()));
    let initial = Arc::new(StubCollector::default().collect(&[]));
    let (_snapshot_tx, snapshot_rx) = watch::channel(initial);
    let (cmd_tx, _cmd_rx) = mpsc::channel(16);
    Arc::new(ControlShared {
        started_at: SystemTime::now(),
        ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        status_rx,
        metrics_rx,
        snapshot_rx,
        cmd_tx,
        alert_counters: Arc::new(monitor_core::alert::AlertCounters::default()),
        token: None,
        journal_path: PathBuf::from("journal.jsonl"),
        // F33 字段 (并行接线):本文件不覆盖 /v1/config,watch 初始 Null。
        config_view: {
            let (_tx, rx) = tokio::sync::watch::channel(Arc::new(serde_json::Value::Null));
            rx
        },
    })
}

/// `--insecure` 等价测试客户端：跳过证书校验 (自签场景，mirror monitorctl)。
fn insecure_client() -> reqwest::Client {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap()
}

/// 起 TLS 服务并返回 base url:`serve` 自行 bind，与首个请求存在竞态 ——
/// 先占随机端口探位再放手，https /health 轮询重试 (bind 被抢则换端口重来，
/// /health 免鉴权恒 200，与 tests/status_page.rs 同一口径)。
/// F38:`client_ca = Some` 时装配 mTLS(握手要求客户端证书),探活也改用
/// 带 CA 信任 + 客户端证书的请求(无证书客户端进不来,/health 不再是探位
/// 信号 —— 探位与断言用同一把「钥匙」)。
async fn spawn_tls_server(
    shared: Arc<ControlShared>,
    cert: &Path,
    key: &Path,
    client_ca: Option<&Path>,
    probe_client: Option<reqwest::Client>,
) -> String {
    let tls = load_tls_config(Some(cert), Some(key), client_ca)
        .await
        .expect("tls material must load")
        .expect("cert/key pair given");
    for _ in 0..10 {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let handle = {
            let shared = shared.clone();
            let tls = tls.clone();
            tokio::spawn(async move {
                let _ = serve(addr, shared, Some(tls)).await;
            })
        };
        for _ in 0..50 {
            let healthy = match &probe_client {
                // mTLS:探活请求自身带客户端证书。
                Some(client) => client
                    .get(format!("https://{addr}/health"))
                    .send()
                    .await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false),
                // 纯 HTTPS (F32):insecure 客户端即可。
                None => insecure_client()
                    .get(format!("https://{addr}/health"))
                    .send()
                    .await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false),
            };
            if healthy {
                return format!("https://{addr}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        handle.abort();
    }
    panic!("failed to start TLS control plane after 10 attempts");
}

/// 主链路 (F32 验收):自签证书起服务 → 跳校验客户端 https 可达 (/health 与
/// 受保护端点 /status);默认校验客户端对自签拒信 (信任链);明文 http 同
/// 端口不可达 (纯 HTTPS，无明文降级)。
#[tokio::test]
async fn tls_serves_https_and_rejects_plain_http() {
    let dir = temp_dir("serve");
    let (cert, key) = write_self_signed(&dir);
    let base = spawn_tls_server(shared_for_test(), &cert, &key, None, None).await;

    // --insecure 等价：跳过校验 → https /health 200(浅存活，空体)
    let resp = insecure_client()
        .get(format!("{base}/health"))
        .send()
        .await
        .expect("https with insecure client must reach");
    assert_eq!(resp.status().as_u16(), 200);

    // 受保护端点同样经 TLS 可达 (GET /status;未配置 token → 200)
    let resp = insecure_client()
        .get(format!("{base}/status"))
        .send()
        .await
        .expect("https /status must reach");
    assert_eq!(resp.status().as_u16(), 200);

    // 默认校验客户端：自签不在信任链 → 证书错误 (诚实拒信，不静默放行)
    assert!(
        reqwest::get(&base).await.is_err(),
        "default client must reject self-signed cert"
    );

    // 明文 http 打 TLS 端口：服务端在握手层回 alert → 连接错误 (无降级);
    // 有界等待，防异常路径挂死测试。
    let http_base = base.replacen("https://", "http://", 1);
    let attempt = tokio::time::timeout(
        Duration::from_secs(5),
        reqwest::get(format!("{http_base}/health")),
    )
    .await
    .expect("plain http attempt must not hang");
    assert!(attempt.is_err(), "plain http must fail against TLS port");

    let _ = std::fs::remove_dir_all(&dir);
}

/// 坏 PEM(垃圾内容)→ `load_tls_config` Err(对应 runtime 装配期早退、
/// 退出码 2);错误信息不含文件内容 —— 仅路径与解析原因 (私钥纪律)。
#[tokio::test]
async fn bad_pem_rejected_at_load() {
    let dir = temp_dir("badpem");
    let cert_content = "not a pem at all\n";
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert_content).unwrap();
    std::fs::write(
        &key_path,
        "-----BEGIN PRIVATE KEY-----\ngarbage\n-----END PRIVATE KEY-----\n",
    )
    .unwrap();

    let err = load_tls_config(Some(&cert_path), Some(&key_path), None)
        .await
        .expect_err("garbage pem must fail to load");
    let msg = err.to_string();
    assert!(
        msg.starts_with("control error: control tls load failed"),
        "错误带 control 来源前缀:{msg}"
    );
    assert!(!msg.contains(cert_content), "错误不得回显文件内容:{msg}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 成对校验兜底：单边有值 (程序化构造绕过 config validate)→ Err(Config);
/// 双 None → Ok(None)(纯 HTTP 现状，serve 走 axum::serve 零改动路径)。
/// F38:`client_ca` 脱离 tls_cert/tls_key 单配同样拒绝(Config)。
#[tokio::test]
async fn tls_paths_must_be_paired() {
    let dir = temp_dir("pair");
    let (cert, _key) = write_self_signed(&dir);

    let err = load_tls_config(Some(&cert), None, None)
        .await
        .expect_err("single-sided tls config must be rejected");
    assert!(
        matches!(err, monitor_core::error::CoreError::Config(_)),
        "单边配置应报 Config 错误:{err}"
    );
    assert!(
        load_tls_config(None, Some(&cert), None).await.is_err(),
        "另一单边组合同样拒绝"
    );
    assert!(
        load_tls_config(None, None, None).await.unwrap().is_none(),
        "双 None = 纯 HTTP(现状)"
    );
    assert!(
        load_tls_config(None, None, Some(&cert)).await.is_err(),
        "client_ca_cert 脱离 HTTPS 单配 → Config 错误"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// F38 主链路:测试 CA 同签服务端与客户端证书 →
/// ① 带 `reqwest::Identity` + CA 信任的客户端(monitorctl
///    `--client-cert/--client-key` 同一构建 API)经 mTLS 到 /health 200;
/// ② 不带客户端证书(信任服务端 CA)→ TLS 握手失败(服务端要求证书,
///    无 HTTP 层交互);
/// ③ 坏 CA(另一自签 CA)签发的客户端证书 → 握手失败(不可信证书拒绝);
/// ④ 服务端对 /status 同样可达(mTLS 不改变端点语义,鉴权仍在 HTTP 层)。
#[tokio::test]
async fn mtls_client_cert_required_and_untrusted_rejected() {
    let dir = temp_dir("mtls");
    let (ca_pem, issuer) = test_ca();
    let ca_path = dir.join("ca.pem");
    std::fs::write(&ca_path, &ca_pem).unwrap();

    // 服务端 leaf:SAN = 127.0.0.1(客户端按 IP URL 连接,webpki 校验 IP SAN)。
    let (server_cert, server_key) = issue_leaf(&dir, "server", "127.0.0.1", &issuer);
    // 客户端 leaf(SAN 任意)与坏 CA 签发的客户端 leaf。
    let (client_cert, client_key) = issue_leaf(&dir, "client", "monitorctl-client", &issuer);
    let (_rogue_ca_pem, rogue_issuer) = test_ca();
    let (rogue_cert, rogue_key) = issue_leaf(&dir, "rogue", "rogue-client", &rogue_issuer);

    // 良性客户端:信任 CA + 客户端证书(同一构建路径 mirror monitorctl
    // build_client:证书 + 私钥 PEM 拼接 → Identity::from_pem)。
    let mut identity_pem = std::fs::read(&client_cert).unwrap();
    identity_pem.extend_from_slice(&std::fs::read(&client_key).unwrap());
    let trusted = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(ca_pem.as_bytes()).unwrap())
        .identity(reqwest::Identity::from_pem(&identity_pem).unwrap())
        .build()
        .unwrap();

    let base = spawn_tls_server(
        shared_for_test(),
        &server_cert,
        &server_key,
        Some(&ca_path),
        Some(trusted.clone()),
    )
    .await;

    // ① 良性客户端 → /health 200(浅存活,空体)
    let resp = trusted
        .get(format!("{base}/health"))
        .send()
        .await
        .expect("mTLS client with trusted certificate must reach");
    assert_eq!(resp.status().as_u16(), 200);

    // ④ /status 同样可达:mTLS 在 TLS 层,不改变 HTTP 端点语义
    let resp = trusted
        .get(format!("{base}/status"))
        .send()
        .await
        .expect("mTLS client must reach protected endpoint");
    assert_eq!(resp.status().as_u16(), 200);

    // ② 无客户端证书:信任服务端 CA 但不带 Identity → 握手被服务端拒绝
    // (certificate_required alert → 连接错误;有界等待防异常路径挂死)。
    let anon = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(ca_pem.as_bytes()).unwrap())
        .build()
        .unwrap();
    let attempt = tokio::time::timeout(Duration::from_secs(5), anon.get(&base).send()).await;
    assert!(
        attempt.is_err() || attempt.unwrap().is_err(),
        "client without certificate must fail the TLS handshake"
    );

    // ③ 坏 CA 签发的客户端证书:另一自签 CA 不在信任锚 → 握手拒绝
    let mut rogue_pem = std::fs::read(&rogue_cert).unwrap();
    rogue_pem.extend_from_slice(&std::fs::read(&rogue_key).unwrap());
    let rogue = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(ca_pem.as_bytes()).unwrap())
        .identity(reqwest::Identity::from_pem(&rogue_pem).unwrap())
        .build()
        .unwrap();
    let attempt = tokio::time::timeout(Duration::from_secs(5), rogue.get(&base).send()).await;
    assert!(
        attempt.is_err() || attempt.unwrap().is_err(),
        "client certificate from an untrusted CA must fail the TLS handshake"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// F38:坏 CA 材料 → 装配期 `Err`(与 F32 坏服务端证书同列早退):
/// 垃圾 PEM / 空文件(无证书段);错误含路径、不含文件内容。
#[tokio::test]
async fn bad_client_ca_rejected_at_load() {
    let dir = temp_dir("badca");
    let (server_cert, server_key) = write_self_signed(&dir);

    let garbage_ca = dir.join("ca-garbage.pem");
    std::fs::write(&garbage_ca, "definitely not a pem\n").unwrap();
    let err = load_tls_config(Some(&server_cert), Some(&server_key), Some(&garbage_ca))
        .await
        .expect_err("garbage CA must fail at load");
    let msg = err.to_string();
    assert!(msg.contains("ca-garbage.pem"), "错误带 CA 路径上下文:{msg}");
    assert!(!msg.contains("definitely"), "错误不得回显文件内容:{msg}");

    let empty_ca = dir.join("ca-empty.pem");
    std::fs::write(&empty_ca, "").unwrap();
    let err = load_tls_config(Some(&server_cert), Some(&server_key), Some(&empty_ca))
        .await
        .expect_err("empty CA must fail at load");
    assert!(
        err.to_string().contains("no certificate"),
        "空 CA 报「无证书」:{err}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// F38 monitorctl Identity 构建路径(单元级):`reqwest::Identity::from_pem`
/// 对 rcgen 产物(leaf 证书 PEM + PKCS#8 私钥 PEM 拼接)可用;坏 PEM 拒绝。
/// monitorctl 无 rcgen dev 依赖,此测试在 monitor-core 侧覆盖同一 API
/// (`build_client` → `Identity::from_pem`)。
#[test]
fn reqwest_identity_accepts_rcgen_pem() {
    let (_ca_pem, issuer) = test_ca();
    let params = rcgen::CertificateParams::new(vec!["monitorctl-client".into()]).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, &issuer).unwrap();

    // 证书 + 私钥拼接(monitorctl resolve_client_identity 同口径)。
    let mut pem = cert.pem().into_bytes();
    pem.extend_from_slice(key.serialize_pem().as_bytes());
    reqwest::Identity::from_pem(&pem).expect("rcgen PEM product must build an Identity");

    // 垃圾 PEM → Err(不 panic)
    assert!(reqwest::Identity::from_pem(b"not a pem\n").is_err());
}

/// runtime 装配期早退 (F32 验收「加载坏密钥启动失败」):坏 PEM → `run()`
/// 返回 Err 且**不创建 data_dir**(TLS 加载与端口 bind 同列早失败点，先于
/// journal/data_dir 装配);main 对 Err 的既有映射即退出码 2(monitor-agent
/// main.rs:`Err(e)` → `process::exit(2)`,本测试验证 Err 路径本体)。
#[tokio::test]
async fn runtime_fails_fast_on_bad_tls_material() {
    let dir = temp_dir("runbad");
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, "not a pem at all\n").unwrap();
    std::fs::write(&key_path, "garbage key bytes\n").unwrap();

    let mut cfg = monitor_core::config::Config::from_toml_str("").unwrap();
    cfg.control.listen = "127.0.0.1:0".parse().unwrap();
    cfg.control.tls_cert = Some(cert_path);
    cfg.control.tls_key = Some(key_path);
    let data_dir = std::env::temp_dir().join(format!(
        "sw-tls-rundata-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&data_dir);
    cfg.agent.data_dir = data_dir.clone();

    let result = monitor_core::runtime::run(cfg).await;
    assert!(result.is_err(), "bad TLS material must fail startup");
    assert!(!data_dir.exists(), "失败早退不得创建 data_dir");
    let _ = std::fs::remove_dir_all(&dir);
}
