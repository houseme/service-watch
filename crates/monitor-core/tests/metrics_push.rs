//! F34 指标推送 HMAC 签名集成测试 (DESIGN §7.3 / ROADMAP F34):
//! - 有 secret:axum stub 收推送，服务端用相同密钥对**收到的原始 body 字节**
//!   重算 HMAC-SHA256 并与 `X-Service-Watch-Signature` 头比对 (验签 stub 模式
//!   与 tests/alert_routing.rs 的 VerifyHookStub 相同;比对恒等即可 —— 生产
//!   接收方应使用恒定时间比较，见 RUNBOOK §3.4);
//! - None:推送照常投递且**不带**签名头 (行为不变，默认向后兼容)。
//!
//! 门控:http-client (与被测 `alert::metrics_push` 及共享签名 helper 同门控);
//! no-default 组合 (http-client 关闭) 本文件整体退出编译，CI 只跑 check。

#![cfg(feature = "http-client")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use monitor_core::model::control::MetricsReport;
use tokio::sync::watch;

/// 记录 (签名头，原始 body) 的推送 stub;`secret = Some(s)` 时服务端重算
/// HMAC 比对 (匹配 → 200，否则 400 —— 签名错配会走推送连败路径，由断言暴露)。
struct VerifyPushStub {
    url: url::Url,
    hits: Arc<Mutex<Vec<PushHit>>>,
}

#[derive(Clone)]
struct PushHit {
    signature: Option<String>,
    body: Vec<u8>,
}

impl VerifyPushStub {
    async fn spawn(secret: Option<String>) -> Self {
        let hits = Arc::new(Mutex::new(Vec::new()));
        let app = {
            let hits = hits.clone();
            axum::Router::new().route(
                "/metrics",
                axum::routing::post(
                    move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                        let hits = hits.clone();
                        async move {
                            let signature = headers
                                .get("X-Service-Watch-Signature")
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_string);
                            let verified = match secret {
                                // 服务端用相同 secret 重算 HMAC 比对原始 body。
                                Some(s) => {
                                    signature.as_deref()
                                        == Some(expected_signature(&s, &body).as_str())
                                }
                                None => true,
                            };
                            hits.lock().unwrap().push(PushHit {
                                signature,
                                body: body.to_vec(),
                            });
                            if verified {
                                axum::http::StatusCode::OK
                            } else {
                                axum::http::StatusCode::BAD_REQUEST
                            }
                        }
                    },
                ),
            )
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            url: url::Url::parse(&format!("http://{addr}/metrics")).unwrap(),
            hits,
        }
    }

    fn hits(&self) -> Vec<PushHit> {
        self.hits.lock().unwrap().clone()
    }
}

/// 测试侧 HMAC-SHA256 → hex 小写 (与被测实现独立重算，互为对照)。
fn hmac_sha256_hex(secret: &str, body: &[u8]) -> String {
    // hmac 0.13 (digest 0.11):`new_from_slice` 随 `KeyInit` trait 提供。
    use hmac::{KeyInit, Mac};
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(body);
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// 期望的签名头值 `sha256=<hex>`。
fn expected_signature(secret: &str, body: &[u8]) -> String {
    format!("sha256={}", hmac_sha256_hex(secret, body))
}

/// 起推送任务 (真实短 interval,stub 真实 socket)。
#[allow(clippy::too_many_arguments)]
fn spawn_pusher(
    url: url::Url,
    secret: Option<String>,
    metrics_rx: watch::Receiver<Arc<MetricsReport>>,
    shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(monitor_core::alert::metrics_push::run_metrics_pusher(
        url,
        Duration::from_millis(150),
        Duration::from_secs(2),
        secret,
        metrics_rx,
        shutdown,
        None,
    ))
}

/// 等待 stub 收到 ≥ n 次推送 (真实网络，轮询预算 ~4s)。
async fn wait_hits(stub: &VerifyPushStub, n: usize) -> Vec<PushHit> {
    for _ in 0..80 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let hits = stub.hits();
        if hits.len() >= n {
            return hits;
        }
    }
    stub.hits()
}

/// F34:有 secret → 每次推送附验签通过的签名头 (签名对象 = 收到的原始 body
/// 字节),payload 与未签名时一致 (可反序列化为 MetricsReport)。
#[tokio::test(flavor = "multi_thread")]
async fn signed_push_carries_verifiable_signature_header() {
    let stub = VerifyPushStub::spawn(Some("Jefe".into())).await;
    let (metrics_tx, metrics_rx) = watch::channel(Arc::new(MetricsReport::default()));
    let (shutdown_tx, shutdown) = watch::channel(false);
    let task = spawn_pusher(stub.url.clone(), Some("Jefe".into()), metrics_rx, shutdown);
    metrics_tx.send_replace(Arc::new(MetricsReport {
        probe_executed: 3,
        ..Default::default()
    }));

    let hits = wait_hits(&stub, 2).await;
    assert!(hits.len() >= 2, "必须按周期重复投递");
    for hit in &hits {
        serde_json::from_slice::<MetricsReport>(&hit.body)
            .expect("签名推送的 body 仍是 MetricsReport JSON");
        assert_eq!(
            hit.signature.as_deref(),
            Some(expected_signature("Jefe", &hit.body).as_str()),
            "签名头 = 服务端按收到的原始 body 重算的 HMAC"
        );
    }
    shutdown_tx.send(true).ok();
    let _ = task.await;
}

/// F34:None 密钥 → 推送照常投递且不带签名头 (行为不变)。
#[tokio::test(flavor = "multi_thread")]
async fn unsigned_push_omits_signature_header() {
    let stub = VerifyPushStub::spawn(None).await;
    let (metrics_tx, metrics_rx) = watch::channel(Arc::new(MetricsReport::default()));
    let (shutdown_tx, shutdown) = watch::channel(false);
    let task = spawn_pusher(stub.url.clone(), None, metrics_rx, shutdown);
    metrics_tx.send_replace(Arc::new(MetricsReport::default()));

    let hits = wait_hits(&stub, 1).await;
    assert!(!hits.is_empty(), "None 密钥不改变投递行为");
    assert_eq!(
        hits[0].signature, None,
        "None 密钥不加 X-Service-Watch-Signature 头"
    );
    shutdown_tx.send(true).ok();
    let _ = task.await;
}
