//! F26 按目标告警路由集成测试 (DESIGN §7.3 / ROADMAP F26):
//! - 双 webhook stub (axum，与 tests/http_probe.rs 的 stub 模式相同):路由
//!   命中 target 的告警落 A 端点，未命中 target 落默认端点;
//! - spool 注入失败事件后，启动重放按**当前配置**路由 (重放走分发任务
//!   持有的同一 notifier 实例 → RoutingNotifier 自动生效)。
//!
//! F29 (ROADMAP F29):webhook HMAC 签名 —— 验签 stub 读取原始 body 与
//! `X-Service-Watch-Signature` 头，服务端用相同 secret 重算 HMAC-SHA256 比对：
//! 有 secret 端点验签通过、无 secret 端点断言无签名头、密钥错配按投递失败;
//! spool 重放「带签名」端到端 (第十轮验证接受项清偿):注入积压 + 有 secret
//! 的 notifier → 重放 → 验签通过 + spool 确认移除。
//!
//! F37 (ROADMAP F37):自定义投递头 —— 双 stub 各自断言收到自定义头
//! (路由级合并覆盖全局 / 非同名键共存) 且与签名头独立共存;spool 重放
//! 亦带自定义头 (与签名共用同一条重放路径)。
//!
//! P3(第九轮验证):webhook 在 http-client feature 门控内 —— no-default
//! 组合下整个测试文件退出编译 (对齐库内门控),CI 的 no-default 只跑 check。

#![cfg(feature = "http-client")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use monitor_core::alert::spool;
use monitor_core::alert::webhook::WebhookNotifier;
use monitor_core::alert::{AlertCounters, AlertQueue, Notifier, RoutingNotifier, run_dispatcher};
use monitor_core::config::{AlertCfg, WebhookFormat};
use monitor_core::model::alert::{AlertEvent, Category, Severity};
use tokio::sync::watch;

/// 记录 POST body 的 webhook stub (axum;返回 200 让投递确认成功)。
struct HookStub {
    url: url::Url,
    hits: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl HookStub {
    async fn spawn() -> Self {
        let hits = Arc::new(Mutex::new(Vec::new()));
        let app = {
            let hits = hits.clone();
            axum::Router::new().route(
                "/hook",
                axum::routing::post(
                    move |axum::extract::Json(body): axum::extract::Json<serde_json::Value>| {
                        let hits = hits.clone();
                        async move {
                            hits.lock().unwrap().push(body);
                            axum::http::StatusCode::OK
                        }
                    },
                ),
            )
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            url: url::Url::parse(&format!("http://{addr}/hook")).unwrap(),
            hits,
        }
    }
}

/// 收集 (通道标签，target) 的 Notifier 假实现 (spool 重放路由断言用)。
struct Recording {
    tag: &'static str,
    hits: Arc<Mutex<Vec<(&'static str, String)>>>,
}

impl Recording {
    fn new(tag: &'static str, hits: Arc<Mutex<Vec<(&'static str, String)>>>) -> Arc<Self> {
        Arc::new(Self { tag, hits })
    }
}

#[async_trait]
impl Notifier for Recording {
    async fn notify(&self, event: &AlertEvent) -> monitor_core::error::Result<()> {
        self.hits
            .lock()
            .unwrap()
            .push((self.tag, event.target.clone()));
        Ok(())
    }
}

/// 新鲜时间戳的事件 (UNIX_EPOCH 事件会被 spool 重放的 7 天过期策略丢弃)。
fn event(target: &str) -> AlertEvent {
    let now = std::time::SystemTime::now();
    AlertEvent {
        incident: AlertEvent::incident_key(target, "api"),
        category: Category::Alert,
        severity: Severity::Critical,
        target: target.into(),
        check: "api".into(),
        reason: "probe failed".into(),
        observed: None,
        first_seen: now,
        last_seen: now,
        occurrences: 1,
    }
}

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sw-alert-routing-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 双 webhook stub:route 命中 target 的告警落 A 端点，未命中落默认端点。
/// 端到端走真实 AlertQueue + run_dispatcher + WebhookNotifier (JSON 格式)。
#[tokio::test]
async fn routed_target_hits_route_endpoint_and_unmatched_falls_back() {
    let edge_hook = HookStub::spawn().await;
    let default_hook = HookStub::spawn().await;
    let edge_notifier = WebhookNotifier::new(
        edge_hook.url.clone(),
        WebhookFormat::Json,
        Duration::from_secs(2),
        None,
        std::collections::BTreeMap::new(),
    )
    .unwrap();
    let default_notifier = WebhookNotifier::new(
        default_hook.url.clone(),
        WebhookFormat::Json,
        Duration::from_secs(2),
        None,
        std::collections::BTreeMap::new(),
    )
    .unwrap();
    let routing = RoutingNotifier::new(
        Arc::new(default_notifier) as Arc<dyn Notifier>,
        vec![("edge".into(), Arc::new(edge_notifier) as Arc<dyn Notifier>)],
    );

    let counters = Arc::new(AlertCounters::default());

    let queue = AlertQueue::new(counters.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(run_dispatcher(
        queue.clone(),
        Arc::new(routing) as Arc<dyn Notifier>,
        AlertCfg {
            retries: 0,
            timeout: Duration::from_secs(2),
            ..Default::default()
        },
        counters.clone(),
        shutdown_rx,
        None,
    ));
    queue.push(event("edge-worker-1"));
    queue.push(event("db-primary"));

    for _ in 0..100 {
        if edge_hook.hits.lock().unwrap().len() == 1 && default_hook.hits.lock().unwrap().len() == 1
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    {
        let hits = edge_hook.hits.lock().unwrap();
        assert_eq!(hits.len(), 1, "命中 target 的告警落 A 端点");
        assert_eq!(hits[0]["target"], "edge-worker-1");
    }
    {
        let hits = default_hook.hits.lock().unwrap();
        assert_eq!(hits.len(), 1, "未命中 target 落默认端点");
        assert_eq!(hits[0]["target"], "db-primary");
    }
    shutdown_tx.send(true).ok();
    let _ = task.await;
}

/// spool 注入失败事件后，启动重放按**当前配置**路由：重放走分发任务持有的
/// 同一 notifier 实例 (run_dispatcher → replay_spool → deliver → notify),
/// RoutingNotifier 使历史积压按启动时配置送达对应通道，成功确认后移除。
#[tokio::test]
async fn spool_replay_routes_by_current_config() {
    let dir = tmp_dir("spool-replay");
    // 模拟上一轮投递失败的积压事件 (target = edge-9)
    assert!(spool::append(&dir, &event("edge-9")).await);

    let hits: Arc<Mutex<Vec<(&'static str, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let edge = Recording::new("edge", hits.clone());
    let default = Recording::new("default", hits.clone());
    let routing = RoutingNotifier::new(default, vec![("edge".into(), edge as Arc<dyn Notifier>)]);

    let counters = Arc::new(AlertCounters::default());
    let queue = AlertQueue::new(counters.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(run_dispatcher(
        queue.clone(),
        Arc::new(routing) as Arc<dyn Notifier>,
        AlertCfg {
            retries: 0,
            ..Default::default()
        },
        counters.clone(),
        shutdown_rx,
        Some(dir.clone()),
    ));

    for _ in 0..100 {
        if !hits.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        *hits.lock().unwrap(),
        vec![("edge", "edge-9".to_string())],
        "spool 重放按当前配置路由到 edge 通道 (不走默认)"
    );
    assert!(
        !dir.join(spool::SPOOL_FILE).exists(),
        "成功确认后 spool 记录移除"
    );
    shutdown_tx.send(true).ok();
    let _ = task.await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// F29:webhook HMAC 签名 (验签 stub 服务端重算 HMAC 比对)
// ---------------------------------------------------------------------------

/// F29 记录条目：收到的签名头与**原始 body 字节**(签名对象就是这份字节)。
/// F37:另记录全部收到的头 (HTTP/2 传输后头名恒为小写，与 RUNBOOK §3.4 口径
/// 一致)，供自定义投递头断言复用。
#[cfg(feature = "notify-webhook")]
#[derive(Clone)]
struct SignedHit {
    signature: Option<String>,
    content_type: Option<String>,
    body: Vec<u8>,
    headers: Vec<(String, String)>,
}

/// F29 验签 webhook stub:`secret = Some(s)` 时服务端对原始 body 重算
/// HMAC-SHA256 并与收到的 `X-Service-Watch-Signature` 头比对 (匹配 → 200,
/// 否则 400);`secret = None` 只记录 (供调用方断言头不存在)。
#[cfg(feature = "notify-webhook")]
struct VerifyHookStub {
    url: url::Url,
    hits: Arc<Mutex<Vec<SignedHit>>>,
}

#[cfg(feature = "notify-webhook")]
impl VerifyHookStub {
    async fn spawn(secret: Option<String>) -> Self {
        let hits = Arc::new(Mutex::new(Vec::new()));
        let app = {
            let hits = hits.clone();
            axum::Router::new().route(
                "/hook",
                axum::routing::post(
                    move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                        let hits = hits.clone();
                        async move {
                            let signature = headers
                                .get("X-Service-Watch-Signature")
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_string);
                            let content_type = headers
                                .get(axum::http::header::CONTENT_TYPE)
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_string);
                            let all_headers: Vec<(String, String)> = headers
                                .iter()
                                .filter_map(|(name, value)| {
                                    value
                                        .to_str()
                                        .ok()
                                        .map(|v| (name.as_str().to_string(), v.to_string()))
                                })
                                .collect();
                            let verified = match secret {
                                // 服务端用相同 secret 重算 HMAC，恒等比对接收方
                                // 语义 (测试用 == 即可;生产接收方应恒定时间比较)。
                                Some(s) => {
                                    signature.as_deref()
                                        == Some(expected_signature(&s, &body).as_str())
                                }
                                None => true,
                            };
                            hits.lock().unwrap().push(SignedHit {
                                signature,
                                content_type,
                                body: body.to_vec(),
                                headers: all_headers,
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
            url: url::Url::parse(&format!("http://{addr}/hook")).unwrap(),
            hits,
        }
    }

    fn hits(&self) -> Vec<SignedHit> {
        self.hits.lock().unwrap().clone()
    }
}

/// 测试侧 HMAC-SHA256 → hex 小写 (与被测实现独立重算，互为对照)。
#[cfg(feature = "notify-webhook")]
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
#[cfg(feature = "notify-webhook")]
fn expected_signature(secret: &str, body: &[u8]) -> String {
    format!("sha256={}", hmac_sha256_hex(secret, body))
}

/// 有 secret 端点验签通过 (slack/json 两格式),无 secret 端点断言无签名头。
#[cfg(feature = "notify-webhook")]
#[tokio::test]
async fn signature_header_present_with_secret_and_absent_without() {
    let hook = VerifyHookStub::spawn(Some("Jefe".into())).await;
    let bare = VerifyHookStub::spawn(None).await;

    for format in [WebhookFormat::Json, WebhookFormat::Slack] {
        let signed = WebhookNotifier::new(
            hook.url.clone(),
            format,
            Duration::from_secs(2),
            Some("Jefe".into()),
            std::collections::BTreeMap::new(),
        )
        .unwrap();
        signed.notify(&event("signed-target")).await.unwrap();
    }
    let unsigned = WebhookNotifier::new(
        bare.url.clone(),
        WebhookFormat::Json,
        Duration::from_secs(2),
        None,
        std::collections::BTreeMap::new(),
    )
    .unwrap();
    unsigned.notify(&event("bare-target")).await.unwrap();

    let hits = hook.hits();
    assert_eq!(hits.len(), 2, "slack/json 两格式各一次投递");
    for hit in &hits {
        // 签名对象 = 实际发送的原始 body 字节：服务端重算必然一致。
        assert_eq!(
            hit.signature.as_deref(),
            Some(expected_signature("Jefe", &hit.body).as_str()),
            "验签必须通过 (body 字节与签名同一份)"
        );
        assert_eq!(
            hit.content_type.as_deref(),
            Some("application/json"),
            "content-type 与 .json() 时代保持一致"
        );
    }
    let bare_hits = bare.hits();
    assert_eq!(bare_hits.len(), 1);
    assert!(
        bare_hits[0].signature.is_none(),
        "无 secret 裸投递不带签名头"
    );
}

/// 分发链路 (AlertQueue + run_dispatcher):路由级 secret 覆盖全局，默认通道用
/// 全局;端到端投递全部验签通过。spool 重放走同一 notifier 实例 → 重放亦签名。
#[cfg(feature = "notify-webhook")]
#[tokio::test]
async fn dispatcher_delivers_route_and_default_channels_with_signatures() {
    let route_hook = VerifyHookStub::spawn(Some("route-secret".into())).await;
    let default_hook = VerifyHookStub::spawn(Some("global-secret".into())).await;
    // 路由级覆盖全局:route 通道用 route-secret (而非 global-secret) 签名。
    let route_notifier = WebhookNotifier::new(
        route_hook.url.clone(),
        WebhookFormat::Json,
        Duration::from_secs(2),
        Some("route-secret".into()),
        std::collections::BTreeMap::new(),
    )
    .unwrap();
    let default_notifier = WebhookNotifier::new(
        default_hook.url.clone(),
        WebhookFormat::Json,
        Duration::from_secs(2),
        Some("global-secret".into()),
        std::collections::BTreeMap::new(),
    )
    .unwrap();
    let routing = RoutingNotifier::new(
        Arc::new(default_notifier) as Arc<dyn Notifier>,
        vec![("edge".into(), Arc::new(route_notifier) as Arc<dyn Notifier>)],
    );

    let counters = Arc::new(AlertCounters::default());
    let queue = AlertQueue::new(counters.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(run_dispatcher(
        queue.clone(),
        Arc::new(routing) as Arc<dyn Notifier>,
        AlertCfg {
            retries: 0,
            timeout: Duration::from_secs(2),
            ..Default::default()
        },
        counters.clone(),
        shutdown_rx,
        None,
    ));
    queue.push(event("edge-worker-1"));
    queue.push(event("db-primary"));

    for _ in 0..100 {
        if counters.sent.load(std::sync::atomic::Ordering::Relaxed) == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        counters.sent.load(std::sync::atomic::Ordering::Relaxed),
        2,
        "两通道投递都确认成功 (= 两端点验签都通过，错签会 400)"
    );
    for (hook, secret) in [
        (&route_hook, "route-secret"),
        (&default_hook, "global-secret"),
    ] {
        let hits = hook.hits();
        assert_eq!(hits.len(), 1, "{secret} 通道各一笔");
        assert_eq!(
            hits[0].signature.as_deref(),
            Some(expected_signature(secret, &hits[0].body).as_str()),
        );
    }
    shutdown_tx.send(true).ok();
    let _ = task.await;
}

/// 密钥错配 → 服务端验签失败 400 → 投递侧按失败计 (签名是真实生效的，
/// 不是"带了就行")。
#[cfg(feature = "notify-webhook")]
#[tokio::test]
async fn signature_mismatch_is_delivery_failure() {
    let hook = VerifyHookStub::spawn(Some("expected-secret".into())).await;
    let wrong = WebhookNotifier::new(
        hook.url.clone(),
        WebhookFormat::Json,
        Duration::from_secs(2),
        Some("wrong-secret".into()),
        std::collections::BTreeMap::new(),
    )
    .unwrap();

    let counters = Arc::new(AlertCounters::default());
    let queue = AlertQueue::new(counters.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(run_dispatcher(
        queue.clone(),
        Arc::new(wrong) as Arc<dyn Notifier>,
        AlertCfg {
            retries: 0,
            timeout: Duration::from_secs(2),
            ..Default::default()
        },
        counters.clone(),
        shutdown_rx,
        None,
    ));
    queue.push(event("mismatched-target"));

    for _ in 0..100 {
        if counters
            .delivery_failures
            .load(std::sync::atomic::Ordering::Relaxed)
            == 1
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        counters
            .delivery_failures
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "验签失败 400 → 投递失败计数"
    );
    let hits = hook.hits();
    assert_eq!(hits.len(), 1);
    assert_ne!(
        hits[0].signature.as_deref(),
        Some(expected_signature("expected-secret", &hits[0].body).as_str()),
        "收到的签名与期望 secret 的 HMAC 不一致 (错签证据)"
    );
    shutdown_tx.send(true).ok();
    let _ = task.await;
}

/// 第十轮验证接受项清偿:spool 重放「带签名」专属端到端 —— spool 注入失败
/// 事件 + 有 secret 的 notifier → 启动重放 → 验签 stub 确认签名正确 (签名
/// 对象 = 重放投递的原始 body 字节) → 成功后 spool 文件移除 (F12 at-least-once
/// 与 F29 签名口径在重放路径上组合生效)。
#[cfg(feature = "notify-webhook")]
#[tokio::test]
async fn spool_replay_delivers_with_valid_signature_and_confirms_removal() {
    let dir = tmp_dir("spool-replay-signed");
    // 模拟上一轮投递失败的积压事件 (新鲜时间戳，避免 7 天过期策略丢弃)
    assert!(spool::append(&dir, &event("signed-target")).await);

    let hook = VerifyHookStub::spawn(Some("replay-secret".into())).await;
    let signed = WebhookNotifier::new(
        hook.url.clone(),
        WebhookFormat::Json,
        Duration::from_secs(2),
        Some("replay-secret".into()),
        std::collections::BTreeMap::new(),
    )
    .unwrap();

    let counters = Arc::new(AlertCounters::default());
    let queue = AlertQueue::new(counters.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(run_dispatcher(
        queue.clone(),
        Arc::new(signed) as Arc<dyn Notifier>,
        AlertCfg {
            retries: 0,
            timeout: Duration::from_secs(2),
            ..Default::default()
        },
        counters.clone(),
        shutdown_rx,
        Some(dir.clone()),
    ));

    for _ in 0..100 {
        if counters.sent.load(std::sync::atomic::Ordering::Relaxed) == 1
            && !dir.join(spool::SPOOL_FILE).exists()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        counters.sent.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "重放投递必须成功 (验签 stub 200)"
    );
    assert!(
        !dir.join(spool::SPOOL_FILE).exists(),
        "成功确认后 spool 记录移除"
    );
    let hits = hook.hits();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].signature.as_deref(),
        Some(expected_signature("replay-secret", &hits[0].body).as_str()),
        "重放投递的签名 = 服务端按收到的原始 body 重算的 HMAC"
    );
    shutdown_tx.send(true).ok();
    let _ = task.await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// F37:自定义投递头 (合并覆盖 / 同名共存 / 签名头独立叠加 / spool 重放)
// ---------------------------------------------------------------------------

/// 便捷取头 (stub 记录的头名经 HTTP/2 已小写)。
#[cfg(feature = "notify-webhook")]
fn header<'a>(hit: &'a SignedHit, name: &str) -> Option<&'a str> {
    hit.headers
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v.as_str())
}

/// 双 stub:路由级 headers 同名键覆盖全局、非同名键共存;默认通道用全局表;
/// 两通道自定义头与签名头 (F29) **独立共存** (签名头不因同名自定义头丢弃 ——
/// 本例自定义头不同名，两通道各验签通过);json (route) 与 slack (default)
/// 两格式同路径生效。
#[cfg(feature = "notify-webhook")]
#[tokio::test]
async fn custom_headers_reach_both_channels_and_coexist_with_signature() {
    let route_hook = VerifyHookStub::spawn(Some("route-secret".into())).await;
    let default_hook = VerifyHookStub::spawn(Some("global-secret".into())).await;

    // 合并语义与 runtime::effective_route_headers 一致：全局为底，路由级覆盖。
    let mut global = std::collections::BTreeMap::new();
    global.insert("X-Auth-Token".to_string(), "global-secret".to_string());
    global.insert("X-Global-Only".to_string(), "g".to_string());
    let mut route_merged = global.clone();
    route_merged.insert("X-Auth-Token".to_string(), "route-secret".to_string());
    route_merged.insert("X-Route-Only".to_string(), "r".to_string());

    let route_notifier = WebhookNotifier::new(
        route_hook.url.clone(),
        WebhookFormat::Json,
        Duration::from_secs(2),
        Some("route-secret".into()),
        route_merged,
    )
    .unwrap();
    let default_notifier = WebhookNotifier::new(
        default_hook.url.clone(),
        WebhookFormat::Slack,
        Duration::from_secs(2),
        Some("global-secret".into()),
        global,
    )
    .unwrap();
    let routing = RoutingNotifier::new(
        Arc::new(default_notifier) as Arc<dyn Notifier>,
        vec![("edge".into(), Arc::new(route_notifier) as Arc<dyn Notifier>)],
    );

    let counters = Arc::new(AlertCounters::default());
    let queue = AlertQueue::new(counters.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(run_dispatcher(
        queue.clone(),
        Arc::new(routing) as Arc<dyn Notifier>,
        AlertCfg {
            retries: 0,
            timeout: Duration::from_secs(2),
            ..Default::default()
        },
        counters.clone(),
        shutdown_rx,
        None,
    ));
    queue.push(event("edge-worker-1"));
    queue.push(event("db-primary"));

    for _ in 0..100 {
        if counters.sent.load(std::sync::atomic::Ordering::Relaxed) == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        counters.sent.load(std::sync::atomic::Ordering::Relaxed),
        2,
        "两通道投递都成功 (自定义头不影响投递语义)"
    );

    // 路由通道 (json):同名键覆盖 + 非同名共存 + 签名头共存。
    let route_hits = route_hook.hits();
    assert_eq!(route_hits.len(), 1);
    let hit = &route_hits[0];
    assert_eq!(
        header(hit, "x-auth-token"),
        Some("route-secret"),
        "路由级同名键覆盖全局"
    );
    assert_eq!(header(hit, "x-global-only"), Some("g"), "非同名键共存");
    assert_eq!(header(hit, "x-route-only"), Some("r"), "非同名键共存");
    assert_eq!(
        hit.signature.as_deref(),
        Some(expected_signature("route-secret", &hit.body).as_str()),
        "签名头 (F29) 与自定义头独立共存"
    );

    // 默认通道 (slack):只用全局表，无路由级键;签名头共存。
    let default_hits = default_hook.hits();
    assert_eq!(default_hits.len(), 1);
    let hit = &default_hits[0];
    assert_eq!(header(hit, "x-auth-token"), Some("global-secret"));
    assert_eq!(header(hit, "x-global-only"), Some("g"));
    assert_eq!(
        header(hit, "x-route-only"),
        None,
        "路由级专属键不得泄入默认通道"
    );
    assert_eq!(
        hit.signature.as_deref(),
        Some(expected_signature("global-secret", &hit.body).as_str()),
        "默认通道签名头共存"
    );
    shutdown_tx.send(true).ok();
    let _ = task.await;
}

/// spool 重放带自定义头：注入积压 + headers/secret notifier → 启动重放 →
/// stub 断言自定义头与签名同时在 (重放走同一 notifier 实例，F37 天然覆盖)。
#[cfg(feature = "notify-webhook")]
#[tokio::test]
async fn spool_replay_delivers_with_custom_headers_and_signature() {
    let dir = tmp_dir("spool-replay-headers");
    assert!(spool::append(&dir, &event("edge-9")).await);

    let hook = VerifyHookStub::spawn(Some("replay-secret".into())).await;
    let mut headers = std::collections::BTreeMap::new();
    headers.insert("X-Auth-Token".to_string(), "replay-token".to_string());
    let notifier = WebhookNotifier::new(
        hook.url.clone(),
        WebhookFormat::Json,
        Duration::from_secs(2),
        Some("replay-secret".into()),
        headers,
    )
    .unwrap();

    let counters = Arc::new(AlertCounters::default());
    let queue = AlertQueue::new(counters.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(run_dispatcher(
        queue.clone(),
        Arc::new(notifier) as Arc<dyn Notifier>,
        AlertCfg {
            retries: 0,
            timeout: Duration::from_secs(2),
            ..Default::default()
        },
        counters.clone(),
        shutdown_rx,
        Some(dir.clone()),
    ));

    for _ in 0..100 {
        if counters.sent.load(std::sync::atomic::Ordering::Relaxed) == 1
            && !dir.join(spool::SPOOL_FILE).exists()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        counters.sent.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "重放投递成功"
    );
    let hits = hook.hits();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        header(&hits[0], "x-auth-token"),
        Some("replay-token"),
        "spool 重放投递带自定义头"
    );
    assert_eq!(
        hits[0].signature.as_deref(),
        Some(expected_signature("replay-secret", &hits[0].body).as_str()),
        "重放投递签名头与自定义头共存"
    );
    shutdown_tx.send(true).ok();
    let _ = task.await;
    let _ = std::fs::remove_dir_all(&dir);
}
