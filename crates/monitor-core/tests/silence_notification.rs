//! F27 目标级静默集成测试 (ROADMAP F27 验收「集成」):真实 eval 调度循环
//! (可控健康 http stub)+ 真实 AlertQueue + `run_dispatcher` + 真实 webhook
//! stub (axum 记录器),端到端验证：
//! - **静默期间 Warning/Critical 不出 webhook**(journal Incident 事件照常写
//!   —— 历史完整性;首次抑制命中恰好一条可见性 Audit);
//! - **Resolve 永不静默**:静默窗口仍生效时恢复通知穿透到 webhook;
//! - **解除静默后新 Critical 正常出 webhook**;
//! - 建立/解除静默的审计边沿落盘 (journal Audit，含 reason 与到期语义)。
//!
//! webhook/分发在 http-client feature 门控内 —— no-default 组合下整个测试
//! 文件退出编译 (对齐 tests/alert_routing.rs 的门控纪律)。

#![cfg(feature = "http-client")]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use monitor_core::alert::webhook::WebhookNotifier;
use monitor_core::alert::{AlertCounters, AlertQueue, Notifier, run_dispatcher};
use monitor_core::config::{AlertCfg, CheckCfg, CheckKindCfg, Config, StatusRange, WebhookFormat};
use monitor_core::eval::SchedulerHandles;
use monitor_core::model::control::{ControlCommand, MetricsReport, StatusReport};
use monitor_core::model::ids::ProcessIdentity;
use monitor_core::recover::journal::JournalEvent;
use monitor_core::recover::process::ProcessManager;
use tokio::sync::{mpsc, watch};

/// 记录 POST body 的 webhook stub (与 tests/alert_routing.rs 同形)。
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

    fn hits(&self) -> Vec<serde_json::Value> {
        self.hits.lock().unwrap().clone()
    }
}

/// 可脚本化健康状态的 http stub (与 tests/http_probe.rs 同形：延迟/状态码
/// 运行时可调;本测试只用状态码)。
struct HealthStub {
    status: Arc<AtomicU64>,
    addr: std::net::SocketAddr,
}

impl HealthStub {
    async fn spawn() -> Self {
        let status = Arc::new(AtomicU64::new(200));
        let app = {
            let status = status.clone();
            axum::Router::new().route(
                "/hz",
                axum::routing::get(move || {
                    let status = status.clone();
                    async move {
                        let code = axum::http::StatusCode::from_u16(
                            u16::try_from(status.load(Ordering::Relaxed)).unwrap_or(200),
                        )
                        .unwrap_or(axum::http::StatusCode::OK);
                        Result::<axum::http::StatusCode, std::convert::Infallible>::Ok(code)
                    }
                }),
            )
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { status, addr }
    }
}

/// 单 http 检查配置:failure/success threshold = 1 (一次失败即 Warning,
/// 连续第二次失败即 Critical，一次成功即 Resolve),renotify 关闭 (窗口内
/// 无重发噪声;Critical 升级事件即验收里的「新 Critical」)。
fn silence_test_cfg(webhook: url::Url, probe: String) -> Config {
    Config {
        checks: vec![CheckCfg {
            name: "api".into(),
            kind: CheckKindCfg::Http {
                url: url::Url::parse(&probe).unwrap(),
                expected_status: StatusRange(200, 299),
            },
            interval: Duration::from_millis(200),
            timeout: Duration::from_millis(150),
        }],
        alert: AlertCfg {
            webhook_url: Some(webhook),
            renotify_interval: Duration::from_secs(3600),
            failure_threshold: 1,
            success_threshold: 1,
            timeout: Duration::from_secs(2),
            retries: 0,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// 等待 webhook 命中数到达目标 (超时即失败，断言信息含已收到的类别)。
async fn wait_hits(hook: &HookStub, want: usize, what: &str) -> Vec<serde_json::Value> {
    for _ in 0..100 {
        let hits = hook.hits();
        if hits.len() >= want {
            return hits;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let hits = hook.hits();
    let cats: Vec<String> = hits
        .iter()
        .map(|h| h["category"].as_str().unwrap_or("?").to_string())
        .collect();
    panic!("等待 {what} 超时：期望 {want} 笔，实得 {cats:?}");
}

/// 端到端主链路 (ROADMAP F27 验收「集成」):
/// 静默 → critical 不出 webhook → Resolve 穿透 → 解除 → 新 Critical 出 webhook。
#[tokio::test(flavor = "multi_thread")]
async fn silence_suppresses_webhook_resolve_penetrates_and_unsilence_restores() {
    let hook = HookStub::spawn().await;
    let stub = HealthStub::spawn().await;
    let cfg = silence_test_cfg(hook.url.clone(), format!("http://{}/hz", stub.addr));

    // ---- 装配 (与 tests/http_probe.rs::spawn_scheduler 同形 + 控制通道 + journal 采集) ----
    let counters = Arc::new(AlertCounters::default());
    let queue = AlertQueue::new(counters.clone());
    let watched: Arc<std::sync::Mutex<Vec<ProcessIdentity>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let (snap_tx, snap_rx) = watch::channel(Arc::new({
        let mut c = monitor_core::collect::create_collector();
        c.collect(&[])
    }));
    {
        let watched = watched.clone();
        std::thread::spawn(move || {
            let mut collector = monitor_core::collect::create_collector();
            loop {
                let list = watched.lock().map(|g| g.clone()).unwrap_or_default();
                let out = collector.collect(&list);
                let _ = snap_tx.send(Arc::new(out));
                std::thread::sleep(Duration::from_millis(200));
            }
        });
    }
    let (journal_tx, mut journal_rx) = mpsc::channel(256);
    let journal_events: Arc<Mutex<Vec<JournalEvent>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let journal_events = journal_events.clone();
        tokio::spawn(async move {
            while let Some(ev) = journal_rx.recv().await {
                journal_events.lock().unwrap().push(ev);
            }
        });
    }
    let (cmd_tx, cmd_rx) = mpsc::channel(8);
    let (metrics_tx, _metrics_rx) = watch::channel(Arc::new(MetricsReport::default()));
    let (status_tx, _status_rx) = watch::channel(Arc::new(StatusReport {
        started_at: std::time::SystemTime::now(),
        ready: false,
        targets: Vec::new(),
    }));
    let (config_view_tx, _config_view_rx) =
        tokio::sync::watch::channel(Arc::new(serde_json::Value::Null));
    let handles = SchedulerHandles {
        snapshot_rx: snap_rx,
        alert: queue.clone(),
        watched,
        journal_tx: journal_tx.into(),
        control_rx: cmd_rx,
        status_tx,
        metrics_tx,
        pm: ProcessManager::new(),
        replayed: Default::default(),
        config_path: Default::default(),
        config_view_tx,
    };
    let eval_task = tokio::spawn(async move {
        let _ = monitor_core::eval::run(cfg, handles).await;
    });

    // 真实分发任务:AlertQueue → WebhookNotifier (JSON 格式，无签名)。
    let notifier = WebhookNotifier::new(
        hook.url.clone(),
        WebhookFormat::Json,
        Duration::from_secs(2),
        None,
        std::collections::BTreeMap::new(),
    )
    .unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let dispatcher = tokio::spawn(run_dispatcher(
        queue.clone(),
        Arc::new(notifier) as Arc<dyn Notifier>,
        AlertCfg {
            retries: 0,
            timeout: Duration::from_secs(2),
            ..Default::default()
        },
        counters.clone(),
        shutdown_rx,
        None,
    ));

    // ---- 阶段 1:健康预热 —— 无告警 ----
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(hook.hits().is_empty(), "健康期不得产生任何 webhook 投递");

    // ---- 静默建立 (60s 窗口，带 reason) ----
    cmd_tx
        .send(ControlCommand::Silence {
            target: "api".into(),
            duration_secs: 60,
            reason: Some("planned deploy".into()),
        })
        .await
        .unwrap();

    // ---- 阶段 2:失败 → Alert(Warning) + Escalate(Critical) 被静默 ----
    stub.status.store(500, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(1600)).await;
    assert!(
        hook.hits().is_empty(),
        "静默期间 critical 通知不得出 webhook,got {:?}",
        hook.hits()
    );
    assert_eq!(
        counters.sent.load(Ordering::Relaxed),
        0,
        "AlertQueue 无投递 (抑制在入队点完成)"
    );

    // ---- 阶段 3:恢复 → Resolve 永不静默，穿透到 webhook (静默仍生效) ----
    stub.status.store(200, Ordering::Relaxed);
    let hits = wait_hits(&hook, 1, "Resolve 穿透静默").await;
    assert_eq!(
        hits[0]["category"], "resolve",
        "第一笔投递必须是恢复通知 (静默窗口内)"
    );

    // ---- 解除静默 ----
    cmd_tx
        .send(ControlCommand::Unsilence {
            target: "api".into(),
        })
        .await
        .unwrap();

    // ---- 阶段 4:再次失败 → 新 Alert + 新 Critical 正常出 webhook ----
    stub.status.store(500, Ordering::Relaxed);
    let hits = wait_hits(&hook, 3, "解除后新告警投递").await;
    assert_eq!(hits[1]["category"], "alert", "解除后 Warning 正常投递");
    assert_eq!(
        hits[2]["category"], "escalate",
        "解除后新 Critical 正常投递"
    );
    assert_eq!(
        counters.sent.load(Ordering::Relaxed),
        3,
        "三笔全部经 AlertQueue → 分发确认成功"
    );

    // ---- journal 审计与历史完整性 ----
    let evs = journal_events.lock().unwrap().clone();
    let audits: Vec<&String> = evs
        .iter()
        .filter_map(|e| match e {
            JournalEvent::Audit { message, .. } => Some(message),
            _ => None,
        })
        .collect();
    assert!(
        audits.iter().any(|m| {
            m.starts_with("silence set for api")
                && m.contains("60s")
                && m.contains("monotonic")
                && m.contains("reason: planned deploy")
        }),
        "建立静默审计 (含时长/单调到期/reason),got {audits:?}"
    );
    assert_eq!(
        audits
            .iter()
            .filter(|m| m.starts_with("notifications silenced for target api"))
            .count(),
        1,
        "静默期首次抑制命中恰好一条可见性 Audit (防刷屏),got {audits:?}"
    );
    assert!(
        audits
            .iter()
            .any(|m| m.starts_with("silence removed for api")),
        "解除静默审计，got {audits:?}"
    );
    // 历史完整性：静默期间 (第一笔 webhook 投递之前) 的 Incident 事件照常写 ——
    // 存在被抑制的 alert 与 escalate 两类 Incident。
    use monitor_core::model::alert::Category;
    for (cat, label) in [(Category::Alert, "alert"), (Category::Escalate, "escalate")] {
        assert!(
            evs.iter().any(|e| matches!(
                e,
                JournalEvent::Incident { category, .. } if *category == cat
            )),
            "静默抑制不影响 journal Incident 事件 (category={label}),got {evs:?}"
        );
    }

    // ---- 收尾 ----
    shutdown_tx.send(true).ok();
    let _ = dispatcher.await;
    eval_task.abort();
    let _ = eval_task.await;
}
