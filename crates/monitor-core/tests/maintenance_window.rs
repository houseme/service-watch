//! F31 定期维护窗口集成测试 (ROADMAP F31 验收「集成」):真实 eval 调度循环
//! (可控健康 http stub)+ 真实 AlertQueue + `run_dispatcher` + 真实 webhook
//! stub (axum 记录器),端到端验证：
//! - **窗口内 critical 不出 webhook**(Alert/Escalate 被抑制;journal Incident
//!   事件照常写 —— 历史完整性;窗口开启写一条 Audit 边沿);
//! - **Resolve 永不抑制**:窗口仍生效时恢复通知穿透到 webhook;
//! - **窗口外正常出 webhook**(days 过滤注入「今天不开窗」的窗口)。
//!
//! 窗口注入方式 (不依赖注入时钟 —— eval 用真实 UTC 墙钟判定):
//! - 「必在窗口」:`start_utc = "00:00"` + `duration = 48h` —— 任一时刻距最近
//!   一次 00:00 开启 ≤ 24h < 48h，恒在窗口内;
//! - 「必不在窗口」:`days` 取**非今天**的星期 (经
//!   `maintenance::weekday_minute_of_day` 换算 +3 天，即使测试跨越 UTC 日界，
//!   ±1 天也不至于命中),`duration = 60s` —— 整个测试期窗口不存在。
//!
//! webhook/分发在 http-client feature 门控内 —— no-default 组合下整个测试
//! 文件退出编译 (对齐 tests/silence_notification.rs 的门控纪律)。

#![cfg(feature = "http-client")]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use monitor_core::alert::webhook::WebhookNotifier;
use monitor_core::alert::{AlertCounters, AlertQueue, Notifier, run_dispatcher};
use monitor_core::config::{
    AlertCfg, CheckCfg, CheckKindCfg, Config, MaintenanceCfg, StatusRange, WebhookFormat,
};
use monitor_core::eval::SchedulerHandles;
use monitor_core::eval::maintenance::{WEEKDAYS, weekday_minute_of_day};
use monitor_core::model::control::{MetricsReport, StatusReport};
use monitor_core::model::ids::ProcessIdentity;
use monitor_core::recover::journal::JournalEvent;
use monitor_core::recover::process::ProcessManager;
use tokio::sync::{mpsc, watch};

/// 记录 POST body 的 webhook stub (与 tests/silence_notification.rs 同形)。
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

/// 可脚本化健康状态的 http stub (与 tests/silence_notification.rs 同形)。
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

/// 单 http 检查 + 一条维护窗口的配置 (threshold = 1:一次失败即 Warning
/// Alert，连续第二次失败即 Critical Escalate，一次成功即 Resolve;renotify
/// 关闭避免窗口内重发噪声)。
fn maintenance_test_cfg(webhook: url::Url, probe: String, windows: Vec<MaintenanceCfg>) -> Config {
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
        maintenance: windows,
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

/// 装配真实调度循环 + 分发任务 (与 tests/silence_notification.rs 同形;
/// webhook stub 由调用方构造并传入，通知器指向同一 url),返回 journal
/// 事件采集句柄与收尾用句柄。
#[allow(clippy::type_complexity)]
async fn spawn_stack(
    cfg: Config,
    hook: &HookStub,
) -> (
    Arc<AlertCounters>,
    Arc<Mutex<Vec<JournalEvent>>>,
    tokio::task::JoinHandle<()>,
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
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
    let (_cmd_tx, cmd_rx) = mpsc::channel(8);
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
    (counters, journal_events, dispatcher, shutdown_tx, eval_task)
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

fn audit_texts(evs: &[JournalEvent]) -> Vec<String> {
    evs.iter()
        .filter_map(|e| match e {
            JournalEvent::Audit { message, .. } => Some(message.clone()),
            _ => None,
        })
        .collect()
}

/// 窗口内:critical 不出 webhook,Resolve 穿透，窗口开启 Audit 边沿落盘，
/// journal Incident 事件照常写 (历史完整性)。
#[tokio::test(flavor = "multi_thread")]
async fn maintenance_window_suppresses_critical_and_resolve_penetrates() {
    let hook = HookStub::spawn().await;
    let stub = HealthStub::spawn().await;
    // 「必在窗口」:00:00 开启 48h —— 对任意真实 UTC 时刻恒在窗口内。
    let cfg = maintenance_test_cfg(
        hook.url.clone(),
        format!("http://{}/hz", stub.addr),
        vec![MaintenanceCfg {
            name: "always".into(),
            targets: vec!["*".into()],
            start_utc: "00:00".into(),
            duration: Duration::from_secs(48 * 3600),
            days: Vec::new(),
        }],
    );

    let (counters, journal_events, dispatcher, shutdown_tx, eval_task) =
        spawn_stack(cfg, &hook).await;

    // ---- 阶段 1:健康预热 —— 无告警 ----
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(hook.hits().is_empty(), "健康期不得产生任何 webhook 投递");

    // ---- 阶段 2:失败 → Alert(Warning) + Escalate(Critical) 被窗口抑制 ----
    stub.status.store(500, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(1600)).await;
    assert!(
        hook.hits().is_empty(),
        "维护窗口内 critical 通知不得出 webhook,got {:?}",
        hook.hits()
    );
    assert_eq!(
        counters.sent.load(Ordering::Relaxed),
        0,
        "AlertQueue 无投递 (抑制在入队点完成)"
    );

    // ---- 阶段 3:恢复 → Resolve 永不抑制，穿透到 webhook (窗口仍生效) ----
    stub.status.store(200, Ordering::Relaxed);
    let hits = wait_hits(&hook, 1, "Resolve 穿透维护窗口").await;
    assert_eq!(
        hits[0]["category"], "resolve",
        "第一笔投递必须是恢复通知 (维护窗口内)"
    );
    assert_eq!(counters.sent.load(Ordering::Relaxed), 1);

    // ---- journal:开启 Audit 边沿 + Incident 历史完整性 ----
    let evs = journal_events.lock().unwrap().clone();
    let audits = audit_texts(&evs);
    assert!(
        audits
            .iter()
            .any(|m| m.starts_with("maintenance window opened: always") && m.contains("UTC")),
        "窗口开启写一条 Audit (含窗口名与 UTC 语义),got {audits:?}"
    );
    assert!(
        !audits
            .iter()
            .any(|m| m.contains("maintenance window closed")),
        "窗口未关闭，不得出现 closed 审计"
    );
    use monitor_core::model::alert::Category;
    for (cat, label) in [(Category::Alert, "alert"), (Category::Escalate, "escalate")] {
        assert!(
            evs.iter().any(|e| matches!(
                e,
                JournalEvent::Incident { category, .. } if *category == cat
            )),
            "维护窗口抑制不影响 journal Incident 事件 (category={label}),got {evs:?}"
        );
    }

    // ---- 收尾 ----
    shutdown_tx.send(true).ok();
    let _ = dispatcher.await;
    eval_task.abort();
    let _ = eval_task.await;
}

/// 窗口外:days 注入「今天不开窗」的窗口，告警正常出 webhook。
#[tokio::test(flavor = "multi_thread")]
async fn outside_maintenance_window_delivers_alerts_normally() {
    let hook = HookStub::spawn().await;
    let stub = HealthStub::spawn().await;
    // 「必不在窗口」:days = 今天 +3 天 (即使测试跨 UTC 日界，±1 天也不命中),
    // 60s 窗口只在那个星期几的 00:00-00:01 存在。
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (today, _) = weekday_minute_of_day(now_secs);
    let out_day = WEEKDAYS[(today + 3) % 7].to_string();
    let cfg = maintenance_test_cfg(
        hook.url.clone(),
        format!("http://{}/hz", stub.addr),
        vec![MaintenanceCfg {
            name: "never".into(),
            targets: vec!["*".into()],
            start_utc: "00:00".into(),
            duration: Duration::from_secs(60),
            days: vec![out_day],
        }],
    );

    let (counters, journal_events, dispatcher, shutdown_tx, eval_task) =
        spawn_stack(cfg, &hook).await;

    // ---- 健康预热 ----
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(hook.hits().is_empty(), "健康期不得产生任何 webhook 投递");

    // ---- 失败 → Alert + Escalate 正常投递 (窗口外) ----
    stub.status.store(500, Ordering::Relaxed);
    let hits = wait_hits(&hook, 2, "窗口外正常告警投递").await;
    assert_eq!(hits[0]["category"], "alert", "窗口外 Warning 正常投递");
    assert_eq!(hits[1]["category"], "escalate", "窗口外 Critical 正常投递");
    assert_eq!(
        counters.sent.load(Ordering::Relaxed),
        2,
        "两笔全部经 AlertQueue → 分发确认成功"
    );

    // ---- journal:窗口从未开启，无任何维护窗口审计 ----
    let evs = journal_events.lock().unwrap().clone();
    let audits = audit_texts(&evs);
    assert!(
        !audits.iter().any(|m| m.contains("maintenance window")),
        "窗口从未开启，不得出现维护窗口审计，got {audits:?}"
    );

    // ---- 收尾 ----
    shutdown_tx.send(true).ok();
    let _ = dispatcher.await;
    eval_task.abort();
    let _ = eval_task.await;
}
