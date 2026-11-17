//! HTTP 探测集成测试 (DESIGN §11.3):真实 axum stub + 真实调度器，
//! 端到端验证「慢响应 → 告警 → 恢复」链路 (F4 验收)。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use monitor_core::alert::{AlertCounters, AlertQueue};
use monitor_core::config::{AlertCfg, CheckCfg, CheckKindCfg, Config, StatusRange};
use monitor_core::eval::SchedulerHandles;
use monitor_core::model::alert::{AlertEvent, Category, Severity};
use monitor_core::model::control::StatusReport;
use monitor_core::model::ids::ProcessIdentity;
use monitor_core::recover::process::ProcessManager;
use tokio::sync::{mpsc, watch};

/// 可脚本化的 HTTP stub:延迟 (毫秒) 与状态码运行时可调。
struct Stub {
    delay_ms: Arc<AtomicU64>,
    status: Arc<AtomicU64>,
    addr: std::net::SocketAddr,
}

impl Stub {
    async fn spawn() -> Self {
        let delay_ms = Arc::new(AtomicU64::new(0));
        let status = Arc::new(AtomicU64::new(200));
        let app = {
            let delay_ms = delay_ms.clone();
            let status = status.clone();
            axum::Router::new().route(
                "/hz",
                axum::routing::get(move || {
                    let delay_ms = delay_ms.clone();
                    let status = status.clone();
                    async move {
                        let d = delay_ms.load(Ordering::Relaxed);
                        if d > 0 {
                            tokio::time::sleep(Duration::from_millis(d)).await;
                        }
                        let code =
                            axum::http::StatusCode::from_u16(status.load(Ordering::Relaxed) as u16)
                                .unwrap_or(axum::http::StatusCode::OK);
                        Result::<axum::http::StatusCode, std::convert::Infallible>::Ok(code)
                    }
                }),
            )
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            delay_ms,
            status,
            addr,
        }
    }
}

/// 组装最小 Config(仅 http 检查)。
fn http_check_config(url: String, timeout_ms: u64, interval_ms: u64) -> Config {
    Config {
        checks: vec![CheckCfg {
            name: "api".into(),
            kind: CheckKindCfg::Http {
                url: url::Url::parse(&url).unwrap(),
                expected_status: StatusRange(200, 299),
            },
            interval: Duration::from_millis(interval_ms),
            timeout: Duration::from_millis(timeout_ms),
        }],
        alert: AlertCfg {
            renotify_interval: Duration::from_secs(3600), // 测试期内不重发
            ..Default::default()
        },
        ..Default::default()
    }
}

/// 调度器装配：自建采集线程 (200ms 快照;与 runtime.collect_loop 同形)+
/// eval 后台任务;返回告警队列 (事件经 pop 排空断言)。
async fn spawn_scheduler(
    cfg: Config,
) -> (
    Arc<AlertQueue>,
    tokio::task::JoinHandle<()>,
    watch::Receiver<Arc<monitor_core::model::control::MetricsReport>>,
) {
    let counters = Arc::new(AlertCounters::default());
    let queue = AlertQueue::new(counters);
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
    let (journal_tx, _journal_rx) = mpsc::channel(64); // 接收端持有不消费 (测试不落盘)
    let (cmd_tx, cmd_rx) = mpsc::channel(4);
    drop(cmd_tx); // 无控制平面
    let (metrics_tx, metrics_rx) = watch::channel(Arc::new(
        monitor_core::model::control::MetricsReport::default(),
    ));
    let (status_tx, _status_rx) = watch::channel(Arc::new(StatusReport {
        started_at: std::time::SystemTime::now(),
        ready: false,
        targets: Vec::new(),
    }));
    let handles = SchedulerHandles {
        snapshot_rx: snap_rx,
        alert: queue.clone(),
        watched,
        journal_tx: journal_tx.into(),
        control_rx: cmd_rx,
        status_tx,
        metrics_tx: metrics_tx.clone(),
        pm: ProcessManager::new(),
        replayed: Default::default(),
        config_path: Default::default(),
    };
    let task = tokio::spawn(async move {
        let _ = monitor_core::eval::run(cfg, handles).await;
    });
    (queue, task, metrics_rx)
}

/// 排空队列 (shutdown watch 恒真:pop 取空即停，不挂起)。
async fn drain(queue: &Arc<AlertQueue>) -> Vec<AlertEvent> {
    let (_, always_shutdown) = watch::channel(true);
    let mut out = Vec::new();
    while let Some(ev) = queue.clone().pop(always_shutdown.clone()).await {
        out.push(ev);
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_response_alerts_and_recovers_without_false_critical() {
    let stub = Stub::spawn().await;
    let url = format!("http://{}/hz", stub.addr);
    let cfg = http_check_config(url, 100, 200);
    let (queue, task, metrics_rx) = spawn_scheduler(cfg).await;

    // 阶段 1:快速 200 —— 首轮探测成功，无事件。
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(
        drain(&queue)
            .await
            .iter()
            .all(|e| e.category == Category::Resolve),
        "预热期不得产生告警"
    );

    // 阶段 2:延迟 300ms > timeout 100ms → 连续失败 → Warning → Critical。
    stub.delay_ms.store(300, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // 阶段 3:恢复 —— 连续 2 次成功 → Resolve。
    stub.delay_ms.store(0, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(900)).await;

    // 运行指标管线:eval 每轮发布 (监控运行情况的验收断言)。
    let m = metrics_rx.borrow().clone();
    assert_eq!(m.targets_total, 1, "必须上报 target 总数");
    assert!(
        m.probe_executed >= 3,
        "预热期应已执行多轮探测，实际 {}",
        m.probe_executed
    );
    assert!(m.uptime_secs < 60, "uptime 应为运行时长");

    let events = drain(&queue).await;
    task.abort();
    assert!(!events.is_empty(), "必须产生事件");
    let cats: Vec<(Category, Severity)> = events.iter().map(|e| (e.category, e.severity)).collect();

    assert!(
        cats.contains(&(Category::Alert, Severity::Warning)),
        "超时失败必须产生 Warning 告警，实际 {cats:?}"
    );
    assert!(
        cats.contains(&(Category::Escalate, Severity::Critical)),
        "连续 failure_threshold 次失败必须升级 Critical，实际 {cats:?}"
    );
    assert!(
        cats.iter().any(|(c, _)| *c == Category::Resolve),
        "连续 success_threshold 次成功必须 Resolve，实际 {cats:?}"
    );
    // 顺序约束:Alert → Escalate → … → Resolve(最后一个 Resolve 在 Escalate 之后)。
    let alert_idx = cats.iter().position(|x| x.0 == Category::Alert).unwrap();
    let esc_idx = cats.iter().position(|x| x.0 == Category::Escalate).unwrap();
    let resolve_idx = cats
        .iter()
        .rposition(|x| x.0 == Category::Resolve)
        .expect("有 Resolve");
    assert!(alert_idx < esc_idx, "Warning 先于 Critical，实际 {cats:?}");
    assert!(resolve_idx > esc_idx, "Resolve 在升级之后，实际 {cats:?}");
}

/// 状态码区间闭边界:199/300 拒绝，200/299 接受 (StatusRange "200-299")。
#[cfg(feature = "probe-http")]
#[tokio::test(flavor = "multi_thread")]
async fn status_range_boundaries() {
    let stub = Stub::spawn().await;
    let url = format!("http://{}/hz", stub.addr);
    let client = reqwest::Client::new();
    let check = CheckCfg {
        name: "b".into(),
        kind: CheckKindCfg::Http {
            url: url::Url::parse(&url).unwrap(),
            expected_status: StatusRange(200, 299),
        },
        interval: Duration::from_secs(1),
        timeout: Duration::from_secs(1),
    };
    for (code, ok) in [(199u64, false), (200, true), (299, true), (300, false)] {
        stub.status.store(code, Ordering::Relaxed);
        let ctx = monitor_core::eval::check::ProbeCtx {
            http: Some(&client),
            process_snapshot: None,
            expected_identity: None,
        };
        let r = monitor_core::eval::check::execute(&check, ctx).await;
        assert_eq!(
            r.is_ok(),
            ok,
            "status {code} 应当 {}",
            if ok { "成功" } else { "失败" }
        );
    }
}
