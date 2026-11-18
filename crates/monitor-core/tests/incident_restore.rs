//! F25 incident 跨重启恢复与重通知：真实 journal 写入 → 重放 → 引擎恢复
//! (ROADMAP v0.4 F25 验收的自动化固化)。
//!
//! 场景：上一次 agent 会话写入 open incident(Alert Warning → Escalate Critical,
//! 无配对 Resolve) 到 journal → 本测试以真实 `run_writer` 落盘、`replay` 重放，
//! 将重放态注入新一轮 agent(`SchedulerHandles.replayed`) 后断言：
//! 1. `/status` 的 incident 字段**立即**呈现恢复的 critical(调度开始前);
//! 2. 启动重通知产出 (类别复用 Alert,severity = 恢复值，reason 前缀
//!    `restored from journal:`),走既有 AlertQueue 链路;
//! 3. 探测恢复后 resolve 正常收口 (Resolve 引用原 incident);
//! 4. 再次故障 → 新 incident 首条通知永不冷却 (新一轮告警可达),
//!    `/metrics` 的 checks_ok|warning|critical 随 live 探测自然再升级
//!    (检查状态机刻意 fresh —— 见 DESIGN「跨重启恢复」取舍)。

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use monitor_core::alert::{AlertCounters, AlertQueue};
use monitor_core::config::{CheckCfg, CheckKindCfg, Config};
use monitor_core::eval::SchedulerHandles;
use monitor_core::model::alert::{AlertEvent, Category, Severity};
use monitor_core::model::control::{StatusReport, TargetMode};
use monitor_core::recover::journal::{self, JournalEvent, JournalState};
use monitor_core::recover::process::ProcessManager;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};

/// 探测目标标签 (独立 check 的 (target, check) = (名字，名字))。
const TARGET: &str = "probe-target";

struct Harness {
    status_rx: watch::Receiver<Arc<StatusReport>>,
    metrics_rx: watch::Receiver<Arc<monitor_core::model::control::MetricsReport>>,
    alert_queue: Arc<AlertQueue>,
    _journal_dir: tempfile_guard::TempDir,
    task: tokio::task::JoinHandle<()>,
}

mod tempfile_guard {
    /// 临时目录 RAII(进程退出即清;不引入 tempfile 依赖)。
    pub struct TempDir(pub std::path::PathBuf);
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

/// 用真实 journal writer 写入「上一次会话」的 open incident:
/// Alert(Warning) → Escalate(Critical),无配对 Resolve。
async fn write_previous_session_journal(path: &std::path::Path) {
    let (tx, rx) = journal::channel(16);
    let file = journal::open_writer(path).expect("open journal");
    let writer = tokio::spawn(journal::run_tracked_writer_with_limit(
        file,
        path.to_path_buf(),
        rx,
        tx.counters().clone(),
        0,
    ));
    let t = SystemTime::now();
    let key = AlertEvent::incident_key(TARGET, TARGET);
    tx.try_send(JournalEvent::Incident {
        at: t,
        incident: key,
        target: TARGET.into(),
        check: TARGET.into(),
        category: Category::Alert,
        severity: Severity::Warning,
        reason: "connect refused".into(),
    })
    .unwrap();
    tx.try_send(JournalEvent::Incident {
        at: t + Duration::from_secs(30),
        incident: key,
        target: TARGET.into(),
        check: TARGET.into(),
        category: Category::Escalate,
        severity: Severity::Critical,
        reason: "still refused".into(),
    })
    .unwrap();
    drop(tx); // 关闭 → writer 返回 Ok(全部落盘)
    writer.await.unwrap().expect("journal writer clean exit");
}

/// 检查配置：独立 tcp check,200ms 间隔 (测试节奏)。
fn probe_cfg(addr: std::net::SocketAddr) -> Config {
    Config {
        checks: vec![CheckCfg {
            name: TARGET.into(),
            kind: CheckKindCfg::Tcp { addr },
            interval: Duration::from_millis(200),
            timeout: Duration::from_millis(800),
        }],
        collect: monitor_core::config::CollectCfg {
            interval: Duration::from_millis(200),
        },
        ..Default::default()
    }
}

/// 启动一轮 agent(eval 单写者 + 真实 journal writer),注入重放态。
async fn spawn_agent(cfg: Config, replayed: JournalState) -> Harness {
    let tmp = tempfile_guard::TempDir(std::env::temp_dir().join(format!(
        "sw-incident-restore-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    )));
    std::fs::create_dir_all(&tmp.0).unwrap();

    let counters = Arc::new(AlertCounters::default());
    let queue = AlertQueue::new(counters);
    let watched: Arc<std::sync::Mutex<Vec<monitor_core::model::ids::ProcessIdentity>>> =
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
    let (journal_tx, journal_rx) = journal::channel(64);
    let journal_path = tmp.0.join("journal.jsonl");
    let journal_file = journal::open_writer(&journal_path).expect("open journal");
    tokio::spawn(journal::run_tracked_writer_with_limit(
        journal_file,
        journal_path,
        journal_rx,
        journal_tx.counters().clone(),
        0,
    ));
    let (cmd_tx, cmd_rx) = mpsc::channel(4);
    drop(cmd_tx); // 控制平面未启用
    let (metrics_tx, metrics_rx) = watch::channel(Arc::new(
        monitor_core::model::control::MetricsReport::default(),
    ));
    let (status_tx, status_rx) = watch::channel(Arc::new(StatusReport {
        started_at: SystemTime::now(),
        ready: false,
        targets: Vec::new(),
    }));
    let (config_view_tx, _config_view_rx) =
        tokio::sync::watch::channel(Arc::new(serde_json::Value::Null));
    let handles = SchedulerHandles {
        snapshot_rx: snap_rx,
        alert: queue.clone(),
        watched,
        journal_tx,
        control_rx: cmd_rx,
        status_tx,
        metrics_tx,
        pm: ProcessManager::new(),
        replayed,
        config_path: Default::default(),
        config_view_tx,
    };
    let task = tokio::spawn(async move {
        let _ = monitor_core::eval::run(cfg, handles).await;
    });
    Harness {
        status_rx,
        metrics_rx,
        alert_queue: queue,
        _journal_dir: tmp,
        task,
    }
}

/// 轮询 /status 直到条件满足或超时。
async fn wait_for_status(
    rx: &mut watch::Receiver<Arc<StatusReport>>,
    timeout: Duration,
    mut pred: impl FnMut(&StatusReport) -> bool,
) -> Arc<StatusReport> {
    wait_for_watch(rx, timeout, move |r: &Arc<StatusReport>| pred(r)).await
}

/// 轮询 watch 值直到条件满足或超时 (通用)。
async fn wait_for_watch<T: Clone>(
    rx: &mut watch::Receiver<T>,
    timeout: Duration,
    mut pred: impl FnMut(&T) -> bool,
) -> T {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let snap = rx.borrow().clone();
        if pred(&snap) {
            return snap;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("wait_for_watch 超时");
        }
        let _ = tokio::time::timeout(Duration::from_millis(100), rx.changed()).await;
    }
}

/// 从告警队列取下一个事件 (5s 超时即失败;shutdown 常闭)。
async fn next_alert(queue: &Arc<AlertQueue>) -> AlertEvent {
    let (_tx, rx) = watch::channel(false);
    tokio::time::timeout(Duration::from_secs(5), queue.clone().pop(rx))
        .await
        .expect("alert event within timeout")
        .expect("queue alive")
}

/// bind :0 后立刻 drop:得到一个大概率保持空闲的端口 (探测目标 = 拒绝连接)。
fn reserve_closed_addr() -> std::net::SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    addr
}

#[tokio::test(flavor = "multi_thread")]
async fn open_incident_restored_renotified_resolved_and_realerted() {
    // ---- 上一次会话：真实 journal 写入 ----
    let tmp = tempfile_guard::TempDir(std::env::temp_dir().join(format!(
        "sw-incident-restore-journal-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    )));
    std::fs::create_dir_all(&tmp.0).unwrap();
    let journal_path = tmp.0.join("journal.jsonl");
    write_previous_session_journal(&journal_path).await;

    // ---- 真实重放:open incident 进入重放态 ----
    let replayed = journal::replay_checked(&journal_path).unwrap_or_default();
    let key = AlertEvent::incident_key(TARGET, TARGET);
    let restored = replayed
        .open_incidents
        .get(&(TARGET.to_string(), TARGET.to_string()))
        .expect("重放必须收集未闭合 incident");
    assert_eq!(restored.incident_key, key);
    assert_eq!(
        restored.severity,
        Severity::Critical,
        "escalate 刷新 severity"
    );
    assert_eq!(restored.reason, "still refused");

    // ---- 新一轮 agent:探测目标先关闭 ----
    let addr = reserve_closed_addr();
    let mut h = spawn_agent(probe_cfg(addr), replayed).await;

    // 1) /status 立即呈现恢复的 critical(调度开始前已注入引擎)。
    //    注:health 字段刻意不判 —— 检查状态机 fresh，级别随 live 探测重新升级
    //    (DESIGN「跨重启恢复」取舍)。
    let report = wait_for_status(&mut h.status_rx, Duration::from_secs(10), |r| {
        r.targets.iter().any(|t| {
            t.name == TARGET
                && t.incident == Some(monitor_core::model::control::HealthLevel::Critical)
        })
    })
    .await;
    let t = report.targets.iter().find(|t| t.name == TARGET).unwrap();
    assert_eq!(t.mode, TargetMode::Check);
    assert_eq!(
        t.incident,
        Some(monitor_core::model::control::HealthLevel::Critical),
        "恢复的 critical 立即呈现"
    );

    // 2) 启动重通知：类别复用 Alert、severity 用恢复值、reason 前缀。
    let renotify = next_alert(&h.alert_queue).await;
    assert_eq!(renotify.incident, key);
    assert_eq!(renotify.category, Category::Alert, "复用 Alert，不新增枚举");
    assert_eq!(renotify.severity, Severity::Critical);
    assert_eq!(renotify.reason, "restored from journal: still refused");

    // ---- 探测恢复：端口开放 → 连续成功 → Resolve 收口 ----
    let listener = TcpListener::bind(addr).await.unwrap();
    let resolve = next_alert(&h.alert_queue).await;
    assert_eq!(
        resolve.category,
        Category::Resolve,
        "恢复后 resolve 正常收口"
    );
    assert_eq!(resolve.incident, key, "Resolve 引用原 incident");
    assert_eq!(
        resolve.severity,
        Severity::Critical,
        "Resolve 携带历史最高 severity"
    );
    drop(listener);

    // ---- 再次故障：新 incident 首条通知永不冷却 ----
    // 1 次失败 → Warning(Alert);连续 3 次 → Critical(Escalate)。
    let reopened = next_alert(&h.alert_queue).await;
    assert_eq!(reopened.category, Category::Alert, "新 incident 首条通知");
    assert_eq!(reopened.severity, Severity::Warning);
    assert_ne!(
        reopened.reason, renotify.reason,
        "新会话 reason 来自 live 探测"
    );
    let escalate = next_alert(&h.alert_queue).await;
    assert_eq!(
        escalate.category,
        Category::Escalate,
        "新一轮 Critical 可告警"
    );
    assert_eq!(escalate.severity, Severity::Critical);

    // 3) /metrics checks_*:critical 随 live 探测再升级后立即反映。
    let metrics = wait_for_watch(
        &mut h.metrics_rx,
        Duration::from_secs(10),
        |m: &Arc<monitor_core::model::control::MetricsReport>| m.checks_critical == 1,
    )
    .await;
    assert_eq!(metrics.checks_critical, 1, "唯一 check 处于 Critical");
    assert_eq!(metrics.checks_ok, 0, "critical 期间不得计入 ok");

    // /status 的 incident 仍呈现 (升级后保持 Critical)。
    let report = wait_for_status(&mut h.status_rx, Duration::from_secs(10), |r| {
        r.targets.iter().any(|t| {
            t.name == TARGET
                && t.incident == Some(monitor_core::model::control::HealthLevel::Critical)
        })
    })
    .await;
    assert_eq!(
        report.targets[0].incident,
        Some(monitor_core::model::control::HealthLevel::Critical)
    );

    h.task.abort();
}

/// 空重放表零行为变化 (向后兼容):不注入、不重通知，探测失败走正常首告警。
#[tokio::test(flavor = "multi_thread")]
async fn empty_replay_table_keeps_behavior_unchanged() {
    let addr = reserve_closed_addr();
    let mut h = spawn_agent(probe_cfg(addr), JournalState::default()).await;

    // 唯一事件:live 探测失败的首条 Alert(Warning),无 restored 前缀。
    let first = next_alert(&h.alert_queue).await;
    assert_eq!(first.category, Category::Alert);
    assert_eq!(first.severity, Severity::Warning);
    assert!(
        !first.reason.contains("restored from journal"),
        "空表不得产出恢复重通知:{first:?}"
    );

    // /status 无恢复注入的 incident(首告警后 incident = Warning，来自 live 状态)。
    let report = wait_for_status(&mut h.status_rx, Duration::from_secs(10), |r| {
        r.targets.iter().any(|t| t.name == TARGET)
    })
    .await;
    let t = report.targets.iter().find(|t| t.name == TARGET).unwrap();
    assert_ne!(
        t.incident,
        Some(monitor_core::model::control::HealthLevel::Critical),
        "空表：不得凭空呈现 critical"
    );

    h.task.abort();
}
