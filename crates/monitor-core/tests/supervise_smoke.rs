//! supervise 真进程冒烟测试 (DESIGN §11.3,EXECUTION 阶段 3 验收的自动化固化)。
//! Unix 专用 (/bin/sleep、kill -9);Windows runner 由 cfg 门排除。
//!
//! 覆盖:① 冷启动 spawn + /status 身份;② kill -9 → 确定性死亡 → 自动拉活;
//! ③ crash loop(/bin/false)→ 预算耗尽 → Fatal 闩锁。

#![cfg(unix)]

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use monitor_core::alert::{AlertCounters, AlertQueue};
use monitor_core::config::{Config, RestartCfg, ServiceCfg};
use monitor_core::eval::SchedulerHandles;
use monitor_core::model::control::{RecoveryState, StatusReport};
use monitor_core::recover::journal::JournalState;
use monitor_core::recover::process::ProcessManager;
use tokio::sync::{mpsc, watch};

struct Harness {
    status_rx: watch::Receiver<Arc<StatusReport>>,
    _journal_dir: tempfile_guard::TempDir,
    task: tokio::task::JoinHandle<()>,
    /// 控制命令出口 (F20 级联测试注入 restart;既有测试未用即自然丢弃)。
    cmd_tx: tokio::sync::mpsc::Sender<monitor_core::model::control::ControlCommand>,
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

fn service_cfg(name: &str, command: &str, args: &[&str], max_restarts: u32) -> ServiceCfg {
    ServiceCfg {
        name: name.into(),
        command: command.into(),
        args: args.iter().map(|s| s.to_string()).collect(),
        workdir: None,
        env: Default::default(),
        console: false,
        windows_service: None,
        depends_on: Vec::new(),
        restart: RestartCfg {
            min_uptime: Duration::from_secs(1),
            backoff_initial: Duration::from_millis(100),
            backoff_max: Duration::from_millis(500),
            max_restarts,
            restart_window: Duration::from_secs(60),
            stop_grace: Duration::from_secs(2),
            startup_grace: Duration::from_secs(1),
            confirmations: 2,
            hung_action: Default::default(),
        },
        activate: None,
    }
}

async fn spawn_agent(services: Vec<ServiceCfg>) -> Harness {
    let tmp = tempfile_guard::TempDir(std::env::temp_dir().join(format!(
        "sw-supervise-test-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().subsec_nanos()
    )));
    let cfg = Config {
        services,
        collect: monitor_core::config::CollectCfg {
            interval: Duration::from_millis(200),
        },
        ..Default::default()
    };

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
    let (journal_tx, journal_rx) = mpsc::channel(64);
    tokio::spawn(monitor_core::recover::journal::run_writer(
        tmp.0.join("journal.jsonl"),
        journal_rx,
    ));
    let (cmd_tx, cmd_rx) = mpsc::channel(4);
    let (metrics_tx, _metrics_rx) = watch::channel(Arc::new(
        monitor_core::model::control::MetricsReport::default(),
    ));
    let (status_tx, status_rx) = watch::channel(Arc::new(StatusReport {
        started_at: SystemTime::now(),
        ready: false,
        targets: Vec::new(),
    }));
    let mut pm = ProcessManager::new();
    for s in &cfg.services {
        pm.register(s.clone());
    }
    let handles = SchedulerHandles {
        snapshot_rx: snap_rx,
        alert: queue,
        watched,
        journal_tx: journal_tx.into(),
        control_rx: cmd_rx,
        status_tx: status_tx.clone(),
        metrics_tx: metrics_tx.clone(),
        pm,
        replayed: JournalState::default(),
        config_path: Default::default(),
    };
    let task = tokio::spawn(async move {
        let _ = monitor_core::eval::run(cfg, handles).await;
    });
    Harness {
        status_rx,
        _journal_dir: tmp,
        task,
        cmd_tx,
    }
}

/// 轮询 /status 直到条件满足或超时。
async fn wait_for(
    rx: &mut watch::Receiver<Arc<StatusReport>>,
    timeout: Duration,
    mut pred: impl FnMut(&StatusReport) -> bool,
) -> Arc<StatusReport> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        {
            let snap = rx.borrow().clone();
            if pred(&snap) {
                return snap;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("wait_for 超时:last = {:#?}", rx.borrow().targets);
        }
        let _ = tokio::time::timeout(Duration::from_millis(300), rx.changed()).await;
    }
}

/// ① 冷启动 spawn:/status 出现 supervise 目标且携带真实身份。
#[tokio::test(flavor = "multi_thread")]
async fn cold_start_spawns_and_reports_identity() {
    let mut h = spawn_agent(vec![service_cfg("sleeper", "/bin/sleep", &["30"], 5)]).await;
    let report = wait_for(&mut h.status_rx, Duration::from_secs(10), |r| {
        r.targets.iter().any(|t| {
            t.name == "sleeper"
                && t.identity.is_some()
                && t.recovery == Some(RecoveryState::Starting)
        })
    })
    .await;
    let t = report.targets.iter().find(|t| t.name == "sleeper").unwrap();
    assert!(t.identity.unwrap().pid.0 > 1, "必须携带真实 pid");
    h.task.abort();
}

/// ② kill -9 → 确定性死亡 → backoff 后自动拉活 (新 pid)+ RESTARTS=1。
#[tokio::test(flavor = "multi_thread")]
async fn kill9_child_is_restarted() {
    let mut h = spawn_agent(vec![service_cfg("sleeper", "/bin/sleep", &["30"], 5)]).await;
    wait_for(&mut h.status_rx, Duration::from_secs(10), |r| {
        r.targets
            .iter()
            .any(|t| t.name == "sleeper" && t.identity.is_some())
    })
    .await;
    let first_pid = h.status_rx.borrow().targets[0].identity.unwrap().pid.0;

    // 模拟外部 kill -9(确定性死亡，不经 agent)
    unsafe { libc_kill(first_pid) };

    let report = wait_for(&mut h.status_rx, Duration::from_secs(10), |r| {
        r.targets
            .iter()
            .any(|t| t.name == "sleeper" && t.identity.is_some_and(|i| i.pid.0 != first_pid))
    })
    .await;
    let t = report.targets.iter().find(|t| t.name == "sleeper").unwrap();
    assert_ne!(t.identity.unwrap().pid.0, first_pid, "必须拉起新进程");
    assert_eq!(t.restarts_in_window, 1, "一次确定性死亡 = 一次自动重启计数");
    h.task.abort();
}

/// ③ crash loop(/bin/false 秒退)→ 滑动窗口预算耗尽 → Fatal 闩锁。
#[tokio::test(flavor = "multi_thread")]
async fn crash_loop_latches_fatal() {
    // max_restarts=2:/bin/false 快退 ×2 → Fatal
    let mut h = spawn_agent(vec![service_cfg("falsey", "/bin/false", &[], 2)]).await;
    let report = wait_for(&mut h.status_rx, Duration::from_secs(20), |r| {
        r.targets
            .iter()
            .any(|t| t.name == "falsey" && t.recovery == Some(RecoveryState::Fatal))
    })
    .await;
    let t = report.targets.iter().find(|t| t.name == "falsey").unwrap();
    assert_eq!(
        t.recovery,
        Some(RecoveryState::Fatal),
        "crash loop 必须 Fatal 闩锁"
    );
    h.task.abort();
}

/// ④ 控制面 restart(P0-2 回归):优雅退出的服务在 deadline 到期后
/// **不得**被残留的强杀兜底误杀 —— 新进程必须存活超过 stop_grace。
#[tokio::test(flavor = "multi_thread")]
async fn control_restart_does_not_kill_new_process() {
    // service_cfg 自带 stop_grace=2s:kill → 自动拉活;若对运行中服务做
    // control restart,deadline(≈2s) 到期时新进程已在跑 —— 旧 bug 在此强杀新进程。
    let mut h = spawn_agent(vec![service_cfg("sleeper", "/bin/sleep", &["30"], 5)]).await;
    wait_for(&mut h.status_rx, Duration::from_secs(10), |r| {
        r.targets
            .iter()
            .any(|t| t.name == "sleeper" && t.identity.is_some())
    })
    .await;
    let first_pid = h.status_rx.borrow().targets[0].identity.unwrap().pid.0;
    unsafe { libc::kill(first_pid as i32, libc::SIGKILL) };

    // 等待自动拉活 (P0-2 场景的前半段)
    let report = wait_for(&mut h.status_rx, Duration::from_secs(10), |r| {
        r.targets
            .iter()
            .any(|t| t.name == "sleeper" && t.identity.is_some_and(|i| i.pid.0 != first_pid))
    })
    .await;
    let second_pid = report.targets[0].identity.unwrap().pid.0;

    // 新进程存活超过 stop_grace(2s)+ 余量：若旧 bug 存在，此窗口内被强杀
    tokio::time::sleep(Duration::from_secs(6)).await;
    let alive = h
        .status_rx
        .borrow()
        .targets
        .iter()
        .any(|t| t.name == "sleeper" && t.identity.is_some_and(|i| i.pid.0 == second_pid));
    assert!(
        alive,
        "新进程必须跨过 stop_grace deadline 存活 (不得被残留兜底强杀)"
    );
    h.task.abort();
}

/// ⑤ F20 级联闭环 (真实进程):restart db → api 计划内停止 (**不计 api 预算**)
/// → db 回 Running → api 被拉回 (新 pid) 且跨过 stop_grace 存活。
#[tokio::test(flavor = "multi_thread")]
async fn control_restart_cascades_stop_and_pullback() {
    let mut h = spawn_agent(vec![service_cfg("db", "/bin/sleep", &["30"], 5), {
        let mut api = service_cfg("api", "/bin/sleep", &["30"], 5);
        api.depends_on = vec!["db".into()];
        api
    }])
    .await;
    // 等 db/api 双 Running
    wait_for(&mut h.status_rx, Duration::from_secs(10), |r| {
        r.targets
            .iter()
            .filter(|t| t.mode == monitor_core::model::control::TargetMode::Supervise)
            .count()
            == 2
            && r.targets
                .iter()
                .all(|t| t.recovery == Some(monitor_core::model::control::RecoveryState::Running))
    })
    .await;
    let db_pid = h
        .status_rx
        .borrow()
        .targets
        .iter()
        .find(|t| t.name == "db")
        .unwrap()
        .identity
        .unwrap()
        .pid
        .0;
    let api_pid = h
        .status_rx
        .borrow()
        .targets
        .iter()
        .find(|t| t.name == "api")
        .unwrap()
        .identity
        .unwrap()
        .pid
        .0;

    // 经控制通道注入 restart db(真实 F20 路径：级联停 api → db 重启 → 拉回 api)
    h.cmd_tx
        .send(monitor_core::model::control::ControlCommand::Restart {
            target: "db".into(),
        })
        .await
        .unwrap();

    // 等 db 新 pid + api 新 pid(拉回)
    let report = wait_for(&mut h.status_rx, Duration::from_secs(15), |r| {
        let db = r.targets.iter().find(|t| t.name == "db");
        let api = r.targets.iter().find(|t| t.name == "api");
        db.is_some_and(|t| t.identity.is_some_and(|i| i.pid.0 != db_pid))
            && api.is_some_and(|t| t.identity.is_some_and(|i| i.pid.0 != api_pid))
    })
    .await;
    let api_new = report.targets.iter().find(|t| t.name == "api").unwrap();
    let db_new = report.targets.iter().find(|t| t.name == "db").unwrap();
    // 预算语义锁定 (第六轮 P3):计划内级联停止**不计 api 预算**(初始 spawn 与
    // 拉回也非自动重启)→ 0;db 的确定性死亡计 1。
    assert_eq!(
        api_new.restarts_in_window, 0,
        "计划内停止/拉回不计 api 预算"
    );
    assert_eq!(db_new.restarts_in_window, 1, "db 确定性死亡计入预算");
    // api 跨过 stop_grace(2s) 存活拉回后的稳定期
    tokio::time::sleep(Duration::from_secs(3)).await;
    let api_alive = h.status_rx.borrow().targets.iter().any(|t| {
        t.name == "api"
            && t.identity.is_some_and(|i| i.pid.0 != api_pid)
            && t.recovery == Some(monitor_core::model::control::RecoveryState::Running)
    });
    assert!(api_alive, "拉回后的 api 必须回 Running 并稳定");
    h.task.abort();
}

/// kill -9(测试内联，避免引入 nix crate)。
unsafe fn libc_kill(pid: u32) {
    unsafe { libc::kill(pid as i32, libc::SIGKILL) };
}
