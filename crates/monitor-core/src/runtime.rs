//! 运行时装配 (DESIGN.md §2.2):采集线程、通道、eval、alert 分发、控制平面、停机。
//!
//! ## 任务拓扑
//!
//! - **collect 线程**(OS 线程，阻塞):独占采集器;快照经 `watch` 广播;
//!   `shutdown` 原子置位后退出。
//! - **journal 写任务**:消费有界通道 (1024) 追加 JSONL;关键事件等待 sync_data
//!   确认 (含入队总计 2s),失败经当前实例的故障通道触发停机。普通审计满队列
//!   时计数丢弃。通道关闭后排空退出。
//! - **alert 分发任务**:消费 AlertQueue(drop-oldest 256;eval 直推，无桥接层),
//!   超时/重试投递;shutdown watch 置位后排空队列再退出。
//! - **eval 任务**(单写者):独占全部状态机;由 runtime abort 停机
//!   (eval 模块 doc「停机语义」)。
//! - **control 任务**:axum serve;由 runtime abort 停机 (control 模块 doc)。
//!
//! ## 配置热重载 (F14)
//!
//! SIGHUP(unix,`SignalKind::hangup`) 与 `POST /v1/reload` 均产生
//! `ControlCommand::Reload` 进既有命令通道，由 eval 单写者读文件 + diff +
//! 应用**目标拓扑**(checks/services/watches 增删改;见 eval::reload)。
//! 其余小节 (agent/log/control/collect/alert/metrics) 变更**要求重启**,不热应用。
//! Windows 无 SIGHUP，热重载仅经控制平面端点触发。
//!
//! ## 停机语义 (v0.1，与 DESIGN §9.1 的差异，如实记录)
//!
//! SIGNAL → 置 shutdown(唤醒采集线程 + 分发排空)→ abort 并等待 eval / control →
//! 有界等待 alert 分发排空 (500ms)→ 关 journal 通道 flush(100ms 窗口)→ 返回。
//! Windows 服务 (F15):`sc stop` 在服务进程内无信号通道，服务入口 (monitor-agent
//! service 模块) 改调 [`request_shutdown`] 进入同一序列。
//!
//! **supervise 子进程在 agent 停机时默认保留运行**(DESIGN §1 原则 1「观察者外置」;
//! Unix 下子进程为 `process_group(0)` 组长，agent 退出不连带杀):`ProcessManager`
//! 由 eval 独占 (契约:`SchedulerHandles.pm` 为所有权字段，无 Arc 可分),
//! runtime 在 abort eval 后无法调用 `shutdown_all`。按 stop_grace 的子进程清理
//! 仅在**恢复流程内**发生;下次启动依据 journal 身份与采集证据对账，存活旧进程
//! 进入只读接管，具体门控见 recover::reconcile。
//!
//! 退出码:main 依据返回的 [`StopReason`] 映射 —— `Signal` → 130(DESIGN §9.3)、
//! `EvalExited` / `PersistenceFailed` → 2(运行错误)、
//! `Clean` → 0。(2026-11-11 接线，替代早期"信号停机返回 0"的 v0.1 简化。)

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::{mpsc, watch};

use crate::alert::{AlertCounters, AlertQueue, Notifier};
use crate::collect::{CollectOutput, Collector, StubCollector, create_collector};
use crate::config::{AlertCfg, Config};
use crate::error::Result;
use crate::model::alert::AlertEvent;
#[cfg(unix)]
use crate::model::control::ControlCommand;
use crate::model::control::{MetricsReport, StatusReport};
use crate::recover::journal::{self, JournalEvent};

/// journal 有界通道容量。关键状态等待落盘确认，审计满队列时计数丢弃。
const JOURNAL_CHANNEL_CAP: usize = 1024;
/// 控制命令通道容量 (DESIGN §2.2:满 → control 返回 503)。
const CONTROL_CHANNEL_CAP: usize = 16;
/// 停机时 alert 分发的排空窗口 (重试退避可达 10s+,v0.1 有界放弃)。
const DISPATCH_DRAIN_WINDOW: std::time::Duration = std::time::Duration::from_millis(500);
/// 停机时 journal flush 窗口 (任务书约定 100ms)。
const JOURNAL_FLUSH_WINDOW: std::time::Duration = std::time::Duration::from_millis(100);
/// F14 热重载默认配置路径 (与 monitor-agent CLI 缺省一致，DESIGN §9.1)。
const DEFAULT_CONFIG_PATH: &str = "service-watch.toml";

/// 停机原因 (main 据此映射退出码:Clean→0 / Signal→130 / 运行故障→2)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// F15 进程内停机请求 (`request_shutdown`;Windows SCM stop 走此入口)。
    Clean,
    /// 收到 SIGTERM/SIGINT(DESIGN §9.3 退出码 130)。
    Signal,
    /// eval 任务意外退出 (panic/错误)—— 停机兜底路径，服务管理器据此 restart。
    EvalExited,
    /// 关键 journal 记录无法在有界窗口内持久化，停止当前实例的自动恢复。
    PersistenceFailed,
}

/// F15 Windows Service:进程内停机请求点 (SCM stop 无信号通道;服务入口收到
/// Stop/Shutdown 后调用，停机 select 经 REQUEST_NOTIFY 分支走同一「停机语义」)。
///
/// **进程级全局态 (第六轮 P3 已升级为防御实现)**:`run_with_config` 入口处
/// 复位 `REQUESTED`,停机分支仅在 `REQUESTED == true` 时生效 —— 残留 permit
/// 触发分支但标志已复位时被忽略。同一进程内可串行多次 `run()`(先前的
/// 「嵌入者不得二次调用」契约作废)。
static REQUESTED: AtomicBool = AtomicBool::new(false);
static REQUEST_NOTIFY: std::sync::OnceLock<tokio::sync::Notify> = std::sync::OnceLock::new();

/// 请求 agent 优雅停机 (线程安全;幂等;返回 true = 本次为首个停机请求)。
pub fn request_shutdown() -> bool {
    let first = !REQUESTED.swap(true, Ordering::SeqCst);
    // notify_one 在没有等待者时保留 permit，也允许后续 run 再次被请求停机。
    REQUEST_NOTIFY
        .get_or_init(tokio::sync::Notify::new)
        .notify_one();
    first
}

/// agent 主入口：装配全部任务并阻塞至停机信号 (SIGTERM/SIGINT)。
/// 停机顺序见模块 doc「停机语义」(v0.1:子进程默认保留运行，不按 stop_grace 清理)。
///
/// F14:配置路径未知时按设计默认 `./service-watch.toml` 作为热重载目标;
/// monitor-agent 显式 `--config` 时请走 [`run_with_config`] 传入实际路径。
pub async fn run(cfg: Config) -> Result<StopReason> {
    run_with_config(cfg, None).await
}

/// 带**启动配置路径**的装配入口 (F14 配置热重载缺省目标)。
/// SIGHUP 与 `POST /v1/reload`(path 缺省) 都重读本路径指定的文件。
pub async fn run_with_config(cfg: Config, config_path: Option<PathBuf>) -> Result<StopReason> {
    // 第六轮 P3 防御：上次 run 遗留的 REQUESTED 置位在本次启动时复位 ——
    // 库形态嵌入者可串行多次 run;stale permit 由停机分支守卫吸收。
    REQUESTED.store(false, Ordering::SeqCst);
    cfg.validate()
        .map_err(|errors| crate::error::CoreError::Config(errors.join("\n")))?;
    let config_path = config_path.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
    // 启动前完成所有可能失败的系统订阅/监听，避免已有恢复任务后才发现端口冲突。
    #[cfg(feature = "control-http")]
    let control_listener = if cfg.control.enabled {
        Some(tokio::net::TcpListener::bind(cfg.control.listen).await?)
    } else {
        None
    };
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    #[cfg(unix)]
    let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;

    tracing::info!(
        config_path = %config_path.display(),
        data_dir = %cfg.agent.data_dir.display(),
        checks = cfg.checks.len(),
        services = cfg.services.len(),
        watches = cfg.watches.len(),
        collect_interval = ?cfg.collect.interval,
        "monitor agent starting"
    );

    // ---- 准备:data_dir + journal 重放与可持久化预检 ----
    std::fs::create_dir_all(&cfg.agent.data_dir)?;
    let journal_path = cfg.agent.data_dir.join("journal.jsonl");
    let replayed = journal::replay_checked(&journal_path)?;
    let open_path = journal_path.clone();
    let journal_file = tokio::task::spawn_blocking(move || journal::open_writer(&open_path))
        .await
        .map_err(|e| {
            crate::error::CoreError::Recover(format!("journal init task failed: {e}"))
        })??;
    if !replayed.fatal.is_empty() {
        tracing::warn!(
            targets = ?replayed.fatal,
            "journal replay: fatal latches from previous run; explicit reset required"
        );
    }
    let started_at = SystemTime::now();

    // ---- 通道与共享状态 ----
    let counters = Arc::new(AlertCounters::default());
    let queue = AlertQueue::new(counters.clone());
    // watched PID 回传:eval 每轮全量写入，collect 线程每轮读取 (部分刷新协议)。
    let watched: Arc<std::sync::Mutex<Vec<crate::model::ids::ProcessIdentity>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let (journal_tx, journal_rx) = journal::channel(JOURNAL_CHANNEL_CAP);
    let journal_counters = journal_tx.counters().clone();
    let (cmd_tx, cmd_rx) = mpsc::channel(CONTROL_CHANNEL_CAP);
    let (status_tx, status_rx) = watch::channel(Arc::new(StatusReport {
        started_at,
        ready: false,
        targets: Vec::new(),
    }));
    let (metrics_tx, metrics_rx) = watch::channel(Arc::new(MetricsReport::default()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let ready_flag = Arc::new(AtomicBool::new(false));
    let shutdown_flag = Arc::new(AtomicBool::new(false));

    // ---- collect 线程 (阻塞;快照经 watch 广播)----
    let (snapshot_rx, collect_thread) =
        collect_loop(cfg.clone(), shutdown_flag.clone(), watched.clone());

    // ---- journal 写任务 ----
    let journal_task = tokio::spawn(journal::run_tracked_writer(
        journal_file,
        journal_path,
        journal_rx,
        journal_tx.counters().clone(),
    ));

    // ---- F12 spool 目录 (投递失败的事件落盘，at-least-once)----
    let spool_dir = cfg.agent.data_dir.join("spool");
    std::fs::create_dir_all(&spool_dir).ok();
    // ---- 指标周期推送 (ROADMAP「指标周期推送」;单次启动，失败即放弃下轮覆盖)----
    #[cfg(feature = "http-client")]
    if cfg.metrics.enabled
        && let Some(metrics_url) = cfg.metrics.url.clone()
    {
        tokio::spawn(crate::alert::metrics_push::run_metrics_pusher(
            metrics_url,
            cfg.metrics.interval,
            cfg.metrics.timeout,
            metrics_rx.clone(),
            shutdown_rx.clone(),
            Some(journal_tx.clone()),
        ));
        tracing::info!(interval = ?cfg.metrics.interval, "metrics push enabled");
    }

    #[cfg(not(feature = "http-client"))]
    if cfg.metrics.enabled {
        // P3(第七轮):与 webhook 的 parity 提示 —— feature 关闭时推送静默失效
        tracing::warn!(
            "metrics push enabled but http-client feature disabled; periodic push inactive"
        );
    }

    // ---- deadman 心跳 (单次启动，与分发任务解耦)----
    #[cfg(feature = "notify-webhook")]
    let deadman_task = cfg.alert.deadman_url.clone().map(|deadman_url| {
        tokio::spawn(crate::alert::run_deadman(deadman_url, shutdown_rx.clone()))
    });

    // ---- alert 分发任务 (panic 监督：最多重建 3 次;正常 shutdown 退出即停)----
    let notifier = build_notifier(&cfg.alert);
    let mut dispatch_supervisor = tokio::spawn({
        let queue = queue.clone();
        let notifier = notifier.clone();
        let alert_cfg = cfg.alert.clone();
        let counters = counters.clone();
        let journal_tx = journal_tx.clone();
        let shutdown_rx = shutdown_rx.clone();
        async move {
            for attempt in 0..=3u32 {
                let mut children = tokio::task::JoinSet::new();
                children.spawn(crate::alert::run_dispatcher(
                    queue.clone(),
                    notifier.clone(),
                    alert_cfg.clone(),
                    counters.clone(),
                    shutdown_rx.clone(),
                    Some(spool_dir.clone()),
                ));
                // JoinSet 的 Drop 会 abort 在途子任务，监督任务取消可传递到分发器。
                match children
                    .join_next()
                    .await
                    .expect("dispatcher child registered")
                {
                    Ok(()) => break, // shutdown 正常退出
                    Err(e) if e.is_panic() && attempt < 3 => {
                        tracing::error!(attempt, summary = %e, "alert dispatcher panicked; restarting");
                    }
                    Err(e) => {
                        tracing::error!(summary = %e, "alert dispatcher terminated; giving up");
                        // 断流可见性 (评审 P2):give-up 写 journal 审计，
                        // 运维可经 journal/外部监控发现告警管道已停。
                        let _ = journal_tx.try_send(JournalEvent::Audit {
                            at: SystemTime::now(),
                            message: "alert dispatcher gave up after repeated panics; alert stream halted".into(),
                        });
                        break;
                    }
                }
            }
        }
    });

    // ---- eval(单写者):注册 supervise 目标后移交进程管理器 ----
    let mut pm = crate::recover::process::ProcessManager::new();
    for svc in &cfg.services {
        pm.register(svc.clone());
    }
    let handles = crate::eval::SchedulerHandles {
        snapshot_rx: snapshot_rx.clone(),
        alert: queue.clone(),
        watched,
        journal_tx: journal_tx.clone(),
        control_rx: cmd_rx,
        status_tx,
        metrics_tx,
        pm,
        replayed,
        config_path,
    };
    let mut eval_task = tokio::spawn(crate::eval::run(cfg.clone(), handles));

    // 正常停机取消句柄。
    let eval_abort = eval_task.abort_handle();
    let dispatch_abort = dispatch_supervisor.abort_handle();

    // ---- 控制平面 ----
    // shared 无条件构建 (字段均为普通类型，与 feature 无关);control 未启用或
    // 无 control-http feature 时立即 drop(eval 侧 send 失败仅 debug 日志)。
    type ControlTask = tokio::task::JoinHandle<Result<()>>;
    let shared = Arc::new(crate::control::ControlShared {
        started_at,
        ready: ready_flag.clone(),
        status_rx,
        metrics_rx,
        snapshot_rx,
        cmd_tx: cmd_tx.clone(),
        alert_counters: counters.clone(),
    });
    // 原件立即释放:control 启用时通道由 shared 持有;未启用则通道关闭，
    // eval 的命令 select 分支停用 (recv 返回 None，不算停机信号)。
    // F14:SIGHUP 热重载专用出口 (仅 unix;上面保留的 clone 由运行循环持有，
    // 控制平面未启用时 SIGHUP 重载依然可用)。
    #[cfg(unix)]
    let hup_tx = cmd_tx.clone();
    drop(cmd_tx);

    let control_task: Option<ControlTask> = if cfg.control.enabled {
        #[cfg(feature = "control-http")]
        {
            let listener = control_listener.expect("enabled control listener prebound");
            let handle = tokio::spawn(crate::control::bind_and_serve(listener, shared));
            tracing::info!(listen = %cfg.control.listen, "control plane listening");
            Some(handle)
        }
        #[cfg(not(feature = "control-http"))]
        {
            tracing::warn!("control.enabled = true 但未启用 control-http feature;控制平面不启动");
            drop(shared);
            None
        }
    } else {
        tracing::info!("control plane disabled (control.enabled = false)");
        drop(shared);
        None
    };

    // ---- 就绪：全部任务已起 (降级采集不影响 ready,DESIGN §9.2)----
    ready_flag.store(true, Ordering::Relaxed);
    tracing::info!("all tasks started; agent ready");

    // ---- 运行：等待停机信号 (eval 意外退出同样触发停机兜底)----
    let reason;
    #[cfg(unix)]
    {
        // F14:SIGHUP → 配置热重载 (经保留的命令通道转发，eval 单写者内应用;
        // 与 SIGTERM/SIGINT 不同，不触发停机)。
        let stop_reason = tokio::select! {
            reason = async {
                loop {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => { tracing::info!(signal = "SIGINT", "shutdown signal received"); break StopReason::Signal }
                        _ = sigterm.recv() => { tracing::info!(signal = "SIGTERM", "shutdown signal received"); break StopReason::Signal }
                        _ = sighup.recv() => {
                            tracing::info!(signal = "SIGHUP", "reload signal received");
                            match hup_tx.try_send(ControlCommand::Reload { path: None }) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    tracing::warn!("reload command queue full; SIGHUP reload dropped");
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    tracing::debug!("command channel closed; SIGHUP reload dropped");
                                }
                            }
                        }
                        _ = REQUEST_NOTIFY.get_or_init(tokio::sync::Notify::new).notified(),
                            if REQUESTED.load(Ordering::SeqCst) => {
                            tracing::info!("in-process shutdown requested (F15 service stop)");
                            break StopReason::Clean
                        }
                    }
                }
            } => reason,
            _ = journal_counters.failure_notified() => {
                tracing::error!("journal persistence failed; stopping recovery runtime");
                StopReason::PersistenceFailed
            }
            res = &mut eval_task => {
                tracing::error!(result = ?res, "eval task exited unexpectedly; shutting down");
                StopReason::EvalExited
            }
        };
        reason = stop_reason;
    }
    #[cfg(not(unix))]
    {
        let stop_reason = tokio::select! {
            reason = async {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!(signal = "SIGINT", "shutdown signal received");
                        StopReason::Signal
                    }
                    _ = REQUEST_NOTIFY.get_or_init(tokio::sync::Notify::new).notified(),
                        if REQUESTED.load(Ordering::SeqCst) => {
                        tracing::info!("in-process shutdown requested (F15 service stop)");
                        StopReason::Clean
                    }
                }
            } => reason,
            _ = journal_counters.failure_notified() => {
                tracing::error!("journal persistence failed; stopping recovery runtime");
                StopReason::PersistenceFailed
            }
            res = &mut eval_task => {
                tracing::error!(result = ?res, "eval task exited unexpectedly; shutting down");
                StopReason::EvalExited
            }
        };
        reason = stop_reason;
    }

    // ---- 停机序列 (模块 doc「停机语义」)----
    // 1) 协作停机：采集线程下一轮退出;alert 分发进入「排空队列后退出」模式。
    shutdown_flag.store(true, Ordering::Relaxed);
    let _ = shutdown_tx.send(true);

    // 2) 单写者状态机与控制平面无协作停机契约 → abort(eval / control 模块 doc)。
    eval_abort.abort();
    if reason != StopReason::EvalExited {
        let _ = (&mut eval_task).await;
    }
    if let Some(handle) = control_task {
        handle.abort();
        let _ = handle.await;
    }
    #[cfg(feature = "notify-webhook")]
    if let Some(handle) = deadman_task {
        handle.abort();
        let _ = handle.await;
    }
    if let Some(thread) = collect_thread {
        // 唤醒 interval 等待，由采集线程自己观察 shutdown 收尾。
        thread.thread().unpark();
    }

    // 3) alert:分发任务在窗口内尽力排空 (重试循环不响应 shutdown —— 最后尽力
    //    投递)。窗口超时：剩余队列事件**计入 dropped 并写 journal 审计**
    //    (并发评审 P1:静默丢弃无审计 —— 单笔投递最坏 ~47s vs 500ms 窗口)。
    if tokio::time::timeout(DISPATCH_DRAIN_WINDOW, &mut dispatch_supervisor)
        .await
        .is_err()
    {
        // 评审 P2:库形态下 run() 返回后，在途重试最长 ~47s 仍在调用方 runtime 上
        // 跑且不响应 shutdown —— 窗口过期即 abort，尾巴有界。
        let abandoned = queue.len();
        if abandoned > 0 {
            counters
                .dropped
                .fetch_add(abandoned as u64, Ordering::Relaxed);
            let _ = journal_tx.try_send(JournalEvent::Audit {
                at: SystemTime::now(),
                message: format!(
                    "shutdown drain window elapsed: {abandoned} queued alert(s) abandoned"
                ),
            });
            tracing::warn!(
                abandoned,
                "alert dispatcher drain window elapsed; queued events counted as dropped"
            );
        } else {
            tracing::warn!("alert dispatcher drain window elapsed; in-flight delivery abandoned");
        }
        dispatch_abort.abort();
        let _ = dispatch_supervisor.await;
    }

    // 4) journal flush:eval abort 已释放其 journal_tx;关闭运行时侧 sender,
    //    writer 排空既有事件后退出 (100ms 窗口)。
    drop(journal_tx);
    match tokio::time::timeout(JOURNAL_FLUSH_WINDOW, journal_task).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => tracing::warn!(source = %e, "journal writer exited with error"),
        Ok(Err(join_err)) => tracing::warn!(source = %join_err, "journal writer join failed"),
        Err(_) => tracing::warn!("journal flush window elapsed; pending events may be lost"),
    }

    let reason = if journal_counters.has_failed() {
        StopReason::PersistenceFailed
    } else {
        reason
    };
    tracing::info!(?reason, "shutdown complete");
    Ok(reason)
}

/// 采集线程入口 (阻塞;由 runtime 用 std::thread 启动)。
/// 循环:collect → watch 发送 → 按 collect.interval 睡眠;shutdown 置位即退出。
///
/// watched 集合由 eval 每轮全量写入 (`SchedulerHandles.watched`,与采集线程
/// 共享 `Arc<Mutex<Vec<_>>>`;锁内仅 Vec 克隆，中毒按空集恢复),本线程每轮读取
/// 后做部分刷新 —— 进程级 liveness 因此有快照证据，配合亲生退出事件双通道判定。
pub(crate) fn collect_loop(
    cfg: Config,
    shutdown: Arc<AtomicBool>,
    watched: Arc<std::sync::Mutex<Vec<crate::model::ids::ProcessIdentity>>>,
) -> (
    watch::Receiver<Arc<CollectOutput>>,
    Option<std::thread::JoinHandle<()>>,
) {
    // 初始快照：采集线程首采前，消费端看到的是诚实降级值 (不伪造 0)。
    let initial: Arc<CollectOutput> =
        Arc::new(StubCollector::failed("collect thread warming up".into()).collect(&[]));
    let (tx, rx) = watch::channel(initial);
    let interval = cfg.collect.interval;

    let spawned = std::thread::Builder::new()
        .name("monitor-collect".into())
        .spawn(move || {
            let mut collector = safe_create_collector();
            let mut retry_at = None;
            std::thread::park_timeout(Duration::from_millis(250));
            loop {
                if shutdown.load(Ordering::Relaxed) {
                    tracing::debug!("collect thread exiting (shutdown observed)");
                    break;
                }
                let watched_list = match watched.lock() {
                    Ok(guard) => guard.clone(),
                    Err(poisoned) => poisoned.into_inner().clone(),
                };
                let out = Arc::new(collect_with_retry(
                    &mut collector,
                    &mut retry_at,
                    &watched_list,
                    Instant::now(),
                    safe_create_collector,
                ));
                // 无接收者 (eval 已终止) 时发送失败 —— 忽略，线程由 shutdown 收尾。
                let _ = tx.send(out);
                std::thread::park_timeout(interval);
            }
        });
    match spawned {
        Ok(thread) => (rx, Some(thread)),
        Err(e) => {
            tracing::error!(source = %e, "collect thread spawn failed; snapshots unavailable");
            (rx, None)
        }
    }
}

const COLLECTOR_RETRY_INTERVAL: Duration = Duration::from_secs(30);

fn safe_create_collector() -> Box<dyn Collector> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(create_collector)).unwrap_or_else(|_| {
        tracing::error!("collector init panicked; starting from stub");
        Box::new(StubCollector::failed("collector init panicked".into()))
    })
}

fn collect_with_retry(
    collector: &mut Box<dyn Collector>,
    retry_at: &mut Option<Instant>,
    watched: &[crate::model::ids::ProcessIdentity],
    now: Instant,
    mut factory: impl FnMut() -> Box<dyn Collector>,
) -> CollectOutput {
    if retry_at.is_some_and(|deadline| now >= deadline) {
        *collector = factory();
        *retry_at = None;
    }
    let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| collector.collect(watched)));
    match result {
        Ok(output) => {
            let init_failed = output.hardware.degradations.iter().any(|reason| {
                matches!(
                    reason,
                    crate::model::snapshot::Degradation::CollectorInit { .. }
                )
            });
            if init_failed {
                retry_at.get_or_insert(now + COLLECTOR_RETRY_INTERVAL);
            } else {
                *retry_at = None;
            }
            output
        }
        Err(_) => {
            tracing::error!("collector panicked; publishing degradation and scheduling init retry");
            *collector = Box::new(StubCollector::failed("collector panicked".into()));
            *retry_at = Some(now + COLLECTOR_RETRY_INTERVAL);
            collector.collect(watched)
        }
    }
}

/// dump-snapshot 的可序列化外形 (`CollectOutput` 本身未实现 Serialize;
/// `HardwareSnapshot` / `ProcessSnapshot` 均已实现，借引用零拷贝复用)。
#[derive(serde::Serialize)]
struct SnapshotDump<'a> {
    hardware: &'a crate::model::snapshot::HardwareSnapshot,
    processes: &'a [crate::model::snapshot::ProcessSnapshot],
}

/// 单次采集并以 pretty JSON 返回 (monitor-agent `--dump-snapshot` 与
/// monitorctl `dump-snapshot` 共用实现;watched 为空 → 仅系统级数据)。
/// 同步阻塞调用 (内部 sysinfo 刷新),不得置于 async 上下文热路径。
pub fn collect_snapshot_json(watched: &[crate::model::ids::ProcessIdentity]) -> Result<String> {
    let mut collector = create_collector();
    let out = collector.collect(watched);
    let dump = SnapshotDump {
        hardware: &out.hardware,
        processes: &out.processes,
    };
    Ok(serde_json::to_string_pretty(&dump)?)
}

/// no-op 通知器：未配置 `webhook_url`(或构建失败) 时的降级实现 ——
/// 事件仍入 journal 与结构化日志;分发任务视其为投递成功 (alerts_sent 计数
/// 随之增长，如实反映「分发管道工作正常」而非「外部系统已收到」)。
struct NoopNotifier;

#[async_trait::async_trait]
impl Notifier for NoopNotifier {
    async fn notify(&self, _event: &AlertEvent) -> Result<()> {
        Ok(())
    }
}

/// 通知器装配:`webhook_url` 有值 → `WebhookNotifier`(payload 格式由
/// `webhook_format` 决定，DESIGN §7.3);无值或构建失败 → [`NoopNotifier`]。
fn build_notifier(cfg: &AlertCfg) -> Arc<dyn Notifier> {
    #[cfg(feature = "notify-webhook")]
    if let Some(url) = cfg.webhook_url.clone() {
        return match crate::alert::webhook::WebhookNotifier::new(
            url,
            cfg.webhook_format,
            cfg.timeout,
        ) {
            Ok(n) => {
                tracing::info!(
                    format = ?cfg.webhook_format,
                    timeout = ?cfg.timeout,
                    "webhook notifier enabled"
                );
                Arc::new(n)
            }
            Err(e) => {
                tracing::warn!(source = %e, "webhook notifier build failed; alerts 仅记录不投递");
                Arc::new(NoopNotifier)
            }
        };
    }
    #[cfg(not(feature = "notify-webhook"))]
    if cfg.webhook_url.is_some() {
        tracing::warn!(
            "webhook_url configured but notify-webhook feature disabled; alerts 仅记录不投递"
        );
    }
    Arc::new(NoopNotifier)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct HealthyCollector;

    impl Collector for HealthyCollector {
        fn collect(&mut self, watched: &[crate::model::ids::ProcessIdentity]) -> CollectOutput {
            let mut output = StubCollector::default().collect(watched);
            output.hardware.degradations.clear();
            output
        }
    }

    #[test]
    fn degraded_collector_retries_after_bounded_delay() {
        let now = Instant::now();
        let mut collector: Box<dyn Collector> = Box::new(StubCollector::default());
        let mut retry_at = None;
        let mut attempts = 0;
        let output = collect_with_retry(&mut collector, &mut retry_at, &[], now, || {
            attempts += 1;
            Box::new(HealthyCollector)
        });
        assert!(!output.hardware.degradations.is_empty());
        assert_eq!(attempts, 0);
        let due = retry_at.unwrap();
        let output = collect_with_retry(&mut collector, &mut retry_at, &[], due, || {
            attempts += 1;
            Box::new(HealthyCollector)
        });
        assert!(output.hardware.degradations.is_empty());
        assert_eq!(attempts, 1);
    }

    #[test]
    fn healthy_collector_is_retained_with_long_collection_interval() {
        let now = Instant::now();
        let mut collector: Box<dyn Collector> = Box::new(HealthyCollector);
        let mut retry_at = None;
        for sampled_at in [now, now + COLLECTOR_RETRY_INTERVAL * 2] {
            let output = collect_with_retry(&mut collector, &mut retry_at, &[], sampled_at, || {
                panic!("健康采集器不能因为采样间隔长被重建")
            });
            assert!(output.hardware.degradations.is_empty());
            assert!(retry_at.is_none());
        }
    }

    #[tokio::test]
    async fn invalid_programmatic_config_fails_before_creating_data_dir() {
        let mut cfg = Config::from_toml_str("").unwrap();
        cfg.agent.data_dir =
            std::env::temp_dir().join(format!("sw-invalid-config-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cfg.agent.data_dir);
        cfg.collect.interval = Duration::ZERO;
        let data_dir = cfg.agent.data_dir.clone();
        assert!(matches!(
            run(cfg).await,
            Err(crate::error::CoreError::Config(_))
        ));
        assert!(!data_dir.exists());
    }

    #[cfg(feature = "control-http")]
    #[tokio::test]
    async fn occupied_control_port_fails_before_creating_data_dir() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut cfg = Config::from_toml_str("").unwrap();
        cfg.control.listen = listener.local_addr().unwrap();
        cfg.agent.data_dir =
            std::env::temp_dir().join(format!("sw-bind-failure-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cfg.agent.data_dir);
        let data_dir = cfg.agent.data_dir.clone();
        assert!(run(cfg).await.is_err());
        assert!(!data_dir.exists());
    }
}
