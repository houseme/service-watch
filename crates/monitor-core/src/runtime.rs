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
///
/// 唤醒语义约束 (第十七轮 P0 教训):消费端 select 的 `notified()` 分支**不得
/// 加 `if REQUESTED` 前置守卫** —— 分支禁用时 `notified()` 未注册等待者，
/// `notify_one` 存的 permit 不会唤醒任务，无其他唤醒源即死锁 (SCM stop 实弹)。
/// 正确写法：分支恒启用，体内查 `REQUESTED` 过滤上一轮 run 残留的 permit。
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
    // F32:TLS 证书/私钥与端口 bind 同列「装配期早失败」点 —— 坏 PEM/缺失
    // 文件与端口占用一样在 data_dir 创建前暴露 (Err 早退 → main 退出码 2),
    // 不进主循环。加载结果 (None = 纯 HTTP 现状) 移交控制面 spawn 分支;错误
    // 只含路径与解析原因，私钥内容不落日志 (control::load_tls_config)。
    // P3(第十一轮验证):与 bind 同受 control.enabled 门控 —— 控制面关闭时
    // TLS 配置不会被消费，坏材料不应阻止 agent 启动。
    // F38:client_ca_cert(Some = mTLS 客户端证书校验) 随同一装配点加载，
    // CA 缺失/坏 PEM 同列早退 (verifier 内嵌于 RustlsConfig，bind_and_serve
    // 签名不变)。
    #[cfg(feature = "control-http")]
    let control_tls = if cfg.control.enabled {
        crate::control::load_tls_config(
            cfg.control.tls_cert.as_deref(),
            cfg.control.tls_key.as_deref(),
            cfg.control.client_ca_cert.as_deref(),
        )
        .await?
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
    // F24:轮转阈值从 `agent.journal_max_size` 传入 (0 = 完全禁用轮转);
    // 确认式持久化语义不变 —— journal_tx.counters() 链路未动。
    let journal_task = tokio::spawn(journal::run_tracked_writer_with_limit(
        journal_file,
        journal_path,
        journal_rx,
        journal_tx.counters().clone(),
        cfg.agent.journal_max_size,
    ));

    // ---- F12 spool 目录 (投递失败的事件落盘，at-least-once)----
    let spool_dir = cfg.agent.data_dir.join("spool");
    std::fs::create_dir_all(&spool_dir).ok();
    // ---- 指标周期推送 (ROADMAP「指标周期推送」;单次启动，失败即放弃下轮覆盖)----
    #[cfg(feature = "http-client")]
    let metrics_push_task = if cfg.metrics.enabled
        && let Some(metrics_url) = cfg.metrics.url.clone()
    {
        // P2(第八轮):JoinHandle 必须保留 —— pusher 持有 journal_tx 克隆且
        // POST 在途不观察 shutdown，停机须显式 abort+await 释放 sender,
        // 否则 journal flush 窗口必然超时、队列内事件丢失。
        let handle = tokio::spawn(crate::alert::metrics_push::run_metrics_pusher(
            metrics_url,
            cfg.metrics.interval,
            cfg.metrics.timeout,
            // F34:推送签名密钥 (None = 不签名);密钥值不进日志，只报布尔。
            cfg.metrics.secret.clone(),
            metrics_rx.clone(),
            shutdown_rx.clone(),
            Some(journal_tx.clone()),
        ));
        tracing::info!(
            interval = ?cfg.metrics.interval,
            signed = cfg.metrics.secret.is_some(),
            "metrics push enabled"
        );
        Some(handle)
    } else {
        None
    };

    #[cfg(not(feature = "http-client"))]
    if cfg.metrics.enabled {
        // P3(第七轮):与 webhook 的 parity 提示 —— feature 关闭时推送静默失效
        tracing::warn!(
            "metrics push enabled but http-client feature disabled; periodic push inactive"
        );
    }

    // ---- F39 指标历史持久化 (独立 writer 任务;消费 metrics watch)----
    // 与 metrics push 无 feature 耦合 (纯文件出口，不依赖 reqwest)——任何
    // feature 组合下 history_path 配置即生效。只持有 metrics watch 接收端，
    // 不持 journal_tx;JoinHandle 保留，停机在 metrics pusher 收口之后
    // abort+await (与第十二轮 metrics pusher 收口同款)。
    let metrics_history_task = if let Some(history_path) = cfg.metrics.history_path.clone() {
        let handle = tokio::spawn(crate::metrics_history::run_history_writer(
            history_path.clone(),
            metrics_rx.clone(),
        ));
        tracing::info!(
            history_path = %history_path.display(),
            rotate_at = crate::metrics_history::HISTORY_ROTATE_THRESHOLD,
            "metrics history persistence enabled"
        );
        Some(handle)
    } else {
        None
    };

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
    // F33:生效配置视图通道 —— 初始快照由启动 cfg 构造;eval reload 成功后经
    // tx 推送新值，控制面经 rx 只读透传 (reload 后即新值)。
    let (config_view_tx, config_view_rx) =
        tokio::sync::watch::channel(Arc::new(serde_json::json!(cfg.redacted())));
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
        config_view_tx,
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
        token: cfg.control.token.clone(), // F23:可选 Bearer 令牌传入控制平面
        // F28:/v1/journal 尾部查询 —— journal_path 本体已移交 journal writer,
        // 此处按同一规则重算 (data_dir 固定约定，无二次漂移面)。
        journal_path: cfg.agent.data_dir.join("journal.jsonl"),
        // F33:/v1/config 生效配置视图 —— watch 接收端，初始值 = 启动快照，
        // eval reload 成功后刷新 (redacted() 占位后序列化，明文密钥不出 agent;
        // 控制平面只读透传，字段 doc)。
        config_view: config_view_rx,
    });
    // 原件立即释放:control 启用时通道由 shared 持有;未启用则通道关闭，
    // eval 的命令 select 分支停用 (recv 返回 None，不算停机信号);config_view
    // 出口例外 —— 无接收端时 send 静默丢弃 (F33 handle doc 注明),非 debug 日志。
    // F14:SIGHUP 热重载专用出口 (仅 unix;上面保留的 clone 由运行循环持有，
    // 控制平面未启用时 SIGHUP 重载依然可用)。
    #[cfg(unix)]
    let hup_tx = cmd_tx.clone();
    drop(cmd_tx);

    let control_task: Option<ControlTask> = if cfg.control.enabled {
        #[cfg(feature = "control-http")]
        {
            let listener = control_listener.expect("enabled control listener prebound");
            // F32:control_tls = None → 纯 HTTP(现状);Some(config) → 同一预
            // bind 监听器换 rustls acceptor，纯 HTTPS。接受循环仍由 runtime
            // abort 停机 (control 模块 doc「停机与 409 语义」+「F32 控制面 HTTPS」)。
            let handle = tokio::spawn(crate::control::bind_and_serve(
                listener,
                shared,
                control_tls,
            ));
            tracing::info!(
                listen = %cfg.control.listen,
                tls = cfg.control.tls_cert.is_some(),
                "control plane listening"
            );
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
                        // F15 停机请求:notified **不带 if 守卫**(第十七轮 P0:
                        // 守卫在 REQUESTED=false 时禁用分支 → notified 未注册
                        // 等待者 → notify_one 只存 permit 不唤醒，Windows 服务
                        // 形态无其他唤醒源 → select 永不重 poll,SCM stop 死锁
                        // 于 STOP_PENDING)。permit 恒注册，唤醒后在体内查标志
                        // 过滤残留 permit(入口已复位)。
                        _ = REQUEST_NOTIFY.get_or_init(tokio::sync::Notify::new).notified() => {
                            if REQUESTED.load(Ordering::SeqCst) {
                                tracing::info!("in-process shutdown requested (F15 service stop)");
                                break StopReason::Clean
                            }
                            // 上一次 run 残留的 permit:标志已复位，吞掉继续等待。
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
                loop {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {
                            tracing::info!(signal = "SIGINT", "shutdown signal received");
                            break StopReason::Signal
                        }
                        // F15 停机请求：同 unix 分支 —— notified 不带 if 守卫
                        // (第十七轮 P0:守卫禁用分支导致 permit 不唤醒，SCM
                        // stop 死锁于 STOP_PENDING;F36 首跑实弹证据),体内查
                        // 标志过滤残留 permit。
                        _ = REQUEST_NOTIFY.get_or_init(tokio::sync::Notify::new).notified() => {
                            if REQUESTED.load(Ordering::SeqCst) {
                                tracing::info!("in-process shutdown requested (F15 service stop)");
                                break StopReason::Clean
                            }
                            // 上一次 run 残留的 permit:标志已复位，吞掉继续等待。
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

    // P2(第八轮):指标推送任务先收口 (abort+await 释放其 journal_tx 克隆),
    // 再关运行时侧 sender —— 否则下方 drop 不闭合通道，flush 窗口必超时。
    #[cfg(feature = "http-client")]
    if let Some(handle) = metrics_push_task {
        handle.abort();
        let _ = handle.await;
    }

    // F39:指标历史 writer 同款收口 (abort+await) —— 位置在 metrics pusher
    // 之后、journal drop 之前。writer 只持 metrics watch 接收端，无 journal_tx
    // 需要释放;abort 只能落在 `changed().await` 点 (写入段无 await，不会截断
    // 半行)，最后在途样本可能不落盘 —— 观测数据出口，可接受。
    if let Some(handle) = metrics_history_task {
        handle.abort();
        let _ = handle.await;
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
/// F26:`routes` 非空时以默认通道为兜底构建 [`RoutingNotifier`]——每条 route
/// 一个独立 `WebhookNotifier`(url/format 取 route 配置);投递语义 (超时/退避/
/// 队列/spool) 由分发层统一处理，路由只选目标。构建失败的 route 记 WARN 后
/// 跳过：该 pattern 永不命中，对应 target 回落默认通道 (与 webhook_url 构建
/// 失败降级 Noop 口径一致)。`webhook_url` 为空且无匹配时回落 Noop = 事件
/// 不外投 (与现状「空 = 不投递」一致，事件仍入 journal/日志)。
/// F29:默认通道用 `webhook_secret`,每条 route 用 [`effective_route_secret`]
/// (路由级覆盖全局;两者皆 None 不签名)。密钥值不进日志。
/// F37:默认通道传全局 `[alert] headers`,每条 route 用 [`effective_route_headers`]
/// (全局为底，路由级同名键覆盖)。头值可能是敏感 token，不进日志。
fn build_notifier(cfg: &AlertCfg) -> Arc<dyn Notifier> {
    let default = build_default_notifier(cfg);
    if cfg.routes.is_empty() {
        return default; // 空 routes = 行为与现状完全一致
    }
    #[cfg(feature = "notify-webhook")]
    let routes: Vec<(String, Arc<dyn Notifier>)> = cfg
        .routes
        .iter()
        .filter_map(|route| {
            let secret =
                effective_route_secret(route.secret.as_deref(), cfg.webhook_secret.as_deref());
            let headers = effective_route_headers(&route.headers, &cfg.headers);
            match crate::alert::webhook::WebhookNotifier::new(
                route.url.clone(),
                route.format,
                cfg.timeout,
                secret,
                headers,
            ) {
                Ok(n) => Some((route.r#match.clone(), Arc::new(n) as Arc<dyn Notifier>)),
                Err(e) => {
                    tracing::warn!(
                        r#match = %route.r#match,
                        source = %e,
                        "route webhook build failed; 该路由 target 回落默认通道"
                    );
                    None
                }
            }
        })
        .collect();
    #[cfg(not(feature = "notify-webhook"))]
    let routes: Vec<(String, Arc<dyn Notifier>)> = {
        tracing::warn!(
            "alert.routes configured but notify-webhook feature disabled; routes inactive (全部回落默认通道)"
        );
        Vec::new()
    };
    // P3(第九轮验证):同时上报配置条数与存活条数 —— 构建失败跳过的 route
    // 会让两者不一致，只报 len 会误导运维。
    tracing::info!(
        routes = routes.len(),
        configured = cfg.routes.len(),
        "alert target routing enabled"
    );
    Arc::new(crate::alert::RoutingNotifier::new(default, routes))
}

/// F29:路由级签名密钥解析 —— `route.secret` **覆盖**全局 `webhook_secret`,
/// 两者皆 None = 不签名 (纯函数，便于单测)。
#[cfg(feature = "notify-webhook")]
fn effective_route_secret(route: Option<&str>, global: Option<&str>) -> Option<String> {
    route.or(global).map(str::to_string)
}

/// F37:路由级投递头合并 —— 全局 `[alert] headers` 为底，`route.headers` 同名键
/// **覆盖**全局，非同名键共存 (BTreeMap 顺序插入即覆盖;纯函数，便于单测)。
/// 头值可能是敏感 token:只随 notifier 进请求头，不落日志/审计。
#[cfg(feature = "notify-webhook")]
fn effective_route_headers(
    route: &std::collections::BTreeMap<String, String>,
    global: &std::collections::BTreeMap<String, String>,
) -> std::collections::BTreeMap<String, String> {
    let mut merged = global.clone();
    for (name, value) in route {
        merged.insert(name.clone(), value.clone());
    }
    merged
}

/// 默认 (兜底) 通道装配：既有 `webhook_url` 逻辑，行为不变。
fn build_default_notifier(cfg: &AlertCfg) -> Arc<dyn Notifier> {
    #[cfg(feature = "notify-webhook")]
    if let Some(url) = cfg.webhook_url.clone() {
        return match crate::alert::webhook::WebhookNotifier::new(
            url,
            cfg.webhook_format,
            cfg.timeout,
            cfg.webhook_secret.clone(),
            cfg.headers.clone(),
        ) {
            Ok(n) => {
                // F29:signed 只报「是否启用签名」,密钥值不落日志。
                // F37:headers 数量只报规模，头名/头值都不落日志。
                tracing::info!(
                    format = ?cfg.webhook_format,
                    timeout = ?cfg.timeout,
                    signed = cfg.webhook_secret.is_some(),
                    headers = cfg.headers.len(),
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

    // ---- F29:路由级签名密钥解析 ----

    /// `route.secret` 覆盖全局 `webhook_secret`;两者皆 None 不签名。
    #[cfg(feature = "notify-webhook")]
    #[test]
    fn route_secret_overrides_global_webhook_secret() {
        assert_eq!(
            effective_route_secret(Some("route"), Some("global")),
            Some("route".into()),
            "路由级覆盖全局"
        );
        assert_eq!(
            effective_route_secret(None, Some("global")),
            Some("global".into()),
            "路由未配置回落全局"
        );
        assert_eq!(
            effective_route_secret(Some("route"), None),
            Some("route".into()),
            "仅路由配置 = 只对该路由签名"
        );
        assert_eq!(
            effective_route_secret(None, None),
            None,
            "两者皆 None = 不签名"
        );
    }

    // ---- F37:路由级投递头合并 ----

    /// 合并语义:`route.headers` 同名键**覆盖**全局，非同名键共存;空表各自成立。
    #[cfg(feature = "notify-webhook")]
    #[test]
    fn route_headers_override_global_and_nonconflicting_coexist() {
        let mut global = std::collections::BTreeMap::new();
        global.insert("X-Auth-Token".to_string(), "global-secret".to_string());
        global.insert("X-Global-Only".to_string(), "g".to_string());
        let mut route = std::collections::BTreeMap::new();
        route.insert("X-Auth-Token".to_string(), "route-secret".to_string());
        route.insert("X-Route-Only".to_string(), "r".to_string());

        let merged = effective_route_headers(&route, &global);
        assert_eq!(merged.len(), 3, "同名键覆盖后 3 键共存");
        assert_eq!(
            merged.get("X-Auth-Token").map(String::as_str),
            Some("route-secret"),
            "路由级同名键覆盖全局"
        );
        assert_eq!(merged.get("X-Global-Only").map(String::as_str), Some("g"));
        assert_eq!(merged.get("X-Route-Only").map(String::as_str), Some("r"));

        // 路由未配 headers → 原样继承全局。
        let inherited = effective_route_headers(&std::collections::BTreeMap::new(), &global);
        assert_eq!(inherited, global, "空路由表 = 原样继承全局");
        // 全局未配 → 只有路由级。
        let mut only_route = std::collections::BTreeMap::new();
        only_route.insert("X-Auth-Token".to_string(), "route-secret".to_string());
        let solo = effective_route_headers(&only_route, &std::collections::BTreeMap::new());
        assert_eq!(solo, only_route, "空全局表 = 仅路由级");
    }

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
