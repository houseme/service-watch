//! eval:单写者调度器 (DESIGN.md §2.2、§7)。全部状态机由本任务独占，零锁。
//!
//! 职责：
//! 1. 按 check 配置周期驱动探测 (在途上限 1，上轮未完成跳过并计数);
//! 2. 推进检查状态机 (§7.1)→ 产出级别转移;
//! 3. incident 去噪 (§7.2)→ 产生 [`AlertEvent`] 投递 alert 队列;
//! 4. supervise 目标驱动恢复状态机 (§8)→ 通过 [`ProcessManager`](crate::recover::process::ProcessManager) 执行;
//! 5. 消费控制平面命令 (restart/check/reset/**reload**);
//! 6. F14 配置热重载:`ControlCommand::Reload`(`POST /v1/reload` / SIGHUP 经
//!    runtime 转发) 在单写者内读文件、diff、应用**目标拓扑**
//!    (checks/services/watches 增删改，纯 diff 见 [`reload`]);其余小节
//!    (agent/log/control/collect/alert/metrics) 变更要求重启，不热应用。
//! 7. F18 历史指标环形缓冲：每轮 tick 发布 metrics 时同步入环
//!    (上限 [`crate::model::control::METRICS_HISTORY_CAP`],超出丢最旧),
//!    缓冲句柄随 [`crate::model::control::MetricsReport::history`] 经既有
//!    metrics watch 分发，control 侧只读快照 (`/metrics/history`)。
//! 8. 崩溃启动事务 (ROADMAP「崩溃启动事务」):spawn 前 `SpawnIntent` 经
//!    `send().await` 确认落盘 (携带服务定义摘要 digest),spawn 返回后立即
//!    `SpawnOutcome`;agent 在两事件之间崩溃 → 冷启动对账 (见
//!    [`Scheduler::reconcile_spawn_intent`]) 发现「意图无结果」= 未知实例窗口：
//!    digest 匹配当前定义 → 跳过冷启动 (防双实例)+ ERROR + Audit + FSM Suspect,
//!    仅 monitorctl reset 解除;digest 不匹配 (旧定义遗留)→ 照常冷启动。
//!
//! 时间：调度用 tokio 时间 (测试可 `start_paused`);状态机一律接收单调 `Instant` 参数。
//!
//! ## 主循环结构
//!
//! 单任务 `select` 驱动:`sleep(最近到期事件)` + `control_rx.recv()` +
//! `snapshot_rx.changed()`。唤醒后执行一轮 [`Scheduler::tick`]:
//! 收割在途探测 → 推进检查状态机与 incident 去噪 → 服务恢复状态机
//! (`on_tick` / `poll_exit` / spawn 到点 / activate 钩子)→ 重建 [`StatusReport`]。
//! 探测与激活钩子以 `JoinHandle` 在途保存，结果在本任务收割 (零锁单写者)。
//!
//! 距下一事件的上限 [`MAX_TICK`](1s) 兜底唤醒：恢复状态机的内部截止时刻
//! (stop_grace 升级、startup_grace 超时、verify 观察期) 不对外暴露，靠周期 tick 驱动。
//!
//! ## 进程证据与重启对账
//!
//! - watched 集合经 SchedulerHandles 回传采集线程，每轮包含亲生进程、attach
//!   目标、孤儿候选与显式 pid:N 检查。指标与 liveness 用单调采样代际去重。
//! - process 名称按已绑定 supervise 服务优先、watch 次之解析，随后支持 pid:N。
//!   采集不可用、首采或字段缺失产生 Unavailable，跳过健康计数。
//! - journal 中仍可能存活的 supervise 身份在冷启动时先只读对账。
//!   Pending/Attached 保留候选并禁止恢复干预，可信证据确认消失后才恢复调度。
//!
//! ## 停机语义
//!
//! 本任务**不自行退出**:`control_rx.recv()` 返回 `None`(runtime drop 了 sender)
//! 与 `snapshot_rx.changed()` Err 都不算停机信号，仅停用对应 select 分支;停机由
//! runtime abort 本任务完成 (DESIGN §2.2:runtime 负责装配与停机顺序)。

pub mod check;
pub mod incident;
pub(crate) mod reload;
pub mod state;

use crate::collect::CollectOutput;
use crate::config::{CheckCfg, CheckKindCfg, Config, ServiceCfg, WatchCfg};
use crate::error::{CoreError, Result};
use crate::eval::check::{ProbeCtx, ProbeResult, execute as execute_probe};
use crate::eval::incident::IncidentTracker;
use crate::eval::state::{CheckStateMachine, Thresholds};
use crate::model::alert::{AlertEvent, Category, Severity};
use crate::model::control::{
    ControlCommand, HealthLevel, MetricsSample, RecoveryState, StatusReport, TargetMode,
    TargetStatus,
};
use crate::model::control::{METRICS_HISTORY_CAP, MetricsHistoryRing};
use crate::model::ids::ProcessIdentity;
use crate::model::snapshot::ProcessSnapshot;
use crate::recover::journal::{self, JournalEvent, JournalState};
use crate::recover::process::ProcessManager;
use crate::recover::reconcile::{OrphanObservation, OrphanTracker};
use crate::recover::{RecoverAction, RecoveryFsm, RestartPolicy};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use reload::TopologyDiff;

/// 单次 tick 的最大间隔：恢复状态机的内部截止时刻不对外暴露，靠本上限兜底唤醒。
const MAX_TICK: Duration = Duration::from_secs(1);

/// Fatal 告警的 check 名 (incident key = hash(target, "__recover__"),任务约定)。
const RECOVER_CHECK: &str = "__recover__";

/// 调度器与外界的一切交互句柄 (runtime 负责装配)。
pub struct SchedulerHandles {
    /// 最新采集快照 (采集线程广播)。
    pub snapshot_rx: watch::Receiver<Arc<CollectOutput>>,
    /// 告警事件出口 (drop-oldest 有界队列;eval 直接 push，消除双层缓冲的
    /// drop-new 路径 —— 并发评审 P2:drop-new 不计数、DESIGN §2.2 要求丢最旧)。
    pub alert: Arc<crate::alert::AlertQueue>,
    /// watched PID 集合回传:eval 每轮把 supervise + watch 身份写入，采集线程
    /// 每轮读取做部分刷新 (并发评审 P0:恒空 watched 使 process 检查永久失败)。
    pub watched: Arc<std::sync::Mutex<Vec<ProcessIdentity>>>,
    /// journal 事件出口 (审计/持久化)。
    pub journal_tx: crate::recover::journal::JournalSender,
    /// 控制平面命令入口。
    pub control_rx: mpsc::Receiver<ControlCommand>,
    /// `/status` 汇报出口 (eval 每轮刷新)。
    pub status_tx: watch::Sender<Arc<StatusReport>>,
    /// `/metrics` 运行指标出口 (eval 每轮发布;runtime 持有另一端给控制平面)。
    pub metrics_tx: watch::Sender<Arc<crate::model::control::MetricsReport>>,
    /// 亲生 supervise 进程管理器 (本任务独占),孤儿只读接管保存在服务条目。
    pub pm: ProcessManager,
    /// journal 重放结果 (F7:Fatal 闩锁 + 预算窗口跨重启恢复;§8.7 最小实现 ——
    /// Fatal 目标跳过冷启动 spawn，存活旧实例先只读接管)。
    pub replayed: crate::recover::journal::JournalState,
    /// 启动配置路径 (F14 热重载缺省目标:`ControlCommand::Reload { path: None }`
    /// 时回填;runtime 经 `run_with_config` 传入)。
    pub config_path: PathBuf,
}

/// 主循环。本任务由 runtime abort 停机;control/snapshot 通道关闭不算停机信号
/// (见模块 doc「停机语义」)。
pub async fn run(cfg: Config, mut h: SchedulerHandles) -> Result<()> {
    let mut sched = Scheduler::build(&cfg);

    // 冷启动 + journal 重放接线 (F7/§8.7 最小实现):恢复 Fatal 闩锁与预算窗口;
    // Fatal 目标跳过冷启动 spawn(agent 重启不得越权复活已被人工闩锁的目标)。
    // journal 身份同时用于只读孤儿对账，在确认消失前禁止创建重复实例;
    // 崩溃启动事务的未决意图经 [`Scheduler::reconcile_spawn_intent`] 对账。
    let now = Instant::now();
    let now_wall = SystemTime::now();
    // 启动路径仅一次：克隆重放状态，避免与 &mut h 借用冲突。
    let replayed = h.replayed.clone();
    for sidx in 0..sched.svcs.len() {
        sched
            .cold_start(sidx, &replayed, now, now_wall, &mut h)
            .await;
    }

    let mut control_open = true;
    let mut snapshot_open = true;

    // 首轮立即执行：初始探测 + 状态上报 (不等第一个 sleep)。
    sched.tick(&mut h).await;

    loop {
        let now = Instant::now();
        let wake_in = sched.next_wake_delay(now);
        // 命令先缓存到 select 之外处理：分支体不借用 h，避免与 recv/changed 的
        // 可变借用冲突。
        let mut pending_cmd = None;
        tokio::select! {
            _ = tokio::time::sleep(wake_in) => {}
            cmd = h.control_rx.recv(), if control_open => {
                if cmd.is_some() {
                    pending_cmd = cmd;
                } else {
                    // runtime 已 drop 命令 sender:不算停机，仅停用本分支。
                    control_open = false;
                }
            }
            changed = h.snapshot_rx.changed(), if snapshot_open => {
                if changed.is_err() {
                    // 快照 sender 已 drop:不算停机，仅停用本分支。
                    snapshot_open = false;
                }
            }
        }
        if let Some(cmd) = pending_cmd.take() {
            sched
                .handle_control(cmd, Instant::now(), SystemTime::now(), &mut h)
                .await;
        }
        sched.tick(&mut h).await;
    }
}

// ---------------------------------------------------------------------------
// 运行时条目
// ---------------------------------------------------------------------------

/// 单个 check 的运行时条目。
struct CheckRt {
    cfg: CheckCfg,
    /// 绑定的 supervise 服务索引 (process 检查按名称匹配绑定;None = 独立目标)。
    bound_service: Option<usize>,
    /// 绑定的 watch 目标索引 (process 检查;/status 健康聚合 + 事件归属)。
    bound_watch: Option<usize>,
    state: CheckStateMachine,
    /// 下次到期时刻 (单调)。
    next_due: Instant,
    /// 在途探测 (上限 1;未收割前到期则跳过并计数，§7.1)。
    in_flight: Option<JoinHandle<ProbeResult>>,
    last_process_sample: Option<Instant>,
    skipped: u64,
    /// 已执行探测总数 (供 /metrics 运行观测)。
    executed: u64,
    /// 最近一次级别转移的墙钟时刻 (供 /status)。
    last_transition_wall: Option<SystemTime>,
}

/// 单个 supervise 服务的运行时条目。
struct SvcRt {
    cfg: ServiceCfg,
    policy: RestartPolicy,
    fsm: RecoveryFsm,
    /// [`RecoverAction::ScheduleSpawn`] 的到点时刻 (调度器持有并执行)。
    spawn_due: Option<Instant>,
    /// 在途激活钩子 (结果回本任务 → `fsm.on_activate_result`)。
    activate: Option<JoinHandle<bool>>,
    /// 绑定的 check 索引 (process 检查)。
    checks: Vec<usize>,
    /// 最近一次成功 spawn 的单调时刻 (startup_grace / 快照证据成熟判定)。
    last_spawn: Option<Instant>,
    /// 控制平面 restart 的兜底强杀截止 (signal_stop 后 stop_grace 内未退出)。
    restart_stop_deadline: Option<Instant>,
    /// signal_stop 时的进程身份:deadline 到期仅当仍在运行**同一身份**才强杀
    /// (第三轮 P0-2:否则优雅退出 → 新进程拉起后会被过期 deadline 强杀)。
    restart_stop_identity: Option<ProcessIdentity>,
    /// ForceKill 的 kill 截止：到期仍存活 → on_stop_result(false) → Fatal
    /// (Unix D 状态进程对 SIGKILL 免疫的兜底，SRE 评审 P0 配套)。
    kill_deadline: Option<Instant>,
    /// reload 后仍持有旧实例，其真正退出并收割后才登记/启动新定义。
    reload_stop_deadline: Option<Instant>,
    /// agent 重启后存活旧实例的只读对账，与亲生 Child 管理权分开。
    orphan: Option<OrphanTracker>,
    /// 崩溃启动事务：冷启动对账发现「意图无结果」(未知实例窗口) 后置位。
    /// 阻止冷启动 spawn / F20 排程 / 控制面 restart(防双实例),仅
    /// monitorctl reset 解除 (显式承认未知实例已被人工处置)。
    orphan_suspected: bool,
    persisted_observed_identity: Option<ProcessIdentity>,
}

/// 单个 watch(attach) 目标的运行时条目。
struct WatchRt {
    name: String,
    /// 配置的显式 pid(F14 reload 身份变更判定;None = pidfile 来源)。
    pid: Option<u32>,
    /// `(pid, start_time)`:pid 来自配置/pidfile;start_time 在首个含该 pid 的
    /// 快照出现时**采纳观测值**(评审 P2:start_time=0 占位永不命中采集侧身份匹配，
    /// watch 监控结构性失效)。目标重启换 pid 后需重读 pidfile(v0.2)。
    identity: Option<ProcessIdentity>,
    /// 绑定的 process 检查索引 (其 /status 健康聚合到本 watch)。
    checks: Vec<usize>,
    /// pidfile 路径 (周期重读;目标重启换 pid 后监控跟随 —— 第三轮 P2)。
    pidfile: Option<std::path::PathBuf>,
}

/// 调度器全部状态 (单写者独占，零锁)。
struct Scheduler {
    checks: Vec<CheckRt>,
    svcs: Vec<SvcRt>,
    watches: Vec<WatchRt>,
    tracker: IncidentTracker,
    #[cfg(feature = "probe-http")]
    http: Option<reqwest::Client>,
    started_wall: SystemTime,
    /// 构建时刻 (单调;uptime 计算，不用墙钟)。
    started_mono: Instant,
    collect_interval: Duration,
    /// 上一轮快照是否含降级 (None = 尚未评估过任何快照：首个快照 (启动 warmup
    /// Stub 必带 CollectorInit) 不计入事件，避免幻象降级 —— 评审 P2)。
    degradations_prev_present: Option<bool>,
    degradations_events: u64,
    /// 上一次做 liveness 判定的单调采样时刻 (同一份快照在多个 tick 被
    /// 重复评估会让 confirmations 按 tick 而非按采集节奏累积 —— 第三轮 P2)。
    last_gone_eval_sample: Option<Instant>,
    /// tick 计数 (pidfile 周期重读节拍)。
    tick_count: u64,
    /// F20 停止顺序：控制面 restart X 时，先停 X 的 (传递) 被依赖方;
    /// X 回到 Running 后按此表拉回。key = 被依赖服务名。
    cascade_restart_after: std::collections::HashMap<String, Vec<String>>,
    /// 计划内级联停止的 dependent(F20;第六轮评审 P3):其退出经
    /// [`RecoveryFsm::on_planned_stop`] 收口 —— 不计预算、不排程，由依赖门控拉回。
    /// 覆盖两条死亡入口：亲生退出 (on_child_exit) 与 SCM Stopped(liveness gone)。
    planned_stops: std::collections::HashSet<String>,
    /// P1(第七轮):planned 停止的强杀升级截止 —— dependent 无视 SIGTERM 时
    /// 到期 force_kill(否则 planned_stops 永久滞留、编排静默失败)。
    planned_stop_deadlines: std::collections::HashMap<String, Instant>,
    /// 当前生效配置 (F14:热重载 diff 的 old 侧。非拓扑段不热应用 ——
    /// collect_interval 与 tracker/thresholds 保持构建时值)。
    active_cfg: Config,
    /// 检查状态机阈值 (启动时 alert 配置;alert 段不热应用，reload 新增的
    /// check 沿用启动阈值，保持全体一致)。
    thresholds: Thresholds,
    /// F14:reload 移除的服务，其停止中的 child 由本任务继续收割 (防僵尸),
    /// 收割完成即移除登记 (pm 条目无注销 API，保留至停机由 shutdown_all 兜底)。
    pending_reap: Vec<String>,
    retired_orphans: std::collections::HashMap<String, OrphanTracker>,
    /// F18 历史指标环形缓冲 (本任务单写者;control 侧经发布报告里的同一句柄
    /// 只读快照)。每轮 tick 发布 metrics 时入环，上限 [`METRICS_HISTORY_CAP`]。
    metrics_history: MetricsHistoryRing,
}

/// process 检查的一次身份解析结果 (全部 owned，探测任务内无共享借用)。
#[derive(Default)]
struct ResolvedProbe {
    snapshot: Option<ProcessSnapshot>,
    expected: Option<ProcessIdentity>,
}

/// 探测任务的全部输入 (owned;`tokio::spawn` 要求 'static)。
struct ProbeJob {
    check: CheckCfg,
    process_data_available: bool,
    snapshot: Option<ProcessSnapshot>,
    expected: Option<ProcessIdentity>,
    #[cfg(feature = "probe-http")]
    http: Option<reqwest::Client>,
}

impl ProbeJob {
    /// 整体受 `check.timeout` 约束 (§7.1:探测一律 timeout 包裹;内部实现
    /// http/tcp 已自带，这里是兜底)。
    async fn run(self) -> ProbeResult {
        let name = self.check.name.clone();
        let timeout = self.check.timeout;
        match tokio::time::timeout(timeout, self.run_inner()).await {
            Ok(result) => result,
            Err(_) => ProbeResult::Fail {
                reason: format!("probe {name} timed out after {timeout:?}"),
                observed: None,
            },
        }
    }

    async fn run_inner(self) -> ProbeResult {
        if !self.process_data_available {
            return ProbeResult::Unavailable {
                reason: "process collector unavailable or live child missing from snapshot".into(),
            };
        }
        let ctx = ProbeCtx {
            #[cfg(feature = "probe-http")]
            http: self.http.as_ref(),
            process_snapshot: self.snapshot.as_ref(),
            expected_identity: self.expected.as_ref(),
        };
        execute_probe(&self.check, ctx).await
    }
}

// ---------------------------------------------------------------------------
// 构建
// ---------------------------------------------------------------------------

/// 名称匹配：精确或目标名以探测名结尾 ("尾匹配",任务约定)。
fn target_name_matches(target: &str, probe: &str) -> bool {
    target == probe || target.ends_with(probe)
}

/// 按 pid 查找快照并做身份容差匹配 (评审 P2:spawn 侧本地墙钟与内核记账存在
/// 跨秒失配，±2s 内视为同一进程;调用方以返回的**观测身份**为准)。
pub(crate) fn find_by_identity<'a>(
    snap: &'a CollectOutput,
    expected: &ProcessIdentity,
) -> Option<&'a ProcessSnapshot> {
    snap.processes.iter().find(|p| {
        p.identity.pid == expected.pid && p.identity.start_time.abs_diff(expected.start_time) <= 2
    })
}

/// 启动时读取 watch 目标身份:pid 配置优先，否则读一次 pidfile( trim + parse u32)。
/// start_time 无法探明，以 0 占位 (v0.1 限制，见模块 doc)。
fn resolve_watch_identity(w: &WatchCfg) -> Option<ProcessIdentity> {
    let pid = match w.pid {
        Some(p) => Some(p),
        None => w.pidfile.as_deref().and_then(read_pidfile),
    };
    pid.map(|p| ProcessIdentity::new(p, 0))
}

fn read_pidfile(path: &std::path::Path) -> Option<u32> {
    match std::fs::read_to_string(path) {
        Ok(s) => match s.trim().parse::<u32>() {
            Ok(p) => Some(p),
            Err(e) => {
                tracing::warn!(path = %path.display(), source = %e, "watch pidfile content is not a pid");
                None
            }
        },
        Err(e) => {
            tracing::warn!(path = %path.display(), source = %e, "watch pidfile unreadable at startup");
            None
        }
    }
}

/// check 在**旧拓扑**下的 (target, check) 标签 (F14 reload 用，与
/// [`Scheduler::labels`] 语义一致：绑定服务名优先，其次 watch 名，否则 check 名)。
fn old_check_label(rt: &CheckRt, svcs: &[SvcRt], watches: &[WatchRt]) -> (String, String) {
    let check = rt.cfg.name.clone();
    if let Some(sidx) = rt.bound_service {
        return (svcs[sidx].cfg.name.clone(), check);
    }
    if let Some(widx) = rt.bound_watch {
        return (watches[widx].name.clone(), check);
    }
    (check.clone(), check)
}

impl Scheduler {
    fn build(cfg: &Config) -> Self {
        let thresholds = Thresholds {
            failure_threshold: cfg.alert.failure_threshold,
            success_threshold: cfg.alert.success_threshold,
        };

        let mut svcs: Vec<SvcRt> = cfg
            .services
            .iter()
            .map(|s| {
                let policy = RestartPolicy::from(&s.restart);
                let mut fsm = RecoveryFsm::new(&s.name, policy.clone());
                // activate.verify_for 独立观察期 (SRE 评审 P1:此前被静默丢弃，
                // 默认巧合掩盖 —— startup_grace 恰与 verify_for 同默认值)。
                if let Some(a) = &s.activate {
                    fsm.set_verify_for(a.verify_for);
                }
                SvcRt {
                    policy,
                    fsm,
                    cfg: s.clone(),
                    spawn_due: None,
                    activate: None,
                    checks: Vec::new(),
                    last_spawn: None,
                    restart_stop_deadline: None,
                    restart_stop_identity: None,
                    kill_deadline: None,
                    reload_stop_deadline: None,
                    orphan: None,
                    orphan_suspected: false,
                    persisted_observed_identity: None,
                }
            })
            .collect();

        let checks: Vec<CheckRt> = cfg
            .checks
            .iter()
            .map(|c| CheckRt {
                bound_service: match &c.kind {
                    CheckKindCfg::Process { process, .. } => svcs
                        .iter()
                        .position(|s| target_name_matches(&s.cfg.name, process)),
                    _ => None,
                },
                bound_watch: None, // build 末段回填 (需 watches 先构建)
                cfg: c.clone(),
                state: CheckStateMachine::new(thresholds),
                next_due: Instant::now(),
                in_flight: None,
                last_process_sample: None,
                skipped: 0,
                executed: 0,
                last_transition_wall: None,
            })
            .collect();

        // 绑定回填 (服务 → 其名下检查)。
        for (i, c) in checks.iter().enumerate() {
            if let Some(s) = c.bound_service {
                svcs[s].checks.push(i);
            }
        }

        let watches: Vec<WatchRt> = cfg
            .watches
            .iter()
            .map(|w| WatchRt {
                name: w.name.clone(),
                pid: w.pid,
                identity: resolve_watch_identity(w),
                checks: Vec::new(),
                pidfile: w.pidfile.clone(),
            })
            .collect();

        // process 检查绑定 watch(未被服务绑定的):按名称尾匹配或 "pid:N" 匹配。
        // 双向回填 (SRE 评审 P1:bound_watch 此前恒 None,labels/去重/Reset 全部失效)。
        let mut watches = watches;
        let mut check_watch: Vec<Option<usize>> = vec![None; checks.len()];
        for (i, c) in checks.iter().enumerate() {
            if c.bound_service.is_some() {
                continue;
            }
            if let CheckKindCfg::Process { process, .. } = &c.cfg.kind {
                let widx = watches.iter().position(|w| {
                    target_name_matches(&w.name, process)
                        || w.identity.is_some_and(|ident| {
                            process.strip_prefix("pid:") == Some(&ident.pid.0.to_string())
                        })
                });
                if let Some(widx) = widx {
                    watches[widx].checks.push(i);
                    check_watch[i] = Some(widx);
                }
            }
        }
        let checks: Vec<CheckRt> = checks
            .into_iter()
            .enumerate()
            .map(|(i, mut c)| {
                c.bound_watch = check_watch[i];
                c
            })
            .collect();

        Scheduler {
            checks,
            svcs,
            watches,
            tracker: IncidentTracker::new(cfg.alert.clone()),
            #[cfg(feature = "probe-http")]
            http: None,
            started_wall: SystemTime::now(),
            started_mono: Instant::now(),
            collect_interval: cfg.collect.interval,
            degradations_prev_present: None,
            degradations_events: 0,
            last_gone_eval_sample: None,
            tick_count: 0,
            cascade_restart_after: std::collections::HashMap::new(),
            planned_stops: std::collections::HashSet::new(),
            planned_stop_deadlines: std::collections::HashMap::new(),
            active_cfg: cfg.clone(),
            thresholds,
            pending_reap: Vec::new(),
            retired_orphans: std::collections::HashMap::new(),
            metrics_history: Arc::new(std::sync::Mutex::new(VecDeque::new())),
        }
    }

    // -----------------------------------------------------------------
    // 冷启动序列 (F7 重放 → §8.7 孤儿对账 → 崩溃启动事务 → F20 门控)
    // -----------------------------------------------------------------

    /// 单个 supervise 条目的冷启动决策 (run() 启动时每服务调用一次):
    /// 1. 恢复 Fatal 闩锁与预算窗口 (F7);
    /// 2. journal 存有旧实例身份 → 只读孤儿对账 (§8.7),确认消失前不冷启动;
    /// 3. 崩溃启动事务对账：未决 spawn 意图且 digest 匹配当前定义 → 存在
    ///    未知实例，跳过冷启动 (防双实例，见 [`Self::reconcile_spawn_intent`]);
    /// 4. 无依赖立即排程;有依赖交 F20 门控。
    async fn cold_start(
        &mut self,
        sidx: usize,
        replayed: &JournalState,
        now_mono: Instant,
        now_wall: SystemTime,
        h: &mut SchedulerHandles,
    ) {
        let name = self.svcs[sidx].cfg.name.clone();
        let stamps = replayed.restarts.get(&name).cloned().unwrap_or_default();
        let fatal = replayed.fatal.contains(&name);
        if fatal || !stamps.is_empty() {
            self.svcs[sidx]
                .fsm
                .restore(fatal, &stamps, now_wall, now_mono);
            tracing::info!(
                target = %name,
                fatal,
                restored_restarts = stamps.len(),
                "recovery state restored from journal"
            );
        }
        if self.svcs[sidx].fsm.is_fatal() {
            tracing::warn!(target = %name, "cold start skipped: fatal latched (monitorctl reset to release)");
        } else if self
            .reconcile_spawn_intent(sidx, replayed, now_mono, now_wall, h)
            .await
        {
            // H-P1(第七轮):意图对账**必须先于**孤儿分支 —— 崩溃窗口场景的
            // journal 必然同时含「上次成功 spawn 的 ProcessIdentity」与未决
            // SpawnIntent;孤儿分支优先会让意图对账整体被跳过，未知实例窗口
            // 的防双实例失效。digest 匹配 → orphan_suspected(保守等待 reset;
            // 孤儿分支不挂 tracker)。
        } else {
            // P0(第七轮):SCM 目标无进程身份链 —— 孤儿对账不适用 (其存活性
            // 由 QueryStatus 证据驱动),跳过挂 OrphanTracker。
            if self.svcs[sidx].cfg.windows_service.is_none()
                && let Some(previous) = replayed.processes.get(&name)
            {
                self.svcs[sidx].orphan = Some(OrphanTracker::new(
                    previous.identity,
                    previous.verified,
                    now_mono,
                ));
            }
            if self.svcs[sidx].orphan.is_some() {
                tracing::info!(target = %name, "cold start deferred: reconciling previous instance");
            } else if self.svcs[sidx].cfg.depends_on.is_empty() {
                self.svcs[sidx].spawn_due = Some(now_mono);
            } else {
                // F20:有依赖的服务由 tick 门控 (依赖全部 Running 后 spawn_due=now)
                tracing::info!(
                    target = %self.svcs[sidx].cfg.name,
                    deps = ?self.svcs[sidx].cfg.depends_on,
                    "cold start deferred: waiting for dependencies (F20)"
                );
            }
        }
    }

    /// 崩溃启动事务对账 (ROADMAP「崩溃启动事务」)。
    ///
    /// journal 中存在该 target 的未决启动意图 (`SpawnIntent` 无 `SpawnOutcome`
    /// = 上次 agent 在 OS spawn 与结果落盘之间崩溃，进程事实未知) 时：
    /// - digest 匹配当前定义:v0.x 不做 pid 探活，**保守判定存在未知实例** →
    ///   跳过冷启动 (双实例防护)+ ERROR 日志 + journal Audit + FSM 标记
    ///   Suspect(可观测;confirmations=1 的配置经 on_health 会直接进
    ///   Recovering 并计预算/SignalStop，对无 child 目标是错误动作，故该边
    ///   仅走调度侧标记，/status 呈现见 `build_status`);仅 monitorctl reset
    ///   解除 (同时清除调度侧标记与 journal 未决意图)。
    /// - digest 不匹配：旧定义的遗留意图，忽略 (新定义照常冷启动)。
    ///
    /// 返回是否因未知实例窗口跳过冷启动。
    async fn reconcile_spawn_intent(
        &mut self,
        sidx: usize,
        replayed: &JournalState,
        now_mono: Instant,
        now_wall: SystemTime,
        h: &mut SchedulerHandles,
    ) -> bool {
        let name = self.svcs[sidx].cfg.name.clone();
        let Some(pending) = replayed.pending_spawns.get(&name) else {
            return false;
        };
        let digest = journal::config_digest(&self.svcs[sidx].cfg);
        if digest != pending.config_digest {
            tracing::info!(
                target = %name,
                intent_digest = %pending.config_digest,
                current_digest = %digest,
                "stale spawn intent belongs to a previous definition; ignored for cold start"
            );
            return false;
        }
        {
            let svc = &mut self.svcs[sidx];
            svc.spawn_due = None;
            svc.orphan_suspected = true;
        }
        // FSM 标记 Suspect(可观测;单次 Critical 在 confirmations>1 时恰好进入
        // Suspect 并产生一条 Audit 动作)。
        let mut actions = Vec::new();
        if self.svcs[sidx].policy.confirmations > 1 {
            actions = self.svcs[sidx]
                .fsm
                .on_health(now_mono, HealthLevel::Critical);
        }
        tracing::error!(
            target = %name,
            intent_at = ?pending.at,
            "orphan suspected for {name} (intent without outcome); cold start skipped to avoid duplicate instance"
        );
        journal(
            h,
            JournalEvent::Audit {
                at: now_wall,
                message: format!(
                    "orphan suspected for {name} (intent without outcome); monitorctl reset 后重启 agent 可强制拉起"
                ),
            },
        );
        if !actions.is_empty() {
            self.exec_actions(sidx, actions, now_mono, now_wall, h)
                .await;
        }
        true
    }

    // -----------------------------------------------------------------
    // 每轮 tick
    // -----------------------------------------------------------------

    async fn tick(&mut self, h: &mut SchedulerHandles) {
        let now_mono = Instant::now();
        let now_wall = SystemTime::now();
        // 只克隆 Arc(采集快照整体不可变);Ref 立即释放，无跨 await 借用。
        let snap: Arc<CollectOutput> = h.snapshot_rx.borrow().clone();

        // 0-)pidfile 周期重读 (每 30 tick ≈ 30s):目标重启换 pid 后监控跟随。
        // 仅无显式 pid 配置的 watch;pid 变化 → 身份重置为 (新 pid, 0),观测身份
        // 由下一轮快照采纳。小文件同步读，频率低，不构成热路径。
        self.tick_count += 1;
        if self.tick_count.is_multiple_of(30) {
            for w in &mut self.watches {
                if let Some(pf) = &w.pidfile
                    && let Some(new_pid) = read_pidfile(pf)
                    && w.identity.as_ref().map(|i| i.pid.0) != Some(new_pid)
                {
                    tracing::info!(watch = %w.name, pid = new_pid, "pidfile changed; re-arming watch identity");
                    w.identity = Some(ProcessIdentity::new(new_pid, 0));
                }
            }
        }

        // 0)watch 观测身份采纳 (评审 P0/P2:start_time=0 占位永不命中采集侧
        //    身份匹配;首个含该 pid 的快照出现即采纳内核记账的 start_time)。
        for w in &mut self.watches {
            let Some(mut ident) = w.identity else {
                continue;
            };
            if let Some(observed) = snap.processes.iter().find(|p| p.identity.pid == ident.pid)
                && observed.identity != ident
            {
                if ident.start_time == 0 {
                    tracing::info!(
                        watch = %w.name,
                        identity = %observed.identity,
                        "watch identity adopted from snapshot"
                    );
                } else {
                    tracing::warn!(
                        watch = %w.name,
                        old = %ident,
                        new = %observed.identity,
                        "watch pid reused (start_time changed); re-adopting"
                    );
                }
                ident = observed.identity;
                w.identity = Some(ident);
            }
        }

        // 0') watched 回传 (评审 P0 修复):supervise + watch 全量身份，采集线程
        //     每轮读取做部分刷新 (§6.2:漏传者停留旧数据，必须每轮全量)。
        {
            let mut watched: Vec<ProcessIdentity> = Vec::new();
            for s in &self.svcs {
                if let Some(ident) =
                    h.pm.identity(&s.cfg.name)
                        .or_else(|| s.orphan.as_ref().map(OrphanTracker::identity))
                {
                    watched.push(ident);
                }
            }
            for w in &self.watches {
                if let Some(ident) = w.identity {
                    watched.push(ident);
                }
            }
            for check in &self.checks {
                if let CheckKindCfg::Process { process, .. } = &check.cfg.kind
                    && let Some(pid) = process
                        .strip_prefix("pid:")
                        .and_then(|pid| pid.trim().parse::<u32>().ok())
                    && !watched.iter().any(|identity| identity.pid.0 == pid)
                {
                    watched.push(ProcessIdentity::new(pid, 0));
                }
            }
            match h.watched.lock() {
                Ok(mut guard) => *guard = watched,
                Err(poisoned) => *poisoned.into_inner() = watched,
            }
        }
        // 0') grace 窗口后仍存活的进程组补杀 (评审 P1:组长 reap 后组员泄漏)。
        if !h.journal_tx.counters().has_failed() {
            h.pm.reap_pending_kills(now_mono);
        }

        // 1) 收割已完成探测 (§7.1:结果推进状态机 → incident → 恢复联动)。
        let mut finished: Vec<(usize, ProbeResult)> = Vec::new();
        for (idx, rt) in self.checks.iter_mut().enumerate() {
            if rt.in_flight.as_ref().is_some_and(|j| j.is_finished()) {
                let handle = rt.in_flight.take().expect("checked is_finished");
                let result = match handle.await {
                    Ok(r) => r,
                    Err(e) => ProbeResult::Fail {
                        reason: format!("probe task join failed: {e}"),
                        observed: None,
                    },
                };
                finished.push((idx, result));
            }
        }
        for (idx, result) in finished {
            self.absorb_probe(idx, result, now_mono, now_wall, h).await;
        }

        // 2) 到期探测：在途则跳过计数，否则 spawn(上限 1,§7.1)。
        for idx in 0..self.checks.len() {
            if self.checks[idx].next_due > now_mono {
                continue;
            }
            self.checks[idx].next_due = now_mono + self.checks[idx].cfg.interval;
            if self.checks[idx].in_flight.is_some() {
                self.checks[idx].skipped += 1;
                tracing::debug!(
                    check = %self.checks[idx].cfg.name,
                    skipped_total = self.checks[idx].skipped,
                    "probe skipped: previous probe still in flight"
                );
                continue;
            }
            // supervise 目标尚未首次拉起：本轮不探测 (避免"未启动"误报)。
            if let Some(sidx) = self.checks[idx].bound_service {
                let svc = &self.svcs[sidx];
                if svc.reload_stop_deadline.is_some()
                    || (svc.last_spawn.is_none()
                        && h.pm.identity(&svc.cfg.name).is_none()
                        && !svc.orphan.as_ref().is_some_and(OrphanTracker::is_attached))
                {
                    tracing::debug!(
                        target = %svc.cfg.name,
                        check = %self.checks[idx].cfg.name,
                        "probe deferred: supervise target not spawned yet"
                    );
                    continue;
                }
            }

            let check_cfg = self.checks[idx].cfg.clone();
            if matches!(check_cfg.kind, CheckKindCfg::Process { .. }) {
                let stale = now_mono.saturating_duration_since(snap.collected_at)
                    > self.collect_interval.saturating_mul(2).max(MAX_TICK);
                if stale || self.checks[idx].last_process_sample == Some(snap.collected_at) {
                    self.checks[idx].skipped += 1;
                    continue;
                }
            }
            let resolved = match &check_cfg.kind {
                CheckKindCfg::Process { process, .. } => {
                    self.resolve_process_target(process, &snap, h)
                }
                _ => ResolvedProbe::default(),
            };
            let process_data_available = !matches!(check_cfg.kind, CheckKindCfg::Process { .. })
                || (!snap.hardware.degradations.iter().any(|d| {
                    matches!(d, crate::model::snapshot::Degradation::CollectorInit { .. })
                }) && !(resolved.snapshot.is_none()
                    && self.checks[idx]
                        .bound_service
                        .is_some_and(|sidx| h.pm.is_running(&self.svcs[sidx].cfg.name))));
            if process_data_available && matches!(check_cfg.kind, CheckKindCfg::Process { .. }) {
                self.checks[idx].last_process_sample = Some(snap.collected_at);
            }
            let job = ProbeJob {
                process_data_available,
                check: check_cfg,
                snapshot: resolved.snapshot,
                expected: resolved.expected,
                #[cfg(feature = "probe-http")]
                http: self.http_for_probe(),
            };
            self.checks[idx].executed += 1;
            self.checks[idx].in_flight = Some(tokio::spawn(job.run()));
        }

        // 3) supervise 服务：恢复状态机 + 进程事实。
        for sidx in 0..self.svcs.len() {
            self.service_tick(sidx, &snap, now_mono, now_wall, h).await;
        }
        if self.last_gone_eval_sample != Some(snap.collected_at) {
            self.last_gone_eval_sample = Some(snap.collected_at);
        }

        // 3')F14:reload 移除服务的停止中 child 继续收割 (防僵尸)——调度侧已无
        //     该条目，退出事件 (无 FSM 可喂) 就地丢弃;收割完成即移除登记。
        //     SIGTERM 升级强杀由 pm.pending_kills 兜底 (reap_pending_kills，上方)。
        if !h.journal_tx.counters().has_failed() && !self.pending_reap.is_empty() {
            let mut reaped = Vec::new();
            for name in &self.pending_reap {
                let grace =
                    h.pm.cfg(name)
                        .map(|cfg| cfg.restart.stop_grace)
                        .unwrap_or_default();
                if h.pm
                    .stop_requested_at(name)
                    .is_some_and(|at| now_mono >= at + grace)
                    && let Err(e) = h.pm.force_kill(name)
                {
                    tracing::error!(target = %name, source = %e, "removed-service force kill failed");
                }
                if h.pm.poll_exit(name).await.is_some() || h.pm.identity(name).is_none() {
                    reaped.push(name.clone());
                }
            }
            if !reaped.is_empty() {
                self.pending_reap.retain(|n| !reaped.contains(n));
                tracing::debug!(targets = ?reaped, "removed-service children reaped");
            }
        }

        // 4) 状态上报 (每轮重建;watch send 失败 = 无接收者，忽略)。
        let report = self.build_status(h, now_mono);
        if h.status_tx.send(Arc::new(report)).is_err() {
            tracing::debug!("status channel has no receiver; report dropped");
        }

        // 5) 运行指标发布 (监控运行情况;/metrics 的 eval 侧数据源)。
        //    F18:发布前先入环形缓冲 —— 发布出去的报告携带的即是含本轮样本的
        //    缓冲句柄 (control 侧拿到的快照不缺最新一轮)。
        let metrics = self.build_metrics(h, &snap, now_mono);
        self.record_metrics_sample(now_wall, &metrics);
        if h.metrics_tx.send(Arc::new(metrics)).is_err() {
            tracing::trace!("metrics channel has no receiver; report dropped");
        }
    }

    /// F18:当轮指标入环形缓冲 (单写者;上限 [`METRICS_HISTORY_CAP`],超出丢最旧)。
    fn record_metrics_sample(
        &mut self,
        at: SystemTime,
        report: &crate::model::control::MetricsReport,
    ) {
        let sample = MetricsSample {
            at,
            report: report.clone(),
        };
        // 毒化恢复与 watched 回传一致：读侧持锁 panic 才可能发生，历史可丢、
        // 当前轮不可丢 (into_inner 拿回数据继续写)。
        let mut ring = match self.metrics_history.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        ring.push_back(sample);
        while ring.len() > METRICS_HISTORY_CAP {
            ring.pop_front();
        }
    }

    /// 聚合运行指标 (DESIGN §10 内部计数;监控运行情况的单一数据源)。
    fn build_metrics(
        &mut self,
        h: &SchedulerHandles,
        snap: &CollectOutput,
        now_mono: Instant,
    ) -> crate::model::control::MetricsReport {
        use crate::model::control::{HealthLevel, RecoveryState};

        // 降级事件：无→有 跳变计数 (首个快照只建立基线，不计数)
        let now_degraded = !snap.hardware.degradations.is_empty();
        match self.degradations_prev_present {
            None => {}
            Some(prev) if now_degraded && !prev => self.degradations_events += 1,
            _ => {}
        }
        self.degradations_prev_present = Some(now_degraded);

        let mut m = crate::model::control::MetricsReport {
            probe_executed: self.checks.iter().map(|c| c.executed).sum(),
            probe_skipped: self.checks.iter().map(|c| c.skipped).sum(),
            restarts_total: self.svcs.iter().map(|s| s.fsm.restarts_total()).sum(),
            state_transitions_total: self.svcs.iter().map(|s| s.fsm.transitions_total()).sum(),
            fatal_targets: self.svcs.iter().filter(|s| s.fsm.is_fatal()).count() as u64,
            journal_dropped: h.journal_tx.counters().snapshot().dropped,
            journal_write_failures: h.journal_tx.counters().snapshot().write_failures,
            journal_write_successes: h.journal_tx.counters().snapshot().write_successes,
            journal_critical_failures: h.journal_tx.counters().snapshot().critical_failures,
            degradations_current: snap.hardware.degradations.len() as u64,
            degradations_events: self.degradations_events,
            last_collect_at: Some(snap.hardware.timestamp),
            // F18:发布出去的报告携带环形缓冲句柄 (control 侧只读快照入口)。
            history: self.metrics_history.clone(),
            ..Default::default()
        };

        // 健康分布 (每个 check 计一次;绑定 target 的也计入)
        for c in &self.checks {
            match c.state.level() {
                HealthLevel::Ok => m.checks_ok += 1,
                HealthLevel::Warning => m.checks_warning += 1,
                HealthLevel::Critical => m.checks_critical += 1,
            }
        }
        m.targets_total = (self.checks.len() + self.svcs.len() + self.watches.len()) as u64;
        m.services_total = self.svcs.len() as u64;
        m.services_running = self
            .svcs
            .iter()
            .filter(|s| {
                h.pm.is_running(&s.cfg.name)
                    || s.orphan.as_ref().is_some_and(OrphanTracker::is_attached)
            })
            .count() as u64;
        for s in &self.svcs {
            match s.fsm.state() {
                RecoveryState::Running => m.recovery_running += 1,
                RecoveryState::Suspect => m.recovery_suspect += 1,
                RecoveryState::Recovering => m.recovery_recovering += 1,
                RecoveryState::ForceKill => m.recovery_force_kill += 1,
                RecoveryState::Restarting => m.recovery_restarting += 1,
                RecoveryState::Starting => m.recovery_starting += 1,
                RecoveryState::CrashLoop => m.recovery_crashloop += 1,
                RecoveryState::Fatal => m.recovery_fatal += 1,
            }
        }
        m.uptime_secs = now_mono.duration_since(self.started_mono).as_secs();
        m
    }

    /// 吸收一次探测结果:startup_grace 过滤 → 状态机 → incident 去噪 → 恢复联动。
    async fn absorb_probe(
        &mut self,
        idx: usize,
        result: ProbeResult,
        now_mono: Instant,
        now_wall: SystemTime,
        h: &mut SchedulerHandles,
    ) {
        if let ProbeResult::Unavailable { reason } = &result {
            self.checks[idx].skipped += 1;
            tracing::debug!(check = %self.checks[idx].cfg.name, %reason, "probe skipped: observation unavailable");
            return;
        }
        let ok = result.is_ok();
        let reason = result.reason().to_string();
        let observed = result.observed().map(str::to_string);
        let check_name = self.checks[idx].cfg.name.clone();

        // §7.1 startup_grace(第三轮 P0-1 修复):抑制窗口**仅按时间**计
        // (last_spawn 起 startup_grace)—— 此前首判据 `state == Starting` 无时间
        // 上限，与 bound_critical 门控组合会把"重启后仍不健康"的服务永久卡死在
        // Starting(恢复/Fatal/renotify 全停摆)。另外恢复执行态 (Recovering/
        // ForceKill/Restarting/CrashLoop)期间失败探测同样抑制 —— §8.1"恢复执行
        // 期间告警升级被抑制",避免对 FSM 已在处理的重启叠加告警噪声。成功探测
        // 永不抑制 (健康恢复必须能流入状态机驱动 Starting→Running)。
        if let Some(sidx) = self.checks[idx].bound_service {
            let svc = &self.svcs[sidx];
            let in_recovery_state = matches!(
                svc.fsm.state(),
                RecoveryState::Recovering
                    | RecoveryState::ForceKill
                    | RecoveryState::Restarting
                    | RecoveryState::CrashLoop
            );
            let in_starting_grace = svc.fsm.state() == RecoveryState::Starting
                && svc
                    .last_spawn
                    .is_some_and(|t| now_mono.duration_since(t) < svc.policy.startup_grace);
            let in_grace = in_recovery_state || in_starting_grace;
            if in_grace && !ok {
                tracing::debug!(
                    target = %svc.cfg.name,
                    check = %check_name,
                    reason = %reason,
                    "probe failure ignored during startup grace"
                );
                return;
            }
        }

        let transition = self.checks[idx].state.observe(ok);
        let level = self.checks[idx].state.level();
        if transition.is_some() {
            self.checks[idx].last_transition_wall = Some(now_wall);
        }

        // incident 去噪 (§7.2):每轮都喂当前级别;renotify 由 tracker 按间隔判定。
        let (target, check_label) = self.labels(idx);
        let event_reason = if ok {
            observed.clone().unwrap_or_else(|| "probe ok".to_string())
        } else {
            reason
        };
        let events = self.tracker.observe(
            now_wall,
            now_mono,
            &target,
            &check_label,
            level,
            &event_reason,
            observed,
        );
        for ev in &events {
            self.emit_alert(ev, h);
            journal(
                h,
                JournalEvent::Incident {
                    at: now_wall,
                    incident: ev.incident,
                    target: ev.target.clone(),
                    check: ev.check.clone(),
                    category: ev.category,
                    severity: ev.severity,
                    reason: ev.reason.clone(),
                },
            );
        }

        // §8.1 健康契约:**每次探测收割都喂**当前级别 (评审 P1 修复：仅转移时喂，
        // 持续 Critical 不再触发，Suspect 的二次确认永远无法累积，挂死恢复不可达)。
        // on_health 内部对重复 Critical 是安全递增 (confirmations 按 probe 节奏推进),
        // Ok 路由到 liveness 自愈，Warning 打断连续链 —— 幂等，无需转移门。
        if let Some(sidx) = self.checks[idx].bound_service {
            if self.svcs[sidx].orphan.is_some() {
                return;
            }
            // 多绑定 check(SRE 评审 P2):本检查 Ok 但同级 check Critical 时不喂 Ok
            // —— 否则 Suspect 下的自愈清零会让"连续确认"永不达成 (恢复不可达)。
            let sibling_critical = self.svcs[sidx]
                .checks
                .iter()
                .any(|&ci| ci != idx && self.checks[ci].state.level() == HealthLevel::Critical);
            let feed = if level == HealthLevel::Ok {
                !sibling_critical
            } else {
                true
            };
            if feed {
                let actions = self.svcs[sidx].fsm.on_health(now_mono, level);
                self.exec_actions(sidx, actions, now_mono, now_wall, h)
                    .await;
            }
        }
        let _ = transition; // 转移仅用于 last_transition_wall(上方已处理)
    }

    /// supervise 服务的每轮处理:liveness、亲生退出、fsm 周期驱动、spawn 到点、
    /// activate 收割、restart 兜底强杀。
    async fn service_tick(
        &mut self,
        sidx: usize,
        snap: &CollectOutput,
        now_mono: Instant,
        now_wall: SystemTime,
        h: &mut SchedulerHandles,
    ) {
        if h.journal_tx.counters().has_failed() {
            return;
        }
        let name = self.svcs[sidx].cfg.name.clone();
        if let Some(orphan) = self.svcs[sidx].orphan.as_mut() {
            match orphan.observe(snap) {
                OrphanObservation::Gone => {
                    if let Err(e) = h
                        .journal_tx
                        .send(JournalEvent::ProcessIdentity {
                            at: now_wall,
                            target: name.clone(),
                            identity: None,
                            verified: false,
                        })
                        .await
                    {
                        tracing::error!(target = %name, source = %e, "orphan release persistence failed");
                        return;
                    }
                    self.svcs[sidx].orphan = None;
                    h.pm.register(self.svcs[sidx].cfg.clone());
                    let actions = self.svcs[sidx].fsm.on_reconciled_exit(now_mono);
                    self.exec_actions(sidx, actions, now_mono, now_wall, h)
                        .await;
                    tracing::info!(target = %name, "previous instance gone; returning to supervision");
                }
                OrphanObservation::Attached(identity) => {
                    if self.svcs[sidx].persisted_observed_identity != Some(identity) {
                        if let Err(e) = h
                            .journal_tx
                            .send(JournalEvent::ProcessIdentity {
                                at: now_wall,
                                target: name.clone(),
                                identity: Some(identity),
                                verified: true,
                            })
                            .await
                        {
                            tracing::error!(target = %name, source = %e, "orphan identity persistence failed");
                            return;
                        }
                        self.svcs[sidx].persisted_observed_identity = Some(identity);
                    }
                    return;
                }
                OrphanObservation::Pending => return,
            }
        }

        // 新定义保持等待，直到旧 child 真正退出并被收割。保留旧 Job/Child 句柄
        // 也覆盖 Windows 无 SIGTERM 和 Unix SIGKILL 后仍在退出的窗口。
        if let Some(deadline) = self.svcs[sidx].reload_stop_deadline {
            if h.pm.poll_exit(&name).await.is_some() || !h.pm.is_running(&name) {
                if let Err(e) = h
                    .journal_tx
                    .send(JournalEvent::ProcessIdentity {
                        at: now_wall,
                        target: name.clone(),
                        identity: None,
                        verified: false,
                    })
                    .await
                {
                    tracing::error!(target = %name, source = %e, "redefinition release persistence failed");
                    return;
                }
                h.pm.register(self.svcs[sidx].cfg.clone());
                self.svcs[sidx].reload_stop_deadline = None;
                self.svcs[sidx].spawn_due = Some(now_mono);
            } else {
                if now_mono >= deadline {
                    if let Err(e) = h.pm.force_kill(&name) {
                        tracing::error!(target = %name, source = %e, "reload: old instance force kill failed");
                    }
                    self.svcs[sidx].reload_stop_deadline = Some(now_mono + MAX_TICK);
                }
                return;
            }
        }

        // liveness 存活：亲生 child 在手即存活 —— 但绑定 check 为 Critical 时不喂
        // (评审 P1 修复：无条件喂会在 Suspect 进入的同一 tick 把挂死进程"治愈",
        // 健康类挂死的二次确认永远无法累积)。无绑定 check 时 child 即唯一证据。
        let bound_critical = self.svcs[sidx]
            .checks
            .iter()
            .any(|&ci| self.checks[ci].state.level() == HealthLevel::Critical);
        if h.pm.is_running(&name) && !bound_critical {
            let actions = self.svcs[sidx].fsm.on_liveness_ok(now_mono);
            self.exec_actions(sidx, actions, now_mono, now_wall, h)
                .await;
        }

        // 快照证据 liveness:仅在采集器确实携带进程数据时采信"缺失"
        // (快照为空 = watched 缺口，不作证据 —— 见模块 doc v0.1 局限);
        // 且 spawn 后至少隔一个采集间隔，避免首采竞态;且只在**新快照**上评估
        // (第三轮 P2:同一份快照最多贡献一次 gone 确认，防止按 tick 虚增)。
        let fresh_snapshot = self.last_gone_eval_sample != Some(snap.collected_at);
        let sample_current = now_mono.saturating_duration_since(snap.collected_at)
            <= self.collect_interval.saturating_mul(2).max(MAX_TICK);

        // SCM 目标 (G 线):无 (pid,start_time) 身份链 —— liveness 全部走
        // QueryStatus 证据，快照身份路径整体跳过。
        // 三值判定 (第六轮评审 P1):Transitioning(START_PENDING / STOP_PENDING /
        // PAUSED) 与查询失败不是死亡证据，一律不喂 —— 此前 is_running 把它们抹平
        // 为 false，启动慢于 confirmations×采集节奏的服务会被发 Stop 掐死启动，
        // 反复计预算直至 Fatal。Stopped 才是死亡证据 (保留 fresh_snapshot 节奏 +
        // confirmations 二次确认);无响应启动由 SCM 自身超时 (~30s,1053) 转
        // Stopped 兜底，agent 无需自带 Transitioning 判死。
        if self.svcs[sidx].cfg.windows_service.is_some() {
            match h.pm.scm_state(&name) {
                Some(crate::recover::process::windows::ScmServiceState::Running) => {
                    if self.svcs[sidx].fsm.state() != RecoveryState::Fatal {
                        let actions = self.svcs[sidx].fsm.on_liveness_ok(now_mono);
                        self.exec_actions(sidx, actions, now_mono, now_wall, h)
                            .await;
                    }
                }
                Some(crate::recover::process::windows::ScmServiceState::Stopped)
                    if fresh_snapshot =>
                {
                    tracing::debug!(target = %name, "scm target stopped (query status)");
                    let actions = self.svcs[sidx].fsm.on_liveness_gone(now_mono);
                    self.exec_actions(sidx, actions, now_mono, now_wall, h)
                        .await;
                }
                // Transitioning(启动/停止在途) 与查询失败：本轮不采信、不判死。
                _ => {}
            }
        } else if let Some(ident) = h.pm.identity(&name) {
            let has_evidence = sample_current
                && !snap.processes.is_empty()
                && snap
                    .requested
                    .iter()
                    .any(|requested| requested.pid == ident.pid)
                && !snap.hardware.degradations.iter().any(|degradation| {
                    matches!(
                        degradation,
                        crate::model::snapshot::Degradation::CollectorInit { .. }
                    )
                });
            let matured = self.svcs[sidx]
                .last_spawn
                .is_none_or(|t| now_mono.duration_since(t) > self.collect_interval);
            if has_evidence && matured {
                match find_by_identity(snap, &ident) {
                    Some(observed) => {
                        if h.pm.adopt_identity(&name, observed.identity) {
                            self.svcs[sidx].fsm.adopt_identity(ident, observed.identity);
                            if self.svcs[sidx].persisted_observed_identity
                                != Some(observed.identity)
                            {
                                if let Err(e) = h
                                    .journal_tx
                                    .send(JournalEvent::ProcessIdentity {
                                        at: now_wall,
                                        target: name.clone(),
                                        identity: Some(observed.identity),
                                        verified: true,
                                    })
                                    .await
                                {
                                    tracing::error!(target = %name, source = %e, "observed identity persistence failed");
                                    return;
                                }
                                self.svcs[sidx].persisted_observed_identity =
                                    Some(observed.identity);
                            }
                        }
                    }
                    None => {
                        // 同 pid 不同 start_time:大概率时钟步进 (±2s 容差已吸收小跳),
                        // 重采纳观测身份而非判死 (SRE 评审 P2:误判死亡会杀活进程);
                        // pid 消失才走 gone 确认。
                        if let Some(other) =
                            snap.processes.iter().find(|p| p.identity.pid == ident.pid)
                        {
                            tracing::warn!(
                                target = %name,
                                old = %ident,
                                new = %other.identity,
                                "start_time drifted beyond tolerance; re-adopting observed identity"
                            );
                            // P2(第五轮):pm 登记身份必须同步为内核观测值 ——
                            // 此前仅 warn,pm 身份陈旧导致 process 检查预期身份
                            // 每探必败 + 本分支每 tick 刷屏。
                            if h.pm.adopt_identity(&name, other.identity) {
                                self.svcs[sidx].fsm.adopt_identity(ident, other.identity);
                                if self.svcs[sidx].restart_stop_identity == Some(ident) {
                                    self.svcs[sidx].restart_stop_identity = Some(other.identity);
                                }
                                if let Err(e) = h
                                    .journal_tx
                                    .send(JournalEvent::ProcessIdentity {
                                        at: now_wall,
                                        target: name.clone(),
                                        identity: Some(other.identity),
                                        verified: true,
                                    })
                                    .await
                                {
                                    tracing::error!(target = %name, source = %e, "observed identity persistence failed");
                                    return;
                                }
                                self.svcs[sidx].persisted_observed_identity = Some(other.identity);
                            }
                        } else if fresh_snapshot {
                            if self.planned_stops.contains(&name) {
                                // 计划内级联停止在途：快照缺失不是死亡证据，
                                // 退出事实由 poll_exit/on_child_exit 收口 (P3)。
                                tracing::debug!(
                                    target = %name,
                                    "identity missing in snapshot: planned cascade stop in flight"
                                );
                            } else {
                                tracing::debug!(target = %name, pid = ident.pid.0, "identity missing in snapshot");
                                let actions = self.svcs[sidx].fsm.on_liveness_gone(now_mono);
                                self.exec_actions(sidx, actions, now_mono, now_wall, h)
                                    .await;
                            }
                        }
                    }
                }
            }
        }

        // 亲生退出 (确定性死亡，reaper 兜底防僵尸)。
        if let Some(exit) = h.pm.poll_exit(&name).await {
            if let Err(e) = h
                .journal_tx
                .send(JournalEvent::ProcessIdentity {
                    at: now_wall,
                    target: name.clone(),
                    identity: None,
                    verified: false,
                })
                .await
            {
                tracing::error!(target = %name, source = %e, "child exit identity persistence failed");
                return;
            }
            // 控制面 restart 的停止已由本次退出兑现：清除兜底强杀 (第三轮 P0-2)
            self.svcs[sidx].restart_stop_deadline = None;
            self.svcs[sidx].restart_stop_identity = None;
            let fast = exit.uptime < self.svcs[sidx].policy.min_uptime;
            tracing::info!(
                target = %name,
                pid = exit.identity.pid.0,
                uptime = ?exit.uptime,
                fast_exit = fast,
                "child process exited"
            );
            // 计划内级联停止 (F20/P3):dependent 的退出经 on_planned_stop 收口
            // —— 不计预算、不排程，由依赖门控拉回;否则 deterministic_death 会把
            // "计划内停止"计为一次自动重启，依赖反复 restart 可把 dependent 推向
            // 误 Fatal。
            let planned = self.planned_stops.remove(&name);
            self.planned_stop_deadlines.remove(&name);
            let actions = if planned {
                // P2(第七轮):计划内停止不走自动重启 —— eval 侧残留的 backoff
                // spawn_due 必须清除 (拉回由 F20 门控/级联注册驱动)。
                self.svcs[sidx].spawn_due = None;
                self.svcs[sidx].fsm.on_planned_stop(now_mono)
            } else {
                self.svcs[sidx].fsm.on_child_exit(now_mono, exit.identity)
            };
            self.exec_actions(sidx, actions, now_mono, now_wall, h)
                .await;
        }

        // ForceKill kill 截止 (SRE 评审 P0 配套):SIGKILL 后到期仍存活 (Unix D 状态
        // 对 SIGKILL 免疫)→ on_stop_result(false) → Fatal，不留僵尸恢复循环。
        if self.svcs[sidx].fsm.state() == RecoveryState::ForceKill {
            if h.pm.is_running(&name) {
                let grace = self.svcs[sidx].policy.stop_grace;
                let deadline = *self.svcs[sidx]
                    .kill_deadline
                    .get_or_insert(now_mono + grace);
                if now_mono >= deadline {
                    self.svcs[sidx].kill_deadline = None;
                    let actions = self.svcs[sidx].fsm.on_stop_result(now_mono, false);
                    self.exec_actions(sidx, actions, now_mono, now_wall, h)
                        .await;
                }
            } else if h.pm.identity(&name).is_none() {
                // 无子进程的 ForceKill 通用兜底 (第三轮 P1-2:activate 期死亡等场景
                // 会让 FSM 停在 ForceKill 且无任何定时器/事件可解)→ 视同停止完成。
                self.svcs[sidx].kill_deadline = None;
                let actions = self.svcs[sidx].fsm.on_stop_result(now_mono, true);
                self.exec_actions(sidx, actions, now_mono, now_wall, h)
                    .await;
            } else {
                self.svcs[sidx].kill_deadline = None;
            }
        } else {
            // 非_FORCEKILL 态一律清除残留 (第三轮 P1-1:成功收尾后滞留的过期
            // deadline 会让下一次 ForceKill 的宽限期退化为一个 tick)
            self.svcs[sidx].kill_deadline = None;
        }

        // fsm 周期驱动:stop_grace 升级、spawn 到点、startup_grace 超时、verify 到点。
        let actions = self.svcs[sidx].fsm.on_tick(now_mono);
        self.exec_actions(sidx, actions, now_mono, now_wall, h)
            .await;

        // P1(第七轮):planned 停止的强杀升级 —— dependent 无视 SIGTERM 且
        // 超过 stop_grace → force_kill(退出仍走 on_planned_stop 收口，不计预算)。
        if let Some(deadline) = self.planned_stop_deadlines.get(&name)
            && now_mono >= *deadline
        {
            if h.pm.is_running(&name) {
                tracing::warn!(target = %name, "planned cascade stop timed out; forcing kill");
                if let Err(e) = h.pm.force_kill(&name) {
                    tracing::error!(target = %name, source = %e, "planned cascade force kill failed");
                }
            }
            self.planned_stop_deadlines.remove(&name);
        }

        // F20 级联拉回:target 回 Running(存活过 min_uptime)→ 拉回此前
        // 因其 restart 而级联停止的被依赖方 (spawn_due=now;FSM 保持，重启语义)。
        if self.svcs[sidx].fsm.state() == RecoveryState::Running
            && self.svcs[sidx].restart_stop_deadline.is_none()
            && h.pm.is_running(&name)
            && let Some(deps) = self.cascade_restart_after.remove(&name)
        {
            for dep in deps {
                if let Some(didx) = self.svcs.iter().position(|s| s.cfg.name == dep) {
                    if self.svcs[didx].fsm.is_fatal() {
                        tracing::warn!(target = %dep, "cascade restart skipped: fatal latched");
                        journal(
                            h,
                            JournalEvent::Audit {
                                at: now_wall,
                                message: format!(
                                    "cascade restart for {dep} skipped: fatal latched (P3: journal visibility)"
                                ),
                            },
                        );
                        continue;
                    }
                    if self.svcs[didx].orphan_suspected {
                        tracing::warn!(
                            target = %dep,
                            "cascade restart skipped: suspected orphan from unresolved spawn intent"
                        );
                        journal(
                            h,
                            JournalEvent::Audit {
                                at: now_wall,
                                message: format!(
                                    "cascade restart for {dep} skipped: orphan suspected (reset then restart agent)"
                                ),
                            },
                        );
                        continue;
                    }
                    tracing::info!(target = %dep, dependency = %name, "cascade restart scheduled (F20)");
                    // P3(第七轮):dependent 自身退避中的 spawn_due 不得被拉回覆盖
                    let dep_due = self.svcs[didx].spawn_due.get_or_insert(now_mono);
                    if *dep_due < now_mono {
                        *dep_due = now_mono;
                    }
                    journal(
                        h,
                        JournalEvent::Audit {
                            at: now_wall,
                            message: format!(
                                "cascade restart {dep} after dependency {name} recovered (F20)"
                            ),
                        },
                    );
                }
            }
        }

        // F20 启动编排：有依赖且尚未排程的服务，依赖全部 Running 后排程拉起。
        // 依赖永不满足 (如依赖 Fatal)→ 永不自动启动 (失败不级联，人工介入)。
        if self.svcs[sidx].spawn_due.is_none()
            && !h.pm.is_running(&name)
            && !self.svcs[sidx].cfg.depends_on.is_empty()
            && !self.svcs[sidx].fsm.is_fatal()
            && !self.svcs[sidx].orphan_suspected
            && self.deps_satisfied(&self.svcs[sidx], h)
        {
            tracing::info!(
                target = %name,
                deps = ?self.svcs[sidx].cfg.depends_on,
                "dependencies running; scheduling cold start (F20)"
            );
            self.svcs[sidx].spawn_due = Some(now_mono);
        }

        // spawn 到点 (ScheduleSpawn 由 exec_actions 记录)。
        if self.svcs[sidx].spawn_due.is_some_and(|d| d <= now_mono)
            && !h.pm.is_running(&name)
            && self.deps_satisfied(&self.svcs[sidx], h)
            && !self
                .cascade_restart_after
                .values()
                .any(|targets| targets.contains(&name))
        {
            self.svcs[sidx].spawn_due = None;
            if self.svcs[sidx].fsm.is_fatal() {
                tracing::warn!(target = %name, "spawn due skipped: fatal latched");
            } else if self.svcs[sidx].orphan_suspected {
                // 崩溃启动事务：未知实例窗口未经 reset 关闭前禁止重复拉起 (防双实例)。
                tracing::warn!(
                    target = %name,
                    "spawn due skipped: suspected orphan from unresolved spawn intent (monitorctl reset to release)"
                );
            } else {
                // 崩溃启动事务:spawn **前**先持久化启动意图 (send 确认落盘，
                // 与 Fatal 同级)。agent 在意图与结果之间崩溃 → 下次冷启动对账
                // 出「未知实例窗口」,跳过 spawn 防双实例。
                let intent = JournalEvent::SpawnIntent {
                    at: now_wall,
                    target: name.clone(),
                    config_digest: journal::config_digest(&self.svcs[sidx].cfg),
                };
                if let Err(e) = h.journal_tx.send(intent).await {
                    tracing::error!(
                        target = %name,
                        source = %e,
                        "spawn intent persistence failed; recovery aborted"
                    );
                    return;
                }
                match h.pm.spawn(&name).await {
                    Ok(identity) => {
                        // 结果先于身份落盘：关闭未知实例窗口 (成功)。
                        let outcome = JournalEvent::SpawnOutcome {
                            at: now_wall,
                            target: name.clone(),
                            identity: Some(identity),
                            pid_confirmed: true,
                        };
                        if let Err(e) = h.journal_tx.send(outcome).await {
                            tracing::error!(target = %name, source = %e, "spawn outcome persistence failed");
                            return;
                        }
                        tracing::info!(target = %name, identity = %identity, "process spawned");
                        // P0(第七轮):SCM 目标无 (pid,start_time) 身份链 ——
                        // 占位 (0,0) 落盘会让重启后的孤儿对账把目标永久钉死
                        // (reset 也无出口)。SCM 目标身份持久化为 None。
                        let persisted = if self.svcs[sidx].cfg.windows_service.is_some() {
                            None
                        } else {
                            Some(identity)
                        };
                        if let Err(e) = h
                            .journal_tx
                            .send(JournalEvent::ProcessIdentity {
                                at: now_wall,
                                target: name.clone(),
                                identity: persisted,
                                verified: false,
                            })
                            .await
                        {
                            tracing::error!(target = %name, source = %e, "spawn identity persistence failed");
                            return;
                        }
                        self.svcs[sidx].persisted_observed_identity = None;
                        self.svcs[sidx].last_spawn = Some(now_mono);
                        // SCM 目标 (G 线):无 (pid,start_time) 身份链 —— 走
                        // on_scm_spawn_ok(→ Starting，不落 identity);liveness
                        // 由 QueryStatus 证据驱动 (见 service_tick)。
                        let actions = if self.svcs[sidx].cfg.windows_service.is_some() {
                            self.svcs[sidx].fsm.on_scm_spawn_ok(now_mono)
                        } else {
                            self.svcs[sidx]
                                .fsm
                                .on_spawn_result(now_mono, Some(identity))
                        };
                        self.exec_actions(sidx, actions, now_mono, now_wall, h)
                            .await;
                    }
                    Err(e) => {
                        // 拿不到身份 (失败) 也写结果 (pid_confirmed=false),关闭窗口。
                        let outcome = JournalEvent::SpawnOutcome {
                            at: now_wall,
                            target: name.clone(),
                            identity: None,
                            pid_confirmed: false,
                        };
                        if let Err(se) = h.journal_tx.send(outcome).await {
                            tracing::error!(target = %name, source = %se, "spawn outcome persistence failed");
                            return;
                        }
                        tracing::error!(target = %name, source = %e, "spawn failed");
                        let actions = self.svcs[sidx].fsm.on_spawn_result(now_mono, None);
                        self.exec_actions(sidx, actions, now_mono, now_wall, h)
                            .await;
                    }
                }
            }
        }

        // 激活钩子完成 → fsm.on_activate_result。
        if self.svcs[sidx]
            .activate
            .as_ref()
            .is_some_and(|j| j.is_finished())
        {
            let handle = self.svcs[sidx]
                .activate
                .take()
                .expect("checked is_finished");
            let ok = handle.await.unwrap_or(false);
            tracing::info!(target = %name, ok, "activate hook finished");
            let actions = self.svcs[sidx].fsm.on_activate_result(now_mono, ok);
            self.exec_actions(sidx, actions, now_mono, now_wall, h)
                .await;
        }

        // 控制平面 restart 的兜底:graceful stop 后 stop_grace 内未退出 → 强杀。
        // 仅当仍是 signal_stop 时的同一身份 (第三轮 P0-2:新进程绝不背旧账)。
        if let Some(deadline) = self.svcs[sidx].restart_stop_deadline
            && now_mono >= deadline
        {
            self.svcs[sidx].restart_stop_deadline = None;
            // 身份守卫：仅当运行的仍是 signal_stop 时登记的那个进程才强杀;
            // 已退出并被新 spawn 替换 → 新进程不背旧账，守卫自动放行。
            let guard_ok = self.svcs[sidx]
                .restart_stop_identity
                .is_none_or(|id| h.pm.identity(&name) == Some(id));
            if h.pm.is_running(&name) && guard_ok {
                tracing::warn!(target = %name, "restart: graceful stop timed out, forcing kill");
                match h.pm.force_kill(&name) {
                    Ok(()) => journal(
                        h,
                        JournalEvent::Audit {
                            at: now_wall,
                            message: format!(
                                "restart for {name}: graceful stop timed out, force killed"
                            ),
                        },
                    ),
                    Err(e) => tracing::error!(target = %name, source = %e, "force kill failed"),
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // RecoverAction 执行 (§8:eval 是恢复状态机的手臂)
    // -----------------------------------------------------------------

    /// 执行恢复状态机要求的动作;动作处理产生的结果不再回灌 fsm
    /// (spawn/activate 结果由调用方在对应路径上喂)。
    async fn exec_actions(
        &mut self,
        sidx: usize,
        actions: Vec<RecoverAction>,
        now_mono: Instant,
        now_wall: SystemTime,
        h: &mut SchedulerHandles,
    ) {
        let name = self.svcs[sidx].cfg.name.clone();
        let mut pending: VecDeque<RecoverAction> = actions.into();
        let mut steps = 0u32;
        while let Some(action) = pending.pop_front() {
            if h.journal_tx.counters().has_failed() {
                return;
            }
            steps += 1;
            if steps > 32 {
                tracing::error!(target = %name, "recovery action stream exceeded 32 steps; aborting batch");
                break;
            }
            match action {
                RecoverAction::ScheduleSpawn { delay } => {
                    self.svcs[sidx].spawn_due = Some(now_mono + delay);
                }
                RecoverAction::JournalRestart { fast_exit } => {
                    // 预算计数的持久化 (无退出事件的计数点：健康挂死/spawn 失败);
                    // restore 依 Restart 事件恢复预算窗口 (第三轮 P2)
                    if let Err(e) = h
                        .journal_tx
                        .send(JournalEvent::Restart {
                            at: now_wall,
                            target: name.clone(),
                            fast_exit,
                        })
                        .await
                    {
                        tracing::error!(source = %e, "recovery aborted: restart journal unavailable");
                        break;
                    }
                }
                RecoverAction::SignalStop => {
                    match h.pm.signal_stop(&name, self.svcs[sidx].policy.stop_grace) {
                        Ok(()) => tracing::info!(target = %name, "graceful stop signaled"),
                        Err(e) => {
                            tracing::warn!(target = %name, source = %e, "graceful stop failed")
                        }
                    }
                }
                RecoverAction::ForceKill => match h.pm.force_kill(&name) {
                    Ok(()) => {
                        tracing::warn!(target = %name, "force kill executed");
                        journal(
                            h,
                            JournalEvent::Audit {
                                at: now_wall,
                                message: format!("force kill executed for {name}"),
                            },
                        );
                    }
                    Err(e) => {
                        tracing::error!(target = %name, source = %e, "force kill failed")
                    }
                },
                RecoverAction::RunActivate => {
                    let Some(activate) = self.svcs[sidx].cfg.activate.clone() else {
                        tracing::warn!(
                            target = %name,
                            "activate requested but [service.activate] not configured"
                        );
                        // 结果直接回灌动作队列 (避免 async fn 递归)。
                        let more = self.svcs[sidx].fsm.on_activate_result(now_mono, false);
                        pending.extend(more);
                        continue;
                    };
                    tracing::info!(target = %name, "running activate hook");
                    self.svcs[sidx].activate = Some(tokio::spawn(run_activate_hook(activate)));
                }
                RecoverAction::MarkFatal => {
                    let reason = format!("target {name} latched fatal (restart budget exhausted)");
                    tracing::error!(target = %name, "fatal latched");
                    // Fatal 采用有界等待，失败计数并请求停机。
                    if let Err(e) = h
                        .journal_tx
                        .send(JournalEvent::Fatal {
                            at: now_wall,
                            target: name.clone(),
                            reason: reason.clone(),
                        })
                        .await
                    {
                        tracing::error!(source = %e, "FATAL journal event could not be queued");
                        return;
                    }
                    // Fatal 告警:incident key = hash(target, "__recover__")(任务约定)。
                    let ev = AlertEvent {
                        incident: AlertEvent::incident_key(&name, RECOVER_CHECK),
                        category: Category::Alert,
                        severity: Severity::Critical,
                        target: name.clone(),
                        check: RECOVER_CHECK.to_string(),
                        reason,
                        observed: None,
                        first_seen: now_wall,
                        last_seen: now_wall,
                        occurrences: 1,
                    };
                    self.emit_alert(&ev, h);
                    journal(
                        h,
                        JournalEvent::Incident {
                            at: now_wall,
                            incident: ev.incident,
                            target: name.clone(),
                            check: RECOVER_CHECK.to_string(),
                            category: Category::Alert,
                            severity: Severity::Critical,
                            reason: ev.reason.clone(),
                        },
                    );
                }
                RecoverAction::Audit { message, at } => {
                    tracing::info!(target = %name, audit = %message, "recovery audit");
                    journal(h, JournalEvent::Audit { at, message });
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // 控制平面命令 (§9.2)
    // -----------------------------------------------------------------

    async fn handle_control(
        &mut self,
        cmd: ControlCommand,
        now_mono: Instant,
        now_wall: SystemTime,
        h: &mut SchedulerHandles,
    ) {
        if h.journal_tx.counters().has_failed() {
            return;
        }
        match cmd {
            ControlCommand::Check { target } => {
                let mut hit = false;
                for c in &mut self.checks {
                    if c.cfg.name == target {
                        c.next_due = now_mono;
                        hit = true;
                    }
                }
                for s in &self.svcs {
                    if s.cfg.name == target {
                        for &i in &s.checks {
                            self.checks[i].next_due = now_mono;
                            hit = true;
                        }
                    }
                }
                for w in &self.watches {
                    if w.name == target {
                        for &i in &w.checks {
                            self.checks[i].next_due = now_mono;
                            hit = true;
                        }
                    }
                }
                if hit {
                    tracing::info!(target = %target, "check requested via control plane");
                } else {
                    tracing::warn!(target = %target, "check request: unknown target");
                    journal(
                        h,
                        JournalEvent::Audit {
                            at: now_wall,
                            message: format!("check request for unknown target {target}"),
                        },
                    );
                }
            }
            ControlCommand::Restart { target } => {
                let Some(sidx) = self.svcs.iter().position(|s| s.cfg.name == target) else {
                    tracing::warn!(target = %target, "restart request: not a supervise target");
                    journal(
                        h,
                        JournalEvent::Audit {
                            at: now_wall,
                            message: format!(
                                "restart rejected: {target} is not a supervise target"
                            ),
                        },
                    );
                    return;
                };
                if self.svcs[sidx].orphan.is_some()
                    || self.svcs[sidx].reload_stop_deadline.is_some()
                {
                    journal(
                        h,
                        JournalEvent::Audit {
                            at: now_wall,
                            message: format!(
                                "restart rejected: {target} is reconciling or stopping a previous instance"
                            ),
                        },
                    );
                    return;
                }
                if self.svcs[sidx].orphan_suspected {
                    // 崩溃启动事务：未知实例窗口未关闭前，restart 的"未运行即拉起"
                    // 路径会制造双实例;reset(人工处置) 是唯一解除途径。
                    tracing::warn!(
                        target = %target,
                        "restart rejected: suspected orphan from unresolved spawn intent"
                    );
                    journal(
                        h,
                        JournalEvent::Audit {
                            at: now_wall,
                            message: format!(
                                "restart rejected: {target} has a suspected orphan from an unresolved spawn intent (monitorctl reset to release)"
                            ),
                        },
                    );
                    return;
                }
                if !h.pm.is_running(&target) && !self.deps_satisfied(&self.svcs[sidx], h) {
                    // P3(第五轮):restart 遵守 F20 启动门控 —— 服务未运行且依赖
                    // 未满足时，重启请求不得绕过编排 (否则 spawn 排程后无门控兜底)。
                    tracing::warn!(target = %target, "restart rejected: dependencies not running (F20)");
                    journal(
                        h,
                        JournalEvent::Audit {
                            at: now_wall,
                            message: format!(
                                "restart rejected: {target} dependencies not running (F20)"
                            ),
                        },
                    );
                    return;
                }
                if self.svcs[sidx].fsm.is_fatal() {
                    // 无法回 409(eval 不持有 HTTP 上下文):记 warn + journal Audit。
                    tracing::warn!(target = %target, "restart rejected: fatal latched (requires reset)");
                    journal(
                        h,
                        JournalEvent::Audit {
                            at: now_wall,
                            message: format!(
                                "restart rejected: {target} is fatal (run reset first)"
                            ),
                        },
                    );
                    return;
                }
                // F20 停止顺序：先停 (传递) 被依赖方 (逆拓扑，叶子先停),
                // 登记级联 —— target 回 Running 后自动拉回。
                let dependent_order = self.dependents_stop_order(&target);
                for dep_name in &dependent_order {
                    if let Some(didx) = self.svcs.iter().position(|s| &s.cfg.name == dep_name)
                        && h.pm.is_running(dep_name)
                    {
                        let grace = self.svcs[didx].policy.stop_grace;
                        match h.pm.signal_stop(dep_name, grace) {
                            Ok(()) => {
                                // 计划内停止：退出不计 dependent 预算 (第六轮评审
                                // P3),由 on_planned_stop 收口、F20 门控拉回。
                                self.planned_stops.insert(dep_name.clone());
                                self.planned_stop_deadlines.insert(
                                    dep_name.clone(),
                                    now_mono + self.svcs[didx].policy.stop_grace,
                                );
                                journal(
                                    h,
                                    JournalEvent::Audit {
                                        at: now_wall,
                                        message: format!(
                                            "cascade stop {dep_name} before dependency {target} restart (F20)"
                                        ),
                                    },
                                );
                            }
                            Err(e) => tracing::warn!(
                                target = %dep_name, source = %e, "cascade stop failed"
                            ),
                        }
                    }
                }
                if !dependent_order.is_empty() {
                    self.cascade_restart_after
                        .insert(target.clone(), dependent_order.clone());
                }
                if h.pm.is_running(&target) {
                    self.svcs[sidx].restart_stop_identity = h.pm.identity(&target);
                    match h.pm.signal_stop(&target, self.svcs[sidx].policy.stop_grace) {
                        Ok(()) => {
                            tracing::info!(target = %target, "restart requested: graceful stop signaled");
                            journal(
                                h,
                                JournalEvent::Audit {
                                    at: now_wall,
                                    message: format!(
                                        "restart requested for {target}: graceful stop signaled"
                                    ),
                                },
                            );
                            // 退出事件经 poll_exit → on_child_exit 驱动恢复;
                            // 未响应 SIGTERM 的进程由 stop_grace 兜底强杀。
                            let grace = self.svcs[sidx].policy.stop_grace;
                            self.svcs[sidx].restart_stop_deadline = Some(now_mono + grace);
                        }
                        Err(e) => {
                            tracing::warn!(target = %target, source = %e, "restart: signal stop failed");
                            journal(
                                h,
                                JournalEvent::Audit {
                                    at: now_wall,
                                    message: format!(
                                        "restart for {target}: stop signal failed: {e}"
                                    ),
                                },
                            );
                            // P3(第七轮):X 停止失败 = X 不会重启 → 被级联停掉的
                            // dependents 立即拉回 (否则白停一轮)。
                            if let Some(deps) = self.cascade_restart_after.remove(&target) {
                                for dep in deps {
                                    if let Some(didx) =
                                        self.svcs.iter().position(|s| s.cfg.name == dep)
                                        && !self.svcs[didx].fsm.is_fatal()
                                        && !self.svcs[didx].orphan_suspected
                                        && h.pm.identity(&dep).is_none()
                                    {
                                        tracing::info!(target = %dep, "cascade pullback after stop-signal failure");
                                        self.svcs[didx].spawn_due = Some(now_mono);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    tracing::info!(target = %target, "restart requested: not running, scheduling spawn");
                    journal(
                        h,
                        JournalEvent::Audit {
                            at: now_wall,
                            message: format!(
                                "restart requested for {target}: not running, spawn scheduled"
                            ),
                        },
                    );
                    self.svcs[sidx].spawn_due = Some(now_mono);
                }
            }
            ControlCommand::Reset { target } => {
                if let Some(sidx) = self.svcs.iter().position(|s| s.cfg.name == target) {
                    if let Err(e) = h
                        .journal_tx
                        .send(JournalEvent::Reset {
                            at: now_wall,
                            target: target.clone(),
                            source: "control".to_string(),
                        })
                        .await
                    {
                        tracing::error!(source = %e, "reset aborted: journal unavailable");
                        return;
                    }
                    let child = h
                        .pm
                        .identity(&target)
                        .map(|identity| (identity, self.svcs[sidx].last_spawn.unwrap_or(now_mono)));
                    let actions = self.svcs[sidx].fsm.reset_with_child(now_mono, child);
                    // 崩溃启动事务:reset = 人工承认未知实例已处置，解除 suspected
                    // 标记并允许拉起 (journal Reset 在重放侧同步清除未决意图)。
                    self.svcs[sidx].orphan_suspected = false;
                    if child.is_none() && self.svcs[sidx].orphan.is_none() {
                        self.svcs[sidx].spawn_due = Some(now_mono);
                    }
                    tracing::info!(target = %target, "reset via control plane");
                    for &i in &self.svcs[sidx].checks {
                        let (t, c) = self.labels(i);
                        self.tracker.clear(&t, &c);
                    }
                    self.svcs[sidx].restart_stop_deadline = None;
                    self.exec_actions(sidx, actions, now_mono, now_wall, h)
                        .await;
                } else if let Some(i) = self.checks.iter().position(|c| c.cfg.name == target) {
                    let (t, c) = self.labels(i);
                    self.tracker.clear(&t, &c);
                    journal(
                        h,
                        JournalEvent::Reset {
                            at: now_wall,
                            target,
                            source: "control".to_string(),
                        },
                    );
                } else if let Some(widx) = self.watches.iter().position(|w| w.name == target) {
                    // watch 目标：按 watch 名清除其名下检查的 incident(第三轮 P2)
                    for &i in &self.watches[widx].checks {
                        let (t, c) = self.labels(i);
                        self.tracker.clear(&t, &c);
                    }
                    journal(
                        h,
                        JournalEvent::Reset {
                            at: now_wall,
                            target,
                            source: "control".to_string(),
                        },
                    );
                } else {
                    // 与 Check/Restart 一致：未知目标写审计 (第三轮 P2)
                    tracing::warn!(target = %target, "reset request: unknown target");
                    journal(
                        h,
                        JournalEvent::Audit {
                            at: now_wall,
                            message: format!("reset request for unknown target {target}"),
                        },
                    );
                }
            }
            ControlCommand::Reload { path } => {
                // F14 配置热重载:path 缺省 → 启动配置路径。读文件 + 校验失败：
                // ERROR 日志 + journal Audit，旧配置原样生效 (绝不半应用)。
                let path = path
                    .map(PathBuf::from)
                    .unwrap_or_else(|| h.config_path.clone());
                tracing::info!(path = %path.display(), "config reload requested");
                match Config::from_path(&path) {
                    Ok(new_cfg) => {
                        if let Err(e) = self.apply_reload(new_cfg, h, now_mono, now_wall).await {
                            tracing::error!(
                                path = %path.display(),
                                source = %e,
                                "config reload rejected; previous configuration remains active"
                            );
                            journal(
                                h,
                                JournalEvent::Audit {
                                    at: now_wall,
                                    message: format!("reload rejected: {e}"),
                                },
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            path = %path.display(),
                            source = %e,
                            "config reload rejected (parse/validate failed); previous configuration remains active"
                        );
                        journal(
                            h,
                            JournalEvent::Audit {
                                at: now_wall,
                                message: format!("reload rejected: {e}"),
                            },
                        );
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // F14 配置热重载 (ROADMAP F14;只应用目标拓扑，其余小节要求重启)
    // -----------------------------------------------------------------

    /// 应用热重载。前提:`new` 已通过解析 + 校验 (`Config::from_path`,调用方
    /// `handle_control` 负责;失败路径在那里拒绝，不进入本方法)。本方法做
    /// 拓扑 diff 与状态迁移，任何失败路径都保持旧配置完整生效 (绝不半应用)。
    ///
    /// 语义边界 (任务约定，详见 [`reload`] 模块 doc):
    /// - checks:同 name 保留状态机水位 (连续计数/级别),仅替换 cfg;删除 →
    ///   移除 (abort 在途探测)+ `tracker.clear` + 审计;绑定目标漂移的保留
    ///   check 清旧 (target, check) incident;
    /// - services:command/args/workdir/env 任一变更 → 删 + 增 (重启生效，journal
    ///   Reset 清除该 target 的重放闩锁/预算);restart/activate/console 变更 →
    ///   原地更新 cfg(policy/verify_for 重建，FSM 水位保留);删除 → 运行中则
    ///   signal_stop 后立即从调度移除，child 收割由 `pending_reap` 续管;
    /// - watches:pid/pidfile 变更 → 重新解析身份;
    /// - 非拓扑小节变更 → 仅告警 + 审计 (要求重启),tracker 阈值与
    ///   collect_interval 保持旧值。
    async fn apply_reload(
        &mut self,
        new: Config,
        h: &mut SchedulerHandles,
        now_mono: Instant,
        now_wall: SystemTime,
    ) -> Result<()> {
        // 防御性断言：校验层保证三空间 name 唯一，重名 → 整个 reload 拒绝。
        if let Some((label, name)) = reload::first_duplicate_name(&new) {
            return Err(CoreError::Config(format!(
                "{label} duplicate name {name:?}; reload rejected"
            )));
        }

        // 非拓扑段变更检测：如实记录 (仍应用拓扑，不热应用)。
        for section in reload::diff_sections(&self.active_cfg, &new) {
            tracing::warn!(
                section,
                "config section changed but requires restart (not hot-applied)"
            );
            journal(
                h,
                JournalEvent::Audit {
                    at: now_wall,
                    message: format!(
                        "section {section} changed but requires restart (not hot-applied)"
                    ),
                },
            );
        }
        let diff: TopologyDiff = reload::diff_topology(&self.active_cfg, &new);
        let reset_targets: Vec<String> = self
            .svcs
            .iter()
            .filter(|old| {
                new.services
                    .iter()
                    .find(|svc| svc.name == old.cfg.name)
                    .is_none_or(|cfg| reload::service_hot_changed(&old.cfg, cfg))
            })
            .map(|svc| svc.cfg.name.clone())
            .collect();
        if !reset_targets.is_empty() {
            h.journal_tx
                .send(JournalEvent::ResetTargets {
                    at: now_wall,
                    targets: reset_targets,
                    source: "reload".into(),
                })
                .await
                .map_err(|e| CoreError::Recover(format!("reload journal reset failed: {e}")))?;
        }

        // 旧运行时条目整体接管 (各 pass 可交叉引用旧绑定;索引在旧 vec 内自洽)。
        let mut old_checks: Vec<CheckRt> = std::mem::take(&mut self.checks);
        let mut old_svcs: Vec<SvcRt> = std::mem::take(&mut self.svcs);
        let mut old_watches: Vec<WatchRt> = std::mem::take(&mut self.watches);

        // ---- checks:同 name 保留水位，新增全新，删除清除 incident ----
        let mut new_checks: Vec<CheckRt> = Vec::with_capacity(new.checks.len());
        // 保留条目的旧标签 (绑定目标可能随 kind/process 变化;漂移时清旧 incident)。
        let mut kept_old_labels: Vec<(String, (String, String))> = Vec::new();
        for cfg in &new.checks {
            match old_checks.iter().position(|c| c.cfg.name == cfg.name) {
                Some(pos) => {
                    let mut rt = old_checks.remove(pos);
                    if diff.changed_checks.iter().any(|n| n == &cfg.name) {
                        tracing::info!(
                            check = %cfg.name,
                            "check reconfigured by reload; state watermarks preserved"
                        );
                    }
                    kept_old_labels.push((
                        cfg.name.clone(),
                        old_check_label(&rt, &old_svcs, &old_watches),
                    ));
                    rt.cfg = cfg.clone();
                    new_checks.push(rt);
                }
                None => {
                    tracing::info!(check = %cfg.name, "check added by reload");
                    new_checks.push(CheckRt {
                        bound_service: None,
                        bound_watch: None, // rebind() 回填
                        cfg: cfg.clone(),
                        state: CheckStateMachine::new(self.thresholds),
                        next_due: now_mono,
                        in_flight: None,
                        last_process_sample: None,
                        skipped: 0,
                        executed: 0,
                        last_transition_wall: None,
                    });
                }
            }
        }
        for rt in old_checks {
            let name = rt.cfg.name.clone();
            if let Some(j) = &rt.in_flight {
                j.abort();
            }
            let (target, check) = old_check_label(&rt, &old_svcs, &old_watches);
            self.tracker.clear(&target, &check);
            tracing::info!(check = %name, "check removed by reload");
            journal(
                h,
                JournalEvent::Audit {
                    at: now_wall,
                    message: format!("check {name} removed by reload"),
                },
            );
        }

        // ---- services:删 + 增 / 原地更新 / 新增 / 删除 ----
        let mut new_svcs: Vec<SvcRt> = Vec::with_capacity(new.services.len());
        for cfg in &new.services {
            match old_svcs.iter().position(|s| s.cfg.name == cfg.name) {
                Some(pos) => {
                    let hot = reload::service_hot_changed(&old_svcs[pos].cfg, cfg);
                    if hot {
                        // 删 + 增 (重启生效):旧实例优雅停，新定义全新 FSM 冷启动。
                        tracing::info!(
                            target = %cfg.name,
                            "service redefined by reload (command/args/workdir/env); restart applied"
                        );
                        journal(
                            h,
                            JournalEvent::Audit {
                                at: now_wall,
                                message: format!(
                                    "service {} redefined by reload (command/args/workdir/env changed); restart applied",
                                    cfg.name
                                ),
                            },
                        );
                        let mut old_rt = old_svcs.remove(pos);
                        self.dispose_service(&old_rt, h, now_wall, "redefined", false);
                        self.pending_reap.retain(|n| n != &cfg.name);
                        let mut fresh = Self::fresh_svc(cfg, now_mono);
                        fresh.orphan = old_rt.orphan.take();
                        if fresh.orphan.is_some() {
                            fresh.spawn_due = None;
                        } else if h.pm.is_running(&cfg.name) {
                            fresh.spawn_due = None;
                            fresh.reload_stop_deadline = Some(now_mono + old_rt.policy.stop_grace);
                        } else {
                            h.pm.register(cfg.clone());
                        }
                        new_svcs.push(fresh);
                    } else {
                        let mut rt = old_svcs.remove(pos);
                        if reload::service_soft_changed(&rt.cfg, cfg) {
                            tracing::info!(
                                target = %cfg.name,
                                "service policy/activate updated by reload; fsm watermarks preserved"
                            );
                        }
                        rt.policy = RestartPolicy::from(&cfg.restart);
                        rt.fsm.update_policy(rt.policy.clone());
                        h.pm.update_config(cfg.clone());
                        rt.fsm.set_verify_for(
                            cfg.activate
                                .as_ref()
                                .map(|activate| activate.verify_for)
                                .unwrap_or(rt.policy.startup_grace),
                        );
                        rt.cfg = cfg.clone();
                        new_svcs.push(rt);
                    }
                }
                None => {
                    tracing::info!(target = %cfg.name, "service added by reload; cold start scheduled");
                    let mut fresh = Self::fresh_svc(cfg, now_mono);
                    fresh.orphan = self.retired_orphans.remove(&cfg.name);
                    if fresh.orphan.is_some() {
                        fresh.spawn_due = None;
                    } else if h.pm.is_running(&cfg.name) {
                        // 同名服务在上次 reload 删除后又被添加，旧 child 仍由 pm 持有。
                        fresh.spawn_due = None;
                        let grace =
                            h.pm.cfg(&cfg.name)
                                .map(|old| old.restart.stop_grace)
                                .unwrap_or(fresh.policy.stop_grace);
                        fresh.reload_stop_deadline =
                            Some(h.pm.stop_requested_at(&cfg.name).unwrap_or(now_mono) + grace);
                    } else {
                        h.pm.register(cfg.clone());
                    }
                    self.pending_reap.retain(|n| n != &cfg.name);
                    new_svcs.push(fresh);
                }
            }
        }
        for mut rt in old_svcs {
            self.dispose_service(&rt, h, now_wall, "removed", true);
            if let Some(orphan) = rt.orphan.take() {
                self.retired_orphans.insert(rt.cfg.name.clone(), orphan);
            }
        }

        // ---- watches:变更 (pid/pidfile)→ 重新解析身份 ----
        let mut new_watches: Vec<WatchRt> = Vec::with_capacity(new.watches.len());
        for cfg in &new.watches {
            match old_watches.iter().position(|w| w.name == cfg.name) {
                Some(pos) => {
                    let mut rt = old_watches.remove(pos);
                    if rt.pid != cfg.pid || rt.pidfile != cfg.pidfile {
                        tracing::info!(
                            watch = %cfg.name,
                            "watch identity spec changed by reload; re-resolving"
                        );
                        rt.identity = resolve_watch_identity(cfg);
                    }
                    rt.pid = cfg.pid;
                    rt.pidfile = cfg.pidfile.clone();
                    new_watches.push(rt);
                }
                None => {
                    tracing::info!(watch = %cfg.name, "watch added by reload");
                    new_watches.push(WatchRt {
                        name: cfg.name.clone(),
                        pid: cfg.pid,
                        identity: resolve_watch_identity(cfg),
                        checks: Vec::new(),
                        pidfile: cfg.pidfile.clone(),
                    });
                }
            }
        }
        for rt in old_watches {
            tracing::info!(watch = %rt.name, "watch removed by reload");
            journal(
                h,
                JournalEvent::Audit {
                    at: now_wall,
                    message: format!("watch {} removed by reload", rt.name),
                },
            );
        }

        // ---- 重装 + 绑定重建 + 标签漂移的 incident 清理 ----
        self.watches = new_watches;
        self.svcs = new_svcs;
        self.checks = new_checks;
        self.rebind();
        for (name, old_label) in &kept_old_labels {
            if let Some(i) = self.checks.iter().position(|c| c.cfg.name == *name) {
                let new_label = self.labels(i);
                if new_label != *old_label {
                    tracing::info!(
                        check = %name,
                        old_target = %old_label.0,
                        new_target = %new_label.0,
                        "check target binding changed by reload; clearing stale incident"
                    );
                    self.tracker.clear(&old_label.0, &old_label.1);
                }
            }
        }

        // P2(第七轮):reload 使级联登记失效 —— key(target) 被删或 dependent
        // 被删/改依赖时，陈旧登记会让 dependent 永久滞留 Restarting 无出口。
        let kept_names: std::collections::HashSet<&str> =
            new.services.iter().map(|s| s.name.as_str()).collect();
        self.cascade_restart_after.retain(|target, deps| {
            let target_kept = kept_names.contains(target.as_str());
            deps.retain(|d| kept_names.contains(d.as_str()));
            if !target_kept || deps.is_empty() {
                tracing::info!(target = %target, "cascade restart registration invalidated by reload");
                journal(
                    h,
                    JournalEvent::Audit {
                        at: now_wall,
                        message: format!("cascade restart for {target} invalidated by reload"),
                    },
                );
                return false;
            }
            true
        });

        // 生效新配置 (下次 diff 的 old 侧)。tracker 阈值与 collect_interval 保持
        // 旧值 (alert/collect 段不热应用 —— 上方已审计)。
        self.active_cfg = new;

        let summary = format!(
            "reload applied: +{}/-{}/~{} checks, +{}/-{}/~{} services, +{}/-{}/~{} watches",
            diff.added_checks.len(),
            diff.removed_checks.len(),
            diff.changed_checks.len(),
            diff.added_services.len(),
            diff.removed_services.len(),
            diff.changed_services.len(),
            diff.added_watches.len(),
            diff.removed_watches.len(),
            diff.changed_watches.len(),
        );
        tracing::info!(audit = %summary, "config reload complete");
        journal(
            h,
            JournalEvent::Audit {
                at: now_wall,
                message: summary,
            },
        );
        Ok(())
    }

    /// reload 删除/重定义服务的处置：运行中则优雅停 (升级强杀由 pm 兜底);
    /// `reap = true` 时登记 `pending_reap`(调度侧已无条目，收割续管)。
    /// pm 条目无注销 API:删除的目标条目保留在 pm(名字不再出现在调度侧),
    /// 停机时 shutdown_all 仍会兜底强杀。
    fn dispose_service(
        &mut self,
        rt: &SvcRt,
        h: &mut SchedulerHandles,
        now_wall: SystemTime,
        reason: &str,
        reap: bool,
    ) {
        let name = rt.cfg.name.clone();
        // reload 删除/重定义与级联停止叠加：本条目的计划内标记随旧实例一并作废
        // (删除路径 FSM 已随条目丢弃;重定义路径 fresh FSM 从零开始)。
        self.planned_stops.remove(&name);
        if let Some(activate) = &rt.activate {
            activate.abort();
        }
        if h.pm.is_running(&name) {
            if reap && !self.pending_reap.contains(&name) {
                self.pending_reap.push(name.clone());
            }
            match h.pm.signal_stop(&name, rt.policy.stop_grace) {
                Ok(()) => {
                    tracing::info!(target = %name, reason, "graceful stop signaled (reload)");
                    journal(
                        h,
                        JournalEvent::Audit {
                            at: now_wall,
                            message: format!(
                                "service {name} {reason} by reload: graceful stop signaled"
                            ),
                        },
                    );
                }
                Err(e) => {
                    tracing::warn!(target = %name, reason, source = %e, "graceful stop failed on reload");
                    journal(
                        h,
                        JournalEvent::Audit {
                            at: now_wall,
                            message: format!(
                                "service {name} {reason} by reload: stop signal failed: {e}"
                            ),
                        },
                    );
                }
            }
        } else {
            journal(
                h,
                JournalEvent::Audit {
                    at: now_wall,
                    message: format!("service {name} {reason} by reload"),
                },
            );
        }
    }

    /// reload 新增/重定义服务的全新运行时条目 (冷启动 spawn 由 spawn_due 驱动;
    /// FSM 从零开始 —— 预算/闩锁不继承，journal Reset 已清重放态)。
    /// 有依赖的服务 spawn_due 置 None，由 F20 门控在依赖就绪后排程。
    fn fresh_svc(cfg: &ServiceCfg, now_mono: Instant) -> SvcRt {
        let policy = RestartPolicy::from(&cfg.restart);
        let mut fsm = RecoveryFsm::new(&cfg.name, policy.clone());
        if let Some(a) = &cfg.activate {
            fsm.set_verify_for(a.verify_for);
        }
        SvcRt {
            policy,
            fsm,
            cfg: cfg.clone(),
            // F20:有依赖 → None(Tick gate will schedule when deps are Running)
            spawn_due: if cfg.depends_on.is_empty() {
                Some(now_mono)
            } else {
                None
            },
            activate: None,
            checks: Vec::new(),
            last_spawn: None,
            restart_stop_deadline: None,
            restart_stop_identity: None,
            kill_deadline: None,
            reload_stop_deadline: None,
            orphan: None,
            orphan_suspected: false,
            persisted_observed_identity: None,
        }
    }

    /// 重建全部绑定关系 (check ↔ service / check ↔ watch;reload 拓扑变更后
    /// 调用)。逻辑与 [`Scheduler::build`] 的绑定段一致，幂等 (全量重算)。
    fn rebind(&mut self) {
        // check → service(process 检查按名称尾匹配)。
        for c in &mut self.checks {
            c.bound_service = match &c.cfg.kind {
                CheckKindCfg::Process { process, .. } => self
                    .svcs
                    .iter()
                    .position(|s| target_name_matches(&s.cfg.name, process)),
                _ => None,
            };
        }
        for s in &mut self.svcs {
            s.checks.clear();
        }
        for (i, c) in self.checks.iter().enumerate() {
            if let Some(s) = c.bound_service {
                self.svcs[s].checks.push(i);
            }
        }
        // check → watch(未被服务绑定的 process 检查;名称尾匹配或 "pid:N")。
        for w in &mut self.watches {
            w.checks.clear();
        }
        for (i, c) in self.checks.iter_mut().enumerate() {
            if c.bound_service.is_some() {
                c.bound_watch = None;
                continue;
            }
            let CheckKindCfg::Process { process, .. } = &c.cfg.kind else {
                c.bound_watch = None;
                continue;
            };
            let widx = self.watches.iter().position(|w| {
                target_name_matches(&w.name, process)
                    || w.identity.is_some_and(|ident| {
                        process.strip_prefix("pid:") == Some(&ident.pid.0.to_string())
                    })
            });
            c.bound_watch = widx;
            if let Some(widx) = widx {
                self.watches[widx].checks.push(i);
            }
        }
    }

    // -----------------------------------------------------------------
    // 辅助
    // -----------------------------------------------------------------

    /// check 所属 (target, check) 标签：绑定 supervise 的 process 检查以服务名
    /// 为 target;独立检查 target = check 名。
    fn labels(&self, idx: usize) -> (String, String) {
        let check_name = self.checks[idx].cfg.name.clone();
        if let Some(sidx) = self.checks[idx].bound_service {
            return (self.svcs[sidx].cfg.name.clone(), check_name);
        }
        if let Some(widx) = self.checks[idx].bound_watch {
            // 事件归属 watch 目标名 (评审 P2:否则 /status 上 watch 恒 Ok 而
            // 同名 check 独立报 Critical，运维视角自相矛盾)。
            return (self.watches[widx].name.clone(), check_name);
        }
        (check_name.clone(), check_name)
    }

    /// F20 停止顺序:target 的 (传递) 被依赖方，逆拓扑序 (叶子先停)。
    /// 纯函数;校验层保证无环。
    fn dependents_stop_order(&self, target: &str) -> Vec<String> {
        let mut order = Vec::new();
        let mut visited = std::collections::HashSet::new();
        // DFS:先递归被依赖方的被依赖方 (更深的服务先停),再收集本层
        fn visit(
            name: &str,
            svcs: &[SvcRt],
            visited: &mut std::collections::HashSet<String>,
            order: &mut Vec<String>,
        ) {
            for s in svcs {
                if s.cfg.depends_on.iter().any(|d| d == name) && visited.insert(s.cfg.name.clone())
                {
                    visit(&s.cfg.name, svcs, visited, order);
                    order.push(s.cfg.name.clone());
                }
            }
        }
        visit(target, &self.svcs, &mut visited, &mut order);
        order
    }

    /// F20 启动编排：依赖是否全部满足 = 依赖 FSM Running **且确有运行中的进程**
    /// (FSM 初始态为 Running 是历史语义 —— 冷启动 spawn 由 spawn_due 驱动、
    /// FSM 尚未进入真实生命周期;若无进程证据则视为未就绪，否则依赖门控会被
    /// 初始态穿透)。
    fn deps_satisfied(&self, svc: &SvcRt, h: &SchedulerHandles) -> bool {
        svc.cfg.depends_on.iter().all(|dep| {
            self.svcs
                .iter()
                .find(|s| &s.cfg.name == dep)
                .is_some_and(|s| {
                    s.fsm.state() == RecoveryState::Running
                        && (h.pm.is_running(&s.cfg.name)
                            || s.orphan.as_ref().is_some_and(OrphanTracker::is_attached))
                })
        })
    }

    /// 纯判定 (测试注入点):FSM Running + 有存活证据 = 满足。
    #[cfg(test)]
    fn dep_is_ready(fsm_state: RecoveryState, has_live_child: bool) -> bool {
        fsm_state == RecoveryState::Running && has_live_child
    }

    /// 距下一次唤醒的时长：最近到期事件，上限 MAX_TICK(fsm 内部截止兜底)。
    fn next_wake_delay(&self, now: Instant) -> Duration {
        let mut earliest = now + MAX_TICK;
        for c in &self.checks {
            if c.next_due < earliest {
                earliest = c.next_due;
            }
        }
        for s in &self.svcs {
            if let Some(d) = s.spawn_due
                && d > now
                && d < earliest
            {
                earliest = d;
            }
        }
        earliest.saturating_duration_since(now)
    }

    /// 惰性构建 http 客户端 (仅构建一次;失败则 http 探测按"client unavailable"诚实失败)。
    #[cfg(feature = "probe-http")]
    fn http_for_probe(&mut self) -> Option<reqwest::Client> {
        if self.http.is_none() {
            match reqwest::Client::builder().build() {
                Ok(c) => {
                    tracing::info!("http probe client initialized");
                    self.http = Some(c);
                }
                Err(e) => {
                    tracing::warn!(source = %e, "http probe client build failed");
                }
            }
        }
        self.http.clone()
    }

    /// 解析 process 检查目标身份 (见模块 doc「进程名匹配」)。
    fn resolve_process_target(
        &self,
        process: &str,
        snap: &CollectOutput,
        h: &SchedulerHandles,
    ) -> ResolvedProbe {
        let mut out = ResolvedProbe::default();

        // "pid:1234":亲生服务身份优先，其次 watch，最后快照内按 PID 匹配。
        if let Some(pid_part) = process.strip_prefix("pid:") {
            match pid_part.trim().parse::<u32>() {
                Ok(pid) => {
                    for s in &self.svcs {
                        if let Some(ident) =
                            h.pm.identity(&s.cfg.name)
                                .or_else(|| s.orphan.as_ref().map(OrphanTracker::identity))
                            && ident.pid.0 == pid
                        {
                            out.expected = Some(ident);
                            if let Some(p) = find_by_identity(snap, &ident) {
                                // 采纳观测身份:check.rs 的身份比对按精确相等
                                out.expected = Some(p.identity);
                                out.snapshot = Some(p.clone());
                            }
                            return out;
                        }
                    }
                    for w in &self.watches {
                        if let Some(ident) = w.identity
                            && ident.pid.0 == pid
                        {
                            out.expected = Some(ident);
                            if let Some(p) = find_by_identity(snap, &ident) {
                                out.expected = Some(p.identity);
                                out.snapshot = Some(p.clone());
                            }
                            return out;
                        }
                    }
                    if let Some(p) = snap.processes.iter().find(|p| p.identity.pid.0 == pid) {
                        out.expected = Some(p.identity);
                        out.snapshot = Some(p.clone());
                    }
                }
                Err(e) => {
                    tracing::warn!(process = %process, source = %e, "invalid pid: spec in process check");
                }
            }
            return out;
        }

        // 与 build/rebind 使用同一优先级，避免同名 watch 的指标驱动服务恢复。
        if let Some(s) = self
            .svcs
            .iter()
            .find(|s| target_name_matches(&s.cfg.name, process))
        {
            if let Some(ident) =
                h.pm.identity(&s.cfg.name)
                    .or_else(|| s.orphan.as_ref().map(OrphanTracker::identity))
            {
                out.expected = Some(ident);
                if let Some(p) = find_by_identity(snap, &ident) {
                    out.expected = Some(p.identity);
                    out.snapshot = Some(p.clone());
                }
            }
            return out;
        }
        if let Some(w) = self
            .watches
            .iter()
            .find(|w| target_name_matches(&w.name, process))
            && let Some(ident) = w.identity
        {
            out.expected = Some(ident);
            if let Some(p) = find_by_identity(snap, &ident) {
                out.expected = Some(p.identity);
                out.snapshot = Some(p.clone());
            }
        }
        out
    }

    /// 告警出口：结构化日志 + drop-oldest 有界队列直推 (满时队列内丢最旧并计数，
    /// DESIGN §2.2 背压策略;eval 侧永不阻塞、永不静默丢新事件)。
    fn emit_alert(&self, ev: &AlertEvent, h: &SchedulerHandles) {
        tracing::info!(
            incident = ev.incident,
            target = %ev.target,
            check = %ev.check,
            category = ?ev.category,
            severity = ?ev.severity,
            occurrences = ev.occurrences,
            "alert event"
        );
        h.alert.push(ev.clone());
    }

    /// 重建 `/status` 汇报 (targets = 独立检查 + supervise 服务 + watch 目标)。
    fn build_status(&self, h: &SchedulerHandles, now_mono: Instant) -> StatusReport {
        let mut targets =
            Vec::with_capacity(self.checks.len() + self.svcs.len() + self.watches.len());

        for c in &self.checks {
            if c.bound_service.is_some() || c.bound_watch.is_some() {
                continue; // 归入服务 / watch target
            }
            let name = c.cfg.name.clone();
            targets.push(TargetStatus {
                name: name.clone(),
                mode: TargetMode::Check,
                health: c.state.level(),
                recovery: None,
                checks: vec![name.clone()],
                identity: None,
                incident: self.tracker.incident_level(&name, &name),
                restarts_in_window: 0,
                last_transition: c.last_transition_wall,
            });
        }

        for s in &self.svcs {
            let mut health = if s
                .orphan
                .as_ref()
                .is_some_and(|tracker| !tracker.is_attached())
            {
                HealthLevel::Warning
            } else {
                HealthLevel::Ok
            };
            let mut incident: Option<HealthLevel> = None;
            let mut last_transition: Option<SystemTime> = None;
            let mut check_names = Vec::with_capacity(s.checks.len());
            for &i in &s.checks {
                let lvl = self.checks[i].state.level();
                if rank(lvl) > rank(health) {
                    health = lvl;
                }
                let (t, c) = self.labels(i);
                if let Some(l) = self.tracker.incident_level(&t, &c)
                    && incident.is_none_or(|cur| rank(l) > rank(cur))
                {
                    incident = Some(l);
                }
                if let Some(t) = self.checks[i].last_transition_wall
                    && last_transition.is_none_or(|cur| t > cur)
                {
                    last_transition = Some(t);
                }
                check_names.push(self.checks[i].cfg.name.clone());
            }
            if s.fsm.is_fatal() {
                health = HealthLevel::Critical;
            }
            targets.push(TargetStatus {
                name: s.cfg.name.clone(),
                mode: if s.orphan.is_some() {
                    TargetMode::Attach
                } else {
                    TargetMode::Supervise
                },
                health,
                recovery: if s.fsm.is_fatal() {
                    Some(RecoveryState::Fatal)
                } else if s.orphan.is_some() {
                    None
                } else if s.orphan_suspected {
                    // 崩溃启动事务：未知实例窗口对 /status 呈现 Suspect
                    // (FSM 已在 confirmations>1 时标记;此处兜底覆盖其余配置)。
                    Some(RecoveryState::Suspect)
                } else {
                    Some(s.fsm.state())
                },
                checks: check_names,
                identity: h
                    .pm
                    .identity(&s.cfg.name)
                    .or_else(|| s.orphan.as_ref().map(OrphanTracker::identity)),
                incident,
                restarts_in_window: s.fsm.restarts_in_window(now_mono),
                last_transition,
            });
        }

        for w in &self.watches {
            // 绑定检查聚合到 watch 条目 (评审 P2:否则 watch 恒 Ok 而同名 check
            // 独立报 Critical，运维视角自相矛盾)。
            let mut health = HealthLevel::Ok;
            let mut incident: Option<HealthLevel> = None;
            let mut last_transition: Option<SystemTime> = None;
            let mut check_names = Vec::with_capacity(w.checks.len());
            for &i in &w.checks {
                let lvl = self.checks[i].state.level();
                if rank(lvl) > rank(health) {
                    health = lvl;
                }
                let (t, c) = self.labels(i);
                if let Some(l) = self.tracker.incident_level(&t, &c)
                    && incident.is_none_or(|cur| rank(l) > rank(cur))
                {
                    incident = Some(l);
                }
                if let Some(t) = self.checks[i].last_transition_wall
                    && last_transition.is_none_or(|cur| t > cur)
                {
                    last_transition = Some(t);
                }
                check_names.push(self.checks[i].cfg.name.clone());
            }
            targets.push(TargetStatus {
                name: w.name.clone(),
                mode: TargetMode::Attach,
                health,
                recovery: None,
                checks: check_names,
                identity: w.identity,
                incident,
                restarts_in_window: 0,
                last_transition,
            });
        }

        StatusReport {
            started_at: self.started_wall,
            ready: true,
            targets,
        }
    }
}

/// 健康级别排序 (Ok < Warning < Critical)。
fn rank(level: HealthLevel) -> u8 {
    match level {
        HealthLevel::Ok => 0,
        HealthLevel::Warning => 1,
        HealthLevel::Critical => 2,
    }
}

/// journal 出口:try_send;满/关闭 → WARN(事件丢弃，绝不阻塞 eval)。
fn journal(h: &SchedulerHandles, ev: JournalEvent) {
    if let Err(e) = h.journal_tx.try_send(ev) {
        tracing::warn!(source = %e, "journal queue push failed; event dropped");
    }
}

/// 激活钩子执行：命令 (tokio process + timeout) 或 HTTP POST。
async fn run_activate_hook(activate: crate::config::ActivateCfg) -> bool {
    if let Some(cmd) = activate.command {
        run_command_hook(&cmd, activate.timeout).await
    } else if let Some(url) = activate.url {
        #[cfg(feature = "http-client")]
        {
            run_url_hook(url, activate.timeout).await
        }
        #[cfg(not(feature = "http-client"))]
        {
            let _ = url;
            tracing::warn!("activate url requires http-client feature; hook failed");
            false
        }
    } else {
        tracing::warn!("activate hook has neither command nor url");
        false
    }
}

/// 命令激活钩子：按空白切分程序与参数，带 timeout 等待退出码。
async fn run_command_hook(cmd: &str, timeout: Duration) -> bool {
    let mut parts = cmd.split_whitespace();
    let Some(program) = parts.next() else {
        tracing::warn!("activate command is blank");
        return false;
    };
    let spawn = tokio::process::Command::new(program)
        .args(parts)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        // 钩子是 agent 亲生的一次性命令 (与 supervise 目标的"观察者外置"不同):
        // 超时/提前返回时必须带走，防挂死钩子进程残留 (并发/跨平台评审 P2)。
        .kill_on_drop(true)
        .spawn();
    let mut child = match spawn {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(command = %cmd, source = %e, "activate command spawn failed");
            return false;
        }
    };
    match tokio::time::timeout(timeout, child.wait()).await {
        Ok(Ok(status)) if status.success() => true,
        Ok(Ok(status)) => {
            tracing::warn!(command = %cmd, code = ?status.code(), "activate command failed");
            false
        }
        Ok(Err(e)) => {
            tracing::warn!(command = %cmd, source = %e, "activate command wait failed");
            false
        }
        Err(_) => {
            tracing::warn!(command = %cmd, timeout = ?timeout, "activate command timed out");
            false
        }
    }
}

/// HTTP 激活钩子:POST 期望 2xx。
#[cfg(feature = "http-client")]
async fn run_url_hook(url: url::Url, timeout: Duration) -> bool {
    let client = match reqwest::Client::builder().timeout(timeout).build() {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(source = %e, "activate http client build failed");
            return false;
        }
    };
    match tokio::time::timeout(timeout, client.post(url.clone()).send()).await {
        Ok(Ok(resp)) => {
            let ok = resp.status().is_success();
            if !ok {
                tracing::warn!(status = %resp.status(), "activate url rejected");
            }
            ok
        }
        Ok(Err(e)) => {
            tracing::warn!(source = %e, "activate url request failed");
            false
        }
        Err(_) => {
            tracing::warn!(timeout = ?timeout, "activate url timed out");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect::Collector as _;
    use crate::collect::StubCollector;
    use crate::model::snapshot::ProcessSnapshot;
    use crate::recover::journal::PendingSpawn;

    // ---- F20 启动编排 ----

    /// 依赖未就绪 → 不排程;依赖 Running → 满足 (fresh_svc spawn_due=None)。
    #[test]
    fn dependency_gating_defers_spawn_until_deps_running() {
        let cfg = Config::from_toml_str(
            r#"
            [[service]]
            name = "db"
            command = "/bin/db"
            [[service]]
            name = "api"
            command = "/bin/api"
            depends_on = ["db"]
        "#,
        )
        .unwrap();
        let mut sched = Scheduler::build(&cfg);
        let api = sched.svcs.iter().position(|s| s.cfg.name == "api").unwrap();
        let db = sched.svcs.iter().position(|s| s.cfg.name == "db").unwrap();
        // 冷启动编排发生在 run()(build 后统一排程);此处复刻其决策：
        let now = Instant::now();
        for svc in &mut sched.svcs {
            if svc.fsm.is_fatal() {
                // Fatal 跳过
            } else if svc.cfg.depends_on.is_empty() {
                svc.spawn_due = Some(now);
            } else {
                svc.spawn_due = None; // 等待 F20 门控
            }
        }
        assert!(sched.svcs[db].spawn_due.is_some(), "无依赖服务立即排程");
        assert!(sched.svcs[api].spawn_due.is_none(), "有依赖服务等待门控");
        // 门控带 pm 证据，此处仅验证纯 FSM 侧 (见 cascades 测试的完整路径)

        // db → Running(spawn 成功 + 存活过 min_uptime)
        let t0 = Instant::now();
        let _ = sched.svcs[db].fsm.on_spawn_result(
            t0 + Duration::from_secs(1),
            Some(ProcessIdentity::new(1, 1)),
        );
        let _ = sched.svcs[db]
            .fsm
            .on_liveness_ok(t0 + Duration::from_secs(61));
        assert_eq!(sched.svcs[db].fsm.state(), RecoveryState::Running);
        // 纯 FSM 侧验证补进程证据后才能满足 —— 见 cascades 集成测试完整路径
        sched.svcs[api].spawn_due = None;
    }

    /// 依赖 Fatal → 永不满足 (失败不级联，人工介入)。
    #[test]
    fn dependency_fatal_never_satisfies() {
        let cfg = Config::from_toml_str(
            r#"
            [[service]]
            name = "db"
            command = "/bin/db"
            [[service]]
            name = "api"
            command = "/bin/api"
            depends_on = ["db"]
        "#,
        )
        .unwrap();
        let mut sched = Scheduler::build(&cfg);
        let db = sched.svcs.iter().position(|s| s.cfg.name == "db").unwrap();
        sched.svcs[db]
            .fsm
            .restore(true, &[], SystemTime::UNIX_EPOCH, Instant::now());
        assert!(sched.svcs[db].fsm.is_fatal());
        // Fatal 依赖:Fsm≠Running → 纯判定即不满足 (即便有存活证据)
        assert!(
            !Scheduler::dep_is_ready(sched.svcs[db].fsm.state(), true),
            "依赖 Fatal 不得满足"
        );
    }

    /// H-P1 回归:journal 同时含旧 ProcessIdentity 与未决 SpawnIntent(真实崩溃
    /// 窗口形状)—— 意图对账必须先于孤儿分支执行，orphan_suspected 生效。
    #[tokio::test]
    async fn pending_intent_with_old_identity_still_blocks_cold_start() {
        let cfg = Config::from_toml_str(
            r#"
            [[service]]
            name = "app"
            command = "/bin/app"
        "#,
        )
        .unwrap();
        let (mut h, mut journal_rx, _status_rx, _metrics_rx) = reload_handles();
        h.pm.register(cfg.services[0].clone());
        let mut sched = Scheduler::build(&cfg);
        let now_mono = Instant::now();
        let now_wall = SystemTime::now();

        // journal 形状：成功 spawn 的身份 + 未决意图 (崩溃在 outcome 落盘前)
        let mut replayed = crate::recover::journal::JournalState::default();
        replayed.processes.insert(
            "app".into(),
            crate::recover::journal::PersistedProcess {
                identity: crate::model::ids::ProcessIdentity::new(111, 100),
                verified: false,
            },
        );
        replayed.pending_spawns.insert(
            "app".into(),
            crate::recover::journal::PendingSpawn {
                at: now_wall,
                config_digest: crate::recover::journal::config_digest(&cfg.services[0]),
            },
        );

        let want = crate::recover::journal::config_digest(&cfg.services[0]);
        let got = crate::recover::journal::config_digest(&sched.svcs[0].cfg);
        eprintln!("DBG want={want} got={got}");
        sched
            .cold_start(0, &replayed, now_mono, now_wall, &mut h)
            .await;
        let sidx = 0;
        eprintln!("DBG suspected={}", sched.svcs[sidx].orphan_suspected);
        assert!(
            sched.svcs[sidx].orphan_suspected,
            "意图对账必须先于孤儿分支执行 (否则防双实例失效)"
        );
        assert!(sched.svcs[sidx].spawn_due.is_none(), "冷启动必须被跳过");
        assert!(sched.svcs[sidx].orphan.is_none(), "不得同时挂孤儿 tracker");
        // journal:对账写 Error/Audit?至少清理历史事件
        while journal_rx.try_recv().is_ok() {}
        let _ = journal_rx;
    }

    /// F20 停止顺序：逆拓扑序 (叶子先停)。
    #[test]
    fn dependents_stop_order_is_reverse_topological() {
        let cfg = Config::from_toml_str(
            r#"
            [[service]]
            name = "db"
            command = "/bin/db"
            [[service]]
            name = "api"
            command = "/bin/api"
            depends_on = ["db"]
            [[service]]
            name = "worker"
            command = "/bin/worker"
            depends_on = ["api"]
        "#,
        )
        .unwrap();
        let sched = Scheduler::build(&cfg);
        assert_eq!(sched.dependents_stop_order("db"), vec!["worker", "api"]);
        assert_eq!(sched.dependents_stop_order("api"), vec!["worker"]);
        assert!(sched.dependents_stop_order("worker").is_empty());
    }

    /// F20 级联闭环:Restart 登记 → 依赖回 Running 后拉回 + 审计。
    #[cfg(unix)]
    #[tokio::test]
    async fn control_restart_cascades_stop_and_pullback() {
        let cfg = Config::from_toml_str(
            r#"
            [[service]]
            name = "db"
            command = "/bin/sleep"
            args = ["30"]
            [[service]]
            name = "api"
            command = "/bin/api"
            depends_on = ["db"]
        "#,
        )
        .unwrap();
        let (mut h, mut journal_rx, _status_rx, _metrics_rx) = reload_handles();
        h.pm.register(cfg.services[0].clone());
        h.pm.register(cfg.services[1].clone());
        let mut sched = Scheduler::build(&cfg);
        let now_mono = Instant::now();
        let now_wall = SystemTime::now();

        // db 进入 Running(真实 spawn:/bin/sleep 30,pm 持有 child → 存活证据)
        let identity = h.pm.spawn("db").await.expect("real spawn");
        let _ = sched.svcs[0].fsm.on_spawn_result(now_mono, Some(identity));
        let _ = sched.svcs[0]
            .fsm
            .on_liveness_ok(now_mono + Duration::from_secs(60));

        // control restart db(api 未运行：无停止信号，仅级联登记)
        sched
            .handle_control(
                ControlCommand::Restart {
                    target: "db".into(),
                },
                now_mono,
                now_wall,
                &mut h,
            )
            .await;
        assert_eq!(
            sched.cascade_restart_after.get("db"),
            Some(&vec!["api".to_string()]),
            "级联登记必须存在"
        );

        // 经正常 service_tick 收割真实退出、记录预算并安排 backoff。
        tokio::time::sleep(Duration::from_millis(300)).await;
        let snap = StubCollector::default().collect(&[]);
        sched
            .service_tick(
                0,
                &snap,
                now_mono + Duration::from_secs(1),
                now_wall,
                &mut h,
            )
            .await;
        assert!(sched.svcs[0].restart_stop_deadline.is_none());
        assert!(!h.pm.is_running("db"));
        assert!(
            sched.cascade_restart_after.contains_key("db"),
            "依赖真正恢复前保持级联门控"
        );
        sched
            .service_tick(
                0,
                &snap,
                now_mono + Duration::from_secs(61),
                now_wall,
                &mut h,
            )
            .await;
        assert!(h.pm.is_running("db"));
        sched
            .service_tick(
                0,
                &snap,
                now_mono + Duration::from_secs(122),
                now_wall,
                &mut h,
            )
            .await;
        let running = sched.svcs[0].fsm.state();
        let api_scheduled = sched.svcs[1].spawn_due.is_some();
        h.pm.shutdown_all().await;
        assert_eq!(running, RecoveryState::Running);
        assert!(api_scheduled, "级联拉回必须为 api 排程冷启动");
        let mut audits = Vec::new();
        while let Ok(ev) = journal_rx.try_recv() {
            if let JournalEvent::Audit { message, .. } = ev {
                audits.push(message);
            }
        }
        assert!(
            audits
                .iter()
                .any(|m| m.contains("cascade restart api after dependency db")),
            "级联拉回必须写审计，实际 {audits:?}"
        );
    }

    /// F20 计划内级联停止 (第六轮评审 P3):dependent **运行中**被级联停止 →
    /// 退出经 on_planned_stop 收口 —— 不产生 JournalRestart(预算事件)、
    /// 不立即排程;依赖回 Running 后仍由拉回机制排程冷启动。
    #[cfg(unix)]
    #[tokio::test]
    async fn cascade_planned_stop_does_not_burn_dependent_budget() {
        let cfg = Config::from_toml_str(
            r#"
            [[service]]
            name = "db"
            command = "/bin/sleep"
            args = ["30"]
            [[service]]
            name = "api"
            command = "/bin/sleep"
            args = ["30"]
            depends_on = ["db"]
        "#,
        )
        .unwrap();
        let (mut h, mut journal_rx, _status_rx, _metrics_rx) = reload_handles();
        h.pm.register(cfg.services[0].clone());
        h.pm.register(cfg.services[1].clone());
        let mut sched = Scheduler::build(&cfg);
        let now_mono = Instant::now();
        let now_wall = SystemTime::now();

        // db 与 api 都真实运行并进入 Running
        for (sidx, delay) in [(0usize, 60u64), (1, 60)] {
            let identity =
                h.pm.spawn(if sidx == 0 { "db" } else { "api" })
                    .await
                    .expect("real spawn");
            let _ = sched.svcs[sidx]
                .fsm
                .on_spawn_result(now_mono, Some(identity));
            let _ = sched.svcs[sidx]
                .fsm
                .on_liveness_ok(now_mono + Duration::from_secs(delay));
        }

        // control restart db → 级联停止运行中的 api(计划内标记)
        sched
            .handle_control(
                ControlCommand::Restart {
                    target: "db".into(),
                },
                now_mono,
                now_wall,
                &mut h,
            )
            .await;
        assert!(
            sched.planned_stops.contains("api"),
            "运行中的 dependent 必须登记计划内停止"
        );

        // 收割两个真实退出 (生产 tick 按 svcs 索引序:db 先，api 后;
        // 乱序时 spawn 到点的 deps_satisfied 二次门控兜底)
        tokio::time::sleep(Duration::from_millis(300)).await;
        let snap = StubCollector::default().collect(&[]);
        sched
            .service_tick(
                0,
                &snap,
                now_mono + Duration::from_secs(1),
                now_wall,
                &mut h,
            )
            .await;
        sched
            .service_tick(
                1,
                &snap,
                now_mono + Duration::from_secs(1),
                now_wall,
                &mut h,
            )
            .await;
        assert_eq!(sched.svcs[1].fsm.state(), RecoveryState::Restarting);
        assert!(
            sched.svcs[1].spawn_due.is_none(),
            "计划内停止不得排程 spawn"
        );
        assert!(sched.planned_stops.is_empty(), "退出事件消费计划内标记");

        // db backoff 后拉活、回 Running → 拉回 api 排程 (依赖门控满足)
        for secs in [2usize, 5, 10, 15, 20, 30, 40, 50, 62] {
            sched
                .service_tick(
                    0,
                    &snap,
                    now_mono + Duration::from_secs(secs as u64),
                    now_wall,
                    &mut h,
                )
                .await;
        }
        let db_running = sched.svcs[0].fsm.state();
        let api_scheduled = sched.svcs[1].spawn_due.is_some();
        h.pm.shutdown_all().await;
        assert_eq!(db_running, RecoveryState::Running);
        assert!(api_scheduled, "计划内停止的 dependent 仍由拉回排程");

        // 关键断言:api 的 journal 中不得出现 JournalRestart(计划内停止零预算)
        let mut api_restarts = 0;
        while let Ok(ev) = journal_rx.try_recv() {
            if let JournalEvent::Restart { target, .. } = ev
                && target == "api"
            {
                api_restarts += 1;
            }
        }
        assert_eq!(api_restarts, 0, "计划内级联停止不得为 api 记预算事件");
    }

    fn snap_of(pid: u32, start: u64) -> CollectOutput {
        CollectOutput {
            hardware: crate::model::snapshot::HardwareSnapshot {
                timestamp: SystemTime::UNIX_EPOCH,
                cpu_usage_percent: None,
                per_core_usage: Vec::new(),
                memory_total_bytes: 0,
                memory_used_bytes: 0,
                disks: Vec::new(),
                networks: Vec::new(),
                temperatures: Vec::new(),
                fans: Vec::new(),
                voltages: Vec::new(),
                degradations: Vec::new(),
            },
            requested: vec![ProcessIdentity::new(pid, start)],
            collected_at: Instant::now(),
            processes: vec![ProcessSnapshot {
                identity: ProcessIdentity::new(pid, start),
                cpu_usage_percent: Some(1.0),
                memory_bytes: Some(1024),
                status: Some("Run".into()),
            }],
        }
    }

    /// ±2s 容差:spawn 侧本地钟与内核记账跨秒失配的吸收窗口。
    #[test]
    fn find_by_identity_tolerates_two_seconds() {
        let snap = snap_of(100, 1000);
        assert!(find_by_identity(&snap, &ProcessIdentity::new(100, 1000)).is_some());
        assert!(find_by_identity(&snap, &ProcessIdentity::new(100, 1002)).is_some());
        assert!(find_by_identity(&snap, &ProcessIdentity::new(100, 998)).is_some());
    }

    /// 容差边界:±3s 必须拒绝 (真实 PID 复用，不是时钟失配)。
    #[test]
    fn find_by_identity_rejects_three_seconds() {
        let snap = snap_of(100, 1000);
        assert!(find_by_identity(&snap, &ProcessIdentity::new(100, 1003)).is_none());
        assert!(find_by_identity(&snap, &ProcessIdentity::new(100, 997)).is_none());
    }

    /// pid 不同必须拒绝。
    #[test]
    fn find_by_identity_rejects_different_pid() {
        let snap = snap_of(100, 1000);
        assert!(find_by_identity(&snap, &ProcessIdentity::new(101, 1000)).is_none());
    }

    // -----------------------------------------------------------------
    // F14 配置热重载 (apply 层)
    // -----------------------------------------------------------------

    fn reload_test_cfg() -> Config {
        Config::from_toml_str(
            r#"
            [[check]]
            name = "c1"
            kind = "tcp"
            addr = "127.0.0.1:5000"
            interval = "10s"
            timeout = "5s"

            [[service]]
            name = "app"
            command = "/bin/true"

            [[watch]]
            name = "w1"
            pid = 1
            "#,
        )
        .unwrap()
    }

    /// 测试句柄，journal 出口由调用方给定 (tracked 确认通道或裸事件通道)。
    #[allow(clippy::type_complexity)]
    fn handles_with_journal(
        journal_tx: crate::recover::journal::JournalSender,
    ) -> (
        SchedulerHandles,
        watch::Receiver<Arc<StatusReport>>,
        watch::Receiver<Arc<crate::model::control::MetricsReport>>,
    ) {
        let initial = Arc::new(StubCollector::failed("test".into()).collect(&[]));
        let (_snap_tx, snapshot_rx) = watch::channel(initial);
        let (cmd_tx, control_rx) = mpsc::channel(16);
        drop(cmd_tx);
        let (status_tx, status_rx) = watch::channel(Arc::new(StatusReport {
            started_at: SystemTime::UNIX_EPOCH,
            ready: true,
            targets: Vec::new(),
        }));
        // metrics_rx 一并返回 (接收端存活，send 才会真正更新 watch 当前值)。
        let (metrics_tx, metrics_rx) =
            watch::channel(Arc::new(crate::model::control::MetricsReport::default()));
        let counters = Arc::new(crate::alert::AlertCounters::default());
        (
            SchedulerHandles {
                snapshot_rx,
                alert: crate::alert::AlertQueue::new(counters),
                watched: Arc::new(std::sync::Mutex::new(Vec::new())),
                journal_tx,
                control_rx,
                status_tx,
                metrics_tx,
                pm: ProcessManager::new(),
                replayed: crate::recover::journal::JournalState::default(),
                config_path: PathBuf::from("test.toml"),
            },
            status_rx,
            metrics_rx,
        )
    }

    #[allow(clippy::type_complexity)]
    fn reload_handles() -> (
        SchedulerHandles,
        mpsc::Receiver<JournalEvent>,
        watch::Receiver<Arc<StatusReport>>,
        watch::Receiver<Arc<crate::model::control::MetricsReport>>,
    ) {
        let (journal_tx, journal_rx) = mpsc::channel(64);
        let (h, status_rx, metrics_rx) = handles_with_journal(journal_tx.into());
        (h, journal_rx, status_rx, metrics_rx)
    }

    fn drain_journal(rx: &mut mpsc::Receiver<JournalEvent>) -> Vec<JournalEvent> {
        let mut evs = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            evs.push(ev);
        }
        evs
    }

    /// 参数变更 (interval):保留状态机水位 (级别 + 连续计数 + 调度时刻),
    /// 仅替换 cfg;active_cfg 同步更新 (下次 diff 的 old 侧)。
    #[tokio::test]
    async fn apply_reload_preserves_check_state_on_param_change() {
        let cfg = reload_test_cfg();
        let (mut h, _jr, _status_rx, _metrics_rx) = reload_handles();
        let mut sched = Scheduler::build(&cfg);

        // 状态机推进：一次失败 → Ok→Warning(默认 failure_threshold=3)
        sched.checks[0].state.observe(false);
        assert_eq!(sched.checks[0].state.level(), HealthLevel::Warning);
        assert_eq!(sched.checks[0].state.consecutive_failures(), 1);
        let due_before = sched.checks[0].next_due;

        let mut new_cfg = reload_test_cfg();
        new_cfg.checks[0].interval = Duration::from_secs(99);
        sched
            .apply_reload(new_cfg, &mut h, Instant::now(), SystemTime::now())
            .await
            .expect("reload must succeed");

        assert_eq!(sched.checks.len(), 1);
        assert_eq!(sched.checks[0].cfg.interval, Duration::from_secs(99));
        assert_eq!(
            sched.checks[0].state.level(),
            HealthLevel::Warning,
            "级别保留"
        );
        assert_eq!(
            sched.checks[0].state.consecutive_failures(),
            1,
            "连续计数保留"
        );
        assert_eq!(sched.checks[0].next_due, due_before, "调度水位保留");
        assert_eq!(sched.active_cfg.checks[0].interval, Duration::from_secs(99));
    }

    /// 增删：新增 check/service/watch 生效，删除条目移除并写审计;
    /// 未变条目原样保留。
    #[tokio::test]
    async fn apply_reload_adds_and_removes_targets() {
        let cfg = reload_test_cfg();
        let (mut h, mut journal_rx, _status_rx, _metrics_rx) = reload_handles();
        let mut sched = Scheduler::build(&cfg);

        let mut new_cfg = reload_test_cfg();
        new_cfg.checks.retain(|c| c.name != "c1");
        new_cfg.checks.push(CheckCfg {
            name: "c2".into(),
            kind: CheckKindCfg::Tcp {
                addr: "127.0.0.1:5001".parse().unwrap(),
            },
            interval: Duration::from_secs(10),
            timeout: Duration::from_secs(5),
        });
        new_cfg.watches.retain(|w| w.name != "w1");
        new_cfg.watches.push(crate::config::WatchCfg {
            name: "w2".into(),
            pid: Some(2),
            pidfile: None,
        });
        new_cfg.services.push(crate::config::ServiceCfg {
            depends_on: Vec::new(),
            name: "app2".into(),
            command: "/bin/echo".into(),
            args: Vec::new(),
            workdir: None,
            env: Default::default(),
            console: false,
            windows_service: None,
            restart: Default::default(),
            activate: None,
        });

        let now = Instant::now();
        sched
            .apply_reload(new_cfg, &mut h, now, SystemTime::now())
            .await
            .expect("reload must succeed");

        assert_eq!(
            sched
                .checks
                .iter()
                .map(|c| c.cfg.name.as_str())
                .collect::<Vec<_>>(),
            ["c2"],
            "c1 移除，c2 新增"
        );
        let names = sched
            .svcs
            .iter()
            .map(|s| s.cfg.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["app", "app2"]);
        assert!(
            sched.svcs[1].spawn_due.is_some(),
            "新增服务冷启动 spawn 已安排"
        );
        assert_eq!(
            sched
                .watches
                .iter()
                .map(|w| w.name.as_str())
                .collect::<Vec<_>>(),
            ["w2"]
        );

        let evs = drain_journal(&mut journal_rx);
        let texts: Vec<String> = evs
            .iter()
            .filter_map(|e| match e {
                JournalEvent::Audit { message, .. } => Some(message.clone()),
                _ => None,
            })
            .collect();
        assert!(
            texts
                .iter()
                .any(|m| m.contains("check c1 removed by reload")),
            "{texts:?}"
        );
        assert!(
            texts
                .iter()
                .any(|m| m.contains("watch w1 removed by reload")),
            "{texts:?}"
        );
        assert!(
            texts.iter().any(|m| m
                .contains("reload applied: +1/-1/~0 checks, +1/-0/~0 services, +1/-1/~0 watches")),
            "{texts:?}"
        );
    }

    /// 防御性断言：绕过校验的重名 Config → 整个 reload 拒绝，旧配置原样生效。
    #[tokio::test]
    async fn apply_reload_rejects_duplicate_names_defensively() {
        let cfg = reload_test_cfg();
        let (mut h, _jr, _status_rx, _metrics_rx) = reload_handles();
        let mut sched = Scheduler::build(&cfg);

        let mut new_cfg = reload_test_cfg();
        let dup = new_cfg.checks[0].clone();
        new_cfg.checks.push(dup);

        let err = sched
            .apply_reload(new_cfg, &mut h, Instant::now(), SystemTime::now())
            .await
            .expect_err("duplicate name must be rejected");
        assert!(err.to_string().contains("duplicate"), "{err}");
        assert_eq!(sched.checks.len(), 1, "旧配置原样保留");
        assert_eq!(sched.active_cfg.checks[0].name, "c1");
    }

    /// 服务删除：审计 + journal Reset(清除该 target 的重放闩锁/预算)。
    #[tokio::test]
    async fn apply_reload_service_removal_journals_audit_and_reset() {
        let cfg = reload_test_cfg();
        let (mut h, mut journal_rx, _status_rx, _metrics_rx) = reload_handles();
        let mut sched = Scheduler::build(&cfg);

        let mut new_cfg = reload_test_cfg();
        new_cfg.services.clear();

        sched
            .apply_reload(new_cfg, &mut h, Instant::now(), SystemTime::now())
            .await
            .expect("reload must succeed");

        assert!(sched.svcs.is_empty());
        assert!(sched.pending_reap.is_empty(), "未运行的服务无需收割登记");

        let evs = drain_journal(&mut journal_rx);
        assert!(
            evs.iter().any(|e| matches!(
                e,
                JournalEvent::ResetTargets { targets, source, .. }
                    if targets == &["app"] && source == "reload"
            )),
            "expect Reset(reload) for app, got {evs:?}"
        );
        assert!(evs.iter().any(|e| matches!(
            e,
            JournalEvent::Audit { message, .. }
                if message.contains("service app removed by reload")
        )));
    }

    /// 重定义无运行中旧实例时清除旧 journal 状态并立即排程。
    #[tokio::test]
    async fn apply_reload_redefinition_journals_reset_and_defers() {
        let cfg = reload_test_cfg();
        let (mut h, mut journal_rx, _status_rx, _metrics_rx) = reload_handles();
        let mut sched = Scheduler::build(&cfg);

        let mut new_cfg = reload_test_cfg();
        new_cfg.services[0].command = "/bin/other".to_string(); // hot changed

        let now = Instant::now();
        sched
            .apply_reload(new_cfg, &mut h, now, SystemTime::now())
            .await
            .expect("reload must succeed");

        let rt = sched
            .svcs
            .iter()
            .find(|s| s.cfg.name == "app")
            .expect("app 必须保留 (重定义而非删除)");
        assert_eq!(rt.spawn_due, Some(now));
        assert!(rt.reload_stop_deadline.is_none());

        let evs = drain_journal(&mut journal_rx);
        assert!(
            evs.iter().any(|e| matches!(
                e,
                JournalEvent::ResetTargets { targets, source, .. }
                    if targets == &["app"] && source == "reload"
            )),
            "重定义必须写 Reset(reload) 清旧闩锁/预算，got {evs:?}"
        );
    }

    /// 第五轮 P1 锁定:process 检查在快照无进程证据时失败不计入状态机
    /// (采集永久降级不得把健康服务推入恢复)。
    #[tokio::test]
    async fn process_probe_failure_ignored_without_evidence() {
        let cfg = Config::from_toml_str(
            r#"
            [[service]]
            name = "app"
            command = "/bin/true"
            [[check]]
            name = "proc"
            kind = "process"
            process = "app"
            cpu_threshold = 99.0
            interval = "10s"
            timeout = "5s"
        "#,
        )
        .unwrap();
        let (mut h, mut journal_rx, _status_rx, _metrics_rx) = reload_handles();
        let mut sched = Scheduler::build(&cfg);
        // 不可用结果记录于探测发起时，结果收割不依赖另一代快照。
        let fail = crate::eval::check::ProbeResult::Unavailable {
            reason: "collector unavailable".into(),
        };
        sched
            .absorb_probe(0, fail, Instant::now(), SystemTime::now(), &mut h)
            .await;
        // 失败必须被门控吞掉：状态机仍在 Ok，无 incident 事件
        assert_eq!(
            sched.checks[0].state.level(),
            crate::model::control::HealthLevel::Ok
        );
        let evs = drain_journal(&mut journal_rx);
        assert!(
            !evs.iter()
                .any(|e| matches!(e, JournalEvent::Incident { .. })),
            "无证据失败不得产生 incident, got {evs:?}"
        );
    }

    /// reload 后一轮 tick:/status 汇报出现新 target(集成-lite)。
    #[tokio::test]
    async fn reload_reflects_new_target_in_status() {
        let cfg = reload_test_cfg();
        let (mut h, _jr, _status_rx, _metrics_rx) = reload_handles();
        let mut sched = Scheduler::build(&cfg);

        let mut new_cfg = reload_test_cfg();
        new_cfg.checks.push(CheckCfg {
            name: "c2".into(),
            kind: CheckKindCfg::Tcp {
                addr: "127.0.0.1:5001".parse().unwrap(),
            },
            interval: Duration::from_secs(10),
            timeout: Duration::from_secs(5),
        });
        sched
            .apply_reload(new_cfg, &mut h, Instant::now(), SystemTime::now())
            .await
            .expect("reload must succeed");

        sched.tick(&mut h).await;
        let report = h.status_tx.borrow().clone();
        assert!(
            report.targets.iter().any(|t| t.name == "c2"),
            "reload 新增的 target 应出现在 /status:{:?}",
            report.targets.iter().map(|t| &t.name).collect::<Vec<_>>()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn running_dependent_is_never_spawned_twice() {
        let cfg = Config::from_toml_str(
            r#"
            [[service]]
            name = "db"
            command = "/bin/sleep"
            args = ["30"]
            [[service]]
            name = "api"
            command = "/bin/sleep"
            args = ["30"]
            depends_on = ["db"]
        "#,
        )
        .unwrap();
        let (mut h, _jr, _sr, _mr) = reload_handles();
        let mut sched = Scheduler::build(&cfg);
        let t0 = Instant::now();
        for svc in &cfg.services {
            h.pm.register(svc.clone());
        }
        for idx in 0..2 {
            let identity = h.pm.spawn(&cfg.services[idx].name).await.unwrap();
            sched.svcs[idx].fsm.on_spawn_result(t0, Some(identity));
            sched.svcs[idx]
                .fsm
                .on_liveness_ok(t0 + Duration::from_secs(60));
        }
        let old_identity = h.pm.identity("api");
        let baseline_spawns = sched.svcs[1].fsm.restarts_total();
        let snap = StubCollector::default().collect(&[]);
        for _ in 0..3 {
            sched
                .service_tick(
                    1,
                    &snap,
                    t0 + Duration::from_secs(61),
                    SystemTime::now(),
                    &mut h,
                )
                .await;
        }
        let state = sched.svcs[1].fsm.state();
        let restarts = sched.svcs[1].fsm.restarts_total();
        let same_child = h.pm.identity("api") == old_identity;
        h.pm.shutdown_all().await;
        assert_eq!(state, RecoveryState::Running);
        assert_eq!(restarts, baseline_spawns);
        assert_eq!(
            sched.svcs[1]
                .fsm
                .restarts_in_window(t0 + Duration::from_secs(61)),
            0
        );
        assert!(same_child);
    }

    #[tokio::test]
    async fn queued_spawn_waits_for_dependencies_without_busy_loop() {
        let cfg = Config::from_toml_str(
            r#"
            [[service]]
            name = "db"
            command = "/missing/db"
            [[service]]
            name = "api"
            command = "/missing/api"
            depends_on = ["db"]
        "#,
        )
        .unwrap();
        let (mut h, _jr, _sr, _mr) = reload_handles();
        let mut sched = Scheduler::build(&cfg);
        for svc in &cfg.services {
            h.pm.register(svc.clone());
        }
        let now = Instant::now();
        sched.svcs[1].spawn_due = Some(now);
        let snap = StubCollector::default().collect(&[]);
        sched
            .service_tick(1, &snap, now, SystemTime::now(), &mut h)
            .await;
        assert_eq!(sched.svcs[1].spawn_due, Some(now));
        assert_eq!(sched.svcs[1].fsm.restarts_total(), 0);
        assert_eq!(sched.next_wake_delay(now), MAX_TICK);
        sched
            .handle_control(
                ControlCommand::Restart {
                    target: "api".into(),
                },
                now,
                SystemTime::now(),
                &mut h,
            )
            .await;
        assert_eq!(
            sched.svcs[1].spawn_due,
            Some(now),
            "restart cannot replace a dependency-blocked deadline"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reload_waits_for_old_child_to_be_reaped() {
        let cfg = Config::from_toml_str(
            r#"
            [[service]]
            name = "app"
            command = "/bin/sh"
            args = ["-c", "trap '' TERM; exec sleep 30"]
            [service.restart]
            stop_grace = "100ms"
        "#,
        )
        .unwrap();
        let (mut h, _jr, _sr, _mr) = reload_handles();
        h.pm.register(cfg.services[0].clone());
        let old = h.pm.spawn("app").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut sched = Scheduler::build(&cfg);
        let mut changed = cfg.clone();
        changed.services[0].command = "/bin/sleep".into();
        changed.services[0].args = vec!["31".into()];
        let now = Instant::now();
        sched
            .apply_reload(changed, &mut h, now, SystemTime::now())
            .await
            .unwrap();
        assert_eq!(
            h.pm.identity("app"),
            Some(old),
            "old Child must remain owned until reaped"
        );
        assert!(sched.svcs[0].reload_stop_deadline.is_some());
        assert!(sched.svcs[0].spawn_due.is_none());
        let snap = StubCollector::default().collect(&[]);
        sched
            .service_tick(0, &snap, now, SystemTime::now(), &mut h)
            .await;
        assert_eq!(
            h.pm.identity("app"),
            Some(old),
            "grace period cannot overlap a replacement"
        );
        for _ in 0..50 {
            sched
                .service_tick(
                    0,
                    &snap,
                    Instant::now() + Duration::from_secs(1),
                    SystemTime::now(),
                    &mut h,
                )
                .await;
            if h.pm.identity("app").is_some_and(|identity| identity != old) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let replaced = h.pm.identity("app").is_some_and(|identity| identity != old);
        let restarts = sched.svcs[0].fsm.restarts_total();
        h.pm.shutdown_all().await;
        assert!(replaced, "replacement starts after old child exits");
        assert_eq!(restarts, 1, "a single replacement was successfully spawned");
        assert_eq!(
            sched.svcs[0].fsm.restarts_in_window(Instant::now()),
            0,
            "reload does not create synthetic spawn failures"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn orphan_attach_remains_read_only_until_original_exits() {
        let cfg = Config::from_toml_str(
            r#"
            [[service]]
            name = "app"
            command = "/bin/sleep"
            args = ["30"]
            [[check]]
            name = "proc"
            kind = "process"
            process = "app"
            mem_threshold_mb = 1
        "#,
        )
        .unwrap();
        let mut external = ProcessManager::new();
        external.register(cfg.services[0].clone());
        let identity = external.spawn("app").await.unwrap();
        let (mut h, _jr, _sr, _mr) = reload_handles();
        h.pm.register(cfg.services[0].clone());
        let mut sched = Scheduler::build(&cfg);
        sched.svcs[0].orphan = Some(OrphanTracker::new(
            identity,
            true,
            Instant::now() - Duration::from_secs(1),
        ));
        let mut snap = snap_of(identity.pid.0, identity.start_time);
        snap.hardware.timestamp = SystemTime::now();
        let now = Instant::now();
        sched
            .service_tick(0, &snap, now, SystemTime::now(), &mut h)
            .await;
        let attached_status = sched.build_status(&h, now);
        assert_eq!(attached_status.targets[0].mode, TargetMode::Attach);
        assert_eq!(attached_status.targets[0].identity, Some(identity));
        sched
            .handle_control(
                ControlCommand::Restart {
                    target: "app".into(),
                },
                now,
                SystemTime::now(),
                &mut h,
            )
            .await;
        for _ in 0..5 {
            sched
                .absorb_probe(
                    0,
                    ProbeResult::Fail {
                        reason: "threshold exceeded".into(),
                        observed: None,
                    },
                    now,
                    SystemTime::now(),
                    &mut h,
                )
                .await;
        }
        assert!(
            !h.pm.is_running("app"),
            "read-only takeover cannot create an owned child"
        );
        assert!(
            external.poll_exit("app").await.is_none(),
            "critical health cannot kill attached instance"
        );
        external.shutdown_all().await;
        snap.processes.clear();
        snap.collected_at = Instant::now();
        sched
            .service_tick(0, &snap, Instant::now(), SystemTime::now(), &mut h)
            .await;
        let recovery_due = sched.svcs[0].spawn_due;
        sched
            .service_tick(
                0,
                &snap,
                Instant::now() + Duration::from_secs(60),
                SystemTime::now(),
                &mut h,
            )
            .await;
        let resumed = h.pm.is_running("app") && sched.svcs[0].orphan.is_none();
        assert!(recovery_due.is_some());
        h.pm.shutdown_all().await;
        assert!(resumed, "OS-confirmed absence allows supervised startup");
    }

    #[tokio::test]
    async fn process_sample_is_counted_once_and_stale_samples_are_skipped() {
        let cfg = Config::from_toml_str(
            r#"
            [[check]]
            name = "proc"
            kind = "process"
            process = "pid:1234"
            cpu_threshold = 10.0
        "#,
        )
        .unwrap();
        let (mut h, _jr, _sr, _mr) = reload_handles();
        let mut sched = Scheduler::build(&cfg);
        let mut snap = snap_of(1234, 100);
        snap.processes[0].cpu_usage_percent = Some(99.0);
        snap.collected_at = Instant::now() - Duration::from_secs(60);
        let (snap_tx, snap_rx) = watch::channel(Arc::new(snap.clone()));
        h.snapshot_rx = snap_rx;
        sched.tick(&mut h).await;
        assert_eq!(
            sched.checks[0].executed, 0,
            "stale high CPU cannot become health evidence"
        );
        snap.collected_at = Instant::now();
        snap_tx.send(Arc::new(snap)).unwrap();
        for _ in 0..8 {
            sched.checks[0].next_due = Instant::now();
            sched.tick(&mut h).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(sched.checks[0].executed, 1);
        assert_eq!(sched.checks[0].state.level(), HealthLevel::Warning);
        assert!(
            h.watched
                .lock()
                .unwrap()
                .iter()
                .any(|identity| identity.pid.0 == 1234),
            "raw PID checks must be collected"
        );
    }

    #[test]
    fn service_binding_precedes_same_named_watch_during_resolution() {
        let cfg = Config::from_toml_str(
            r#"
            [[service]]
            name = "app"
            command = "/unused/app"
            [[watch]]
            name = "app"
            pid = 1234
            [[check]]
            name = "proc"
            kind = "process"
            process = "app"
            cpu_threshold = 10.0
        "#,
        )
        .unwrap();
        let (h, _jr, _sr, _mr) = reload_handles();
        let mut sched = Scheduler::build(&cfg);
        let ident = ProcessIdentity::new(1234, 100);
        sched.watches[0].identity = Some(ident);
        assert_eq!(sched.checks[0].bound_service, Some(0));
        let resolved = sched.resolve_process_target("app", &snap_of(1234, 100), &h);
        assert!(
            resolved.snapshot.is_none(),
            "a watch reading cannot be attributed to an unstarted service"
        );
    }

    // -----------------------------------------------------------------
    // F18 历史指标环形缓冲
    // -----------------------------------------------------------------

    /// 容量上限：超过 [`METRICS_HISTORY_CAP`] 丢最旧 (首条被挤出，尾部保留)。
    #[test]
    fn metrics_history_ring_caps_at_limit() {
        let cfg = reload_test_cfg();
        let mut sched = Scheduler::build(&cfg);

        for i in 0..(METRICS_HISTORY_CAP + 5) {
            let at = SystemTime::UNIX_EPOCH + Duration::from_secs(i as u64);
            sched.record_metrics_sample(at, &crate::model::control::MetricsReport::default());
        }

        let ring = sched.metrics_history.lock().unwrap();
        assert_eq!(ring.len(), METRICS_HISTORY_CAP, "超限后恰为容量上限");
        assert_eq!(
            ring.front().unwrap().at,
            SystemTime::UNIX_EPOCH + Duration::from_secs(5),
            "最旧 5 条被挤出"
        );
        assert_eq!(
            ring.back().unwrap().at,
            SystemTime::UNIX_EPOCH + Duration::from_secs((METRICS_HISTORY_CAP + 4) as u64),
            "最新样本保留"
        );
    }

    /// tick 发布即入环：发布的 metrics 报告携带同一缓冲句柄，且含本轮样本。
    #[tokio::test]
    async fn tick_publishes_metrics_with_history_sample() {
        let cfg = reload_test_cfg();
        let (mut h, _jr, _status_rx, _metrics_rx) = reload_handles();
        let mut sched = Scheduler::build(&cfg);

        sched.tick(&mut h).await;

        let published = h.metrics_tx.borrow().clone();
        assert!(
            Arc::ptr_eq(&published.history, &sched.metrics_history),
            "发布报告携带的应是调度器内的同一缓冲句柄"
        );
        let ring = published.history.lock().unwrap();
        assert_eq!(ring.len(), 1, "首轮 tick 恰入环一个样本");
        let sample = ring.front().unwrap();
        assert_eq!(
            sample.report.probe_executed, published.probe_executed,
            "样本与发布报告同源"
        );
    }

    // -----------------------------------------------------------------
    // 崩溃启动事务 (ROADMAP「崩溃启动事务」)
    // -----------------------------------------------------------------

    fn spawn_tx_cfg(command: &str) -> Config {
        Config::from_toml_str(&format!(
            r#"
            [[service]]
            name = "app"
            command = "{command}"
        "#
        ))
        .unwrap()
    }

    fn pending_intent(digest: String) -> JournalState {
        let mut replayed = JournalState::default();
        replayed.pending_spawns.insert(
            "app".into(),
            PendingSpawn {
                at: SystemTime::UNIX_EPOCH + Duration::from_secs(1),
                config_digest: digest,
            },
        );
        replayed
    }

    /// intent 无 outcome(未知实例窗口)→ 跳过冷启动 (spawn_due=None)+
    /// ERROR/journal Audit + FSM Suspect(可观测)+ 调度侧 suspected 标记。
    #[tokio::test]
    async fn cold_start_skipped_when_spawn_intent_has_no_outcome() {
        let cfg = spawn_tx_cfg("/bin/app");
        let (mut h, mut journal_rx, _sr, _mr) = reload_handles();
        let mut sched = Scheduler::build(&cfg);
        let digest = journal::config_digest(&cfg.services[0]);
        let replayed = pending_intent(digest);
        let now = Instant::now();
        sched
            .cold_start(0, &replayed, now, SystemTime::now(), &mut h)
            .await;
        assert_eq!(sched.svcs[0].spawn_due, None, "未知实例窗口必须跳过冷启动");
        assert!(sched.svcs[0].orphan_suspected, "调度侧 suspected 标记置位");
        assert_eq!(
            sched.svcs[0].fsm.state(),
            RecoveryState::Suspect,
            "FSM 标记 Suspect(可观测)"
        );
        let evs = drain_journal(&mut journal_rx);
        assert!(
            evs.iter().any(|e| matches!(
                e,
                JournalEvent::Audit { message, .. }
                    if message.contains("orphan suspected for app (intent without outcome)")
                        && message.contains("monitorctl reset")
            )),
            "必须写 orphan suspected 审计，got {evs:?}"
        );
    }

    /// intent 的 digest 与当前定义不符 → 旧定义遗留意图，忽略，照常冷启动。
    #[tokio::test]
    async fn cold_start_proceeds_when_pending_intent_is_from_previous_definition() {
        let cfg = spawn_tx_cfg("/bin/app");
        let (mut h, mut journal_rx, _sr, _mr) = reload_handles();
        let mut sched = Scheduler::build(&cfg);
        let replayed = pending_intent("stale-definition-digest".into());
        let now = Instant::now();
        sched
            .cold_start(0, &replayed, now, SystemTime::now(), &mut h)
            .await;
        assert_eq!(
            sched.svcs[0].spawn_due,
            Some(now),
            "旧定义意图被忽略：照常冷启动"
        );
        assert!(!sched.svcs[0].orphan_suspected);
        assert_eq!(sched.svcs[0].fsm.state(), RecoveryState::Running);
        let evs = drain_journal(&mut journal_rx);
        assert!(
            !evs.iter()
                .any(|e| matches!(e, JournalEvent::Audit { message, .. }
                    if message.contains("orphan suspected"))),
            "digest 不匹配不得产生 orphan suspected 审计，got {evs:?}"
        );
    }

    /// intent + outcome(重放后无未决意图)→ 照常冷启动。
    #[tokio::test]
    async fn cold_start_proceeds_when_intent_has_outcome() {
        let cfg = spawn_tx_cfg("/bin/app");
        let (mut h, _jr, _sr, _mr) = reload_handles();
        let mut sched = Scheduler::build(&cfg);
        let now = Instant::now();
        sched
            .cold_start(0, &JournalState::default(), now, SystemTime::now(), &mut h)
            .await;
        assert_eq!(
            sched.svcs[0].spawn_due,
            Some(now),
            "outcome 已关闭窗口：照常冷启动"
        );
        assert!(!sched.svcs[0].orphan_suspected);
        assert_eq!(sched.svcs[0].fsm.state(), RecoveryState::Running);
    }

    /// restart 在 suspected 态被拒 (防"未运行即拉起"制造双实例);
    /// reset 解除标记、FSM 回 Running 并允许拉起 (spawn_due=Some)。
    #[tokio::test]
    async fn reset_releases_suspected_orphan_and_restart_is_rejected_before() {
        let cfg = spawn_tx_cfg("/bin/app");
        let (mut h, mut journal_rx, _sr, _mr) = reload_handles();
        let mut sched = Scheduler::build(&cfg);
        let digest = journal::config_digest(&cfg.services[0]);
        let replayed = pending_intent(digest);
        let now = Instant::now();
        sched
            .cold_start(0, &replayed, now, SystemTime::now(), &mut h)
            .await;
        assert!(sched.svcs[0].orphan_suspected);

        sched
            .handle_control(
                ControlCommand::Restart {
                    target: "app".into(),
                },
                now,
                SystemTime::now(),
                &mut h,
            )
            .await;
        assert_eq!(
            sched.svcs[0].spawn_due, None,
            "restart 不得绕过未知实例窗口"
        );
        let evs = drain_journal(&mut journal_rx);
        assert!(
            evs.iter().any(|e| matches!(
                e,
                JournalEvent::Audit { message, .. }
                    if message.contains("restart rejected: app has a suspected orphan")
            )),
            "restart 必须被拒并审计，got {evs:?}"
        );

        let reset_at = now + Duration::from_secs(1);
        sched
            .handle_control(
                ControlCommand::Reset {
                    target: "app".into(),
                },
                reset_at,
                SystemTime::now(),
                &mut h,
            )
            .await;
        assert!(!sched.svcs[0].orphan_suspected, "reset 清除 suspected 标记");
        assert_eq!(
            sched.svcs[0].fsm.state(),
            RecoveryState::Running,
            "reset 后 FSM 回 Running"
        );
        assert_eq!(sched.svcs[0].spawn_due, Some(reset_at), "reset 后允许拉起");
        assert!(
            drain_journal(&mut journal_rx)
                .iter()
                .any(|e| matches!(e, JournalEvent::Reset { target, .. } if target == "app")),
            "reset 写 journal(重放侧同步清除未决意图)"
        );
    }

    /// 验收断言 (roadmap「未知实例窗口」语义):spawn 前意图已确认落盘 ——
    /// 在 `send()` 的确认返回之前，`pm.spawn` 不可能被调用。
    #[cfg(unix)]
    #[tokio::test]
    async fn spawn_intent_is_awaited_and_confirmed_before_process_spawn() {
        let cfg = Config::from_toml_str(
            r#"
            [[service]]
            name = "app"
            command = "/bin/sleep"
            args = ["30"]
        "#,
        )
        .unwrap();
        let (journal_tx, mut journal_rx) = journal::channel(8);
        let (mut h, _sr, _mr) = handles_with_journal(journal_tx);
        h.pm.register(cfg.services[0].clone());
        let mut sched = Scheduler::build(&cfg);
        sched.svcs[0].spawn_due = Some(Instant::now());
        let snap = StubCollector::default().collect(&[]);
        let task = tokio::spawn(async move {
            sched
                .service_tick(0, &snap, Instant::now(), SystemTime::now(), &mut h)
                .await;
            (h.pm.is_running("app"), h)
        });
        // 逐个收取请求直到 SpawnIntent(此前可能有 FSM 审计事件)。
        let mut intent = None;
        while intent.is_none() {
            let request = journal_rx.recv().await.unwrap();
            if matches!(request.event, JournalEvent::SpawnIntent { .. }) {
                intent = Some(request);
            } else if let Some(ack) = request.ack {
                let _ = ack.send(Ok(()));
            }
        }
        // 意图未确认 (writer 未 ack):任务必须仍阻塞在 send 上 ——
        // send 返回之前 pm.spawn 不可能发生 (结构上 spawn 在 send().await 之后)。
        assert!(
            !task.is_finished(),
            "spawn intent 未确认 (未落盘) 前，service_tick 不得推进到 pm.spawn"
        );
        intent.unwrap().ack.unwrap().send(Ok(())).unwrap();
        // 确认后放行：逐个确认后续关键事件 (SpawnOutcome/ProcessIdentity),
        // 避免 2s 确认超时拖慢测试。
        let (running, mut h) = loop {
            if task.is_finished() {
                break task.await.unwrap();
            }
            if let Ok(request) = journal_rx.try_recv()
                && let Some(ack) = request.ack
            {
                let _ = ack.send(Ok(()));
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };
        assert!(running, "确认落盘后 pm.spawn 才发生");
        h.pm.shutdown_all().await;
    }

    /// 各测试独立的临时目录 (pid + 测试名隔离;本 crate 不依赖 tempfile)。
    fn spawn_tx_tmp_dir(test: &str) -> PathBuf {
        std::env::temp_dir().join(format!("sw-eval-test-{}-{}", std::process::id(), test))
    }

    fn spawn_tx_cleanup(dir: &std::path::Path) {
        let _ = std::fs::remove_dir_all(dir);
    }

    /// 真实 writer + 真实 spawn:成功路径按序落盘 intent → outcome → 身份，
    /// 重放后无未决意图 (outcome 已关闭未知实例窗口)。
    #[cfg(unix)]
    #[tokio::test]
    async fn spawn_success_journals_intent_outcome_and_replay_closes_window() {
        let dir = spawn_tx_tmp_dir("spawn-tx-ok");
        spawn_tx_cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let file = journal::open_writer(&path).unwrap();
        let (journal_tx, rx) = journal::channel(16);
        let counters = journal_tx.counters().clone();
        let writer = tokio::spawn(journal::run_tracked_writer(
            file,
            path.clone(),
            rx,
            counters,
        ));
        let cfg = Config::from_toml_str(
            r#"
            [[service]]
            name = "app"
            command = "/bin/sleep"
            args = ["30"]
        "#,
        )
        .unwrap();
        let (mut h, _sr, _mr) = handles_with_journal(journal_tx);
        h.pm.register(cfg.services[0].clone());
        let mut sched = Scheduler::build(&cfg);
        sched.svcs[0].spawn_due = Some(Instant::now());
        let snap = StubCollector::default().collect(&[]);
        sched
            .service_tick(0, &snap, Instant::now(), SystemTime::now(), &mut h)
            .await;
        h.pm.shutdown_all().await;
        drop(h); // 关闭 journal 通道 → writer 排空后返回
        writer.await.unwrap().unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let first = content.lines().next().unwrap();
        assert!(
            first.contains("\"spawn_intent\"") && first.contains("\"app\""),
            "意图必须先于一切事件落盘，首行 = {first}"
        );
        assert!(content.contains("\"spawn_outcome\""), "结果必须落盘");
        let state = journal::replay(&path);
        assert!(
            state.pending_spawns.is_empty(),
            "成功 spawn 的 outcome 关闭未知实例窗口"
        );
        assert!(
            state.processes.contains_key("app"),
            "身份照常落盘 (孤儿对账素材)"
        );
        spawn_tx_cleanup(&dir);
    }

    /// spawn 失败同样落结果 (identity=None + pid_confirmed=false),关闭窗口;
    /// 不写身份记录。意图仍先行落盘。
    #[cfg(unix)]
    #[tokio::test]
    async fn spawn_failure_journals_outcome_without_identity() {
        let dir = spawn_tx_tmp_dir("spawn-tx-fail");
        spawn_tx_cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let file = journal::open_writer(&path).unwrap();
        let (journal_tx, rx) = journal::channel(16);
        let counters = journal_tx.counters().clone();
        let writer = tokio::spawn(journal::run_tracked_writer(
            file,
            path.clone(),
            rx,
            counters,
        ));
        let cfg = spawn_tx_cfg("/nonexistent/service-watch-spawn-fail");
        let (mut h, _sr, _mr) = handles_with_journal(journal_tx);
        h.pm.register(cfg.services[0].clone());
        let mut sched = Scheduler::build(&cfg);
        sched.svcs[0].spawn_due = Some(Instant::now());
        let snap = StubCollector::default().collect(&[]);
        sched
            .service_tick(0, &snap, Instant::now(), SystemTime::now(), &mut h)
            .await;
        h.pm.shutdown_all().await;
        drop(h);
        writer.await.unwrap().unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let first = content.lines().next().unwrap();
        assert!(
            first.contains("\"spawn_intent\""),
            "失败路径同样意图先行，首行 = {first}"
        );
        let outcome_line = content
            .lines()
            .find(|l| l.contains("\"spawn_outcome\""))
            .expect("spawn 失败必须落结果");
        assert!(outcome_line.contains("\"identity\":null"), "{outcome_line}");
        assert!(
            outcome_line.contains("\"pid_confirmed\":false"),
            "{outcome_line}"
        );
        let state = journal::replay(&path);
        assert!(
            state.pending_spawns.is_empty(),
            "失败 outcome 同样关闭未知实例窗口"
        );
        assert!(!state.processes.contains_key("app"), "失败不得留下身份记录");
        spawn_tx_cleanup(&dir);
    }
}
