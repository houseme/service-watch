//! 恢复状态机 (DESIGN.md §8 全转移表)。纯逻辑：时间由调用方注入单调 `Instant`,
//! 动作以 [`RecoverAction`] 返回，由 eval 调度器执行 (本模块不做任何 I/O)。
//!
//! 约定 (§8.2):
//! - 本模块返回 [`RecoverAction::ScheduleSpawn`] 时内部同步记录 `spawn_due`;
//!   调度器每轮调用 [`RecoveryFsm::on_tick`],spawn_due 到点时返回
//!   `ScheduleSpawn { delay: 0 }` 表示"现在该 spawn 了",真正执行由调度器做。
//! - 调度器必须在 spawn 尝试后回填 [`RecoveryFsm::on_spawn_result`](无论成败),
//!   否则状态机会停在 Restarting。
//! - 强杀 (`ForceKill`) 发出后，调度器应稍作轮询再回报 `on_stop_result(false)`;
//!   FSM 收到 ForceKill 下的 `false` 即按 §8.2 判 Fatal(Unix D 状态兜底)。

pub mod journal;
pub mod policy;
pub mod process;
pub mod reconcile;

use std::time::{Duration, Instant, SystemTime};

use crate::config::HungAction;
use crate::model::control::{HealthLevel, RecoveryState};
use crate::model::ids::ProcessIdentity;

pub use policy::{Backoff, BudgetWindow, RestartPolicy};

/// 恢复状态机要求调度器执行的动作。
#[derive(Debug, Clone)]
pub enum RecoverAction {
    /// 按 backoff 延迟后 spawn(调度器记录 spawn 截止时间，on_tick 到点再真正执行)。
    ScheduleSpawn { delay: Duration },
    /// graceful 停止 (SIGTERM / CTRL_BREAK 尝试)。
    SignalStop,
    /// 强杀 (SIGKILL / TerminateJobObject)。
    ForceKill,
    /// 执行激活钩子 ( HungAction::Activate )。
    RunActivate,
    /// 进入 Fatal(调度器发 journal + Critical 告警)。
    MarkFatal,
    /// 每次预算计数对应一条 journal 记录。仅由 FSM 计数点产生，
    /// child 退出和手动停止本身不额外产生预算记录。
    JournalRestart { fast_exit: bool },
    /// 需要写 journal 的审计记录。
    Audit { message: String, at: SystemTime },
}

/// 恢复状态机 (supervise 目标独占一份)。
#[derive(Debug)]
pub struct RecoveryFsm {
    name: String,
    policy: RestartPolicy,
    state: RecoveryState,
    budget: BudgetWindow,
    backoff: Backoff,
    /// 进入当前状态的单调时刻。
    entered_at: Instant,
    /// 健康类失败的连续确认计数 (Suspect 判定)。
    suspect_confirmations: u32,
    /// liveness 消失的连续确认计数。
    gone_confirmations: u32,
    /// Fatal 闩锁。
    fatal: bool,
    /// spawn 截止时间 (ScheduleSpawn 后由调度器在到点时调用 `on_spawn_due`)。
    spawn_due: Option<Instant>,
    /// force-kill 升级截断时间 (SignalStop 后 stop_grace 内未停止则升级)。
    stop_deadline: Option<Instant>,
    /// 最近一次 spawn 的身份与时刻 (用于 min_uptime / startup_grace 判定)。
    last_spawn: Option<(ProcessIdentity, Instant)>,
    /// activate 观察期截止。
    verify_deadline: Option<Instant>,
    /// 窗口内重启总次数 (供 /status;不受窗口滑动影响)。
    restarts_total: u64,
    /// 状态转移总次数 (供 /metrics 运行观测)。
    transitions_total: u64,
    /// activate 成功后的观察期时长 (对应 ActivateCfg.verify_for;调度器经
    /// [`RecoveryFsm::set_verify_for`] 注入，缺省以 startup_grace 兜底)。
    verify_for: Duration,
    /// 最近一次健康反馈 (verify 观察期到点时判定 activate 成败的依据)。
    last_health: Option<HealthLevel>,
    /// 构造时刻的单调/墙钟锚点:Audit 时间戳由注入的 `Instant` 派生，
    /// 纯逻辑路径不再读墙钟 (构造时的唯一一次除外)。
    mono_anchor: Instant,
    wall_anchor: SystemTime,
}

impl RecoveryFsm {
    pub fn new(name: &str, policy: RestartPolicy) -> Self {
        let mono = Instant::now();
        let verify_for = policy.startup_grace;
        Self {
            name: name.to_string(),
            budget: BudgetWindow::from_policy(&policy),
            backoff: Backoff::from_policy(&policy),
            policy,
            state: RecoveryState::Running,
            entered_at: mono,
            suspect_confirmations: 0,
            gone_confirmations: 0,
            fatal: false,
            spawn_due: None,
            stop_deadline: None,
            last_spawn: None,
            verify_deadline: None,
            restarts_total: 0,
            transitions_total: 0,
            verify_for,
            last_health: None,
            mono_anchor: mono,
            wall_anchor: SystemTime::now(),
        }
    }

    /// 注入 activate 观察期时长 (ActivateCfg.verify_for;new 时以 startup_grace 兜底)。
    pub fn set_verify_for(&mut self, verify_for: Duration) {
        self.verify_for = verify_for;
    }

    /// 热更新恢复策略，保留状态、预算素材、退避次数及已建立的操作截止时间。
    /// 新参数用于后续事件，在途停止/激活仍按已经建立的截止时间收尾。
    pub fn update_policy(&mut self, policy: RestartPolicy) {
        self.budget
            .update_limits(policy.max_restarts, policy.restart_window);
        self.backoff
            .update_limits(policy.backoff_initial, policy.backoff_max);
        self.policy = policy;
    }

    /// 与 ProcessManager 同步亲生子进程的观测身份，保留原 spawn 时间。
    pub fn adopt_identity(&mut self, previous: ProcessIdentity, observed: ProcessIdentity) {
        if previous.pid != observed.pid {
            return;
        }
        if let Some((identity, _)) = self.last_spawn.as_mut()
            && *identity == previous
        {
            *identity = observed;
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn state(&self) -> RecoveryState {
        self.state
    }

    pub fn is_fatal(&self) -> bool {
        self.fatal
    }

    pub fn restarts_in_window(&self, now: Instant) -> u32 {
        self.budget.count(now)
    }

    pub fn restarts_total(&self) -> u64 {
        self.restarts_total
    }

    /// 状态转移总次数 (运行观测;含进入 Fatal)。
    pub fn transitions_total(&self) -> u64 {
        self.transitions_total
    }

    // ---- 事件入口 (实现代理 C:按 §8.2 全转移表实现) ----

    /// 亲生进程退出 (确定性死亡路径)。
    pub fn on_child_exit(&mut self, now: Instant, identity: ProcessIdentity) -> Vec<RecoverAction> {
        if self.fatal {
            return vec![];
        }
        // §8.3:身份校验——只认自己最近 spawn 的孩子;不匹配视为陈旧事件忽略。
        let spawned_at = match self.last_spawn {
            Some((id, at)) if id == identity => at,
            _ => return vec![],
        };
        let uptime = now.duration_since(spawned_at);
        match self.state {
            RecoveryState::Starting => {
                self.last_spawn = None;
                // Starting 内退出:uptime < min_uptime → CrashLoop(计入 budget,§8.2);
                // 已超过 min_uptime 的死亡与 Running 同路径。
                if uptime < self.policy.min_uptime {
                    self.crashloop_on_death(now, uptime)
                } else {
                    self.deterministic_death(now, Some(uptime))
                }
            }
            RecoveryState::Running | RecoveryState::Suspect => {
                self.last_spawn = None;
                self.deterministic_death(now, Some(uptime))
            }
            // 恢复执行中收到孩子退出：
            RecoveryState::Recovering if self.stop_deadline.is_some() => {
                // stop 待确认:reaper 先于轮询发现退出 → 视同 stop 完成。
                self.stop_confirmed(now)
            }
            RecoveryState::Recovering if self.verify_deadline.is_some() => {
                // activate 观察期内进程死亡：恢复失败 → 计入 budget，直接排重启。
                self.verify_deadline = None;
                self.last_spawn = None;
                self.fail_recover_and_respawn(now, "activate 观察期内进程退出")
            }
            RecoveryState::ForceKill => {
                // SIGKILL 已送达且进程死亡：等同 stop 完成 (评审 P0 修复 —— 此前
                // 落入空臂被当陈旧事件，FSM 永久卡死在 ForceKill，服务不再拉起)。
                self.last_spawn = None;
                self.stop_confirmed(now)
            }
            // 其余恢复态无活孩子;死亡路径排程中 (waiting backoff) 的重复退出是陈旧事件。
            _ => vec![],
        }
    }

    /// 计划内级联停止确认 (F20;第六轮评审 P3 修复):dependent 因依赖 restart
    /// 被**级联停止**,其退出不是故障 —— 与 `on_child_exit` 的 deterministic_death
    /// 不同:**不计预算、不排程 spawn**(由 F20 依赖门控在依赖回 Running 后拉回，
    /// 见调度器 tick 的 "dependencies running; scheduling cold start" 分支)。
    /// 受理：非 Fatal 且处于任何未闩锁状态 (signal_stop 后 FSM 状态未变，退出
    /// 事件到达时按此收口;重复调用是幂等收口)。
    pub fn on_planned_stop(&mut self, now: Instant) -> Vec<RecoverAction> {
        if self.fatal {
            return vec![];
        }
        self.gone_confirmations = 0;
        self.clear_recovery();
        self.last_spawn = None;
        self.enter(RecoveryState::Restarting, now);
        vec![self.audit(
            now,
            "计划内级联停止确认 (依赖重启中):不计预算，由 F20 依赖门控拉回".into(),
        )]
    }

    /// attach/liveness 报告目标消失一次 (内部累计到 confirmations 才触发)。
    pub fn on_liveness_gone(&mut self, now: Instant) -> Vec<RecoverAction> {
        if self.fatal {
            return vec![];
        }
        match self.state {
            RecoveryState::Running | RecoveryState::Suspect | RecoveryState::Starting => {
                self.gone_confirmations += 1;
                if self.gone_confirmations < self.policy.confirmations {
                    return vec![]; // 未到阈值，无动作
                }
                self.gone_confirmations = 0;
                let uptime = self
                    .last_spawn
                    .map(|(_, at)| now.duration_since(at))
                    .unwrap_or(Duration::ZERO);
                self.last_spawn = None;
                // 连续 confirmations 次查无 (pid,start_time) → 确定性死亡 (§8.1)
                if self.state == RecoveryState::Starting && uptime < self.policy.min_uptime {
                    self.crashloop_on_death(now, uptime)
                } else {
                    self.deterministic_death(now, Some(uptime))
                }
            }
            // 恢复执行中不再受理 (verify 超时/stop grace 是既定兜底路径)
            _ => vec![],
        }
    }

    /// liveness 存活 (清空消失计数;Suspect 下连续成功确认自愈回 Running)。
    pub fn on_liveness_ok(&mut self, now: Instant) -> Vec<RecoverAction> {
        self.gone_confirmations = 0;
        if self.fatal {
            return vec![];
        }
        match self.state {
            RecoveryState::Suspect => {
                // 连续成功由调用方保证 (§8.1)→ 自愈回 Running
                self.clear_recovery();
                self.backoff.reset();
                self.enter(RecoveryState::Running, now);
                vec![self.audit(now, "健康恢复确认，Suspect 自愈回 Running".into())]
            }
            RecoveryState::Starting => {
                if now.duration_since(self.entered_at) >= self.policy.min_uptime {
                    // 存活 > min_uptime 且健康首次成功 → Running;backoff 重置 (§8.2)
                    self.clear_recovery();
                    self.backoff.reset();
                    self.enter(RecoveryState::Running, now);
                    vec![self.audit(
                        now,
                        format!(
                            "启动完成 (存活 ≥ min_uptime {:?}),进入 Running",
                            self.policy.min_uptime
                        ),
                    )]
                } else {
                    vec![]
                }
            }
            // activate 观察期内健康恢复 → 验证成功，回 Running(§8.5)
            RecoveryState::Recovering if self.verify_deadline.is_some() => {
                self.finish_verify_success(now)
            }
            _ => vec![],
        }
    }

    /// 检查状态机级别变化 (健康类失败触发 Suspect/二次确认;§8.1 触发契约)。
    pub fn on_health(&mut self, now: Instant, level: HealthLevel) -> Vec<RecoverAction> {
        self.last_health = Some(level);
        if self.fatal {
            return vec![];
        }
        match level {
            // 健康：等同 liveness 存活 (自愈 / Starting→Running / verify 成功)
            HealthLevel::Ok => self.on_liveness_ok(now),
            HealthLevel::Warning => {
                // 无恢复动作;Suspect 下 Warning 打断"连续 Critical"链
                if self.state == RecoveryState::Suspect {
                    self.suspect_confirmations = 0;
                }
                vec![]
            }
            HealthLevel::Critical => self.on_health_critical(now),
        }
    }

    /// 周期驱动:stop_grace 升级、spawn 到点、startup_grace 超时、verify 观察期到点。
    pub fn on_tick(&mut self, now: Instant) -> Vec<RecoverAction> {
        if self.fatal {
            return vec![];
        }
        match self.state {
            RecoveryState::Recovering => {
                if let Some(deadline) = self.stop_deadline {
                    if now >= deadline {
                        // graceful 超时 → ForceKill(§8.2)
                        self.stop_deadline = None;
                        self.enter(RecoveryState::ForceKill, now);
                        return vec![
                            self.audit(now, "stop_grace 超时未退出，升级强杀".into()),
                            RecoverAction::ForceKill,
                        ];
                    }
                    return vec![]; // 仍在等待退出
                }
                if let Some(deadline) = self.verify_deadline {
                    if now >= deadline {
                        self.verify_deadline = None;
                        return self.evaluate_verify(now);
                    }
                    return vec![]; // 观察期未到
                }
                self.fire_spawn_due(now)
            }
            RecoveryState::Restarting | RecoveryState::CrashLoop => self.fire_spawn_due(now),
            // Running/Suspect/Starting/ForceKill 无定时职责：
            // - Starting 的 startup_grace 门在 on_health 里判 (超时后走正常路径);
            // - ForceKill 只等 stop 回报。
            _ => vec![],
        }
    }

    /// 调度器报告 spawn 结果 (Ok=新进程身份;Err=spawn 失败 → CrashLoop 计入 budget)。
    ///
    /// 受理范围：除 Starting(spawn 已在途，重复结果视为陈旧事件) 与 Fatal 外的
    /// 全部状态——**初始 spawn 在 Running 到达**;控制面重启可在 Suspect 到达;
    /// 恢复态按对应转移推进。
    pub fn on_spawn_result(
        &mut self,
        now: Instant,
        identity: Option<ProcessIdentity>,
    ) -> Vec<RecoverAction> {
        if self.fatal || self.state == RecoveryState::Starting {
            return vec![];
        }
        self.spawn_due = None;
        // 每次真实 spawn 尝试计入总重启数 (供 /status;预算计数见下)
        self.restarts_total = self.restarts_total.saturating_add(1);
        match identity {
            Some(id) => {
                self.clear_recovery();
                self.enter(RecoveryState::Starting, now);
                self.last_spawn = Some((id, now));
                vec![self.audit(
                    now,
                    format!(
                        "spawn 成功 ({id}),进入 Starting(startup_grace={:?})",
                        self.policy.startup_grace
                    ),
                )]
            }
            None => {
                self.last_spawn = None;
                // 所有自动重启均计数，含 spawn 失败重试 (§8.2 预算语义)
                self.budget.record(now);
                let mut actions = vec![
                    self.audit(now, "spawn 失败，计入重启预算".into()),
                    RecoverAction::JournalRestart { fast_exit: false },
                ];
                if self.budget.would_exceed(now) {
                    return self.enter_fatal(now, actions, "重启预算耗尽 (spawn 失败重试)");
                }
                self.enter(RecoveryState::CrashLoop, now);
                let delay = self.backoff.next_delay();
                self.spawn_due = Some(now + delay);
                actions.push(RecoverAction::ScheduleSpawn { delay });
                actions
            }
        }
    }

    /// SCM 目标 spawn 成功的专用入口 (`windows_service` 目标没有 (pid, start_time)
    /// 进程身份可登记;ROADMAP「原生 Windows Service 目标恢复」)。
    ///
    /// 与 [`Self::on_spawn_result`] 的 Some 分支同语义:→ Starting、清恢复在途
    /// 状态、计入 restarts_total;差别仅在**不记 identity**(last_spawn 保持
    /// None)—— SCM 目标的存活/死亡判定走 QueryStatus liveness
    /// ([`Self::on_liveness_ok`] / [`Self::on_liveness_gone`]),不走
    /// on_child_exit 的身份匹配。受理范围与 on_spawn_result 一致。
    pub fn on_scm_spawn_ok(&mut self, now: Instant) -> Vec<RecoverAction> {
        if self.fatal || self.state == RecoveryState::Starting {
            return vec![];
        }
        self.spawn_due = None;
        // 与 on_spawn_result(Some) 对齐：每次真实启动尝试计入总重启数
        self.restarts_total = self.restarts_total.saturating_add(1);
        self.clear_recovery();
        self.enter(RecoveryState::Starting, now);
        vec![self.audit(
            now,
            "SCM 服务启动成功，进入 Starting(无进程身份;QueryStatus 即 liveness 证据)".into(),
        )]
    }

    /// 调度器报告 stop 结果 (gone=true 表示已确认退出)。
    pub fn on_stop_result(&mut self, now: Instant, gone: bool) -> Vec<RecoverAction> {
        if self.fatal {
            return vec![];
        }
        match self.state {
            RecoveryState::ForceKill => {
                if gone {
                    self.stop_confirmed(now)
                } else {
                    // §8.2:ForceKill 失败 (Unix D 状态等)→ Fatal 闩锁
                    self.enter_fatal(now, vec![], "强杀失败 (进程未退出，可能处于不可中断 IO)")
                }
            }
            RecoveryState::Recovering if self.stop_deadline.is_some() => {
                if gone {
                    self.stop_confirmed(now)
                } else if self.stop_deadline.is_some_and(|dl| now >= dl) {
                    // stop_grace 已超时仍未退出 → 升级强杀 (on_tick 亦会兜底)
                    self.stop_deadline = None;
                    self.enter(RecoveryState::ForceKill, now);
                    vec![
                        self.audit(now, "stop_grace 超时仍未退出，升级强杀".into()),
                        RecoverAction::ForceKill,
                    ]
                } else {
                    vec![] // grace 未到，继续等待
                }
            }
            _ => vec![],
        }
    }

    /// 激活钩子结果 (Ok=已执行，进入 verify_for 观察)。
    pub fn on_activate_result(&mut self, now: Instant, ok: bool) -> Vec<RecoverAction> {
        if self.fatal {
            return vec![];
        }
        // 仅在 Recovering(hung_action=Activate) 且无任何在途动作时受理
        if self.state != RecoveryState::Recovering
            || self.verify_deadline.is_some()
            || self.stop_deadline.is_some()
            || self.spawn_due.is_some()
        {
            return vec![];
        }
        if ok {
            // 钩子成功 → verify_for 观察期：期内健康恢复 → Running;到点仍无 → 停止重启 (§8.5)
            self.verify_deadline = Some(now + self.verify_for);
            self.last_health = None; // 从此刻起重新观察健康
            vec![self.audit(
                now,
                format!(
                    "activate 钩子执行成功，进入 verify_for={:?} 观察期",
                    self.verify_for
                ),
            )]
        } else {
            // 钩子失败/超时 → 停止并重启，计入 budget(§8.2)
            self.budget.record(now);
            let mut actions = vec![
                self.audit(now, "activate 钩子失败，停止并重启 (计入重启预算)".into()),
                RecoverAction::JournalRestart { fast_exit: false },
            ];
            if self.budget.would_exceed(now) {
                return self.enter_fatal(now, actions, "重启预算耗尽 (activate 钩子失败)");
            }
            self.stop_deadline = Some(now + self.policy.stop_grace);
            actions.push(RecoverAction::SignalStop);
            actions
        }
    }

    /// 控制平面 reset:解除 Fatal、清预算、回 Running。
    pub fn reset(&mut self, now: Instant) -> Vec<RecoverAction> {
        self.fatal = false;
        self.budget.clear();
        self.backoff.reset();
        self.clear_recovery();
        self.last_spawn = None;
        self.enter(RecoveryState::Running, now);
        vec![self.audit(
            now,
            "控制面 reset:解除 Fatal 闩锁、清空重启预算，回到 Running".into(),
        )]
    }

    /// 控制平面 reset 后同步当前仍存活的亲生 child，保持原启动时刻。
    /// child 身份由独占 ProcessManager 提供，reset 本身不计为一次 spawn。
    pub fn reset_with_child(
        &mut self,
        now: Instant,
        child: Option<(ProcessIdentity, Instant)>,
    ) -> Vec<RecoverAction> {
        let actions = self.reset(now);
        self.last_spawn = child;
        actions
    }

    /// 已确认接管身份消失，按一次确定性死亡受预算与退避约束。
    /// 孤儿原始启动的单调时刻未知，不作 fast-exit 判定。
    pub fn on_reconciled_exit(&mut self, now: Instant) -> Vec<RecoverAction> {
        if self.fatal {
            return vec![];
        }
        self.last_spawn = None;
        self.deterministic_death(now, None)
    }

    /// 从 journal 恢复 (Fatal 闩锁 + budget 时间戳)。
    pub fn restore(
        &mut self,
        fatal: bool,
        restarts: &[SystemTime],
        now_wall: SystemTime,
        now_mono: Instant,
    ) {
        self.fatal = fatal;
        if fatal {
            self.state = RecoveryState::Fatal;
        }
        self.entered_at = now_mono;
        self.clear_recovery();
        self.last_spawn = None;
        self.budget.restore(restarts, now_wall, now_mono);
    }

    // ---- 内部转移 ----

    /// 确定性死亡 → Recovering + 按 backoff 排程 spawn(不经 Suspect,§8.1)。
    ///
    /// budget 语义 (DESIGN §8.2 尾注):**所有自动重启均计数**——慢死亡也记录，
    /// 防止"每 11 分钟死一次"的服务以 backoff_max 节奏无限重启而不触发 Fatal;
    /// fast-exit 的 Starting 路径由 [`Self::crashloop_on_death`] 计数，不经过这里。
    fn deterministic_death(
        &mut self,
        now: Instant,
        uptime: Option<Duration>,
    ) -> Vec<RecoverAction> {
        self.clear_recovery();
        self.budget.record(now);
        let fast = uptime.is_some_and(|duration| duration < self.policy.min_uptime);
        let mut actions = vec![
            self.audit(
                now,
                format!(
                    "孩子进程退出 (确定性死亡，uptime={:?}{}) 进入恢复",
                    uptime,
                    if fast { ",fast_exit" } else { "" }
                ),
            ),
            RecoverAction::JournalRestart { fast_exit: fast },
        ];
        if self.budget.would_exceed(now) {
            return self.enter_fatal(now, actions, "重启预算耗尽 (确定性死亡)");
        }
        self.enter(RecoveryState::Recovering, now);
        let delay = self.backoff.next_delay();
        self.spawn_due = Some(now + delay);
        actions.push(RecoverAction::ScheduleSpawn { delay });
        actions
    }

    /// Starting 内 fast-exit → CrashLoop:计入 budget，可能 Fatal(§8.2)。
    fn crashloop_on_death(&mut self, now: Instant, uptime: Duration) -> Vec<RecoverAction> {
        self.budget.record(now);
        let mut actions = vec![
            self.audit(
                now,
                format!(
                    "进程退出过快 (uptime={:?} < min_uptime={:?}),计入重启预算",
                    uptime, self.policy.min_uptime
                ),
            ),
            RecoverAction::JournalRestart { fast_exit: true },
        ];
        if self.budget.would_exceed(now) {
            return self.enter_fatal(now, actions, "重启预算耗尽 (快速退出循环)");
        }
        self.enter(RecoveryState::CrashLoop, now);
        let delay = self.backoff.next_delay();
        self.spawn_due = Some(now + delay);
        actions.push(RecoverAction::ScheduleSpawn { delay });
        actions
    }

    /// 健康类失败达到 confirmations → 进入 Recovering，按 hung_action 执行 (§8.1/§8.5)。
    ///
    /// budget:仅 **Restart** 分支计一次 (SRE 评审 P1:健康类挂死的自动重启此前
    /// 零计数，"反复死锁但响应 SIGTERM"的服务永不 Fatal)。Activate 分支不计 ——
    /// 激活成功意味着**没有发生重启**;激活失败转为重启时在各自失败点
    /// (钩子失败 / verify 超时 / 观察期死亡) 计数，恰好一次。
    fn begin_recovery(&mut self, now: Instant) -> Vec<RecoverAction> {
        self.clear_recovery();
        self.enter(RecoveryState::Recovering, now);
        let mut actions = vec![self.audit(
            now,
            format!(
                "健康失败确认 ×{},进入恢复 (hung_action={:?})",
                self.policy.confirmations, self.policy.hung_action
            ),
        )];
        match self.policy.hung_action {
            HungAction::Restart => {
                self.budget.record(now);
                actions.push(RecoverAction::JournalRestart { fast_exit: false });
                if self.budget.would_exceed(now) {
                    return self.enter_fatal(now, actions, "重启预算耗尽 (健康类挂死)");
                }
                self.stop_deadline = Some(now + self.policy.stop_grace);
                actions.push(RecoverAction::SignalStop);
            }
            HungAction::Activate => {
                actions.push(RecoverAction::RunActivate);
            }
        }
        actions
    }

    /// Critical 的受理窗口:Running/Suspect 直接受理;Starting 仅在
    /// startup_grace 过后受理 (启动期失败不算挂死，§8.2 Starting 边);
    /// 恢复执行期间 (Recovering/ForceKill/Restarting/CrashLoop) 忽略，
    /// 防"刚重启被判挂死"(§8.1)。
    fn on_health_critical(&mut self, now: Instant) -> Vec<RecoverAction> {
        let eligible = match self.state {
            RecoveryState::Running | RecoveryState::Suspect => true,
            RecoveryState::Starting => {
                now.duration_since(self.entered_at) >= self.policy.startup_grace
            }
            _ => false,
        };
        if !eligible {
            return vec![];
        }
        self.suspect_confirmations += 1;
        if self.suspect_confirmations >= self.policy.confirmations {
            self.begin_recovery(now)
        } else if self.state != RecoveryState::Suspect {
            self.enter(RecoveryState::Suspect, now);
            vec![self.audit(
                now,
                format!(
                    "健康 Critical,Suspect 待二次确认 ({}/{})",
                    self.suspect_confirmations, self.policy.confirmations
                ),
            )]
        } else {
            vec![] // 已在 Suspect，计数推进但未达阈值
        }
    }

    /// stop(或强杀) 确认完成 → Restarting + 按 backoff 排程 spawn(§8.2)。
    fn stop_confirmed(&mut self, now: Instant) -> Vec<RecoverAction> {
        self.stop_deadline = None;
        self.verify_deadline = None;
        self.last_spawn = None;
        self.enter(RecoveryState::Restarting, now);
        let delay = self.backoff.next_delay();
        self.spawn_due = Some(now + delay);
        vec![
            self.audit(now, "停止完成，backoff 后重启".into()),
            RecoverAction::ScheduleSpawn { delay },
        ]
    }

    /// 恢复失败 (verify 失败/钩子失败/观察期死亡)→ 计入 budget 并排重启。
    fn fail_recover_and_respawn(&mut self, now: Instant, reason: &str) -> Vec<RecoverAction> {
        self.budget.record(now);
        let mut actions = vec![
            self.audit(now, format!("{reason},恢复失败计入重启预算，直接排程重启")),
            RecoverAction::JournalRestart { fast_exit: false },
        ];
        if self.budget.would_exceed(now) {
            return self.enter_fatal(now, actions, "重启预算耗尽 (恢复失败)");
        }
        self.enter(RecoveryState::Restarting, now);
        let delay = self.backoff.next_delay();
        self.spawn_due = Some(now + delay);
        actions.push(RecoverAction::ScheduleSpawn { delay });
        actions
    }

    /// verify 观察期到点：最近健康反馈为 Ok → 成功回 Running;否则停止并重启 (§8.2)。
    fn evaluate_verify(&mut self, now: Instant) -> Vec<RecoverAction> {
        if self.last_health == Some(HealthLevel::Ok) {
            self.finish_verify_success(now)
        } else {
            // 验证失败 → stop → Restarting(计入 budget,§8.2 activate 边)
            self.budget.record(now);
            let mut actions = vec![
                self.audit(
                    now,
                    format!(
                        "activate 观察期到点健康未恢复 (last_health={:?}),停止并重启 (计入重启预算)",
                        self.last_health
                    ),
                ),
                RecoverAction::JournalRestart { fast_exit: false },
            ];
            if self.budget.would_exceed(now) {
                return self.enter_fatal(now, actions, "重启预算耗尽 (activate 验证失败)");
            }
            self.stop_deadline = Some(now + self.policy.stop_grace);
            actions.push(RecoverAction::SignalStop);
            actions
        }
    }

    /// verify 成功 → Running(进程长期存活，backoff 一并重置，§8.2 尾注)。
    fn finish_verify_success(&mut self, now: Instant) -> Vec<RecoverAction> {
        self.clear_recovery();
        self.backoff.reset();
        self.enter(RecoveryState::Running, now);
        vec![self.audit(
            now,
            "activate 验证通过 (观察期内健康恢复),回 Running".into(),
        )]
    }

    /// 进入 Fatal 闩锁 (窗口滑过不自动恢复，仅 reset 解除，§8.2)。
    fn enter_fatal(
        &mut self,
        now: Instant,
        mut actions: Vec<RecoverAction>,
        reason: &str,
    ) -> Vec<RecoverAction> {
        self.clear_recovery();
        self.last_spawn = None;
        self.enter(RecoveryState::Fatal, now);
        self.fatal = true;
        actions.push(self.audit(now, format!("进入 Fatal:{reason}")));
        actions.push(RecoverAction::MarkFatal);
        actions
    }

    /// spawn_due 到点 → 回 `ScheduleSpawn{delay:0}`,由调度器真正执行 (约定见模块 doc)。
    fn fire_spawn_due(&mut self, now: Instant) -> Vec<RecoverAction> {
        if !self.spawn_due.is_some_and(|due| now >= due) {
            return vec![];
        }
        self.spawn_due = None;
        let mut actions = vec![];
        if self.state != RecoveryState::Restarting {
            self.enter(RecoveryState::Restarting, now);
            actions.push(self.audit(now, "backoff 到点，发起 spawn".into()));
        }
        actions.push(RecoverAction::ScheduleSpawn {
            delay: Duration::ZERO,
        });
        actions
    }

    /// 进入新状态并刷新 entered_at。
    fn enter(&mut self, to: RecoveryState, now: Instant) {
        self.state = to;
        self.entered_at = now;
        self.transitions_total = self.transitions_total.saturating_add(1);
    }

    /// 清理一轮恢复的在途状态 (新恢复轮从干净状态开始)。
    fn clear_recovery(&mut self) {
        self.spawn_due = None;
        self.stop_deadline = None;
        self.verify_deadline = None;
        self.suspect_confirmations = 0;
        self.gone_confirmations = 0;
        self.last_health = None;
    }

    /// 由注入的单调时刻派生墙钟 (Audit.at 用;构造锚点换算，不读系统钟)。
    fn wall_now(&self, now: Instant) -> SystemTime {
        self.wall_anchor
            .checked_add(now.duration_since(self.mono_anchor))
            .unwrap_or(self.wall_anchor)
    }

    fn audit(&self, now: Instant, message: String) -> RecoverAction {
        RecoverAction::Audit {
            message: format!("[{}] {}", self.name, message),
            at: self.wall_now(now),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RestartCfg;

    const MIN_UPTIME: Duration = Duration::from_secs(10);
    const STARTUP_GRACE: Duration = Duration::from_secs(30);

    /// t0 之后第 s 秒 (全部注入 Instant，不读系统钟)。
    fn t_at(t0: Instant, secs: u64) -> Instant {
        t0.checked_add(Duration::from_secs(secs))
            .expect("test time overflow")
    }

    fn policy_with(confirmations: u32, max_restarts: u32, hung: HungAction) -> RestartPolicy {
        let c = RestartCfg {
            min_uptime: MIN_UPTIME,
            startup_grace: STARTUP_GRACE,
            stop_grace: Duration::from_secs(15),
            backoff_initial: Duration::from_secs(1),
            backoff_max: Duration::from_secs(8),
            max_restarts,
            restart_window: Duration::from_secs(60),
            confirmations,
            hung_action: hung,
        };
        RestartPolicy::from(&c)
    }

    fn fsm_at(
        _t0: Instant,
        confirmations: u32,
        max_restarts: u32,
        hung: HungAction,
    ) -> RecoveryFsm {
        RecoveryFsm::new("svc", policy_with(confirmations, max_restarts, hung))
    }

    fn ident(pid: u32) -> ProcessIdentity {
        ProcessIdentity::new(pid, 1700000000)
    }

    /// 标准开局:spawn 成功 (Starting)→ 存活过 min_uptime 且健康 OK(Running)。
    fn running(fsm: &mut RecoveryFsm, t0: Instant, pid: u32) {
        let _ = fsm.on_spawn_result(t_at(t0, 1), Some(ident(pid)));
        let _ = fsm.on_liveness_ok(t_at(t0, 1 + MIN_UPTIME.as_secs()));
        assert_eq!(fsm.state(), RecoveryState::Running);
    }

    fn find_schedule(actions: &[RecoverAction]) -> Option<Duration> {
        actions.iter().find_map(|a| match a {
            RecoverAction::ScheduleSpawn { delay } => Some(*delay),
            _ => None,
        })
    }

    fn assert_audit(actions: &[RecoverAction]) {
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, RecoverAction::Audit { .. })),
            "状态转移必须产生 Audit 记录：{actions:?}"
        );
    }

    #[test]
    fn policy_update_preserves_watermarks_and_applies_confirmation_and_action() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 3, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        assert_eq!(fsm.state(), RecoveryState::Suspect);
        let transitions = fsm.transitions_total();

        fsm.update_policy(policy_with(2, 5, HungAction::Activate));
        assert_eq!(fsm.state(), RecoveryState::Suspect);
        assert_eq!(fsm.transitions_total(), transitions);
        let actions = fsm.on_health(t_at(t0, 101), HealthLevel::Critical);
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, RecoverAction::RunActivate))
        );
        assert!(
            !actions
                .iter()
                .any(|action| matches!(action, RecoverAction::SignalStop))
        );
    }

    #[test]
    fn policy_update_preserves_budget_backoff_and_existing_deadline() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        fsm.budget.record(t_at(t0, 1));
        fsm.backoff.next_delay();
        fsm.stop_deadline = Some(t_at(t0, 20));
        let mut policy = policy_with(2, 1, HungAction::Restart);
        policy.backoff_initial = Duration::from_secs(5);
        policy.backoff_max = Duration::from_secs(20);
        policy.restart_window = Duration::from_secs(10);
        fsm.update_policy(policy);

        assert_eq!(fsm.restarts_in_window(t_at(t0, 2)), 1);
        assert!(fsm.budget.would_exceed(t_at(t0, 2)));
        assert_eq!(fsm.restarts_in_window(t_at(t0, 12)), 0);
        assert_eq!(fsm.backoff.next_delay(), Duration::from_secs(10));
        assert_eq!(fsm.stop_deadline, Some(t_at(t0, 20)));
    }

    #[test]
    fn policy_update_allows_in_flight_activate_to_finish() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 1, 5, HungAction::Activate);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        fsm.update_policy(policy_with(1, 5, HungAction::Restart));
        let actions = fsm.on_activate_result(t_at(t0, 101), true);
        assert!(!actions.is_empty());
        assert!(fsm.verify_deadline.is_some());
        let _ = fsm.on_health(t_at(t0, 102), HealthLevel::Ok);
        assert_eq!(fsm.state(), RecoveryState::Running);
    }

    #[test]
    fn health_restart_budget_has_one_journal_event_through_child_exit() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 1, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let mut actions = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        actions.extend(fsm.on_child_exit(t_at(t0, 101), ident(100)));
        assert_eq!(fsm.restarts_in_window(t_at(t0, 101)), 1);
        assert_eq!(
            actions
                .iter()
                .filter(|action| matches!(action, RecoverAction::JournalRestart { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn adopted_identity_accepts_observed_exit_and_preserves_spawn_time() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        let original = ident(100);
        let observed = ProcessIdentity::new(100, original.start_time + 10);
        running(&mut fsm, t0, 100);
        let spawned_at = fsm.last_spawn.unwrap().1;
        fsm.adopt_identity(original, observed);
        assert_eq!(fsm.last_spawn, Some((observed, spawned_at)));
        assert!(fsm.on_child_exit(t_at(t0, 100), original).is_empty());
        let actions = fsm.on_child_exit(t_at(t0, 101), observed);
        assert_eq!(fsm.state(), RecoveryState::Recovering);
        assert!(find_schedule(&actions).is_some());
        assert_eq!(
            actions
                .iter()
                .filter(|action| matches!(action, RecoverAction::JournalRestart { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn adopted_identity_rejects_another_pid_or_stale_previous_identity() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let before = fsm.last_spawn;
        fsm.adopt_identity(ident(100), ident(101));
        assert_eq!(fsm.last_spawn, before);
        fsm.adopt_identity(ProcessIdentity::new(100, 1), ProcessIdentity::new(100, 2));
        assert_eq!(fsm.last_spawn, before);
    }

    #[test]
    fn reset_with_live_child_preserves_exit_tracking_and_spawn_counters() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 1, 1, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let child = fsm.last_spawn;
        let spawns = fsm.restarts_total();
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        assert!(fsm.is_fatal());

        let _ = fsm.reset_with_child(t_at(t0, 101), child);
        assert!(!fsm.is_fatal());
        assert_eq!(fsm.last_spawn, child);
        assert_eq!(fsm.restarts_total(), spawns);
        let mut policy = policy_with(1, 5, HungAction::Restart);
        policy.min_uptime = Duration::from_secs(100);
        fsm.update_policy(policy);
        let actions = fsm.on_child_exit(t_at(t0, 102), ident(100));
        assert!(find_schedule(&actions).is_some());
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, RecoverAction::JournalRestart { fast_exit: false }))
        );
    }

    #[test]
    fn reconciled_exit_honors_restored_budget_and_fatal_latch() {
        let t0 = Instant::now();
        let wall = SystemTime::now();
        let mut fsm = fsm_at(t0, 1, 2, HungAction::Restart);
        fsm.restore(false, &[wall], wall, t0);
        let actions = fsm.on_reconciled_exit(t_at(t0, 1));
        assert!(fsm.is_fatal());
        assert_eq!(fsm.restarts_in_window(t_at(t0, 1)), 2);
        assert!(find_schedule(&actions).is_none());
        assert_eq!(
            actions
                .iter()
                .filter(|action| matches!(action, RecoverAction::JournalRestart { .. }))
                .count(),
            1
        );
        assert!(fsm.on_reconciled_exit(t_at(t0, 2)).is_empty());
    }

    #[test]
    fn activate_failures_latch_fatal_and_journal_budget_at_limit() {
        let t0 = Instant::now();
        for hook_succeeds in [false, true] {
            let mut fsm = fsm_at(t0, 1, 1, HungAction::Activate);
            running(&mut fsm, t0, 100);
            fsm.set_verify_for(Duration::from_secs(1));
            let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
            let hook_actions = fsm.on_activate_result(t_at(t0, 101), hook_succeeds);
            let actions = if hook_succeeds {
                fsm.on_tick(t_at(t0, 102))
            } else {
                hook_actions
            };
            assert!(fsm.is_fatal());
            assert_eq!(fsm.restarts_in_window(t_at(t0, 102)), 1);
            assert_eq!(
                actions
                    .iter()
                    .filter(|action| matches!(action, RecoverAction::JournalRestart { .. }))
                    .count(),
                1
            );
            assert!(
                actions
                    .iter()
                    .any(|action| matches!(action, RecoverAction::MarkFatal))
            );
            assert!(
                !actions
                    .iter()
                    .any(|action| matches!(action, RecoverAction::SignalStop))
            );
        }
    }

    // ---- §8.2 边:Running --确定性死亡--> Recovering ----
    #[test]
    fn child_exit_running_goes_recovering_not_suspect() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let actions = fsm.on_child_exit(t_at(t0, 1 + MIN_UPTIME.as_secs() + 60), ident(100));
        assert_eq!(fsm.state(), RecoveryState::Recovering);
        assert!(!fsm.is_fatal());
        assert_eq!(find_schedule(&actions), Some(Duration::from_secs(1))); // backoff 首档
        assert_audit(&actions);
    }

    // ---- §8.2 边:Starting --退出且 uptime<min_uptime--> CrashLoop(计入 budget) ----
    #[test]
    fn child_exit_starting_fast_goes_crashloop_and_counts_budget() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        let _ = fsm.on_spawn_result(t_at(t0, 1), Some(ident(100)));
        assert_eq!(fsm.state(), RecoveryState::Starting);
        // uptime 2s < min_uptime 10s
        let actions = fsm.on_child_exit(t_at(t0, 3), ident(100));
        assert_eq!(fsm.state(), RecoveryState::CrashLoop);
        assert_eq!(
            fsm.restarts_in_window(t_at(t0, 3)),
            1,
            "fast exit 计入 budget"
        );
        assert_eq!(find_schedule(&actions), Some(Duration::from_secs(1)));
    }

    // ---- Starting 内已过 min_uptime 的死亡 → 与 Running 同路径 ----
    #[test]
    fn child_exit_starting_after_min_uptime_goes_recovering() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        let _ = fsm.on_spawn_result(t_at(t0, 1), Some(ident(100)));
        let actions = fsm.on_child_exit(t_at(t0, 1 + MIN_UPTIME.as_secs() + 1), ident(100));
        assert_eq!(fsm.state(), RecoveryState::Recovering);
        assert_eq!(
            fsm.restarts_in_window(t_at(t0, 60)),
            1,
            "慢死亡同样计入 budget(DESIGN §8.2:所有自动重启均计数，防'每 11 分钟死一次'永不过载)"
        );
        assert_audit(&actions);
    }

    // ---- 运行观测:transitions_total 随每次状态转移递增 ----
    #[test]
    fn transitions_total_counts_every_enter() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        let base = fsm.transitions_total();
        running(&mut fsm, t0, 100); // 至少 Running 迁移一次
        assert!(fsm.transitions_total() > base, "enter 必须递增计数");
        let after_running = fsm.transitions_total();
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical); // Running→Suspect
        assert_eq!(fsm.transitions_total(), after_running + 1);
        let _ = fsm.on_liveness_ok(t_at(t0, 110)); // Suspect→Running
        assert_eq!(fsm.transitions_total(), after_running + 2);
    }

    // ---- 修复回归:SRE P0 —— ForceKill 态收到 child_exit 必须收尾进 Restarting
    // (此前落入空臂，FSM 永久卡死，SIGKILL 成功后服务不再拉起) ----
    #[test]
    fn force_kill_success_child_exit_goes_restarting() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        // 健康挂死：两次 Critical → begin_recovery(SignalStop) → grace 超时 → ForceKill
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let _ = fsm.on_health(t_at(t0, 200), HealthLevel::Critical);
        assert_eq!(fsm.state(), RecoveryState::Recovering);
        // stop_deadline = t+200+15s;到点未确认 → ForceKill
        let _ = fsm.on_tick(t_at(t0, 200 + 16));
        assert_eq!(fsm.state(), RecoveryState::ForceKill);
        // SIGKILL 成功，进程死亡 (亲生退出事件)
        let actions = fsm.on_child_exit(t_at(t0, 220), ident(100));
        assert_eq!(
            fsm.state(),
            RecoveryState::Restarting,
            "ForceKill 收到孩子退出必须收尾进 Restarting(评审 P0 回归)"
        );
        assert!(find_schedule(&actions).is_some());
        // 排程到点 → spawn 成功 → Starting(整链闭合)
        let _ = fsm.on_tick(t_at(t0, 121));
        let _ = fsm.on_spawn_result(t_at(t0, 121), Some(ident(101)));
        assert_eq!(fsm.state(), RecoveryState::Starting);
    }

    // ---- 修复回归:SRE P1 —— 健康类挂死的自动重启计入 budget( Restart 分支) ----
    #[test]
    fn health_hang_restart_counts_budget() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let _ = fsm.on_health(t_at(t0, 200), HealthLevel::Critical); // 确认 → begin_recovery
        assert_eq!(
            fsm.restarts_in_window(t_at(t0, 200)),
            1,
            "健康类挂死的自动重启必须计入 budget(此前零计数，永不 Fatal)"
        );
    }

    // ---- 语义:activate 成功 + verify 内恢复 = 未发生重启，不计 budget ----
    #[test]
    fn activate_success_recovery_counts_no_budget() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Activate);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let _ = fsm.on_health(t_at(t0, 200), HealthLevel::Critical); // begin_recovery(RunActivate)
        assert_eq!(fsm.state(), RecoveryState::Recovering);
        let _ = fsm.on_activate_result(t_at(t0, 201), true);
        // verify 观察期内健康恢复 → Running
        let _ = fsm.on_liveness_ok(t_at(t0, 210));
        assert_eq!(fsm.state(), RecoveryState::Running);
        assert_eq!(
            fsm.restarts_in_window(t_at(t0, 210)),
            0,
            "激活成功 = 未重启，不计 budget"
        );
    }

    // ---- "每 11 分钟慢死一次"→ 预算耗尽 Fatal(本修复的动机场景) ----
    #[test]
    fn repeated_slow_deaths_exhaust_budget_and_latch_fatal() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 3, HungAction::Restart); // max_restarts=3
        running(&mut fsm, t0, 100);
        // 循环：慢死亡 (>min_uptime)→ 排程 → spawn 成功 → 存活过 min_uptime → 再死亡。
        // 节奏：死亡@T → spawn@T+2(backoff 1s)→ 存活 11s → 死亡@T+13;
        // 三次死亡全部落在 60s 预算窗口内 (滑动窗口语义)。
        let mut now_secs = 100u64;
        for round in 0..3u32 {
            let actions = fsm.on_child_exit(t_at(t0, now_secs), ident(100 + round));
            if round < 2 {
                assert!(
                    find_schedule(&actions).is_some(),
                    "预算未耗尽：慢死亡必须排程重启"
                );
            } else {
                // 第 3 次死亡：窗口内 budget=3 → Fatal，不再排程 (终态闩锁)
                assert!(find_schedule(&actions).is_none(), "预算耗尽不得再排程");
            }
            let fire = t_at(t0, now_secs + 2);
            let _ = fsm.on_tick(fire);
            let _ = fsm.on_spawn_result(fire, Some(ident(101 + round)));
            if round < 2 {
                assert_eq!(fsm.state(), RecoveryState::Starting);
            }
            now_secs += 13;
        }
        // 第 3 次慢死亡：窗口内 budget=3 → Fatal
        assert!(fsm.is_fatal(), "连续慢死亡耗尽预算必须 Fatal");
        assert_eq!(fsm.state(), RecoveryState::Fatal);
    }

    // ---- 边界 (测试专家：变异存活):恰在 startup_grace 时刻的 Critical 受理 ----
    #[test]
    fn health_critical_exactly_at_startup_grace_enters_suspect() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        let _ = fsm.on_spawn_result(t_at(t0, 1), Some(ident(100))); // Starting @ t0+1
        // entered_at = t0+1;恰在 entered_at + STARTUP_GRACE(t0+31) 的 Critical 必须受理
        let _ = fsm.on_health(t_at(t0, 1 + STARTUP_GRACE.as_secs()), HealthLevel::Critical);
        assert_eq!(
            fsm.state(),
            RecoveryState::Suspect,
            "恰在 startup_grace 边界的 Critical 必须受理 (>= 语义)"
        );
    }

    // ---- 陈旧身份 (§8.3) 忽略 ----
    #[test]
    fn child_exit_with_stale_identity_is_ignored() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let actions = fsm.on_child_exit(t_at(t0, 100), ident(999));
        assert!(actions.is_empty());
        assert_eq!(fsm.state(), RecoveryState::Running);
    }

    // ---- on_liveness_gone 累计到 confirmations 才触发 (默认 2 次) ----
    #[test]
    fn liveness_gone_requires_confirmations() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let a1 = fsm.on_liveness_gone(t_at(t0, 90));
        assert!(a1.is_empty(), "首次消失未达阈值，无动作");
        assert_eq!(fsm.state(), RecoveryState::Running);
        let a2 = fsm.on_liveness_gone(t_at(t0, 95));
        assert_eq!(fsm.state(), RecoveryState::Recovering);
        assert_eq!(find_schedule(&a2), Some(Duration::from_secs(1)));
    }

    // ---- on_liveness_ok 清空 gone 累计 ----
    #[test]
    fn liveness_ok_clears_gone_count() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_liveness_gone(t_at(t0, 90));
        let _ = fsm.on_liveness_ok(t_at(t0, 91)); // 清计数
        let _ = fsm.on_liveness_gone(t_at(t0, 92)); // 又只有 1 次
        assert_eq!(fsm.state(), RecoveryState::Running, "单次消失不应触发死亡");
        let _ = fsm.on_liveness_gone(t_at(t0, 93)); // 第 2 次
        assert_eq!(fsm.state(), RecoveryState::Recovering);
    }

    // ---- §8.2 边:Running --健康失败达阈值--> Suspect(confirmations=2 二次确认) ----
    #[test]
    fn health_critical_running_enters_suspect_first() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let actions = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        assert_eq!(fsm.state(), RecoveryState::Suspect);
        assert!(find_schedule(&actions).is_none(), "Suspect 阶段无恢复动作");
        assert_audit(&actions);
    }

    // ---- §8.2 边:Suspect --再次确认失败--> Recovering(hung_action=Restart → SignalStop) ----
    #[test]
    fn suspect_second_critical_recovers_with_signal_stop() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let actions = fsm.on_health(t_at(t0, 110), HealthLevel::Critical);
        assert_eq!(fsm.state(), RecoveryState::Recovering);
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, RecoverAction::SignalStop)),
            "hung_action=Restart → SignalStop: {actions:?}"
        );
        assert_audit(&actions);
    }

    // ---- confirmations=1:单次 Critical 直接恢复 (边界语义) ----
    #[test]
    fn confirmations_1_recovers_on_first_critical() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 1, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let actions = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        assert_eq!(fsm.state(), RecoveryState::Recovering);
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, RecoverAction::SignalStop))
        );
    }

    // ---- §8.2 边:Suspect --确认恢复--> Running ----
    #[test]
    fn suspect_self_heals_on_liveness_ok() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let actions = fsm.on_liveness_ok(t_at(t0, 110));
        assert_eq!(fsm.state(), RecoveryState::Running);
        assert_audit(&actions);
    }

    // ---- 健康 Ok 亦可确认自愈 ----
    #[test]
    fn suspect_self_heals_on_health_ok() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let actions = fsm.on_health(t_at(t0, 110), HealthLevel::Ok);
        assert_eq!(fsm.state(), RecoveryState::Running);
        assert_audit(&actions);
    }

    // ---- Warning 无恢复动作;Suspect 下 Warning 打断连续 Critical 链 ----
    #[test]
    fn warning_has_no_recovery_action_and_breaks_chain() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let actions = fsm.on_health(t_at(t0, 100), HealthLevel::Warning);
        assert!(actions.is_empty());
        assert_eq!(fsm.state(), RecoveryState::Running);
        let _ = fsm.on_health(t_at(t0, 110), HealthLevel::Critical); // Suspect(1/2)
        let _ = fsm.on_health(t_at(t0, 120), HealthLevel::Warning); // 打断链
        let _ = fsm.on_health(t_at(t0, 130), HealthLevel::Critical); // 重新 1/2
        assert_eq!(
            fsm.state(),
            RecoveryState::Suspect,
            "链被打断后单次 Critical 不触发"
        );
        let _ = fsm.on_health(t_at(t0, 140), HealthLevel::Critical);
        assert_eq!(fsm.state(), RecoveryState::Recovering);
    }

    // ---- §8.1/§8.2:恢复执行期间健康失败被忽略 (防"刚重启被判挂死") ----
    #[test]
    fn health_critical_ignored_during_recovery() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_child_exit(t_at(t0, 100), ident(100)); // Recovering
        assert!(
            fsm.on_health(t_at(t0, 105), HealthLevel::Critical)
                .is_empty()
        );
        assert_eq!(fsm.state(), RecoveryState::Recovering);
        let _ = fsm.on_tick(t_at(t0, 101)); // → Restarting(spawn 已发起)
        assert!(
            fsm.on_health(t_at(t0, 107), HealthLevel::Critical)
                .is_empty()
        );
        assert_eq!(fsm.state(), RecoveryState::Restarting);
        let _ = fsm.on_spawn_result(t_at(t0, 108), Some(ident(101))); // → Starting
        assert!(
            fsm.on_health(t_at(t0, 109), HealthLevel::Critical)
                .is_empty()
        );
        assert_eq!(fsm.state(), RecoveryState::Starting, "startup_grace 内忽略");
    }

    // ---- §8.2 边:Starting --startup_grace 内健康失败被忽略 ----
    #[test]
    fn health_critical_ignored_within_startup_grace() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        let _ = fsm.on_spawn_result(t_at(t0, 1), Some(ident(100)));
        let actions = fsm.on_health(t_at(t0, 20), HealthLevel::Critical); // grace=30s 内
        assert!(actions.is_empty());
        assert_eq!(fsm.state(), RecoveryState::Starting, "grace 内不进 Suspect");
    }

    // ---- §8.2 边:Starting --健康持续失败超过 startup_grace--> Suspect ----
    #[test]
    fn health_critical_after_startup_grace_enters_suspect() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        let _ = fsm.on_spawn_result(t_at(t0, 1), Some(ident(100)));
        let actions = fsm.on_health(
            t_at(t0, 1 + STARTUP_GRACE.as_secs() + 1),
            HealthLevel::Critical,
        );
        assert_eq!(fsm.state(), RecoveryState::Suspect);
        assert!(find_schedule(&actions).is_none());
    }

    // ---- §8.2 边:Starting --存活>min_uptime 且健康首次成功--> Running(backoff reset) ----
    #[test]
    fn starting_moves_to_running_after_min_uptime_and_resets_backoff() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        // 先制造 backoff 档位：死亡一次再 spawn 成功
        running(&mut fsm, t0, 100);
        let _ = fsm.on_child_exit(t_at(t0, 100), ident(100)); // backoff attempt=1
        let _ = fsm.on_spawn_result(t_at(t0, 102), Some(ident(101))); // Starting
        assert_eq!(fsm.state(), RecoveryState::Starting);
        // 未到 min_uptime 的健康成功：停留 Starting
        let a = fsm.on_liveness_ok(t_at(t0, 105));
        assert!(a.is_empty());
        assert_eq!(fsm.state(), RecoveryState::Starting);
        // 过 min_uptime 且健康成功 → Running
        let actions = fsm.on_liveness_ok(t_at(t0, 112));
        assert_eq!(fsm.state(), RecoveryState::Running);
        assert_audit(&actions);
        // backoff 已重置：下次死亡又是首档 1s
        let actions = fsm.on_child_exit(t_at(t0, 200), ident(101));
        assert_eq!(find_schedule(&actions), Some(Duration::from_secs(1)));
    }

    // ---- §8.2 边:Recovering --graceful 成功--> Restarting ----
    #[test]
    fn stop_confirmed_goes_restarting_with_backoff() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let _ = fsm.on_health(t_at(t0, 110), HealthLevel::Critical); // Recovering + SignalStop
        let actions = fsm.on_stop_result(t_at(t0, 112), true);
        assert_eq!(fsm.state(), RecoveryState::Restarting);
        assert_eq!(find_schedule(&actions), Some(Duration::from_secs(1)));
    }

    // ---- reaper 事件在 stop 待确认期间到达 → 视同 stop 完成 ----
    #[test]
    fn child_exit_during_stop_pending_counts_as_stop_confirmed() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let _ = fsm.on_health(t_at(t0, 110), HealthLevel::Critical); // Recovering + SignalStop
        let actions = fsm.on_child_exit(t_at(t0, 111), ident(100));
        assert_eq!(fsm.state(), RecoveryState::Restarting);
        assert!(find_schedule(&actions).is_some());
    }

    // ---- F20 计划内级联停止：退出不计预算、不排程 (第六轮评审 P3)----
    #[test]
    fn planned_stop_does_not_record_budget_or_schedule_spawn() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let actions = fsm.on_planned_stop(t_at(t0, 111));
        assert_eq!(fsm.state(), RecoveryState::Restarting);
        // 关键不变量：无 ScheduleSpawn(拉回由 F20 依赖门控负责)、无 JournalRestart
        // (预算持久化动作;计划内停止不得烧 dependent 预算)。
        assert!(
            find_schedule(&actions).is_none(),
            "计划内停止不得排程 spawn"
        );
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, RecoverAction::JournalRestart { .. })),
            "计划内停止不得产生预算事件"
        );
        // 幂等收口：重复确认 (如快照缺失与退出事件先后到达) 不重复转移。
        let again = fsm.on_planned_stop(t_at(t0, 112));
        assert_eq!(fsm.state(), RecoveryState::Restarting);
        assert!(find_schedule(&again).is_none());
        // 对照：同样的退出若走 on_child_exit(deterministic_death) 会记录预算。
        let mut fsm2 = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm2, t0, 100);
        let _ = fsm2.on_child_exit(t_at(t0, 111), ident(100));
        assert!(
            fsm2.budget.count(t_at(t0, 112)) >= 1,
            "对照：确定性死亡路径必须计预算"
        );
    }

    // ---- §8.2 边:Recovering --graceful 超时--> ForceKill --成功--> Restarting ----
    #[test]
    fn stop_grace_timeout_escalates_to_force_kill_then_restarting() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let _ = fsm.on_health(t_at(t0, 110), HealthLevel::Critical); // Recovering + SignalStop @110
        // grace=15s:未到点 on_tick/on_stop_result(false) 都不升级
        assert!(fsm.on_tick(t_at(t0, 120)).is_empty());
        assert!(fsm.on_stop_result(t_at(t0, 121), false).is_empty());
        // 到点升级 (tick 驱动)
        let actions = fsm.on_tick(t_at(t0, 125));
        assert_eq!(fsm.state(), RecoveryState::ForceKill);
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, RecoverAction::ForceKill))
        );
        // 强杀成功 → Restarting
        let actions = fsm.on_stop_result(t_at(t0, 126), true);
        assert_eq!(fsm.state(), RecoveryState::Restarting);
        assert!(find_schedule(&actions).is_some());
    }

    // ---- stop_result(false) 亦可在 grace 超时后升级 ForceKill ----
    #[test]
    fn stop_result_false_after_grace_escalates_to_force_kill() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let _ = fsm.on_health(t_at(t0, 110), HealthLevel::Critical); // SignalStop @110
        let actions = fsm.on_stop_result(t_at(t0, 130), false); // 130 >= 110+15
        assert_eq!(fsm.state(), RecoveryState::ForceKill);
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, RecoverAction::ForceKill))
        );
    }

    // ---- §8.2 边:ForceKill --失败--> Fatal ----
    #[test]
    fn force_kill_failure_goes_fatal() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let _ = fsm.on_health(t_at(t0, 110), HealthLevel::Critical);
        let _ = fsm.on_tick(t_at(t0, 125)); // → ForceKill
        let actions = fsm.on_stop_result(t_at(t0, 130), false);
        assert_eq!(fsm.state(), RecoveryState::Fatal);
        assert!(fsm.is_fatal());
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, RecoverAction::MarkFatal))
        );
        assert_audit(&actions);
    }

    // ---- §8.2 边:Restarting --backoff 后 spawn 成功--> Starting ----
    #[test]
    fn spawn_due_tick_fires_zero_delay_and_spawn_ok_goes_starting() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_child_exit(t_at(t0, 100), ident(100)); // Recovering, spawn_due=101
        assert!(fsm.on_tick(t_at(t0, 100)).is_empty(), "backoff 未到点");
        let actions = fsm.on_tick(t_at(t0, 101)); // 到点
        assert_eq!(fsm.state(), RecoveryState::Restarting);
        assert_eq!(
            find_schedule(&actions),
            Some(Duration::ZERO),
            "到点回报 delay=0"
        );
        let actions = fsm.on_spawn_result(t_at(t0, 101), Some(ident(101)));
        assert_eq!(fsm.state(), RecoveryState::Starting);
        assert_audit(&actions);
    }

    // ---- §8.2 边:Restarting --spawn 失败--> CrashLoop(计入 budget) ----
    #[test]
    fn spawn_failure_goes_crashloop_and_counts_budget() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_child_exit(t_at(t0, 100), ident(100)); // 死亡计入 budget(1),消耗 backoff 第 1 档
        let _ = fsm.on_tick(t_at(t0, 101));
        let actions = fsm.on_spawn_result(t_at(t0, 101), None);
        assert_eq!(fsm.state(), RecoveryState::CrashLoop);
        assert_eq!(
            fsm.restarts_in_window(t_at(t0, 101)),
            2,
            "死亡与 spawn 失败均计数 (§8.2 全量计数)"
        );
        assert_eq!(
            find_schedule(&actions),
            Some(Duration::from_secs(2)),
            "backoff 第 2 档"
        );
    }

    // ---- SCM 目标生命周期 (on_scm_spawn_ok;ROADMAP 原生 Windows Service) ----

    /// on_scm_spawn_ok → Starting，不记 identity(last_spawn 保持 None)。
    #[test]
    fn scm_spawn_ok_goes_starting_without_identity() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        let actions = fsm.on_scm_spawn_ok(t_at(t0, 1));
        assert_eq!(fsm.state(), RecoveryState::Starting);
        assert!(fsm.last_spawn.is_none(), "SCM 目标不落身份");
        assert_eq!(fsm.restarts_total(), 1, "与 on_spawn_result(Some) 对齐计数");
        assert_audit(&actions);
        let _ = fsm.on_liveness_ok(t_at(t0, 1 + MIN_UPTIME.as_secs()));
        assert_eq!(fsm.state(), RecoveryState::Running, "QueryStatus 证据推进");
    }

    /// SCM 死亡路径:QueryStatus 消失经 on_liveness_gone(无身份匹配) 驱动
    /// 确认死亡 → Recovering 排程;重启后再走 on_scm_spawn_ok 整链闭合。
    #[test]
    fn scm_lifecycle_death_via_liveness_gone_and_respawn() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        let _ = fsm.on_scm_spawn_ok(t_at(t0, 1));
        let _ = fsm.on_liveness_ok(t_at(t0, 1 + MIN_UPTIME.as_secs()));
        assert_eq!(fsm.state(), RecoveryState::Running);
        // 服务停止 (外部或故障):连续 confirmations 次查询非 Running
        let a1 = fsm.on_liveness_gone(t_at(t0, 100));
        assert!(a1.is_empty(), "首次消失未达阈值");
        let a2 = fsm.on_liveness_gone(t_at(t0, 105));
        assert_eq!(fsm.state(), RecoveryState::Recovering);
        assert!(find_schedule(&a2).is_some());
        // backoff 到点 → SCM start 幂等成功 → on_scm_spawn_ok → Starting
        let _ = fsm.on_tick(t_at(t0, 106));
        let actions = fsm.on_scm_spawn_ok(t_at(t0, 106));
        assert_eq!(fsm.state(), RecoveryState::Starting);
        assert_audit(&actions);
        assert!(fsm.last_spawn.is_none());
    }

    /// SCM 启动失败：与进程目标同路径 (on_spawn_result(None) → CrashLoop 计入
    /// budget;SCM 诚实降级/服务启动失败都归此边)。
    #[test]
    fn scm_spawn_failure_counts_budget_like_process_path() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        let _ = fsm.on_scm_spawn_ok(t_at(t0, 1));
        let _ = fsm.on_liveness_ok(t_at(t0, 11));
        let _ = fsm.on_liveness_gone(t_at(t0, 20));
        let _ = fsm.on_liveness_gone(t_at(t0, 25)); // Recovering
        let _ = fsm.on_tick(t_at(t0, 26));
        let actions = fsm.on_spawn_result(t_at(t0, 26), None);
        assert_eq!(fsm.state(), RecoveryState::CrashLoop);
        assert!(find_schedule(&actions).is_some());
        // 死亡确认 (25s) 与 spawn 失败 (26s) 各计一次 (§8.2 全量计数)
        assert_eq!(fsm.restarts_in_window(t_at(t0, 26)), 2);
    }

    /// on_scm_spawn_ok 受理门控:Fatal / Starting 内重复回报视为陈旧事件。
    #[test]
    fn scm_spawn_ok_ignored_when_fatal_or_starting() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 1, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_child_exit(t_at(t0, 100), ident(100)); // budget=1 → Fatal
        assert!(fsm.is_fatal());
        let before = fsm.restarts_total();
        assert!(fsm.on_scm_spawn_ok(t_at(t0, 102)).is_empty(), "Fatal 忽略");
        assert_eq!(fsm.restarts_total(), before);

        let mut fsm2 = fsm_at(t0, 2, 5, HungAction::Restart);
        let _ = fsm2.on_scm_spawn_ok(t_at(t0, 1));
        let total = fsm2.restarts_total();
        assert!(
            fsm2.on_scm_spawn_ok(t_at(t0, 2)).is_empty(),
            "Starting 内重复回报视为陈旧事件"
        );
        assert_eq!(fsm2.restarts_total(), total);
        assert_eq!(fsm2.state(), RecoveryState::Starting);
    }

    // ---- §8.2 边:CrashLoop --backoff 重试--> Restarting(指数退避) ----
    #[test]
    fn crashloop_retries_with_exponential_backoff() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let death = fsm.on_child_exit(t_at(t0, 100), ident(100));
        assert_eq!(
            find_schedule(&death),
            Some(Duration::from_secs(1)),
            "死亡首档 1s"
        );
        // 循环:tick 到点发起 spawn(delay=0)→ 失败 → CrashLoop(2s,4s,…)
        let mut delays = Vec::new();
        for i in 0..3u64 {
            let fire = fsm.on_tick(t_at(t0, 101 + 10 * i));
            assert_eq!(
                find_schedule(&fire),
                Some(Duration::ZERO),
                "到点回报 delay=0"
            );
            let res = fsm.on_spawn_result(t_at(t0, 101 + 10 * i), None);
            delays.push(find_schedule(&res).expect("spawn 失败必须排程重试"));
        }
        assert_eq!(delays, [2, 4, 8].map(Duration::from_secs));
    }

    // ---- §8.2 边:CrashLoop --预算耗尽--> Fatal + Critical(MarkFatal) ----
    #[test]
    fn budget_exhaustion_goes_fatal_with_mark_fatal() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 3, HungAction::Restart); // max_restarts=3
        running(&mut fsm, t0, 100);
        let _ = fsm.on_child_exit(t_at(t0, 100), ident(100)); // budget=1(死亡计数)
        // spawn 失败 ×2:第 3 次记录 (即第 2 次 spawn 失败)budget=3 → Fatal
        let mut last = Vec::new();
        for i in 0..2u64 {
            let _ = fsm.on_tick(t_at(t0, 101 + 10 * i));
            last = fsm.on_spawn_result(t_at(t0, 101 + 10 * i), None);
        }
        assert_eq!(fsm.state(), RecoveryState::Fatal);
        assert!(fsm.is_fatal());
        assert!(last.iter().any(|a| matches!(a, RecoverAction::MarkFatal)));
        assert_audit(&last);
    }

    // ---- Fatal 态：一切触发忽略 ----
    #[test]
    fn fatal_ignores_all_triggers() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 1, HungAction::Restart); // max_restarts=1，一次即 Fatal
        running(&mut fsm, t0, 100);
        let _ = fsm.on_child_exit(t_at(t0, 100), ident(100));
        let _ = fsm.on_tick(t_at(t0, 101));
        let _ = fsm.on_spawn_result(t_at(t0, 101), None); // Fatal
        assert!(fsm.is_fatal());
        assert!(
            fsm.on_health(t_at(t0, 110), HealthLevel::Critical)
                .is_empty()
        );
        assert!(fsm.on_child_exit(t_at(t0, 111), ident(100)).is_empty());
        assert!(fsm.on_liveness_gone(t_at(t0, 112)).is_empty());
        assert!(fsm.on_tick(t_at(t0, 113)).is_empty());
        assert!(fsm.on_spawn_result(t_at(t0, 114), None).is_empty());
        assert!(fsm.on_stop_result(t_at(t0, 115), false).is_empty());
        assert!(fsm.on_activate_result(t_at(t0, 116), false).is_empty());
        assert_eq!(fsm.state(), RecoveryState::Fatal);
    }

    // ---- §8.2 边:Fatal --reset--> Running，清预算，可再拉活 ----
    #[test]
    fn reset_releases_fatal_and_allows_recovery() {
        let t0 = Instant::now();
        // max_restarts=2:预算耗尽 → Fatal → reset 清零;reset 后单次慢死亡
        // (budget=1 < 2) 应正常进 Recovering 而非再次 Fatal。
        let mut fsm = fsm_at(t0, 2, 2, HungAction::Restart);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_child_exit(t_at(t0, 100), ident(100));
        let _ = fsm.on_tick(t_at(t0, 101));
        let _ = fsm.on_spawn_result(t_at(t0, 101), None); // Fatal
        let actions = fsm.reset(t_at(t0, 120));
        assert_eq!(fsm.state(), RecoveryState::Running);
        assert!(!fsm.is_fatal());
        assert_eq!(fsm.restarts_in_window(t_at(t0, 120)), 0, "预算已清");
        assert_audit(&actions);
        // reset 后重新 spawn 并可再次进入恢复路径
        let _ = fsm.on_spawn_result(t_at(t0, 125), Some(ident(101))); // → Starting
        let actions = fsm.on_child_exit(t_at(t0, 136), ident(101)); // uptime 11s ≥ min_uptime
        assert_eq!(fsm.state(), RecoveryState::Recovering);
        assert_eq!(
            find_schedule(&actions),
            Some(Duration::from_secs(1)),
            "backoff 已随 reset 重置"
        );
    }

    // ---- §8.2 边:Recovering --activate 钩子成功且 verify_for 内健康恢复--> Running ----
    #[test]
    fn activate_success_and_verify_recovery_goes_running() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Activate);
        fsm.set_verify_for(Duration::from_secs(20));
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let actions = fsm.on_health(t_at(t0, 110), HealthLevel::Critical);
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, RecoverAction::RunActivate))
        );
        assert_eq!(fsm.state(), RecoveryState::Recovering);
        let actions = fsm.on_activate_result(t_at(t0, 112), true);
        assert!(find_schedule(&actions).is_none(), "activate 成功不排 spawn");
        assert_eq!(
            fsm.state(),
            RecoveryState::Recovering,
            "留 Recovering 等健康恢复"
        );
        let actions = fsm.on_liveness_ok(t_at(t0, 115));
        assert_eq!(fsm.state(), RecoveryState::Running);
        assert_audit(&actions);
    }

    // ---- §8.2 边:Recovering --activate 验证失败--> stop → Restarting(计入 budget) ----
    #[test]
    fn activate_verify_timeout_without_recovery_stops_and_restarts() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Activate);
        fsm.set_verify_for(Duration::from_secs(20));
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let _ = fsm.on_health(t_at(t0, 110), HealthLevel::Critical); // RunActivate
        let _ = fsm.on_activate_result(t_at(t0, 112), true); // verify 至 132
        assert!(fsm.on_tick(t_at(t0, 130)).is_empty(), "观察期未到");
        let actions = fsm.on_tick(t_at(t0, 132)); // 到点，观察期内仅有 Critical
        assert_eq!(fsm.state(), RecoveryState::Recovering, "stop 待确认");
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, RecoverAction::SignalStop))
        );
        assert_eq!(
            fsm.restarts_in_window(t_at(t0, 132)),
            1,
            "验证失败计入 budget"
        );
        let actions = fsm.on_stop_result(t_at(t0, 135), true);
        assert_eq!(fsm.state(), RecoveryState::Restarting);
        assert!(find_schedule(&actions).is_some());
    }

    // ---- activate 钩子失败 → 计入 budget + SignalStop ----
    #[test]
    fn activate_hook_failure_counts_budget_and_stops() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Activate);
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let _ = fsm.on_health(t_at(t0, 110), HealthLevel::Critical);
        let actions = fsm.on_activate_result(t_at(t0, 112), false);
        assert_eq!(fsm.restarts_in_window(t_at(t0, 112)), 1);
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, RecoverAction::SignalStop))
        );
        let actions = fsm.on_stop_result(t_at(t0, 115), true);
        assert_eq!(fsm.state(), RecoveryState::Restarting);
        assert!(find_schedule(&actions).is_some());
    }

    // ---- observe 期内进程死亡：恢复失败 → 计入 budget 直接排重启 ----
    #[test]
    fn child_exit_during_verify_restarts_directly() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Activate);
        fsm.set_verify_for(Duration::from_secs(20));
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let _ = fsm.on_health(t_at(t0, 110), HealthLevel::Critical);
        let _ = fsm.on_activate_result(t_at(t0, 112), true);
        let actions = fsm.on_child_exit(t_at(t0, 115), ident(100));
        assert_eq!(fsm.state(), RecoveryState::Restarting);
        assert!(find_schedule(&actions).is_some());
        assert_eq!(fsm.restarts_in_window(t_at(t0, 115)), 1);
    }

    // ---- verify 到点时最近健康反馈非 Ok(仅 Warning)→ 验证失败 ----
    #[test]
    fn verify_deadline_with_warning_feedback_fails() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Activate);
        fsm.set_verify_for(Duration::from_secs(20));
        running(&mut fsm, t0, 100);
        let _ = fsm.on_health(t_at(t0, 100), HealthLevel::Critical);
        let _ = fsm.on_health(t_at(t0, 110), HealthLevel::Critical);
        let _ = fsm.on_activate_result(t_at(t0, 112), true);
        let _ = fsm.on_health(t_at(t0, 120), HealthLevel::Warning); // 观察期内仅 Warning，无恢复
        let actions = fsm.on_tick(t_at(t0, 132)); // 到点：最近反馈非 Ok → 停止并重启
        assert_eq!(fsm.state(), RecoveryState::Recovering, "stop 待确认");
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, RecoverAction::SignalStop))
        );
        assert_eq!(fsm.restarts_in_window(t_at(t0, 132)), 1);
    }

    // ---- restore:Fatal 闩锁与 budget 恢复 ----
    #[test]
    fn restore_fatal_and_budget() {
        let t0 = Instant::now();
        let mut fsm = fsm_at(t0, 2, 5, HungAction::Restart);
        let wall = SystemTime::now();
        let stamps = vec![wall - Duration::from_secs(5), wall - Duration::from_secs(1)];
        fsm.restore(true, &stamps, wall, t_at(t0, 50));
        assert!(fsm.is_fatal());
        assert_eq!(fsm.state(), RecoveryState::Fatal);
        assert_eq!(fsm.restarts_in_window(t_at(t0, 50)), 2);
        fsm.reset(t_at(t0, 60));
        assert!(!fsm.is_fatal());
        assert_eq!(fsm.state(), RecoveryState::Running);
        assert_eq!(fsm.restarts_in_window(t_at(t0, 60)), 0);
    }
}
