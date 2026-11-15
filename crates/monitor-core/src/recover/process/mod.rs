//! 进程管理(supervise:spawn / stop / poll-exit),eval 任务独占。
//!
//! 平台契约(DESIGN.md §8.4/§8.6):
//! - Unix:`process_group(0)` spawn(子进程成为组长,pgid == pid);stop = `killpg(SIGTERM)`,
//!   force = `killpg(SIGKILL)`;`try_wait` 兜底 reap 防僵尸。
//! - Windows:spawn 后立即 `AssignProcessToJobObject`(Job 不带 KILL_ON_JOB_CLOSE,
//!   见 [`windows`] 模块 doc 的 CREATE_SUSPENDED 偏差说明);stop 先对 `console = true`
//!   的 target 尝试 `GenerateConsoleCtrlEvent(CTRL_BREAK)`,grace 超时由调用方
//!   升级 `TerminateJobObject`;`try_wait` 兜底 reap 防僵尸。
//! - 禁止 daemonize 目标(双 fork 会立刻"退出"导致误判)。
//!
//! SCM 目标契约(`windows_service` 配置字段,ROADMAP「原生 Windows Service 目标恢复」):
//! - `cfg.windows_service` 有值即 SCM 目标,全部启停/存活判定按名字分派到
//!   SCM(`ScmOps`:start/stop/query),对 eval 完全透明 —— is_running/spawn/
//!   signal_stop/force_kill/poll_exit 已按名字调用,SCM 与普通进程走同一入口;
//! - spawn = `Start`(已 Running 视为成功,幂等);is_running = QueryStatus ==
//!   Running(每轮查询即 liveness 证据,替代快照证据);signal_stop = `Stop`
//!   (不阻塞,停止确认由 poll_exit + FSM stop_deadline 驱动);force_kill =
//!   再次 `Stop`(SCM 无更强手段,如实降级);poll_exit = 会话内 QueryStatus
//!   报 Stopped 视为退出(code=None,uptime 用 spawned_at);
//! - SCM 目标没有 (pid, start_time) 身份:entry.identity 恒 None,身份链
//!   (watched 回传 / 快照证据 / adopt_identity)不登记 SCM 目标;spawn 返回
//!   (0,0) 占位值仅为满足既有签名,eval 的 SCM 分派应改走
//!   `RecoveryFsm::on_scm_spawn_ok`(recover/mod.rs,不落身份);
//! - 非 Windows / 未启用 `scm-target`:spawn/stop 诚实降级为错误,liveness/
//!   告警路径照常(见 [`windows::UnavailableScmOps`])。
//!
//! 身份不变量(§8.3):entry 中的 identity 全部由本管理器在 spawn 时登记,
//! 子进程由我方持有且 reap 前我方 pid 不可能被复用,故停止/强杀凭
//! identity(而非裸 PID 查找)操作即是 (pid, start_time) 的充分校验。
//! SCM 目标是上述不变量的显式例外:无 child 句柄、无身份,启停凭服务名
//! (SCM 自身是权威校验方),不参与 (pid, start_time) 校验链。

// unix 模块依赖 libc(cfg(unix) 依赖),Windows 交叉编译时不参与编译
#[cfg(unix)]
pub mod unix;
pub mod windows;

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime};

use crate::config::ServiceCfg;
use crate::error::{CoreError, Result};
use crate::model::ids::ProcessIdentity;

/// 一次进程退出的事实。
#[derive(Debug, Clone)]
pub struct ProcessExit {
    pub identity: ProcessIdentity,
    pub code: Option<i32>,
    #[cfg(unix)]
    pub signal: Option<i32>,
    /// 本次运行时长(供 min_uptime / crash loop 判定)。
    pub uptime: Duration,
}

/// 单个 supervise 目标的运行时条目。
#[derive(Debug)]
struct ProcessEntry {
    cfg: ServiceCfg,
    /// 亲生子进程句柄;SCM 目标恒为 None(启停凭服务名经 [`windows::ScmOps`])。
    child: Option<tokio::process::Child>,
    identity: Option<ProcessIdentity>,
    spawned_at: Option<(Instant, SystemTime)>,
    /// stop 已发起的时刻(grace 升级判定用)。
    stop_requested_at: Option<Instant>,
    /// Windows Job Object 句柄(强杀覆盖整棵进程树);Unix 恒为 None 不存在。
    #[cfg(windows)]
    job: Option<windows::WindowsJob>,
}

impl ProcessEntry {
    /// SCM 目标视图:Some(服务名) = 受 SCM 管理的目标(与 child 互斥)。
    /// 直接取自 cfg(单一事实来源):update_config 热更后自动跟随,无失步风险
    /// —— 这是「ScmHandle 变体 / 独立字段」之外改动最小且两平台编译等价的方案。
    fn scm_service(&self) -> Option<&str> {
        self.cfg.windows_service.as_deref()
    }
}

/// supervise 进程管理器。**由 eval 任务独占**(零锁)。
#[derive(Debug)]
pub struct ProcessManager {
    entries: HashMap<String, ProcessEntry>,
    /// 待强杀的进程组(unix):SIGTERM 后组长先退出被 reap,但组内忽略 SIGTERM 的
    /// 幸存组员仍占用资源 —— 组长 reap 后 pgid 不可达,必须在此登记,到期补
    /// `killpg(SIGKILL)`(评审发现 P1:孤儿组员泄漏)。(pgid, 强杀截止时刻)
    ///
    /// 已知取舍(2026-11-11 系统面评审):组长 reap 后 pgid 可能被内核复用给
    /// 无关进程组,宽限窗口内的补杀存在**秒级误杀窗口**(低 pid + 快速回绕环境)。
    /// 权衡:泄漏组员占用端口等资源是确定性故障,误杀是低概率事件;Linux 精确
    /// 方案(pidfd)留待后续,本窗口如实记录。
    pending_kills: Vec<(u32, Instant)>,
    /// SCM 目标操作后端(生产 = [`windows::platform_scm_ops`];
    /// 测试经 [`Self::with_scm_ops`] 注入假实现)。
    scm_ops: std::sync::Arc<dyn windows::ScmOps>,
}

impl Default for ProcessManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessManager {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            pending_kills: Vec::new(),
            scm_ops: windows::platform_scm_ops(),
        }
    }

    /// 测试注入:以指定 SCM 后端构造(假实现驱动生命周期单测)。
    pub fn with_scm_ops(ops: std::sync::Arc<dyn windows::ScmOps>) -> Self {
        Self {
            entries: HashMap::new(),
            pending_kills: Vec::new(),
            scm_ops: ops,
        }
    }

    /// 注册 supervise 目标(不启动)。
    pub fn register(&mut self, cfg: ServiceCfg) {
        // 双实例防护仅针对亲生 child:SCM 目标无句柄,服务当前 Running 与否
        // 不阻止登记(冷启动时服务可能已在运行,spawn 幂等语义处理)。
        if self
            .entries
            .get(&cfg.name)
            .map(|e| e.child.is_some())
            .unwrap_or(false)
        {
            tracing::error!(target = %cfg.name, "registration rejected: existing child must be reaped first");
            return;
        }
        self.entries.insert(
            cfg.name.clone(),
            ProcessEntry {
                cfg,
                child: None,
                identity: None,
                spawned_at: None,
                stop_requested_at: None,
                #[cfg(windows)]
                job: None,
            },
        );
    }

    /// 更新后续启动/停止配置,保留当前 Child、Job 与身份。
    pub fn update_config(&mut self, cfg: ServiceCfg) {
        if let Some(entry) = self.entries.get_mut(&cfg.name) {
            entry.cfg = cfg;
        }
    }

    pub fn cfg(&self, name: &str) -> Option<&ServiceCfg> {
        self.entries.get(name).map(|e| &e.cfg)
    }

    pub fn identity(&self, name: &str) -> Option<ProcessIdentity> {
        self.entries.get(name).and_then(|e| e.identity)
    }

    /// 观测身份采纳(第五轮 P2):spawn 侧本地钟与内核记账漂移超出容差时,
    /// eval 经此把登记身份同步为采集观测值 —— process 检查的预期身份、
    /// watched 回传、杀/重启前的身份校验全部随之更新。同值幂等。
    pub fn adopt_identity(&mut self, name: &str, identity: ProcessIdentity) -> bool {
        let Some(entry) = self.entries.get_mut(name) else {
            return false;
        };
        // 采纳仅修正亲生存活 Child 的 start_time,禁止凭快照创建管理权或换 pid。
        let has_owned_child =
            entry.child.as_ref().and_then(|child| child.id()) == Some(identity.pid.0);
        if !has_owned_child || !entry.identity.is_some_and(|old| old.pid == identity.pid) {
            return false;
        }
        entry.identity = Some(identity);
        true
    }

    pub fn is_running(&self, name: &str) -> bool {
        let Some(entry) = self.entries.get(name) else {
            return false;
        };
        if let Some(svc) = entry.scm_service() {
            // SCM 契约:QueryStatus == Running 即存活证据(每轮查询,替代快照
            // 证据)。查询失败 = 无法证明存活 → false(经 confirmations 走
            // 死亡/重启路径;热路径不逐次打日志,poll_exit 失败时才 warn)。
            return matches!(
                self.scm_ops.query(svc),
                Ok(windows::ScmServiceState::Running)
            );
        }
        entry.child.is_some()
    }

    /// SCM 目标的三值状态查询(第六轮评审 P1 修复的配套出口):
    /// `is_running` 把 Transitioning(START_PENDING / STOP_PENDING / PAUSED)与
    /// 查询失败都抹平为 false —— liveness 消费端无法区分"启动进行中的正常过渡"
    /// 与"确认死亡"。本方法透传 [`windows::ScmServiceState`]:
    /// - `Some(Running)` → 存活证据;
    /// - `Some(Stopped)` → 确认死亡,可喂 on_liveness_gone;
    /// - `Some(Transitioning)` → **在途,既不喂 ok 也不喂 gone**(SCM 自身对
    ///   无响应启动有 ~30s 超时转 Stopped,agent 无需自带 Transitioning 判死);
    /// - `None` → 非 SCM 目标,或查询失败(失败不是死亡证据)。
    pub fn scm_state(&self, name: &str) -> Option<windows::ScmServiceState> {
        let entry = self.entries.get(name)?;
        let svc = entry.scm_service()?;
        self.scm_ops.query(svc).ok()
    }

    /// 最近一次 spawn 的墙钟时刻(journal `Restart{at}` / 孤儿对账用)。
    pub fn spawned_at_wall(&self, name: &str) -> Option<SystemTime> {
        self.entries
            .get(name)
            .and_then(|e| e.spawned_at.map(|(_, wall)| wall))
    }

    /// stop 已发起的时刻(调用方决定 grace 升级节奏时读取)。
    pub fn stop_requested_at(&self, name: &str) -> Option<Instant> {
        self.entries.get(name).and_then(|e| e.stop_requested_at)
    }

    /// spawn 目标进程(平台隔离见模块 doc)。成功即更新 identity。
    ///
    /// `cfg.windows_service` 有值时走 SCM 启动路径(分派透明,eval 无感知):
    /// `Start("")`,已 Running 视为成功(幂等)。SCM 目标返回 (0,0) 占位身份
    /// —— 仅满足既有签名;entry.identity 保持 None,身份链不登记。eval 的 SCM
    /// 分派应改走 `RecoveryFsm::on_scm_spawn_ok`(recover/mod.rs)。
    pub async fn spawn(&mut self, name: &str) -> Result<ProcessIdentity> {
        let entry = self
            .entries
            .get_mut(name)
            .ok_or_else(|| CoreError::Recover(format!("未知 supervise 目标 {name:?}")))?;
        if entry.child.is_some() {
            // 双实例防护:旧 child 未 reap 前绝不允许二次 spawn
            return Err(CoreError::Recover(format!(
                "{name:?}: 进程已在运行,拒绝重复 spawn"
            )));
        }
        // SCM 目标分派(ROADMAP「原生 Windows Service 目标恢复」)
        if let Some(svc) = entry.scm_service().map(str::to_owned) {
            if entry.spawned_at.is_some() {
                // 与 child 防护对称:上一会话的退出尚未经 poll_exit 确认前,
                // 不允许重入(防 FSM 停止待确认期误触发第二次 Start)
                return Err(CoreError::Recover(format!(
                    "{name:?}: SCM 会话尚未确认退出,拒绝重复 spawn"
                )));
            }
            return match self.scm_ops.start(&svc) {
                Ok(outcome) => {
                    tracing::info!(
                        target = %name, scm = %svc, outcome = ?outcome,
                        "SCM 服务启动请求完成(已运行视为成功,幂等)"
                    );
                    entry.identity = None; // SCM 无 (pid,start_time) 身份
                    entry.spawned_at = Some((Instant::now(), SystemTime::now()));
                    entry.stop_requested_at = None;
                    // 占位身份:与 poll_exit 的退出占位配对,仅供 eval 的
                    // on_spawn_result/on_child_exit 身份匹配在未接线 SCM 分派时
                    // 保持机械自洽;身份链(watched/快照/adopt)不使用。
                    Ok(ProcessIdentity::new(0, 0))
                }
                Err(e) => Err(CoreError::Recover(format!(
                    "{name:?}: SCM 服务 {svc:?} 启动失败: {e}"
                ))),
            };
        }
        #[cfg(unix)]
        let (child, identity) = unix::spawn_group_leader(&entry.cfg)?;
        #[cfg(windows)]
        let (child, job, identity) = windows::spawn_job_managed(&entry.cfg)?;
        entry.child = Some(child);
        entry.identity = Some(identity);
        entry.spawned_at = Some((Instant::now(), SystemTime::now()));
        entry.stop_requested_at = None;
        #[cfg(windows)]
        {
            entry.job = Some(job);
        }
        Ok(identity)
    }

    /// graceful 停止第一步:发 SIGTERM/CTRL_BREAK(不等待)。
    /// 返回后调用方靠 `poll_exit` + stop_grace 计时决定是否 `force_kill`。
    /// `grace`:组员强杀升级窗口 —— 组长被 reap 后组内幸存组员由
    /// [`Self::reap_pending_kills`] 在到期时补 `killpg(SIGKILL)`。
    ///
    /// SCM 目标:发 `Stop` 控制码即返回(非阻塞,不拖住 eval 单写者);停止
    /// 确认由 poll_exit(QueryStatus)+ FSM stop_deadline 驱动,grace 超时的
    /// 升级(force = 再次 Stop)与 SIGTERM 路径同构。
    pub fn signal_stop(&mut self, name: &str, grace: Duration) -> Result<()> {
        #[cfg(not(unix))]
        let _ = grace;
        let entry = self
            .entries
            .get_mut(name)
            .ok_or_else(|| CoreError::Recover(format!("未知 supervise 目标 {name:?}")))?;
        // SCM 分派(在 identity 检查之前:SCM 目标恒无身份)
        if let Some(svc) = entry.scm_service().map(str::to_owned) {
            entry.stop_requested_at = Some(Instant::now());
            return match self.scm_ops.stop(&svc) {
                Ok(()) => {
                    tracing::info!(
                        target = %name, scm = %svc, grace = ?grace,
                        "SCM Stop 已发送;stop_grace 内由 QueryStatus 确认停止(幂等:已停止/停止中视为成功)"
                    );
                    Ok(())
                }
                Err(e) => Err(CoreError::Recover(format!(
                    "{name:?}: SCM 服务 {svc:?} 停止失败: {e}"
                ))),
            };
        }
        let Some(identity) = entry.identity else {
            return Err(CoreError::Recover(format!(
                "{name:?}: 无运行中进程,无法 stop"
            )));
        };
        entry.stop_requested_at = Some(Instant::now());
        let pid = identity.pid.0;
        #[cfg(unix)]
        self.pending_kills.push((pid, Instant::now() + grace));
        #[cfg(unix)]
        {
            // 目标已退出(ESRCH)在 signal_group 内折算为 Ok;其余错误如实上报
            unix::signal_group(pid, unix::SIGTERM)?;
        }
        #[cfg(windows)]
        {
            // Windows 无 killpg 等价物:console = true 尝试 CTRL_BREAK(best effort,
            // 生成失败仅记审计——stop_grace 超时后由调用方升级 TerminateJobObject);
            // 非 console 目标直接依赖升级路径(§8.6:Terminate 为基线)。
            if entry.cfg.console {
                if let Err(e) = windows::try_ctrl_break(pid) {
                    tracing::warn!(
                        target = %name, pid, error = %e,
                        "CTRL_BREAK 发送失败;将依赖 stop_grace 升级强杀"
                    );
                }
            } else {
                tracing::info!(
                    target = %name, pid,
                    "console = false:Windows 无 graceful stop 手段,等待 stop_grace 升级"
                );
            }
        }
        Ok(())
    }

    /// 强杀:SIGKILL / TerminateJobObject。
    ///
    /// SCM 目标没有更强手段:force = 再次 `Stop`(如实降级;审计由 eval 的
    /// "force kill executed" journal 注明)。已停止/停止中幂等成功。
    pub fn force_kill(&mut self, name: &str) -> Result<()> {
        let entry = self
            .entries
            .get_mut(name)
            .ok_or_else(|| CoreError::Recover(format!("未知 supervise 目标 {name:?}")))?;
        // SCM 分派(在 identity 检查之前:SCM 目标恒无身份)
        if let Some(svc) = entry.scm_service().map(str::to_owned) {
            return match self.scm_ops.stop(&svc) {
                Ok(()) => {
                    tracing::warn!(
                        target = %name, scm = %svc,
                        "SCM 目标无 Terminate 等价物:force_kill 以 Stop 兜底(如实降级)"
                    );
                    Ok(())
                }
                Err(e) => Err(CoreError::Recover(format!(
                    "{name:?}: SCM 服务 {svc:?} 强制停止失败: {e}"
                ))),
            };
        }
        let Some(identity) = entry.identity else {
            return Err(CoreError::Recover(format!(
                "{name:?}: 无运行中进程,无法强杀"
            )));
        };
        let pid = identity.pid.0;
        #[cfg(unix)]
        {
            unix::signal_group(pid, unix::SIGKILL)?;
        }
        #[cfg(windows)]
        {
            let mut terminated = false;
            if let Some(job) = entry.job.as_ref() {
                match job.terminate() {
                    Ok(()) => terminated = true,
                    Err(e) => {
                        tracing::warn!(target = %name, pid, error = %e, "TerminateJobObject 失败,回退 TerminateProcess")
                    }
                }
            }
            if !terminated {
                // Job 句柄缺失/失败时按进程句柄兜底(仅覆盖组长,如实降级)
                let handle = entry.child.as_ref().and_then(|c| c.raw_handle());
                match handle.map(windows::terminate_process_handle) {
                    Some(Ok(())) => terminated = true,
                    Some(Err(e)) => {
                        tracing::warn!(target = %name, pid, error = %e, "TerminateProcess 兜底失败")
                    }
                    None => {}
                }
            }
            if !terminated {
                return Err(CoreError::Recover(format!(
                    "{name:?}: 强杀失败(TerminateJobObject/TerminateProcess 均未成功)"
                )));
            }
        }
        Ok(())
    }

    /// 到期补杀:对 `signal_stop` 登记的进程组,在 grace 窗口过后仍存活时
    /// 发 `killpg(SIGKILL)`(组长已 reap、组内幸存组员的唯一清理路径)。
    /// ESRCH(组已空)由 `signal_group` 折算 Ok 后移除登记。每轮 tick 调用。
    pub fn reap_pending_kills(&mut self, now: Instant) {
        #[cfg(unix)]
        {
            let due: Vec<(u32, Instant)> = self
                .pending_kills
                .iter()
                .copied()
                .filter(|(_, deadline)| *deadline <= now)
                .collect();
            for (pgid, _) in due {
                match unix::signal_group(pgid, unix::SIGKILL) {
                    Ok(()) => {
                        tracing::info!(pgid, "grace 窗口后补杀进程组(SIGKILL)");
                    }
                    Err(e) => {
                        tracing::warn!(pgid, error = %e, "补杀进程组失败(放弃,移除登记)");
                    }
                }
            }
            self.pending_kills.retain(|(_, deadline)| *deadline > now);
        }
        #[cfg(not(unix))]
        {
            // Windows:Job 句柄在 poll_exit 后 Drop,组树由 TerminateJobObject 全覆盖,
            // 无"组长 reap 后组员残留"问题;登记恒空。
            let _ = now;
            self.pending_kills.clear();
        }
    }

    /// 轮询并 reap 已退出的子进程(防僵尸)。非目标名返回 None。
    ///
    /// SCM 目标:本会话 spawn 过(spawned_at 在手)且 QueryStatus 报 Stopped
    /// 视为退出一次(code=None,uptime 用 spawned_at);START_PENDING /
    /// STOP_PENDING 等过渡态视为未确认停止(不误报);查询失败本轮按未退出
    /// 处理(与 try_wait 失败同构)。退出上报后清 spawned_at(下次 spawn 前提)。
    pub async fn poll_exit(&mut self, name: &str) -> Option<ProcessExit> {
        let entry = self.entries.get_mut(name)?;
        // SCM 分派(child 恒 None,必须在 child 路径之前判)
        if let Some(svc) = entry.scm_service().map(str::to_owned) {
            // 未 spawn 或退出已上报:无可轮询(spawned_at 是 SCM 会话在途标记)
            entry.spawned_at.as_ref()?;
            return match self.scm_ops.query(&svc) {
                Ok(windows::ScmServiceState::Stopped) => {
                    let uptime = entry
                        .spawned_at
                        .take()
                        .map(|(mono, _)| mono.elapsed())
                        .unwrap_or_default();
                    entry.stop_requested_at = None;
                    tracing::info!(target = %name, scm = %svc, uptime = ?uptime, "SCM 服务已停止");
                    Some(ProcessExit {
                        // SCM 无进程身份:占位 (0,0) 与 spawn 的占位返回值配对,
                        // 仅供 eval 的 on_child_exit 身份匹配在未接线 SCM 分派时
                        // 保持机械自洽;身份链不使用。
                        identity: ProcessIdentity::new(0, 0),
                        code: None,
                        #[cfg(unix)]
                        signal: None,
                        uptime,
                    })
                }
                Ok(_) => None,
                Err(e) => {
                    tracing::warn!(
                        target = %name, scm = %svc, error = %e,
                        "SCM 状态查询失败,本轮按未退出处理"
                    );
                    None
                }
            };
        }
        entry.child.as_ref()?;
        // try_wait 是同步方法,直接调用:Some(status)=已退出,None=仍在运行
        let status = match entry.child.as_mut()?.try_wait() {
            Ok(Some(status)) => status,
            Ok(None) => return None,
            Err(e) => {
                tracing::warn!(target = %name, error = %e, "try_wait 失败,本轮按未退出处理");
                return None;
            }
        };
        // 已退出:清空运行态(下次 spawn 前提),构造退出事实
        let identity = match entry.identity.take() {
            Some(id) => id,
            None => {
                // spawn 一定登记过 identity;此处仅为防御(取不到 pid 记 0)
                let pid = entry.child.as_ref().and_then(|c| c.id()).unwrap_or(0);
                ProcessIdentity::new(pid, 0)
            }
        };
        let uptime = entry
            .spawned_at
            .take()
            .map(|(mono, _)| mono.elapsed())
            .unwrap_or_default(); // 观测滞后使 uptime 略偏大:对 min_uptime 判定偏保守,安全
        entry.child = None;
        entry.stop_requested_at = None;
        #[cfg(windows)]
        {
            entry.job = None; // Drop → CloseHandle
        }
        Some(ProcessExit {
            identity,
            code: status.code(),
            #[cfg(unix)]
            signal: std::os::unix::process::ExitStatusExt::signal(&status),
            uptime,
        })
    }

    /// 停机路径:对全部存活子进程执行 force kill 并等待回收。
    ///
    /// SCM 目标:仅停本会话 spawn 过的服务(spawned_at 在手)—— Stop + 有界
    /// 等待(500ms 轮询,上限 stop_grace);未 spawn 过的服务不越权停止
    /// (观察者外置)。失败仅记日志,SCM 无更强手段。
    pub async fn shutdown_all(&mut self) {
        let ops = self.scm_ops.clone();
        for (name, entry) in self.entries.iter_mut() {
            if let Some(svc) = entry.scm_service().map(str::to_owned) {
                if entry.spawned_at.take().is_some() {
                    let grace = entry.cfg.restart.stop_grace;
                    if let Err(e) = ops.stop(&svc) {
                        tracing::warn!(
                            target = %name, scm = %svc, error = %e,
                            "shutdown: SCM Stop 失败"
                        );
                    } else if !windows::wait_scm_stopped(ops.as_ref(), &svc, grace).await {
                        tracing::warn!(
                            target = %name, scm = %svc,
                            "shutdown: stop_grace 内未确认停止(如实放弃,SCM 无更强手段)"
                        );
                    }
                }
                entry.stop_requested_at = None;
                continue;
            }
            if entry.child.is_none() {
                continue;
            }
            // 先按平台强杀(目标可能已自行退出:信号/终止失败仅记日志,wait 兜底)
            #[cfg(unix)]
            if let Some(pid) = entry.child.as_ref().and_then(|c| c.id())
                && let Err(e) = unix::signal_group(pid, unix::SIGKILL)
            {
                tracing::warn!(target = %name, pid, error = %e, "shutdown: killpg(SIGKILL) 失败");
            }
            #[cfg(windows)]
            {
                let mut terminated = false;
                if let Some(job) = entry.job.as_ref() {
                    if let Err(e) = job.terminate() {
                        tracing::warn!(target = %name, error = %e, "shutdown: TerminateJobObject 失败");
                    } else {
                        terminated = true;
                    }
                }
                if !terminated {
                    let handle = entry.child.as_ref().and_then(|c| c.raw_handle());
                    if let Some(handle) = handle {
                        if let Err(e) = windows::terminate_process_handle(handle) {
                            tracing::warn!(target = %name, error = %e, "shutdown: TerminateProcess 兜底失败");
                        }
                    }
                }
            }
            // 阻塞等待 reap,防僵尸
            if let Some(child) = entry.child.as_mut()
                && let Err(e) = child.wait().await
            {
                tracing::warn!(target = %name, error = %e, "shutdown: wait 回收失败");
            }
            entry.child = None;
            entry.identity = None;
            entry.spawned_at = None;
            entry.stop_requested_at = None;
            #[cfg(windows)]
            {
                entry.job = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RestartCfg;
    use std::sync::Arc;

    fn process_cfg(name: &str, command: &str) -> ServiceCfg {
        ServiceCfg {
            name: name.into(),
            command: command.into(),
            args: Vec::new(),
            workdir: None,
            env: Default::default(),
            console: false,
            windows_service: None,
            depends_on: Vec::new(),
            restart: RestartCfg::default(),
            activate: None,
        }
    }

    fn scm_cfg(name: &str, scm: &str) -> ServiceCfg {
        ServiceCfg {
            windows_service: Some(scm.into()),
            ..process_cfg(name, "")
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn adoption_and_registration_preserve_owned_child() {
        let mut pm = ProcessManager::new();
        let cfg = process_cfg("app", "/bin/sleep");
        pm.register(cfg.clone());
        let initial = pm.spawn("app").await.unwrap();
        let observed = ProcessIdentity::new(initial.pid.0, initial.start_time + 10);
        let adopted = pm.adopt_identity("app", observed);
        let rejected = !pm.adopt_identity(
            "app",
            ProcessIdentity::new(initial.pid.0 + 1, observed.start_time),
        );
        pm.register(cfg);
        let preserved = pm.identity("app") == Some(observed) && pm.is_running("app");
        pm.shutdown_all().await;
        assert!(adopted);
        assert!(rejected);
        assert!(
            preserved,
            "registration cannot replace an owned child handle"
        );
    }

    /// adopt_identity:登记身份同步为观测值;同值幂等(第五轮 P2 锁定)。
    #[test]
    fn adopt_identity_updates_and_is_idempotent() {
        let mut pm = ProcessManager::new();
        let cfg = process_cfg("app", "/bin/app");
        pm.register(cfg.clone());
        assert!(pm.identity("app").is_none());

        let observed = ProcessIdentity::new(42, 1000);
        assert!(!pm.adopt_identity("app", observed), "无 Child 不得采纳身份");
        assert!(pm.identity("app").is_none());
        // 下面用持有 Child 的测试覆盖真实身份更新。
        let entry = pm.entries.get_mut("app").unwrap();
        entry.identity = Some(observed);
        assert!(!pm.adopt_identity("app", observed));

        pm.adopt_identity("app", observed); // 同值幂等
        assert_eq!(pm.identity("app"), Some(observed));

        let drifted = ProcessIdentity::new(42, 2000);
        assert!(!pm.adopt_identity("app", drifted));
        assert_eq!(
            pm.identity("app"),
            Some(observed),
            "无 Child 不得跟随观测值"
        );
    }

    // ---- SCM 目标生命周期(Fake 注入;ROADMAP 原生 Windows Service 目标恢复) ----

    /// spawn(幂等 start)→ is_running(QueryStatus)→ 无身份登记。
    #[tokio::test]
    async fn scm_spawn_starts_service_and_keeps_identity_none() {
        let fake = Arc::new(windows::FakeScmOps::new(false));
        let mut pm = ProcessManager::with_scm_ops(fake.clone());
        pm.register(scm_cfg("wsvc", "W3SVC"));

        assert!(!pm.is_running("wsvc"), "Stopped 服务不算运行");
        let id = pm.spawn("wsvc").await.unwrap();
        assert_eq!(fake.starts(), 1);
        assert_eq!(id, ProcessIdentity::new(0, 0), "占位身份(仅满足签名)");
        assert!(pm.identity("wsvc").is_none(), "SCM 目标不得登记身份");
        assert!(pm.is_running("wsvc"), "Start 后 QueryStatus == Running");
        assert!(pm.spawned_at_wall("wsvc").is_some());
        // 幂等:会话内退出未确认前拒绝重入
        assert!(pm.spawn("wsvc").await.is_err());
    }

    /// 冷启动时服务已在运行:spawn 幂等成功(AlreadyRunning),不发 Start。
    #[tokio::test]
    async fn scm_spawn_is_idempotent_when_already_running() {
        let fake = Arc::new(windows::FakeScmOps::new(true));
        let mut pm = ProcessManager::with_scm_ops(fake.clone());
        pm.register(scm_cfg("wsvc", "W3SVC"));
        assert!(pm.is_running("wsvc"));
        let id = pm.spawn("wsvc").await.unwrap();
        assert_eq!(id, ProcessIdentity::new(0, 0));
        assert_eq!(fake.starts(), 0, "已 Running 不重复发 Start");
        assert!(pm.identity("wsvc").is_none());
    }

    /// poll_exit:Running 不误报;Stopped 上报一次退出(code=None);
    /// 过渡态(START_PENDING)不误报;上报后回到可 spawn 状态。
    #[tokio::test]
    async fn scm_poll_exit_emits_once_on_stopped_transition() {
        let fake = Arc::new(windows::FakeScmOps::new(false));
        let mut pm = ProcessManager::with_scm_ops(fake.clone());
        pm.register(scm_cfg("wsvc", "W3SVC"));
        pm.spawn("wsvc").await.unwrap();

        assert!(pm.poll_exit("wsvc").await.is_none(), "Running 不算退出");

        fake.set_transitioning();
        assert!(pm.poll_exit("wsvc").await.is_none(), "过渡态不误报退出");

        fake.set_running(false);
        let exit = pm.poll_exit("wsvc").await.expect("Stopped 必须上报退出");
        assert_eq!(exit.code, None, "SCM 退出无退出码");
        assert_eq!(exit.identity, ProcessIdentity::new(0, 0), "占位身份");
        #[cfg(unix)]
        assert_eq!(exit.signal, None);
        assert!(pm.spawned_at_wall("wsvc").is_none(), "上报后会话结束");
        assert!(!pm.is_running("wsvc"));
        assert!(pm.poll_exit("wsvc").await.is_none(), "只上报一次");
        // 会话已结束:可再次 spawn(恢复路径)
        pm.spawn("wsvc").await.unwrap();
        assert_eq!(fake.starts(), 2);
    }

    /// signal_stop:发 Stop(幂等)、登记 stop_requested_at、poll_exit 确认退出。
    #[tokio::test]
    async fn scm_signal_stop_sends_stop_and_confirms_via_poll_exit() {
        let fake = Arc::new(windows::FakeScmOps::new(false));
        let mut pm = ProcessManager::with_scm_ops(fake.clone());
        pm.register(scm_cfg("wsvc", "W3SVC"));
        pm.spawn("wsvc").await.unwrap();

        pm.signal_stop("wsvc", Duration::from_secs(15)).unwrap();
        assert_eq!(fake.stops(), 1);
        assert!(pm.stop_requested_at("wsvc").is_some());

        // 服务配合:停止完成 → poll_exit 上报退出(FSM on_child_exit 由此驱动)
        let exit = pm.poll_exit("wsvc").await.expect("停止后退出");
        assert_eq!(exit.code, None);
        assert!(pm.stop_requested_at("wsvc").is_none(), "退出后清停止标记");

        // 已停止的会话再 signal_stop:幂等 Ok(不再有退出可上报)
        pm.signal_stop("wsvc", Duration::from_secs(1)).unwrap();
        assert_eq!(fake.stops(), 2);
        assert!(pm.poll_exit("wsvc").await.is_none());
    }

    /// force_kill:SCM 无更强手段 → 再次 Stop(幂等;已停止也成功)。
    #[tokio::test]
    async fn scm_force_kill_degrades_to_stop() {
        let fake = Arc::new(windows::FakeScmOps::new(false));
        let mut pm = ProcessManager::with_scm_ops(fake.clone());
        pm.register(scm_cfg("wsvc", "W3SVC"));
        pm.spawn("wsvc").await.unwrap();

        pm.force_kill("wsvc").unwrap();
        assert_eq!(fake.stops(), 1);
        // 已停止( grace 内 FSM 升级时服务已自行退出):幂等成功
        pm.force_kill("wsvc").unwrap();
        assert_eq!(fake.stops(), 2);
    }

    /// shutdown_all:停本会话 spawn 过的 SCM 服务并等待;未 spawn 不越权停止。
    #[tokio::test]
    async fn scm_shutdown_stops_only_session_started_services() {
        let fake = Arc::new(windows::FakeScmOps::new(false));
        let mut pm = ProcessManager::with_scm_ops(fake.clone());
        pm.register(scm_cfg("started", "SvcA"));
        pm.register(scm_cfg("external", "SvcB"));
        pm.spawn("started").await.unwrap();
        // "external" 从未 spawn(冷启动时已 Running 由 spawn 幂等接管的场景也
        // 会置 spawned_at;这里模拟纯登记未接管的边界)。

        pm.shutdown_all().await;
        assert_eq!(fake.stops(), 1, "只停本会话 spawn 过的服务");
        assert!(pm.spawned_at_wall("started").is_none());
    }

    /// SCM 目标的 adopt_identity 永远拒绝(无 child 句柄、无身份)。
    #[tokio::test]
    async fn scm_targets_never_adopt_process_identity() {
        let fake = Arc::new(windows::FakeScmOps::new(false));
        let mut pm = ProcessManager::with_scm_ops(fake);
        pm.register(scm_cfg("wsvc", "W3SVC"));
        pm.spawn("wsvc").await.unwrap();
        assert!(
            !pm.adopt_identity("wsvc", ProcessIdentity::new(1234, 77)),
            "SCM 目标不参与 (pid,start_time) 身份链"
        );
        assert!(pm.identity("wsvc").is_none());
    }

    /// QueryStatus 查询失败:is_running 按不可证明存活处理(false),
    /// poll_exit 本轮按未退出处理(与 try_wait 失败同构,会话保持)。
    #[tokio::test]
    async fn scm_query_failure_is_not_proof_of_death() {
        let fake = Arc::new(windows::FakeScmOps::new(false));
        let mut pm = ProcessManager::with_scm_ops(fake.clone());
        pm.register(scm_cfg("wsvc", "W3SVC"));
        pm.spawn("wsvc").await.unwrap();

        fake.fail_query(true);
        assert!(!pm.is_running("wsvc"), "查询失败不得声称存活");
        assert!(pm.poll_exit("wsvc").await.is_none(), "查询失败不得判死");
        assert!(
            pm.spawned_at_wall("wsvc").is_some(),
            "会话必须保持(退出判定未被消费)"
        );
        // 恢复采集:回到 Running,退出判定仍然可用
        fake.fail_query(false);
        assert!(pm.is_running("wsvc"));
        fake.set_running(false);
        assert!(pm.poll_exit("wsvc").await.is_some());
    }

    /// 非 Windows / 未启用 scm-target:spawn 诚实降级为错误(停止亦然)。
    /// 平台门控:启用 scm-target 的 Windows 构建里 RealScmOps 可用,本测试跳过。
    #[cfg(not(all(windows, feature = "scm-target")))]
    #[tokio::test]
    async fn scm_spawn_honestly_degrades_without_scm_backend() {
        let mut pm = ProcessManager::new(); // 生产后端 = UnavailableScmOps
        pm.register(scm_cfg("wsvc", "W3SVC"));
        let err = pm.spawn("wsvc").await.unwrap_err().to_string();
        assert!(err.contains("SCM"), "{err}");
        assert!(!pm.is_running("wsvc"));
        assert!(pm.signal_stop("wsvc", Duration::from_secs(1)).is_err());
        assert!(
            !windows::scm_available(),
            "本测试仅在 SCM 后端不可用的构建组合下运行"
        );
    }
}
