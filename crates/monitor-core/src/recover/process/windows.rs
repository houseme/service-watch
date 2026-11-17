//! Windows 进程原语:Job Object 隔离与终止 (DESIGN.md §8.6)+ SCM 目标契约
//! (ROADMAP「原生 Windows Service 目标恢复」)。
//!
//! 本文件在全部平台参与编译;Windows 专属逻辑经 `#[cfg(windows)]` 门控，
//! Unix 平台为空实现桩。
//! 依赖 `windows-sys`:Job Object(CreateJobObjectW / SetInformationJobObject /
//! AssignProcessToJobObject / TerminateJobObject)、CREATE_NEW_PROCESS_GROUP
//! (console = true 时)、GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT)。
//! SCM 目标额外依赖 `windows-service` crate(`scm-target` feature 门控，
//! 仅 cfg(windows) 拉起;经高层 API 操作，本模块不新增 unsafe)。
//!
//! 契约：
//! - Job **不带** KILL_ON_JOB_CLOSE(观察者外置原则:agent 死亡不连带杀服务);
//! - stop 基线 = TerminateJobObject + 审计日志;console = true 可先尝试
//!   GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pgid),失败/超时照常升级;
//! - **与 §8.6 的偏差 (如实记录)**:设计要求 CREATE_SUSPENDED → assign →
//!   ResumeThread 以消除 spawn→assign 间子孙逃逸竞态。本实现改为 spawn 返回后
//!   **立即** AssignProcessToJobObject，原因:tokio `Child` 不暴露主线程句柄，
//!   而 ResumeThread 需要枚举线程 (ToolHelp 特性) 不在本 crate 固定依赖集内;
//!   挂起进程若无法恢复将永久卡死，两害取轻。残余竞态窗口为 spawn 返回到
//!   assign 之间的微秒级;且 Job 无 KILL_ON_JOB_CLOSE，逃逸子孙的最坏后果
//!   是"脱离树清理"而非误杀，与 §8.6 基线 (TerminateJobObject + 审计) 一致。
//!
//! SCM 目标 (`windows_service` 配置字段) 契约：
//! - start = `Start("")`,已 Running / 启动已在途视为成功 (幂等;审计语义由
//!   eval 层处理);stop = `Stop`,已停止 / 停止中视为成功 (幂等);
//! - liveness = `QueryStatus == Running`(每轮查询，替代快照证据);
//! - force = 再次 `Stop`(SCM 没有 Terminate 等价物，如实降级并审计注明);
//! - 等待状态迁移 = 500ms 间隔轮询 QueryStatus，上限 stop_grace
//!   ([`wait_scm_stopped`],停机路径使用;运行中停止的确认由 FSM
//!   stop_deadline + poll_exit 驱动，signal_stop 不阻塞 eval 单写者);
//! - 非 Windows 平台 / 未启用 `scm-target`:`UnavailableScmOps` 诚实降级为
//!   错误，spawn 按失败处理，liveness/告警路径照常。

use crate::config::ServiceCfg;
use crate::error::Result;
use crate::model::ids::ProcessIdentity;

#[cfg(windows)]
use std::process::Stdio;
#[cfg(windows)]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(windows)]
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
#[cfg(windows)]
use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent};
#[cfg(windows)]
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JobObjectExtendedLimitInformation, SetInformationJobObject, TerminateJobObject,
};
#[cfg(windows)]
use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, TerminateProcess};

/// Windows 侧 spawn 返回的句柄集 (job 句柄由本结构持有并负责关闭)。
#[cfg(windows)]
#[derive(Debug)]
pub struct WindowsJob {
    handle: HANDLE,
}

// SAFETY:`HANDLE` 是裸指针因此 `!Send`,但 Job Object 句柄是进程级内核对象，
// `CloseHandle`/`TerminateJobObject` 可在任意线程调用;本结构是句柄的唯一所有者
// (单一所有权转移语义),跨线程移动不产生并发访问。`ProcessManager` 随 eval
// 任务经 `tokio::spawn` 移动，必须满足 Send。
#[cfg(windows)]
unsafe impl Send for WindowsJob {}

#[cfg(windows)]
impl WindowsJob {
    /// 创建未命名 Job Object 并写入"零限额"扩展信息：
    /// `LimitFlags = 0` 即**不带** JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE(§8.6:
    /// agent 死亡不连带杀服务;"agent 死则服务死"不作为选项提供)。
    pub fn create() -> Result<Self> {
        // SAFETY:两个参数均为空指针 (默认安全属性 + 未命名 Job),无借出引用;
        // 失败时返回空句柄，用 last_os_error 还原 Win32 错误码。
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(crate::error::CoreError::Recover(format!(
                "CreateJobObjectW 失败：{}",
                std::io::Error::last_os_error()
            )));
        }
        // 全零扩展限额 = 无任何限制 (LimitFlags = 0，尤其无 KILL_ON_JOB_CLOSE)。
        // POD C 结构体按 SDK 语义全零合法，zeroed 是最小且稳定的构造方式。
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        limits.BasicLimitInformation.LimitFlags = 0;
        // SAFETY:limits 为 JobObjectExtendedLimitInformation 类别的合法缓冲，
        // 长度取该类型实际大小，仅本次调用读取，栈上存活至调用结束。
        let ok = unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                &limits as *const _ as *const core::ffi::c_void,
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if ok == 0 {
            let err = std::io::Error::last_os_error();
            // SAFETY:关闭刚创建、尚未对外发布的句柄，防泄漏
            unsafe { CloseHandle(handle) };
            return Err(crate::error::CoreError::Recover(format!(
                "SetInformationJobObject(JobObjectExtendedLimitInformation) 失败：{err}"
            )));
        }
        Ok(Self { handle })
    }

    /// 原始 Job 句柄 (AssignProcessToJobObject 用)。
    #[allow(dead_code)]
    pub fn handle(&self) -> HANDLE {
        self.handle
    }

    /// 强杀整棵进程树 (TerminateJobObject;Job 内全部进程立即终止)。
    pub fn terminate(&self) -> std::io::Result<()> {
        // SAFETY:句柄由 create() 持有且有效;退出码取任意非零值
        if unsafe { TerminateJobObject(self.handle, 1) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(windows)]
impl Drop for WindowsJob {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            // SAFETY:析构唯一入口，之后句柄不再被使用
            unsafe { CloseHandle(self.handle) };
        }
    }
}

/// spawn 并纳入 Job Object 管理。
///
/// 流程：创建 Job(零限额)→ spawn(console = true 时加 CREATE_NEW_PROCESS_GROUP,
/// 使目标自成进程组、可接收 CTRL_BREAK)→ **立即** AssignProcessToJobObject →
/// 记录 `(pid, start_time)` 身份 (start_time 取本地墙钟，误差由双字段吸收，
/// 见 unix::spawn_group_leader 注释)。assign 失败则 start_kill 拒收该进程，
/// 防止脱离管理的目标存活。
#[cfg(windows)]
pub fn spawn_job_managed(
    cfg: &ServiceCfg,
) -> Result<(tokio::process::Child, WindowsJob, ProcessIdentity)> {
    let job = WindowsJob::create()?;
    let mut cmd = tokio::process::Command::new(&cfg.command);
    cmd.args(&cfg.args);
    if let Some(workdir) = &cfg.workdir {
        cmd.current_dir(workdir);
    }
    for (key, value) in &cfg.env {
        cmd.env(key, value);
    }
    cmd.kill_on_drop(false) // 观察者原则:drop 不杀服务
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null()); // 管道无人读会撑满导致目标阻塞
    if cfg.console {
        // 新进程组：使 GenerateConsoleCtrlEvent(CTRL_BREAK, pid) 可选送达 (§8.6)
        cmd.creation_flags(CREATE_NEW_PROCESS_GROUP);
    }
    let mut child = cmd.spawn()?;
    let Some(raw) = child.raw_handle() else {
        // 拿不到进程句柄即无法入 Job:立即终止，杜绝脱管进程
        let _ = child.start_kill();
        return Err(crate::error::CoreError::Recover(format!(
            "{:?}: Child::raw_handle 不可用，进程已终止",
            cfg.name
        )));
    };
    // SAFETY:raw 为我方刚 spawn 的子进程句柄 (所有权仍归 Child，本调用不关闭它),
    // job.handle() 由本函数持有;调用仅建立归属关系。
    if unsafe { AssignProcessToJobObject(job.handle(), raw) } == 0 {
        let err = std::io::Error::last_os_error();
        let _ = child.start_kill(); // 未入 Job 的进程立即终止，防逃逸
        return Err(crate::error::CoreError::Recover(format!(
            "{:?}: AssignProcessToJobObject 失败 ({err}),进程已终止",
            cfg.name
        )));
    }
    let pid = child.id().ok_or_else(|| {
        let _ = child.start_kill();
        crate::error::CoreError::Recover(format!("{:?}: spawn 后未取得 pid", cfg.name))
    })?;
    let start_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    Ok((child, job, ProcessIdentity::new(pid, start_time)))
}

/// 对 `console = true` 的 target 尝试 CTRL_BREAK(§8.6:不保证生效，超时照常升级)。
#[cfg(windows)]
pub fn try_ctrl_break(pgid: u32) -> std::io::Result<()> {
    // SAFETY:仅传两个整型;pgid 为我方亲生子进程 pid(经 CREATE_NEW_PROCESS_GROUP
    // 成为进程组 id),事件仅投递到该组。
    if unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pgid) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// 按进程句柄强杀 (Job 兜底路径;仅覆盖组长，不含子孙)。
#[cfg(windows)]
pub fn terminate_process_handle(handle: HANDLE) -> std::io::Result<()> {
    // SAFETY:句柄来自 tokio Child::raw_handle(我方亲生子进程),仅请求终止，
    // 不关闭句柄 (所有权归 Child)。
    if unsafe { TerminateProcess(handle, 1) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

// ---- 非 Windows 平台桩 (不参与编译逻辑，仅为模块完整) ----
#[cfg(not(windows))]
#[derive(Debug)]
pub struct WindowsJob {
    _private: (),
}

#[cfg(not(windows))]
impl WindowsJob {
    pub fn terminate(&self) -> std::io::Result<()> {
        Err(std::io::Error::other("windows-only"))
    }
}

#[cfg(not(windows))]
pub fn spawn_job_managed(
    _cfg: &ServiceCfg,
) -> Result<(tokio::process::Child, WindowsJob, ProcessIdentity)> {
    Err(crate::error::CoreError::Recover("windows-only".into()))
}

// ---------------------------------------------------------------------------
// SCM 目标契约 (ROADMAP「原生 Windows Service 目标恢复」)
// ---------------------------------------------------------------------------

/// SCM 服务的归一状态 (`QueryStatus` 的三值投影：非 Running/Stopped 的
/// START_PENDING / STOP_PENDING / PAUSED 等一律归入过渡态)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScmServiceState {
    /// SERVICE_RUNNING:唯一被 [`ProcessManager::is_running`] 采信的存活证据。
    Running,
    /// SERVICE_STOPPED:poll_exit 视为退出。
    Stopped,
    /// 启动/停止/恢复在途或暂停态：视为"未确认停止"(不误报退出)、
    /// 也未确认运行 (liveness 不喂 ok)。
    Transitioning,
}

/// [`ScmOps::start`] 的结果 (幂等语义：已在运行或启动已在途 → AlreadyRunning)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScmStartOutcome {
    /// 已发出 `Start("")`。
    Started,
    /// 服务已 Running / 启动已在途：幂等成功，未重复发 Start。
    AlreadyRunning,
}

/// SCM 操作抽象。生产实现:[`RealScmOps`](cfg(windows)+`scm-target`) 与
/// [`UnavailableScmOps`](其余平台/feature 组合的诚实降级);测试注入假实现。
/// 全部为同步方法：本地 SCM RPC 是毫秒级调用，且 `ProcessManager` 的
/// stop/force_kill 本就是同步契约。
pub trait ScmOps: Send + Sync + std::fmt::Debug {
    fn start(&self, service: &str) -> std::io::Result<ScmStartOutcome>;
    fn stop(&self, service: &str) -> std::io::Result<()>;
    fn query(&self, service: &str) -> std::io::Result<ScmServiceState>;
}

/// SCM 目标后端在当前构建组合下是否可用 (cfg(windows) + `scm-target` feature)。
pub const fn scm_available() -> bool {
    cfg!(all(windows, feature = "scm-target"))
}

/// 生产后端选择：可用时走 `windows-service` crate 高层 API，否则诚实降级。
pub fn platform_scm_ops() -> std::sync::Arc<dyn ScmOps> {
    #[cfg(all(windows, feature = "scm-target"))]
    if scm_available() {
        return std::sync::Arc::new(RealScmOps);
    }
    std::sync::Arc::new(UnavailableScmOps)
}

/// windows_service::Error → io::Error:Winapi 变体展开内核 io::Error
/// (保留原始 OS 错误码，如 1060 = 服务不存在),其余按 Display 文本包装。
#[cfg(all(windows, feature = "scm-target"))]
fn scm_io_error(e: windows_service::Error) -> std::io::Error {
    match e {
        windows_service::Error::Winapi(io) => io,
        other => std::io::Error::other(other.to_string()),
    }
}

/// 生产实现：经 `windows-service` crate 高层 API 操作本地 SCM。
/// 每次调用临时打开 Manager/Service 句柄 (ScHandle Drop 关闭，无长持句柄);
/// 全部走安全高层 API，无 unsafe。
#[cfg(all(windows, feature = "scm-target"))]
#[derive(Debug)]
pub struct RealScmOps;

#[cfg(all(windows, feature = "scm-target"))]
impl ScmOps for RealScmOps {
    fn start(&self, service: &str) -> std::io::Result<ScmStartOutcome> {
        use windows_service::service::{ServiceAccess, ServiceState};
        use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};
        let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
            .map_err(scm_io_error)?;
        let svc = manager
            .open_service(service, ServiceAccess::START | ServiceAccess::QUERY_STATUS)
            .map_err(scm_io_error)?;
        // 幂等：已 Running / 启动已在途不重复发 Start(重复 Start 报
        // ERROR_SERVICE_ALREADY_RUNNING，预先查询消除该错误路径)。
        match svc.query_status().map_err(scm_io_error)?.current_state {
            ServiceState::Running | ServiceState::StartPending => {
                Ok(ScmStartOutcome::AlreadyRunning)
            }
            _ => {
                // SCM 启动无参数 (DESIGN §8.6 服务形态：参数属 SCM 配置，不属目标配置)
                svc.start::<&std::ffi::OsStr>(&[]).map_err(scm_io_error)?;
                Ok(ScmStartOutcome::Started)
            }
        }
    }

    fn stop(&self, service: &str) -> std::io::Result<()> {
        use windows_service::service::{ServiceAccess, ServiceState};
        use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};
        let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
            .map_err(scm_io_error)?;
        let svc = manager
            .open_service(service, ServiceAccess::STOP | ServiceAccess::QUERY_STATUS)
            .map_err(scm_io_error)?;
        // 幂等：已停止 / 停止中不重复发 Stop(重复 Stop 会报
        // ERROR_SERVICE_CANNOT_ACCEPT_CTRL, FSM grace 重试路径依赖此幂等)。
        match svc.query_status().map_err(scm_io_error)?.current_state {
            ServiceState::Stopped | ServiceState::StopPending => Ok(()),
            _ => svc.stop().map(|_| ()).map_err(scm_io_error),
        }
    }

    fn query(&self, service: &str) -> std::io::Result<ScmServiceState> {
        use windows_service::service::{ServiceAccess, ServiceState};
        use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};
        let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
            .map_err(scm_io_error)?;
        let svc = manager
            .open_service(service, ServiceAccess::QUERY_STATUS)
            .map_err(scm_io_error)?;
        match svc.query_status().map_err(scm_io_error)?.current_state {
            ServiceState::Running => Ok(ScmServiceState::Running),
            ServiceState::Stopped => Ok(ScmServiceState::Stopped),
            _ => Ok(ScmServiceState::Transitioning),
        }
    }
}

/// 诚实降级桩：非 Windows 平台，或 Windows 下未启用 `scm-target` feature。
/// 所有操作返回描述性错误 —— spawn 按失败处理 (FSM CrashLoop → 预算 →
/// Fatal 告警),liveness/告警路径照常，绝不假装成功。
#[derive(Debug)]
pub struct UnavailableScmOps;

fn scm_unavailable_error() -> std::io::Error {
    let why = if cfg!(windows) {
        "monitor-core 未启用 scm-target feature"
    } else {
        "当前平台不是 Windows"
    };
    std::io::Error::other(format!(
        "windows_service 目标不可用:{why}(需要 Windows + monitor-core 的 scm-target feature;诚实降级:spawn 按失败处理)"
    ))
}

impl ScmOps for UnavailableScmOps {
    fn start(&self, _service: &str) -> std::io::Result<ScmStartOutcome> {
        Err(scm_unavailable_error())
    }

    fn stop(&self, _service: &str) -> std::io::Result<()> {
        Err(scm_unavailable_error())
    }

    fn query(&self, _service: &str) -> std::io::Result<ScmServiceState> {
        Err(scm_unavailable_error())
    }
}

/// Stop 后的有界等待:500ms 间隔轮询 QueryStatus，累计等待不超过 `grace`。
/// 返回是否观测到 Stopped(超时或查询失败返回 false，由调用方决定升级或
/// 如实放弃)。停机路径 ([`ProcessManager::shutdown_all`]) 使用;运行中停止的
/// 确认由 FSM stop_deadline + poll_exit 驱动，signal_stop 不阻塞 eval 单写者。
pub async fn wait_scm_stopped(ops: &dyn ScmOps, service: &str, grace: std::time::Duration) -> bool {
    let poll = std::time::Duration::from_millis(500);
    let mut waited = std::time::Duration::ZERO;
    loop {
        match ops.query(service) {
            Ok(ScmServiceState::Stopped) => return true,
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(scm = %service, error = %e, "SCM 状态查询失败，停止等待提前结束");
                return false;
            }
        }
        if waited >= grace {
            tracing::warn!(scm = %service, grace = ?grace, "stop_grace 内未观测到 SERVICE_STOPPED");
            return false;
        }
        let step = poll.min(grace - waited);
        tokio::time::sleep(step).await;
        waited += step;
    }
}

/// 测试假实现：内存状态机 + 调用计数 (lifecycle 测试注入)。
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct FakeScmOps {
    inner: std::sync::Mutex<FakeScmState>,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct FakeScmState {
    running: bool,
    /// 覆盖 query 返回：报告过渡态 (START_PENDING 场景)。
    transitioning: bool,
    starts: u32,
    stops: u32,
    fail_start: bool,
    fail_query: bool,
}

#[cfg(test)]
impl FakeScmOps {
    pub(crate) fn new(running: bool) -> Self {
        Self {
            inner: std::sync::Mutex::new(FakeScmState {
                running,
                ..Default::default()
            }),
        }
    }

    pub(crate) fn set_running(&self, running: bool) {
        let mut s = self.inner.lock().unwrap();
        s.running = running;
        s.transitioning = false;
    }

    pub(crate) fn set_transitioning(&self) {
        let mut s = self.inner.lock().unwrap();
        s.transitioning = true;
    }

    /// 注入点 (scm_target.rs 有自带 Fake;本方法为对称保留，当前无调用方)
    #[allow(dead_code)]
    pub(crate) fn fail_start(&self, on: bool) {
        self.inner.lock().unwrap().fail_start = on;
    }

    pub(crate) fn fail_query(&self, on: bool) {
        self.inner.lock().unwrap().fail_query = on;
    }

    pub(crate) fn starts(&self) -> u32 {
        self.inner.lock().unwrap().starts
    }

    pub(crate) fn stops(&self) -> u32 {
        self.inner.lock().unwrap().stops
    }
}

#[cfg(test)]
impl ScmOps for FakeScmOps {
    fn start(&self, _service: &str) -> std::io::Result<ScmStartOutcome> {
        let mut s = self.inner.lock().unwrap();
        if s.fail_start {
            return Err(std::io::Error::other("fake start failure"));
        }
        if s.running {
            return Ok(ScmStartOutcome::AlreadyRunning);
        }
        s.running = true;
        s.starts += 1;
        Ok(ScmStartOutcome::Started)
    }

    fn stop(&self, _service: &str) -> std::io::Result<()> {
        let mut s = self.inner.lock().unwrap();
        s.stops += 1;
        s.running = false;
        s.transitioning = false;
        Ok(())
    }

    fn query(&self, _service: &str) -> std::io::Result<ScmServiceState> {
        let s = self.inner.lock().unwrap();
        if s.fail_query {
            return Err(std::io::Error::other("fake query failure"));
        }
        if s.transitioning {
            return Ok(ScmServiceState::Transitioning);
        }
        if s.running {
            Ok(ScmServiceState::Running)
        } else {
            Ok(ScmServiceState::Stopped)
        }
    }
}
