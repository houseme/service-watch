//! Agent 重启后的只读孤儿接管。
//!
//! 只有本轮明确请求过候选身份、采集器可用的新快照才参与对账。
//! `Attached` 与 `Pending` 都要求调用方保留 watched 身份并禁止 spawn、stop、
//! force-kill 与 activate。`Gone` 才允许调用方恢复 supervise 调度。

use std::time::{Duration, Instant};

use crate::collect::CollectOutput;
use crate::model::ids::ProcessIdentity;
use crate::model::snapshot::Degradation;

/// 未验证身份的同 pid 不同 start_time 持续到本时长后裁决 Gone(第六轮评审 P2):
/// spawn 侧本地钟与内核记账偏差 >±2s 时身份以 unverified 落盘,若旧进程死后 PID
/// 被复用,`Some(_) => Pending` 分支没有任何出口 —— 对账永久挂起,服务永回不到
/// 监管(且 journal ProcessIdentity 持久,每次重启都重新进入)。真实时钟偏差场景
/// 内几乎必然先经历一次同身份匹配而 verified;超时放行是为 PID 复用兜底,偏保守。
const RECONCILE_MISMATCH_TIMEOUT: Duration = Duration::from_secs(600);

/// 一次孤儿对账的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanObservation {
    /// 当前证据无法确认候选身份，继续只读等待。
    Pending,
    /// 已确认候选身份，作为只读 attach 目标监控。
    Attached(ProcessIdentity),
    /// 原身份已消失，可以恢复 supervise 调度。
    Gone,
}

/// 一个 journal 候选身份的接管状态，独占于 eval。
#[derive(Debug)]
pub struct OrphanTracker {
    identity: ProcessIdentity,
    /// spawn 侧的本地墙钟只是估计，采集器确认过的 start_time 才能裁决 PID 复用。
    verified: bool,
    started_at: Instant,
    observation: OrphanObservation,
    /// 未验证身份出现"同 pid 不同 start_time"时的首个快照时刻(超时裁决 Gone;
    /// 以快照 collected_at 为时钟)。
    mismatch_since: Option<Instant>,
}

impl OrphanTracker {
    /// `verified` 表示身份的 start_time 来自内核采集观测。
    pub fn new(identity: ProcessIdentity, verified: bool, started_at: Instant) -> Self {
        Self {
            identity,
            verified,
            started_at,
            observation: OrphanObservation::Pending,
            mismatch_since: None,
        }
    }

    /// 待接管或已接管的身份，调用方每轮将它加入 watched 集合。
    pub fn identity(&self) -> ProcessIdentity {
        self.identity
    }

    /// 最近一次对账结果。
    pub fn observation(&self) -> OrphanObservation {
        self.observation
    }

    /// 当前是否已确认只读接管。
    pub fn is_attached(&self) -> bool {
        matches!(self.observation, OrphanObservation::Attached(_))
    }

    /// 当前是否已确认原身份消失。
    pub fn is_gone(&self) -> bool {
        self.observation == OrphanObservation::Gone
    }

    /// 用采集快照更新对账结果。
    ///
    /// 缺少进程快照时再以只读 OS 查询确认 PID 已消失。权限错误与采集失败
    /// 保持等待，防止暂时无法读进程指标时创建重复实例。
    pub fn observe(&mut self, output: &CollectOutput) -> OrphanObservation {
        self.observe_with_presence(output, process_presence)
    }

    fn observe_with_presence(
        &mut self,
        output: &CollectOutput,
        presence: impl FnOnce(u32) -> ProcessPresence,
    ) -> OrphanObservation {
        if self.is_gone()
            || output.collected_at <= self.started_at
            || !output.requested.contains(&self.identity)
            || output
                .hardware
                .degradations
                .iter()
                .any(|item| matches!(item, Degradation::CollectorInit { .. }))
        {
            return self.observation;
        }

        // 失配计时以快照时钟(collected_at,单调、随证据注入)为准:门控已保证
        // 快照晚于 started_at,且每轮快照 collected_at 严格递增。
        self.observation = match output
            .processes
            .iter()
            .find(|process| process.identity.pid == self.identity.pid)
        {
            Some(process) if process.identity == self.identity => {
                self.verified = true;
                self.mismatch_since = None;
                OrphanObservation::Attached(self.identity)
            }
            Some(_) if self.verified => OrphanObservation::Gone,
            // PID 复用嫌疑兜底:未验证身份的同 pid 不同 start_time 持续超过
            // RECONCILE_MISMATCH_TIMEOUT → 裁决 Gone,恢复监管(否则对账永久
            // Pending,见常量注)。
            Some(_) => {
                let since = *self.mismatch_since.get_or_insert(output.collected_at);
                if output.collected_at.duration_since(since) >= RECONCILE_MISMATCH_TIMEOUT {
                    OrphanObservation::Gone
                } else {
                    self.observation
                }
            }
            None if presence(self.identity.pid.0) == ProcessPresence::Absent => {
                OrphanObservation::Gone
            }
            None => self.observation,
        };
        if self.observation != OrphanObservation::Pending {
            self.mismatch_since = None;
        }
        self.observation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessPresence {
    Present,
    Absent,
    Unknown,
}

#[cfg(unix)]
fn process_presence(pid: u32) -> ProcessPresence {
    // 转换失败与 PID 0 都不能传给 kill，后者会查询整个调用进程组。
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return ProcessPresence::Unknown;
    };
    if pid <= 0 {
        return ProcessPresence::Unknown;
    }
    // SAFETY:正 PID 与信号 0 只查询存在性，不投递信号或改变进程状态。
    if unsafe { libc::kill(pid, 0) } == 0 {
        return ProcessPresence::Present;
    }
    match std::io::Error::last_os_error().raw_os_error() {
        Some(libc::ESRCH) => ProcessPresence::Absent,
        Some(libc::EPERM) => ProcessPresence::Present,
        _ => ProcessPresence::Unknown,
    }
}

#[cfg(windows)]
fn process_presence(pid: u32) -> ProcessPresence {
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_INVALID_PARAMETER, GetLastError, WAIT_OBJECT_0, WAIT_TIMEOUT,
    };
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_SYNCHRONIZE, WaitForSingleObject,
    };

    if pid == 0 {
        return ProcessPresence::Unknown;
    }
    // SAFETY:请求只读查询权限，返回的句柄在所有成功路径关闭。
    let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
    if handle.is_null() {
        // SAFETY:在失败 API 后立即读取线程局部错误。
        return if unsafe { GetLastError() } == ERROR_INVALID_PARAMETER {
            ProcessPresence::Absent
        } else {
            ProcessPresence::Unknown
        };
    }
    // SAFETY:handle 为独占持有的有效进程句柄，0 超时立即返回。
    // 查询进程对象的退出信号，避免将已退出进程的退出码 259 误判为 STILL_ACTIVE。
    let state = unsafe { WaitForSingleObject(handle, 0) };
    // SAFETY:唯一句柄在本次查询后释放，后续不再使用。
    unsafe { CloseHandle(handle) };
    match state {
        WAIT_OBJECT_0 => ProcessPresence::Absent,
        WAIT_TIMEOUT => ProcessPresence::Present,
        _ => ProcessPresence::Unknown,
    }
}

#[cfg(not(any(unix, windows)))]
fn process_presence(_pid: u32) -> ProcessPresence {
    ProcessPresence::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect::{Collector, StubCollector};
    use crate::model::snapshot::ProcessSnapshot;
    use std::time::{Duration, UNIX_EPOCH};

    fn fixture(identity: ProcessIdentity) -> (OrphanTracker, CollectOutput) {
        let started = Instant::now();
        let tracker = OrphanTracker::new(identity, true, started);
        let mut output = StubCollector::default().collect(&[identity]);
        output.collected_at = started + Duration::from_secs(1);
        output.hardware.degradations.clear();
        (tracker, output)
    }

    fn process(identity: ProcessIdentity) -> ProcessSnapshot {
        ProcessSnapshot {
            identity,
            cpu_usage_percent: None,
            memory_bytes: None,
            status: None,
        }
    }

    #[test]
    fn exact_identity_attaches_without_os_absence_query() {
        let identity = ProcessIdentity::new(42, 99);
        let (mut tracker, mut output) = fixture(identity);
        output.processes.push(process(identity));
        let observation = tracker.observe_with_presence(&output, |_| panic!("身份已确认"));
        assert_eq!(observation, OrphanObservation::Attached(identity));
        assert!(tracker.is_attached());
        assert_eq!(tracker.identity(), identity);
    }

    #[test]
    fn wall_clock_rollback_does_not_block_fresh_monotonic_evidence() {
        let identity = ProcessIdentity::new(42, 99);
        let (mut tracker, mut output) = fixture(identity);
        output.hardware.timestamp = UNIX_EPOCH - Duration::from_secs(1);
        output.processes.push(process(identity));
        assert_eq!(
            tracker.observe_with_presence(&output, |_| panic!("身份已确认")),
            OrphanObservation::Attached(identity)
        );
    }

    #[test]
    fn unrequested_and_old_snapshots_never_release_a_candidate() {
        let identity = ProcessIdentity::new(42, 99);
        let (mut tracker, mut output) = fixture(identity);
        output.requested.clear();
        assert_eq!(
            tracker.observe_with_presence(&output, |_| panic!("未请求身份")),
            OrphanObservation::Pending
        );
        output.requested.push(identity);
        output.collected_at = tracker.started_at;
        assert_eq!(
            tracker.observe_with_presence(&output, |_| panic!("旧快照")),
            OrphanObservation::Pending
        );
    }

    #[test]
    fn degraded_collector_never_supplies_absence_evidence() {
        let identity = ProcessIdentity::new(42, 99);
        let (mut tracker, mut output) = fixture(identity);
        output
            .hardware
            .degradations
            .push(Degradation::CollectorInit {
                source: "unavailable".into(),
            });
        assert_eq!(
            tracker.observe_with_presence(&output, |_| panic!("采集器降级")),
            OrphanObservation::Pending
        );
    }

    #[test]
    fn missing_snapshot_needs_os_absence_confirmation() {
        let identity = ProcessIdentity::new(42, 99);
        let (mut tracker, output) = fixture(identity);
        for presence in [ProcessPresence::Present, ProcessPresence::Unknown] {
            assert_eq!(
                tracker.observe_with_presence(&output, |_| presence),
                OrphanObservation::Pending
            );
        }
        assert_eq!(
            tracker.observe_with_presence(&output, |_| ProcessPresence::Absent),
            OrphanObservation::Gone
        );
        assert!(tracker.is_gone());
    }

    #[test]
    fn verified_identity_mismatch_releases_reused_pid() {
        let identity = ProcessIdentity::new(42, 99);
        let (mut tracker, mut output) = fixture(identity);
        output
            .processes
            .push(process(ProcessIdentity::new(42, 101)));
        assert_eq!(
            tracker.observe_with_presence(&output, |_| panic!("已确认身份改变")),
            OrphanObservation::Gone
        );
    }

    #[test]
    fn unverified_spawn_clock_mismatch_stays_pending() {
        let identity = ProcessIdentity::new(42, 99);
        let (mut tracker, mut output) = fixture(identity);
        tracker.verified = false;
        output
            .processes
            .push(process(ProcessIdentity::new(42, 102)));
        assert_eq!(
            tracker.observe_with_presence(&output, |_| panic!("身份尚未核实")),
            OrphanObservation::Pending
        );
    }

    /// 未验证身份的同 pid 不同 start_time 持续超时 → 裁决 Gone 恢复监管
    /// (第六轮评审 P2:此前该分支无出口,时钟偏差写入的身份遇 PID 复用
    /// 会让对账永久 Pending)。窗口内(含边界前)保持 Pending;同 pid 的
    /// start_time 恢复匹配则立即 verified + Attached。
    #[test]
    fn unverified_prolonged_mismatch_releases_reused_pid() {
        let identity = ProcessIdentity::new(42, 99);
        let (mut tracker, mut output) = fixture(identity);
        tracker.verified = false;
        output
            .processes
            .push(process(ProcessIdentity::new(42, 102)));
        let first = tracker.observe_with_presence(&output, |_| panic!("失配首观"));
        assert_eq!(first, OrphanObservation::Pending);

        // 失配未满超时:保持 Pending
        let mut mid = output.clone();
        mid.collected_at =
            output.collected_at + RECONCILE_MISMATCH_TIMEOUT - Duration::from_secs(1);
        assert_eq!(
            tracker.observe_with_presence(&mid, |_| panic!("失配中期")),
            OrphanObservation::Pending
        );

        // 超过超时:裁决 Gone
        let mut late = output.clone();
        late.collected_at = output.collected_at + RECONCILE_MISMATCH_TIMEOUT;
        assert_eq!(
            tracker.observe_with_presence(&late, |_| panic!("失配超时")),
            OrphanObservation::Gone
        );
        assert!(tracker.is_gone());

        // 对照:失配期间 start_time 恢复匹配 → 立即 verified + Attached(不超时)
        let (mut tracker2, mut output2) = fixture(identity);
        tracker2.verified = false;
        output2
            .processes
            .push(process(ProcessIdentity::new(42, 102)));
        assert_eq!(
            tracker2.observe_with_presence(&output2, |_| panic!("失配首观")),
            OrphanObservation::Pending
        );
        output2.processes[0] = process(identity);
        assert_eq!(
            tracker2.observe_with_presence(&output2, |_| panic!("恢复匹配")),
            OrphanObservation::Attached(identity)
        );
        assert!(tracker2.verified);
    }

    #[test]
    fn later_degradation_retains_read_only_attachment() {
        let identity = ProcessIdentity::new(42, 99);
        let (mut tracker, mut output) = fixture(identity);
        output.processes.push(process(identity));
        tracker.observe_with_presence(&output, |_| ProcessPresence::Unknown);
        output.processes.clear();
        output
            .hardware
            .degradations
            .push(Degradation::CollectorInit {
                source: "retrying".into(),
            });
        assert_eq!(
            tracker.observe_with_presence(&output, |_| ProcessPresence::Absent),
            OrphanObservation::Attached(identity)
        );
    }

    #[cfg(unix)]
    #[test]
    fn own_process_presence_is_read_only_and_invalid_pid_is_unknown() {
        assert_eq!(
            process_presence(std::process::id()),
            ProcessPresence::Present
        );
        assert_eq!(process_presence(0), ProcessPresence::Unknown);
        assert_eq!(process_presence(u32::MAX), ProcessPresence::Unknown);
    }
}
