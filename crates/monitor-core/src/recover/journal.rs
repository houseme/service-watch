//! journal:append-only JSONL 持久化 (DESIGN.md §8.7、ROADMAP F7)。
//!
//! 记录 incident 转移、自动重启、Fatal 闩锁、reset 与审计;agent 重启后重放，
//! 恢复 budget 窗口与 Fatal。写线程独占文件 I/O,eval 只发消息。
//!
//! 文件 I/O 在独占的阻塞任务执行。产品路径的关键事件通过
//! [`JournalSender::send`] 等待 `sync_data` 确认，有界超时或写失败触发
//! 持久化故障停机。普通审计走 `try_send`,满队列时计数并报告丢弃。
//!
//! 重放语义:**按行顺序应用**——同 target 的 `Reset` 清除其 Fatal 与既有重启
//! 计数素材，其后新的 `Fatal`/`Restart` 照常生效 (§8.7 孤儿对账的前置数据)。
//!
//! ## 崩溃启动事务 (ROADMAP「崩溃启动事务」)
//!
//! 成对事件消除「OS spawn 成功到第一条身份落盘确认」之间的未知实例窗口：
//! - [`JournalEvent::SpawnIntent`]:spawn **前**由 eval 以 `send().await` 确认
//!   落盘 (与 Fatal 同级),携带 [`config_digest`](服务定义摘要);
//! - [`JournalEvent::SpawnOutcome`]:spawn 返回后立即落盘 (成功带身份，
//!   失败 `identity: None` + `pid_confirmed: false`),关闭窗口。
//!
//! agent 在两事件之间崩溃 → 重放后 [`JournalState::pending_spawns`] 留下
//! 未决意图;eval 冷启动据此对账 (digest 匹配当前定义 → 存在未知实例，
//! 跳过冷启动防双实例)。`SpawnOutcome` 与 `Reset`/`ResetTargets` 关闭
//! 对应条目 (reset = 人工承认未知实例已处置，重启后允许拉起)。
//!
//! ## 体积上限与单代轮转 (F24)
//!
//! 产品写入口 [`run_tracked_writer_with_limit`] 接受 `max_size`(字节;来自
//! `agent.journal_max_size`,0 = 完全禁用轮转)。写事件后实测当前文件尺寸，
//! 超限即轮转：当前 `journal.jsonl` 整体改名 `journal.1.jsonl` (覆盖上一代，
//! **只保留一代**),立即重建当前文件并写入一条轮转 Audit。改名失败降级为
//! 保留当前文件继续写 + WARN (不停机、不丢事件;体积上限是运维目标不是
//! 安全边界)。重放顺序 `journal.1 → journal.jsonl`,Fatal 闩锁/预算窗口/
//! 未决 SpawnIntent 跨轮转保持。

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use std::time::SystemTime;
use tokio::sync::{mpsc, oneshot};

use crate::config::ServiceCfg;
use crate::error::Result;
use crate::model::alert::{Category, Severity};
use crate::model::ids::ProcessIdentity;

/// 关键事件入队及落盘确认的总等待上限。超时后停止当前运行实例的恢复动作。
const CRITICAL_WRITE_TIMEOUT: Duration = Duration::from_secs(2);

/// journal 管道的运行时计数。失败累计值不依赖 writer 自身的可写性。
#[derive(Debug, Default)]
pub struct JournalCounters {
    pub dropped: AtomicU64,
    pub write_failures: AtomicU64,
    pub write_successes: AtomicU64,
    pub critical_failures: AtomicU64,
    /// F24:轮转失败降级次数 (rename/重建失败，保留当前文件继续写)。
    /// 仅周期化 WARN + 此计数;/metrics 未暴露 (轮转降级非持久化故障)。
    pub rotation_failures: AtomicU64,
    failed: AtomicBool,
    failure_notify: tokio::sync::Notify,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct JournalStats {
    pub dropped: u64,
    pub write_failures: u64,
    pub write_successes: u64,
    pub critical_failures: u64,
}

impl JournalCounters {
    pub fn has_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub async fn failure_notified(&self) {
        if !self.has_failed() {
            self.failure_notify.notified().await;
        }
    }

    /// 标记持久化故障 (闩锁;writer 写失败与演练/外部观察共用入口)。
    pub fn fail(&self) {
        self.failed.store(true, Ordering::Release);
        self.failure_notify.notify_one();
    }

    pub fn snapshot(&self) -> JournalStats {
        JournalStats {
            dropped: self.dropped.load(Ordering::Relaxed),
            write_failures: self.write_failures.load(Ordering::Relaxed),
            write_successes: self.write_successes.load(Ordering::Relaxed),
            critical_failures: self.critical_failures.load(Ordering::Relaxed),
        }
    }

    fn dropped(&self) {
        let total = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
        // 持续拥塞时周期重复报告，同时避免每条事件产生一条日志。
        if total == 1 || total.is_multiple_of(64) {
            tracing::error!(dropped = total, "journal queue unavailable; event dropped");
        }
    }
}

/// 带失败计数及关键写入确认的 journal 出口。
#[derive(Clone, Debug)]
pub struct JournalSender {
    tx: mpsc::Sender<JournalRequest>,
    counters: Arc<JournalCounters>,
}

#[derive(Debug)]
pub struct JournalRequest {
    pub event: JournalEvent,
    pub(crate) ack: Option<oneshot::Sender<std::result::Result<(), String>>>,
}

/// 创建运行时使用的有界持久化通道。
pub fn channel(capacity: usize) -> (JournalSender, mpsc::Receiver<JournalRequest>) {
    let (tx, rx) = mpsc::channel(capacity);
    (
        JournalSender {
            tx,
            counters: Arc::new(JournalCounters::default()),
        },
        rx,
    )
}

impl JournalSender {
    pub fn counters(&self) -> &Arc<JournalCounters> {
        &self.counters
    }

    /// 普通事件仅尝试入队。满/关闭的每次失败均入 dropped 指标。
    pub fn try_send(
        &self,
        event: JournalEvent,
    ) -> std::result::Result<(), mpsc::error::TrySendError<JournalEvent>> {
        let critical = event.is_critical();
        let result = self
            .tx
            .try_send(JournalRequest { event, ack: None })
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(req) => mpsc::error::TrySendError::Full(req.event),
                mpsc::error::TrySendError::Closed(req) => {
                    mpsc::error::TrySendError::Closed(req.event)
                }
            });
        if result.is_err() {
            self.counters.dropped();
            if critical {
                self.counters
                    .critical_failures
                    .fetch_add(1, Ordering::Relaxed);
                self.counters.fail();
            }
        }
        result
    }

    /// 关键状态等待 writer 的 sync_data 确认。2s 总预算覆盖队列背压与写入。
    /// 失败触发故障停机，避免当前 agent 继续在不可持久化的预算上自动恢复。
    pub async fn send(&self, event: JournalEvent) -> std::result::Result<(), String> {
        let mut queued = false;
        let attempt = async {
            let (ack, received) = oneshot::channel();
            self.tx
                .send(JournalRequest {
                    event,
                    ack: Some(ack),
                })
                .await
                .map_err(|e| e.to_string())?;
            queued = true;
            received.await.map_err(|e| e.to_string())?
        };
        let result = tokio::time::timeout(CRITICAL_WRITE_TIMEOUT, attempt)
            .await
            .unwrap_or_else(|_| Err("journal persistence confirmation timed out".into()));
        if result.is_err() {
            self.counters
                .critical_failures
                .fetch_add(1, Ordering::Relaxed);
            if !queued {
                self.counters.dropped();
            }
            self.counters.fail();
        }
        result
    }
}

/// 单条 journal 记录 (JSONL 一行)。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JournalEvent {
    Incident {
        at: SystemTime,
        incident: u64,
        target: String,
        check: String,
        category: Category,
        severity: Severity,
        reason: String,
    },
    Restart {
        at: SystemTime,
        target: String,
        /// uptime < min_uptime 视为 crash loop 嫌疑 (信息用;计数不分快慢)。
        fast_exit: bool,
    },
    Fatal {
        at: SystemTime,
        target: String,
        reason: String,
    },
    Reset {
        at: SystemTime,
        target: String,
        source: String,
    },
    /// 热重载在变更内存拓扑前一次性提交全部重定义/删除目标的预算重置。
    ResetTargets {
        at: SystemTime,
        targets: Vec<String>,
        source: String,
    },
    /// supervise 身份对账素材。采集器确认前的 spawn 时间戳须标记未验证。
    ProcessIdentity {
        at: SystemTime,
        target: String,
        identity: Option<ProcessIdentity>,
        #[serde(default)]
        verified: bool,
    },
    /// 崩溃启动事务:spawn 前落盘的启动意图 (await 确认，与 Fatal 同级)。
    /// `config_digest` 为服务定义摘要，供冷启动对账识别意图属于当前还是旧定义。
    SpawnIntent {
        at: SystemTime,
        target: String,
        config_digest: String,
    },
    /// 崩溃启动事务:spawn 结果 (成功带身份;失败无身份且未确认 pid),
    /// 关闭对应 [`JournalEvent::SpawnIntent`] 开启的未知实例窗口。
    SpawnOutcome {
        at: SystemTime,
        target: String,
        identity: Option<ProcessIdentity>,
        pid_confirmed: bool,
    },
    Audit {
        at: SystemTime,
        message: String,
    },
}

impl JournalEvent {
    fn is_critical(&self) -> bool {
        matches!(
            self,
            Self::Restart { .. }
                | Self::Fatal { .. }
                | Self::Reset { .. }
                | Self::ResetTargets { .. }
                | Self::ProcessIdentity { .. }
                | Self::SpawnIntent { .. }
                | Self::SpawnOutcome { .. }
        )
    }
}

/// 上次 agent 保存的进程身份与观测可信度。
#[derive(Debug, Clone, Copy)]
pub struct PersistedProcess {
    pub identity: ProcessIdentity,
    pub verified: bool,
}

/// 崩溃启动事务的未决意图 (target 级;`SpawnOutcome`/`Reset` 关闭即移除)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSpawn {
    /// 意图落盘时刻 (墙钟;诊断用)。
    pub at: SystemTime,
    /// 意图对应的服务定义摘要 ([`config_digest`])。
    pub config_digest: String,
}

/// 重放结果 (agent 启动时加载)。
#[derive(Debug, Default, Clone)]
pub struct JournalState {
    /// target → 窗口内自动重启时间戳 (重放时按 restart_window 过滤由调用方做)。
    pub restarts: BTreeMap<String, Vec<SystemTime>>,
    pub fatal: HashSet<String>,
    pub last_reset: BTreeMap<String, SystemTime>,
    pub processes: BTreeMap<String, PersistedProcess>,
    /// 崩溃启动事务:target → 未决启动意图 (意图已落盘、结果未到 =
    /// 上次 agent 在 spawn 与结果落盘之间崩溃，存在未知实例)。
    pub pending_spawns: BTreeMap<String, PendingSpawn>,
    /// F25: incidents still open when replay finishes, keyed by (target, check) —
    /// alert/escalate opens the gate (updating severity), resolve closes it,
    /// flap summaries leave the gate untouched. Reset/ResetTargets from the
    /// recover domain do **not** clear real check-alert entries (a manual reset
    /// clears Fatal/budget, not check alert state) — the sole exception is the
    /// Fatal synthetic incident `(target, RECOVER_CHECK)`, which belongs to the
    /// recovery domain and IS closed by Reset/ResetTargets (round-9 review P2).
    pub open_incidents: BTreeMap<(String, String), PersistedIncident>,
    /// F25: resolve events that referenced no open incident during replay
    /// (historical leftovers / half-line pairing loss). Idempotently discarded;
    /// counted for diagnostics only.
    pub orphan_resolves: u64,
}

/// F25: snapshot of an incident still open after replay (material for the
/// eval side to restore alert state and re-notify).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedIncident {
    pub incident_key: u64,
    pub target: String,
    pub check: String,
    pub severity: crate::model::alert::Severity,
    pub reason: String,
    /// Wall-clock time of the latest alert/escalate event (seed for the
    /// re-notify window start).
    pub last_event_at: SystemTime,
}

/// 服务定义摘要 (崩溃启动事务对账键):[`ServiceCfg`] 经 serde JSON 序列化
/// (字段按声明序，确定性) 后交给 `DefaultHasher`(`new()` 固定密钥，
/// **跨进程稳定** —— 这是跨重启对账的前提，不得改用 `RandomState`)。
/// 十六进制字符串;序列化失败 (不可能，配置层已校验) 以 `"null"` 兜底。
pub fn config_digest(cfg: &ServiceCfg) -> String {
    // H-P2:digest 只覆盖**热字段集**(name + command/args/workdir/env/windows_service
    // —— 与 reload::service_hot_changed 对齐)+ FNV-1a-64(跨工具链稳定，
    // DefaultHasher 不保证跨 rustc 版本;升级 agent 二进制恰是崩溃后重启高发
    // 场景，软字段变更 (restart/activate) 不得改变意图归属 → 防 fail-open 双实例)。
    use std::hash::Hasher;
    let mut h = Fnv64::default();
    for piece in [
        cfg.name.as_str(),
        cfg.command.as_str(),
        &serde_json::to_string(&cfg.args).unwrap_or_default(),
        &serde_json::to_string(&cfg.workdir).unwrap_or_default(),
        &serde_json::to_string(&cfg.env).unwrap_or_default(),
        &serde_json::to_string(&cfg.windows_service).unwrap_or_default(),
    ] {
        h.write(piece.as_bytes());
        h.write(b"\x1f"); // 单元分隔符，防拼接歧义
    }
    format!("{:016x}", h.finish())
}

/// FNV-1a-64(零依赖;跨 rustc 版本稳定 —— H-P3)。
struct Fnv64(u64);
impl Default for Fnv64 {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}
impl std::hash::Hasher for Fnv64 {
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= b as u64;
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    fn finish(&self) -> u64 {
        self.0
    }
}

/// F24:上一代 journal 路径。`journal.jsonl` → 同目录 `journal.1.jsonl`
/// (只保留一代，二次轮转覆盖);无扩展名或多级扩展名同样成立。
pub(crate) fn gen1_path(path: &Path) -> PathBuf {
    match path.file_name().and_then(|n| n.to_str()) {
        Some(name) => match name.rsplit_once('.') {
            Some((stem, _)) if !stem.is_empty() => path.with_file_name(format!("{stem}.1.jsonl")),
            _ => path.with_file_name(format!("{name}.1.jsonl")),
        },
        None => path.with_file_name("journal.1.jsonl"),
    }
}

/// 打开 journal 并确认可持久化，应在任何恢复任务启动前调用。
pub fn open_writer(path: &Path) -> Result<std::fs::File> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)?;
    if file.metadata()?.len() > 0 {
        file.seek(SeekFrom::End(-1))?;
        let mut last = [0];
        file.read_exact(&mut last)?;
        if last[0] != b'\n' {
            // 为旧的完整无换行记录及崩溃半行划定边界，使后续已确认记录可重放。
            file.write_all(b"\n")?;
        }
    }
    file.sync_data()?;
    Ok(file)
}

/// 产品写入口。文件已由 runtime 在任务启动前打开并确认可写。
/// `max_size`:journal 轮转阈值 (字节;0 = 完全禁用轮转，F24)。阈值从
/// `agent.journal_max_size` 经 runtime 启动处传入;确认式持久化语义不变。
pub async fn run_tracked_writer_with_limit(
    file: std::fs::File,
    path: PathBuf,
    mut rx: mpsc::Receiver<JournalRequest>,
    counters: Arc<JournalCounters>,
    max_size: u64,
) -> Result<()> {
    let failures = counters.clone();
    let result = tokio::task::spawn_blocking(move || {
        write_events(file, path, counters, max_size, || rx.blocking_recv())
    })
    .await
    .map_err(|e| {
        failures.fail();
        crate::error::CoreError::Recover(format!("journal 写任务异常退出：{e}"))
    })?;
    if result.is_err() {
        failures.fail();
    }
    result
}

fn write_events(
    mut file: std::fs::File,
    path: PathBuf,
    counters: Arc<JournalCounters>,
    max_size: u64,
    mut receive: impl FnMut() -> Option<JournalRequest>,
) -> Result<()> {
    while let Some(request) = receive() {
        let critical = request.event.is_critical();
        let result = (|| -> Result<()> {
            let mut line = serde_json::to_vec(&request.event)?;
            line.push(b'\n');
            file.write_all(&line)?;
            // File 没有用户态缓冲。关键状态必须在确认前提交到持久介质。
            if critical || request.ack.is_some() {
                file.sync_data()?;
            }
            Ok(())
        })();
        if let Some(ack) = request.ack {
            let _ = ack.send(result.as_ref().map(|_| ()).map_err(ToString::to_string));
        }
        if let Err(e) = result {
            counters.write_failures.fetch_add(1, Ordering::Relaxed);
            counters.dropped();
            tracing::error!(path = %path.display(), error = %e, "journal write failed; stopping recovery runtime");
            counters.fail();
            // 写失败可能留下半行。立即关闭通道，防止后续记录接在半行后被重放丢弃。
            return Err(e);
        }
        counters.write_successes.fetch_add(1, Ordering::Relaxed);
        // F24:写**后**实测当前文件尺寸决定是否轮转 (选写后而非写前预判的
        // 理由:metadata 读到的是内核眼中的真实大小，外部 copytruncate 轮转
        // 工具 (RUNBOOK §6.3) 把文件清零后，累计字节数式预判会立即误轮转，
        // 实测天然兼容;journal 事件频率低，一次 metadata 系统调用成本可忽略)。
        // 阈值是软性运维目标：单行超出阈值的一行溢出 (~几百字节) 可接受。
        if max_size > 0
            && let Ok(meta) = file.metadata()
            && meta.len() > max_size
        {
            match rotate_file(file, &path, meta.len(), SystemTime::now()) {
                Ok(new_file) => file = new_file,
                Err(e) => {
                    // 降级 (F24):改名/重建失败保留当前文件继续写 —— 不停机、
                    // 不丢事件，超限文件继续增长直至运维介入 (体积上限是运维
                    // 目标不是安全边界)。句柄已在 rotate_file 中释放，必须重开
                    // 当前文件;重开失败属真实持久化故障，走既有停机路径。
                    // P3(第九轮验证):计数 + 周期化 WARN (对齐 dropped 模式),
                    // 持续失败时不再逐事件刷屏。
                    let n = counters.rotation_failures.fetch_add(1, Ordering::Relaxed) + 1;
                    if n == 1 || n.is_multiple_of(64) {
                        tracing::warn!(
                            path = %path.display(),
                            failures = n,
                            error = %e,
                            "journal rotation failed; keeping current file (size cap is an ops target, not a safety boundary)"
                        );
                    }
                    file = open_writer(&path)?;
                }
            }
        }
    }
    if let Err(e) = file.sync_data() {
        counters.write_failures.fetch_add(1, Ordering::Relaxed);
        counters.fail();
        return Err(e.into());
    }
    Ok(())
}

/// F24:单代轮转。步骤：释放当前句柄 → 删除旧 `journal.1.jsonl`(若存在) →
/// 当前文件原子改名 `journal.1.jsonl` → 立即重建当前文件 → 向**新文件**写
/// 一条 Audit(含轮转前后字节数，边界一次性 sync_data 落盘;轮转低频，sync
/// 成本可忽略)。
///
/// Windows 语义：打开中的文件不能 rename，必须先释放句柄再改名 (先 drop 后
/// rename)。`std::fs::File` 无用户态缓冲，数据已直接进入内核，无需显式 flush。
/// rename 同目录原子完成，不产生交错行;任何一步失败由调用方降级处理。
///
/// P3(第九轮验证):改名成功后的 Audit 行写入失败**不算轮转失败** —— 轮转
/// 实际已发生，降级为 WARN "audit line lost" 并返回 Ok(new_file);Audit 仅
/// 存档，其丢失不影响任何恢复语义 (调用方无需再重开文件)。
///
/// 注：阈值应显著大于单行长度 (一行 ~数百字节)。阈值小于轮转 audit 一行时
/// 会退化为逐事件轮转 (每写一条就轮转一次，行为仍正确 —— 一代保留语义不变，
/// 但历史事件只存活一代),配置层/运维应避免。慢盘环境下轮转与下一关键事件
/// 共享单写线程的 2s 确认预算 (本地 fs 轮转为毫秒级，理论边缘;见 RUNBOOK)。
fn rotate_file(
    file: std::fs::File,
    path: &Path,
    rotated_bytes: u64,
    at: SystemTime,
) -> Result<std::fs::File> {
    let gen1 = gen1_path(path);
    drop(file);
    if gen1.exists() {
        std::fs::remove_file(&gen1)?;
    }
    std::fs::rename(path, &gen1)?;
    let mut new_file = open_writer(path)?;
    let audit = JournalEvent::Audit {
        at,
        message: format!(
            "journal rotated: {rotated_bytes} bytes -> {} (one generation kept); current file restarted (0 bytes)",
            gen1.display()
        ),
    };
    let audit_result = serde_json::to_vec(&audit).map(|mut line| {
        line.push(b'\n');
        line
    });
    match audit_result {
        Ok(line) => {
            if let Err(e) = new_file
                .write_all(&line)
                .and_then(|()| new_file.sync_data())
            {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "journal rotation completed but audit line lost (archive-only; recovery semantics unaffected)"
                );
            }
        }
        Err(e) => {
            tracing::warn!(
                path = %path.display(),
                error = %e,
                "journal rotation completed but audit line lost (archive-only; recovery semantics unaffected)"
            );
        }
    }
    Ok(new_file)
}

/// 运行时重放入口。无法读取既有日志时拒绝启动，避免空状态绕过 Fatal 闩锁。
///
/// F24:`journal.1.jsonl` 存在时先重放上一代，再重放当前文件，顺序 apply 到
/// 同一 [`JournalState`] —— Fatal 闩锁/预算窗口/未决 SpawnIntent 跨轮转保持
/// (行顺序语义与单文件一致;rename 同目录原子，不产生跨文件交错行)。
/// `journal_max_size = 0` 时 journal.1 永不产生，NotFound 分支天然兼容;
/// 单文件不存在路径行为不变。
pub fn replay_checked(path: &Path) -> Result<JournalState> {
    let mut state = JournalState::default();
    replay_file(&gen1_path(path), &mut state)?;
    replay_file(path, &mut state)?;
    Ok(state)
}

/// 单文件重放 (字节级读取 + 坏行跳过;NotFound 视为空文件)。
/// F24 从 replay_checked 拆出以支持上一代文件;语义不变。
fn replay_file(path: &Path, state: &mut JournalState) -> Result<()> {
    // H-P2:字节级读取 —— 断电撕裂点落在多字节 UTF-8 字符内部时，
    // read_to_string 会整体失败 (InvalidData) 并升级为 agent 拒绝启动;
    // 逐字节切行 + 尾行 lossy 净化，与 open_writer 半行定界哲学一致。
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let content = String::from_utf8_lossy(&bytes);
    for (no, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<JournalEvent>(line) {
            Ok(event) => apply(&event, state),
            Err(e) => {
                tracing::warn!(line = no + 1, error = %e, "journal: 坏行跳过");
            }
        }
    }
    Ok(())
}

/// Reset/ResetTargets 共用:解除 Fatal 闩锁 + 清空预算 + 清未决意图 + 关闭
/// Fatal 合成 incident + 记录 reset 时点。Reset 语义 = 人工承认未知实例已
/// 处置 (DESIGN §8.2 / FSM reset):重放时同样必须清掉该 target 的既有重启
/// 素材，否则 monitorctl reset 后重启 agent,Reset 之前的旧重启记录会复活
/// 并继续计入预算窗口 (第五轮评审:模块 doc 声称清除但 apply 未实现)。
/// P2(第九轮验证):Fatal 的合成 incident (check = RECOVER_CHECK) 属恢复域
/// —— reset 解除闩锁即同时闭门，否则此后每次重启都会对已 reset 的 target
/// 重放虚假 "latched fatal" 重通知。(eval 侧 restore_incidents 另有 fatal
/// 集合防御层。)
fn reset_one(state: &mut JournalState, target: &str, at: SystemTime) {
    state.fatal.remove(target);
    state.restarts.remove(target);
    state.pending_spawns.remove(target);
    state.open_incidents.remove(&(
        target.to_string(),
        crate::model::alert::RECOVER_CHECK.to_string(),
    ));
    state.last_reset.insert(target.to_string(), at);
}

/// 按行顺序应用单条事件 (Reset 之后同 target 的新 Fatal 仍要置位)。
fn apply(event: &JournalEvent, state: &mut JournalState) {
    match event {
        JournalEvent::Restart { at, target, .. } => {
            state.restarts.entry(target.clone()).or_default().push(*at);
        }
        JournalEvent::Fatal { target, .. } => {
            state.fatal.insert(target.clone());
        }
        JournalEvent::Reset { at, target, .. } => {
            reset_one(state, target, *at);
        }
        JournalEvent::ResetTargets { at, targets, .. } => {
            for target in targets {
                reset_one(state, target, *at);
            }
        }
        JournalEvent::ProcessIdentity {
            target,
            identity,
            verified,
            ..
        } => {
            if let Some(identity) = identity {
                state.processes.insert(
                    target.clone(),
                    PersistedProcess {
                        identity: *identity,
                        verified: *verified,
                    },
                );
            } else {
                state.processes.remove(target);
            }
        }
        // 崩溃启动事务：新意图重新开启未知实例窗口;结果 (成功或失败) 关闭之。
        JournalEvent::SpawnIntent {
            at,
            target,
            config_digest,
        } => {
            state.pending_spawns.insert(
                target.clone(),
                PendingSpawn {
                    at: *at,
                    config_digest: config_digest.clone(),
                },
            );
        }
        JournalEvent::SpawnOutcome {
            target,
            identity,
            pid_confirmed,
            ..
        } => {
            state.pending_spawns.remove(target);
            // H-P2:成功 outcome 回填身份 —— intent→outcome→ProcessIdentity 三段
            // 之间崩溃时，重放不再拿陈旧身份做孤儿对账 (残余双实例窗口关闭)。
            if *pid_confirmed && let Some(id) = identity {
                state.processes.insert(
                    target.clone(),
                    PersistedProcess {
                        identity: *id,
                        verified: false,
                    },
                );
            }
        }
        JournalEvent::Incident {
            at,
            incident,
            target,
            check,
            category,
            severity,
            reason,
        } => {
            // F25: open-set maintenance. alert/escalate opens the gate
            // (recording the latest severity and reason); resolve closes it; a
            // flap-suppression summary leaves the gate as-is (at summary time
            // the incident may be open or closed and replay has no third-state
            // evidence, so the current state is kept conservatively — the
            // difference from the old F7 "trail does not survive restart"
            // semantics is exactly this table).
            use crate::model::alert::Category;
            let key = (target.clone(), check.clone());
            match category {
                Category::Resolve => {
                    // F25:配对完整性 —— resolve 引用不存在的 open incident
                    // (历史遗留/半行丢失配对) 时幂等丢弃并计数，供诊断。
                    if state.open_incidents.remove(&key).is_none() {
                        state.orphan_resolves += 1;
                    }
                }
                Category::Alert | Category::Escalate => {
                    state.open_incidents.insert(
                        key,
                        PersistedIncident {
                            incident_key: *incident,
                            target: target.clone(),
                            check: check.clone(),
                            severity: *severity,
                            reason: reason.clone(),
                            last_event_at: *at,
                        },
                    );
                }
                Category::Flap => {}
            }
        }
        // audit is archived only; it takes no part in recovery state rebuild
        JournalEvent::Audit { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn tracked_sender_waits_for_persistence_confirmation() {
        let (sender, mut rx) = channel(1);
        let task = tokio::spawn(async move {
            sender
                .send(JournalEvent::Fatal {
                    at: base(),
                    target: "app".into(),
                    reason: "budget".into(),
                })
                .await
        });
        let request = rx.recv().await.unwrap();
        assert!(!task.is_finished(), "入队成功仍需等待 writer 确认");
        request.ack.unwrap().send(Ok(())).unwrap();
        assert!(task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn tracked_writer_acks_durable_recovery_state() {
        let dir = tmp_dir("tracked");
        cleanup(&dir);
        let path = dir.join("journal.jsonl");
        let file = open_writer(&path).unwrap();
        let (sender, rx) = channel(2);
        let counters = sender.counters().clone();
        let writer = tokio::spawn(run_tracked_writer_with_limit(
            file,
            path.clone(),
            rx,
            counters.clone(),
            0,
        ));
        sender
            .send(JournalEvent::Fatal {
                at: base(),
                target: "app".into(),
                reason: "budget".into(),
            })
            .await
            .unwrap();
        assert!(replay_checked(&path).unwrap().fatal.contains("app"));
        drop(sender);
        writer.await.unwrap().unwrap();
        assert_eq!(counters.snapshot().write_successes, 1);
        assert_eq!(counters.snapshot().write_failures, 0);
        cleanup(&dir);
    }

    #[test]
    fn queue_rejection_is_counted_every_time() {
        let (sender, _rx) = channel(1);
        let event = || JournalEvent::Audit {
            at: base(),
            message: "test".into(),
        };
        sender.try_send(event()).unwrap();
        assert!(sender.try_send(event()).is_err());
        assert!(sender.try_send(event()).is_err());
        assert_eq!(sender.counters().snapshot().dropped, 2);
    }

    #[test]
    fn batch_reset_is_atomic_and_preserves_process_identity() {
        let mut state = JournalState::default();
        for target in ["a", "b"] {
            apply(
                &JournalEvent::Fatal {
                    at: base(),
                    target: target.into(),
                    reason: "budget".into(),
                },
                &mut state,
            );
            apply(
                &JournalEvent::Restart {
                    at: base(),
                    target: target.into(),
                    fast_exit: false,
                },
                &mut state,
            );
        }
        let identity = ProcessIdentity::new(42, 1000);
        apply(
            &JournalEvent::ProcessIdentity {
                at: base(),
                target: "a".into(),
                identity: Some(identity),
                verified: true,
            },
            &mut state,
        );
        apply(
            &JournalEvent::ResetTargets {
                at: base(),
                targets: vec!["a".into(), "b".into()],
                source: "reload".into(),
            },
            &mut state,
        );
        assert!(state.fatal.is_empty());
        assert!(state.restarts.is_empty());
        assert_eq!(state.last_reset.len(), 2);
        assert_eq!(state.processes["a"].identity, identity);
        assert!(state.processes["a"].verified);
        apply(
            &JournalEvent::ProcessIdentity {
                at: base(),
                target: "a".into(),
                identity: None,
                verified: false,
            },
            &mut state,
        );
        assert!(state.processes.is_empty());
    }

    #[test]
    fn checked_replay_rejects_unreadable_journal() {
        let dir = tmp_dir("directory-is-not-journal");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        assert!(replay_checked(&dir).is_err());
        cleanup(&dir);
    }

    // ---- 崩溃启动事务:SpawnIntent/SpawnOutcome 序列化 + 重放收集 ----

    /// 重放收集每个 target 的最后状态:intent 无 outcome → pending 保留;
    /// intent+outcome(成功或失败)→ 窗口关闭;Reset/ResetTargets 清除。
    #[test]
    fn spawn_transaction_replay_collects_pending_and_closes_on_outcome_or_reset() {
        let dir = tmp_dir("spawn-tx-replay");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let t = base();
        let line = |e: &JournalEvent| serde_json::to_string(e).unwrap();
        let content = format!(
            "{}\n{}\n{}\n{}\n{}\n",
            line(&JournalEvent::SpawnIntent {
                at: t,
                target: "app".into(),
                config_digest: "digest-a".into(),
            }),
            line(&JournalEvent::SpawnIntent {
                at: t,
                target: "web".into(),
                config_digest: "digest-b".into(),
            }),
            line(&JournalEvent::SpawnOutcome {
                at: t + Duration::from_secs(1),
                target: "web".into(),
                identity: Some(ProcessIdentity::new(7, 100)),
                pid_confirmed: true,
            }),
            line(&JournalEvent::SpawnIntent {
                at: t + Duration::from_secs(2),
                target: "db".into(),
                config_digest: "digest-c".into(),
            }),
            // spawn 失败同样落结果并关闭窗口
            line(&JournalEvent::SpawnOutcome {
                at: t + Duration::from_secs(3),
                target: "db".into(),
                identity: None,
                pid_confirmed: false,
            }),
        );
        std::fs::write(&path, content).unwrap();
        let state = replay_checked(&path).unwrap_or_default();
        assert_eq!(state.pending_spawns.len(), 1, "仅 app 留下未决意图");
        assert_eq!(state.pending_spawns["app"].config_digest, "digest-a");
        assert_eq!(state.pending_spawns["app"].at, t);
        assert!(
            !state.pending_spawns.contains_key("web"),
            "成功 outcome 关闭窗口"
        );
        assert!(
            !state.pending_spawns.contains_key("db"),
            "失败 outcome 同样关闭窗口"
        );
        cleanup(&dir);
    }

    /// Reset / ResetTargets 清除未决意图:monitorctl reset 后重启 agent
    /// 不再因旧意图跳过冷启动 (「reset 后可强制拉起」语义)。
    #[test]
    fn spawn_pending_intent_is_cleared_by_reset_events() {
        let t = base();
        let mut state = JournalState::default();
        apply(
            &JournalEvent::SpawnIntent {
                at: t,
                target: "app".into(),
                config_digest: "d".into(),
            },
            &mut state,
        );
        apply(
            &JournalEvent::Reset {
                at: t,
                target: "app".into(),
                source: "monitorctl".into(),
            },
            &mut state,
        );
        assert!(state.pending_spawns.is_empty(), "Reset 清除未决意图");
        apply(
            &JournalEvent::SpawnIntent {
                at: t,
                target: "app".into(),
                config_digest: "d".into(),
            },
            &mut state,
        );
        apply(
            &JournalEvent::ResetTargets {
                at: t,
                targets: vec!["app".into()],
                source: "reload".into(),
            },
            &mut state,
        );
        assert!(state.pending_spawns.is_empty(), "ResetTargets 清除未决意图");
    }

    /// 两类事务事件必须走关键通道 (await 确认落盘),否则窗口语义失效。
    #[test]
    fn spawn_transaction_events_are_critical() {
        let intent = JournalEvent::SpawnIntent {
            at: base(),
            target: "app".into(),
            config_digest: "d".into(),
        };
        let outcome = JournalEvent::SpawnOutcome {
            at: base(),
            target: "app".into(),
            identity: None,
            pid_confirmed: false,
        };
        assert!(intent.is_critical());
        assert!(outcome.is_critical());
        // serde 往返 (反序列化侧字段齐全)
        let round: JournalEvent =
            serde_json::from_str(&serde_json::to_string(&intent).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_string(&round).unwrap(),
            serde_json::to_string(&intent).unwrap()
        );
    }

    /// 摘要语义：同定义同值;command/args 变更即变 (旧定义意图据此识别)。
    #[test]
    fn config_digest_is_stable_and_tracks_definition_changes() {
        let a = sample_service("/bin/app", &["30"]);
        assert_eq!(config_digest(&a), config_digest(&a.clone()), "同定义同值");
        let mut b = a.clone();
        b.command = "/bin/other".into();
        assert_ne!(config_digest(&a), config_digest(&b), "command 变更即变");
        let mut c = a.clone();
        c.args = vec!["31".into()];
        assert_ne!(config_digest(&a), config_digest(&c), "args 变更即变");
    }

    fn sample_service(command: &str, args: &[&str]) -> ServiceCfg {
        ServiceCfg {
            name: "app".into(),
            command: command.into(),
            args: args.iter().map(|s| (*s).to_string()).collect(),
            workdir: None,
            env: Default::default(),
            console: false,
            windows_service: None,
            depends_on: Vec::new(),
            restart: crate::config::RestartCfg::default(),
            activate: None,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn critical_confirmation_timeout_is_bounded_and_instance_scoped() {
        let (sender, mut rx) = channel(1);
        let counters = sender.counters().clone();
        let (other, _other_rx) = channel(1);
        let task = tokio::spawn(async move {
            sender
                .send(JournalEvent::Fatal {
                    at: base(),
                    target: "app".into(),
                    reason: "budget".into(),
                })
                .await
        });
        let _unconfirmed = rx.recv().await.unwrap();
        tokio::time::advance(CRITICAL_WRITE_TIMEOUT).await;
        assert!(task.await.unwrap().is_err());
        assert!(counters.has_failed());
        assert_eq!(counters.snapshot().critical_failures, 1);
        assert!(
            !other.counters().has_failed(),
            "故障只属于创建该 sender 的 runtime"
        );
    }

    #[tokio::test]
    async fn opening_partial_tail_preserves_later_durable_fatal() {
        let dir = tmp_dir("partial-tail");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        std::fs::write(&path, r#"{"type":"audit""#).unwrap();
        let file = open_writer(&path).unwrap();
        let (sender, rx) = channel(1);
        let writer = tokio::spawn(run_tracked_writer_with_limit(
            file,
            path.clone(),
            rx,
            sender.counters().clone(),
            0,
        ));
        sender
            .send(JournalEvent::Fatal {
                at: base(),
                target: "app".into(),
                reason: "budget".into(),
            })
            .await
            .unwrap();
        drop(sender);
        writer.await.unwrap().unwrap();
        assert!(replay_checked(&path).unwrap().fatal.contains("app"));
        cleanup(&dir);
    }

    #[test]
    fn failed_write_rejects_confirmation_and_marks_persistence_failure() {
        let dir = tmp_dir("read-only-writer");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        std::fs::write(&path, "").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let counters = Arc::new(JournalCounters::default());
        let (ack, mut receipt) = oneshot::channel();
        let mut request = Some(JournalRequest {
            event: JournalEvent::Fatal {
                at: base(),
                target: "app".into(),
                reason: "budget".into(),
            },
            ack: Some(ack),
        });
        assert!(write_events(file, path, counters.clone(), 0, || request.take()).is_err());
        assert!(receipt.try_recv().unwrap().is_err());
        assert!(counters.has_failed());
        assert_eq!(counters.snapshot().write_failures, 1);
        assert_eq!(counters.snapshot().write_successes, 0);
        cleanup(&dir);
    }

    /// 各测试独立的临时目录 (本 crate 不依赖 tempfile;用 pid + 测试名隔离)。
    fn tmp_dir(test: &str) -> PathBuf {
        std::env::temp_dir().join(format!("sw-journal-test-{}-{}", std::process::id(), test))
    }

    fn cleanup(dir: &Path) {
        let _ = std::fs::remove_dir_all(dir);
    }

    /// 单测助手:打开文件并启动产品 writer(禁轮转),事件经 JournalSender 投递。
    fn spawn_test_writer(
        path: &Path,
        cap: usize,
    ) -> (JournalSender, tokio::task::JoinHandle<Result<()>>) {
        let (tx, rx) = channel(cap);
        let file = open_writer(path).expect("open journal");
        let writer = tokio::spawn(run_tracked_writer_with_limit(
            file,
            path.to_path_buf(),
            rx,
            tx.counters().clone(),
            0,
        ));
        (tx, writer)
    }

    fn base() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
    }

    #[tokio::test]
    async fn writer_appends_and_replay_restores_all_kinds() {
        let dir = tmp_dir("basic");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let (tx, writer) = spawn_test_writer(&path, 16);
        let t = base();
        tx.send(JournalEvent::Restart {
            at: t,
            target: "web-1".into(),
            fast_exit: true,
        })
        .await
        .unwrap();
        tx.send(JournalEvent::Restart {
            at: t + Duration::from_secs(3),
            target: "web-1".into(),
            fast_exit: false,
        })
        .await
        .unwrap();
        tx.send(JournalEvent::Restart {
            at: t + Duration::from_secs(5),
            target: "db-1".into(),
            fast_exit: false,
        })
        .await
        .unwrap();
        tx.send(JournalEvent::Fatal {
            at: t + Duration::from_secs(6),
            target: "db-1".into(),
            reason: "budget".into(),
        })
        .await
        .unwrap();
        tx.send(JournalEvent::Audit {
            at: t + Duration::from_secs(7),
            message: "audit line".into(),
        })
        .await
        .unwrap();
        tx.send(JournalEvent::Incident {
            at: t,
            incident: 42,
            target: "web-1".into(),
            check: "http".into(),
            category: Category::Alert,
            severity: Severity::Warning,
            reason: "down".into(),
        })
        .await
        .unwrap();
        drop(tx); // 关闭 → writer 返回 Ok
        writer.await.unwrap().unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content.lines().count(), 6, "逐行 JSONL");

        let state = replay_checked(&path).unwrap_or_default();
        assert_eq!(state.restarts["web-1"].len(), 2);
        assert_eq!(state.restarts["db-1"].len(), 1);
        assert!(state.fatal.contains("db-1"));
        assert!(!state.fatal.contains("web-1"));
        assert!(state.last_reset.is_empty());
        assert_eq!(
            state.open_incidents.len(),
            1,
            "alert opens and no resolve follows -> still open after replay (F25)"
        );
        let inc = state
            .open_incidents
            .get(&("web-1".into(), "http".into()))
            .unwrap();
        assert_eq!(inc.incident_key, 42);
        assert_eq!(inc.severity, Severity::Warning);
        cleanup(&dir);
    }

    #[tokio::test]
    async fn replay_collects_still_open_incidents_f25() {
        let dir = tmp_dir("open-incidents");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let (tx, writer) = spawn_test_writer(&path, 16);
        let t = base();
        let inc_event = |category: Category,
                         severity: Severity,
                         at: SystemTime,
                         incident: u64,
                         check: &str,
                         reason: &str| {
            tx.try_send(JournalEvent::Incident {
                at,
                incident,
                target: "api".into(),
                check: check.into(),
                category,
                severity,
                reason: reason.into(),
            })
            .unwrap();
        };
        inc_event(Category::Alert, Severity::Warning, t, 1, "http", "down");
        inc_event(
            Category::Escalate,
            Severity::Critical,
            t + Duration::from_secs(30),
            1,
            "http",
            "still down",
        );
        inc_event(
            Category::Alert,
            Severity::Warning,
            t + Duration::from_secs(40),
            2,
            "tcp",
            "refused",
        );
        inc_event(
            Category::Flap,
            Severity::Warning,
            t + Duration::from_secs(45),
            2,
            "tcp",
            "flapping",
        );
        inc_event(
            Category::Resolve,
            Severity::Warning,
            t + Duration::from_secs(50),
            2,
            "tcp",
            "back",
        );
        // 孤儿 resolve:引用从未 open 过的 (target, check) → 幂等丢弃 + 计数
        inc_event(
            Category::Resolve,
            Severity::Warning,
            t + Duration::from_secs(55),
            3,
            "grpc",
            "never opened",
        );
        tx.try_send(JournalEvent::Reset {
            at: t + Duration::from_secs(60),
            target: "api".into(),
            source: "control".into(),
        })
        .unwrap();
        drop(tx); // 关闭 → writer 返回 Ok
        writer.await.unwrap().unwrap();

        let state = replay_checked(&path).unwrap_or_default();
        assert_eq!(
            state.open_incidents.len(),
            1,
            "resolve closed it; recover-domain Reset does not clear the alert domain"
        );
        let inc = state
            .open_incidents
            .get(&("api".into(), "http".into()))
            .unwrap();
        assert_eq!(inc.incident_key, 1);
        assert_eq!(
            inc.severity,
            Severity::Critical,
            "escalate updates severity"
        );
        assert_eq!(inc.reason, "still down");
        assert_eq!(inc.last_event_at, t + Duration::from_secs(30));
        assert_eq!(state.orphan_resolves, 1, "无配对 resolve 丢弃并计数 (F25)");
        cleanup(&dir);
    }

    #[tokio::test]
    async fn reset_closes_recover_domain_synthetic_incident_gate() {
        // P2(第九轮验证):Fatal 的合成 incident (target, "__recover__") 属恢复
        // 域 —— Reset/ResetTargets 必须闭门，否则 reset 后每次重启都重放虚假
        // "latched fatal" 重通知。真实 check 的 incident 不受 Reset 影响。
        let dir = tmp_dir("recover-gate");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let (tx, writer) = spawn_test_writer(&path, 16);
        let t = base();
        tx.try_send(JournalEvent::Fatal {
            at: t,
            target: "app".into(),
            reason: "budget".into(),
        })
        .unwrap();
        tx.try_send(JournalEvent::Incident {
            at: t,
            incident: 7,
            target: "app".into(),
            check: crate::model::alert::RECOVER_CHECK.into(),
            category: Category::Alert,
            severity: Severity::Critical,
            reason: "target app latched fatal".into(),
        })
        .unwrap();
        tx.try_send(JournalEvent::Reset {
            at: t + Duration::from_secs(10),
            target: "app".into(),
            source: "control".into(),
        })
        .unwrap();
        drop(tx);
        writer.await.unwrap().unwrap();

        let state = replay_checked(&path).unwrap_or_default();
        assert!(!state.fatal.contains("app"), "reset 解除 Fatal");
        assert!(
            state.open_incidents.is_empty(),
            "恢复域合成 incident 随 reset 闭门，不再产生虚假重通知"
        );
        cleanup(&dir);
    }

    #[tokio::test]
    async fn writer_creates_missing_parent_dirs() {
        let dir = tmp_dir("mkdir");
        cleanup(&dir);
        let path = dir.join("nested/deeper/journal.jsonl");
        let (tx, writer) = spawn_test_writer(&path, 4);
        tx.send(JournalEvent::Audit {
            at: base(),
            message: "first".into(),
        })
        .await
        .unwrap();
        drop(tx);
        writer.await.unwrap().unwrap();
        assert!(path.is_file(), "父目录应被自动创建");
        cleanup(&dir);
    }

    #[test]
    fn replay_skips_bad_lines_and_empty_lines() {
        let dir = tmp_dir("badlines");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let t = base();
        let restart_line = serde_json::to_string(&JournalEvent::Restart {
            at: t,
            target: "a".into(),
            fast_exit: false,
        })
        .unwrap();
        let fatal_line = serde_json::to_string(&JournalEvent::Fatal {
            at: t,
            target: "a".into(),
            reason: "x".into(),
        })
        .unwrap();
        std::fs::write(
            &path,
            format!("{restart_line}\nnot-json-at-all\n\n{{\"type\":\"restart\"}}\n{fatal_line}\n"),
        )
        .unwrap();
        let state = replay_checked(&path).unwrap_or_default();
        assert_eq!(
            state.restarts["a"].len(),
            1,
            "bad/missing-field lines are skipped; good lines are kept"
        );
        assert!(state.fatal.contains("a"));
        cleanup(&dir);
    }

    #[test]
    fn replay_missing_file_yields_empty_state() {
        let dir = tmp_dir("missing");
        let state = replay_checked(&dir.join("nope.jsonl")).unwrap_or_default();
        assert!(state.restarts.is_empty());
        assert!(state.fatal.is_empty());
    }

    /// Reset 清空该 target 的重启计数素材;其后新 Restart 照常累积
    /// (第五轮评审:apply 此前只清 Fatal,reset 后重启 agent 旧预算复活)。
    #[test]
    fn replay_reset_clears_restart_history_and_later_restarts_apply() {
        let dir = tmp_dir("reset-restarts");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let t = base();
        let mk = |at: SystemTime, target: &str| {
            serde_json::to_string(&JournalEvent::Restart {
                at,
                target: target.into(),
                fast_exit: false,
            })
            .unwrap()
        };
        let reset = serde_json::to_string(&JournalEvent::Reset {
            at: t + Duration::from_secs(5),
            target: "a".into(),
            source: "monitorctl".into(),
        })
        .unwrap();
        std::fs::write(
            &path,
            format!(
                "{}\n{}\n{}\n{}\n",
                mk(t, "a"),
                mk(t + Duration::from_secs(1), "a"),
                reset,
                mk(t + Duration::from_secs(9), "a")
            ),
        )
        .unwrap();
        let state = replay_checked(&path).unwrap_or_default();
        assert_eq!(
            state.restarts["a"].len(),
            1,
            "Reset 前的 2 条旧重启被清，仅 Reset 后的 1 条保留"
        );
        assert_eq!(state.last_reset["a"], t + Duration::from_secs(5));
        cleanup(&dir);
    }

    // ---- 重放顺序语义:Reset 清 Fatal;其后同 target 新 Fatal 仍置位 ----
    #[tokio::test]
    async fn reset_clears_fatal_and_later_fatal_reapplies() {
        let dir = tmp_dir("reset");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let (tx, writer) = spawn_test_writer(&path, 8);
        let t = base();
        tx.send(JournalEvent::Fatal {
            at: t,
            target: "app".into(),
            reason: "old".into(),
        })
        .await
        .unwrap();
        tx.send(JournalEvent::Reset {
            at: t + Duration::from_secs(1),
            target: "app".into(),
            source: "monitorctl".into(),
        })
        .await
        .unwrap();
        tx.send(JournalEvent::Fatal {
            at: t + Duration::from_secs(2),
            target: "app".into(),
            reason: "new".into(),
        })
        .await
        .unwrap();
        tx.send(JournalEvent::Reset {
            at: t + Duration::from_secs(3),
            target: "other".into(),
            source: "monitorctl".into(),
        })
        .await
        .unwrap();
        drop(tx);
        writer.await.unwrap().unwrap();

        let state = replay_checked(&path).unwrap_or_default();
        assert!(state.fatal.contains("app"), "Reset 后的新 Fatal 必须置位");
        assert!(!state.fatal.contains("other"));
        assert_eq!(state.last_reset["app"], t + Duration::from_secs(1));
        assert_eq!(state.last_reset["other"], t + Duration::from_secs(3));

        // 只有 Reset、无后续 Fatal:Fatal 被清除
        let path2 = dir.join("journal2.jsonl");
        let (tx2, writer2) = spawn_test_writer(&path2, 8);
        tx2.send(JournalEvent::Fatal {
            at: t,
            target: "app".into(),
            reason: "old".into(),
        })
        .await
        .unwrap();
        tx2.send(JournalEvent::Reset {
            at: t,
            target: "app".into(),
            source: "monitorctl".into(),
        })
        .await
        .unwrap();
        drop(tx2);
        writer2.await.unwrap().unwrap();
        let state2 = replay_checked(&path2).unwrap_or_default();
        assert!(!state2.fatal.contains("app"), "Reset 应清除 Fatal 闩锁");
        cleanup(&dir);
    }

    // ---- F24:journal 体积上限与单代轮转 ----

    /// F24 测试助手：打开文件并起带轮转阈值的 tracked writer (产品路径)。
    fn spawn_writer_with_limit(
        path: &Path,
        max_size: u64,
    ) -> (
        JournalSender,
        tokio::task::JoinHandle<Result<()>>,
        Arc<JournalCounters>,
    ) {
        let file = open_writer(path).unwrap();
        let (sender, rx) = channel(16);
        let counters = sender.counters().clone();
        let writer = tokio::spawn(run_tracked_writer_with_limit(
            file,
            path.to_path_buf(),
            rx,
            counters.clone(),
            max_size,
        ));
        (sender, writer, counters)
    }

    /// 定长填充审计消息 (每行 ~150 字节，便于用 200 字节小阈值稳定触发轮转)。
    fn filler(i: u64) -> String {
        format!("filler-{i:060}")
    }

    fn audit(msg: String) -> JournalEvent {
        JournalEvent::Audit {
            at: base(),
            message: msg,
        }
    }

    /// 超限触发轮转:journal.1 生成且当前文件续写，轮转 audit 落盘新文件
    /// (含轮转前后字节数;前 = 上一代实际大小)。阈值 500 字节 —— 须显著
    /// 大于轮转 audit 一行 (~250 字节，含绝对路径),否则 audit 本身会立即
    /// 再次触发轮转 (行为正确但断言的是退化形态，见 rotate_file doc)。
    #[tokio::test]
    async fn size_limit_triggers_rotation_and_current_file_continues() {
        let dir = tmp_dir("rotate-basic");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let (sender, writer, _counters) = spawn_writer_with_limit(&path, 500);
        for i in 0..5 {
            sender.send(audit(filler(i))).await.unwrap();
        }
        drop(sender);
        writer.await.unwrap().unwrap();

        let gen1 = gen1_path(&path);
        assert!(gen1.is_file(), "超限必须产生上一代 journal.1.jsonl");
        let old = std::fs::read_to_string(&gen1).unwrap();
        assert!(
            old.contains(&filler(0)) && old.contains(&filler(3)),
            "前四条事件留在上一代"
        );
        assert!(!old.contains(&filler(4)), "轮转后的事件写当前文件");

        // audit 落盘**新文件**首行，含轮转前后字节数
        let cur = std::fs::read_to_string(&path).unwrap();
        let cur_lines: Vec<&str> = cur.lines().collect();
        assert_eq!(cur_lines.len(), 2, "当前文件 = 轮转 audit + 1 条续写事件");
        let first: JournalEvent = serde_json::from_str(cur_lines[0]).unwrap();
        match first {
            JournalEvent::Audit { message, .. } => {
                assert!(
                    message.starts_with("journal rotated: "),
                    "轮转必须向新文件写 audit: {message}"
                );
                assert!(
                    message.contains(&format!(
                        "{} bytes",
                        std::fs::metadata(&gen1).unwrap().len()
                    )),
                    "audit 含轮转前字节数 (上一代实际大小): {message}"
                );
                assert!(
                    message.contains("(0 bytes)"),
                    "audit 含轮转后字节数：{message}"
                );
            }
            _ => panic!("新文件首行必须是轮转 audit"),
        }
        assert!(cur.contains(&filler(4)), "当前文件续写轮转后的事件");
        cleanup(&dir);
    }

    /// 二次轮转覆盖旧 journal.1 (只保留一代;首代事件随覆盖丢弃，属预期取舍)。
    #[tokio::test]
    async fn second_rotation_overwrites_previous_generation() {
        let dir = tmp_dir("rotate-twice");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let (sender, writer, _counters) = spawn_writer_with_limit(&path, 500);
        for i in 0..10 {
            sender.send(audit(filler(i))).await.unwrap();
        }
        drop(sender);
        writer.await.unwrap().unwrap();

        let gen1 = gen1_path(&path);
        assert!(gen1.is_file());
        let old = std::fs::read_to_string(&gen1).unwrap();
        assert!(
            !old.contains(&filler(0)),
            "二次轮转必须覆盖旧 journal.1 (首代事件被丢弃)"
        );
        assert!(old.contains(&filler(9)), "上一代 = 最近一次轮转的内容");
        let cur = std::fs::read_to_string(&path).unwrap();
        assert!(!cur.contains(&filler(0)));
        assert_eq!(cur.lines().count(), 1, "当前文件只剩最新一轮转 audit");
        assert!(replay_checked(&path).is_ok(), "多代重放不报错");
        cleanup(&dir);
    }

    /// `journal_max_size = 0` 完全禁用轮转:journal.1 永不产生。
    #[tokio::test]
    async fn max_size_zero_disables_rotation() {
        let dir = tmp_dir("rotate-disabled");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let (sender, writer, _counters) = spawn_writer_with_limit(&path, 0);
        for i in 0..6 {
            sender.send(audit(filler(i))).await.unwrap();
        }
        drop(sender);
        writer.await.unwrap().unwrap();

        assert!(!gen1_path(&path).exists(), "max_size = 0 必须完全不轮转");
        let cur = std::fs::read_to_string(&path).unwrap();
        for i in 0..6 {
            assert!(cur.contains(&filler(i)), "全部事件留在单文件");
        }
        assert_eq!(cur.lines().count(), 6);
        cleanup(&dir);
    }

    /// Fatal 写入 → 轮转 → 重启重放仍 Fatal (跨轮转闩锁契约)。
    #[tokio::test]
    async fn fatal_before_rotation_survives_replay() {
        let dir = tmp_dir("rotate-fatal");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let (sender, writer, _counters) = spawn_writer_with_limit(&path, 500);
        sender
            .send(JournalEvent::Fatal {
                at: base(),
                target: "app".into(),
                reason: "budget".into(),
            })
            .await
            .unwrap();
        for i in 0..4 {
            sender.send(audit(filler(i))).await.unwrap();
        }
        drop(sender);
        writer.await.unwrap().unwrap();

        let gen1 = gen1_path(&path);
        assert!(gen1.is_file(), "Fatal 之后超限仍应轮转");
        assert!(
            std::fs::read_to_string(&gen1)
                .unwrap()
                .contains("\"fatal\""),
            "Fatal 记录在上一代文件"
        );
        let state = replay_checked(&path).unwrap();
        assert!(state.fatal.contains("app"), "Fatal 跨轮转重放保持");
        cleanup(&dir);
    }

    /// 上一代 journal.1 中的未决 SpawnIntent 重放后仍阻塞同名冷启动
    /// (pending_spawns 非空;崩溃启动事务语义跨轮转保持)。
    #[tokio::test]
    async fn pending_spawn_in_rotated_generation_blocks_cold_start_semantics() {
        let dir = tmp_dir("rotate-spawn-intent");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let (sender, writer, _counters) = spawn_writer_with_limit(&path, 500);
        sender
            .send(JournalEvent::SpawnIntent {
                at: base(),
                target: "app".into(),
                config_digest: "digest-1".into(),
            })
            .await
            .unwrap();
        for i in 0..4 {
            sender.send(audit(filler(i))).await.unwrap();
        }
        drop(sender);
        writer.await.unwrap().unwrap();

        let state = replay_checked(&path).unwrap();
        assert_eq!(
            state.pending_spawns.len(),
            1,
            "journal.1 内的未决 SpawnIntent 重放后必须保留"
        );
        assert_eq!(state.pending_spawns["app"].config_digest, "digest-1");
        cleanup(&dir);
    }

    /// 轮转后坏行/半行隔离行为不回归：当前文件的崩溃半行由 open_writer
    /// 划界，重放跳过坏行，后续已确认记录可重放 (且上一代 Fatal 保持)。
    #[tokio::test]
    async fn bad_line_after_rotation_is_isolated_and_replay_continues() {
        let dir = tmp_dir("rotate-badline");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let (sender, writer, _counters) = spawn_writer_with_limit(&path, 500);
        sender
            .send(JournalEvent::Fatal {
                at: base(),
                target: "app".into(),
                reason: "budget".into(),
            })
            .await
            .unwrap();
        for i in 0..4 {
            sender.send(audit(filler(i))).await.unwrap();
        }
        drop(sender);
        writer.await.unwrap().unwrap();

        // 模拟崩溃半行：当前文件尾部追加无换行半行
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(b"{\"type\":\"audi");
        std::fs::write(&path, &bytes).unwrap();

        // 重启 writer:open_writer 划定边界，后续 Fatal 正常落盘
        let (sender, writer, _counters) = spawn_writer_with_limit(&path, 0);
        sender
            .send(JournalEvent::Fatal {
                at: base(),
                target: "app2".into(),
                reason: "budget".into(),
            })
            .await
            .unwrap();
        drop(sender);
        writer.await.unwrap().unwrap();

        let state = replay_checked(&path).unwrap();
        assert!(state.fatal.contains("app"), "上一代 Fatal 保持");
        assert!(
            state.fatal.contains("app2"),
            "半行定界后新 Fatal 可重放 (坏行隔离不回归)"
        );
        cleanup(&dir);
    }

    /// 改名失败降级：保留当前文件继续写，不停机、不丢事件、不误报持久化
    /// 故障 (体积上限是运维目标不是安全边界)。构造方式:journal.1 路径
    /// 预置为目录，remove_file 必然失败 → 轮转失败走降级分支。
    #[tokio::test]
    async fn rotation_failure_degrades_to_keep_writing() {
        let dir = tmp_dir("rotate-degrade");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let gen1 = gen1_path(&path);
        std::fs::create_dir_all(&gen1).unwrap(); // 使轮转的删除/改名必然失败
        let (sender, writer, counters) = spawn_writer_with_limit(&path, 500);
        sender
            .send(JournalEvent::Fatal {
                at: base(),
                target: "app".into(),
                reason: "budget".into(),
            })
            .await
            .unwrap();
        for i in 0..4 {
            sender.send(audit(filler(i))).await.unwrap();
        }
        drop(sender);
        writer.await.unwrap().unwrap();

        let stats = counters.snapshot();
        assert_eq!(stats.write_successes, 5, "降级路径不停机:全部事件照常落盘");
        assert_eq!(stats.write_failures, 0, "轮转失败不算写失败");
        assert!(!counters.has_failed(), "降级不得触发持久化故障闩锁");
        let cur = std::fs::read_to_string(&path).unwrap();
        for i in 0..4 {
            assert!(cur.contains(&filler(i)), "当前文件保留并继续增长");
        }
        assert!(gen1.is_dir(), "降级路径不得破坏现场");
        cleanup(&dir);
    }
}
