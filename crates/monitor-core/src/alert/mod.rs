//! 告警分发 (DESIGN.md §7.2、§7.3)。
//!
//! - drop-oldest 有界队列 (`AlertQueue`):满则丢最旧 + 审计计数;
//! - 每笔投递 timeout + 指数退避重试;最终失败记 error + 计数;
//! - deadman 心跳 (可选)。

// webhook 通知器依赖 reqwest(http-client feature);无 C 依赖的交叉编译
// feature 组合 (DESIGN §6.5) 下不参与编译。spool 不依赖 reqwest，恒参与编译。
// 指标周期推送依赖 reqwest(http-client feature),与 webhook 同款门控。
#[cfg(feature = "http-client")]
pub mod metrics_push;
pub mod spool;
#[cfg(feature = "http-client")]
pub mod webhook;

use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

use crate::config::AlertCfg;
use crate::error::Result;
use crate::model::alert::AlertEvent;
use crate::model::control::MetricsReport;

/// dyn 兼容通知器 (edition 2024 下原生 AFIT 非 dyn 兼容，统一 `#[async_trait]`)。
#[async_trait]
pub trait Notifier: Send + Sync {
    async fn notify(&self, event: &AlertEvent) -> Result<()>;
}

/// 内部计数 (/metrics 暴露)。
#[derive(Debug, Default)]
pub struct AlertCounters {
    pub sent: AtomicU64,
    pub delivery_failures: AtomicU64,
    pub dropped: AtomicU64,
}

impl AlertCounters {
    pub fn to_metrics(&self) -> MetricsReport {
        MetricsReport {
            alerts_sent: self.sent.load(Ordering::Relaxed),
            alert_delivery_failures: self.delivery_failures.load(Ordering::Relaxed),
            alerts_dropped: self.dropped.load(Ordering::Relaxed),
            ..Default::default()
        }
    }
}

/// drop-oldest 有界队列:eval 侧 push(满丢最旧 + dropped 计数),分发任务侧 pop。
#[derive(Debug)]
pub struct AlertQueue {
    inner: Mutex<std::collections::VecDeque<AlertEvent>>,
    cap: usize,
    notify: tokio::sync::Notify,
    counters: Arc<AlertCounters>,
}

pub const ALERT_QUEUE_CAP: usize = 256;

impl AlertQueue {
    pub fn new(counters: Arc<AlertCounters>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(std::collections::VecDeque::with_capacity(ALERT_QUEUE_CAP)),
            cap: ALERT_QUEUE_CAP,
            notify: tokio::sync::Notify::new(),
            counters,
        })
    }

    /// 满则丢最旧并计数 (审计日志由调用方或本方法 tracing 输出)。
    /// 锁中毒用 `into_inner()` 恢复：锁内仅 VecDeque 操作，无结构不变量可破坏。
    pub fn push(self: &Arc<Self>, event: AlertEvent) {
        let dropped = {
            let mut q = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let dropped = if q.len() >= self.cap {
                q.pop_front()
            } else {
                None
            };
            q.push_back(event);
            dropped
        };
        if let Some(event) = dropped {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(target = %event.target, check = %event.check, "alert queue full: dropped oldest");
        }
        self.notify.notify_one();
    }

    /// 队列当前长度 (停机审计用)。
    pub fn len(&self) -> usize {
        self.len_inner()
    }

    /// 队列是否为空。
    pub fn is_empty(&self) -> bool {
        self.len_inner() == 0
    }

    fn len_inner(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// 阻塞取一个事件;shutdown 信号为 true 时返回 None。
    ///
    /// shutdown sender 被 drop(`changed()` 返回 Err) 不算"继续等":视同上游已
    /// 终结——排空队列后退出，避免 select 的 Err 臂恒就绪造成忙转 (库形态下
    /// runtime 之外的宿主可能长期驻留)。
    pub async fn pop(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> Option<AlertEvent> {
        let mut sender_dropped = false;
        loop {
            let maybe = {
                let mut q = self
                    .inner
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                q.pop_front()
            };
            if let Some(ev) = maybe {
                return Some(ev);
            }
            let shutdown_now = *shutdown.borrow_and_update();
            if shutdown_now {
                return None;
            }
            if sender_dropped {
                // 上游已终结且队列空：不可能再有新事件。
                return None;
            }
            tokio::select! {
                _ = self.notify.notified() => {}
                r = shutdown.changed() => {
                    if r.is_err() {
                        sender_dropped = true; // 下一轮：排空即退
                    }
                }
            }
        }
    }
}

/// 重试退避基数 (1s × 2^n,DESIGN §7.3)。
const BACKOFF_BASE: std::time::Duration = std::time::Duration::from_secs(1);
/// 退避封顶。
const BACKOFF_CAP: std::time::Duration = std::time::Duration::from_secs(10);
/// deadman 心跳间隔 (DESIGN §7.3:30s 周期 POST)。
#[cfg(feature = "notify-webhook")]
const DEADMAN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// deadman 心跳任务 (由 runtime 启动**一次**;dispatcher panic 重建不重复 ——
/// 评审 P2:多次重建会累积并行心跳)。30s 周期 POST `{}`;失败仅 WARN。
#[cfg(feature = "notify-webhook")]
pub async fn run_deadman(url: url::Url, mut shutdown: watch::Receiver<bool>) {
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(source = %e, "deadman client build failed; heartbeat disabled");
            return;
        }
    };
    let mut interval = tokio::time::interval(DEADMAN_INTERVAL);
    loop {
        if *shutdown.borrow() {
            return;
        }
        tokio::select! {
            _ = interval.tick() => {
                let result = client.post(url.clone()).json(&serde_json::json!({})).send().await;
                match result.and_then(reqwest::Response::error_for_status) {
                    Ok(_) => {}
                    Err(e) => tracing::warn!(source = %e, "deadman heartbeat failed"),
                }
            }
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow_and_update() { return; }
            }
        }
    }
}

/// 分发任务：消费 [`AlertQueue`],带超时与重试投递;deadman 由 runtime 独立启动。
/// `spool_dir`:`Some(dir)` 时，重试耗尽的失败事件落盘 (F12 at-least-once),
/// 启动及每隔 [`spool::SPOOL_REQUEUE_INTERVAL`] 直接重试磁盘事件，投递成功后确认移除。
///
/// - `queue.pop` 返回 `None`(shutdown 置位) 即退出;退出前会把当前事件投递完
///   (重试循环不响应 shutdown，保证停机前的最后尽力投递)。
/// - 每笔投递：最多 `cfg.retries` 次重试 (共 `retries + 1` 次尝试),指数退避
///   1s×2^n 封顶 10s;单次尝试整体受 `cfg.timeout` 约束 (webhook client 内部
///   亦有超时，这里是分发层的兜底)。
/// - 成功 → `counters.sent + 1`;全部失败 → `counters.delivery_failures + 1`
///   + `ERROR` 审计日志 (含 incident/target/check/reason)。
/// - deadman:独立任务每 30s POST `{}`,失败仅 `WARN`,绝不阻塞分发主循环。
pub async fn run_dispatcher(
    queue: Arc<AlertQueue>,
    notifier: Arc<dyn Notifier>,
    cfg: AlertCfg,
    counters: Arc<AlertCounters>,
    shutdown: watch::Receiver<bool>,
    spool_dir: Option<std::path::PathBuf>,
) {
    let mut shutdown = shutdown;
    // 持久事件直接投递并等待成功确认，全程保留磁盘副本。
    // 启动和后续周期均走同一路径，避免回队后 drop-oldest/崩溃丢失。
    if let Some(dir) = &spool_dir
        && !*shutdown.borrow()
    {
        replay_spool(dir, &notifier, &cfg, &counters).await;
    }
    // spool 周期重放节拍 (有 spool 目录时;首拍延后一个周期 —— interval 的
    // 立即首拍必然空跑，白占一个 select 轮次且缩短首个重放延迟语义)
    let requeue = spool_dir.is_some();
    let mut requeue_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + spool::SPOOL_REQUEUE_INTERVAL,
        spool::SPOOL_REQUEUE_INTERVAL,
    );
    requeue_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let next_event = if requeue {
            tokio::select! {
                ev = queue.clone().pop(shutdown.clone()) => ev,
                _ = requeue_tick.tick(), if !*shutdown.borrow_and_update() => {
                    if let Some(dir) = &spool_dir {
                        replay_spool(dir, &notifier, &cfg, &counters).await;
                    }
                    continue;
                }
            }
        } else {
            queue.clone().pop(shutdown.clone()).await
        };
        let Some(event) = next_event else {
            return; // shutdown 置位且队列已空
        };
        let delivered = deliver(&event, &notifier, &cfg, &counters).await;
        // F12 at-least-once:重试耗尽 → spool 落盘，由周期重放/启动重放接力
        if !delivered && let Some(dir) = &spool_dir {
            let spooled = spool::append(dir, &event).await;
            if !spooled {
                tracing::error!(
                    incident = event.incident,
                    target = %event.target,
                    check = %event.check,
                    "alert spool write failed; event lost from retry path (still in journal)"
                );
            }
        }
    }
}

async fn replay_spool(
    directory: &std::path::Path,
    notifier: &Arc<dyn Notifier>,
    cfg: &AlertCfg,
    counters: &AlertCounters,
) {
    spool::replay_deliver(
        directory,
        std::time::SystemTime::now(),
        spool::SPOOL_REQUEUE_BATCH,
        |event| async move { deliver(&event, notifier, cfg, counters).await },
    )
    .await;
}

/// 单笔投递 (含全部重试)。成功返回 true。
async fn deliver(
    event: &AlertEvent,
    notifier: &Arc<dyn Notifier>,
    cfg: &AlertCfg,
    counters: &AlertCounters,
) -> bool {
    let attempts = cfg.retries.saturating_add(1); // 首次 + retries 次重试
    let mut last_error = String::from("no attempt made");
    for attempt in 0..attempts {
        if attempt > 0 {
            // 指数退避：第 n 次重试前睡 1s × 2^(n-1),封顶 10s。
            let shift = (attempt - 1).min(4); // 2^4 = 16 → 已超 10s 封顶
            let delay = BACKOFF_BASE.saturating_mul(1u32 << shift).min(BACKOFF_CAP);
            tokio::time::sleep(delay).await;
        }
        // panic 隔离 (评审 P2):单次投递在独立任务执行 —— notifier panic 在
        // 任务边界变为 JoinError::is_panic，不毒化分发任务，按失败走重试路径。
        let ev = event.clone();
        let notifier = notifier.clone();
        let timeout = cfg.timeout;
        // JoinSet 在分发任务被取消时同时中止在途通知器，避免脱离宿主继续运行。
        let mut attempt_task = tokio::task::JoinSet::new();
        attempt_task
            .spawn(async move { tokio::time::timeout(timeout, notifier.notify(&ev)).await });
        let joined = attempt_task
            .join_next()
            .await
            .expect("one delivery task registered");

        match joined {
            Err(join_err) if join_err.is_panic() => {
                tracing::error!(
                    incident = event.incident,
                    target = %event.target,
                    check = %event.check,
                    summary = %join_err,
                    "notifier panicked during delivery"
                );
                last_error = "notifier panicked".to_string();
                continue;
            }
            Err(join_err) => {
                last_error = format!("delivery task failed: {join_err}");
                continue;
            }
            Ok(Ok(Ok(()))) => {
                counters.sent.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    incident = event.incident,
                    target = %event.target,
                    check = %event.check,
                    attempt = attempt,
                    "alert delivered"
                );
                return true;
            }
            Ok(Err(_)) => {
                // timeout 臂：单次尝试超时 (退避后重试)
                last_error = format!("delivery timed out after {:?}", cfg.timeout);
                continue;
            }
            Ok(Ok(Err(e))) => {
                last_error = e.to_string();
                tracing::debug!(
                    incident = event.incident,
                    target = %event.target,
                    check = %event.check,
                    attempt = attempt,
                    error = %e,
                    "alert delivery attempt failed"
                );
            }
        }
    }
    counters.delivery_failures.fetch_add(1, Ordering::Relaxed);
    tracing::error!(
        incident = event.incident,
        target = %event.target,
        check = %event.check,
        reason = %last_error,
        attempts = attempts,
        "alert delivery failed after all retries"
    );
    false
}

// ---------------------------------------------------------------------------
// 测试 (paused 虚拟时间：重试退避不消耗真实时间)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::alert::{Category, Severity};
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::SystemTime;

    /// drop-oldest:队列满时丢最旧并计数 (F5 验收:"队列满丢最旧有审计")。
    #[test]
    fn queue_full_drops_oldest_and_counts() {
        let counters = Arc::new(AlertCounters::default());
        let queue = AlertQueue::new(counters.clone());
        for i in 0..(ALERT_QUEUE_CAP as u64 + 1) {
            queue.push(sample_event_seq(i));
        }
        assert_eq!(
            counters.dropped.load(Ordering::Relaxed),
            1,
            "超出容量 1 个：必须丢 1 个最旧"
        );
        // 同步断言内部顺序：首个事件应是 occurrences=2(最旧的 1 被丢)
        let front = queue.inner.lock().unwrap().front().cloned();
        assert_eq!(
            front.map(|e| e.occurrences),
            Some(2),
            "最旧事件 (occurrences=1) 必须被丢，队首应为 occurrences=2"
        );
        // pop 路径：上游终结 (sender drop) 后排空队列再退出 (不忙转、不丢中间事件)
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (tx, rx) = tokio::sync::watch::channel(false);
        drop(tx);
        let first = rt.block_on(queue.clone().pop(rx));
        assert_eq!(first.map(|e| e.occurrences), Some(2), "pop 首个 = 队首");
        let rest: Vec<u64> = std::iter::from_fn(|| {
            rt.block_on(async { queue.clone().pop(tokio::sync::watch::channel(true).1).await })
        })
        .take(ALERT_QUEUE_CAP)
        .map(|e| e.occurrences)
        .collect();
        assert_eq!(rest.len(), ALERT_QUEUE_CAP - 1, "其余事件全部可取出");
        assert_eq!(
            *rest.last().unwrap(),
            ALERT_QUEUE_CAP as u64 + 1,
            "最新事件保留"
        );
    }

    #[test]
    fn concurrent_push_preserves_capacity_and_drop_count() {
        let counters = Arc::new(AlertCounters::default());
        let queue = AlertQueue::new(counters.clone());
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let queue = queue.clone();
                scope.spawn(move || {
                    for i in 0..1000 {
                        queue.push(sample_event_seq(i));
                    }
                });
            }
        });
        assert_eq!(queue.len(), ALERT_QUEUE_CAP);
        assert_eq!(
            counters.dropped.load(Ordering::Relaxed),
            8000 - ALERT_QUEUE_CAP as u64
        );
    }

    /// F12 集成：重试耗尽 → 事件落盘 spool(以真实 fs 断言 at-least-once 闭环)。
    #[tokio::test]
    async fn delivery_exhaustion_spools_event() {
        let dir = std::env::temp_dir().join(format!(
            "sw-spool-dispatch-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let counters = Arc::new(AlertCounters::default());
        let queue = AlertQueue::new(counters.clone());
        // 脚本耗尽返回 Ok:塞一个 Err 保证首次投递失败 (retries=0 → 一次机会)
        let notifier = MockNotifier {
            script: Mutex::new(VecDeque::from(vec![Err("boom".into())])),
            calls: Default::default(),
        };
        let cfg = AlertCfg {
            retries: 0,
            ..Default::default()
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run_dispatcher(
            queue.clone(),
            Arc::new(notifier) as Arc<dyn Notifier>,
            cfg,
            counters.clone(),
            shutdown_rx,
            Some(dir.clone()),
        ));
        // 事件用新鲜时间戳 (UNIX_EPOCH 的事件会被重放的 7 天过期策略正确丢弃 ——
        // 那条路径由 spool 单测覆盖;此处验证 at-least-once 闭环本身)。
        let mut ev = sample_event_seq(9);
        ev.last_seen = SystemTime::now();
        ev.first_seen = ev.last_seen;
        queue.push(ev);
        // 失败 → 落盘 (以**内容非空**为准:create(true) 先建文件，写入稍后)
        let mut spooled = false;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let non_empty = std::fs::read_to_string(dir.join(crate::alert::spool::SPOOL_FILE))
                .map(|c| !c.trim().is_empty())
                .unwrap_or(false);
            if non_empty {
                spooled = true;
                break;
            }
        }
        assert!(spooled, "投递耗尽后事件必须落盘");
        assert_eq!(counters.delivery_failures.load(Ordering::Relaxed), 1);

        // 重建 dispatcher 模拟 agent 重启，启动重放直接投递，成功后确认移除。
        shutdown_tx.send(true).ok();
        let _ = task.await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run_dispatcher(
            queue.clone(),
            MockNotifier::always_ok(),
            AlertCfg {
                retries: 0,
                ..Default::default()
            },
            counters.clone(),
            shutdown_rx,
            Some(dir.clone()),
        ));
        for _ in 0..50 {
            if counters.sent.load(Ordering::Relaxed) == 1 && !dir.join(spool::SPOOL_FILE).exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(counters.sent.load(Ordering::Relaxed), 1);
        assert!(!dir.join(spool::SPOOL_FILE).exists(), "成功投递后确认移除");
        shutdown_tx.send(true).ok();
        let _ = task.await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 构造区分性事件 (occurrences 递增;与既有 sample_event 区分)。
    fn sample_event_seq(i: u64) -> AlertEvent {
        AlertEvent {
            incident: 1,
            category: Category::Alert,
            severity: Severity::Warning,
            target: "t".into(),
            check: "c".into(),
            reason: "x".into(),
            observed: None,
            first_seen: SystemTime::UNIX_EPOCH,
            last_seen: SystemTime::UNIX_EPOCH,
            occurrences: i + 1,
        }
    }

    /// 可脚本化行为的 Notifier 假实现：按脚本逐个返回;脚本耗尽后返回 Ok。
    struct MockNotifier {
        script: Mutex<VecDeque<std::result::Result<(), String>>>,
        calls: std::sync::atomic::AtomicU64,
    }

    impl MockNotifier {
        fn always_ok() -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(VecDeque::new()),
                calls: Default::default(),
            })
        }
        fn scripted(outcomes: &[&str]) -> Arc<Self> {
            let script = outcomes
                .iter()
                .map(|&o| {
                    if o == "ok" {
                        std::result::Result::Ok::<(), String>(())
                    } else {
                        Err(o.to_string())
                    }
                })
                .collect();
            Arc::new(Self {
                script: Mutex::new(script),
                calls: Default::default(),
            })
        }
        fn calls(&self) -> u64 {
            self.calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl Notifier for MockNotifier {
        async fn notify(&self, _event: &AlertEvent) -> Result<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let next = self
                .script
                .lock()
                .expect("script lock poisoned")
                .pop_front();
            match next {
                Some(Ok(())) | None => Ok(()),
                Some(Err(msg)) => Err(crate::error::CoreError::Alert(msg)),
            }
        }
    }

    fn sample_event() -> AlertEvent {
        AlertEvent {
            incident: AlertEvent::incident_key("t", "c"),
            category: Category::Alert,
            severity: Severity::Warning,
            target: "t".into(),
            check: "c".into(),
            reason: "probe failed".into(),
            observed: None,
            first_seen: SystemTime::UNIX_EPOCH,
            last_seen: SystemTime::UNIX_EPOCH,
            occurrences: 1,
        }
    }

    /// current-thread + paused 时钟：退避 sleep 由虚拟时间自动推进。
    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .start_paused(true)
            .enable_all()
            .build()
            .expect("runtime")
    }

    /// 轮询等待条件成立 (paused 时钟下 sleep 自动推进，喂出被阻塞的任务)。
    async fn wait_for(cond: impl Fn() -> bool) {
        for _ in 0..5000 {
            if cond() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("condition not reached within virtual time budget");
    }

    #[test]
    fn dispatcher_delivers_and_counts_sent() {
        rt().block_on(async {
            let counters = Arc::new(AlertCounters::default());
            let queue = AlertQueue::new(counters.clone());
            let notifier = MockNotifier::always_ok();
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let task = tokio::spawn(run_dispatcher(
                queue.clone(),
                notifier.clone(),
                AlertCfg::default(),
                counters.clone(),
                shutdown_rx,
                None,
            ));
            queue.push(sample_event());
            wait_for(|| counters.sent.load(Ordering::Relaxed) == 1).await;
            assert_eq!(notifier.calls(), 1);
            assert_eq!(counters.delivery_failures.load(Ordering::Relaxed), 0);

            shutdown_tx.send(true).expect("shutdown send");
            wait_for(|| task.is_finished()).await;
        });
    }

    #[test]
    fn dispatcher_exits_on_shutdown_with_empty_queue() {
        rt().block_on(async {
            let counters = Arc::new(AlertCounters::default());
            let queue = AlertQueue::new(counters.clone());
            let notifier = MockNotifier::always_ok();
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let task = tokio::spawn(run_dispatcher(
                queue.clone(),
                notifier.clone(),
                AlertCfg::default(),
                counters.clone(),
                shutdown_rx,
                None,
            ));
            // 无事件:shutdown 置位后 pop 返回 None，任务退出。
            shutdown_tx.send(true).expect("shutdown send");
            wait_for(|| task.is_finished()).await;
            assert_eq!(notifier.calls(), 0);
        });
    }

    #[test]
    fn dispatcher_retries_with_backoff_then_fails() {
        rt().block_on(async {
            let counters = Arc::new(AlertCounters::default());
            let queue = AlertQueue::new(counters.clone());
            // 恒失败;retries=2 → 共 3 次尝试 (1 + 2 次重试)。
            let notifier = MockNotifier::scripted(&["boom", "boom", "boom", "boom"]);
            let cfg = AlertCfg {
                retries: 2,
                timeout: std::time::Duration::from_millis(50),
                ..Default::default()
            };
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let task = tokio::spawn(run_dispatcher(
                queue.clone(),
                notifier.clone(),
                cfg,
                counters.clone(),
                shutdown_rx,
                None,
            ));
            queue.push(sample_event());
            wait_for(|| counters.delivery_failures.load(Ordering::Relaxed) == 1).await;
            assert_eq!(notifier.calls(), 3, "首次尝试 + 2 次重试");
            assert_eq!(counters.sent.load(Ordering::Relaxed), 0);

            shutdown_tx.send(true).expect("shutdown send");
            wait_for(|| task.is_finished()).await;
        });
    }

    #[test]
    fn dispatcher_retry_succeeds_after_transient_failures() {
        rt().block_on(async {
            let counters = Arc::new(AlertCounters::default());
            let queue = AlertQueue::new(counters.clone());
            let notifier = MockNotifier::scripted(&["err1", "err2", "ok"]);
            let cfg = AlertCfg {
                retries: 3,
                timeout: std::time::Duration::from_millis(50),
                ..Default::default()
            };
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let task = tokio::spawn(run_dispatcher(
                queue.clone(),
                notifier.clone(),
                cfg,
                counters.clone(),
                shutdown_rx,
                None,
            ));
            queue.push(sample_event());
            wait_for(|| counters.sent.load(Ordering::Relaxed) == 1).await;
            assert_eq!(notifier.calls(), 3, "两次失败后第三次成功");
            assert_eq!(counters.delivery_failures.load(Ordering::Relaxed), 0);

            shutdown_tx.send(true).expect("shutdown send");
            wait_for(|| task.is_finished()).await;
        });
    }

    /// deadman 已配置时不影响分发主循环 (心跳目标不可达仅记 WARN)。
    #[cfg(feature = "notify-webhook")]
    #[test]
    fn deadman_configured_does_not_block_dispatch() {
        rt().block_on(async {
            let counters = Arc::new(AlertCounters::default());
            let queue = AlertQueue::new(counters.clone());
            let notifier = MockNotifier::always_ok();
            let cfg = AlertCfg {
                deadman_url: Some("http://127.0.0.1:9/".parse().expect("url")),
                ..Default::default()
            };
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let task = tokio::spawn(run_dispatcher(
                queue.clone(),
                notifier.clone(),
                cfg,
                counters.clone(),
                shutdown_rx,
                None,
            ));
            queue.push(sample_event());
            wait_for(|| counters.sent.load(Ordering::Relaxed) == 1).await;

            shutdown_tx.send(true).expect("shutdown send");
            wait_for(|| task.is_finished()).await;
        });
    }
}
