//! 告警 spool 落盘 (ROADMAP F12,at-least-once)。
//!
//! 失败事件追加到 `{data_dir}/spool/pending.jsonl`,启动/周期重试时始终保留
//! 磁盘副本，收到通知器成功确认后再原子更新文件。进程在确认前崩溃会重复投递。
//! 超过 [`SPOOL_MAX_AGE`] 的事件按保留期丢弃，坏行原样保留并记 WARN。
//!
//! 同目录追加和重放通过共享互斥串行执行。同步磁盘事务持有互斥许可，
//! 外层任务取消后仍保留许可直到系统调用完成，防止晚到的 rename 覆盖新记录。
//! I/O 错误保留原文件并记录日志，追加成功包含 flush + sync_data。

use std::collections::HashMap;
use std::io::{Read as _, Seek as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, SystemTime};

use crate::model::alert::AlertEvent;

/// spool 单文件名 (目录由 runtime 传入:`{data_dir}/spool`)。
pub const SPOOL_FILE: &str = "pending.jsonl";
/// 确认事务的 rename 中转名 (replace_pending 临时文件)。与 [`SPOOL_FILE`]
/// 一同构成 spool 目录的保留路径面 (config 校验的 history_path 碰撞守卫
/// 单一事实源，第十九轮评审 P3)。
pub const SPOOL_TEMP_FILE: &str = "pending.jsonl.next";
/// spool 事件最长保留期 (重放时超龄丢弃)。
pub const SPOOL_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 3600);
/// 周期重放的节拍 (分发任务内)。
pub const SPOOL_REQUEUE_INTERVAL: Duration = Duration::from_secs(60);
/// 每次启动/周期重放最多尝试的事件数 (限制单轮积压处理量)。
pub const SPOOL_REQUEUE_BATCH: usize = 16;

/// 同一进程内、同一路径的运行实例共享许可，Weak 条目在后续调用时清理。
async fn directory_guard(directory: &Path) -> std::io::Result<tokio::sync::OwnedMutexGuard<()>> {
    type DirectoryLocks = Mutex<HashMap<PathBuf, Weak<tokio::sync::Mutex<()>>>>;
    static LOCKS: OnceLock<DirectoryLocks> = OnceLock::new();
    let directory = directory.to_path_buf();
    let key = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&directory)?;
        std::fs::canonicalize(directory)
    })
    .await
    .map_err(std::io::Error::other)??;
    let lock = {
        let mut locks = LOCKS
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
            lock
        } else {
            let lock = Arc::new(tokio::sync::Mutex::new(()));
            locks.insert(key, Arc::downgrade(&lock));
            lock
        }
    };
    Ok(lock.lock_owned().await)
}

/// 追加一条事件到 spool 文件，成功包含 sync_data 确认。
pub async fn append(spool_dir: &Path, event: &AlertEvent) -> bool {
    let mut line = match serde_json::to_vec(event) {
        Ok(line) => line,
        Err(e) => {
            tracing::warn!(target = %event.target, source = %e, "spool: 事件序列化失败");
            return false;
        }
    };
    line.push(b'\n');
    let guard = match directory_guard(spool_dir).await {
        Ok(guard) => guard,
        Err(e) => {
            tracing::warn!(source = %e, "spool: 获取目录许可失败");
            return false;
        }
    };
    let directory = spool_dir.to_path_buf();
    let result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let _guard = guard;
        std::fs::create_dir_all(&directory)?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(directory.join(SPOOL_FILE))?;
        // 隔离中断写入的尾行，后续完整事件保持独立可解析。
        if file.metadata()?.len() > 0 {
            file.seek(std::io::SeekFrom::End(-1))?;
            let mut last = [0_u8; 1];
            file.read_exact(&mut last)?;
            if last[0] != b'\n' {
                file.write_all(b"\n")?;
            }
        }
        file.write_all(&line)?;
        file.flush()?;
        file.sync_data()
    })
    .await;
    match result {
        Ok(Ok(())) => {
            tracing::info!(incident = event.incident, target = %event.target, check = %event.check, "alert spooled for retry (delivery exhausted)");
            true
        }
        failure => {
            tracing::warn!(path = %spool_dir.display(), result = ?failure, "spool: 追加持久化失败");
            false
        }
    }
}

/// 重放结果。
#[derive(Debug, Default, Clone, Copy)]
pub struct ReplayStats {
    /// 成功确认的事件数量 (兼容原回队 helper 的字段名)。
    pub requeued: usize,
    pub attempted: usize,
    pub failed: usize,
    pub expired: usize,
    pub corrupt_lines: usize,
}

/// 将 spool 的所有权交给同步回调。调用方须在返回前可靠地接管事件。
/// 产品分发路径使用 [`replay_deliver`],其成功确认发生在通知器完成之后。
/// `max_events` 为本次尝试数上限，0 表示全部。
pub async fn replay(
    spool_dir: &Path,
    now: SystemTime,
    max_events: usize,
    mut push: impl FnMut(AlertEvent),
) -> ReplayStats {
    replay_deliver(spool_dir, now, max_events, |event| {
        push(event);
        std::future::ready(true)
    })
    .await
}

/// 直接重试磁盘事件，回调返回 true 才确认移除。false 保留原行供下次重试。
/// 文件在全部异步回调结束前保持原样，取消或崩溃会保留未确认磁盘副本。
/// 持有同目录许可直到本轮确认完成，回调不得再次调用同目录 spool 操作。
pub async fn replay_deliver<F, Fut>(
    spool_dir: &Path,
    now: SystemTime,
    max_events: usize,
    mut deliver: F,
) -> ReplayStats
where
    F: FnMut(AlertEvent) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let mut stats = ReplayStats::default();
    let path = spool_dir.join(SPOOL_FILE);
    let guard = match directory_guard(spool_dir).await {
        Ok(guard) => guard,
        Err(e) => {
            tracing::warn!(source = %e, "spool: 获取目录许可失败");
            return stats;
        }
    };
    let read_path = path.clone();
    let (result, guard) =
        match tokio::task::spawn_blocking(move || (std::fs::read(read_path), guard)).await {
            Ok(read) => read,
            Err(e) => {
                tracing::warn!(source = %e, "spool: 读取任务失败");
                return stats;
            }
        };
    let content = match result {
        Ok(content) => content,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return stats,
        Err(e) => {
            tracing::warn!(path = %path.display(), source = %e, "spool: 读取失败");
            return stats;
        }
    };
    if content.iter().all(u8::is_ascii_whitespace) {
        return stats;
    }

    let mut remain = Vec::new();
    let mut retry = Vec::new();
    for (lineno, line) in content.split(|byte| *byte == b'\n').enumerate() {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match serde_json::from_slice::<AlertEvent>(line) {
            Ok(ev) => {
                let expired = now
                    .duration_since(ev.last_seen)
                    .map(|age| age > SPOOL_MAX_AGE)
                    .unwrap_or(false);
                if expired {
                    stats.expired += 1;
                    tracing::warn!(
                        incident = ev.incident,
                        target = %ev.target,
                        "spool: event expired, dropped (age > 7d)"
                    );
                    continue;
                }
                if max_events == 0 || stats.attempted < max_events {
                    stats.attempted += 1;
                    if deliver(ev).await {
                        stats.requeued += 1;
                        continue;
                    }
                    stats.failed += 1;
                    retry.extend_from_slice(line);
                    retry.push(b'\n');
                    continue;
                }
                // 未尝试事件留在前面，失败项移到末尾避免永久坏 payload 饿死后续事件。
                remain.extend_from_slice(line);
                remain.push(b'\n');
            }
            Err(e) => {
                stats.corrupt_lines += 1;
                remain.extend_from_slice(line);
                remain.push(b'\n');
                tracing::warn!(path = %path.display(), lineno, source = %e, "spool: 坏行原样保留");
            }
        }
    }

    if stats.requeued + stats.expired + stats.failed == 0 {
        return stats;
    }
    remain.extend_from_slice(&retry);
    let update_path = path.clone();
    // 整笔磁盘事务持有许可直到完成，取消异步等待不会提前解锁。
    let update = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let _guard = guard;
        if remain.is_empty() {
            std::fs::remove_file(&update_path)
        } else {
            replace_pending(&update_path, &remain)
        }
    })
    .await;
    if !matches!(update, Ok(Ok(()))) {
        tracing::warn!(path = %path.display(), result = ?update, "spool: 确认文件更新失败，下轮可能重复投递");
    }
    stats
}

fn replace_pending(path: &Path, content: &[u8]) -> std::io::Result<()> {
    let temporary = path.with_file_name(SPOOL_TEMP_FILE);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(content)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temporary, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::alert::{Category, Severity};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::SystemTime;

    /// `age_secs` = 距今的年龄 (锚定真实 now;过期判定按 `last_seen` 距 now)。
    fn event(occ: u64, age_secs: u64) -> AlertEvent {
        let last_seen = SystemTime::now()
            .checked_sub(Duration::from_secs(age_secs))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        AlertEvent {
            incident: 1,
            category: Category::Alert,
            severity: Severity::Critical,
            target: "web-1".into(),
            check: "api".into(),
            reason: "boom".into(),
            observed: None,
            first_seen: last_seen,
            last_seen,
            occurrences: occ,
        }
    }

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "sw-spool-{tag}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[tokio::test]
    async fn append_then_replay_requeues_and_removes_file() {
        let dir = tmpdir("replay");
        assert!(append(&dir, &event(1, 60)).await);
        assert!(append(&dir, &event(2, 60)).await);
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let s = {
            let seen = seen.clone();
            replay(&dir, SystemTime::now(), 0, move |ev| {
                seen.lock().unwrap().push(ev.occurrences)
            })
            .await
        };
        assert_eq!(s.requeued, 2);
        assert_eq!(*seen.lock().unwrap(), vec![1, 2], "重放保序");
        assert!(!dir.join(SPOOL_FILE).exists(), "全部消费后文件删除");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn replay_drops_expired_events() {
        let dir = tmpdir("expired");
        assert!(append(&dir, &event(1, 60)).await); // 新鲜
        assert!(append(&dir, &event(2, SPOOL_MAX_AGE.as_secs() + 3600)).await); // 过期
        let n = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let s = {
            let n = n.clone();
            replay(&dir, SystemTime::now(), 0, move |_| {
                n.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            })
            .await
        };
        assert_eq!(s.requeued, 1);
        assert_eq!(s.expired, 1);
        assert_eq!(n.load(std::sync::atomic::Ordering::Relaxed), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn replay_batch_limit_keeps_remainder() {
        let dir = tmpdir("batch");
        for i in 1..=3 {
            assert!(append(&dir, &event(i, 60)).await);
        }
        let got = Arc::new(std::sync::Mutex::new(Vec::new()));
        let s = {
            let got = got.clone();
            replay(&dir, SystemTime::now(), 2, move |ev| {
                got.lock().unwrap().push(ev.occurrences)
            })
            .await
        };
        assert_eq!(s.requeued, 2);
        assert_eq!(*got.lock().unwrap(), vec![1, 2]);
        assert!(dir.join(SPOOL_FILE).exists(), "剩余事件保留在文件");
        // 第二轮全量放行
        let s2 = replay(&dir, SystemTime::now(), 0, |_| {}).await;
        assert_eq!(s2.requeued, 1);
        assert!(!dir.join(SPOOL_FILE).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn corrupt_lines_are_preserved() {
        let dir = tmpdir("corrupt");
        std::fs::write(dir.join(SPOOL_FILE), "{not json}\n").unwrap();
        assert!(append(&dir, &event(1, 60)).await);
        let s = replay(&dir, SystemTime::now(), 0, |_| {}).await;
        assert_eq!(s.requeued, 1);
        assert_eq!(s.corrupt_lines, 1);
        let kept = std::fs::read_to_string(dir.join(SPOOL_FILE)).unwrap();
        assert_eq!(kept.trim(), "{not json}", "坏行原样保留 (不吞数据)");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn truncated_invalid_utf8_tail_is_isolated_before_append() {
        let dir = tmpdir("truncated");
        assert!(append(&dir, &event(1, 60)).await);
        let mut bytes = std::fs::read(dir.join(SPOOL_FILE)).unwrap();
        bytes.extend_from_slice(b"{\"reason\":\"\xe4\xb8");
        std::fs::write(dir.join(SPOOL_FILE), bytes).unwrap();
        assert!(append(&dir, &event(2, 60)).await);
        let mut delivered = Vec::new();
        let stats = replay(&dir, SystemTime::now(), 0, |event| {
            delivered.push(event.occurrences);
        })
        .await;
        assert_eq!(delivered, vec![1, 2]);
        assert_eq!(stats.corrupt_lines, 1);
        assert_eq!(
            std::fs::read(dir.join(SPOOL_FILE)).unwrap(),
            b"{\"reason\":\"\xe4\xb8\n"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn failed_delivery_remains_until_successful_acknowledgement() {
        let dir = tmpdir("ack");
        assert!(append(&dir, &event(1, 60)).await);
        assert!(append(&dir, &event(2, 60)).await);
        let stats = replay_deliver(&dir, SystemTime::now(), 1, |_| async { false }).await;
        assert_eq!(stats.attempted, 1);
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.requeued, 0);
        assert_eq!(
            std::fs::read_to_string(dir.join(SPOOL_FILE))
                .unwrap()
                .lines()
                .count(),
            2
        );
        let stats = replay_deliver(&dir, SystemTime::now(), 1, |_| async { true }).await;
        assert_eq!(stats.requeued, 1);
        let remaining = std::fs::read_to_string(dir.join(SPOOL_FILE)).unwrap();
        let event: AlertEvent = serde_json::from_str(remaining.trim()).unwrap();
        assert_eq!(event.occurrences, 1);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn failed_batch_rotates_behind_unattempted_events() {
        let dir = tmpdir("fair");
        for index in 1..=17 {
            assert!(append(&dir, &event(index, 60)).await);
        }
        let first = replay_deliver(&dir, SystemTime::now(), 16, |_| async { false }).await;
        assert_eq!(first.failed, 16);
        let second = replay_deliver(&dir, SystemTime::now(), 1, |event| async move {
            assert_eq!(event.occurrences, 17);
            true
        })
        .await;
        assert_eq!(second.requeued, 1);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn cancelled_delivery_keeps_original_spool() {
        let dir = tmpdir("cancel");
        assert!(append(&dir, &event(1, 60)).await);
        let original = std::fs::read(dir.join(SPOOL_FILE)).unwrap();
        let result = tokio::time::timeout(
            Duration::from_millis(20),
            replay_deliver(&dir, SystemTime::now(), 1, |_| {
                std::future::pending::<bool>()
            }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(std::fs::read(dir.join(SPOOL_FILE)).unwrap(), original);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn replay_missing_file_is_noop() {
        let dir = tmpdir("missing");
        let s = replay(&dir, SystemTime::now(), 0, |_| {}).await;
        assert_eq!(s.requeued, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
