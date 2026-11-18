//! F39 指标历史持久化:`[metrics] history_path`(可选)把 eval 每轮发布的运行
//! 指标样本以 JSONL 追加落盘 —— 纯**观测数据出口**，不做重放/查询(分析走
//! jq/外部工具，DESIGN §10)。
//!
//! ## 行格式 (= `/metrics/history` 单样本对象)
//! 一行 = [`crate::model::control::MetricsSample`] 的 serde 形态
//! `{"at": <采样时刻>, "report": <MetricsReport>}`,与 `GET /metrics/history`
//! 响应内的样本对象完全同构 —— 落盘文件可直接按该端点的语义离线分析。
//! `at` 由 writer 在消费时刻盖戳 (与 eval 入环同轮，亚毫秒偏差;watch 通道
//! 不携带采样时刻，取舍记录见 DESIGN §10)。
//!
//! ## 消费语义
//! - 独立任务消费 metrics watch 克隆(`watch::Receiver<Arc<MetricsReport>>`),
//!   **每条变更写一行**，绝不阻塞 eval 单写者 (watch 旁路，eval 零改动);
//! - watch 合并语义：writer 消费滞后时中间样本被最新值覆盖 —— 极端滞后下
//!   落盘的是最新值而非全部中间轮 (指标是瞬时快照，跳轮可接受，如实记录);
//! - **首样本照写**：启动时 watch 当前值 (可能是全零默认报告) 也落一行，
//!   不去重 —— 重启追加不截断，历史文件跨重启连续;
//! - 全部 metrics 发送端 drop (`changed()` 返回 Err) = 上游终结，writer 退出。
//!
//! ## 文件与轮转 (复用 journal F24 单代模式)
//! - 打开模式 append+create (重启追加);单写者整行写入，写入段无 await 点 ——
//!   runtime 停机 abort 只落在 `changed().await`，不会截断半行 (打开时也**不做**
//!   journal 式半行定界:进程崩溃恰好落在 write_all 中途才可能产生残行，
//!   概率极低，分析侧容忍即可);
//! - 轮转 = 写**后**实测当前文件尺寸 (与 journal F24 同款:内核实测天然兼容
//!   外部 truncate)，超 [`HISTORY_ROTATE_THRESHOLD`] (10 MiB) → 改名 `.1`
//!   (只保留一代，二次轮转覆盖)，gen1 路径复用 [`crate::recover::journal::
//!   gen1_path`] 单一事实源;阈值是软性运维目标，单行溢出可接受;
//! - 轮转失败 WARN 降级继续写当前文件 (不停机 —— 体积上限是运维目标不是
//!   安全边界，与 F24 同口径);轮转成功只写一条 `tracing::info`，**不写
//!   journal audit**(journal 是恢复域，历史文件是观测出口，两域隔离);
//! - 无逐行 fsync (数据可靠性 = OS 页缓存;观测数据可容忍崩溃丢尾部)。
//!
//! ## 运维边界 (RUNBOOK §1.2e)
//! 外部 copytruncate 工具 (清零原文件) 与本 writer 兼容 (O_APPEND + 实测尺寸
//! 判断，journal 同款);**rename 式**外部轮转无效 (writer 持 fd 续写旧 inode);
//! 外部工具不得删除/改名 history 主文件本身。

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::watch;

use crate::model::control::{MetricsReport, MetricsSample};

/// F39:历史文件轮转阈值 (10 MiB，与 `agent.journal_max_size` 默认一致)。
/// 写后实测尺寸超阈值即改名 `.1` 保留一代;0 = 完全禁用轮转 (仅测试用)。
pub const HISTORY_ROTATE_THRESHOLD: u64 = 10 * 1024 * 1024;

/// 写失败 WARN 采样间隔 (对齐 journal 轮转失败的模式:首次 + 每 64 次)。
const WARN_SAMPLE_EVERY: u64 = 64;

/// 产品入口:默认阈值 [`HISTORY_ROTATE_THRESHOLD`]。测试用
/// [`run_history_writer_with_limit`] 注入小阈值驱动轮转。
pub async fn run_history_writer(path: PathBuf, metrics_rx: watch::Receiver<Arc<MetricsReport>>) {
    run_history_writer_with_limit(path, metrics_rx, HISTORY_ROTATE_THRESHOLD).await
}

/// 历史写任务。`max_size`:轮转阈值 (字节;0 = 禁用轮转,测试注入小阈值)。
/// 打开失败/轮转降级重开失败 = 路径真实故障 → ERROR 后任务退出 (历史持久化
/// 静默失效不可取;agent 不因此停机 —— 观测出口非恢复域)。
pub async fn run_history_writer_with_limit(
    path: PathBuf,
    mut metrics_rx: watch::Receiver<Arc<MetricsReport>>,
    max_size: u64,
) {
    let mut file = match open_history_file(&path) {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(
                path = %path.display(),
                source = %e,
                "metrics history open failed; history persistence disabled for this run"
            );
            return;
        }
    };
    let mut write_failures: u64 = 0;
    let mut rotation_failures: u64 = 0;

    // F39 取舍:首样本照写 (不去重) —— watch 当前值先落一行，重启追加不截断，
    // 历史跨重启连续 (可能是一行全零默认报告，如实记录)。
    let initial = metrics_rx.borrow_and_update().clone();
    file = match write_and_maybe_rotate(
        file,
        &path,
        &initial,
        max_size,
        &mut write_failures,
        &mut rotation_failures,
    ) {
        Some(f) => f,
        None => return, // 路径故障 (内部已 ERROR)
    };
    loop {
        // changed() Err = 全部发送端已 drop:上游终结，不可能再有新样本
        // (对齐 AlertQueue::pop 的 sender-dropped 处理，防 Err 臂忙转)。
        if metrics_rx.changed().await.is_err() {
            return;
        }
        let report = metrics_rx.borrow_and_update().clone();
        file = match write_and_maybe_rotate(
            file,
            &path,
            &report,
            max_size,
            &mut write_failures,
            &mut rotation_failures,
        ) {
            Some(f) => f,
            None => return,
        };
    }
}

/// 写一行样本并在写后按实测尺寸轮转。返回 `None` = 句柄已不可恢复 (轮转降级
/// 重开失败)，任务必须终止;`Some(file)` = 存活句柄 (含写失败降级继续)。
/// 同步函数且无 await —— abort 不会落在本函数内，整行写入不被截断。
fn write_and_maybe_rotate(
    mut file: std::fs::File,
    path: &Path,
    report: &Arc<MetricsReport>,
    max_size: u64,
    write_failures: &mut u64,
    rotation_failures: &mut u64,
) -> Option<std::fs::File> {
    // 行 = /metrics/history 单样本对象;at 由 writer 盖戳 (消费时刻)。
    let sample = MetricsSample {
        at: SystemTime::now(),
        report: (**report).clone(),
    };
    let mut line = match serde_json::to_vec(&sample) {
        Ok(v) => v,
        Err(e) => {
            // 理论路径 (MetricsSample 序列化实际不触发);诚实失败面，不计轮转。
            *write_failures += 1;
            warn_sampled(*write_failures, || {
                tracing::warn!(path = %path.display(), source = %e, "metrics history serialize failed; sample skipped");
            });
            return Some(file);
        }
    };
    line.push(b'\n');
    if let Err(e) = file.write_all(&line) {
        *write_failures += 1;
        warn_sampled(*write_failures, || {
            tracing::warn!(path = %path.display(), source = %e, "metrics history write failed; keeping current file (observability outlet, best effort)");
        });
        // 句柄仍持有，下一轮继续写 (观测数据尽力而为，不终止任务)。
        return Some(file);
    }
    // 写后实测 (journal F24 同款:外部 truncate 清零后不会误轮转);0 = 禁用。
    if max_size == 0 {
        return Some(file);
    }
    let Ok(meta) = file.metadata() else {
        return Some(file);
    };
    if meta.len() <= max_size {
        return Some(file);
    }
    // 轮转:释放句柄 (Windows 打开中的文件不可 rename) → 删旧 .1 → 改名 →
    // 重建当前文件 (journal F24 同款顺序;gen1 路径复用 journal 单一事实源)。
    let rotated_bytes = meta.len();
    drop(file);
    match rotate_and_reopen(path) {
        Ok(new_file) => {
            // 只写 tracing::info，不写 journal audit —— journal 是恢复域，
            // 历史文件是观测出口 (与 F24 的 journal 轮转审计刻意不同)。
            tracing::info!(
                path = %path.display(),
                rotated_bytes,
                "metrics history rotated (one generation kept); current file restarted"
            );
            Some(new_file)
        }
        Err(e) => {
            *rotation_failures += 1;
            warn_sampled(*rotation_failures, || {
                tracing::warn!(path = %path.display(), source = %e, "metrics history rotation failed; keeping current file (size cap is an ops target, not a safety boundary)");
            });
            // 句柄已在轮转尝试中释放，必须重开当前文件继续 (journal 降级同款);
            // 重开失败 = 真实路径故障，ERROR 后任务终止 (不伪装继续)。
            match open_history_file(path) {
                Ok(f) => Some(f),
                Err(reopen) => {
                    tracing::error!(
                        path = %path.display(),
                        source = %e,
                        reopen = %reopen,
                        "metrics history reopen failed after rotation error; history persistence stopped"
                    );
                    None
                }
            }
        }
    }
}

/// 采样 WARN:首次 + 每 [`WARN_SAMPLE_EVERY`] 次 (防逐样本刷屏，journal 同款)。
fn warn_sampled(count: u64, log: impl FnOnce()) {
    if count == 1 || count.is_multiple_of(WARN_SAMPLE_EVERY) {
        log();
    }
}

/// 单代轮转:删旧 `.1` → 当前文件改名 `.1` → 重建当前文件。
/// `gen1_path` 复用 recover::journal 的单一事实源 (`history.jsonl` →
/// `history.1.jsonl`,无扩展名同样成立)。
fn rotate_and_reopen(path: &Path) -> std::io::Result<std::fs::File> {
    let gen1 = crate::recover::journal::gen1_path(path);
    if gen1.exists() {
        std::fs::remove_file(&gen1)?;
    }
    std::fs::rename(path, &gen1)?;
    open_history_file(path)
}

/// 打开历史文件 (append+create;父目录随建)。不做 journal 式半行定界 ——
/// 单写者整行写入，只有崩溃落在 write_all 中途才可能产生残行 (模块 doc)。
fn open_history_file(path: &Path) -> std::io::Result<std::fs::File> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "sw-metrics-history-{tag}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ))
    }

    fn report(probe: u64) -> Arc<MetricsReport> {
        Arc::new(MetricsReport {
            probe_executed: probe,
            ..Default::default()
        })
    }

    fn read_lines(path: &Path) -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(String::from)
            .collect()
    }

    /// 等待文件行数达标 (await 轮询让出运行时线程 —— current-thread 测试
    /// runtime 下 std::thread::sleep 会饿死 writer 任务)。
    async fn wait_lines(path: &Path, want: usize) -> Vec<String> {
        for _ in 0..200 {
            let lines = read_lines(path);
            if lines.len() >= want {
                return lines;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("file {} did not reach {want} lines in time", path.display());
    }

    /// 每样本一行;行 = MetricsSample serde 形态 (顶层恰为 at + report 两键，
    /// 与 /metrics/history 样本对象同构)，report 内容与发布值一致。
    #[tokio::test]
    async fn writes_one_line_per_sample_in_metrics_sample_shape() {
        let path = tmp_path("shape");
        let (tx, rx) = watch::channel(report(0));
        let task = tokio::spawn(run_history_writer_with_limit(path.clone(), rx, 0));
        // 首样本 (watch 初始值 = 默认报告) 照写。
        wait_lines(&path, 1).await;
        tx.send_replace(report(7));
        let lines = wait_lines(&path, 2).await;
        let v: serde_json::Value = serde_json::from_str(&lines[1]).unwrap();
        let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            vec!["at", "report"],
            "顶层键 = MetricsSample serde 形态"
        );
        assert_eq!(v["report"]["probe_executed"], 7, "report 与发布值一致");
        let parsed: MetricsSample = serde_json::from_str(&lines[1]).unwrap();
        assert!(parsed.at > SystemTime::UNIX_EPOCH, "at = writer 盖戳时刻");
        drop(tx);
        let _ = task.await;
        let _ = std::fs::remove_file(&path);
    }

    /// 轮转 (小阈值):每行写完即超限 → `.1` 只保留一代，上一代被覆盖;
    /// 当前文件重建后从零续写 (极端阈值下当前文件稳态恒为空 —— 行写入后
    /// 立即轮走，断言以 `.1` 内容为准)。
    #[tokio::test]
    async fn rotation_keeps_single_generation() {
        let path = tmp_path("rotate");
        let gen1 = crate::recover::journal::gen1_path(&path);
        let (tx, rx) = watch::channel(report(0));
        // 阈值 1 字节 → 每行写完必然超限轮转。
        let task = tokio::spawn(run_history_writer_with_limit(path.clone(), rx, 1));
        // 逐个发送并等待该样本出现在 `.1` (轮转完成的确定性证据)。
        for probe in [1, 2, 3] {
            tx.send_replace(report(probe));
            wait_gen1_probe(&gen1, probe).await;
        }
        // 稳态:`.1` = 最新写入行 (probe=3);probe 0/1/2 三代已被覆盖淘汰。
        let gen1_lines = read_lines(&gen1);
        let kept: MetricsSample = serde_json::from_str(&gen1_lines[0]).unwrap();
        assert_eq!(
            kept.report.probe_executed, 3,
            "`.1` 保留的是最新一代 (二次轮转覆盖旧代)"
        );
        assert_eq!(gen1_lines.len(), 1, "只保留一代 (单行)");
        assert!(
            read_lines(&path).is_empty(),
            "当前文件轮转后从零续写 (刚写完即又超限轮转，暂为空)"
        );
        drop(tx);
        let _ = task.await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&gen1);
    }

    /// 等待 `.1` 首行 report.probe_executed == probe (轮转完成的确定性证据)。
    async fn wait_gen1_probe(gen1: &Path, probe: u64) {
        for _ in 0..200 {
            let hit = read_lines(gen1).first().is_some_and(|l| {
                serde_json::from_str::<MetricsSample>(l)
                    .map(|s| s.report.probe_executed == probe)
                    .unwrap_or(false)
            });
            if hit {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("gen1 {gen1:?} never received probe={probe} in time");
    }

    /// `max_size = 0` 完全禁用轮转:`.1` 永不产生 (journal F24 同口径)。
    #[tokio::test]
    async fn max_size_zero_disables_rotation() {
        let path = tmp_path("norotate");
        let gen1 = crate::recover::journal::gen1_path(&path);
        let (tx, rx) = watch::channel(report(0));
        let task = tokio::spawn(run_history_writer_with_limit(path.clone(), rx, 0));
        wait_lines(&path, 1).await;
        tx.send_replace(report(1));
        let lines = wait_lines(&path, 2).await;
        assert_eq!(lines.len(), 2, "全部行都在当前文件");
        assert!(!gen1.exists(), "禁用轮转时 `.1` 永不产生");
        drop(tx);
        let _ = task.await;
        let _ = std::fs::remove_file(&path);
    }

    /// 重启追加不截断:两次 writer 会话 (模拟 agent 重启) 写同一文件，
    /// 前会话行保留，后会话续写追加。
    #[tokio::test]
    async fn restart_appends_without_truncation() {
        let path = tmp_path("append");
        // 会话 1:初始默认行 + probe=7。
        let (tx, rx) = watch::channel(report(0));
        let task = tokio::spawn(run_history_writer_with_limit(path.clone(), rx, 0));
        wait_lines(&path, 1).await;
        tx.send_replace(report(7));
        wait_lines(&path, 2).await;
        drop(tx);
        let _ = task.await;
        // 会话 2:重启 (不截断) —— 初始默认行 + probe=9 追加。
        let (tx, rx) = watch::channel(report(0));
        let task = tokio::spawn(run_history_writer_with_limit(path.clone(), rx, 0));
        wait_lines(&path, 3).await;
        tx.send_replace(report(9));
        let lines = wait_lines(&path, 4).await;
        let probes: Vec<u64> = lines
            .iter()
            .map(|l| {
                let s: MetricsSample = serde_json::from_str(l).unwrap();
                s.report.probe_executed
            })
            .collect();
        assert_eq!(
            probes,
            vec![0, 7, 0, 9],
            "会话 1 的行保留 (追加不截断)，会话 2 续写"
        );
        drop(tx);
        let _ = task.await;
        let _ = std::fs::remove_file(&path);
    }
}
