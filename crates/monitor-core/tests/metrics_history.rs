//! F39 指标历史持久化集成测试 (DESIGN §10 / ROADMAP F39):
//! - watch 通道直接喂 N 个样本 (不经 axum —— writer 是纯文件出口):
//!   文件行数 = 首样本 (照写) + N,每行 = `/metrics/history` 单样本对象
//!   (`{"at", "report"}`，report 与发布值逐字段一致);
//! - 单代轮转:小阈值下 `.1` 保留最新一代 (复用 journal gen1 布局)，
//!   当前文件重建后继续追加;重启追加语义由 src/metrics_history.rs 单测覆盖。
//!
//! 无 feature 门控:writer 不依赖 reqwest (纯文件出口，与 metrics push 解耦)，
//! 任何 feature 组合下编译 (no-default 组合 CI 跑 check 验证)。

use std::sync::Arc;
use std::time::Duration;

use monitor_core::metrics_history::{run_history_writer, run_history_writer_with_limit};
use monitor_core::model::control::{MetricsReport, MetricsSample};
use tokio::sync::watch;

fn tmp_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "sw-metrics-history-it-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    ))
}

fn report(probe: u64) -> Arc<MetricsReport> {
    Arc::new(MetricsReport {
        probe_executed: probe,
        restarts_total: probe / 2,
        ..Default::default()
    })
}

fn read_lines(path: &std::path::Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(String::from)
        .collect()
}

/// 轮询等待行数达标 (await 让出运行时线程，防饿死 writer 任务)。
async fn wait_lines(path: &std::path::Path, want: usize) -> Vec<String> {
    for _ in 0..200 {
        let lines = read_lines(path);
        if lines.len() >= want {
            return lines;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("file {} did not reach {want} lines in time", path.display());
}

/// 产品入口 (默认 10 MiB 阈值):喂 3 个样本 → 4 行 (首样本照写 + 3 条),
/// 每行反序列化为 MetricsSample 且 report 与发布值完全一致、at 单调不减。
#[tokio::test]
async fn product_entry_persists_each_published_sample_as_history_shape() {
    let path = tmp_path("product");
    let (tx, rx) = watch::channel(report(0));
    let task = tokio::spawn(run_history_writer(path.clone(), rx));
    wait_lines(&path, 1).await; // 首样本 (watch 初始值) 照写

    // 逐个发送并等待落盘 (避开 watch 合并语义，保证逐行可断言)。
    let mut published: Vec<serde_json::Value> = Vec::new();
    for probe in [11, 22, 33] {
        let sample = report(probe);
        published.push(serde_json::to_value(&*sample).unwrap());
        tx.send_replace(sample);
        wait_lines(&path, published.len() + 1).await;
    }

    let lines = read_lines(&path);
    assert_eq!(lines.len(), 4, "首样本 + 3 条发布样本");
    let mut prev_at = None;
    for (line, expected) in lines[1..].iter().zip(&published) {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["at", "report"], "行 = /metrics/history 样本对象");
        assert_eq!(
            &v["report"], expected,
            "report 与发布值逐字段一致 (probe={})",
            expected["probe_executed"]
        );
        let parsed: MetricsSample = serde_json::from_str(line).unwrap();
        assert!(parsed.at > std::time::SystemTime::UNIX_EPOCH);
        if let Some(prev) = prev_at {
            assert!(parsed.at >= prev, "at 按写入顺序单调不减");
        }
        prev_at = Some(parsed.at);
    }
    drop(tx);
    let _ = task.await;
    let _ = std::fs::remove_file(&path);
}

/// gen1 布局的测试侧推导 (与 recover::journal::gen1_path 对无扩展名路径的
/// 规则一致:`<name>.1.jsonl`;该布局本身由 journal 单测锁定)。
fn gen1_of(path: &std::path::Path) -> std::path::PathBuf {
    path.with_file_name(format!(
        "{}.1.jsonl",
        path.file_name().unwrap().to_string_lossy()
    ))
}

/// 单代轮转 (小阈值):`.1` 保留最新一代且内容与最后发布样本一致，
/// 更早的代被覆盖淘汰;当前文件重建后从零续写 (极端阈值下当前文件稳态恒空)。
#[tokio::test]
async fn rotation_keeps_latest_generation_at_gen1_path() {
    let path = tmp_path("rotate");
    let gen1 = gen1_of(&path);
    let (tx, rx) = watch::channel(report(0));
    // 阈值 1 字节 → 每行写完必然超限轮转 (软阈值的最坏形态，行为仍正确)。
    let task = tokio::spawn(run_history_writer_with_limit(path.clone(), rx, 1));
    // 逐个发送并等待该样本出现在 `.1` (轮转完成的确定性证据)。
    for probe in [1, 2, 3] {
        tx.send_replace(report(probe));
        wait_gen1_probe(&gen1, probe).await;
    }

    let gen1_lines = read_lines(&gen1);
    let kept: MetricsSample = serde_json::from_str(&gen1_lines[0]).unwrap();
    assert_eq!(
        kept.report.probe_executed, 3,
        "`.1` 保留最新一代 (二次轮转覆盖旧代，只保留一代)"
    );
    assert_eq!(gen1_lines.len(), 1, "只保留一代 (单行)");
    assert!(
        read_lines(&path).is_empty(),
        "当前文件轮转后从零续写 (刚写完即超限轮转，暂为空)"
    );
    drop(tx);
    let _ = task.await;
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&gen1);
}

/// 等待 `.1` 首行 report.probe_executed == probe (轮转完成的确定性证据)。
async fn wait_gen1_probe(gen1: &std::path::Path, probe: u64) {
    for _ in 0..200 {
        let hit = read_lines(gen1).first().is_some_and(|l| {
            serde_json::from_str::<MetricsSample>(l)
                .map(|s| s.report.probe_executed == probe)
                .unwrap_or(false)
        });
        if hit {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("gen1 {gen1:?} never received probe={probe} in time");
}
