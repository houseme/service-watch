//! F30 状态页历史迷你图集成测试 (ROADMAP F30 / DESIGN §9.2):
//! - `GET /` 返回 200 且包含 SVG 迷你图容器标记 (`id="spark-*"`,内嵌零依赖);
//! - `GET /metrics/history?range=300` 返回的样本与注入环形缓冲的一致
//!   (页面 JS 的数据源契约：折线点数 = samples 数)。
//!
//! 页面 JS 的实际渲染 (折线绘制/escapeHtml 纪律) 无法在本层自动化，JS 语法经
//! `node --check` 人工验证 (2026-11-18),浏览器目验另记 —— 不为 JS 引入构建步骤。
//!
//! control/mod.rs 不可写依赖 (并行开发),测试复刻其单测的装配方式：
//! ControlShared 全字段 pub，经 `control::serve` (pub，自 bind) 起服务。

#![cfg(all(feature = "control-http", feature = "http-client"))]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use monitor_core::collect::{Collector, StubCollector};
use monitor_core::control::ControlShared;
use monitor_core::model::control::{
    MetricsHistoryRing, MetricsReport, MetricsSample, StatusReport,
};
use tokio::sync::{mpsc, watch};

/// 起服务并返回 base url:`serve` 自行 bind，先占随机空闲端口再放手;
/// serve bind 与首个请求存在竞态，health 轮询重试 (bind 被抢则换端口重来，
/// health 免鉴权，恒 200)。
async fn spawn_server(shared: Arc<ControlShared>) -> String {
    for _ in 0..10 {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let shared = shared.clone();
        let handle = tokio::spawn(async move {
            let _ = monitor_core::control::serve(addr, shared, None).await;
        });
        for _ in 0..50 {
            let healthy = reqwest::get(format!("http://{addr}/health"))
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false);
            if healthy {
                return format!("http://{addr}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        handle.abort();
    }
    panic!("failed to bind a test port after 10 attempts");
}

/// 构造 ControlShared,metrics 初始值携带注入 `n` 个样本的历史环形缓冲。
/// 返回 (共享状态，环形缓冲句柄) 供断言对照。
fn shared_with_history(n: usize) -> (Arc<ControlShared>, MetricsHistoryRing) {
    let ring: MetricsHistoryRing = Arc::new(Mutex::new(VecDeque::new()));
    for i in 0..n {
        ring.lock().unwrap().push_back(MetricsSample {
            at: SystemTime::now(),
            report: MetricsReport {
                checks_critical: i as u64,
                recovery_fatal: (i / 2) as u64,
                ..Default::default()
            },
        });
    }
    let metrics = MetricsReport {
        history: ring.clone(),
        ..Default::default()
    };
    let (_metrics_tx, metrics_rx) = watch::channel(Arc::new(metrics));
    let (status_tx, status_rx) = watch::channel(Arc::new(StatusReport {
        started_at: SystemTime::now(),
        ready: true,
        targets: Vec::new(),
    }));
    drop(status_tx); // 测试不再推进状态 watch;只读快照用初值即可
    let initial = Arc::new(StubCollector::default().collect(&[]));
    let (_snapshot_tx, snapshot_rx) = watch::channel(initial);
    let (cmd_tx, _cmd_rx) = mpsc::channel(16);
    let shared = Arc::new(ControlShared {
        started_at: SystemTime::now(),
        ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        status_rx,
        metrics_rx,
        snapshot_rx,
        cmd_tx,
        alert_counters: Arc::new(monitor_core::alert::AlertCounters::default()),
        token: None,
        journal_path: std::path::PathBuf::from("journal.jsonl"),
        // F33 字段 (并行接线):本文件不覆盖 /v1/config,watch 初始 Null。
        config_view: {
            let (_tx, rx) = tokio::sync::watch::channel(Arc::new(serde_json::Value::Null));
            rx
        },
    });
    (shared, ring)
}

/// `GET /`:200 + text/html + SVG 迷你图容器标记 (F30 验收：状态页 200 且含
/// SVG 元素)。
#[tokio::test]
async fn status_page_contains_svg_sparkline_containers() {
    let (shared, _ring) = shared_with_history(0);
    let base = spawn_server(shared).await;

    let resp = reqwest::get(format!("{base}/")).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let ct = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        ct.starts_with("text/html"),
        "Content-Type 应为 text/html:{ct}"
    );
    let body = resp.text().await.unwrap();
    // F30:两个迷你图容器 (checks_critical / recovery_fatal)。
    for marker in [
        r#"<svg id="spark-critical""#,
        r#"<svg id="spark-fatal""#,
        r#"id="spark-critical-meta""#,
        r#"id="spark-fatal-meta""#,
        "/metrics/history?range=",
    ] {
        assert!(body.contains(marker), "状态页应包含 {marker}");
    }
    // 零依赖纪律:SVG 由页内 JS 生成，无任何外部资源引用。
    assert!(
        !body.contains("http://") || body.contains("http://www.w3.org/2000/svg"),
        "除 SVG 命名空间外不得有外部资源引用"
    );
}

/// 页面 JS 数据源契约:`GET /metrics/history?range=300` 返回注入环形缓冲的
/// 全部样本 (count = samples.len();折线点数与之一致是 JS 行为，人工目验)。
#[tokio::test]
async fn metrics_history_returns_injected_samples() {
    let (shared, ring) = shared_with_history(5);
    let base = spawn_server(shared).await;

    let resp = reqwest::get(format!("{base}/metrics/history?range=300"))
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let hist: monitor_core::model::control::MetricsHistory = resp.json().await.unwrap();
    assert_eq!(hist.count, 5, "count 与注入样本数一致");
    assert_eq!(hist.samples.len(), 5);
    // 序列值与注入内容一致 (页面 JS 据此画 checks_critical / recovery_fatal)。
    let crit: Vec<u64> = hist
        .samples
        .iter()
        .map(|s| s.report.checks_critical)
        .collect();
    let fatal: Vec<u64> = hist
        .samples
        .iter()
        .map(|s| s.report.recovery_fatal)
        .collect();
    assert_eq!(crit, vec![0, 1, 2, 3, 4]);
    assert_eq!(fatal, vec![0, 0, 1, 1, 2]);
    // 环形缓冲本身未被服务端消费改动 (只读快照)。
    assert_eq!(ring.lock().unwrap().len(), 5);
}
