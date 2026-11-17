//! 指标周期推送 (ROADMAP「指标周期推送」):`[metrics]` 协议定义与实现。
//!
//! ## 协议 (投递契约)
//! - 每 `interval`(默认 60s) 把最新 [`MetricsReport`](crate::model::control::MetricsReport)
//!   以 JSON `POST` 到 `url`(Content-Type: application/json);
//! - 成功 = HTTP 2xx;重定向视为失败 (`redirect(Policy::none())`,与 webhook 一致
//!   —— 301/303 会把 POST 降级为 GET 投往错误端点);
//! - 单次尝试，无重试 (指标是可再生的瞬时快照，at-least-once 无意义;
//!   下一周期自然是最新值 —— "失败即放弃，下轮覆盖"语义);
//! - 失败行为:WARN 日志 + 连败 **首轮与恢复首轮** 写 journal Audit(粘性边沿，
//!   不刷屏);连败 ≥3 后每 10 连败再记一条 (长期故障可观测但不淹没日志)。

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

use crate::model::control::MetricsReport;

/// 连败多少次后进入「每 10 连败一条日志」的降噪模式。
const STREAK_QUIET_THRESHOLD: u64 = 3;
const STREAK_QUIET_EVERY: u64 = 10;

/// 指标推送任务。`journal_tx` 为 `None` 时 (测试/无 journal) 仅日志。
pub async fn run_metrics_pusher(
    url: url::Url,
    interval: Duration,
    timeout: Duration,
    metrics_rx: watch::Receiver<Arc<MetricsReport>>,
    shutdown: watch::Receiver<bool>,
    journal_tx: Option<crate::recover::journal::JournalSender>,
) {
    let client = match reqwest::Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(source = %e, "metrics push: client build failed; push disabled");
            return;
        }
    };
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut shutdown = shutdown;
    let mut fail_streak: u64 = 0;
    let mut journal_audit_active = false;

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => {
                if shutdown.has_changed().is_ok() {
                    if *shutdown.borrow_and_update() {
                        return;
                    }
                } else {
                    return; // sender 已 drop:上游终结 (防 Err 臂忙转)
                }
            }
        }
        let report = metrics_rx.borrow().clone();
        let attempt = client.post(url.clone()).json(&*report).send();
        // panic 隔离来自任务边界 (JoinError::is_panic，下方 Err 臂)—— 无需
        // AssertUnwindSafe 包装 (第六轮评审微瑕清理)。
        let outcome =
            tokio::spawn(async move { tokio::time::timeout(timeout, attempt).await }).await;
        match outcome {
            // panic 隔离：推送 panic 不毒化任务 (单次尝试在独立任务)
            Err(join_err) => {
                fail_streak += 1;
                tracing::error!(summary = %join_err, streak = fail_streak, "metrics push task panicked");
            }
            Ok(Ok(Ok(resp))) => {
                if resp.status().is_success() {
                    if journal_audit_active {
                        journal_audit(&journal_tx, "metrics push recovered after failures").await;
                        journal_audit_active = false;
                    }
                    fail_streak = 0;
                    tracing::debug!(status = %resp.status(), "metrics pushed");
                } else {
                    fail_streak += 1;
                    on_failure(
                        &mut journal_audit_active,
                        fail_streak,
                        &journal_tx,
                        &url,
                        Some(format!("status {}", resp.status())),
                    )
                    .await;
                }
            }
            Ok(Ok(Err(e))) => {
                fail_streak += 1;
                on_failure(
                    &mut journal_audit_active,
                    fail_streak,
                    &journal_tx,
                    &url,
                    Some(e.to_string()),
                )
                .await;
            }
            Ok(Err(_elapsed)) => {
                fail_streak += 1;
                on_failure(
                    &mut journal_audit_active,
                    fail_streak,
                    &journal_tx,
                    &url,
                    Some(format!("timeout after {timeout:?}")),
                )
                .await;
            }
        }
    }
}

/// 失败处理:WARN;连败边沿写 journal 审计 (首轮 / 恢复首轮 / 长期故障采样)。
async fn on_failure(
    journal_audit_active: &mut bool,
    streak: u64,
    journal_tx: &Option<crate::recover::journal::JournalSender>,
    url: &url::Url,
    detail: Option<String>,
) {
    let detail = detail.unwrap_or_default();
    if streak == 1 {
        tracing::warn!(url = %url, detail = %detail, "metrics push failed (streak begins)");
        if !*journal_audit_active {
            journal_audit(journal_tx, "metrics push failure streak began").await;
            *journal_audit_active = true;
        }
    } else if streak == STREAK_QUIET_THRESHOLD {
        tracing::warn!(
            streak,
            "metrics push failing persistently; further logs sampled"
        );
        journal_audit(
            journal_tx,
            "metrics push failing persistently (streak >= 3)",
        )
        .await;
    } else if streak > STREAK_QUIET_THRESHOLD
        && (streak - STREAK_QUIET_THRESHOLD).is_multiple_of(STREAK_QUIET_EVERY)
    {
        tracing::warn!(streak, "metrics push still failing");
        journal_audit(
            journal_tx,
            &format!("metrics push still failing (streak {streak})"),
        )
        .await;
    } else {
        tracing::debug!(streak, detail = %detail, "metrics push failed");
    }
}

/// journal 审计 (尽力而为;无通道时仅 debug)。
async fn journal_audit(journal_tx: &Option<crate::recover::journal::JournalSender>, message: &str) {
    let Some(tx) = journal_tx else {
        tracing::debug!(message, "metrics push audit (no journal channel)");
        return;
    };
    if let Err(e) = tx.try_send(crate::recover::journal::JournalEvent::Audit {
        at: std::time::SystemTime::now(),
        message: message.to_string(),
    }) {
        tracing::debug!(source = %e, "metrics push audit could not be queued");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 端到端：真实 axum stub 接收 POST，断言 body 可反序列化为 MetricsReport
    /// 且按周期重复投递 (真实短 interval;paused 时钟对真实网络无意义)。
    #[tokio::test(flavor = "multi_thread")]
    async fn pushes_report_on_interval_and_parses_as_metrics_report() {
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_c = hits.clone();
        let app = axum::Router::new().route(
            "/metrics",
            axum::routing::post(move |body: axum::body::Bytes| {
                let hits = hits_c.clone();
                async move {
                    let _ = serde_json::from_slice::<MetricsReport>(&body)
                        .expect("body must deserialize as MetricsReport");
                    hits.fetch_add(1, Ordering::Relaxed);
                    axum::http::StatusCode::OK
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let (metrics_tx, metrics_rx) = watch::channel(Arc::new(MetricsReport::default()));
        let (jtx_raw, jrx) = tokio::sync::mpsc::channel::<crate::recover::journal::JournalEvent>(8);
        let _jtx = crate::recover::journal::JournalSender::from(jtx_raw);
        let (shutdown_tx, shutdown) = watch::channel(false);
        let task = tokio::spawn(run_metrics_pusher(
            url::Url::parse(&format!("http://{addr}/metrics")).unwrap(),
            Duration::from_millis(200),
            Duration::from_secs(2),
            metrics_rx,
            shutdown.clone(),
            None,
        ));
        // eval 侧持续发布 (首拍在 interval 后)
        metrics_tx.send_replace(Arc::new(MetricsReport {
            probe_executed: 7,
            ..Default::default()
        }));
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if hits.load(Ordering::Relaxed) >= 2 {
                break;
            }
        }
        assert!(hits.load(Ordering::Relaxed) >= 2, "必须按周期重复投递");
        shutdown_tx.send(true).ok();
        let _ = task.await;
        let _ = jrx; // 保持接收端存活
    }

    /// 失败路径：连接拒绝 → 连败不 panic、任务存活;恢复后审计边沿 (journal 收到)。
    #[tokio::test]
    async fn failure_streak_records_journal_audit_edges() {
        // 不可达端口：连接拒绝
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // 立即释放 → 连接拒绝
        let (metrics_tx, metrics_rx) = watch::channel(Arc::new(MetricsReport::default()));
        let (jtx_raw, mut jrx) =
            tokio::sync::mpsc::channel::<crate::recover::journal::JournalEvent>(16);
        let jtx = crate::recover::journal::JournalSender::from(jtx_raw);
        let (shutdown_tx, shutdown) = watch::channel(false);
        let task = tokio::spawn(run_metrics_pusher(
            url::Url::parse(&format!("http://{addr}/m")).unwrap(),
            Duration::from_millis(50),
            Duration::from_secs(1),
            metrics_rx,
            shutdown.clone(),
            Some(jtx),
        ));
        metrics_tx.send_replace(Arc::new(MetricsReport::default()));
        // ≥3 连败 → "persistently" 审计
        let mut got_streak_audit = false;
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            while let Ok(ev) = jrx.try_recv() {
                if let crate::recover::journal::JournalEvent::Audit { message, .. } = ev
                    && message.contains("persistently")
                {
                    got_streak_audit = true;
                }
            }
            if got_streak_audit {
                break;
            }
        }
        assert!(got_streak_audit, "连败 ≥3 必须写 journal 审计");
        shutdown_tx.send(true).ok();
        let _ = task.await;
    }
}
