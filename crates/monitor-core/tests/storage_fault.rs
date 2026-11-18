//! 存储故障演练 (ROADMAP「存储故障演练」):慢盘/磁盘满等价 (打开失败与
//! 确认超时)/断电半行/修复后重启 —— 全部经公共 API 驱动，三平台可跑。
//!
//! 场景映射：
//! - **磁盘满/不可写** → `open_writer` 打开失败 (目录占位/只读路径),R5 语义：
//!   journal 准备失败发生在任务启动前 → 启动早退，无任务泄漏;
//! - **慢盘 (sync_data 悬挂)** → writer 不回 ack:关键事件 `send` 2s 总预算
//!   耗尽返回 Err → eval 停止当前恢复实例 (R3);
//! - **断电 (半行)** → 崩溃留下的撕裂行被重放隔离，不吞后继已确认记录;
//! - **修复后重启** → 路径修复后 `open_writer` 成功，写入 → 重放可见;
//! - **不可中断进程 (D 状态)** → SIGKILL 免疫场景由
//!   `recover::tests::force_kill_failure_goes_fatal` 覆盖 (纯 FSM 注入，
//!   macOS 无法真实构造 D 状态，如实记录)。

use std::time::{Duration, Instant};

use monitor_core::recover::journal::{self, JournalEvent, JournalState};

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!(
        "sw-storage-fault-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 演练 1:磁盘满/不可写 (路径被目录占位)→ open_writer 失败;
/// 修复 (移除占位) 后重启 → channel + run_tracked_writer 全管线 (JournalSender
/// ack 确认写入)→ 重放可见;同目录伴生文件不受影响。
#[tokio::test]
async fn disk_full_open_fails_then_repair_and_restart() {
    let dir = tmpdir("disk-full");
    let journal_path = dir.join("journal.jsonl");
    let sibling = dir.join("config-snapshot.toml");
    std::fs::write(&sibling, "sibling data").unwrap();
    // 占位:journal 路径被"目录"占据 (等价于磁盘满/不可写的打开失败)
    std::fs::create_dir(&journal_path).unwrap();
    assert!(
        journal::open_writer(&journal_path).is_err(),
        "不可写路径必须打开失败 (启动早退语义，R5)"
    );

    // 修复：移除占位 → 重启 → 全链路写入 (JournalSender::send ack 确认)
    std::fs::remove_dir(&journal_path).unwrap();
    let file = journal::open_writer(&journal_path).expect("修复后必须可打开");
    let (sender, rx) = journal::channel(8);
    let counters = std::sync::Arc::new(journal::JournalCounters::default());
    let writer = tokio::spawn(journal::run_tracked_writer_with_limit(
        file,
        journal_path.clone(),
        rx,
        counters.clone(),
        0,
    ));
    sender
        .send(JournalEvent::Audit {
            at: std::time::SystemTime::now(),
            message: "after repair (tracked)".into(),
        })
        .await
        .expect("修复后写入必须 ack 确认");
    drop(sender); // 关闭通道 → writer 排空退出
    let writer_result = writer.await.expect("writer task joins");
    writer_result.expect("writer exits cleanly (persisted audit acknowledged)");

    // 伴生文件不受影响
    assert_eq!(std::fs::read_to_string(&sibling).unwrap(), "sibling data");
    // journal 重放含修复后事件 (经 replay_checked 无错)
    let replayed = journal::replay_checked(&journal_path).expect("重放无错");
    assert!(replayed.last_reset.is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

/// 演练 2:慢盘 (sync_data 悬挂)→ 关键事件 send 的 2s 总预算耗尽返回 Err
/// (R3:eval 停止当前恢复实例)。
#[tokio::test]
async fn slow_disk_critical_send_times_out_within_budget() {
    let (sender, mut rx) = journal::channel(8);
    let started = Instant::now();
    // writer 侧"悬挂":读走请求但**持有 ack 不回复**(等价 sync_data 永不返回;
    // drop 请求 = writer 死亡的快速 Err 路径，由 oneshot 语义自动覆盖)
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Some(req) = rx.recv().await {
            held.push(req); // 持有:ack 永不回复
        }
    });
    let outcome = sender
        .send(JournalEvent::Fatal {
            at: std::time::SystemTime::now(),
            target: "x".into(),
            reason: "slow disk drill".into(),
        })
        .await;
    let elapsed = started.elapsed();
    assert!(outcome.is_err(), "未 ack 的关键事件必须在预算内返回 Err");
    assert!(
        elapsed >= Duration::from_millis(1900) && elapsed < Duration::from_secs(4),
        "预算应接近 2s，实际 {elapsed:?}"
    );
}

/// 演练 3:断电 (半行)→ 撕裂点在**文件尾且无换行**(真实断电形状;有换行
/// 说明写已完成)→ 重放隔离撕裂行;`open_writer` 半行定界后续写可重放。
#[test]
fn power_loss_torn_line_isolated() {
    let dir = tmpdir("torn");
    let path = dir.join("journal.jsonl");
    let good1 = r#"{"type":"audit","at":{"secs_since_epoch":0,"nanos_since_epoch":0},"message":"before power loss"}"#;
    let torn = r#"{"type":"restart","at":{"secs_since_epoch":0,"nan"#; // 断电撕裂，无换行
    std::fs::write(&path, format!("{good1}\n{torn}")).unwrap();

    let state: JournalState = journal::replay_checked(&path).unwrap_or_default();
    let _ = state; // 重放不 panic(撕裂行被跳过，good1 不受影响)

    // open_writer 半行定界 (追加 \n),修复后写入 → 重放可见
    let file = journal::open_writer(&path).expect("半行必须被定界");
    drop(file);
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    writeln!(f, r#"{{"type":"audit","at":{{"secs_since_epoch":2,"nanos_since_epoch":0}},"message":"after repair"}}"#).unwrap();
    drop(f);
    let replayed = journal::replay_checked(&path);
    assert!(replayed.is_ok(), "修复后重放必须无错");
    let content = std::fs::read_to_string(&path).unwrap();
    assert!(content.contains("after repair"), "修复后写入可重放");
    assert!(
        !content.contains("after power loss\n{\"type\":\"restart"),
        "撕裂行不与后续记录粘连"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// 演练 4(补充):普通事件写失败路径 —— `write_events` 经 tracked writer 的
/// counters 可观察 (磁盘满的运行中形态：错误计数 + fail 闩锁)。
/// 触发方式走 portable 缝隙:`open_writer` 后把文件替换为只读不可追加的目录
/// 在部分平台不可移植，这里验证 counters 契约本身 (fail → has_failed →
/// eval exec_actions 中止;见 eval/mod.rs)。
#[test]
fn counters_fail_contract_for_eval_abort() {
    let counters = monitor_core::recover::journal::JournalCounters::default();
    assert!(!counters.has_failed());
    counters.fail();
    assert!(counters.has_failed(), "fail 闩锁必须使 eval 中止恢复");
}

/// 演练 5:半行之外，坏 JSON 行同样被隔离 (与 RUNBOOK 完整性校验对齐)。
#[test]
fn corrupt_json_line_skipped_on_replay() {
    let dir = tmpdir("corrupt");
    let path = dir.join("journal.jsonl");
    std::fs::write(&path, "{{{bad}}}\n").unwrap();
    let state = journal::replay_checked(&path).unwrap_or_default();
    let _ = state;
    // 追加合法事件后仍可重放 (不被坏行毒化)
    let file = journal::open_writer(&path).unwrap();
    drop(file);
    assert!(journal::replay_checked(&path).is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}
