//! F15 `request_shutdown` 回归 (第十七轮 P0，实弹证据 = F36 windows-scm 首跑：
//! `sc stop` 后服务死锁于 STOP_PENDING 60s)。
//!
//! 根因：停机 select 的 `notified()` 分支带 `if REQUESTED` 前置守卫 —— select
//! 驻留时 `REQUESTED=false` 分支被禁用，`notified()` 未注册等待者，`notify_one`
//! 只存 permit **不唤醒任务**;Windows 服务形态无 ctrl_c/eval 退出等其他唤醒
//! 源，select 永不重 poll → 死锁。修复后 permit 恒注册、体内查标志过滤残留
//! permit。本测试锁两个时序：
//!
//! ① **驻留后唤醒**(死锁形态):runtime 装配完成、停机 select 已驻留
//!    (REQUESTED=false) 才发请求 → 必须有界返回 Clean;
//! ② **残留 permit 吞噬 + 二次停机**(防御语义):run 结束后先发一次请求
//!    (无运行者，permit 滞留),再启动第二个 run(入口复位 REQUESTED)——
//!    残留 permit 必须被吞掉 (run 不得立即停),随后真实请求仍能有界停机。
//!
//! 两阶段串行于同一测试:REQUESTED/REQUEST_NOTIFY 是进程级全局态，并行测试
//! 会互踩。unix 分支在 macOS/Linux 生效，not(unix) 分支随 ci.yml Windows
//! 作业生效 —— 两形态同构，任一平台红都说明回归。

use std::time::{Duration, SystemTime};

use monitor_core::config::Config;

fn temp_data_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sw-shutdown-{tag}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn minimal_config() -> Config {
    let mut cfg = Config::from_toml_str("").unwrap();
    cfg.agent.data_dir = temp_data_dir("cfg");
    cfg
}

#[tokio::test]
async fn request_shutdown_wakes_parked_runtime_and_stale_permit_is_swallowed() {
    // ---- ① 驻留后唤醒：装配完成、select 已驻留才发请求 (丢失唤醒时序)----
    let cfg = minimal_config();
    let data_dir = cfg.agent.data_dir.clone();
    let mut first_run = tokio::spawn(monitor_core::runtime::run_with_config(cfg, None));
    // 过装配期再发请求：确保停机 select 已以 REQUESTED=false 驻留 (分支禁用
    // 正是旧代码丢失唤醒的窗口;500ms 远大于空拓扑装配耗时)。
    tokio::time::sleep(Duration::from_millis(500)).await;

    let first = monitor_core::runtime::request_shutdown();
    assert!(first, "无先前请求时首个停机请求应返回 true");

    let reason = tokio::time::timeout(Duration::from_secs(10), &mut first_run)
        .await
        .expect("request_shutdown 必须有界唤醒已驻留的 runtime(丢失唤醒回归)")
        .expect("run task must not panic")
        .expect("run must succeed");
    assert_eq!(reason, monitor_core::runtime::StopReason::Clean);
    let _ = std::fs::remove_dir_all(&data_dir);

    // ---- ② 残留 permit 吞噬:run 结束后的请求滞留 permit ----
    // 无运行者时再发一次请求 (REQUESTED=true + permit 滞留)。
    monitor_core::runtime::request_shutdown();

    let cfg2 = minimal_config();
    let data_dir2 = cfg2.agent.data_dir.clone();
    let mut second_run = tokio::spawn(monitor_core::runtime::run_with_config(cfg2, None));
    // 入口已复位 REQUESTED:残留 permit 只允许被「吞掉」—— run 不得立即停机
    // (驻留观察窗口内必须仍存活)。
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(
        !second_run.is_finished(),
        "残留 permit 必须被吞掉：第二个 run 不得被上一次的滞留请求立即停机"
    );

    // 真实请求仍有界生效。
    let first_again = monitor_core::runtime::request_shutdown();
    assert!(first_again, "复位后的首个停机请求应返回 true");
    let reason = tokio::time::timeout(Duration::from_secs(10), &mut second_run)
        .await
        .expect("二次 run 的停机请求同样必须有界生效")
        .expect("run task must not panic")
        .expect("run must succeed");
    assert_eq!(reason, monitor_core::runtime::StopReason::Clean);
    let _ = std::fs::remove_dir_all(&data_dir2);
}
