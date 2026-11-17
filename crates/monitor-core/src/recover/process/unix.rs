//! Unix 进程原语：进程组 spawn 与信号 (DESIGN.md §8.4)。

use std::process::Stdio;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::ServiceCfg;
use crate::error::{CoreError, Result};
use crate::model::ids::ProcessIdentity;

/// spawn 配置的命令为**新进程组组长**(`process_group(0)`,等价 setpgid)。
/// 返回 (子进程，身份)。身份的 start_time 取 spawn 墙钟 (sysinfo 对亲生进程的
/// 校验在采集层做;这里以本地钟即可，误差容忍由 `(pid,start_time)` 双字段吸收：
/// 即便本地钟与内核记账有秒级偏差，`start_time` 仍由我们单方记录、单方比对，
/// 复用判定的一致性不受影响)。
pub fn spawn_group_leader(cfg: &ServiceCfg) -> Result<(tokio::process::Child, ProcessIdentity)> {
    let mut cmd = tokio::process::Command::new(&cfg.command);
    cmd.args(&cfg.args);
    if let Some(workdir) = &cfg.workdir {
        cmd.current_dir(workdir);
    }
    for (key, value) in &cfg.env {
        cmd.env(key, value);
    }
    cmd.process_group(0) // 子进程自成进程组:pgid == pid,killpg 足以覆盖其子孙 (§8.4)
        .kill_on_drop(false) // 观察者原则:agent 侧 drop 不连带杀服务
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null()); // 管道无人读会撑满导致目标阻塞;统一弃流
    let child = cmd.spawn()?;
    // spawn 到取 pid 之间子进程不会消失 (我们是父进程，未 reap 前我方 pid 独占);
    // 取不到 pid 属于平台异常，按 spawn 失败处理。
    let pid = child
        .id()
        .ok_or_else(|| CoreError::Recover(format!("{:?}: spawn 后未取得 pid", cfg.name)))?;
    let start_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0); // 墙钟早于 epoch(系统钟异常):夹到 0，双字段校验仍自洽
    Ok((child, ProcessIdentity::new(pid, start_time)))
}

/// 向进程组发信号 (组长 pgid == pid)。
pub fn signal_group(pgid: u32, sig: i32) -> std::io::Result<()> {
    // SAFETY:killpg 仅接收两个整型参数，无指针，async-signal-safe;pgid/sig
    // 由 ProcessManager 从 spawn 时记录的身份派生。
    let rc = unsafe { libc::killpg(pgid as libc::pid_t, sig) };
    if rc == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::ESRCH) {
        Ok(()) // 进程组已不存在 = 目标已退出，视为成功
    } else {
        Err(err)
    }
}

pub const SIGTERM: i32 = libc::SIGTERM;
pub const SIGKILL: i32 = libc::SIGKILL;
