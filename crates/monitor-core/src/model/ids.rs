//! 进程身份:newtype 与 `(pid, start_time)` 二元组(DESIGN.md §8.3,PID 复用防护)。

use serde::{Deserialize, Serialize};

/// 进程 ID newtype,不向公共 API 泄漏 `sysinfo::Pid`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProcessId(pub u32);

/// 进程身份:PID + 启动时间(epoch 秒)。
///
/// 一切存活判定、停止、接管动作前必须校验此二元组;不匹配视为 PID 已复用。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcessIdentity {
    pub pid: ProcessId,
    pub start_time: u64,
}

impl ProcessIdentity {
    pub fn new(pid: u32, start_time: u64) -> Self {
        Self {
            pid: ProcessId(pid),
            start_time,
        }
    }
}

impl std::fmt::Display for ProcessIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pid={} start={}", self.pid.0, self.start_time)
    }
}
