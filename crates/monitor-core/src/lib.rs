//! `monitor-core` — 跨平台服务与硬件监控库。
//!
//! 数据流单向:`collect → eval → alert / recover`(DESIGN.md §2)。
//! 全部状态机由单一 eval 任务独占 (零锁单写者);跨任务只传不可变消息。

pub mod alert;
pub mod collect;
pub mod config;
pub mod control;
pub mod error;
pub mod eval;
/// F39:指标历史持久化 (顶层模块 —— 观测数据出口，不属 alert/恢复域)。
pub mod metrics_history;
pub mod model;
pub mod recover;
pub mod runtime;
