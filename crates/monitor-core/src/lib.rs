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
pub mod model;
pub mod recover;
pub mod runtime;
