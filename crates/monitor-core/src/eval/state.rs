//! 检查状态机(DESIGN.md §7.1)。纯逻辑,时间由调用方以单调 `Instant` 注入。
//!
//! ```text
//! Ok ──1 次失败──▶ Warning ──连续 failure_threshold 次失败──▶ Critical
//!  ▲                 │                 │
//!  └──连续 success_threshold 次成功──┴───────────────┘
//! ```

use crate::model::control::HealthLevel;

/// 状态机参数。
#[derive(Debug, Clone, Copy)]
pub struct Thresholds {
    pub failure_threshold: u32,
    pub success_threshold: u32,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            failure_threshold: 3,
            success_threshold: 2,
        }
    }
}

#[derive(Debug)]
pub struct CheckStateMachine {
    thresholds: Thresholds,
    level: HealthLevel,
    consecutive_failures: u32,
    consecutive_successes: u32,
}

impl CheckStateMachine {
    pub fn new(thresholds: Thresholds) -> Self {
        let failure_threshold = thresholds.failure_threshold.max(1);
        let success_threshold = thresholds.success_threshold.max(1);
        Self {
            thresholds: Thresholds {
                failure_threshold,
                success_threshold,
            },
            level: HealthLevel::Ok,
            consecutive_failures: 0,
            consecutive_successes: 0,
        }
    }

    pub fn level(&self) -> HealthLevel {
        self.level
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// 观测一次探测结果,返回发生的级别转移(无转移则 `None`)。
    pub fn observe(&mut self, ok: bool) -> Option<HealthLevel> {
        let prev = self.level;
        if ok {
            self.consecutive_failures = 0;
            self.consecutive_successes = self.consecutive_successes.saturating_add(1);
            if self.level != HealthLevel::Ok
                && self.consecutive_successes >= self.thresholds.success_threshold
            {
                self.level = HealthLevel::Ok;
                self.consecutive_successes = 0;
            }
        } else {
            self.consecutive_successes = 0;
            self.consecutive_failures = self.consecutive_failures.saturating_add(1);
            match self.level {
                HealthLevel::Ok => {
                    self.level = HealthLevel::Warning;
                }
                HealthLevel::Warning => {
                    if self.consecutive_failures >= self.thresholds.failure_threshold {
                        self.level = HealthLevel::Critical;
                    }
                }
                HealthLevel::Critical => {}
            }
        }
        (self.level != prev).then_some(self.level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sm(failure: u32, success: u32) -> CheckStateMachine {
        CheckStateMachine::new(Thresholds {
            failure_threshold: failure,
            success_threshold: success,
        })
    }

    #[test]
    fn one_failure_enters_warning() {
        let mut s = sm(3, 2);
        assert_eq!(s.observe(false), Some(HealthLevel::Warning));
    }

    #[test]
    fn escalation_after_threshold() {
        let mut s = sm(3, 2);
        assert_eq!(s.observe(false), Some(HealthLevel::Warning));
        assert_eq!(s.observe(false), None);
        assert_eq!(s.observe(false), Some(HealthLevel::Critical));
        // Critical 内继续失败不再转移
        assert_eq!(s.observe(false), None);
    }

    #[test]
    fn recovery_requires_consecutive_successes() {
        let mut s = sm(3, 2);
        s.observe(false);
        s.observe(false);
        s.observe(false); // Critical
        assert_eq!(s.observe(true), None); // 1 次成功:不恢复
        assert_eq!(s.level(), HealthLevel::Critical);
        assert_eq!(s.observe(true), Some(HealthLevel::Ok)); // 连续第 2 次:恢复
    }

    #[test]
    fn success_resets_failure_streak() {
        let mut s = sm(3, 2);
        s.observe(false);
        s.observe(true); // 中断连败
        assert_eq!(s.consecutive_failures(), 0);
        assert_eq!(s.level(), HealthLevel::Warning); // 仍在 Warning(未恢复)
        assert_eq!(s.observe(false), None); // 重新计数,不升级
    }

    #[test]
    fn warning_resolves_to_ok() {
        let mut s = sm(3, 2);
        s.observe(false); // Warning
        assert_eq!(s.observe(true), None);
        assert_eq!(s.observe(true), Some(HealthLevel::Ok));
    }
}
