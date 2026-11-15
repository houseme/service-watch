//! 告警事件模型与 incident key(DESIGN.md §7.2)。
//!
//! 关键不变量:`incident = hash(target, check)`,**不含 severity/reason**——
//! 升级(Warning→Critical)不产生“第二起事故”,reason 漂移不产生新 fingerprint。

use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Warning,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Category {
    /// 新 incident 首次通知(Warning 或直接 Critical)。
    Alert,
    /// Warning → Critical 升级。
    Escalate,
    /// incident 恢复(引用原 incident;仅当曾发过通知时产生)。
    Resolve,
    /// flap 抑制摘要。
    Flap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    /// incident key = hash(target, check)。
    pub incident: u64,
    pub category: Category,
    /// Resolve 事件携带原 incident 的最高 severity。
    pub severity: Severity,
    pub target: String,
    pub check: String,
    /// 人类可读原因;不参与去噪。
    pub reason: String,
    pub observed: Option<String>,
    pub first_seen: SystemTime,
    pub last_seen: SystemTime,
    pub occurrences: u64,
}

impl AlertEvent {
    /// 结构化日志字段名约定(DESIGN.md §10)。
    pub fn incident_key(target: &str, check: &str) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        target.hash(&mut h);
        check.hash(&mut h);
        h.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incident_key_ignores_severity_and_reason() {
        let a = AlertEvent::incident_key("web-1", "api-health");
        let b = AlertEvent::incident_key("web-1", "api-health");
        let c = AlertEvent::incident_key("web-1", "tcp-probe");
        let d = AlertEvent::incident_key("web-2", "api-health");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
    }
}
