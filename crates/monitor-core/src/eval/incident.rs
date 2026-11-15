//! incident 去噪与生命周期(DESIGN.md §7.2 —— SRE review 修订的核心)。
//!
//! 规则(实现代理 B 必须逐条覆盖单测):
//! 1. incident key = hash(target, check),不含 severity/reason;
//! 2. incident 首次通知**永不抑制**;
//! 3. 持续期同 severity 重复通知受 `renotify_interval` 抑制;Escalate 总是发送;
//! 4. Resolve 引用原 incident;仅当该 incident 曾发出过通知时才产生;
//! 5. flap:`flap_window` 内 open/close ≥ `flap_threshold` → 抑制,只发一条
//!    `Flap` 摘要(含计数);抑制期随窗口滑动自然解除。
//!
//! ## 调用约定
//!
//! [`IncidentTracker::observe`] 在**每次探测完成后**以检查状态机的**当前级别**调用
//! (不必只在级别转移时调用):同一级别重复观测由 renotify 间隔判定是否重发,
//! `Ok` 表示本次观测恢复。两种喂法(仅转移 / 每轮喂)均正确;每轮喂才能触发 renotify。
//!
//! ## 时钟纪律(DESIGN §5)
//!
//! `now_mono`(单调)用于一切时长判定(renotify 间隔、flap 窗口/解除)——
//! 墙钟回拨会永久抑制重通知、前跳会造成虚假重发(并发评审 P1);`now_wall`
//! 仅作事件时间戳(first_seen/last_seen 展示)。
//!
//! ## 实现取舍(DESIGN §7.2 规则 5 细化)
//!
//! - 进入 flap 抑制的那一刻,与跨越阈值那次转移同时发生的 Alert/Resolve 被**吞掉**,
//!   只发那一条 `Flap` 摘要;抑制期内 Escalate 同样吞掉(trail 状态仍照常更新,
//!   解除后可据此发出正确 severity 的通知)。
//! - `Flap` 摘要**不计入** `notified`:只听说过"在抖动"的观察者不应收到一条凭空的 Resolve。
//! - incident 关闭(Resolve)即从 trails 移除:复发是**新 incident**,首次通知不受
//!   上一起的 `last_notified` 冷却影响(规则 2 的关键)。open/close 时间戳留在
//!   `transitions` 环里供 flap 判定跨 incident 累计。

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Instant, SystemTime};

use crate::config::AlertCfg;
use crate::model::alert::{AlertEvent, Category, Severity};
use crate::model::control::HealthLevel;

/// transitions 环的硬上限(防御病态抖动下的内存增长;窗口内计数才是判定依据)。
const TRANSITIONS_RING_CAP: usize = 1024;

/// 每个 incident 的运行时轨迹(open 期间存在,Resolve 即移除)。
#[derive(Debug, Clone)]
struct IncidentTrail {
    /// 当前级别(供 /status 与 renotify severity)。
    level: HealthLevel,
    /// 本次 incident 生存期内达到过的最高级别(Resolve 携带其 severity)。
    peak: HealthLevel,
    /// 打开以来失败观测次数。
    occurrences: u64,
    first_seen: SystemTime,
    last_seen: SystemTime,
    /// 已对外发出过通知(Alert/Escalate;决定 Resolve 是否产生)。
    notified: bool,
    /// 上次通知时刻(renotify 间隔判定;单调时钟)。
    last_notified: Option<Instant>,
}

#[derive(Debug, Default)]
pub struct IncidentTracker {
    cfg: AlertCfg,
    trails: HashMap<u64, IncidentTrail>,
    /// flap 环形记录:key → (open/close 时刻),仅保留 flap_window 内。
    /// (单调时刻用于窗口判定,墙钟用于 Flap 摘要的 first_seen 展示。)
    transitions: HashMap<u64, VecDeque<(Instant, SystemTime)>>,
    flap_suppressed: HashSet<u64>,
}

/// 健康级别排序(Ok < Warning < Critical)。
fn rank(level: HealthLevel) -> u8 {
    match level {
        HealthLevel::Ok => 0,
        HealthLevel::Warning => 1,
        HealthLevel::Critical => 2,
    }
}

fn severity_of(level: HealthLevel) -> Severity {
    match level {
        HealthLevel::Critical => Severity::Critical,
        HealthLevel::Warning | HealthLevel::Ok => Severity::Warning,
    }
}

/// 一次观测的定位信息(incident key + 事件标签),收敛内部分派函数的参数数量。
struct Observed<'a> {
    key: u64,
    target: &'a str,
    check: &'a str,
}

impl IncidentTracker {
    pub fn new(cfg: AlertCfg) -> Self {
        Self {
            cfg,
            trails: HashMap::new(),
            transitions: HashMap::new(),
            flap_suppressed: HashSet::new(),
        }
    }

    /// 观测一次级别(来自检查状态机;Ok 表示无 incident)。
    /// 返回本次需要投递的事件列表(已应用全部去噪规则)。
    #[allow(clippy::too_many_arguments)]
    pub fn observe(
        &mut self,
        now_wall: SystemTime,
        now_mono: Instant,
        target: &str,
        check: &str,
        level: HealthLevel,
        reason: &str,
        observed: Option<String>,
    ) -> Vec<AlertEvent> {
        let ctx = Observed {
            key: AlertEvent::incident_key(target, check),
            target,
            check,
        };
        match level {
            HealthLevel::Ok => self.observe_ok(&ctx, now_wall, now_mono, reason, observed),
            HealthLevel::Warning | HealthLevel::Critical => {
                self.observe_failure(&ctx, now_wall, now_mono, level, reason, observed)
            }
        }
    }

    /// 当前未闭合 incident 的最高级别(供 /status)。
    pub fn incident_level(&self, target: &str, check: &str) -> Option<HealthLevel> {
        let key = AlertEvent::incident_key(target, check);
        self.trails.get(&key).map(|t| t.level)
    }

    /// 清除指定 (target, check) 的全部去噪状态(trail/flap 环/抑制),
    /// 供控制平面 reset 使用:清除后立即复发按**新 incident** 处理。
    pub fn clear(&mut self, target: &str, check: &str) {
        let key = AlertEvent::incident_key(target, check);
        self.trails.remove(&key);
        self.transitions.remove(&key);
        self.flap_suppressed.remove(&key);
    }

    // -----------------------------------------------------------------
    // Ok 观测:闭合 trail → 可能 Resolve / flap 跨越
    // -----------------------------------------------------------------

    fn observe_ok(
        &mut self,
        ctx: &Observed<'_>,
        now: SystemTime,
        now_mono: Instant,
        reason: &str,
        observed: Option<String>,
    ) -> Vec<AlertEvent> {
        let mut events = Vec::new();
        self.prune_and_maybe_release(ctx.key, now_mono);

        if let Some(trail) = self.trails.remove(&ctx.key) {
            // close 转移:计入 flap 环。
            self.record_transition(ctx.key, now, now_mono);
            let count = self.window_count(ctx.key, now_mono);
            let newly_suppressed =
                count >= self.flap_threshold() && self.flap_suppressed.insert(ctx.key);
            if newly_suppressed {
                // 跨越阈值的一刻:吞掉 Resolve,只发 Flap 摘要。
                events.push(self.flap_event(ctx, now, count, Some(&trail)));
            } else if !self.flap_suppressed.contains(&ctx.key) {
                // 规则 4:仅当曾发出过通知才产生 Resolve;severity 用历史最高级别。
                if trail.notified {
                    events.push(AlertEvent {
                        incident: ctx.key,
                        category: Category::Resolve,
                        severity: severity_of(trail.peak),
                        target: ctx.target.to_string(),
                        check: ctx.check.to_string(),
                        reason: reason.to_string(),
                        observed,
                        first_seen: trail.first_seen,
                        last_seen: now,
                        occurrences: trail.occurrences,
                    });
                }
            }
            // 抑制期内:Resolve 吞掉(trail 已移除 = incident 已闭合)。
        }
        events
    }

    // -----------------------------------------------------------------
    // Warning/Critical 观测:open / 持续(renotify)/ 升级
    // -----------------------------------------------------------------

    fn observe_failure(
        &mut self,
        ctx: &Observed<'_>,
        now: SystemTime,
        now_mono: Instant,
        level: HealthLevel,
        reason: &str,
        observed: Option<String>,
    ) -> Vec<AlertEvent> {
        let mut events = Vec::new();
        self.prune_and_maybe_release(ctx.key, now_mono);
        let suppressed = self.flap_suppressed.contains(&ctx.key);

        if let Some(trail) = self.trails.get_mut(&ctx.key) {
            // 持续期:累计 + 判定升级。
            trail.occurrences = trail.occurrences.saturating_add(1);
            trail.last_seen = now;
            let escalated = level == HealthLevel::Critical && trail.level == HealthLevel::Warning;
            if rank(level) > rank(trail.level) {
                trail.level = level;
            }
            if rank(level) > rank(trail.peak) {
                trail.peak = level;
            }

            if !suppressed {
                if escalated {
                    if trail.notified {
                        // 规则 3:Escalate 总是发送(不受 renotify 间隔约束)。
                        events.push(make_event(
                            ctx.key,
                            Category::Escalate,
                            Severity::Critical,
                            ctx.target,
                            ctx.check,
                            reason,
                            observed,
                            trail,
                            now,
                        ));
                    } else {
                        // 升级但从未通知过(如抑制期吞掉了首次 Alert):按首次通知发。
                        events.push(make_event(
                            ctx.key,
                            Category::Alert,
                            Severity::Critical,
                            ctx.target,
                            ctx.check,
                            reason,
                            observed,
                            trail,
                            now,
                        ));
                    }
                    trail.notified = true;
                    trail.last_notified = Some(now_mono);
                } else if !trail.notified {
                    // 规则 2:首次通知永不抑制(抑制解除后补发的首次通知也走这里)。
                    let severity = severity_of(trail.level);
                    events.push(make_event(
                        ctx.key,
                        Category::Alert,
                        severity,
                        ctx.target,
                        ctx.check,
                        reason,
                        observed,
                        trail,
                        now,
                    ));
                    trail.notified = true;
                    trail.last_notified = Some(now_mono);
                } else if let Some(last) = trail.last_notified {
                    // 规则 3:同 severity 持续期重发受 renotify_interval 抑制(单调时钟)。
                    if now_mono.duration_since(last) >= self.cfg.renotify_interval {
                        let severity = severity_of(trail.level);
                        events.push(make_event(
                            ctx.key,
                            Category::Alert,
                            severity,
                            ctx.target,
                            ctx.check,
                            reason,
                            observed,
                            trail,
                            now,
                        ));
                        trail.last_notified = Some(now_mono);
                    }
                }
            }
            // 抑制期内:所有通知吞掉,trail 状态已更新。
        } else {
            // open 转移:计入 flap 环并新建 trail。
            self.record_transition(ctx.key, now, now_mono);
            let count = self.window_count(ctx.key, now_mono);
            let trail = IncidentTrail {
                level,
                peak: level,
                occurrences: 1,
                first_seen: now,
                last_seen: now,
                notified: false,
                last_notified: None,
            };
            self.trails.insert(ctx.key, trail);

            let newly_suppressed =
                count >= self.flap_threshold() && self.flap_suppressed.insert(ctx.key);
            if newly_suppressed {
                // 跨越阈值的一刻:吞掉本次 Alert,只发 Flap 摘要(notified 保持 false)。
                events.push(self.flap_event(ctx, now, count, None));
            } else if !suppressed {
                // 规则 2:首次通知永不抑制 —— 不受任何历史 last_notified 冷却影响。
                let trail = self.trails.get_mut(&ctx.key).expect("just inserted");
                let severity = severity_of(level);
                events.push(make_event(
                    ctx.key,
                    Category::Alert,
                    severity,
                    ctx.target,
                    ctx.check,
                    reason,
                    observed,
                    trail,
                    now,
                ));
                trail.notified = true;
                trail.last_notified = Some(now_mono);
            }
            // 已处于抑制期:open 的 Alert 吞掉。
        }
        events
    }

    // -----------------------------------------------------------------
    // 内部工具
    // -----------------------------------------------------------------

    /// flap 阈值(下限 1,防配置为 0 时恒抑制)。
    fn flap_threshold(&self) -> usize {
        self.cfg.flap_threshold.max(1) as usize
    }

    /// 记录一次 open/close 转移时间戳(单调 + 墙钟)。
    fn record_transition(&mut self, key: u64, at_wall: SystemTime, at_mono: Instant) {
        let ring = self.transitions.entry(key).or_default();
        ring.push_back((at_mono, at_wall));
        while ring.len() > TRANSITIONS_RING_CAP {
            ring.pop_front();
        }
    }

    /// 窗口内 open+close 转移总数(单调时钟判定;调用前应已 prune)。
    fn window_count(&self, key: u64, now_mono: Instant) -> usize {
        self.transitions
            .get(&key)
            .map(|ring| {
                ring.iter()
                    .filter(|&&(t, _)| now_mono.duration_since(t) <= self.cfg.flap_window)
                    .count()
            })
            .unwrap_or(0)
    }

    /// 滑出窗口的旧转移;若抑制中且窗口内计数已低于阈值 → 自动解除(规则 5)。
    fn prune_and_maybe_release(&mut self, key: u64, now_mono: Instant) {
        if let Some(ring) = self.transitions.get_mut(&key) {
            while let Some(&(t, _)) = ring.front() {
                if now_mono.duration_since(t) > self.cfg.flap_window {
                    ring.pop_front();
                } else {
                    break;
                }
            }
            if ring.is_empty() {
                self.transitions.remove(&key);
            }
        }
        if self.flap_suppressed.contains(&key)
            && self.window_count(key, now_mono) < self.flap_threshold()
        {
            self.flap_suppressed.remove(&key);
        }
    }

    /// Flap 摘要事件(category=Flap,occurrences=窗口内转移数)。
    /// first_seen 取窗口内最早转移时刻(摘要覆盖整段抖动);severity 取当前
    /// trail 的历史最高级别(无 trail 时 Warning 兜底)。
    fn flap_event(
        &self,
        ctx: &Observed<'_>,
        now: SystemTime,
        count: usize,
        trail: Option<&IncidentTrail>,
    ) -> AlertEvent {
        let severity = trail
            .map(|t| severity_of(t.peak))
            .unwrap_or(Severity::Warning);
        let window_first = self
            .transitions
            .get(&ctx.key)
            .and_then(|ring| ring.front().map(|&(_, wall)| wall));
        let first_seen = window_first
            .or_else(|| trail.map(|t| t.first_seen))
            .unwrap_or(now);
        AlertEvent {
            incident: ctx.key,
            category: Category::Flap,
            severity,
            target: ctx.target.to_string(),
            check: ctx.check.to_string(),
            reason: format!(
                "flap detected: {count} open/close transitions within {:?}",
                self.cfg.flap_window
            ),
            observed: None,
            first_seen,
            last_seen: now,
            occurrences: count as u64,
        }
    }
}

/// 通用事件构造(字段取自 trail 当前状态;自由函数以便在 `get_mut` 作用域内调用)。
#[allow(clippy::too_many_arguments)]
fn make_event(
    key: u64,
    category: Category,
    severity: Severity,
    target: &str,
    check: &str,
    reason: &str,
    observed: Option<String>,
    trail: &IncidentTrail,
    now: SystemTime,
) -> AlertEvent {
    AlertEvent {
        incident: key,
        category,
        severity,
        target: target.to_string(),
        check: check.to_string(),
        reason: reason.to_string(),
        observed,
        first_seen: trail.first_seen,
        last_seen: now,
        occurrences: trail.occurrences,
    }
}

// ---------------------------------------------------------------------------
// 测试:与 §7.2 规则一一对应(时间全部注入,不依赖真实时钟)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;
    use std::time::Duration;

    /// 单调时钟测试基点(进程内恒定,与墙钟注入同刻度推进)。
    fn m0_base() -> Instant {
        static M0: OnceLock<Instant> = OnceLock::new();
        *M0.get_or_init(Instant::now)
    }

    /// 构造 AlertCfg:renotify 间隔 / flap 窗口 / flap 阈值可调,其余默认。
    fn cfg(renotify_secs: u64, flap_window_secs: u64, flap_threshold: u32) -> AlertCfg {
        AlertCfg {
            renotify_interval: Duration::from_secs(renotify_secs),
            flap_window: Duration::from_secs(flap_window_secs),
            flap_threshold,
            ..Default::default()
        }
    }

    /// 注入时刻:`SystemTime::UNIX_EPOCH + secs`(墙钟,事件时间戳用)。
    fn t(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    /// 注入时刻(单调,时长判定用):以真实 Instant 为基,与墙钟同刻度推进。
    fn mono(secs: u64) -> Instant {
        m0_base() + Duration::from_secs(secs)
    }

    // 规则 1:incident key = hash(target, check),不含 severity/reason;
    // 升级不产生"第二起事故"。
    #[test]
    fn rule1_escalation_keeps_same_incident_key() {
        let mut tr = IncidentTracker::new(cfg(900, 3600, 100));
        let first = tr.observe(
            t(0),
            mono(0),
            "web",
            "http",
            HealthLevel::Warning,
            "timeout",
            None,
        );
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].category, Category::Alert);
        let key = first[0].incident;

        let escalate = tr.observe(
            t(1),
            mono(1),
            "web",
            "http",
            HealthLevel::Critical,
            "conn refused",
            None,
        );
        assert_eq!(escalate.len(), 1);
        assert_eq!(escalate[0].category, Category::Escalate);
        assert_eq!(escalate[0].incident, key, "升级必须沿用同一 incident key");

        // 不同 check → 不同 key(同一 target)。
        let other = tr.observe(t(2), mono(2), "web", "tcp", HealthLevel::Warning, "x", None);
        assert_eq!(other.len(), 1);
        assert_ne!(other[0].incident, key);
    }

    // 规则 2:首次通知永不抑制 —— Resolve 后立刻复发,新 Alert 不受上一起
    // incident 的 last_notified 冷却影响。
    #[test]
    fn rule2_recurrence_right_after_resolve_alerts_immediately() {
        let mut tr = IncidentTracker::new(cfg(900, 3600, 100));
        tr.observe(
            t(0),
            mono(0),
            "web",
            "c",
            HealthLevel::Warning,
            "fail",
            None,
        );
        tr.observe(
            t(1),
            mono(1),
            "web",
            "c",
            HealthLevel::Critical,
            "fail",
            None,
        );
        let resolve = tr.observe(t(2), mono(2), "web", "c", HealthLevel::Ok, "ok", None);
        assert_eq!(resolve.len(), 1);
        assert_eq!(resolve[0].category, Category::Resolve);
        assert_eq!(
            resolve[0].severity,
            Severity::Critical,
            "Resolve 携带历史最高 severity"
        );

        // 距上次通知仅 1s,远小于 renotify_interval(900s):复发必须立即通知。
        let again = tr.observe(
            t(3),
            mono(3),
            "web",
            "c",
            HealthLevel::Warning,
            "fail",
            None,
        );
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].category, Category::Alert);
        assert_eq!(again[0].occurrences, 1, "新 incident 从 1 重新累计");
        assert_eq!(tr.incident_level("web", "c"), Some(HealthLevel::Warning));
    }

    // 规则 3:持续期同 severity 重复通知受 renotify_interval 抑制。
    #[test]
    fn rule3_renotify_gated_by_interval() {
        let mut tr = IncidentTracker::new(cfg(60, 3600, 100));
        assert!(
            !tr.observe(t(0), mono(0), "a", "c", HealthLevel::Warning, "fail", None)
                .is_empty()
        );

        // 间隔内重复观测:抑制。
        assert!(
            tr.observe(
                t(30),
                mono(30),
                "a",
                "c",
                HealthLevel::Warning,
                "fail",
                None
            )
            .is_empty(),
            "renotify_interval 内的重发必须被抑制"
        );

        // 超过间隔(61s ≥ 60s):同 severity 重发。
        let re = tr.observe(
            t(61),
            mono(61),
            "a",
            "c",
            HealthLevel::Warning,
            "fail",
            None,
        );
        assert_eq!(re.len(), 1);
        assert_eq!(re[0].category, Category::Alert);
        assert_eq!(re[0].severity, Severity::Warning);
        assert_eq!(re[0].occurrences, 3);

        // 重发后重新计时:间隔内再次抑制。
        assert!(
            tr.observe(
                t(90),
                mono(90),
                "a",
                "c",
                HealthLevel::Warning,
                "fail",
                None
            )
            .is_empty()
        );
    }

    // 规则 3 边界:恰好等于 renotify_interval 必须重发(锁定 >= 语义,杀伤 > 变异)。
    #[test]
    fn rule3_renotify_at_exact_interval() {
        let mut tr = IncidentTracker::new(cfg(60, 3600, 100));
        assert!(
            !tr.observe(t(0), mono(0), "a", "c", HealthLevel::Warning, "fail", None)
                .is_empty()
        );
        // interval-1:抑制
        assert!(
            tr.observe(
                t(59),
                mono(59),
                "a",
                "c",
                HealthLevel::Warning,
                "fail",
                None
            )
            .is_empty()
        );
        // 恰好 interval:重发
        let re = tr.observe(
            t(60),
            mono(60),
            "a",
            "c",
            HealthLevel::Warning,
            "fail",
            None,
        );
        assert_eq!(re.len(), 1, "恰好 renotify_interval 必须重发(>= 边界)");
        // 重发后 interval-1:再次抑制
        assert!(
            tr.observe(
                t(119),
                mono(119),
                "a",
                "c",
                HealthLevel::Warning,
                "fail",
                None
            )
            .is_empty()
        );
    }

    // 规则 3 补充:Escalate(Warning → Critical)总是发送,不受 renotify 间隔约束。
    #[test]
    fn escalate_always_sent_despite_recent_notification() {
        let mut tr = IncidentTracker::new(cfg(900, 3600, 100));
        tr.observe(t(0), mono(0), "a", "c", HealthLevel::Warning, "fail", None);
        // 距首次通知仅 1s:升级仍必须发送。
        let esc = tr.observe(t(1), mono(1), "a", "c", HealthLevel::Critical, "fail", None);
        assert_eq!(esc.len(), 1);
        assert_eq!(esc[0].category, Category::Escalate);
        assert_eq!(esc[0].severity, Severity::Critical);
        assert_eq!(tr.incident_level("a", "c"), Some(HealthLevel::Critical));
    }

    // 规则 4:Resolve 引用原 incident,仅当曾发出过通知时才产生。
    // 通过 flap 抑制构造"从未通知过"的 incident(threshold=3:open2 跨越阈值,
    // Alert 被吞 → trail.notified = false → close 不产生 Resolve)。
    #[test]
    fn rule4_resolve_only_when_previously_notified() {
        let mut tr = IncidentTracker::new(cfg(900, 3600, 3));
        // open1:count=1 < 3 → 正常 Alert。
        let e1 = tr.observe(t(0), mono(0), "a", "c", HealthLevel::Warning, "fail", None);
        assert_eq!(e1.len(), 1);
        assert_eq!(e1[0].category, Category::Alert);
        // close1:count=2 < 3,已通知 → Resolve。
        let e2 = tr.observe(t(1), mono(1), "a", "c", HealthLevel::Ok, "ok", None);
        assert_eq!(e2.len(), 1);
        assert_eq!(e2[0].category, Category::Resolve);
        // open2:count=3 → 跨越阈值 → Flap 摘要,Alert 被吞。
        let e3 = tr.observe(t(2), mono(2), "a", "c", HealthLevel::Warning, "fail", None);
        assert_eq!(e3.len(), 1);
        assert_eq!(e3[0].category, Category::Flap);
        // close2:处于抑制期且从未通知过 → 无 Resolve。
        assert!(
            tr.observe(t(3), mono(3), "a", "c", HealthLevel::Ok, "ok", None)
                .is_empty(),
            "未通知过的 incident 不得产生 Resolve"
        );
        // 抑制期内继续抖动:Alert/Resolve 全部吞掉,不再重复发 Flap。
        assert!(
            tr.observe(t(4), mono(4), "a", "c", HealthLevel::Warning, "fail", None)
                .is_empty()
        );
        assert!(
            tr.observe(t(5), mono(5), "a", "c", HealthLevel::Ok, "ok", None)
                .is_empty()
        );
        // 窗口滑出 → 自动解除 → 新 incident 首次通知恢复正常。
        let e7 = tr.observe(
            t(4000),
            mono(4000),
            "a",
            "c",
            HealthLevel::Warning,
            "fail",
            None,
        );
        assert_eq!(e7.len(), 1);
        assert_eq!(e7[0].category, Category::Alert);
    }

    // 规则 5:flap 抑制的进入、期内吞没、随窗口滑动自动解除。
    #[test]
    fn rule5_flap_suppression_and_release() {
        let mut tr = IncidentTracker::new(cfg(900, 100, 3));
        let e1 = tr.observe(t(0), mono(0), "a", "c", HealthLevel::Warning, "fail", None);
        assert_eq!(e1[0].category, Category::Alert);
        let e2 = tr.observe(t(1), mono(1), "a", "c", HealthLevel::Ok, "ok", None);
        assert_eq!(e2[0].category, Category::Resolve);
        let e3 = tr.observe(t(2), mono(2), "a", "c", HealthLevel::Warning, "fail", None);
        assert_eq!(e3.len(), 1, "跨越阈值的一刻只发 Flap 摘要");
        assert_eq!(e3[0].category, Category::Flap);
        assert_eq!(e3[0].occurrences, 3, "Flap.occurrences = 窗口内转移数");
        assert_eq!(e3[0].severity, Severity::Warning);
        assert_eq!(
            e3[0].first_seen,
            t(0),
            "Flap 引用首起 incident 的 first_seen"
        );

        // 抑制期内:Alert/Escalate/Resolve 全部吞掉,不再重复 Flap。
        assert!(
            tr.observe(t(3), mono(3), "a", "c", HealthLevel::Ok, "ok", None)
                .is_empty()
        );
        assert!(
            tr.observe(t(4), mono(4), "a", "c", HealthLevel::Warning, "fail", None)
                .is_empty()
        );
        let esc = tr.observe(t(5), mono(5), "a", "c", HealthLevel::Critical, "fail", None);
        assert!(
            esc.is_empty(),
            "抑制期内 Escalate 一并吞掉(trail 状态仍更新)"
        );
        assert_eq!(tr.incident_level("a", "c"), Some(HealthLevel::Critical));

        // 窗口滑动:t0..t5 全部滑出(window=100)→ 计数 0 < 3 → 自动解除。
        // 解除时闭合未通知过的 trail:不补发 Resolve。
        assert!(
            tr.observe(t(200), mono(200), "a", "c", HealthLevel::Ok, "ok", None)
                .is_empty()
        );
        // 解除后新 incident:首次通知不受抑制。
        let after = tr.observe(
            t(201),
            mono(201),
            "a",
            "c",
            HealthLevel::Warning,
            "fail",
            None,
        );
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].category, Category::Alert);
    }

    // occurrences / first_seen / last_seen 正确累计。
    #[test]
    fn occurrences_and_seen_timestamps() {
        let mut tr = IncidentTracker::new(cfg(60, 3600, 100));
        let open = tr.observe(
            t(10),
            mono(10),
            "a",
            "c",
            HealthLevel::Warning,
            "fail",
            None,
        );
        assert_eq!(open[0].first_seen, t(10));
        assert_eq!(open[0].last_seen, t(10));
        assert_eq!(open[0].occurrences, 1);

        assert!(
            tr.observe(
                t(20),
                mono(20),
                "a",
                "c",
                HealthLevel::Warning,
                "fail",
                None
            )
            .is_empty()
        );
        let re = tr.observe(
            t(71),
            mono(71),
            "a",
            "c",
            HealthLevel::Warning,
            "fail",
            None,
        );
        assert_eq!(re[0].occurrences, 3);
        assert_eq!(
            re[0].first_seen,
            t(10),
            "first_seen 固定为 incident 打开时刻"
        );
        assert_eq!(re[0].last_seen, t(71));

        let resolve = tr.observe(t(80), mono(80), "a", "c", HealthLevel::Ok, "ok", None);
        assert_eq!(resolve[0].occurrences, 3);
        assert_eq!(resolve[0].first_seen, t(10));
        assert_eq!(resolve[0].last_seen, t(80));
        assert_eq!(
            resolve[0].incident, open[0].incident,
            "Resolve 引用原 incident"
        );
    }

    // clear(reset 控制平面):trail/flap 环/抑制全部清空,复发按新 incident 处理。
    #[test]
    fn clear_drops_trail_and_suppression() {
        let mut tr = IncidentTracker::new(cfg(900, 3600, 3));
        tr.observe(t(0), mono(0), "a", "c", HealthLevel::Warning, "fail", None);
        tr.observe(t(1), mono(1), "a", "c", HealthLevel::Ok, "ok", None);
        let flap = tr.observe(t(2), mono(2), "a", "c", HealthLevel::Warning, "fail", None);
        assert_eq!(flap[0].category, Category::Flap);
        assert_eq!(tr.incident_level("a", "c"), Some(HealthLevel::Warning));

        tr.clear("a", "c");
        assert_eq!(
            tr.incident_level("a", "c"),
            None,
            "clear 后无未闭合 incident"
        );

        // 清除后立即复发:不受抑制与冷却影响。
        let e = tr.observe(t(3), mono(3), "a", "c", HealthLevel::Warning, "fail", None);
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].category, Category::Alert);
    }

    // 升级但从未通知过(抑制期吞掉了首次 Alert,随后解除):按首次通知补发。
    #[test]
    fn unnotified_escalation_sends_alert_not_escalate() {
        let mut tr = IncidentTracker::new(cfg(900, 3600, 3));
        tr.observe(t(0), mono(0), "a", "c", HealthLevel::Warning, "fail", None);
        tr.observe(t(1), mono(1), "a", "c", HealthLevel::Ok, "ok", None);
        tr.observe(t(2), mono(2), "a", "c", HealthLevel::Warning, "fail", None); // 跨越 → Flap,Alert 吞
        tr.observe(t(3), mono(3), "a", "c", HealthLevel::Critical, "fail", None); // 抑制内 Escalate 吞
        // 窗口滑出解除后:trail 未通知过 → 以当前(Critical)级别补发首次 Alert。
        let e = tr.observe(
            t(4000),
            mono(4000),
            "a",
            "c",
            HealthLevel::Critical,
            "fail",
            None,
        );
        assert_eq!(e.len(), 1);
        assert_eq!(
            e[0].category,
            Category::Alert,
            "从未通知过 → 首次通知而非 Escalate"
        );
        assert_eq!(e[0].severity, Severity::Critical);
    }
}
