//! 恢复策略原语：退避、滑动窗口重启预算 (DESIGN.md §8.2)。
//!
//! 预算语义:**所有自动重启均计数**(含 spawn 失败重试);窗口为滑动窗口;
//! 耗尽 → Fatal 闩锁，窗口滑过**不**自动恢复，仅控制平面 reset 解除。

use std::collections::VecDeque;
use std::time::{Duration, Instant, SystemTime};

use crate::config::RestartCfg;

/// 从配置固化策略 (校验已在 config 层完成)。
#[derive(Debug, Clone)]
pub struct RestartPolicy {
    pub min_uptime: Duration,
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
    pub max_restarts: u32,
    pub restart_window: Duration,
    pub stop_grace: Duration,
    pub startup_grace: Duration,
    pub confirmations: u32,
    pub hung_action: crate::config::HungAction,
}

impl From<&RestartCfg> for RestartPolicy {
    fn from(c: &RestartCfg) -> Self {
        Self {
            min_uptime: c.min_uptime,
            backoff_initial: c.backoff_initial,
            backoff_max: c.backoff_max,
            max_restarts: c.max_restarts,
            restart_window: c.restart_window,
            stop_grace: c.stop_grace,
            startup_grace: c.startup_grace,
            confirmations: c.confirmations.max(1),
            hung_action: c.hung_action,
        }
    }
}

/// 指数退避:`initial × 2^(attempt-1)`,封顶 max;成功运行 min_uptime 后 reset。
#[derive(Debug, Clone)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    attempt: u32,
}

impl Backoff {
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max,
            attempt: 0,
        }
    }

    pub fn from_policy(p: &RestartPolicy) -> Self {
        Self::new(p.backoff_initial, p.backoff_max)
    }

    /// 热更新延迟边界，保留已经累计的失败次数。
    pub fn update_limits(&mut self, initial: Duration, max: Duration) {
        self.initial = initial;
        self.max = max;
    }

    /// 下一次退避时长并推进计数。(不命名 `next`:避免与 `Iterator::next` 混淆)
    pub fn next_delay(&mut self) -> Duration {
        self.attempt = self.attempt.saturating_add(1);
        let mult = 2u32.saturating_pow(self.attempt.saturating_sub(1).min(30));
        self.initial.saturating_mul(mult).min(self.max)
    }

    /// 仅读取当前尝试次数。
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

/// 滑动窗口重启预算 (ring buffer)。
#[derive(Debug)]
pub struct BudgetWindow {
    max_restarts: u32,
    window: Duration,
    stamps: VecDeque<Instant>,
}

impl BudgetWindow {
    pub fn new(max_restarts: u32, window: Duration) -> Self {
        Self {
            max_restarts: max_restarts.max(1),
            window,
            stamps: VecDeque::new(),
        }
    }

    pub fn from_policy(p: &RestartPolicy) -> Self {
        Self::new(p.max_restarts, p.restart_window)
    }

    /// 热更新预算边界，保留现有时间戳，不解除 Fatal 或清空预算。
    pub fn update_limits(&mut self, max_restarts: u32, window: Duration) {
        self.max_restarts = max_restarts.max(1);
        self.window = window;
    }

    fn evict(&mut self, now: Instant) {
        while let Some(&t) = self.stamps.front() {
            if now.duration_since(t) > self.window {
                self.stamps.pop_front();
            } else {
                break;
            }
        }
    }

    /// 记录一次自动重启。
    pub fn record(&mut self, now: Instant) {
        self.evict(now);
        self.stamps.push_back(now);
    }

    /// 窗口内计数。
    pub fn count(&self, now: Instant) -> u32 {
        // evict 是 &mut;这里只读计算
        self.stamps
            .iter()
            .filter(|&&t| now.duration_since(t) <= self.window)
            .count() as u32
    }

    /// 记录后是否已耗尽。
    pub fn would_exceed(&self, now: Instant) -> bool {
        self.count(now) >= self.max_restarts
    }

    /// 清空 (reset 命令)。
    pub fn clear(&mut self) {
        self.stamps.clear();
    }

    /// 从 journal 恢复 (墙钟时间戳 → 单调 Instant)。
    /// 跨重启的时间基用线性映射近似:`mono ≈ now_mono + (t - now_wall)`;
    /// - **早于滑动窗口的记录直接丢弃**(第五轮评审 P1):journal 累积全部历史
    ///   Restart，跨 reboot/长挂起后单调钟不含离线时长,线性映射会把旧戳夹到
    ///   now 附近 → 窗口虚增、首次真实死亡即误 Fatal。窗口过滤以墙钟差为准，
    ///   与重启场景无关、确定可用。
    /// - 窗口内的戳 `checked_sub` 溢出 (极长 uptime 的罕见分支) 与未来时间戳
    ///   (墙钟回拨) 才夹到 now。
    pub fn restore(&mut self, stamps: &[SystemTime], now_wall: SystemTime, now_mono: Instant) {
        self.stamps.clear();
        for t in stamps {
            match now_wall.duration_since(*t) {
                Ok(ago) if ago > self.window => {}
                Ok(ago) => self
                    .stamps
                    .push_back(now_mono.checked_sub(ago).unwrap_or(now_mono)),
                Err(_) => self.stamps.push_back(now_mono), // 未来时间戳 (墙钟回拨):夹到 now
            }
        }
    }

    /// 导出为墙钟时间戳 (持久化用)。
    pub fn to_wall(&self, now_mono: Instant, now_wall: SystemTime) -> Vec<SystemTime> {
        self.stamps
            .iter()
            .map(|&t| {
                // Instant::duration_since 自 1.60 起饱和为零 (不 panic)
                let ago = now_mono.duration_since(t);
                now_wall.checked_sub(ago).unwrap_or(now_wall)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_caps() {
        let mut b = Backoff::new(Duration::from_secs(1), Duration::from_secs(10));
        assert_eq!(b.next_delay(), Duration::from_secs(1));
        assert_eq!(b.next_delay(), Duration::from_secs(2));
        assert_eq!(b.next_delay(), Duration::from_secs(4));
        assert_eq!(b.next_delay(), Duration::from_secs(8));
        assert_eq!(b.next_delay(), Duration::from_secs(10)); // cap
        assert_eq!(b.next_delay(), Duration::from_secs(10));
        b.reset();
        assert_eq!(b.next_delay(), Duration::from_secs(1));
    }

    #[test]
    fn budget_sliding_window() {
        let mut budget = BudgetWindow::new(3, Duration::from_secs(10));
        let t0 = Instant::now();
        budget.record(t0);
        budget.record(t0 + Duration::from_secs(1));
        assert!(!budget.would_exceed(t0 + Duration::from_secs(2)));
        budget.record(t0 + Duration::from_secs(2));
        assert!(budget.would_exceed(t0 + Duration::from_secs(3)));
        // 旧记录滑出窗口
        assert!(!budget.would_exceed(t0 + Duration::from_secs(11)));
    }

    #[test]
    fn budget_restore_roundtrip() {
        let mut budget = BudgetWindow::new(5, Duration::from_secs(60));
        let t0 = Instant::now();
        let w0 = SystemTime::now();
        budget.record(t0 - Duration::from_secs(5));
        budget.record(t0 - Duration::from_secs(2));
        let wall = budget.to_wall(t0, w0);
        assert_eq!(wall.len(), 2);
        let mut restored = BudgetWindow::new(5, Duration::from_secs(60));
        restored.restore(&wall, w0, t0);
        assert_eq!(restored.count(t0), 2);
    }

    /// 跨 reboot/长挂起:窗口外的旧时间戳必须丢弃,不得夹到 now 虚增窗口
    /// (第五轮评审 P1:夹 now 使"一周 5 次零散死亡"重启后一次真死即误 Fatal)。
    #[test]
    fn budget_restore_drops_stamps_outside_window() {
        let mut budget = BudgetWindow::new(5, Duration::from_secs(60));
        let t0 = Instant::now();
        let w0 = SystemTime::now();
        let old = w0.checked_sub(Duration::from_secs(3600)).unwrap(); // 1h 前，远超 60s 窗口
        let fresh = w0.checked_sub(Duration::from_secs(10)).unwrap();
        budget.restore(&[old, old, fresh], w0, t0);
        assert_eq!(budget.count(t0), 1, "窗口外旧戳丢弃;窗口内新鲜戳保留");
    }
}
