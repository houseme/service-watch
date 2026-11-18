//! F31 定期维护窗口 (ROADMAP v0.6 F31):UTC 墙钟驱动的周期性通知抑制。
//!
//! 判定为**纯函数**,时间一律以 epoch 秒注入 (调用方从注入的 `SystemTime`
//! 换算，本模块不读钟 —— 与 eval 其余状态机的时间注入纪律一致):
//! - [`weekday_minute_of_day`]:epoch 秒 → (星期索引，当日分钟),UTC 手工历法;
//! - [`MaintenanceWindow::contains`]:窗口匹配，含跨 midnight (start + duration
//!   越日界) 与 `days` 子集过滤。
//!
//! 全部时刻按 **UTC** 计算 (std 无时区库，如实限定;不支持 DST —— 固定 UTC
//! 偏移下 DST 切换只会让窗口相对本地钟平移 1h，语义仍是确定的)。窗口表为
//! eval **构建时快照** (来自 `cfg.maintenance`;该段不热应用，变更要求重启),
//! 判定无任何持久状态：按 UTC 时钟自然重入，agent 重启后自动生效 (§7.3)。

use std::time::Duration;

use crate::config::MaintenanceCfg;

/// 星期索引 → 配置用小写缩写 (Mon=0..Sun=6;与 config.rs 校验合法值一致)。
pub const WEEKDAYS: [&str; 7] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];

/// UTC 手工历法换算 (纯函数):epoch 秒 → `(星期索引，当日分钟)`。
///
/// 星期索引 Mon=0..Sun=6;锚点:epoch 0 = 1970-01-01 是**周四** (索引 3),
/// 此后按天循环 —— 无需完整历法表，`days = epoch / 86400` 即绝对日序
/// (1970-01-01 起),`(days + 3) % 7` 恒为星期索引。分钟同样只做整除：
/// 秒级粒度对窗口判定足够 (配置校验要求 duration ≥ 60s)。
pub fn weekday_minute_of_day(epoch_secs: u64) -> (usize, u32) {
    let days = epoch_secs / 86_400;
    let minute_of_day = (epoch_secs % 86_400) / 60;
    (weekday_of_day(days), minute_of_day as u32)
}

/// 绝对日序 (自 1970-01-01) 的星期索引 (Mon=0..Sun=6)。
fn weekday_of_day(days: u64) -> usize {
    ((days + 3) % 7) as usize
}

/// 解析 `"HH:MM"` (UTC) → 当日分钟 0..=1439 (纯函数)。两位数字、HH < 24、
/// MM < 60;其余一律拒绝 (与 config.rs 校验同一口径，此处兜底防御绕过
/// 校验直构的配置)。
pub fn parse_hhmm(s: &str) -> Result<u32, String> {
    let parts: Vec<&str> = s.split(':').collect();
    let ok = parts.len() == 2
        && parts
            .iter()
            .all(|p| p.len() == 2 && p.chars().all(|c| c.is_ascii_digit()))
        && parts[0].parse::<u32>().map(|h| h < 24).unwrap_or(false)
        && parts[1].parse::<u32>().map(|m| m < 60).unwrap_or(false);
    if !ok {
        return Err(format!(
            "start_utc {s:?} must be UTC \"HH:MM\" (two digits, HH < 24, MM < 60)"
        ));
    }
    let (h, m) = (
        parts[0].parse::<u32>().unwrap_or(0),
        parts[1].parse::<u32>().unwrap_or(0),
    );
    Ok(h * 60 + m)
}

/// 单条维护窗口的 eval 侧判定形态 (构建时从 [`MaintenanceCfg`] 解析一次，
/// emit_alert/tick 每次调用零解析开销)。
#[derive(Debug, Clone)]
pub(crate) struct MaintenanceWindow {
    /// 可选名称 (Audit 边沿标识;空 = 报 targets)。
    pub name: String,
    /// target 前缀匹配列表 (`*` = 全部;语义同 `[[alert.routes]]` 顺序任意 ——
    /// 窗口间是「任一命中即抑制」的并集，无顺序语义)。
    pub targets: Vec<String>,
    /// 开启时刻 (当日分钟 0..=1439,UTC)。
    pub start_minute: u32,
    /// 窗口时长 (≥ 60s 由配置校验保证;此处不做上限假设)。
    pub duration: Duration,
    /// 生效星期集合 (`None` = 每天;`Some` 的下标即 [`WEEKDAYS`] 索引)。
    pub days: Option<[bool; 7]>,
}

impl MaintenanceWindow {
    /// 从配置条目解析;`Err` = 配置非法 (校验层应已拒绝，此处兜底：
    /// 调用方 [`Self::from_cfgs`] 会 WARN 并跳过，绝不 panic)。
    pub fn parse(cfg: &MaintenanceCfg) -> Result<Self, String> {
        let start_minute = parse_hhmm(&cfg.start_utc)?;
        let days = if cfg.days.is_empty() {
            None
        } else {
            let mut set = [false; 7];
            for d in &cfg.days {
                let name = d.trim().to_ascii_lowercase();
                let idx = WEEKDAYS.iter().position(|w| *w == name).ok_or_else(|| {
                    format!("invalid day {name:?} (use mon tue wed thu fri sat sun)")
                })?;
                set[idx] = true;
            }
            Some(set)
        };
        Ok(Self {
            name: cfg.name.clone(),
            targets: cfg.targets.clone(),
            start_minute,
            duration: cfg.duration,
            days,
        })
    }

    /// 批量解析 (Scheduler::build 用):非法条目 WARN 后跳过 (与「校验失败整体
    /// 拒绝」不冲突 —— 能走到这里说明配置已通过校验，防御直构场景)。
    pub(crate) fn from_cfgs(cfgs: &[MaintenanceCfg]) -> Vec<Self> {
        cfgs.iter()
            .filter_map(|c| match Self::parse(c) {
                Ok(w) => Some(w),
                Err(e) => {
                    tracing::warn!(
                        name = %c.name,
                        reason = %e,
                        "maintenance window ignored: invalid definition"
                    );
                    None
                }
            })
            .collect()
    }

    /// target 匹配 (与 `[[alert.routes]]` 同语义):`*` 通配全部，否则前缀
    /// `starts_with`;空 pattern 恒不命中 (防御配置绕过校验)。
    pub fn matches_target(&self, target: &str) -> bool {
        self.targets
            .iter()
            .any(|p| !p.is_empty() && (p == "*" || target.starts_with(p.as_str())))
    }

    /// 该绝对日序是否为合法开启日 (days 过滤;`None` = 每天)。
    fn day_matches(&self, weekday: usize) -> bool {
        self.days.is_none_or(|d| d[weekday])
    }

    /// 当前 epoch 秒是否落在窗口内 (纯函数，含跨 midnight)。
    ///
    /// 算法取「**开启时刻起的 elapsed**」法：对每个候选开启日 `d`(星期命中),
    /// 窗口区间为 `[d×86400 + start, d×86400 + start + duration)`,当前时刻
    /// 落入任一区间即在窗口内。相比按 minute_of_day 拆「当日 / 次日」两段
    /// 区间的做法，本方法对**任意** duration(跨 1 个或多个日界，甚至 ≥ 24h)
    /// 统一精确，且「次日零点后仍在窗口内」的部分天然归属**开启日**的星期 ——
    /// days 过滤无歧义 (两段法需要额外约定越界部分按开启日还是当日判定)。
    /// 候选日只需回看 `⌊duration/86400⌋ + 1` 天：更早的开启日窗口必然已结束。
    pub fn contains(&self, epoch_secs: u64) -> bool {
        let dur = self.duration.as_secs();
        let days = epoch_secs / 86_400;
        let reach = dur / 86_400 + 1;
        (0..=reach).any(|j| {
            let Some(d) = days.checked_sub(j) else {
                return false; // epoch 之前的日序不存在 (现网时钟必 ≥ epoch;防御)
            };
            if !self.day_matches(weekday_of_day(d)) {
                return false;
            }
            let open = d * 86_400 + self.start_minute as u64 * 60;
            epoch_secs >= open && epoch_secs < open + dur
        })
    }

    /// Audit/日志用标识 (ROADMAP F31):有 name 报 name，无 name 报 targets。
    pub fn label(&self) -> String {
        if self.name.trim().is_empty() {
            format!("targets=[{}]", self.targets.join(", "))
        } else {
            self.name.clone()
        }
    }
}

// ---------------------------------------------------------------------------
// 测试 (锚点:2026-11-18 是周三;epoch 1795005296 = 2026-11-18T12:34:56Z;
// 绝对日序 20775 = 2026-11-18(周三),20773 = 2026-11-16(周一))
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// 已知日期锚点:epoch 0 = 1970-01-01 周四 00:00;2026-11-18T12:34:56Z
    /// = 周三 12:34;次日界 (86400) = 周五 00:00;当日最后一分钟 23:59。
    #[test]
    fn weekday_minute_of_day_known_anchors() {
        assert_eq!(weekday_minute_of_day(0), (3, 0), "1970-01-01 周四 00:00");
        assert_eq!(
            weekday_minute_of_day(1_795_005_296),
            (2, 12 * 60 + 34),
            "2026-11-18T12:34:56Z 周三 12:34"
        );
        assert_eq!(
            weekday_minute_of_day(86_399),
            (3, 23 * 60 + 59),
            "当日 23:59"
        );
        assert_eq!(
            weekday_minute_of_day(86_400),
            (4, 0),
            "1970-01-02 周五 00:00"
        );
        // 7 天循环:epoch 0 + 7 天回到周四
        assert_eq!(weekday_minute_of_day(7 * 86_400).0, 3);
    }

    #[test]
    fn parse_hhmm_accepts_valid_and_rejects_invalid() {
        assert_eq!(parse_hhmm("00:00").unwrap(), 0);
        assert_eq!(parse_hhmm("02:30").unwrap(), 150);
        assert_eq!(parse_hhmm("23:59").unwrap(), 23 * 60 + 59);
        for bad in [
            "24:00", "2:00", "12:60", "ab:cd", "1:2", "-1:00", "", "12:", "1234",
        ] {
            assert!(parse_hhmm(bad).is_err(), "{bad:?} 应拒绝");
        }
    }

    fn window(start: &str, dur_secs: u64, days: &[&str]) -> MaintenanceWindow {
        let cfg = MaintenanceCfg {
            name: "w".into(),
            targets: vec!["*".into()],
            start_utc: start.into(),
            duration: Duration::from_secs(dur_secs),
            days: days.iter().map(|d| d.to_string()).collect(),
        };
        MaintenanceWindow::parse(&cfg).unwrap()
    }

    /// 以绝对日序 + 当日秒偏移构造 epoch (避免测试里写长数字)。
    fn at(days: u64, hhmm: u32, secs: u64) -> u64 {
        days * 86_400 + hhmm as u64 * 60 + secs
    }

    #[test]
    fn contains_same_day_boundaries() {
        let w = window("02:00", 3_600, &[]);
        let wed = 20_775; // 2026-11-18 周三
        assert!(!w.contains(at(wed, 60 + 59, 59)), "01:59:59 窗口外");
        assert!(w.contains(at(wed, 2 * 60, 0)), "开启时刻 (含)");
        assert!(w.contains(at(wed, 2 * 60 + 59, 59)), "结束前 1s (含)");
        assert!(!w.contains(at(wed, 3 * 60, 0)), "结束时刻 (不含)");
    }

    /// 跨 midnight:start 23:00 + 3h → 次日 02:00 结束;次日凌晨部分归属
    /// 开启日 (周三开启的窗口，周三/周四凌晨均命中)。
    #[test]
    fn contains_crosses_midnight() {
        let w = window("23:00", 10_800, &[]);
        let (wed, thu) = (20_775, 20_776);
        assert!(!w.contains(at(wed, 22 * 60 + 59, 59)), "开启前 1s");
        assert!(w.contains(at(wed, 23 * 60, 0)), "周三 23:00 开启 (含)");
        assert!(w.contains(at(thu, 30, 0)), "周四 00:30 仍在窗口内");
        assert!(w.contains(at(thu, 60 + 59, 59)), "结束前 1s (次日)");
        assert!(!w.contains(at(thu, 2 * 60, 0)), "次日 02:00 结束 (不含)");
    }

    /// days 过滤：只作用于**开启日**的星期。
    #[test]
    fn contains_filters_by_day_of_week() {
        let mon_only = window("02:00", 3_600, &["mon"]);
        let (mon, wed) = (20_773, 20_775); // 2026-11-16 周一 / 11-18 周三
        assert!(mon_only.contains(at(mon, 2 * 60 + 30, 0)), "周一命中");
        assert!(!mon_only.contains(at(wed, 2 * 60 + 30, 0)), "周三不开窗");
        // 跨 midnight + days:周一 23:00 开启 3h，周二凌晨仍在窗口内
        let mon_night = window("23:00", 10_800, &["mon"]);
        assert!(
            mon_night.contains(at(mon + 1, 60, 0)),
            "周二 01:00 归属周一开启"
        );
        assert!(
            !mon_night.contains(at(mon + 1, 23 * 60, 0)),
            "周二 23:00 无开启"
        );
    }

    /// duration ≥ 24h 的长窗口：跨多个日界仍精确 (elapsed 法的关键卖点)。
    #[test]
    fn contains_multi_day_duration() {
        let wed = 20_775;
        // start 12:00 + 24h:覆盖 [周三 12:00, 周四 12:00);周四 12:00 由当日
        // 新开启接续 (days 缺省全周)。
        let daylong = window("12:00", 86_400, &[]);
        assert!(daylong.contains(at(wed, 12 * 60, 0)));
        assert!(
            daylong.contains(at(wed + 1, 11 * 60 + 59, 59)),
            "周四 11:59 (elapsed 23:59)"
        );
        assert!(
            daylong.contains(at(wed + 1, 12 * 60, 0)),
            "周四 12:00 新开启接续"
        );
        // days = ["wed"]:周四无新开启，elapsed 恰满 24h 即出窗。
        let wed_only = window("12:00", 86_400, &["wed"]);
        assert!(wed_only.contains(at(wed + 1, 11 * 60 + 59, 59)));
        assert!(
            !wed_only.contains(at(wed + 1, 12 * 60, 0)),
            "开启日不在 days"
        );
        // start 00:00 + 48h:对任意时刻恒在窗口内 (集成测试用它注入「必在窗口」)。
        let always = window("00:00", 2 * 86_400, &[]);
        for probe in [1u64, 86_399, 1_795_005_296] {
            assert!(always.contains(probe), "epoch {probe} 恒在窗口");
        }
    }

    #[test]
    fn parse_days_case_insensitive_and_default_all_week() {
        let w = window("02:00", 3_600, &["MON", "Fri"]);
        assert_eq!(
            w.days,
            Some([true, false, false, false, true, false, false])
        );
        let all = window("02:00", 3_600, &[]);
        assert_eq!(all.days, None, "days 缺省 = 每天");
        assert!(
            MaintenanceWindow::parse(&MaintenanceCfg {
                name: "w".into(),
                targets: vec!["*".into()],
                start_utc: "02:00".into(),
                duration: Duration::from_secs(3_600),
                days: vec!["moon".into()],
            })
            .is_err(),
            "非法星期拒绝"
        );
    }

    #[test]
    fn matches_target_prefix_and_wildcard() {
        let mut w = window("02:00", 3_600, &[]);
        w.targets = vec!["*".into()];
        assert!(w.matches_target("anything"));
        w.targets = vec!["edge".into()];
        assert!(w.matches_target("edge"));
        assert!(w.matches_target("edge-1"), "前缀匹配");
        assert!(!w.matches_target("web-1"));
        w.targets = vec!["".into()];
        assert!(!w.matches_target("anything"), "空 pattern 恒不命中 (防御)");
    }

    #[test]
    fn label_reports_name_or_targets() {
        let mut w = window("02:00", 3_600, &[]);
        w.name = "nightly".into();
        assert_eq!(w.label(), "nightly");
        w.name = String::new();
        w.targets = vec!["api".into(), "edge".into()];
        assert_eq!(w.label(), "targets=[api, edge]");
    }

    /// from_cfgs:非法条目跳过不 panic (防御绕过校验的直构路径)。
    #[test]
    fn from_cfgs_skips_invalid_entries() {
        let good = MaintenanceCfg {
            name: "ok".into(),
            targets: vec!["*".into()],
            start_utc: "02:00".into(),
            duration: Duration::from_secs(3_600),
            days: Vec::new(),
        };
        let bad = MaintenanceCfg {
            start_utc: "9:00".into(),
            ..good.clone()
        };
        let windows = MaintenanceWindow::from_cfgs(&[good, bad]);
        assert_eq!(windows.len(), 1, "非法条目 WARN 跳过");
        assert_eq!(windows[0].name, "ok");
    }
}
