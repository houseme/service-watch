//! eval/reload:F14 配置热重载的 **diff 纯函数**(应用侧见 [`super::Scheduler::apply_reload`])。
//!
//! 语义(v0.2 范围,ROADMAP F14):
//! - 热重载**只应用目标拓扑**(checks / services / watches 的增/删/改);
//! - 其余小节(`agent` / `log` / `control` / `collect` / `alert` / `metrics`)
//!   变更**要求重启**:仅检测并审计(`section X changed but requires restart
//!   (not hot-applied)`),不热应用;
//! - services:command / args / workdir / env 任一变更 → 视为删+增(重启生效);
//!   restart / activate / console 等软字段变更 → 原地更新 cfg,FSM 水位保留;
//!
//! ## 相等性判定
//!
//! 基于 serde 序列化后的 [`serde_json::Value`] 比较:
//! - `serde_json::Map` 默认为 `BTreeMap`,**字段书写顺序无关**(重排 TOML 键序不误报);
//! - Vec(args)按序比较(顺序即语义);整体按 name 匹配(条目重排不误报);
//! - Duration 走 human-readable 自定义序列化,`"5s"` 与 `"5000ms"` 视为**不同**
//!   (语义等价但记为变更 —— 偏保守,如实记录);
//! - 非有限浮点(TOML `nan`/`inf`)序列化为 `null`(serde_json 行为),两侧同为
//!   null 时视为相等。

use crate::config::{Config, ServiceCfg};

/// 拓扑 diff 结果(全部为 name 列表;apply 侧按 name 匹配保留运行时状态水位)。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct TopologyDiff {
    pub added_checks: Vec<String>,
    pub removed_checks: Vec<String>,
    pub changed_checks: Vec<String>,
    pub added_services: Vec<String>,
    pub removed_services: Vec<String>,
    pub changed_services: Vec<String>,
    pub added_watches: Vec<String>,
    pub removed_watches: Vec<String>,
    pub changed_watches: Vec<String>,
}

impl TopologyDiff {
    /// 拓扑完全一致(不作为 fast-path 使用:reload 始终写审计,可追溯;
    /// 供测试断言)。
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.added_checks.is_empty()
            && self.removed_checks.is_empty()
            && self.changed_checks.is_empty()
            && self.added_services.is_empty()
            && self.removed_services.is_empty()
            && self.changed_services.is_empty()
            && self.added_watches.is_empty()
            && self.removed_watches.is_empty()
            && self.changed_watches.is_empty()
    }
}

/// 序列化为 [`serde_json::Value`](失败 → `Null`:配置序列化不可能失败,兜底偏保守)。
fn json_of<T: serde::Serialize>(v: &T) -> serde_json::Value {
    serde_json::to_value(v).unwrap_or(serde_json::Value::Null)
}

/// 配置条目相等性(serde 序列化比较,见模块 doc「相等性判定」)。
fn cfg_eq<T: serde::Serialize>(a: &T, b: &T) -> bool {
    json_of(a) == json_of(b)
}

/// service 的「重启生效」字段(command/args/workdir/env)是否变更
/// (任一变更 → 删+增:旧实例优雅停、新定义全新 FSM 冷启动)。
pub(crate) fn service_hot_changed(a: &ServiceCfg, b: &ServiceCfg) -> bool {
    let (va, vb) = (json_of(a), json_of(b));
    va["command"] != vb["command"]
        || va["args"] != vb["args"]
        || va["workdir"] != vb["workdir"]
        || va["env"] != vb["env"]
        // windows_service 变更 = 普通 supervise ↔ SCM 目标形态切换(重启生效;
        // G 线合并同步点)
        || va["windows_service"] != vb["windows_service"]
}

/// service 软字段(restart 策略 / activate / console)是否变更
/// (原地更新 cfg,policy/verify_for 重建,FSM 水位保留,不触发重启)。
pub(crate) fn service_soft_changed(a: &ServiceCfg, b: &ServiceCfg) -> bool {
    !cfg_eq(a, b) && !service_hot_changed(a, b)
}

/// 拓扑 diff(纯函数):checks / services / watches 各空间按 name 比对。
/// 前提:old / new 各自 name 唯一(`Config::validate` 保证;防御性复检见
/// [`first_duplicate_name`])。
pub(crate) fn diff_topology(old: &Config, new: &Config) -> TopologyDiff {
    let mut d = TopologyDiff::default();

    for c in &new.checks {
        match old.checks.iter().find(|o| o.name == c.name) {
            Some(o) => {
                if !cfg_eq(o, c) {
                    d.changed_checks.push(c.name.clone());
                }
            }
            None => d.added_checks.push(c.name.clone()),
        }
    }
    for o in &old.checks {
        if !new.checks.iter().any(|c| c.name == o.name) {
            d.removed_checks.push(o.name.clone());
        }
    }

    for s in &new.services {
        match old.services.iter().find(|o| o.name == s.name) {
            Some(o) => {
                if !cfg_eq(o, s) {
                    d.changed_services.push(s.name.clone());
                }
            }
            None => d.added_services.push(s.name.clone()),
        }
    }
    for o in &old.services {
        if !new.services.iter().any(|s| s.name == o.name) {
            d.removed_services.push(o.name.clone());
        }
    }

    for w in &new.watches {
        match old.watches.iter().find(|o| o.name == w.name) {
            Some(o) => {
                if !cfg_eq(o, w) {
                    d.changed_watches.push(w.name.clone());
                }
            }
            None => d.added_watches.push(w.name.clone()),
        }
    }
    for o in &old.watches {
        if !new.watches.iter().any(|w| w.name == o.name) {
            d.removed_watches.push(o.name.clone());
        }
    }

    d
}

/// 非拓扑小节变更检测(纯函数):返回变更的小节名(固定顺序 agent → log →
/// control → collect → alert → metrics)。这些段**要求重启**,不热应用。
pub(crate) fn diff_sections(old: &Config, new: &Config) -> Vec<&'static str> {
    let (o, n) = (json_of(old), json_of(new));
    let get = |v: &serde_json::Value, key: &str| v.get(key).cloned().unwrap_or_default();
    ["agent", "log", "control", "collect", "alert", "metrics"]
        .into_iter()
        .filter(|section| get(&o, section) != get(&n, section))
        .collect()
}

/// 防御性断言:三空间 name 唯一(校验层 `Config::validate` 已保证;仅在绕过
/// 校验直接构造 `Config` 时可达)。返回 (空间, 首个重名)。
pub(crate) fn first_duplicate_name(cfg: &Config) -> Option<(&'static str, String)> {
    for (label, names) in [
        (
            "check",
            cfg.checks
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
        ),
        (
            "service",
            cfg.services
                .iter()
                .map(|s| s.name.clone())
                .collect::<Vec<_>>(),
        ),
        (
            "watch",
            cfg.watches
                .iter()
                .map(|w| w.name.clone())
                .collect::<Vec<_>>(),
        ),
    ] {
        let mut seen = std::collections::HashSet::new();
        for n in names {
            if !seen.insert(n.clone()) {
                return Some((label, n));
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(toml: &str) -> Config {
        Config::from_toml_str(toml).expect("test config must parse")
    }

    const BASE: &str = r#"
        [[check]]
        name = "c1"
        kind = "tcp"
        addr = "127.0.0.1:5000"

        [[check]]
        name = "c2"
        kind = "tcp"
        addr = "127.0.0.1:5001"

        [[service]]
        name = "app"
        command = "/bin/true"
        args = ["--once"]

        [[watch]]
        name = "w1"
        pid = 100
    "#;

    #[test]
    fn diff_no_changes_when_identical() {
        let a = cfg(BASE);
        let b = cfg(BASE);
        let d = diff_topology(&a, &b);
        assert!(d.is_empty(), "{d:?}");
        assert!(diff_sections(&a, &b).is_empty());
    }

    /// 条目在 TOML 中重排位置(name 相同)不算变更:匹配按 name,不按位置。
    #[test]
    fn diff_reordering_entries_is_not_a_change() {
        let a = cfg(BASE);
        let b = cfg(r#"
            [[service]]
            name = "app"
            args = ["--once"]
            command = "/bin/true"

            [[watch]]
            name = "w1"
            pid = 100

            [[check]]
            name = "c2"
            kind = "tcp"
            addr = "127.0.0.1:5001"

            [[check]]
            name = "c1"
            kind = "tcp"
            addr = "127.0.0.1:5000"
        "#);
        let d = diff_topology(&a, &b);
        assert!(d.is_empty(), "{d:?}");
    }

    #[test]
    fn diff_detects_added_and_removed_checks() {
        let a = cfg(BASE);
        let b = cfg(r#"
            [[check]]
            name = "c2"
            kind = "tcp"
            addr = "127.0.0.1:5001"

            [[check]]
            name = "c9"
            kind = "tcp"
            addr = "127.0.0.1:5009"
        "#);
        let d = diff_topology(&a, &b);
        assert_eq!(d.added_checks, ["c9"]);
        assert_eq!(d.removed_checks, ["c1"]);
        assert!(d.changed_checks.is_empty());
    }

    /// 参数变更(interval/timeout)记为 changed(既非新增也非删除)——
    /// 应用侧据此**保留状态机水位**,仅替换 cfg。
    #[test]
    fn diff_check_param_change_is_changed_not_add_remove() {
        let a = cfg(BASE);
        let b = cfg(r#"
            [[check]]
            name = "c1"
            kind = "tcp"
            addr = "127.0.0.1:5000"
            interval = "20s"
            timeout = "8s"

            [[check]]
            name = "c2"
            kind = "tcp"
            addr = "127.0.0.1:5001"
        "#);
        let d = diff_topology(&a, &b);
        assert_eq!(d.changed_checks, ["c1"]);
        assert!(d.added_checks.is_empty());
        assert!(d.removed_checks.is_empty());
    }

    /// kind 变更(http → tcp)同样记为 changed(应用侧保留 name 对应的状态机)。
    #[test]
    fn diff_check_kind_change_detected() {
        let a = cfg(BASE);
        let b = cfg(r#"
            [[check]]
            name = "c1"
            kind = "process"
            process = "pid:1234"
            cpu_threshold = 90.0

            [[check]]
            name = "c2"
            kind = "tcp"
            addr = "127.0.0.1:5001"
        "#);
        let d = diff_topology(&a, &b);
        assert_eq!(d.changed_checks, ["c1"]);
    }

    #[test]
    fn diff_service_hot_field_change() {
        let a = cfg(BASE);
        let b = cfg(r#"
            [[check]]
            name = "c1"
            kind = "tcp"
            addr = "127.0.0.1:5000"

            [[check]]
            name = "c2"
            kind = "tcp"
            addr = "127.0.0.1:5001"

            [[service]]
            name = "app"
            command = "/bin/echo"
            args = ["--once"]

            [[watch]]
            name = "w1"
            pid = 100
        "#);
        let d = diff_topology(&a, &b);
        assert_eq!(d.changed_services, ["app"]);
        assert!(service_hot_changed(&a.services[0], &b.services[0]));
        assert!(!service_soft_changed(&a.services[0], &b.services[0]));
    }

    /// restart 策略变更属软字段:记 changed,但应用侧原地更新(不重启生效)。
    #[test]
    fn diff_service_soft_field_change() {
        let a = cfg(BASE);
        let b = cfg(r#"
            [[check]]
            name = "c1"
            kind = "tcp"
            addr = "127.0.0.1:5000"

            [[check]]
            name = "c2"
            kind = "tcp"
            addr = "127.0.0.1:5001"

            [[service]]
            name = "app"
            command = "/bin/true"
            args = ["--once"]
            [service.restart]
            stop_grace = "30s"

            [[watch]]
            name = "w1"
            pid = 100
        "#);
        let d = diff_topology(&a, &b);
        assert_eq!(d.changed_services, ["app"]);
        assert!(!service_hot_changed(&a.services[0], &b.services[0]));
        assert!(service_soft_changed(&a.services[0], &b.services[0]));
    }

    #[test]
    fn diff_watch_pid_change_detected() {
        let a = cfg(BASE);
        let b = cfg(r#"
            [[check]]
            name = "c1"
            kind = "tcp"
            addr = "127.0.0.1:5000"

            [[check]]
            name = "c2"
            kind = "tcp"
            addr = "127.0.0.1:5001"

            [[service]]
            name = "app"
            command = "/bin/true"
            args = ["--once"]

            [[watch]]
            name = "w1"
            pid = 200
        "#);
        let d = diff_topology(&a, &b);
        assert_eq!(d.changed_watches, ["w1"]);
        assert!(d.added_watches.is_empty() && d.removed_watches.is_empty());
    }

    /// 多空间混合:新增 check + 删除 service + 变更 watch,一次 diff 全部识别。
    #[test]
    fn diff_mixed_across_spaces() {
        let a = cfg(BASE);
        let b = cfg(r#"
            [[check]]
            name = "c1"
            kind = "tcp"
            addr = "127.0.0.1:5000"

            [[check]]
            name = "c2"
            kind = "tcp"
            addr = "127.0.0.1:5001"

            [[check]]
            name = "c3"
            kind = "tcp"
            addr = "127.0.0.1:5003"

            [[watch]]
            name = "w1"
            pidfile = "/run/w1.pid"

            [[watch]]
            name = "w2"
            pid = 7
        "#);
        let d = diff_topology(&a, &b);
        assert_eq!(d.added_checks, ["c3"]);
        assert_eq!(d.added_watches, ["w2"]);
        assert_eq!(d.changed_watches, ["w1"]);
        assert_eq!(d.removed_services, ["app"]);
        assert!(d.added_services.is_empty() && d.removed_checks.is_empty());
    }

    /// 非拓扑小节变更检测:collect/alert 变更被点名,拓扑变更不误报。
    #[test]
    fn diff_sections_detects_non_topology_changes_only() {
        let a = cfg(BASE);
        let topology_only = cfg(r#"
            [[check]]
            name = "c1"
            kind = "tcp"
            addr = "127.0.0.1:5000"

            [[check]]
            name = "c2"
            kind = "tcp"
            addr = "127.0.0.1:5001"

            [[check]]
            name = "c99"
            kind = "tcp"
            addr = "127.0.0.1:5099"

            [[service]]
            name = "app"
            command = "/bin/true"
            args = ["--once"]

            [[watch]]
            name = "w1"
            pid = 100
        "#);
        assert!(diff_sections(&a, &topology_only).is_empty());

        let sections_changed = cfg(r#"
            [agent]
            data_dir = "./data2"

            [collect]
            interval = "2s"

            [alert]
            failure_threshold = 5

            [[check]]
            name = "c1"
            kind = "tcp"
            addr = "127.0.0.1:5000"

            [[check]]
            name = "c2"
            kind = "tcp"
            addr = "127.0.0.1:5001"

            [[service]]
            name = "app"
            command = "/bin/true"
            args = ["--once"]

            [[watch]]
            name = "w1"
            pid = 100
        "#);
        assert_eq!(
            diff_sections(&a, &sections_changed),
            ["agent", "collect", "alert"]
        );
    }

    /// 防御性断言:绕过校验构造的重名 Config 被点名(check / service 空间独立)。
    #[test]
    fn first_duplicate_name_finds_conflict_per_space() {
        let mut bad = cfg(BASE);
        let dup = bad.checks[0].clone();
        bad.checks.push(dup);
        assert_eq!(
            first_duplicate_name(&bad),
            Some(("check", "c1".to_string()))
        );

        let mut bad_svc = cfg(BASE);
        let dup_svc = bad_svc.services[0].clone();
        bad_svc.services.push(dup_svc);
        assert_eq!(
            first_duplicate_name(&bad_svc),
            Some(("service", "app".to_string()))
        );

        assert_eq!(first_duplicate_name(&cfg(BASE)), None);
    }
}
