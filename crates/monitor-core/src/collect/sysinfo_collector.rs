//! sysinfo 0.40 采集器实现 (DESIGN.md §6.1 API 锚点、§6.2 部分刷新协议、§6.3 温度可信过滤)。
//!
//! 实现要点：
//! - `System::new()` 返回 Err → `CoreError::Collect`(外层 `create_collector` 降级 Stub);
//! - CPU 首采在构造期完成：隔 [`sysinfo::MINIMUM_CPU_UPDATE_INTERVAL`] 双采样
//!   `refresh_cpu_usage`(睡眠只发生这一次);之后每轮 `collect` 直接读数，绝不睡眠;
//! - 每轮部分刷新:cpu_usage + memory + disks + networks + components(温度)+
//!   全量 watched PID 一次传入 `refresh_processes(Some(&pids), true)`;
//!   全量 `refresh_all` 禁止 (观测开销);
//! - 死亡判定交给 eval(快照缺失即未刷新),本层只产出 `(pid, start_time)`
//!   匹配的进程 (身份不匹配 = PID 复用，同样视为未刷新);
//! - 温度逐值可信过滤 -20..=150 ℃(闭边界),越界丢弃 +
//!   [`Degradation::TemperatureImplausible`];`tdev`/`als-temp` 等已知噪声源整条跳过;
//!   同类 Degradation 每轮最多一条。

use std::time::SystemTime;

use sysinfo::{
    Components, Disks, MINIMUM_CPU_UPDATE_INTERVAL, Networks, Pid, ProcessesToUpdate, System,
};

use crate::collect::{CollectOutput, Collector};
use crate::error::{CoreError, Result};
use crate::model::ids::ProcessIdentity;
use crate::model::snapshot::{
    Degradation, DiskUsage, HardwareSnapshot, NetIo, ProcessSnapshot, Temperature,
};

/// 温度可信区间闭边界 (℃)。实测 macOS 存在 -9201 ℃ 级垃圾读数 (DESIGN §6.3)。
const TEMPERATURE_MIN_C: f32 = -20.0;
const TEMPERATURE_MAX_C: f32 = 150.0;

/// 已知噪声传感器 label 子串 (DESIGN §6.3):整条跳过，不产出也不记 Degradation。
const NOISE_LABEL_SUBSTRINGS: [&str; 2] = ["tdev", "als-temp"];

/// 温度可信过滤纯函数 (DESIGN §6.3):-20..=150 ℃ 之外 (含 NaN/±Inf)视为垃圾读数。
fn plausible(celsius: f32) -> bool {
    (TEMPERATURE_MIN_C..=TEMPERATURE_MAX_C).contains(&celsius)
}

/// 噪声源 label 判定:`tdev`/`als-temp` 等子串命中即跳过。
fn is_noise_label(label: &str) -> bool {
    NOISE_LABEL_SUBSTRINGS
        .iter()
        .any(|needle| label.contains(needle))
}

/// 子采集器 (components/disks/networks) 初始化：失败不致命，降级为 None 并记日志。
fn init_or_warn<T>(
    what: &'static str,
    init: impl FnOnce() -> std::result::Result<T, sysinfo::Error>,
) -> Option<T> {
    match init() {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::warn!(
                component = what,
                source = %e,
                "sysinfo sub-collector init failed, related data unavailable"
            );
            None
        }
    }
}

/// sysinfo 0.40 同步采集器。独占 [`System`],由单一线程驱动 ([`Collector`] 契约)。
pub struct SysCollector {
    system: System,
    /// 温度组件;初始化失败时为 None(每轮记 `TemperatureUnavailable`,不伪造)。
    components: Option<Components>,
    /// 磁盘列表;初始化失败时为 None(产出空 Vec，不伪造)。
    disks: Option<Disks>,
    /// 网络接口;初始化失败时为 None(产出空 Vec，不伪造)。
    networks: Option<Networks>,
    /// 构造期已完成 CPU 双采样则 true;false 时 `collect` 产出
    /// `cpu_usage_percent = None` + [`Degradation::CpuFirstSample`]。
    cpu_ready: bool,
}

impl SysCollector {
    pub fn new() -> Result<Self> {
        let mut system = System::new()
            .map_err(|e| CoreError::Collect(format!("sysinfo system init failed: {e}")))?;

        // CPU 首采 (DESIGN §6.1):双采样必须隔 MINIMUM_CPU_UPDATE_INTERVAL 才有效。
        // 睡眠只允许发生在构造期这一次;collect 路径绝不睡眠。
        system.refresh_cpu_usage();
        std::thread::sleep(MINIMUM_CPU_UPDATE_INTERVAL);
        system.refresh_cpu_usage();

        // 温度/磁盘/网络初始化失败不致命：降级为 None，对应数据按轮记 Degradation/空列表。
        let components = init_or_warn("components", Components::new_with_refreshed_list);
        let disks = init_or_warn("disks", Disks::new_with_refreshed_list);
        let networks = init_or_warn("networks", Networks::new_with_refreshed_list);

        tracing::info!(
            collector = "sysinfo",
            cpu_ready = true,
            "sysinfo collector initialized"
        );
        Ok(Self {
            system,
            components,
            disks,
            networks,
            cpu_ready: true,
        })
    }

    /// 温度采集 + 可信过滤 (DESIGN §6.3)。同类 Degradation 每轮最多一条。
    fn collect_temperatures(&mut self, degradations: &mut Vec<Degradation>) -> Vec<Temperature> {
        let Some(components) = self.components.as_mut() else {
            degradations.push(Degradation::TemperatureUnavailable {
                platform: std::env::consts::OS,
            });
            return Vec::new();
        };
        components.refresh(true);

        let mut temperatures = Vec::new();
        let mut saw_unavailable = false;
        let mut saw_implausible = false;
        let mut saw_any_sensor = false;
        for component in components.iter() {
            saw_any_sensor = true;
            let label = component.label();
            if is_noise_label(label) {
                continue; // 已知噪声源:tdev/als-temp 等，整条跳过
            }
            match component.temperature() {
                Some(raw) if plausible(raw) => {
                    temperatures.push(Temperature {
                        label: label.to_owned(),
                        celsius: Some(raw),
                    });
                }
                Some(raw) => {
                    // 越界垃圾读数：丢弃，不产出 Temperature。
                    if !saw_implausible {
                        saw_implausible = true;
                        degradations.push(Degradation::TemperatureImplausible {
                            label: label.to_owned(),
                            raw,
                        });
                    }
                }
                None => {
                    temperatures.push(Temperature {
                        label: label.to_owned(),
                        celsius: None,
                    });
                    saw_unavailable = true;
                }
            }
        }
        // 平台无温度数据 (组件列表为空) 或存在不可用传感器 → 一条 TemperatureUnavailable。
        if saw_unavailable || !saw_any_sensor {
            degradations.push(Degradation::TemperatureUnavailable {
                platform: std::env::consts::OS,
            });
        }
        temperatures
    }
}

impl Collector for SysCollector {
    fn collect(&mut self, watched: &[ProcessIdentity]) -> CollectOutput {
        let mut degradations: Vec<Degradation> = Vec::new();

        // CPU:首个有效采样前不伪造数值 (None + Degradation,DESIGN §5)。
        let (cpu_usage_percent, per_core_usage) = if self.cpu_ready {
            self.system.refresh_cpu_usage();
            (
                Some(self.system.global_cpu_usage()),
                self.system.cpus().iter().map(|c| Some(c.usage())).collect(),
            )
        } else {
            degradations.push(Degradation::CpuFirstSample {
                note: "awaiting two refreshes spaced by MINIMUM_CPU_UPDATE_INTERVAL",
            });
            (None, Vec::new())
        };

        // 内存。
        self.system.refresh_memory();
        let memory_total_bytes = self.system.total_memory();
        let memory_used_bytes = self.system.used_memory();

        // 磁盘 (初始化失败保持空列表，不伪造)。
        if let Some(disks) = self.disks.as_mut() {
            disks.refresh(true);
        }
        let disks: Vec<DiskUsage> = self
            .disks
            .as_ref()
            .map(|ds| {
                ds.iter()
                    .map(|d| DiskUsage {
                        name: d.name().to_string_lossy().into_owned(),
                        total_bytes: d.total_space(),
                        available_bytes: d.available_space(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        // 网络。
        if let Some(networks) = self.networks.as_mut() {
            networks.refresh(true);
        }
        let networks: Vec<NetIo> = self
            .networks
            .as_ref()
            .map(|ns| {
                ns.iter()
                    .map(|(name, data)| NetIo {
                        name: name.to_string_lossy().into_owned(),
                        received_bytes: data.received(),
                        transmitted_bytes: data.transmitted(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        // 温度 (§6.3 可信过滤)。
        let temperatures = self.collect_temperatures(&mut degradations);

        // 进程部分刷新 (DESIGN §6.2):本轮全部 watched PID 一次性传入，漏传者停留旧数据。
        let mut pids: Vec<Pid> = Vec::with_capacity(watched.len());
        for identity in watched {
            let pid = Pid::from_u32(identity.pid.0);
            if !pids.contains(&pid) {
                pids.push(pid);
            }
        }
        let refreshed = self
            .system
            .refresh_processes(ProcessesToUpdate::Some(&pids), true);
        tracing::debug!(refreshed, requested = pids.len(), "partial process refresh");

        // 对本轮刷新到的 watched PID 产出快照，**上报观测身份**(内核记账的
        // start_time)。PID 复用判定在消费端 (eval):预期身份与观测身份不符
        // → 原进程已死/被复用，由 eval 采纳或判死 (DESIGN §8.3;±2s 容差吸收
        // spawn 侧本地钟与内核记账的跨秒失配)。本层是事实观察者，不做裁决。
        let processes: Vec<ProcessSnapshot> = {
            let mut seen: Vec<u32> = Vec::with_capacity(watched.len());
            watched
                .iter()
                .filter_map(|identity| {
                    let pid = identity.pid.0;
                    if seen.contains(&pid) {
                        return None; // 去重：同一 pid 多个身份条目只产出一个快照
                    }
                    let process = self.system.processes().get(&Pid::from_u32(pid))?;
                    seen.push(pid);
                    Some(ProcessSnapshot {
                        identity: ProcessIdentity::new(pid, process.start_time()),
                        cpu_usage_percent: Some(process.cpu_usage()),
                        memory_bytes: Some(process.memory()),
                        status: Some(process.status().to_string()),
                    })
                })
                .collect()
        };

        tracing::debug!(
            watched = watched.len(),
            refreshed_processes = processes.len(),
            temperatures = temperatures.len(),
            degradations = degradations.len(),
            "collect finished"
        );

        CollectOutput {
            collected_at: std::time::Instant::now(),
            requested: watched.to_vec(),
            hardware: HardwareSnapshot {
                timestamp: SystemTime::now(),
                cpu_usage_percent,
                per_core_usage,
                memory_total_bytes,
                memory_used_bytes,
                disks,
                networks,
                temperatures,
                fans: Vec::new(),
                voltages: Vec::new(),
                degradations,
            },
            processes,
        }
    }
}

impl std::fmt::Display for SysCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SysCollector(cpu_ready={})", self.cpu_ready)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// (1) 真实构造成功 (含构造期一次性 CPU 双采样睡眠;不依赖 CI 环境)。
    #[test]
    fn new_succeeds_on_host() {
        let collector = SysCollector::new().expect("SysCollector::new should succeed on host");
        assert!(
            collector.cpu_ready,
            "构造期双采样完成后 cpu_ready 必须为 true"
        );
    }

    /// (2) 空 watched 采集:macOS 上 memory_total_bytes > 0,CPU 有有效值;
    /// 不对温度列表做平台断言 (Windows 等无温度平台允许为空)。
    #[test]
    fn collect_empty_watched_returns_real_memory_snapshot() {
        let mut collector = SysCollector::new().expect("collector init");
        let out = collector.collect(&[]);
        assert!(
            out.hardware.memory_total_bytes > 0,
            "memory_total_bytes must be > 0 on macOS, got {}",
            out.hardware.memory_total_bytes
        );
        assert!(
            out.hardware.memory_used_bytes > 0,
            "memory_used_bytes must be > 0"
        );
        assert!(out.processes.is_empty(), "空 watched 不得产出进程快照");
        let cpu = out
            .hardware
            .cpu_usage_percent
            .expect("构造期已双采样，cpu_usage_percent 必须为 Some");
        assert!(
            (0.0..=100.0).contains(&cpu),
            "cpu usage out of plausible range: {cpu}"
        );
        // 温度可信区间内的产出值必须全部可信 (过滤生效)。
        for t in &out.hardware.temperatures {
            if let Some(c) = t.celsius {
                assert!(
                    plausible(c),
                    "implausible temperature leaked: {} {}℃",
                    t.label,
                    c
                );
            }
        }
    }

    /// (3) 温度可信过滤纯函数：闭边界含端点，-9201 ℃ 级垃圾值被拒。
    #[test]
    fn temperature_plausibility_filter() {
        assert!(!plausible(-9201.0), "macOS 实测垃圾值必须被拒");
        assert!(plausible(45.0));
        assert!(plausible(-20.0), "下边界为闭区间");
        assert!(plausible(150.0), "上边界为闭区间");
        assert!(!plausible(-20.1));
        assert!(!plausible(150.1));
        assert!(!plausible(f32::NAN), "NaN 不可信");
        assert!(!plausible(f32::INFINITY), "+Inf 不可信");
    }

    /// 噪声源 label 跳过判定 (DESIGN §6.3:tdev/als-temp)。
    #[test]
    fn noise_label_detection() {
        assert!(is_noise_label("PM tdev"));
        assert!(is_noise_label("als-temp0"));
        assert!(!is_noise_label("PM AL0"));
        assert!(!is_noise_label("CPU Core 1"));
    }

    /// (4) §6.2 部分刷新协议:watched PID 刷新后产出快照，身份为**观测值**
    /// (内核记账 start_time);预期与观测不符的裁决在消费端 (eval 容差 + 采纳)。
    #[test]
    fn collect_refreshes_watched_process_by_identity() {
        let mut collector = SysCollector::new().expect("collector init");
        let my_pid = std::process::id();
        // 先探测自身启动时间，构造合法身份 (测试进程必然存活，确定性成立)。
        let self_pid = Pid::from_u32(my_pid);
        collector
            .system
            .refresh_processes(ProcessesToUpdate::Some(&[self_pid]), false);
        let start_time = collector
            .system
            .processes()
            .get(&self_pid)
            .expect("self process must be visible after refresh")
            .start_time();
        let watched = vec![ProcessIdentity::new(my_pid, start_time)];

        let out = collector.collect(&watched);
        assert_eq!(out.processes.len(), 1, "watched 进程必须产出快照");
        let snap = &out.processes[0];
        assert_eq!(snap.identity.pid.0, my_pid);
        assert_eq!(
            snap.identity.start_time, start_time,
            "上报观测身份 (内核记账)"
        );
        assert!(
            snap.cpu_usage_percent.is_some(),
            "已刷新进程必须有 cpu 读数"
        );
        assert!(
            matches!(snap.memory_bytes, Some(m) if m > 0),
            "运行中进程的内存必须 > 0"
        );
        assert!(
            snap.status.as_deref().is_some_and(|s| !s.is_empty()),
            "status 必须有值"
        );

        // 观测身份语义：预期身份的 start_time 与内核不符时，**仍产出观测快照**
        // (消费端 eval 据此做容差匹配/复用裁决/采纳);快照 identity 为内核观测值。
        let stale = vec![ProcessIdentity::new(my_pid, start_time + 3600)];
        let out2 = collector.collect(&stale);
        assert_eq!(out2.processes.len(), 1, "观测身份必须如实上报");
        assert_eq!(
            out2.processes[0].identity.start_time, start_time,
            "快照身份是内核观测值，不是传入的预期值"
        );
    }
}
