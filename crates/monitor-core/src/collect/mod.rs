//! 采集层(DESIGN.md §6)。采集运行在专用阻塞线程,快照经 watch 广播。
//!
//! [`Collector`] 是同步 trait(实现方持有 `sysinfo::System`,必须独占);
//! [`CollectOutput`] 是不可变快照,跨线程传递。

#[cfg(feature = "collect-deep-hardware")]
mod deep;
#[cfg(feature = "collect-sysinfo")]
pub mod sysinfo_collector;

use crate::model::snapshot::{HardwareSnapshot, ProcessSnapshot};

/// 一次采集的全部产出。
#[derive(Debug, Clone)]
pub struct CollectOutput {
    pub hardware: HardwareSnapshot,
    /// 本轮采集完成的单调时间,用于消费端快照代际与对账启动边界判定。
    pub collected_at: std::time::Instant,
    /// 本轮确实请求查询的进程集合。消费端据此区分未查询与查询后缺失。
    pub requested: Vec<crate::model::ids::ProcessIdentity>,
    /// 仅包含本轮 watched 集合内仍存活的进程(部分刷新协议,§6.2)。
    pub processes: Vec<ProcessSnapshot>,
}

impl CollectOutput {
    /// 以进程身份查找快照(`(pid, start_time)` 匹配才返回,防 PID 复用)。
    pub fn find(&self, identity: &crate::model::ids::ProcessIdentity) -> Option<&ProcessSnapshot> {
        self.processes.iter().find(|p| &p.identity == identity)
    }
}

/// 同步采集器。实现必须:
/// 1. 独占 `sysinfo::System`,由单一线程驱动(禁止多线程共享);
/// 2. 每轮对 `watched` 全量传入部分刷新(漏传者停留旧数据);
/// 3. 一切失败转为 [`crate::model::snapshot::Degradation`],不 panic、不伪造数据。
pub trait Collector: Send {
    fn collect(&mut self, watched: &[crate::model::ids::ProcessIdentity]) -> CollectOutput;
}

/// 构造默认采集器:优先 sysinfo,初始化失败降级为 [`StubCollector`](记录 Degradation)。
pub fn create_collector() -> Box<dyn Collector> {
    let base = create_base_collector();
    #[cfg(feature = "collect-deep-hardware")]
    {
        Box::new(deep::DeepCollector::new(base))
    }
    #[cfg(not(feature = "collect-deep-hardware"))]
    {
        base
    }
}

fn create_base_collector() -> Box<dyn Collector> {
    #[cfg(feature = "collect-sysinfo")]
    match sysinfo_collector::SysCollector::new() {
        Ok(c) => return Box::new(c),
        Err(e) => {
            tracing::warn!(degraded = "collector_init", source = %e, "sysinfo collector unavailable, using stub");
        }
    }
    Box::new(StubCollector::default())
}

/// 零数据采集器:测试与 sysinfo 不可用时的降级实现。
/// 输出携带 `Degradation::CollectorInit`,字段为空/None,不伪造数值。
#[derive(Debug, Default)]
pub struct StubCollector {
    init_error: Option<String>,
}

impl StubCollector {
    pub fn failed(source: String) -> Self {
        Self {
            init_error: Some(source),
        }
    }
}

impl Collector for StubCollector {
    fn collect(&mut self, watched: &[crate::model::ids::ProcessIdentity]) -> CollectOutput {
        use crate::model::snapshot::*;
        let mut degradations = vec![Degradation::CollectorInit {
            source: self.init_error.clone().unwrap_or_else(|| "stub".into()),
        }];
        let mut hardware = HardwareSnapshot {
            timestamp: std::time::SystemTime::now(),
            cpu_usage_percent: None,
            per_core_usage: Vec::new(),
            memory_total_bytes: 0,
            memory_used_bytes: 0,
            disks: Vec::new(),
            networks: Vec::new(),
            temperatures: Vec::new(),
            fans: Vec::new(),
            voltages: Vec::new(),
            degradations: Vec::new(),
        };
        hardware.degradations.append(&mut degradations);
        CollectOutput {
            hardware,
            collected_at: std::time::Instant::now(),
            requested: watched.to_vec(),
            processes: Vec::new(),
        }
    }
}
