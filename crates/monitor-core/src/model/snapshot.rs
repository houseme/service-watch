//! 采集快照与降级模型(DESIGN.md §5、§6)。

use serde::Serialize;
use std::time::SystemTime;

use super::ids::ProcessIdentity;

#[derive(Debug, Clone, Serialize)]
pub struct HardwareSnapshot {
    /// 事件时间戳(仅展示;一切时长判定用单调时钟)。
    pub timestamp: SystemTime,
    /// 全局 CPU 使用率;`None` = 首采尚未生效(不伪造 0,配 Degradation)。
    pub cpu_usage_percent: Option<f32>,
    pub per_core_usage: Vec<Option<f32>>,
    pub memory_total_bytes: u64,
    pub memory_used_bytes: u64,
    pub disks: Vec<DiskUsage>,
    pub networks: Vec<NetIo>,
    /// 空 Vec + 对应 Degradation 表示平台无温度数据。
    pub temperatures: Vec<Temperature>,
    /// 深度硬件采集的风扇转速。未启用 `collect-deep-hardware` 时为空。
    pub fans: Vec<FanSpeed>,
    /// 深度硬件采集的电压。Linux 为 hwmon 驱动上报的芯片引脚电压。
    pub voltages: Vec<Voltage>,
    pub degradations: Vec<Degradation>,
}

/// 风扇传感器。停止转动的实测 0 RPM 是有效读数,读取失败为 None。
#[derive(Debug, Clone, Serialize)]
pub struct FanSpeed {
    pub label: String,
    pub rpm: Option<f32>,
}

/// 电压传感器,以伏为单位,不推断主板电阻分压比例。
#[derive(Debug, Clone, Serialize)]
pub struct Voltage {
    pub label: String,
    pub volts: Option<f32>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeepSensorKind {
    Fan,
    Voltage,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SensorUnavailableReason {
    Unsupported,
    NoSensors,
    PermissionDenied,
    Io,
    InvalidData,
    Disabled,
    Fault,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiskUsage {
    pub name: String,
    pub total_bytes: u64,
    pub available_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct NetIo {
    pub name: String,
    pub received_bytes: u64,
    pub transmitted_bytes: u64,
}

/// 单个温度传感器读数;`None` = 该传感器本次不可用(不伪造)。
#[derive(Debug, Clone, Serialize)]
pub struct Temperature {
    pub label: String,
    pub celsius: Option<f32>,
}

/// 进程级快照(部分刷新协议产出,DESIGN.md §6.2)。
#[derive(Debug, Clone, Serialize)]
pub struct ProcessSnapshot {
    pub identity: ProcessIdentity,
    /// `None` = 本轮未刷新到该进程(可能已死亡,死亡判定见 §6.2)。
    pub cpu_usage_percent: Option<f32>,
    pub memory_bytes: Option<u64>,
    pub status: Option<String>,
}

/// 机读降级枚举(非自由文本,DESIGN.md §5)。
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", content = "detail")]
pub enum Degradation {
    TemperatureUnavailable {
        platform: &'static str,
    },
    TemperatureImplausible {
        label: String,
        raw: f32,
    },
    CpuFirstSample {
        note: &'static str,
    },
    CollectorInit {
        source: String,
    },
    DeepHardwareUnavailable {
        platform: &'static str,
        sensor: DeepSensorKind,
        reason: SensorUnavailableReason,
    },
    SensorRead {
        label: String,
        sensor: DeepSensorKind,
        reason: SensorUnavailableReason,
    },
}

impl Degradation {
    /// 日志/告警用短标识(结构化字段 `degraded`)。
    pub fn kind(&self) -> &'static str {
        match self {
            Self::TemperatureUnavailable { .. } => "temperature_unavailable",
            Self::TemperatureImplausible { .. } => "temperature_implausible",
            Self::CpuFirstSample { .. } => "cpu_first_sample",
            Self::CollectorInit { .. } => "collector_init",
            Self::DeepHardwareUnavailable { .. } => "deep_hardware_unavailable",
            Self::SensorRead { .. } => "sensor_read",
        }
    }
}
