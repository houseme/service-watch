//! 可选只读深度采集。通过包装默认采集器接入 runtime 与 dump-snapshot。

#[cfg(any(target_os = "linux", test))]
mod hwmon;
#[cfg(target_os = "macos")]
mod smc;

use crate::collect::{CollectOutput, Collector};
use crate::model::ids::ProcessIdentity;
use crate::model::snapshot::{
    DeepSensorKind, Degradation, FanSpeed, SensorUnavailableReason, Voltage,
};

pub(super) struct DeepCollector {
    base: Box<dyn Collector>,
}

impl DeepCollector {
    pub(super) fn new(base: Box<dyn Collector>) -> Self {
        Self { base }
    }
}

#[derive(Default)]
struct Readings {
    fans: Vec<FanSpeed>,
    voltages: Vec<Voltage>,
    degradations: Vec<Degradation>,
}

impl Readings {
    fn unavailable(&mut self, sensor: DeepSensorKind, reason: SensorUnavailableReason) {
        self.degradations
            .push(Degradation::DeepHardwareUnavailable {
                platform: std::env::consts::OS,
                sensor,
                reason,
            });
    }

    #[cfg(any(target_os = "linux", target_os = "macos", test))]
    fn missing_sensors(&mut self) {
        if self.fans.is_empty() {
            self.unavailable(DeepSensorKind::Fan, SensorUnavailableReason::NoSensors);
        }
        if self.voltages.is_empty() {
            self.unavailable(DeepSensorKind::Voltage, SensorUnavailableReason::NoSensors);
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos", test))]
    fn failed_sensor(
        &mut self,
        label: &str,
        sensor: DeepSensorKind,
        reason: SensorUnavailableReason,
    ) {
        self.degradations.push(Degradation::SensorRead {
            label: label.to_owned(),
            sensor,
            reason,
        });
    }
}

impl Collector for DeepCollector {
    fn collect(&mut self, watched: &[ProcessIdentity]) -> CollectOutput {
        let mut out = self.base.collect(watched);
        #[cfg(target_os = "linux")]
        let readings = hwmon::collect(std::path::Path::new("/sys/class/hwmon"));
        #[cfg(target_os = "macos")]
        let readings = smc::collect();
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let readings = {
            let mut readings = Readings::default();
            readings.unavailable(DeepSensorKind::Fan, SensorUnavailableReason::Unsupported);
            readings.unavailable(
                DeepSensorKind::Voltage,
                SensorUnavailableReason::Unsupported,
            );
            readings
        };
        out.hardware.fans = readings.fans;
        out.hardware.voltages = readings.voltages;
        out.hardware.degradations.extend(readings.degradations);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect::StubCollector;

    #[test]
    fn factory_wrapper_preserves_base_degradation_and_processes() {
        let mut collector = DeepCollector::new(Box::new(StubCollector::default()));
        let out = collector.collect(&[]);
        assert!(out.processes.is_empty());
        assert!(
            out.hardware
                .degradations
                .iter()
                .any(|d| { matches!(d, Degradation::CollectorInit { .. }) })
        );
        // 每类硬件必须有读数或显式降级,实际数量由运行平台决定。
        assert!(
            !out.hardware.fans.is_empty()
                || out.hardware.degradations.iter().any(|d| {
                    matches!(
                        d,
                        Degradation::DeepHardwareUnavailable {
                            sensor: DeepSensorKind::Fan,
                            ..
                        }
                    )
                })
        );
        assert!(
            !out.hardware.voltages.is_empty()
                || out.hardware.degradations.iter().any(|d| {
                    matches!(
                        d,
                        Degradation::DeepHardwareUnavailable {
                            sensor: DeepSensorKind::Voltage,
                            ..
                        }
                    )
                })
        );
    }
}
