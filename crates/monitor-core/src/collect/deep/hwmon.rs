//! Linux hwmon 标准 sysfs ABI，只读 fanN_input(RPM)/inN_input(mV)。
//! 单位与 fault 语义依据 https://docs.kernel.org/hwmon/sysfs-interface.html
//! (核对日期 2026-11-12)。电压保留驱动的引脚读数，不推断主板分压比例。

use std::fs;
use std::io;
use std::path::Path;

use super::Readings;
use crate::model::snapshot::{DeepSensorKind, FanSpeed, SensorUnavailableReason, Voltage};

fn reason(error: io::Error) -> SensorUnavailableReason {
    match error.kind() {
        io::ErrorKind::PermissionDenied => SensorUnavailableReason::PermissionDenied,
        io::ErrorKind::NotFound => SensorUnavailableReason::NoSensors,
        _ => SensorUnavailableReason::Io,
    }
}

/// 返回标准输入属性的通道名。阈值、PWM、名称相似的文件均不参与采集。
fn channel(name: &str) -> Option<(&str, DeepSensorKind)> {
    let stem = name.strip_suffix("_input")?;
    let (digits, sensor) = if let Some(digits) = stem.strip_prefix("fan") {
        (digits, DeepSensorKind::Fan)
    } else {
        (stem.strip_prefix("in")?, DeepSensorKind::Voltage)
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((stem, sensor))
}

/// 可选 enable/fault 属性缺失时按标准默认可读处理。属性存在但读失败则降级。
fn check_flags(dir: &Path, stem: &str) -> Result<(), SensorUnavailableReason> {
    for (suffix, rejected, failure) in [
        ("enable", 0, SensorUnavailableReason::Disabled),
        ("fault", 1, SensorUnavailableReason::Fault),
    ] {
        match fs::read_to_string(dir.join(format!("{stem}_{suffix}"))) {
            Ok(raw) => {
                let value = raw
                    .trim()
                    .parse::<u8>()
                    .map_err(|_| SensorUnavailableReason::InvalidData)?;
                if value > 1 {
                    return Err(SensorUnavailableReason::InvalidData);
                }
                if value == rejected {
                    return Err(failure);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(reason(e)),
        }
    }
    Ok(())
}

pub(super) fn collect(root: &Path) -> Readings {
    let mut readings = Readings::default();
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(e) => {
            let failure = reason(e);
            readings.unavailable(DeepSensorKind::Fan, failure);
            readings.unavailable(DeepSensorKind::Voltage, failure);
            return readings;
        }
    };
    let mut dirs = Vec::new();
    for entry in entries {
        match entry {
            Ok(entry) => dirs.push(entry.path()),
            Err(e) => {
                let failure = reason(e);
                readings.failed_sensor("hwmon", DeepSensorKind::Fan, failure);
                readings.failed_sensor("hwmon", DeepSensorKind::Voltage, failure);
            }
        }
    }
    dirs.sort();
    for dir in dirs {
        let name = dir.file_name().unwrap_or_default().to_string_lossy();
        if !name
            .strip_prefix("hwmon")
            .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
        {
            continue;
        }
        let chip_name = fs::read_to_string(dir.join("name")).unwrap_or_default();
        let chip = format!("{name}:{}", chip_name.trim());
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) => {
                let failure = reason(e);
                readings.failed_sensor(&chip, DeepSensorKind::Fan, failure);
                readings.failed_sensor(&chip, DeepSensorKind::Voltage, failure);
                continue;
            }
        };
        let mut paths = Vec::new();
        for entry in entries {
            match entry {
                Ok(entry) => paths.push(entry.path()),
                Err(e) => {
                    let failure = reason(e);
                    readings.failed_sensor(&chip, DeepSensorKind::Fan, failure);
                    readings.failed_sensor(&chip, DeepSensorKind::Voltage, failure);
                }
            }
        }
        paths.sort();
        for path in paths {
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            let Some((stem, sensor)) = channel(name) else {
                continue;
            };
            let display = fs::read_to_string(dir.join(format!("{stem}_label"))).unwrap_or_default();
            let label = if display.trim().is_empty() {
                format!("{chip}/{stem}")
            } else {
                format!("{chip}/{stem}:{}", display.trim())
            };
            let raw =
                check_flags(&dir, stem).and_then(|()| fs::read_to_string(&path).map_err(reason));
            let value = raw.and_then(|raw| match sensor {
                DeepSensorKind::Fan => raw
                    .trim()
                    .parse::<u32>()
                    .map(|v| v as f32)
                    .map_err(|_| SensorUnavailableReason::InvalidData),
                DeepSensorKind::Voltage => raw
                    .trim()
                    .parse::<i64>()
                    .map(|v| v as f32 / 1000.0)
                    .map_err(|_| SensorUnavailableReason::InvalidData),
            });
            if let Err(failure) = value {
                readings.failed_sensor(&label, sensor, failure);
            }
            match sensor {
                DeepSensorKind::Fan => readings.fans.push(FanSpeed {
                    label,
                    rpm: value.ok(),
                }),
                DeepSensorKind::Voltage => readings.voltages.push(Voltage {
                    label,
                    volts: value.ok(),
                }),
            }
        }
    }
    readings.missing_sensors();
    readings
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::snapshot::Degradation;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            static SEQUENCE: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "service-watch-hwmon-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(path.join("hwmon0")).unwrap();
            Self(path)
        }

        fn write(&self, name: &str, value: &str) {
            fs::write(self.0.join("hwmon0").join(name), value).unwrap();
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn hwmon_units_labels_zero_rpm_and_negative_voltage() {
        let fixture = Fixture::new();
        fixture.write("name", "nct6775\n");
        fixture.write("fan1_input", "0\n");
        fixture.write("fan1_label", "CPU fan\n");
        fixture.write("fan1_min", "999");
        fixture.write("in0_input", "1250\n");
        fixture.write("in1_input", "-12000\n");
        fixture.write("pwm1_input", "123");
        fixture.write("in_bad_input", "123");
        let out = collect(&fixture.0);
        assert_eq!(out.fans.len(), 1);
        assert_eq!(out.fans[0].rpm, Some(0.0));
        assert_eq!(out.fans[0].label, "hwmon0:nct6775/fan1:CPU fan");
        assert_eq!(out.voltages.len(), 2);
        assert_eq!(out.voltages[0].volts, Some(1.25));
        assert_eq!(out.voltages[1].volts, Some(-12.0));
        assert!(out.degradations.is_empty());
    }

    #[test]
    fn hwmon_invalid_disabled_fault_and_removed_input_are_explicit() {
        let fixture = Fixture::new();
        fixture.write("fan1_input", "NaN");
        fixture.write("fan2_input", "1000");
        fixture.write("fan2_fault", "1");
        fixture.write("in0_input", "0");
        fixture.write("in0_enable", "0");
        let out = collect(&fixture.0);
        assert!(out.fans.iter().all(|fan| fan.rpm.is_none()));
        assert!(out.voltages[0].volts.is_none());
        for expected in ["invalid_data", "fault", "disabled"] {
            assert!(
                serde_json::to_string(&out.degradations)
                    .unwrap()
                    .contains(expected)
            );
        }
        fs::remove_file(fixture.0.join("hwmon0/fan1_input")).unwrap();
        let out = collect(&fixture.0);
        assert_eq!(out.fans.len(), 1, "已移除传感器不得沿用旧读数");
        assert!(out.degradations.iter().any(|d| matches!(
            d,
            Degradation::SensorRead {
                reason: SensorUnavailableReason::Fault,
                ..
            }
        )));
    }

    #[test]
    fn hwmon_absence_and_io_errors_are_explicit() {
        let fixture = Fixture::new();
        let empty = collect(&fixture.0);
        assert_eq!(empty.degradations.len(), 2);
        let missing = collect(&fixture.0.join("missing"));
        assert_eq!(missing.degradations.len(), 2);
        fixture.write("fan1_input", "1");
        fs::create_dir(fixture.0.join("hwmon0/fan1_fault")).unwrap();
        let failed = collect(&fixture.0);
        assert!(failed.fans[0].rpm.is_none());
        assert!(failed.degradations.iter().any(|d| matches!(
            d,
            Degradation::SensorRead {
                reason: SensorUnavailableReason::Io | SensorUnavailableReason::PermissionDenied,
                ..
            }
        )));
    }
}
