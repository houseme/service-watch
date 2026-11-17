//! AppleSMC 只读用户客户端。IOKit ABI 由本机 SDK IOKitLib.h 核对，
//! SMC wire 布局/类型由 sysinfo 0.40.1 和 Stats SMC 实现交叉核对 (2026-11-12)。
//! https://github.com/exelban/stats/blob/master/SMC/smc.swift
//!
//! 每轮仅探测固定候选键，不枚举全部 SMC keyspace。服务或键不可用即降级，
//! 不写入任何控制键、不要求提权。连接在本轮结束后关闭，下轮自然重试。

use std::ffi::{c_char, c_void};
use std::mem::{offset_of, size_of};

use super::Readings;
use crate::model::snapshot::{DeepSensorKind, FanSpeed, SensorUnavailableReason, Voltage};

/// 选定 SMC 电脑供电轨的验读范围。±100 V 是宽松的低压供电可信界，
/// 用于拒绝类型解码正确但数量级异常的读数，并非某一机型的硬件额定值。
/// 保留 0 V(电源门控) 与负电压轨。未知单位的整数编码在应用本范围前直接拒绝。
const SMC_VOLTAGE_ABS_LIMIT: f32 = 100.0;

#[repr(C)]
#[derive(Default)]
struct Version {
    major: u8,
    minor: u8,
    build: u8,
    reserved: u8,
    release: u16,
}

#[repr(C)]
#[derive(Default)]
struct PowerLimit {
    version: u16,
    length: u16,
    cpu: u32,
    gpu: u32,
    memory: u32,
}

#[repr(C)]
#[derive(Default)]
struct KeyInfo {
    size: u32,
    data_type: u32,
    attributes: u8,
    reserved: [u8; 3],
}

#[repr(C)]
#[derive(Default)]
struct KeyData {
    key: u32,
    version: Version,
    version_padding: [u8; 2],
    power_limit: PowerLimit,
    info: KeyInfo,
    result: u8,
    status: u8,
    command: u8,
    command_padding: u8,
    index: u32,
    bytes: [u8; 32],
}

// AppleSMC 外部方法要求固定 wire 布局，两种 macOS 架构均需满足。
const _: () = assert!(size_of::<KeyData>() == 80);
const _: () = assert!(offset_of!(KeyData, result) == 40);
const _: () = assert!(offset_of!(KeyData, bytes) == 48);

#[link(name = "IOKit", kind = "framework")]
unsafe extern "C" {
    fn IOServiceMatching(name: *const c_char) -> *mut c_void;
    fn IOServiceGetMatchingService(main_port: u32, matching: *mut c_void) -> u32;
    fn IOServiceOpen(service: u32, task: u32, client_type: u32, connection: *mut u32) -> i32;
    fn IOObjectRelease(object: u32) -> i32;
    fn IOServiceClose(connection: u32) -> i32;
    fn IOConnectCallStructMethod(
        connection: u32,
        selector: u32,
        input: *const c_void,
        input_size: usize,
        output: *mut c_void,
        output_size: *mut usize,
    ) -> i32;
}

unsafe extern "C" {
    static mach_task_self_: u32;
}

struct Connection(u32);

impl Connection {
    fn open() -> Result<Self, SensorUnavailableReason> {
        // SAFETY: NUL-terminated constant, returned dictionary is checked before use.
        let matching = unsafe { IOServiceMatching(c"AppleSMC".as_ptr()) };
        if matching.is_null() {
            return Err(SensorUnavailableReason::NoSensors);
        }
        // SAFETY: IOKit consumes the owned matching dictionary. Main port 0 requests
        // the default port. A nonzero service handle is released exactly once below.
        let service = unsafe { IOServiceGetMatchingService(0, matching) };
        if service == 0 {
            return Err(SensorUnavailableReason::NoSensors);
        }
        let mut connection = 0;
        // SAFETY: service is an owned live IOKit object, mach_task_self_ is initialized
        // by libSystem, and connection points to writable u32 storage for this call.
        let result = unsafe { IOServiceOpen(service, mach_task_self_, 0, &mut connection) };
        // SAFETY: service was acquired above and is no longer used after release.
        unsafe { IOObjectRelease(service) };
        if result != 0 || connection == 0 {
            return Err(io_reason(result));
        }
        Ok(Self(connection))
    }

    fn call(&self, input: &KeyData) -> Result<KeyData, SensorUnavailableReason> {
        let mut output = KeyData::default();
        let mut size = size_of::<KeyData>();
        // SAFETY: self owns a live user-client connection. Both structs have the
        // asserted C wire layout. Input remains live and output writable for the
        // synchronous call. IOKit receives each buffer's actual allocation size.
        let result = unsafe {
            IOConnectCallStructMethod(
                self.0,
                2,
                std::ptr::from_ref(input).cast(),
                size_of::<KeyData>(),
                std::ptr::from_mut(&mut output).cast(),
                &mut size,
            )
        };
        if result != 0 {
            return Err(io_reason(result));
        }
        if size != size_of::<KeyData>() {
            return Err(SensorUnavailableReason::InvalidData);
        }
        Ok(output)
    }

    fn read(&self, key: [u8; 4]) -> Result<Option<Value>, SensorUnavailableReason> {
        let mut input = KeyData {
            key: u32::from_be_bytes(key),
            command: 9, // READ_KEYINFO
            ..Default::default()
        };
        let metadata = self.call(&input)?;
        // SMC 0x84 = key not found. Other protocol errors remain visible.
        if metadata.result == 0x84 {
            return Ok(None);
        }
        if metadata.result != 0 {
            return Err(SensorUnavailableReason::Io);
        }
        let size = metadata.info.size as usize;
        if size == 0 || size > input.bytes.len() {
            return Err(SensorUnavailableReason::InvalidData);
        }
        input.info.size = metadata.info.size;
        input.command = 5; // READ_BYTES
        let output = self.call(&input)?;
        if output.result != 0 {
            return Err(SensorUnavailableReason::Io);
        }
        Ok(Some(Value {
            data_type: metadata.info.data_type.to_be_bytes(),
            size,
            bytes: output.bytes,
        }))
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // SAFETY: the sole owner closes the live connection exactly once.
        unsafe { IOServiceClose(self.0) };
    }
}

fn io_reason(code: i32) -> SensorUnavailableReason {
    // IOKit common errors kIOReturnNotPrivileged / kIOReturnNotPermitted.
    match code as u32 {
        0xe00002c1 | 0xe00002e2 => SensorUnavailableReason::PermissionDenied,
        _ => SensorUnavailableReason::Io,
    }
}

struct Value {
    data_type: [u8; 4],
    size: usize,
    bytes: [u8; 32],
}

impl Value {
    fn number(&self) -> Option<f32> {
        let bytes = self.bytes.get(..self.size)?;
        let value = match (&self.data_type, bytes) {
            (b"ui8 ", [v]) => f32::from(*v),
            (b"ui16", [a, b]) => f32::from(u16::from_be_bytes([*a, *b])),
            (b"ui32", [a, b, c, d]) => u32::from_be_bytes([*a, *b, *c, *d]) as f32,
            (b"flt ", [a, b, c, d]) => f32::from_le_bytes([*a, *b, *c, *d]),
            ([sign @ (b'f' | b's'), b'p', integer, fraction], [a, b]) => {
                let integer = char::from(*integer).to_digit(16)?;
                let fraction = char::from(*fraction).to_digit(16)?;
                let expected = if *sign == b'f' { 16 } else { 15 };
                if integer + fraction != expected {
                    return None;
                }
                let raw = if *sign == b'f' {
                    f32::from(u16::from_be_bytes([*a, *b]))
                } else {
                    f32::from(i16::from_be_bytes([*a, *b]))
                };
                raw / (1_u32 << fraction) as f32
            }
            _ => return None,
        };
        value.is_finite().then_some(value)
    }

    fn volts(&self) -> Option<f32> {
        // V 前缀不足以证明数值单位。实机 VBUS 是 ui32/01000000,属于未证实
        // 电压单位的整数数据，不能作为 16,777,216 V 输出。仅接受具备已核对
        // 电压编码约定的浮点/定点类型，其长度与有限性仍由 number 验证。
        match self.data_type {
            [b'f' | b's', b'p', _, _] | [b'f', b'l', b't', b' '] => {}
            _ => return None,
        }
        let value = self.number()?;
        // fpe2 电压使用 mV，其它受支持的 fp/sp/flt 电压使用 V。
        let volts = if self.data_type == *b"fpe2" {
            value / 1000.0
        } else {
            value
        };
        (-SMC_VOLTAGE_ABS_LIMIT..=SMC_VOLTAGE_ABS_LIMIT)
            .contains(&volts)
            .then_some(volts)
    }
}

pub(super) fn collect() -> Readings {
    let mut readings = Readings::default();
    let connection = match Connection::open() {
        Ok(connection) => connection,
        Err(failure) => {
            readings.unavailable(DeepSensorKind::Fan, failure);
            readings.unavailable(DeepSensorKind::Voltage, failure);
            return readings;
        }
    };
    // 4-character SMC fan keys have a single decimal index. Missing keys are
    // normal across fanless machines and models with fewer fans.
    for index in b'0'..=b'9' {
        let key = [b'F', index, b'A', b'c'];
        let label = format!("SMC/{}", String::from_utf8_lossy(&key));
        let value = match connection.read(key) {
            Ok(None) => continue,
            Ok(Some(value)) => value
                .number()
                .filter(|rpm| *rpm >= 0.0)
                .ok_or(SensorUnavailableReason::InvalidData),
            Err(failure) => Err(failure),
        };
        if let Err(failure) = value {
            readings.failed_sensor(&label, DeepSensorKind::Fan, failure);
        }
        readings.fans.push(FanSpeed {
            label,
            rpm: value.ok(),
        });
    }
    // Bounded common voltage keys. Names remain literal SMC keys because their
    // physical meaning varies by model. A missing key is never substituted.
    for key in [
        *b"VP0C", *b"VC0C", *b"VC0c", *b"VC0R", *b"VG0C", *b"VM0R", *b"VBAT", *b"VD0R", *b"VBUS",
    ] {
        let label = format!("SMC/{}", String::from_utf8_lossy(&key));
        let value = match connection.read(key) {
            Ok(None) => continue,
            Ok(Some(value)) => value.volts().ok_or(SensorUnavailableReason::InvalidData),
            Err(failure) => Err(failure),
        };
        if let Err(failure) = value {
            readings.failed_sensor(&label, DeepSensorKind::Voltage, failure);
        }
        readings.voltages.push(Voltage {
            label,
            volts: value.ok(),
        });
    }
    readings.missing_sensors();
    readings
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(data_type: [u8; 4], bytes: &[u8]) -> Value {
        let mut value = Value {
            data_type,
            size: bytes.len(),
            bytes: [0; 32],
        };
        value.bytes[..bytes.len()].copy_from_slice(bytes);
        value
    }

    #[test]
    fn smc_fixed_float_integer_decoders_and_units() {
        assert_eq!(value(*b"fpe2", &[0x5d, 0xc0]).number(), Some(6000.0));
        assert_eq!(
            value(*b"flt ", &4957_f32.to_le_bytes()).number(),
            Some(4957.0)
        );
        assert_eq!(value(*b"fp2e", &[0x50, 0]).volts(), Some(1.25));
        assert_eq!(value(*b"fpe2", &[0x13, 0x88]).volts(), Some(1.25));
        assert_eq!(value(*b"sp78", &[0xff, 0]).number(), Some(-1.0));
        assert_eq!(value(*b"ui16", &[0x01, 0]).number(), Some(256.0));
        assert_eq!(value(*b"ui32", &[0, 0, 1, 0]).number(), Some(256.0));
        assert_eq!(value(*b"ui8 ", &[0]).number(), Some(0.0));
    }

    #[test]
    fn smc_rejects_bad_sizes_types_and_nonfinite_values() {
        assert!(value(*b"fpe2", &[]).number().is_none());
        assert!(value(*b"fpe2", &[0]).number().is_none());
        assert!(value(*b"fpff", &[0, 1]).number().is_none());
        assert!(value(*b"str ", &[0]).number().is_none());
        assert!(value(*b"flt ", &f32::NAN.to_le_bytes()).number().is_none());
        assert!(
            value(*b"flt ", &f32::INFINITY.to_le_bytes())
                .number()
                .is_none()
        );
        let mut oversized = value(*b"fpe2", &[0, 1]);
        oversized.size = 100;
        assert!(oversized.number().is_none());
    }

    #[test]
    fn smc_voltage_requires_known_units_and_trusted_magnitude() {
        // 当前主机 VBUS 的原始类型/内容。整数能解码，物理单位没有电压合同。
        let vbus = value(*b"ui32", &[0x01, 0x00, 0x00, 0x00]);
        assert_eq!(vbus.number(), Some(16_777_216.0));
        assert!(vbus.volts().is_none());
        assert!(value(*b"ui8 ", &[1]).volts().is_none());
        assert!(value(*b"ui16", &[0, 12]).volts().is_none());

        // VD0R 的实测 flt 原始值属于受支持的电压编码及可信范围。
        let observed = value(*b"flt ", &[0x0b, 0x69, 0xde, 0x41]);
        assert_eq!(
            observed.volts(),
            Some(f32::from_le_bytes([0x0b, 0x69, 0xde, 0x41]))
        );
        for valid in [-100.0_f32, -12.0, 0.0, 28.0, 100.0] {
            assert_eq!(value(*b"flt ", &valid.to_le_bytes()).volts(), Some(valid));
        }
        for invalid in [-100.01_f32, 100.01, 16_777_216.0, f32::NAN, f32::INFINITY] {
            assert!(value(*b"flt ", &invalid.to_le_bytes()).volts().is_none());
        }
        assert!(value(*b"fpf1", &[0xff, 0xff]).volts().is_none());
    }
}
