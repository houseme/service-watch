//! monitor-agent:薄入口(CLI → tracing → monitor_core::runtime::run)。
//!
//! CLI(DESIGN §9.1;任务约定为顶层 flags,而非 `run` 子命令):
//! `monitor-agent [--config <path>] [--log-json] [--dump-snapshot] [--check-config <path>] [--version]`
//!
//! F15 Windows Service 形态(ROADMAP v0.2;隐藏 flags,仅由 install-service.ps1
//! 写入 SCM binPath,控制台手工调用请见 service 模块 doc):
//! `monitor-agent --service-run --config <path> [--service-name <name>] [--log-file <path>] [--log-json]`
//! 非 Windows / 未启用 `windows-service` feature 的构建:打印固定消息并以退出码 1 拒绝
//! (单元测试锁定,见 `tests::service_run_platform_gate`)。
//!
//! 退出码(DESIGN §9.3):0 成功;1 配置错误;2 运行错误。
//! `runtime::StopReason::Signal` 映射 130,受控停机映射 0。

use std::path::{Path, PathBuf};

use clap::Parser;

mod service;

/// 默认配置路径(DESIGN §9.1:./service-watch.toml)。
const DEFAULT_CONFIG: &str = "service-watch.toml";

/// F15:Windows 服务名默认值(须与 install-service.ps1 / `sc.exe create <name>` 一致)。
const DEFAULT_SERVICE_NAME: &str = "service-watch";

#[derive(Debug, Parser)]
#[command(
    name = "monitor-agent",
    version,
    about = "service-watch monitoring agent"
)]
struct Cli {
    /// 配置文件路径(TOML;默认读 ./service-watch.toml,不存在则用内置默认配置)
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,

    /// 结构化 JSON 日志输出(等价 [log].json = true)
    #[arg(long)]
    log_json: bool,

    /// 采集一次并以 pretty JSON 输出后退出(空 watched,仅系统级;不进入运行时)
    #[arg(long)]
    dump_snapshot: bool,

    /// 离线校验配置文件后退出(0 = 合法,1 = 错误)
    #[arg(long, value_name = "PATH")]
    check_config: Option<PathBuf>,

    /// F15:以 Windows 服务(SCM 感知)形态运行;仅 SCM 经 binPath 携带,
    /// 非 Windows / 无 windows-service feature 的构建拒绝并退出 1
    #[arg(long, hide = true)]
    service_run: bool,

    /// F15:SCM 注册的服务名(须与 `sc.exe create <name>` 一致;默认 service-watch)
    #[arg(long, value_name = "NAME", hide = true)]
    service_name: Option<String>,

    /// F15:日志文件路径(服务模式无 console;追加写;默认 exe 同目录 agent.log)
    #[arg(long, value_name = "PATH", hide = true)]
    log_file: Option<PathBuf>,
}

fn main() {
    let cli = Cli::parse();

    // F15 服务模式:分派到 service 模块(不返回 —— 所有出口均 process::exit)。
    // 服务进程的 CWD 是 System32,默认路径 ./service-watch.toml 无意义 → 必须显式 --config。
    if cli.service_run {
        if let Some(reason) = service::unsupported_reason() {
            eprintln!("{reason}");
            std::process::exit(1);
        }
        let Some(config_path) = cli.config.as_deref() else {
            eprintln!("service mode requires --config <path>");
            std::process::exit(1);
        };
        let service_name = cli
            .service_name
            .as_deref()
            .unwrap_or(DEFAULT_SERVICE_NAME)
            .to_string();
        service::dispatch(
            config_path,
            &service_name,
            cli.log_file.as_deref(),
            cli.log_json,
        );
    }

    // --check-config:离线校验(DESIGN §9.3:配置错误 = 1)
    if let Some(path) = cli.check_config.as_deref() {
        match monitor_core::config::Config::from_path(path) {
            Ok(_) => {
                println!("config ok: {}", path.display());
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("config error: {e}");
                std::process::exit(1);
            }
        }
    }

    // --dump-snapshot:单次采集 JSON 输出(不经运行时;显式 --config 先校验快速失败)
    if cli.dump_snapshot
        && let Some(path) = cli.config.as_deref()
        && let Err(e) = monitor_core::config::Config::from_path(path)
    {
        eprintln!("config error: {e}");
        std::process::exit(1);
    }
    if cli.dump_snapshot {
        match monitor_core::runtime::collect_snapshot_json(&[]) {
            Ok(json) => {
                println!("{json}");
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("dump-snapshot failed: {e}");
                std::process::exit(2);
            }
        }
    }

    // 默认 run:解析配置 → tracing → runtime::run → 退出码映射
    let (cfg, config_note) = load_config(cli.config.as_deref()).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(1);
    });

    init_tracing(cli.log_json || cfg.log.json);
    tracing::info!(config = %config_note, "monitor-agent starting");

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("tokio runtime init failed: {e}");
            std::process::exit(2);
        }
    };
    // F14 接线:传入配置路径 —— 显式 --config 时 SIGHUP/缺省 reload 回读同一文件
    // (此前缺省回退 ./service-watch.toml,已实测确认该缺口)。
    let config_path = cli.config.clone();
    let result = runtime.block_on(monitor_core::runtime::run_with_config(cfg, config_path));
    // 退出码表(DESIGN §9.3):0 成功 / 130 信号 / 2 运行错误
    match result {
        Ok(monitor_core::runtime::StopReason::Clean) => std::process::exit(0),
        Ok(monitor_core::runtime::StopReason::Signal) => std::process::exit(130),
        Ok(monitor_core::runtime::StopReason::PersistenceFailed) => {
            eprintln!("agent exited: recovery journal persistence failed (see logs)");
            std::process::exit(2);
        }
        Ok(monitor_core::runtime::StopReason::EvalExited) => {
            eprintln!("agent exited: eval task fault (see logs)");
            std::process::exit(2);
        }
        Err(e) => {
            eprintln!("agent error: {e}");
            std::process::exit(2);
        }
    }
}

/// 统一配置装载(DESIGN §9.1 默认路径逻辑;console 与 F15 服务两条入口共用)。
/// Err 携带已格式化的错误消息 —— 呈现方式由调用方决定(console eprintln / 服务模式写日志文件)。
fn load_config(path: Option<&Path>) -> Result<(monitor_core::config::Config, String), String> {
    match path {
        Some(path) => monitor_core::config::Config::from_path(path)
            .map(|c| (c, path.display().to_string()))
            .map_err(|e| format!("config error: {e}")),
        None => {
            let default_path = Path::new(DEFAULT_CONFIG);
            if default_path.exists() {
                monitor_core::config::Config::from_path(default_path)
                    .map(|c| (c, DEFAULT_CONFIG.to_string()))
                    .map_err(|e| format!("config error ({DEFAULT_CONFIG}): {e}"))
            } else {
                // 默认路径不存在且未显式指定:内置全默认配置仍可运行
                // (无 check/service/watch;控制平面 /health /ready /status /metrics 可用)。
                // 注:传 `[agent]` 空表而非空串 —— 让 AgentCfg 走字段级默认
                // `data_dir = "./data"`(空串会命中 derive Default 的空路径)。
                monitor_core::config::Config::from_toml_str("[agent]")
                    .map(|c| (c, "(built-in defaults; no config file found)".to_string()))
                    .map_err(|e| format!("internal error: built-in default config rejected: {e}"))
            }
        }
    }
}

/// tracing 初始化(DESIGN §10):RUST_LOG EnvFilter(默认 info)+ JSON 层开关。
fn init_tracing(json: bool) {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}

/// F15:服务模式的文件 writer(追加)。仅 Windows + windows-service 的 `imp` 使用
/// (其余形态服务模式走桩,无日志重定向),cfg 门控避免 dead_code 告警。
///
/// 共享同一 append 模式句柄:每次 `write` 为单次 syscall,O_APPEND/FILE_APPEND_DATA
/// 语义保证落到文件尾(多线程交错不撕裂单次 write)。MakeWriter 每事件返回轻量克隆
/// (Arc),不走逐事件 open。
#[cfg(all(windows, feature = "windows-service"))]
struct AppendFile(std::sync::Arc<std::fs::File>);

#[cfg(all(windows, feature = "windows-service"))]
struct AppendWriter(std::sync::Arc<std::fs::File>);

#[cfg(all(windows, feature = "windows-service"))]
impl std::io::Write for AppendWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // 经 `impl Write for &File` 委托(File 的 write 为 &self,多线程并发安全)。
        std::io::Write::write(&mut &*self.0, buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        std::io::Write::flush(&mut &*self.0)
    }
}

#[cfg(all(windows, feature = "windows-service"))]
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for AppendFile {
    type Writer = AppendWriter;

    fn make_writer(&'a self) -> Self::Writer {
        AppendWriter(std::sync::Arc::clone(&self.0))
    }
}

/// tracing 初始化到文件(F15 服务模式:无 console 可去;open 失败由调用方决定回退)。
#[cfg(all(windows, feature = "windows-service"))]
fn init_tracing_to_file(json: bool, path: &Path) -> std::io::Result<()> {
    use tracing_subscriber::EnvFilter;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(AppendFile(std::sync::Arc::new(file)));
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// F15:--service-run 的平台/feature 拒绝逻辑锁定。
    /// 支持 = cfg(windows) + windows-service feature;其余形态必须以固定消息拒绝。
    #[test]
    fn service_run_platform_gate() {
        const EXPECTED: &str = "service mode requires Windows + windows-service feature";
        if cfg!(all(windows, feature = "windows-service")) {
            assert_eq!(service::unsupported_reason(), None);
        } else {
            assert_eq!(service::unsupported_reason(), Some(EXPECTED));
        }
    }

    /// load_config:显式路径 Ok/Err 的消息契约(main 与 service 两条入口共用)。
    #[test]
    fn load_config_explicit_path() {
        let dir = std::env::temp_dir().join("monitor-agent-load-config-test");
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let ok = dir.join("ok.toml");
        std::fs::write(&ok, "[agent]\n").expect("write config");
        let (cfg, note) = load_config(Some(&ok)).expect("valid config loads");
        assert_eq!(note, ok.display().to_string());
        // [agent] 空表 → AgentCfg 字段级默认 data_dir("./data",main.rs 历史 comment)。
        assert_eq!(cfg.agent.data_dir, PathBuf::from("./data"));

        let bad = dir.join("bad.toml");
        std::fs::write(&bad, "not [ valid toml (((( ").expect("write bad config");
        let err = load_config(Some(&bad)).unwrap_err();
        assert!(err.starts_with("config error"), "unexpected: {err}");
    }

    /// load_config:默认路径不存在 → 内置默认配置(none 分支与 console 主路径共用)。
    #[test]
    fn load_config_builtin_defaults() {
        // cargo test 的 CWD = crate 根;其下若有人放置 service-watch.toml 则本测试不适用。
        if Path::new(DEFAULT_CONFIG).exists() {
            return;
        }
        let (_, note) = load_config(None).expect("built-in defaults always valid");
        assert!(note.contains("built-in defaults"), "unexpected: {note}");
    }
}
