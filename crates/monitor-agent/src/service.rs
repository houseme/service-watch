//! F15:Windows Service 形态(SCM 感知,ROADMAP v0.2)。
//!
//! ## 编译期两形态
//!
//! - `all(windows, feature = "windows-service")` → 真实 SCM 集成(下方 `imp`)。
//! - 其余 target / feature 组合 → 桩:服务模式以固定消息退出 1
//!   (`UNSUPPORTED_MSG`;单元测试锁定见 main.rs `tests::service_run_platform_gate`)。
//!
//! ## 服务生命周期(SCM 调用 → 报告状态 → 停机)
//!
//! ```text
//! sc start / SCM 自启
//!   └─ CreateProcess(binPath = monitor-agent.exe --service-run --config ...)
//!        └─ main → dispatch(dispatch 线程):
//!             1. 文件日志初始化(--log-file,追加;缺省 exe 同目录 agent.log)
//!             2. load_config(必须显式 --config;服务 CWD = System32,默认路径无意义)
//!             3. 上下文写入 OnceLock → service_dispatcher::start(阻塞至服务停止)
//!                  └─ SCM 线程 → service_main → run_service:
//!                       1. 注册 control handler(Stop/Shutdown → 停机通道;
//!                          Interrogate → NoError;其余 → NotImplemented)
//!                       2. 报告 StartPending(controls_accepted = empty)
//!                       3. tokio runtime block_on:
//!                            spawn run_with_config(cfg, Some(--config))
//!                            → 报告 Running(STOP | SHUTDOWN;此后 SCM 才允许投递 Stop)
//!                            → select! { run_task 结束 | stop_rx 触发 }
//!  sc stop
//!   └─ SCM 投递 SERVICE_CONTROL_STOP → handler 送停机通道即返回(SCM 契约:
//!        handler 必须尽快返回)→ select 的 stop 分支:
//!          报告 StopPending(controls_accepted = empty,wait hint 30s)
//!          → monitor_core::runtime::request_shutdown()(F15:进程内停机请求,
//!            runtime 停机 select 的 notify 分支;permit 语义,无丢失窗口)
//!          → run 走与信号停机完全相同的完整停机序列(采集线程停 + alert 分发
//!            排空 + journal flush,runtime.rs「停机语义」)→ StopReason::Clean
//!          → 报告 Stopped(退出码 0,不计 SCM failure)→ 进程退出 0
//! ```
//!
//! 这替代了 v0.1 的 TerminateProcess 基线(DESIGN §8.6):标准构建(无
//! `windows-service` feature)下 `sc stop` = 强杀;本形态把 SERVICE_CONTROL_STOP
//! 接到 runtime 的进程内停机入口 —— 不走 console 事件(AllocConsole/CTRL_C
//! 注入存在丢失窗口与会话 0 兼容性风险,未采用),被监管子进程照常保留
//! (Job Object 无 KILL_ON_JOB_CLOSE,观察者外置)。
//!
//! ## 失败路径(如实记录)
//!
//! - Windows 事件日志写入(ReportEvent/ETW)未实现:失败仅 `tracing::error!` 到
//!   日志文件;SCM 自身对服务生命周期留痕(System 日志 7031/7034/7036)可兜底观测。
//! - run 任务 panic / 返回 Err:报告 Stopped(退出码 2),SCM 计入 failure 动作
//!   (restart/5000)。run 的停机序列有界(≤ ~1.6s,见 runtime「停机语义」),
//!   不设 abort 兜底。
//! - 服务模式下 `--config` 缺失/非法:进程在 StartServiceCtrlDispatcher 之前退出 1,
//!   SCM 以异常终止(7034)处理并触发 failure 动作。
//! - Running/StopPending 报告失败:run_service 提前返回错误(退出码 2),SCM 按
//!   异常终止处理。
//!
//! ## 日志
//!
//! 服务无 console:tracing 输出重定向到文件(`--log-file`,追加),仍走
//! tracing-subscriber(main.rs `init_tracing_to_file`,EnvFilter 同 console 形态)。

use std::path::Path;

/// 平台/feature 不满足时的固定拒绝消息(单元测试锁定)。
pub(crate) const UNSUPPORTED_MSG: &str = "service mode requires Windows + windows-service feature";

/// 服务模式是否受支持;`None` = 支持(cfg(windows) + windows-service feature)。
pub(crate) fn unsupported_reason() -> Option<&'static str> {
    if cfg!(all(windows, feature = "windows-service")) {
        None
    } else {
        Some(UNSUPPORTED_MSG)
    }
}

/// 服务模式入口(main 已先行校验 `unsupported_reason()` 与 `--config`)。
/// 不返回:所有出口均 `std::process::exit`。
pub(crate) fn dispatch(
    config_path: &Path,
    service_name: &str,
    log_file: Option<&Path>,
    log_json: bool,
) -> ! {
    imp::dispatch(config_path, service_name, log_file, log_json)
}

#[cfg(all(windows, feature = "windows-service"))]
mod imp {
    use std::ffi::OsString;
    use std::path::{Path, PathBuf};
    use std::sync::OnceLock;
    use std::time::Duration;

    use windows_service::service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
        ServiceType,
    };
    use windows_service::service_control_handler::{
        self, ServiceControlHandlerResult, ServiceStatusHandle,
    };
    use windows_service::{define_windows_service, service_dispatcher};

    /// StartPending 的 wait hint(handler 装配与 StartPending 报告的窗口)。
    const START_PENDING_HINT: Duration = Duration::from_secs(30);
    /// StopPending 的 wait hint(run 停机序列 ~1.6s 有界,余量给 SCM 状态同步)。
    const STOP_PENDING_HINT: Duration = Duration::from_secs(30);
    /// 服务模式下 `--log-file` 缺省时的日志文件名(exe 同目录)。
    const DEFAULT_LOG_NAME: &str = "agent.log";

    /// pre-dispatch 装载、service_main 消费(dispatch 线程写,SCM 线程读;
    /// `service_dispatcher::start` 之前写入并阻塞,无竞争)。
    struct ServiceCtx {
        config: monitor_core::config::Config,
        config_path: PathBuf,
        service_name: String,
    }

    static SERVICE_CTX: OnceLock<ServiceCtx> = OnceLock::new();

    define_windows_service!(ffi_service_main, service_main);

    pub(super) fn dispatch(
        config_path: &Path,
        service_name: &str,
        log_file: Option<&Path>,
        log_json: bool,
    ) -> ! {
        // 1) 日志先行(配置错误也要有处可查;文件 open 失败回退 stdout —— SCM 形态
        //    下 stdout 无处可去,仅控制台手工调试时可见)
        let log_path = match log_file {
            Some(p) => p.to_path_buf(),
            None => default_log_path(),
        };
        match crate::init_tracing_to_file(log_json, &log_path) {
            Ok(()) => tracing::info!(
                log = %log_path.display(),
                "service-mode file logging initialized"
            ),
            Err(e) => {
                eprintln!(
                    "service log init failed ({}): {e} — falling back to stdout",
                    log_path.display()
                );
                crate::init_tracing(log_json);
            }
        }

        // 2) 配置装载(复用 main 的 load_config;错误 = 配置错误,退出码 1,
        //    DESIGN §9.3 —— 此时未进 dispatcher,SCM 按异常终止 7034 处理)
        let (config, config_note) = match crate::load_config(Some(config_path)) {
            Ok(ok) => ok,
            Err(e) => {
                tracing::error!(error = %e, "service start aborted: config load failed");
                eprintln!("{e}");
                std::process::exit(1);
            }
        };

        // 3) 上下文传给 SCM 线程(start() 调用后 dispatch 线程阻塞,无竞争)
        if SERVICE_CTX
            .set(ServiceCtx {
                config,
                config_path: config_path.to_path_buf(),
                service_name: service_name.to_string(),
            })
            .is_err()
        {
            tracing::error!("internal error: service context already set");
            std::process::exit(2);
        }

        // 4) 进入 SCM dispatcher(阻塞至服务 Stopped;service_main 在 SCM 线程上运行)
        tracing::info!(
            config = %config_note,
            service = %service_name,
            "entering service dispatcher"
        );
        match service_dispatcher::start(service_name, ffi_service_main) {
            Ok(()) => {
                // service_main 已报告 Stopped;SCM 侧状态即真相,进程退出码无观察者。
                std::process::exit(0);
            }
            Err(e) => {
                // 典型:控制台手工运行(无 SCM 连接,ERROR_FAILED_SERVICE_CONNECTION_CONNECT
                // 1063)或 --service-name 与 SCM 注册名不一致。
                tracing::error!(error = %e, "service dispatcher failed");
                eprintln!("service dispatcher failed: {e}");
                std::process::exit(2);
            }
        }
    }

    /// --log-file 缺省:exe 同目录 agent.log(服务 CWD = System32,不可作日志落点)。
    fn default_log_path() -> PathBuf {
        std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(|dir| dir.join(DEFAULT_LOG_NAME)))
            .unwrap_or_else(|| PathBuf::from(DEFAULT_LOG_NAME))
    }

    /// SCM 线程上的服务主函数(dispatcher 经 ffi_service_main 调入)。
    fn service_main(_args: Vec<OsString>) {
        if let Err(e) = run_service() {
            // 此路径下若 control handler 尚未注册,SCM 以"启动失败/异常终止"处理
            // (System 日志 7031/7034 可见;failure 动作 restart/5000)。
            tracing::error!(error = %e, "windows service run failed");
            eprintln!("windows service run failed: {e}");
            std::process::exit(2);
        }
        // 正常路径:Stopped 已在 run_service 内报告,SCM 侧状态即真相。
        std::process::exit(0);
    }

    fn run_service() -> windows_service::Result<()> {
        let ctx = SERVICE_CTX.get().expect("service context set by dispatch");

        // ---- 1. control handler(SCM 要求 ServiceMain 尽快装配;30s 窗口第一步)----
        // 接受 Stop/Shutdown(DESIGN §8.6 语义升级:SCM stop = 优雅停机入口)。
        // handler 只投递停机通道(SCM 线程无 async 上下文;mpsc::Sender::send 为
        // 借用方法,FnMut 闭包可重复调用;unbounded 消息至多 1-2 条)即返回;
        // StopPending 报告与 request_shutdown 由 block_on 的 select 分支完成
        // (见步骤 3)。接收端已弃后的 send 失败被忽略。Interrogate 必答 NoError
        // (SCM 例行询问);其余未实现的控制如实回 NotImplemented。
        let (stop_tx, mut stop_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let status_handle =
            service_control_handler::register(&ctx.service_name, move |event| match event {
                ServiceControl::Stop | ServiceControl::Shutdown => {
                    let _ = stop_tx.send(());
                    ServiceControlHandlerResult::NoError
                }
                ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
                _ => ServiceControlHandlerResult::NotImplemented,
            })?;

        // ---- 2. StartPending(controls_accepted = empty)----
        report(
            &status_handle,
            ServiceState::StartPending,
            ServiceControlAccept::empty(),
            ServiceExitCode::NO_ERROR,
            START_PENDING_HINT,
        )?;

        // ---- 3. runtime + run(spawn 后报告 Running,把"可被 stop"窗口压到最小)----
        let rt = tokio::runtime::Runtime::new().map_err(windows_service::Error::Winapi)?;
        let config = ctx.config.clone();
        let config_path = ctx.config_path.clone();
        let outcome = rt.block_on(async {
            // run_with_config(显式 --config 作热重载目标,DESIGN F14 契约)。
            // &mut JoinHandle:JoinHandle 为 Unpin Future,stop 分支内可再次 await。
            let mut run_task = tokio::spawn(monitor_core::runtime::run_with_config(
                config,
                Some(config_path),
            ));
            // 报告 Running —— 此后 SCM 才允许投递 Stop/Shutdown。报告失败 = 无法
            // 进入 Running,经 outcome Err 提前中止(见下方 match)。
            report(
                &status_handle,
                ServiceState::Running,
                ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
                ServiceExitCode::NO_ERROR,
                Duration::ZERO,
            )?;
            tokio::select! {
                res = &mut run_task => Ok(res),
                _ = stop_rx.recv() => {
                    // SCM stop:先 StopPending(controls 清空,SCM 不再投递控制),
                    // 再触发进程内优雅停机(F15 request_shutdown → runtime notify
                    // 分支 → 与信号停机相同的完整停机序列)。recv 对发送端已弃的
                    // Err 同样进入停机(通道关闭只发生在 handler 释放之后)。
                    report(
                        &status_handle,
                        ServiceState::StopPending,
                        ServiceControlAccept::empty(),
                        ServiceExitCode::NO_ERROR,
                        STOP_PENDING_HINT,
                    )?;
                    monitor_core::runtime::request_shutdown();
                    Ok(run_task.await)
                }
            }
        });
        rt.shutdown_timeout(Duration::from_secs(1));

        // ---- 4. Stopped 报告(exit code 供 SCM 7031/7034 事件与 failure 判定)----
        // 状态报告失败(Running/StopPending 未成功):SCM 按异常终止处理,提前返回。
        let join_res = match outcome {
            Ok(res) => res,
            Err(e) => {
                tracing::error!(error = %e, "service status report failed; aborting run_service");
                return Err(e);
            }
        };
        // 退出码归一(DESIGN §9.3;0 = 受控停止,不计 SCM failure)。
        let (exit_code, reason) = match join_res {
            Ok(Ok(monitor_core::runtime::StopReason::Clean)) => (0u32, "clean"),
            Ok(Ok(monitor_core::runtime::StopReason::Signal)) => {
                // 服务形态无 console,信号分支正常不可达;防御性视为受控停止。
                (0, "signal (console debug run)")
            }
            Ok(Ok(monitor_core::runtime::StopReason::PersistenceFailed)) => {
                tracing::error!("recovery journal persistence failed; service exits with code 2");
                (2, "journal persistence failed")
            }
            Ok(Ok(monitor_core::runtime::StopReason::EvalExited)) => {
                tracing::error!(
                    "eval task fault; service exits with code 2 (SCM failure action restarts)"
                );
                (2, "eval task exited unexpectedly")
            }
            Ok(Err(e)) => {
                tracing::error!(error = %e, "agent runtime error; service exits with code 2");
                (2, "agent runtime error")
            }
            Err(join_err) => {
                // run task panic —— 非优雅,如实记录(failure 动作负责重启)。
                tracing::error!(error = %join_err, "run task panicked; service exits with code 2");
                (2, "run task panicked")
            }
        };
        tracing::info!(exit_code, reason, "reporting Stopped to SCM");
        let exit = if exit_code == 0 {
            ServiceExitCode::NO_ERROR
        } else {
            ServiceExitCode::Win32(exit_code)
        };
        if let Err(e) = report(
            &status_handle,
            ServiceState::Stopped,
            ServiceControlAccept::empty(),
            exit,
            Duration::ZERO,
        ) {
            // Stopped 报告失败:SCM 将按异常终止处理(7034)并触发 failure 动作;如实记录。
            tracing::error!(error = %e, "failed to report Stopped to SCM");
        }
        Ok(())
    }

    /// SCM 状态报告(OWN_PROCESS;checkpoint 恒 0 —— 本服务的 Start/Stop 均为
    /// 短时序,不设多阶段进度)。
    fn report(
        handle: &ServiceStatusHandle,
        state: ServiceState,
        controls_accepted: ServiceControlAccept,
        exit_code: ServiceExitCode,
        wait_hint: Duration,
    ) -> windows_service::Result<()> {
        handle.set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: state,
            controls_accepted,
            exit_code,
            checkpoint: 0,
            wait_hint,
            process_id: None,
        })
    }
}

#[cfg(not(all(windows, feature = "windows-service")))]
mod imp {
    use std::path::Path;

    pub(super) fn dispatch(
        _config_path: &Path,
        _service_name: &str,
        _log_file: Option<&Path>,
        _log_json: bool,
    ) -> ! {
        // main 的 gate(unsupported_reason)保证不达;防御性兜底。
        eprintln!("{}", super::UNSUPPORTED_MSG);
        std::process::exit(1);
    }
}
