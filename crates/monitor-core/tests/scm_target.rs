//! SCM 目标 (windows_service) 运行时契约测试 (ROADMAP「原生 Windows Service 目标恢复」)。
//!
//! 不触真实 SCM:注入进程内假后端 (`ScmOps` 公开 trait 的测试实现),验证
//! `ProcessManager` 对 SCM 目标的全生命周期分派 —— spawn 幂等、QueryStatus
//! liveness、Stop 确认、force_kill 如实降级、停机只停本会话启动的服务。
//! 平台无关 (macOS/Windows 同一套断言);真实 SCM 高层 API 的编译证据由
//! `cargo check --target x86_64-pc-windows-msvc --features scm-target` 提供。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use monitor_core::config::{RestartCfg, ServiceCfg};
use monitor_core::model::control::RecoveryState;
use monitor_core::model::ids::ProcessIdentity;
use monitor_core::recover::process::ProcessManager;
use monitor_core::recover::process::windows::{ScmOps, ScmServiceState, ScmStartOutcome};
use monitor_core::recover::{RecoverAction, RecoveryFsm, RestartPolicy};

/// 测试假后端：内存状态机 + 调用计数 (与 src 内单测的 Fake 同语义，此处为
/// 集成测试独立实现 —— 单测 Fake 是 pub(crate),不跨 crate)。
#[derive(Debug, Default)]
struct FakeScm {
    inner: Mutex<FakeState>,
}

#[derive(Debug, Default)]
struct FakeState {
    running: bool,
    transitioning: bool,
    starts: u32,
    stops: u32,
    fail_start: bool,
    fail_query: bool,
}

impl FakeScm {
    fn new(running: bool) -> Self {
        Self {
            inner: Mutex::new(FakeState {
                running,
                ..Default::default()
            }),
        }
    }

    fn set_running(&self, running: bool) {
        let mut s = self.inner.lock().unwrap();
        s.running = running;
        s.transitioning = false;
    }

    fn starts(&self) -> u32 {
        self.inner.lock().unwrap().starts
    }

    fn stops(&self) -> u32 {
        self.inner.lock().unwrap().stops
    }
}

impl ScmOps for FakeScm {
    fn start(&self, _service: &str) -> std::io::Result<ScmStartOutcome> {
        let mut s = self.inner.lock().unwrap();
        if s.fail_start {
            return Err(std::io::Error::other("fake start failure"));
        }
        if s.running {
            return Ok(ScmStartOutcome::AlreadyRunning);
        }
        s.running = true;
        s.starts += 1;
        Ok(ScmStartOutcome::Started)
    }

    fn stop(&self, _service: &str) -> std::io::Result<()> {
        let mut s = self.inner.lock().unwrap();
        s.stops += 1;
        s.running = false;
        s.transitioning = false;
        Ok(())
    }

    fn query(&self, _service: &str) -> std::io::Result<ScmServiceState> {
        let s = self.inner.lock().unwrap();
        if s.fail_query {
            return Err(std::io::Error::other("fake query failure"));
        }
        if s.transitioning {
            return Ok(ScmServiceState::Transitioning);
        }
        if s.running {
            Ok(ScmServiceState::Running)
        } else {
            Ok(ScmServiceState::Stopped)
        }
    }
}

fn scm_cfg(name: &str, scm: &str) -> ServiceCfg {
    ServiceCfg {
        name: name.into(),
        command: String::new(),
        args: Vec::new(),
        workdir: None,
        env: Default::default(),
        console: false,
        windows_service: Some(scm.into()),
        depends_on: Vec::new(),
        restart: RestartCfg::default(),
        activate: None,
    }
}

/// 全生命周期:spawn(幂等 start)→ QueryStatus liveness → 停止 → 退出确认
/// → 再 spawn。配合 FSM:on_scm_spawn_ok → liveness ok/gone 驱动恢复。
#[tokio::test]
async fn scm_lifecycle_end_to_end_with_fsm() {
    let fake = Arc::new(FakeScm::new(false));
    let mut pm = ProcessManager::with_scm_ops(fake.clone());
    pm.register(scm_cfg("wsvc", "W3SVC"));

    let mut fsm = RecoveryFsm::new("wsvc", test_policy());
    let t0 = std::time::Instant::now();
    let tick = |secs: u64| t0 + Duration::from_secs(secs);

    // 冷启动：服务停止 → spawn 门通过 → Start
    assert!(!pm.is_running("wsvc"));
    let id = pm.spawn("wsvc").await.unwrap();
    assert_eq!(fake.starts(), 1);
    // 占位身份:eval 未接线 SCM 分派时的机械兼容值;身份链不登记
    assert_eq!(id, ProcessIdentity::new(0, 0));
    assert!(pm.identity("wsvc").is_none(), "SCM 目标不得登记进程身份");
    let actions = fsm.on_scm_spawn_ok(tick(1));
    assert_eq!(fsm.state(), RecoveryState::Starting);
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, RecoverAction::Audit { .. }))
    );

    // liveness:QueryStatus == Running 即证据;存活过 min_uptime → Running
    assert!(pm.is_running("wsvc"));
    let _ = fsm.on_liveness_ok(tick(11));
    assert_eq!(fsm.state(), RecoveryState::Running);

    // 服务停止:is_running false;poll_exit 上报一次退出 (code=None)
    fake.set_running(false);
    assert!(!pm.is_running("wsvc"));
    let exit = pm.poll_exit("wsvc").await.expect("Stopped 必须上报退出");
    assert_eq!(exit.code, None);
    assert!(pm.poll_exit("wsvc").await.is_none(), "只上报一次");

    // FSM 恢复:gone 确认 → Recovering → 排程 → 重启 (SCM 再 Start)→ Starting
    let _ = fsm.on_liveness_gone(tick(20));
    let actions = fsm.on_liveness_gone(tick(25));
    assert_eq!(fsm.state(), RecoveryState::Recovering);
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, RecoverAction::ScheduleSpawn { .. }))
    );
    let _ = fsm.on_tick(tick(26));
    pm.spawn("wsvc").await.unwrap();
    assert_eq!(fake.starts(), 2);
    let actions = fsm.on_scm_spawn_ok(tick(26));
    assert_eq!(fsm.state(), RecoveryState::Starting);
    assert!(
        fsm.on_child_exit(tick(30), ProcessIdentity::new(0, 0))
            .is_empty(),
        "on_scm_spawn_ok 不落身份：退出事件必须被身份校验拒绝 (gone 路径才是 SCM 死亡入口)"
    );
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, RecoverAction::Audit { .. }))
    );
}

/// 冷启动时服务已运行:spawn 幂等成功 (不重复发 Start),监控照常。
#[tokio::test]
async fn scm_spawn_is_idempotent_for_already_running_service() {
    let fake = Arc::new(FakeScm::new(true));
    let mut pm = ProcessManager::with_scm_ops(fake.clone());
    pm.register(scm_cfg("wsvc", "W3SVC"));
    assert!(pm.is_running("wsvc"));
    pm.spawn("wsvc").await.unwrap();
    assert_eq!(fake.starts(), 0, "已 Running 不重复发 Start");
    assert!(pm.identity("wsvc").is_none());
}

/// 三值证据出口 (第六轮评审 P1 配套):scm_state 透传 Transitioning / Stopped /
/// Running / 查询失败，eval 据此区分"启动在途的正常过渡"与"确认死亡"——
/// 过渡态不得被抹平为 false 后喂 on_liveness_gone(否则慢启动服务被 Stop 掐死)。
#[tokio::test]
async fn scm_state_exposes_transitioning_and_failure_distinctly() {
    let fake = Arc::new(FakeScm::new(false));
    let mut pm = ProcessManager::with_scm_ops(fake.clone());
    pm.register(scm_cfg("wsvc", "W3SVC"));

    // 非 SCM 目标 → None(与查询失败区分的责任在调用方:eval 分支已按配置分流)
    let mut plain = ProcessManager::new();
    plain.register(ServiceCfg {
        name: "native".into(),
        command: "/bin/x".into(),
        args: Vec::new(),
        workdir: None,
        env: Default::default(),
        console: false,
        windows_service: None,
        depends_on: Vec::new(),
        restart: RestartCfg::default(),
        activate: None,
    });
    assert!(plain.scm_state("native").is_none(), "非 SCM 目标恒 None");

    // spawn 后过渡态 (START_PENDING):Transitioning 可见，poll_exit 不误报退出
    pm.spawn("wsvc").await.unwrap();
    fake.inner.lock().unwrap().transitioning = true;
    assert_eq!(
        pm.scm_state("wsvc"),
        Some(ScmServiceState::Transitioning),
        "启动在途必须与确认死亡可区分"
    );
    assert!(pm.poll_exit("wsvc").await.is_none(), "过渡态不是退出");
    assert!(!pm.is_running("wsvc"), "过渡态未确认运行 (契约不变)");

    // 过渡结束 → Running;再停止 → Stopped(死亡证据)
    fake.set_running(true);
    assert_eq!(pm.scm_state("wsvc"), Some(ScmServiceState::Running));
    fake.set_running(false);
    assert_eq!(pm.scm_state("wsvc"), Some(ScmServiceState::Stopped));

    // 查询失败 → None(eval 分支在 SCM 配置内将 None 视为"本轮不采信")
    fake.inner.lock().unwrap().fail_query = true;
    assert!(pm.scm_state("wsvc").is_none());
}

/// 停止路径:signal_stop 发 Stop(不阻塞)、stop_grace 内 QueryStatus 确认;
/// force_kill 无更强手段 → 再次 Stop(幂等);查询失败不判死。
#[tokio::test]
async fn scm_stop_force_kill_and_query_failure_semantics() {
    let fake = Arc::new(FakeScm::new(false));
    let mut pm = ProcessManager::with_scm_ops(fake.clone());
    pm.register(scm_cfg("wsvc", "W3SVC"));
    pm.spawn("wsvc").await.unwrap();

    // 查询失败:is_running 不可证明存活 (false),poll_exit 不判死
    fake.inner.lock().unwrap().fail_query = true;
    assert!(!pm.is_running("wsvc"));
    assert!(pm.poll_exit("wsvc").await.is_none());
    fake.inner.lock().unwrap().fail_query = false;

    // graceful stop:发 Stop 即返回 (不阻塞);停止完成由 QueryStatus 确认。
    // 真实服务停止需要时间 (过渡态不误报的语义由 end_to_end 用例覆盖);
    // 假实现即时停止 → poll_exit 直接确认退出一次。
    pm.signal_stop("wsvc", Duration::from_secs(15)).unwrap();
    assert_eq!(fake.stops(), 1);
    assert!(
        pm.stop_requested_at("wsvc").is_some(),
        "stop 已登记 (grace 升级判定用)"
    );
    let exit = pm.poll_exit("wsvc").await.expect("停止完成 → 退出确认");
    assert_eq!(exit.code, None);
    assert!(pm.spawned_at_wall("wsvc").is_none(), "退出上报后会话结束");

    // force_kill:SCM 无 Terminate → 再次 Stop(幂等;会话已结束仍成功，
    // FSM grace 升级路径依赖此语义),但无会话则不再有退出可上报
    pm.force_kill("wsvc").unwrap();
    assert_eq!(fake.stops(), 2);
    assert!(pm.poll_exit("wsvc").await.is_none());
}

/// 启动失败诚实上报 (非 Windows/无 feature 时生产后端同样返回 Err):
/// eval 以 on_spawn_result(None) 喂 FSM → CrashLoop 计入预算。
#[tokio::test]
async fn scm_start_failure_maps_to_spawn_error() {
    let fake = Arc::new(FakeScm::new(false));
    fake.inner.lock().unwrap().fail_start = true;
    let mut pm = ProcessManager::with_scm_ops(fake);
    pm.register(scm_cfg("wsvc", "W3SVC"));
    let err = pm.spawn("wsvc").await.unwrap_err().to_string();
    assert!(err.contains("SCM"), "{err}");
    let mut fsm = RecoveryFsm::new("wsvc", test_policy());
    let t0 = std::time::Instant::now();
    let actions = fsm.on_spawn_result(t0, None);
    assert_eq!(fsm.state(), RecoveryState::CrashLoop);
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, RecoverAction::JournalRestart { .. }))
    );
}

/// 停机边界:shutdown_all 只停本会话 spawn 过的服务 (观察者外置)。
#[tokio::test]
async fn scm_shutdown_stops_only_session_started_services() {
    let fake = Arc::new(FakeScm::new(false));
    let mut pm = ProcessManager::with_scm_ops(fake.clone());
    pm.register(scm_cfg("started", "SvcA"));
    pm.register(scm_cfg("external", "SvcB"));
    pm.spawn("started").await.unwrap();
    pm.shutdown_all().await;
    assert_eq!(fake.stops(), 1, "未 spawn 过的 SCM 服务不被停机");
    assert!(pm.spawned_at_wall("started").is_none());
}

fn test_policy() -> RestartPolicy {
    let cfg = RestartCfg {
        min_uptime: Duration::from_secs(10),
        startup_grace: Duration::from_secs(30),
        stop_grace: Duration::from_secs(15),
        backoff_initial: Duration::from_secs(1),
        backoff_max: Duration::from_secs(8),
        max_restarts: 5,
        restart_window: Duration::from_secs(60),
        confirmations: 2,
        hung_action: Default::default(),
    };
    RestartPolicy::from(&cfg)
}

/// 配置校验:windows_service 与 command 二选一、与进程启动字段互斥 (运行时
/// 证据;与 src/config.rs 内单测同源，此处经公开 API 复验)。
#[test]
fn config_validation_scm_mutual_exclusion() {
    // 合法:SCM 目标 + restart/depends_on 照常
    let ok = r#"
        [[service]]
        name = "wsvc"
        windows_service = "W3SVC"
        depends_on = ["db"]
        [service.restart]
        stop_grace = "30s"
        [[service]]
        name = "db"
        command = "/bin/db"
    "#;
    let cfg = monitor_core::config::Config::from_toml_str(ok).unwrap();
    assert_eq!(cfg.services[0].windows_service.as_deref(), Some("W3SVC"));
    assert_eq!(cfg.services[0].command, "");

    // 互斥逐项拒绝
    for field in [
        "command = '/bin/app'",
        "args = ['--x']",
        "workdir = '/tmp'",
        "console = true",
    ] {
        let source = format!("[[service]]\nname = 'app'\nwindows_service = 'W3SVC'\n{field}");
        let err = monitor_core::config::Config::from_toml_str(&source)
            .unwrap_err()
            .to_string();
        assert!(err.contains("windows_service"), "{source}: {err}");
    }
    // activate 互斥
    let activate = "[[service]]\nname = 'app'\nwindows_service = 'W3SVC'\n[service.activate]\ncommand = 'hook'";
    assert!(monitor_core::config::Config::from_toml_str(activate).is_err());
    // 二选一：都配 / 都缺 / 空白
    let both = "[[service]]\nname = 'app'\ncommand = '/bin/app'\nwindows_service = 'W3SVC'";
    assert!(monitor_core::config::Config::from_toml_str(both).is_err());
    let neither = "[[service]]\nname = 'app'";
    assert!(monitor_core::config::Config::from_toml_str(neither).is_err());
    let blank = "[[service]]\nname = 'app'\nwindows_service = ' '";
    assert!(monitor_core::config::Config::from_toml_str(blank).is_err());
}
