# BENCHMARK.md — 同类项目对照分析 (背景研究，非规约)

> 自 AGENT.md 拆出 (2026-11-10)。本表是选型依据存档;工程约束以 AGENT.md 为准。

| 项目 | 定位 | 可借鉴点 | 关键缺口 (相对于本需求) |
|---|---|---|---|
| **StatBeacon**(Joel Löf) | 单文件 Rust 监控 agent,sysinfo + tokio + reqwest,CPU/内存/温度阈值告警，POST 到 HTTP/Slack endpoint | 极简「采集—阈值判断—告警投递」闭环，TOML 配置，Slack webhook 直投 | 无进程守护/重启;无超时检测;单文件结构不可复用 |
| **Vigil**(valeriansaliou) | 分布式微服务状态页，接收 reporter 上报，支持 Slack/SMS 告警 | 状态页与上报分离;告警去噪与状态模型 | 依赖外部 reporter 主动上报，非本地进程级监控;无硬件采集与进程重启 |
| **Malkuth**(celestia-island) | 可组合服务监督工具箱:spawn、健康探测、失败重启、连接排空、滚动更新 | Supervised workers 模型;可插拔 transport;watchdog CLI | 偏监督协议与滚动发布;跨平台深度依赖 Unix 信号与 socket |
| **shep / shep-daemon** | Rust 进程管理器:restart policy、指数退避、restart budget、min_uptime、SIGKILL 升级、内存天花板重启 | **生产级恢复策略模板**:backoff、budget、graceful→SIGKILL、进程组清理 | 明确不支持 Windows;告警偏 webhook 轻量 |
| **flux-keeper**(Lucineer) | Health monitoring、stuck detection、watchdog,per-check interval/timeout/threshold,Ok→Warning→Critical | **超时与连续失败语义**:timeout 未上报即失败;连续失败阈值区分抖动与死亡 | 面向 agent fleet，非系统级硬件 + 服务混合监控 |
| **Guardian**(ark-asa-server-manager) | sysinfo 低开销进程监控，crash detection、auto-restart、crash counter、rolling log | 部分刷新策略 (`ProcessesToUpdate`) 避免全量扫描;多次验证再确认失败 | 耦合 Tauri 桌面应用;无跨平台硬件传感器抽象 |
| **cuda-keeper-core** | 便携 Rust 监控库，零运行时依赖 (仅 serde),Linux 读 /proc，其他平台优雅降级 | 「observer cannot be observed」;库与部署形态解耦 | 硬件采集深度有限;告警自建;无内置进程重启 |
| **task-supervisor**(crates.io) | 监督 Tokio 任务:health-check interval、restart limits、backoff、dead-task threshold | 异步任务级恢复策略，「拉活」语义 | 只监督进程内 Tokio 任务，不覆盖 OS 进程 |

**对照结论**:没有单一项目同时满足全部需求。工程路线——sysinfo 做跨平台采集底座、shep 的恢复策略模型做拉活、flux-keeper 的超时/连续失败语义做告警判定、StatBeacon 的轻量投递做通知，在 Rust 2024 lib/bin 分离 workspace 中自建统一抽象。

**review 后的重要修正**(详见 DESIGN.md):原表「CTRL_BREAK → TerminateProcess 降级」经核实不可行 (CTRL_BREAK 只对共享 console 且带 CREATE_NEW_PROCESS_GROUP 的进程生效),Windows 路径已改为 TerminateProcess 基线 + Job Object;`ProcessesToUpdate` 部分刷新策略经 sysinfo 0.40.1 实测可行，但每轮必须传入全部 watched PID。
