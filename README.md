# service-watch

跨平台服务与硬件监控工具，使用 Rust 2024 构建。核心逻辑位于 `monitor-core`，`monitor-agent` 运行调度与控制平面，`monitorctl` 提供查询和操作入口。

- CPU、内存、磁盘、网络、温度与进程指标采集，缺失数据通过 `Option` 和机读降级原因呈现。
- HTTP、TCP、进程指标与进程存活检查，支持 incident 去重、升级、恢复通知、flap 抑制和 webhook 重试。
- supervise 进程恢复，包含连续确认、预热期、退避、滑动重启预算、Fatal 闩锁与依赖启动顺序。
- 控制平面、只读 Web 状态页、Prometheus、历史指标、终端 watch 和目标拓扑热重载。
- 可选 `collect-deep-hardware` 深度采集，提供 Linux hwmon 与 macOS AppleSMC 风扇、电压读数。

## 快速开始

```bash
cargo build --release -p monitor-agent -p monitorctl
./target/release/monitor-agent --check-config config/config.example.toml
./target/release/monitor-agent --dump-snapshot
```

创建 `service-watch.toml`，将 `command` 换成实际可执行文件路径。

```toml
[agent]
data_dir = "./data"

[[service]]
name = "my-app"
command = "/usr/local/bin/my-app"

[[check]]
name = "my-app"
kind = "http"
url = "http://127.0.0.1:8080/healthz"
interval = "10s"
timeout = "5s"
```

检查与 service 同名时，健康结果会参与该服务的恢复判定。独立命名的检查只提供告警。完整配置涵盖 webhook、依赖、重启策略和 attach 目标。示例中的路径需按本机部署修改。[完整配置](config/config.example.toml)

```bash
./target/release/monitor-agent --config service-watch.toml

# 在另一个终端运行
./target/release/monitorctl status
./target/release/monitorctl watch --interval 2
./target/release/monitorctl metrics --prom
./target/release/monitorctl restart my-app
./target/release/monitorctl reset my-app
```

浏览器打开 [本机状态页](http://127.0.0.1:9170/)。Fatal 目标需要先处理故障并执行 `reset`。`restart`、`check`、`reset` 返回 202 表示命令已入队，实际结果通过 `status` 与 journal 核对。

## 功能边界

- attach 目标只监控。agent 退出时保留 supervise 子进程，重启后根据 journal 身份和新采集证据进行孤儿对账。
- `SIGHUP` 和 `POST /v1/reload` 应用 check、service、watch 拓扑变更。其他配置段需要重启。服务重定义会等待旧实例收割后启动新实例。
- JSON 指标位于 `/metrics`，Prometheus 格式可用 `?format=prometheus`，历史接口为 `/metrics/history?range=300`。`[metrics]` 周期推送字段目前预留。
- webhook 失败事件写入 spool，重放在收到成功确认后删除记录，7 天过期。进程被强杀前尚未落盘的内存事件仍有丢失窗口。
- 控制平面默认监听回环地址，当前接口使用本机信任边界。

## 平台与可选构建

| 能力 | Linux | macOS | Windows |
|---|---|---|---|
| 基础硬件与进程采集 | sysinfo | sysinfo | sysinfo |
| 温度 | 取决于 hwmon | 取决于机型接口 | 不可用时显式降级 |
| 风扇、电压 | hwmon，只读 | AppleSMC，只读，按机型可用 | 显式降级 |
| supervise 恢复 | 进程组 | 进程组 | Job Object |
| agent 系统守护 | systemd | launchd | SCM，需 `windows-service` |

```bash
cargo build --release -p monitor-agent -p monitorctl --features collect-deep-hardware
cargo run -p monitor-agent --features collect-deep-hardware -- --dump-snapshot

# 在 Windows 构建 SCM 服务入口
cargo build --release -p monitor-agent --features windows-service
```

深度采集默认关闭。传感器缺失、权限拒绝、故障与无效值均有机读降级。Linux 电压保留驱动报告的芯片引脚语义，单位转换为伏。macOS 使用常见 SMC 键并按类型过滤，电压可信范围选择为 ±100 V，不同机型可暴露不同传感器。

Windows 默认 HTTP/TLS 构建使用 rustls 与 aws-lc-rs，需要 NASM 和 CMake。本次本地验证工具链为 stable rustc 1.99.1，核对日期为 2026-11-12。版本与平台验证结果见评审记录。

## 文档

| 文档 | 内容 |
|---|---|
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | 完整部署与使用指南，覆盖配置、三平台安装、日常操作、升级和排障 |
| [ROADMAP.md](ROADMAP.md) | 功能规范、验收条件、进度与待办 |
| [docs/REVIEW-2026-11-12.md](docs/REVIEW-2026-11-12.md) | 多专家评审、修复与验证记录 |
| [DESIGN.md](DESIGN.md) | 配置、所有权、状态机与平台契约 |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | 热重载、Fatal、journal 与投递故障处置 |
| [docs/deploy/](docs/deploy/README.md) | systemd、launchd、Windows 部署与性能脚本 |
| [CONTRIBUTING.md](CONTRIBUTING.md) | 开发与验证命令 |
| [AGENT.md](AGENT.md) | 工程硬约束 |
| [EXECUTION.md](EXECUTION.md) | 分阶段执行计划 |
| [BENCHMARK.md](BENCHMARK.md) | 选型依据 |

所有工作区 crate 使用 Rust 2024 和 Apache-2.0，清单省略 `rust-version`。安全恢复使用进程身份校验，agent 生命周期独立于被监控服务。

## License

[Apache-2.0](LICENSE)
