# 部署与使用指南

核对日期为 2026-11-12，适用于当前工作区实现。本文从构建和配置开始，覆盖 Linux、macOS、Windows 安装与日常操作。平台模板位于 `docs/deploy/`，详细状态机和排障流程见运维手册。[部署模板](deploy/README.md) · [运维手册](RUNBOOK.md)

## 1. 部署方式与目录

`monitor-agent` 负责采集、检查、告警和 supervise 恢复，`monitorctl` 通过本机 HTTP 控制面查询与操作。agent 使用前台进程形态，由系统服务管理器守护。

| 项目 | Linux | macOS | Windows |
|---|---|---|---|
| 服务管理器 | systemd | launchd | SCM |
| agent 路径 | `/usr/local/bin/monitor-agent` | `/usr/local/bin/monitor-agent` | `C:\service-watch\monitor-agent.exe` |
| 配置路径 | `/etc/service-watch/config.toml` | `/etc/service-watch/config.toml` | `C:\service-watch\config.toml` |
| 数据目录 | `/var/lib/service-watch` | `/var/lib/service-watch` | `C:\service-watch\data` |
| 日志 | journald | `/var/log/service-watch/` | `C:\service-watch\data\agent.log` |
| 运行账户 | 模板使用 `svcwatch` | 模板使用 root，可配置 UserName | 脚本使用 LocalSystem，可配置专用账户 |

每个 agent 实例使用独立的数据目录和控制端口。数据目录保存 `journal.jsonl` 与 `spool/pending.jsonl`，运行账户需要读写权限。配置、目标可执行文件和 pidfile 需要对运行账户可读。

supervise 子进程继承 agent 的运行账户及系统服务环境。`service.env`、`service.workdir` 用于设置目标自己的环境与工作目录。部署时为服务命令、工作目录、数据目录和 pidfile 填写绝对路径。

## 2. 构建与安装前检查

在项目根目录运行，构建机安装 Rust stable 与平台所需的编译工具链。

```bash
cargo build --release -p monitor-agent -p monitorctl
./target/release/monitor-agent --version
./target/release/monitorctl --help
```

启用风扇、电压深度采集时，使用对应 feature 重新构建。

```bash
cargo build --release -p monitor-agent -p monitorctl --features collect-deep-hardware
./target/release/monitor-agent --dump-snapshot
```

Linux 使用 hwmon，macOS 使用 AppleSMC，Windows 返回机读 unsupported 降级。macOS 采集固定候选键，按类型解码，电压可信范围选为 ±100 V。传感器不可用时查看 `hardware.degradations`，其中会列出权限拒绝、无传感器或无效数据等原因。

### Windows 构建

在 Windows 的 Rust MSVC 开发环境中运行。默认 HTTP/TLS 构建依赖 NASM 与 CMake，SCM 部署需要 `windows-service` feature。

```powershell
choco install -y nasm cmake
nasm -v
cmake --version
cargo build --release -p monitor-agent --features windows-service
cargo build --release -p monitorctl
```

需要深度采集时，agent 使用 `--features windows-service,collect-deep-hardware`，ctl 使用 `--features collect-deep-hardware`。产物位于 `target\release\`。

## 3. 准备配置

复制完整模板作为起点，按实际主机修改目标、路径及 URL。模板中的应用路径需要替换为实际部署路径。[完整配置](../config/config.example.toml)

```bash
cp config/config.example.toml service-watch.toml
```

### 3.1 先运行独立 HTTP 监控

以下配置可保存为 `service-watch.toml`。HTTP URL 指向已运行应用的健康端点，服务部署时按第 1 节修改 `data_dir`。

```toml
[agent]
data_dir = "./data"

[control]
enabled = true
listen = "127.0.0.1:9170"

[collect]
interval = "5s"

[log]
json = true

[alert]
webhook_url = ""
webhook_format = "json"
failure_threshold = 3
success_threshold = 2
renotify_interval = "15m"

[[check]]
name = "external-api"
kind = "http"
url = "http://127.0.0.1:8080/healthz"
expected_status = "200-299"
interval = "10s"
timeout = "5s"
```

`webhook_url` 填写实际 http/https 通知地址后启用通知，Slack 接口可配置 `webhook_format = "slack"`。独立 HTTP 检查产生健康状态和告警。

### 3.2 交给 agent 启动与恢复应用

将下面的 service 和同名 check 加入配置，并按需替换第 3.1 节的独立检查。示例以 Unix 应用路径演示，Windows 可写 `command = "C:/apps/my-app/my-app.exe"` 和相应 workdir。

```toml
[[service]]
name = "my-app"
command = "/srv/my-app/bin/my-app"
args = ["--port", "8080"]
workdir = "/srv/my-app"
env = { RUST_LOG = "info" }

[service.restart]
min_uptime = "10s"
startup_grace = "30s"
stop_grace = "15s"
backoff_initial = "1s"
backoff_max = "60s"
max_restarts = 5
restart_window = "10m"
confirmations = 2
hung_action = "restart"

[[check]]
name = "my-app"
kind = "http"
url = "http://127.0.0.1:8080/healthz"
interval = "10s"
timeout = "5s"
```

应用以前台形式运行。agent 持有亲生进程，观察退出事件并执行恢复。检查与 service 同名时，健康结果绑定到该服务，持续健康失败可触发恢复。现有实现也支持名称尾匹配，多服务配置采用明确的同名检查便于核对绑定。

重启预算耗尽后进入 Fatal，修复根因后执行 `monitorctl reset my-app`。依赖服务可通过 `depends_on = ["db"]` 指定，引用的服务需同时出现在配置中，依赖全部达到 Running 且有进程证据后才启动。

### 3.3 监控已有进程

对已有服务添加 watch，并通过 process 检查监控资源使用。下面示例可追加到配置中。

```toml
[[watch]]
name = "existing-app"
pidfile = "/run/existing-app.pid"

[[check]]
name = "existing-app"
kind = "process"
process = "existing-app"
cpu_threshold = 90.0
mem_threshold_mb = 1024
interval = "10s"
timeout = "5s"
```

watch 使用 attach 语义，进程管理由该应用原有管理器负责。Windows 或没有 pidfile 的场景可将 `pidfile` 替换为 `pid = 1234`。process 也支持 `process = "pid:1234"`，以及已配置 service/watch 的名称。

### 3.4 校验与前台试运行

```bash
./target/release/monitor-agent --check-config service-watch.toml
./target/release/monitor-agent --config service-watch.toml
```

校验成功输出 `config ok`，退出码为 0。该命令核对 TOML 和配置约束，目标二进制可执行性、URL 连通性及运行账户权限通过实际启动确认。

另开终端检查控制面。`/health` 使用 HTTP 200 表示浅存活，`/ready` 返回就绪 JSON。

```bash
curl -fsS -o /dev/null http://127.0.0.1:9170/health
curl -fsS http://127.0.0.1:9170/ready
./target/release/monitorctl status
./target/release/monitorctl metrics
```

确认后结束前台 agent，再交由系统管理器运行。SIGINT/SIGTERM 退出码为 130，被监管子进程保留，下一次 agent 启动会进行身份对账。

## 4. Linux 安装为 systemd 服务

以下命令适用于提供 `useradd`、`nologin` 的 Linux 系统。账户已存在时复用该账户，其他发行版按本机账户工具创建等效专用账户。

```bash
getent passwd svcwatch || sudo useradd --system --user-group \
  --home-dir /var/lib/service-watch --shell /usr/sbin/nologin svcwatch

sudo install -m 0755 target/release/monitor-agent /usr/local/bin/monitor-agent
sudo install -m 0755 target/release/monitorctl /usr/local/bin/monitorctl
sudo install -d -m 0750 -o root -g svcwatch /etc/service-watch
sudo install -d -m 0750 -o svcwatch -g svcwatch /var/lib/service-watch
sudo install -m 0640 -o root -g svcwatch service-watch.toml /etc/service-watch/config.toml
```

编辑已安装配置，将 `data_dir` 设置为 `/var/lib/service-watch`，并检查目标程序、workdir 和配置文件权限。随后以运行账户校验，再安装 unit。

```bash
sudo -u svcwatch /usr/local/bin/monitor-agent --check-config /etc/service-watch/config.toml
sudo install -m 0644 docs/deploy/systemd/service-watch.service \
  /etc/systemd/system/service-watch.service
sudo systemctl daemon-reload
sudo systemctl enable --now service-watch
sudo systemctl status service-watch --no-pager
sudo journalctl -u service-watch -n 50 --no-pager
curl -fsS http://127.0.0.1:9170/ready
monitorctl status
```

模板使用 `ProtectSystem=strict`、`ProtectHome=true`、`PrivateTmp=true` 和 `NoNewPrivileges=true`，子进程继承这些限制。目标需要写 `/srv/my-app` 时，通过 `sudo systemctl edit service-watch` 添加所需路径，并授予 svcwatch 文件权限。

```ini
[Service]
ReadWritePaths=/srv/my-app
```

模板的 `KillMode=process` 保留被监管子进程，`SuccessExitStatus=130` 将信号停机视为成功。服务管理器只负责 agent 生命周期。[systemd 模板与说明](deploy/systemd/README.md)

## 5. macOS 安装为 LaunchDaemon

以下步骤使用现有模板的 root 运行身份。使用专用账户时，在 plist 设置 UserName/GroupName，并同步调整配置、数据和日志目录权限。

```bash
sudo install -m 0755 target/release/monitor-agent /usr/local/bin/monitor-agent
sudo install -m 0755 target/release/monitorctl /usr/local/bin/monitorctl
sudo mkdir -p /etc/service-watch /var/lib/service-watch /var/log/service-watch
sudo install -m 0640 -o root -g wheel service-watch.toml /etc/service-watch/config.toml
```

将配置中的 `data_dir` 改为 `/var/lib/service-watch`，再加载服务。

```bash
sudo /usr/local/bin/monitor-agent --check-config /etc/service-watch/config.toml
plutil -lint docs/deploy/launchd/com.houseme.service-watch.plist
sudo install -m 0644 -o root -g wheel \
  docs/deploy/launchd/com.houseme.service-watch.plist \
  /Library/LaunchDaemons/com.houseme.service-watch.plist
sudo launchctl bootstrap system /Library/LaunchDaemons/com.houseme.service-watch.plist
sudo launchctl print system/com.houseme.service-watch
curl -fsS http://127.0.0.1:9170/ready
monitorctl status
```

日志位于 `/var/log/service-watch/agent.log` 与 `agent.err.log`。停止服务使用 `launchctl bootout`，重新加载使用 `bootstrap`。模板的 KeepAlive 会在 job 保持加载时重启非零退出的 agent。[launchd 模板与说明](deploy/launchd/README.md)

## 6. Windows 安装为 SCM 服务

在管理员 PowerShell 中执行。先按第 2 节构建带 `windows-service` 的 agent，再准备目录和文件。

```powershell
New-Item -ItemType Directory -Force -Path C:\service-watch\data | Out-Null
Copy-Item target\release\monitor-agent.exe C:\service-watch\monitor-agent.exe
Copy-Item target\release\monitorctl.exe C:\service-watch\monitorctl.exe
Copy-Item service-watch.toml C:\service-watch\config.toml
```

修改配置，使用 `data_dir = "C:/service-watch/data"`，将应用路径及 workdir 改为实际 Windows 路径。随后安装并验证。

```powershell
& C:\service-watch\monitor-agent.exe --check-config C:\service-watch\config.toml
.\docs\deploy\windows\install-service.ps1
Get-Service service-watch
Invoke-RestMethod http://127.0.0.1:9170/ready
& C:\service-watch\monitorctl.exe status
Get-Content C:\service-watch\data\agent.log -Tail 50
```

脚本使用 New-Service 注册 SCM 入口，配置文件日志、失败恢复动作与 `failureflag 1`。可通过 InstallDir、ConfigPath、DataDir、ServiceName 指定路径和名称，DataDir 与配置中的 `agent.data_dir` 保持一致。专用账户还需具备服务登录权限及目标文件权限。

`Stop-Service service-watch` 经 SCM handler 执行停机，`Start-Service service-watch` 再次启动。当前 SCM 功能管理 agent 本身，既有 Windows 系统服务可通过 watch 监控。[Windows 安装参数与说明](deploy/windows/README.md)

## 7. 日常使用

以下命令在二进制已加入 PATH 时运行，Windows 使用对应 `.exe`。

| 操作 | 命令 |
|---|---|
| 状态表格 | `monitorctl status` |
| 原始状态 JSON | `monitorctl status --json` |
| 持续观察 | `monitorctl watch --interval 2` |
| JSON Lines 观察 | `monitorctl watch --interval 2 --json` |
| 观察时遇错退出 | `monitorctl watch --exit-on-error` |
| 立即探测 | `monitorctl check my-app` |
| 重启 supervise 目标 | `monitorctl restart my-app` |
| 清除 Fatal 和预算 | `monitorctl reset my-app` |
| JSON 指标 | `monitorctl metrics` |
| Prometheus 指标 | `monitorctl metrics --prom` |
| 本机一次硬件快照 | `monitorctl dump-snapshot` |
| 指定控制面 | `monitorctl --url http://127.0.0.1:9270 status` |

浏览器访问本机状态页，历史指标也可直接查询。

```bash
curl -fsS 'http://127.0.0.1:9170/metrics/history?range=300'
```

状态页地址为 `http://127.0.0.1:9170/`，Prometheus 抓取地址为 `/metrics?format=prometheus`。`/ready` 表示调度已启动，各目标是否健康通过 `/status` 确认。[控制面与告警语义](RUNBOOK.md)

restart、check、reset 返回 202 表示命令已入队，随后通过 status 和 journal 确认执行结果。Fatal 目标的 restart 返回 409，先处理故障再 reset。检查 journal 时可观察 restart、fatal、reset、reset_targets、process_identity 和 audit 事件。

## 8. 配置热重载

修改实际启动配置，先离线校验，再向运行中的 agent 提交 reload。

```bash
monitor-agent --check-config /etc/service-watch/config.toml
curl -fsS -X POST http://127.0.0.1:9170/v1/reload
monitorctl status
```

缺省重读启动时传入的配置路径，也可显式指定文件。

```bash
curl -fsS -X POST http://127.0.0.1:9170/v1/reload \
  -H 'Content-Type: application/json' \
  --data '{"path":"/etc/service-watch/config.toml"}'
```

Windows 使用 PowerShell 触发同一端点。

```powershell
Invoke-WebRequest -Method Post -Uri http://127.0.0.1:9170/v1/reload
```

check、service、watch 拓扑增删改支持热应用，服务重定义会停止并收割旧实例后启动新定义。agent、log、control、collect、alert、metrics 段变更通过重启 agent 生效。reload 返回 202 后，通过日志和 journal 审计确认应用结果，解析或校验失败保留当前拓扑。Unix 也支持向 agent 主进程发送 SIGHUP。

## 9. agent 重启与孤儿接管

agent 退出时保留 supervise 子进程，重启后读取 journal 的身份记录。精确确认旧身份存活后，状态呈现为 attach，继续只读监控。确认旧身份消失后，按当前配置、重启预算与退避恢复 supervise。

接管期间的进程操作由原有外部管理方式或运维处理。对账缺证据时，先检查采集权限、pid/start_time 和降级原因。Fatal 闩锁跨 agent 重启保持，解除操作仍使用 reset。

旧版 journal 缺少身份记录，以及 OS spawn 成功到首条身份确认之间的中断场景，需要人工核查遗留实例。详细持久化和对账边界见评审记录。[本轮验证与待验收项](REVIEW-2026-11-12.md)

## 10. 升级、回滚与卸载

升级前记录版本、状态和配置。停止 agent 后，备份完整数据目录、配置及旧二进制，备份需覆盖 journal 与 spool。

```bash
monitor-agent --version
monitorctl status --json > status-before.json
sudo systemctl stop service-watch
sudo tar -czf service-watch-backup.tgz \
  -C / var/lib/service-watch etc/service-watch
```

macOS 停止命令为 `sudo launchctl bootout system/com.houseme.service-watch`，Windows 为 `Stop-Service service-watch`。备份位置选择运行账户可控且有足够空间的目录。

替换二进制后重新校验配置，启动服务并依次确认 `/ready`、目标状态、journal 错误计数和告警投递。Unix 可先写入同目录临时文件，再用 mv 切换二进制，保留旧二进制用于回滚。

回滚时先核对目标版本的配置和 journal schema 兼容性，保留最新数据副本，再切换旧二进制与兼容配置。恢复历史 journal 会回退预算、Fatal 和身份状态，应结合目标当前 PID 与启动时间完成对账。具体平台的替换和卸载命令见部署总览。[升级与卸载](deploy/README.md)

卸载先停止并禁用 agent 的系统服务，处理保留运行的应用进程，再按保留期限归档配置、journal、spool 和日志。

## 11. 常见问题

| 现象 | 核对与处理 |
|---|---|
| agent 启动即退出 | 用实际服务账户执行 check-config，查看系统服务日志和退出码 |
| 9170 被占用 | 查询占用进程，调整 control.listen 后同步 monitorctl 的 --url |
| monitorctl 退出 4 | 检查 agent 运行状态、控制端口及 control-http 构建 feature |
| 配置通过但应用起不来 | 核对 command、args、workdir、账户权限和系统服务环境 |
| systemd 下目标写文件失败 | 检查目录属主和 ReadWritePaths，以及目标继承的服务隔离设置 |
| journal 写失败或确认超时 | 检查磁盘空间、数据目录权限与存储状态，恢复后检查原件再启动 |
| status 长期 attach 或等待 | 核对旧进程身份、采集权限和进程存在性证据 |
| Fatal 后未恢复 | 处理实际故障，再执行 reset 并观察后续状态 |
| webhook 不达 | 核对 URL、格式、网络和 alert_delivery_failures，检查 spool 与 journal |
| 温度、风扇或电压为空 | 查看 hardware.degradations，确认机型接口、权限和构建 feature |

agent 成功退出码为 0，配置错误为 1，运行或关键持久化错误为 2，信号退出为 130。monitorctl 不可达为 4，操作失败通常为 2，目标不存在的 3 仍取决于端点实际支持。详细状态和恢复流程见运维手册。[故障处置](RUNBOOK.md)

## 12. 验收与已知能力范围

部署后核对目标身份、健康检查绑定、实际通知投递，以及服务管理器对 agent 的启动和停止行为。深度采集在目标机查看真实读数或明确降级，长期性能通过现有脚本按实际负载验证。

```bash
bash docs/deploy/perf-check.sh ./target/release/monitor-agent 60
```

该脚本用于专门的本机合成负载验收，默认占用控制端口 19190，使用独立临时数据目录。短时通过提供快速回归证据，24 小时长跑与泄漏分析仍列在 roadmap。

控制面采用本机信任边界，默认监听回环地址。`[metrics]` 周期推送字段目前预留，运行指标通过 HTTP 接口读取。Windows SCM 真机验收、更多硬件机型覆盖、磁盘故障演练与启动事务窗口继续按 roadmap 跟踪。[后续计划](../ROADMAP.md)
