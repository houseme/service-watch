# service-watch Windows 部署 (SCM / sc.exe)

> 脚本：[install-service.ps1](install-service.ps1)(`#Requires -RunAsAdministrator`)。
> 总览/升级/卸载见 [../README.md](../README.md),日常处置见 [../../RUNBOOK.md](../../RUNBOOK.md)。

## 1. 构建 (先读:NASM + CMake)

TLS 栈为 rustls + **aws-lc-rs**(reqwest 0.13 起无 ring 备选)。默认 feature 组合
(含 `probe-http` / `notify-webhook` / `control-http`) 构建需要 **NASM 与 CMake**:

```powershell
# MSVC 工具链 + NASM/CMake
rustup default stable-x86_64-pc-windows-msvc
choco install nasm cmake        # 或手动安装并加入 PATH
nasm -v; cmake --version        # 两个都必须能在 PATH 中找到

# F15 SCM 形态(生产推荐):加 windows-service feature,--service-run 才生效
cargo build --release -p monitor-agent --features windows-service
cargo build --release -p monitorctl
# 产物:target\release\monitor-agent.exe / monitorctl.exe
```

**降级组合**(无 http，不需要 NASM/CMake;F15 形态同样可叠加
`--features windows-service`):

```powershell
cargo build --release -p monitor-agent -p monitorctl `
  --no-default-features --features collect-sysinfo,windows-service
```

代价 (务必知晓):失去 HTTP 探测、Webhook 告警与控制平面 —— `monitorctl` 无法
连接 agent(`/health /status` 等端点不存在),只剩硬件/进程采集与 TCP/process
检查。生产部署不建议降级;确要降级时，告警可改用外部轮询 `journal.jsonl` 的
旁路方案。

## 2. 目录布局与安装

脚本默认使用以下目录，服务注册通过结构化参数处理路径与凭据。

| 内容 | 路径 |
|---|---|
| 二进制 | `C:\service-watch\monitor-agent.exe`、`monitorctl.exe` |
| 配置 | `C:\service-watch\config.toml`(模板：`config\config.example.toml`) |
| data_dir(journal) | `C:\service-watch\data`(须与 config `[agent].data_dir` 一致) |

```powershell
# 0) 放置文件(以管理员 PowerShell 执行)
New-Item -ItemType Directory -Force -Path C:\service-watch | Out-Null
Copy-Item target\release\monitor-agent.exe C:\service-watch\
Copy-Item target\release\monitorctl.exe    C:\service-watch\
Copy-Item config\config.example.toml       C:\service-watch\config.toml
# notepad C:\service-watch\config.toml
#   [agent].data_dir = "C:\service-watch\data"
#   [[service]] command = "C:/apps/my-app/app.exe"   # TOML 中正斜杠免转义,或写 "C:\\apps\\..."

# 1) 自检
C:\service-watch\monitor-agent.exe --check-config C:\service-watch\config.toml

# 2) 安装为服务(脚本内置:已存在检查、路径校验、配置校验、失败恢复动作)
Set-ExecutionPolicy -Scope Process Bypass -Force
.\docs\deploy\windows\install-service.ps1
# 可选参数:-InstallDir / -ConfigPath / -DataDir / -ServiceName
#           -ServiceAccount .\svcwatch -ServiceAccountPassword <pwd>   # 专用账户
#           -NoStart                                                   # 只注册不启动
```

脚本行为摘要：

- `New-Service` 通过 `BinaryPathName` 注册 SCM 入口，专用账户使用 `PSCredential`，配置路径与日志路径带引号。
- `sc.exe failure` 失败恢复 (见 §3);
- 专用账户时用 `icacls` 授予 DataDir 完全控制 (需另具备"作为服务登录"权利);
- 防火墙**默认不建规则**——控制平面只听 `127.0.0.1:9170`,回环流量不经过防火墙
  入站过滤;将来 listen 改非回环时的参考命令见脚本注释 (注意 v0.1 控制平面无鉴权);
- 启动后轮询 `GET /ready` 至多 30s 并汇报。

## 3. 失败恢复动作 (sc failure)

安装脚本已执行等价命令：

```powershell
sc.exe failure service-watch reset= 86400 actions= restart/5000/restart/5000/restart/5000
sc.exe qfailure service-watch          # 核对
```

- `actions= restart/5000/...`:第 1、2 及后续次失败都在 5 秒后重启 (三段槽位);
- `reset= 86400`:24 小时无失败则失败计数清零 (否则"第 N 次"语义会跨天累积);
- 触发范围：**异常退出/崩溃**才计入失败;`sc stop` 这类受控停止不计入
  (F15 形态下停机以 `Stopped` + `NO_ERROR` 报告，成功受控停止保持成功码)。
- 语义对照 (DESIGN §8.6):v0.1 停止基线 = `TerminateJobObject`/`TerminateProcess`
  + 审计日志;`console = true` 的目标才可能收到 CTRL_BREAK(不保证生效，超时照常
  升级)。对 agent 本体 —— 见 §5 两形态对比。

## 4. 卸载

```powershell
Stop-Service service-watch -Force      # SCM 控制处理入口执行停机
sc.exe delete service-watch           # 标记删除,句柄释放后符号链接消失
Remove-Item -Recurse -Force C:\service-watch    # 含 journal.jsonl 与 data\agent.log,确认后再删
# 被监管子进程保留(观察者外置):Get-Process 查看,由人工或外部机制接管
```

## 5. 服务入口与停机语义

原生 SCM 服务部署使用 `--features windows-service` 构建，并通过 `--service-run` 进入 dispatcher 与 control handler。标准构建支持控制台运行，传入 `--service-run` 时退出 1。SCM 服务程序需要 dispatcher、ServiceMain 和控制处理入口。[Microsoft 服务程序任务](https://learn.microsoft.com/en-us/windows/win32/services/service-program-tasks)

`sc stop` 或 `Stop-Service` 发送 `SERVICE_CONTROL_STOP`，handler 调用 `runtime::request_shutdown()`，随后停止采集、终止调度、排空告警并等待 journal。停机等待受窗口限制，超时和持久化错误写日志。正常停止报告 `Stopped` 与成功码，运行故障报告错误码。被监管子进程使用不带 `KILL_ON_JOB_CLOSE` 的 Job Object，agent 退出时保留运行。

安装脚本配置 `failure actions` 和 `failureflag 1`，覆盖崩溃以及报告 `Stopped` 时携带非零错误码的退出。成功的受控停止保持成功码。修改该 flag 的生效时点按系统文档确认。[Microsoft 失败恢复标志](https://learn.microsoft.com/en-us/windows/win32/api/winsvc/ns-winsvc-service_failure_actions_flag)

默认采集与 Windows SCM 模式的 CI 编译线已加入工作流 (cross-check 交叉编译 +
windows runner 单测)。SCM 安装、启动、停止、优雅停机、热重载、错误退出自动
恢复与 Job Object 孤儿存活的自动化验收见 §5.1;交互式 console 事件演练与
真实目标服务场景仍需 Windows 真机人工验收。本地 macOS 的交叉 `cargo check`
只提供编译证据。

### 5.1 CI SCM 演练 (F36,`.github/workflows/windows-scm.yml`)

windows-latest runner 用**同一份 install-service.ps1**(人工用法零改动，经
`-InstallDir / -ConfigPath / -DataDir / -ServiceName` 参数化安装) 在每次
push/PR 上自动演练 SCM 生命周期，断言链：真实脚本安装 (New-Service +
failure actions + Start-Service + /ready 轮询)→ `sc query` RUNNING
(binPath 含 `--service-run`、`sc qfailure` restart/5000)→ `GET /health`
200 → 服务模式文件日志 (启动标记) 与 journal.jsonl(spawn 事务行) 落盘 →
`sc stop` → 轮询 STOPPED(≤60s,StopPending hint 30s 的 2 倍)→ `/health`
拒绝连接 (进程确已退出)→ agent.log 停机标记 (`in-process shutdown
requested` / `shutdown complete` / `reason=Clean`)→ **Job Object 孤儿断言**
(监督子进程 ping -t 在 agent 退出后必须存活 —— 观察者外置契约)→ 服务已存在
时重装 fail-fast(脚本 §3 的"已存在"守卫，断言匹配 ASCII 片段 `(state=`
—— CI runner OEM 代码页 CJK 有损编码免疫)→ `sc delete` → 按"先 delete 再
install"的文档化重装路径成功 (含 9170 端口重绑)→ **热重载断言**(POST
/v1/reload 增量拓扑 → `/status` 反映新 check)→ **错误退出恢复**(强杀进程 →
SCM failure action 自动拉起 → /health 恢复)→ 卸载清理 (失败路径也执行，并
dump 服务状态/qc/SCM System 事件日志/日志尾部/journal 尾部到 CI 输出)。

**不覆盖**(仍属人工验收):交互式 console 事件演练、真实目标服务场景。
(原「failure actions 实际触发」已由错误退出恢复断言覆盖;Job Object 的
存活语义已由孤儿断言覆盖，交互式 console 事件注入除外。)journal 断言口径与实现语义对齐：监督目标
spawn 事务 (SpawnIntent/SpawnOutcome) 是确定性落盘行;优雅停机不写专门
journal 行 (drain 窗口超时才写 audit),停机证据 = agent.log 停机标记 +
SCM STOPPED。

## 6. 日志与事件查看

- **服务模式 = 文件日志**(F15):服务无 console,tracing 经 `--log-file` 追加写
  文件 (安装脚本固定为 `<DataDir>\agent.log`;EnvFilter 同 console 形态，
  `RUST_LOG` 可调)。事件类信息 (incident/restart/fatal/reset/audit) 以
  `C:\service-watch\data\journal.jsonl` 为准;运行指标用 `monitorctl metrics`。
  Windows 事件日志/ETW 写入未实现 (F15 已知留白);服务生命周期由 SCM 留痕兜底
  (见下)。
- **Windows 事件日志**(SCM 自身对服务生命周期留痕):

```powershell
# 服务崩溃/异常退出(含退出码)—— 事件 ID 7031/7034/7036
Get-WinEvent -FilterHashtable @{LogName='System'; ProviderName='Service Control Manager'} -MaxEvents 20 |
  Format-Table TimeCreated, Id, Message -AutoSize

# 服务失败恢复动作执行记录同样出现在 System 日志,按 Message 搜服务名:
Get-WinEvent -FilterHashtable @{LogName='System'} -MaxEvents 200 |
  Where-Object { $_.Message -match 'service-watch' } |
  Select-Object TimeCreated, Id, Message
```

- 应用层诊断：`monitorctl status` / `monitorctl metrics --prom`;崩溃转储按系统
  默认 WER 设置 (生产上可配 LocalDumps 收集 monitor-agent.exe 的 dmp)。

## 7. 常见故障表

| 现象 | 诊断 | 处置 |
|---|---|---|
| `sc create` 报 1073(服务已存在) | 重复安装 | `sc.exe delete` 后重装，或换 `-ServiceName` |
| 服务启动后立即停止 (退出码 1),事件日志/日志文件见 "service mode requires Windows + windows-service feature" | binPath 带 `--service-run` 但二进制是标准构建 (无 F15 feature) | 按 §1 用 `--features windows-service` 重编，替换 exe 后 `sc start` |
| 服务启动后立即停止 (退出码 1),日志文件见 config error | 配置错误 | 事件日志 7031/7034 可见退出码;`--check-config` 修配置 |
| 反复重启 (约 5s 间隔) | failure 动作生效中，持续启动失败 | 见上;`sc qfailure` 看计数 (24h 无失败自动清零);修复根因为主，重跑 `sc.exe failure ... reset= 86400 actions= ...` 仅重置策略不计数 |
| bind 失败 (9170 被占) | 旧实例或其他进程占用 | `netstat -ano | findstr :9170` → `tasklist /fi "PID eq <pid>"`;释放或改 `[control].listen` |
| `monitorctl` unreachable(退出码 4) | `[control].enabled=false` / 地址不一致 / 服务没起 | `Invoke-RestMethod http://127.0.0.1:9170/health` 定位 |
| `monitorctl metrics` 连接被拒但服务 running | 用了降级构建 (无 control-http feature) | 换默认 feature 构建 (需 NASM+CMake，见 §1) |
| journal 没有增长且日志 (若有) 有写入失败 | DataDir 权限/磁盘满 | 专用账户跑 `icacls` 授权;检查磁盘;查看 journal 错误指标和退出原因，恢复权限或磁盘空间后重启 |
| 升级后 Fatal target 不拉起 | 终态闩锁跨重启保留 (DESIGN §8.7) | 设计行为;修根因后 `monitorctl reset <target>` |
| NASM 报 `unrecognized` / CMake 缺失 | aws-lc-rs 构建依赖缺失 | 见 §1;确认 PATH 后重跑构建 |
| SCM 目标一直 CrashLoop，日志见 spawn 失败 | 服务名不存在 (1060)/账户无启停权限/构建未启用 `scm-target` | `sc query <name>` 核对服务名;检查账户权限;按 §8 确认构建 feature |

## 8. SCM 目标监控 (原生 Windows Service 目标)

除"spawn 一个本地可执行文件"外，agent 还可以把**已安装的 Windows 服务**作为
supervise 目标：`service.windows_service` 填 SCM 服务名 (`sc create <name>` 的
名字，非显示名;`sc query` 可查)。启停完全交给 SCM,agent 不再 spawn 本地
进程 —— 这是 ROADMAP「原生 Windows Service 目标恢复」的目标类型，与 F15
(agent 自身的服务形态) 互为独立：前者监控第三方服务，后者是 agent 自己的
SCM 入口。

### 8.1 构建

`scm-target` 是 monitor-core 的 feature，默认关闭 (默认构建零影响),仅在
`cfg(windows)` 下拉入 `windows-service` crate。构建同时支持 F15 服务形态与
SCM 目标的 agent:

```powershell
cargo build --release -p monitor-agent --features windows-service,monitor-core/scm-target
```

(仅监控 SCM 目标、不需要 agent 自身服务形态时，`--features monitor-core/scm-target` 即可。)

### 8.2 配置

```toml
[[service]]
name = "w3svc"                # monitor 内部目标名 (与其它 service/check 空间独立)
windows_service = "W3SVC"     # SCM 服务名 (sc create 的名字，非显示名)

[service.restart]             # 恢复策略照常适用
stop_grace = "30s"
max_restarts = 5
```

校验规则 (违反即配置错误，退出码 1):

- `windows_service` 与 `command` **二选一**;
- SCM 管理启停，`args` / `workdir` / `env` / `console` / `activate` 一律
  不支持 (配置即报错);
- `depends_on` / `restart` 照常适用 (启动编排与恢复策略语义不变;
  `hung_action = "activate"` 因 activate 互斥而不可用)。

### 8.3 启停与存活语义

| 操作 | 语义 |
|---|---|
| 启动 (spawn) | `Start("")`;服务已 Running / 启动已在途视为成功 (幂等，审计记 outcome) |
| 存活 (liveness) | 每轮 `QueryStatus == SERVICE_RUNNING` 即证据，替代 sysinfo 快照证据 |
| 停止 (signal_stop) | 发 `Stop` 控制码即返回 (不阻塞调度循环);停止确认由 QueryStatus 轮询驱动，`stop_grace` 超时升级 |
| 强杀 (force_kill) | **没有更强手段**:再次 `Stop`(如实降级，审计注明) |
| 退出判定 | `SERVICE_STOPPED` 视为一次退出 (无退出码);START_PENDING / STOP_PENDING 视为过渡态，不误报 |

死亡检测、backoff、重启预算、Fatal 闩锁与普通目标完全一致 (健康的 bind
check 照常联动)。区别:SCM 目标没有 `(pid, start_time)` 进程身份 ——
journal 不落进程身份、快照身份采纳/孤儿对账不适用;控制面 restart /
monitorctl restart 的"优雅停 → 拉起"语义不变。

### 8.4 限制

- 权限:agent 账户需对目标服务有 `SERVICE_START` / `SERVICE_STOP` 权限
  (默认 LocalSystem 足够;§2 的专用账户需额外授权或用服务 ACL `sc sdset`);
- 构建门槛：`scm-target` 关闭或非 Windows 构建时，配置可加载，但 spawn
  诚实降级为错误 —— 目标按"启动失败"进 CrashLoop → 预算 → Fatal 告警，
  不假装成功 (liveness/告警路径照常);
- 开销：每次 QueryStatus 是一次本地 SCM RPC(毫秒级);is_running 每轮多次
  调用，服务数量极大时留意 `[collect].interval`;
- 可观测性:SCM 目标无退出码/stdout 可读，诊断结合 Windows 事件日志 (§6)
  与服务自身日志;
- 停机边界:agent 优雅停机只停**本会话启动过**的 SCM 服务 (500ms 轮询，
  上限 stop_grace);外部预先在运行的服务不由 agent 停机 (观察者外置原则)。
