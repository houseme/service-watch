# service-watch Windows 部署文档 (从上到下逐步执行)

按顺序执行即可完成部署。第 1、2 步在**构建机**上执行，其余在**目标机**的管理员 PowerShell 中执行;构建机与目标机是同一台时忽略该区分。

---

## 第 1 步：准备构建环境

需要 Rust stable(MSVC 工具链)、NASM、CMake(默认 TLS 构建依赖)。

```powershell
# 没装 Rust:到 https://rustup.rs 下载 rustup-init.exe 运行,然后:
rustup default stable-x86_64-pc-windows-msvc

# 有 Chocolatey 一行装齐;没有就去 nasm/cmake 官网下载安装并加入 PATH
choco install -y nasm cmake

nasm -v            # 两个都能打印版本才能继续
cmake --version
```

## 第 2 步：构建

在项目根目录：

```powershell
cargo build --release -p monitor-agent --features windows-service
cargo build --release -p monitorctl
```

产物：`target\release\monitor-agent.exe` 和 `target\release\monitorctl.exe`。
构建机不是目标机时，把这两个 exe 连同 `docs\deploy\windows\install-service.ps1` 一起拷到目标机。

## 第 3 步：放置文件 (目标机)

```powershell
New-Item -ItemType Directory -Force -Path C:\service-watch\data | Out-Null
Copy-Item target\release\monitor-agent.exe C:\service-watch\
Copy-Item target\release\monitorctl.exe    C:\service-watch\
Copy-Item docs\deploy\windows\install-service.ps1 C:\service-watch\
```

## 第 4 步：写配置

把下面内容保存为 `C:\service-watch\config.toml`,只改三处：应用 exe 路径、workdir、健康检查 URL。

```toml
[agent]
data_dir = "C:/service-watch/data"      # journal 与日志目录

[control]
enabled = true
listen = "127.0.0.1:9170"               # monitorctl 用的端口，只听本机

[collect]
interval = "5s"

[log]
json = false

[alert]
webhook_url = ""                        # 填上 http/https 地址即启用告警通知
webhook_format = "json"
failure_threshold = 3                   # 连续失败 3 次 → 告警
success_threshold = 2                   # 连续成功 2 次 → 恢复
renotify_interval = "15m"

# ---- agent 负责拉起并看护的应用 (不需要就删掉这两段) ----
[[service]]
name = "my-app"
command = "C:/apps/my-app/my-app.exe"   # 改成实际路径;TOML 里写正斜杠免转义
args = []
workdir = "C:/apps/my-app"

[service.restart]
max_restarts = 5                        # 10 分钟窗口内最多自动重启 5 次，耗尽进 Fatal

# ---- 健康检查 (与 service 同名：持续不健康会触发重启) ----
[[check]]
name = "my-app"
kind = "http"
url = "http://127.0.0.1:8080/healthz"   # 改成实际健康端点
expected_status = "200-299"
interval = "10s"
timeout = "5s"
```

只监控、不让 agent 拉起应用：删掉 `[[service]]` 和 `[service.restart]` 两段，保留 `[[check]]`。

## 第 5 步：校验配置

```powershell
C:\service-watch\monitor-agent.exe --check-config C:\service-watch\config.toml
```

输出 `config ok` 才继续;报错按提示修配置后重跑。

## 第 6 步：安装为 Windows 服务

```powershell
Set-ExecutionPolicy -Scope Process Bypass -Force
C:\service-watch\install-service.ps1
```

脚本自动完成：注册 SCM 服务 (名 `service-watch`)、崩溃后 5 秒自动重启、启动服务并等待就绪 (最多 30 秒)。默认以 LocalSystem 运行;专用账户、自定义路径用参数覆盖 (如 `-InstallDir D:\service-watch ...`),全部参数见 `docs\deploy\windows\README.md`。

## 第 7 步：验证

逐条执行，全部正常即部署完成：

```powershell
Get-Service service-watch                              # Status 应为 Running
Invoke-RestMethod http://127.0.0.1:9170/ready          # 应返回就绪 JSON
C:\service-watch\monitorctl.exe status                 # 目标状态表,目标应为 Running/Healthy
Get-Content C:\service-watch\data\agent.log -Tail 20   # 无持续报错
```

---

## 日常操作

| 操作 | 命令 |
|---|---|
| 看状态 | `C:\service-watch\monitorctl.exe status` |
| 持续观察 | `C:\service-watch\monitorctl.exe watch --interval 2` |
| 立即探测一次 | `C:\service-watch\monitorctl.exe check my-app` |
| 手动重启目标 | `C:\service-watch\monitorctl.exe restart my-app` |
| 清除 Fatal(修完根因后) | `C:\service-watch\monitorctl.exe reset my-app` |
| 看指标 | `C:\service-watch\monitorctl.exe metrics` |
| 改配置后热重载 | `Invoke-WebRequest -Method Post -Uri http://127.0.0.1:9170/v1/reload` |
| 停 / 启 agent 服务 | `Stop-Service service-watch` / `Start-Service service-watch` |

日志与事件位置：

| 内容 | 位置 |
|---|---|
| 应用日志 | `C:\service-watch\data\agent.log` |
| 事件流水 (告警/重启/Fatal/审计) | `C:\service-watch\data\journal.jsonl` |
| 服务崩溃留痕 | Windows 事件日志 (下条命令) |

```powershell
Get-WinEvent -FilterHashtable @{LogName='System'; ProviderName='Service Control Manager'} -MaxEvents 20 |
  Format-Table TimeCreated, Id, Message -AutoSize
```

## 升级

```powershell
Stop-Service service-watch
Copy-Item 新monitor-agent.exe C:\service-watch\monitor-agent.exe -Force
C:\service-watch\monitor-agent.exe --check-config C:\service-watch\config.toml   # 新二进制过一遍校验
Start-Service service-watch
Invoke-RestMethod http://127.0.0.1:9170/ready
```

## 卸载

```powershell
Stop-Service service-watch -Force
sc.exe delete service-watch
Remove-Item -Recurse -Force C:\service-watch    # 含 journal 与日志,确认后再删
```

## 常见问题速查

| 现象 | 处理 |
|---|---|
| `--check-config` 报错退出 | 按报错修 config.toml;路径写正斜杠 `C:/...` 或双反斜杠 `C:\\...` |
| 服务启动后立即停止 (退出码 1) | agent.log 见 config error → 修配置;见 "windows-service feature" → 按第 2 步重编 agent |
| 服务反复重启 (约 5 秒一次) | 崩溃自动恢复生效中;查 agent.log 修根因 |
| 9170 端口被占 | `netstat -ano | findstr :9170` 找占用进程释放;或改配置 `[control].listen` 后重载 |
| `monitorctl` 报 unreachable | 服务没起或端口不一致：`Invoke-RestMethod http://127.0.0.1:9170/health` 定位 |
| NASM/CMake 构建报错 | 构建依赖缺失，回第 1 步确认 PATH 后重编 |

更完整的排障与语义说明：`docs/deploy/windows/README.md`、`docs/RUNBOOK.md`。
