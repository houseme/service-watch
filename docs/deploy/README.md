# service-watch 部署总览

> 首次部署可按[部署与使用指南](../DEPLOYMENT.md)从构建、配置到安装逐步执行。
> 本文与当前工作区实现同步。语义以 [DESIGN.md](../../DESIGN.md) 为准，进度边界见
> [ROADMAP.md](../../ROADMAP.md);日常操作与故障处置见 [docs/RUNBOOK.md](../RUNBOOK.md)。
>
> **核心原则 (DESIGN §1 原则 1，观察者外置)**:agent 由外部 service manager
> (systemd / launchd / SCM) 守护，与被监控服务不同生共死。agent 停机时被监管
> 子进程默认保留 (DESIGN §8.7),因此三平台的守护配置都刻意"只管 agent 本体"。

## 1. 三种平台形态对比

| | Linux | macOS | Windows |
|---|---|---|---|
| 守护机制 | systemd(system unit) | launchd(LaunchDaemon) | Service Control Manager(`sc.exe`) |
| unit 文件 | `docs/deploy/systemd/service-watch.service` → `/etc/systemd/system/` | `docs/deploy/launchd/com.houseme.service-watch.plist` → `/Library/LaunchDaemons/` | `docs/deploy/windows/install-service.ps1`(写注册表 ImagePath) |
| 运行身份 | 专用用户 `svcwatch` | root(LaunchDaemon 默认;可 `UserName` 降权) | `LocalSystem`(可 `-ServiceAccount` 换专用账户) |
| 重启策略 | `Restart=on-failure` + `RestartSec=5s` + StartLimit(防疯狂重启) | `KeepAlive={SuccessfulExit=false, Crashed=true}` + `ThrottleInterval=5` | `sc failure actions= restart/5000`(失败恢复动作) |
| 停止语义 | `systemctl stop` 发 SIGTERM → agent 优雅停机，退出码 **130**(DESIGN §9.3;unit 以 `SuccessExitStatus=130` 记为干净退出);`KillMode=process` 保证被监管子进程保留 | `launchctl bootout` 发 SIGTERM → 同上 (job 已卸载，不会被 KeepAlive 拉起);`AbandonProcessGroup=true` 同理 | 启用 `windows-service`，`sc stop` 经 SCM handler 调用停机入口，保留被监管子进程 |
| 日志去向 | journald(`journalctl -u service-watch`) | `/var/log/service-watch/*.log`(StandardOutPath/StandardErrorPath) | 安装脚本通过 `--log-file` 写入 `<DataDir>\agent.log`，事件另写 journal |
| 持久化目录 | `/var/lib/service-watch` | `/var/lib/service-watch` | `C:\service-watch\data` |
| 配置目录 | `/etc/service-watch/config.toml` | `/etc/service-watch/config.toml` | `C:\service-watch\config.toml` |
| 构建要求 | rustup stable(MSRV 约 1.95+) | 同左 (`aarch64`/`x86_64`-apple-darwin) | 同左 + **NASM 与 CMake**(见 §2) |
| 特别注意 | SELinux enforcing 时检查 `svcwatch` 对二进制/目录的 label | 合盖休眠会整体暂停 agent(见 launchd/README §App Nap) | 路径含空格的引号转义;`console = true` 才有 CTRL_BREAK 尝试 |

三平台共同点：

- 控制平面默认仅监听 **`127.0.0.1:9170`**(config `[control].listen`),`monitorctl` 走 HTTP;
  不建议改为非回环地址 (远程暴露时应叠加 [control] token (F23) 与 TLS/mTLS (F32/F38)，见 RUNBOOK §1.2d/§1.2e"改非回环需自行承担风险")。
- journal 位于 `<data_dir>/journal.jsonl`,append-only,**目录对运行账户必须可写**。
- journal 和 spool 写入 `data_dir`，服务模式日志还写入 `--log-file` 指定位置，运行账户需要相应目录权限。

## 2. 前置条件

### 2.1 构建 (全平台)

```bash
# rustup stable 工具链(sysinfo 0.40 声明 MSRV 1.95,以较新 stable 为准)
rustup update stable

# 工作区构建:agent + ctl 两个二进制
cargo build --release -p monitor-agent -p monitorctl
```

产物在 `target/release/`(Windows 为 `target/release/*.exe`):`monitor-agent`、`monitorctl`。

### 2.2 Windows 构建附加要求

TLS 栈为 rustls + **aws-lc-rs**(reqwest 0.13 起 ring 无备选),凡启用 http 相关
feature(默认组合含 `probe-http` / `notify-webhook` / `control-http`) 的构建需要
**NASM 与 CMake**:

```powershell
choco install nasm cmake
# 确认 nasm.exe 在 PATH 中(Chocolatey 通常已注入),CMake 同理
cmake --version; nasm -v
```

无 http 的降级组合**不需要** NASM/CMake:

```powershell
cargo build --release -p monitor-agent -p monitorctl `
  --no-default-features --features collect-sysinfo
```

代价：失去 HTTP 探测、Webhook 告警与控制平面 (`monitorctl` 将无法连接 agent),
仅剩硬件/进程采集与 TCP/process 检查。生产部署**不建议**降级。

### 2.3 二进制与目录布局 (Linux/macOS 示例)

```bash
sudo install -m 0755 target/release/monitor-agent  /usr/local/bin/
sudo install -m 0755 target/release/monitorctl     /usr/local/bin/

sudo mkdir -p /etc/service-watch /var/lib/service-watch
sudo install -m 0640 config/config.example.toml /etc/service-watch/config.toml  # 起点模板,按需修改
```

`/etc/service-watch/config.toml` 中的 `data_dir` 必须指向运行账户可写的目录，
推荐 `/var/lib/service-watch`(见各平台 README 的属主设置)。

### 2.4 安装前配置检查 (三平台通用，强烈建议)

```bash
monitor-agent --check-config /etc/service-watch/config.toml   # 0 = 合法;1 = 配置错误(退出码见 DESIGN §9.3)
monitor-agent --dump-snapshot                                  # 单次采集 JSON,验证采集层在该机器可用
```

任何一次 `systemctl start` / `launchctl kickstart` / `sc start` 之前先过这一步，
可避免"配置错误 → 守护循环重启 → 触发节流"的噪音。

## 3. 升级流程 (滚动替换二进制)

原则：**先备份 journal，再换二进制;升级不触碰 journal 语义**(journal 由启动时
重放恢复 Fatal 闩锁与重启预算，DESIGN §8.7;新旧版本 journal schema 一致时才能
跨版本保留闩锁，跨大版本升级见版本发布说明)。

```bash
# 0. 记录当前状态,便于回滚比对
monitorctl status --json > /tmp/service-watch-status-before.json

# 1. 停止并备份持久化数据(journal 是唯一状态载体)
sudo systemctl stop service-watch            # launchd: sudo launchctl bootout system/com.houseme.service-watch
sudo cp -a /var/lib/service-watch/journal.jsonl \
           /var/lib/service-watch/journal.jsonl.bak-$(date +%Y%m%d%H%M%S)

# 2. 替换二进制(先落新文件再原子换名,避免半写状态)
sudo install -m 0755 target/release/monitor-agent  /usr/local/bin/monitor-agent.new
sudo install -m 0755 target/release/monitorctl     /usr/local/bin/monitorctl.new
sudo mv /usr/local/bin/monitor-agent.new  /usr/local/bin/monitor-agent
sudo mv /usr/local/bin/monitorctl.new     /usr/local/bin/monitorctl
/usr/local/bin/monitor-agent --version

# 3. 配置若同步更新,先离线校验
monitor-agent --check-config /etc/service-watch/config.toml

# 4. 启动
sudo systemctl start service-watch           # launchd: sudo launchctl bootstrap system <plist>

# 5. 验证:ready 必须翻绿(配置加载成功 + 调度循环已启动;降级采集不影响 ready)
curl -fsS http://127.0.0.1:9170/ready        # 期望 {"ready":true},HTTP 200
monitorctl status                             # 比对 targets 与升级前一致
journalctl -u service-watch -n 50 --no-pager  # launchd: tail /var/log/service-watch/agent.log
```

回滚 = 同一套流程反向执行 (旧二进制 + `journal.jsonl.bak-*`)。注意：**Fatal
闩锁在停机期间不衰减**(终态闩锁，窗口滑过也不自动恢复，DESIGN §8.2),若升级前
存在 Fatal target，重启后它仍保持 Fatal 并跳过冷启动 spawn，需人工
`monitorctl reset <target>`。

### 3.1 配置变更如何生效

先对实际配置运行 `monitor-agent --check-config`。F14 支持 Unix SIGHUP 和 `POST /v1/reload`，缺省使用启动配置路径。check、service、watch 拓扑增删改通过 reload 应用，服务重定义等待旧实例收割。202 表示入队，应用结果通过 journal 与 status 核对。

agent、log、control、collect、alert、metrics 段变更要求重启。重启时被监管进程保留，恢复后进行身份对账。完整操作和边界见 RUNBOOK §1.4 与 §7。

## 4. 卸载流程

Linux(systemd):

```bash
sudo systemctl disable --now service-watch
sudo rm /etc/systemd/system/service-watch.service && sudo systemctl daemon-reload
# 被监管子进程保留(KillMode=process),确认处置后手动结束:
ps -eo pid,ppid,pgid,cmd | grep -v grep | grep <被监管进程名>
sudo rm -rf /etc/service-watch            # 配置(先自行备份)
sudo rm -rf /var/lib/service-watch        # 数据,含 journal.jsonl —— 确认不再需要再删
sudo userdel svcwatch                      # 可选:删除运行账户
```

macOS(launchd):

```bash
sudo launchctl bootout system/com.houseme.service-watch
sudo rm /Library/LaunchDaemons/com.houseme.service-watch.plist
sudo rm -rf /etc/service-watch /var/lib/service-watch /var/log/service-watch
```

Windows(SCM):

```powershell
Stop-Service service-watch -Force          # SCM handler 执行停机，子进程保留
sc.exe delete service-watch
Remove-Item -Recurse -Force C:\service-watch
```

卸载后遗留的被监管进程是**设计行为**(观察者外置):它们已脱离 agent 管控，
由外部 service manager 或人工接管。

## 5. 常见问题 (FAQ)

### 5.1 端口 9170 被占用

现象:agent 启动即报 bind 失败 (运行错误，退出码 2;systemd 下单元进入 failed)。

```bash
# Linux / macOS
lsof -nP -iTCP:9170 -sTCP:LISTEN
# Windows
netstat -ano | findstr :9170
```

处置：释放占用者，或改 `[control].listen`(仍建议绑定回环地址);改后必须
`monitor-agent --check-config` 通过再启动。`monitorctl` 需以 `--url` 指向新地址。

### 5.2 data_dir 权限不足 (journal 写入失败)

启动前校验 journal 可读可写。运行中关键状态确认超时或写入失败时，实例停止自动恢复并退出 2。普通审计拥塞会增加 journal_dropped，写错误和关键确认失败分别增加 journal_write_failures、journal_critical_failures。保留 journal 原件，修复权限、空间或存储问题后再启动。

```bash
sudo chown svcwatch:svcwatch /var/lib/service-watch
sudo chmod 750 /var/lib/service-watch
sudo -u svcwatch test -w /var/lib/service-watch && echo writable
```

### 5.3 journal.jsonl 损坏 (半行/坏行) 后的重放行为

- 重放**逐行顺序应用，坏行直接跳过**(journal.rs 重放语义;单测覆盖),不会导致
  agent 拒绝启动;
- 按行顺序意味着：同 target 的 `Reset` 会清除其之前的 Fatal 与重启计数素材，其后
  新的 `Fatal`/`Restart` 照常生效——**不要**手工删行来"洗掉"Fatal，顺序被破坏后
  重放结果不可预期;正确做法是 `monitorctl reset`(写审计) 或整文件备份后回滚;
- 手工修复步骤：停 agent → `cp journal.jsonl journal.jsonl.bak-*` → 用
  `jq -c . journal.jsonl` 找出报错行号 → 将坏行 (通常为断电半行，位于文件末尾)
  剔除为 `journal.jsonl.repaired` → 替换原文件 → 启动。

### 5.4 配置错误引发的"重启节流/失败状态"

配置错误 → 退出码 1 → 守护者按失败重启 → 反复失败触发节流：

- systemd:`StartLimitBurst` 用尽后单元进入 `failed`(停止重启),`journalctl -u`
  可见配置错误详情;修配置后 `systemctl reset-failed service-watch` 再 start;
- launchd:按 `ThrottleInterval=5` 持续低速重试;`launchctl print system/…` 观察
  last exit status;修配置后 `kickstart`;
- Windows:失败恢复动作按 5s 间隔重启;`sc qfailure service-watch` 查看计数。

根治：每次改配置先 `monitor-agent --check-config`。

### 5.5 monitorctl 连不上 agent(退出码 4)

依次检查:agent 是否在运行 (`systemctl status` / `launchctl print` / `sc query`)
→ `[control].enabled = true`?→ 监听地址与 `--url` 是否一致 → 防火墙 (仅当你把
listen 改成了非回环，见各平台 README)。

### 5.6 Fatal 目标 `restart` 被拒

`monitorctl restart <target>` 对 Fatal 目标无效 (DESIGN §9.2:先 reset)。
v0.1 实现细节 (2026-11-11 接线):control 平面经 status watch 判定，Fatal 目标
**直接返回 409** + 提示先 reset;非 Fatal 一律 202 入队 (eval 侧保留防御性拒绝 +
journal 审计)。monitorctl 收到 409 时提示 `monitorctl reset <target>` 并以退出
码 2 结束。处置流程见 RUNBOOK §1.3。

### 5.7 agent 停机后，被监管的服务还在跑 (不是 bug)

agent 停机时保留被监管子进程。重启后精确核对持久化身份，确认存活的实例以只读 attach 接管。身份或采集证据不足时继续等待，确认旧身份消失后按预算与退避恢复 supervise。旧 journal 无身份记录或 spawn 到身份确认之间被强杀时，仍需检查遗留实例。

三平台守护配置使用 `KillMode=process`、`AbandonProcessGroup=true` 或不带 `KILL_ON_JOB_CLOSE` 的 Job Object，维持进程生命周期独立。attach 期间的操作与故障处置见 RUNBOOK §7。

## 6. 文件清单

```text
docs/deploy/
├── README.md                          # 本文：总览/前置/升级/卸载/FAQ
├── systemd/
│   ├── service-watch.service          # Linux 生产 unit(注释逐项说明)
│   └── README.md                      # systemd 安装/验证/日志/故障表
├── launchd/
│   ├── com.houseme.service-watch.plist# macOS LaunchDaemon
│   └── README.md                      # launchd 安装/验证/卸载/休眠注意
└── windows/
    ├── install-service.ps1            # Windows 服务安装脚本 (参数化)
    └── README.md                      # 构建约束/安装卸载/sc failure/事件日志
```
