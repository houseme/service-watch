# service-watch macOS 部署文档 (从上到下逐步执行)

按顺序执行即可完成部署。第 1、2 步在**构建机**上执行，其余在**目标机**上执行;构建机与目标机是同一台时忽略该区分。macOS 以 LaunchDaemon(root) 形态常驻。

---

## 第 1 步：准备构建环境

需要 Xcode 命令行工具、Rust stable、CMake。

```bash
xcode-select --install           # 没装 Xcode CLT 时
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh    # 没装 Rust

brew install cmake               # aws-lc-rs(TLS)构建需要
cmake --version                  # 有版本号才能继续
```

## 第 2 步：构建

在项目根目录：

```bash
cargo build --release -p monitor-agent -p monitorctl
```

产物：`target/release/monitor-agent` 和 `target/release/monitorctl`。
构建机不是目标机时，把这两个二进制连同 `docs/deploy/launchd/com.houseme.service-watch.plist` 一起拷到目标机。

## 第 3 步：安装文件 (目标机)

```bash
sudo install -m 0755 target/release/monitor-agent /usr/local/bin/monitor-agent
sudo install -m 0755 target/release/monitorctl /usr/local/bin/monitorctl
sudo mkdir -p /etc/service-watch /var/lib/service-watch /var/log/service-watch
```

## 第 4 步：写配置

把下面内容保存为 `/etc/service-watch/config.toml`,只改三处：应用二进制路径、workdir、健康检查 URL。

```toml
[agent]
data_dir = "/var/lib/service-watch"     # journal 目录

[control]
enabled = true
listen = "127.0.0.1:9170"               # monitorctl 用的端口，只听本机

[collect]
interval = "5s"

[log]
json = true                             # LaunchDaemon 用 --log-json，保持一致

[alert]
webhook_url = ""                        # 填上 http/https 地址即启用告警通知
webhook_format = "json"
failure_threshold = 3                   # 连续失败 3 次 → 告警
success_threshold = 2                   # 连续成功 2 次 → 恢复
renotify_interval = "15m"

# ---- agent 负责拉起并看护的应用 (不需要就删掉这两段) ----
[[service]]
name = "my-app"
command = "/opt/my-app/bin/my-app"      # 改成实际路径
args = []
workdir = "/opt/my-app"

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

```bash
sudo install -m 0640 -o root -g wheel /path/to/your-config.toml /etc/service-watch/config.toml
sudo /usr/local/bin/monitor-agent --check-config /etc/service-watch/config.toml
```

输出 `config ok` 才继续;报错按提示修配置后重跑。

## 第 6 步：安装 LaunchDaemon

```bash
plutil -lint docs/deploy/launchd/com.houseme.service-watch.plist    # XML 校验
sudo install -m 0644 -o root -g wheel \
  docs/deploy/launchd/com.houseme.service-watch.plist \
  /Library/LaunchDaemons/com.houseme.service-watch.plist
sudo launchctl bootstrap system /Library/LaunchDaemons/com.houseme.service-watch.plist
```

plist 要求 root:wheel 0644，否则 launchd 拒载。KeepAlive 语义：仅异常退出 (非零退出码) 或被信号杀死时自动重启 (5 秒节流),开机自启。

## 第 7 步：验证

逐条执行，全部正常即部署完成：

```bash
sudo launchctl print system/com.houseme.service-watch | head -5   # state 应为 running
curl -fsS http://127.0.0.1:9170/ready                             # 应返回就绪 JSON
monitorctl status                                                 # 目标状态表,目标应为 Running/Healthy
tail -20 /var/log/service-watch/agent.log                         # 无持续报错
```

---

## 日常操作

| 操作 | 命令 |
|---|---|
| 看状态 | `monitorctl status` |
| 持续观察 | `monitorctl watch --interval 2` |
| 立即探测一次 | `monitorctl check my-app` |
| 手动重启目标 | `monitorctl restart my-app` |
| 清除 Fatal(修完根因后) | `monitorctl reset my-app` |
| 看指标 | `monitorctl metrics` |
| 改配置后热重载 | `curl -fsS -X POST http://127.0.0.1:9170/v1/reload` |
| 停 agent | `sudo launchctl bootout system/com.houseme.service-watch` |
| 启 agent | `sudo launchctl bootstrap system /Library/LaunchDaemons/com.houseme.service-watch.plist` |
| 看 launchd 详情 | `sudo launchctl print system/com.houseme.service-watch` |

日志位置：

| 内容 | 位置 |
|---|---|
| 运行日志 | `/var/log/service-watch/agent.log`(错误在 `agent.err.log`) |
| 事件流水 (告警/重启/Fatal/审计) | `/var/lib/service-watch/journal.jsonl` |

## 升级

```bash
sudo launchctl bootout system/com.houseme.service-watch
sudo install -m 0755 新monitor-agent /usr/local/bin/monitor-agent
sudo /usr/local/bin/monitor-agent --check-config /etc/service-watch/config.toml
sudo launchctl bootstrap system /Library/LaunchDaemons/com.houseme.service-watch.plist
curl -fsS http://127.0.0.1:9170/ready
```

## 卸载

```bash
sudo launchctl bootout system/com.houseme.service-watch
sudo rm /Library/LaunchDaemons/com.houseme.service-watch.plist
# /etc/service-watch、/var/lib/service-watch(含 journal)、/var/log/service-watch 按保留期限归档后再删
# 被监管子进程保留运行:ps 查看,由人工或外部机制接管
```

## 常见问题速查

| 现象 | 处理 |
|---|---|
| `bootstrap` 报已加载/异常 | 先 `bootout` 再 `bootstrap`;确认 plist 属主 root:wheel 权限 0644 |
| agent 起不来，`agent.err.log` 见 config error | 按报错修 config.toml;重跑 `--check-config` |
| 每 5 秒重启一次 | KeepAlive 在拉异常退出;`agent.err.log` 找 config error，修根因 |
| 9170 被占 | `lsof -i :9170` 找占用进程释放;或改配置 `[control].listen` 后重启 agent(`bootout`+`bootstrap` —— control 段不走热重载),并同步 `monitorctl --url` |
| `monitorctl` 报 unreachable | agent 没起或端口不一致：`curl -fsS http://127.0.0.1:9170/health` 定位 |
| 控制面要走 HTTPS | `[control]` 配 `tls_cert`/`tls_key`(PEM，成对) 后 `bootout`+`bootstrap` 重启 agent,monitorctl 用 `--url https://...`;自签需 CA 入信任链，临时测试可 `--insecure`(RUNBOOK §1.2d) |
| 合盖/休眠后没有采集 | macOS 休眠会整体暂停 agent，开机恢复;常驻监控请防休眠或用服务器形态 |
| 温度/风扇读数为空 | 查看 `hardware.degradations` 原因;机型不支持即正常降级 |

更完整的排障与语义说明：`docs/deploy/launchd/README.md`、`docs/RUNBOOK.md`。
