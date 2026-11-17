# service-watch Linux 部署文档 (从上到下逐步执行)

按顺序执行即可完成部署。第 1、2 步在**构建机**上执行，其余在**目标机**上执行;构建机与目标机是同一台时忽略该区分。

---

## 第 1 步：准备构建环境

需要 Rust stable 与编译工具链 (gcc、make、cmake、perl)。

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh    # 没装 Rust
source "$HOME/.cargo/env"

# Debian/Ubuntu:
sudo apt install -y build-essential cmake perl
# RHEL/CentOS:
sudo dnf install -y gcc make cmake perl

cmake --version    # 有版本号才能继续
```

## 第 2 步：构建

在项目根目录：

```bash
cargo build --release -p monitor-agent -p monitorctl
```

产物：`target/release/monitor-agent` 和 `target/release/monitorctl`。
构建机不是目标机时，把这两个二进制连同 `docs/deploy/systemd/service-watch.service` 一起拷到目标机。

## 第 3 步：创建账户与安装文件 (目标机)

```bash
# 专用运行账户(已存在则复用;RHEL 系 nologin 路径为 /sbin/nologin)
getent passwd svcwatch || sudo useradd --system --user-group \
  --home-dir /var/lib/service-watch --shell /usr/sbin/nologin svcwatch

# 二进制
sudo install -m 0755 target/release/monitor-agent /usr/local/bin/monitor-agent
sudo install -m 0755 target/release/monitorctl /usr/local/bin/monitorctl

# 目录与权限
sudo install -d -m 0750 -o root -g svcwatch /etc/service-watch
sudo install -d -m 0750 -o svcwatch -g svcwatch /var/lib/service-watch
```

## 第 4 步：写配置

把下面内容保存为 `/etc/service-watch/config.toml`,只改三处：应用二进制路径、workdir、健康检查 URL。

```toml
[agent]
data_dir = "/var/lib/service-watch"     # journal 目录，svcwatch 必须可写

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
command = "/srv/my-app/bin/my-app"      # 改成实际路径
args = []
workdir = "/srv/my-app"

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
sudo install -m 0640 -o root -g svcwatch /path/to/your-config.toml /etc/service-watch/config.toml
sudo -u svcwatch /usr/local/bin/monitor-agent --check-config /etc/service-watch/config.toml
```

输出 `config ok` 才继续;报错按提示修配置后重跑。(必须以 svcwatch 身份校验——服务运行时读配置的就是这个账户。)

## 第 6 步：安装 systemd 服务

```bash
sudo install -m 0644 docs/deploy/systemd/service-watch.service /etc/systemd/system/service-watch.service
sudo systemctl daemon-reload
sudo systemctl enable --now service-watch
```

unit 自带启动前配置校验 (`ExecStartPre`)、崩溃自动重启 (5 秒退避)、5 分钟内最多 10 次的熔断。`data_dir` 若改了路径，同步修改 unit 里的 `ReadWritePaths=` 后再 `daemon-reload`。

## 第 7 步：验证

逐条执行，全部正常即部署完成：

```bash
systemctl status service-watch --no-pager       # active (running)
curl -fsS http://127.0.0.1:9170/ready           # 应返回就绪 JSON
monitorctl status                               # 目标状态表,目标应为 Running/Healthy
sudo journalctl -u service-watch -n 50 --no-pager   # 无持续报错
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
| 停 / 启 agent 服务 | `sudo systemctl stop service-watch` / `sudo systemctl start service-watch` |
| 看 agent 日志 | `sudo journalctl -u service-watch -f` |

事件流水 (告警/重启/Fatal/审计) 落盘在 `/var/lib/service-watch/journal.jsonl`。

## 升级

```bash
sudo systemctl stop service-watch
sudo install -m 0755 新monitor-agent /usr/local/bin/monitor-agent
sudo -u svcwatch /usr/local/bin/monitor-agent --check-config /etc/service-watch/config.toml
sudo systemctl start service-watch
curl -fsS http://127.0.0.1:9170/ready
```

## 卸载

```bash
sudo systemctl disable --now service-watch
sudo rm /etc/systemd/system/service-watch.service
sudo systemctl daemon-reload
# /etc/service-watch、/var/lib/service-watch(含 journal)按保留期限归档后再删
# 被监管子进程保留运行:ps 查看,由人工或外部机制接管
```

## 常见问题速查

| 现象 | 处理 |
|---|---|
| 服务启动失败，`journalctl` 见 config error | 按报错修 config.toml;以 svcwatch 跑 `--check-config` 确认 |
| 反复重启后变 failed 不再拉起 | 熔断生效 (300s 内 10 次);修根因后 `sudo systemctl reset-failed service-watch` 再 start |
| 9170 被占 | `sudo ss -ltnp | grep 9170` 找占用进程释放;或改配置 `[control].listen` 后重载 |
| `monitorctl` 报 unreachable | 服务没起或端口不一致：`curl -fsS http://127.0.0.1:9170/health` 定位 |
| 目标应用写文件失败 | systemd 沙箱限制：目录属主给 svcwatch，并确认 unit `ReadWritePaths=` 覆盖该路径 |
| 目标长期 attach 或等待 | 旧进程身份对账未完成，核对采集权限与进程存在性证据 |

更完整的排障与语义说明：`docs/deploy/systemd/README.md`、`docs/RUNBOOK.md`。
