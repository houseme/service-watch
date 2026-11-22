# service-watch 用户使用指南(中文)

> 适用版本:main 分支(v0.3.0,含 v0.4–v0.8 全部功能)。语义权威为 [DESIGN.md](../../DESIGN.md);部署细节见 [docs/DEPLOYMENT.md](../DEPLOYMENT.md);故障处置见 [docs/RUNBOOK.md](../RUNBOOK.md)。英文版:[user-guide.en.md](user-guide.en.md)。
>
> 本指南面向使用 service-watch 的运维人员与用户,按功能与使用场景组织:从概念、构建安装、快速上手,到配置参考、告警、指标、控制平面、恢复语义、部署、运维与排障。

---

## 目录

1. [简介](#1-简介)
2. [核心概念](#2-核心概念)
3. [构建与安装](#3-构建与安装)
4. [快速开始](#4-快速开始)
5. [配置参考](#5-配置参考)
6. [监控目标:check / service / watch](#6-监控目标check--service--watch)
7. [告警与通知](#7-告警与通知)
8. [降噪:静默与维护窗口](#8-降噪静默与维护窗口)
9. [指标与可观测性](#9-指标与可观测性)
10. [控制平面与 monitorctl](#10-控制平面与-monitorctl)
11. [恢复语义:进程拉活与 Fatal](#11-恢复语义进程拉活与-fatal)
12. [配置热重载](#12-配置热重载)
13. [部署为系统服务](#13-部署为系统服务)
14. [日常运维](#14-日常运维)
15. [故障排查](#15-故障排查)
16. [升级、回滚与卸载](#16-升级回滚与卸载)
17. [附录](#17-附录)

---

## 1. 简介

service-watch 是用 Rust 2024 编写的**跨平台服务与硬件监控工具**,覆盖三条能力线:

- **硬件监控**:CPU、内存、磁盘、网络、温度与进程指标采集;不可用数据以机读降级原因如实呈现,不伪造 0。
- **异常告警**:HTTP / TCP / 进程指标 / 进程存活四类检查,带事故(incident)去重、升级、恢复通知、抖动(flap)抑制、webhook 重试与持久化补投。
- **恢复拉活**:supervise 风格的进程恢复——连续确认、启动宽限、指数退避、滑动重启预算、Fatal 终态闩锁与依赖启动编排。

| 组件 | 职责 |
|---|---|
| `monitor-agent` | 守护进程:运行 `collect → evaluate → alert → recover` 主循环,提供本机 HTTP 控制平面,监管被托管进程 |
| `monitorctl` | 运维 CLI:面向控制平面查询与操作,另提供本机硬件快照工具 |
| `monitor-core` | 库:配置、采集、评估、告警、恢复、控制平面的全部核心逻辑 |

**设计边界(重要)**:

- 观察者外置:agent 独立于被监控服务的生命周期;agent 退出不会连带杀掉被监控服务,被监控服务也不负责拉起 agent。
- attach(`watch`)目标**只监控、永不杀/重启**;只有 `[[service]]` 监管目标会被 agent 拉活。
- agent 无 daemonize 模式,前台运行,交由 systemd / launchd / Windows SCM 托管。
- 被监管目标禁止 daemonize 型程序(双 fork 会被误判为立即死亡而反复拉起),必须以前台进程形态运行。
- 控制平面默认只监听回环地址 `127.0.0.1:9170`,依赖本机信任边界;远程使用需叠加 TLS / Bearer token / mTLS 信任层(见 §10.4)。

---

## 2. 核心概念

理解以下概念后,后续各章的配置与操作都会很直观。

### 2.1 三类监控目标

| 目标类型 | 配置段 | agent 行为 |
|---|---|---|
| **check(检查)** | `[[check]]` | 周期探测(http / tcp / process),产生健康状态与告警;不管理进程 |
| **service(监管)** | `[[service]]` | agent 亲生拉起的进程:监控 + 告警 + **崩溃自动拉活**,受重启策略约束 |
| **watch(attach)** | `[[watch]]` | 已有进程的"收养"监控:只读,永不杀/重启;以 `pid` 或 `pidfile` 标识 |

**同名绑定规则**:某 check 与某 service 同 `name` 时,该 check 的健康结论参与该 service 的恢复判定(健康失败达到条件会触发拉活);独立命名的 check 只提供告警。

### 2.2 检查状态机与事故(incident)

每条 check 独立维护状态机:

```text
Ok ──1 次失败──▶ Warning ──连续 failure_threshold 次失败──▶ Critical
 ▲                  │                │
 └──────连续 success_threshold 次成功──────────────┘
```

- `incident_key = hash(target, check)`,同一目标同一检查永远同 key;Warning → Critical 升级是**同一起事故**的升级,不产生第二起事故。
- 事故生命周期:`open(Warning) → escalate(Critical) → resolve(Ok)`;事件类别 `Alert / Escalate / Resolve / Flap`。
- 时间判定全部用单调时钟,宿主机墙钟回拨不会造成"永久抑制"或"告警风暴"。

### 2.3 恢复状态机与 Fatal 闩锁

监管目标按状态机推进:`running → suspect → recovering → restarting → starting → crashloop → fatal` 等(全转移表见 §11)。关键点:

- 健康类失败需连续 `confirmations`(默认 2)次确认才触发恢复;`startup_grace` 预热期内的失败不计入。
- 滑动窗口 `restart_window`(默认 10m)内自动重启达到 `max_restarts`(默认 5)→ **Fatal 闩锁**。
- Fatal 是终态:**永不自动恢复**(窗口滑过、agent 重启都不恢复),唯一出口是人工 `monitorctl reset`(写审计)。

### 2.4 journal(事件日志)

`{data_dir}/journal.jsonl` 是 append-only JSONL 事件日志,记录 `incident / restart / fatal / reset / reset_targets / process_identity / spawn_intent / spawn_outcome / audit` 九类事件。它是恢复域的事实源:agent 重启后从 journal 重放 Fatal、预算与进程身份,进行孤儿对账。journal 支持内置单代轮转(默认 10 MiB → `journal.1.jsonl`)与 `monitorctl journal` 尾部查询(见 §9.4、§14)。

### 2.5 控制平面

agent 内置 HTTP 控制平面(默认 `127.0.0.1:9170`):状态页、状态/指标查询、restart/check/reset/silence 等操作命令、journal 查询与热重载入口。`monitorctl` 是它的薄客户端。

---

## 3. 构建与安装

### 3.1 从源码构建

需要 Rust stable 工具链。

```bash
cargo build --release -p monitor-agent -p monitorctl
# 产物:./target/release/monitor-agent 与 ./target/release/monitorctl
```

### 3.2 可选 feature

| Feature | 作用 |
|---|---|
| `collect-deep-hardware` | 深度传感器采集:Linux hwmon / macOS AppleSMC 的风扇转速与电压(默认关闭) |
| `windows-service` | Windows SCM 服务入口(`--service-run`,仅 Windows) |

```bash
# 深度硬件采集(agent 与 monitorctl 均可使用)
cargo build --release -p monitor-agent -p monitorctl --features collect-deep-hardware

# Windows SCM 服务形态
cargo build --release -p monitor-agent --features windows-service
```

**Windows 构建注意**:默认 HTTP/TLS 栈为 rustls + aws-lc-rs,需要 NASM 与 CMake(`choco install -y nasm cmake`)。

### 3.3 平台能力矩阵

| 能力 | Linux | macOS | Windows |
|---|---|---|---|
| CPU/内存/磁盘/网络/进程 | sysinfo | sysinfo | sysinfo |
| 温度 | hwmon 依赖 | 默认可用(SMC/HID,经可信过滤) | 常为空,机读降级 |
| 风扇/电压(深度采集) | hwmon 只读 | AppleSMC 只读,机型相关 | 机读 unsupported 降级 |
| supervise 隔离 | 进程组 | 进程组 | Job Object |
| graceful 停止 | `killpg(SIGTERM)` | 同左 | TerminateProcess 基线;`console=true` 可尝试 CTRL_BREAK |
| 强杀 | `killpg(SIGKILL)` | 同左 | `TerminateJobObject` |
| 系统守护 | systemd | launchd | SCM(`windows-service` feature) |

### 3.4 获取发布产物

GitHub Release 提供 Linux x86_64 与 Windows x86_64 资产(tag 触发构建);macOS 资产自 v0.3.0 后的下一 tag 起恢复。也可参照各平台部署文档自行构建。

---

## 4. 快速开始

### 4.1 五分钟上手(独立 HTTP 监控,不拉活进程)

```bash
# 1) 离线校验示例配置(退出码 0 = 通过)
./target/release/monitor-agent --check-config config/config.example.toml

# 2) 采集一次硬件快照即退出(不启动运行时),验证采集层
./target/release/monitor-agent --dump-snapshot

# 3) 创建最小配置 service-watch.toml
```

```toml
[agent]
data_dir = "./data"

[control]
enabled = true
listen = "127.0.0.1:9170"

[alert]
webhook_url = ""              # 填实际地址后启用通知;空 = 只记 journal/日志
webhook_format = "json"       # json | slack
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

```bash
# 4) 前台启动
./target/release/monitor-agent --config service-watch.toml

# 5) 另开终端验证
curl -fsS http://127.0.0.1:9170/health    # 200 = 进程活着(浅存活)
curl -fsS http://127.0.0.1:9170/ready     # 200 = 配置加载 + 调度已启动
./target/release/monitorctl status
./target/release/monitorctl metrics --prom
```

浏览器打开 [http://127.0.0.1:9170/](http://127.0.0.1:9170/) 查看只读状态页(含关键指标迷你折线)。

### 4.2 加入进程拉活(service + 同名 check)

```toml
[[service]]
name = "my-app"
command = "/srv/my-app/bin/my-app"   # 必须前台运行,禁止 daemonize 型
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
name = "my-app"                      # 与 service 同名 → 健康结论参与恢复判定
kind = "http"
url = "http://127.0.0.1:8080/healthz"
interval = "10s"
timeout = "5s"
```

常用操作:

```bash
./target/release/monitorctl restart my-app   # 手动重启(202 = 已入队)
./target/release/monitorctl check my-app     # 让绑定检查立即到期
./target/release/monitorctl reset my-app     # Fatal 后先修根因,再解除闩锁
```

### 4.3 监控已有进程(watch,永不干预)

```toml
[[watch]]
name = "existing-app"
pidfile = "/run/existing-app.pid"    # 或 pid = 1234

[[check]]
name = "existing-app"
kind = "process"
process = "existing-app"             # 也支持 "pid:1234"
cpu_threshold = 90.0
mem_threshold_mb = 1024
interval = "10s"
timeout = "5s"
```

---

## 5. 配置参考

### 5.1 加载与校验

- 默认读取 `./service-watch.toml`;`--config <path>` 指定路径。
- **未知键一律拒绝**(`deny_unknown_fields`),写错键名在 `--check-config` 即报错。
- 时长字段接受 `"10s"` / `"5m"` 形式字符串。
- 完整带注释模板:[config/config.example.toml](../../config/config.example.toml)(与 DESIGN.md §4 一一对应,由单测保证可解析)。

```bash
./target/release/monitor-agent --check-config service-watch.toml   # 输出 config ok,退出码 0
```

### 5.2 配置段总览

| 段 | 用途 | 关键字段 |
|---|---|---|
| `[agent]` | 数据位置 | `data_dir`(journal/spool 根目录,默认 `./data`)、`journal_max_size`(轮转阈值,默认 10 MiB,`0`=禁用) |
| `[log]` | 日志 | `json` = 结构化 JSON 日志 |
| `[control]` | HTTP 控制平面 | `enabled`、`listen`(默认仅回环)、`token`(可选 Bearer)、`tls_cert/tls_key`(可选 HTTPS)、`client_ca_cert`(可选 mTLS) |
| `[collect]` | 采集节奏 | `interval`(默认 5s) |
| `[alert]` | webhook 告警投递 | 见 §7 |
| `[metrics]` | 指标周期推送/持久化 | 见 §9.3、§9.5 |
| `[[check]]` | 检查目标(可多个) | 见 §6 |
| `[[service]]` | 监管目标(可多个) | 见 §6 |
| `[[watch]]` | attach 目标(可多个) | 见 §6 |
| `[[alert.routes]]` | 按目标告警路由(可多个) | 见 §7.4 |
| `[[maintenance]]` | 定期维护窗口(可多个) | 见 §8.2 |

### 5.3 默认值总表

| 参数 | 默认值 | 参数 | 默认值 |
|---|---|---|---|
| `agent.journal_max_size` | 10 MiB(`0`=禁用轮转) | `alert.failure_threshold` | 3 |
| `alert.timeout` | 10s | `alert.success_threshold` | 2 |
| `alert.retries` | 3 | `alert.renotify_interval` | 15m |
| `alert.flap_window` | 30m | `alert.flap_threshold` | 5 |
| `restart.min_uptime` | 10s | `restart.backoff_initial` | 1s |
| `restart.backoff_max` | 60s | `restart.max_restarts` | 5 |
| `restart.restart_window` | 10m | `restart.stop_grace` | 15s |
| `restart.startup_grace` | 30s | `restart.confirmations` | 2 |
| `activate.timeout` | 10s | `activate.verify_for` | 30s |
| `control.listen` | `127.0.0.1:9170` | `metrics.interval` | 60s |
| `maintenance.duration` | 1h(校验下限 60s) | `maintenance.days` | 缺省 = 每天 |

### 5.4 主要校验规则(违反即配置错误,退出码 1)

1. 每个 check `timeout < interval`;check / collect / `[metrics]`(启用时)间隔均 ≥ 100ms。
2. http check:`url` 合法且 scheme 为 http/https;`expected_status` 形如 `"200-299"`。
3. process check:`process` 非空,cpu/mem 阈值至少一个,CPU 阈值有限且非负。
4. supervise:`command` 非空;`backoff_initial ≤ backoff_max`;`startup_grace ≥ min_uptime`;`depends_on` 引用必须存在、禁止自依赖与环。
5. `hung_action = "activate"` 时必须配置 `[service.activate]`,钩子 command/url 必须且只能配置一个。
6. check/service/watch 名称各自唯一且非空;`data_dir` 非空并可创建;告警阈值至少 1;`alert.retries ≤ 100`;webhook/deadman URL scheme 为 http/https。
7. `[[watch]]` 的 `pidfile` 非空路径。
8. `[control] tls_cert/tls_key` 成对必填;`client_ca_cert` 必须与 TLS 同配。
9. `[[alert.routes]]` 的 `match` 非空白、url scheme 合法;`webhook_secret` / 路由级 `secret` / `[metrics] secret` 不得为纯空白。
10. `[alert] headers` / 路由级 `headers`:头名必须为 RFC 7230 token,值非空且不含 CR/LF/控制字符(仅可见 ASCII 与 TAB);表内/跨表仅大小写不同的头名拒绝。
11. `[[maintenance]]`:`targets` 非空,`start_utc` 为两位 `HH:MM`(UTC),`duration` ≥ 60s,`days` 仅 `mon tue wed thu fri sat sun`。

---

## 6. 监控目标:check / service / watch

### 6.1 check(检查目标)

```toml
[[check]]
name = "api-health"                     # 全局唯一
kind = "http"                           # http | tcp | process
url = "http://127.0.0.1:8080/healthz"   # http 必填
expected_status = "200-299"             # 可选,默认 200-299(含边界);探测跟随重定向(≤10 跳),302 链终点在区间内判 OK
interval = "10s"                        # 必须 > timeout
timeout = "5s"
```

三种 `kind`:

- **http**:`url` + `expected_status`。需要严格判定时把 `url` 指向最终端点。
- **tcp**:`addr = "127.0.0.1:5432"` 直连判定。
- **process**:对具名进程(`process = "my-app"`,可为已配置 service/watch 的名称)或 `pid:1234` 设 `cpu_threshold` / `mem_threshold_mb` 阈值,至少一个。

**在途探测上限 1**:上一轮未完成则跳过本轮并记 `probe_skipped` 计数;`timeout < interval` 由配置校验保证,探测不会堆积。宿主机休眠唤醒后的首轮探测丢弃并记 degraded。

### 6.2 service(监管目标)

```toml
[[service]]
name = "my-app"
command = "/usr/local/bin/my-app"       # 必填;前台进程形态
args = ["--port", "8080"]
workdir = "/srv/my-app"
env = { RUST_LOG = "info" }
console = false                         # Windows:true = CREATE_NEW_PROCESS_GROUP,允许尝试 CTRL_BREAK
depends_on = ["db"]                     # 可选:依赖全部 Running 且有进程证据后才启动

[service.restart]                       # 全部字段可省略,默认值见 §5.3
min_uptime = "10s"                      # 存活低于此 = crash loop 嫌疑(fast_exit)
backoff_initial = "1s"                  # 重启退避起步
backoff_max = "60s"                     # 退避封顶(指数 ×2)
max_restarts = 5                        # restart_window 滑动窗口内最大自动重启次数
restart_window = "10m"
stop_grace = "15s"                      # graceful → 强杀升级时限
startup_grace = "30s"                   # 预热期:失败不计入状态机
confirmations = 2                       # 健康类失败触发恢复的确认次数
hung_action = "restart"                 # restart | activate

[service.activate]                      # hung_action = "activate" 时必填
command = "/usr/local/bin/kick-my-app"  # 或 url = "http://...";两者必须且只能配一个
timeout = "10s"
verify_for = "30s"                      # 激活后观察期,仍失败转 Restart
```

要点:

- **依赖编排**(`depends_on`):依赖全部达到 Running 且有进程证据后才冷启动;依赖 Fatal → 依赖方永不自动启动(失败不级联,人工介入)。控制面 restart 某服务时先按逆拓扑序停其被依赖方,恢复后级联拉回。
- **挂死处置**(`hung_action`):`restart` 走完整重启;`activate` 先执行激活钩子(命令或 HTTP),`verify_for` 观察期内健康恢复 → Running,否则转 Restart 并计入预算。

### 6.3 watch(attach 目标)

```toml
[[watch]]
name = "systemd-nginx"
pidfile = "/run/nginx.pid"              # 或 pid = 1234
```

只监控告警,永不杀/重启;进程管理由其原有管理器(如 systemd)负责。

---

## 7. 告警与通知

### 7.1 阈值与通知规则

| 规则 | 默认值 | 含义 |
|---|---|---|
| 首条通知永不抑制 | — | 事故第一条告警必达(杜绝 Resolve 后 15 分钟内复发被冷却吞掉的盲区) |
| `failure_threshold` | 3 | 连续失败次数 → Warning(alert)→ Critical(escalate) |
| `success_threshold` | 2 | 连续成功次数 → Ok(resolve);Warning → Ok 同样发 Resolve |
| `renotify_interval` | 15m | 事故持续期间同 severity 重复通知的间隔;**Escalate(Warning→Critical)总是发送** |
| `flap_window` / `flap_threshold` | 30m / 5 | 窗口内 open/close 达标 → 只发一条 Flap 摘要(含计数),抑制期结束自动退出 |

Resolve 语义:引用原事故且**总是发送**(前提:该事故曾发出过通知);从未通知过的事故恢复时不发 Resolve(避免噪音)。webhook 接收端请按 `category=resolve` 收敛告警面板,不要按 severity 猜。

### 7.2 webhook 投递

```toml
[alert]
webhook_url = "https://hooks.example.com/alert"   # 空 = 不投递,事件仍入 journal/日志
webhook_format = "json"                            # json(AlertEvent 原样)| slack(Slack/Discord 兼容 {"text":...})
timeout = "10s"                                    # 单次投递超时
retries = 3                                        # 失败指数退避重试:1s 起步 ×2,封顶 10s;上限 100
deadman_url = ""                                   # 可选心跳 POST("agent 活着但失聪"由外部心跳兜底)
```

- 最终失败记 ERROR 日志 + `alert_delivery_failures` 计数。
- 有界队列(256)满 → 丢最旧 + `alerts_dropped` 计数(告警时效优先于完整性)。
- **投递失败持久化(spool)**:失败事件写入 `{data_dir}/spool/pending.jsonl`,重放成功确认后删除,7 天过期。注意:强杀场景下未落盘的内存事件存在丢失窗口。
- `deadman_url` 周期心跳 POST,失败仅记日志;生产建议配置,配合外部心跳监控兜底。

### 7.3 HMAC 签名(F29/F34)

```toml
[alert]
webhook_secret = "change-me"       # 全局签名密钥;缺省(不写)= 不签名
[[alert.routes]]
match = "edge"
url = "https://hooks.example.com/edge"
secret = "edge-only"               # 路由级覆盖全局

[metrics]
secret = "push-secret"             # 指标推送同口径签名
```

配置后每笔投递/推送附头 `X-Service-Watch-Signature: sha256=<hex hmac-sha256(body)>`。签名对象 = 实际发送的 body 字节。接收端验签(务必恒定时间比较):

```bash
# openssl 一行验签($body = 收到的原始字节,$sig = 头里的 hex,去掉 "sha256=" 前缀)
printf '%s' "$body" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print $2}' | grep -qx "$sig"
```

```python
# python(hmac.verify / compare_digest 内部为恒定时间比较)
import hmac, hashlib
hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest() == sig.removeprefix("sha256=")
```

注意:HTTP/2 与严格接收端看到的头名为全小写 `x-service-watch-signature`(RFC 9110 头名大小写不敏感)。密钥与签名值都不进日志。

### 7.4 按目标路由(F26)

```toml
[[alert.routes]]
match = "edge"                        # target 名称前缀首匹配;或 "*" 通配全部
url = "https://hooks.example.com/edge"
format = "slack"                      # 缺省 json
```

- 按配置顺序首条匹配;无匹配回落 `webhook_url`(默认通道 = 兜底)。
- 路由命中与否不影响超时/退避/队列/spool 语义。
- **spool 积压重放按 agent 启动时的当前配置重新路由**——改路由配置并重启后,旧积压按新配置送达。

### 7.5 自定义投递头(F37)

```toml
[alert]
headers = { X-Auth-Token = "global-secret" }          # 全局表;默认通道用全局表

[[alert.routes]]
match = "edge"
url = "https://hooks.example.com/edge"
headers = { X-Auth-Token = "route-secret", X-Team = "edge" }  # 同名键覆盖全局,非同名键共存
```

- 每笔投递(slack/json 与 spool 重放同路径)全部附带;与签名头独立共存。
- **不要自定义保留头** `X-Service-Watch-Signature`(会产生两个同名头,多数接收端取第一个或拒绝);`content-type`/`host` 同理勿覆盖。
- 头值可能是敏感 token:只进请求头,不落日志/审计。

---

## 8. 降噪:静默与维护窗口

两个机制都**只抑制通知**:`Alert / Escalate / Flap` 不投递;`Resolve`(恢复通知)与 Fatal 闩锁合成告警**恒穿透**;journal Incident 事件照常写(事后可查证);检查状态机、恢复 FSM、重启预算完全不受影响。

### 8.1 目标级静默(F27,一次性,运行时操作)

```bash
# 部署前静默 30 分钟,带审计原因
monitorctl silence my-app --duration 1800 --reason "planned deploy v2.3.0"

# 部署完成,提前解除(写审计,剩余窗口作废)
monitorctl unsilence my-app

# 查静默审计(Audit 事件无 target 字段,不要加 --target)
monitorctl journal --type audit --tail 20
```

语义与边界:

- 到期自动解除(单调时钟 deadline,与墙钟回拨无关),到点写一条 audit。
- **agent 重启后静默不保留**(内存态;重启即失忆,宁多响不漏报)。需要跨重启的静默,重启后重设。
- 重复 silence 同一 target = 后到覆盖(窗口从新命令时刻重算)。
- `--duration 0` 或超 7 天(604800s)、未知 target:CLI 仍显示 `accepted (202)`,实际拒绝看 journal 审计。
- 静默期首次抑制命中写一条可见性 audit,期内不逐条记录(防刷屏)。
- 解除静默后,进行中事故的下一通知最多延迟一个 `renotify_interval`(默认 15m)。

### 8.2 定期维护窗口(F31,周期性,配置声明)

```toml
[[maintenance]]
name = "nightly-backup"        # 可选;缺省 = 审计报 targets
targets = ["db-*"]             # target 前缀或 ["*"] = 全部
start_utc = "18:30"            # UTC "HH:MM"(把本地时刻换算成 UTC 后填写)
duration = "30m"               # ≥ 60s;可跨午夜
days = ["mon", "wed", "fri"]   # 可选;缺省 = 每天
```

- **全部时刻按 UTC**,不支持 DST(夏令时切换表现为窗口相对本地钟平移 1h)。
- 跨午夜合法:`start_utc = "23:00"` + `duration = "3h"` → 次日 02:00 结束;越界部分归属**开启日**的星期。
- 窗口开启/关闭各写一条 journal audit;agent 重启后按 UTC 时钟自动重入(与 silence 的"重启即失忆"不同——维护窗口是配置声明的长期意图)。
- **不在热重载范围**:变更后重启 agent 生效。
- 与 silence 叠加:任一命中即抑制,无优先级。

---

## 9. 指标与可观测性

### 9.1 HTTP 指标端点

| 端点 | 内容 |
|---|---|
| `GET /metrics` | JSON `MetricsReport`;`?format=prometheus`(或 `Accept: text/plain`)输出 Prometheus 文本,指标前缀 `service_watch_` |
| `GET /metrics/history?range=<secs>` | 历史窗口;缺省 300s,上限 7200s;超过 600 点等距降采样(保留首尾) |
| `GET /status` | 各目标健康 + 恢复状态机位置(结构见 §10.2) |
| `GET /ready` | 配置加载成功 + 调度已启动为 200;**采集降级不影响 ready** |
| `GET /health` | 浅存活,恒 200,**恒免鉴权** |
| `GET /` | 只读 HTML 状态页(页内 JS 每 2s 轮询 `/status` + `/metrics`,并绘制关键指标迷你折线) |

### 9.2 关键指标(巡检基线)

| 分组 | 字段 | 含义 |
|---|---|---|
| 告警管道 | `alerts_sent` / `alert_delivery_failures` / `alerts_dropped` | 已发通知 / 投递最终失败 / 队列满丢最旧 |
| journal | `journal_dropped` / `journal_write_failures` / `journal_write_successes` / `journal_critical_failures` | 普通事件丢弃 / 写失败 / 写成功 / 关键确认失败 |
| 探测 | `probe_executed` / `probe_skipped` | 已执行 / 在途上限 1 被跳过(skipped 持续增长 = interval 过紧或目标过慢) |
| 恢复 | `restarts_total` / `state_transitions_total` / `fatal_targets` 及 `recovery_*` 状态分布 | 恢复活跃度与 Fatal 数 |
| 采集 | `degradations_current` / `degradations_events` / `last_collect_at` | 当前降级数 / 降级跳变 / 最近采集时间 |
| 概览 | `targets_total` / `checks_{ok,warning,critical}` / `services_{total,running}` / `uptime_secs` | 面板直接取用 |

**巡检基线告警建议**:`alert_delivery_failures` 增长、`alerts_dropped` 增长、`probe_skipped` 持续增长、`recovery_fatal > 0`、`degradations_events` 突增、`last_collect_at` 停滞。

Prometheus 文本与 JSON 字段同名(前缀 `service_watch_`);唯一例外:`last_collect_at` 输出为 `service_watch_last_collect_timestamp_seconds`(尚未完成采集时整行不输出——抓取侧表现为无数据而非 0)。

### 9.3 指标周期推送(`[metrics]`)

```toml
[metrics]
enabled = true
url = "http://collector:9090/ingest"   # POST MetricsReport JSON
interval = "60s"                       # timeout < interval(校验强制)
timeout = "10s"
# secret = "change-me"                 # 可选 HMAC 签名(F34)
```

语义:单次尝试、失败即放弃(下一周期自然是最新值);连败边沿写 journal 审计(粘性边沿不刷屏,恢复首轮亦审计)。抓取器更推荐拉模式 `GET /metrics?format=prometheus`(鉴权开启时同样需携带 token)。

### 9.4 journal 查询与结构

journal 位于 `{data_dir}/journal.jsonl`,事件九类(示例与字段见 [RUNBOOK §2.1](../RUNBOOK.md)):

```jsonc
{"type":"incident","at":{...},"incident":1234567890,"target":"my-app","check":"api-health",
 "category":"alert","severity":"warning","reason":"HTTP probe failed: connect timeout"}
{"type":"restart","at":{...},"target":"my-app","fast_exit":true}
{"type":"fatal","at":{...},"target":"my-app","reason":"restart budget exhausted: 5 restarts in 10m window"}
{"type":"reset","at":{...},"target":"my-app","source":"control"}
{"type":"audit","at":{...},"message":"..."}
{"type":"reset_targets","at":{...},"targets":["my-app"],"source":"reload"}
{"type":"process_identity","at":{...},"target":"my-app","identity":{"pid":1234,"start_time":1731...},"verified":true}
{"type":"spawn_intent","at":{...},"target":"my-app","config_digest":"a1b2c3d4e5f60708"}
{"type":"spawn_outcome","at":{...},"target":"my-app","identity":{...},"pid_confirmed":true}
```

查询方式:

```bash
# 经控制平面(可远程、可鉴权;含上一代 journal.1.jsonl 合并尾读)
monitorctl journal --tail 50
monitorctl journal --target my-app --type fatal --tail 20 --json

# 本机全量 jq
J=/var/lib/service-watch/journal.jsonl
jq -r 'select(.type=="fatal") | "\(.at.secs_since_epoch | todate)\t\(.target)\t\(.reason)"' "$J"
jq -r 'select(.type=="restart") | .target' "$J" | sort | uniq -c | sort -rn | head -10
```

### 9.5 指标历史持久化(F39)

```toml
[metrics]
# history_path = "./data/metrics-history.jsonl"   # 缺省 = 仅内存环形缓冲
```

配置后每轮指标样本以 JSONL 追加落盘,一行 = `/metrics/history` 单样本对象 `{"at":..., "report":{...}}`。分析走 jq:

```bash
jq -c '.report | {probe_executed, alerts_dropped, recovery_fatal}' ./data/metrics-history.jsonl | tail -20
jq -c 'select(.report.recovery_fatal > 0)' ./data/metrics-history.jsonl
```

要点:重启**追加不截断**(不要指向会被启动脚本清空的路径);超 10 MiB 内置单代轮转(`metrics-history.1.jsonl`,轮转只写 tracing 日志);满载约 80–90 MB/日,轮转是预期常态;外部轮转工具 copytruncate 兼容、rename 式无效、**不得删除主文件**;文件打不开时该功能本次运行停用,agent 照常启动。

---

## 10. 控制平面与 monitorctl

### 10.1 端点一览

默认 `http://127.0.0.1:9170`。

| 端点 | 方法 | 行为 |
|---|---|---|
| `/health` | GET | 浅存活,恒 200,恒免鉴权 |
| `/ready` | GET | 配置加载 + 调度启动判定;503 = 尚未就绪 |
| `/status` | GET | 全目标状态报告 |
| `/metrics`、`/metrics/history` | GET | 指标(§9.1) |
| `/` | GET | 只读状态页 |
| `/v1/restart/{target}` | POST | 入队优雅重启;**Fatal 目标 409**(提示先 reset) |
| `/v1/check/{target}` | POST | 绑定检查立即到期 |
| `/v1/reset/{target}` | POST | 清除 Fatal 闩锁 + 重启预算(写审计) |
| `/v1/silence/{target}` | POST / DELETE | 目标级静默 / 提前解除(body `{"duration":<秒>,"reason":"<可选>"}`) |
| `/v1/journal?tail=&target=&type=&since=` | GET | journal 尾部查询(tail 缺省 200,上限 1000) |
| `/v1/config` | GET | 生效配置视图(token/签名密钥/投递头值以 `***redacted***` 占位,**不可回灌**) |
| `/v1/reload` | POST | 热重载拓扑(body 可选 `{"path":"..."}`),见 §12 |

**入队语义**:`restart / check / reset / silence / unsilence / reload` 返回 **202 = 已入队**(命令通道容量 16,满则 503);入队**不校验目标存在性**,打错名字也会 accepted,无效目标由 eval 侧拒绝并写 journal 审计——操作后请以 `status` 与 journal 确认实际效果。

### 10.2 `/status` 结构

```jsonc
{"started_at": {...}, "ready": true,
 "targets": [{
   "name": "my-app",
   "mode": "supervise",              // supervise | attach
   "health": "ok",                   // ok | warning | critical(检查视角聚合)
   "recovery": "running",            // null | running | suspect | recovering | force_kill
                                     //      | restarting | starting | crashloop | fatal
   "checks": ["api-health"],
   "identity": {"pid": 4242, "start_time": 1731...},
   "incident": null,                 // incident 级别(ok|warning|critical)
   "restarts_in_window": 0,
   "last_transition": {...}
 }]}
```

### 10.3 monitorctl 命令

```bash
monitorctl [--url <URL>] [--token <TOKEN>] [--insecure]
           [--client-cert <PATH> --client-key <PATH>] <COMMAND>
```

| 操作 | 命令 |
|---|---|
| 状态表格 / 原始 JSON | `monitorctl status` / `monitorctl status --json` |
| 持续观察(全屏 Δ 历史) | `monitorctl watch --interval 2` |
| JSON Lines 观察 / 遇错退出 | `monitorctl watch --interval 2 --json` / `monitorctl watch --exit-on-error` |
| 立即探测 | `monitorctl check my-app` |
| 重启监管目标 | `monitorctl restart my-app` |
| 清除 Fatal 与预算 | `monitorctl reset my-app` |
| 静默 / 解除 | `monitorctl silence my-app --duration 1800 --reason "planned deploy"` / `monitorctl unsilence my-app` |
| journal 查询 | `monitorctl journal --tail 50` / `monitorctl journal --target my-app --type fatal --tail 20 --json` |
| JSON / Prometheus 指标 | `monitorctl metrics` / `monitorctl metrics --prom` |
| 生效配置(表格/JSON) | `monitorctl config` / `monitorctl config --json` |
| 本机硬件快照(不经 agent) | `monitorctl dump-snapshot` |
| 指定控制面 | `monitorctl --url http://127.0.0.1:9270 status` |
| 携带 token | `--token <TOKEN>` 或环境变量 `SERVICE_WATCH_TOKEN`(flag 优先) |

**monitorctl 退出码**:`0` 成功;`1` 配置错误(dump-snapshot);`2` 运行错误(含 409/非 2xx);`3` 目标不存在(保留码,基本不出现——无效目标由 agent 侧拒绝并写审计);`4` agent 不可达。脚本化巡检请按退出码分支,不要解析文案。monitorctl 自身日志默认静默,设 `RUST_LOG` 才输出(stderr)。

### 10.4 传输安全三件套(独立配置、独立叠加)

1. **HTTPS(F32)**:`[control]` 配 `tls_cert` + `tls_key`(PEM 路径,成对必填)→ 同一 `listen` 地址以纯 HTTPS 监听。明文请求打 TLS 端口只会得到连接错误。证书加载失败启动即退(退出码 2)。**轮换 = 重启**。

   ```toml
   [control]
   listen = "127.0.0.1:9170"
   tls_cert = "/etc/service-watch/tls/cert.pem"   # leaf 证书在前,后跟中间链
   tls_key  = "/etc/service-watch/tls/key.pem"    # 权限 0600
   ```

   自签测试证书生成:

   ```bash
   openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
     -keyout key.pem -out cert.pem -days 365 \
     -subj "/CN=service-watch" -addext "subjectAltName=IP:127.0.0.1,DNS:localhost"
   ```

   monitorctl 侧 `--url https://...` 走系统信任链;自签/私有 CA 需装入系统信任存储,临时测试可 `--insecure`(**仅测试场景**,关闭校验意味着中间人不可检测)。

2. **Bearer token(F23)**:`[control] token = "..."` 后,除 `GET /health` 外全部端点要求 `Authorization: Bearer <token>`,缺失/错误一律 401(不区分失败原因,恒定时间比较,token 不落日志)。未配置 token 时 agent 忽略任何 Authorization 头。

3. **mTLS(F38)**:`[control] client_ca_cert`(必须与 TLS 同配)→ TLS 握手强制客户端证书由该 CA 签发;monitorctl 用成对 `--client-cert` + `--client-key`:

   ```bash
   monitorctl --url https://127.0.0.1:9170 \
     --client-cert client-cert.pem --client-key client-key.pem \
     --token "$SERVICE_WATCH_TOKEN" status
   ```

   客户端证书过期表现为 monitorctl 退出 4,先查 `openssl x509 -enddate`。

**典型组合**:公网远程运维 = TLS + Bearer + mTLS 全开;本机部署 = 三者皆关。token 只是控制面访问凭据不是安全边界——`listen` 保持回环、防火墙收紧仍是最外层防线。

**鉴权与状态页**:状态页页内 JS 无法携带自定义头——鉴权开启后浏览器直开 `/` 会 401;如需页面请经反向代理注入头,或对浏览器来源放开防火墙而保持 token 只给 API 调用方。

---

## 11. 恢复语义:进程拉活与 Fatal

### 11.1 触发契约

| 判定 | 依据 | 路径 |
|---|---|---|
| 确定性死亡 | 亲生进程退出事件;或 attach 目标 liveness 连续 `confirmations` 次查无 `(pid, start_time)` | 直接 → Recovering |
| 健康类失败 | 检查 Critical 且进程存活 | → Suspect,连续 `confirmations` 次确认 → Recovering |
| 挂死(Hung) | Critical 且存活且失败均为超时 | 同上;Recovering 优先 `hung_action` |

恢复执行期间该目标的告警升级被抑制(避免"刚重启就被判挂死"循环);Starting 宽限期仅按时间计(`last_spawn + startup_grace`)。

### 11.2 恢复状态机(全转移表)

```text
Running      --确定性死亡------------------------▶ Recovering
Running      --健康失败达阈值(进程在)------------▶ Suspect
Suspect      --再次确认失败----------------------▶ Recovering
Suspect      --确认恢复(连续成功)--------------▶ Running
Recovering   --activate:钩子成功且观察期健康恢复--▶ Running
Recovering   --activate:超时/验证失败------------▶ stop → Restarting(计入预算)
Recovering   --restart:graceful 成功-------------▶ Restarting
Recovering   --graceful 超时---------------------▶ ForceKill --成功--▶ Restarting
ForceKill    --失败(如 Unix D 状态)-------------▶ Fatal
Restarting   --backoff 后 spawn 成功-------------▶ Starting(startup_grace)
Restarting   --spawn 失败(二进制缺失/端口占用)---▶ CrashLoop(计入预算)
Starting     --存活 > min_uptime 且健康首次成功---▶ Running
Starting     --进程退出且 uptime < min_uptime-----▶ CrashLoop(计入预算)
Starting     --健康持续失败超过 startup_grace+2×interval --▶ Suspect
CrashLoop    --backoff 重试,窗口内计数未满--------▶ Restarting(指数退避)
CrashLoop    --restart_budget 窗口计数耗尽--------▶ Fatal + Critical 告警
Fatal        --仅 monitorctl reset(审计)---------▶ Running(重新计数)
(任意态)     --agent 重启------------------------▶ 从 journal 重放 + 孤儿对账
```

- 退避:`backoff_initial × 2^n`,封顶 `backoff_max`;Running 持续 `min_uptime` 后重置。
- **所有自动重启(含 spawn 失败重试)均计入预算**——防"每 11 分钟死一次"绕过预算。
- 一切杀/重启动作前必须校验 `(pid, start_time)` 身份;裸 PID 永远不作为杀进程依据(PID 复用防护)。

### 11.3 崩溃安全与孤儿对账

- 每次 spawn 前先落盘 `spawn_intent`,结果落盘 `spawn_outcome`;关键事件等待 `sync_data` 确认(2s 超时或写入错误 → PersistenceFailed 停机,退出码 2)。
- agent 重启后:重放 journal 恢复 Fatal/预算/身份;确认旧身份仍存活 → 转只读 attach(接管期间禁止 stop/kill/activate/restart);确认旧身份消失 → 按当前配置与预算恢复 supervise。
- 被监管子进程与 attach 目标在 agent 退出后**继续存活**;Fatal 闩锁跨 agent 重启保持。
- 一个 `data_dir` 只由一个 agent 实例使用。

### 11.4 Fatal 处置流程(每步都别跳)

```bash
# 1) 发现:status 中 recovery=fatal(通常伴随 Critical 告警)
monitorctl status

# 2) 查 journal:Fatal 原因与重启时间线
jq -c 'select(.type=="fatal" or ((.type=="restart") and .target=="<target>"))' \
   /var/lib/service-watch/journal.jsonl | tail -30

# 3) 定位根因:依赖未起 / 二进制或配置错 / 端口被占 / OOM / 路径权限
#    fast_exit=true 为主的重启 → 崩溃循环,看被监管进程自身日志

# 4) 修根因 —— reset 只解除闩锁,不修任何东西;不修根因 reset 后只会再次 Fatal

# 5) 解除闩锁(写审计,清空 Fatal + 重启预算)
monitorctl reset <target>

# 6) 立即验证
monitorctl check <target>
monitorctl status        # 期望 recovery: starting → running
```

**不要手工编辑 journal 来"洗掉" Fatal**(会破坏重放顺序语义);历史教训用 `journal.jsonl.bak-*` 整体备份。

---

## 12. 配置热重载

`SIGHUP`(Unix)与 `POST /v1/reload` 应用 **check / service / watch 拓扑变更**(增删/重定义目标):

```bash
# 先离线校验实际启动配置
monitor-agent --check-config /etc/service-watch/config.toml

# 提交 reload(缺省重读启动路径;也可显式指定)
curl -fsS -X POST http://127.0.0.1:9170/v1/reload
curl -fsS -X POST http://127.0.0.1:9170/v1/reload \
  -H 'Content-Type: application/json' \
  --data '{"path":"/etc/service-watch/config.toml"}'

# Windows PowerShell
Invoke-WebRequest -Method Post -Uri http://127.0.0.1:9170/v1/reload
```

范围与语义:

- **热应用**:checks/services/watches 增删改(name 匹配);check 参数变更保留状态机水位;service 重定义 = 先停止并收割旧实例,再启动新定义;删除与重定义前持久化 Reset,清除旧定义的 Fatal 与预算。
- **要求重启**:`[agent] / [log] / [control] / [collect] / [alert] / [metrics] / [[maintenance]]` 段变更(reload 会记审计)。
- 解析或校验失败**整体拒绝**,旧配置原样生效;202 后通过日志与 journal 审计确认应用结果。
- reload 剪除被删 target 的静默条目(同名重新加入不会复活旧静默)。

---

## 13. 部署为系统服务

完整分平台步骤见 [docs/DEPLOYMENT.md](../DEPLOYMENT.md) 与 [docs/deploy/](../deploy/README.md);此处为摘要。

### 13.1 目录与路径约定

| 项目 | Linux | macOS | Windows |
|---|---|---|---|
| 服务管理器 | systemd | launchd | SCM |
| agent 路径 | `/usr/local/bin/monitor-agent` | `/usr/local/bin/monitor-agent` | `C:\service-watch\monitor-agent.exe` |
| 配置路径 | `/etc/service-watch/config.toml` | `/etc/service-watch/config.toml` | `C:\service-watch\config.toml` |
| 数据目录 | `/var/lib/service-watch` | `/var/lib/service-watch` | `C:\service-watch\data` |
| 日志 | journald | `/var/log/service-watch/` | `C:\service-watch\data\agent.log`(服务模式) |
| 运行账户 | 模板 `svcwatch` | 模板 root(可配 UserName) | LocalSystem(可配专用账户) |

每个 agent 实例使用独立的数据目录和控制端口;运行账户需要对数据目录的读写权限,对配置、目标可执行文件和 pidfile 的读权限。

### 13.2 Linux(systemd)

```bash
getent passwd svcwatch || sudo useradd --system --user-group \
  --home-dir /var/lib/service-watch --shell /usr/sbin/nologin svcwatch

sudo install -m 0755 target/release/monitor-agent /usr/local/bin/monitor-agent
sudo install -m 0755 target/release/monitorctl /usr/local/bin/monitorctl
sudo install -d -m 0750 -o root -g svcwatch /etc/service-watch
sudo install -d -m 0750 -o svcwatch -g svcwatch /var/lib/service-watch
sudo install -m 0640 -o root -g svcwatch service-watch.toml /etc/service-watch/config.toml

sudo -u svcwatch /usr/local/bin/monitor-agent --check-config /etc/service-watch/config.toml
sudo install -m 0644 docs/deploy/systemd/service-watch.service /etc/systemd/system/service-watch.service
sudo systemctl daemon-reload && sudo systemctl enable --now service-watch
curl -fsS http://127.0.0.1:9170/ready && monitorctl status
```

模板要点:`ProtectSystem=strict` 等加固(目标需写额外路径时 `systemctl edit` 加 `ReadWritePaths`);`KillMode=process` 保留被监管子进程;`SuccessExitStatus=130` 将信号停机视为成功。

### 13.3 macOS(launchd)

```bash
sudo install -m 0755 target/release/monitor-agent /usr/local/bin/monitor-agent
sudo install -m 0755 target/release/monitorctl /usr/local/bin/monitorctl
sudo mkdir -p /etc/service-watch /var/lib/service-watch /var/log/service-watch
sudo install -m 0640 -o root -g wheel service-watch.toml /etc/service-watch/config.toml

plutil -lint docs/deploy/launchd/com.houseme.service-watch.plist
sudo install -m 0644 -o root -g wheel \
  docs/deploy/launchd/com.houseme.service-watch.plist \
  /Library/LaunchDaemons/com.houseme.service-watch.plist
sudo launchctl bootstrap system /Library/LaunchDaemons/com.houseme.service-watch.plist
sudo launchctl print system/com.houseme.service-watch
```

停止 `sudo launchctl bootout system/com.houseme.service-watch`;日志在 `/var/log/service-watch/`。

### 13.4 Windows(SCM)

管理员 PowerShell;agent 需以 `--features windows-service` 构建:

```powershell
New-Item -ItemType Directory -Force -Path C:\service-watch\data | Out-Null
Copy-Item target\release\monitor-agent.exe C:\service-watch\monitor-agent.exe
Copy-Item target\release\monitorctl.exe C:\service-watch\monitorctl.exe
Copy-Item service-watch.toml C:\service-watch\config.toml

& C:\service-watch\monitor-agent.exe --check-config C:\service-watch\config.toml
.\docs\deploy\windows\install-service.ps1     # 可用 InstallDir/ConfigPath/DataDir/ServiceName 参数
Get-Service service-watch
Invoke-RestMethod http://127.0.0.1:9170/ready
```

`Stop-Service` / `Start-Service` 经 SCM handler 停启;脚本配置失败恢复动作与 `failureflag 1`。SCM 管理 agent 本身;既有 Windows 系统服务可用 watch 监控,或 `[[service]] windows_service = "<服务名>"`(feature `scm-target`)走 SCM API 启停。

### 13.5 agent 退出码

`0` 正常;`1` 配置错误;`2` 运行/关键持久化错误;`130` SIGINT/SIGTERM 优雅停机(systemd unit 视为成功)。

---

## 14. 日常运维

### 14.1 速查卡

| 我要…… | 命令 |
|---|---|
| 看全局状态 | `monitorctl status`(JSON: `--json`) |
| 看指标计数 | `monitorctl metrics`(`--prom`) |
| 手动拉活 | `monitorctl restart <target>` |
| 修完立即验证 | `monitorctl check <target>` |
| 解除 Fatal | `monitorctl reset <target>` |
| 部署前降噪 | `monitorctl silence <t> --duration 1800 --reason "deploy"` |
| 本机采集自检 | `monitorctl dump-snapshot` |
| 事件时间线 | `jq -c . <data_dir>/journal.jsonl \| less` 或 `monitorctl journal --tail 50` |
| 浅存活 / 就绪 | `curl -fsS http://127.0.0.1:9170/health` / `.../ready` |
| 生效配置核对 | `monitorctl config`(`--json`) |

### 14.2 agent 重启与孤儿接管

agent 退出时保留 supervise 子进程;重启后读取 journal 身份记录:精确确认旧身份存活 → 状态呈现 attach,继续只读监控;确认消失 → 按当前配置与预算恢复 supervise。Fatal 闩锁跨重启保持,解除仍用 reset。接管期间进程操作由原有外部管理方式处理。

重启后的行为预期(F25):

- 仍 open 的事故恢复进引擎并**各重通知一次**(`reason` 带 `restored from journal:` 前缀)——这是特性,让运维感知"重启时仍有未闭合故障"。
- `/metrics` 的 `checks_*` 与 target `health` fresh 起步,随 live 探测在数个 interval 内回到真实水位;不要把重启后头几个 interval 的读数当"故障已恢复"——以 `incident` 字段与通知为准。

### 14.3 容量与性能预算

- agent 自身 CPU 验收线:< 2% 单核(5s 采集间隔;间隔调大近似线性下降)。
- 内存:24h 长跑完成门 PASS(2026-11-17):RSS 稳态增长 176KB,常驻数十 MB 量级。
- journal 只记事件,安静系统接近零增长;单行约 150–300B。100 check × 10 incident/日 ≈ 2–5 MB/日。
- 快速验收:`bash docs/deploy/perf-check.sh ./target/release/monitor-agent 60`(独立端口 19190 + 临时数据目录);24h 复验:`bash docs/deploy/soak-24h.sh`。
- 日志轮转配方(logrotate copytruncate / newsyslog 局限与 copy+truncate 替代 / Windows 计划任务)见 [RUNBOOK §6.3](../RUNBOOK.md)。

---

## 15. 故障排查

### 15.1 症状决策树(摘要)

```text
【A】agent 不监听(monitorctl 退出 4)
├─ 守护进程在吗?systemctl status / launchctl print / sc query
│  ├─ 起不来:看 journalctl / agent.err.log
│  │   ├─ "config error:" + 退出 1     → --check-config 修配置
│  │   ├─ Start limit hit              → systemd reset-failed 后再起
│  │   └─ Address already in use(退 2)→ 查 9170 占用,改 [control].listen
│  └─ 在跑但连不上:
│      ├─ [control].enabled = false? / listen 与 --url 不一致?
│      ├─ HTTPS/mTLS 失败              → 核对信任链与 --client-cert/--client-key,查证书有效期
│      └─ HTTP 401                     → 加 --token 或设 SERVICE_WATCH_TOKEN

【B】check 全面 Critical
├─ 目标真挂了?curl/nc 直连;挂了 → supervise 拉活,attach 只告警(设计)
└─ 目标活着但判死:网络路径(代理/DNS/防火墙);probe_skipped 看 interval 是否过紧;
   休眠唤醒首轮丢弃 → 等下一轮

【C】supervise 目标反复重启
├─ status:recovery=crashloop?restarts_in_window 逼近 max_restarts?
├─ journal restart 事件 fast_exit 分布:true 为主 → 崩溃循环看目标自身日志;
│  false 为主 → 运行一段时间才死(资源/上游依赖/被外部杀)
├─ spawn 失败(二进制缺失/端口占用/workdir 权限)→ journal audit + ERROR
└─ 预算耗尽 → Fatal → §11.4 处置流程

【D】webhook 不达
├─ alert_delivery_failures 在涨 → 看日志 delivery error(DNS/证书/出口网络/端点 4xx 5xx)
├─ alerts_dropped 在涨 → 端点太慢,先修延迟(事件仍在 journal)
└─ 都为 0 但没通知 → 通知抑制(renotify/flap/静默/维护窗口);journal 有 incident 吗?

【E】agent 自身健康
├─ journal 写失败 → data_dir 权限、磁盘空间;关键写入失败实例退出 2
├─ degradations_events 突增 → 采集器初始化/温度异常,看 degraded 字段
└─ 内存/CPU 异常 → 对照 uptime_secs 与重启次数
```

### 15.2 常见问题速查

| 现象 | 核对与处理 |
|---|---|
| agent 启动即退出 | 用实际服务账户执行 check-config,看服务日志与退出码 |
| 9170 被占用 | 查占用进程,调整 control.listen 后同步 monitorctl --url |
| monitorctl 退出 4 | agent 是否在跑、控制端口、HTTPS/mTLS 证书信任链 |
| 配置通过但应用起不来 | 核对 command/args/workdir/账户权限;**目标禁止 daemonize 型**(双 fork 会被误判死亡反复拉起) |
| systemd 下目标写文件失败 | 目录属主、ReadWritePaths、继承的服务隔离设置 |
| journal 写失败 | 磁盘空间、数据目录权限;关键确认超时会停机,恢复后先检查 journal 再启动 |
| status 长期 attach | 旧进程身份、采集权限、进程存在性证据 |
| Fatal 后未恢复 | 先处理实际故障,再 reset 并观察后续状态 |
| webhook 不达 | URL/格式/网络 + alert_delivery_failures;查 spool 与 journal |
| 温度/风扇/电压为空 | 看 `hardware.degradations` 与降级原因;确认机型接口、权限与构建 feature |
| 鉴权开启后浏览器打不开状态页 | 页内 JS 无法带 token;经反向代理注入头或对浏览器放开网络限制 |
| 重启后收到 "restored from journal" 通知 | 特性:重启时仍有未闭合事故的恢复重通知(F25) |

---

## 16. 升级、回滚与卸载

升级前记录版本、状态和配置;停止 agent;**备份完整数据目录(journal + spool)、配置及旧二进制**:

```bash
monitor-agent --version
monitorctl status --json > status-before.json
sudo systemctl stop service-watch
sudo tar -czf service-watch-backup.tgz -C / var/lib/service-watch etc/service-watch
```

(macOS 停止:`sudo launchctl bootout system/com.houseme.service-watch`;Windows:`Stop-Service service-watch`。)

替换二进制后重新校验配置、启动服务,依次确认 `/ready`、目标状态、journal 错误计数与告警投递。Unix 可先写同目录临时文件再 `mv` 切换,保留旧二进制用于回滚。

回滚:先核对目标版本的配置与 journal schema 兼容性;恢复历史 journal 会回退预算、Fatal 与身份状态,应结合目标当前 PID 与启动时间完成对账。

卸载:停止并禁用系统服务 → 处理保留运行的应用进程 → 按保留期限归档配置、journal、spool 与日志。

---

## 17. 附录

### 17.1 monitor-agent 命令行

| 命令 | 说明 |
|---|---|
| `monitor-agent [--config <path>] [--log-json]` | 主循环(默认读 `./service-watch.toml`;顶层 flag 形态,无 run 子命令) |
| `monitor-agent --dump-snapshot [--config <path>]` | 采集一次输出 JSON 后退出 |
| `monitor-agent --check-config <path>` | 离线校验配置 |
| `monitor-agent --version` | 版本 |
| `monitor-agent --service-run`(Windows,`windows-service` feature) | SCM 服务入口 |
| `--log-file <path>` | 服务模式文件日志 |

### 17.2 统一退出码

| 码 | 含义(agent / monitorctl 通用语义) |
|---|---|
| 0 | 成功 |
| 1 | 配置错误 |
| 2 | 运行错误(agent:含关键持久化错误;monitorctl:含 409/非 2xx) |
| 3 | 目标不存在(monitorctl 保留码,基本不出现) |
| 4 | agent 不可达(monitorctl) |
| 130 | SIGINT/SIGTERM 优雅停机(agent) |

### 17.3 数据文件一览

| 文件 | 内容 |
|---|---|
| `{data_dir}/journal.jsonl` | 事件日志(append-only JSONL;恢复域事实源) |
| `{data_dir}/journal.1.jsonl` | journal 内置轮转的上一代(可随时归档/删除) |
| `{data_dir}/spool/pending.jsonl` | 投递失败事件补投队列(成功确认后删除,7 天过期) |
| `[metrics] history_path` 指向的文件 | 指标历史 JSONL(`.1` 为轮转上一代) |

### 17.4 文档地图

| 文档 | 内容 |
|---|---|
| [DESIGN.md](../../DESIGN.md) | 语义权威:配置 schema、所有权、状态机、平台契约 |
| [docs/DEPLOYMENT.md](../DEPLOYMENT.md) | 完整部署与操作手册 |
| [docs/RUNBOOK.md](../RUNBOOK.md) | 运维手册:journal/jq、告警语义、决策树、容量预算、轮转 |
| [docs/DEPLOY-LINUX/-MACOS/-WINDOWS.md](../DEPLOYMENT.md) | 分平台安装 |
| [docs/deploy/](../deploy/README.md) | systemd/launchd/Windows 模板与性能脚本 |
| [ROADMAP.md](../../ROADMAP.md) | 功能规范、验收与进度 |
| [CHANGELOG.md](../../CHANGELOG.md) | 变更日志 |
| [developer-guide.zh.md](developer-guide.zh.md) | 开发者指南(本仓库文档 v1) |

---

*本文档基于 2026-11-22 的 main 分支实现整理;语义如与 [DESIGN.md](../../DESIGN.md) 冲突,以 DESIGN.md 为准。*
