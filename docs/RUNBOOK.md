# service-watch RUNBOOK(运维手册)

> 适用版本:v0.1.x。语义以 [DESIGN.md](../DESIGN.md) 为准 (§7 检查/告警、
> §8 恢复、§9 CLI/控制平面、§10 可观测性);部署入口见
> [docs/deploy/README.md](deploy/README.md)。
>
> 约定：示例中 agent 控制平面为默认 `http://127.0.0.1:9170`;journal 位于
> `<data_dir>/journal.jsonl`。程序缺省 `data_dir` 为 `./data`，部署模板通常使用
> `/var/lib/service-watch/` 或 `C:\service-watch\data\`。所有时长判定用单调时钟，事件时间戳
> (`at`) 为墙钟 (serde 序列化为 `{"secs_since_epoch":u64,"nanos_since_epoch":u32}`)。

## 0. 速查卡

| 我要…… | 命令 |
|---|---|
| 看全局状态 | `monitorctl status`(JSON: `--json`) |
| 看指标计数 | `monitorctl metrics`(`--prom` 得 Prometheus 文本) |
| 手动拉活 | `monitorctl restart <target>` |
| 修完立即验证探测 | `monitorctl check <target>`(绑定检查立即到期) |
| 解除 Fatal 闩锁 | `monitorctl reset <target>` |
| 本机采集自检 (不经 agent) | `monitorctl dump-snapshot` |
| 事件时间线 | `jq -c . <data_dir>/journal.jsonl | less` |
| agent 浅存活 | `curl -fsS http://127.0.0.1:9170/health` |
| agent 就绪判定 | `curl -fsS http://127.0.0.1:9170/ready` |

## 1. 日常操作

### 1.1 monitorctl 命令语义

`monitorctl` 是控制平面的薄 HTTP 客户端 (DESIGN §9.2)。全局参数
`--url <URL>`(默认 `http://127.0.0.1:9170`),子命令：

| 子命令 | HTTP | 语义 | 典型输出/退出码 |
|---|---|---|---|
| `status [--json]` | `GET /status` | 全 target 状态与 incident 摘要 | 表格/原样 JSON;失败 2/4 |
| `restart <target>` | `POST /v1/restart/{target}` | 手动重启 supervise 目标 | `accepted (202)` = 已入队;**Fatal 态被拒** |
| `check <target>` | `POST /v1/check/{target}` | 绑定检查立即到期 (下一轮调度立即执行) | `accepted (202)`;用于改配置/修复后的即时验证 |
| `reset <target>` | `POST /v1/reset/{target}` | 解除 Fatal 闩锁 + 清空重启预算 (写审计) | `accepted (202)`;journal 增 `reset` 事件 |
| `metrics [--prom]` | `GET /metrics` | 运行指标 (JSON 默认;`--prom` 走 `?format=prometheus`) | 文本/JSON;失败 2/4 |
| `dump-snapshot [--config <path>]` | (本地) | 单次采集 JSON,**不经 agent**,用于验证采集层 | 退出码 0/1(配置)/2(采集失败) |

monitorctl 退出码 (DESIGN §9.3):`0` 成功;`2` 运行错误 (非 2xx 响应，含 409);
`3` 目标不存在 (保留码，见 §1.2);`4` agent 不可达 (连接失败)。脚本化巡检请
按退出码分支，不要解析文案。

### 1.2 v0.1 控制平面实现注意 (与 DESIGN §9.2 的差异，如实记录)

- 控制端点**入队即 202**(命令通道容量 16，满则 503 —— DESIGN §2.2 背压),
  **不校验 target 是否存在**:`restart/check/reset` 打错名字也会 `accepted`,
  无效目标由 eval 侧拒绝 (WARN 日志 + journal 审计)。操作后请以
  `monitorctl status` / journal 确认效果，退出码 3(目标不存在) 在 v0.1 基本不会出现。
- 「Fatal 态 restart → HTTP 409」已接线 (DESIGN §9.2,2026-11-11):control 平面
  经 status watch 判定，Fatal 目标**直接返回 409** + 提示先 reset;非 Fatal 一律
  202 入队 (eval 侧保留防御性拒绝 + journal 审计)。monitorctl 收到 409 时输出
  "run `monitorctl reset <target>` first" 并以退出码 2 结束。
- attach 目标永不杀/重启 (DESIGN §1 原则、§8.4):对 attach 目标的 `restart`
  请求同样由 eval 侧拒绝并留审计。
- 命令通道满 (`503`)= agent 过载或停机中;先查 `/health` 与守护进程状态。

### 1.2b 值班跟随 (watch)

```bash
monitorctl watch --interval 2                       # 全屏跟随,变化打 Δ 历史
monitorctl watch --json | jq 'select(.targets[] | .health == "critical")'   # 管道消费
monitorctl watch --exit-on-error                    # 脚本/短生命周期调用(不可达即退 4)
```

## 1.2c 指标周期推送 ([metrics])

```toml
[metrics]
enabled = true
url = "http://collector:9090/ingest"    # POST MetricsReport JSON
interval = "60s"                        # timeout < interval(校验强制)
timeout = "10s"
```

语义：单次尝试、失败即放弃 (下一周期自然是最新值，at-least-once 无意义);
连败 1 次 / 达到 3 次 / 每 10 连败写 journal 审计 (粘性边沿，不刷屏),恢复首轮亦审计。
抓取器更推荐拉模式：`GET /metrics?format=prometheus`。

## 1.3 Fatal 闩锁处置流程

**为什么会有 Fatal**:supervise 目标在 `restart_window`(默认 10m) 滑动窗口内
自动重启达到 `max_restarts`(默认 5) 即进入 Fatal;**所有自动重启均计数**
(含 spawn 失败与慢死亡，防"每 11 分钟死一次"绕过预算);graceful→强杀升级后
`ForceKill` 失败 (如 Unix D 状态) 也直接 Fatal。Fatal 是**终态闩锁**:

- 不自动恢复 (窗口滑过、agent 重启都不恢复;DESIGN §8.2);
- agent 重启后从 journal 重放仍保持 Fatal，且**跳过冷启动 spawn**——agent 不得
  越权复活被人工闩锁的目标 (DESIGN §8.7);
- 唯一出口 = `monitorctl reset`(人工，写审计)。

处置步骤 (每步都别跳):

```bash
# 1) 发现:status 中 recovery=fatal;通常伴随 Critical 告警
monitorctl status

# 2) 查 journal:Fatal 原因与重启时间线(reason 通常是 restart budget 耗尽或 ForceKill 失败)
jq -c 'select(.type=="fatal" or ((.type=="restart") and .target=="<target>"))' \
   /var/lib/service-watch/journal.jsonl | tail -30

# 3) 定位根因(常见):依赖未起 / 二进制或配置错 / 端口被占 / OOM / 路径权限
#    fast_exit=true 的重启(存活 < min_uptime)指向崩溃循环;见 §5 决策树

# 4) 修根因 —— reset 只解除闩锁,不修任何东西;不修根因 reset 后只会再次 Fatal

# 5) 解除闩锁(写审计,清空 Fatal + 重启预算)
monitorctl reset <target>

# 6) 立即验证:check 让绑定探测立即到期;观察恢复状态机推进
monitorctl check <target>
monitorctl status        # 期望 recovery: starting → running(startup_grace 后)
```

审计与顺序语义:journal 按**行顺序**重放——同 target 的 `Reset` 清除其之前的
Fatal 与重启计数素材，其后新的 `Fatal`/`Restart` 照常生效。因此**不要**手工编辑
journal 来"洗掉"Fatal(会破坏重放顺序语义);历史教训用 `journal.jsonl.bak-*`
整体备份。

### 1.4 配置变更如何生效

先离线校验实际启动配置，再提交 reload。

```bash
monitor-agent --check-config /etc/service-watch/config.toml
curl -fsS -X POST http://127.0.0.1:9170/v1/reload
# Unix 也可向 agent 发送 SIGHUP
```

`POST /v1/reload` 缺省重读 agent 启动时的配置路径，也可提供 JSON body `{"path":"/path/to/config.toml"}`。202 表示已入队，解析、校验与应用结果通过 journal 审计和 `monitorctl status` 确认。

- check、service、watch 增删改通过 reload 生效。check 保留健康水位，service 命令重定义先停止并收割旧实例，再启动新定义。删除与重定义前持久化 Reset，清除旧定义的 Fatal 和预算。
- agent、log、control、collect、alert、metrics 段变更需要重启，reload 会记录审计。
- 无效配置整体拒绝。未满足依赖的服务继续等待，Fatal 目标遵循闩锁语义。

```bash
sudo systemctl restart service-watch
# macOS 使用 launchctl，Windows 使用 Restart-Service
```

## 2. journal.jsonl:结构与查询

### 2.1 事件 schema

append-only JSONL，由独立阻塞写任务处理。Restart、Fatal、Reset、批量 ResetTargets 与进程身份事件等待 sync_data 确认，2 秒内未确认或写入失败会停止当前恢复实例并退出 2。普通 Incident/Audit 队列拥塞时计数丢弃。旧 journal 读取失败会拒绝启动，应保留原件并修复存储或损坏记录。

```jsonc
// 1. incident:检查/告警状态机转移(含 flap 抑制摘要)
{"type":"incident","at":{"secs_since_epoch":1731...,"nanos_since_epoch":0},
 "incident":1234567890,          // incident_key = hash(target_name, check_name),不含 severity/reason
 "target":"my-app","check":"api-health",
 "category":"alert",             // alert | escalate | resolve | flap(小写)
 "severity":"warning",           // warning | critical(小写)
 "reason":"HTTP probe failed: connect timeout"}

// 2. restart:supervise 目标的自动重启(全部自动重启均入账)
{"type":"restart","at":{...},"target":"my-app",
 "fast_exit":true}               // 存活 < min_uptime,crash loop 嫌疑(信息用;计数不分快慢)

// 3. fatal:重启预算耗尽或 ForceKill 失败(终态闩锁)
{"type":"fatal","at":{...},"target":"my-app",
 "reason":"restart budget exhausted: 5 restarts in 10m window"}

// 4. reset:人工解除闩锁(monitorctl reset;source 标识来源,
//    v0.1 控制面命令统一记 "control",不区分调用方)
{"type":"reset","at":{...},"target":"my-app","source":"control"}

// 5. audit:其他审计动作(无效控制命令拒绝、ForceKill 失败等)
{"type":"audit","at":{...},"message":"restart rejected: target my-app is in fatal state"}

// 6. reset_targets:reload 删除/重定义的批量预算重置
{"type":"reset_targets","at":{...},"targets":["my-app"],"source":"reload"}

// 7. process_identity:孤儿对账素材，identity=null 表示旧实例已消失
{"type":"process_identity","at":{...},"target":"my-app",
 "identity":{"pid":1234,"start_time":1731...},"verified":true}
```

### 2.2 常用 jq 查询

```bash
J=/var/lib/service-watch/journal.jsonl

# —— 今日/近期 Fatal 列表 ——
jq -r 'select(.type=="fatal")
       | "\(.at.secs_since_epoch | todate)\t\(.target)\t\(.reason)"' "$J"   # 全量,UTC 时间

# 最近 24h:
jq -c 'select(.type=="fatal" and .at.secs_since_epoch >= now - 86400)' "$J"

# "今天 00:00" 起(本地时区,先算零点):
T0=$(date -v0H -v0M -v0S +%s)          # macOS/BSD;Linux(GNU)用: date -d 'today 00:00' +%s
jq --argjson t0 "$T0" -c \
   'select(.type=="fatal" and .at.secs_since_epoch >= $t0)' "$J"

# —— 重启频率 top target(次数 + 其中 fast_exit 占比)——
jq -r 'select(.type=="restart") | .target' "$J" | sort | uniq -c | sort -rn | head -10

# 明细版:次数 / fast_exit 次数 / 最近一次重启时间
jq -s 'map(select(.type=="restart"))
       | group_by(.target)
       | map({target: .[0].target,
              restarts: length,
              fast_exits: (map(select(.fast_exit)) | length),
              last: (map(.at.secs_since_epoch) | max)})
       | sort_by(-.restarts)' "$J"

# —— flap:被抑制的 flap 摘要事件(直接证据)——
jq -c 'select(.type=="incident" and .category=="flap")' "$J"

# 疑似 flap(间接证据):最近 30m 内同 incident 的 open/close 转移 >= flap_threshold(5)
jq -s --argjson since "$(( $(date +%s) - 1800 ))" '
  [ .[] | select(.type=="incident")
        | select(.category=="alert" or .category=="resolve")
        | select(.at.secs_since_epoch >= $since)
        | .incident ]
  | group_by(.) | map({incident: .[0], transitions: length})
  | sort_by(-.transitions) | .[] | select(.transitions >= 5)' "$J"

# —— reset 审计(谁在什么时候解除过闩锁)——
jq -r 'select(.type=="reset")
       | "\(.at.secs_since_epoch | todate)\t\(.target)\t\(.source)"' "$J"

# —— 校验文件完整性(坏行会在此暴露;重放会跳过它们)——
jq -c . "$J" > /dev/null || echo "存在坏行,见 deploy/README.md §5.3"
```

注：`jq -s` 会整文件载入内存，常规事件量 (见 §6.2) 完全无压力;若 journal 已
数百 MB，先轮转 (§6.3) 再分析。

## 3. 告警语义与处置

### 3.1 incident 模型 (DESIGN §7.2，权威)

- `incident_key = hash(target_name, check_name)`,**不含 severity/reason**:
  同一目标同一检查永远同 key,Warning→Critical 升级是**同一起事故的 escalate**,
  不产生"第二起事故"。
- 生命周期：`open(Warning, 事件 category=alert) → escalate(Critical) → resolve(Ok)`;
  状态机:`1 次失败 → Warning;连续 failure_threshold(默认 3) 次 → Critical;
  连续 success_threshold(默认 2) 次成功 → Ok`(Warning→Ok 同样发 Resolve)。

### 3.2 通知抑制 (为什么"没收到重复告警"通常是特性不是故障)

| 规则 | 默认值 | 含义 |
|---|---|---|
| 首条通知永不抑制 | — | incident 第一条告警必达 (杜绝 Resolved 后 15 分钟内复发被冷却吞掉的盲区) |
| `renotify_interval` | 15m | incident 持续期间，同 severity 的重复通知按此间隔重发;**Escalate(Warning→Critical) 总是发送** |
| `flap_window` / `flap_threshold` | 30m / 5 | 窗口内 open/close 次数达标 → 进入抑制态，只发一条 `Flap` 摘要 (含计数);抑制期结束自动退出 |

判定用单调时钟：宿主机墙钟回拨不会造成"永久抑制"或"狂轰滥炸"(DESIGN 偏差表)。
抑制只作用于通知，**journal 的 incident 事件照记不误**——"没收到通知但 journal
有事件"先看本表，再看 §3.4 投递故障。

### 3.3 Resolve 语义

- `Resolve` 引用原 incident 且**总是发送**(前提：该 incident 曾发出过通知);
- 从未发过通知的 incident 恢复时不发 Resolve(没有收件上下文，避免噪音);
- `Resolved` 后立刻复发能再次告警 (首条永不抑制);webhook 侧请按
  `category=resolve` 收敛告警面板，而不是按 severity 猜。

### 3.4 webhook 投递故障

- 每笔投递带 `alert.timeout`(默认 10s) 与 `retries` 次指数退避 (1s 起步 ×2,
  封顶 10s);最终失败记 ERROR + `alert_delivery_failures` 计数。
- 有界队列 (256) 满 → **丢最旧 + WARN 审计** + `alerts_dropped` 计数 (告警时效
  优先于完整性;持久化补投在 v0.2 F12)。观察到 dropped 增长 = 事件生产速率
  超过投递能力，先解决 webhook 端点变慢，而不是调大队列。
- `deadman_url`(可选) 周期心跳 POST，失败仅记日志——"agent 活着但失聪 (webhook
  通道挂了)"由外部心跳监控兜底，生产建议配置。
- `webhook_format=slack` 需要 Slack/Discord 兼容端点 (`{"text": ...}`);通用
  JSON 格式投递 AlertEvent 原样 (字段见 DESIGN §5)。

## 4. 健康检查端点 (DESIGN §9.2)

| 端点 | 成功 | 失败 | 字段说明 |
|---|---|---|---|
| `GET /health` | 200 空体 | 连接拒绝=进程死 | **浅存活**:HTTP 栈可达即 200，不反映配置/调度状态 |
| `GET /ready` | 200 `{"ready":true}` | 503 `{"ready":false}` | 配置加载成功 + 调度循环已启动;**降级采集不影响 ready**(温度缺失走 Degradation，属正常降级) |
| `GET /status` | 200 StatusReport | — | 见下 |
| `GET /metrics` | 200 JSON | — | `?format=prometheus` 或 `Accept: text/plain` 得 Prometheus 文本 (前缀 `service_watch_`) |

`/status` 结构：

```jsonc
{"started_at": {...}, "ready": true,
 "targets": [{
   "name": "my-app",
   "mode": "supervise",              // supervise | attach
   "health": "ok",                   // ok | warning | critical(检查视角聚合)
   "recovery": "running",            // null | running | suspect | recovering | force_kill
                                     //      | restarting | starting | crashloop | fatal
                                     //      (DESIGN §8.2 恢复状态机,小写)
   "checks": ["api-health"],         // 绑定的检查名
   "identity": {"pid": 4242, "start_time": 1731...},  // (pid, start_time) 二元组,DESIGN §8.3
   "incident": null,                 // incident 级别(ok|warning|critical)
   "restarts_in_window": 0,          // 当前滑动窗口内自动重启计数(逼近 max_restarts 即告警)
   "last_transition": {...}
 }]}
```

`/metrics` 字段 (JSON):

| 分组 | 字段 | 含义 |
|---|---|---|
| 告警管道 | `alerts_sent` / `alert_delivery_failures` / `alerts_dropped` | 已发通知数 / 投递最终失败数 / 队列满丢最旧数 |
| journal | `journal_dropped` / `journal_write_failures` / `journal_write_successes` / `journal_critical_failures` | 普通事件入队丢弃 / 写失败 / 写成功 / 关键确认失败 |
| 探测 | `probe_executed` / `probe_skipped` | 已执行 / **在途上限 1 被跳过**的探测数 (skipped 持续增长=interval 过紧或目标过慢) |
| 恢复 | `restarts_total` / `state_transitions_total` / `fatal_targets` | 当前进程成功 spawn 累计，含冷启动 / 状态机转移总数 / 当前 Fatal 目标数 |
| 采集 | `degradations_current` / `degradations_events` / `last_collect_at` | 当前快照降级条目数 (瞬时) / 降级跳变次数 / 最近采集时间 (None=采集器未出活) |
| 概览 | `targets_total` / `checks_ok|warning|critical` / `services_total` / `services_running` / `recovery_running|suspect|recovering|force_kill|restarting|starting|crashloop|fatal` / `uptime_secs` | 面板/巡检直接取用 |

Prometheus 文本 (`--prom` / `?format=prometheus`) 与 JSON 字段同名 (前缀
`service_watch_`),唯一例外：`last_collect_at` 在文本格式中输出为
`service_watch_last_collect_timestamp_seconds`(epoch 秒，未采集时输出 0)。

运维基线告警建议：`alert_delivery_failures` 增长、`alerts_dropped` 增长、
`probe_skipped` 持续增长、`recovery_fatal > 0`、`degradations_events` 突增、
`last_collect_at` 停滞 (采集线程卡死)。

## 5. 故障排查决策树

```text
【A】agent 不监听 9170(monitorctl 退出码 4)
├─ 守护进程在吗？systemctl status / launchctl print / sc query
│  ├─ 起不来 (journalctl / agent.err.log):
│  │   ├─ "config error:" + 退出码 1        → monitor-agent --check-config 修配置
│  │   ├─ Start limit hit(systemd)         → §deploy FAQ 5.4;reset-failed 后再起
│  │   └─ "Address already in use"(退出码 2)→ lsof/netstat 查 9170，释放或改 [control].listen
│  └─ 在跑但连不上：
│      ├─ [control].enabled = false?         → 开启并重启 agent
│      ├─ listen 与 monitorctl --url 不一致  → 对齐
│      └─ 防火墙 (仅当 listen 改为非回环)    → 收回环或按需放行 (v0.1 无鉴权，慎改)
│
【B】check 全面 Critical
├─ 目标真挂了？curl / nc 直连目标;挂了 → supervise 由恢复状态机接管，attach 只告警 (设计)
├─ 目标活着但 agent 判死：
│   ├─ agent 到目标的网络路径 (代理/DNS/防火墙)—— agent 不走系统代理的话确认直连
│   ├─ timeout < interval 校验通过吗 (配置层保证);interval 过紧看 /metrics probe_skipped
│   └─ 宿主机休眠唤醒后首轮探测丢弃并记 degraded(DESIGN §7.1)→ 等下一轮自然恢复
│
【C】supervise 目标反复重启
├─ monitorctl status:recovery=crashloop / restarts_in_window 逼近 max_restarts?
├─ journal:type=restart 的 fast_exit 分布 (§2.2 查询)
│   ├─ fast_exit=true 为主 → 崩溃循环：看被监管进程自身日志 (依赖/配置/权限/端口)
│   └─ fast_exit=false 为主 → 运行一段时间才死：资源 (OOM)、上游依赖、被外部杀
├─ spawn 失败 (二进制缺失/端口占用/workdir 权限)→ journal audit + ERROR 日志
│   注意:agent 禁止 daemonize 型目标 (DESIGN §8.4)——双 fork 目标会被误判立即死亡
│   而重复拉起，务必以前台进程形态配置 [[service]].command
└─ 预算耗尽 → Fatal(终态)→ 走 §1.3 处置流程
│
【D】webhook 不达 (告警面板没动静但目标确有故障)
├─ /metrics:alert_delivery_failures 在涨？→ 看日志 delivery error(超时/退避后失败)
│   ├─ DNS/证书/出口网络 → 常规网络排查
│   └─ 端点 4xx/5xx → webhook_format 与端点是否匹配 (slack 需 {"text":...})
├─ alerts_dropped 在涨？→ 投递能力 < 事件速率 (背压丢最旧)→ 修端点延迟，事件仍在 journal
└─ 两项都 0 但没通知 → 通知抑制？看 §3.2(renotify/flap);journal incident 事件在吗？
│
【E】agent 自身健康
├─ journal 写入失败 (WARN/ERROR "journal:")→ 检查 data_dir 权限、磁盘空间与 journal 错误指标，关键写入失败后实例退出 2
├─ degradations_events 突增 → 采集器初始化失败/温度异常 (DESIGN §6.3 过滤),看 degraded 字段
└─ 内存/CPU 异常 → §6.1 预算核对;uptime_secs 与重启次数对照 (谁在重启 agent)
```

## 6. 容量与性能预算

### 6.1 CPU / 内存

- agent 自身 CPU 验收线：**< 2% 单核**(`collect.interval` 默认 5s;ROADMAP F3)。
  间隔调大 (如 10s/15s)成本近似线性下降;process 检查只做 watched PID 的部分
  刷新 (DESIGN §6.2),不随全机进程数增长。
- 在途探测上限 1 + `timeout < interval` 配置校验 ⇒ 探测不会堆积;调度退化为
  "跳过并计数"(`probe_skipped`),不会雪崩。
- 内存:v0.1 无硬预算 (24h 长跑验证在 v0.2 F17),经验上常驻数十 MB 量级;
  巡检关注 RSS 缓慢增长 + `/metrics` 的 `uptime_secs` 与 `state_transitions_total`
  对照是否同步增长 (排除"重启清零"的假象)。

### 6.2 journal 增长估计

journal **只记事件**(incident 转移/重启/Fatal/Reset/审计),采集快照不入
journal——安静系统接近零增长。

- 单行约 150–300 B(与 target/check/reason 长度相关)。
- 上界感：单 incident 在 renotify=15m 下全天持续告警 ≈ 96 条 renotify +
  转移事件 ≈ 100 条 ≈ 25 KB/日;flap 抑制生效后进一步收敛 (30m 窗口 5 次转移
  后只记 Flap 摘要，事件本身仍入 journal 但不再翻倍)。
- 量级示例:100 个 check、每天都有 10 个 incident → 约 2–5 MB/日 → 年 < 2 GB。
  **建议无论如何配置轮转**(§6.3),并把 `jq -c .` 校验纳入季度巡检。

### 6.3 日志轮转

**Linux — logrotate(推荐 copytruncate)**:agent 与被监管写者均以 O_APPEND
持有 fd,copytruncate 后下一次写入自动回到文件头，无需通知进程。

```text
# /etc/logrotate.d/service-watch
/var/lib/service-watch/journal.jsonl
/var/log/service-watch/*.log
{
    daily
    rotate 14
    maxsize 100M
    copytruncate        # copy 后原地 truncate;copy 与 truncate 之间有极小丢写窗口
    compress            # (事件频率低，窗口内丢失概率可忽略;介意可改停机轮转)
    delaycompress
    missingok
    notifempty
    su svcwatch svcwatch
}
```

**macOS — newsyslog 片段**:

```text
# /etc/newsyslog.d/service-watch.conf
# logfilename                          [owner:group] mode count size when  flags
/var/log/service-watch/agent.log                    644  7  1024 *     J
/var/log/service-watch/agent.err.log                644  7  1024 *     J
# J = bzip2 压缩;7 份;1024 KB 触发;when=* 表示按 size 与 count 判断
```

**注意 (newsyslog 局限，务必知晓)**:newsyslog 默认 rename 式轮转对"进程持有
fd 持续 append"的文件无效——launchd 代持的 `agent.log` 与 agent 自持的
`journal.jsonl` 在 rename 后仍写旧 inode，新文件保持为空 (经典 macOS 坑)。
macOS 上对这三个文件 (journal + 两个日志) 推荐 copy+truncate 定时任务替代：

```bash
#!/bin/zsh
# /usr/local/libexec/service-watch-rotate — O_APPEND 写者安全的轮转
set -euo pipefail
TS=$(date +%Y%m%d%H%M%S)
for f in /var/log/service-watch/agent.log \
         /var/log/service-watch/agent.err.log \
         /var/lib/service-watch/journal.jsonl; do
  [ -s "$f" ] || continue
  d=$(dirname "$f")/archive; mkdir -p "$d"
  cp "$f" "$d/$(basename "$f").$TS"
  : > "$f"                       # truncate;下一次 append 自动从 0 开始
done
# 清理 14 天前的归档
find /var/log/service-watch/archive /var/lib/service-watch/archive \
     -type f -mtime +14 -delete
```

```xml
<!-- /Library/LaunchDaemons/com.houseme.service-watch.rotate.plist(每日 03:30)-->
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>com.houseme.service-watch.rotate</string>
  <key>ProgramArguments</key>
  <array><string>/usr/local/libexec/service-watch-rotate</string></array>
  <key>StartCalendarInterval</key>
  <dict><key>Hour</key><integer>3</integer><key>Minute</key><integer>30</integer></dict>
</dict></plist>
```

```bash
sudo chmod 755 /usr/local/libexec/service-watch-rotate
sudo chown root:wheel /Library/LaunchDaemons/com.houseme.service-watch.rotate.plist
sudo launchctl bootstrap system /Library/LaunchDaemons/com.houseme.service-watch.rotate.plist
```

copy+truncate 同样存在 copy 与 truncate 之间的极小竞态窗口 (同时发生的写入会被
truncate 吞掉，但通常已在 copy 之前落盘的部分保留);对事件级 journal 可接受，
要求严格不丢则改为"停 agent → 移动 → 起 agent"窗口式轮转。

**Windows**:journal 与服务模式文件日志用计划任务 + 同款 copy+truncate
PowerShell 片段;v0.1 stdout 无处落盘 (见 deploy/windows/README §6),无日志
轮转需求。


## 7. 孤儿对账与持久化故障

agent 重启后先核对 journal 记录的进程身份。确认存活时 `/status` 显示 attach，继续检查与告警，禁止控制面 restart、激活和停止操作。已闩锁的 Fatal 仍保持可见。只有确认旧身份消失后才按当前配置恢复 supervise，自动恢复消耗预算并遵循 backoff。

初始化降级、权限不足或未验证身份与观测值漂移时，对账持续等待。先恢复采集权限并核对进程状态，避免另起同名实例。旧版 journal 未保存身份，以及 spawn 成功到首次身份确认前发生崩溃的场景，仍需要运维核查遗留进程。

一个 data_dir 由一个 agent 实例使用。同一进程内的 spool 事务按目录互斥，重放收到成功确认后才移除，取消时保留原件。普通内存队列与尚未落盘事件仍可能在强杀时丢失。磁盘真实卡死中的阻塞系统调用无法由 Rust 任务取消，关键确认超时会停止调度，存储恢复后应先检查 journal 再重新启动。
