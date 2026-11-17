# Changelog

All notable changes to this project will be documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/); versioning: 0.x 阶段 minor=功能,y.z=修复。

## [Unreleased]

### 2026-11-12 多专家复核与继续实现

- F21 可选深度硬件采集，Linux hwmon 与 macOS AppleSMC 风扇、电压，Windows 显式降级，支持两个 CLI 的快照入口。
- 补齐 journal 进程身份记录与孤儿只读接管，单调采样时间和 requested 集合共同约束证据，消失后的恢复纳入预算及退避。
- 修复服务重定义覆盖旧 Child/Job、重复 spawn、依赖门控、软策略未同步 FSM、同名 service/watch 错配和独立 PID 检查缺采集。
- 修复采集降级、指标缺失和重复旧快照误触发恢复，限制亲生同 PID 身份采纳并同步 FSM，reset 保留活进程身份。
- 修复预算 journal 重复计数与 activate 越限，关键状态等待 sync_data 确认，2 秒确认失败停止当前实例，暴露 journal 成功、失败和丢弃指标。
- 修复启动 bind 失败任务泄漏、分发子任务取消、初始化失败不重试，以及 journal/spool 半尾行恢复。
- 修复 spool 确认前删盘、取消后后台 rename 竞态、无效 UTF8 阻塞与失败批次饿死，修复并发队列容量突破。
- 修复缺省配置段、超域时长、非有限阈值与钩子校验，补 Windows feature CI、结构化服务注册与性能脚本隔离清理。
- README、DESIGN、RUNBOOK、ROADMAP 与本轮评审记录按实际产品路径更新。


- **F14 配置热重载**:SIGHUP + `POST /v1/reload`;拓扑增量生效(check 水位保留 / service 重定义删+增 / 失败整体拒绝);非拓扑段变更要求重启(审计);删除服务写 journal Reset(防下次启动越权复活)
- **F15 Windows Service 形态**:`windows-service` feature + `--service-run`(SCM 感知,优雅停机替代 TerminateProcess 基线;`runtime::request_shutdown()`);服务模式文件日志(`--log-file`);install-service.ps1/README 两形态对比
- **F18 历史指标**:`/metrics/history?range=`(7200 样本环 ≈2h、降采样 ≤600)
- **F22 Web 状态页**:`GET /` 只读页(JS 轮询,零外部依赖,escapeHtml)
- **存储故障演练**:tests/storage_fault.rs 5 演练(打开失败→早退、慢盘 2s 预算、断电半行隔离、坏行、fail 契约)
- **崩溃启动事务**:SpawnIntent(落盘确认)+ SpawnOutcome + digest 对账(未知实例窗口消除;reset 解除)
- **原生 Windows Service 目标恢复**:`windows_service` 目标类型(ScmOps trait;QueryStatus liveness;幂等启停;force 降级如实;6 集成测试 + msvc 交叉)
- **指标周期推送**:`[metrics].enabled/url/timeout` → 周期 POST MetricsReport(失败即放弃下轮覆盖;连败边沿 journal 审计;panic 隔离)
- **F20 多服务编排**:`depends_on` 启动门控(FSM Running + 进程证据;Fatal 不级联)+ **停止顺序级联**(restart X → 逆拓扑先停被依赖方 → X 回 Running 后自动拉回;Fatal 跳过;全链审计)
- **F17 完成门 PASS**:24h 长跑(1412 采样)CPU 均值 0.46% < 2% 预算、RSS 稳态增长 176KB、零 ready 失败、exit 130;soak-24h.sh 默认目录改持久路径(前两次 /var/tmp 被清理器抹掉的教训)
- **F17 性能验证脚本**:`docs/deploy/perf-check.sh`(合成负载 CPU/RSS 度量,阈值判定 <2% 单核;实测 0.37% PASS;24h 长跑留人工)

### Fixed(第三轮系统面评审,2026-11-11 — 无 P0,14 P2 全修)

- 非默认 feature 组合编译破坏(collect 模块门控缺失、prometheus 测试门控)—— CI 盲区,本地实测 `--no-default-features` 两组合通过
- 启动幻象降级事件(首个 warmup 快照不计入跳变);首采与构造期双采样仅隔微秒(强制隔 250ms)
- `create_collector()` 构造纳入 panic 监督(此前线程可静默死亡,eval 消费冻结证据)
- deadman 心跳改 runtime 单次启动(dispatcher 重建不再累积并行心跳)+ 客户端 10s 超时;dispatcher 三次重建耗尽后写 journal 审计(断流可见性);drain 窗口过期后 abort 分发任务(库形态尾巴有界)
- webhook 禁用重定向(301/303 会把告警 POST 降级为 GET 投往错误端点)
- `/metrics` 协商:`?format=json` 显式短路(可压过 Accept);`last_collect` 恒输出(时有时无的序列对抓取器不友好);uptime 取 eval 单调源与墙钟源的 max(回拨防护)
- monitorctl target percent-encode(含 `/ ? #` 的名字不再改写请求路径);MetricsReport `#[serde(default)]`(滚动升级兼容);pending_kills 的 pgid 复用秒级误杀窗口如实记录(权衡:泄漏组员是确定性故障)

### Added(第三轮评审与部署配套,2026-11-11)

- **部署配套**:`docs/deploy/`(systemd unit[KillMode=process + 安全加固]/ launchd plist / Windows install-service.ps1)+ `docs/RUNBOOK.md`(journal jq 查询、Fatal 处置流程、故障决策树、日志轮转)
- **F15 Windows Service 形态**:`windows-service` feature + `--service-run`(SCM 感知,优雅停机替代 TerminateProcess 基线;`runtime::request_shutdown()`);服务模式文件日志(`--log-file`);install-service.ps1/README 两形态对比
- **F18 历史指标**:`/metrics/history?range=`(7200 样本环 ≈2h、降采样 ≤600)
- **F22 Web 状态页**:`GET /` 只读页(JS 轮询,零外部依赖,escapeHtml)
- **存储故障演练**:tests/storage_fault.rs 5 演练(打开失败→早退、慢盘 2s 预算、断电半行隔离、坏行、fail 契约)
- **崩溃启动事务**:SpawnIntent(落盘确认)+ SpawnOutcome + digest 对账(未知实例窗口消除;reset 解除)
- **原生 Windows Service 目标恢复**:`windows_service` 目标类型(ScmOps trait;QueryStatus liveness;幂等启停;force 降级如实;6 集成测试 + msvc 交叉)
- **指标周期推送**:`[metrics].enabled/url/timeout` → 周期 POST MetricsReport(失败即放弃下轮覆盖;连败边沿 journal 审计;panic 隔离)
- **F20 多服务编排**:`depends_on` 启动门控(FSM Running + 进程证据;Fatal 不级联)+ **停止顺序级联**(restart X → 逆拓扑先停被依赖方 → X 回 Running 后自动拉回;Fatal 跳过;全链审计)
- **F17 完成门 PASS**:24h 长跑(1412 采样)CPU 均值 0.46% < 2% 预算、RSS 稳态增长 176KB、零 ready 失败、exit 130;soak-24h.sh 默认目录改持久路径(前两次 /var/tmp 被清理器抹掉的教训)
- **F17 性能验证脚本**:`docs/deploy/perf-check.sh`(合成负载 CPU/RSS 度量,阈值判定 <2% 单核;实测 0.37% PASS;24h 长跑留人工)
- **F12 告警 spool 落盘(at-least-once)**:投递重试耗尽的事件持久化到 `{data_dir}/spool/pending.jsonl`,启动重放 + 周期重放接力投递;7 天过期、坏行保留、批量限流(ROADMAP F12)
- **第三轮评审修复(2 P0 + 2 P1 + 9 P2)**:
  - [P0] Starting 态 grace 过滤无时间上限 × bound_critical 门控 → 不健康服务 FSM 永久卡死(改纯时间窗 + 恢复态探测抑制,对齐 §8.1)
  - [P0] 控制面 restart 的强杀兜底在优雅退出后误杀新进程(deadline 清除 + 身份守卫)
  - [P1] kill 截止残留使后续宽限期退化为一个 tick;activate 期死亡卡死 ForceKill(activate_pending 臂 + 无子 ForceKill 通用兜底)
  - [P2] 多绑定 Warning 饿死确认链、liveness 确认改快照代际、Check/Reset 识别 watch 目标、Reset 未知目标审计、恢复期探测噪声、journal 预算事件(JournalRestart)保真跨重启 restore、pidfile 周期重读(30s)
- **退出码 130**(SIGTERM/SIGINT,StopReason;EvalExited→2)+ 三层 panic 监督(collect catch_unwind 重建采集器 / dispatcher 最多重建 3 次 / 单次投递任务边界隔离)

### Added(运行情况监控,2026-11-11)

- `/metrics` 真实运行指标:eval 每轮发布(告警管道计数、探测执行/跳过、恢复状态机转移与状态分布、降级瞬时值与事件数、健康分布、uptime),替代 v0.1 的恒 0 占位
- Prometheus 文本格式(`?format=prometheus` / `Accept: text/plain`,`service_watch_` 前缀,exposition format)—— ROADMAP F13
- `monitorctl metrics [--prom]` 子命令
- `POST /v1/restart/{target}` Fatal 态 → 409(经 status watch 判定;v0.1 缺口关闭)

### Added(v0.1.0 MVP — ROADMAP F1–F11)

- **workspace**:monitor-core(lib)/ monitor-agent(bin)/ monitorctl(bin);edition 2024、Apache-2.0、无 rust-version、重依赖 feature 化(`collect-sysinfo` / `http-client` / `probe-http` / `notify-webhook` / `control-http`)。
- **配置系统**:完整 TOML schema(DESIGN §4)、`deny_unknown_fields`、交叉校验(`timeout < interval` 等)、人类可读时长(`"10s"`/`"1h30m"`)、`config.example.toml` 由单测锁定。
- **采集**:sysinfo 0.40 采集器(CPU 首采降级、部分刷新协议、温度可信过滤 -20..150 ℃ + 噪声 label 跳过、观测身份上报);专用阻塞线程 + watch 广播;`--dump-snapshot`。
- **检查引擎**:Liveness/Http/Tcp/Process 四类;Ok→Warning→Critical 状态机(success_threshold 抗抖);在途探测上限 1;timeout 包裹。
- **告警引擎**:incident 模型(key 不含 severity/reason)、首次通知永不抑制、renotify 间隔、Escalate 总发、Resolve 条件发送、flap 抑制/解除;drop-oldest 有界队列 + 重试退避 + deadman 心跳;Slack/JSON 双格式。
- **恢复**:supervise/attach 双模式;`(pid, start_time)` 身份校验;恢复状态机全转移表(§8.2,42 单测);滑动窗口 budget + Fatal 闩锁;startup_grace;HungAction(activate 超时+验证);Unix 进程组 / Windows Job Object;grace 后组员补杀(SIGKILL);JSONL journal(Fatal/budget 跨重启持久)。
- **控制平面**:axum(默认 127.0.0.1:9170):/health /ready /status /metrics + restart/check/reset;monitorctl 客户端(status/restart/check/reset/dump-snapshot)。
- **观测性**:tracing EnvFilter + JSON 输出;结构化字段约定;E2E 验证(kill -9 拉活、Fatal 闩锁跨重启、退出码 0/1/2/4)。

### Fixed(第二轮评审,2026-11-11)

- [P0] ForceKill 收尾断裂:强杀成功后 `on_child_exit` 落入空臂,FSM 永久卡死、服务不再拉起 → ForceKill 态 child_exit 走 stop_confirmed;eval 侧 kill 截止 → D 状态兜底 Fatal(E2E:SIGTERM 免疫进程 → SIGKILL → 新 PID 拉起)
- [P1] 健康类挂死的自动重启零计数(永不 Fatal)→ begin_recovery(Restart 分支)计 budget;deterministic_death 补 would_exceed → Fatal
- [P1] `activate.verify_for` 配置被静默丢弃(默认巧合掩盖)→ build 时接线 set_verify_for
- [P1] `bound_watch` 恒 None → 双向回填,watch 事件归属/去重/Reset 生效
- [P1] journal 重放未接产品路径(replay 仅 warn)→ replayed 注入 eval,Fatal 目标跳过冷启动 spawn(E2E 实测 restored_restarts)
- [P2] 多绑定 check 交叉喂养破坏连续确认;时钟步进 ±2s 容差外误判死亡(重采纳);webhook/边界/drop-oldest/容差测试补齐

### Fixed(第一轮专家评审,2026-11-11)

- [P0] watched 集合恒空导致 process 检查永久失败 → eval↔collect 回传通道 + 采集器上报观测身份。
- [P0] Windows 目标编译断裂(Job Object 句柄 !Send)→ `unsafe impl Send`(单一所有权论证)。
- [P1] 健康类挂死恢复不可达(仅转移喂 on_health + liveness 无条件自愈)→ 每探必喂 + Critical 门控。
- [P1] incident 用墙钟判时长(回拨抑制重通知)→ 全部单调时钟。
- [P1] restart budget 慢死亡不计数 → 所有自动重启均计数。
- [P1] 停机丢告警无审计 → drain 超时计数 + journal 审计。
- [P1] 组长 reap 后组内幸存组员泄漏 → pending_kills 到期补 SIGKILL。
- [P1] `dur::parse` 巨大数值 panic → 统一转为配置错误(退出码 1)。
- [P1] reqwest 0.13 rustls=aws-lc-rs 的 Windows 工具链要求 → README/CI 记录(NASM+CMake)。
- [P2] AlertQueue 中毒恢复、pop 忙转、drop-new 计数;activate 钩子 kill_on_drop;spawn 本地钟跨秒失配 ±2s 容差;watch /status 健康聚合。

### 已知 v0.1 简化(记录于代码 doc 与 ROADMAP)

- agent 停机时 supervise 子进程默认保留(观察者外置;孤儿 attach 接管已在本轮补齐，见 DESIGN §8.7)。
- 注:早前列出的「journal 重放未接线」「Fatal 态 409 未实现」「pidfile 不周期重读」均已在此后的修复轮次关闭(见上方 Fixed/Added 条目)。
