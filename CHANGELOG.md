# Changelog

All notable changes to this project will be documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/); versioning: 0.x 阶段 minor=功能，y.z=修复。

## [Unreleased]

### 2026-11-18 macOS runner 恢复 (macos-14 镜像退役换 macos-15)

- ci.yml test 矩阵 `macos-14` → `macos-15`:macos-14 镜像 2026-11-02 从 GitHub Actions 机队退役 (报 `configuration 'macos-14-arm64-us-default' is not supported`;此前「runner 池枯竭」即退役过渡期表现)。
- release.yml 恢复 darwin 双目标原生构建矩阵 (macos-15 arm64 runner,x86_64-apple-darwin 交叉，按暂缓前的原始口径)—— macOS 发布资产自下一 tag 起恢复。

## [0.3.0] — 2026-11-18

v0.4-v0.8 全部功能 (F23-F39) 与第八~十六轮评审修复 (第十五/十六轮:F37 头名折叠/跨表校验、history/journal/spool 路径碰撞守卫、文档全面对坑等，均已随本版交付)。**0.x 阶段 minor=功能**;本版含库 API 破坏性变更 (见 v0.7 节注记：`run_metrics_pusher` 增第 4 参 `secret`、`WebhookNotifier::new` 增第 4 参 `secret` 与第 5 参 `headers`),故 0.2.0 → 0.3.0。

发布资产仅 Linux x86_64 与 Windows x86_64;macOS 资产暂缓 (runner 池枯竭 + 交叉链三方案均不可行，恢复计划见 release.yml 注释)。

### 发布修正 (2026-11-18 同日重指 tag 重出产物;初版 Windows 件含下述 P0，已覆盖)

- [P0] **Windows 服务 `sc stop` 死锁于 STOP_PENDING**(第十七轮评审，F36 首跑两次实弹复现):停机 select 的 `REQUEST_NOTIFY.notified()` 分支带 `if REQUESTED` 前置守卫 —— select 驻留时分支被禁用、`notified()` 未注册等待者，`request_shutdown` 的 `notify_one` 只存 permit 不唤醒任务;服务形态无 ctrl_c/eval 等其他唤醒源，runtime 永不进入停机序列。修复：分支恒启用、体内查 `REQUESTED` 过滤残留 permit(unix 与 not(unix) 两分支同构);新增 `tests/inprocess_shutdown.rs` 回归 (修复前 10.5s 超时红实证咬合)。
- 服务停机加固 (实弹调试追加):`request_shutdown` 先于 StopPending 报告调用;`run_task.await` 加 30s wait-hint 超时兜底 (超时 abort 并按受控停止报告 STOPPED —— SCM 契约优先，操作员显式 stop 不触发 failure 重启)。
- 服务模式文件日志禁用 ANSI(pretty 色码包裹字段值致 `reason=Clean` 断言失配)。
- 实机验收:windows-scm 演练全绿 (SCM 安装/优雅停机/孤儿/热重载/强杀自愈),ROADMAP Windows 实机验收 ☑。

## [0.2.0] — 2026-11-17

第一个 tag 发布 (v0.2 完成门 PASS 后切版本)。v0.1 + v0.2 + v0.3 可实现项全部交付，详见下方各条目与 ROADMAP。

### 2026-11-18 v0.8 功能 (可操作性增强) 与第十四轮评审修复

新增 (ROADMAP v0.8，全部默认向后兼容):

- F37 webhook 自定义投递头：`[alert] headers`(全局)+ 路由级 `headers`(同名覆盖全局，签名头独立共存);RFC 7230 token 名 + 可见 ASCII 值校验 (与 http crate 字节规则对齐);线上头名小写。
- F38 控制面 mTLS:`[control] client_ca_cert`(可选) 配置后 TLS 握手强制客户端证书 (严格 CA 解析);monitorctl `--client-cert/--client-key`(成对);与 `--insecure`/`--token` 正交可叠加。rustls 0.23 直接声明 (不新增编译单元，feature 并集与现状一致)。
- F39 指标历史持久化：`[metrics] history_path`(可选)—— 每样本 JSONL 追加落盘 (行 = /metrics/history 单样本对象),10 MiB 单代轮转，重启追加，崩溃残行定界;不做重放/查询 (纯数据出口)。

修复 (第十四轮双专家验证):

- [P2] header 值校验漏非 ASCII(is_control 不覆盖 ≥ U+00A0,"café" 通过 config 但被构造期丢弃 → 接收方缺头 401)→ 值校验对齐 http crate 字节规则。
- [P3] 历史文件打开时半行定界 (爆炸半径 2 行 → 1 行);mTLS 握手拒绝测试区分超时与 TLS alert;--insecure+mTLS 组合测试补齐;CA 解析注释如实化 (非证书段静默跳过);F37 线上头名小写/保留头警示;F39 时间源措辞如实化;交叉引用修正 (§1.2e/§1.2f)。

### 2026-11-18 v0.7 功能 (收尾攻坚) 与第十二轮评审修复

新增 (ROADMAP v0.7，全部默认向后兼容;**注意：库 API 有破坏性变更**,即本版 0.2.0 → 0.3.0 的动因 —— `run_metrics_pusher` 增第 4 参 `secret`、`WebhookNotifier::new` 增第 4 参 `secret`(F37 再增第 5 参 `headers`)):

- F34 指标推送 HMAC 签名：`[metrics] secret` 配置后推送附 `X-Service-Watch-Signature: sha256=<hex>`(与 F29 webhook 签名同口径);签名 helper 下沉 `alert/signature.rs`(http-client 门控共享层，hmac/sha2 随 http-client 拉入)。
- F35 journal 查询覆盖轮转上一代：`GET /v1/journal` 当 `journal.1.jsonl` 存在时合并尾读 (gen1 在前，时间序);读失败并入 `truncated=true`(保守口径);monitorctl 零改动。
- F36 Windows runner SCM 实机演练:CI 新作业在 windows-latest 上用真实 install-service.ps1 安装服务 → RUNNING → /health → spawn 事务 (pid_confirmed:true)→ sc stop 优雅退出 (reason=Clean)→ **Job Object 孤儿断言 (监督子进程存活)** → 幂等两段 → **热重载断言 (POST /v1/reload → /status)** → **错误退出自愈 (强杀 → SCM failure action 拉起 → health 恢复)** → 清理;rust-cache + timeout 30min + 诊断覆盖 cancelled 与 SCM 事件日志。**CI 首跑绿待 push 后验证**。
- 硬件实机覆盖 (部分):Apple Silicon 实机证据已采集 (本机 darwin arm64,2 风扇 RPM/电压/17 温度传感器，VBUS 无效读数机读降级);Linux hwmon/Intel 仍需真机。

修复 (第十二轮双专家验证):

- [P1] `Config::redacted()` 漏 `[metrics].secret` —— /v1/config 与 monitorctl config 泄明文推送密钥 → 补脱敏 + 两处测试锁定。
- [P1] windows-scm 幂等断言匹配中文消息 —— CI runner OEM 代码页 CJK 有损编码必然假红 → 改匹配 ASCII 片段 `(state=`。
- [P2] journal gen1 读失败 truncated 不置位 (响应看似完整却缺整代历史)→ 读失败并入 truncated=true;[P2] spawn_outcome 不证明 spawn 成功 (失败也落盘)→ 断言 pid_confirmed:true;[P2] 无缓存 release 构建 timeout 偏紧 → rust-cache + 30min。
- [P3] gen1_path 双处实现去重 (journal.rs pub(crate),control 委托);diagnostics 覆盖 cancelled + SCM System 事件日志;三 workflow permissions 显式化;孤儿化措辞修正。

### 2026-11-18 v0.6 功能 (远程可信与运维节奏) 与第十一轮评审修复

新增 (ROADMAP v0.6，全部默认向后兼容):

- F31 定期维护窗口：`[[maintenance]]`(UTC 时刻/星期/前缀 targets，跨 midnight 精确);窗口内 Alert/Escalate/Flap 通知抑制 (复用 F27 管线，Resolve/Fatal 恒穿透),开/关 Audit 边沿恰好一次;判定纯函数 (手工 UTC 历法，1970→2100 对拍)。
- F32 控制面 HTTPS:`[control] tls_cert`/`tls_key`(成对) → axum-server rustls 同端口 HTTPS;加载失败装配期早退 (退出码 2);monitorctl `--url https://...` 可用 + `--insecure`(自签/测试场景);TLS 加载与 bind 同受 control.enabled 门控。
- F33 生效配置查看：`GET /v1/config` + monitorctl `config [--json]` —— **合并视图**(拓扑段随 reload 刷新，要求重启的段恒为启动值),token/签名密钥 `***redacted***` 占位不可回灌。

修复 (第十一轮双专家验证):

- [P1] reload diff_sections 漏 maintenance 段 —— 窗口变更 reload 零审计，运维无从得知新窗口未生效 → 补列 + 单测。
- [P2] /v1/config 初版推整份新文件 —— reload 后把未生效的 control/alert/collect/maintenance 值展示成已生效 → 改合并视图 (watch 通道 `config_view_tx/rx`,eval reload 成功即推送)。
- [P3] TLS 加载不受 control.enabled 门控;DESIGN F33 过时偏差描述;RUNBOOK 证书链 leaf-first 说明;F31 墙钟跳变下 Audit 边沿形态如实限定;状态页 meta 行显示数据真实最大值 (全零序列不再误导性显示 "max 1")。
- F30 状态页人工目验完成 (浏览器实测：全零序列贴基线;critical 折线 0→1 阶跃清晰)。

### 2026-11-18 v0.5 功能 (运维降噪与可观测出口) 与第十轮评审修复

新增 (ROADMAP v0.5，全部默认向后兼容):

- F27 目标级告警静默：`POST/DELETE /v1/silence/{target}`(单调 deadline 自动过期，重复设置覆盖;静默建立/解除/过期/首次抑制各写 journal Audit);**Resolve 与 Fatal 闩锁告警恒穿透**;monitorctl `silence`/`unsilence`。
- F28 journal 尾部查询：`GET /v1/journal?tail&target&type&since`(512KB 字节尾读 + spawn_blocking，坏行跳过，truncated 标志，受 F23 鉴权);monitorctl `journal [--json]`。
- F29 webhook HMAC 签名：`[alert] webhook_secret` + 路由级 `secret`(路由级覆盖全局),投递附 `X-Service-Watch-Signature: sha256=<hex hmac-sha256(body)>`;hmac/sha2 随 notify-webhook feature(纯 Rust);RFC 4231 向量锁定。
- F30 状态页历史迷你图：零依赖 SVG 折线 (checks_critical/recovery_fatal,/metrics/history 拉取，失败隔离，XSS 纪律延续)。

修复 (第十轮双专家验证):

- [P1] 静默误吞 Fatal 闩锁告警且永不补发 (无 tracker trail/renotify 兜底)→ Fatal 合成告警与 Resolve 同等豁免 + 单测。
- [P2] RUNBOOK 静默审计查询示例恒空 (Audit 无 target 字段，`--target` 过滤误用)→ 示例修正;DESIGN §4.1 `webhook_secret = ""` 模板与校验矛盾 (照抄启动失败)→ 模板注释化。
- [P3] journal 端点非 ENOENT 读失败静默伪装成空结果 → 服务端 WARN;monitorctl 表格输出剥离 C0/C1 控制字符 (终端注入防护);status_page.rs 门控补 http-client;迷你图恒定非零序列改画中线 (防"贴顶打满"误读);USAGE.md §13 过时"预留"表述改为已交付。

### 2026-11-18 v0.4 功能 (可信边界与运维闭环) 与第八/九轮评审修复

新增 (ROADMAP v0.4，全部默认向后兼容):

- F23 控制面可选访问令牌：`[control] token` 配置后除 `GET /health` 外全部端点要求 `Authorization: Bearer <token>`(缺失/错误 401);恒定时间比较 (零新依赖);monitorctl `--token` flag + `SERVICE_WATCH_TOKEN` 环境变量 (flag 优先)。
- F24 journal 体积上限与轮转：`[agent] journal_max_size`(默认 10 MiB,0 = 禁用) 超限轮转 `journal.1.jsonl`(仅保留一代);改名失败降级继续写不停机;重放先上一代后当前，Fatal/预算/未决 SpawnIntent 跨轮转保持。
- F25 incident 跨重启恢复与重通知：重放恢复仍 open 的 incident(含 warning trail),启动即重通知一次并重置 renotify 窗口;flap 计数器 fresh 不恢复 (取舍见偏差表);孤儿 resolve 幂等丢弃 + `orphan_resolves` 计数;Fatal 合成 incident 随 Reset 闭门。
- F26 按目标告警路由：`[[alert.routes]]` 顺序首条前缀匹配 (`*` 通配全部),无匹配回落 `webhook_url`;spool 重放按当前配置重新路由。

修复 (第八/九轮评审，双专家验证):

- [P2] reload 删除/重定义不清理 `planned_stop_deadlines`,残留过期强杀截止可对同名新实例误触 force_kill。
- [P2] metrics pusher 持有 journal_tx 克隆且 POST 在途不响应 shutdown，停机 flush 窗口必然超时丢事件 → JoinHandle 保留，停机 abort+await 收口。
- [P2] `/metrics` uptime 单调源从未被消费 (copy 块漏拷贝，墙钟永远胜出);F25 × `__recover__` 组合在 reset 后每次重启发虚假 Fatal 重通知 —— 双双修复 + 锁定测试。
- [P3] `alert.retries` 上界 (≤100)、`watch.pidfile` 空路径拒绝、路由空 pattern 恒不命中、轮转失败计数 + 周期化 WARN、轮转 audit 行丢失文案如实化、`tests/alert_routing.rs` no-default 门控、routes 日志上报 configured/active。
- 文档 12 处事实核对修正：`[metrics]` 推送已实现 (不再"预留")、`[control].listen` 需重启非热重载、RUNBOOK 适用版本/退出码 1/journal schema 补 spawn_intent|spawn_outcome、端点清单补全、24h 长跑 F17 PASS 状态同步。

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


- **F14 配置热重载**:SIGHUP + `POST /v1/reload`;拓扑增量生效 (check 水位保留 / service 重定义删 + 增 / 失败整体拒绝);非拓扑段变更要求重启 (审计);删除服务写 journal Reset(防下次启动越权复活)
- **F15 Windows Service 形态**:`windows-service` feature + `--service-run`(SCM 感知，优雅停机替代 TerminateProcess 基线;`runtime::request_shutdown()`);服务模式文件日志 (`--log-file`);install-service.ps1/README 两形态对比
- **F18 历史指标**:`/metrics/history?range=`(7200 样本环 ≈2h、降采样 ≤600)
- **F22 Web 状态页**:`GET /` 只读页 (JS 轮询，零外部依赖，escapeHtml)
- **存储故障演练**:tests/storage_fault.rs 5 演练 (打开失败→早退、慢盘 2s 预算、断电半行隔离、坏行、fail 契约)
- **崩溃启动事务**:SpawnIntent(落盘确认)+ SpawnOutcome + digest 对账 (未知实例窗口消除;reset 解除)
- **原生 Windows Service 目标恢复**:`windows_service` 目标类型 (ScmOps trait;QueryStatus liveness;幂等启停;force 降级如实;6 集成测试 + msvc 交叉)
- **指标周期推送**:`[metrics].enabled/url/timeout` → 周期 POST MetricsReport(失败即放弃下轮覆盖;连败边沿 journal 审计;panic 隔离)
- **F20 多服务编排**:`depends_on` 启动门控 (FSM Running + 进程证据;Fatal 不级联)+ **停止顺序级联**(restart X → 逆拓扑先停被依赖方 → X 回 Running 后自动拉回;Fatal 跳过;全链审计)
- **F17 完成门 PASS**:24h 长跑 (1412 采样)CPU 均值 0.46% < 2% 预算、RSS 稳态增长 176KB、零 ready 失败、exit 130;soak-24h.sh 默认目录改持久路径 (前两次 /var/tmp 被清理器抹掉的教训)
- **F17 性能验证脚本**:`docs/deploy/perf-check.sh`(合成负载 CPU/RSS 度量，阈值判定 <2% 单核;实测 0.37% PASS;24h 长跑留人工)

### Fixed(第三轮系统面评审，2026-11-11 — 无 P0,14 P2 全修)

- 非默认 feature 组合编译破坏 (collect 模块门控缺失、prometheus 测试门控)—— CI 盲区，本地实测 `--no-default-features` 两组合通过
- 启动幻象降级事件 (首个 warmup 快照不计入跳变);首采与构造期双采样仅隔微秒 (强制隔 250ms)
- `create_collector()` 构造纳入 panic 监督 (此前线程可静默死亡，eval 消费冻结证据)
- deadman 心跳改 runtime 单次启动 (dispatcher 重建不再累积并行心跳)+ 客户端 10s 超时;dispatcher 三次重建耗尽后写 journal 审计 (断流可见性);drain 窗口过期后 abort 分发任务 (库形态尾巴有界)
- webhook 禁用重定向 (301/303 会把告警 POST 降级为 GET 投往错误端点)
- `/metrics` 协商：`?format=json` 显式短路 (可压过 Accept);`last_collect` 恒输出 (时有时无的序列对抓取器不友好);uptime 取 eval 单调源与墙钟源的 max(回拨防护)
- monitorctl target percent-encode(含 `/ ? #` 的名字不再改写请求路径);MetricsReport `#[serde(default)]`(滚动升级兼容);pending_kills 的 pgid 复用秒级误杀窗口如实记录 (权衡：泄漏组员是确定性故障)

### Added(第三轮评审与部署配套，2026-11-11)

- **部署配套**:`docs/deploy/`(systemd unit[KillMode=process + 安全加固]/ launchd plist / Windows install-service.ps1)+ `docs/RUNBOOK.md`(journal jq 查询、Fatal 处置流程、故障决策树、日志轮转)
- **F15 Windows Service 形态**:`windows-service` feature + `--service-run`(SCM 感知，优雅停机替代 TerminateProcess 基线;`runtime::request_shutdown()`);服务模式文件日志 (`--log-file`);install-service.ps1/README 两形态对比
- **F18 历史指标**:`/metrics/history?range=`(7200 样本环 ≈2h、降采样 ≤600)
- **F22 Web 状态页**:`GET /` 只读页 (JS 轮询，零外部依赖，escapeHtml)
- **存储故障演练**:tests/storage_fault.rs 5 演练 (打开失败→早退、慢盘 2s 预算、断电半行隔离、坏行、fail 契约)
- **崩溃启动事务**:SpawnIntent(落盘确认)+ SpawnOutcome + digest 对账 (未知实例窗口消除;reset 解除)
- **原生 Windows Service 目标恢复**:`windows_service` 目标类型 (ScmOps trait;QueryStatus liveness;幂等启停;force 降级如实;6 集成测试 + msvc 交叉)
- **指标周期推送**:`[metrics].enabled/url/timeout` → 周期 POST MetricsReport(失败即放弃下轮覆盖;连败边沿 journal 审计;panic 隔离)
- **F20 多服务编排**:`depends_on` 启动门控 (FSM Running + 进程证据;Fatal 不级联)+ **停止顺序级联**(restart X → 逆拓扑先停被依赖方 → X 回 Running 后自动拉回;Fatal 跳过;全链审计)
- **F17 完成门 PASS**:24h 长跑 (1412 采样)CPU 均值 0.46% < 2% 预算、RSS 稳态增长 176KB、零 ready 失败、exit 130;soak-24h.sh 默认目录改持久路径 (前两次 /var/tmp 被清理器抹掉的教训)
- **F17 性能验证脚本**:`docs/deploy/perf-check.sh`(合成负载 CPU/RSS 度量，阈值判定 <2% 单核;实测 0.37% PASS;24h 长跑留人工)
- **F12 告警 spool 落盘 (at-least-once)**:投递重试耗尽的事件持久化到 `{data_dir}/spool/pending.jsonl`,启动重放 + 周期重放接力投递;7 天过期、坏行保留、批量限流 (ROADMAP F12)
- **第三轮评审修复 (2 P0 + 2 P1 + 9 P2)**:
  - [P0] Starting 态 grace 过滤无时间上限 × bound_critical 门控 → 不健康服务 FSM 永久卡死 (改纯时间窗 + 恢复态探测抑制，对齐 §8.1)
  - [P0] 控制面 restart 的强杀兜底在优雅退出后误杀新进程 (deadline 清除 + 身份守卫)
  - [P1] kill 截止残留使后续宽限期退化为一个 tick;activate 期死亡卡死 ForceKill(activate_pending 臂 + 无子 ForceKill 通用兜底)
  - [P2] 多绑定 Warning 饿死确认链、liveness 确认改快照代际、Check/Reset 识别 watch 目标、Reset 未知目标审计、恢复期探测噪声、journal 预算事件 (JournalRestart) 保真跨重启 restore、pidfile 周期重读 (30s)
- **退出码 130**(SIGTERM/SIGINT,StopReason;EvalExited→2)+ 三层 panic 监督 (collect catch_unwind 重建采集器 / dispatcher 最多重建 3 次 / 单次投递任务边界隔离)

### Added(运行情况监控，2026-11-11)

- `/metrics` 真实运行指标:eval 每轮发布 (告警管道计数、探测执行/跳过、恢复状态机转移与状态分布、降级瞬时值与事件数、健康分布、uptime),替代 v0.1 的恒 0 占位
- Prometheus 文本格式 (`?format=prometheus` / `Accept: text/plain`,`service_watch_` 前缀，exposition format)—— ROADMAP F13
- `monitorctl metrics [--prom]` 子命令
- `POST /v1/restart/{target}` Fatal 态 → 409(经 status watch 判定;v0.1 缺口关闭)

### Added(v0.1.0 MVP — ROADMAP F1–F11)

- **workspace**:monitor-core(lib)/ monitor-agent(bin)/ monitorctl(bin);edition 2024、Apache-2.0、无 rust-version、重依赖 feature 化 (`collect-sysinfo` / `http-client` / `probe-http` / `notify-webhook` / `control-http`)。
- **配置系统**:完整 TOML schema(DESIGN §4)、`deny_unknown_fields`、交叉校验 (`timeout < interval` 等)、人类可读时长 (`"10s"`/`"1h30m"`)、`config.example.toml` 由单测锁定。
- **采集**:sysinfo 0.40 采集器 (CPU 首采降级、部分刷新协议、温度可信过滤 -20..150 ℃ + 噪声 label 跳过、观测身份上报);专用阻塞线程 + watch 广播;`--dump-snapshot`。
- **检查引擎**:Liveness/Http/Tcp/Process 四类;Ok→Warning→Critical 状态机 (success_threshold 抗抖);在途探测上限 1;timeout 包裹。
- **告警引擎**:incident 模型 (key 不含 severity/reason)、首次通知永不抑制、renotify 间隔、Escalate 总发、Resolve 条件发送、flap 抑制/解除;drop-oldest 有界队列 + 重试退避 + deadman 心跳;Slack/JSON 双格式。
- **恢复**:supervise/attach 双模式;`(pid, start_time)` 身份校验;恢复状态机全转移表 (§8.2,42 单测);滑动窗口 budget + Fatal 闩锁;startup_grace;HungAction(activate 超时 + 验证);Unix 进程组 / Windows Job Object;grace 后组员补杀 (SIGKILL);JSONL journal(Fatal/budget 跨重启持久)。
- **控制平面**:axum(默认 127.0.0.1:9170):/health /ready /status /metrics + restart/check/reset;monitorctl 客户端 (status/restart/check/reset/dump-snapshot)。
- **观测性**:tracing EnvFilter + JSON 输出;结构化字段约定;E2E 验证 (kill -9 拉活、Fatal 闩锁跨重启、退出码 0/1/2/4)。

### Fixed(第二轮评审，2026-11-11)

- [P0] ForceKill 收尾断裂：强杀成功后 `on_child_exit` 落入空臂，FSM 永久卡死、服务不再拉起 → ForceKill 态 child_exit 走 stop_confirmed;eval 侧 kill 截止 → D 状态兜底 Fatal(E2E:SIGTERM 免疫进程 → SIGKILL → 新 PID 拉起)
- [P1] 健康类挂死的自动重启零计数 (永不 Fatal)→ begin_recovery(Restart 分支) 计 budget;deterministic_death 补 would_exceed → Fatal
- [P1] `activate.verify_for` 配置被静默丢弃 (默认巧合掩盖)→ build 时接线 set_verify_for
- [P1] `bound_watch` 恒 None → 双向回填，watch 事件归属/去重/Reset 生效
- [P1] journal 重放未接产品路径 (replay 仅 warn)→ replayed 注入 eval,Fatal 目标跳过冷启动 spawn(E2E 实测 restored_restarts)
- [P2] 多绑定 check 交叉喂养破坏连续确认;时钟步进 ±2s 容差外误判死亡 (重采纳);webhook/边界/drop-oldest/容差测试补齐

### Fixed(第一轮专家评审，2026-11-11)

- [P0] watched 集合恒空导致 process 检查永久失败 → eval↔collect 回传通道 + 采集器上报观测身份。
- [P0] Windows 目标编译断裂 (Job Object 句柄 !Send)→ `unsafe impl Send`(单一所有权论证)。
- [P1] 健康类挂死恢复不可达 (仅转移喂 on_health + liveness 无条件自愈)→ 每探必喂 + Critical 门控。
- [P1] incident 用墙钟判时长 (回拨抑制重通知)→ 全部单调时钟。
- [P1] restart budget 慢死亡不计数 → 所有自动重启均计数。
- [P1] 停机丢告警无审计 → drain 超时计数 + journal 审计。
- [P1] 组长 reap 后组内幸存组员泄漏 → pending_kills 到期补 SIGKILL。
- [P1] `dur::parse` 巨大数值 panic → 统一转为配置错误 (退出码 1)。
- [P1] reqwest 0.13 rustls=aws-lc-rs 的 Windows 工具链要求 → README/CI 记录 (NASM+CMake)。
- [P2] AlertQueue 中毒恢复、pop 忙转、drop-new 计数;activate 钩子 kill_on_drop;spawn 本地钟跨秒失配 ±2s 容差;watch /status 健康聚合。

### 已知 v0.1 简化 (记录于代码 doc 与 ROADMAP)

- agent 停机时 supervise 子进程默认保留 (观察者外置;孤儿 attach 接管已在本轮补齐，见 DESIGN §8.7)。
- 注：早前列出的「journal 重放未接线」「Fatal 态 409 未实现」「pidfile 不周期重读」均已在此后的修复轮次关闭 (见上方 Fixed/Added 条目)。
