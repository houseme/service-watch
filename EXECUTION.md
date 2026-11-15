# EXECUTION.md

> 执行计划。范围与进度以 [ROADMAP.md](ROADMAP.md) 为准,实现细节以 [DESIGN.md](DESIGN.md) 为准。

## 1. 执行策略

「先纵向切片,再横向铺开平台」:先在开发机(macOS)打通「采集→判定→告警→恢复→控制平面」闭环(v0.1,ROADMAP F1–F11),再以 CI 矩阵补齐 Linux/Windows 编译与降级路径,最后做加固与运维化(v0.2+)。

## 2. 阶段计划

### 阶段 0:骨架与约束校验 → ROADMAP F1

**动作**:workspace 三 crate;edition 2024 / Apache-2.0 / 无 rust-version / resolver 3;feature 化重依赖;LICENSE、.gitignore、rustfmt、CI 骨架。

**验收**:

- `cargo check --workspace` 通过;`cargo metadata` 无任何 `rust_version`(用 AGENT.md §5 的 jq 断言,非 grep)。
- 交叉 `cargo check --target x86_64-pc-windows-msvc`(先 `rustup target add`,若未装)仅覆盖默认 feature 中无 C 依赖的部分(DESIGN §6.5;reqwest/ring 不在本地 Windows 交叉范围)。

### 阶段 1:采集 → ROADMAP F3(依赖 F2 配置)

**动作**:Config schema + 校验(§4);HardwareSnapshot/ProcessSnapshot/Degradation(§5);sysinfo 0.40 锚点适配(§6.1);部分刷新协议(§6.2);温度可信过滤(§6.3);专用采集线程 + watch 广播(§2.2)。

**验收**:

- `monitor-agent --dump-snapshot` 输出合法 JSON,CPU/内存/磁盘非空;温度 None 必伴随 Degradation。
- CPU 首采路径有单测(注入时钟 + mock),不出现伪造 0。
- 默认 5s 间隔下 agent 自身 CPU < 2%(单核)。

### 阶段 2:检查与告警 → ROADMAP F4、F5

**动作**:检查状态机(§7.1,含 success_threshold、在途上限 1、跳轮计数);incident 模型与四类事件(§7.2);Notifier trait + WebhookNotifier + 分发背压(§7.3);deadman 心跳。

**验收**:

- 注入时钟单测覆盖 §7.1/§7.2 全部转移与抑制规则,包括:fingerprint 不随 severity/reason 变化、Resolved 后立即复发能再告警、flap 抑制、队列满丢最旧有审计。
- 集成(HTTP stub):慢响应(> timeout)计失败;连续 2 失败后恢复 → 仅 Warning + Resolve,**无 Critical**(原「2 失败即 Resolved」验收已修正语义)。
- webhook 不可达时:重试 → 审计日志 + 计数递增,主循环不受影响。

### 阶段 3:恢复 → ROADMAP F6、F7

**动作**:恢复状态机全转移表(§8.2);触发契约(§8.1);`(pid,start_time)` 身份(§8.3);parenting(§8.4);HungAction(§8.5);Windows Job(§8.6);journal 持久化与孤儿对账(§8.7)。

**验收**:

- kill -9 → backoff 内拉活,重启计数递增;退出事件路径无需二次确认。
- 挂死脚本(健康超时 + 进程存活)→ Suspect 确认 → Restart 或 Activate(含 activate 超时与 verify_for)。
- crash loop 耗尽 budget → Fatal,**窗口滑过不自动恢复**;`monitorctl reset` 解除并写审计。
- 杀掉 agent 再启动:journal 重放后 Fatal 仍在、budget 窗口仍有效;孤儿进程被 attach 接管,不双实例。
- attach 目标在任意失败下永不执行杀/重启(单测断言)。

### 阶段 4:控制平面、CLI、观测性 → ROADMAP F8–F10

**动作**:axum 控制平面(§9.2 端点表);monitorctl 子命令与退出码(§9.3);agent 优雅退出，SIGTERM/SIGINT 停探测、保留 supervise 子进程并 flush journal，下次启动按持久化身份对账;tracing 约定(§10)。

**验收**:

- `monitorctl status` 拿到运行态;`restart` 在 Fatal 态返回 409 并提示 reset;agent 不可达退出码 4。
- `--check-config` 非法配置退出码 1;SIGTERM 后进程在 grace 内退出、子进程已停、journal 完整。
- `/ready` 不因温度等降级而 false;JSON 日志每行可被 jq 解析。

### 阶段 5:加固与运维化 → ROADMAP v0.2

spool 落盘 at-least-once、Prometheus 文本 /metrics、热重载、Windows Service、deploy 示例、24h 性能预算验证。

## 3. 风险与回滚

| 风险 | 缓解 |
|---|---|
| 依赖 MSRV 高于本机(sysinfo 0.40.1 声明 1.95) | 升 toolchain 优先;README 声明实测 MSRV;CI 加最低工具链 check job |
| Windows 信号语义差异 | 基线路径 TerminateProcess + Job Object,审计日志;CTRL_BREAK 仅显式启用且不强求 |
| Windows 完整验证依赖 runner | 两条验收线(DESIGN §6.5):本地/CI 交叉 check(无 C 依赖 feature)+ windows-latest 全量 |
| agent 单点 | /health /ready + 外部 service manager + deadman;不做双向重启依赖 |
| 时间跳变/休眠唤醒 | 判定一律单调时钟;唤醒后首轮探测丢弃记 degraded |
| 温度垃圾值(实测 -9201 ℃) | 可信过滤 -20..150 ℃,越界丢弃 + Degradation |
