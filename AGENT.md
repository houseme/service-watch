# AGENT.md

## 1. 角色定义

本 AGENT 是「跨平台服务运行监控系统 service-watch」的**工程执行代理**。职责：在 Rust 2024 edition 约束下，维护一个可复用监控库 (`monitor-core`) 与两个可执行入口 (`monitor-agent`、`monitorctl`),覆盖三条能力线：跨平台硬件监控、异常/超时告警、死机/挂死恢复拉活。

**规约分工**(有冲突时按此顺序裁决):`AGENT.md`(硬约束)> `DESIGN.md`(实现细节)> `ROADMAP.md`(范围与进度)> `EXECUTION.md`(执行计划)。

## 2. 硬性约束 (永久记忆)

以下约束在任何后续开发中**不得放松**:

| 约束 | 要求 |
|---|---|
| **Edition** | 所有 `Cargo.toml` 中 `edition = "2024"` |
| **禁止 `rust-version`** | 任何 crate 的 `Cargo.toml` **不得包含** `rust-version` 字段 (注意：依赖自身 MSRV 仍会被解析器强制，如 sysinfo 0.40.1 声明 1.95;处理顺序：升 toolchain > 回退依赖并文档注明) |
| **License** | 所有 crate `license = "Apache-2.0"`,仓库根保留 LICENSE 全文 |
| **依赖版本** | 允许最新 stable/RC/beta,以可编译解析为准;文档引用版本必须带核对日期 |
| **架构** | lib 与 bin 分离;核心逻辑一律在 `monitor-core`,bin 只做入口 |
| **Crate 拆分** | `crates/monitor-core`(lib)、`crates/monitor-agent`(bin)、`crates/monitorctl`(bin);无 default-members |
| **跨平台** | Linux / macOS / Windows 可编译可用;平台特有实现放 `#[cfg(target_os = ...)]`/target-specific dependency |
| **恢复边界** | agent 独立于被监控服务;不与被监控服务同生共死 (Windows Job 默认不带 KILL_ON_JOB_CLOSE) |
| **安全动作** | 一切杀/重启动作必须以 `(pid, start_time)` 身份校验为前提;attach 目标永不执行杀/重启 |

## 3. 工作区结构

```text
service-watch/
├── Cargo.toml              # workspace, resolver = "3", 无 default-members
├── AGENT.md / DESIGN.md / ROADMAP.md / EXECUTION.md / BENCHMARK.md / README.md
├── crates/
│   ├── monitor-core/       # lib:config / model / collect / eval / alert / recover / control
│   ├── monitor-agent/      # bin:clap 入口 → tracing → monitor_core::run()
│   └── monitorctl/         # bin:控制平面客户端 + 本地 dump-snapshot
├── config/config.example.toml
└── .github/workflows/
    ├── ci.yml              # lint / constraints / test / cross-check
    └── release.yml         # tag 触发四平台发布构建，产物上传 GitHub Release
```

## 4. 能力线责任映射

| 能力线 | 归属模块 | 不可委托的部分 |
|---|---|---|
| 跨平台硬件监控 | `collect` | CPU/内存/磁盘/温度;不可用即 `Option` + `Degradation`,不伪造 |
| 异常/超时告警 | `eval` + `alert` | incident 去噪、renotify、flap 抑制、投递重试与背压 |
| 死机/挂死恢复 | `recover` | `(pid,start_time)` 身份校验、backoff、budget、graceful→强杀升级、进程组/Job 清理 |

## 5. 执行纪律

1. **约束自检**(可失败断言，修改 Cargo.toml 后必跑;`source == null` 限定工作区成员、`rust_version != null` 判真实值——现代 cargo 会为未设置的包恒定输出 `"rust_version": null`,`has()` 类守卫恒真):
   `cargo metadata --format-version 1 | jq -e '[.packages[] | select(.source == null) | select(.rust_version != null)] | length == 0'`
2. 每次提交前：`cargo fmt --check` + `cargo clippy --workspace -- -D warnings` + `cargo test --workspace`。
3. 新增平台支持：必须同时提供该平台编译检查与降级说明 (写进 DESIGN §6.4 矩阵)。
4. **重启策略默认保守**:健康类失败必须连续 `confirmations`(默认 2) 次确认才进入恢复;确定性死亡 (退出事件/连续 liveness 确认) 除外。
5. 文档示例与实际 `Cargo.toml` 冲突时，以可编译状态为准，并立即修正文档 (记录到 ROADMAP「已知偏差」)。
6. 版本引用一律带核对日期;核对周期每季度一次。
