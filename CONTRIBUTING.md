# Contributing

## 构建与测试

```bash
cargo build --workspace
cargo test --workspace                 # 80+ 单测(全部确定性,无真实 sleep)
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

平台验证两条线 (DESIGN §6.5):

1. 本地/CI 交叉 check(无 C 依赖 feature 组合):
   `cargo check --target x86_64-pc-windows-msvc -p monitor-core --no-default-features --features collect-sysinfo`
2. 完整 Windows 验证:CI `windows-latest` runner(需 NASM + CMake，见 ci.yml)。

## 硬性约束 (AGENT.md，违反即 CI 失败)

- 所有 crate `edition = "2024"`、`license = "Apache-2.0"`;
- **任何 Cargo.toml 禁止 `rust-version` 字段**(自检：`cargo metadata --format-version 1 | jq -e '[.packages[] | select(.source == null) | select(.rust_version != null)] | length == 0'`);
- 文档引用依赖版本必须带核对日期。

## PR 检查清单

- [ ] `cargo fmt --check` / `clippy -D warnings` / `cargo test --workspace` 全绿
- [ ] 新行为有确定性测试 (时间判定用注入参数，不用真实 sleep)
- [ ] 语义变更同步更新 DESIGN.md(状态机/默认值/schema) 与 ROADMAP(状态/偏差表)
- [ ] 跨平台影响：平台分支有降级说明 (DESIGN §6.4 矩阵)
- [ ] 告警/恢复语义变更：对照 §7.2 五条规则与 §8.2 全转移表补测试
- [ ] CHANGELOG.md 增加条目

## 代码纪律

- 单写者原则：状态机只在 eval 任务内变动;跨任务只传不可变消息 (DESIGN §2.2)。
- 降级可观测：不可用数据用 `Option` + `Degradation` 枚举，禁止伪造 0。
- 时长判定一律单调时钟 (`Instant`);`SystemTime` 仅作事件时间戳。
- 杀/重启动作必须以 `(pid, start_time)` 身份校验为前提;attach 目标永不干预。
- 运行路径禁止 `unwrap()`/`panic!`(锁中毒用 `into_inner()` 恢复);错误统一 `CoreError`。
- 日志：恢复/告警/降级必须 `INFO`+ 且含结构化字段 (target/check/incident/degraded)。


## 可选功能验证

```bash
cargo test --workspace --features collect-deep-hardware
cargo clippy --workspace --all-targets --features collect-deep-hardware -- -D warnings
cargo check --workspace --no-default-features --features collect-sysinfo
cargo check --workspace --no-default-features --features collect-sysinfo,control-http
cargo check -p monitor-agent --target x86_64-pc-windows-msvc --no-default-features --features collect-deep-hardware,windows-service
```

本机 HTTP 测试需要允许绑定回环端口。Windows SCM 的交叉检查需要预装对应 Rust target，安装和停机行为由 Windows 实机测试确认。
