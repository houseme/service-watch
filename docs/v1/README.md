# docs/v1 — 用户与开发者指南(v1 Documentation Set)

本目录是 service-watch 的结构化文档集 v1:按功能与使用场景组织的用户指南,以及面向源码贡献者的开发者指南,各含中英文版本。内容基于 main 分支(v0.3.0,含 v0.4–v0.8 全部功能),整理日期 **2026-11-22**。

| 文档 | 语言 | 读者 |
|---|---|---|
| [用户指南(中文)](user-guide.zh.md) | 中文 | 运维/使用者:概念、构建安装、快速上手、配置参考、告警、指标、控制平面、恢复语义、部署、排障 |
| [User Guide (English)](user-guide.en.md) | English | 同上,英文版 |
| [开发者指南(中文)](developer-guide.zh.md) | 中文 | 贡献者:设计原则、架构与并发、模块地图、各子系统实现、feature/构建、测试、工作流纪律、扩展点 |
| [Developer Guide (English)](developer-guide.en.md) | English | 同上,英文版 |

## 阅读路径建议

- **只想用起来**:用户指南 §3–§4(构建与快速开始)→ §13(部署为系统服务)。
- **配置与调参**:用户指南 §5–§9;完整带注释模板见 [config/config.example.toml](../../config/config.example.toml)。
- **线上排障**:用户指南 §15 → [RUNBOOK](../RUNBOOK.md)(含 jq 配方与决策树全文)。
- **第一次读代码**:开发者指南 §1–§4(原则/架构/模块地图)→ 按 §4 的模块表定位源码。
- **改语义/加功能**:开发者指南 §7–§10(子系统)+ §16(扩展点)+ [DESIGN.md](../../DESIGN.md) 对应章节。

## 语义权威与冲突裁决

本目录为导览与整理性文档。语义如与下列权威冲突,按 `AGENT.md > DESIGN.md > ROADMAP.md > EXECUTION.md` 顺序裁决;运维操作细节以 [DEPLOYMENT.md](../DEPLOYMENT.md) 与 [RUNBOOK.md](../RUNBOOK.md) 为准。
