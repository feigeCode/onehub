# OneHub

OneHub 是一款基于 Rust + GPUI 构建的现代化多协议连接工具。它支持数据库连接（MySQL / PostgreSQL / SQLite 等）、SSH 连接、Redis、MongoDB 以及 LLM 服务（OpenAI, Claude, Ollama, Qwen, DeepSeek 等）等多种协议，旨在为开发者提供统一、快速、稳定的连接与管理体验。

## Features

- **多协议支持**: 支持数据库连接（MySQL / PostgreSQL / SQLite 等）、SSH 连接、Redis、MongoDB 等多种协议。
- **LLM 集成**: 内置对多种大语言模型服务的支持（OpenAI, Claude, Ollama, Qwen, DeepSeek 等）。
- **现代化界面**: 基于 GPUI 构建，提供流畅的用户体验。
- **高性能**: 利用 Rust 的性能优势，确保稳定高效的连接管理。
- **统一管理**: 提供统一的界面来管理各种类型的连接和服务。
- **跨平台**: 支持在多个操作系统上运行。
- **可扩展架构**: 模块化设计，易于扩展新的连接类型和服务。

## Development Todo

Here's a checklist of features and enhancements planned for OneHub:

- [x] 基础应用框架 (Application Framework)
- [x] 统一标签页管理 (Tab Container)
- [x] 主页界面 (Home Interface)
- [x] 设置面板 (Settings Panel)
- [ ] 数据库连接支持 (MySQL, PostgreSQL, SQLite)
- [ ] SSH 连接功能
- [ ] Redis 连接管理
- [ ] MongoDB 连接支持
- [ ] LLM 服务连接 (OpenAI, Claude, Ollama, Qwen, DeepSeek)
- [ ] 连接配置保存与加载
- [ ] 连接历史记录管理
- [ ] 安全认证机制
- [ ] 用户自定义主题
- [ ] 详细的文档和教程

## Architecture

OneHub 采用模块化架构设计：

- **main**: 主应用程序入口点，负责初始化应用和创建主窗口
- **crates/core**: 核心业务逻辑和数据结构
- **crates/ui**: UI 组件和界面相关功能
- **apps/db**: 数据库连接相关功能
- **crates/provider-***: 不同 LLM 服务提供商的实现
- **examples**: 各种功能示例和演示

## Building

要构建 OneHub，请确保您已安装最新版本的 Rust：

```bash
# 克隆仓库后，使用 cargo 构建
cargo build --release
```

## License

The primary license used by this software is the Apache License 2.0, supplemented by the OneHub License.

Apache-2.0
