# Hermes Rust 完善计划

基于原版 Python Hermes 的完整功能清单，制定 Rust 版本的系统性完善路线图。

**状态更新时间**: 2026-06-29 20:50 CST

---

## 原版 Python Hermes 架构概览

- **代码规模**: 4339 个 Python 文件，377MB
- **核心模块**: agent, gateway, tools, skills, providers, cron, mcp, acp, tui, web, plugins
- **平台适配器**: 17+ 个消息平台（Telegram, Discord, QQ, WeChat, Signal 等）
- **工具数量**: 30+ 内置工具
- **部署方式**: CLI, Gateway, TUI, Web UI, Docker

---

## 当前 Rust 实现状态总结

### ✅ 已完成核心功能（13/13 Parity Items）

1. **Agent Loop** - 完整实现，包括多轮对话、工具调用、上下文管理
2. **LLM Providers** - 10+ 提供商支持（Anthropic, OpenAI, OpenRouter, Custom 等）
3. **Tool System** - 30 个工具已实现
4. **Gateway** - 17 个平台适配器框架已搭建
5. **Memory System** - 文件存储 + 8 个外部插件支持
6. **Skills** - 文件存储 + hub 客户端
7. **MCP** - 客户端和服务器实现
8. **Cron** - 调度和持久化
9. **Config** - YAML/JSON/env 合并加载
10. **Intelligence** - 智能路由、prompt 构建、使用统计
11. **Environments** - 6 种终端后端（Local, Docker, SSH, Daytona, Modal, Singularity）
12. **CLI/TUI** - 22 个斜杠命令
13. **Parity Tests** - Golden fixture 测试框架

### 🚧 部分实现（需完善）

#### QQ Bot Gateway（优先级：🔴 Critical）
- ✅ WebSocket 连接和消息接收
- ✅ C2C 流式协议（msg_type=2 Markdown）
- ✅ Markdown 渲染锁定（避免 40034016 错误）
- ✅ 元数据 footer（model/provider/timing）
- ✅ Stream end notice（可配置）
- 🔴 **Typing indicator**（已实现未测试）
- 🔴 **工具调用进度反馈**（缺失）
- 🟡 Progress card 实现（空壳）
- 🟡 流式并发冲突修复（40034021 错误）
- 🟢 文件/图片/语音上传（adapter 已有占位，未实现）
- 🟢 Keyboard 按钮支持（原版有 keyboards.py）

#### 其他平台适配器（优先级：🟢 Low）
- 大部分平台只有基础框架，需要参考 Python 实现完善
- Telegram, Discord, Signal 等主流平台优先

#### Agent Loop 增强（优先级：🟡 Medium）
- 🟡 工具执行期间的进度回调（critical for QQ bot）
- 🟡 流式输出中断控制（部分实现）
- 🟢 Background review（nudge counters 已实现）
- 🟢 Checkpoint/rollback on tool errors（已实现）

### ❌ 缺失功能

#### 1. TUI/Web UI（优先级：🟢 Low）
- 原版有完整的 TUI（ui-tui/）和 Web 界面（web/）
- Rust 版本目前仅支持 CLI 和 Gateway
- 需要评估是否使用 ratatui 重新实现

#### 2. 插件系统（优先级：🟡 Medium）
- 原版 `plugins/` 目录支持动态加载 Python 插件
- Rust 版本需要设计插件架构（可能基于 WASM 或动态库）

#### 3. 数据生成/评估工具（优先级：🟢 Low）
- 原版 `datagen-config-examples/` 和评估脚本
- Rust 有 `hermes-eval` crate 但功能较弱

#### 4. 多语言支持（优先级：🟢 Low）
- 原版 `locales/` 目录支持 i18n
- Rust 版本目前硬编码英文

#### 5. Web 服务器增强（优先级：🟡 Medium）
- `hermes-server` crate 有基础 HTTP/WebSocket
- 缺少原版的 `/chat`, `/tools`, `/skills` 等 REST API

---

## 完善路线图

### Phase 1: QQ Bot Gateway 完善（1-2 周）

**目标**: 让 QQ bot 体验达到原版 Python 水平

#### Week 1: 工具调用反馈
- [ ] **Day 1-2**: 实现工具调用进度回调
  - 在 `GatewayRuntimeContext` 添加进度回调字段
  - `AgentLoop::execute_tool_calls()` 报告工具开始/完成
  - Gateway 累积进度行并调用 `send_progress_card()`
- [ ] **Day 3**: 实现 QQ bot `send_progress_card()`
  - 参考 Python adapter.py:802-839
  - 实现进度去重和防抖（`max_progress_messages: 2`）
- [ ] **Day 4**: 修复流式并发冲突
  - 分析 40034021 错误原因（"其它流式消息发送中"）
  - 改进 `c2c_stream_state` 锁管理和队列机制
- [ ] **Day 5-6**: 测试和调优
  - 长时间 QQ 交互测试（多工具、多轮对话）
  - 监控 40034016, 40034021, 40054018 错误
- [ ] **Day 7**: Typing indicator 测试和修复

#### Week 2: 媒体和高级功能
- [ ] **Day 1-2**: 文件上传实现
  - 图片、语音、文档上传到 QQ
  - 参考 Python `chunked_upload.py` 和 `crypto.py`
- [ ] **Day 3**: Keyboard 按钮支持
  - 参考 Python `keyboards.py`
  - 实现 inline keyboard 和 reply markup
- [ ] **Day 4-5**: QQ group 支持完善
  - 群组消息处理优化
  - @提及检测和响应
- [ ] **Day 6-7**: 压力测试和优化
  - 并发用户测试
  - 内存和 CPU 优化

**验收标准**:
- ✅ QQ 用户看到"正在输入"指示器
- ✅ 工具调用期间显示进度卡片（例如："🔧 web_search"）
- ✅ 流式输出无 40034021 错误
- ✅ 可以发送图片、文件、语音
- ✅ Inline 按钮正常工作

---

### Phase 2: 其他平台适配器完善（2-3 周）

**目标**: Telegram, Discord, Signal 等主流平台达到生产可用

#### Week 3: Telegram
- [ ] 完善 Telegram adapter
  - 参考 Python `gateway/platforms/telegram/adapter.py`
  - 实现所有消息类型（text, photo, document, voice, video）
  - Inline keyboard 和 callback query
  - 文件上传/下载
- [ ] 测试 Telegram bot 核心流程

#### Week 4: Discord
- [ ] 完善 Discord adapter
  - 参考 Python `gateway/platforms/discord/adapter.py`
  - Slash commands 支持
  - Embed 消息
  - 文件上传
  - Thread 支持
- [ ] 测试 Discord bot 核心流程

#### Week 5: Signal 和其他
- [ ] Signal adapter 完善
- [ ] WhatsApp adapter 完善
- [ ] Matrix adapter 完善
- [ ] 其他平台按优先级逐步完善

**验收标准**:
- ✅ Telegram/Discord/Signal 基础消息收发
- ✅ 流式输出和工具调用反馈
- ✅ 文件和媒体处理
- ✅ 平台特有功能（buttons, threads, embeds）

---

### Phase 3: TUI 实现（2-3 周）

**目标**: 使用 ratatui 实现终端 UI

#### Week 6-7: 核心 TUI 框架
- [ ] **Day 1-3**: ratatui 基础架构
  - 主界面布局（chat, status, input）
  - 事件循环和键盘快捷键
  - 多会话管理
- [ ] **Day 4-5**: 消息渲染
  - Markdown 渲染（使用 comrak 或 termimad）
  - 语法高亮（代码块）
  - 工具调用和结果显示
- [ ] **Day 6-7**: 流式输出
  - 实时 token 更新
  - 进度条和 spinner
- [ ] **Day 8-10**: 交互功能
  - 斜杠命令补全
  - 历史记录浏览
  - 会话切换
- [ ] **Day 11-14**: 抛光和测试
  - 性能优化（避免闪烁）
  - 错误处理和恢复
  - 用户测试和反馈

#### Week 8: 高级 TUI 功能
- [ ] 工具批准界面（approve/deny 工具调用）
- [ ] 配置界面（model, personality, tools）
- [ ] 调试面板（日志、指标）
- [ ] 多窗格支持（side-by-side）

**验收标准**:
- ✅ 流畅的终端交互体验
- ✅ Markdown 和代码渲染正确
- ✅ 流式输出实时更新
- ✅ 支持所有 CLI 功能

---

### Phase 4: Web UI 和 REST API（3-4 周）

**目标**: 提供浏览器访问和 HTTP API

#### Week 9-10: REST API
- [ ] **hermes-server** 完善
  - `/chat` - 对话 API（SSE 流式）
  - `/tools` - 工具列表和调用
  - `/skills` - 技能管理
  - `/sessions` - 会话管理
  - `/config` - 配置读写
  - `/memory` - 记忆查询
- [ ] API 认证和授权（JWT 或 API key）
- [ ] WebSocket 实时通信
- [ ] OpenAPI/Swagger 文档生成

#### Week 11-12: Web UI（选项 A：使用现有前端）
- [ ] 调研原版 Python `web/` 目录的前端技术栈
- [ ] 适配 Rust API 端点
- [ ] 打包为静态资源嵌入 binary

#### Week 11-12: Web UI（选项 B：从头实现）
- [ ] 选择前端框架（推荐 Leptos/Dioxus for Rust，或 React/Vue）
- [ ] 实现聊天界面
- [ ] 实现配置界面
- [ ] 实现工具和技能管理界面

**验收标准**:
- ✅ RESTful API 完整可用
- ✅ Web UI 可以进行基本对话
- ✅ 流式输出在浏览器中实时显示
- ✅ 可以管理工具、技能、配置

---

### Phase 5: 插件系统和扩展性（2-3 周）

**目标**: 支持第三方扩展

#### Week 13-14: 插件架构设计
- [ ] 调研插件方案：
  - Option 1: WASM（安全沙箱，跨平台）
  - Option 2: 动态库（性能好，但平台相关）
  - Option 3: 外部进程 + IPC（隔离性强）
- [ ] 设计插件 API 接口
- [ ] 实现插件加载和生命周期管理
- [ ] 插件示例和文档

#### Week 15: 插件生态
- [ ] 移植几个常用 Python 插件到 Rust
- [ ] 插件仓库和发现机制
- [ ] 插件安全审计工具

**验收标准**:
- ✅ 可以加载和卸载插件
- ✅ 插件可以注册工具、技能、平台适配器
- ✅ 至少 3 个示例插件可用

---

### Phase 6: 性能优化和生产强化（2 周）

**目标**: 生产级稳定性和性能

#### Week 16: 性能优化
- [ ] Profiling 和性能基准测试
- [ ] 内存泄漏检测和修复
- [ ] 异步任务调优
- [ ] 数据库查询优化（SQLite）
- [ ] 编译优化（LTO, PGO）

#### Week 17: 生产强化
- [ ] 完整错误处理和恢复
- [ ] 日志结构化和分级
- [ ] 监控指标完善（Prometheus）
- [ ] 分布式追踪（OpenTelemetry）
- [ ] 健康检查和优雅关闭
- [ ] Docker 镜像优化
- [ ] CI/CD 流水线完善

**验收标准**:
- ✅ 内存占用 < 50MB（idle）
- ✅ 无内存泄漏
- ✅ 99%ile 响应延迟 < 100ms
- ✅ 7x24 稳定运行
- ✅ 完整的可观测性

---

### Phase 7: 生态和文档（持续）

**目标**: 社区友好，易于上手

#### 文档
- [ ] 用户手册（安装、配置、使用）
- [ ] 开发者指南（架构、贡献流程）
- [ ] API 参考文档
- [ ] 插件开发指南
- [ ] 常见问题 FAQ

#### 示例和教程
- [ ] 快速开始示例
- [ ] 平台部署指南（Docker, K8s, systemd）
- [ ] 自定义工具开发教程
- [ ] 自定义平台适配器教程

#### 测试
- [ ] 单元测试覆盖率 > 80%
- [ ] 集成测试套件
- [ ] E2E 测试（真实平台）
- [ ] Parity 测试扩展

---

## 优先级矩阵

| 功能模块 | 优先级 | 预估工作量 | 用户价值 | 技术风险 |
|---------|-------|----------|---------|---------|
| QQ Bot 工具进度反馈 | 🔴 P0 | 3-5 天 | 极高 | 低 |
| QQ Bot 流式并发修复 | 🔴 P0 | 2-3 天 | 极高 | 中 |
| QQ Bot 媒体上传 | 🟡 P1 | 5-7 天 | 高 | 中 |
| Telegram 完善 | 🟡 P1 | 7-10 天 | 高 | 低 |
| Discord 完善 | 🟡 P1 | 7-10 天 | 高 | 低 |
| TUI 实现 | 🟡 P2 | 14-21 天 | 中 | 中 |
| Web UI | 🟢 P2 | 14-21 天 | 中 | 低 |
| REST API 完善 | 🟢 P2 | 10-14 天 | 中 | 低 |
| 插件系统 | 🟢 P3 | 14-21 天 | 低 | 高 |
| 多语言 i18n | 🟢 P3 | 5-7 天 | 低 | 低 |

---

## 技术债务清单

### 当前已知问题
1. **流式冲突** - QQ bot 40034021 错误
2. **工具进度缺失** - 工具执行期间无反馈
3. **平台适配器不完整** - 大部分只有基础框架
4. **错误处理不统一** - 部分错误被吞没或 panic
5. **日志级别混乱** - debug/info/warn 边界不清晰
6. **测试覆盖率低** - 主要集中在 core，其他 crate 较少

### 架构改进方向
1. **工具系统解耦** - 考虑将工具从 `hermes-tools` 移到独立 crate
2. **平台适配器插件化** - 支持外部平台适配器
3. **配置热重载** - 无需重启即可更新配置
4. **分布式支持** - 多实例协同和负载均衡
5. **存储抽象** - 支持 PostgreSQL, Redis 等后端

---

## 资源参考

### 原版 Python 代码位置（Armbian）
- 主代码：`/root/.hermes/hermes-agent/`
- QQ Bot adapter：`/root/.hermes/hermes-agent/gateway/platforms/qqbot/adapter.py`
- Gateway 主循环：`/root/.hermes/hermes-agent/gateway/run.py`
- 工具定义：`/root/.hermes/hermes-agent/tools/`

### Rust 代码结构
- QQ Bot adapter：`crates/hermes-gateway/src/platforms/qqbot.rs`
- Gateway 主逻辑：`crates/hermes-gateway/src/gateway.rs`
- Agent loop：`crates/hermes-agent/src/agent_loop.rs`
- 工具注册：`crates/hermes-tools/src/register_builtins.rs`

### 关键 Python 参考文件
- `gateway/platforms/qqbot/adapter.py` (3564 行) - QQ bot 完整实现
- `gateway/platforms/base.py` - 平台适配器基类
- `gateway/run.py` - Gateway 主循环和消息路由
- `agent/agent_loop.py` - Agent 核心逻辑
- `tools/__init__.py` - 工具发现和注册

---

## 近期行动计划（本周）

### 2026-06-30 Mon
- [ ] 测试 typing indicator 功能
- [ ] 实现工具调用进度回调（`GatewayRuntimeContext` 扩展）
- [ ] `AgentLoop::execute_tool_calls()` 报告工具开始/完成

### 2026-07-01 Tue
- [ ] Gateway 累积进度行
- [ ] 实现 QQ bot `send_progress_card()`
- [ ] 本地测试工具进度显示

### 2026-07-02 Wed
- [ ] 分析流式并发冲突原因
- [ ] 设计流式状态管理方案
- [ ] 实现流式队列或锁优化

### 2026-07-03 Thu
- [ ] 修复 40034021 错误
- [ ] 压力测试流式输出
- [ ] 长时间 QQ 对话测试

### 2026-07-04 Fri
- [ ] 部署到 Armbian 测试
- [ ] 监控生产日志
- [ ] Bug 修复和调优

### 2026-07-05 Sat-Sun
- [ ] 代码审查和重构
- [ ] 文档更新
- [ ] 下周计划制定

---

## 成功指标

### 短期目标（1 个月内）
- ✅ QQ Bot 体验无缝，工具调用有反馈
- ✅ Telegram/Discord 基本可用
- ✅ 无 40034021 等流式错误
- ✅ 内存占用稳定 < 50MB

### 中期目标（3 个月内）
- ✅ TUI 可用，体验流畅
- ✅ REST API 完整，Web UI 基础可用
- ✅ 至少 5 个平台适配器生产就绪
- ✅ 测试覆盖率 > 70%

### 长期目标（6 个月内）
- ✅ 插件系统稳定
- ✅ 性能超越 Python 版本 2-3 倍
- ✅ 社区贡献者 > 10 人
- ✅ 生产部署 > 100 实例

---

## 结论

Rust 版 Hermes 已经完成了核心功能的骨架，接下来的重点是：

1. **立即行动**：完善 QQ Bot 工具进度反馈（P0）
2. **短期重点**：主流平台适配器完善（Telegram, Discord）
3. **中期目标**：TUI 和 Web UI 实现
4. **长期愿景**：插件生态和性能优化

整个计划预计 4-6 个月完成，可以根据实际进展和用户反馈动态调整优先级。
