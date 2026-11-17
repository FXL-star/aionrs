# Aion CLI 本地工具扩展说明

本文整理自“Aion CLI 是否支持扩展本地工具”的源码问答，依据 2026-11-05 查阅的 `aionrs` 源码。文中的 Aion CLI 指当前仓库的 Rust CLI 与 Agent 引擎。

## 支持情况

Aion CLI 支持扩展本地工具，主要有两条路径：通过本地 MCP Server 接入工具，或在 Rust 代码中实现并注册原生工具。

| 需求 | 接入方式 | 是否需要重新编译 CLI |
| --- | --- | --- |
| 将自己的 Python、Node 或 Java 程序作为独立工具提供给模型 | 本地 MCP Server，通过配置接入 | 不需要 |
| 在 Rust 引擎中直接执行自定义工具逻辑 | 实现 `Tool` 并注册到 `ToolRegistry` | 修改现成 CLI 时需要；嵌入式使用时编译自己的集成程序 |
| 让模型运行已有命令或脚本 | 内置 `ExecCommand`，可配合 Skill 说明用法 | 不需要 |

需要独立工具名和结构化参数，并希望独立维护工具程序时，优先采用本地 MCP。

## 方式一：本地 MCP Server

### 配置示例

在运行项目的 `.aionrs.toml` 中加入以下配置，将示例路径替换为实际路径：

```toml
[mcp.servers.my_tools]
transport = "stdio"
command = "python"
args = ["D:/tools/my_tools/server.py"]
deferred = false
```

`server.py` 需要是实现 MCP 协议的服务程序，普通脚本仅打印执行结果不足以完成接入。`python` 需要能从运行 CLI 的环境中找到，也可以改为解释器的完整路径。

各字段的含义：

- `my_tools`：这组 MCP 服务的配置名称。
- `transport = "stdio"`：由 CLI 启动本地子进程，通过标准输入、标准输出通信。
- `command` 和 `args`：启动程序及其参数，可以换成 Node、Java 或其他可执行程序。
- `deferred = false`：启动时提供完整工具 Schema，便于模型直接发现工具。当前源码在省略此字段时默认启用延迟加载，可通过 `ToolSearch` 按需发现工具。

也可以配置到全局配置文件；运行 `aionrs config path` 可查看全局配置文件位置。配置层级见 [入门文档](getting-started.md)。

### 运行流程

1. CLI 读取 MCP 配置并启动本地服务进程。
2. 完成 MCP 初始化握手，通过 `tools/list` 获取工具列表。
3. 将服务提供的工具包装为 `McpToolProxy`，注册到工具注册表。
4. 模型选择工具并提供结构化参数。
5. 工具代理通过 MCP 发起 `tools/call`，将执行结果交回 Agent。

这条路径已经在当前源码中接通，无需为每个自定义 MCP 工具修改 CLI。

| 源码入口 | 作用 |
| --- | --- |
| [`McpServerConfig`](../crates/aion-config/src/config.rs#L51) | 定义传输方式、启动命令、参数、环境变量及延迟加载配置 |
| [`StdioTransport::spawn`](../crates/aion-mcp/src/transport/stdio.rs#L22) | 启动本地进程并建立标准输入、输出管道 |
| [`McpManager`](../crates/aion-mcp/src/manager.rs#L137) | 建立连接、初始化并发现工具 |
| [`AgentBootstrap::connect_mcp`](../crates/aion-agent/src/bootstrap.rs#L245) | 在引擎初始化期间连接 MCP 并注册工具 |
| [`McpToolProxy::execute`](../crates/aion-mcp/src/tool_proxy.rs#L75) | 将工具执行请求转交给对应 MCP 服务 |

更多传输方式和配置示例见 [MCP 接入文档](mcp.md)。默认值应以当前源码为准。

## 方式二：原生 Rust 工具

如果需要在引擎进程内实现工具，可以实现 [`Tool` 接口](../crates/aion-tools/src/tool.rs#L44)。接口要求提供：

- `name()`：工具名称。
- `description()`：供模型理解用途的说明。
- `input_schema()`：输入参数的 JSON Schema。
- `is_concurrency_safe()`：是否允许并发执行。
- `execute()`：实际执行逻辑，返回 `ToolResult`。
- `category()`：工具分类。

完成实现后，可以通过引擎公开的注册表入口注册。下面是注册位置的示意，假设已有可变的 `engine`，且 `MyTool` 已实现上述接口：

```rust
engine.registry_mut().register(Box::new(MyTool::new()));
```

相关源码入口：

- [`AgentEngine::registry_mut`](../crates/aion-agent/src/engine.rs#L362)：取得引擎工具注册表的可变引用。
- [`ToolRegistry::register`](../crates/aion-tools/src/registry.rs#L19)：接收并保存 `Box<dyn Tool>`。
- [`AgentBootstrap::build_builtin_registry`](../crates/aion-agent/src/bootstrap.rs#L220)：现有内置工具的集中注册位置，可作为修改 CLI 时的接入参考。

这种方式属于源码或嵌入式集成。修改现成 CLI 的内置工具集合需要加入实现、接入注册流程并重新编译；作为库嵌入时，需要编译自己的集成程序。

## 已有脚本与插件加载的边界

如果只是希望模型执行已有本地脚本，可以直接使用内置 `ExecCommand`，再通过 Skill 描述脚本用途、参数和操作步骤。执行入口仍然是 `ExecCommand`；Skill 本身不会把脚本注册成具有独立参数 Schema 的新工具。

当前查阅的源码中未发现“把脚本或 DLL 放入工具目录即可自动注册为原生工具”的通用插件加载机制。可配置接入的独立工具优先使用 MCP，进程内工具则通过 Rust 接口实现与注册。

相关说明见 [内置工具](tools.md) 和 [Skills](skills.md)。

## 核查范围

上述结论来自配置结构、工具接口、注册入口和 MCP 执行链路的静态源码核查。配置中的 `server.py` 与代码中的 `MyTool` 均为接入示意，本文未创建或运行这些工具。
