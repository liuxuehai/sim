# Sim Workflow 系统设计说明

## 项目概述

Sim 是一个基于可视化画布的 AI Agent 工作流构建平台，允许用户通过拖拽的方式设计、连接和运行复杂的 AI 工作流。该系统采用现代化的技术栈，提供了完整的工作流生命周期管理，从设计到部署再到执行。

## 核心架构

### 1. 技术栈

- **前端框架**: Next.js (App Router)
- **运行时**: Bun
- **数据库**: PostgreSQL + Drizzle ORM + pgvector
- **状态管理**: Zustand
- **流程编辑器**: ReactFlow
- **实时通信**: Socket.io
- **后台任务**: Trigger.dev
- **代码执行**: E2B

### 2. 项目结构

```
apps/sim/
├── app/                    # Next.js 应用路由
├── blocks/                 # 工作流组件定义
├── executor/               # 工作流执行引擎
├── lib/workflows/          # 工作流核心逻辑
├── stores/                 # 状态管理
├── tools/                  # 集成工具库
├── triggers/               # 触发器定义
└── socket-server/          # 实时协作服务
```

## 工作流系统设计

### 1. 核心概念

#### Block（组件）

- **定义**: 工作流的基本执行单元，每个Block代表一个特定的功能
- **类型**:
  - `blocks`: 核心逻辑组件（Agent、条件判断、循环等）
  - `tools`: 集成工具（API调用、数据库操作等）
  - `triggers`: 触发器（手动触发、定时任务、Webhook等）

#### Edge（连接）

- **定义**: 连接Block之间的数据流和控制流
- **特性**: 支持条件分支、错误处理、循环控制

#### Workflow（工作流）

- **定义**: 由多个Block和Edge组成的完整执行图
- **执行模式**: 基于DAG（有向无环图）的执行引擎

### 2. 组件系统架构

#### Block配置结构

```typescript
interface BlockConfig {
  type: string                    // 组件类型标识
  name: string                    // 显示名称
  description: string             // 组件描述
  category: BlockCategory         // 分类：blocks/tools/triggers
  icon: BlockIcon                 // 图标组件
  subBlocks: SubBlockConfig[]     // 子配置项
  tools: {                        // 工具配置
    access: string[]              // 可访问的工具列表
    config?: {                    // 动态配置
      tool: (params) => string    // 工具选择逻辑
      params?: (params) => object // 参数转换逻辑
    }
  }
  inputs: Record<string, ParamConfig>     // 输入参数定义
  outputs: Record<string, OutputConfig>   // 输出结果定义
}
```

#### SubBlock系统

SubBlock提供了丰富的UI组件类型：

- `short-input/long-input`: 文本输入
- `dropdown/combobox`: 选择器
- `code`: 代码编辑器
- `oauth-input`: OAuth认证
- `tool-input`: 工具配置
- `condition-input`: 条件逻辑
- `file-upload`: 文件上传
- `messages-input`: 消息历史输入

### 3. Agent组件设计

Agent是系统的核心组件，具有以下特性：

#### 核心功能

- **LLM集成**: 支持多种AI模型（OpenAI、Anthropic、Google等）
- **工具调用**: 可以调用其他集成工具
- **结构化输出**: 支持JSON Schema定义的响应格式
- **记忆管理**: 支持对话记忆、滑动窗口记忆
- **温度控制**: 可调节响应的随机性

#### 配置项

```typescript
// 主要配置项
{
  messages: [],              // 消息历史
  model: 'claude-sonnet-4-5', // AI模型
  tools: [],                 // 可用工具列表
  responseFormat: {},        // 响应格式定义
  temperature: 0.3,          // 温度参数
  memoryType: 'none',        // 记忆类型
  conversationId: '',        // 对话ID
}
```

## 执行引擎设计

### 1. DAG执行器（DAGExecutor）

#### 核心职责

- 将工作流转换为DAG结构
- 管理Block的执行顺序和依赖关系
- 处理并行执行和循环控制
- 提供执行状态监控和错误处理

#### 执行流程

```typescript
class DAGExecutor {
  async execute(workflowId: string, triggerBlockId?: string): Promise<ExecutionResult> {
    // 1. 构建DAG
    const dag = this.dagBuilder.build(this.workflow, triggerBlockId)

    // 2. 创建执行上下文
    const { context, state } = this.createExecutionContext(workflowId, triggerBlockId)

    // 3. 初始化执行组件
    const resolver = new VariableResolver(...)
    const blockExecutor = new BlockExecutor(...)
    const engine = new ExecutionEngine(...)

    // 4. 开始执行
    return await engine.run(triggerBlockId)
  }
}
```

### 2. 执行引擎（ExecutionEngine）

#### 队列管理

- **就绪队列**: 管理可执行的Block
- **执行跟踪**: 监控正在执行的Block
- **依赖解析**: 处理Block间的依赖关系

#### 执行策略

- **并发执行**: 支持无依赖Block的并行执行
- **错误处理**: 提供完整的错误传播和恢复机制
- **暂停恢复**: 支持人工干预和断点续传

### 3. 编排器系统

#### LoopOrchestrator（循环编排器）

- 支持多种循环类型：`for`、`forEach`、`while`、`doWhile`
- 管理循环变量和迭代状态
- 处理循环内部的并行执行

#### ParallelOrchestrator（并行编排器）

- 支持集合并行和计数并行
- 管理分支执行状态
- 合并并行分支的执行结果

## 状态管理系统

### 1. WorkflowStore（工作流状态）

#### 核心状态

```typescript
interface WorkflowState {
  blocks: Record<string, Block>      // Block集合
  edges: Edge[]                      // 连接关系
  loops: Record<string, LoopBlock>   // 循环Block
  parallels: Record<string, ParallelBlock> // 并行Block
  lastSaved: number                  // 最后保存时间
  deploymentStatuses: Record<string, DeploymentStatus> // 部署状态
}
```

#### 主要操作

- `addBlock/removeBlock`: Block的增删操作
- `addEdge/removeEdge`: 连接的管理
- `updateBlockPosition`: 位置更新
- `toggleBlockEnabled`: 启用/禁用Block
- `duplicateBlock`: Block复制

### 2. SubBlockStore（子组件状态）

管理每个Block内部SubBlock的值状态：

```typescript
interface SubBlockStore {
  workflowValues: {
    [workflowId: string]: {
      [blockId: string]: {
        [subBlockId: string]: any
      }
    }
  }
}
```

### 3. ExecutionStore（执行状态）

跟踪工作流执行过程中的状态：

- 执行日志
- Block执行状态
- 错误信息
- 执行路径

## 实时协作系统

### 1. Socket.io集成

#### 事件类型

- `workflow:update`: 工作流更新
- `block:add/remove/update`: Block操作
- `edge:add/remove`: 连接操作
- `execution:start/complete/error`: 执行状态

#### 协作机制

- **操作同步**: 实时同步用户操作
- **冲突解决**: 基于时间戳的冲突处理
- **状态一致性**: 确保多用户间状态同步

### 2. 版本控制

#### Diff系统

- **状态比较**: 比较工作流的不同版本
- **变更追踪**: 跟踪Block和Edge的变更
- **可视化差异**: 在画布上高亮显示变更

## 部署和执行

### 1. 部署流程

#### 状态序列化

```typescript
function buildWorkflowStateForTemplate(workflowId: string) {
  const { blocks, edges } = workflowStore
  const loops = workflowStore.generateLoopBlocks()
  const parallels = workflowStore.generateParallelBlocks()

  return {
    blocks,
    edges,
    loops,
    parallels,
    lastSaved: Date.now(),
  }
}
```

#### 部署验证

- 检查Block配置完整性
- 验证连接关系有效性
- 确保必需参数已配置

### 2. 执行模式

#### 手动执行

- 用户主动触发
- 支持指定起始Block
- 提供实时执行反馈

#### 自动触发

- Webhook触发
- 定时任务触发
- 事件驱动触发

#### 调试模式

- 断点设置
- 单步执行
- 状态检查

## 扩展性设计

### 1. Block扩展

#### 新Block开发

```typescript
export const CustomBlock: BlockConfig = {
  type: 'custom',
  name: 'Custom Block',
  description: 'Custom functionality',
  category: 'blocks',
  icon: CustomIcon,
  subBlocks: [...],
  tools: { access: [...] },
  inputs: {...},
  outputs: {...}
}
```

#### 注册机制

```typescript
// 在registry.ts中注册
export const registry: Record<string, BlockConfig> = {
  // ... 其他Block
  custom: CustomBlock,
}
```

### 2. 工具集成

#### 工具开发

- 标准化的工具接口
- 参数验证和类型检查
- 错误处理和重试机制

#### 认证集成

- OAuth 2.0支持
- API Key管理
- 多种认证模式

### 3. 触发器扩展

#### 自定义触发器

- Webhook集成
- 第三方服务集成
- 事件监听机制

## 性能优化

### 1. 执行优化

#### 并行执行

- 无依赖Block的并发执行
- 资源池管理
- 执行队列优化

#### 缓存机制

- Block输出缓存
- 工具调用缓存
- 状态快照缓存

### 2. 前端优化

#### 虚拟化渲染

- 大型工作流的性能优化
- 按需加载Block组件
- 画布渲染优化

#### 状态管理优化

- 增量更新
- 选择性重渲染
- 内存管理

## 安全性考虑

### 1. 数据安全

#### 敏感信息保护

- API Key加密存储
- 传输过程加密
- 访问权限控制

#### 执行沙箱

- 代码执行隔离
- 资源访问限制
- 恶意代码防护

### 2. 权限管理

#### 工作区权限

- 多级权限控制
- 协作权限管理
- 资源访问控制

## 总结

Sim的工作流系统采用了模块化、可扩展的设计架构，通过DAG执行引擎、组件化Block系统、实时协作机制等核心技术，提供了一个强大而灵活的AI工作流构建平台。系统支持从简单的单一Agent到复杂的多步骤、多分支工作流，能够满足各种AI应用场景的需求。

关键设计亮点：

1. **组件化架构**: 通过Block和SubBlock系统实现高度可扩展性
2. **DAG执行引擎**: 提供高效、可靠的工作流执行能力
3. **实时协作**: 支持多用户同时编辑和协作
4. **丰富的集成**: 内置100+工具和服务集成
5. **可视化设计**: 直观的拖拽式工作流设计体验

这种设计使得Sim能够成为一个功能强大、易于使用的AI工作流平台，适合从个人开发者到企业级应用的各种使用场景。
