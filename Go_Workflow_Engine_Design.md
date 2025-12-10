# Go语言工作流执行引擎设计方案

## 项目概述

基于对Sim工作流系统的分析，设计一个用Go语言实现的高性能工作流执行引擎。该引擎将替换现有的TypeScript执行器，提供更好的性能、并发处理能力和资源管理，同时保持与现有前端系统的完全兼容。

## 整体架构设计

### 1. 系统架构图

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │   API Gateway   │    │  Go Workflow    │
│   (React/TS)    │◄──►│   (Next.js)     │◄──►│    Engine       │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │   PostgreSQL    │    │     Redis       │
                       │   (Workflow     │    │   (Cache &      │
                       │    Metadata)    │    │    Queue)       │
                       └─────────────────┘    └─────────────────┘
```

### 2. 技术栈选择

- **核心语言**: Go 1.21+
- **Web框架**: Gin/Fiber
- **数据库**: PostgreSQL (兼容现有schema)
- **缓存/队列**: Redis
- **消息队列**: Redis Streams / RabbitMQ
- **配置管理**: Viper
- **日志**: Zap
- **监控**: Prometheus + Grafana
- **容器化**: Docker + Kubernetes

## 核心模块设计

### 1. 项目结构

```
workflow-engine/
├── cmd/
│   ├── server/          # HTTP服务器入口
│   ├── worker/          # 工作流执行器
│   └── cli/             # 命令行工具
├── internal/
│   ├── api/             # HTTP API处理
│   ├── engine/          # 工作流执行引擎
│   ├── blocks/          # Block实现
│   ├── dag/             # DAG构建和管理
│   ├── executor/        # 执行器实现
│   ├── storage/         # 数据存储层
│   ├── queue/           # 队列管理
│   ├── websocket/       # WebSocket实时通信
│   └── config/          # 配置管理
├── pkg/
│   ├── types/           # 公共类型定义
│   ├── utils/           # 工具函数
│   └── errors/          # 错误处理
├── deployments/         # 部署配置
├── scripts/             # 构建脚本
└── docs/                # 文档
```

### 2. 核心类型定义

```go
// pkg/types/workflow.go
package types

import (
    "encoding/json"
    "time"
)

// Workflow 工作流定义
type Workflow struct {
    ID          string                 `json:"id" db:"id"`
    Name        string                 `json:"name" db:"name"`
    Description string                 `json:"description" db:"description"`
    Blocks      map[string]*Block      `json:"blocks" db:"blocks"`
    Edges       []*Edge                `json:"edges" db:"edges"`
    Loops       map[string]*LoopBlock  `json:"loops" db:"loops"`
    Parallels   map[string]*ParallelBlock `json:"parallels" db:"parallels"`
    Variables   map[string]interface{} `json:"variables" db:"variables"`
    CreatedAt   time.Time              `json:"created_at" db:"created_at"`
    UpdatedAt   time.Time              `json:"updated_at" db:"updated_at"`
}

// Block 工作流块定义
type Block struct {
    ID            string                 `json:"id"`
    Type          string                 `json:"type"`
    Name          string                 `json:"name"`
    Position      Position               `json:"position"`
    SubBlocks     map[string]*SubBlock   `json:"subBlocks"`
    Outputs       map[string]interface{} `json:"outputs"`
    Enabled       bool                   `json:"enabled"`
    TriggerMode   bool                   `json:"triggerMode"`
    AdvancedMode  bool                   `json:"advancedMode"`
    Data          map[string]interface{} `json:"data"`
}

// SubBlock 子块配置
type SubBlock struct {
    ID    string      `json:"id"`
    Type  string      `json:"type"`
    Value interface{} `json:"value"`
}

// Edge 连接边定义
type Edge struct {
    ID           string                 `json:"id"`
    Source       string                 `json:"source"`
    Target       string                 `json:"target"`
    SourceHandle string                 `json:"sourceHandle"`
    TargetHandle string                 `json:"targetHandle"`
    Type         string                 `json:"type"`
    Data         map[string]interface{} `json:"data"`
}

// ExecutionContext 执行上下文
type ExecutionContext struct {
    WorkflowID      string                 `json:"workflow_id"`
    ExecutionID     string                 `json:"execution_id"`
    UserID          string                 `json:"user_id"`
    WorkspaceID     string                 `json:"workspace_id"`
    Variables       map[string]interface{} `json:"variables"`
    Environment     map[string]string      `json:"environment"`
    StartTime       time.Time              `json:"start_time"`
    IsDeployed      bool                   `json:"is_deployed"`
    Stream          bool                   `json:"stream"`
    SelectedOutputs []string               `json:"selected_outputs"`
}

// ExecutionResult 执行结果
type ExecutionResult struct {
    Success     bool                   `json:"success"`
    Output      map[string]interface{} `json:"output"`
    Error       string                 `json:"error,omitempty"`
    Logs        []*ExecutionLog        `json:"logs"`
    Metadata    *ExecutionMetadata     `json:"metadata"`
    Status      string                 `json:"status"`
    PausePoints []*PausePoint          `json:"pause_points,omitempty"`
}
```

### 3. DAG构建器实现

```go
// internal/dag/builder.go
package dag

import (
    "fmt"
    "workflow-engine/pkg/types"
)

// DAGBuilder DAG构建器
type DAGBuilder struct {
    nodes map[string]*Node
    edges map[string]*Edge
}

// Node DAG节点
type Node struct {
    ID            string
    Block         *types.Block
    IncomingEdges map[string]*Edge
    OutgoingEdges map[string]*Edge
    Dependencies  []string
    Dependents    []string
}

// NewDAGBuilder 创建DAG构建器
func NewDAGBuilder() *DAGBuilder {
    return &DAGBuilder{
        nodes: make(map[string]*Node),
        edges: make(map[string]*Edge),
    }
}

// Build 构建DAG
func (b *DAGBuilder) Build(workflow *types.Workflow, triggerBlockID string) (*DAG, error) {
    // 1. 创建节点
    for blockID, block := range workflow.Blocks {
        node := &Node{
            ID:            blockID,
            Block:         block,
            IncomingEdges: make(map[string]*Edge),
            OutgoingEdges: make(map[string]*Edge),
            Dependencies:  []string{},
            Dependents:    []string{},
        }
        b.nodes[blockID] = node
    }

    // 2. 创建边和依赖关系
    for _, edge := range workflow.Edges {
        dagEdge := &Edge{
            ID:     edge.ID,
            Source: edge.Source,
            Target: edge.Target,
            Data:   edge.Data,
        }
        b.edges[edge.ID] = dagEdge

        // 建立依赖关系
        if sourceNode, exists := b.nodes[edge.Source]; exists {
            sourceNode.OutgoingEdges[edge.ID] = dagEdge
            sourceNode.Dependents = append(sourceNode.Dependents, edge.Target)
        }

        if targetNode, exists := b.nodes[edge.Target]; exists {
            targetNode.IncomingEdges[edge.ID] = dagEdge
            targetNode.Dependencies = append(targetNode.Dependencies, edge.Source)
        }
    }

    // 3. 验证DAG有效性
    if err := b.validateDAG(); err != nil {
        return nil, fmt.Errorf("invalid DAG: %w", err)
    }

    return &DAG{
        Nodes: b.nodes,
        Edges: b.edges,
    }, nil
}

// validateDAG 验证DAG有效性
func (b *DAGBuilder) validateDAG() error {
    visited := make(map[string]bool)
    recStack := make(map[string]bool)

    for nodeID := range b.nodes {
        if !visited[nodeID] {
            if b.hasCycle(nodeID, visited, recStack) {
                return fmt.Errorf("cycle detected in workflow")
            }
        }
    }
    return nil
}

// hasCycle 检查是否存在循环
func (b *DAGBuilder) hasCycle(nodeID string, visited, recStack map[string]bool) bool {
    visited[nodeID] = true
    recStack[nodeID] = true

    node := b.nodes[nodeID]
    for _, dependent := range node.Dependents {
        if !visited[dependent] {
            if b.hasCycle(dependent, visited, recStack) {
                return true
            }
        } else if recStack[dependent] {
            return true
        }
    }

    recStack[nodeID] = false
    return false
}
```

### 4. 执行引擎实现

```go
// internal/engine/executor.go
package engine

import (
    "context"
    "fmt"
    "sync"
    "time"
    "workflow-engine/internal/dag"
    "workflow-engine/pkg/types"
)

// WorkflowExecutor 工作流执行器
type WorkflowExecutor struct {
    dagBuilder    *dag.DAGBuilder
    blockRegistry *BlockRegistry
    logger        Logger
    storage       Storage
    queue         Queue
}

// Execute 执行工作流
func (e *WorkflowExecutor) Execute(ctx context.Context, workflow *types.Workflow, execCtx *types.ExecutionContext) (*types.ExecutionResult, error) {
    startTime := time.Now()

    // 1. 构建DAG
    dag, err := e.dagBuilder.Build(workflow, "")
    if err != nil {
        return nil, fmt.Errorf("failed to build DAG: %w", err)
    }

    // 2. 初始化执行状态
    executionState := &ExecutionState{
        ExecutedNodes:   make(map[string]bool),
        NodeOutputs:     make(map[string]map[string]interface{}),
        ExecutionLogs:   []*types.ExecutionLog{},
        PausedNodes:     make(map[string]*types.PausePoint),
        mutex:           &sync.RWMutex{},
    }

    // 3. 创建执行引擎
    engine := &ExecutionEngine{
        dag:             dag,
        executionState:  executionState,
        blockRegistry:   e.blockRegistry,
        logger:          e.logger,
        readyQueue:      make(chan string, 100),
        executingNodes:  make(map[string]bool),
        maxConcurrency:  10, // 可配置
    }

    // 4. 开始执行
    result, err := engine.Run(ctx, execCtx)
    if err != nil {
        return nil, fmt.Errorf("execution failed: %w", err)
    }

    // 5. 计算执行时间
    result.Metadata.Duration = time.Since(startTime)
    result.Metadata.StartTime = startTime
    result.Metadata.EndTime = time.Now()

    return result, nil
}

// ExecutionEngine 执行引擎
type ExecutionEngine struct {
    dag             *dag.DAG
    executionState  *ExecutionState
    blockRegistry   *BlockRegistry
    logger          Logger
    readyQueue      chan string
    executingNodes  map[string]bool
    maxConcurrency  int
    wg              sync.WaitGroup
    ctx             context.Context
    cancel          context.CancelFunc
}

// Run 运行执行引擎
func (e *ExecutionEngine) Run(ctx context.Context, execCtx *types.ExecutionContext) (*types.ExecutionResult, error) {
    e.ctx, e.cancel = context.WithCancel(ctx)
    defer e.cancel()

    // 1. 初始化就绪队列
    e.initializeReadyQueue()

    // 2. 启动工作协程
    for i := 0; i < e.maxConcurrency; i++ {
        e.wg.Add(1)
        go e.worker(execCtx)
    }

    // 3. 等待所有节点执行完成
    e.wg.Wait()

    // 4. 检查是否有暂停的节点
    if len(e.executionState.PausedNodes) > 0 {
        return e.buildPausedResult(), nil
    }

    // 5. 构建最终结果
    return e.buildFinalResult(), nil
}

// worker 工作协程
func (e *ExecutionEngine) worker(execCtx *types.ExecutionContext) {
    defer e.wg.Done()

    for {
        select {
        case <-e.ctx.Done():
            return
        case nodeID, ok := <-e.readyQueue:
            if !ok {
                return
            }
            e.executeNode(nodeID, execCtx)
        }
    }
}

// executeNode 执行节点
func (e *ExecutionEngine) executeNode(nodeID string, execCtx *types.ExecutionContext) {
    node := e.dag.Nodes[nodeID]
    if node == nil {
        e.logger.Error("Node not found", "nodeID", nodeID)
        return
    }

    // 1. 获取Block执行器
    blockExecutor, err := e.blockRegistry.GetExecutor(node.Block.Type)
    if err != nil {
        e.logger.Error("Failed to get block executor", "error", err)
        return
    }

    // 2. 准备输入数据
    inputs := e.prepareInputs(node)

    // 3. 执行Block
    output, err := blockExecutor.Execute(e.ctx, node.Block, inputs, execCtx)
    if err != nil {
        e.logger.Error("Block execution failed", "error", err)
        return
    }

    // 4. 处理执行结果
    e.handleNodeCompletion(nodeID, output)

    // 5. 检查并添加新的就绪节点
    e.checkAndEnqueueReadyNodes()
}
```

### 5. Block注册表和执行器

```go
// internal/blocks/registry.go
package blocks

import (
    "context"
    "fmt"
    "workflow-engine/pkg/types"
)

// BlockExecutor Block执行器接口
type BlockExecutor interface {
    Execute(ctx context.Context, block *types.Block, inputs map[string]interface{}, execCtx *types.ExecutionContext) (map[string]interface{}, error)
    GetType() string
    Validate(block *types.Block) error
}

// BlockRegistry Block注册表
type BlockRegistry struct {
    executors map[string]BlockExecutor
}

// NewBlockRegistry 创建Block注册表
func NewBlockRegistry() *BlockRegistry {
    registry := &BlockRegistry{
        executors: make(map[string]BlockExecutor),
    }

    // 注册内置Block执行器
    registry.Register(&AgentBlockExecutor{})
    registry.Register(&ConditionBlockExecutor{})
    registry.Register(&APIBlockExecutor{})
    registry.Register(&FunctionBlockExecutor{})

    return registry
}

// Register 注册Block执行器
func (r *BlockRegistry) Register(executor BlockExecutor) {
    r.executors[executor.GetType()] = executor
}

// GetExecutor 获取Block执行器
func (r *BlockRegistry) GetExecutor(blockType string) (BlockExecutor, error) {
    executor, exists := r.executors[blockType]
    if !exists {
        return nil, fmt.Errorf("no executor found for block type: %s", blockType)
    }
    return executor, nil
}
```

### 6. Agent Block执行器实现

```go
// internal/blocks/agent.go
package blocks

import (
    "context"
    "fmt"
    "workflow-engine/internal/llm"
    "workflow-engine/pkg/types"
)

// AgentBlockExecutor Agent Block执行器
type AgentBlockExecutor struct {
    llmProvider llm.Provider
}

// Execute 执行Agent Block
func (e *AgentBlockExecutor) Execute(ctx context.Context, block *types.Block, inputs map[string]interface{}, execCtx *types.ExecutionContext) (map[string]interface{}, error) {
    // 1. 解析配置
    config, err := e.parseConfig(block)
    if err != nil {
        return nil, fmt.Errorf("failed to parse agent config: %w", err)
    }

    // 2. 准备消息
    messages, err := e.prepareMessages(config, inputs)
    if err != nil {
        return nil, fmt.Errorf("failed to prepare messages: %w", err)
    }

    // 3. 调用LLM
    response, err := e.llmProvider.Chat(ctx, &llm.ChatRequest{
        Model:          config.Model,
        Messages:       messages,
        Temperature:    config.Temperature,
        ResponseFormat: config.ResponseFormat,
        Tools:          config.Tools,
        APIKey:         config.APIKey,
    })
    if err != nil {
        return nil, fmt.Errorf("LLM call failed: %w", err)
    }

    // 4. 构建输出
    output := map[string]interface{}{
        "content":   response.Content,
        "model":     response.Model,
        "tokens":    response.Tokens,
        "toolCalls": response.ToolCalls,
    }

    // 5. 处理结构化输出
    if config.ResponseFormat != nil && response.StructuredOutput != nil {
        for key, value := range response.StructuredOutput {
            output[key] = value
        }
    }

    return output, nil
}

// GetType 获取Block类型
func (e *AgentBlockExecutor) GetType() string {
    return "agent"
}

// Validate 验证Block配置
func (e *AgentBlockExecutor) Validate(block *types.Block) error {
    if block.SubBlocks["model"] == nil || block.SubBlocks["model"].Value == "" {
        return fmt.Errorf("model is required")
    }
    return nil
}
```

### 7. HTTP API服务

```go
// internal/api/server.go
package api

import (
    "net/http"
    "workflow-engine/internal/engine"
    "workflow-engine/pkg/types"

    "github.com/gin-gonic/gin"
)

// Server HTTP API服务器
type Server struct {
    engine   *engine.WorkflowExecutor
    router   *gin.Engine
    storage  Storage
    queue    Queue
}

// executeWorkflow 执行工作流
func (s *Server) executeWorkflow(c *gin.Context) {
    workflowID := c.Param("id")

    var req ExecuteWorkflowRequest
    if err := c.ShouldBindJSON(&req); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    // 1. 获取工作流定义
    workflow, err := s.storage.GetWorkflow(c.Request.Context(), workflowID)
    if err != nil {
        c.JSON(http.StatusNotFound, gin.H{"error": "workflow not found"})
        return
    }

    // 2. 创建执行上下文
    execCtx := &types.ExecutionContext{
        WorkflowID:      workflowID,
        ExecutionID:     generateExecutionID(),
        UserID:          req.UserID,
        WorkspaceID:     req.WorkspaceID,
        Variables:       req.Variables,
        Environment:     req.Environment,
        IsDeployed:      req.IsDeployed,
        Stream:          req.Stream,
        SelectedOutputs: req.SelectedOutputs,
    }

    // 3. 执行工作流
    result, err := s.engine.Execute(c.Request.Context(), workflow, execCtx)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }

    c.JSON(http.StatusOK, result)
}
```

### 8. 配置管理

```go
// internal/config/config.go
package config

import (
    "github.com/spf13/viper"
)

// Config 应用配置
type Config struct {
    Server   ServerConfig   `mapstructure:"server"`
    Database DatabaseConfig `mapstructure:"database"`
    Redis    RedisConfig    `mapstructure:"redis"`
    LLM      LLMConfig      `mapstructure:"llm"`
    Logging  LoggingConfig  `mapstructure:"logging"`
}

// Load 加载配置
func Load() (*Config, error) {
    viper.SetConfigName("config")
    viper.SetConfigType("yaml")
    viper.AddConfigPath(".")
    viper.AddConfigPath("./configs")

    // 设置默认值
    setDefaults()

    // 读取环境变量
    viper.AutomaticEnv()

    if err := viper.ReadInConfig(); err != nil {
        return nil, err
    }

    var config Config
    if err := viper.Unmarshal(&config); err != nil {
        return nil, err
    }

    return &config, nil
}
```

### 9. 部署配置

```yaml
# deployments/docker-compose.yml
version: '3.8'

services:
  workflow-engine:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "8080:8080"
    environment:
      - DATABASE_HOST=postgres
      - REDIS_HOST=redis
    depends_on:
      - postgres
      - redis

  postgres:
    image: pgvector/pgvector:pg15
    environment:
      - POSTGRES_DB=workflow_engine
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=password
    ports:
      - "5432:5432"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
```

## 核心功能实现

### 1. 并发执行优化

- 使用Goroutine池管理并发执行
- 实现智能的依赖解析和调度
- 支持Block级别的并行执行

### 2. 内存管理

- 使用对象池减少GC压力
- 实现流式处理大数据集
- 优化数据结构减少内存占用

### 3. 缓存策略

- Redis缓存Block输出结果
- 本地缓存工作流定义
- 智能缓存失效机制

### 4. 监控和观测

- Prometheus指标收集
- 分布式链路追踪
- 实时性能监控

## 与前端集成

### 1. API兼容性

- 保持与现有TypeScript API的完全兼容
- 支持相同的请求/响应格式
- 无缝替换现有执行引擎

### 2. WebSocket支持

- 实时执行状态推送
- 支持流式输出
- 协作功能支持

### 3. 错误处理

- 统一的错误格式
- 详细的错误信息
- 错误恢复机制

## 性能优势

### 1. 执行性能

- Go语言的高性能特性
- 原生并发支持
- 更低的内存占用

### 2. 扩展性

- 水平扩展能力
- 负载均衡支持
- 微服务架构

### 3. 可靠性

- 更好的错误处理
- 资源管理优化
- 系统稳定性提升

## 总结

这个Go语言工作流执行引擎设计方案提供了：

1. **高性能**: 基于Goroutine的并发执行模型
2. **可扩展**: 模块化的Block注册表系统
3. **可靠性**: 完整的错误处理和恢复机制
4. **兼容性**: 与现有前端系统完全兼容
5. **可观测**: 完整的监控和日志系统

通过这个设计，可以实现一个高性能、可扩展的工作流执行引擎，同时保持与现有Sim系统的完全兼容。
