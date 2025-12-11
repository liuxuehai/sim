# Go语言工作流执行引擎设计方案

## 项目概述

基于对Sim工作流系统的分析，设计一个用Go语言实现的高性能工作流执行引擎。该引擎将替换现有的TypeScript执行器，提供更好的性能、并发处理能力和资源管理，同时保持与现有前端系统的完全兼容。

## 整体架构设计

### 1. 系统架构图

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │   Go Workflow   │    │  Go Workflow    │
│   (React/TS)    │◄──►│   API Server    │◄──►│    Engine       │
│                 │    │   (Hertz)       │    │   (Temporal)    │
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
- **Web框架**: CloudWeGo Hertz (高性能HTTP框架，内置配置和日志)
- **工作流引擎**: Temporal (分布式工作流编排)
- **数据库**: PostgreSQL (使用现有schema)
- **ORM**: Ent (Facebook开源的Go实体框架)
- **缓存/队列**: Redis
- **配置管理**: Hertz Config (内置配置管理)
- **日志**: Hertz Logger (内置日志系统)
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
├── ent/
│   ├── schema/          # Ent Schema定义
│   ├── migrate/         # 数据库迁移
│   └── generated/       # Ent生成的代码
├── internal/
│   ├── api/             # Hertz HTTP API处理
│   ├── websocket/       # WebSocket实时通信
│   ├── workflows/       # Temporal工作流定义
│   ├── activities/      # Temporal Activities
│   ├── blocks/          # Block执行器实现
│   ├── repository/      # 数据访问层
│   ├── config/          # 配置管理
│   └── llm/             # LLM提供商集成
├── pkg/
│   ├── utils/           # 工具函数
│   └── errors/          # 错误处理
├── configs/             # 配置文件
├── deployments/         # 部署配置
├── scripts/             # 构建脚本
└── docs/                # 文档
```

### 2. Ent Schema定义 (基于现有数据库Schema)

```go
// ent/schema/workflow.go
package schema

import (
    "time"
    "entgo.io/ent"
    "entgo.io/ent/schema/edge"
    "entgo.io/ent/schema/field"
    "entgo.io/ent/schema/index"
)

// Workflow holds the schema definition for the Workflow entity.
type Workflow struct {
    ent.Schema
}

// Fields of the Workflow.
func (Workflow) Fields() []ent.Field {
    return []ent.Field{
        field.String("id").
            StorageKey("id").
            Unique(),
        field.String("user_id").
            NotEmpty(),
        field.String("workspace_id").
            Optional().
            Nillable(),
        field.String("folder_id").
            Optional().
            Nillable(),
        field.String("name").
            NotEmpty(),
        field.String("description").
            Optional().
            Nillable(),
        field.String("color").
            Default("#3972F6"),
        field.Time("last_synced"),
        field.Time("created_at").
            Default(time.Now),
        field.Time("updated_at").
            Default(time.Now).
            UpdateDefault(time.Now),
        field.Bool("is_deployed").
            Default(false),
        field.Time("deployed_at").
            Optional().
            Nillable(),
        field.Int("run_count").
            Default(0),
        field.Time("last_run_at").
            Optional().
            Nillable(),
        field.JSON("variables", map[string]interface{}{}).
            Default(map[string]interface{}{}),
    }
}

// Edges of the Workflow.
func (Workflow) Edges() []ent.Edge {
    return []ent.Edge{
        edge.To("blocks", WorkflowBlock.Type),
        edge.To("edges", WorkflowEdge.Type),
        edge.To("subflows", WorkflowSubflow.Type),
        edge.To("execution_logs", WorkflowExecutionLog.Type),
        edge.To("paused_executions", PausedExecution.Type),
    }
}

// Indexes of the Workflow.
func (Workflow) Indexes() []ent.Index {
    return []ent.Index{
        index.Fields("user_id"),
        index.Fields("workspace_id"),
        index.Fields("user_id", "workspace_id"),
    }
}

// WorkflowBlock holds the schema definition for the WorkflowBlock entity.
type WorkflowBlock struct {
    ent.Schema
}

// Fields of the WorkflowBlock.
func (WorkflowBlock) Fields() []ent.Field {
    return []ent.Field{
        field.String("id").
            StorageKey("id").
            Unique(),
        field.String("workflow_id").
            NotEmpty(),
        field.String("type").
            NotEmpty(),
        field.String("name").
            NotEmpty(),
        field.Float("position_x"),
        field.Float("position_y"),
        field.Bool("enabled").
            Default(true),
        field.Bool("horizontal_handles").
            Default(true),
        field.Bool("is_wide").
            Default(false),
        field.Bool("advanced_mode").
            Default(false),
        field.Bool("trigger_mode").
            Default(false),
        field.Float("height").
            Default(0),
        field.JSON("sub_blocks", map[string]interface{}{}).
            Default(map[string]interface{}{}),
        field.JSON("outputs", map[string]interface{}{}).
            Default(map[string]interface{}{}),
        field.JSON("data", map[string]interface{}{}).
            Optional(),
        field.Time("created_at").
            Default(time.Now),
        field.Time("updated_at").
            Default(time.Now).
            UpdateDefault(time.Now),
    }
}

// Edges of the WorkflowBlock.
func (WorkflowBlock) Edges() []ent.Edge {
    return []ent.Edge{
        edge.From("workflow", Workflow.Type).
            Ref("blocks").
            Field("workflow_id").
            Unique().
            Required(),
    }
}

// Indexes of the WorkflowBlock.
func (WorkflowBlock) Indexes() []ent.Index {
    return []ent.Index{
        index.Fields("workflow_id"),
    }
}

// WorkflowEdge holds the schema definition for the WorkflowEdge entity.
type WorkflowEdge struct {
    ent.Schema
}

// Fields of the WorkflowEdge.
func (WorkflowEdge) Fields() []ent.Field {
    return []ent.Field{
        field.String("id").
            StorageKey("id").
            Unique(),
        field.String("workflow_id").
            NotEmpty(),
        field.String("source_block_id").
            NotEmpty(),
        field.String("target_block_id").
            NotEmpty(),
        field.String("source_handle").
            Optional().
            Nillable(),
        field.String("target_handle").
            Optional().
            Nillable(),
        field.Time("created_at").
            Default(time.Now),
    }
}

// Edges of the WorkflowEdge.
func (WorkflowEdge) Edges() []ent.Edge {
    return []ent.Edge{
        edge.From("workflow", Workflow.Type).
            Ref("edges").
            Field("workflow_id").
            Unique().
            Required(),
    }
}

// Indexes of the WorkflowEdge.
func (WorkflowEdge) Indexes() []ent.Index {
    return []ent.Index{
        index.Fields("workflow_id"),
        index.Fields("workflow_id", "source_block_id"),
        index.Fields("workflow_id", "target_block_id"),
    }
}

// WorkflowSubflow holds the schema definition for the WorkflowSubflow entity.
type WorkflowSubflow struct {
    ent.Schema
}

// Fields of the WorkflowSubflow.
func (WorkflowSubflow) Fields() []ent.Field {
    return []ent.Field{
        field.String("id").
            StorageKey("id").
            Unique(),
        field.String("workflow_id").
            NotEmpty(),
        field.String("type").
            NotEmpty(), // 'loop' or 'parallel'
        field.JSON("config", map[string]interface{}{}).
            Default(map[string]interface{}{}),
        field.Time("created_at").
            Default(time.Now),
        field.Time("updated_at").
            Default(time.Now).
            UpdateDefault(time.Now),
    }
}

// Edges of the WorkflowSubflow.
func (WorkflowSubflow) Edges() []ent.Edge {
    return []ent.Edge{
        edge.From("workflow", Workflow.Type).
            Ref("subflows").
            Field("workflow_id").
            Unique().
            Required(),
    }
}

// Indexes of the WorkflowSubflow.
func (WorkflowSubflow) Indexes() []ent.Index {
    return []ent.Index{
        index.Fields("workflow_id"),
        index.Fields("workflow_id", "type"),
    }
}

// WorkflowExecutionLog holds the schema definition for the WorkflowExecutionLog entity.
type WorkflowExecutionLog struct {
    ent.Schema
}

// Fields of the WorkflowExecutionLog.
func (WorkflowExecutionLog) Fields() []ent.Field {
    return []ent.Field{
        field.String("id").
            StorageKey("id").
            Unique(),
        field.String("workflow_id").
            NotEmpty(),
        field.String("execution_id").
            NotEmpty(),
        field.String("state_snapshot_id").
            NotEmpty(),
        field.String("level").
            NotEmpty(),
        field.String("trigger").
            NotEmpty(),
        field.Time("started_at"),
        field.Time("ended_at").
            Optional().
            Nillable(),
        field.Int("total_duration_ms").
            Optional().
            Nillable(),
        field.JSON("execution_data", map[string]interface{}{}).
            Default(map[string]interface{}{}),
        field.JSON("cost", map[string]interface{}{}).
            Optional(),
        field.JSON("files", map[string]interface{}{}).
            Optional(),
        field.Time("created_at").
            Default(time.Now),
    }
}

// Edges of the WorkflowExecutionLog.
func (WorkflowExecutionLog) Edges() []ent.Edge {
    return []ent.Edge{
        edge.From("workflow", Workflow.Type).
            Ref("execution_logs").
            Field("workflow_id").
            Unique().
            Required(),
    }
}

// Indexes of the WorkflowExecutionLog.
func (WorkflowExecutionLog) Indexes() []ent.Index {
    return []ent.Index{
        index.Fields("workflow_id"),
        index.Fields("execution_id").Unique(),
        index.Fields("workflow_id", "started_at"),
    }
}

// PausedExecution holds the schema definition for the PausedExecution entity.
type PausedExecution struct {
    ent.Schema
}

// Fields of the PausedExecution.
func (PausedExecution) Fields() []ent.Field {
    return []ent.Field{
        field.String("id").
            StorageKey("id").
            Unique(),
        field.String("workflow_id").
            NotEmpty(),
        field.String("execution_id").
            NotEmpty(),
        field.JSON("execution_snapshot", map[string]interface{}{}).
            NotEmpty(),
        field.JSON("pause_points", []interface{}{}).
            NotEmpty(),
        field.Int("total_pause_count"),
        field.Int("resumed_count").
            Default(0),
        field.String("status").
            Default("paused"),
        field.JSON("metadata", map[string]interface{}{}).
            Default(map[string]interface{}{}),
        field.Time("paused_at").
            Default(time.Now),
        field.Time("updated_at").
            Default(time.Now).
            UpdateDefault(time.Now),
        field.Time("expires_at").
            Optional().
            Nillable(),
    }
}

// Edges of the PausedExecution.
func (PausedExecution) Edges() []ent.Edge {
    return []ent.Edge{
        edge.From("workflow", Workflow.Type).
            Ref("paused_executions").
            Field("workflow_id").
            Unique().
            Required(),
    }
}

// Indexes of the PausedExecution.
func (PausedExecution) Indexes() []ent.Index {
    return []ent.Index{
        index.Fields("workflow_id"),
        index.Fields("execution_id").Unique(),
        index.Fields("status"),
    }
}

// ExecutionContext 执行上下文
type ExecutionContext struct {
    WorkflowID      string                 `json:"workflow_id"`
    ExecutionID     string                 `json:"execution_id"`
    UserID          string                 `json:"user_id"`
    WorkspaceID     *string                `json:"workspace_id"`
    Variables       map[string]interface{} `json:"variables"`
    Environment     map[string]string      `json:"environment"`
    StartTime       time.Time              `json:"start_time"`
    IsDeployed      bool                   `json:"is_deployed"`
    Stream          bool                   `json:"stream"`
    SelectedOutputs []string               `json:"selected_outputs"`
    TriggerType     string                 `json:"trigger_type"`
}

// ExecutionResult 执行结果
type ExecutionResult struct {
    Success     bool                   `json:"success"`
    Output      map[string]interface{} `json:"output"`
    Error       string                 `json:"error,omitempty"`
    Logs        []ExecutionLog         `json:"logs"`
    Metadata    *ExecutionMetadata     `json:"metadata"`
    Status      string                 `json:"status"`
    PausePoints []PausePoint           `json:"pause_points,omitempty"`
}

// ExecutionLog 执行日志
type ExecutionLog struct {
    Level     string    `json:"level"`
    Message   string    `json:"message"`
    Timestamp time.Time `json:"timestamp"`
    BlockID   string    `json:"block_id,omitempty"`
    Data      map[string]interface{} `json:"data,omitempty"`
}

// ExecutionMetadata 执行元数据
type ExecutionMetadata struct {
    RequestID     string        `json:"request_id"`
    ExecutionID   string        `json:"execution_id"`
    WorkflowID    string        `json:"workflow_id"`
    WorkspaceID   *string       `json:"workspace_id"`
    UserID        string        `json:"user_id"`
    TriggerType   string        `json:"trigger_type"`
    StartTime     time.Time     `json:"start_time"`
    EndTime       *time.Time    `json:"end_time"`
    Duration      time.Duration `json:"duration"`
}

// PausePoint 暂停点
type PausePoint struct {
    BlockID     string                 `json:"block_id"`
    ContextID   string                 `json:"context_id"`
    Message     string                 `json:"message"`
    InputSchema map[string]interface{} `json:"input_schema,omitempty"`
}
```

### 3. Temporal工作流定义

```go
// internal/workflows/workflow.go
package workflows

import (
    "context"
    "time"
    "go.temporal.io/sdk/workflow"
    "workflow-engine/pkg/models"
)

// WorkflowExecutionInput 工作流执行输入
type WorkflowExecutionInput struct {
    WorkflowID      string                 `json:"workflow_id"`
    ExecutionID     string                 `json:"execution_id"`
    UserID          string                 `json:"user_id"`
    WorkspaceID     *string                `json:"workspace_id"`
    Variables       map[string]interface{} `json:"variables"`
    Environment     map[string]string      `json:"environment"`
    TriggerType     string                 `json:"trigger_type"`
    IsDeployed      bool                   `json:"is_deployed"`
    Stream          bool                   `json:"stream"`
    SelectedOutputs []string               `json:"selected_outputs"`
}

// SimWorkflow Temporal工作流定义
func SimWorkflow(ctx workflow.Context, input WorkflowExecutionInput) (*models.ExecutionResult, error) {
    logger := workflow.GetLogger(ctx)
    
    logger.Info("Starting workflow execution", 
        "workflowId", input.WorkflowID,
        "executionId", input.ExecutionID,
        "userId", input.UserID)

    // 1. 获取工作流定义
    var workflowData *models.Workflow
    err := workflow.ExecuteActivity(ctx, GetWorkflowActivity, input.WorkflowID).Get(ctx, &workflowData)
    if err != nil {
        return nil, err
    }

    // 2. 构建执行DAG
    var dag *DAG
    err = workflow.ExecuteActivity(ctx, BuildDAGActivity, workflowData).Get(ctx, &dag)
    if err != nil {
        return nil, err
    }

    // 3. 初始化执行状态
    executionState := &ExecutionState{
        ExecutedBlocks:  make(map[string]bool),
        BlockOutputs:    make(map[string]map[string]interface{}),
        ExecutionLogs:   []models.ExecutionLog{},
        PausedBlocks:    make(map[string]*models.PausePoint),
        Variables:       input.Variables,
        Environment:     input.Environment,
    }

    // 4. 执行工作流
    result, err := executeWorkflowDAG(ctx, dag, executionState, input)
    if err != nil {
        return nil, err
    }

    logger.Info("Workflow execution completed",
        "workflowId", input.WorkflowID,
        "executionId", input.ExecutionID,
        "success", result.Success)

    return result, nil
}

// executeWorkflowDAG 执行工作流DAG
func executeWorkflowDAG(ctx workflow.Context, dag *DAG, state *ExecutionState, input WorkflowExecutionInput) (*models.ExecutionResult, error) {
    // 找到起始节点
    startNodes := findStartNodes(dag)
    
    // 使用Temporal的并行执行能力
    var futures []workflow.Future
    
    for _, startNode := range startNodes {
        future := workflow.ExecuteActivity(ctx, ExecuteBlockActivity, ExecuteBlockInput{
            Block:           startNode.Block,
            Inputs:          make(map[string]interface{}),
            ExecutionState:  state,
            ExecutionInput:  input,
        })
        futures = append(futures, future)
    }

    // 等待所有起始节点完成
    for _, future := range futures {
        var blockResult *BlockExecutionResult
        err := future.Get(ctx, &blockResult)
        if err != nil {
            return &models.ExecutionResult{
                Success: false,
                Error:   err.Error(),
                Status:  "failed",
            }, nil
        }
        
        // 更新执行状态
        state.ExecutedBlocks[blockResult.BlockID] = true
        state.BlockOutputs[blockResult.BlockID] = blockResult.Outputs
        state.ExecutionLogs = append(state.ExecutionLogs, blockResult.Logs...)
    }

    // 继续执行后续节点
    return continueExecution(ctx, dag, state, input)
}

// continueExecution 继续执行后续节点
func continueExecution(ctx workflow.Context, dag *DAG, state *ExecutionState, input WorkflowExecutionInput) (*models.ExecutionResult, error) {
    // 查找可执行的节点
    readyNodes := findReadyNodes(dag, state)
    
    if len(readyNodes) == 0 {
        // 所有节点执行完成
        return &models.ExecutionResult{
            Success: true,
            Output:  collectFinalOutputs(state, input.SelectedOutputs),
            Status:  "completed",
            Logs:    state.ExecutionLogs,
        }, nil
    }

    // 并行执行就绪节点
    var futures []workflow.Future
    
    for _, node := range readyNodes {
        inputs := prepareBlockInputs(node, state)
        
        future := workflow.ExecuteActivity(ctx, ExecuteBlockActivity, ExecuteBlockInput{
            Block:           node.Block,
            Inputs:          inputs,
            ExecutionState:  state,
            ExecutionInput:  input,
        })
        futures = append(futures, future)
    }

    // 等待节点执行完成
    for _, future := range futures {
        var blockResult *BlockExecutionResult
        err := future.Get(ctx, &blockResult)
        if err != nil {
            return &models.ExecutionResult{
                Success: false,
                Error:   err.Error(),
                Status:  "failed",
            }, nil
        }
        
        // 检查是否有暂停点
        if blockResult.PausePoint != nil {
            state.PausedBlocks[blockResult.BlockID] = blockResult.PausePoint
            return &models.ExecutionResult{
                Success:     true,
                Status:      "paused",
                PausePoints: []models.PausePoint{*blockResult.PausePoint},
            }, nil
        }
        
        // 更新执行状态
        state.ExecutedBlocks[blockResult.BlockID] = true
        state.BlockOutputs[blockResult.BlockID] = blockResult.Outputs
        state.ExecutionLogs = append(state.ExecutionLogs, blockResult.Logs...)
    }

    // 递归继续执行
    return continueExecution(ctx, dag, state, input)
}
```

### 4. Temporal Activities实现

```go
// internal/activities/workflow_activities.go
package activities

import (
    "context"
    "fmt"
    "go.temporal.io/sdk/activity"
    "workflow-engine/pkg/models"
    "workflow-engine/internal/repository"
    "workflow-engine/internal/blocks"
)

// WorkflowActivities 工作流相关的Activities
type WorkflowActivities struct {
    workflowRepo  repository.WorkflowRepository
    blockRegistry *blocks.BlockRegistry
}

// GetWorkflowActivity 获取工作流定义
func (a *WorkflowActivities) GetWorkflowActivity(ctx context.Context, workflowID string) (*ent.Workflow, error) {
    logger := activity.GetLogger(ctx)
    logger.Info("Getting workflow", "workflowId", workflowID)

    workflow, err := a.workflowRepo.GetWorkflowWithBlocks(ctx, workflowID)
    if err != nil {
        return nil, fmt.Errorf("failed to get workflow: %w", err)
    }

    return workflow, nil
}

// BuildDAGActivity 构建DAG
func (a *WorkflowActivities) BuildDAGActivity(ctx context.Context, workflow *ent.Workflow) (*DAG, error) {
    logger := activity.GetLogger(ctx)
    logger.Info("Building DAG", "workflowId", workflow.ID)

    dag := &DAG{
        Nodes: make(map[string]*DAGNode),
        Edges: make(map[string]*DAGEdge),
    }

    // 1. 创建节点
    for _, block := range workflow.Edges.Blocks {
        node := &DAGNode{
            ID:            block.ID,
            Block:         block,
            IncomingEdges: make(map[string]*DAGEdge),
            OutgoingEdges: make(map[string]*DAGEdge),
            Dependencies:  []string{},
            Dependents:    []string{},
        }
        dag.Nodes[block.ID] = node
    }

    // 2. 创建边和依赖关系
    for _, edge := range workflow.Edges.Edges {
        dagEdge := &DAGEdge{
            ID:     edge.ID,
            Source: edge.SourceBlockID,
            Target: edge.TargetBlockID,
        }
        dag.Edges[edge.ID] = dagEdge

        // 建立依赖关系
        if sourceNode, exists := dag.Nodes[edge.SourceBlockID]; exists {
            sourceNode.OutgoingEdges[edge.ID] = dagEdge
            sourceNode.Dependents = append(sourceNode.Dependents, edge.TargetBlockID)
        }

        if targetNode, exists := dag.Nodes[edge.TargetBlockID]; exists {
            targetNode.IncomingEdges[edge.ID] = dagEdge
            targetNode.Dependencies = append(targetNode.Dependencies, edge.SourceBlockID)
        }
    }

    // 3. 验证DAG有效性
    if err := validateDAG(dag); err != nil {
        return nil, fmt.Errorf("invalid DAG: %w", err)
    }

    return dag, nil
}

// ExecuteBlockActivity 执行单个Block
func (a *WorkflowActivities) ExecuteBlockActivity(ctx context.Context, input ExecuteBlockInput) (*BlockExecutionResult, error) {
    logger := activity.GetLogger(ctx)
    logger.Info("Executing block", "blockId", input.Block.ID, "blockType", input.Block.Type)

    // 1. 获取Block执行器
    blockExecutor, err := a.blockRegistry.GetExecutor(input.Block.Type)
    if err != nil {
        return nil, fmt.Errorf("failed to get block executor for type %s: %w", input.Block.Type, err)
    }

    // 2. 创建执行上下文
    execCtx := &models.ExecutionContext{
        WorkflowID:      input.ExecutionInput.WorkflowID,
        ExecutionID:     input.ExecutionInput.ExecutionID,
        UserID:          input.ExecutionInput.UserID,
        WorkspaceID:     input.ExecutionInput.WorkspaceID,
        Variables:       input.ExecutionInput.Variables,
        Environment:     input.ExecutionInput.Environment,
        TriggerType:     input.ExecutionInput.TriggerType,
        IsDeployed:      input.ExecutionInput.IsDeployed,
        Stream:          input.ExecutionInput.Stream,
        SelectedOutputs: input.ExecutionInput.SelectedOutputs,
    }

    // 3. 执行Block
    result, err := blockExecutor.Execute(ctx, input.Block, input.Inputs, execCtx)
    if err != nil {
        return &BlockExecutionResult{
            BlockID: input.Block.ID,
            Success: false,
            Error:   err.Error(),
            Logs: []models.ExecutionLog{
                {
                    Level:     "error",
                    Message:   fmt.Sprintf("Block execution failed: %s", err.Error()),
                    Timestamp: time.Now(),
                    BlockID:   input.Block.ID,
                },
            },
        }, nil
    }

    return &BlockExecutionResult{
        BlockID:    input.Block.ID,
        Success:    true,
        Outputs:    result.Outputs,
        Logs:       result.Logs,
        PausePoint: result.PausePoint,
    }, nil
}

// DAG相关结构体
type DAG struct {
    Nodes map[string]*DAGNode `json:"nodes"`
    Edges map[string]*DAGEdge `json:"edges"`
}

type DAGNode struct {
    ID            string                    `json:"id"`
    Block         *ent.WorkflowBlock        `json:"block"`
    IncomingEdges map[string]*DAGEdge       `json:"incoming_edges"`
    OutgoingEdges map[string]*DAGEdge       `json:"outgoing_edges"`
    Dependencies  []string                  `json:"dependencies"`
    Dependents    []string                  `json:"dependents"`
}

type DAGEdge struct {
    ID     string `json:"id"`
    Source string `json:"source"`
    Target string `json:"target"`
}

// ExecuteBlockInput Block执行输入
type ExecuteBlockInput struct {
    Block          *ent.WorkflowBlock         `json:"block"`
    Inputs         map[string]interface{}     `json:"inputs"`
    ExecutionState *ExecutionState            `json:"execution_state"`
    ExecutionInput WorkflowExecutionInput     `json:"execution_input"`
}

// BlockExecutionResult Block执行结果
type BlockExecutionResult struct {
    BlockID    string                     `json:"block_id"`
    Success    bool                       `json:"success"`
    Outputs    map[string]interface{}     `json:"outputs"`
    Error      string                     `json:"error,omitempty"`
    Logs       []models.ExecutionLog      `json:"logs"`
    PausePoint *models.PausePoint         `json:"pause_point,omitempty"`
}

// ExecutionState 执行状态
type ExecutionState struct {
    ExecutedBlocks map[string]bool                    `json:"executed_blocks"`
    BlockOutputs   map[string]map[string]interface{} `json:"block_outputs"`
    ExecutionLogs  []models.ExecutionLog              `json:"execution_logs"`
    PausedBlocks   map[string]*models.PausePoint      `json:"paused_blocks"`
    Variables      map[string]interface{}             `json:"variables"`
    Environment    map[string]string                  `json:"environment"`
}

// validateDAG 验证DAG有效性
func validateDAG(dag *DAG) error {
    visited := make(map[string]bool)
    recStack := make(map[string]bool)

    for nodeID := range dag.Nodes {
        if !visited[nodeID] {
            if hasCycle(dag, nodeID, visited, recStack) {
                return fmt.Errorf("cycle detected in workflow")
            }
        }
    }
    return nil
}

// hasCycle 检查是否存在循环
func hasCycle(dag *DAG, nodeID string, visited, recStack map[string]bool) bool {
    visited[nodeID] = true
    recStack[nodeID] = true

    node := dag.Nodes[nodeID]
    for _, dependent := range node.Dependents {
        if !visited[dependent] {
            if hasCycle(dag, dependent, visited, recStack) {
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

### 7. WebSocket实时通信实现

```go
// internal/websocket/hub.go
package websocket

import (
    "context"
    "fmt"
    "net/http"
    "os"
    "time"
    
    "github.com/cloudwego/hertz/pkg/app"
    "github.com/cloudwego/hertz/pkg/app/server"
    "github.com/cloudwego/hertz/pkg/common/hlog"
    "github.com/cloudwego/hertz/pkg/common/utils"
    "go.temporal.io/sdk/client"
    
    "workflow-engine/internal/config"
    "workflow-engine/internal/workflows"
    "workflow-engine/internal/repository"
)

// Server Hertz HTTP API服务器
type Server struct {
    hertzServer    *server.Hertz
    temporalClient client.Client
    workflowRepo   repository.WorkflowRepository
    config         *config.Config
}

// NewServer 创建新的API服务器
func NewServer(cfg *config.Config, temporalClient client.Client, workflowRepo repository.WorkflowRepository) *Server {
    // 配置Hertz服务器选项
    opts := []server.Option{
        server.WithHostPorts(fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)),
        server.WithReadTimeout(30 * time.Second),
        server.WithWriteTimeout(30 * time.Second),
        server.WithIdleTimeout(60 * time.Second),
    }
    
    h := server.Default(opts...)
    
    // 配置日志
    hlog.SetLevel(hlog.LevelInfo)
    hlog.SetOutput(os.Stdout)
    
    s := &Server{
        hertzServer:    h,
        temporalClient: temporalClient,
        workflowRepo:   workflowRepo,
        config:         cfg,
    }
    
    s.setupRoutes()
    s.setupMiddleware()
    return s
}

// setupMiddleware 设置中间件
func (s *Server) setupMiddleware() {
    // 请求日志中间件
    s.hertzServer.Use(func(c context.Context, ctx *app.RequestContext) {
        start := time.Now()
        ctx.Next(c)
        
        hlog.CtxInfof(c, "Request: %s %s - Status: %d - Duration: %v",
            string(ctx.Method()), string(ctx.Path()),
            ctx.Response.StatusCode(), time.Since(start))
    })
    
    // 错误恢复中间件
    s.hertzServer.Use(func(c context.Context, ctx *app.RequestContext) {
        defer func() {
            if err := recover(); err != nil {
                hlog.CtxErrorf(c, "Panic recovered: %v", err)
                ctx.JSON(http.StatusInternalServerError, utils.H{
                    "error": "Internal server error",
                })
            }
        }()
        ctx.Next(c)
    })
}

// setupRoutes 设置路由
func (s *Server) setupRoutes() {
    // 工作流执行相关路由
    s.hertzServer.POST("/api/workflows/:id/execute", s.executeWorkflow)
    s.hertzServer.GET("/api/workflows/:id/executions/:executionId", s.getExecutionStatus)
    s.hertzServer.POST("/api/workflows/:id/executions/:executionId/resume", s.resumeExecution)
    
    // 健康检查
    s.hertzServer.GET("/health", s.healthCheck)
}

// ExecuteWorkflowRequest 执行工作流请求
type ExecuteWorkflowRequest struct {
    UserID          string                 `json:"user_id" binding:"required"`
    WorkspaceID     *string                `json:"workspace_id"`
    Variables       map[string]interface{} `json:"variables"`
    Environment     map[string]string      `json:"environment"`
    TriggerType     string                 `json:"trigger_type"`
    IsDeployed      bool                   `json:"is_deployed"`
    Stream          bool                   `json:"stream"`
    SelectedOutputs []string               `json:"selected_outputs"`
}

// executeWorkflow 执行工作流
func (s *Server) executeWorkflow(ctx context.Context, c *app.RequestContext) {
    workflowID := c.Param("id")
    
    hlog.CtxInfof(ctx, "Executing workflow: %s", workflowID)

    var req ExecuteWorkflowRequest
    if err := c.BindAndValidate(&req); err != nil {
        hlog.CtxErrorf(ctx, "Invalid request: %v", err)
        c.JSON(http.StatusBadRequest, utils.H{"error": err.Error()})
        return
    }

    // 1. 验证工作流是否存在
    workflow, err := s.workflowRepo.GetByID(ctx, workflowID)
    if err != nil {
        hlog.CtxErrorf(ctx, "Workflow not found: %s, error: %v", workflowID, err)
        c.JSON(http.StatusNotFound, utils.H{"error": "workflow not found"})
        return
    }

    // 2. 生成执行ID
    executionID := generateExecutionID()
    hlog.CtxInfof(ctx, "Generated execution ID: %s for workflow: %s", executionID, workflowID)

    // 3. 创建Temporal工作流输入
    input := workflows.WorkflowExecutionInput{
        WorkflowID:      workflowID,
        ExecutionID:     executionID,
        UserID:          req.UserID,
        WorkspaceID:     req.WorkspaceID,
        Variables:       req.Variables,
        Environment:     req.Environment,
        TriggerType:     req.TriggerType,
        IsDeployed:      req.IsDeployed,
        Stream:          req.Stream,
        SelectedOutputs: req.SelectedOutputs,
    }

    // 4. 启动Temporal工作流
    workflowOptions := client.StartWorkflowOptions{
        ID:        executionID,
        TaskQueue: s.config.Temporal.TaskQueue,
    }

    workflowRun, err := s.temporalClient.ExecuteWorkflow(ctx, workflowOptions, workflows.SimWorkflow, input)
    if err != nil {
        hlog.CtxErrorf(ctx, "Failed to start workflow: %v", err)
        c.JSON(http.StatusInternalServerError, utils.H{"error": err.Error()})
        return
    }

    // 5. 如果是同步执行，等待结果
    if !req.Stream {
        hlog.CtxInfof(ctx, "Waiting for synchronous execution result")
        var result *ExecutionResult
        err = workflowRun.Get(ctx, &result)
        if err != nil {
            hlog.CtxErrorf(ctx, "Workflow execution failed: %v", err)
            c.JSON(http.StatusInternalServerError, utils.H{"error": err.Error()})
            return
        }
        
        hlog.CtxInfof(ctx, "Workflow execution completed successfully")
        c.JSON(http.StatusOK, result)
        return
    }

    // 6. 异步执行，返回执行ID
    hlog.CtxInfof(ctx, "Started asynchronous workflow execution")
    c.JSON(http.StatusAccepted, utils.H{
        "execution_id": executionID,
        "workflow_id":  workflowID,
        "status":       "running",
    })
}

// getExecutionStatus 获取执行状态
func (s *Server) getExecutionStatus(ctx context.Context, c *app.RequestContext) {
    workflowID := c.Param("id")
    executionID := c.Param("executionId")

    // 查询Temporal工作流状态
    workflowRun := s.temporalClient.GetWorkflow(ctx, executionID, "")
    
    // 检查工作流是否完成
    var result *models.ExecutionResult
    err := workflowRun.Get(ctx, &result)
    if err != nil {
        // 工作流还在运行中
        c.JSON(http.StatusOK, utils.H{
            "execution_id": executionID,
            "workflow_id":  workflowID,
            "status":       "running",
        })
        return
    }

    c.JSON(http.StatusOK, result)
}

// resumeExecution 恢复暂停的执行
func (s *Server) resumeExecution(ctx context.Context, c *app.RequestContext) {
    workflowID := c.Param("id")
    executionID := c.Param("executionId")

    var req ResumeExecutionRequest
    if err := c.BindAndValidate(&req); err != nil {
        c.JSON(http.StatusBadRequest, utils.H{"error": err.Error()})
        return
    }

    // 发送信号给Temporal工作流以恢复执行
    err := s.temporalClient.SignalWorkflow(ctx, executionID, "", "resume-signal", req.ResumeInput)
    if err != nil {
        c.JSON(http.StatusInternalServerError, utils.H{"error": err.Error()})
        return
    }

    c.JSON(http.StatusOK, utils.H{
        "execution_id": executionID,
        "workflow_id":  workflowID,
        "status":       "resumed",
    })
}

// healthCheck 健康检查
func (s *Server) healthCheck(ctx context.Context, c *app.RequestContext) {
    c.JSON(http.StatusOK, utils.H{
        "status": "healthy",
        "service": "go-workflow-engine",
    })
}

// ResumeExecutionRequest 恢复执行请求
type ResumeExecutionRequest struct {
    ResumeInput map[string]interface{} `json:"resume_input"`
}

// Start 启动服务器
func (s *Server) Start() error {
    return s.hertzServer.Run()
}

// generateExecutionID 生成执行ID
func generateExecutionID() string {
    return fmt.Sprintf("exec_%d", time.Now().UnixNano())
}

// ExecutionResult 执行结果结构体
type ExecutionResult struct {
    Success     bool                   `json:"success"`
    Output      map[string]interface{} `json:"output"`
    Error       string                 `json:"error,omitempty"`
    Logs        []ExecutionLog         `json:"logs"`
    Metadata    *ExecutionMetadata     `json:"metadata"`
    Status      string                 `json:"status"`
    PausePoints []PausePoint           `json:"pause_points,omitempty"`
}

// ExecutionLog 执行日志
type ExecutionLog struct {
    Level     string                 `json:"level"`
    Message   string                 `json:"message"`
    Timestamp time.Time              `json:"timestamp"`
    BlockID   string                 `json:"block_id,omitempty"`
    Data      map[string]interface{} `json:"data,omitempty"`
}

// ExecutionMetadata 执行元数据
type ExecutionMetadata struct {
    RequestID     string        `json:"request_id"`
    ExecutionID   string        `json:"execution_id"`
    WorkflowID    string        `json:"workflow_id"`
    WorkspaceID   *string       `json:"workspace_id"`
    UserID        string        `json:"user_id"`
    TriggerType   string        `json:"trigger_type"`
    StartTime     time.Time     `json:"start_time"`
    EndTime       *time.Time    `json:"end_time"`
    Duration      time.Duration `json:"duration"`
}

// PausePoint 暂停点
type PausePoint struct {
    BlockID     string                 `json:"block_id"`
    ContextID   string                 `json:"context_id"`
    Message     string                 `json:"message"`
    InputSchema map[string]interface{} `json:"input_schema,omitempty"`
}
```

### 8. Block执行器实现

```go
// internal/blocks/registry.go
package blocks

import (
    "context"
    "fmt"
    "workflow-engine/ent"
)

// BlockExecutor Block执行器接口
type BlockExecutor interface {
    Execute(ctx context.Context, block *ent.WorkflowBlock, inputs map[string]interface{}, execCtx *ExecutionContext) (*BlockResult, error)
    GetType() string
    Validate(block *ent.WorkflowBlock) error
}

// BlockResult Block执行结果
type BlockResult struct {
    Outputs    map[string]interface{} `json:"outputs"`
    Logs       []ExecutionLog         `json:"logs"`
    PausePoint *PausePoint            `json:"pause_point,omitempty"`
}

// ExecutionContext 执行上下文
type ExecutionContext struct {
    WorkflowID      string                 `json:"workflow_id"`
    ExecutionID     string                 `json:"execution_id"`
    UserID          string                 `json:"user_id"`
    WorkspaceID     *string                `json:"workspace_id"`
    Variables       map[string]interface{} `json:"variables"`
    Environment     map[string]string      `json:"environment"`
    TriggerType     string                 `json:"trigger_type"`
    IsDeployed      bool                   `json:"is_deployed"`
    Stream          bool                   `json:"stream"`
    SelectedOutputs []string               `json:"selected_outputs"`
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
    registry.Register(&StarterBlockExecutor{})

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

// internal/blocks/agent.go
package blocks

import (
    "context"
    "encoding/json"
    "fmt"
    "time"
    
    "github.com/cloudwego/hertz/pkg/common/hlog"
    "workflow-engine/ent"
    "workflow-engine/internal/llm"
)

// AgentBlockExecutor Agent Block执行器
type AgentBlockExecutor struct {
    llmProvider llm.Provider
}

// Execute 执行Agent Block
func (e *AgentBlockExecutor) Execute(ctx context.Context, block *ent.WorkflowBlock, inputs map[string]interface{}, execCtx *ExecutionContext) (*BlockResult, error) {
    hlog.CtxInfof(ctx, "Executing Agent block: %s", block.ID)
    
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

    logs := []ExecutionLog{
        {
            Level:     "info",
            Message:   fmt.Sprintf("Agent block executed successfully, tokens used: %d", response.Tokens),
            Timestamp: time.Now(),
            BlockID:   block.ID,
            Data: map[string]interface{}{
                "model":  response.Model,
                "tokens": response.Tokens,
            },
        },
    }

    return &BlockResult{
        Outputs: output,
        Logs:    logs,
    }, nil
}

// GetType 获取Block类型
func (e *AgentBlockExecutor) GetType() string {
    return "agent"
}

// Validate 验证Block配置
func (e *AgentBlockExecutor) Validate(block *ent.WorkflowBlock) error {
    var subBlocks map[string]interface{}
    if err := json.Unmarshal(block.SubBlocks, &subBlocks); err != nil {
        return fmt.Errorf("invalid sub_blocks JSON: %w", err)
    }
    
    if model, exists := subBlocks["model"]; !exists || model == "" {
        return fmt.Errorf("model is required")
    }
    return nil
}

// parseConfig 解析Agent配置
func (e *AgentBlockExecutor) parseConfig(block *ent.WorkflowBlock) (*AgentConfig, error) {
    var subBlocks map[string]interface{}
    if err := json.Unmarshal(block.SubBlocks, &subBlocks); err != nil {
        return nil, err
    }
    
    config := &AgentConfig{}
    
    if model, ok := subBlocks["model"].(string); ok {
        config.Model = model
    }
    
    if temp, ok := subBlocks["temperature"].(float64); ok {
        config.Temperature = temp
    } else {
        config.Temperature = 0.7 // 默认值
    }
    
    return config, nil
}

// prepareMessages 准备消息
func (e *AgentBlockExecutor) prepareMessages(config *AgentConfig, inputs map[string]interface{}) ([]llm.Message, error) {
    messages := []llm.Message{}
    
    if systemPrompt, ok := inputs["system_prompt"].(string); ok && systemPrompt != "" {
        messages = append(messages, llm.Message{
            Role:    "system",
            Content: systemPrompt,
        })
    }
    
    if userMessage, ok := inputs["user_message"].(string); ok && userMessage != "" {
        messages = append(messages, llm.Message{
            Role:    "user",
            Content: userMessage,
        })
    }
    
    return messages, nil
}

// AgentConfig Agent配置
type AgentConfig struct {
    Model          string                 `json:"model"`
    Temperature    float64                `json:"temperature"`
    ResponseFormat map[string]interface{} `json:"response_format,omitempty"`
    Tools          []interface{}          `json:"tools,omitempty"`
    APIKey         string                 `json:"api_key,omitempty"`
}
```

### 9. LLM提供商接口

```go
// internal/llm/provider.go
package llm

import (
    "context"
)

// Provider LLM提供商接口
type Provider interface {
    Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error)
    GetName() string
}

// ChatRequest 聊天请求
type ChatRequest struct {
    Model          string                 `json:"model"`
    Messages       []Message              `json:"messages"`
    Temperature    float64                `json:"temperature"`
    ResponseFormat map[string]interface{} `json:"response_format,omitempty"`
    Tools          []interface{}          `json:"tools,omitempty"`
    APIKey         string                 `json:"api_key,omitempty"`
}

// ChatResponse 聊天响应
type ChatResponse struct {
    Content          string                 `json:"content"`
    Model            string                 `json:"model"`
    Tokens           int                    `json:"tokens"`
    ToolCalls        []interface{}          `json:"tool_calls,omitempty"`
    StructuredOutput map[string]interface{} `json:"structured_output,omitempty"`
}

// Message 消息
type Message struct {
    Role    string `json:"role"`
    Content string `json:"content"`
}

// internal/llm/openai.go
package llm

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "time"
    
    "github.com/cloudwego/hertz/pkg/common/hlog"
)

// OpenAIProvider OpenAI提供商
type OpenAIProvider struct {
    baseURL string
    client  *http.Client
}

// NewOpenAIProvider 创建OpenAI提供商
func NewOpenAIProvider(baseURL string) *OpenAIProvider {
    return &OpenAIProvider{
        baseURL: baseURL,
        client: &http.Client{
            Timeout: 60 * time.Second,
        },
    }
}

// Chat 发送聊天请求
func (p *OpenAIProvider) Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
    hlog.CtxInfof(ctx, "Calling OpenAI API with model: %s", req.Model)
    
    // 构建请求体
    requestBody := map[string]interface{}{
        "model":       req.Model,
        "messages":    req.Messages,
        "temperature": req.Temperature,
    }
    
    if req.ResponseFormat != nil {
        requestBody["response_format"] = req.ResponseFormat
    }
    
    if req.Tools != nil && len(req.Tools) > 0 {
        requestBody["tools"] = req.Tools
    }
    
    jsonData, err := json.Marshal(requestBody)
    if err != nil {
        return nil, fmt.Errorf("failed to marshal request: %w", err)
    }
    
    // 创建HTTP请求
    httpReq, err := http.NewRequestWithContext(ctx, "POST", p.baseURL+"/chat/completions", bytes.NewBuffer(jsonData))
    if err != nil {
        return nil, fmt.Errorf("failed to create request: %w", err)
    }
    
    httpReq.Header.Set("Content-Type", "application/json")
    if req.APIKey != "" {
        httpReq.Header.Set("Authorization", "Bearer "+req.APIKey)
    }
    
    // 发送请求
    resp, err := p.client.Do(httpReq)
    if err != nil {
        return nil, fmt.Errorf("failed to send request: %w", err)
    }
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("API request failed with status: %d", resp.StatusCode)
    }
    
    // 解析响应
    var apiResp OpenAIResponse
    if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
        return nil, fmt.Errorf("failed to decode response: %w", err)
    }
    
    if len(apiResp.Choices) == 0 {
        return nil, fmt.Errorf("no choices in response")
    }
    
    choice := apiResp.Choices[0]
    
    return &ChatResponse{
        Content: choice.Message.Content,
        Model:   apiResp.Model,
        Tokens:  apiResp.Usage.TotalTokens,
    }, nil
}

// GetName 获取提供商名称
func (p *OpenAIProvider) GetName() string {
    return "openai"
}

// OpenAI API响应结构
type OpenAIResponse struct {
    ID      string `json:"id"`
    Object  string `json:"object"`
    Created int64  `json:"created"`
    Model   string `json:"model"`
    Choices []struct {
        Index   int `json:"index"`
        Message struct {
            Role    string `json:"role"`
            Content string `json:"content"`
        } `json:"message"`
        FinishReason string `json:"finish_reason"`
    } `json:"choices"`
    Usage struct {
        PromptTokens     int `json:"prompt_tokens"`
        CompletionTokens int `json:"completion_tokens"`
        TotalTokens      int `json:"total_tokens"`
    } `json:"usage"`
}
```

### 10. 数据访问层实现

```go
// internal/repository/workflow_repository.go
package repository

import (
    "context"
    "workflow-engine/ent"
    "workflow-engine/ent/workflow"
    "workflow-engine/ent/workflowexecutionlog"
    "workflow-engine/ent/pausedexecution"
)

// WorkflowRepository 工作流数据访问接口
type WorkflowRepository interface {
    GetByID(ctx context.Context, id string) (*ent.Workflow, error)
    GetWorkflowWithBlocks(ctx context.Context, id string) (*ent.Workflow, error)
    CreateExecutionLog(ctx context.Context, log *ent.WorkflowExecutionLog) error
    CreatePausedExecution(ctx context.Context, paused *ent.PausedExecution) error
    GetPausedExecution(ctx context.Context, executionID string) (*ent.PausedExecution, error)
    UpdatePausedExecution(ctx context.Context, paused *ent.PausedExecution) error
}

// workflowRepository Ent实现
type workflowRepository struct {
    client *ent.Client
}

// NewWorkflowRepository 创建工作流仓库
func NewWorkflowRepository(client *ent.Client) WorkflowRepository {
    return &workflowRepository{client: client}
}

// GetByID 根据ID获取工作流
func (r *workflowRepository) GetByID(ctx context.Context, id string) (*ent.Workflow, error) {
    return r.client.Workflow.
        Query().
        Where(workflow.ID(id)).
        Only(ctx)
}

// GetWorkflowWithBlocks 获取包含块和边的完整工作流
func (r *workflowRepository) GetWorkflowWithBlocks(ctx context.Context, id string) (*ent.Workflow, error) {
    return r.client.Workflow.
        Query().
        Where(workflow.ID(id)).
        WithBlocks().
        WithEdges().
        WithSubflows().
        Only(ctx)
}

// CreateExecutionLog 创建执行日志
func (r *workflowRepository) CreateExecutionLog(ctx context.Context, log *ent.WorkflowExecutionLog) error {
    _, err := r.client.WorkflowExecutionLog.
        Create().
        SetID(log.ID).
        SetWorkflowID(log.WorkflowID).
        SetExecutionID(log.ExecutionID).
        SetStateSnapshotID(log.StateSnapshotID).
        SetLevel(log.Level).
        SetTrigger(log.Trigger).
        SetStartedAt(log.StartedAt).
        SetNillableEndedAt(log.EndedAt).
        SetNillableTotalDurationMs(log.TotalDurationMs).
        SetExecutionData(log.ExecutionData).
        SetNillableCost(log.Cost).
        SetNillableFiles(log.Files).
        Save(ctx)
    return err
}

// CreatePausedExecution 创建暂停执行记录
func (r *workflowRepository) CreatePausedExecution(ctx context.Context, paused *ent.PausedExecution) error {
    _, err := r.client.PausedExecution.
        Create().
        SetID(paused.ID).
        SetWorkflowID(paused.WorkflowID).
        SetExecutionID(paused.ExecutionID).
        SetExecutionSnapshot(paused.ExecutionSnapshot).
        SetPausePoints(paused.PausePoints).
        SetTotalPauseCount(paused.TotalPauseCount).
        SetResumedCount(paused.ResumedCount).
        SetStatus(paused.Status).
        SetMetadata(paused.Metadata).
        SetPausedAt(paused.PausedAt).
        SetNillableExpiresAt(paused.ExpiresAt).
        Save(ctx)
    return err
}

// GetPausedExecution 获取暂停执行记录
func (r *workflowRepository) GetPausedExecution(ctx context.Context, executionID string) (*ent.PausedExecution, error) {
    return r.client.PausedExecution.
        Query().
        Where(pausedexecution.ExecutionID(executionID)).
        Only(ctx)
}

// UpdatePausedExecution 更新暂停执行记录
func (r *workflowRepository) UpdatePausedExecution(ctx context.Context, paused *ent.PausedExecution) error {
    return r.client.PausedExecution.
        UpdateOneID(paused.ID).
        SetResumedCount(paused.ResumedCount).
        SetStatus(paused.Status).
        SetMetadata(paused.Metadata).
        SetUpdatedAt(paused.UpdatedAt).
        SetNillableExpiresAt(paused.ExpiresAt).
        Exec(ctx)
}

// internal/config/config.go
package config

import (
    "os"
    "strconv"
)

// Config 应用配置
type Config struct {
    Server   ServerConfig
    Database DatabaseConfig
    Redis    RedisConfig
    Temporal TemporalConfig
    LLM      LLMConfig
}

// ServerConfig 服务器配置
type ServerConfig struct {
    Host string
    Port int
}

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
    Host     string
    Port     int
    User     string
    Password string
    DBName   string
    SSLMode  string
}

// RedisConfig Redis配置
type RedisConfig struct {
    Host     string
    Port     int
    Password string
    DB       int
}

// TemporalConfig Temporal配置
type TemporalConfig struct {
    HostPort  string
    Namespace string
    TaskQueue string
}

// LLMConfig LLM配置
type LLMConfig struct {
    DefaultProvider string
    Providers       map[string]string
}

// Load 从环境变量加载配置
func Load() *Config {
    return &Config{
        Server: ServerConfig{
            Host: getEnv("SERVER_HOST", "0.0.0.0"),
            Port: getEnvInt("SERVER_PORT", 8080),
        },
        Database: DatabaseConfig{
            Host:     getEnv("DATABASE_HOST", "localhost"),
            Port:     getEnvInt("DATABASE_PORT", 5432),
            User:     getEnv("DATABASE_USER", "postgres"),
            Password: getEnv("DATABASE_PASSWORD", "password"),
            DBName:   getEnv("DATABASE_NAME", "sim"),
            SSLMode:  getEnv("DATABASE_SSLMODE", "disable"),
        },
        Redis: RedisConfig{
            Host:     getEnv("REDIS_HOST", "localhost"),
            Port:     getEnvInt("REDIS_PORT", 6379),
            Password: getEnv("REDIS_PASSWORD", ""),
            DB:       getEnvInt("REDIS_DB", 0),
        },
        Temporal: TemporalConfig{
            HostPort:  getEnv("TEMPORAL_HOST_PORT", "localhost:7233"),
            Namespace: getEnv("TEMPORAL_NAMESPACE", "default"),
            TaskQueue: getEnv("TEMPORAL_TASK_QUEUE", "sim-workflow-queue"),
        },
        LLM: LLMConfig{
            DefaultProvider: getEnv("LLM_DEFAULT_PROVIDER", "openai"),
            Providers: map[string]string{
                "openai":    getEnv("OPENAI_API_URL", "https://api.openai.com/v1"),
                "anthropic": getEnv("ANTHROPIC_API_URL", "https://api.anthropic.com"),
            },
        },
    }
}

// getEnv 获取环境变量，如果不存在则返回默认值
func getEnv(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}

// getEnvInt 获取整数类型的环境变量
func getEnvInt(key string, defaultValue int) int {
    if value := os.Getenv(key); value != "" {
        if intValue, err := strconv.Atoi(value); err == nil {
            return intValue
        }
    }
    return defaultValue
}
```

### 9. 主程序入口

```go
// cmd/server/main.go
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "syscall"
    
    "github.com/cloudwego/hertz/pkg/common/hlog"
    "go.temporal.io/sdk/client"
    "go.temporal.io/sdk/worker"
    "workflow-engine/ent"
    
    "workflow-engine/internal/api"
    "workflow-engine/internal/activities"
    "workflow-engine/internal/blocks"
    "workflow-engine/internal/config"
    "workflow-engine/internal/repository"
    "workflow-engine/internal/workflows"
)

func main() {
    // 1. 加载配置
    cfg := config.Load()
    
    // 2. 配置Hertz日志
    hlog.SetLevel(hlog.LevelInfo)
    hlog.SetOutput(os.Stdout)
    hlog.Info("Starting Go Workflow Engine...")

    // 3. 连接数据库
    dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
        cfg.Database.Host, cfg.Database.Port, cfg.Database.User,
        cfg.Database.Password, cfg.Database.DBName, cfg.Database.SSLMode)
    
    entClient, err := ent.Open("postgres", dsn)
    if err != nil {
        hlog.Fatalf("Failed to connect to database: %v", err)
    }
    defer entClient.Close()

    // 运行数据库迁移
    if err := entClient.Schema.Create(context.Background()); err != nil {
        hlog.Fatalf("Failed to create schema: %v", err)
    }
    hlog.Info("Database connection established and schema created")

    // 4. 创建仓库
    workflowRepo := repository.NewWorkflowRepository(entClient)

    // 5. 连接Temporal
    temporalClient, err := client.Dial(client.Options{
        HostPort:  cfg.Temporal.HostPort,
        Namespace: cfg.Temporal.Namespace,
    })
    if err != nil {
        hlog.Fatalf("Failed to connect to Temporal: %v", err)
    }
    defer temporalClient.Close()
    hlog.Infof("Connected to Temporal at %s", cfg.Temporal.HostPort)

    // 6. 创建Block注册表
    blockRegistry := blocks.NewBlockRegistry()

    // 7. 创建Activities
    workflowActivities := &activities.WorkflowActivities{
        WorkflowRepo:  workflowRepo,
        BlockRegistry: blockRegistry,
    }

    // 8. 启动Temporal Worker
    w := worker.New(temporalClient, cfg.Temporal.TaskQueue, worker.Options{})
    w.RegisterWorkflow(workflows.SimWorkflow)
    w.RegisterActivity(workflowActivities.GetWorkflowActivity)
    w.RegisterActivity(workflowActivities.BuildDAGActivity)
    w.RegisterActivity(workflowActivities.ExecuteBlockActivity)

    // 9. 启动API服务器
    apiServer := api.NewServer(cfg, temporalClient, workflowRepo)

    // 10. 启动服务
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // 启动Temporal Worker
    go func() {
        hlog.Info("Starting Temporal worker...")
        if err := w.Run(worker.InterruptCh()); err != nil {
            hlog.Fatalf("Failed to start Temporal worker: %v", err)
        }
    }()

    // 启动API服务器
    go func() {
        hlog.Infof("Starting API server on %s:%d", cfg.Server.Host, cfg.Server.Port)
        if err := apiServer.Start(); err != nil {
            hlog.Fatalf("Failed to start API server: %v", err)
        }
    }()

    // 等待中断信号
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh

    hlog.Info("Shutting down gracefully...")
    cancel()
}

// cmd/worker/main.go
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "syscall"
    
    "github.com/cloudwego/hertz/pkg/common/hlog"
    "go.temporal.io/sdk/client"
    "go.temporal.io/sdk/worker"
    "workflow-engine/ent"
    
    "workflow-engine/internal/activities"
    "workflow-engine/internal/blocks"
    "workflow-engine/internal/config"
    "workflow-engine/internal/repository"
    "workflow-engine/internal/workflows"
)

func main() {
    // 专用的Worker进程，用于扩展处理能力
    cfg := config.Load()
    
    // 配置日志
    hlog.SetLevel(hlog.LevelInfo)
    hlog.SetOutput(os.Stdout)
    hlog.Info("Starting Workflow Worker...")

    // 连接数据库和Temporal
    dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
        cfg.Database.Host, cfg.Database.Port, cfg.Database.User,
        cfg.Database.Password, cfg.Database.DBName, cfg.Database.SSLMode)
    
    entClient, err := ent.Open("postgres", dsn)
    if err != nil {
        hlog.Fatalf("Failed to connect to database: %v", err)
    }
    defer entClient.Close()
    hlog.Info("Database connection established")

    temporalClient, err := client.Dial(client.Options{
        HostPort:  cfg.Temporal.HostPort,
        Namespace: cfg.Temporal.Namespace,
    })
    if err != nil {
        hlog.Fatalf("Failed to connect to Temporal: %v", err)
    }
    defer temporalClient.Close()
    hlog.Infof("Connected to Temporal at %s", cfg.Temporal.HostPort)

    // 创建Worker
    workflowRepo := repository.NewWorkflowRepository(entClient)
    blockRegistry := blocks.NewBlockRegistry()
    workflowActivities := &activities.WorkflowActivities{
        WorkflowRepo:  workflowRepo,
        BlockRegistry: blockRegistry,
    }

    w := worker.New(temporalClient, cfg.Temporal.TaskQueue, worker.Options{})
    w.RegisterWorkflow(workflows.SimWorkflow)
    w.RegisterActivity(workflowActivities.GetWorkflowActivity)
    w.RegisterActivity(workflowActivities.BuildDAGActivity)
    w.RegisterActivity(workflowActivities.ExecuteBlockActivity)

    // 启动Worker
    go func() {
        hlog.Infof("Starting worker for task queue: %s", cfg.Temporal.TaskQueue)
        if err := w.Run(worker.InterruptCh()); err != nil {
            hlog.Fatalf("Failed to start worker: %v", err)
        }
    }()

    // 等待中断信号
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh

    hlog.Info("Worker shutting down gracefully...")
}

## 项目初始化和依赖管理

### 1. Go模块初始化

```bash
# 初始化Go模块
go mod init workflow-engine

# 添加主要依赖
go get github.com/cloudwego/hertz
go get go.temporal.io/sdk
go get entgo.io/ent/cmd/ent
go get github.com/lib/pq  # PostgreSQL驱动
go get github.com/go-redis/redis/v8
```

### 2. go.mod文件

```go
module workflow-engine

go 1.21

require (
    entgo.io/ent v0.12.4
    github.com/cloudwego/hertz v0.7.2
    github.com/go-redis/redis/v8 v8.11.5
    github.com/lib/pq v1.10.9
    go.temporal.io/sdk v1.24.0
)

require (
    // 其他间接依赖会自动添加
)
```

### 3. 初始化Ent项目

```bash
# 安装Ent CLI
go install entgo.io/ent/cmd/ent@latest

# 初始化Ent项目
go mod init workflow-engine
ent init --target ent/schema Workflow WorkflowBlock WorkflowEdge WorkflowSubflow WorkflowExecutionLog PausedExecution

# 生成代码
go generate ./ent
```

### 2. Ent配置文件

```go
// ent/generate.go
package ent

//go:generate go run -mod=mod entgo.io/ent/cmd/ent generate --target ./generated --feature sql/versioned-migration ./schema
```

### 3. 数据库迁移

```go
// internal/migrate/migrate.go
package migrate

import (
    "context"
    "workflow-engine/ent"
    "workflow-engine/ent/migrate"
)

// AutoMigrate 自动迁移数据库
func AutoMigrate(ctx context.Context, client *ent.Client) error {
    return client.Schema.Create(
        ctx,
        migrate.WithDropIndex(true),
        migrate.WithDropColumn(true),
    )
}

// CreateMigrationFiles 创建迁移文件
func CreateMigrationFiles(ctx context.Context, client *ent.Client, name string) error {
    return client.Schema.Diff(ctx, migrate.WithMigrationMode(migrate.ModeReplay))
}
```

### 4. Makefile命令

```makefile
# Makefile
.PHONY: generate migrate build run test clean docker

# 生成Ent代码
generate:
	go generate ./ent

# 创建迁移文件
migrate-create:
	go run -mod=mod ./cmd/migrate create $(name)

# 运行迁移
migrate-up:
	go run -mod=mod ./cmd/migrate up

# 构建项目
build:
	CGO_ENABLED=0 GOOS=linux go build -o bin/server ./cmd/server
	CGO_ENABLED=0 GOOS=linux go build -o bin/worker ./cmd/worker

# 本地构建
build-local:
	go build -o bin/server ./cmd/server
	go build -o bin/worker ./cmd/worker

# 运行服务器
run-server:
	go run ./cmd/server

# 运行Worker
run-worker:
	go run ./cmd/worker

# 运行测试
test:
	go test -v ./...

# 运行测试覆盖率
test-coverage:
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# 代码格式化
fmt:
	go fmt ./...

# 代码检查
lint:
	golangci-lint run

# 清理生成的文件
clean:
	rm -rf ent/generated
	rm -rf bin/
	rm -f coverage.out coverage.html

# Docker构建
docker-build:
	docker build -t workflow-engine:latest .

# Docker运行
docker-run:
	docker-compose up -d

# 停止Docker
docker-stop:
	docker-compose down

# 查看日志
logs:
	docker-compose logs -f workflow-engine

# 完整部署
deploy: clean generate build docker-build docker-run

# 开发环境启动
dev:
	docker-compose -f docker-compose.dev.yml up -d
```

### 5. 开发环境配置

```yaml
# docker-compose.dev.yml
version: '3.8'

services:
  postgres:
    image: pgvector/pgvector:pg15
    environment:
      - POSTGRES_DB=sim
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=password
    ports:
      - "5432:5432"
    volumes:
      - postgres_dev_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  temporal:
    image: temporalio/auto-setup:1.22.0
    ports:
      - "7233:7233"
      - "8233:8233"
    environment:
      - DB=postgresql
      - DB_PORT=5432
      - POSTGRES_USER=temporal
      - POSTGRES_PWD=temporal
      - POSTGRES_SEEDS=postgres
    depends_on:
      - postgres

volumes:
  postgres_dev_data:

### 6. 项目工具脚本

```bash
#!/bin/bash
# scripts/setup.sh - 项目初始化脚本

set -e

echo "🚀 Setting up Go Workflow Engine..."

# 检查Go版本
if ! command -v go &> /dev/null; then
    echo "❌ Go is not installed. Please install Go 1.21 or later."
    exit 1
fi

GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
if [[ "$(printf '%s\n' "1.21" "$GO_VERSION" | sort -V | head -n1)" != "1.21" ]]; then
    echo "❌ Go version 1.21 or later is required. Current version: $GO_VERSION"
    exit 1
fi

echo "✅ Go version: $GO_VERSION"

# 安装依赖
echo "📦 Installing dependencies..."
go mod tidy
go mod download

# 安装开发工具
echo "🔧 Installing development tools..."
go install entgo.io/ent/cmd/ent@latest
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# 生成Ent代码
echo "🏗️  Generating Ent code..."
go generate ./ent

# 创建必要的目录
mkdir -p bin
mkdir -p logs
mkdir -p configs

# 复制示例配置
if [ ! -f .env ]; then
    echo "📝 Creating .env file..."
    cat > .env << EOF
# 服务器配置
SERVER_HOST=0.0.0.0
SERVER_PORT=8080

# 数据库配置
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_USER=postgres
DATABASE_PASSWORD=password
DATABASE_NAME=sim
DATABASE_SSLMODE=disable

# Redis配置
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_DB=0

# Temporal配置
TEMPORAL_HOST_PORT=localhost:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_TASK_QUEUE=sim-workflow-queue

# LLM配置
LLM_DEFAULT_PROVIDER=openai
OPENAI_API_URL=https://api.openai.com/v1
ANTHROPIC_API_URL=https://api.anthropic.com

# API Keys (请填入实际的API密钥)
OPENAI_API_KEY=your-openai-api-key
ANTHROPIC_API_KEY=your-anthropic-api-key
EOF
fi

echo "✅ Setup completed successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Update .env file with your actual configuration"
echo "2. Start development services: make dev"
echo "3. Run the server: make run-server"
echo "4. Run the worker: make run-worker"
echo ""
echo "🔗 Useful commands:"
echo "  make build          - Build the project"
echo "  make test           - Run tests"
echo "  make generate       - Generate Ent code"
echo "  make docker-build   - Build Docker image"
echo "  make deploy         - Full deployment"
```

```bash
#!/bin/bash
# scripts/test.sh - 测试脚本

set -e

echo "🧪 Running Go Workflow Engine tests..."

# 运行单元测试
echo "📋 Running unit tests..."
go test -v -race ./internal/...

# 运行集成测试
echo "🔗 Running integration tests..."
go test -v -tags=integration ./tests/...

# 生成测试覆盖率报告
echo "📊 Generating coverage report..."
go test -v -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html

echo "✅ All tests passed!"
echo "📈 Coverage report generated: coverage.html"
```

### 7. CI/CD配置

```yaml
# .github/workflows/ci.yml
name: CI

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      postgres:
        image: pgvector/pgvector:pg15
        env:
          POSTGRES_PASSWORD: password
          POSTGRES_DB: sim_test
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 5432:5432
      
      redis:
        image: redis:7-alpine
        options: >-
          --health-cmd "redis-cli ping"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 6379:6379

    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Go
      uses: actions/setup-go@v4
      with:
        go-version: '1.21'
    
    - name: Cache Go modules
      uses: actions/cache@v3
      with:
        path: ~/go/pkg/mod
        key: ${{ runner.os }}-go-${{ hashFiles('**/go.sum') }}
        restore-keys: |
          ${{ runner.os }}-go-
    
    - name: Install dependencies
      run: |
        go mod download
        go install entgo.io/ent/cmd/ent@latest
    
    - name: Generate code
      run: go generate ./ent
    
    - name: Run tests
      env:
        DATABASE_HOST: localhost
        DATABASE_PORT: 5432
        DATABASE_USER: postgres
        DATABASE_PASSWORD: password
        DATABASE_NAME: sim_test
        REDIS_HOST: localhost
        REDIS_PORT: 6379
      run: |
        go test -v -race -coverprofile=coverage.out ./...
    
    - name: Upload coverage to Codecov
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.out
    
    - name: Build
      run: |
        make build
    
    - name: Lint
      uses: golangci/golangci-lint-action@v3
      with:
        version: latest

  docker:
    runs-on: ubuntu-latest
    needs: test
    
    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Docker Buildx
      uses: docker/setup-buildx-action@v3
    
    - name: Build Docker image
      run: |
        docker build -t workflow-engine:${{ github.sha }} .
        docker build -t workflow-engine:latest .
```
```

## 部署配置

### 1. Docker配置

```dockerfile
# Dockerfile
FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o workflow-engine ./cmd/server

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/workflow-engine .
COPY --from=builder /app/configs ./configs

CMD ["./workflow-engine"]
```

```yaml
# docker-compose.yml
version: '3.8'

services:
  workflow-engine:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "8080:8080"
    environment:
      - SERVER_HOST=0.0.0.0
      - SERVER_PORT=8080
      - DATABASE_HOST=postgres
      - DATABASE_PORT=5432
      - DATABASE_USER=postgres
      - DATABASE_PASSWORD=password
      - DATABASE_NAME=sim
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - TEMPORAL_HOST_PORT=temporal:7233
      - TEMPORAL_NAMESPACE=default
      - TEMPORAL_TASK_QUEUE=sim-workflow-queue
    depends_on:
      - postgres
      - redis
      - temporal

  temporal:
    image: temporalio/auto-setup:1.22.0
    ports:
      - "7233:7233"
      - "8233:8233"
    environment:
      - DB=postgresql
      - DB_PORT=5432
      - POSTGRES_USER=temporal
      - POSTGRES_PWD=temporal
      - POSTGRES_SEEDS=postgres
    depends_on:
      - postgres

  postgres:
    image: pgvector/pgvector:pg15
    environment:
      - POSTGRES_DB=sim
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=password
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

volumes:
  postgres_data:
```

### 2. 环境变量配置

```bash
# .env 文件
# 服务器配置
SERVER_HOST=0.0.0.0
SERVER_PORT=8080

# 数据库配置
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_USER=postgres
DATABASE_PASSWORD=password
DATABASE_NAME=sim
DATABASE_SSLMODE=disable

# Redis配置
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_DB=0

# Temporal配置
TEMPORAL_HOST_PORT=localhost:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_TASK_QUEUE=sim-workflow-queue

# LLM配置
LLM_DEFAULT_PROVIDER=openai
OPENAI_API_URL=https://api.openai.com/v1
ANTHROPIC_API_URL=https://api.anthropic.com

# OpenAI API Key (如果需要)
OPENAI_API_KEY=your-openai-api-key
ANTHROPIC_API_KEY=your-anthropic-api-key
```

## 核心功能实现

### 1. Temporal工作流优势

- **可靠性**: 自动重试和错误恢复
- **可扩展性**: 水平扩展Worker节点
- **可观测性**: 内置监控和追踪
- **状态管理**: 自动持久化执行状态
- **暂停/恢复**: 原生支持人工干预

### 2. 高性能特性

- **并发执行**: Temporal自动管理并发
- **资源优化**: Go语言的高效内存管理
- **缓存策略**: Redis缓存Block输出结果
- **连接池**: 数据库连接池优化

### 3. 监控和观测

- **Temporal UI**: 工作流执行可视化
- **Prometheus指标**: 自定义业务指标
- **结构化日志**: 便于问题排查
- **分布式追踪**: 完整的执行链路

## 与现有系统集成

### 1. API兼容性

- **相同接口**: 保持与TypeScript版本相同的API
- **数据格式**: 使用相同的请求/响应结构
- **错误处理**: 统一的错误码和消息格式

### 2. 数据库兼容

- **现有Schema**: 直接使用现有数据库表结构
- **Ent Schema**: 类型安全的数据库操作
- **自动迁移**: Ent自动生成迁移文件
- **事务支持**: 保证数据一致性

### 3. 渐进式迁移

- **并行运行**: 可与现有TypeScript引擎并行
- **流量切换**: 逐步将流量切换到Go引擎
- **回滚机制**: 支持快速回滚到原系统

## 性能优势

### 1. 执行性能

- **编译语言**: Go的原生性能优势
- **并发模型**: Goroutine轻量级并发
- **内存效率**: 更低的内存占用和GC压力

### 2. 扩展性

- **水平扩展**: 支持多Worker节点
- **负载均衡**: Temporal自动负载分配
- **资源隔离**: 不同类型Block可分配到不同Worker

### 3. 可靠性

- **故障恢复**: Temporal自动处理节点故障
- **重试机制**: 可配置的重试策略
- **状态持久化**: 执行状态自动保存

## 总结

这个基于Go + Hertz + Temporal + Ent的工作流执行引擎设计方案提供了：

1. **高性能**: Go语言 + Temporal分布式架构
2. **高可靠**: 自动故障恢复和状态管理  
3. **易扩展**: 水平扩展和模块化设计
4. **强兼容**: 完全兼容现有数据库和API
5. **类型安全**: Ent提供编译时类型检查
6. **可观测**: 完整的监控和日志系统

通过使用Temporal作为工作流编排引擎，我们获得了企业级的可靠性和扩展性，Hertz框架提供了高性能的HTTP服务能力，而Ent确保了类型安全的数据库操作。整个系统可以无缝替换现有的TypeScript执行引擎，提供更好的性能和稳定性。

## Ent的优势

1. **类型安全**: 编译时检查，减少运行时错误
2. **代码生成**: 自动生成CRUD操作和查询构建器
3. **Schema优先**: 通过Schema定义驱动开发
4. **迁移管理**: 自动生成和管理数据库迁移
5. **关系处理**: 优雅的关联查询和预加载
6. **性能优化**: 自动查询优化和批量操作
