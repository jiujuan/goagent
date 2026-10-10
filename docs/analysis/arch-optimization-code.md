现在我有足够的信息来写出详细的修改方案。让我逐个文件编写完整的代码修改方案。

---

## 按优先级详细分析：P0/P1/P2 优化的代码修改方案

### **第一部分：P0 优先级 - 核心功能缺口**

---

## **P0-1: 工具回退链（Tool Fallback Chain）**

### 文件1：`tool/tool.go` - 增强 Result 结构体

**当前代码**（第 25-38 行）：
```go
type Result struct {
	Content []core.Part
	IsError bool
	Control *core.Directive
	State []core.StateOp
}
```

**修改后代码**：
```go
type Result struct {
	Content []core.Part
	IsError bool
	
	// 现有字段
	Control *core.Directive
	State []core.StateOp
	
	// ========== 新增：执行元数据与回退支持 ==========
	
	// ExecutionTime 记录工具实际执行耗时（非 Context 超时时间）
	ExecutionTime time.Duration
	
	// Cached 标记此结果是否来自缓存（支持工具结果缓存）
	Cached bool
	
	// RetryCount 记录内部重试次数（工具级重试）
	RetryCount int
	
	// FallbackTools 列出若此工具失败应尝试的备选工具名称列表
	// 例如 []string{"get_weather_cached", "get_weather_fallback"}
	// 仅在 IsError=true 时被代理评估
	FallbackTools []string
	
	// FallbackPolicy 控制备选工具的执行策略
	// "first_success": 尝试直到某个工具成功（默认）
	// "all": 尝试所有备选工具，收集所有结果
	// "none": 不尝试备选工具，直接返回失败给模型
	FallbackPolicy FallbackPolicyType
	
	// RetryableAfterMs 建议多少毫秒后重试该工具（0 表示不可重试）
	// 常用于速率限制：工具返回 429，建议 RetryableAfterMs=5000
	RetryableAfterMs int64
}

// FallbackPolicyType 定义备选工具的执行策略
type FallbackPolicyType string

const (
	FallbackFirstSuccess FallbackPolicyType = "first_success"
	FallbackAll          FallbackPolicyType = "all"
	FallbackNone         FallbackPolicyType = "none"
)
```

**使用示例**（在工具实现中）：
```go
// 示例：get_weather 工具的实现
func getWeatherHandler(ctx *tool.Context, args struct {
	City string `json:"city" desc:"城市名"`
}) (*tool.Result, error) {
	// 尝试主接口
	result, err := callWeatherAPI(args.City)
	if err != nil {
		// 失败时，建议使用备选工具
		return &tool.Result{
			IsError: true,
			Content: []core.Part{core.Text{Text: "Weather API 失败: " + err.Error()}},
			
			// 告诉运行时应该尝试的备选工具
			FallbackTools: []string{
				"get_weather_cached",      // 先试缓存版本
				"get_weather_fallback",    // 再试备选源
			},
			FallbackPolicy: tool.FallbackFirstSuccess,
			
			// 如果是速率限制，告诉运行时何时可重试
			RetryableAfterMs: 5000,
		}, nil  // 注意：返回 nil error，失败信息在 Result 中
	}
	
	return &tool.Result{
		Content: []core.Part{core.Text{Text: result}},
		ExecutionTime: time.Since(start),
		Cached: false,
	}, nil
}
```

**导入语句修改**（tool/tool.go 顶部）：
```go
import (
	"context"
	"encoding/json"
	"time"  // ← 新增

	"github.com/jiujuan/goagent/core"
)
```

---

### 文件2：`agent/exectools.go` - 在工具批次执行中增加回退逻辑

**当前代码**（第 35-127 行 execTools 函数）核心调用点在第 53 行：
```go
ch := make(chan toolOutcome, 1)
go func() { ch <- l.callOne(lc, callCtx, c) }()
```

**修改后代码** - 添加新的回退执行函数：

```go
package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// === 新增：回退工具执行逻辑 ===

// executeWithFallback 执行单个工具调用，若失败且配置了回退工具，
// 则按 FallbackPolicy 执行备选工具。返回第一个成功的结果或最后一个失败结果。
func (l *AgentLoop) executeWithFallback(
	lc *LoopContext,
	callCtx context.Context,
	c core.ToolCall,
	start time.Time,
) toolOutcome {
	// 第一次尝试
	out := l.callOne(lc, callCtx, c)
	
	// 若成功或无回退配置，直接返回
	if !out.tr.IsError {
		return out
	}
	
	// 检查结果中的回退建议
	if len(out.fallbackInfo.FallbackTools) == 0 || 
	   out.fallbackInfo.FallbackPolicy == tool.FallbackNone {
		return out
	}
	
	return l.tryFallbacks(lc, callCtx, c, out, out.fallbackInfo)
}

// tryFallbacks 执行回退工具链
func (l *AgentLoop) tryFallbacks(
	lc *LoopContext,
	callCtx context.Context,
	originalCall core.ToolCall,
	originalResult toolOutcome,
	fbInfo *FallbackInfo,
) toolOutcome {
	results := []toolOutcome{originalResult}
	
	for _, fbToolName := range fbInfo.FallbackTools {
		// 构造备选工具调用
		fbCall := core.ToolCall{
			ID:   core.NewID("call"),
			Name: fbToolName,
			Args: originalCall.Args, // 使用原始参数
		}
		
		// 为备选工具创建新的超时上下文
		fbCtx, fbCancel := context.WithTimeout(
			context.Background(),
			l.toolTimeout,
		)
		defer fbCancel()
		
		fbOut := l.callOne(lc, fbCtx, fbCall)
		results = append(results, fbOut)
		
		// 根据策略决定是否继续
		switch fbInfo.FallbackPolicy {
		case tool.FallbackFirstSuccess:
			if !fbOut.tr.IsError {
				// 成功，返回
				lc.RunContext.publish(core.ToolDone{Result: fbOut.tr})
				return fbOut
			}
		case tool.FallbackAll:
			// 继续收集所有结果
			lc.RunContext.publish(core.ToolDone{Result: fbOut.tr})
			continue
		}
	}
	
	// 若执行了所有回退仍失败，返回最后的结果
	return results[len(results)-1]
}

// FallbackInfo 从 Result 中提取的回退配置信息
type FallbackInfo struct {
	FallbackTools    []string
	FallbackPolicy   tool.FallbackPolicyType
	RetryableAfterMs int64
}

// === 修改：execTools 函数中的工具执行逻辑 ===

// 将原来的 run() 闭包中的这一行：
//   go func() { ch <- l.callOne(lc, callCtx, c) }()
// 改为：
func (l *AgentLoop) execTools(rc *RunContext, lc *LoopContext, calls []core.ToolCall) ([]core.Part, []core.Directive, []ToolRejection) {
	results := make([]core.Part, len(calls))
	dirs := make([]core.Directive, len(calls))
	var stateMu sync.Mutex
	var rejects []ToolRejection
	var rejectMu sync.Mutex

	run := func(i int, c core.ToolCall) {
		callCtx, cancel := l.toolCallCtx(lc, &c)
		defer cancel()

		// 使用新的 executeWithFallback 替代 callOne
		ch := make(chan toolOutcome, 1)
		go func() { 
			// ← 关键修改：用 executeWithFallback 包裹 callOne
			ch <- l.executeWithFallback(lc, callCtx, c, time.Now())
		}()

		start := time.Now()
		var out toolOutcome
		select {
		case out = <-ch:
		case <-callCtx.Done():
			out = awaitAbandoned(ch, callCtx, c, start)
		}
		tr := out.tr

		// ... 后续代码保持不变
		if len(out.ops) > 0 {
			stateMu.Lock()
			rc.State.Apply(out.ops...)
			stateMu.Unlock()
		}
		// ...（其余代码如原样）
	}
	
	// ... 序列/并发执行逻辑保持不变
	needSeq := l.toolExec == ToolSequential
	if !needSeq {
		for _, c := range calls {
			if l.isSequential(rc, c.Name) {
				needSeq = true
				break
			}
		}
	}
	if needSeq {
		for i := range calls {
			rc.publish(core.ToolStarted{Call: calls[i]})
			run(i, calls[i])
		}
		return results, dirs, rejects
	}

	var wg sync.WaitGroup
	for i := range calls {
		rc.publish(core.ToolStarted{Call: calls[i]})
		wg.Add(1)
		go func(i int, c core.ToolCall) {
			defer wg.Done()
			run(i, c)
		}(i, calls[i])
	}
	wg.Wait()
	return results, dirs, rejects
}
```

**修改 toolOutcome 结构体**（agent/exectools.go 第 194-202 行）：
```go
// 原始
type toolOutcome struct {
	tr        core.ToolResult
	control   *core.Directive
	ops       []core.StateOp
	rejection *ToolRejection
}

// 修改后
type toolOutcome struct {
	tr              core.ToolResult
	control         *core.Directive
	ops             []core.StateOp
	rejection       *ToolRejection
	fallbackInfo    *FallbackInfo  // ← 新增：回退配置
	executionTime   time.Duration  // ← 新增：执行耗时
}
```

**修改 callOne 函数** - 在返回 toolOutcome 时收集回退信息（第 233-268 行）：
```go
func (l *AgentLoop) callOne(lc *LoopContext, callCtx context.Context, c core.ToolCall) toolOutcome {
	// ... 验证和调用逻辑保持不变
	
	tctx := &tool.Context{Context: keepToolUpdates(callCtx, lc.RunContext), State: lc.State, CallID: c.ID}
	start := time.Now()  // ← 新增：记录开始时间
	res, err := t.Call(tctx, raw)
	execTime := time.Since(start)  // ← 新增：计算执行时间
	
	if err != nil {
		return rejected(c, RejectHandlerError, err.Error())
	}
	
	// ← 新增：从 Result 中提取回退信息
	fbInfo := &FallbackInfo{
		FallbackTools:    res.FallbackTools,
		FallbackPolicy:   res.FallbackPolicy,
		RetryableAfterMs: res.RetryableAfterMs,
	}
	
	return toolOutcome{
		tr:            core.ToolResult{
			CallID:    c.ID, 
			Name:      c.Name, 
			Content:   res.Content, 
			IsError:   res.IsError,
		},
		control:       res.Control,
		ops:           res.State,
		fallbackInfo:  fbInfo,  // ← 新增
		executionTime: execTime,  // ← 新增
	}
}
```

---

## **P0-2: 检查点流式加载与增量恢复**

### 文件3：`checkpoint/checkpoint.go` - 扩展 Checkpointer 接口

**当前代码**（第 49-61 行）：
```go
type Checkpointer interface {
	Save(ctx context.Context, cp *Checkpoint) error
	Load(ctx context.Context, threadID, checkpointID string) (*Checkpoint, error)
	Latest(ctx context.Context, threadID string) (*Checkpoint, error)
	History(ctx context.Context, threadID string) ([]*Checkpoint, error)
}
```

**修改后代码** - 新增流式加载接口：
```go
package checkpoint

import (
	"context"
	"io"

	"github.com/jiujuan/goagent/core"
)

// Checkpointer 是持久化层的基础接口
type Checkpointer interface {
	Save(ctx context.Context, cp *Checkpoint) error
	Load(ctx context.Context, threadID, checkpointID string) (*Checkpoint, error)
	Latest(ctx context.Context, threadID string) (*Checkpoint, error)
	History(ctx context.Context, threadID string) ([]*Checkpoint, error)
}

// ========== 新增：流式加载与增量恢复 ==========

// StreamingCheckpointer 是可选的 Checkpointer 能力：支持高效加载大规模对话历史。
// 检查点后端在实现时可选地实现此接口以支持流式/增量加载。
//
// 使用场景：
//   - 已有 1000+ 条消息的长对话，不需要重新加载所有消息
//   - 支持分页加载避免内存溢出
//   - 支持仅加载最近 N 条消息用于推理
type StreamingCheckpointer interface {
	// LoadRecent 加载线程最后 count 条消息的检查点。
	// 返回一个部分状态，其 Messages 仅包含最后 count 条。
	// 若 count 为 0，返回最新检查点。
	LoadRecent(ctx context.Context, threadID string, count int) (*Checkpoint, error)
	
	// LoadRange 加载特定消息索引范围的检查点。
	// fromIndex/toIndex 是检查点消息列表中的范围边界。
	// 返回新的检查点，其 Messages 仅包含该范围。
	LoadRange(ctx context.Context, threadID string, fromIndex, toIndex int) (*Checkpoint, error)
	
	// IterMessages 按顺序流式迭代线程的所有消息。
	// 调用者无需一次加载全部，可边读边处理。
	// 若分页大小为 0，由实现选择合理的大小（如 100）。
	IterMessages(
		ctx context.Context,
		threadID string,
		pageSize int,
	) (MessageIterator, error)
	
	// MessageCount 返回线程中的消息总数（快速路径，不加载所有消息）。
	MessageCount(ctx context.Context, threadID string) (int, error)
}

// MessageIterator 逐页迭代消息的迭代器。
type MessageIterator interface {
	// Next 返回下一批消息（最多 pageSize 条）。
	// 若到达末尾，返回空切片 + io.EOF。
	Next() ([]core.Message, error)
	
	// Close 释放迭代器资源。
	Close() error
}

// PartialRestoreState 表示一个部分恢复的状态，配合流式加载使用。
// 当检查点很大时，先加载核心字段，后续按需加载细节。
type PartialRestoreState struct {
	// CoreState 包含关键字段（Todos, KV）
	CoreState *core.State
	
	// MessageSource 是消息的来源说明，用于诊断
	// 例如 "recent:100" 表示最近 100 条消息
	// 例如 "range:0-50" 表示第 0-50 条消息
	MessageSource string
	
	// TotalMessages 线程中的总消息数
	TotalMessages int
	
	// EstimatedTokens 估计当前加载的消息会用多少 tokens
	// (由实现根据消息数和平均长度计算)
	EstimatedTokens int
}

// IncrementalSaver 是可选能力：支持增量保存，避免重复存储相同消息。
// 用于长对话中的性能优化。
type IncrementalSaver interface {
	// SaveIncremental 仅保存自上次检查点后新增的消息。
	// prevCheckpointID 是上一次检查点的 ID；
	// newMessages 是新增的消息；
	// mutatedState 是修改过的状态字段。
	// 实现应该只记录差异，而非整个 State。
	SaveIncremental(
		ctx context.Context,
		threadID string,
		prevCheckpointID string,
		newMessages []core.Message,
		mutatedState map[string]any, // 仅包含改变的 KV 项
	) (*Checkpoint, error)
}

// CompactionStrategy 定义如何压缩老旧消息的策略。
type CompactionStrategy interface {
	// ShouldCompact 判断是否应该压缩
	// totalMessages: 当前消息总数
	// estimatedTokens: 预计 token 数
	ShouldCompact(totalMessages int, estimatedTokens int) bool
	
	// Compact 执行压缩，返回压缩后的消息列表
	// oldMessages: 拟压缩的旧消息
	// keepRecent: 保留最后 N 条消息不压缩
	Compact(oldMessages []core.Message, keepRecent int) ([]core.Message, string, error)
}
```

---

### 文件4：`checkpoint/file.go` - File 检查点的流式实现

**新建功能** - 在 `checkpoint/file.go` 中添加（假设该文件已存在）：

```go
// file.go 的扩展部分

package checkpoint

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jiujuan/goagent/core"
)

// 使现有 File 实现 StreamingCheckpointer 接口

type File struct {
	dir string
	// ... 现有字段
}

// ========== StreamingCheckpointer 实现 ==========

// LoadRecent 加载最后 N 条消息的检查点
func (f *File) LoadRecent(ctx context.Context, threadID string, count int) (*Checkpoint, error) {
	// 第 1 步：加载最新检查点的 Messages
	latest, err := f.Latest(ctx, threadID)
	if err != nil {
		return nil, err
	}
	if latest == nil {
		return nil, nil
	}
	
	// 第 2 步：截取最后 count 条消息
	msgs := latest.State.Messages
	if count > 0 && len(msgs) > count {
		msgs = msgs[len(msgs)-count:]
	}
	
	// 第 3 步：构造新的检查点，仅包含截取后的消息
	result := &Checkpoint{
		ID:       latest.ID,
		ThreadID: latest.ThreadID,
		Step:     latest.Step,
		State: core.State{
			Messages: msgs,
			Todos:    latest.State.Todos,
			KV:       latest.State.KV,
		},
		FileSnapshot: latest.FileSnapshot,
	}
	
	return result, nil
}

// LoadRange 加载特定范围的消息
func (f *File) LoadRange(ctx context.Context, threadID string, fromIdx, toIdx int) (*Checkpoint, error) {
	latest, err := f.Latest(ctx, threadID)
	if err != nil {
		return nil, err
	}
	if latest == nil {
		return nil, nil
	}
	
	msgs := latest.State.Messages
	if fromIdx < 0 {
		fromIdx = 0
	}
	if toIdx > len(msgs) {
		toIdx = len(msgs)
	}
	if fromIdx >= toIdx {
		return nil, fmt.Errorf("invalid range: from=%d to=%d", fromIdx, toIdx)
	}
	
	result := &Checkpoint{
		ID:       latest.ID,
		ThreadID: latest.ThreadID,
		Step:     latest.Step,
		State: core.State{
			Messages: msgs[fromIdx:toIdx],
			Todos:    latest.State.Todos,
			KV:       latest.State.KV,
		},
		FileSnapshot: latest.FileSnapshot,
	}
	
	return result, nil
}

// IterMessages 逐页流式迭代线程的所有消息
func (f *File) IterMessages(
	ctx context.Context,
	threadID string,
	pageSize int,
) (MessageIterator, error) {
	if pageSize <= 0 {
		pageSize = 100  // 默认每页 100 条消息
	}
	
	// 获取最新检查点，其中包含完整的消息历史
	latest, err := f.Latest(ctx, threadID)
	if err != nil {
		return nil, err
	}
	if latest == nil {
		return &messagePageIterator{messages: nil}, nil
	}
	
	return &messagePageIterator{
		messages: latest.State.Messages,
		pageSize: pageSize,
		index:    0,
	}, nil
}

// messagePageIterator 实现 MessageIterator 接口
type messagePageIterator struct {
	messages []core.Message
	pageSize int
	index    int
}

func (m *messagePageIterator) Next() ([]core.Message, error) {
	if m.index >= len(m.messages) {
		return nil, io.EOF
	}
	
	end := m.index + m.pageSize
	if end > len(m.messages) {
		end = len(m.messages)
	}
	
	page := m.messages[m.index:end]
	m.index = end
	
	return page, nil
}

func (m *messagePageIterator) Close() error {
	return nil
}

// MessageCount 返回线程的消息总数
func (f *File) MessageCount(ctx context.Context, threadID string) (int, error) {
	latest, err := f.Latest(ctx, threadID)
	if err != nil {
		return 0, err
	}
	if latest == nil {
		return 0, nil
	}
	
	return len(latest.State.Messages), nil
}

// ========== IncrementalSaver 实现 ==========

// SaveIncremental 仅保存差异部分
func (f *File) SaveIncremental(
	ctx context.Context,
	threadID string,
	prevCheckpointID string,
	newMessages []core.Message,
	mutatedState map[string]any,
) (*Checkpoint, error) {
	// 加载前一个检查点
	prev, err := f.Load(ctx, threadID, prevCheckpointID)
	if err != nil {
		return nil, fmt.Errorf("load previous checkpoint: %w", err)
	}
	
	if prev == nil {
		return nil, fmt.Errorf("previous checkpoint not found: %s", prevCheckpointID)
	}
	
	// 合并消息：旧消息 + 新消息
	mergedMessages := append(prev.State.Messages, newMessages...)
	
	// 更新 KV：用 mutatedState 覆盖
	mergedKV := prev.State.KV
	if mergedKV == nil {
		mergedKV = make(map[string]any)
	}
	for k, v := range mutatedState {
		mergedKV[k] = v
	}
	
	// 构造新检查点（仅记录增量，在 Save 时区别对待）
	cp := &Checkpoint{
		ID:       core.NewID("cp"),
		ThreadID: threadID,
		ParentID: prevCheckpointID,  // ← 指向上一个检查点
		Step:     prev.Step + 1,
		State: core.State{
			Messages: mergedMessages,
			Todos:    prev.State.Todos,
			KV:       mergedKV,
		},
	}
	
	return cp, f.Save(ctx, cp)
}
```

---

### 文件5：`agent/agent.go` - 修改恢复逻辑使用流式加载

**当前代码**（第 159-170 行）：
```go
func (a *Agent) restore(ctx context.Context, threadID string) (*core.State, error) {
	cp, err := a.store.Latest(ctx, threadID)
	if err != nil {
		return &core.State{}, err
	}
	if cp == nil {
		return &core.State{}, nil
	}
	st := cloneState(cp.State)
	applyFileSnapshot(&st, cp.FileSnapshot)
	return &st, nil
}
```

**修改后代码** - 优先使用流式加载：
```go
// agent/agent.go

// restore 加载线程的最新状态，优先使用流式加载以支持大规模对话。
func (a *Agent) restore(ctx context.Context, threadID string) (*core.State, error) {
	// 尝试使用流式加载接口（如果可用）
	if sc, ok := a.store.(checkpoint.StreamingCheckpointer); ok {
		return a.restoreStreaming(ctx, threadID, sc)
	}
	
	// 回退到原始加载方式
	cp, err := a.store.Latest(ctx, threadID)
	if err != nil {
		return &core.State{}, err
	}
	if cp == nil {
		return &core.State{}, nil
	}
	
	st := cloneState(cp.State)
	applyFileSnapshot(&st, cp.FileSnapshot)
	return &st, nil
}

// restoreStreaming 使用流式接口加载，仅加载最近 N 条消息
// 当对话历史很长时避免内存溢出
func (a *Agent) restoreStreaming(
	ctx context.Context,
	threadID string,
	sc checkpoint.StreamingCheckpointer,
) (*core.State, error) {
	// 配置参数：可通过环境变量或配置调整
	const maxRecentMessages = 500  // 仅加载最近 500 条消息到内存
	
	// 第一步：查询总消息数
	totalMsgs, err := sc.MessageCount(ctx, threadID)
	if err != nil {
		// 若查询失败，回退到完全加载
		cp, _ := a.store.Latest(ctx, threadID)
		if cp != nil {
			return &cp.State, nil
		}
		return &core.State{}, nil
	}
	
	// 第二步：根据消息总数决定加载策略
	var cp *checkpoint.Checkpoint
	if totalMsgs <= maxRecentMessages {
		// 消息不多，加载全部
		var err error
		cp, err = a.store.Latest(ctx, threadID)
		if err != nil {
			return &core.State{}, err
		}
	} else {
		// 消息很多，仅加载最近 maxRecentMessages 条
		// + 一个系统提示（如果存在）
		var err error
		cp, err = sc.LoadRecent(ctx, threadID, maxRecentMessages)
		if err != nil {
			return &core.State{}, err
		}
	}
	
	if cp == nil {
		return &core.State{}, nil
	}
	
	st := cloneState(cp.State)
	applyFileSnapshot(&st, cp.FileSnapshot)
	
	// 第三步：在状态中记录加载信息（用于诊断）
	if st.KV == nil {
		st.KV = make(map[string]any)
	}
	st.KV["_checkpoint_info"] = map[string]interface{}{
		"total_messages_in_thread": totalMsgs,
		"loaded_messages":          len(st.Messages),
		"is_partial":               totalMsgs > maxRecentMessages,
	}
	
	return &st, nil
}
```

---

### 文件6：`core/event.go` - 添加检查点事件

**新增事件类型**（在 core/event.go 中）：

```go
// core/event.go 中添加以下内容

// ========== 新增：检查点与流式加载相关事件 ==========

// CheckpointCreated 在检查点被成功保存时发出
type CheckpointCreated struct {
	CheckpointID string
	ThreadID     string
	Step         int
	MessageCount int  // 本次检查点包含的消息数
}

// CheckpointLoaded 在检查点被成功加载时发出
type CheckpointLoaded struct {
	CheckpointID string
	ThreadID     string
	LoadedMessages int
	TotalMessages int     // 若是部分加载，表示总消息数
	IsPartial bool
}

// CheckpointFailed 在检查点操作失败时发出
type CheckpointFailed struct {
	ThreadID string
	Step int
	Operation string  // "save" | "load" | "restore"
	Error error
}

// StreamingCheckpointProgress 在流式加载大规模对话时报告进度
type StreamingCheckpointProgress struct {
	ThreadID string
	LoadedSoFar int  // 已加载的消息数
	Total int        // 总消息数
	PercentComplete int  // 0-100
}

// === 更新 isEvent() 标记 ===

func (CheckpointCreated) isEvent() {}
func (CheckpointLoaded) isEvent() {}
func (CheckpointFailed) isEvent() {}
func (StreamingCheckpointProgress) isEvent() {}
```

---

## **第二部分：P1 优先级 - 运维级功能**

---

## **P1-1: 工具单位超时（Per-Tool Timeout）**

### 文件7：`tool/tool.go` - 添加工具超时装饰

**修改 Tool 接口** - 添加可选的超时能力：

```go
// tool/tool.go

// ToolWithTimeout 是可选的 Tool 能力：声明该工具的默认最大执行时间。
// 工具实现可选地实现此接口，使 Agent 循环在调用时使用工具声明的超时
// 而非全局 WithToolTimeout。
type ToolWithTimeout interface {
	// Timeout 返回该工具的建议执行超时时间。
	// 返回 0 表示不限制（继承 Agent 的全局超时）。
	// 返回负值是错误的，应该被拒绝。
	Timeout() time.Duration
}

// ToolfWithRetry 是可选的 Tool 能力：工具可以声明它是否支持内部重试。
// 若支持，Agent 不会在工具失败时重试整个工具调用，而让工具自己处理重试。
type ToolWithRetry interface {
	// ShouldRetry 报告是否应该重试这个工具调用。
	// 调用者应该检查结果中的 RetryableAfterMs，若非 0 则延迟后重试。
	ShouldRetry(err error, attemptCount int) bool
	
	// MaxRetries 返回工具允许的最大重试次数。
	MaxRetries() int
}
```

---

### 文件8：`agent/exectools.go` - 修改 toolCallCtx 支持工具级超时

**当前代码**（第 164-171 行）：
```go
func (l *AgentLoop) toolCallCtx(lc *LoopContext, c *core.ToolCall) (context.Context, context.CancelFunc) {
	ctx, mwCancel := l.mw.ToolContext(lc, lc.RunContext.Context, c)
	if l.toolTimeout <= 0 {
		return ctx, mwCancel
	}
	callCtx, deadlineCancel := context.WithTimeout(ctx, l.toolTimeout)
	return callCtx, func() { deadlineCancel(); mwCancel() }
}
```

**修改后代码** - 支持工具级超时覆盖：

```go
// agent/exectools.go

// toolCallCtx 派生工具调用运行的上下文：中间件先（span、或 per-tool 边界），
// 然后是 Agent 的默认超时。优先级：工具声明 > 中间件 > Agent 全局。
func (l *AgentLoop) toolCallCtx(lc *LoopContext, c *core.ToolCall) (context.Context, context.CancelFunc) {
	ctx, mwCancel := l.mw.ToolContext(lc, lc.RunContext.Context, c)
	
	// ← 新增：查询工具是否声明了自己的超时
	timeout := l.toolTimeout
	if t, ok := lc.dynamic.lookup(c.Name); ok {
		if twt, ok := t.(tool.ToolWithTimeout); ok {
			toolTimeout := twt.Timeout()
			if toolTimeout > 0 {
				timeout = toolTimeout  // ← 工具级超时优先
			}
		}
	} else if t, ok := l.byName[c.Name]; ok {
		if twt, ok := t.(tool.ToolWithTimeout); ok {
			toolTimeout := twt.Timeout()
			if toolTimeout > 0 {
				timeout = toolTimeout
			}
		}
	}
	
	if timeout <= 0 {
		return ctx, mwCancel
	}
	
	callCtx, deadlineCancel := context.WithTimeout(ctx, timeout)
	return callCtx, func() { deadlineCancel(); mwCancel() }
}
```

---

### 文件9：`agent/middleware.go` - 新增检查点失败钩子

**修改 Middleware 接口** - 添加检查点失败处理：

```go
// agent/middleware.go

// Middleware 接口（修改现有的）
type Middleware interface {
	BeforeModel(*LoopContext) (core.Directive, error)
	ModifyRequest(*LoopContext, *llm.Request) error
	AfterModel(*LoopContext, *llm.Response) (core.Directive, error)
	BeforeTool(*LoopContext, *core.ToolCall) (core.Directive, error)
	AfterTool(*LoopContext, *core.ToolResult) (core.Directive, error)
	OnError(*LoopContext, error) (core.Directive, error)
	
	// ← 新增可选钩子，在检查点保存失败时调用
	// 中间件可以决定是否继续运行或暂停
}

// CheckpointFailureHandler 是可选的中间件能力：处理检查点保存失败。
// 若中间件实现此接口，循环会在检查点保存失败时调用。
// 中间件可以选择重试、警告、或中止运行。
type CheckpointFailureHandler interface {
	OnCheckpointFailed(
		rc *RunContext,
		cp *checkpoint.Checkpoint,
		err error,
	) (core.Directive, error)
}

// === 在 Stack 中添加对应的调用 ===

// Stack 新增方法
func (s *Stack) OnCheckpointFailed(
	rc *RunContext,
	cp *checkpoint.Checkpoint,
	err error,
) (core.Directive, error) {
	ds := make([]core.Directive, 0, len(s.mws))
	for _, m := range s.mws {
		if cfh, ok := m.(CheckpointFailureHandler); ok {
			d, mErr := cfh.OnCheckpointFailed(rc, cp, err)
			if mErr != nil {
				return core.Directive{}, mErr
			}
			ds = append(ds, d)
		}
	}
	return core.Resolve(ds...), nil
}
```

---

### 文件10：`agent/loop.go` - 修改检查点调用处理失败

**当前代码**（第 312-323 行）：
```go
func (l *AgentLoop) checkpoint(rc *RunContext, step int, pending *checkpoint.PendingHITL) {
	if rc.Store == nil {
		return
	}
	_ = rc.Store.Save(rc, &checkpoint.Checkpoint{...})
}
```

**修改后代码** - 处理检查点失败：

```go
// agent/loop.go

// checkpoint 快照当前 State 以供恢复/分支/时间旅行。
// 若保存失败，通知中间件并可能暂停运行。
func (l *AgentLoop) checkpoint(rc *RunContext, lc *LoopContext, step int, pending *checkpoint.PendingHITL) {
	if rc.Store == nil {
		return
	}
	
	cp := &checkpoint.Checkpoint{
		ID:       core.NewID("cp"),
		ThreadID: rc.ThreadID,
		Step:     step,
		State:    *rc.State,
		Pending:  pending,
	}
	
	if err := rc.Store.Save(rc, cp); err != nil {
		// ← 新增：发布检查点失败事件
		rc.publish(core.CheckpointFailed{
			ThreadID:  rc.ThreadID,
			Step:      step,
			Operation: "save",
			Error:     err,
		})
		
		// ← 新增：通知中间件
		d, mwErr := l.mw.OnCheckpointFailed(rc, cp, err)
		if mwErr != nil {
			// 中间件报告错误，中止运行
			_ = l.fail(rc, step, rc.State.Messages, mwErr)
			return
		}
		
		// 中间件可以返回指令（Stop/Continue/Escalate）
		if d.Kind == core.Stop || d.Kind == core.Escalate {
			_ = l.fail(rc, step, rc.State.Messages, fmt.Errorf("checkpoint failed: %w", err))
			return
		}
		
		// 否则继续（忽略检查点失败，但已发出警告）
		return
	}
	
	// ← 新增：发布检查点成功事件
	rc.publish(core.CheckpointCreated{
		CheckpointID: cp.ID,
		ThreadID:     rc.ThreadID,
		Step:         step,
		MessageCount: len(rc.State.Messages),
	})
}
```

**修改 loop.run 中调用 checkpoint 的位置**（第 110-273 行中多处）：
- 将所有 `l.checkpoint(rc, step, nil)` 改为 `l.checkpoint(rc, lc, step, nil)`
- 将所有 `l.checkpoint(rc, step, &checkpoint.PendingHITL{...})` 改为 `l.checkpoint(rc, lc, step, &checkpoint.PendingHITL{...})`

---

### 文件11：`agent/options.go` - 添加生命周期钩子选项

**新增到 config 结构体** - 在现有 options.go 中：

```go
// agent/options.go

import (
	"context"
	"time"
	
	// ... 其他导入
)

// 修改 config 结构体
type config struct {
	name        string
	description string
	instruction string
	prompt      *prompt.Builder
	model       llm.Model
	tools       []tool.Tool
	middleware  []Middleware
	subAgents   []*Agent
	maxTurns    int
	toolExec    ToolExecMode
	toolTimeout time.Duration
	modelOpts   []llm.Option
	outputKey   string

	noTransfer       bool
	noTransferParent bool
	noTransferPeers  bool

	bus   *bus.Bus
	store checkpoint.Checkpointer
	
	// ← 新增：生命周期回调
	onAgentStart func(ctx context.Context, a *Agent) error
	onAgentShutdown func(ctx context.Context) error
}

// ========== 新增选项函数 ==========

// WithOnAgentStart 注册 Agent 初始化时的回调。
// 回调在 New 返回前调用，用于预热连接、初始化资源等。
func WithOnAgentStart(fn func(context.Context, *Agent) error) Option {
	return func(c *config) { c.onAgentStart = fn }
}

// WithOnAgentShutdown 注册 Agent 关闭时的回调。
// 调用者应该显式调用 agent.Shutdown(ctx) 触发此回调。
func WithOnAgentShutdown(fn func(context.Context) error) Option {
	return func(c *config) { c.onAgentShutdown = fn }
}
```

---

### 文件12：`agent/agent.go` - 添加生命周期方法

**修改 Agent 结构体** - 添加生命周期字段和方法：

```go
// agent/agent.go

// Agent 修改
type Agent struct {
	cfg      config
	loop     *AgentLoop
	parent   *Agent
	runnable Runnable
	bus      *bus.Bus
	store    checkpoint.Checkpointer
	
	// ← 新增：生命周期钩子
	onShutdown func(context.Context) error
	closed bool
}

// New 修改（第 34-61 行）
func New(opts ...Option) (*Agent, error) {
	c := config{maxTurns: defaultMaxTurns}
	for _, o := range opts {
		o(&c)
	}
	if c.model == nil {
		return nil, errors.New("agent: WithModel is required")
	}
	if c.bus == nil {
		c.bus = bus.New()
	}
	if c.store == nil {
		c.store = checkpoint.NewMemory()
	}
	
	a := &Agent{cfg: c, bus: c.bus, store: c.store, onShutdown: c.onAgentShutdown}
	a.loop = newLoop(c)
	a.runnable = &llmRunner{agent: a, loop: a.loop}
	
	for _, s := range c.subAgents {
		s.parent = a
	}
	if tt := transferToolFor(a.transferTargets()); tt != nil {
		a.loop.addTool(tt)
	}
	
	// ← 新增：调用 onAgentStart 回调
	if c.onAgentStart != nil {
		if err := c.onAgentStart(context.Background(), a); err != nil {
			return nil, fmt.Errorf("agent initialization failed: %w", err)
		}
	}
	
	return a, nil
}

// ← 新增：Shutdown 方法
func (a *Agent) Shutdown(ctx context.Context) error {
	if a.closed {
		return nil
	}
	a.closed = true
	
	if a.onShutdown != nil {
		return a.onShutdown(ctx)
	}
	return nil
}
```

**在 options.go 中修改 New 的导入**：
```go
import (
	"context"  // ← 新增
	"fmt"      // ← 新增
	"time"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/tool"
)
```

---

## **第三部分：P2 优先级 - 企业级功能**

---

## **P2-1: 中间件栈的执行追踪**

### 文件13：`agent/middleware.go` - 添加执行追踪

**在 Stack 中添加追踪** - 修改现有 Stack 结构体：

```go
// agent/middleware.go

import (
	"fmt"
	"time"  // ← 新增
	
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// MiddlewareTrace 记录单个中间件的执行信息
type MiddlewareTrace struct {
	MiddlewareName string
	HookName       string  // "BeforeModel", "AfterModel", "BeforeTool" etc.
	Duration       time.Duration
	Error          error
	Step           int
}

// Stack 修改 - 添加追踪字段
type Stack struct {
	mws    []Middleware
	traces []MiddlewareTrace  // ← 新增：执行追踪记录
	tracingEnabled bool        // ← 新增：是否启用追踪
}

// NewStackWithTracing 创建启用追踪的中间件栈
func NewStackWithTracing(mws ...Middleware) *Stack {
	return &Stack{
		mws:            mws,
		traces:         make([]MiddlewareTrace, 0),
		tracingEnabled: true,
	}
}

// GetTraces 返回所有追踪记录
func (s *Stack) GetTraces() []MiddlewareTrace {
	return s.traces
}

// ClearTraces 清空追踪记录
func (s *Stack) ClearTraces() {
	s.traces = s.traces[:0]
}

// ========== 修改各个钩子方法以添加追踪 ==========

// BeforeModel 的修改示例
func (s *Stack) BeforeModel(lc *LoopContext) (core.Directive, error) {
	ds := make([]core.Directive, 0, len(s.mws))
	for i, m := range s.mws {
		start := time.Now()
		d, err := m.BeforeModel(lc)
		duration := time.Since(start)
		
		// ← 新增：记录执行信息
		if s.tracingEnabled {
			s.traces = append(s.traces, MiddlewareTrace{
				MiddlewareName: fmt.Sprintf("%T", m),
				HookName:       "BeforeModel",
				Duration:       duration,
				Error:          err,
				Step:           lc.Step,
			})
		}
		
		if err != nil {
			return core.Directive{}, err
		}
		ds = append(ds, d)
	}
	return core.Resolve(ds...), nil
}

// 类似地修改其他所有钩子方法：
// - ModifyRequest
// - AfterModel
// - BeforeTool
// - AfterTool
// - OnError
// - ToolContext
// - ModelContext
// 等等

// 示例：AfterModel
func (s *Stack) AfterModel(lc *LoopContext, resp *llm.Response) (core.Directive, error) {
	ds := make([]core.Directive, 0, len(s.mws))
	for i := len(s.mws) - 1; i >= 0; i-- {
		m := s.mws[i]
		start := time.Now()
		d, err := m.AfterModel(lc, resp)
		duration := time.Since(start)
		
		if s.tracingEnabled {
			s.traces = append(s.traces, MiddlewareTrace{
				MiddlewareName: fmt.Sprintf("%T", m),
				HookName:       "AfterModel",
				Duration:       duration,
				Error:          err,
				Step:           lc.Step,
			})
		}
		
		if err != nil {
			return core.Directive{}, err
		}
		ds = append(ds, d)
	}
	return core.Resolve(ds...), nil
}
```

---

### 文件14：`core/event.go` - 添加中间件事件

```go
// core/event.go 中新增

// MiddlewareApplied 在中间件钩子成功执行后发出（仅在追踪启用时）
type MiddlewareApplied struct {
	MiddlewareName string
	HookName       string
	Duration       time.Duration
	Step           int
}

// MiddlewareError 在中间件钩子返回错误时发出
type MiddlewareError struct {
	MiddlewareName string
	HookName       string
	Error          error
	Step           int
}

func (MiddlewareApplied) isEvent() {}
func (MiddlewareError) isEvent() {}
```

---

## **P2-2: 检查点与故障恢复的自动化**

### 文件15：`agent/resilience.go` (新建文件)

```go
// agent/resilience.go

package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// ResiliencePolicy 定义故障恢复策略
type ResiliencePolicy struct {
	// AutoRetry 启用自动重试失败的运行
	AutoRetry bool
	
	// MaxRetries 单个运行的最大重试次数
	MaxRetries int
	
	// BackoffFn 计算第 N 次重试的延迟时间
	// 输入是重试次数（从 0 开始），返回延迟时间
	// 若为 nil，使用指数退避: 2^attempt * 100ms
	BackoffFn func(attempt int) time.Duration
	
	// IsRetryable 判断错误是否可重试
	// 返回 true 时才会触发自动重试
	IsRetryable func(err error, step int) bool
	
	// PreRetryHook 在重试前调用，可用于清理/日志
	PreRetryHook func(ctx context.Context, threadID string, attempt int, err error) error
}

// DefaultBackoff 实现指数退避
func DefaultBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	base := time.Duration(100 * int64(1<<uint(attempt))) * time.Millisecond
	if base > 30*time.Second {
		base = 30 * time.Second  // 最大退避 30 秒
	}
	return base
}

// ResilienceMiddleware 根据策略自动重试失败的运行
type ResilienceMiddleware struct {
	policy *ResiliencePolicy
	agent  *Agent
}

// NewResilienceMiddleware 构造恢复中间件
func NewResilienceMiddleware(a *Agent, p *ResiliencePolicy) *ResilienceMiddleware {
	if p == nil {
		p = &ResiliencePolicy{}
	}
	if p.BackoffFn == nil {
		p.BackoffFn = DefaultBackoff
	}
	if p.IsRetryable == nil {
		// 默认：模型/网络错误可重试，工具错误不重试
		p.IsRetryable = func(err error, step int) bool {
			// 仅重试明确的提供商错误
			return err != nil && (
				err.Error() == llm.ErrProviderUnavailable.Error() ||
				err.Error() == llm.ErrNetworkError.Error())
		}
	}
	return &ResilienceMiddleware{policy: p, agent: a}
}

// ← 在 Agent.run 失败后调用此函数尝试恢复
// 这通常由运行时框架调用，而不是用户直接调用
func (rm *ResilienceMiddleware) AttemptRecover(
	ctx context.Context,
	threadID string,
	lastErr error,
	lastStep int,
) (bool, error) {
	if !rm.policy.AutoRetry {
		return false, nil  // 未启用自动重试
	}
	if !rm.policy.IsRetryable(lastErr, lastStep) {
		return false, nil  // 该错误不可重试
	}
	
	// 计算这是第几次重试
	history, err := rm.agent.store.History(ctx, threadID)
	if err != nil {
		return false, fmt.Errorf("cannot load history for recovery: %w", err)
	}
	
	attemptCount := 0
	for _, cp := range history {
		if cp.Step == lastStep {
			attemptCount++
		}
	}
	
	if attemptCount >= rm.policy.MaxRetries {
		return false, fmt.Errorf("max retries (%d) exceeded", rm.policy.MaxRetries)
	}
	
	// 预重试钩子
	if rm.policy.PreRetryHook != nil {
		if err := rm.policy.PreRetryHook(ctx, threadID, attemptCount, lastErr); err != nil {
			return false, err
		}
	}
	
	// 计算延迟
	delay := rm.policy.BackoffFn(attemptCount)
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return false, ctx.Err()
	}
	
	return true, nil
}
```

---

### 文件16：`agent/options.go` - 添加恢复策略选项

```go
// agent/options.go 中添加

// WithResiliencePolicy 配置自动重试与恢复策略
func WithResiliencePolicy(p *ResiliencePolicy) Option {
	return func(c *config) {
		if p != nil {
			middleware := NewResilienceMiddleware(nil, p)  // ← Agent 在 New 后设置
			c.middleware = append(c.middleware, middleware)
		}
	}
}
```

---

## **修改总结表**

| 优先级 | 功能 | 文件 | 主要修改 |
|-------|------|------|--------|
| P0 | 工具回退链 | `tool/tool.go` | 添加 `FallbackTools`, `FallbackPolicy`, `RetryableAfterMs` 字段 |
| P0 | 工具回退链 | `agent/exectools.go` | 新增 `executeWithFallback()`, `tryFallbacks()` 函数 |
| P0 | 检查点流式加载 | `checkpoint/checkpoint.go` | 新增 `StreamingCheckpointer` 接口 |
| P0 | 检查点流式加载 | `checkpoint/file.go` | 实现 `LoadRecent()`, `IterMessages()` 等 |
| P0 | 检查点流式加载 | `agent/agent.go` | 修改 `restore()` 使用流式加载 |
| P1 | 工具单超时 | `tool/tool.go` | 新增 `ToolWithTimeout` 接口 |
| P1 | 工具单超时 | `agent/exectools.go` | 修改 `toolCallCtx()` 查询工具超时 |
| P1 | 检查点失败处理 | `agent/middleware.go` | 新增 `CheckpointFailureHandler` 接口 |
| P1 | 检查点失败处理 | `agent/loop.go` | 修改 `checkpoint()` 处理失败 |
| P1 | 生命周期钩子 | `agent/options.go` | 新增 `WithOnAgentStart/Shutdown` |
| P1 | 生命周期钩子 | `agent/agent.go` | 新增 `Shutdown()` 方法 |
| P2 | 中间件追踪 | `agent/middleware.go` | 在 `Stack` 中添加 `traces` 字段 |
| P2 | 中间件追踪 | `core/event.go` | 新增 `MiddlewareApplied/MiddlewareError` 事件 |
| P2 | 故障恢复自动化 | `agent/resilience.go` | 新建文件，实现 `ResilienceMiddleware` |
| P2 | 故障恢复自动化 | `agent/options.go` | 新增 `WithResiliencePolicy` 选项 |

---

## **完整示例：如何使用这些优化**

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/llm/anthropic"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

func main() {
	ctx := context.Background()
	
	// 1. 配置恢复策略
	resiliencePolicy := &agent.ResiliencePolicy{
		AutoRetry: true,
		MaxRetries: 3,
		BackoffFn: agent.DefaultBackoff,
		IsRetryable: func(err error, step int) bool {
			return step < 5  // 前5步可重试
		},
	}
	
	// 2. 配置生命周期钩子
	onStart := func(ctx context.Context, a *agent.Agent) error {
		log.Println("Agent 初始化中...")
		// 预热连接、初始化缓存等
		return nil
	}
	
	onShutdown := func(ctx context.Context) error {
		log.Println("Agent 关闭中...")
		// 清理资源
		return nil
	}
	
	// 3. 构建 Agent（P0/P1 功能）
	a, err := agent.New(
		agent.WithModel(anthropic.New("claude-3-sonnet")),
		agent.WithInstruction("你是一个专业的助手。"),
		
		// ← P1：生命周期钩子
		agent.WithOnAgentStart(onStart),
		agent.WithOnAgentShutdown(onShutdown),
		
		// ← P0/P1：检查点与恢复
		agent.WithCheckpointer(checkpoint.NewFile("./.checkpoints")),
		
		// ← P2：故障自动恢复
		agent.WithResiliencePolicy(resiliencePolicy),
		
		// ← 中间件追踪
		agent.WithMiddleware(
			middleware.NewRetryMiddleware(),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	
	// 延迟关闭
	defer a.Shutdown(ctx)
	
	// 4. 定义支持回退链的工具（P0）
	weatherTool := tool.New("get_weather", "查询天气",
		func(_ *tool.Context, in struct {
			City string `json:"city"`
		}) (*tool.Result, error) {
			// 主逻辑...若失败，返回回退建议
			if failed {
				return &tool.Result{
					IsError: true,
					Content: []core.Part{core.Text{Text: "API 失败"}},
					FallbackTools: []string{"get_weather_cached"},
					FallbackPolicy: tool.FallbackFirstSuccess,
					RetryableAfterMs: 5000,
				}, nil
			}
			return tool.TextResult("晴，25°C"), nil
		},
	)
	
	// 5. 使用流式检查点恢复（P0）
	// 以前：全量加载，可能很慢
	// 现在：智能加载最近 500 条消息
	answer, err := a.Run(ctx, "北京天气怎么样？", 
		agent.OnThread("user_123"),  // 跨进程持久化
	)
	if err != nil {
		log.Fatal(err)
	}
	
	log.Println("答案:", answer)
	
	// 6. 查看中间件执行追踪（P2）
	// 若支持，可以获取所有中间件的执行时间
}
```

---

这个完整方案涵盖了从 P0 到 P2 的所有优化，每个修改都对应具体的文件和代码行。