package plan

import (
	"encoding/json"
	"fmt"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/tool"
)

// draftKey is the session-state key under which a planner writes its proposed
// plan (as a JSON string), for the PlanAgent to parse and execute.
const draftKey = "plan:draft"

// draft is the wire form of a dynamically-generated plan: what a planner LLM
// emits through the set_plan tool. Executors are named (resolved against the
// PlanAgent's Tools/Agents registries), since code cannot cross the model
// boundary.
type draft struct {
	ID    string      `json:"id" desc:"计划唯一标识"`
	Goal  string      `json:"goal" desc:"计划目标"`
	Steps []draftStep `json:"steps" desc:"步骤列表，构成有向无环图"`
}

type draftStep struct {
	ID           string      `json:"id" desc:"步骤唯一标识"`
	Name         string      `json:"name" desc:"步骤名称"`
	Desc         string      `json:"desc" desc:"步骤说明"`
	DependsOn    []string    `json:"depends_on" desc:"依赖的上游步骤 ID 列表，无依赖则为空"`
	Executor     draftExec   `json:"executor" desc:"如何执行该步骤"`
	OnError      ErrorPolicy `json:"on_error" desc:"出错策略：空=fail 中止，skip=跳过，continue=继续"`
	NeedApproval bool        `json:"need_approval" desc:"执行前是否需要人工审批"`
}

type draftExec struct {
	Type string          `json:"type" desc:"执行器类型：tool 或 agent"`
	Name string          `json:"name" desc:"工具名或 agent 名"`
	Args json.RawMessage `json:"args" desc:"工具参数（JSON 对象），agent 执行器可省略"`
}

// SetPlanTool returns the tool a planner agent uses to register its plan. The
// model calls it once with the full DAG; the plan is stashed in session state
// (and StateDelta, so it persists) for the enclosing PlanAgent to execute.
func SetPlanTool() tool.Tool {
	return tool.New("set_plan", "登记一份带依赖关系的执行计划（DAG）",
		func(ctx *tool.Context, in draft) (string, error) {
			b, err := json.Marshal(in)
			if err != nil {
				return "", err
			}
			ctx.State.Set(draftKey, string(b))
			if ctx.Actions != nil {
				if ctx.Actions.StateDelta == nil {
					ctx.Actions.StateDelta = map[string]any{}
				}
				ctx.Actions.StateDelta[draftKey] = string(b)
			}
			return fmt.Sprintf("已登记 %d 步执行计划。", len(in.Steps)), nil
		})
}

// Parse builds a *Plan from a planner's draft JSON, resolving each step's named
// executor against the provided tool/agent registries. It returns an error if a
// step references an unknown tool/agent or an unknown executor type; the
// resulting plan is then subject to the usual Validate before scheduling.
func Parse(raw []byte, tools []tool.Tool, agents []agent.Agent) (*Plan, error) {
	var d draft
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("plan: parse draft: %w", err)
	}
	toolByName := tool.ByName(tools)
	agentByName := make(map[string]agent.Agent, len(agents))
	for _, ag := range agents {
		agentByName[ag.Name()] = ag
	}

	p := &Plan{ID: d.ID, Goal: d.Goal}
	if p.ID == "" {
		p.ID = "plan"
	}
	for _, ds := range d.Steps {
		st := &Step{
			ID: ds.ID, Name: ds.Name, Description: ds.Desc,
			DependsOn: ds.DependsOn, OnError: ds.OnError, NeedApproval: ds.NeedApproval,
		}
		switch ds.Executor.Type {
		case "tool":
			tl := toolByName[ds.Executor.Name]
			if tl == nil {
				return nil, fmt.Errorf("plan: step %q references unknown tool %q", ds.ID, ds.Executor.Name)
			}
			st.Exec = ToolExecutor{Tool: tl, Args: ds.Executor.Args}
		case "agent":
			ag := agentByName[ds.Executor.Name]
			if ag == nil {
				return nil, fmt.Errorf("plan: step %q references unknown agent %q", ds.ID, ds.Executor.Name)
			}
			st.Exec = AgentExecutor{Agent: ag}
		default:
			return nil, fmt.Errorf("plan: step %q has unknown executor type %q", ds.ID, ds.Executor.Type)
		}
		p.Steps = append(p.Steps, st)
	}
	return p, nil
}
