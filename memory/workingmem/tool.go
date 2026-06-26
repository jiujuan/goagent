package workingmem

import (
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// updateArgs is the argument schema for update_working_memory. All fields are
// optional; each present field applies one mutation.
type updateArgs struct {
	Goal          string `json:"goal,omitempty" desc:"设置当前任务目标（留空则不修改目标）"`
	AddTodo       string `json:"add_todo,omitempty" desc:"新增一条待办，值为待办文本；返回结果会带上分配的 id"`
	ResolveTodoID string `json:"resolve_todo_id,omitempty" desc:"将指定 id 的待办标记为已完成"`
	NoteKey       string `json:"note_key,omitempty" desc:"记录一条关键事实的键，需与 note_val 配对"`
	NoteVal       string `json:"note_val,omitempty" desc:"关键事实的值，需与 note_key 配对"`
}

// UpdateTool returns a tool the model calls to maintain its working memory.
// Writes go through ctx.Actions.StateDelta so they are folded into the
// resulting event and persisted by the session store (a direct State.Set would
// not survive a FileStore restart). See ADR 0017.
func UpdateTool() tool.Tool {
	return tool.New("update_working_memory",
		"维护跨轮的工作记忆：设置当前目标、增删待办、记录关键事实。任务有阶段性进展或确定了关键约束时调用，使其在上下文被压缩后仍然保留。",
		func(ctx *tool.Context, in updateArgs) (string, error) {
			snap := readSnapshot(ctx.State)

			var addedID string
			if in.Goal != "" {
				snap.Goal = in.Goal
			}
			if in.AddTodo != "" {
				addedID = core.NewID("todo")
				snap.Todos = append(snap.Todos, Todo{ID: addedID, Text: in.AddTodo})
			}
			if in.ResolveTodoID != "" {
				for i := range snap.Todos {
					if snap.Todos[i].ID == in.ResolveTodoID {
						snap.Todos[i].Done = true
					}
				}
			}
			if in.NoteKey != "" {
				if snap.Notes == nil {
					snap.Notes = map[string]string{}
				}
				snap.Notes[in.NoteKey] = in.NoteVal
			}

			if ctx.Actions != nil {
				if ctx.Actions.StateDelta == nil {
					ctx.Actions.StateDelta = map[string]any{}
				}
				ctx.Actions.StateDelta[stateKey] = encodeSnapshot(snap)
			}

			if addedID != "" {
				return "工作记忆已更新（新待办 id: " + addedID + "）", nil
			}
			return "工作记忆已更新", nil
		})
}
