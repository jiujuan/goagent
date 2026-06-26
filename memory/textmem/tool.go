package textmem

import (
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/tool"
)

// saveArgs is the argument schema for save_memory.
type saveArgs struct {
	Name string `json:"name" desc:"记忆条目的短名（kebab-case，作为文件名与索引标识，重名会覆盖）"`
	Desc string `json:"description" desc:"一行摘要，会进入索引供检索"`
	Type string `json:"type,omitempty" desc:"分类：user|feedback|project|reference"`
	Body string `json:"body" desc:"记忆正文（Markdown）"`
}

// SaveTool returns a tool the model calls to persist a curated long-term memory.
func SaveTool(store Store) tool.Tool {
	return tool.New("save_memory",
		"把一条值得长期保留、可读的事实写入长期记忆（用户偏好、项目约定、被纠正的反馈等）。会出现在长期记忆索引中。",
		func(ctx *tool.Context, in saveArgs) (string, error) {
			if strings.TrimSpace(in.Name) == "" {
				return "", fmt.Errorf("name 不能为空")
			}
			if err := store.Save(ctx, Entry{Name: in.Name, Desc: in.Desc, Type: in.Type, Body: in.Body}); err != nil {
				return "", err
			}
			return "已保存记忆：" + in.Name, nil
		})
}

// readArgs is the argument schema for read_memory.
type readArgs struct {
	Name string `json:"name" desc:"要读取的记忆条目 name（见长期记忆索引）"`
}

// ReadTool returns a tool the model calls to read a memory entry's full body.
func ReadTool(store Store) tool.Tool {
	return tool.New("read_memory",
		"按 name 读取一条长期记忆的完整内容。索引里看到相关条目时调用。",
		func(ctx *tool.Context, in readArgs) (string, error) {
			e, err := store.Read(ctx, in.Name)
			if err != nil {
				return "（未找到该记忆条目）", nil
			}
			return e.Body, nil
		})
}
