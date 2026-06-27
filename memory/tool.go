package memory

import (
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/tool"
)

// searchArgs is the argument schema for the search_memory tool.
type searchArgs struct {
	Query string `json:"query" desc:"要在长期记忆/知识库中检索的关键词或问题"`
}

// SearchTool returns a tool the model can call to retrieve up to k relevant
// documents from the store. This is the LLM-driven retrieval style: the model
// decides when memory is needed. Pair it with an instruction telling the model
// it can call search_memory.
func SearchTool(store Store, k int) tool.Tool {
	if k <= 0 {
		k = 4
	}
	return tool.New("search_memory", "在长期记忆/知识库中检索与查询最相关的内容。需要事实、历史背景或外部知识时调用。",
		func(ctx *tool.Context, in searchArgs) (string, error) {
			docs, err := store.Search(ctx, in.Query, k)
			if err != nil {
				return "", err
			}
			if len(docs) == 0 {
				return "（没有找到相关内容）", nil
			}
			var b strings.Builder
			for i, d := range docs {
				fmt.Fprintf(&b, "[%d] (相关度 %.2f) %s\n", i+1, d.Score, d.Content)
			}
			return strings.TrimRight(b.String(), "\n"), nil
		})
}
