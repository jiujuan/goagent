package runtime_test

import (
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/runtime"
)

// recorder is a test middleware that logs which hooks fire (to verify ordering)
// and can return a configured directive from BeforeModel / BeforeTool.
type recorder struct {
	runtime.BaseMiddleware
	name        string
	log         *[]string
	beforeModel core.Directive
	beforeTool  core.Directive
	modifySys   string // if set, ModifyRequest appends it to req.System
}

func (r recorder) BeforeModel(*runtime.LoopContext) (core.Directive, error) {
	*r.log = append(*r.log, "BM:"+r.name)
	return r.beforeModel, nil
}

func (r recorder) ModifyRequest(_ *runtime.LoopContext, req *llm.Request) error {
	*r.log = append(*r.log, "MR:"+r.name)
	if r.modifySys != "" {
		req.System += r.modifySys
	}
	return nil
}

func (r recorder) AfterModel(*runtime.LoopContext, *llm.Response) (core.Directive, error) {
	*r.log = append(*r.log, "AM:"+r.name)
	return core.Directive{}, nil
}

func (r recorder) BeforeTool(*runtime.LoopContext, *core.ToolCall) (core.Directive, error) {
	*r.log = append(*r.log, "BT:"+r.name)
	return r.beforeTool, nil
}

func (r recorder) AfterTool(*runtime.LoopContext, *core.ToolResult) (core.Directive, error) {
	*r.log = append(*r.log, "AT:"+r.name)
	return core.Directive{}, nil
}
