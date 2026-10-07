// Command context-window-relevance 演示给 middleware.ImportanceWeighted 注入
// 自定义打分器的形状：同一段脚本跑两遍，一遍用内置启发式打分，一遍用
// embeddings.Embedder 算余弦相似度的打分器，比较两边留下的是哪些消息。
//
// 重点是"注入远端打分器时该怎么做"这三件事，它们都写在这里而不是库里：
//
//   - 打分器自己管缓存，键是消息内容的哈希；缓存不进 State.KV（它是性能缓存，
//     不是要跨进程续存的账本，进 KV 会让每步 checkpoint 多带一份 map）。缓存的值
//     是"与当前问题的相似度"，所以问题一换整个缓存都作废，只能丢掉重算——增量淘
//     汰在这里没有意义。
//   - 每步只把"没缓存的那部分"送去编码，一次批量调用。
//   - 条数有上限，满了整体丢弃，不做逐出策略。
//
// 全程 llm/mock + embeddings/mock，无网络、无密钥，可重复执行：
//
//	go run ./examples/context-window-relevance
//
// 说明：离线用的 embeddings/mock 是词袋哈希向量，它的相似度与关键词重叠同源，
// 所以本例不是两种打分器的优劣对比，也不是"看保留集分歧"的对比——最后会把两遍
// 的保留集逐项比一遍，相同就打印相同。能看清的是注入打分器之后角色先验不再参与、
// 位置项仍由策略来加，以及缓存实际省下多少次编码。
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/embeddings"
	embedmock "github.com/jiujuan/goagent/embeddings/mock"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

const (
	toolTurns = 8   // 前 8 次模型调用都要调工具
	budget    = 700 // 两遍共用的 token 预算
)

// 一条与开头问题相关、但按时间已经很旧的消息：两遍都想看它会不会被留下。
const oldRelevant = "预算上限的口径：只算消息历史，系统提示与工具定义由校准吸收。"

func main() {
	emb := &embeddingScorer{emb: embedmock.New(), cap: 512, cache: map[string]float64{}}

	passes := []struct {
		title string
		strat middleware.WindowStrategy
		stats func() string
	}{
		{
			title: "1. 内置启发式打分（角色先验 + 关键词重叠，零调用）",
			strat: middleware.ImportanceWeighted(middleware.ImportanceOptions{BudgetTokens: budget}),
			stats: func() string { return "不涉及远端调用，也没有缓存" },
		},
		{
			title: "2. 注入 embeddings 打分器（余弦相似度 + 内容哈希缓存）",
			strat: middleware.ImportanceWeighted(middleware.ImportanceOptions{BudgetTokens: budget, Scorer: emb}),
			stats: emb.stats,
		},
	}

	var results []passResult
	for _, p := range passes {
		results = append(results, runPass(p.title, p.strat, p.stats))
	}
	fmt.Println("\n读法：两遍的预算、脚本、工具完全相同，差别只在内容分由谁给。")
	fmt.Println("     换成真实 embedding 服务时，这个形状不用改：缓存、批量、上限都在打分器自己手里。")
	summary, lines := diffKept(results[0], results[1])
	fmt.Printf("     保留集逐项比对：%s\n", summary)
	for _, l := range lines {
		fmt.Printf("     %s\n", l)
	}
}

// passResult 是一遍跑完之后可比对的东西：最后一次输入的消息条数，以及保留下来的
// 内容类别（按内容前缀去重，所以同样内容的重复条目只算一类）。
type passResult struct {
	msgs   int
	labels []string
}

// diffKept 把两遍的保留集逐项比一遍。相同就明说相同——离线 mock 的词袋相似度与
// 关键词重叠同源，这里本来就未必有分歧可看。
func diffKept(a, b passResult) (string, []string) {
	inA := map[string]bool{}
	inB := map[string]bool{}
	for _, k := range a.labels {
		inA[k] = true
	}
	for _, k := range b.labels {
		inB[k] = true
	}
	var onlyA, onlyB []string
	for _, k := range a.labels {
		if !inB[k] {
			onlyA = append(onlyA, k)
		}
	}
	for _, k := range b.labels {
		if !inA[k] {
			onlyB = append(onlyB, k)
		}
	}
	if len(onlyA) == 0 && len(onlyB) == 0 {
		if a.msgs == b.msgs {
			return fmt.Sprintf("两遍完全相同（各 %d 条消息、%d 类内容）", a.msgs, len(a.labels)), nil
		}
		return fmt.Sprintf("两遍内容类别相同，但条数不同（第 1 遍 %d 条、第 2 遍 %d 条）", a.msgs, b.msgs), nil
	}
	lines := make([]string, 0, len(onlyA)+len(onlyB))
	for _, k := range onlyA {
		lines = append(lines, "只在第 1 遍："+k)
	}
	for _, k := range onlyB {
		lines = append(lines, "只在第 2 遍："+k)
	}
	return fmt.Sprintf("有分歧：第 1 遍独有 %d 类，第 2 遍独有 %d 类", len(onlyA), len(onlyB)), lines
}

func runPass(title string, strat middleware.WindowStrategy, stats func() string) passResult {
	calls := 0
	var lastSent []core.Message
	conv := mock.New("model", func(req *llm.Request) *llm.Response {
		calls++
		lastSent = req.Messages
		if calls == 1 {
			// 第 1 步先让模型调 policy，把那条口径说明放进历史；它字面撞上了问题里的
			// "预算"和"定"，位置却会越来越旧，正是两遍要对比的那条。
			return mock.CallTool("c1", "policy", "{}")
		}
		if calls <= toolTurns {
			return mock.CallTool(fmt.Sprintf("c%d", calls), "fetch", fmt.Sprintf(`{"n":%d}`, calls))
		}
		return mock.Text("收尾完成。")
	})

	a, err := agent.New(
		agent.WithModel(conv),
		agent.WithTools(policyTool(), fetchTool()),
		agent.WithMiddleware(middleware.Window(middleware.WindowOptions{Strategy: strat})),
		agent.WithMaxTurns(toolTurns+3),
	)
	if err != nil {
		panic(err)
	}

	run := a.Stream(context.Background(), "怎么给这次迁移定预算？")
	trimmed := 0
	for ev, err := range run.Iter() {
		if err != nil {
			panic(err)
		}
		if _, ok := ev.(core.WindowTrimmed); ok {
			trimmed++
		}
	}
	if _, err := run.Wait(); err != nil {
		panic(err)
	}

	fmt.Printf("\n== %s\n", title)
	fmt.Printf("   模型最后一次输入：%d 条消息，裁剪事件 %d 次\n", len(lastSent), trimmed)
	// 只说末次输入里有没有，不替中间步下结论——那由上一行的裁剪次数说明。
	fmt.Printf("   那条旧的口径说明（末次输入）：%s\n", whereIs(lastSent, oldRelevant))
	fmt.Printf("   打分器：%s\n", stats())
	for _, m := range outline(lastSent) {
		fmt.Printf("     %s\n", m)
	}
	return passResult{msgs: len(lastSent), labels: labels(lastSent)}
}

// embeddingScorer 是 middleware.MessageScorer 的一种实现形状：内容哈希缓存 +
// 每步一次批量编码。
type embeddingScorer struct {
	emb   embeddings.Embedder
	cache map[string]float64 // 消息内容哈希 -> 与当前问题的相似度
	cap   int

	refKey   string    // 当前问题内容的键
	refVec   []float32 // 当前问题的向量，省得每步重编
	scored   int       // 被打分的消息条数（累计）
	embedded int       // 真正送去编码的消息条数（累计，不含参考文本）
	hits     int       // 直接数出来的缓存命中次数（不用 scored-embedded 推算）
	refVecs  int       // 为参考文本本身花的编码次数（单独计，免得混进上面两个口径）
}

// ScoreContent 给每条消息打一个与 ref 的相似度。它不看位置——位置项由
// ImportanceWeighted 自己加，否则哈希缓存会把过时的位置分冻住。
func (s *embeddingScorer) ScoreContent(ctx context.Context, ref string, msgs []core.Message) ([]float64, error) {
	out := make([]float64, len(msgs))
	keys := make([]string, len(msgs))

	// 问题变了就丢掉旧的参考向量。缓存也必须一起丢：里面存的是"与上一个问题的
	// 相似度"，留着就是拿旧问题的尺子给新历史打分。
	if key := hashText(ref); key != s.refKey {
		s.refKey, s.refVec = key, nil
		s.cache = map[string]float64{}
	}
	texts := make([]string, 0, len(msgs))
	idx := make([]int, 0, len(msgs))

	for i, m := range msgs {
		text := msgText(m)
		keys[i] = hashText(text)
		if v, hit := s.cache[keys[i]]; hit {
			s.hits++
			out[i] = v
			continue
		}
		texts = append(texts, text)
		idx = append(idx, i)
	}

	refCost := 0
	if s.refVec == nil {
		refCost = 1
	}
	if len(texts) == 0 && refCost == 0 {
		s.scored += len(msgs)
		return out, nil
	}

	// 批量是接口逼出来的：Embed 收的是 []string，所以"这一步只编没缓存的那部分"
	// 天然就是一次调用。参考文本和消息拼在同一批里，省掉一次往返。
	input := make([]string, 0, refCost+len(texts))
	if refCost > 0 {
		input = append(input, ref)
	}
	input = append(input, texts...)
	vecs, err := s.emb.Embed(ctx, input)
	if err != nil {
		return nil, err // 返回错误会让这一步不裁剪
	}
	s.embedded += len(texts)
	s.refVecs += refCost
	if refCost > 0 {
		s.refVec = vecs[0]
	}
	for k, i := range idx {
		// 缓存相似度而不是向量：同一条消息在历史里往后移动，不会改变它与
		// "当前问题"的相似度，这一部分才是可以复用的。
		s.cache[keys[i]] = cosine(s.refVec, vecs[refCost+k])
		out[i] = s.cache[keys[i]]
	}
	if len(s.cache) > s.cap {
		s.cache = map[string]float64{} // 满了整体丢弃，不做逐出策略
	}
	s.scored += len(msgs)
	return out, nil
}

func (s *embeddingScorer) stats() string {
	return fmt.Sprintf("打分 %d 条消息，其中送去编码 %d 条、命中缓存 %d 次；另外为参考文本本身编码 %d 次，当前缓存 %d 项",
		s.scored, s.embedded, s.hits, s.refVecs, len(s.cache))
}

// msgText 取一条消息里模型能读到的全部文本，包括工具结果的内容——
// core.Message.Text() 只拼顶层 Text part，工具消息在这里会是空串。
func msgText(m core.Message) string {
	s := m.Text()
	for _, p := range m.Parts {
		tr, ok := p.(core.ToolResult)
		if !ok {
			continue
		}
		for _, c := range tr.Content {
			if t, ok := c.(core.Text); ok {
				s += t.Text
			}
		}
	}
	return s
}

func hashText(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}

func cosine(a, b []float32) float64 {
	if len(a) == 0 || len(b) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

func whereIs(msgs []core.Message, want string) string {
	for i, m := range msgs {
		if strings.Contains(msgText(m), want) {
			return fmt.Sprintf("在，第 %d 条（%s）", i, m.Role)
		}
	}
	return "不在（这一步没发给模型）"
}

// outline 把最终输入压成几行短标注，便于两遍对比。
func outline(msgs []core.Message) []string {
	out := make([]string, 0, len(msgs))
	for i, m := range msgs {
		t := msgPrefix(msgText(m))
		out = append(out, fmt.Sprintf("第 %2d 条 [%s] %s", i, m.Role, t))
	}
	return out
}

// labels 给保留下来的消息归类，用于逐项比对两遍：同内容的重复条目（比如两条一摸
// 一样的工具输出）只算一类，所以比对说的是"内容类别"，条数另算。排序后再比，输出
// 与消息顺序无关，可重复。
func labels(msgs []core.Message) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		k := fmt.Sprintf("%s｜%s", m.Role, msgPrefix(msgText(m)))
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func msgPrefix(s string) string {
	r := []rune(strings.ReplaceAll(s, "\n", " "))
	if len(r) > 28 {
		return string(r[:28]) + "…"
	}
	if len(r) == 0 {
		return "(工具调用)"
	}
	return string(r)
}

func policyTool() tool.Tool {
	return tool.New("policy", "记录一条口径", func(_ *tool.Context, _ struct{}) (string, error) {
		return oldRelevant, nil
	})
}

// fetchTool 每次返回不同长度、不同编号的数据，所以两遍比的是"留了哪几条"，而不是
// "留了几条长得一模一样的东西"——内容全同的话，位置差异会被去重后的比对吞掉。
func fetchTool() tool.Tool {
	return tool.New("fetch", "取一段与问题无关的数据", func(_ *tool.Context, in struct {
		N int `json:"n" desc:"第几次取数"`
	}) (string, error) {
		// 长度随 n 变化，让每一轮的体积不同，预算的边际决定才有东西可分。下限要保证
		// 八轮加起来明显超过 budget，否则窗口一步都不会裁，两遍比的就是"都全留"。
		window := 900 + (in.N*137)%400
		return fmt.Sprintf("fetch 第 %d 次 %d 字节 %s", in.N, in.N*7919,
			strings.Repeat("y", window)), nil
	})
}
