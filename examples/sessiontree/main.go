// Command sessiontree is a guided tour of goagent's conversation-tree backend:
// every public API for branching, fork, branch listing, and persistent
// re-summarization, exercised end to end against a file-backed store.
//
// Usage:
//
//	go run ./examples/sessiontree
//
// The program is split into numbered sections; each prints what it does and the
// resulting state so you can follow the API by reading the output next to the
// code.
//
// # The model
//
// A session's history is an append-only event log, but every event carries a
// ParentID, so the log is really a TREE. The "active conversation" a model sees
// is the path from the active leaf back to the root. A purely linear chat is the
// degenerate tree where each event's parent is its predecessor — so nothing
// about ordinary usage changes.
//
// # APIs demonstrated
//
//	session.NewFileStore(dir)                      persistent (JSONL) store
//	store.GetOrCreate(ctx, app, user, sessionID)   load/create a session
//	store.Append(ctx, s, event)                    commit an event (advances the leaf)
//	s.Messages() / s.Events() / s.Leaf() / s.State()   project the active branch
//
//	tree := store.(session.TreeStore)              optional tree extension
//	tree.Checkout(ctx, s, eventID)                 move the active leaf (then Append branches)
//	tree.Branches(ctx, s)                          list branch tips ([]session.Ref)
//	tree.Fork(ctx, s, fromID, newSessionID)        copy a path into a new session
//
//	session.Summarize(ctx, store, s, cutID, text)  write a persistent summary node
//	middleware.Resummarize(ctx, store, s, model, opts)  auto-compact past a threshold
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/session"
)

const (
	appName = "tree-demo"
	userID  = "user-1"
)

func main() {
	ctx := context.Background()

	// A throwaway directory so the example is self-contained and re-runnable.
	dir, err := os.MkdirTemp("", "goagent-tree-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fmt.Printf("会话落盘目录：%s\n", dir)

	// Hold the concrete store behind the Store interface, then detect the
	// optional tree capability with a type assertion — exactly how application
	// code should opt in without coupling to a specific backend.
	fileStore, err := session.NewFileStore(dir)
	if err != nil {
		log.Fatal(err)
	}
	var store session.Store = fileStore
	tree, ok := store.(session.TreeStore)
	if !ok {
		log.Fatal("store 不支持会话树")
	}

	section1Base(ctx, store)
	idA1 := lookupEvent(ctx, store, "main", "A1") // branch/fork/summarize anchor

	section2Branch(ctx, store, tree, idA1)
	section3Branches(ctx, tree)
	section4CheckoutBack(ctx, store, tree)
	section5Fork(ctx, store, tree, idA1)
	section6Summarize(ctx, store)
	section7Resummarize(ctx, store)
	section8Reload(ctx, dir)

	fmt.Println("\n✅ 完成。每一节都演示了一个会话树 API；上面的输出即其效果。")
}

// --- 1. Base linear conversation -------------------------------------------

func section1Base(ctx context.Context, store session.Store) {
	banner("1. 基础线性会话：Append / Messages / Leaf")
	s := get(ctx, store, "main")

	// Append commits events and advances the active leaf. A linear chat is just
	// repeated Append at the leaf.
	say(ctx, store, s, "user", "Q1：帮我规划一次三天旅行")
	say(ctx, store, s, "assistant", "A1：好的，先确定城市和预算")
	say(ctx, store, s, "user", "Q2：去成都，预算 3000")
	say(ctx, store, s, "assistant", "A2：建议第一天宽窄巷子……")

	dump("活动分支消息", s.Messages())
	fmt.Printf("当前活动叶 Leaf = %s\n", short(s.Leaf()))
	fmt.Printf("原始事件日志 Events = %d 条（投影 Messages = %d 条）\n", len(s.Events()), len(s.Messages()))
}

// --- 2. Checkout an earlier node, then branch ------------------------------

func section2Branch(ctx context.Context, store session.Store, tree session.TreeStore, idA1 string) {
	banner("2. 回到历史节点并开新分支：Checkout + Append")
	s := get(ctx, store, "main")

	// Checkout moves the active leaf back to A1 and rebuilds state along that
	// path. The next Append becomes a CHILD of A1 — a new branch — without
	// touching the original A2 continuation.
	must(tree.Checkout(ctx, s, idA1))
	fmt.Printf("Checkout 到 A1（Leaf=%s）后，活动消息回到分叉点：\n", short(s.Leaf()))
	dump("  活动分支", s.Messages())

	say(ctx, store, s, "user", "Q2'：改成去重庆，预算 5000")
	say(ctx, store, s, "assistant", "A2'：那第一天洪崖洞……")
	dump("追加新分支后的活动消息", s.Messages())
	fmt.Println("注意：原来的 A2（成都方案）仍在树里，只是不在当前活动路径上。")
}

// --- 3. List branch tips ----------------------------------------------------

func section3Branches(ctx context.Context, tree session.TreeStore) {
	banner("3. 列出所有分支：Branches -> []session.Ref")
	s := get(ctx, treeStore(tree), "main") // reuse same store via helper
	refs, err := tree.Branches(ctx, s)
	must(err)
	fmt.Printf("共有 %d 个分支末梢（tip = 没有子事件的叶）：\n", len(refs))
	for _, r := range refs {
		marker := "  "
		if r.Active {
			marker = "→ " // 当前活动分支
		}
		fmt.Printf("   %s%s  leaf=%s  active=%v\n", marker, r.Name, short(r.LeafEventID), r.Active)
	}
	fmt.Println("两个 tip 分别是成都方案(A2) 和重庆方案(A2')，→ 标记当前所在分支。")
}

// --- 4. Checkout back to the original trunk --------------------------------

func section4CheckoutBack(ctx context.Context, store session.Store, tree session.TreeStore) {
	banner("4. 切回原分支：Checkout 到另一个 tip")
	s := get(ctx, store, "main")

	// Find the non-active tip (the original 成都 branch) and check it out.
	refs, err := tree.Branches(ctx, s)
	must(err)
	var target string
	for _, r := range refs {
		if !r.Active {
			target = r.LeafEventID
		}
	}
	must(tree.Checkout(ctx, s, target))
	fmt.Printf("切回成都分支（Leaf=%s）：\n", short(s.Leaf()))
	dump("  活动分支", s.Messages())
	fmt.Println("State 会随分支切换沿新路径重放——分支间互不污染。")
}

// --- 5. Fork a node into a brand-new session -------------------------------

func section5Fork(ctx context.Context, store session.Store, tree session.TreeStore, idA1 string) {
	banner("5. 从某节点复制出新会话：Fork")
	s := get(ctx, store, "main")

	// Fork copies the path root..A1 into a separate session, leaving the
	// original untouched. Great for "用这段已铺垫的上下文做模板，派生多个后续"。
	forked, err := tree.Fork(ctx, s, idA1, "forked")
	must(err)
	dump("新会话 forked 的初始历史（root..A1 的拷贝）", forked.Messages())

	// Continue the fork independently.
	say(ctx, store, forked, "user", "Q：换个思路，做预算无上限的高端行程")
	say(ctx, store, forked, "assistant", "A：那建议私人向导 + 米其林……")
	dump("fork 独立追加后的历史", forked.Messages())

	main := get(ctx, store, "main")
	fmt.Printf("原会话 main 不受影响，仍为 %d 条消息。\n", len(main.Messages()))
}

// --- 6. Manual summary node -------------------------------------------------

func section6Summarize(ctx context.Context, store session.Store) {
	banner("6. 手动写持久摘要节点：session.Summarize")
	s := get(ctx, store, "main") // active = 成都分支：Q1,A1,Q2,A2

	cut := lookupEvent(ctx, store, "main", "A1") // 摘要覆盖 root..A1
	before := s.Messages()
	must(session.Summarize(ctx, store, s, cut, "【摘要】用户要去成都玩三天，预算3000，已确定大方向。"))

	fmt.Println("Summarize 覆盖 root..A1，并把摘要作为新 leaf 追加。")
	dump("投影前", before)
	dump("投影后（摘要替换前缀，其后消息保留）", s.Messages())
	fmt.Printf("原始事件日志仍是 %d 条（什么都没删，摘要是纯追加）。\n", len(s.Events()))
	fmt.Println("State 不受影响：摘要只是“视图”，状态仍沿完整路径重放。")
}

// --- 7. Automatic, persistent re-summarization ------------------------------

func section7Resummarize(ctx context.Context, store session.Store) {
	banner("7. 超阈值自动压缩并落盘：middleware.Resummarize")
	s := get(ctx, store, "long")

	// Seed a long conversation so we cross the (tiny, for-demo) threshold.
	for i := 1; i <= 8; i++ {
		say(ctx, store, s, "user", fmt.Sprintf("第 %d 轮：%s", i, strings.Repeat("内容", 12)))
		say(ctx, store, s, "assistant", fmt.Sprintf("第 %d 轮回复：%s", i, strings.Repeat("好的", 12)))
	}
	before := len(s.Messages())

	// A cheap summarizer model (here a mock returning fixed text). In real use
	// this would be a small/fast model with no tools.
	summarizer := mock.New("summarizer", func(*llm.Request) *llm.Response {
		return mock.Text("用户进行了多轮关于行程内容的问答，要点已归纳。")
	})

	did, err := middleware.Resummarize(ctx, store, s, summarizer, &middleware.CompactionOptions{
		MaxTokens:        80, // 故意调小以触发；真实场景按模型上下文调
		KeepRecentTokens: 30,
	})
	must(err)
	fmt.Printf("触发压缩 = %v；投影消息 %d -> %d 条（旧前缀被摘要替换）。\n", did, before, len(s.Messages()))
	dump("压缩后的活动消息（首条为摘要）", s.Messages())
	fmt.Printf("原始事件 %d 条全部保留在 JSONL 中，可审计、可回溯。\n", len(s.Events()))
	fmt.Println("再次调用 Resummarize 会追加更靠后的摘要节点，自动 supersede 旧摘要。")
}

// --- 8. Reload from disk: everything persists -------------------------------

func section8Reload(ctx context.Context, dir string) {
	banner("8. 全部落盘：用全新 Store 从磁盘恢复")
	fresh, err := session.NewFileStore(dir)
	must(err)

	main, err := fresh.GetOrCreate(ctx, appName, userID, "main")
	must(err)
	long, err := fresh.GetOrCreate(ctx, appName, userID, "long")
	must(err)

	fmt.Println("新进程/新 Store 读取同一目录，分支选择(Leaf)与摘要节点都恢复如初：")
	fmt.Printf("  main：Leaf=%s，投影 %d 条（含摘要）\n", short(main.Leaf()), len(main.Messages()))
	fmt.Printf("  long：Leaf=%s，投影 %d 条（含摘要）\n", short(long.Leaf()), len(long.Messages()))
	dump("  恢复后的 main 活动消息", main.Messages())
}

// --- helpers ----------------------------------------------------------------

// say appends a text event authored by author and returns it (so callers can
// keep its ID). Role is inferred from author.
func say(ctx context.Context, store session.Store, s *session.Session, author, text string) *core.Event {
	role := core.RoleAssistant
	if author == "user" {
		role = core.RoleUser
	}
	m := core.Message{Role: role, Parts: []core.Part{core.Text{Text: text}}}
	e := &core.Event{Author: author, Message: &m}
	must(store.Append(ctx, s, e))
	return e
}

func get(ctx context.Context, store session.Store, sessionID string) *session.Session {
	s, err := store.GetOrCreate(ctx, appName, userID, sessionID)
	must(err)
	return s
}

// lookupEvent finds the ID of the first event in a session whose message text
// begins with prefix (e.g. "A1"). Used to grab anchor nodes by their label.
func lookupEvent(ctx context.Context, store session.Store, sessionID, prefix string) string {
	s := get(ctx, store, sessionID)
	for _, e := range s.Events() {
		if e.Message != nil && strings.HasPrefix(e.Message.Text(), prefix) {
			return e.ID
		}
	}
	// Not on the active path? scan all branches via a checkout-free walk is not
	// exposed; for the demo the anchor (A1) is always on the active path.
	log.Fatalf("未找到事件 %q（可能不在活动路径上）", prefix)
	return ""
}

func dump(label string, msgs []core.Message) {
	fmt.Printf("%s（%d 条）：\n", label, len(msgs))
	for i, m := range msgs {
		fmt.Printf("   %d. [%-9s] %s\n", i+1, m.Role, oneline(m.Text()))
	}
}

func banner(title string) {
	fmt.Printf("\n%s\n%s\n", title, strings.Repeat("─", 60))
}

func short(id string) string {
	if len(id) > 8 {
		return id[len(id)-8:]
	}
	return id
}

func oneline(s string) string {
	if len([]rune(s)) > 32 {
		return string([]rune(s)[:32]) + "…"
	}
	return s
}

// treeStore recovers the underlying Store from a TreeStore (it embeds Store) so
// helpers can share one value.
func treeStore(t session.TreeStore) session.Store { return t }

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
