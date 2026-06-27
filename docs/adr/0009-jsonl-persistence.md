# ADR 0009：会话持久化为 append-only JSONL，靠重放事件恢复

状态：已接受

## 背景

会话需要落盘以便进程重启后恢复。pi 用可插拔的 `SessionStorage`（含 jsonl-repo），
adk-go 用可插拔的 `session.Service`（含 database/vertexai 后端）。本框架已有
`session.Store` 接口（ADR 0003 的线性 append-only 模型），只需新增一个文件后端。

## 决策

新增 `session.FileStore`，实现 `Store`：

- **布局**：每个会话一个文件 `<dir>/<appName>/<userID>/<sessionID>.jsonl`，每行一个
  序列化的 `core.Event`（路径段做安全化处理）。
- **写**：`Append` 把事件序列化为一行追加写入，并提交到内存 session。
- **恢复**：`GetOrCreate` 若命中缓存则复用；否则读文件、**逐行重放事件**
  （`session.commit`）以重建消息日志**和**派生状态（`Actions.StateDelta`），再缓存。

事件能 round-trip 的前提是 `core.Message` 可 JSON 往返。由于 `Part` 是密封接口，
默认 JSON 无法反序列化，故给 `Message` 加了**带 `type` 标签的自定义
Marshal/Unmarshal**（`tool_result` 的嵌套 `Content` 同样处理）——见
`core/messagejson.go`。

## 理由

- **单一真相源**：事件日志即历史；状态是事件 `StateDelta` 的重放结果，无需单独持久化
  状态文件，天然一致。
- **append-only**：写入简单、可追加、易审计；崩溃后最多丢未刷盘的最后一行。
- **可插拔**：与 `InMemoryStore` 同接口，Runner 无感知；未来 DB/对象存储后端只需再实
  现一次 `Store`。

## 后果

- `Actions.StateDelta` 的值是 `map[string]any`，JSON 往返会把数字变成 `float64`、丢失
  具体 Go 类型。对当前用途（OutputKey 存字符串）无影响；需要强类型状态时应自行编码。
- 每次 `Append` 打开/追加/关闭文件一次（简单、安全）。高吞吐场景可改为持有文件句柄或
  批量刷盘。
- 当前无文件锁，假设单进程访问一个 session 目录。多进程并发需加锁或换 DB 后端。

## 备选方案

- **持久化整个 session 快照**：每次重写整文件，写放大大、并发更危险。append-only 更优。
- **单独持久化 state**：与事件日志可能不一致；重放派生更可靠。
