// Package middleware provides concrete, reusable capabilities for the agent
// runtime, implemented as agent.Middleware (loop hooks):
//
//   - Tracing     — observe each turn / tool / error (the "add logic" pattern)
//   - RateLimit   — token-bucket throttle on model calls (BeforeModel)
//   - Permission  — gate tool calls: approve / ask (HITL) / deny (BeforeTool)
//   - Compaction  — summarize old history when it grows too long (ModifyRequest)
//   - RAG         — inject retrieved context into the system prompt (ModifyRequest)
//
// plus RetryModel, a model decorator (not a loop hook) that retries the bare
// model call with backoff — the natural place for retry.
//
// It imports agent (for the Middleware interface and LoopContext); agent never
// imports middleware, so the dependency graph stays acyclic.
package middleware
