package skills

import (
	"strings"
)

// splitFrontmatter parses a SKILL.md byte slice into its YAML frontmatter
// fields and the remaining body. The frontmatter is the block delimited by a
// leading "---" line and the next "---" line, e.g.
//
//	---
//	name: pdf
//	description: Work with PDF files
//	allowed-tools: [run_command, web_fetch]
//	---
//	<body...>
//
// Only the minimal YAML subset this framework needs is supported: scalar
// values (quotes stripped) and string lists, written either inline as
// "[a, b]" or as a block of "- item" lines. Values are returned as string or
// []string in meta. If src has no frontmatter block, meta is nil and body is
// the whole input — Load treats a missing name as "not a skill".
func splitFrontmatter(src []byte) (meta map[string]any, body string) {
	text := string(src)
	// A frontmatter block must be the very first thing in the file (after an
	// optional UTF-8 BOM). Normalise CRLF so the parser is OS-agnostic.
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.TrimPrefix(text, "\uFEFF") // strip UTF-8 BOM if present

	if !strings.HasPrefix(text, "---\n") && text != "---" {
		return nil, text
	}
	rest := strings.TrimPrefix(text, "---\n")
	end := strings.Index(rest, "\n---")
	if end < 0 {
		// Opening fence but no close: treat the whole thing as body, no meta.
		return nil, text
	}
	header := rest[:end]
	body = rest[end+len("\n---"):]
	body = strings.TrimPrefix(body, "\n") // drop the newline after the fence
	body = strings.TrimLeft(body, "\n")   // and any blank lines before content

	return parseHeader(header), body
}

// parseHeader turns the frontmatter body into a key→(string|[]string) map. It
// understands "key: scalar", inline "key: [a, b]", and block lists where the
// key has no inline value and subsequent "- item" lines supply the elements.
func parseHeader(header string) map[string]any {
	meta := map[string]any{}
	var pendingKey string // key awaiting block-list "- item" lines

	for _, raw := range strings.Split(header, "\n") {
		line := strings.TrimRight(raw, " \t")
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		// A block-list element appends to the most recent empty-valued key.
		if item, ok := strings.CutPrefix(trimmed, "-"); ok && pendingKey != "" {
			cur, _ := meta[pendingKey].([]string)
			meta[pendingKey] = append(cur, unquote(strings.TrimSpace(item)))
			continue
		}

		key, value, ok := strings.Cut(trimmed, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		pendingKey = ""

		switch {
		case value == "":
			// Either an empty value or the head of a block list. Seed an empty
			// slice so a following "- item" run attaches here; a key that ends
			// up with no items renders as an empty list, harmless for our use.
			meta[key] = []string{}
			pendingKey = key
		case strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]"):
			meta[key] = parseInlineList(value)
		default:
			meta[key] = unquote(value)
		}
	}
	return meta
}

// parseInlineList parses a "[a, b, c]" flow sequence into a string slice.
func parseInlineList(value string) []string {
	inner := strings.TrimSpace(value[1 : len(value)-1])
	if inner == "" {
		return []string{}
	}
	parts := strings.Split(inner, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := unquote(strings.TrimSpace(p)); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// unquote strips a single pair of matching surrounding quotes, if present.
func unquote(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// strList coerces a meta value into a []string: a single scalar becomes a
// one-element slice, a list passes through, anything else yields nil.
func strList(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case string:
		if t == "" {
			return nil
		}
		return []string{t}
	default:
		return nil
	}
}

// metaString reads a string-valued field, returning "" when absent or non-scalar.
func metaString(meta map[string]any, key string) string {
	s, _ := meta[key].(string)
	return s
}
