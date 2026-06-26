package textmem

import "strings"

// decode parses a memory file into an Entry. The frontmatter is a leading
// "---" block of "key: value" scalar lines (only name/description/type are
// read); everything after the closing fence is the Body. A file without
// frontmatter is treated as a bare body with the given fallback name. This is a
// minimal parser (cf. skill.splitFrontmatter, which is unexported and supports
// lists we do not need here).
func decode(fallbackName string, src []byte) Entry {
	text := strings.ReplaceAll(string(src), "\r\n", "\n")

	e := Entry{Name: fallbackName}
	if !strings.HasPrefix(text, "---\n") {
		e.Body = strings.TrimSpace(text)
		return e
	}
	rest := strings.TrimPrefix(text, "---\n")
	end := strings.Index(rest, "\n---")
	if end < 0 {
		e.Body = strings.TrimSpace(text)
		return e
	}
	header, body := rest[:end], rest[end+len("\n---"):]
	e.Body = strings.TrimLeft(strings.TrimPrefix(body, "\n"), "\n")
	e.Body = strings.TrimRight(e.Body, "\n")

	for _, line := range strings.Split(header, "\n") {
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		switch key {
		case "name":
			if val != "" {
				e.Name = val
			}
		case "description":
			e.Desc = val
		case "type":
			e.Type = val
		}
	}
	return e
}
