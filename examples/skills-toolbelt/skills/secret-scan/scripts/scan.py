#!/usr/bin/env python3
"""启发式密钥扫描：在传入文本中逐行匹配常见的凭证模式。

整段待扫描文本作为单个参数传入（命中时返回行号、类型与脱敏片段）。

用法:
    python3 scan.py "AWS_KEY=AKIAIOSFODNN7EXAMPLE\\napi=sk-abc123..."
"""
import re
import sys

# (类型, 正则) —— 故意保守，宁可多报也尽量不漏常见形态。
PATTERNS = [
    ("AWS Access Key ID", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("OpenAI-style API key", re.compile(r"\bsk-[A-Za-z0-9]{16,}\b")),
    ("Private key (PEM)", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
    ("Bearer token", re.compile(r"\bBearer\s+[A-Za-z0-9._\-]{12,}\b")),
    ("Hardcoded password/token", re.compile(r"(?i)\b(?:password|passwd|pwd|secret|token|api[_-]?key)\s*[:=]\s*\S{6,}")),
]


def mask(s):
    s = s.strip()
    if len(s) <= 8:
        return s[0] + "***"
    return s[:4] + "***" + s[-4:]


def main(argv):
    if not argv:
        print("error: 未提供待扫描文本", file=sys.stderr)
        return 2
    text = "\n".join(argv)

    hits = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        for kind, rx in PATTERNS:
            m = rx.search(line)
            if m:
                hits.append((lineno, kind, mask(m.group(0))))

    if not hits:
        print("OK: 未发现明显的密钥/凭证（启发式扫描，仅供参考）")
        return 0

    print(f"WARNING: 命中 {len(hits)} 处疑似密钥：")
    for lineno, kind, snippet in hits:
        print(f"  - 第 {lineno} 行 [{kind}]: {snippet}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
