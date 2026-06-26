#!/usr/bin/env python3
"""把标题转成 URL slug。整个标题作为单个参数传入。

用法:
    python3 slugify.py "Fix login timeout bug"   ->  fix-login-timeout-bug
"""
import re
import sys


def slugify(text):
    text = text.strip().lower()
    # 非字母数字（含中日韩等 \w 之外的字符）统一转连字符
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def main(argv):
    if not argv:
        print("error: 未提供标题", file=sys.stderr)
        return 2
    title = " ".join(argv)
    print(slugify(title))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
