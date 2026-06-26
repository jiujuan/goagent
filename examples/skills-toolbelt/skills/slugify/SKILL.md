---
name: slugify
description: 把一段标题文本转换成 URL 友好的 slug（运行内置脚本，不要手算）
allowed-tools: [use_skill, run_skill_script]
---
# 生成 URL slug

当需要把一个标题转成 URL slug 时：

1. 调用 `run_skill_script`，skill = `slugify`，script = `scripts/slugify.py`，
   把**整个标题作为一个参数**放进 `args`（例如 `args: ["Fix login timeout bug"]`）。
2. 脚本会输出规范化后的 slug（小写、非字母数字转连字符、去除多余连字符）。
3. 把脚本输出的 slug 原样报告给用户，不要自己改写。
