---
name: secret-scan
description: 扫描一段文本/配置，找出疑似硬编码的密钥或凭证（运行内置扫描脚本）
allowed-tools: [use_skill, run_skill_script]
---
# 密钥泄露扫描

当用户给出一段配置、代码或日志、并担心其中含有密钥时：

1. 调用 `run_skill_script`，skill = `secret-scan`，script = `scripts/scan.py`，
   把**需要扫描的整段文本作为一个参数**放进 `args`。
2. 脚本会逐行匹配常见的密钥模式（AWS Access Key、私钥 PEM 头、`sk-` 形式的
   API key、`password=`/`token=` 赋值等），输出命中的行号、类型与脱敏片段。
3. 如脚本报告有命中，向用户**明确警告**并逐条列出；若无命中，说明未发现明显密钥
   （但提醒这只是启发式扫描，不能替代正式的密钥扫描工具）。
