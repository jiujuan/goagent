---
name: sales-report
description: 基于内置的月度销售数据，运行统计脚本并产出结构化的销售分析报告
allowed-tools: [use_skill, run_skill_script]
---
# 销售分析报告工作流

当用户要求生成销售分析报告时，**严格按顺序**执行下面每一步，不要跳步，也不要
凭记忆心算任何统计量——所有数字必须来自脚本输出：

1. 调用 `use_skill`，参数 `resource: "dataset.md"`，读取每个月的销售额原始数据。
2. 调用 `use_skill`，参数 `resource: "report-template.md"`，了解最终报告应有的章节结构。
3. 从 dataset.md 中**按时间顺序**提取每个月的销售额数字，作为 `args` 列表传给
   `run_skill_script`（skill = `sales-report`，script = `scripts/stats.py`）。
   脚本会输出 count / sum / mean / median / min / max，以及首月到末月的总增长率。
4. 用脚本返回的**真实数字**填充模板，写出最终报告。报告必须包含模板要求的全部
   章节，并在“关键发现”里至少给出 2 条基于这些指标的洞察。
