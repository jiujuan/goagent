#!/usr/bin/env python3
"""统计脚本（技能 sales-report 的 Level-3 资源）。

把若干数字作为命令行参数传入，输出 count/sum/mean/median/min/max，
以及首个样本到最后一个样本的总增长率。模型应当读取本脚本的输出，
而不是自己心算统计量。

用法:
    python3 stats.py 120 98 145 ...
"""
import sys


def main(argv):
    nums = []
    for raw in argv:
        try:
            nums.append(float(raw))
        except ValueError:
            print(f"error: 无法解析为数字: {raw!r}", file=sys.stderr)
            return 2
    if not nums:
        print("error: 未提供任何数字", file=sys.stderr)
        return 2

    n = len(nums)
    total = sum(nums)
    mean = total / n

    ordered = sorted(nums)
    mid = n // 2
    if n % 2:
        median = ordered[mid]
    else:
        median = (ordered[mid - 1] + ordered[mid]) / 2

    first, last = nums[0], nums[-1]
    growth = (last - first) / first * 100 if first else float("nan")

    def fmt(x):
        return f"{x:.2f}".rstrip("0").rstrip(".")

    print("=== 销售统计结果 ===")
    print(f"count   (样本数)   : {n}")
    print(f"sum     (总额)     : {fmt(total)}")
    print(f"mean    (均值)     : {fmt(mean)}")
    print(f"median  (中位数)   : {fmt(median)}")
    print(f"min     (最低)     : {fmt(min(nums))}")
    print(f"max     (最高)     : {fmt(max(nums))}")
    print(f"growth  (总增长率) : {fmt(growth)}%  (首月 {fmt(first)} → 末月 {fmt(last)})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
