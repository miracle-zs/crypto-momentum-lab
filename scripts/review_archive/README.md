# 审查复现脚本

2026-09-24 的 round 2/6 复现脚本已从 `local_optimization/reports/` 移至此处。
相关修复的正式回归测试保留在 `local_optimization/tests/`。
这些脚本不属于日常研究流水线。round 2 保存原始历史源码，依赖已移除的
`test_opportunity_and_wfa_repair` 和 `_init_verification_worker` 等旧接口，
不能在当前本地研究版本直接运行；复现需要当时的本地研究源码和数据备份。
`local_optimization/` 被 Git 忽略，仅 checkout 仓库基线不能恢复这些本地文件。
round 6 已验证可在当前工作区显式运行。

在仓库根目录运行：

```bash
rtk proxy .venv/bin/python -m scripts.review_archive.review_round6_20260924_repro
```

旧研究源码如需完整复现，可使用清理前 Git 基线
`02e6581f3bc71feac0f91f84fa405460ea26730f` 的独立 checkout。
