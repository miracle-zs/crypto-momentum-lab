# 历史研究脚本

保存 Orderflow 特征、Liquidation、K 线边界、历史排名和阶段回测课题，不属于当前实盘或日常研究入口，也不承诺兼容当前代码。

旧 Research、Replay/Paper、Shadow CLI 与 Compression/Liquidation 已退役。依赖它们的脚本需在基线 02e6581f3bc71feac0f91f84fa405460ea26730f 的独立 checkout 复现，不为运行档案恢复旧生产依赖。专属回放测试不进入默认测试集。

当前寻优与实盘对照使用 local_optimization，见[研究手册](../../docs/runbooks/local-full-data-optimization.md)。它被 Git 忽略，完整复现还需本地源码和数据备份。脚本中的旧参数、文件名和建议不自动成为当前交易配置。
