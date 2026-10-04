# 历史维护工具

这些工具不参与实盘进程或本地研究流水线，不随默认测试集运行。
保留源码用于事故审计和历史账本修复；移动文件不表示历史数据已经修复完成。

固定账户与币种的 `repair_server_state_20260929.py` 事故补丁已删除：依赖已移除的私有函数，旧源码可从 Git 历史查阅。

- `legacy_order_identity_repair.py`：历史 client order ID 碰撞审计工具。
  默认只读，只有 `--apply` 才记录修复标记。可在仓库根目录显式运行：

  ```bash
  rtk proxy .venv/bin/python -m scripts.maintenance_archive.legacy_order_identity_repair --help
  ```

保留的审计单测可单独执行：

```bash
rtk proxy .venv/bin/pytest scripts/maintenance_archive/tests -q
```

本次清理没有执行这些工具，没有读取或修改生产数据库。
