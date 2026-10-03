# 旧运行入口与策略清理计划

日期：2026-10-03。清理前基线：`02e6581f3bc71feac0f91f84fa405460ea26730f`。

## 范围与边界

保留 Orderflow 实盘、账户同步、行情服务、研究采集器、运维看板和
`local_optimization/` 寻优、回放、对账。清理依据是这些实际入口的代码依赖，
不是“策略名相同”或“文件体积大”。本次只修改本地仓库，不部署、不修改数据库。

历史账本、数据库模型、Alembic 迁移、Live 的 shadow preflight/订单抑制能力保留。
`research_collector/` 和 Parquet 写入/物化能力保留。日常寻优 HTML 与 MTM CSV 保留。
工作区已有的其他文档修改不覆盖。

## 执行顺序

1. [x] 记录测试与 lint 基线，提取 Live/账户测试借用的 Shadow/Paper fixture。
2. [x] 移除旧 `cml-research`、`cml-strategy-runner`、`cml-shadow-operation` CLI，
   删除其应用模块、独立实现及专属测试。
3. [x] 移除 Compression/Liquidation 策略及专属测试；registry 只支持 Orderflow，
   保留其参数构造、覆盖和哈希行为。移除旧 Parquet 读取接口及仅供其使用的解析函数。
4. [x] 删除四个 retired Paper Compose 服务和其专用模板；保留部署脚本对旧容器的
   停止/删除兼容逻辑，更新部署测试和现行手册。
5. [x] 将历史修复工具、两个 review repro 集中归档；研究归档脚本注明复现所需历史
   基线，移除它们在默认测试集中的专属测试。更新 README 和清理清单。
6. [x] 检查剩余引用与 CLI/构建入口，运行单元、smoke、本地寻优及可用的集成/E2E
   测试；对照基线区分原有失败和新增失败，记录结果。

## 验证记录

- 清理前单元/smoke 基线：3283 passed，5 skipped。
- fixture 提取后 Live/账户/故障注入验证：928 passed。
- 策略注册、Parquet 写入、架构导入边界、Compose 与部署验证：234 passed。
- 本地研究与归档审计：122 passed（其中本地研究 120，归档审计 2）。
- 活跃源码、测试和本地研究的 AST 导入扫描：没有指向已删除模块的导入。
- 修改的 registry、Parquet、fixture、策略和部署清单测试 Ruff 检查通过。
- 全库 Ruff 基线 216 项，清理后剩 207 项，均位于未修改文件；未扩大为全库格式整治。
- Shell 语法与 `git diff --check` 通过。
- 清理后单元/smoke：3094 passed，5 skipped；与基线相同的两类 warning。
- 假 WebSocket、订单抑制与故障注入 E2E：11 passed。
- registry 分支简化后，注册/Live 配置/Profile 追加验证：17 passed。
- 全部默认测试成功收集：3272 项；没有因旧模块删除而发生导入/收集错误。
- Orderflow 配置构造与四个参数转换/覆盖 helper 的 AST 与清理前基线一致。
- Editable 安装及非 editable wheel 构建通过；安装元数据仅注册五个现行 CLI，
  三个旧命令的虚拟环境 wrapper 已移除。wheel 中没有被退役的包。
- Live 与研究采集 CLI 的 `--help` 启动通过。
- 数据库验证受限：默认全量测试在数据库 fixture 处超时后中止；独立验证
  `tests/integration/persistence/test_migrations.py` 同样在连接阶段失败，错误为
  `psycopg.errors.ConnectionTimeout`。`127.0.0.1:54329/cml_test` 能建立 TCP 连接，
  但 asyncpg 握手也在 3 秒内超时；Docker engine 查询在 4 秒内超时。
  未把数据库集成/E2E 报告为通过，也没有重启服务或修改数据库来绕过环境问题。
- Round 6 归档复现可执行；Round 2 依赖本地研究已移除的旧测试/私有接口，仅保存
  历史源码并标明需要当时的本地研究备份，不承诺当前兼容。

仓库清理完成。数据库集成验证需在可用的本地测试 PostgreSQL 恢复后执行：

```bash
rtk proxy .venv/bin/pytest tests/ -q
```

归档位置：`scripts/maintenance_archive/`、`scripts/review_archive/`，历史研究保留
在 `scripts/research_archive/`。完整旧实现从清理前 Git 基线复现。
`local_optimization/` 被 Git 忽略，本次在那里只移动两个 repro 并更新对应报告命令；
其算法、数据、HTML 与 CSV 没有修改。
