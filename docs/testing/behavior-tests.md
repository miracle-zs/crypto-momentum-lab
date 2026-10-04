# 测试与交易行为回归

测试约束交易结果，不约束实现必须经过哪些类、调用几次私有方法或使用哪种缓存/复制方式。先验证替代覆盖，再删除旧测试；不整体清空真实故障的回归依据。

## 2026-10-04 已完成的清理

|范围|删除或改写|替代验证|
|---|---|---|
|看板静态资源|删除 27 个 JS/CSS 源码字符串与内部布局断言；取消固定前端模块名单|保留页面挂载、相对资源地址、依赖文件可解析检查；执行现有前端行为测试；新增空报告、时间线排序/输入不变、HTML 转义三个行为测试|
|账本事务|删除两个私有 staged copy 对象身份/容器复制方式测试|真实 PostgreSQL 注入提交失败，通过公开 read 检查内存数量和版本不变，重启恢复检查数据库仍为原状态|
|账本读取与事实|删除缓存容器大小和私有缓存字段断言；幂等下单测试通过 observe 输入覆盖事实，不直接设置 journal|相同事实版本稳定、新事实版本变化；事实哈希确定且区分仓位身份；相同请求返回原收据、改变数量构成冲突|
|账户调度|删除 account-4 灰度启用常量断言|保留该账户真实预留、批次归属、UNKNOWN、成交结算等交易结果测试|
|退出链路|取消必须调用指定 allocator 的 monkeypatch 计数测试|直接验证亏损 K 线产生全量 reduce-only 市价退出请求|
|宽限期|扩大原有计时行为测试为 1/8 根 K 线|截止前不退出；截止后无需新行情即可取消限价并生成 reduce-only 市价兜底；重试身份稳定|

本轮已审查全部 278 个单元测试文件，累计修改 40 个单元测试文件。与当前 HEAD 相比，单元测试函数定义由 2184 个降至 2142 个，净减少 42 个；这不是 pytest 参数化展开后的用例数量。新增三个前端行为测试，并改写 PostgreSQL 事务回滚验证。本轮测试清理没有修改生产逻辑；工作区另有此前架构简化留下的生产代码改动。

|进一步处理的范围|结果验证|
|---|---|
|命令仓库与账本恢复|通过 restore、observe、read 验证重复事件、已有成交身份、事实缺失冲突和重启后的数量；删除仓库固定调用顺序与私有集合形状断言|
|成交水位与事实缓存|验证旧累计成交重放不重复计量、新成交正确推进、相同事实哈希稳定；删除缓存命中计数、共享对象身份与内部容器大小断言|
|终态结算与外部平仓恢复|使用公开恢复入口和持久化工作单元验证数量、恢复要求及持久化恢复标记；删除账本私有状态集合断言|
|账户快照与 Hub|通过实际事件持久化验证重复快照合并、数量变化及时保存；通过协议输出验证序列缺口后的恢复和账户隔离|
|REST 与账户调度|通过实际请求的 timeout 扩展验证有限超时，通过闲置后再次下单验证恢复；删除工作线程字典布局断言|
|运行装配与退出重试|验证装配成功/失败后的资源释放、订单结果、取消后重新启动仍处理原行情；删除对象身份、缓存字段与重试计数等冗余断言|
|仓位修复|损坏恢复通过公开读取报错，正常恢复通过数量和版本验证；不再要求指定私有集合的内容|
|行情补桶|验证长时间静默后的连续桶和无虚构成交；删除前驱查询次数、内部堆形状等实现约束|
|看板缓存与研究选择|通过淘汰后重新加载、失败后重试、实际峰值并发和业务选择结果验证；删除私有缓存/任务集合布局断言|

## 必须保留的交易契约

|行为|现有验证位置|
|---|---|
|8 根 K 线到期退出、初始宽限期不重开|tests/unit/live_rollout/test_exits.py、test_exit_processor.py、test_grace_limit_exchange_contract.py|
|排队取消不 POST，已发包取消保留未知结果|tests/unit/execution_account/orders/test_coordinator.py|
|网络超时/非法响应后单订单反查，禁止盲目重发|tests/unit/execution_account/test_binance_client.py、orders/test_coordinator.py|
|慢对账不阻断同标的退出或其他标的下单|tests/unit/execution_account/orders/test_coordinator.py|
|同批次多笔订单数量归属、部分成交与预留守恒|tests/unit/execution_account/orders/test_coordinator.py、tests/unit/execution/test_execution_book.py|
|重复成交、累计成交乱序与重启水位|tests/unit/execution/test_execution_book.py、tests/integration/persistence/test_authority_book_transactions.py|
|订单与预留原子落库、失败不污染内存|tests/integration/persistence/test_authority_book_transactions.py|

## 保留范围与完成边界

本轮确认的源码拼写、纯透传、对象身份、固定内部调用顺序和缓存布局测试已完成清理；有效的交易行为测试无需重写。没有清空测试目录，也没有为缩减行数删除真实事故回归。

仍保留必要的内部故障注入和协议/持久化契约测试：成交游标不能回退、事件未落库不能应用、提交失败不能污染状态、队列溢出与协议序列缺口、历史数据解码、资源上限、配置校验和导入环。这些验证实际约束；并不把“出现私有字段”作为机械删除标准。

本记录表示本轮测试审查与迁移完成，不表示交易系统所有架构简化工作完成。本轮不提交、不推送、不部署。

## 回归结果

- Python 单元、smoke、端到端：2990 passed，1 skipped（真实采集环境未配置），3 个既有警告。
- PostgreSQL 集成：172 passed，1 个既有警告。
- Node 前端行为：63 passed，无失败或跳过。
- 修改文件 Ruff F/I 与 git diff --check 通过。

## 常规验证

在独立本地测试库可用时，从仓库根目录执行。测试数据库配置遵循 [tests/conftest.py](../../tests/conftest.py) 的本地与库名保护，不指向生产库。

```bash
CML_RUN_HUB_NETWORK_TESTS=1 .venv/bin/python -m pytest tests/unit tests/smoke tests/e2e -q
.venv/bin/python -m pytest tests/integration -q
node --test tests/frontend/*.test.mjs
```

故障剧本在 tests/e2e/test_fault_injection_scenarios.py：行情缺口/回放窗口外、持久化恢复缺 epoch、发包中取消后按原身份查询、账户队列溢出后只重放一次、交易所时间倒退不覆盖新事实。不要将测试通过写成真实交易所验收。

本地研究测试位于被 Git 忽略的 local_optimization/tests，可在具备该源码和数据的工作区另行运行，不计入上方仓库回归数量。
