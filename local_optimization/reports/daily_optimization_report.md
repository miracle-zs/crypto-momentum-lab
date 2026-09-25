# 每日盘后本地寻优与对账综合决策简报 (2026-09-24)

> **生成时间**: 2026-09-24 17:14:42 (UTC+8) | **版本号**: `v20260924_171441` | **运行环境**: 本地隔离仿真引擎 (Physical Isolation)

## 1. 核心决策结论 (Executive Summary)

- **当前状态机层级**: **🟡 暂行观察期 (PROVISIONAL)**
- **主推荐生产候选 (S3 复利导向)**: `3/1/1.00%/0.30/1.5/1.25x/cd=0/slots=4` (Calmar: 2.27, 8D稳定性: 90.0%)
- **空间收益峰值候选 (S1 纯收益)**: `3/1/0.75%/0.30/2.0/1.25x/cd=0/slots=3` (+481.59U, MDD: 151.85U)
- **换参操作裁决**: **⏸️ 保持当前实盘参数不变 (继续观察积累)**

---

## 2. 步骤 2: 6 层因果对账审计 (Live vs Replay Reconciliation)

- **全账户对账综合状态**: **⚠️ 部分账户存在因果分歧 (AUDIT_ALERT)**

| 账户 | 账户定位与相位 | 实盘实际收益 | 离线回放收益 | 净值分歧 (USDT / %) | 平均撮合滑点 | 判定结论 |
|:---|:---|---:|---:|---:|---:|:---|
| `primary` | Primary (实盘主账户 · 00m Phase) | `$-102.33` | `$+5.18` | $-107.51 (56.64%) | 846.076 bps | ❌ 执行证据失配 (FAIL · First causal divergence at 2026-09-23 08:02:13.129+00 on SUPERUSDT [fills]: live_only_fill) |
| `acc01` | acc01 (实盘辅账户 1 · 15m Phase) | `$-107.70` | `$+5.18` | $-112.88 (70.57%) | 878.149 bps | ❌ 执行证据失配 (FAIL · First causal divergence at 2026-09-23 08:02:11.739+00 on SUPERUSDT [fills]: live_only_fill) |
| `acc02` | acc02 (实盘进取账户 1 · 30m Phase) | `$-40.09` | `$+5.18` | $-45.28 (27.86%) | 845.969 bps | ❌ 执行证据失配 (FAIL · First causal divergence at 2026-09-23 08:02:13.129+00 on SUPERUSDT [fills]: live_only_fill) |
| `acc03` | acc03 (实盘进取账户 2 · 45m Phase) | `$-117.27` | `$+5.18` | $-122.46 (70.40%) | 899.391 bps | ❌ 执行证据失配 (FAIL · First causal divergence at 2026-09-23 08:02:13.129+00 on SUPERUSDT [fills]: live_only_fill) |

### Primary 主账户 6 层因果对账细分 (L1~L6):

| 审计层级 | 审计对象 | 差异指标 | 判定结论 |
|---|---|---|---|
| **L1** | 标的池 Universe | 70.0% 吻合 | 标的池存在显著差异 |
| **L2** | 信号 Signals | 100.0% 匹配 (26/26 笔) | 信号流与回放存在差异 |
| **L3** | 风控意图 Intents | ⚠️ 人工介入 | 实盘发生2笔手动市价平仓 |
| **L4** | 订单成交 Fills | 846.076 bps | 71.2% 匹配 (37/52 笔) |
| **L5** | 平仓退出 Batches | ⚠️ 提前退出 | 手动市价卖出打断ATR止盈 |
| **L6** | 净值归因 Attribution | -107.51 USDT | 因果漂移率 56.64% |

---

## 3. 步骤 3: 6 场景 8 维网格寻优与 15s MTM 盯市走势

全量参数空间自由寻优 8 个维度（含 `max_open_positions ∈ [1,2,3,4]`），100% 采用 15 秒连续盯市：

| 场景标识 | 场景名称与目标 | 最佳 8D 参数组合 | 净收益 | 最大回撤 | Calmar | Ulcer Index | 8D 稳定性 | 峰值保证金 | 交易笔数 |
|:---|:---|:---|---:|---:|---:|---:|---:|---:|---:|
| `s_m280_pnl_max` | **S1: 280U/纯收益** | `3/1/0.75%/0.30/2.0/1.25x/cd=0/slots=3` | +$481.59 | 9.59% | 3.17 | 0.0439 | 91.7% | $200.0 | 912 |
| `s_m280_balanced` | **S2: 280U/稳健比** | `2/1/0.50%/0.60/1.5/1.0x/cd=0/slots=3` | +$287.05 | 5.35% | 4.93 | 0.0188 | 77.8% | $160.0 | 428 |
| `s_m280_compounding` | **S3: 280U/复利导向** | `3/1/1.00%/0.30/1.5/1.25x/cd=0/slots=4` | +$462.57 | 9.71% | 3.06 | 0.0383 | 90.0% | $200.0 | 689 |
| `s_unc_pnl_max` | **S4: 不设限/纯收益** | `2/1/0.50%/0.30/1.5/1.25x/cd=0/slots=3` | +$490.35 | 13.33% | 2.27 | 0.0558 | 100.0% | $340.0 | 1654 |
| `s_unc_balanced` | **S5: 不设限/稳健比** | `2/1/0.50%/0.60/1.5/1.0x/cd=0/slots=3` | +$287.05 | 5.35% | 4.93 | 0.0188 | 77.8% | $160.0 | 428 |
| `s_unc_compounding` | **S6: 不设限/复利导向** | `3/1/0.75%/0.30/2.0/1.25x/cd=0/slots=3` | +$481.59 | 9.59% | 3.17 | 0.0439 | 91.7% | $200.0 | 912 |
| `b_profile1` | *基线1: 当前实盘金牌* | `2/1/0.75%/0.30/3.0/1.25x/cd=0/slots=2` | +$401.06 | 8.87% | 3.05 | 0.0331 | 100.0% | $140.0 | 573 |
| `b_profile2` | *基线2: 旧版历史基线* | `2/1/0.50%/0.30/4.0/1.5x/cd=0/slots=2` | +$206.51 | 10.64% | 1.50 | 0.0385 | 88.9% | $120.0 | 498 |

> 💡 **同屏交互式看板**: [six_scenarios_equity_comparison.html](six_scenarios_equity_comparison.html) 包含 8 根 15s MTM 净值曲线与动态下钻。

---

## 4. 步骤 4: 状态机三级门禁审核与换参决策

- **当前评估状态**: `insufficient_evidence`
- **状态机审计附注**:
  - Insufficient history: 2 unique days < required 7d.
  - Full-Space Global Optimization Notice: Evaluated across 5,040 parameter candidates (cd=0 fixed) on all 501 symbols and 2,932,872 15s candles.

### 4.1 生产换参指引与风控操作准则

#### 换参执行风控原则：在途持仓冻结机制 (In-flight Freezing)
> [!IMPORTANT]
> **严禁动态覆写存量在途持仓的平仓线！**
> 若触发生产换参，交易引擎必须实施在途持仓冻结机制：
> 1. **存量持仓 (In-flight Positions)**：开仓后已被分配的追踪止盈 ATR、硬止损比例与冷却时间，必须严格锁定原有规则直至完全平仓；
> 2. **新信号 (New Signals)**：新参数仅从热加载完成后的下一个 15s 切片起，对新产生的突破开仓信号生效；
> 3. **槽位调整**：若并发槽位发生变更（如由 1 槽扩充至 2 槽），仅放开新槽位的准入，不干预已有槽位的运行。

---

## 5. 产出文件索引

- **综合决策报告 (当日)**: `reports/daily_optimization_report_2026-09-24.md`
- **综合决策报告 (历史版本)**: `reports/history/daily_optimization_report_20260924_171441.md`
- **15s MTM 看板**: `reports/six_scenarios_equity_comparison.html`
- **15s MTM 看板 (历史版本)**: `output/history/six_scenarios_equity_comparison_20260924_171441.html`
- **持久化实验数据库**: `data/derived/optimization/experiments.db`

_Crypto Momentum Lab · Local Optimization Pipeline_