# Market Data

This context records versioned market states so decisions and datasets can identify which market data they use.

## Language

**Canonical market revision**:
The selected authoritative market-data revision for one symbol, interval, and time bucket. A sequence of these selected revisions across buckets is the canonical market history.
_Avoid_: every raw feed update; the exact revision seen by a particular decision (that is decision-visible data).

## Live trading

**Opening order（开仓订单）**:
一次独立的开仓委托；同一委托的多次部分成交仍属于同一笔开仓。
_Avoid_: 每笔成交、独立退出批次

**Position batch（持仓批次）**:
同方向、同一平仓提交边界之前的开仓成交集合，共用退出规则。追加开仓后采用数量加权的成交均价，时间锚点由最新开仓订单的成交推进。
_Avoid_: 一笔开仓就是一个退出批次

**Decision replay**:
An audit that reconstructs or validates a past strategy decision from its recorded inputs and outcome. It is diagnostic, not part of live order execution; a canonical replay is a counterfactual using the selected market history rather than necessarily the exact input originally seen.

**Dataset manifest**:
An immutable description of a time-bounded market-data set that identifies the exact market revisions included, so research or backtesting can use a repeatable input set.
