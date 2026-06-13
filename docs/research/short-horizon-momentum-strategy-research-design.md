# Crypto Short-Horizon Momentum Strategy Research Design

## 中文执行摘要

本文定义三类加密永续合约分钟级短周期动量策略的独立研究方案：

1. **订单流爆发动量**：识别价格冲击、主动成交失衡、成交强度扩张和
   对手方流动性消耗是否同时发生。它不是简单追涨，而是判断推动价格的
   主动执行是否在 1 到 5 分钟窗口内持续。主要持仓周期为 3 到 15 分钟。
2. **波动压缩突破动量**：先识别低波动、窄区间和参与度收缩，再等待
   价格突破、主动成交和成交强度共同确认。主要持仓周期为 10 到 60 分钟，
   延迟敏感度最低，适合作为第一套完整研究策略。
3. **爆仓瀑布动量**：识别强平触发后的强制执行是否继续传播，而不是
   看到一笔爆仓就直接追单。主要持仓周期为 2 到 10 分钟，公开爆仓流
   并不完整，而且事件期滑点最大，因此研究和实盘难度最高。

三者可能描述同一段行情的不同阶段：

```text
波动压缩
-> 价格突破
-> 主动订单流持续
-> 爆仓加速
-> 订单流衰竭或反转
```

第一阶段必须分别验证三种假设，不能直接把它们拼成一套复杂规则。只有
在证明每种信号具有独立信息后，才考虑分层组合：压缩状态负责筛选机会，
订单流负责确认入场，爆仓活动负责调整置信度或执行紧迫度，并由统一风险
管理器避免重复持仓。

研究保留逐笔成交、买一卖一、爆仓事件以及本地接收时间，但主要特征按
15 秒聚合，并使用 1、3、5 分钟窗口确认趋势。逐笔数据用于重新聚合和
复盘，不代表每笔成交或每秒都要做交易。仅用一分钟 K 线仍无法可靠还原
主动买卖方向、点差和事件先后，但第一阶段不要求完整订单簿重建。所有
结果都必须扣除手续费、点差、延迟、市场冲击、止损滑点和可能发生的
资金费率。

建议把这项研究放到新的 `crypto-momentum-lab` 项目，而不是继续加入
当前 `momentum_alpha`。新策略需要逐笔原始数据留存、15 秒聚合、分钟级
信号确认、事件复盘和独立的研究数据集。它不以 100 毫秒或 1 秒预测为
目标，也不与高频系统竞争。

推荐研究顺序是：

1. 波动压缩突破动量。
2. 订单流爆发动量。
3. 爆仓瀑布动量。

这个顺序主要基于数据与工程风险，而不是预先判断哪一种收益最高。

## 1. Purpose

This document defines a research program for three short-horizon cryptocurrency
momentum strategies:

1. Order-flow impulse momentum.
2. Volatility-compression breakout momentum.
3. Liquidation-cascade momentum.

The goal is not to combine several indicators into one optimized backtest. The
goal is to determine whether each proposed source of momentum has positive,
repeatable expectancy after realistic fees, spread, slippage, latency, funding,
and adverse selection.

These strategies should initially be researched independently. They may react
to the same market move, so running them as three unrelated live strategies
would create duplicated exposure and misleading diversification.

## 2. Core Research Position

Short-horizon momentum is not simply a faster version of daily or weekly
momentum. At horizons from one minute to several tens of minutes:

- raw price continuation is weak and regime-dependent;
- market microstructure noise is large relative to expected profit;
- execution quality can dominate signal quality;
- the cause of the move matters more than the existence of the move;
- event-driven opportunities are preferable to continuous participation.

The common hypothesis behind all three strategies is:

> A short-term price move is more likely to continue when it is being driven by
> persistent aggressive order flow, constrained opposing liquidity, or forced
> execution, and when the remaining expected move materially exceeds total
> execution cost.

The primary feature windows are 1, 3, 5, 15, and 30 minutes. The typical
holding period is 5 to 20 minutes, with a minimum of roughly 2 to 3 minutes and
an extension up to 60 minutes for compression breakouts. One-second direction
prediction, sub-second market making, and latency arbitrage are explicitly out
of scope.

### 2.1 Time-Scale Separation

The system must distinguish data retention, feature aggregation, signal
confirmation, and holding period:

| Layer | Initial time scale |
| --- | --- |
| Raw trade retention | Exchange-native events |
| Base feature aggregation | 15 seconds |
| Initial impulse measurement | 1 minute |
| Momentum confirmation | 3 minutes |
| Broader market state | 5 to 30 minutes |
| Decision reevaluation | Every 15 seconds |
| Typical holding period | 5 to 20 minutes |
| Maximum holding period | 30 to 60 minutes |

Reevaluating every 15 seconds does not mean trading on a 15-second return. It
means detecting promptly when a multi-minute condition has become valid or
invalid. Entries should normally require persistence across multiple buckets
and agreement between the 1-minute impulse, 3-minute confirmation, and broader
market state.

## 3. Shared Scope

### 3.1 Market

- Binance USD-M perpetual futures.
- Both long and short directions.
- Initial universe limited to the most liquid contracts, such as BTCUSDT,
  ETHUSDT, and SOLUSDT.
- Additional symbols may be admitted only after passing explicit spread,
  depth, turnover, and slippage requirements.

The research universe must be point-in-time correct. Delisted symbols and
historical changes in contract availability must not be silently excluded.

### 3.2 Initial Raw Data

- Aggregate trades, including price, quantity, event time, and maker-side flag.
- Best bid and ask updates.
- One-minute klines for slower market-state features.
- Mark price and index price.
- Liquidation-order stream.
- Contract metadata, tick size, quantity step, minimum quantity, and minimum
  notional.
- Account-specific maker and taker commission rates.
- Funding-rate history.
- Open interest where the available timestamp resolution is useful.
- Local receive timestamp for every event, recorded using a monotonic clock in
  addition to exchange event time.

Raw websocket messages must be retained before feature calculation. Derived
features alone are insufficient because feature definitions and replay logic
will change during research.

The initial normalized research table should use 15-second buckets containing:

- open, high, low, close, and return;
- aggressive buy and sell notional;
- trade count and total notional;
- best bid, best ask, spread, and quoted midpoint;
- liquidation count and reported liquidation notional;
- data-quality and missing-event flags.

Incremental depth updates and local order-book reconstruction are optional
second-phase additions. They should be introduced only if the simpler
trade-flow and top-of-book model shows stable predictive value and a clear
research question requires depth information.

### 3.3 Common Cost Model

Every result must be reported after:

- entry and exit commissions;
- bid-ask spread;
- latency between signal observation and order arrival;
- depth-based market impact for aggressive orders;
- stop and liquidation-event slippage;
- funding when a position crosses a funding timestamp.

A signal is not tradable merely because its future mid-price return is
positive. The executable bid or ask and the expected fill must be used.

The expected gross move at entry should normally be at least two to three times
the estimated full round-trip cost. This is a research gate, not a guaranteed
profit threshold.

## 4. Strategy A: Order-Flow Impulse Momentum

### 4.1 Hypothesis

A rapid price move is more likely to continue when aggressive trading remains
strong in the same direction, transaction intensity expands, and opposing
liquidity is being consumed faster than it is replenished.

This strategy attempts to detect a continuing execution process rather than
buying solely because price has recently risen.

### 4.2 Preferred Horizon

- Base aggregation: 15 seconds.
- Signal windows: 1, 3, and 5 minutes.
- Minimum confirmation: multiple consecutive 15-second buckets.
- Expected holding period: 3 to 15 minutes.
- Typical implementation target: 5 to 10 minutes.

### 4.3 Primary Features

#### Standardized Return Impulse

For horizon \(h\):

```text
return_impulse_h =
    log(last_price_t / last_price_t-h)
    / expected_short_horizon_volatility_h
```

The denominator should be estimated using only prior observations. Fixed
percentage thresholds should not be shared across symbols with different
volatility.

#### Aggressive Trade Imbalance

```text
trade_imbalance_h =
    (aggressive_buy_notional_h - aggressive_sell_notional_h)
    / (aggressive_buy_notional_h + aggressive_sell_notional_h)
```

Binance aggregate trades expose whether the buyer is the maker. That field can
be used to infer the aggressive side. The exact interpretation must be tested
against captured examples before research data is trusted.

#### Trade Intensity

```text
notional_intensity_h =
    current_notional_h / historical_expected_notional_h

arrival_intensity_h =
    current_trade_count_h / historical_expected_trade_count_h
```

Baselines should account for symbol and time-of-week effects. Rolling medians
and robust z-scores are preferable to means when activity is heavy-tailed.

#### Optional Order-Book Pressure

Candidate features include:

- top-of-book imbalance;
- depth imbalance across multiple price bands;
- microprice relative to mid-price;
- rate of opposing-depth depletion;
- rate of same-side replenishment;
- spread expansion;
- book slope and local liquidity gaps.

Displayed depth is cancellable and may be deceptive. Book features are
optional second-phase confirmations, not required first-version inputs or
standalone directional truth.

#### Price Acceptance

Candidate confirmation features include:

- break of a recent event-time high or low;
- time spent beyond the breakout level;
- number of repeated trades beyond the level;
- limited immediate retracement;
- absence of large opposing absorption.

### 4.4 Candidate Trigger

A long candidate may require:

- positive standardized return impulse;
- strongly positive aggressive-trade imbalance across at least two horizons;
- elevated trade or notional intensity;
- price acceptance above a recent local high;
- acceptable spread and visible execution depth;
- no evidence that aggressive buying is being absorbed without further price
  progress.

Short entries use symmetric conditions.

Exact thresholds must come from predeclared grids or rolling quantiles and then
be validated out of sample. Symbol-specific hand tuning is not acceptable for
the initial study.

### 4.5 Exit Logic

The primary exit should be signal invalidation:

- aggressive-trade imbalance reverses;
- price stops progressing despite continued aggression;
- opposing liquidity replenishes and absorbs the move;
- price retraces a defined fraction of the initial impulse;
- a trailing stop evaluated on the 15-second state is crossed;
- maximum holding time expires.

A hard emergency stop remains necessary for missing data, websocket delay,
exchange disconnects, and discontinuous price moves.

### 4.6 Main Failure Modes

- Buying the final aggressive trades at the end of an exhausted move.
- Mistaking bid-ask bounce for continuation.
- Treating spoofable displayed depth as executable demand.
- Entering after spread expansion has consumed the expected edge.
- Multiple correlated symbols triggering on the same BTC-led market move.
- Backtests using exchange event time while ignoring local observation delay.

### 4.7 Minimal Viable Research Version

The first version should use aggregate trades, best bid/ask, 15-second
aggregation, and recent 1-to-5-minute price breaks. Full order-book features
should be added only after the simpler model has demonstrated gross predictive
power.

## 5. Strategy B: Volatility-Compression Breakout Momentum

### 5.1 Hypothesis

Low-volatility consolidation can concentrate resting orders and stop orders.
When price leaves the range with expanding participation and directional order
flow, triggered stops and new entries may create a move that persists for
several minutes.

Compression is a market-state filter. The actual entry still requires a
breakout with acceptance and participation.

### 5.2 Preferred Horizon

- Compression window: 5 to 60 minutes.
- Breakout observation: 1 to 3 minutes.
- Expected holding period: 10 to 60 minutes.

This is the least latency-sensitive of the three strategies and is the best
candidate for the first complete research and execution pipeline.

### 5.3 Compression Features

Candidate definitions include:

- realized volatility percentile;
- high-low range relative to ATR or recent median range;
- Bollinger-band width percentile;
- distance between short and medium moving averages;
- declining trade intensity;
- declining absolute order-flow imbalance;
- repeated rejection of both range boundaries;
- stable or narrowing spread.

Compression should be defined relative to each symbol's own recent history,
not by a universal percentage threshold.

### 5.4 Breakout Features

- trade or mid-price crossing the compression boundary;
- breakout distance normalized by recent volatility;
- aggressive-trade imbalance aligned with the breakout;
- notional and trade-arrival intensity expansion;
- multiple trades and minimum dwell time outside the range;
- limited immediate re-entry into the prior range;
- available depth sufficient to enter without consuming the expected edge.

### 5.5 Candidate Trigger

A long candidate may require:

1. A completed compression state.
2. A break above the predefined range.
3. Directionally aligned aggressive order flow.
4. Activity expansion relative to the compression baseline.
5. Price acceptance outside the range.
6. Expected move greater than the full cost threshold.

The range and all thresholds must be frozen before the breakout. Using future
highs, lows, or finalized bars that were incomplete at the signal time creates
look-ahead bias.

### 5.6 Exit Logic

Possible exits include:

- return inside the former range;
- order-flow reversal;
- failure to make progress within a short confirmation interval;
- volatility-scaled trailing stop;
- partial profit-taking followed by a 15-second or one-minute trailing exit;
- maximum holding time.

Research should compare full exits with partial exits, but the simpler full-exit
rule should be the baseline.

### 5.7 Main Failure Modes

- False breakout caused by a single market order.
- Entering after most of the move has already occurred.
- Defining compression with arbitrary indicator combinations.
- Excessive parameter search over windows and band definitions.
- Ignoring market-wide moves that make every symbol appear to break out.
- Backtesting from one-minute OHLC bars that cannot determine intrabar event
  order or executable price.

### 5.8 Minimal Viable Research Version

Start with:

- rolling range or realized-volatility compression;
- aggregate-trade participation;
- best bid/ask;
- fixed, pre-event breakout boundaries;
- event-driven exit simulation.

This version can be studied before full depth reconstruction and should be the
first strategy taken through end-to-end paper trading.

## 6. Strategy C: Liquidation-Cascade Momentum

### 6.1 Hypothesis

Forced market orders can create a positive feedback loop:

```text
price break
-> leveraged positions liquidate
-> forced aggressive orders move price further
-> additional positions liquidate
```

The tradable signal is not the existence of a liquidation. It is evidence that
forced execution is still propagating after the initial event.

### 6.2 Preferred Horizon

- Liquidation aggregation: 15 to 30 seconds.
- Continuation confirmation: 30 to 90 seconds.
- Entry window: 30 seconds to 2 minutes after the event begins.
- Expected holding period: 2 to 10 minutes.

### 6.3 Required Features

- liquidation direction, notional, and arrival clustering;
- price displacement before and after liquidation events;
- aggressive-trade imbalance following the event;
- trade and notional intensity;
- optional opposing-depth depletion and liquidity gaps;
- recovery speed after the first liquidation burst;
- mark-price and last-price divergence;
- market-wide liquidation synchronization across major contracts.

The Binance all-market liquidation stream provides snapshots rather than a
complete liquidation ledger. Reported liquidation notional must therefore be
treated as a censored event indicator, not the true total liquidation amount.

### 6.4 Candidate Trigger

For downward continuation:

1. A meaningful long-liquidation event or cluster occurs.
2. Price breaks a recent low or liquidity pocket.
3. Aggressive selling remains elevated after the reported liquidation.
4. Best-bid conditions remain weak; depth depletion may be used later as an
   optional confirmation.
5. Price fails to recover quickly.
6. The executable downside remaining exceeds the estimated round-trip cost.

The upward version is symmetric for short liquidations.

### 6.5 Exit Logic

Liquidation momentum should exit quickly when:

- forced-event activity stops;
- ordinary aggressive flow no longer continues in the same direction;
- price recovers a defined fraction of the cascade move;
- opposing depth replenishes;
- a sharp V-shaped reversal begins;
- the maximum holding time expires.

The strategy should not average down or add to a position after invalidation.
Its edge, if present, is short-lived and event-specific.

### 6.6 Main Failure Modes

- Entering after the final liquidation print.
- Treating an incomplete public stream as complete market volume.
- Chasing a liquidation directly into a large passive absorber.
- Confusing a temporary mark-price dislocation with executable continuation.
- Suffering severe slippage during the exact events the strategy targets.
- Strategy and stop orders failing during exchange congestion.

### 6.7 Minimal Viable Research Version

First perform event studies without trading:

- align liquidation events with aggregate trades and best bid/ask;
- measure executable forward returns after 30, 60, and 90 seconds and after 2,
  5, and 10 minutes;
- condition results on post-event aggressive-flow continuation;
- compare continuation events with immediate-reversal events.

Only after the event study identifies stable conditional continuation should a
trading simulator be introduced.

## 7. Relationship Between the Strategies

The three strategies represent different parts of a possible momentum process:

```text
compression state
-> breakout
-> persistent aggressive order flow
-> optional liquidation acceleration
-> order-flow exhaustion or reversal
```

However, this relationship must not be assumed in the first analysis. Research
should proceed in two stages.

### Stage 1: Independent Evaluation

Each strategy receives:

- its own event definition;
- its own feature table;
- its own entry and exit baseline;
- its own cost-adjusted results;
- overlap reporting against the other strategies.

This establishes whether each hypothesis has standalone information content.

### Stage 2: Hierarchical Combination

If standalone evidence exists, the preferred combined model is hierarchical:

- compression identifies an eligible market state;
- order-flow impulse confirms the entry;
- liquidation activity increases or decreases confidence;
- one shared position and risk manager prevents duplicate orders.

The liquidation signal should initially modify confidence or urgency rather
than open a second position in the same direction.

An alternative ensemble that lets all three strategies trade independently is
not recommended until overlap, correlation, and incremental contribution have
been measured.

## 8. Research and Backtest Method

### 8.1 Event-Driven Replay

The simulator must replay recorded messages in local receive order, build
15-second feature buckets, and reconstruct the information available at each
decision point. One-minute candles may be used for slower market-state
features, but they must not be the sole representation for order-flow,
liquidation, spread, or executable-price analysis.

The replay engine must support:

- exchange and local timestamps;
- configurable observation and order latency;
- aggressive market orders;
- partial fills;
- cancellation delay;
- spread and depth changes between signal and fill;
- forced position closure on stale or invalid market state.

### 8.2 Labels and Evaluation Horizons

Predictive analysis should measure executable forward returns over several
horizons rather than train directly on one arbitrary profit target.

Suggested horizons:

- 30 and 60 seconds for early response diagnostics;
- 2, 3, 5, 10, 15, 30, and 60 minutes for strategy evaluation.

Both maximum favorable excursion and maximum adverse excursion must be
recorded. Mid-price prediction, executable return, and simulated strategy PnL
must be reported separately.

### 8.3 Validation

- Strict time-ordered train, validation, and test periods.
- Purging and embargo around overlapping event windows.
- Walk-forward parameter selection.
- Separate reporting by symbol, direction, volatility regime, market trend,
  hour of week, and spread regime.
- Parameter-stability surfaces rather than one optimized point.
- Cost stress at base, 1.5 times base, and 2 times base assumptions.
- Latency stress using multiple observation-to-order delays.
- Explicit comparison with simple price-only baselines.

Machine-learning models should not be the first step. The initial objective is
to determine whether a small number of causally plausible features have stable
incremental value.

### 8.4 Minimum Evidence Before Paper Trading

A strategy should not progress merely because aggregate test PnL is positive.
It should demonstrate:

- positive net expectancy in untouched out-of-sample data;
- positive results under at least 1.5 times estimated cost;
- no dependence on one symbol, week, or isolated event;
- similar directional logic for long and short trades, with differences
  documented rather than hidden;
- a broad stable parameter region;
- acceptable drawdown and loss clustering;
- sufficient event frequency to distinguish evidence from a few outliers.

### 8.5 Minimum Evidence Before Live Capital

- Stable raw-data capture and deterministic replay.
- Paper execution using the same signal and order path as live trading.
- Measured signal-to-order latency.
- Measured fill slippage versus the simulator.
- Kill switches for stale data, book desynchronization, disconnects, repeated
  rejects, and daily loss limits.
- Small fixed-risk deployment before any dynamic scaling.

## 9. Recommended New Project Boundary

This research should be developed as a new project rather than added to the
existing `momentum_alpha` repository.

The reasons are architectural rather than cosmetic:

- the existing project evaluates minute and hour strategy states, while this
  research retains tick events, aggregates them into 15-second states, and
  confirms signals across multiple minute windows;
- raw market-data volume and retention requirements are fundamentally larger;
- backtesting needs message replay and latency-aware execution simulation;
- process reliability and performance requirements differ from the current
  leader-rotation system;
- independent repositories prevent experimental short-horizon code from
  increasing operational risk in an existing live system.

The existing repository may retain this document as the transfer brief. The
new project should own all collectors, datasets, research notebooks or scripts,
replay engines, models, paper-trading services, and future live execution.

Suggested project name:

`crypto-momentum-lab`

## 10. Suggested New Project Structure

```text
crypto-momentum-lab/
  README.md
  pyproject.toml
  configs/
  docs/
    research/
    strategy-specs/
    experiment-log/
  src/crypto_momentum_lab/
    capture/
    normalization/
    orderbook/
    features/
    events/
    strategies/
    replay/
    execution/
    risk/
    storage/
  tests/
    unit/
    integration/
    replay/
  data/
    raw/
    normalized/
    features/
  runs/
    event-studies/
    backtests/
    paper/
```

Large datasets should not be committed to Git. Dataset manifests, schemas,
checksums, capture intervals, known gaps, and experiment configurations should
be versioned.

## 11. Recommended Development Order

### Phase 1: Data Integrity

- Capture aggregate trades, best bid/ask, liquidations, one-minute klines, and
  mark price.
- Record exchange and local timestamps.
- Produce and validate deterministic 15-second aggregates.
- Produce gap, lag, reconnect, and message-rate reports.

No strategy conclusion is valid before this phase is reliable.

### Phase 2: Descriptive Event Studies

- Order-flow impulse continuation study.
- Compression breakout acceptance study.
- Liquidation continuation versus reversal study.
- Cross-strategy event-overlap analysis.

This phase should answer whether predictive structure exists before order rules
are optimized.

### Phase 3: Conservative Execution Simulation

- Implement fees, spread, latency, depth, and slippage.
- Establish simple baseline entry and exit rules.
- Reject strategies whose gross signal disappears under realistic execution.

### Phase 4: Walk-Forward Strategy Research

- Evaluate robust feature combinations.
- Freeze parameter-selection procedures.
- Compare independent and hierarchical strategy versions.
- Document every experiment, including negative results.

### Phase 5: Paper Trading

- Run the production data path and execution planner without capital.
- Compare expected and observed fills.
- Reconcile all orders, positions, and stream gaps.

### Phase 6: Small-Risk Live Validation

- Enable one strategy and a small symbol set.
- Use fixed small risk and strict daily loss limits.
- Add strategies or symbols only when live execution matches researched
  assumptions.

## 12. Initial Priority

The recommended research priority is:

1. Volatility-compression breakout momentum.
2. Order-flow impulse momentum.
3. Liquidation-cascade momentum.

Compression breakout comes first because it is less latency-sensitive, easier
to define without a complete order book, and suitable for validating the full
capture-to-paper-trading pipeline.

Order-flow impulse comes second because it likely provides the most reusable
entry and exit confirmation but demands stronger event-time data and execution
modeling.

Liquidation cascade comes third because the public liquidation feed is
incomplete and the targeted periods have the most difficult slippage and
operational conditions.

This priority is an engineering sequence, not a claim that the first strategy
will have the highest eventual return.

## 13. Primary Sources

- Binance USD-M Futures Aggregate Trade Streams:
  <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams>
- Binance USD-M Futures Diff Book Depth Streams:
  <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams>
- Binance USD-M Futures Local Order Book Management:
  <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly>
- Binance USD-M Futures Liquidation Order Streams:
  <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams>
- Binance USD-M Futures All-Market Liquidation Order Streams:
  <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Liquidation-Order-Streams>
- Binance USD-M Futures Commission Rate:
  <https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/User-Commission-Rate>
- Binance USD-M Futures Funding Rate History:
  <https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History>
- High-frequency cryptocurrency return and microstructure study:
  <https://arxiv.org/abs/2009.04200>
- Cryptocurrency limit-order-book forecasting study:
  <https://arxiv.org/abs/2506.05764>

## 14. Explicit Non-Goals

- No assumption that any of the three strategies is profitable before testing.
- No production trading code in the existing `momentum_alpha` project.
- No sub-second latency competition.
- No one-second directional strategy in the initial research.
- No full order-book reconstruction requirement in the initial research.
- No candle-only backtest presented as microstructure evidence.
- No parameter selection on the final test period.
- No use of displayed order-book volume as guaranteed executable liquidity.
- No simultaneous duplicate positions from overlapping strategy triggers.
