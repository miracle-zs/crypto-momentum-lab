# Live Trading Context

This context defines how an exchange position is divided into strategy-managed
batches so entry timing and exit timing remain understandable across account
snapshots.

## Language

**持仓批次（Position Batch）**:
在同一方向持仓中，从一次开仓成交开始，到下一张平仓单提交前的连续成交集合。
同一批次的追加开仓会更新该批次的开仓锚点；平仓单提交后发生的开仓成交属于新批次。
_Avoid_: 单笔持仓、每笔成交都是一个批次

**平仓边界（Exit Boundary）**:
一张平仓单提交的时刻；它结束当前持仓批次，并决定该批次后续的平仓时限。
_Avoid_: 平仓成交时刻、平仓单过期时刻

**追加开仓（Add-on Entry）**:
在当前批次尚未跨过平仓边界时增加同方向仓位的开仓成交，仍与原成交属于同一持仓批次。
_Avoid_: 独立开仓、自动新批次
