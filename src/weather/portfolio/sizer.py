"""
Position sizing — fixed stake for now, Kelly later.

Architecture:
  - daily_budget:  total USDC available today ÷ active events
  - per_event_max: cap total exposure per event
  - decide_trades: returns incremental stakes given current position
"""
import logging
from dataclasses import dataclass

from ..pricer.fv import BucketFV

logger = logging.getLogger(__name__)

# ── config ────────────────────────────────────────────────────────────────────

FIXED_STAKE_PER_TRADE = 1.0    # USDC — CLOB API minimum (~$1)
MAX_EXPOSURE_PER_EVENT = 10.0  # USDC — never more than this on one city/day
KELLY_FRACTION = 0.25          # quarter-Kelly (conservative)
EDGE_THRESHOLD = 0.15         # minimum edge to trade


@dataclass
class TradeInstruction:
    market_id:    str
    bucket_label: str
    side:         str    # "YES"
    price:        float
    stake_usdc:   float
    fv:           float
    edge:         float


def kelly_stake(fv: float, price: float, budget: float) -> float:
    """
    Quarter-Kelly stake for a binary bet.
    f* = (fv - price) / (1 - price)   [fraction of budget]
    """
    if price >= 1.0 or fv <= price:
        return 0.0
    f_full = (fv - price) / (1 - price)
    stake  = KELLY_FRACTION * f_full * budget
    return max(0.0, stake)


def decide_trades(
    bucket_fvs:   list[BucketFV],
    position:     dict[str, float],   # {market_id: usdc_already_held}
    total_usdc:   float,
    active_events: int,
) -> list[TradeInstruction]:
    """
    Given current forecast edges and existing positions, return
    the incremental trades to place.

    Logic:
      1. Filter to buckets with positive edge
      2. Compute target stake per bucket (Kelly-weighted within budget)
      3. Subtract what's already held → incremental trade
      4. Apply min order size and per-event cap
    """
    candidates = [b for b in bucket_fvs if b.edge >= EDGE_THRESHOLD]
    if not candidates:
        return []

    # Budget per event: cap at MAX_EXPOSURE_PER_EVENT.
    # We don't divide by active_events — most events won't have tradeable edge,
    # so spreading total_usdc across all 75 events makes each allocation too
    # small to survive Kelly discounting (falls below $1 CLOB minimum).
    per_event_budget = MAX_EXPOSURE_PER_EVENT

    # already deployed in this event
    total_held = sum(position.values())
    remaining  = max(0.0, per_event_budget - total_held)

    if remaining < FIXED_STAKE_PER_TRADE:
        logger.debug("No remaining budget for this event")
        return []

    # weight candidates by fv (concentrate on most probable bucket with edge)
    total_fv   = sum(b.fv for b in candidates)
    trades     = []

    for b in candidates:
        weight     = b.fv / total_fv if total_fv > 0 else 1 / len(candidates)
        target     = kelly_stake(b.fv, b.market_price, weight * remaining)
        already    = position.get(b.market_id, 0.0)
        incremental = max(0.0, target - already)

        if incremental < FIXED_STAKE_PER_TRADE:
            continue

        trades.append(TradeInstruction(
            market_id    = b.market_id,
            bucket_label = b.bucket_label,
            side         = "YES",
            price        = b.market_price,
            stake_usdc   = round(incremental, 2),
            fv           = b.fv,
            edge         = b.edge,
        ))

    return trades
