"""
Fair value computation — P(bucket) from ECMWF ensemble members.

Each member gives one possible max temperature. The fraction of members
falling into a bucket is our probability estimate for that bucket.

Rounding: Wunderground reports whole °C. We round each member before bucketing.
"""
import logging
from dataclasses import dataclass

import numpy as np

logger = logging.getLogger(__name__)

EDGE_THRESHOLD = 0.15   # minimum edge to consider a trade


@dataclass
class BucketFV:
    market_id:    str
    bucket_label: str
    bucket_low:   float | None
    bucket_high:  float | None
    bucket_type:  str
    fv:           float          # our probability
    market_price: float          # current Polymarket YES ask
    edge:         float          # fv - market_price


def _member_in_bucket(temp: float, bucket_type: str,
                      low: float | None, high: float | None) -> bool:
    """Check if a (rounded) temperature falls in a bucket."""
    t = round(temp)   # Wunderground reports whole °C
    if bucket_type == "exact":
        return t == int(low)
    if bucket_type == "below":
        return t <= int(high)
    if bucket_type == "above":
        return t >= int(low)
    if bucket_type == "range":
        return int(low) <= t <= int(high)
    return False


def compute_fv(members: list[float], markets: list[dict],
               prices: dict[str, float]) -> list[BucketFV]:
    """
    Compute fair value for each bucket market given ensemble members.

    Args:
        members:  list of max temps from ECMWF ensemble (e.g. 51 values)
        markets:  list of market rows from DB (each has bucket fields + market_id)
        prices:   {market_id: yes_ask} from Polymarket

    Returns:
        list of BucketFV sorted by edge descending
    """
    n = len(members)
    if n == 0:
        return []

    results = []
    for mkt in markets:
        mid   = mkt["market_id"]
        btype = mkt["bucket_type"]
        low   = mkt["bucket_low"]
        high  = mkt["bucket_high"]
        label = mkt["bucket_label"]

        if btype == "unknown":
            continue

        hits = sum(1 for m in members
                   if _member_in_bucket(m, btype, low, high))
        fv   = hits / n

        mkt_price = prices.get(mid)
        if mkt_price is None:
            logger.debug(f"No price for market {mid} ({label}), skipping")
            continue

        results.append(BucketFV(
            market_id    = mid,
            bucket_label = label,
            bucket_low   = low,
            bucket_high  = high,
            bucket_type  = btype,
            fv           = fv,
            market_price = mkt_price,
            edge         = fv - mkt_price,
        ))

    results.sort(key=lambda x: x.edge, reverse=True)
    return results


def best_trades(bucket_fvs: list[BucketFV],
                threshold: float = EDGE_THRESHOLD) -> list[BucketFV]:
    """Return buckets with positive edge above threshold."""
    return [b for b in bucket_fvs if b.edge >= threshold]


def print_edge_table(city: str, target_date: str,
                     bucket_fvs: list[BucketFV],
                     members: list[float]):
    """Pretty-print the edge table for a given event."""
    p50 = float(np.median(members))
    print(f"\n{'─'*65}")
    print(f"  {city}  {target_date}   ensemble p50={p50:.1f}°C  n={len(members)}")
    print(f"{'─'*65}")
    print(f"  {'Bucket':<18} {'Model':>6} {'Market':>7} {'Edge':>7}  {'Signal'}")
    print(f"{'─'*65}")
    for b in bucket_fvs:
        flag = "  ◄ BUY" if b.edge >= EDGE_THRESHOLD else ""
        print(f"  {b.bucket_label:<18} {b.fv:>5.1%}  {b.market_price:>6.1%}  "
              f"{b.edge:>+6.1%}{flag}")
    print(f"{'─'*65}")
