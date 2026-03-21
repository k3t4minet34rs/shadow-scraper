"""
Real-time forward-fill and signal detection.

Mirrors the DuckDB forward-fill logic from the analysis notebook,
but processes one CSV row at a time as it arrives from the scraper.
"""
import logging
import math
from dataclasses import dataclass, field
from typing import Optional

from scipy.stats import norm

logger = logging.getLogger(__name__)

MINUTES_PER_YEAR   = 365 * 24 * 60
FALLBACK_VOL_1M    = 0.0012   # typical BTC 1-min realized vol (used before first kline arrives)


@dataclass
class TickState:
    """
    Forward-filled state for one market window.
    Call update() for each new CSV row belonging to this slug.
    """
    slug: str

    # btc_ref = FIRST kline_prev_close seen (market opening reference price)
    btc_ref:    Optional[float] = None
    btc_price:  Optional[float] = None   # latest aggTrade or kline_close
    prev_close: Optional[float] = None   # latest kline_prev_close (for drift)
    vol_1m:     Optional[float] = None   # realized vol from rolling 1m returns
    ofi:        Optional[float] = None   # (bid_vol - ask_vol) / total
    mark_price: Optional[float] = None

    yes_bid: Optional[float] = None
    yes_ask: Optional[float] = None
    no_bid:  Optional[float] = None
    no_ask:  Optional[float] = None

    seconds_remaining: Optional[float] = None

    def update(self, row: dict) -> None:
        """Forward-fill: only overwrite non-empty fields."""
        def _f(key: str) -> Optional[float]:
            v = row.get(key, "")
            try:
                return float(v) if v != "" else None
            except (ValueError, TypeError):
                return None

        # current BTC price: prefer aggTrade (btc_price), fall back to kline_close
        bp = _f("btc_price") or _f("kline_close")
        if bp is not None:
            self.btc_price = bp

        kpc = _f("kline_prev_close")
        if kpc is not None:
            if self.btc_ref is None:
                self.btc_ref = kpc      # FIRST value anchors the reference
            self.prev_close = kpc       # LAST value used for drift

        v = _f("kline_vol_1m")
        if v is not None:
            self.vol_1m = v

        o = _f("ofi")
        if o is not None:
            self.ofi = o

        mp = _f("mark_price")
        if mp is not None:
            self.mark_price = mp

        yb = _f("yes_bid")
        if yb is not None:
            self.yes_bid = yb

        ya = _f("yes_ask")
        if ya is not None:
            self.yes_ask = ya

        nb = _f("no_bid")
        if nb is not None:
            self.no_bid = nb

        na = _f("no_ask")
        if na is not None:
            self.no_ask = na

    # ── fair value ─────────────────────────────────────────────────────────────
    def compute_fv_yes(self, debug: bool = False) -> Optional[float]:
        if not self.btc_price or not self.btc_ref or not self.seconds_remaining:
            return None
        if self.seconds_remaining <= 0:
            return None

        T         = self.seconds_remaining / (365 * 24 * 3600)
        vol_used  = self.vol_1m or FALLBACK_VOL_1M
        sigma     = vol_used * math.sqrt(MINUTES_PER_YEAR)
        log_ret   = math.log(self.btc_price / self.btc_ref)

        drift = 0.0
        if self.prev_close and self.prev_close > 0:
            drift = math.log(self.btc_price / self.prev_close) * MINUTES_PER_YEAR

        sigma_sqrt_T = sigma * math.sqrt(T)
        drift_term   = (drift - 0.5 * sigma ** 2) * T
        d2           = (log_ret + drift_term) / sigma_sqrt_T
        fv           = float(norm.cdf(d2))

        logger.debug(
            f"[fv_calc] secs={self.seconds_remaining:.1f}  "
            f"btc={self.btc_price:.2f}  ref={self.btc_ref:.2f}  "
            f"log_ret={log_ret:.6f}  vol={'fallback' if self.vol_1m is None else f'{self.vol_1m:.6f}'}  "
            f"sigma={sigma:.6f}  drift={drift:.4f}  "
            f"drift_term={drift_term:.6f}  sigma_sqrt_T={sigma_sqrt_T:.6f}  "
            f"d2={d2:.4f}  fv={fv:.4f}"
        )
        return fv

    # ── signal check ───────────────────────────────────────────────────────────
    def check_signal(self, cfg: dict) -> bool:
        if self.yes_ask is None or self.ofi is None or self.seconds_remaining is None:
            return False

        fv = self.compute_fv_yes()
        if fv is None:
            return False

        edge = fv - self.yes_ask
        return (
            edge                  >= cfg["edge_thresh"]   and
            self.ofi              >= cfg["ofi_min"]       and
            self.yes_ask          >= cfg.get("ask_min", 0.0) and
            self.yes_ask          <  cfg["ask_max"]       and
            self.seconds_remaining <= cfg["time_filter_s"] and
            self.seconds_remaining >= cfg["min_secs_left"]
        )

    def signal_info(self) -> dict:
        fv   = self.compute_fv_yes()
        edge = (fv - self.yes_ask) if (fv is not None and self.yes_ask) else None
        return {
            "fv_yes":            fv,
            "edge_yes":          edge,
            "yes_ask":           self.yes_ask,
            "yes_bid":           self.yes_bid,
            "ofi":               self.ofi,
            "mark_price":        self.mark_price,
            "seconds_remaining": self.seconds_remaining,
        }
