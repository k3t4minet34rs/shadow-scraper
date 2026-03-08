"""
shadow-executor  —  real-time signal detection + Polymarket order placement.

Flow:
  1. Load ~500 KB of recent CSV history to forward-fill current market state.
  2. Tail btc_obs.csv for new rows from the scraper.
  3. On each tick: update TickState, check signal conditions.
  4. Signal fires → place BUY YES order via Polymarket CLOB.
  5. One trade per market window (slug). No re-entry.
  6. On market expiry: log resolution + PnL, emit metrics.
  7. Metrics pushed to Victoria Metrics every 10 s.
"""
import asyncio
import csv as _csv
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv

from scraper.market import MarketState as ActiveMarket, fetch_active_market
from .metrics import Metrics
from .order import OrderPlacer
from .signal import TickState
from .store import TradeStore

load_dotenv()
logger = logging.getLogger(__name__)


# ── helpers ───────────────────────────────────────────────────────────────────

def load_config() -> dict:
    path = Path(__file__).parent / "config.yaml"
    with open(path) as f:
        return yaml.safe_load(f)


def seconds_remaining(slug: str, ts_dt: datetime) -> float:
    """slug = btc-updown-5m-<start_unix>  →  market ends at start_unix + 300."""
    try:
        start_unix = int(slug.rsplit("-", 1)[-1])
        end_dt     = datetime.fromtimestamp(start_unix + 300, tz=timezone.utc)
        return (end_dt - ts_dt).total_seconds()
    except Exception:
        return -1.0


def parse_row(line: str, headers: list[str]) -> dict:
    try:
        values = next(_csv.reader([line.strip()]))
        return dict(zip(headers, values))
    except Exception:
        return {}


# ── executor ──────────────────────────────────────────────────────────────────

class Executor:
    def __init__(self, cfg: dict, active: ActiveMarket):
        self.cfg    = cfg
        self.active = active            # live slug + yes_id from scraper.market
        self.sig_cfg = cfg["signal"]
        self.stake   = cfg["order"]["stake_usdc"]

        self.ticks:   dict[str, TickState] = {}   # slug → forward-filled state
        self.traded:  set[str]             = set() # slugs we've already entered
        self.pending: dict[str, dict]      = {}    # slug → {trade_id, yes_ask}

        self.placer  = OrderPlacer(self.stake)
        self.store   = TradeStore(cfg["paths"]["db"])
        self.metrics = Metrics(
            cfg["metrics"]["url"],
            cfg["metrics"]["push_interval_s"],
        )

        # re-register any open trades from a previous run
        for t in self.store.open_trades():
            self.traded.add(t["slug"])
            self.pending[t["slug"]] = t
            logger.info(f"Resuming open trade: slug={t['slug']} id={t['id']}")

    # ── tick processing ───────────────────────────────────────────────────────

    async def process_row(self, row: dict) -> None:
        slug = row.get("slug", "").strip()
        if not slug:
            return

        ts_str = row.get("ts_local", "")
        try:
            ts_dt = datetime.fromisoformat(ts_str)
        except Exception:
            return

        secs = seconds_remaining(slug, ts_dt)
        if secs <= 0 or secs > 300:
            return

        # forward-fill tick state
        if slug not in self.ticks:
            self.ticks[slug] = TickState(slug=slug)
        state = self.ticks[slug]
        state.seconds_remaining = secs
        state.update(row)

        # emit per-tick metrics (throttled inside Victoria Metrics)
        self.metrics.on_tick(slug, state.yes_ask, state.ofi, secs)

        # check resolution of pending trades
        await self._check_resolution(slug, state)

        # check signal (skip if already traded this market)
        if slug in self.traded:
            return
        if not state.check_signal(self.sig_cfg):
            return

        await self._fire_order(slug, state)

    async def _fire_order(self, slug: str, state: TickState) -> None:
        info = state.signal_info()
        logger.info(
            f"SIGNAL  slug={slug[-14:]}  edge={info['edge_yes']:.1%}  "
            f"ask={info['yes_ask']:.3f}  ofi={info['ofi']:.3f}  "
            f"secs={info['seconds_remaining']:.0f}"
        )
        self.metrics.on_signal(
            info["edge_yes"], info["yes_ask"],
            info["ofi"],      info["seconds_remaining"],
        )

        # get YES token id from active market watcher
        yes_token = self.active.yes_id if self.active.slug == slug else None
        if not yes_token:
            logger.warning(f"No yes_token_id for {slug} — skipping")
            return

        t0     = time.monotonic()
        order  = await self.placer.buy_yes(yes_token, state.yes_ask)
        lat_ms = (time.monotonic() - t0) * 1000

        self.metrics.on_order(order["success"], order["price"], order.get("size", 0), lat_ms)

        trade_id = self.store.log_entry(slug, yes_token, info, order, self.stake)

        if order["success"]:
            self.traded.add(slug)
            self.pending[slug] = {
                "id":       trade_id,
                "yes_ask":  state.yes_ask,
                "stake":    self.stake,
            }

    async def _check_resolution(self, slug: str, state: TickState) -> None:
        if slug not in self.pending:
            return
        if state.seconds_remaining > 3:
            return   # market still running
        if not state.btc_price or not state.btc_ref:
            return

        trade    = self.pending.pop(slug)
        resolved = 1 if state.btc_price > state.btc_ref else 0
        yes_ask  = trade["yes_ask"]
        stake    = trade["stake"]
        pnl_g    = ((1 - yes_ask) if resolved else -yes_ask) * stake
        pnl_n    = pnl_g - 0.02 * stake   # 2% Polymarket fee

        self.store.log_resolution(trade["id"], resolved, pnl_g, pnl_n)
        self.metrics.on_resolution(resolved, pnl_n, yes_ask)

    # ── CSV tail ──────────────────────────────────────────────────────────────

    async def tail_csv(self) -> None:
        csv_path = Path(self.cfg["paths"]["csv"])
        logger.info(f"Opening {csv_path}")

        with open(csv_path, "r") as f:
            headers = f.readline().strip().split(",")

            # replay recent history (~500 KB) to hydrate state before going live
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 500_000))
            f.readline()   # discard partial line

            logger.info("Replaying recent history…")
            for line in f:
                row = parse_row(line, headers)
                slug = row.get("slug", "")
                if slug:
                    if slug not in self.ticks:
                        self.ticks[slug] = TickState(slug=slug)
                    self.ticks[slug].update(row)

            logger.info(
                f"Context loaded for {len(self.ticks)} markets. "
                f"Tailing new rows…"
            )

            # live tail
            while True:
                line = f.readline()
                if line:
                    row = parse_row(line, headers)
                    await self.process_row(row)
                else:
                    await asyncio.sleep(0.5)


# ── entry point ───────────────────────────────────────────────────────────────

async def _run() -> None:
    cfg    = load_config()
    active = ActiveMarket()
    stop   = asyncio.Event()

    executor = Executor(cfg, active)

    await asyncio.gather(
        active.watch(stop, poll_interval=30),
        executor.tail_csv(),
        executor.metrics.push_loop(),
    )


def main() -> None:
    logging.basicConfig(
        level   = logging.INFO,
        format  = "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt = "%H:%M:%S",
    )
    asyncio.run(_run())
