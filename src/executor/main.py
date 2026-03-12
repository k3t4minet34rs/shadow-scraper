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
from .redeemer import Redeemer
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

        # verbose signal debug — only when book data is present
        if state.yes_ask and state.ofi is not None and secs <= self.sig_cfg["time_filter_s"]:
            fv   = state.compute_fv_yes()   # one call, reused by check_signal below
            edge = (fv - state.yes_ask) if fv is not None else None
            fv_s   = f"{fv:.3f}"   if fv   is not None else "None"
            edge_s = f"{edge:.3f}" if edge is not None else "None"
            logger.debug(
                f"[signal?] slug={slug[-14:]}  secs={secs:.0f}  "
                f"fv={fv_s}  ask={state.yes_ask:.3f}  "
                f"edge={edge_s}  ofi={state.ofi:.3f}  "
                f"vol_1m={state.vol_1m}  btc_ref={state.btc_ref}"
            )

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

        # mark traded immediately — prevents re-firing if order fails
        self.traded.add(slug)

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
            self.pending[slug] = {
                "id":           trade_id,
                "yes_ask":      state.yes_ask,
                "stake":        self.stake,
                "yes_token_id": yes_token,
            }

    async def _check_resolution(self, slug: str, state: TickState) -> None:
        if slug not in self.pending:
            return
        if state.seconds_remaining > 0:
            return   # market still running

        # market has expired — query actual resolution from Polymarket
        trade   = self.pending.pop(slug)
        yes_ask = trade["yes_ask"]
        stake   = trade["stake"]

        if not state.btc_price or not state.btc_ref:
            self.pending[slug] = trade   # no price data yet, retry next tick
            return

        resolved = 1 if state.btc_price > state.btc_ref else 0

        # correct PnL: stake is cash spent, size = stake/yes_ask contracts
        # win:  receive size*$1 = stake/yes_ask, profit = stake*(1-yes_ask)/yes_ask
        # loss: lose full stake
        pnl_g = stake * (1 - yes_ask) / yes_ask if resolved else -stake
        pnl_n = pnl_g - 0.02 * stake   # 2% Polymarket fee

        logger.info(
            f"Resolved: slug={slug[-14:]}  "
            f"{'WIN' if resolved else 'LOSS'}  pnl_net=${pnl_n:.2f}"
        )
        self.store.log_resolution(trade["id"], resolved, pnl_g, pnl_n)
        self.metrics.on_resolution(resolved, pnl_n, yes_ask)

    # ── CSV tail ──────────────────────────────────────────────────────────────

    async def tail_csv(self) -> None:
        csv_path = Path(self.cfg["paths"]["csv"])
        abs_path = csv_path.resolve()
        logger.info(f"CSV path : {abs_path}")
        logger.info(f"Exists   : {abs_path.exists()}")

        if not abs_path.exists():
            logger.error(
                f"CSV not found at {abs_path}. "
                f"Either rsync it from EC2 or update config.yaml paths.csv"
            )
            return

        with open(abs_path, "r") as f:
            headers = f.readline().strip().split(",")
            logger.info(f"Headers  : {headers}")

            # replay recent history (~500 KB) to hydrate state before going live
            f.seek(0, 2)
            size = f.tell()
            logger.info(f"CSV size : {size / 1_000_000:.1f} MB")
            seek_to = max(0, size - 500_000)
            f.seek(seek_to)
            f.readline()   # discard partial line

            logger.info(f"Replaying from byte {seek_to:,} …")
            replay_rows = 0
            for line in f:
                row = parse_row(line, headers)
                slug = row.get("slug", "")
                if slug:
                    if slug not in self.ticks:
                        self.ticks[slug] = TickState(slug=slug)
                    self.ticks[slug].update(row)
                    replay_rows += 1

            logger.info(
                f"Replay done: {replay_rows:,} rows, "
                f"{len(self.ticks)} markets hydrated"
            )

            # log state of each hydrated market
            for slug, state in self.ticks.items():
                logger.info(
                    f"  market={slug[-14:]}  "
                    f"btc_ref={state.btc_ref}  btc_price={state.btc_price}  "
                    f"yes_ask={state.yes_ask}  ofi={state.ofi}  vol_1m={state.vol_1m}"
                )

            logger.info("Tailing new rows… (waiting for scraper to write)")

            # live tail
            ticks_seen = 0
            while True:
                # detect CSV rotation (file was truncated by scraper)
                cur_pos   = f.tell()
                file_size = abs_path.stat().st_size
                if file_size < cur_pos:
                    logger.info(f"CSV rotated (size {file_size} < pos {cur_pos}) — replaying")
                    seek_to = max(0, file_size - 500_000)
                    f.seek(seek_to)
                    f.readline()   # discard partial line
                    for line in f:
                        row = parse_row(line, headers)
                        slug = row.get("slug", "")
                        if slug:
                            if slug not in self.ticks:
                                self.ticks[slug] = TickState(slug=slug)
                            self.ticks[slug].update(row)

                line = f.readline()
                if line:
                    ticks_seen += 1
                    row = parse_row(line, headers)
                    if ticks_seen % 500 == 0:
                        slug = row.get("slug", "")
                        state = self.ticks.get(slug)
                        logger.info(
                            f"[tick {ticks_seen}] slug={slug[-14:]}  "
                            f"yes_ask={state.yes_ask if state else '?'}  "
                            f"ofi={state.ofi if state else '?'}  "
                            f"secs={state.seconds_remaining if state else '?'}"
                        )
                    await self.process_row(row)
                else:
                    await asyncio.sleep(0.5)


# ── entry point ───────────────────────────────────────────────────────────────

async def _run() -> None:
    cfg      = load_config()
    active   = ActiveMarket()
    stop     = asyncio.Event()
    redeemer = Redeemer()

    executor = Executor(cfg, active)
    await redeemer.start()

    try:
        await asyncio.gather(
            active.watch(stop, poll_interval=30),
            executor.tail_csv(),
            executor.metrics.push_loop(),
        )
    finally:
        await redeemer.stop()


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG logging")
    args, _ = parser.parse_known_args()

    logging.basicConfig(
        level   = logging.DEBUG if args.debug else logging.INFO,
        format  = "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt = "%H:%M:%S",
    )
    asyncio.run(_run())
