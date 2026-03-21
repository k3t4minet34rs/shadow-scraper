"""
weather-bot main loop.

Run modes:
  weather-cycle           — full cycle (discover + forecast + price + trade)
  weather-cycle --dry-run — print edge tables, no orders placed
  weather-cycle --discover-only — just sync events to DB
"""
import argparse
import logging
from datetime import datetime, timezone, date as date_type

from .calibration.wunderground import fetch_daily_high
from .discovery.polymarket import PolymarketDiscovery, parse_event
from .executor.order import WeatherOrderPlacer
from .forecast.openmeteo import fetch_ensemble, percentiles
from .pricer.fv import compute_fv, print_edge_table
from .portfolio.sizer import decide_trades
from .store.db import DB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_BUDGET_USDC  = 200.0   # override with --budget
CUTOFF_LOCAL_HOUR   = 16       # skip today's events once local time passes 4pm
MAX_DAYS_AHEAD      = 1        # only trade today (0) and tomorrow (1)


# ── cycle steps ───────────────────────────────────────────────────────────────

def step_discover(db: DB, poly: PolymarketDiscovery):
    """Fetch active events and sync to DB."""
    raw_events = poly.fetch_events()
    new = 0
    for raw in raw_events:
        ev, mkts = parse_event(raw)
        if not ev:
            continue
        if db.upsert_event(ev):
            new += 1
            logger.info(f"New event: {ev['city']} {ev['target_date']} "
                        f"({ev['station_code']})")
        for mkt in mkts:
            db.upsert_market(mkt)
    logger.info(f"Discovery: {len(raw_events)} events fetched, {new} new")


def step_forecast_and_price(db: DB, poly: PolymarketDiscovery,
                             placer: WeatherOrderPlacer | None = None,
                             dry_run: bool = False,
                             budget: float = DEFAULT_BUDGET_USDC):
    """For each active event: forecast → price → decide → execute."""
    active_events = db.get_active_events()
    logger.info(f"Processing {len(active_events)} active events")

    for event in active_events:
        event_id    = event["event_id"]
        city        = event["city"]
        target_date = event["target_date"]
        lat, lon    = event["lat"], event["lon"]

        logger.info(f"── {city} {target_date} ──")

        # Skip events beyond today + tomorrow — forecast accuracy beyond
        # 1 day ahead isn't reliable enough to trade on.
        today_utc   = datetime.now(timezone.utc).date()
        event_date  = date_type.fromisoformat(target_date)
        days_ahead  = (event_date - today_utc).days
        if days_ahead > MAX_DAYS_AHEAD:
            logger.debug(f"  Skipping — {days_ahead} days ahead (max {MAX_DAYS_AHEAD})")
            continue

        # Skip today's events once local time passes 4pm — market is
        # near-resolved and prices reflect actual observed temperature.
        # Estimate local hour from longitude: UTC_offset ≈ lon / 15
        if event_date == today_utc:
            utc_now_hour  = datetime.now(timezone.utc).hour + datetime.now(timezone.utc).minute / 60
            est_local_hour = (utc_now_hour + lon / 15) % 24
            if est_local_hour >= CUTOFF_LOCAL_HOUR:
                logger.info(f"  Skipping — local time ~{est_local_hour:.1f}h, past {CUTOFF_LOCAL_HOUR}:00 cutoff")
                continue

        # 1. Forecast
        members = fetch_ensemble(lat, lon, target_date)
        if not members:
            logger.warning(f"No ensemble data for {city} {target_date}, skipping")
            continue

        p10, p50, p90 = percentiles(members)
        forecast_id = db.save_forecast(event_id, "icon_seamless",
                                       members, p10, p50, p90)
        db.set_event_status(event_id, "FORECASTED")

        # 2. Fetch current Polymarket prices
        markets = db.get_markets(event_id)
        if not markets:
            logger.warning(f"No markets found for event {event_id}")
            continue

        market_ids   = [m["market_id"] for m in markets]
        prices       = poly.fetch_market_prices(market_ids)
        markets_list = [dict(m) for m in markets]
        bucket_fvs   = compute_fv(members, markets_list, prices)

        # Log predictions
        for b in bucket_fvs:
            db.save_prediction(b.market_id, forecast_id,
                               b.fv, b.market_price)

        # Print edge table always
        print_edge_table(city, target_date, bucket_fvs, members)

        if dry_run:
            continue

        # 3. Decide trades
        position = db.get_position(event_id)
        trades   = decide_trades(bucket_fvs, position,
                                 budget, len(active_events))

        if not trades:
            logger.info(f"No trades for {city} {target_date}")
            continue

        # 4. Execute
        # Build a lookup: market_id → yes_token_id + neg_risk flag
        mkt_by_id = {m["market_id"]: m for m in markets_list}
        neg_risk  = bool(event["neg_risk_id"])

        for t in trades:
            logger.info(
                f"  TRADE  {city} {target_date}  {t.bucket_label}  "
                f"YES @ {t.price:.2f}  stake=${t.stake_usdc:.2f}  "
                f"edge={t.edge:+.1%}  fv={t.fv:.1%}"
            )
            if placer is None:
                continue   # dry-run or no executor

            mkt_row     = mkt_by_id.get(t.market_id, {})
            yes_token   = mkt_row.get("yes_token_id")
            if not yes_token:
                logger.warning(f"No yes_token_id for {t.bucket_label}, skipping")
                continue

            result = placer.place(t, yes_token, neg_risk=neg_risk)
            db.save_order(
                market_id = t.market_id,
                side      = "YES",
                price     = t.price,
                stake     = t.stake_usdc,
                size      = result.get("size"),
                success   = result["success"],
                order_id  = result.get("order_id"),
                error     = result.get("error"),
            )
            if result["success"]:
                db.set_event_status(event_id, "POSITIONED")


def step_resolve(db: DB):
    """
    Fetch Wunderground daily highs for all expired unresolved events,
    determine which bucket won, and save resolutions + bias log.
    """
    from .pricer.fv import _member_in_bucket

    expired = db.get_expired_unresolved()
    if not expired:
        return

    logger.info(f"Resolving {len(expired)} expired events via Wunderground...")

    for ev in expired:
        city        = ev["city"]
        station     = ev["station_code"]
        target_date = ev["target_date"]
        event_id    = ev["event_id"]

        # 1. Fetch actual observed daily high from Wunderground
        observed = fetch_daily_high(station, target_date)
        if observed is None:
            logger.warning(f"  Could not fetch Wunderground data for "
                           f"{city} {station} {target_date} — will retry next cycle")
            continue

        observed_rounded = round(observed)  # Polymarket resolves on whole °C

        # 2. Resolve each bucket market
        markets = db.get_markets(event_id)
        resolved_count = 0
        winner_label   = None

        for mkt in markets:
            market_id = mkt["market_id"]
            if db.is_resolved(market_id):
                continue

            resolved_yes = _member_in_bucket(
                observed_rounded,
                mkt["bucket_type"],
                mkt["bucket_low"],
                mkt["bucket_high"],
            )
            db.save_resolution(market_id, resolved_yes, actual_temp=observed)
            resolved_count += 1
            if resolved_yes:
                winner_label = mkt["bucket_label"]

        # 3. Mark event resolved
        db.set_event_status(event_id, "RESOLVED")

        logger.info(
            f"  RESOLVED  {city} {target_date}  observed={observed}°C"
            f"  winner={winner_label or '?'}  ({resolved_count} buckets)"
        )

        # 4. Compare against latest forecast p50 for bias tracking
        latest = db.latest_forecast(event_id)
        if latest:
            bias = observed - latest["p50"]
            logger.info(
                f"  BIAS      {city} {station}  "
                f"p50={latest['p50']:.1f}°C  obs={observed:.1f}°C  "
                f"bias={bias:+.1f}°C"
            )


# ── entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Weather bot cycle")
    parser.add_argument("--dry-run",       action="store_true",
                        help="Print edge tables, place no orders")
    parser.add_argument("--discover-only", action="store_true",
                        help="Only sync events to DB, skip forecast")
    parser.add_argument("--db",     default="data/weather.db",
                        help="Path to SQLite DB")
    parser.add_argument("--budget", type=float, default=DEFAULT_BUDGET_USDC,
                        help="Available USDC balance for sizing (default 200)")
    args = parser.parse_args()

    db     = DB(args.db)
    poly   = PolymarketDiscovery()
    placer = None

    if not args.dry_run and not args.discover_only:
        try:
            placer = WeatherOrderPlacer()
        except Exception as e:
            logger.error(f"Could not init order placer (no keys?): {e}")
            logger.info("Falling back to dry-run mode")

    try:
        step_discover(db, poly)
        if not args.discover_only:
            step_forecast_and_price(db, poly, placer=placer,
                                    dry_run=args.dry_run,
                                    budget=args.budget)
            step_resolve(db)
    finally:
        poly.close()

    logger.info("Cycle complete.")


if __name__ == "__main__":
    main()
