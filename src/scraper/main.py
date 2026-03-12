"""
Usage:
    python -m shadow.scraper               # uses src/scraper/config.yaml
    python -m shadow.scraper path/to.yaml  # custom config
"""
import asyncio
import sys

import yaml

from .market import MarketState, fetch_active_market
from .writer import Writer


async def run(cfg: dict) -> None:
    stop   = asyncio.Event()
    market = MarketState()
    writer = Writer(
        path=cfg["writer"]["path"],
        flush_interval_s=cfg["writer"].get("flush_interval_s", 5.0),
    )

    # ── initialise market state before starting streams ────────────────────────
    print("Fetching active Polymarket market...")
    slug, yes_id, no_id = await asyncio.to_thread(fetch_active_market)
    market.update(slug, yes_id, no_id)
    print(f"  slug   : {slug}")
    print(f"  YES id : {yes_id}")
    print(f"  NO  id : {no_id}\n")

    tasks = [asyncio.create_task(market.watch(stop))]

    # ── exchanges ──────────────────────────────────────────────────────────────
    if cfg.get("binance", {}).get("enabled"):
        from .exchanges import binance
        tasks.append(asyncio.create_task(
            binance.stream(stop, market, writer, cfg["binance"])
        ))

    if cfg.get("bybit", {}).get("enabled"):
        from .exchanges import bybit
        tasks.append(asyncio.create_task(
            bybit.stream(stop, market, writer, cfg["bybit"])
        ))

    if cfg.get("kraken", {}).get("enabled"):
        from .exchanges import kraken
        tasks.append(asyncio.create_task(
            kraken.stream(stop, market, writer, cfg["kraken"])
        ))

    if cfg.get("coinbase", {}).get("enabled"):
        from .exchanges import coinbase
        tasks.append(asyncio.create_task(
            coinbase.stream(stop, market, writer, cfg["coinbase"])
        ))

    if cfg.get("okx", {}).get("enabled"):
        from .exchanges import okx
        tasks.append(asyncio.create_task(
            okx.stream(stop, market, writer, cfg["okx"])
        ))

    # ── polymarket ─────────────────────────────────────────────────────────────
    if cfg.get("polymarket", {}).get("enabled"):
        from .polymarket import stream as poly
        tasks.append(asyncio.create_task(
            poly.stream(stop, market, writer, cfg["polymarket"])
        ))

    # ── CSV rotation (every 5 days) ───────────────────────────────────────────
    async def _rotate_loop():
        while not stop.is_set():
            await asyncio.sleep(5 * 24 * 3600)
            writer.rotate()
            print(f"CSV rotated")

    tasks.append(asyncio.create_task(_rotate_loop()))

    # ── optional hard stop ─────────────────────────────────────────────────────
    duration = cfg.get("duration_seconds")
    if duration:
        async def _stopper():
            await asyncio.sleep(duration)
            stop.set()
        tasks.append(asyncio.create_task(_stopper()))

    print(f"Streaming to {writer.path} — Ctrl-C to stop\n")
    try:
        await asyncio.gather(*tasks)
    finally:
        stop.set()
        writer.close()
        print(f"\nDone. Data written to {writer.path}")


def main() -> None:
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "src/scraper/config.yaml"
    with open(cfg_path) as f:
        cfg = yaml.safe_load(f)
    try:
        asyncio.run(run(cfg))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
