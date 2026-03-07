import asyncio
import json
import time
from typing import Optional

import requests


GAMMA_BASE = "https://gamma-api.polymarket.com"


def current_slug() -> str:
    now = int(time.time())
    return f"btc-updown-5m-{(now // 300) * 300}"


def parse_clob_ids(s: str):
    ids = json.loads(s)
    if not isinstance(ids, list) or len(ids) != 2:
        raise ValueError(f"Unexpected clobTokenIds: {s!r}")
    return ids[0], ids[1]


def fetch_active_market():
    slug = current_slug()
    r = requests.get(f"{GAMMA_BASE}/markets/slug/{slug}", timeout=10)
    r.raise_for_status()
    data = r.json()
    yes_id, no_id = parse_clob_ids(data["clobTokenIds"])
    return slug, yes_id, no_id


class MarketState:
    """
    Shared state for the currently active Polymarket 5m market.
    Streams read slug/yes_id/no_id from here.
    market_watcher() polls Gamma API and calls update() on rollover.
    """

    def __init__(self):
        self.slug:   Optional[str] = None
        self.yes_id: Optional[str] = None
        self.no_id:  Optional[str] = None
        self.changed = asyncio.Event()

    def update(self, slug: str, yes_id: str, no_id: str) -> None:
        self.slug   = slug
        self.yes_id = yes_id
        self.no_id  = no_id
        self.changed.set()

    async def watch(self, stop: asyncio.Event, poll_interval: int = 30) -> None:
        while not stop.is_set():
            try:
                slug, yes_id, no_id = await asyncio.to_thread(fetch_active_market)
                if slug != self.slug:
                    print(f"[market] rollover → {slug}")
                    self.update(slug, yes_id, no_id)
            except Exception as e:
                print(f"[market] poll error: {e}")
            await asyncio.sleep(poll_interval)
