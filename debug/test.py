import asyncio
import csv
import json
import math
import os
import time
import requests
import websockets
from collections import deque
from datetime import datetime, timezone

BINANCE_URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1m/btcusdt@kline_5m"
POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_BASE  = "https://gamma-api.polymarket.com"
CSV_PATH    = "btc_updown_obs.csv"

# ── market watcher ────────────────────────────────────────────────────────────
def current_slug():
    """Compute the slug for the currently active 5m market."""
    now = int(time.time())
    next_5m = math.ceil(now / 300) * 300
    return f"btc-updown-5m-{next_5m}"

def parse_clob_ids(s):
    ids = json.loads(s)
    if not isinstance(ids, list) or len(ids) != 2:
        raise ValueError(f"Unexpected clobTokenIds: {s!r}")
    return ids[0], ids[1]

def fetch_active_market():
    slug = current_slug()
    r = requests.get(f"{GAMMA_BASE}/markets/slug/{slug}")
    r.raise_for_status()
    data = r.json()
    yes_id, no_id = parse_clob_ids(data["clobTokenIds"])
    return slug, yes_id, no_id
# ── shared state ──────────────────────────────────────────────────────────────
# ── fetch synchronously before anything starts ────────────────────────────────

print("Fetching active BTC up/down 5m market...")
slug, yes_id, no_id = fetch_active_market()
if not slug:
    raise RuntimeError("No active btc-updown-5m market found — check slug pattern above")

print(f"\nActive market : {slug}")
print(f"YES token     : {yes_id}")
print(f"NO  token     : {no_id}\n")

market = {"slug": slug, "yes_id": yes_id, "no_id": no_id}
closes_1m = deque(maxlen=21)
closes_5m = deque(maxlen=21)
VOL_THRESHOLD = 0.002

market = {"slug": None, "yes_id": None, "no_id": None}
state  = {
    "btc": None, "prev_btc": None, "vol_1m": None,
    "yes_bid": None, "yes_ask": None,
    "no_bid":  None, "no_ask":  None,
    "last_trade": None,
}

market_changed = asyncio.Event()  # signals poly stream to reconnect

def realized_vol(closes):
    if len(closes) < 2:
        return None
    rets = [math.log(closes[i] / closes[i-1]) for i in range(1, len(closes))]
    mean = sum(rets) / len(rets)
    var  = sum((r - mean) ** 2 for r in rets) / len(rets)
    return math.sqrt(var)

# ── csv ───────────────────────────────────────────────────────────────────────

CSV_FIELDS = [
    "ts_utc", "slug", "event_type",
    "btc", "prev_btc", "vol_1m",
    "yes_bid", "yes_ask", "no_bid", "no_ask",
    "last_trade",
]

def init_csv():
    write_header = not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0
    f = open(CSV_PATH, "a", newline="")
    writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
    if write_header:
        writer.writeheader()
    return f, writer

csv_file, csv_writer = init_csv()

def write_row(event_type: str):
    csv_writer.writerow({
        "ts_utc":      datetime.utcnow().isoformat(),
        "slug":        market["slug"],
        "event_type":  event_type,
        "btc":         state["btc"],
        "prev_btc":    state["prev_btc"],
        "vol_1m":      f"{state['vol_1m']:.6f}" if state["vol_1m"] else None,
        "yes_bid":     state["yes_bid"],
        "yes_ask":     state["yes_ask"],
        "no_bid":      state["no_bid"],
        "no_ask":      state["no_ask"],
        "last_trade":  state["last_trade"],
    })
    csv_file.flush()

# ── console print (throttled) ─────────────────────────────────────────────────

_last_print = 0.0

def maybe_print():
    global _last_print
    now = time.time()
    if now - _last_print < 1.0:
        return
    _last_print = now
    vol    = state["vol_1m"]
    regime = "HIGH" if vol and vol > VOL_THRESHOLD else "low "
    curr   = float(state["btc"])      if state["btc"]      else None
    prev   = float(state["prev_btc"]) if state["prev_btc"] else None
    chg    = f"{curr - prev:+.2f}"    if curr and prev     else "?"
    print(
        f"{datetime.utcnow().strftime('%H:%M:%S')} | "
        f"[{market['slug'] or '?'}] | "
        f"BTC {state['btc'] or '?':>10} (prev {state['prev_btc'] or '?':>10}, chg {chg:>8}) | "
        f"vol_1m={f'{vol:.5f}' if vol else '?':>9} [{regime}] | "
        f"YES {state['yes_bid'] or '?':>5}/{state['yes_ask'] or '?':>5} | "
        f"NO  {state['no_bid']  or '?':>5}/{state['no_ask']  or '?':>5} | "
        f"last={state['last_trade'] or '?'}"
    )

# ── market watcher task ───────────────────────────────────────────────────────

async def market_watcher(stop: asyncio.Event, poll_interval=30):
    """Polls Gamma API every poll_interval seconds; signals reconnect on market change."""
    while not stop.is_set():
        try:
            slug, yes_id, no_id = await asyncio.to_thread(fetch_active_market)
            if slug != market["slug"]:
                print(f"Market watcher: new market → {slug}")
                market.update({"slug": slug, "yes_id": yes_id, "no_id": no_id})
                state["yes_bid"] = state["yes_ask"] = None
                state["no_bid"]  = state["no_ask"]  = None
                state["last_trade"] = None
                market_changed.set()
        except Exception as e:
            print(f"Market watcher error: {e}")
        await asyncio.sleep(poll_interval)
# ── binance stream ────────────────────────────────────────────────────────────

async def binance_stream(stop: asyncio.Event):
    while not stop.is_set():
        try:
            async with websockets.connect(BINANCE_URL, ping_interval=20) as ws:
                print("Binance: connected")
                async for raw in ws:
                    msg = json.loads(raw)
                    k   = msg["data"]["k"]
                    state["btc"] = k["c"]
                    if k["x"]:
                        c = float(k["c"])
                        if k["i"] == "1m":
                            state["prev_btc"] = closes_1m[-1] if closes_1m else None
                            closes_1m.append(c)
                            state["vol_1m"] = realized_vol(closes_1m)
                        else:
                            closes_5m.append(c)
                    if stop.is_set():
                        break
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Binance: error {e}, reconnecting...")
            await asyncio.sleep(2)

# ── polymarket stream ─────────────────────────────────────────────────────────

async def ping_loop(ws, stop: asyncio.Event):
    while not stop.is_set():
        await asyncio.sleep(20)
        try:
            await ws.send("PING")
        except Exception:
            break

async def polymarket_stream(stop: asyncio.Event):
    while not stop.is_set():
        # wait until we have a market
        while not market["yes_id"] and not stop.is_set():
            await asyncio.sleep(1)

        market_changed.clear()
        yes_id = market["yes_id"]
        no_id  = market["no_id"]

        try:
            async with websockets.connect(POLY_WS_URL, ping_interval=None) as ws:
                print(f"Polymarket: connected → {market['slug']}")
                await ws.send(json.dumps({
                    "type": "market",
                    "assets_ids": [yes_id, no_id],
                    "auth": {},
                }))
                pinger = asyncio.create_task(ping_loop(ws, stop))
                try:
                    async for raw in ws:
                        # reconnect if market rolled over
                        if market_changed.is_set():
                            print("Polymarket: market changed, reconnecting...")
                            break

                        if raw in ("PING", "PONG"):
                            continue

                        events = json.loads(raw)
                        if not isinstance(events, list):
                            events = [events]

                        for ev in events:
                            etype    = ev.get("event_type")
                            asset_id = ev.get("asset_id")
                            is_yes   = asset_id == yes_id

                            if etype == "book":
                                buys  = ev.get("buys") or ev.get("bids") or []
                                sells = ev.get("sells") or ev.get("asks") or []
                                bid = max((float(l["price"]) for l in buys),  default=None)
                                ask = min((float(l["price"]) for l in sells), default=None)
                                if is_yes:
                                    state["yes_bid"], state["yes_ask"] = bid, ask
                                else:
                                    state["no_bid"],  state["no_ask"]  = bid, ask
                                write_row("book")

                            elif etype == "best_bid_ask":
                                bid = float(ev.get("best_bid") or 0) or None
                                ask = float(ev.get("best_ask") or 0) or None
                                if is_yes:
                                    state["yes_bid"], state["yes_ask"] = bid, ask
                                else:
                                    state["no_bid"],  state["no_ask"]  = bid, ask
                                write_row("best_bid_ask")

                            elif etype == "last_trade_price":
                                side = "YES" if is_yes else "NO"
                                state["last_trade"] = f"{side}@{ev.get('price')}"
                                write_row("last_trade_price")

                        maybe_print()

                        if stop.is_set():
                            break
                finally:
                    pinger.cancel()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Polymarket: error {e}, reconnecting...")
            await asyncio.sleep(2)

# ── run ───────────────────────────────────────────────────────────────────────

async def run(duration_seconds=3600):
    stop = asyncio.Event()
    asyncio.get_event_loop().call_later(duration_seconds, stop.set)
    try:
        await asyncio.gather(
            market_watcher(stop, poll_interval=30),
            binance_stream(stop),
            polymarket_stream(stop),
        )
    finally:
        csv_file.close()
        print(f"Done. Data written to {CSV_PATH}")

asyncio.run(run(3600))
