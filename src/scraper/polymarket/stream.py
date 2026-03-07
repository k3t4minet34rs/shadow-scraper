import asyncio
import json

import websockets

from ..market import MarketState
from ..utils import ts_from_ms
from ..writer import Writer

_POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


async def _ping_loop(ws, stop: asyncio.Event):
    while not stop.is_set():
        await asyncio.sleep(20)
        try:
            await ws.send("PING")
        except Exception:
            break


async def stream(stop: asyncio.Event, market: MarketState, writer: Writer, cfg: dict):
    enable_price_change = cfg.get("price_change", False)

    while not stop.is_set():
        # wait for market to be initialised
        while not market.yes_id and not stop.is_set():
            await asyncio.sleep(1)

        market.changed.clear()
        yes_id = market.yes_id
        no_id  = market.no_id

        try:
            async with websockets.connect(_POLY_WS_URL, ping_interval=None) as ws:
                print(f"[polymarket] connected → {market.slug}")
                await ws.send(json.dumps({
                    "type": "market",
                    "assets_ids": [yes_id, no_id],
                    "auth": {},
                }))

                pinger = asyncio.create_task(_ping_loop(ws, stop))
                try:
                    async for raw in ws:
                        if market.changed.is_set():
                            print("[polymarket] market rolled over, reconnecting...")
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
                            ts_ex    = ts_from_ms(ev.get("timestamp"))
                            slug     = market.slug

                            # ── orderbook snapshot / best bid-ask update ───
                            if etype in ("book", "best_bid_ask"):
                                if etype == "book":
                                    buys  = ev.get("buys") or ev.get("bids") or []
                                    sells = ev.get("sells") or ev.get("asks") or []
                                    bid   = max((float(l["price"]) for l in buys),  default=None)
                                    ask   = min((float(l["price"]) for l in sells), default=None)
                                else:
                                    bid = float(ev.get("best_bid") or 0) or None
                                    ask = float(ev.get("best_ask") or 0) or None

                                source = f"poly_{etype}"
                                if is_yes:
                                    writer.write(source, ts_exchange=ts_ex, slug=slug,
                                                 yes_bid=bid, yes_ask=ask)
                                else:
                                    writer.write(source, ts_exchange=ts_ex, slug=slug,
                                                 no_bid=bid, no_ask=ask)

                            # ── last trade ─────────────────────────────────
                            elif etype == "last_trade_price":
                                side = "YES" if is_yes else "NO"
                                writer.write("poly_last_trade", ts_exchange=ts_ex, slug=slug,
                                             last_trade=f"{side}@{ev.get('price')}")

                            # ── price change (orderbook level update) ──────
                            # payload: price, size, side, best_bid, best_ask, hash
                            # 'price' = the level that changed, NOT a trade price
                            elif etype == "price_change" and enable_price_change:
                                bid = float(ev.get("best_bid") or 0) or None
                                ask = float(ev.get("best_ask") or 0) or None
                                if is_yes:
                                    writer.write("poly_price_change", ts_exchange=ts_ex, slug=slug,
                                                 yes_bid=bid, yes_ask=ask)
                                else:
                                    writer.write("poly_price_change", ts_exchange=ts_ex, slug=slug,
                                                 no_bid=bid, no_ask=ask)

                        if stop.is_set():
                            break
                finally:
                    pinger.cancel()

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[polymarket] error: {e}, reconnecting...")
            await asyncio.sleep(2)
