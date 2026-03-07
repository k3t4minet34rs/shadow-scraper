"""
Coinbase Advanced Trade WebSocket — public ticker channel.
Note: if you receive a 401, the channel requires API key auth;
      set COINBASE_API_KEY / COINBASE_API_SECRET env vars and add JWT signing.
"""
import asyncio
import json

import websockets

from ..market import MarketState
from ..writer import Writer

_URL = "wss://advanced-trade-ws.coinbase.com"
_SUB = {
    "type": "subscribe",
    "product_ids": ["BTC-USD"],
    "channel": "ticker",
}


async def stream(stop: asyncio.Event, market: MarketState, writer: Writer, cfg: dict):
    while not stop.is_set():
        try:
            async with websockets.connect(_URL, ping_interval=20) as ws:
                print("[coinbase] connected")
                await ws.send(json.dumps(_SUB))
                async for raw in ws:
                    if stop.is_set():
                        break
                    msg = json.loads(raw)

                    if msg.get("channel") != "ticker":
                        continue

                    ts_ex = msg.get("timestamp")  # already ISO-8601
                    for event in msg.get("events", []):
                        for ticker in event.get("tickers", []):
                            if ticker.get("product_id") != "BTC-USD":
                                continue
                            price = ticker.get("price")
                            bid   = ticker.get("best_bid")
                            ask   = ticker.get("best_ask")
                            vol   = ticker.get("volume_24_h")
                            writer.write(
                                "coinbase_ticker",
                                ts_exchange=ts_ex,
                                slug=market.slug,
                                btc_price=float(price) if price else None,
                                btc_bid=float(bid)     if bid   else None,
                                btc_ask=float(ask)     if ask   else None,
                                btc_vol=float(vol)     if vol   else None,
                            )

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[coinbase] error: {e}, reconnecting...")
            await asyncio.sleep(2)
