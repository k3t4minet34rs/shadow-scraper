"""
Kraken V1 public WebSocket ticker.
Ticker updates arrive as arrays: [channelID, data, "ticker", "XBT/USD"]
No exchange timestamp in v1 ticker — ts_exchange will be None.
"""
import asyncio
import json

import websockets

from ..market import MarketState
from ..writer import Writer

_URL = "wss://ws.kraken.com"
_SUB = {
    "event": "subscribe",
    "pair": ["XBT/USD"],
    "subscription": {"name": "ticker"},
}


async def stream(stop: asyncio.Event, market: MarketState, writer: Writer, cfg: dict):
    while not stop.is_set():
        try:
            async with websockets.connect(_URL, ping_interval=20) as ws:
                print("[kraken] connected")
                await ws.send(json.dumps(_SUB))
                async for raw in ws:
                    if stop.is_set():
                        break
                    msg = json.loads(raw)

                    # ticker update = [channelID, {data}, "ticker", "XBT/USD"]
                    if (
                        isinstance(msg, list)
                        and len(msg) == 4
                        and msg[2] == "ticker"
                    ):
                        data = msg[1]
                        bid  = float(data["b"][0]) if "b" in data else None
                        ask  = float(data["a"][0]) if "a" in data else None
                        last = float(data["c"][0]) if "c" in data else None
                        writer.write(
                            "kraken_ticker",
                            ts_exchange=None,   # v1 ticker has no server timestamp
                            slug=market.slug,
                            btc_price=last,
                            btc_bid=bid,
                            btc_ask=ask,
                        )

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[kraken] error: {e}, reconnecting...")
            await asyncio.sleep(2)
