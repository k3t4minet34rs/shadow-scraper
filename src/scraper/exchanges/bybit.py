"""
Bybit V5 spot public ticker.
Sends snapshot on subscribe, then delta updates (only changed fields).
Both types write to btc_price / btc_bid / btc_ask.
"""
import asyncio
import json

import websockets

from ..market import MarketState
from ..utils import ts_from_ms
from ..writer import Writer

_URL = "wss://stream.bybit.com/v5/public/spot"
_SUB = {"op": "subscribe", "args": ["tickers.BTCUSDT"]}


async def stream(stop: asyncio.Event, market: MarketState, writer: Writer, cfg: dict):
    while not stop.is_set():
        try:
            async with websockets.connect(_URL, ping_interval=20) as ws:
                print("[bybit] connected")
                await ws.send(json.dumps(_SUB))
                async for raw in ws:
                    if stop.is_set():
                        break
                    msg = json.loads(raw)

                    # ignore op confirmations and heartbeats
                    if msg.get("op") or msg.get("ret_msg"):
                        continue

                    if msg.get("topic") == "tickers.BTCUSDT":
                        data = msg.get("data", {})
                        # delta messages only include changed fields — use .get() safely
                        price = data.get("lastPrice")
                        bid   = data.get("bid1Price")
                        ask   = data.get("ask1Price")
                        vol   = data.get("volume24h")
                        writer.write(
                            "bybit_ticker",
                            ts_exchange=ts_from_ms(msg.get("ts")),
                            slug=market.slug,
                            btc_price=float(price) if price else None,
                            btc_bid=float(bid)     if bid   else None,
                            btc_ask=float(ask)     if ask   else None,
                            btc_vol=float(vol)     if vol   else None,
                        )

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[bybit] error: {e}, reconnecting...")
            await asyncio.sleep(2)
