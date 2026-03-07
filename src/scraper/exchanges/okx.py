"""
OKX V5 public WebSocket — tickers channel.
Fires on every price change (sub-second during active trading).
"""
import asyncio
import json

import websockets

from ..market import MarketState
from ..utils import ts_from_ms
from ..writer import Writer

_URL = "wss://ws.okx.com:8443/ws/v5/public"
_SUB = {"op": "subscribe", "args": [{"channel": "tickers", "instId": "BTC-USDT"}]}


async def stream(stop: asyncio.Event, market: MarketState, writer: Writer, cfg: dict):
    while not stop.is_set():
        try:
            async with websockets.connect(_URL, ping_interval=20) as ws:
                print("[okx] connected")
                await ws.send(json.dumps(_SUB))
                async for raw in ws:
                    if stop.is_set():
                        break
                    msg = json.loads(raw)

                    if msg.get("arg", {}).get("channel") != "tickers":
                        continue

                    for ticker in msg.get("data", []):
                        last  = ticker.get("last")
                        bid   = ticker.get("bidPx")
                        ask   = ticker.get("askPx")
                        vol   = ticker.get("vol24h")
                        writer.write(
                            "okx_ticker",
                            ts_exchange=ts_from_ms(ticker.get("ts")),
                            slug=market.slug,
                            btc_price=float(last) if last else None,
                            btc_bid=float(bid)    if bid  else None,
                            btc_ask=float(ask)    if ask  else None,
                            btc_vol=float(vol)    if vol  else None,
                        )

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[okx] error: {e}, reconnecting...")
            await asyncio.sleep(2)
