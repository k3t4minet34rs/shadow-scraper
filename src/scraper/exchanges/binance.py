import asyncio
import json
import math
from collections import deque
from typing import Optional

import websockets

from ..market import MarketState
from ..utils import ts_from_ms
from ..writer import Writer

_SPOT_BASE    = "wss://stream.binance.com:9443/stream?streams="
_FUTURES_URL  = "wss://fstream.binance.com/ws/btcusdt@markPrice@1s"


def _realized_vol(closes: deque) -> Optional[float]:
    if len(closes) < 2:
        return None
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
    mean = sum(rets) / len(rets)
    var  = sum((r - mean) ** 2 for r in rets) / len(rets)
    return math.sqrt(var)


async def _spot(stop: asyncio.Event, market: MarketState, writer: Writer, cfg: dict):
    streams = ["btcusdt@kline_1m", "btcusdt@kline_5m"]
    if cfg.get("aggtrade"):
        streams.append("btcusdt@aggTrade")
    if cfg.get("depth5"):
        streams.append("btcusdt@depth5@100ms")

    url       = _SPOT_BASE + "/".join(streams)
    closes_1m = deque(maxlen=21)
    closes_5m = deque(maxlen=21)

    while not stop.is_set():
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                print("[binance_spot] connected")
                async for raw in ws:
                    if stop.is_set():
                        break
                    msg   = json.loads(raw)
                    stype = msg.get("stream", "")
                    data  = msg.get("data", msg)
                    slug  = market.slug

                    # ── kline ──────────────────────────────────────────────
                    if "kline" in stype:
                        k = data["k"]
                        if k["x"]:  # closed candle only
                            c = float(k["c"])
                            if k["i"] == "1m":
                                prev = closes_1m[-1] if closes_1m else None
                                closes_1m.append(c)
                                vol = _realized_vol(closes_1m)
                                writer.write(
                                    "binance_kline_1m",
                                    ts_exchange=ts_from_ms(data.get("E")),
                                    slug=slug,
                                    btc_price=c,
                                    kline_close=c,
                                    kline_prev_close=prev,
                                    kline_vol_1m=round(vol, 8) if vol else None,
                                )
                            else:
                                closes_5m.append(c)
                                writer.write(
                                    "binance_kline_5m",
                                    ts_exchange=ts_from_ms(data.get("E")),
                                    slug=slug,
                                    btc_price=c,
                                    kline_close=c,
                                )

                    # ── aggtrade ───────────────────────────────────────────
                    elif "aggTrade" in stype and cfg.get("aggtrade"):
                        writer.write(
                            "binance_aggtrade",
                            ts_exchange=ts_from_ms(data.get("T")),  # trade time, not event time
                            slug=slug,
                            btc_price=float(data["p"]),
                            trade_price=float(data["p"]),
                            trade_qty=float(data["q"]),
                            trade_buyer_maker=data["m"],
                        )

                    # ── depth5 ─────────────────────────────────────────────
                    elif "depth5" in stype and cfg.get("depth5"):
                        bids    = data.get("bids", [])
                        asks    = data.get("asks", [])
                        bid_vol = sum(float(b[1]) for b in bids)
                        ask_vol = sum(float(a[1]) for a in asks)
                        total   = bid_vol + ask_vol
                        ofi     = round((bid_vol - ask_vol) / total, 4) if total else 0
                        writer.write(
                            "binance_depth5",
                            ts_exchange=ts_from_ms(data.get("E")),
                            slug=slug,
                            bid_vol_5=round(bid_vol, 4),
                            ask_vol_5=round(ask_vol, 4),
                            ofi=ofi,
                        )

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[binance_spot] error: {e}, reconnecting...")
            await asyncio.sleep(2)


async def _futures(stop: asyncio.Event, market: MarketState, writer: Writer):
    while not stop.is_set():
        try:
            async with websockets.connect(_FUTURES_URL, ping_interval=20) as ws:
                print("[binance_futures] connected (markPrice@1s)")
                async for raw in ws:
                    if stop.is_set():
                        break
                    data = json.loads(raw)
                    writer.write(
                        "binance_mark_price",
                        ts_exchange=ts_from_ms(data.get("E")),
                        slug=market.slug,
                        mark_price=float(data["p"]) if data.get("p") else None,
                        funding_rate=float(data["r"]) if data.get("r") else None,
                    )
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[binance_futures] error: {e}, reconnecting...")
            await asyncio.sleep(2)


async def stream(stop: asyncio.Event, market: MarketState, writer: Writer, cfg: dict):
    tasks = [asyncio.create_task(_spot(stop, market, writer, cfg))]
    if cfg.get("futures"):
        tasks.append(asyncio.create_task(_futures(stop, market, writer)))
    await asyncio.gather(*tasks)
