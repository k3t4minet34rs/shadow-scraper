"""
Binance kline (candlestick) WebSocket client for BTC/USDT.

Subscribes to 1m and 5m klines via a single combined stream connection.
Fires the on_kline callback only on closed candles (k.x == true).

Kline fields passed to callback:
    symbol      — "BTCUSDT"
    interval    — "1m" or "5m"
    open_time   — candle open timestamp (seconds)
    open        — open price (float)
    high        — high price (float)
    low         — low price (float)
    close       — close price (float)
    volume      — base asset volume (float)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

import websockets

logger = logging.getLogger(__name__)

_WS_URL = (
    "wss://stream.binance.com:9443/stream"
    "?streams=btcusdt@kline_1m/btcusdt@kline_5m"
)


@dataclass(slots=True)
class Kline:
    symbol: str
    interval: str       # "1m" or "5m"
    open_time: float    # seconds
    open: float
    high: float
    low: float
    close: float
    volume: float


class BinanceKlineClient:
    """
    WebSocket client that streams closed BTC/USDT 1m and 5m candles from Binance.

    Usage:
        client = BinanceKlineClient(on_kline=my_handler)
        asyncio.create_task(client.run())
        ...
        client.stop()
    """

    def __init__(self, on_kline: Callable[[Kline], Awaitable[None]]) -> None:
        self._on_kline = on_kline
        self._stop = asyncio.Event()
        self._last_msg_time = 0.0
        self._heartbeat_interval = 60.0  # Binance sends a ping every 3m; expect msgs within 2m
        self._check_task: asyncio.Task | None = None

    async def run(self) -> None:
        backoff = 1
        while not self._stop.is_set():
            try:
                async with websockets.connect(_WS_URL, ping_interval=20) as ws:
                    self._last_msg_time = time.time()
                    self._check_task = asyncio.create_task(self._heartbeat_check(ws))
                    backoff = 1
                    logger.info("BinanceKlineClient: connected, streaming 1m/5m BTCUSDT")
                    async for raw in ws:
                        self._last_msg_time = time.time()
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        await self._handle_message(msg)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning(
                    "BinanceKlineClient: WS error: %s; reconnecting in %ss", exc, backoff
                )
                if self._check_task:
                    self._check_task.cancel()
                    self._check_task = None
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _handle_message(self, msg: dict) -> None:
        # Combined stream wraps each message: {"stream": "...", "data": {...}}
        data = msg.get("data", {})
        if data.get("e") != "kline":
            return

        k = data.get("k", {})
        if not k.get("x"):
            return  # candle not yet closed — ignore

        kline = Kline(
            symbol=k["s"],
            interval=k["i"],
            open_time=k["t"] / 1000.0,
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
        )

        logger.debug(
            "BinanceKlineClient: closed %s %s o=%.2f h=%.2f l=%.2f c=%.2f",
            kline.interval, kline.symbol,
            kline.open, kline.high, kline.low, kline.close,
        )

        try:
            await self._on_kline(kline)
        except Exception as exc:
            logger.error("BinanceKlineClient: on_kline handler error: %s", exc, exc_info=True)

    async def _heartbeat_check(self, ws) -> None:
        try:
            while not self._stop.is_set():
                await asyncio.sleep(self._heartbeat_interval)
                stall = time.time() - self._last_msg_time
                if stall > self._heartbeat_interval * 2:
                    logger.warning(
                        "BinanceKlineClient: no msgs for %.0fs, forcing reconnect", stall
                    )
                    await ws.close()
                    break
        except asyncio.CancelledError:
            pass

    def stop(self) -> None:
        self._stop.set()
        if self._check_task:
            self._check_task.cancel()
