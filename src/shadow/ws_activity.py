import asyncio
import json
import logging
import time
from typing import Callable, Awaitable

# ws_activity.py
from opentelemetry import metrics
from .telemetry import get_meter
import websockets

logger = logging.getLogger(__name__)

meter = get_meter()
ws_msg_counter = meter.create_counter("shadow_ws_messages_total")
ws_local_latency = meter.create_histogram("shadow_ws_handle_ms")
ws_total_delay = meter.create_histogram("shadow_ws_total_delay_ms")

class ActivityWSClient:
    def __init__(self, url: str, on_trade: Callable[[dict, float], Awaitable[None]]):
        self._url = url
        self._on_trade = on_trade
        self._stop = asyncio.Event()
        self._last_msg_time = 0.0
        self._heartbeat_interval = 30.0  # Expect msg every 30s under normal load
        self._check_task: asyncio.Task | None = None

    async def run(self) -> None:
        backoff = 1
        while not self._stop.is_set():
            try:
                async with websockets.connect(self._url, ping_interval=10) as ws:
                    self._last_msg_time = time.time()
                    await self._subscribe(ws)
                    self._check_task = asyncio.create_task(self._heartbeat_check(ws))
                    backoff = 1
                    async for raw in ws:
                        self._last_msg_time = time.time()  # Reset on ANY frame
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        await self._handle_message(msg, self._last_msg_time)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("WS error: %s; reconnecting in %ss", e, backoff)
                if self._check_task:
                    self._check_task.cancel()
                    self._check_task = None
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _subscribe(self, ws):
        sub = {
            "action": "subscribe",
            "subscriptions": [
                {"topic": "activity", "type": "*"},
            ],
        }
        await ws.send(json.dumps(sub))
        logger.info("Subscribed to activity")

    async def _handle_message(self, msg: dict, recv_time: float) -> None:
        if msg.get("topic") != "activity":
            return
        payload = msg.get("payload") or {}

        event_type = msg.get("type")

        if event_type != "orders_matched":
            return
        # logger.info(
        #     "WS activity type=%s slug=%s outcome=%s side=%s proxyWallet=%s",
        #     event_type,
        #     payload.get("slug"),
        #     payload.get("outcome"),
        #     payload.get("side"),
        #     payload.get("proxyWallet"),
        # )

        ws_msg_counter.add(1, {"topic": "activity"})

        start = time.time()
        trade_ts = float(payload.get("timestamp", 0.0))

        await self._on_trade(payload, recv_time)

        end = time.time()
        ws_local_latency.record((end - start) * 1000.0)
        raw_delay = end - trade_ts
        if raw_delay < 0:
            raw_delay = 0.0
            # logger.warning("Negative raw delay %.4f for token=%s; clamp to 0.0", raw_delay, payload.get("tokenId"))

        if trade_ts > 0:
            ws_total_delay.record((raw_delay) * 1000.0)

    async def _heartbeat_check(self, ws) -> None:
        """Force-close the socket on stall so the run() loop reconnects."""
        try:
            while not self._stop.is_set():
                await asyncio.sleep(self._heartbeat_interval)
                stall_duration = time.time() - self._last_msg_time
                if stall_duration > self._heartbeat_interval * 2:
                    logger.warning(
                        "Stale WS: no msgs for %.0fs (exp: <%ds), forcing reconnect",
                        stall_duration,
                        self._heartbeat_interval * 2,
                    )
                    # Close the socket — raises ConnectionClosed inside
                    # `async for raw in ws`, which propagates out to the
                    # `except Exception` handler in run() and triggers backoff.
                    # Do NOT touch self._stop here; that's the permanent-stop flag.
                    await ws.close()
                    break
        except asyncio.CancelledError:
            pass

    def stop(self) -> None:
        self._stop.set()
        if self._check_task:
            self._check_task.cancel()
