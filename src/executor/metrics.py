"""
Push metrics to Victoria Metrics via Prometheus text format.
Grafana reads from Victoria Metrics — no Prometheus needed.
"""
import asyncio
import logging
import time
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


class Metrics:
    def __init__(self, victoria_url: str, push_interval_s: int = 10):
        self.url      = victoria_url
        self.interval = push_interval_s
        self._data: dict[str, tuple[float, dict]] = {}   # name → (value, labels)

    # ── setters ───────────────────────────────────────────────────────────────

    def gauge(self, name: str, value: float, labels: dict[str, Any] | None = None) -> None:
        self._data[self._key(name, labels)] = (value, labels or {}, name)

    def _key(self, name: str, labels: dict | None) -> str:
        if not labels:
            return name
        lstr = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{lstr}}}"

    # ── emit helpers (call these at key moments) ──────────────────────────────

    def on_signal(self, edge: float, yes_ask: float, ofi: float, secs: float) -> None:
        self.gauge("executor_signal_edge_yes",          edge)
        self.gauge("executor_signal_yes_ask",           yes_ask)
        self.gauge("executor_signal_ofi",               ofi)
        self.gauge("executor_signal_seconds_remaining", secs)
        self.gauge("executor_signals_total",
                   self._data.get("executor_signals_total", (0,))[0] + 1)

    def on_order(self, success: bool, price: float, size: float, latency_ms: float) -> None:
        self.gauge("executor_order_price",      price)
        self.gauge("executor_order_size",       size)
        self.gauge("executor_order_latency_ms", latency_ms)
        key = "success" if success else "failed"
        self.gauge(f"executor_orders_{key}_total",
                   self._data.get(f"executor_orders_{key}_total", (0,))[0] + 1)

    def on_resolution(self, resolved_yes: int, pnl_net: float, yes_ask: float) -> None:
        self.gauge("executor_trade_resolved_yes", resolved_yes)
        self.gauge("executor_trade_pnl_net",      pnl_net)
        self.gauge("executor_trade_yes_ask",      yes_ask)
        key = "wins" if resolved_yes else "losses"
        self.gauge(f"executor_trades_{key}_total",
                   self._data.get(f"executor_trades_{key}_total", (0,))[0] + 1)
        # running cumulative PnL
        prev = self._data.get("executor_cumulative_pnl_net", (0,))[0]
        self.gauge("executor_cumulative_pnl_net", prev + pnl_net)

    def on_tick(self, slug: str, yes_ask: float | None,
                ofi: float | None, secs: float | None) -> None:
        short = slug[-10:] if slug else "unknown"
        if yes_ask is not None:
            self.gauge("executor_yes_ask",           yes_ask, {"slug": short})
        if ofi is not None:
            self.gauge("executor_ofi",               ofi,     {"slug": short})
        if secs is not None:
            self.gauge("executor_seconds_remaining", secs,    {"slug": short})

    def on_polymarket_lag(self, lag_ms: float) -> None:
        """How long between a BTC price move and yes_ask updating."""
        self.gauge("executor_polymarket_lag_ms", lag_ms)

    # ── push loop ─────────────────────────────────────────────────────────────

    def _build_payload(self) -> str:
        ts  = int(time.time() * 1000)
        lines = []
        for key, entry in self._data.items():
            value = entry[0]
            name  = entry[2] if len(entry) > 2 else key
            labels = entry[1]
            lstr = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
            metric = f'{name}{{{lstr}}} {value} {ts}' if lstr else f'{name} {value} {ts}'
            lines.append(metric)
        return "\n".join(lines)

    async def push(self) -> None:
        payload = self._build_payload()
        if not payload:
            return
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.url, data=payload.encode()) as resp:
                    if resp.status not in (200, 204):
                        logger.warning(f"Metrics push HTTP {resp.status}")
        except Exception as exc:
            logger.debug(f"Metrics push failed (VM down?): {exc}")

    async def push_loop(self) -> None:
        while True:
            await self.push()
            await asyncio.sleep(self.interval)
