import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import clickhouse_connect

logger = logging.getLogger(__name__)

# Column lists match the CREATE TABLE schema exactly.
_LEADER_TRADE_COLS = [
    "event_time", "recv_time", "wallet", "token_id", "event_slug",
    "side", "price", "size", "classified",
]
_MY_ORDER_COLS = [
    "order_time", "order_id", "token_id", "side", "price", "size",
    "order_type", "strategy", "source_leader", "status",
    "fill_price", "fill_size", "fill_time", "latency_total_ms",
]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class ClickHouseWriter:
    """
    Fire-and-forget async writer. Callers call enqueue_* (sync, non-blocking)
    and the internal drain loop batches and flushes to ClickHouse.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        database: str = "default",
        password: str = "",
    ):
        self._host = host
        self._port = port
        self._database = database
        self._password = password
        self._client: clickhouse_connect.driver.AsyncClient | None = None
        self._queue: asyncio.Queue = asyncio.Queue()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._client = await clickhouse_connect.get_async_client(
            host=self._host,
            port=self._port,
            database=self._database,
            password=self._password,
        )
        self._task = asyncio.create_task(self._drain_loop())
        logger.info("ClickHouseWriter started (host=%s db=%s)", self._host, self._database)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    # ---------- public enqueue helpers (sync, non-blocking) ----------

    def enqueue_leader_trade(
        self,
        *,
        event_time: datetime,
        recv_time: datetime,
        wallet: str,
        token_id: str,
        event_slug: str,
        side: str,           # "BUY" | "SELL"
        price: float,
        size: float,
        classified: str,     # "NEW" | "ADD" | "REDUCE" | "CLOSE"
    ) -> None:
        row = [
            event_time, recv_time, wallet, token_id, event_slug,
            side, price, size, classified,
        ]
        self._queue.put_nowait(("leader_trades", row))

    def enqueue_my_order(
        self,
        *,
        order_time: datetime,
        order_id: str,
        token_id: str,
        side: str,
        price: float,
        size: float,
        order_type: str,          # "GTC" | "FOK"
        strategy: str,            # "mirror" | "tp_sl"
        source_leader: str,
        status: str = "PENDING",
        fill_price: Optional[float] = None,
        fill_size: Optional[float] = None,
        fill_time: Optional[datetime] = None,
        latency_total_ms: Optional[float] = None,
    ) -> None:
        row = [
            order_time, order_id, token_id, side, price, size,
            order_type, strategy, source_leader, status,
            fill_price, fill_size, fill_time, latency_total_ms,
        ]
        self._queue.put_nowait(("my_orders", row))

    # ---------- drain loop ----------

    async def _drain_loop(self) -> None:
        while True:
            try:
                # Block until at least one item is available.
                table, row = await self._queue.get()
                batch_leader: list[list] = []
                batch_orders: list[list] = []

                self._route(table, row, batch_leader, batch_orders)

                # Drain everything else that's already queued (non-blocking).
                while not self._queue.empty():
                    table, row = self._queue.get_nowait()
                    self._route(table, row, batch_leader, batch_orders)

                await self._flush(batch_leader, batch_orders)

            except asyncio.CancelledError:
                # Drain whatever is left before exiting.
                batch_leader = []
                batch_orders = []
                while not self._queue.empty():
                    table, row = self._queue.get_nowait()
                    self._route(table, row, batch_leader, batch_orders)
                await self._flush(batch_leader, batch_orders)
                break
            except Exception as e:
                logger.error("ClickHouse drain error: %s", e, exc_info=True)
                await asyncio.sleep(2)

    @staticmethod
    def _route(
        table: str,
        row: list,
        batch_leader: list,
        batch_orders: list,
    ) -> None:
        if table == "leader_trades":
            batch_leader.append(row)
        else:
            batch_orders.append(row)

    async def _flush(self, batch_leader: list, batch_orders: list) -> None:
        if not self._client:
            return
        if batch_leader:
            try:
                await self._client.insert(
                    "leader_trades", batch_leader, column_names=_LEADER_TRADE_COLS
                )
                logger.debug("CH inserted %d leader_trades rows", len(batch_leader))
            except Exception as e:
                logger.error("CH leader_trades insert failed: %s", e)
        if batch_orders:
            try:
                await self._client.insert(
                    "my_orders", batch_orders, column_names=_MY_ORDER_COLS
                )
                logger.debug("CH inserted %d my_orders rows", len(batch_orders))
            except Exception as e:
                logger.error("CH my_orders insert failed: %s", e)
