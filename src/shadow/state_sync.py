"""
L2 state cache — persists MirrorStrategy in-memory state to ClickHouse and
hydrates it on startup so the bot resumes correctly after any restart.

Tables written to (all in the 'default' database, defined in 02-shadow-schema.sql):

    bot_state         — per-token: first_price, buy_seq, entered flag
    bot_entered_slugs — market slugs entered today (prevents both-side entry)
    bot_positions     — open position book snapshot

All three tables partition by Date, so state resets naturally at UTC midnight
without any manual cleanup needed.

Lifecycle:
    sync = StateSync(strategy, host=..., password=...)
    await sync.start()   # connect → hydrate from today's rows → start 30s loop
    # ... bot runs ...
    await sync.stop()    # cancel loop → final flush
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING

import clickhouse_connect

if TYPE_CHECKING:
    from .strategy import MirrorStrategy

logger = logging.getLogger(__name__)

_SYNC_INTERVAL_SECS: float = 30.0

# Column lists must match CREATE TABLE column order exactly.
_BOT_STATE_COLS = ["date", "token_id", "first_price", "buy_seq", "entered", "updated_at"]
_BOT_SLUG_COLS  = ["date", "slug", "entered_at"]
_BOT_POS_COLS   = [
    "date", "token_id", "side", "size", "avg_entry_price",
    "source_leader", "entry_time", "is_closed", "updated_at",
]


class StateSync:
    """
    Async periodic ClickHouse state sync for MirrorStrategy.

    Persists the four collections that would otherwise reset on restart:
      _first_price   — first leader BUY price per token
      _buy_seq       — number of leader BUYs seen per token
      _mom_entered   — tokens the bot has already entered today
      _entered_slugs — market slugs the bot has already entered today
      position_book  — open positions (prevents Gate 3 bypass on restart)

    Insert pattern: ReplacingMergeTree + FINAL on SELECT.  Each _sync_now()
    call upserts all current rows; ClickHouse deduplicates lazily on merge and
    immediately when queried with FINAL.  Tables are small enough (~hundreds of
    rows) that FINAL is fast.
    """

    def __init__(
        self,
        strategy: MirrorStrategy,
        host: str = "localhost",
        port: int = 8123,
        database: str = "default",
        password: str = "",
        sync_interval_secs: float = _SYNC_INTERVAL_SECS,
    ) -> None:
        self._strategy = strategy
        self._host     = host
        self._port     = port
        self._db       = database
        self._password = password
        self._interval = sync_interval_secs

        self._client: clickhouse_connect.driver.AsyncClient | None = None
        self._task: asyncio.Task | None = None

    # ── lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Connect, hydrate in-memory state, then start the background sync loop."""
        self._client = await clickhouse_connect.get_async_client(
            host=self._host,
            port=self._port,
            database=self._db,
            password=self._password,
        )
        logger.info(
            "StateSync connected to ClickHouse %s:%d/%s",
            self._host, self._port, self._db,
        )
        await self._hydrate()
        self._task = asyncio.create_task(self._sync_loop())
        logger.info(
            "StateSync background loop started (interval=%.0fs)", self._interval
        )

    async def stop(self) -> None:
        """Cancel the background loop, then do a final flush so nothing is lost."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._sync_now()
        logger.info("StateSync stopped — final sync written")

    # ── hydration ──────────────────────────────────────────────────────────────

    async def _hydrate(self) -> None:
        """
        Load today's persisted state into strategy's in-memory collections.
        Runs once at startup before the WebSocket feed connects.
        """
        if not self._client:
            return

        today = date.today().isoformat()  # 'YYYY-MM-DD' — ClickHouse Date accepts this

        # ── bot_state → _first_price, _buy_seq, _mom_entered ─────────────────
        try:
            result = await self._client.query(
                f"SELECT token_id, first_price, buy_seq, entered "
                f"FROM {self._db}.bot_state FINAL "
                f"WHERE date = '{today}'"
            )
            n = 0
            for token_id, first_price, buy_seq, entered in result.result_rows:
                fp = float(first_price)
                if fp > 0:
                    # Only restore tracking state for tokens the leader actually bought.
                    self._strategy._first_price[token_id] = fp
                    self._strategy._buy_seq[token_id]     = int(buy_seq)
                if int(entered):
                    self._strategy._mom_entered.add(token_id)
                n += 1
            logger.info(
                "StateSync hydrated %d token states (date=%s)", n, today
            )
        except Exception as exc:
            logger.warning("StateSync hydrate bot_state failed: %s", exc)

        # ── bot_entered_slugs → _entered_slugs ────────────────────────────────
        try:
            result = await self._client.query(
                f"SELECT slug "
                f"FROM {self._db}.bot_entered_slugs FINAL "
                f"WHERE date = '{today}'"
            )
            slugs = [r[0] for r in result.result_rows]
            self._strategy._entered_slugs.update(slugs)
            logger.info("StateSync hydrated %d entered slugs", len(slugs))
        except Exception as exc:
            logger.warning("StateSync hydrate bot_entered_slugs failed: %s", exc)

        # ── bot_positions → position_book ─────────────────────────────────────
        try:
            from .position_book import MyPosition  # local import avoids circularity

            result = await self._client.query(
                f"SELECT token_id, side, size, avg_entry_price, source_leader, entry_time "
                f"FROM {self._db}.bot_positions FINAL "
                f"WHERE date = '{today}' AND is_closed = 0"
            )
            n_pos = 0
            for token_id, side, size, avg_entry_price, source_leader, entry_time in (
                result.result_rows
            ):
                if self._strategy.position_book.has_position(token_id):
                    continue  # already present — shouldn't happen on cold start

                # Normalise timezone — ClickHouse returns naive datetimes.
                if not isinstance(entry_time, datetime):
                    entry_time = datetime.now(timezone.utc)
                elif entry_time.tzinfo is None:
                    entry_time = entry_time.replace(tzinfo=timezone.utc)

                pos = MyPosition(
                    token_id=token_id,
                    side=side,
                    size=float(size),
                    avg_entry_price=float(avg_entry_price),
                    source_leader=source_leader,
                    entry_time=entry_time,
                    last_update=entry_time,
                )
                # Bypass open_or_add so we don't trigger any side-effects.
                self._strategy.position_book._positions[token_id] = pos
                n_pos += 1

            logger.info("StateSync hydrated %d open positions", n_pos)
        except Exception as exc:
            logger.warning("StateSync hydrate bot_positions failed: %s", exc)

    # ── sync loop ──────────────────────────────────────────────────────────────

    async def _sync_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._interval)
                await self._sync_now()
        except asyncio.CancelledError:
            pass

    async def _sync_now(self) -> None:
        """
        Upsert all current in-memory state to ClickHouse.
        Idempotent — safe to call multiple times; ReplacingMergeTree deduplicates.
        """
        if not self._client:
            return

        today = date.today()            # Python date object — clickhouse_connect maps to ClickHouse Date
        now   = datetime.now(timezone.utc)
        s     = self._strategy          # shorthand

        # ── bot_state ─────────────────────────────────────────────────────────
        try:
            all_tokens = set(s._first_price.keys()) | set(s._buy_seq.keys())
            if all_tokens:
                rows = [
                    [
                        today,
                        token_id,
                        s._first_price.get(token_id, 0.0),
                        s._buy_seq.get(token_id, 0),
                        1 if token_id in s._mom_entered else 0,
                        now,
                    ]
                    for token_id in all_tokens
                ]
                await self._client.insert(
                    "bot_state",
                    rows,
                    column_names=_BOT_STATE_COLS,
                )
                logger.debug("StateSync wrote %d bot_state rows", len(rows))
        except Exception as exc:
            logger.warning("StateSync sync bot_state failed: %s", exc)

        # ── bot_entered_slugs ─────────────────────────────────────────────────
        try:
            slugs = list(s._entered_slugs)
            if slugs:
                slug_rows = [[today, slug, now] for slug in slugs]
                await self._client.insert(
                    "bot_entered_slugs",
                    slug_rows,
                    column_names=_BOT_SLUG_COLS,
                )
                logger.debug("StateSync wrote %d slug rows", len(slug_rows))
        except Exception as exc:
            logger.warning("StateSync sync bot_entered_slugs failed: %s", exc)

        # ── bot_positions ─────────────────────────────────────────────────────
        try:
            positions = list(s.position_book.all())
            if positions:
                pos_rows = []
                for pos in positions:
                    et = pos.entry_time
                    if et.tzinfo is None:
                        et = et.replace(tzinfo=timezone.utc)
                    pos_rows.append([
                        today,
                        pos.token_id,
                        pos.side,
                        pos.size,
                        pos.avg_entry_price,
                        pos.source_leader,
                        et,
                        0,   # is_closed — we only keep open positions in memory
                        now,
                    ])
                await self._client.insert(
                    "bot_positions",
                    pos_rows,
                    column_names=_BOT_POS_COLS,
                )
                logger.debug("StateSync wrote %d position rows", len(pos_rows))
        except Exception as exc:
            logger.warning("StateSync sync bot_positions failed: %s", exc)
