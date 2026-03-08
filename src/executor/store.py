"""
SQLite trade log.
One row per trade entry; updated in-place on resolution.
"""
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS trades (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_entry      TEXT    NOT NULL,
    slug          TEXT    NOT NULL,
    yes_token     TEXT,

    -- signal snapshot
    yes_ask       REAL,
    fv_yes        REAL,
    edge_yes      REAL,
    ofi           REAL,
    mark_price    REAL,
    secs_left     REAL,
    stake_usdc    REAL,

    -- order result
    order_id      TEXT,
    order_price   REAL,
    order_size    REAL,
    order_success INTEGER,
    order_error   TEXT,

    -- resolution (filled in later)
    ts_resolved   TEXT,
    resolved_yes  INTEGER,
    pnl_gross     REAL,
    pnl_net       REAL
);
"""


class TradeStore:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute(DDL)
        self.conn.commit()
        logger.info(f"TradeStore ready: {db_path}")

    def log_entry(
        self,
        slug:        str,
        yes_token:   str,
        signal_info: dict,
        order:       dict,
        stake_usdc:  float,
    ) -> int:
        ts = datetime.now(timezone.utc).isoformat()
        cur = self.conn.execute("""
            INSERT INTO trades
                (ts_entry, slug, yes_token,
                 yes_ask, fv_yes, edge_yes, ofi, mark_price, secs_left, stake_usdc,
                 order_id, order_price, order_size, order_success, order_error)
            VALUES (?,?,?, ?,?,?,?,?,?,?, ?,?,?,?,?)
        """, (
            ts, slug, yes_token,
            signal_info.get("yes_ask"),
            signal_info.get("fv_yes"),
            signal_info.get("edge_yes"),
            signal_info.get("ofi"),
            signal_info.get("mark_price"),
            signal_info.get("seconds_remaining"),
            stake_usdc,
            order.get("order_id"),
            order.get("price"),
            order.get("size"),
            1 if order["success"] else 0,
            order.get("error"),
        ))
        self.conn.commit()
        trade_id = cur.lastrowid
        logger.info(f"Trade logged: id={trade_id} slug={slug} success={order['success']}")
        return trade_id

    def log_resolution(
        self,
        trade_id:     int,
        resolved_yes: int,
        pnl_gross:    float,
        pnl_net:      float,
    ) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        self.conn.execute("""
            UPDATE trades
            SET ts_resolved=?, resolved_yes=?, pnl_gross=?, pnl_net=?
            WHERE id=?
        """, (ts, resolved_yes, pnl_gross, pnl_net, trade_id))
        self.conn.commit()
        outcome = "WIN" if resolved_yes else "LOSS"
        logger.info(f"Trade resolved: id={trade_id} {outcome} pnl_net=${pnl_net:.2f}")

    def open_trades(self) -> list[dict]:
        """Return trades that have not been resolved yet."""
        cur = self.conn.execute("""
            SELECT id, slug, yes_ask, stake_usdc
            FROM trades
            WHERE order_success=1 AND resolved_yes IS NULL
        """)
        return [
            {"id": r[0], "slug": r[1], "yes_ask": r[2], "stake_usdc": r[3]}
            for r in cur.fetchall()
        ]
