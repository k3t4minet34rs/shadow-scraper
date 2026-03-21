"""
SQLite store — events, markets, forecasts, predictions, orders, resolutions.
"""
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


DDL = """
CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT PRIMARY KEY,
    slug            TEXT UNIQUE NOT NULL,
    title           TEXT,
    city            TEXT,
    station_code    TEXT,           -- e.g. ZSPD, SAEZ
    wunderground_url TEXT,
    lat             REAL,
    lon             REAL,
    target_date     DATE NOT NULL,
    end_date        TIMESTAMP NOT NULL,
    neg_risk_id     TEXT,
    status          TEXT DEFAULT 'DISCOVERED',  -- DISCOVERED/FORECASTED/POSITIONED/MONITORING/EXPIRED/RESOLVED
    discovered_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS markets (
    market_id       TEXT PRIMARY KEY,
    event_id        TEXT NOT NULL REFERENCES events(event_id),
    question        TEXT,
    bucket_label    TEXT NOT NULL,  -- "14°C", "20°C or below", "25°C or above"
    bucket_low      REAL,           -- null = -inf
    bucket_high     REAL,           -- null = +inf
    bucket_type     TEXT,           -- "exact", "below", "above"
    yes_token_id    TEXT NOT NULL,
    no_token_id     TEXT NOT NULL,
    condition_id    TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS forecasts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT NOT NULL REFERENCES events(event_id),
    fetched_at      TIMESTAMP NOT NULL,
    model           TEXT NOT NULL,  -- "ecmwf_ensemble"
    ensemble_json   TEXT NOT NULL,  -- raw array of member max temps (JSON)
    p10             REAL,
    p50             REAL,
    p90             REAL,
    member_count    INTEGER
);

CREATE TABLE IF NOT EXISTS predictions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id       TEXT NOT NULL REFERENCES markets(market_id),
    forecast_id     INTEGER NOT NULL REFERENCES forecasts(id),
    predicted_at    TIMESTAMP NOT NULL,
    fv              REAL NOT NULL,      -- our model probability
    market_price    REAL NOT NULL,      -- yes_ask at prediction time
    edge            REAL NOT NULL       -- fv - market_price
);

CREATE TABLE IF NOT EXISTS orders (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id       TEXT NOT NULL REFERENCES markets(market_id),
    placed_at       TIMESTAMP NOT NULL,
    side            TEXT NOT NULL,      -- "YES" or "NO"
    price           REAL NOT NULL,
    stake_usdc      REAL NOT NULL,
    size            REAL,               -- contracts purchased
    success         BOOLEAN NOT NULL,
    order_id        TEXT,
    error           TEXT
);

CREATE TABLE IF NOT EXISTS resolutions (
    market_id       TEXT PRIMARY KEY REFERENCES markets(market_id),
    resolved_at     TIMESTAMP,
    resolved_yes    BOOLEAN,
    actual_temp     REAL
);

CREATE INDEX IF NOT EXISTS idx_markets_event    ON markets(event_id);
CREATE INDEX IF NOT EXISTS idx_forecasts_event  ON forecasts(event_id);
CREATE INDEX IF NOT EXISTS idx_predictions_mkt  ON predictions(market_id);
CREATE INDEX IF NOT EXISTS idx_orders_market    ON orders(market_id);
"""


class DB:
    def __init__(self, path: str | Path = "data/weather.db"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    @contextmanager
    def _conn(self):
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA foreign_keys=ON")
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def _init(self):
        with self._conn() as con:
            con.executescript(DDL)

    # ── events ────────────────────────────────────────────────────────────────

    def upsert_event(self, ev: dict) -> bool:
        """Insert or ignore event. Returns True if newly inserted."""
        with self._conn() as con:
            cur = con.execute("""
                INSERT OR IGNORE INTO events
                  (event_id, slug, title, city, station_code, wunderground_url,
                   lat, lon, target_date, end_date, neg_risk_id)
                VALUES
                  (:event_id, :slug, :title, :city, :station_code, :wunderground_url,
                   :lat, :lon, :target_date, :end_date, :neg_risk_id)
            """, ev)
            return cur.rowcount > 0

    def upsert_market(self, mkt: dict):
        with self._conn() as con:
            con.execute("""
                INSERT OR IGNORE INTO markets
                  (market_id, event_id, question, bucket_label,
                   bucket_low, bucket_high, bucket_type,
                   yes_token_id, no_token_id, condition_id)
                VALUES
                  (:market_id, :event_id, :question, :bucket_label,
                   :bucket_low, :bucket_high, :bucket_type,
                   :yes_token_id, :no_token_id, :condition_id)
            """, mkt)

    def get_active_events(self) -> list[sqlite3.Row]:
        with self._conn() as con:
            return con.execute("""
                SELECT * FROM events
                WHERE status NOT IN ('EXPIRED', 'RESOLVED')
                ORDER BY target_date
            """).fetchall()

    def get_expired_unresolved(self) -> list[sqlite3.Row]:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as con:
            return con.execute("""
                SELECT e.* FROM events e
                WHERE e.end_date < ?
                  AND e.status != 'RESOLVED'
            """, (now,)).fetchall()

    def get_markets(self, event_id: str) -> list[sqlite3.Row]:
        with self._conn() as con:
            return con.execute("""
                SELECT * FROM markets WHERE event_id = ?
                ORDER BY bucket_low NULLS FIRST
            """, (event_id,)).fetchall()

    def set_event_status(self, event_id: str, status: str):
        with self._conn() as con:
            con.execute(
                "UPDATE events SET status = ? WHERE event_id = ?",
                (status, event_id)
            )

    # ── forecasts ─────────────────────────────────────────────────────────────

    def save_forecast(self, event_id: str, model: str, members: list[float],
                      p10: float, p50: float, p90: float) -> int:
        with self._conn() as con:
            cur = con.execute("""
                INSERT INTO forecasts
                  (event_id, fetched_at, model, ensemble_json,
                   p10, p50, p90, member_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (event_id, datetime.now(timezone.utc).isoformat(),
                  model, json.dumps(members), p10, p50, p90, len(members)))
            return cur.lastrowid

    def latest_forecast(self, event_id: str) -> sqlite3.Row | None:
        with self._conn() as con:
            return con.execute("""
                SELECT * FROM forecasts
                WHERE event_id = ?
                ORDER BY fetched_at DESC LIMIT 1
            """, (event_id,)).fetchone()

    # ── predictions ───────────────────────────────────────────────────────────

    def save_prediction(self, market_id: str, forecast_id: int,
                        fv: float, market_price: float):
        with self._conn() as con:
            con.execute("""
                INSERT INTO predictions
                  (market_id, forecast_id, predicted_at, fv, market_price, edge)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (market_id, forecast_id,
                  datetime.now(timezone.utc).isoformat(),
                  fv, market_price, fv - market_price))

    # ── orders / positions ────────────────────────────────────────────────────

    def save_order(self, market_id: str, side: str, price: float,
                   stake: float, size: float | None,
                   success: bool, order_id: str | None, error: str | None):
        with self._conn() as con:
            con.execute("""
                INSERT INTO orders
                  (market_id, placed_at, side, price, stake_usdc,
                   size, success, order_id, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (market_id, datetime.now(timezone.utc).isoformat(),
                  side, price, stake, size, success, order_id, error))

    def get_position(self, event_id: str) -> dict[str, float]:
        """Returns {market_id: total_usdc_staked} for successful orders."""
        with self._conn() as con:
            rows = con.execute("""
                SELECT o.market_id, SUM(o.stake_usdc) as total
                FROM orders o
                JOIN markets m ON m.market_id = o.market_id
                WHERE m.event_id = ? AND o.success = TRUE AND o.side = 'YES'
                GROUP BY o.market_id
            """, (event_id,)).fetchall()
        return {r["market_id"]: r["total"] for r in rows}

    # ── resolutions ───────────────────────────────────────────────────────────

    def save_resolution(self, market_id: str, resolved_yes: bool,
                        actual_temp: float | None = None):
        with self._conn() as con:
            con.execute("""
                INSERT OR REPLACE INTO resolutions
                  (market_id, resolved_at, resolved_yes, actual_temp)
                VALUES (?, ?, ?, ?)
            """, (market_id, datetime.now(timezone.utc).isoformat(),
                  resolved_yes, actual_temp))

    def is_resolved(self, market_id: str) -> bool:
        with self._conn() as con:
            row = con.execute(
                "SELECT 1 FROM resolutions WHERE market_id = ?", (market_id,)
            ).fetchone()
            return row is not None
