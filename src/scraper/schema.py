# ── canonical schema ───────────────────────────────────────────────────────────
# Sparse rows: each event only fills its own columns.
# Analysis uses forward-fill to reconstruct full state at any timestamp.

CSV_FIELDS = [
    # ── identity ───────────────────────────────────────────────────────────────
    "ts_local",           # ISO: local receipt time
    "ts_exchange",        # ISO: exchange-reported event time
    "source",             # e.g. "binance_kline_1m", "bybit_ticker", "poly_book"
    "slug",               # active Polymarket market slug

    # ── generic BTC price (all exchange tickers write here) ────────────────────
    # forward-fill by source to get per-exchange view, or mix for cross-exchange
    "btc_price",          # last trade price
    "btc_bid",            # best bid
    "btc_ask",            # best ask
    "btc_vol",            # 24h volume or single trade size (BTC)

    # ── binance kline (fires on closed candle only, ~1/min) ────────────────────
    "kline_close",        # closed candle close price
    "kline_prev_close",   # previous closed candle (reference price proxy for model)
    "kline_vol_1m",       # realized vol from rolling 1m log-returns

    # ── binance aggtrade (~10-50 events/s) ────────────────────────────────────
    "trade_price",        # individual trade price
    "trade_qty",          # trade quantity (BTC)
    "trade_buyer_maker",  # True = buyer was maker (i.e. seller was aggressive taker)

    # ── binance depth5@100ms (~10 events/s) ───────────────────────────────────
    "bid_vol_5",          # total BTC volume across top 5 bid levels
    "ask_vol_5",          # total BTC volume across top 5 ask levels
    "ofi",                # (bid_vol - ask_vol) / (bid_vol + ask_vol), range -1 to +1

    # ── binance futures markPrice@1s ──────────────────────────────────────────
    "mark_price",         # futures mark price (can lead spot by seconds)
    "funding_rate",       # perpetual funding rate (value changes every 8h)

    # ── polymarket ────────────────────────────────────────────────────────────
    "yes_bid",
    "yes_ask",
    "no_bid",
    "no_ask",
    "last_trade",         # e.g. "YES@0.57" or "NO@0.43"
]

# ── DuckDB DDL ─────────────────────────────────────────────────────────────────
# Use when migrating from CSV. Cast ts columns with ::TIMESTAMPTZ.
DUCKDB_DDL = """
CREATE TABLE IF NOT EXISTS ticks (
    -- identity
    ts_local          TIMESTAMPTZ  NOT NULL,
    ts_exchange       TIMESTAMPTZ,
    source            VARCHAR      NOT NULL,
    slug              VARCHAR,

    -- generic BTC price (shared by all exchange tickers)
    btc_price         DOUBLE,
    btc_bid           DOUBLE,
    btc_ask           DOUBLE,
    btc_vol           DOUBLE,

    -- binance kline
    kline_close       DOUBLE,
    kline_prev_close  DOUBLE,
    kline_vol_1m      DOUBLE,

    -- binance aggtrade
    trade_price       DOUBLE,
    trade_qty         DOUBLE,
    trade_buyer_maker BOOLEAN,

    -- binance depth5
    bid_vol_5         DOUBLE,
    ask_vol_5         DOUBLE,
    ofi               DOUBLE,

    -- binance futures
    mark_price        DOUBLE,
    funding_rate      DOUBLE,

    -- polymarket
    yes_bid           DOUBLE,
    yes_ask           DOUBLE,
    no_bid            DOUBLE,
    no_ask            DOUBLE,
    last_trade        VARCHAR
);
"""

# ── DuckDB forward-fill query ──────────────────────────────────────────────────
# Reconstructs full state at every timestamp. Use as a CTE in analysis queries.
FFILL_QUERY = """
SELECT
    ts_local,
    ts_exchange,
    source,
    slug,
    LAST_VALUE(btc_price         IGNORE NULLS) OVER w AS btc_price,
    LAST_VALUE(btc_bid           IGNORE NULLS) OVER w AS btc_bid,
    LAST_VALUE(btc_ask           IGNORE NULLS) OVER w AS btc_ask,
    LAST_VALUE(kline_close       IGNORE NULLS) OVER w AS kline_close,
    LAST_VALUE(kline_prev_close  IGNORE NULLS) OVER w AS kline_prev_close,
    LAST_VALUE(kline_vol_1m      IGNORE NULLS) OVER w AS kline_vol_1m,
    LAST_VALUE(ofi               IGNORE NULLS) OVER w AS ofi,
    LAST_VALUE(mark_price        IGNORE NULLS) OVER w AS mark_price,
    LAST_VALUE(funding_rate      IGNORE NULLS) OVER w AS funding_rate,
    LAST_VALUE(yes_bid           IGNORE NULLS) OVER w AS yes_bid,
    LAST_VALUE(yes_ask           IGNORE NULLS) OVER w AS yes_ask,
    LAST_VALUE(no_bid            IGNORE NULLS) OVER w AS no_bid,
    LAST_VALUE(no_ask            IGNORE NULLS) OVER w AS no_ask,
    LAST_VALUE(last_trade        IGNORE NULLS) OVER w AS last_trade
FROM ticks
WINDOW w AS (ORDER BY ts_local ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
ORDER BY ts_local;
"""
