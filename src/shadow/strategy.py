"""
Momentum copy-trade strategy (MVP2).

Entry rule (from overnight backtests on 273 resolved markets, 303 signals):
  - Leader must have made ≥ 1 prior BUY on this token  (seq >= _MOM_SEQ_MIN)
  - Current price must be ≥ 10% above the first observed price  (rel_price >= 1 + _MOM_THR)
  - Contract must have ≥ 60 s of life remaining
  - We have not already entered this token in this session
  - Per-token inflight mutex prevents concurrent races

Win-rate empirical: 71%, breakeven WR at avg entry ~0.632: 65.4%, edge ≈ 5.5 pp.
Sizing: 2% of live wallet balance per trade (conservative; full Kelly 33%, qtr-Kelly 8.3%).

Old strategy removed:
  - price < 0.45 filter (selected losers — leader DCA-ing down)
  - close_position_complementary (YES+NO hedge bled 2 % fee per round-trip)
  - register_open (never decremented; rely on 30s balance poller instead)
"""

import logging
import re
import time
import uuid
from datetime import datetime, timezone

from .risk import RiskEngine
from .execution import ExecutionClient
from .position_book import PositionBook, MyPosition
from .clickhouse_writer import ClickHouseWriter
from .telemetry import get_meter

# ── Momentum constants ────────────────────────────────────────────────────────
_MOM_SEQ_MIN   = 4      # fire after leader's 5th BUY — within 0.60-0.80 band seq≥4 adds +2pp WR
_MOM_THR       = 0.10   # price must be ≥ 10% above first observed price
_MOM_SECS_MIN  = 90     # skip if contract expires within 90 s (cuts late 5m entries)
# Dead zone: US pre-market + market open. Leader signal degrades 7am–1pm ET.
# ET = UTC-5 (no DST correction — close enough). Block 12:00–18:00 UTC.
_MOM_BLOCK_UTC_START = 12  # 7am ET
_MOM_BLOCK_UTC_END   = 18  # 1pm ET
_MOM_SIZE_PCT  = 0.02   # 2 % of live wallet balance per trade

# Parses slugs like "btc-updown-5m-1772127600" → duration_minutes, open_ts
_SLUG_RE = re.compile(r"-(\d+)m-(\d+)$")

# ── OpenTelemetry meters ──────────────────────────────────────────────────────
meter = get_meter()
leader_trades_executed = meter.create_counter("shadow_leader_trades_executed_total")
leader_trades_skipped  = meter.create_counter("shadow_leader_trades_skipped_total")
orders_placed          = meter.create_counter("shadow_orders_placed_total")
orders_success         = meter.create_counter("shadow_orders_success_total")
orders_failed          = meter.create_counter("shadow_orders_failed_total")
strategy_local_latency = meter.create_histogram("shadow_strategy_handle_ms")
strategy_total_delay   = meter.create_histogram("shadow_strategy_total_delay_ms")

logger = logging.getLogger(__name__)


def _secs_remaining(slug: str | None, now_ts: float) -> float | None:
    """
    Return seconds until contract expiry, or None if the slug can't be parsed.
    Slug format: <name>-<duration_minutes>m-<open_unix_ts>
    Example: "btc-updown-5m-1772127600"
    """
    if not slug:
        return None
    m = _SLUG_RE.search(slug)
    if not m:
        return None
    duration_secs = int(m.group(1)) * 60
    open_ts       = int(m.group(2))
    expiry_ts     = open_ts + duration_secs
    return expiry_ts - now_ts


class MirrorStrategy:
    def __init__(
        self,
        risk: RiskEngine,
        exec_client: ExecutionClient,
        bankroll_usdc: float,          # kept for backward-compat; not used for sizing
        position_book: PositionBook,
        live: bool = False,
        ch_writer: ClickHouseWriter | None = None,
    ):
        self._risk          = risk
        self._exec          = exec_client
        self._bankroll      = bankroll_usdc   # legacy; sizing now uses live balance
        self.position_book  = position_book
        self._live          = live
        self._ch            = ch_writer

        # Per-token inflight mutex (prevents concurrent race on same token).
        self._inflight_tokens: set[str] = set()

        # Momentum state — reset each session (not persisted).
        # Indexed by token_id.
        self._first_price: dict[str, float] = {}   # first BUY price seen from leader
        self._buy_seq:     dict[str, int]   = {}   # number of leader BUYs seen (0-indexed)
        self._mom_entered: set[str]         = set() # tokens where we have already entered
        # One entry per market slug — prevents entering both UP and DOWN tokens
        # of the same binary pair (leader buys both sides; only one should win).
        self._entered_slugs: set[str]       = set()

    # ── main handler ─────────────────────────────────────────────────────────

    async def on_leader_trade(
        self,
        *,
        wallet:    str,
        token_id:  str,
        side:      str,
        price:     float,
        size:      float,
        kind:      str,       # "NEW" | "ADD" | "CLOSE"
        payload:   dict,
        recv_time: float,
    ) -> None:
        trade_ts      = float(payload.get("timestamp", 0.0))
        process_start = time.time()
        side          = side.upper()
        market_slug   = payload.get("slug")

        # ── Update momentum state for every leader BUY ───────────────────────
        # We track state regardless of inflight status so sequence counts are
        # accurate even when we skip firing.
        if side == "BUY" and kind in ("NEW", "ADD"):
            if token_id not in self._first_price:
                self._first_price[token_id] = price
                self._buy_seq[token_id]     = 0
            else:
                self._buy_seq[token_id] = self._buy_seq.get(token_id, 0) + 1

        # ── Inflight mutex ────────────────────────────────────────────────────
        if token_id in self._inflight_tokens:
            logger.debug(
                "Skip leader trade token=%s side=%s kind=%s: already inflight",
                token_id, side, kind,
            )
            leader_trades_skipped.add(1, {"reason": "already_inflight", "wallet": wallet})
            return

        self._inflight_tokens.add(token_id)
        try:
            await self._handle_momentum(
                wallet=wallet,
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                kind=kind,
                market_slug=market_slug,
                trade_ts=trade_ts,
                process_start=process_start,
                recv_time=recv_time,
            )
        finally:
            self._inflight_tokens.discard(token_id)

    # ── momentum entry logic ──────────────────────────────────────────────────

    async def _handle_momentum(
        self,
        *,
        wallet:        str,
        token_id:      str,
        side:          str,
        price:         float,
        size:          float,
        kind:          str,
        market_slug:   str | None,
        trade_ts:      float,
        process_start: float,
        recv_time:     float,
    ) -> None:
        now_ts = time.time()

        # ── Gate 1: only react to leader BUYs ────────────────────────────────
        if side != "BUY" or kind not in ("NEW", "ADD"):
            logger.debug(
                "Skip token=%s side=%s kind=%s: not a leader BUY", token_id, side, kind,
            )
            leader_trades_skipped.add(1, {"reason": f"side_{side.lower()}_or_kind_{kind.lower()}"})
            return

        # ── Gate 2: already entered this token this session ───────────────────
        if token_id in self._mom_entered:
            logger.debug("Skip token=%s: already entered this session", token_id)
            leader_trades_skipped.add(1, {"reason": "already_entered", "wallet": wallet})
            return

        # ── Gate 2b: already entered the other side of this market ────────────
        # The leader buys both UP and DOWN tokens in the same slug. Once we enter
        # one side, lock the whole market — only one binary outcome can pay out.
        if market_slug and market_slug in self._entered_slugs:
            logger.info(
                "Skip token=%s: already entered other side of slug=%s",
                token_id, market_slug,
            )
            leader_trades_skipped.add(1, {"reason": "other_side_already_entered", "wallet": wallet})
            return

        # ── Gate 3: already have an open position (from a prior session run) ──
        if self.position_book.has_position(token_id):
            logger.debug("Skip token=%s: position already open in book", token_id)
            leader_trades_skipped.add(1, {"reason": "already_have_position", "wallet": wallet})
            return

        # ── Gate 4: sequence check ────────────────────────────────────────────
        seq = self._buy_seq.get(token_id, 0)
        if seq < _MOM_SEQ_MIN:
            logger.debug(
                "Skip token=%s: seq=%d < min=%d (waiting for more leader conviction)",
                token_id, seq, _MOM_SEQ_MIN,
            )
            leader_trades_skipped.add(1, {"reason": "seq_too_low", "wallet": wallet})
            return

        # ── Gate 5: momentum threshold ────────────────────────────────────────
        first_price = self._first_price.get(token_id)
        if first_price is None or first_price <= 0:
            logger.debug("Skip token=%s: no valid first_price recorded", token_id)
            leader_trades_skipped.add(1, {"reason": "no_first_price", "wallet": wallet})
            return

        rel_price = price / first_price
        if rel_price < (1.0 + _MOM_THR):
            logger.debug(
                "Skip token=%s: rel_price=%.3f < threshold=%.3f (price not rising enough)",
                token_id, rel_price, 1.0 + _MOM_THR,
            )
            leader_trades_skipped.add(1, {"reason": "mom_threshold_not_met", "wallet": wallet})
            return

        # ── Gate 6a: time-of-day block (US market open dead zone) ────────────
        # Backtest shows WR=53-59% and E[PnL]=-14 to -18% during 9am-12pm ET.
        # Leader's momentum signal breaks down during high-volatility open hours.
        hour_utc = int((now_ts % 86400) // 3600)
        if _MOM_BLOCK_UTC_START <= hour_utc < _MOM_BLOCK_UTC_END:
            logger.debug(
                "Skip token=%s: hour_utc=%d in dead zone [%d,%d)",
                token_id, hour_utc, _MOM_BLOCK_UTC_START, _MOM_BLOCK_UTC_END,
            )
            leader_trades_skipped.add(1, {"reason": "time_dead_zone"})
            return

        # ── Gate 6b: time remaining ───────────────────────────────────────────
        secs_rem = _secs_remaining(market_slug, now_ts)
        if secs_rem is not None and secs_rem < _MOM_SECS_MIN:
            logger.info(
                "Skip token=%s: only %.0fs remaining (slug=%s)", token_id, secs_rem, market_slug,
            )
            leader_trades_skipped.add(1, {"reason": "near_expiry", "wallet": wallet})
            return

        # ── Gate 7: price sanity + minimum price floor ────────────────────────
        # Reject tokens already in loser territory. The leader DCA's into losers
        # at low prices — their second buy at 0.11 after 0.10 looks like +10%
        # momentum but the token is already dead. All live losses were <0.20.
        if price <= 0 or price >= 1.0:
            logger.warning("Invalid price %.6f for token=%s; skip", price, token_id)
            leader_trades_skipped.add(1, {"reason": "bad_price"})
            return
        if price < 0.60:
            logger.debug(
                "Skip token=%s: price=%.3f below floor=0.60 (only enter confirmed leaders)",
                token_id, price,
            )
            leader_trades_skipped.add(1, {"reason": "price_below_floor"})
            return
        if price > 0.80:
            logger.debug(
                "Skip token=%s: price=%.3f above ceiling=0.80 (poor risk/reward)",
                token_id, price,
            )
            leader_trades_skipped.add(1, {"reason": "price_above_ceiling"})
            return

        # ── Gate 8: max concurrent open positions ─────────────────────────────
        n_open = len(list(self.position_book.all()))
        if n_open >= self._risk.cfg.max_open_positions:
            logger.info(
                "Skip token=%s: already at max_open_positions=%d (currently %d open)",
                token_id, self._risk.cfg.max_open_positions, n_open,
            )
            leader_trades_skipped.add(1, {"reason": "max_positions_reached"})
            return

        # ── Sizing: 2% of live wallet balance ─────────────────────────────────
        balance      = self._risk.wallet_balance_usdc

        # ── Gate 9: minimum balance floor ─────────────────────────────────────
        if balance < self._risk.cfg.min_balance_floor_usdc:
            logger.info(
                "Skip token=%s: balance=%.2f below floor=%.2f",
                token_id, balance, self._risk.cfg.min_balance_floor_usdc,
            )
            leader_trades_skipped.add(1, {"reason": "below_balance_floor"})
            return

        size_usdc    = round(balance * _MOM_SIZE_PCT, 2)
        size_usdc    = min(self._risk.cfg.min_notional,
                          min(size_usdc, self._risk.cfg.max_notional_per_trade))

        # ── Risk gate: hard cap ───────────────────────────────────────────────
        if not self._risk.can_open(size_usdc):
            logger.info(
                "Risk veto: cap reached (balance=%.2f open_notional=%.2f), skip token=%s",
                balance, self._risk._open_notional, token_id,
            )
            leader_trades_skipped.add(1, {"reason": "risk_reject"})
            return

        # ── Convert USDC notional → shares ───────────────────────────────────
        size_shares = round(max(size_usdc/ price, self._risk.cfg.min_size_shares) , 2)
        if size_shares <= 0:
            logger.warning("size_shares=%.4f <= 0 for token=%s; skip", size_shares, token_id)
            return

        # ── Aggressive cross: 1 tick above ask ───────────────────────────────
        price       = round(price, 2)
        adj_price   = min(round(price + 0.01, 2), 0.99)
        order_id    = None
        order_time  = datetime.now(timezone.utc)

        leader_trades_executed.add(1, {"wallet": wallet})

        # ── Place order (or dry-run) ──────────────────────────────────────────
        if self._live and (adj_price * size_shares > 1.0):
            order_id = await self._exec.place_open_order(
                token_id=token_id,
                side="BUY",
                price=adj_price,
                size=size_shares,
            )
            orders_placed.add(1, {"side": "BUY"})
        else:
            order_id = f"dry_{uuid.uuid4().hex[:12]}"
            logger.info(
                "DRY-RUN momentum BUY token=%s slug=%s seq=%d rel_price=%.3f "
                "price=%.4f adj_price=%.4f size_usdc=%.2f shares=%.4f id=%s",
                token_id, market_slug, seq, rel_price,
                price, adj_price, size_usdc, size_shares, order_id,
            )

        # ── Telemetry ─────────────────────────────────────────────────────────
        process_end     = time.time()
        total_delay     = max(0.0, process_end - trade_ts) if trade_ts > 0 else 0.0
        process_duration = process_end - process_start

        strategy_local_latency.record(process_duration * 1000.0)
        if trade_ts > 0:
            strategy_total_delay.record(total_delay * 1000.0)

        logger.info(
            "Momentum entry wallet=%s token=%s slug=%s seq=%d "
            "first_price=%.4f cur_price=%.4f rel=%.3f adj_price=%.4f "
            "size_usdc=%.2f shares=%.4f secs_rem=%s proc_dur=%.4fs "
            "total_delay=%.4fs order_id=%s",
            wallet, token_id, market_slug, seq,
            first_price, price, rel_price, adj_price,
            size_usdc, size_shares,
            f"{secs_rem:.0f}" if secs_rem is not None else "?",
            process_duration, total_delay, order_id,
        )

        # ── ClickHouse ────────────────────────────────────────────────────────
        if self._ch and order_id:
            self._ch.enqueue_my_order(
                order_time=order_time,
                order_id=order_id,
                token_id=token_id,
                side="BUY",
                price=adj_price,
                size=size_shares,
                order_type="GTC",
                strategy="momentum",
                source_leader=wallet,
                status="PENDING",
                latency_total_ms=total_delay * 1000.0 if trade_ts > 0 else None,
            )

        # ── Post-fill bookkeeping ─────────────────────────────────────────────
        is_live_fill = self._live and order_id and not order_id.startswith("dry_")

        if is_live_fill:
            orders_success.add(1, {"side": "BUY"})
            # NOTE: do NOT call register_open — it is never decremented because
            # positions settle at binary expiry (no explicit close).
            # Exposure is gated via the live balance poller in can_open().
            self.position_book.open_or_add(
                token_id=token_id,
                side="BUY",
                size=size_shares,
                price=adj_price,
                source_leader=wallet,
            )
        elif self._live:
            orders_failed.add(1, {"side": "BUY"})
            leader_trades_skipped.add(1, {"reason": "order_failed", "wallet": wallet})
            return  # don't mark entered if the order failed

        # Mark token as entered so we don't double-enter this session,
        # and lock the entire market slug so the other side can't fire.
        self._mom_entered.add(token_id)
        if market_slug:
            self._entered_slugs.add(market_slug)

    # ── Shutdown helper ───────────────────────────────────────────────────────

    async def close_all_positions(self) -> None:
        """
        Best-effort close of all open positions at SIGINT / graceful shutdown.
        Momentum positions are intended to hold to binary settlement, so this
        is a safety net for very long-duration contracts only.
        """
        for pos in list(self.position_book.all()):
            token_id   = pos.token_id
            size       = pos.size
            price_hint = pos.avg_entry_price or 0.50

            logger.info(
                "Shutdown close: token=%s side=%s size=%.4f hint=%.4f",
                token_id, pos.side, size, price_hint,
            )
            await self._exec.place_close_order(
                token_id=token_id,
                side=pos.side,
                price=price_hint,
                size=size,
            )
            self.position_book.close_full(token_id)
