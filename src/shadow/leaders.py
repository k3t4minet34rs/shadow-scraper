from typing import Dict, Optional, Literal, Tuple
from datetime import datetime,timezone
import logging

from .types import LeaderState, LeaderPosition

logger = logging.getLogger(__name__)
import requests
# leaders.py
from opentelemetry import metrics
from .telemetry import get_meter

meter = get_meter()
leader_trades_observed = meter.create_counter("shadow_leader_trades_observed_total")
leader_trades_classified = meter.create_counter("shadow_leader_trades_classified_total")
BASE_DATA_API = "https://data-api.polymarket.com"
DEFAULT_POSITIONS_LIMIT = 500
TradeKind = Literal["NEW", "ADD", "CLOSE"]


def get_current_positions(
    user: str,
    *,
    condition_ids: str | None = None,
    event_ids: str | None = None,
    limit: int = DEFAULT_POSITIONS_LIMIT,
    offset: int = 0,
    sort_by: str = "CURRENT",
    sort_direction: str = "DESC",
    session: requests.Session | None = None,
):
    if session is None:
        session = requests.Session()

    params: dict[str, object] = {
        "user": user,
        "limit": limit,
        "offset": offset,
        "sortBy": sort_by,
        "sortDirection": sort_direction,
    }

    if condition_ids:
        params["market"] = condition_ids
    if event_ids:
        params["eventId"] = event_ids

    resp = session.get(f"{BASE_DATA_API}/positions", params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()




class LeaderManager:
    def __init__(self, leader_wallets: list[str]):
        self._leaders: Dict[str, LeaderState] = {
            w.lower(): LeaderState(wallet=w.lower(), sharpe=0.0)
            for w in leader_wallets
        }

    # ---------- basic helpers ----------

    def is_leader(self, wallet: str) -> bool:
        return wallet.lower() in self._leaders

    def _get_leader_state(self, wallet: str) -> LeaderState:
        return self._leaders[wallet.lower()]

    # ---------- hydration from Data API ----------

    def hydrate_positions_from_data_api(self) -> None:
        """
        Blocking call; run once at startup (wrap in asyncio.to_thread in async code).
        """
        for wallet, state in self._leaders.items():
            try:
                positions = get_current_positions(wallet)
            except Exception as e:
                logger.error("Failed to fetch positions for %s: %s", wallet, e)
                continue

            now = datetime.now(timezone.utc)
            state.positions.clear()

            for pos in positions:
                token_id = pos.get("asset")
                if not token_id:
                    continue

                size = float(pos.get("size", 0.0))
                if size == 0:
                    continue

                avg_price = float(pos.get("avgPrice", 0.0))
                cur_price = float(pos.get("curPrice", 0.0))
                outcome = (pos.get("outcome") or "").lower()

                # Treat non‑zero long as BUY; if you later need explicit shorts, adjust here.
                side = "BUY"

                state.positions[token_id] = LeaderPosition(
                    token_id=token_id,
                    side=side,
                    size=size,
                    avg_price=avg_price,
                    last_updated=now,
                    current_price=cur_price,
                )

            logger.info(
                "Hydrated %d positions for leader %s",
                len(state.positions),
                wallet,
            )

    # ---------- trade classification ----------

    def classify_trade(
        self,
        payload: dict,
    ) -> Optional[Tuple[str, str, float, float, str, TradeKind]]:
        """
        Returns:
          (wallet, token_id, size, price, side, kind)
        where kind is NEW / ADD / CLOSE.
        """
        wallet = (payload.get("proxyWallet") or "").lower()
        # logger.info("Wallet activity: %s", wallet)
        if not self.is_leader(wallet):
            return None
        leader_trades_observed.add(1, {"wallet": wallet})
        logger.info("Leader active: %s", wallet)

        token_id = payload.get("asset")
        if not token_id:
            return None

        size = float(payload.get("size", 0.0))
        price = float(payload.get("price", 0.0))
        side = (payload.get("side") or payload.get("direction") or "").upper()
        if side not in ("BUY", "SELL"):
            return None

        now = datetime.now(timezone.utc)
        leader_state = self._get_leader_state(wallet)
        cur_pos = leader_state.positions.get(token_id)
        logging.info("Leader current position: %s", cur_pos)
        # Very simple logic:
        # - If no existing position and side == BUY -> NEW
        # - If existing and side == BUY -> ADD
        # - If existing and side == SELL and size >= existing.size -> CLOSE
        # - Else: treat as ADD/partial reduce later; for MVP1 we log and ignore.

        if cur_pos is None:
            kind: TradeKind = "NEW"
            # Update our view as an open position.
            leader_state.positions[token_id] = LeaderPosition(
                token_id=token_id,
                side="BUY",
                size=size,
                avg_price=price,
                last_updated=now,
                current_price=price,
            )
        else:
            if side == "BUY":
                kind = "ADD"
                new_size = cur_pos.size + size
                new_avg_price = (
                    (cur_pos.avg_price * cur_pos.size + price * size) / new_size
                    if new_size > 0
                    else price
                )
                cur_pos.size = new_size
                cur_pos.avg_price = new_avg_price
                cur_pos.last_updated = now
                cur_pos.current_price = price
            else:  # side == "SELL"
                if size >= cur_pos.size:
                    kind = "CLOSE"
                    # Remove from map; position fully closed.
                    del leader_state.positions[token_id]
                else:
                    # Partial reduce: for MVP1, treat as CLOSE signal but keep some size.
                    kind = "CLOSE"
                    cur_pos.size -= size
                    cur_pos.last_updated = now
                    cur_pos.current_price = price

        logger.info(
            "Leader trade wallet=%s token=%s side=%s size=%.4f price=%.4f kind=%s",
            wallet, token_id, side, size, price, kind,
        )
        leader_trades_classified.add(1, {"wallet": wallet})

        return wallet, token_id, size, price, side, kind