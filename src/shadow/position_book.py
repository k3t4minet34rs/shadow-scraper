from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Iterable


@dataclass
class MyPosition:
    token_id: str
    side: str          # "BUY" or "SELL"
    size: float        # shares
    avg_entry_price: float
    source_leader: str
    entry_time: datetime
    last_update: datetime


class PositionBook:
    """
    In-memory view of our own positions.
    Later you can:
      - add PnL fields,
      - reconcile with Data API / CLOB,
      - persist snapshots to ClickHouse.
    """

    def __init__(self) -> None:
        self._positions: Dict[str, MyPosition] = {}  # token_id -> MyPosition

    # ---------- basic queries ----------

    def has_position(self, token_id: str) -> bool:
        return token_id in self._positions

    def get(self, token_id: str) -> Optional[MyPosition]:
        return self._positions.get(token_id)

    def all(self) -> Iterable[MyPosition]:
        return self._positions.values()

    # ---------- mutation helpers ----------

    def open_or_add(
        self,
        *,
        token_id: str,
        side: str,
        size: float,
        price: float,
        source_leader: str,
    ) -> MyPosition:
        """
        For MVP1 treat everything as a fresh long; if we already
        have a position on this token, we average in on same side.
        """
        now = datetime.now(timezone.utc)
        side = side.upper()

        existing = self._positions.get(token_id)
        if existing is None:
            pos = MyPosition(
                token_id=token_id,
                side=side,
                size=size,
                avg_entry_price=price,
                source_leader=source_leader,
                entry_time=now,
                last_update=now,
            )
            self._positions[token_id] = pos
            return pos

        # Simple same-side add only
        if existing.side != side:
            # For now, just overwrite; later handle true netting.
            existing.side = side
            existing.size = size
            existing.avg_entry_price = price
            existing.source_leader = source_leader
            existing.entry_time = now
            existing.last_update = now
            return existing

        new_size = existing.size + size
        if new_size <= 0:
            # fully flat
            self._positions.pop(token_id, None)
            return existing

        new_avg = (existing.avg_entry_price * existing.size + price * size) / new_size
        existing.size = new_size
        existing.avg_entry_price = new_avg
        existing.last_update = now
        return existing

    def close_full(self, token_id: str) -> Optional[MyPosition]:
        """
        Remove position completely (after we believe we are flat).
        """
        return self._positions.pop(token_id, None)

    def close_partial(self, token_id: str, size: float) -> Optional[MyPosition]:
        """
        Reduce position size; if down to zero, remove.
        """
        pos = self._positions.get(token_id)
        if pos is None:
            return None

        new_size = pos.size - size
        if new_size <= 0:
            self._positions.pop(token_id, None)
            return pos

        pos.size = new_size
        pos.last_update = datetime.now(timezone.utc)
        return pos
