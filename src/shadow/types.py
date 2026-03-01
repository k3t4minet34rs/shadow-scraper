from dataclasses import dataclass, field
from typing import Dict, Optional
from datetime import datetime


@dataclass
class LeaderPosition:
    token_id: str
    side: str  # "BUY" or "SELL"
    size: float
    avg_price: float
    last_updated: datetime
    current_price: float | None = None


@dataclass
class LeaderState:
    wallet: str
    sharpe: float
    positions: Dict[str, LeaderPosition] = field(default_factory=dict)


@dataclass
class MyPosition:
    token_id: str
    side: str
    size: float
    avg_entry_price: float
    source_leader: str
    entry_time: datetime
