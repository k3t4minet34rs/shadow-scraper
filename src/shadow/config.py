from dataclasses import dataclass
from typing import List
from dataclasses import dataclass

@dataclass
class GeneralConfig:
    live: bool = False
    log_level: str = "INFO"



@dataclass
class LeaderConfig:
    wallets: List[str]
    min_sharpe: float = 30.0


@dataclass
class RiskConfig:
    max_capital_usdc: float = 70.0
    stop_at_equity_usdc: float = 10.0
    per_market_max_notional: float = 15.0
    min_notional: float = 1.0           # Polymarket updown minimum is $1
    max_notional_per_trade: float = 5.0
    min_size_shares: int = 5            # Polymarket updown minimum is 5 shares
    max_open_positions: int = 150         # at ~$3/trade × 15 = ~$45 max exposure on $100 bankroll
    min_balance_floor_usdc: float = 10.0  # never open if wallet drops below this


@dataclass
class WSConfig:
    activity_url: str = "wss://ws-live-data.polymarket.com"


@dataclass
class ClobConfig:
    host: str = "https://clob.polymarket.com"
    chain_id: int = 137



@dataclass
class AppConfig:
    general: GeneralConfig
    ws: WSConfig
    clob: ClobConfig
    leaders: LeaderConfig
    risk: RiskConfig
    # sizing, etc.
def load_config() -> AppConfig:
    # For now, hardcode; later read from YAML/TOML.
    return AppConfig(
        general=GeneralConfig(live=False),

        ws=WSConfig(),
        clob=ClobConfig(),
        leaders=LeaderConfig(
            wallets=[
                    # "0x96489abcb9f583d6835c8ef95ffc923d05a86825",
                    # "0xbacd00c9080a82ded56f504ee8810af732b0ab35",
                    # "0x7744bfd749a70020d16a1fcbac1d064761c9999e",
                    "0x1979ae6b7e6534de9c4539d0c205e582ca637c9d"

            ],
            min_sharpe=30.0,
        ),
        risk=RiskConfig(),
    )
