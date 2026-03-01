import asyncio
import logging

from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

from .config import RiskConfig
from .position_book import PositionBook

logger = logging.getLogger(__name__)

_USDC_DECIMALS = 1_000_000  # USDC has 6 decimals on Polygon


class RiskEngine:
    def __init__(self, cfg: RiskConfig, position_book: PositionBook, clob_client=None):
        self.cfg = cfg
        self._open_notional = 0.0
        self._position_book = position_book
        self._clob_client = clob_client
        # Seeded from config as a safe fallback until the first poll completes.
        self._wallet_balance_usdc: float = cfg.max_capital_usdc

    # ---------- public properties ----------

    @property
    def wallet_balance_usdc(self) -> float:
        return self._wallet_balance_usdc

    # ---------- sizing / gate ----------

    def compute_size(self, bankroll_usdc: float) -> float:
        # naive 1% of bankroll, clamped
        target = bankroll_usdc * 0.01
        target = max(self.cfg.min_notional, min(target, self.cfg.max_notional_per_trade))
        return min(target, self.cfg.min_size_shares)

    def can_open(self, notional: float) -> bool:
        # Effective cap is the lower of live wallet balance and the config hard cap.
        effective_cap = min(self._wallet_balance_usdc, self.cfg.max_capital_usdc)
        return self._open_notional + notional <= effective_cap

    def register_open(self, notional: float) -> None:
        self._open_notional += notional

    # ---------- balance polling ----------

    def _fetch_balance_sync(self) -> float | None:
        """Blocking call; run via asyncio.to_thread."""
        if self._clob_client is None:
            return None
        try:
            params = BalanceAllowanceParams(
                AssetType.COLLATERAL,
                token_id=None,
                signature_type=1,
            )
            resp = self._clob_client.get_balance_allowance(params)
            raw = int(resp["balance"])
            return raw / _USDC_DECIMALS
        except Exception as e:
            logger.warning("Balance fetch failed: %s", e)
            return None

    async def run_balance_loop(self, interval_secs: float = 30.0) -> None:
        """
        Async loop that refreshes _wallet_balance_usdc from the CLOB every
        `interval_secs`. Run as a background task alongside the WS client.
        Fetch once immediately on start so can_open() has a real value before
        the first trade arrives.
        """
        while True:
            balance = await asyncio.to_thread(self._fetch_balance_sync)
            if balance is not None:
                old = self._wallet_balance_usdc
                self._wallet_balance_usdc = balance
                logger.info(
                    "Wallet balance updated: %.2f USDC (was %.2f USDC)",
                    balance,
                    old,
                )
            await asyncio.sleep(interval_secs)
