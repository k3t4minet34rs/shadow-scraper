"""
Polymarket CLOB order placement.
Wraps py-clob-client with simple buy-YES logic.
"""
import logging
import os

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.constants import POLYGON

logger = logging.getLogger(__name__)

BUY  = "BUY"
SELL = "SELL"


class OrderPlacer:
    def __init__(self, stake_usdc: float):
        self.stake = stake_usdc

        private_key = os.environ["POLY_PRIVATE_KEY"]
        funder      = os.environ["POLY_FUNDER"]

        self.client = ClobClient(
            host      = "https://clob.polymarket.com",
            chain_id  = POLYGON,
            key       = private_key,
            signature_type = 0,   # EOA wallet
            funder    = funder,
        )

        # derive API credentials from the private key
        creds = self.client.create_or_derive_api_creds()
        self.client.set_api_creds(creds)
        logger.info("CLOB client ready")

    async def buy_yes(self, yes_token_id: str, yes_ask: float) -> dict:
        """
        Place a GTC limit order to buy YES tokens at yes_ask price.
        size = stake / yes_ask  (number of tokens, Polymarket uses 1 token = $1 face value)
        """
        size = round(self.stake / yes_ask, 2)

        logger.info(f"Placing BUY YES  token={yes_token_id[:8]}..  "
                    f"price={yes_ask:.4f}  size={size:.2f}  stake=${self.stake:.2f}")
        try:
            resp = self.client.create_and_post_order(OrderArgs(
                token_id     = yes_token_id,
                price        = yes_ask,
                size         = size,
                side         = BUY,
                fee_rate_bps = 20,   # 2% Polymarket fee
            ), order_type=OrderType.GTC)

            order_id = resp.get("orderID") or resp.get("id", "unknown")
            logger.info(f"Order accepted: {order_id}")
            return {
                "success":  True,
                "order_id": order_id,
                "price":    yes_ask,
                "size":     size,
                "raw":      resp,
            }
        except Exception as exc:
            logger.error(f"Order failed: {exc}")
            return {
                "success": False,
                "error":   str(exc),
                "price":   yes_ask,
                "size":    size,
            }
