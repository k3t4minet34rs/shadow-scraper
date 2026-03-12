"""
Polymarket CLOB order placement.
Wraps py-clob-client with simple buy-YES logic.
"""
import logging
import os

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, PartialCreateOrderOptions
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client.constants import POLYGON

logger = logging.getLogger(__name__)

CLOB_HOST = "https://clob.polymarket.com"


class OrderPlacer:
    def __init__(self, stake_usdc: float):
        self.stake = stake_usdc

        private_key = os.environ["POLY_API_PRIVATE_KEY"]
        funder      = os.environ["WALLET_ADDRESS"]

        # step 1: bare client to derive API creds (no signature_type/funder yet)
        temp   = ClobClient(CLOB_HOST, key=private_key, chain_id=POLYGON)
        creds  = temp.create_or_derive_api_creds()

        # step 2: real client with everything wired up
        self.client = ClobClient(
            CLOB_HOST,
            key            = private_key,
            chain_id       = POLYGON,
            creds          = creds,
            signature_type = 1,
            funder         = funder,
        )
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
            import asyncio
            loop   = asyncio.get_event_loop()
            opts   = PartialCreateOrderOptions(tick_size="0.01", neg_risk=False)
            args   = OrderArgs(token_id=yes_token_id, price=yes_ask, size=size, side=BUY)
            signed = await loop.run_in_executor(
                None, lambda: self.client.create_order(args, opts)
            )
            resp   = await loop.run_in_executor(
                None, lambda: self.client.post_order(signed, orderType=OrderType.GTC)
            )

            order_id = resp.get("orderID", "unknown")
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
