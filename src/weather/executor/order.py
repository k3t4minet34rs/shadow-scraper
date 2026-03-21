"""
Weather market order placer.

Weather events on Polymarket are negRisk markets — all temperature buckets
are mutually exclusive (sum to $1). Orders use neg_risk=True.
"""
import logging
import os

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, PartialCreateOrderOptions
from py_clob_client.order_builder.constants import BUY
from py_clob_client.constants import POLYGON

from ..portfolio.sizer import TradeInstruction

logger = logging.getLogger(__name__)

CLOB_HOST  = "https://clob.polymarket.com"
TICK_SIZE  = "0.01"


class WeatherOrderPlacer:
    def __init__(self):
        private_key = os.environ["POLY_API_PRIVATE_KEY"]
        funder      = os.environ["WALLET_ADDRESS"]

        # step 1: bare client to derive API creds
        temp  = ClobClient(CLOB_HOST, key=private_key, chain_id=POLYGON)
        creds = temp.create_or_derive_api_creds()

        # step 2: real client
        self.client = ClobClient(
            CLOB_HOST,
            key            = private_key,
            chain_id       = POLYGON,
            creds          = creds,
            signature_type = 1,
            funder         = funder,
        )
        logger.info("WeatherOrderPlacer CLOB client ready")

    def place(self, trade: TradeInstruction, yes_token_id: str,
              neg_risk: bool = True) -> dict:
        """
        Place a GTC limit BUY-YES order for a weather bucket.

        size = stake_usdc / price  (token count at face value $1)
        neg_risk=True because all temperature buckets are mutually exclusive.
        """
        size = round(trade.stake_usdc / trade.price, 2)

        logger.info(
            f"Placing weather order: {trade.bucket_label}  "
            f"token={yes_token_id[:10]}..  price={trade.price:.4f}  "
            f"size={size:.2f}  stake=${trade.stake_usdc:.2f}  "
            f"edge={trade.edge:+.1%}"
        )
        try:
            opts   = PartialCreateOrderOptions(tick_size=TICK_SIZE,
                                               neg_risk=neg_risk)
            args   = OrderArgs(token_id=yes_token_id,
                               price=trade.price, size=size, side=BUY)
            signed = self.client.create_order(args, opts)
            resp   = self.client.post_order(signed, orderType=OrderType.GTC)

            order_id = resp.get("orderID", "unknown")
            logger.info(f"Weather order accepted: {order_id}")
            return {
                "success":   True,
                "order_id":  order_id,
                "price":     trade.price,
                "size":      size,
                "stake":     trade.stake_usdc,
                "raw":       resp,
            }
        except Exception as exc:
            logger.error(f"Weather order failed: {exc}")
            return {
                "success": False,
                "error":   str(exc),
                "price":   trade.price,
                "size":    size,
                "stake":   trade.stake_usdc,
            }
