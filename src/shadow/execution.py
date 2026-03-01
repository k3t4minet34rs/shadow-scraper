import logging
import os
from typing import Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, PartialCreateOrderOptions, MarketOrderArgs
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client.constants import POLYGON

logger = logging.getLogger(__name__)

CLOB_BASE_URL = "https://clob.polymarket.com"
CHAIN_ID = POLYGON  # 137


class ExecutionClient:
    def __init__(self):
        private_key = os.environ["POLY_API_PRIVATE_KEY"]
        funder = os.environ["WALLET_ADDRESS"]

        # 1) temp client to derive or fetch API creds
        temp_client = ClobClient(CLOB_BASE_URL, key=private_key, chain_id=CHAIN_ID)
        api_creds = temp_client.create_or_derive_api_creds()

        # 2) main client with creds + funder + signature_type
        self._client = ClobClient(
            CLOB_BASE_URL,
            key=private_key,
            chain_id=CHAIN_ID,
            creds=api_creds,
            signature_type=1,  # 0 = EOA, 1 = proxy (as in your Rust design)
            funder=funder,
        )

    @property
    def client(self):
        """Expose the authenticated ClobClient for shared use (e.g. balance polling)."""
        return self._client
    async def place_open_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
    ) -> Optional[str]:
        # Entry: use FOK so you only copy if you can get the fill now.
        return await self._place_order_internal(
            token_id=token_id,
            side=side,
            price=price,
            size=size,
            fok=False,
        )

    async def place_close_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
    ) -> Optional[str]:
        # Exit: GTC taker order, safer than FOK for closing.
        return await self._place_order_internal(
            token_id=token_id,
            side=side,
            price=price,
            size=size,
            fok=False,
        )

    async def _place_order_internal(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        *,
        fok: bool,
    ) -> Optional[str]:
        side_const = BUY if side.upper() == "BUY" else SELL

        order_args = OrderArgs(
            side=side_const,
            token_id=token_id,
            price=price,
            size=size,
        )

        order_type = OrderType.FOK if fok else OrderType.GTC

        try:
            # optional: pass tick_size / neg_risk via PartialCreateOrderOptions
            opts = PartialCreateOrderOptions(
                tick_size="0.01",
                neg_risk=False,
            )

            signed = self._client.create_order(order_args, opts)
            resp = self._client.post_order(signed, orderType=order_type, post_only=False)

            order_id = resp["orderID"]
            logger.info(
                "Posted order id=%s token=%s side=%s size=%s price=%s type=%s",
                order_id, token_id, side, size, price, order_type,
            )
            return order_id
        except Exception as e:
            logger.error("Order failed: %s", e, exc_info=True)
            return None

    async def place_taker_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
    ) -> Optional[str]:
        order_args = OrderArgs(
            side=BUY if side.upper() == "BUY" else SELL,
            token_id=token_id,
            price=price,
            size=size,
        )
        try:
            # py-clob-client is sync; call in thread if you want non-blocking behavior
            signed = self._client.create_order(order_args)
            resp = self._client.post_order(signed)
            order_id = resp["orderId"]
            logger.info(
                "Posted order id=%s token=%s side=%s size=%s price=%s",
                order_id,
                token_id,
                side,
                size,
                price,
            )
            return order_id
        except Exception as e:
            logger.error("Order failed: %s", e, exc_info=True)
            return None
    async def close_position_market(
        self,
        token_id: str,
        side: str,
        size: float,
        price_hint: float,
    ) -> Optional[str]:
        """
        Naive close: send an order on the opposite side, crossing by 1 tick.
        """
        opposite_side = "SELL" if side.upper() == "BUY" else "BUY"
        adjust = 0.01
        close_price = (
            price_hint - adjust if opposite_side == "SELL" else price_hint + adjust
        )

        return await self.place_taker_order(
            token_id=token_id,
            side=opposite_side,
            price=close_price,
            size=size,
        )
    async def place_market_sell(
        self,
        token_id: str,
        size: float,
        price: float,
    ) -> Optional[str]:
        """
        Market sell `size` shares of token_id using FOK.

        Interpreted as: sell `size` shares at best available prices right now.
        """
        # py-clob-client expects MarketOrderArgs:
        # amount = shares for SELL, order_type determines FOK/FAK.
        order_args = MarketOrderArgs(
            token_id=token_id,
            amount=size,          # shares to sell
            side=SELL,
            price=price,
            order_type=OrderType.FOK,
        )

        try:
            # Let the client compute a marketable price from the book.
            signed = self._client.create_market_order(order_args)
            resp = self._client.post_order(signed, orderType=OrderType.FOK)

            order_id = resp["orderID"]
            logger.info(
                "Posted MARKET SELL id=%s token=%s size=%.4f type=%s",
                order_id,
                token_id,
                size,
                OrderType.FOK,
            )
            return order_id
        except Exception as e:
            logger.error("Market sell failed: %s", e, exc_info=True)
            return None
