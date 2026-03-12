"""
Auto-redemption worker — claims settled Polymarket positions every 5 minutes.

Uses polymarket-apis library for position fetching and gasless relay redemption.

Flow (each cycle):
  1. PolymarketDataClient.get_positions(redeemable=True) → redeemable positions
  2. For each position with cash_pnl > 0:
       PolymarketGaslessWeb3Client.redeem_position(condition_id, amounts, neg_risk)
  3. Skip condition_ids already redeemed this session

Wallet roles:
  POLY_API_PRIVATE_KEY  → EOA private key (relay signer / key holder)
                          The proxy wallet address is derived on-chain from this key
                          by the gasless client — WALLET_ADDRESS is no longer required.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

import httpx

from polymarket_apis import ApiCreds, PolymarketDataClient, PolymarketGaslessWeb3Client

logger = logging.getLogger(__name__)

_REDEEM_INTERVAL = 300.0  # seconds between redemption polls (5 min)


class Redeemer:
    """
    Periodic background worker that redeems settled Polymarket positions
    through the gasless relay.  Requires env vars:

        POLY_API_PRIVATE_KEY           — private key (EOA)
        POLY_BUILDER_API_KEY           — builder API key (isolated rate limit)
        POLY_BUILDER_API_SECRET        — builder API secret
        POLY_BUILDER_API_PASSPHRASE    — builder API passphrase
    """

    def __init__(self) -> None:
        builder_creds = ApiCreds(
            key=os.environ["POLY_BUILDER_API_KEY"],
            secret=os.environ["POLY_BUILDER_API_SECRET"],
            passphrase=os.environ["POLY_BUILDER_API_PASSPHRASE"],
        )

        self._gasless_client = PolymarketGaslessWeb3Client(
            private_key=os.environ["POLY_API_PRIVATE_KEY"],
            builder_creds=builder_creds,
        )
        self._data_client = PolymarketDataClient()

        # Proxy wallet address derived on-chain by the gasless client from the private key.
        self._proxy = self._gasless_client.address

        self._redeemed: set[str] = set()
        self._task: Optional[asyncio.Task] = None

    # ── lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "Redeemer started (gasless relay, interval=%.0fs, proxy=%s)",
            _REDEEM_INTERVAL, self._proxy,
        )

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Redeemer stopped")

    # ── main loop ─────────────────────────────────────────────────────────────

    async def _loop(self) -> None:
        try:
            while True:
                await self._run_once()
                await asyncio.sleep(_REDEEM_INTERVAL)
        except asyncio.CancelledError:
            pass

    async def _run_once(self) -> None:
        try:
            logger.info("Redeemer: fetching redeemable positions for %s", self._proxy)
            positions = await asyncio.to_thread(
                self._data_client.get_positions,
                user=self._proxy,
                redeemable=True,
                size_threshold=0,
                sort_by="CASHPNL",
                limit=500,
            )
            logger.info("Redeemer: fetched %d positions", len(positions))
        except Exception as exc:
            logger.warning("Redeemer: positions fetch failed: %s", exc)
            return

        redeemable = [p for p in positions if p.cash_pnl > 0 and p.size > 0]
        logger.info("Redeemer: %d positions have positive cash_pnl", len(redeemable))
        if not redeemable:
            logger.debug("Redeemer: no redeemable positions this cycle")
            return

        logger.info("Redeemer: %d redeemable position(s) found", len(redeemable))

        for pos in redeemable:
            condition_id = pos.condition_id
            if condition_id in self._redeemed:
                logger.debug("Redeemer: already redeemed %s this session", condition_id)
                continue

            # Build amounts vector: [yes_shares, no_shares].
            # outcome_index=0 → Yes, outcome_index=1 → No.
            amounts = [0.0, 0.0]
            amounts[pos.outcome_index] = pos.size

            logger.info(
                "Redeemer: claiming condition=%s size=%.4f outcome=%s neg_risk=%s title=%r",
                condition_id, pos.size, pos.outcome, pos.negative_risk, pos.title,
            )
            try:
                receipt = await asyncio.to_thread(
                    self._gasless_client.redeem_position,
                    condition_id,
                    amounts,
                    pos.negative_risk,
                )
                self._redeemed.add(condition_id)
                logger.info(
                    "Redeemer: redeemed %s gas_used=%s status=%s",
                    condition_id, receipt.gas_used, receipt.status,
                )
                # Brief pause between redemptions to stay within relay quota.
                await asyncio.sleep(6)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    try:
                        reset = int(exc.response.json().get("resetSeconds", 60))
                    except Exception:
                        reset = 60
                    logger.warning(
                        "Redeemer: relay rate-limited, waiting %ds before next cycle", reset
                    )
                    await asyncio.sleep(reset + 2)
                    return  # skip remaining positions; retry next poll
                logger.error("Redeemer: relay HTTP error %s: %s", condition_id, exc, exc_info=True)
            except Exception as exc:
                logger.error(
                    "Redeemer: redeem failed %s: %s", condition_id, exc, exc_info=True
                )
