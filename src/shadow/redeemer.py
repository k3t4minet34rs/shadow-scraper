"""
Auto-redemption worker — claims settled Polymarket positions every 5 minutes.

Uses Polymarket's gasless relay.  EOA needs ZERO POL — the relay pays gas.

Flow (each cycle):
  1. GET data-api.polymarket.com/positions  → positions held by our PROXY wallet
  2. For each position (size > 0) GET gamma-api.polymarket.com/markets
       to check resolved status + fetch condition_id / negRisk flag
  3. If resolved → _redeem_via_relay(condition_id, amounts, neg_risk)
       a. encode inner call  (NegRiskAdapter.redeemPositions  OR  CTF.redeemPositions)
       b. wrap in            ProxyWalletFactory.proxy([...])
       c. estimate gas       (pure simulation; works with zero POL balance)
       d. GET relay nonce    GET https://relayer-v2.polymarket.com/nonce
       e. build relay struct  b"rlx:" || from || to || data || ... || relay_address
       f. keccak256 + EIP-191 sign the struct
       g. POST to sign server https://builder-signing-server.vercel.app/sign → auth headers
       h. POST to relay      https://relayer-v2.polymarket.com/submit
       i. wait_for_transaction_receipt
  4. Skip condition_ids already redeemed this session

Wallet roles:
  POLY_API_PRIVATE_KEY  → EOA address (relay signer / key holder)
  WALLET_ADDRESS        → Polymarket PROXY wallet (holds positions, appears in UI)
                          NOT the same as EOA — the proxy is derived from it on-chain.

Private key is read once from env and stored in a private attribute.
It is never logged and never transmitted over the network (only the resulting
EIP-191 signature is sent to the relay).
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac as _hmac
import logging
import os
from datetime import UTC, datetime
from json import dumps

import httpx
from eth_account.messages import encode_defunct
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

logger = logging.getLogger(__name__)

# ── Network / timing ──────────────────────────────────────────────────────────

_POLYGON_RPC_URL  = os.environ.get("POLYGON_RPC_URL", "https://polygon.drpc.org")
_DATA_API_BASE    = "https://data-api.polymarket.com"
_REDEEM_INTERVAL  = 300.0   # seconds between redemption polls (5 min)

# ── Polymarket contract addresses (Polygon mainnet) ───────────────────────────

_NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
_CTF_ADDRESS      = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
_PROXY_FACTORY    = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052"
_USDC_ADDRESS     = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"

# ── Gasless relay endpoints / parameters ─────────────────────────────────────

_RELAY_URL        = "https://relayer-v2.polymarket.com"
_RELAY_HUB        = "0xD216153c06E857cD7f72665E0aF1d7D82172F494"
_RELAY_ADDRESS    = "0x7db63fe6d62eb73fb01f8009416f4c2bb4fbda6a"

_HASH_ZERO              = b"\x00" * 32
_FALLBACK_GAS_LIMIT_STR = "2000000"   # 2 M gas — used if estimation fails


# ── Builder auth headers (Level 2 HMAC — own credentials, isolated rate limit) ─

def _build_builder_headers(body: dict) -> dict:
    """
    Create POLY_BUILDER_* auth headers using registered builder credentials.

    Uses POLY_BUILDER_API_KEY / POLY_BUILDER_API_SECRET / POLY_BUILDER_API_PASSPHRASE —
    the credentials obtained from Polymarket's builder registration.  These give an
    isolated per-key rate limit on the relay instead of the shared pool.

    Signing logic mirrors polymarket-apis create_level_2_headers(builder=True):
        message = timestamp + "POST" + "/submit" + str(body).replace("'", '"')
        sig = base64url( HMAC-SHA256( base64url_decode(secret), message ) )
    """
    api_key    = os.environ["POLY_BUILDER_API_KEY"]
    secret     = os.environ["POLY_BUILDER_API_SECRET"]
    passphrase = os.environ["POLY_BUILDER_API_PASSPHRASE"]

    timestamp  = str(int(datetime.now(tz=UTC).timestamp()))
    # Match the library's str(body).replace("'", '"') — works because all values
    # in our relay body are strings (no Python True/False/None to worry about).
    body_str   = str(body).replace("'", '"')
    message    = (timestamp + "POST" + "/submit" + body_str).encode()
    raw_secret = base64.urlsafe_b64decode(secret)
    sig        = base64.urlsafe_b64encode(
        _hmac.new(raw_secret, message, hashlib.sha256).digest()
    ).decode()

    return {
        "POLY_BUILDER_SIGNATURE":  sig,
        "POLY_BUILDER_TIMESTAMP":  timestamp,
        "POLY_BUILDER_API_KEY":    api_key,
        "POLY_BUILDER_PASSPHRASE": passphrase,
    }


# ── Minimal ABIs (only the functions we call) ─────────────────────────────────

_NEG_RISK_ABI = [
    {
        "name": "redeemPositions",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "amounts",     "type": "uint256[]"},
        ],
        "outputs": [],
    }
]

_CTF_ABI = [
    {
        "name": "redeemPositions",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "collateralToken",    "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId",        "type": "bytes32"},
            {"name": "indexSets",          "type": "uint256[]"},
        ],
        "outputs": [],
    }
]

_PROXY_FACTORY_ABI = [
    {
        "name": "proxy",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {
                "name": "txns",
                "type": "tuple[]",
                "components": [
                    {"name": "typeCode", "type": "uint256"},
                    {"name": "to",      "type": "address"},
                    {"name": "value",   "type": "uint256"},
                    {"name": "data",    "type": "bytes"},
                ],
            }
        ],
        "outputs": [],
    }
]


# ── Relay signing struct ───────────────────────────────────────────────────────

def _build_relay_struct(
    from_address: str,
    to: str,
    encoded_data: str,    # hex-encoded proxy_factory calldata (with 0x prefix)
    gas_limit: str,
    nonce: str,
) -> bytes:
    """
    Build the raw bytes struct that is keccak256-hashed then EIP-191 signed
    for the Polymarket gasless relay (proxy-wallet / signature_type=1 variant).

    Layout (matches polymarket-apis create_proxy_struct):
        b"rlx:" | from(20) | to(20) | data(var) |
        tx_fee(32) | gas_price(32) | gas_limit(32) |
        nonce(32) | relay_hub(20) | relay_address(20)

    tx_fee and gas_price are always 0 — the relay covers them.
    """
    zero32 = (0).to_bytes(32, "big")
    raw_data = bytes.fromhex(encoded_data.removeprefix("0x"))
    return (
        b"rlx:"
        + bytes.fromhex(from_address[2:])
        + bytes.fromhex(to[2:])
        + raw_data
        + zero32                                # tx_fee  = 0
        + zero32                                # gas_price = 0
        + int(gas_limit).to_bytes(32, "big")   # gas_limit
        + int(nonce).to_bytes(32, "big")        # nonce
        + bytes.fromhex(_RELAY_HUB[2:])
        + bytes.fromhex(_RELAY_ADDRESS[2:])
    )


# ── Redeemer ──────────────────────────────────────────────────────────────────

class Redeemer:
    """
    Periodic background worker that redeems settled Polymarket positions
    through the gasless relay.  Requires env vars:

        POLY_API_PRIVATE_KEY  — private key (EOA derived from this)
        WALLET_ADDRESS        — Polymarket proxy wallet address
    """

    def __init__(self) -> None:
        self._w3 = Web3(Web3.HTTPProvider(_POLYGON_RPC_URL))
        self._w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        self._pk      = os.environ["POLY_API_PRIVATE_KEY"]
        self._account = self._w3.eth.account.from_key(self._pk)
        self._eoa     = self._account.address           # relay signer; NEVER logged

        # WALLET_ADDRESS is the Polymarket PROXY — do not confuse with EOA
        self._proxy   = Web3.to_checksum_address(os.environ["WALLET_ADDRESS"])

        self._neg_risk = self._w3.eth.contract(
            address=Web3.to_checksum_address(_NEG_RISK_ADAPTER), abi=_NEG_RISK_ABI
        )
        self._ctf = self._w3.eth.contract(
            address=Web3.to_checksum_address(_CTF_ADDRESS), abi=_CTF_ABI
        )
        self._proxy_factory = self._w3.eth.contract(
            address=Web3.to_checksum_address(_PROXY_FACTORY), abi=_PROXY_FACTORY_ABI
        )

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
            positions = await self._fetch_redeemable_positions()
        except Exception as exc:
            logger.warning("Redeemer: positions fetch failed: %s", exc)
            return

        if not positions:
            logger.debug("Redeemer: no redeemable positions this cycle")
            return

        logger.info("Redeemer: %d redeemable position(s) found", len(positions))

        for pos in positions:
            # All fields come directly from the positions API — no gamma lookup needed.
            condition_id = pos.get("conditionId", "")
            neg_risk     = bool(pos.get("negativeRisk", False))
            outcome_idx  = int(pos.get("outcomeIndex", 0))
            size         = float(pos.get("size", 0) or 0)

            if not condition_id or size <= 0:
                continue
            if condition_id in self._redeemed:
                logger.debug("Redeemer: already redeemed %s this session", condition_id)
                continue

            # For neg-risk markets the contract needs [yes_amount, no_amount].
            # For standard CTF we pass index sets [1, 2] — amounts are ignored.
            amounts: list[int]   = [0, 0]
            amounts[outcome_idx] = int(size * 1e6)

            logger.info(
                "Redeemer: claiming condition=%s size=%.4f outcome_idx=%d neg_risk=%s title=%r",
                condition_id, size, outcome_idx, neg_risk, pos.get("title", ""),
            )
            try:
                await asyncio.to_thread(
                    self._redeem_via_relay, condition_id, amounts, neg_risk
                )
                self._redeemed.add(condition_id)
                logger.info("Redeemer: redeemed %s", condition_id)
                # Brief pause between redemptions to stay within relay quota.
                await asyncio.sleep(6)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    # Parse "resets in X seconds" from response body if present
                    try:
                        reset = int(exc.response.json().get("resetSeconds", 60))
                    except Exception:
                        reset = 60
                    logger.warning(
                        "Redeemer: relay rate-limited, waiting %ds before next cycle", reset
                    )
                    await asyncio.sleep(reset + 2)
                    return   # skip remaining positions; retry next poll
                logger.error("Redeemer: relay HTTP error %s: %s", condition_id, exc, exc_info=True)
            except Exception as exc:
                logger.error(
                    "Redeemer: redeem failed %s: %s", condition_id, exc, exc_info=True
                )

    # ── gasless relay transaction ─────────────────────────────────────────────

    def _redeem_via_relay(
        self, condition_id: str, amounts: list[int], neg_risk: bool
    ) -> None:
        """Build, sign, and submit a redemption via Polymarket's gasless relay."""

        cid = bytes.fromhex(condition_id.removeprefix("0x"))

        # 1. Encode the inner contract call
        if neg_risk:
            inner_data: str = self._neg_risk.encode_abi(
                abi_element_identifier="redeemPositions",
                args=[cid, amounts],
            )
            target = _NEG_RISK_ADAPTER
        else:
            inner_data = self._ctf.encode_abi(
                abi_element_identifier="redeemPositions",
                args=[
                    Web3.to_checksum_address(_USDC_ADDRESS),
                    _HASH_ZERO,
                    cid,
                    [1, 2],
                ],
            )
            target = _CTF_ADDRESS

        # 2. Wrap in proxy_factory.proxy([{typeCode:1, to, value:0, data}])
        proxy_txn = {
            "typeCode": 1,
            "to":       Web3.to_checksum_address(target),
            "value":    0,
            "data":     inner_data,   # hex str with 0x prefix
        }
        encoded_txn: str = self._proxy_factory.encode_abi(
            abi_element_identifier="proxy",
            args=[[proxy_txn]],
        )

        # 3. Estimate gas (simulation — no POL required for eth_estimateGas)
        try:
            estimated = self._w3.eth.estimate_gas({
                "from": self._eoa,
                "to":   Web3.to_checksum_address(_PROXY_FACTORY),
                "data": encoded_txn,
            })
            gas_limit = str(int(estimated * 1.3) + 100_000)
        except Exception as exc:
            logger.warning("Redeemer: gas estimation failed (%s) — using fallback", exc)
            gas_limit = _FALLBACK_GAS_LIMIT_STR

        # 4. Fetch relay nonce
        nonce = self._get_relay_nonce()

        # 5. Build the relay signing struct and sign it
        struct     = _build_relay_struct(
            from_address=self._eoa,
            to=_PROXY_FACTORY,
            encoded_data=encoded_txn,
            gas_limit=gas_limit,
            nonce=str(nonce),
        )
        struct_hash = "0x" + self._w3.keccak(struct).hex()
        signature   = self._account.sign_message(
            encode_defunct(hexstr=struct_hash)
        ).signature.hex()

        # 6. Build relay body
        body = {
            "data":        encoded_txn,
            "from":        self._eoa,
            "metadata":    "redeem",
            "nonce":       str(nonce),
            "proxyWallet": self._proxy,
            "signature":   "0x" + signature,
            "signatureParams": {
                "gasPrice":   "0",
                "gasLimit":   gas_limit,
                "relayerFee": "0",
                "relayHub":   _RELAY_HUB,
                "relay":      _RELAY_ADDRESS,
            },
            "to":   _PROXY_FACTORY,
            "type": "PROXY",
        }

        # 7. Build builder auth headers locally (our own key — isolated rate limit).
        #    No call to the shared signing server; no shared quota contention.
        auth_headers = _build_builder_headers(body)

        # 8. Submit to relay
        relay_resp = httpx.post(
            f"{_RELAY_URL}/submit",
            headers=auth_headers,
            content=dumps(body).encode(),
            timeout=30.0,
        )
        relay_resp.raise_for_status()
        result = relay_resp.json()

        tx_hash = result.get("transactionHash")
        logger.info(
            "Redeemer: relay accepted tx=%s state=%s", tx_hash, result.get("state")
        )

        # 9. Wait for on-chain confirmation
        if not tx_hash:
            raise RuntimeError(f"No transactionHash in relay response: {result}")

        receipt = self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        if receipt["status"] != 1:
            raise RuntimeError(f"Relay tx reverted: {tx_hash}")
        logger.info("Redeemer: tx confirmed gas_used=%s", receipt["gasUsed"])

    # ── relay nonce ───────────────────────────────────────────────────────────

    def _get_relay_nonce(self) -> int:
        resp = httpx.get(
            f"{_RELAY_URL}/nonce",
            params={"address": self._eoa, "type": "PROXY"},
            timeout=10.0,
        )
        resp.raise_for_status()
        return int(resp.json()["nonce"])

    # ── Positions API helper ──────────────────────────────────────────────────

    async def _fetch_redeemable_positions(self) -> list[dict]:
        """
        Returns positions that are ready to claim (redeemable=True, cashPnl > 0).

        The positions API already provides conditionId, negativeRisk, outcomeIndex,
        and size — no secondary market lookup is required.
        """
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{_DATA_API_BASE}/positions",
                params={"user": self._proxy, "sortBy": "CASHPNL"},
            )
            resp.raise_for_status()
            all_positions: list[dict] = resp.json()

        return [
            p for p in all_positions
            if p.get("redeemable") is True and float(p.get("cashPnl", 0) or 0) > 0
        ]
