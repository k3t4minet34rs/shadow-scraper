import json
import logging
from typing import Tuple

import requests

logger = logging.getLogger(__name__)

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"


def parse_clob_ids(clob_token_ids_str: str) -> Tuple[str, str]:
    """
    clobTokenIds looks like '["YES_TOKEN_ID","NO_TOKEN_ID"]'.
    """
    ids = json.loads(clob_token_ids_str)
    if not isinstance(ids, list) or len(ids) != 2:
        raise ValueError(f"Unexpected clobTokenIds format: {clob_token_ids_str!r}")
    yes_id, no_id = ids
    return yes_id, no_id


def get_complementary_token_id(event_slug: str, token_id: str) -> str:
    """
    Given an eventSlug and one clob token_id, return the complementary token_id.
    Assumes binary YES/NO style market.
    """
    url = f"{GAMMA_BASE_URL}/markets/slug/{event_slug}"
    resp = requests.get(url, timeout=5)
    resp.raise_for_status()
    market = resp.json()

    clob_token_ids_str = market.get("clobTokenIds")
    if not clob_token_ids_str:
        raise ValueError(f"No clobTokenIds in market for slug={event_slug}")

    yes_id, no_id = parse_clob_ids(clob_token_ids_str)

    if token_id == yes_id:
        return no_id
    if token_id == no_id:
        return yes_id

    raise ValueError(
        f"token_id {token_id} not in clobTokenIds for slug={event_slug}: {clob_token_ids_str}"
    )
