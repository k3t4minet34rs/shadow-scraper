"""
Polymarket discovery — fetch active daily temperature events, parse into DB rows.
"""
import logging
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

GAMMA_URL  = "https://gamma-api.polymarket.com"
TAG_DAILY_TEMP = "103040"   # Polymarket tag for daily temperature markets


# ── bucket parsing ────────────────────────────────────────────────────────────

def parse_bucket(label: str) -> dict:
    """
    Parse a groupItemTitle into (bucket_type, bucket_low, bucket_high).

    Examples:
      "14°C"            → exact,  low=14,   high=14
      "20°C or below"   → below,  low=None, high=20
      "25°C or above"   → above,  low=25,   high=None
      "21°C to 23°C"    → range,  low=21,   high=23   (rare but possible)
    """
    label = label.strip()

    # "X°C or below"
    m = re.match(r"(-?\d+)°C or below", label, re.IGNORECASE)
    if m:
        return {"bucket_type": "below", "bucket_low": None,
                "bucket_high": float(m.group(1))}

    # "X°C or above" / "X°C or higher"
    m = re.match(r"(-?\d+)°C or (?:above|higher)", label, re.IGNORECASE)
    if m:
        return {"bucket_type": "above", "bucket_low": float(m.group(1)),
                "bucket_high": None}

    # "X°C to Y°C"
    m = re.match(r"(-?\d+)°C to (-?\d+)°C", label, re.IGNORECASE)
    if m:
        return {"bucket_type": "range", "bucket_low": float(m.group(1)),
                "bucket_high": float(m.group(2))}

    # "X°C"  (exact)
    m = re.match(r"(-?\d+)°C$", label)
    if m:
        v = float(m.group(1))
        return {"bucket_type": "exact", "bucket_low": v, "bucket_high": v}

    logger.warning(f"Unrecognised bucket label: {label!r}")
    return {"bucket_type": "unknown", "bucket_low": None, "bucket_high": None}


# ── station / location extraction ─────────────────────────────────────────────

# Hardcoded coords for known Wunderground station codes.
# Add new cities as they appear.
STATION_COORDS: dict[str, dict] = {
    "ZSPD": {"city": "Shanghai",      "lat":  31.1443, "lon":  121.8083},
    "SAEZ": {"city": "Buenos Aires",  "lat": -34.8222, "lon":  -58.5358},
    "EGLL": {"city": "London",        "lat":  51.4775, "lon":   -0.4614},
    "EGLC": {"city": "London",        "lat":  51.5048, "lon":    0.0495},
    "KJFK": {"city": "New York",      "lat":  40.6413, "lon":  -73.7781},
    "KLGA": {"city": "New York",      "lat":  40.7772, "lon":  -73.8726},
    "KSFO": {"city": "San Francisco", "lat":  37.6213, "lon": -122.3790},
    "KSEA": {"city": "Seattle",       "lat":  47.4502, "lon": -122.3088},
    "KDAL": {"city": "Dallas",        "lat":  32.8481, "lon":  -96.8512},
    "KATL": {"city": "Atlanta",       "lat":  33.6407, "lon":  -84.4277},
    "KORD": {"city": "Chicago",       "lat":  41.9742, "lon":  -87.9073},
    "RJTT": {"city": "Tokyo",         "lat":  35.5494, "lon":  139.7798},
    "RKSI": {"city": "Seoul",         "lat":  37.4602, "lon":  126.4407},
    "OMDB": {"city": "Dubai",         "lat":  25.2532, "lon":   55.3657},
    "YSSY": {"city": "Sydney",        "lat": -33.9461, "lon":  151.1772},
    "EDDB": {"city": "Berlin",        "lat":  52.3667, "lon":   13.5033},
    "EDDM": {"city": "Munich",        "lat":  48.3538, "lon":   11.7861},
    "LFPG": {"city": "Paris",         "lat":  49.0097, "lon":    2.5479},
    "UUEE": {"city": "Moscow",        "lat":  55.9726, "lon":   37.4146},
    "LTAC": {"city": "Ankara",        "lat":  40.1281, "lon":   32.9951},
    "LLBG": {"city": "Tel Aviv",      "lat":  32.0114, "lon":   34.8867},
    "VIDP": {"city": "Delhi",         "lat":  28.5562, "lon":   77.1000},
    "VILK": {"city": "Lucknow",       "lat":  26.7606, "lon":   80.8893},
    "WSSS": {"city": "Singapore",     "lat":   1.3644, "lon":  103.9915},
    "KMIA": {"city": "Miami",         "lat":  25.7959, "lon":  -80.2870},
    "KLAX": {"city": "Los Angeles",   "lat":  33.9425, "lon": -118.4081},
    "CYYZ": {"city": "Toronto",       "lat":  43.6777, "lon":  -79.6248},
    "SBGR": {"city": "Sao Paulo",     "lat": -23.4356, "lon":  -46.4731},
    # Europe
    "LIMC": {"city": "Milan",         "lat":  45.6306, "lon":    8.7231},
    "LEMD": {"city": "Madrid",        "lat":  40.4983, "lon":   -3.5676},
    "EPWA": {"city": "Warsaw",        "lat":  52.1657, "lon":   20.9671},
    # Oceania
    "NZWN": {"city": "Wellington",    "lat": -41.3272, "lon":  174.8050},
    # China
    "ZUCK": {"city": "Chongqing",     "lat":  29.7192, "lon":  106.6422},
    "ZBAA": {"city": "Beijing",       "lat":  40.0799, "lon":  116.5853},
    "ZHHH": {"city": "Wuhan",         "lat":  30.7838, "lon":  114.2081},
    "ZGGG": {"city": "Guangzhou",     "lat":  23.3924, "lon":  113.2988},
    "ZUUU": {"city": "Chengdu",       "lat":  30.5785, "lon":  103.9469},
}

def extract_station(wunderground_url: str) -> tuple[str | None, dict | None]:
    """
    Extract station code from Wunderground URL.
    e.g. https://www.wunderground.com/history/daily/cn/shanghai/ZSPD
    → ("ZSPD", {"city": "Shanghai", "lat": ..., "lon": ...})
    """
    try:
        parts = urlparse(wunderground_url).path.rstrip("/").split("/")
        code  = parts[-1].upper()
        info  = STATION_COORDS.get(code)
        if info is None:
            logger.warning(f"Unknown station code {code!r} from {wunderground_url}")
        return code, info
    except Exception as e:
        logger.error(f"Failed to parse Wunderground URL {wunderground_url!r}: {e}")
        return None, None


def extract_target_date(end_date_iso: str) -> str:
    """
    end_date of each market is 12:00 UTC on the target date.
    e.g. "2026-03-22" → market resolves on March 22.
    """
    return end_date_iso[:10]   # just the date part


# ── API fetcher ───────────────────────────────────────────────────────────────

class PolymarketDiscovery:
    def __init__(self, timeout: float = 15.0):
        self._client = httpx.Client(
            base_url=GAMMA_URL,
            timeout=timeout,
            headers={"User-Agent": "weather-bot/0.1"},
        )

    def fetch_events(self, limit: int = 100) -> list[dict]:
        """Fetch all active daily temperature events from Polymarket."""
        params = {
            "active": "true",
            "closed": "false",
            "limit":  limit,
            "tag_id": TAG_DAILY_TEMP,
        }
        try:
            r = self._client.get("/events", params=params)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Polymarket fetch failed: {e}")
            return []

    def fetch_market_prices(self, market_ids: list[str]) -> dict[str, float]:
        """
        Fetch current YES ask prices for a list of market IDs.
        Returns {market_id: yes_ask}.
        """
        if not market_ids:
            return {}
        try:
            # Gamma API requires repeated id= params, not comma-joined
            params = [("id", mid) for mid in market_ids] + [("limit", len(market_ids))]
            r = self._client.get("/markets", params=params)
            r.raise_for_status()
            out = {}
            for m in r.json():
                prices = m.get("outcomePrices", "[]")
                if isinstance(prices, str):
                    import json; prices = json.loads(prices)
                if prices:
                    out[str(m["id"])] = float(prices[0])   # YES price
            return out
        except Exception as e:
            logger.error(f"Price fetch failed: {e}")
            return {}

    def close(self):
        self._client.close()


# ── parser: raw API response → DB rows ────────────────────────────────────────

def parse_event(raw: dict) -> tuple[dict | None, list[dict]]:
    """
    Parse one Polymarket event dict into:
      (event_row, [market_row, ...])

    Returns (None, []) if the event can't be parsed cleanly.
    """
    wu_url   = raw.get("resolutionSource", "")
    station, coords = extract_station(wu_url)
    if not station or not coords:
        return None, []

    end_date_iso = raw.get("endDate", "")[:10]
    target_date  = extract_target_date(end_date_iso)

    event_row = {
        "event_id":        str(raw["id"]),
        "slug":            raw.get("slug", ""),
        "title":           raw.get("title", ""),
        "city":            coords["city"],
        "station_code":    station,
        "wunderground_url": wu_url,
        "lat":             coords["lat"],
        "lon":             coords["lon"],
        "target_date":     target_date,
        "end_date":        raw.get("endDate", ""),
        "neg_risk_id":     raw.get("negRiskMarketID"),
    }

    market_rows = []
    for m in raw.get("markets", []):
        label = m.get("groupItemTitle", "").strip()
        if not label:
            continue

        token_ids = m.get("clobTokenIds", "[]")
        if isinstance(token_ids, str):
            import json; token_ids = json.loads(token_ids)
        if len(token_ids) < 2:
            continue

        # Skip Fahrenheit markets — our model is Celsius only
        if "°F" in label:
            continue

        bucket = parse_bucket(label)
        if bucket["bucket_type"] == "unknown":
            continue

        market_rows.append({
            "market_id":    str(m["id"]),
            "event_id":     str(raw["id"]),
            "question":     m.get("question", ""),
            "bucket_label": label,
            **bucket,
            "yes_token_id":  token_ids[0],
            "no_token_id":   token_ids[1],
            "condition_id":  m.get("conditionId"),
        })

    return event_row, market_rows
