"""
Open-Meteo ECMWF ensemble forecast fetcher.
Docs: https://open-meteo.com/en/docs/ensemble-api
"""
import logging
from datetime import date

import httpx
import numpy as np

logger = logging.getLogger(__name__)

ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
MODEL        = "icon_seamless"   # ICON seamless ensemble — ~40 members (DWD)
# Note: ecmwf_ifs04 is the deterministic ECMWF model (no ensemble members).
# icon_seamless returns temperature_2m_max_member01..memberN per day.


def fetch_ensemble(lat: float, lon: float, target_date: date | str) -> list[float] | None:
    """
    Fetch ICON ensemble daily max temperature for a single location and date.

    Returns a list of per-member daily max temps (°C), or None on failure.
    Typically ~40 members for ICON seamless ensemble.

    Uses timezone=auto so the daily period matches the local calendar day,
    consistent with how Wunderground reports its daily high.
    """
    if isinstance(target_date, date):
        target_date = target_date.isoformat()

    params = {
        "latitude":         lat,
        "longitude":        lon,
        "daily":            "temperature_2m_max",
        "models":           MODEL,
        "start_date":       target_date,
        "end_date":         target_date,
        "timezone":         "auto",          # local calendar day = Wunderground day
        "temperature_unit": "celsius",
    }

    try:
        r = httpx.get(ENSEMBLE_URL, params=params, timeout=30.0)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logger.error(f"Open-Meteo fetch failed for ({lat},{lon}) {target_date}: {e}")
        return None

    # Response structure:
    # {"daily": {"time": [...], "temperature_2m_max": [ctrl],
    #            "temperature_2m_max_member01": [v], ...}}
    daily = data.get("daily", {})
    members = []
    for key, values in daily.items():
        if key.startswith("temperature_2m_max") and values:
            v = values[0]
            if v is not None:
                members.append(float(v))

    if not members:
        logger.warning(f"No ensemble members returned for ({lat},{lon}) {target_date}")
        return None

    logger.info(f"Fetched {len(members)} ensemble members for ({lat},{lon}) {target_date} "
                f"p10={np.percentile(members,10):.1f} "
                f"p50={np.percentile(members,50):.1f} "
                f"p90={np.percentile(members,90):.1f}")
    return members


def percentiles(members: list[float]) -> tuple[float, float, float]:
    """Returns (p10, p50, p90)."""
    a = np.array(members)
    return (float(np.percentile(a, 10)),
            float(np.percentile(a, 50)),
            float(np.percentile(a, 90)))
