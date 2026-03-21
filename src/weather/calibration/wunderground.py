"""
Wunderground historical station scraper.

Uses the weather.com internal observations API (key embedded in Wunderground
page source) to fetch hourly METAR observations for a given ICAO station and
date, then returns the daily high = max(hourly temps).

This exactly replicates what Polymarket uses to resolve temperature markets:
the highest temperature recorded at the named airport station on the target date.
"""
import logging
import time
from datetime import date, timedelta

import httpx

logger = logging.getLogger(__name__)

# API key embedded in Wunderground page source (weather.com internal)
_API_KEY  = "e1f10a1e78da46f5b10a1e78da96f525"
_BASE_URL = "https://api.weather.com/v1/location"

# ISO 3166-1 alpha-2 country codes per ICAO station
# Used to build the location string: {STATION}:9:{COUNTRY}
STATION_COUNTRY: dict[str, str] = {
    # Europe
    "EGLL": "GB", "EGLC": "GB",
    "LFPG": "FR",
    "EDDB": "DE", "EDDM": "DE",
    "UUEE": "RU",
    "LTAC": "TR",
    "LLBG": "IL",
    "LIMC": "IT",
    "LEMD": "ES",
    "EPWA": "PL",
    # Asia
    "ZSPD": "CN", "ZUCK": "CN", "ZBAA": "CN", "ZHHH": "CN",
    "ZGGG": "CN", "ZUUU": "CN",
    "RJTT": "JP",
    "RKSI": "KR",
    "OMDB": "AE",
    "VIDP": "IN", "VILK": "IN",
    "WSSS": "SG",
    # Americas
    "KJFK": "US", "KLGA": "US", "KSFO": "US", "KSEA": "US",
    "KDAL": "US", "KATL": "US", "KORD": "US", "KMIA": "US", "KLAX": "US",
    "CYYZ": "CA",
    "SBGR": "BR",
    "SAEZ": "AR",
    # Oceania
    "YSSY": "AU",
    "NZWN": "NZ",
}


def fetch_daily_high(station_code: str, target_date: date | str) -> float | None:
    """
    Fetch the daily high temperature (°C) for an ICAO station on a given date.

    Returns max(hourly METAR temps) — same as Wunderground's displayed daily
    high, and the value Polymarket uses for market resolution.

    Args:
        station_code: ICAO code, e.g. "LFPG", "VILK"
        target_date:  "YYYY-MM-DD" string or date object

    Returns:
        Daily high in °C (float), or None on failure.
    """
    if isinstance(target_date, str):
        target_date = date.fromisoformat(target_date)

    country = STATION_COUNTRY.get(station_code.upper())
    if not country:
        logger.warning(f"No country mapping for station {station_code}")
        return None

    date_str = target_date.strftime("%Y%m%d")
    url = (f"{_BASE_URL}/{station_code}:9:{country}"
           f"/observations/historical.json"
           f"?apiKey={_API_KEY}&units=m"
           f"&startDate={date_str}&endDate={date_str}")

    try:
        r = httpx.get(url, timeout=20.0,
                      headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        obs = r.json().get("observations", [])
    except Exception as e:
        logger.error(f"Wunderground API failed for {station_code} {target_date}: {e}")
        return None

    temps = [o["temp"] for o in obs if o.get("temp") is not None]
    if not temps:
        logger.warning(f"No temperature readings for {station_code} {target_date}")
        return None

    high = float(max(temps))
    logger.info(f"Wunderground {station_code} {target_date}: "
                f"high={high}°C  ({len(temps)} readings, "
                f"range=[{min(temps)}, {max(temps)}])")
    return high


def fetch_batch(
    events: list[dict],
    days_back: int = 60,
    request_delay: float = 1.0,
) -> list[dict]:
    """
    Fetch historical daily highs for a list of resolved events.

    Args:
        events:        list of event dicts with keys: station_code, target_date,
                       wunderground_url (from DB rows)
        days_back:     only fetch events within this many days of today
        request_delay: seconds between requests (avoids rate-limiting)

    Returns:
        list of {station_code, target_date, observed_high, wunderground_url}
    """
    today  = date.today()
    cutoff = today - timedelta(days=days_back)
    results = []

    for ev in events:
        station    = ev.get("station_code", "")
        target_str = ev.get("target_date", "")
        wu_url     = ev.get("wunderground_url", "")

        if not station or not target_str:
            continue

        try:
            target = date.fromisoformat(target_str)
        except ValueError:
            continue

        if target >= today or target < cutoff:
            continue   # skip future and too-old dates

        high = fetch_daily_high(station, target)
        results.append({
            "station_code":     station,
            "target_date":      target_str,
            "observed_high":    high,
            "wunderground_url": wu_url,
        })

        if request_delay > 0:
            time.sleep(request_delay)

    return results


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    station = sys.argv[1] if len(sys.argv) > 1 else "LFPG"
    d_str   = sys.argv[2] if len(sys.argv) > 2 else \
              (date.today() - timedelta(days=1)).isoformat()

    result = fetch_daily_high(station, d_str)
    print(f"\n{station}  {d_str}  daily high = {result}°C")
