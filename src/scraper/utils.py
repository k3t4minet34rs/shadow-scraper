from datetime import datetime, timezone
from typing import Optional


def ts_from_ms(ms) -> Optional[str]:
    """Convert a millisecond epoch integer (or string) to ISO-8601 UTC string."""
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()
    except (ValueError, TypeError, OSError):
        return None


def ts_now() -> str:
    """Current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()
