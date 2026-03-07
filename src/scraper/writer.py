import csv
import os
import time
from typing import Optional

from .schema import CSV_FIELDS
from .utils import ts_now


class Writer:
    """
    Buffered CSV writer. Each row is sparse — only supplied fields are filled.
    Flushes to disk every flush_interval_s seconds (not per-row) to avoid I/O issues
    when high-frequency streams (aggtrade, depth5) are active.
    """

    def __init__(self, path: str, flush_interval_s: float = 5.0):
        self.path             = path
        self.flush_interval_s = flush_interval_s
        self._last_flush      = time.time()

        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
        write_header = not os.path.exists(path) or os.path.getsize(path) == 0

        self._f = open(path, "a", newline="")
        self._w = csv.DictWriter(self._f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if write_header:
            self._w.writeheader()

    def write(
        self,
        source: str,
        ts_exchange: Optional[str] = None,
        slug: Optional[str] = None,
        **fields,
    ) -> None:
        row                = dict.fromkeys(CSV_FIELDS)   # all None (sparse)
        row["ts_local"]    = ts_now()
        row["ts_exchange"] = ts_exchange
        row["source"]      = source
        row["slug"]        = slug
        row.update(fields)
        self._w.writerow(row)

        now = time.time()
        if now - self._last_flush >= self.flush_interval_s:
            self._f.flush()
            self._last_flush = now

    def close(self) -> None:
        self._f.flush()
        self._f.close()
