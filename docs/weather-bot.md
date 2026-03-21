# Weather Bot

Prediction market trading bot for Polymarket daily temperature events.

## Architecture

```
weather-cycle (cron, 4x daily)
│
├── step_discover()          — fetch active events from Polymarket, sync to DB
├── step_forecast_and_price()
│   ├── fetch_ensemble()     — ICON seamless 40-member ensemble (Open-Meteo)
│   ├── compute_fv()         — P(bucket) = fraction of members landing in it
│   ├── decide_trades()      — quarter-Kelly sizing
│   └── WeatherOrderPlacer   — CLOB BUY-YES order (neg_risk=True)
└── step_resolve()           — check expired events, log outcomes
```

### Source files

| File | Purpose |
|------|---------|
| `src/weather/main.py` | Entry point, cycle steps, CLI flags |
| `src/weather/discovery/polymarket.py` | Fetch events + prices from Gamma API |
| `src/weather/forecast/openmeteo.py` | ICON ensemble fetch (40 members) |
| `src/weather/pricer/fv.py` | Fair value = ensemble bucket probability |
| `src/weather/portfolio/sizer.py` | Quarter-Kelly stake sizing |
| `src/weather/executor/order.py` | CLOB order placement (negRisk=True) |
| `src/weather/store/db.py` | SQLite: events, markets, forecasts, predictions, orders, resolutions |
| `src/weather/calibration/wunderground.py` | Scrape Wunderground station history for bias calibration |

### Run commands

```bash
# Dry run — print edge tables, no orders
weather-cycle --dry-run --db data/weather.db

# Live run
weather-cycle --budget 200 --db data/weather.db

# Discover only (sync events, no forecast)
weather-cycle --discover-only --db data/weather.db
```

---

## Resolution mechanics

Polymarket resolves on **Wunderground's daily high** at the named airport ICAO station (e.g. VILK = Lucknow Chaudhary Charan Singh Airport). Key facts:

- Resolution = max temperature recorded by the station over the full local calendar day
- Rounded to **whole °C** (Wunderground displays integers)
- Data considered final ~24h after the date; no revisions after finalisation
- Resolution source URL format: `https://www.wunderground.com/history/daily/{path}`

---

## Model vs resolution source — known biases

### Airport station warm bias
Airport METAR stations (what Wunderground uses) run **1–3°C warmer** than NWP model grid cells:
- Tarmac / jet exhaust / building heat create a local heat island
- ICON seamless at ~7km resolution averages over a wider area that includes cooler surrounding terrain

**Effect:** Our model may systematically underestimate observed temperatures at airport stations. A predicted 29°C could resolve at 31°C.

### Cities most at risk
| City | Station | Expected bias | Notes |
|------|---------|--------------|-------|
| Lucknow | VILK | High (2–3°C) | Hot pre-monsoon season, urban airport |
| Delhi | VIDP | High | IGI airport, heavy UHI |
| Dubai | OMDB | Medium | Desert airport |
| Singapore | WSSS | Medium | Tropical, humid |
| Shanghai | ZSPD | Medium | Pudong is newer/more exposed |
| London | EGLC | Low (0.5°C) | City Airport, near Thames |
| Paris | LFPG | Low (0.5°C) | CDG is semi-rural |
| Warsaw | EPWA | Low | Northern Europe spring |

### What this means for edge tables
Large edges on tropical/hot cities (Lucknow, Dubai, Delhi) should be treated skeptically until bias is calibrated. European spring markets (Warsaw, Paris, London, Munich) are more trustworthy — the bias is small relative to the bucket width (1°C).

---

## Bias calibration (TODO)

**Goal:** For each station, compute `bias = mean(wunderground_high - icon_p50)` over N past days. Apply as a shift to the ensemble members before bucketing.

**Process:**
1. `fetch_batch()` in `calibration/wunderground.py` — scrape past 30–60 days of station highs
2. Compare against what ICON p50 would have predicted (fetch historical ensemble)
3. Compute per-station mean bias and standard deviation
4. Add `station_bias` column to `events` table
5. Shift all ensemble members by `station_bias` before calling `compute_fv()`

**Status:** Scraper built (`calibration/wunderground.py`). Historical ICON data not yet fetched. No corrections applied.

---

## Deployment (TODO)

Run as a cron job on EC2, 4x daily at 02, 08, 14, 20 UTC:

```
# /etc/cron.d/weather-cycle
0 2,8,14,20 * * * admin cd /home/admin/shadow-bot && /home/admin/.local/bin/poetry run weather-cycle --budget 200 --db data/weather.db >> /var/log/weather-cycle.log 2>&1
```

Or as a systemd timer (preferred — restarts on failure):

```ini
# /etc/systemd/system/weather-cycle.timer
[Unit]
Description=Weather prediction market cycle

[Timer]
OnCalendar=*-*-* 02,08,14,20:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

---

## Known issues / station codes

Stations still unknown (skipped by discovery):

| Station | City | Status |
|---------|------|--------|
| NZWN | Wellington | Coords added ✓ |
| LIMC | Milan | Coords added ✓ |
| LEMD | Madrid | Coords added ✓ |
| EPWA | Warsaw | Coords added ✓ |
| ZUCK | Chongqing | Coords added ✓ |
| ZBAA | Beijing | Coords added ✓ |
| ZHHH | Wuhan | Coords added ✓ |

Stations with empty `resolutionSource` (blank URL) appear occasionally — these events are silently skipped.

---

## Calibration log

_Track model accuracy here as data accumulates. Bias = Observed − ICON p50. Positive = model ran cold._

### 2026-03-20 (n=22 cities)

| City | Station | ICON p50 | Observed | Bias | Notes |
|------|---------|----------|----------|------|-------|
| Paris | LFPG | 16.4°C | 17.0°C | +0.6 | |
| Buenos Aires | SAEZ | 29.0°C | 28.0°C | -1.0 | |
| Miami | KMIA | 24.7°C | 24.0°C | -0.7 | Obs below bucket range — no winner |
| Tokyo | RJTT | 12.2°C | 13.0°C | +0.8 | |
| Shanghai | ZSPD | 14.9°C | 14.0°C | -0.9 | |
| Singapore | WSSS | 32.8°C | 33.0°C | +0.2 | |
| London | EGLC | 13.1°C | 14.0°C | +0.9 | |
| Sao Paulo | SBGR | 26.9°C | 26.0°C | -0.9 | |
| Seoul | RKSI | 9.1°C | 11.0°C | **+1.9** | |
| Toronto | CYYZ | 5.4°C | 5.0°C | -0.3 | |
| Seattle | KSEA | 13.3°C | 14.0°C | +0.7 | No buckets in DB (event added before markets upserted) |
| New York | KLGA | 11.9°C | 12.0°C | +0.1 | No buckets in DB |
| Dallas | KDAL | 32.7°C | 32.0°C | -0.7 | No buckets in DB |
| Atlanta | KATL | 23.6°C | 24.0°C | +0.4 | No buckets in DB |
| Chicago | KORD | 17.1°C | 22.0°C | **+4.9** | ⚠ Large outlier — warm front ICON missed |
| Ankara | LTAC | 7.6°C | 7.0°C | -0.6 | |
| Lucknow | VILK | 28.5°C | 26.0°C | **-2.5** | Airport warm bias hypothesis not confirmed |
| Munich | EDDM | 12.1°C | 14.0°C | **+1.9** | |
| Tel Aviv | LLBG | 21.1°C | 22.0°C | +0.9 | |
| Milan | LIMC | 14.9°C | 16.0°C | **+1.1** | |
| Madrid | LEMD | 12.8°C | 11.0°C | **-1.8** | |
| Warsaw | EPWA | 9.9°C | 11.0°C | **+1.1** | |

**Summary (n=22, excl. Chicago outlier):** mean bias = +0.2°C, σ = 1.1°C. ICON essentially unbiased on average. Chicago (+4.9°C) was a mesoscale warm event. Airport warm bias hypothesis not confirmed — Lucknow ran 2.5°C *cooler* than model. One day insufficient for calibration; need 30+ days per station before applying corrections.

---

## Edge table sample (2026-03-22)

From dry run — prices as of ~23:00 UTC 2026-03-20.

| City | Bucket | Model | Market | Edge |
|------|--------|-------|--------|------|
| Warsaw | 14°C | 82.5% | 27.5% | **+55.0%** |
| Seoul | 10°C | 52.5% | 2.5% | **+50.0%** |
| Paris | 14°C | 77.5% | 31.0% | **+46.5%** |
| Wellington | 17°C | 45.0% | 5.2% | **+39.8%** |
| Sao Paulo | 30°C | 70.0% | 27.0% | **+43.0%** |
| Milan | 10°C | 37.5% | 8.5% | **+29.0%** |
| Munich | 12°C | 62.5% | 28.5% | **+34.0%** |
| Buenos Aires | 25°C | 65.0% | 31.5% | **+33.5%** |

_Note: edges this large likely contain unquantified airport bias. Do not trade without calibration data._
