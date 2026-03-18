# bikeraccoon

A Python package for tracking and querying bikeshare trip activity via [GBFS](https://gbfs.org/) feeds.

Bikeraccoon polls GBFS feeds, estimates trips from dock/bike availability changes, and stores the results as Parquet files. Data is exposed via a Python client, a Flask HTTP API, and a Dash web dashboard.

Live data is available at [raccoon.bike](https://raccoon.bike).

## Installation

```bash
git clone https://github.com/mjarrett/bikeraccoon
pip install -e bikeraccoon/
```

For bot/plotting support:
```bash
pip install -e bikeraccoon/[bot]
```

## Python API

```python
import bikeraccoon as br

# List available systems
br.get_systems()

# Connect to a system
api = br.LiveAPI('mobi_vancouver')

# Trip activity — returns a DataFrame
api.get_trips(t1, t2, freq='d')           # all trips
api.get_station_trips(t1, t2, freq='h')   # docked bikes only
api.get_free_bike_trips(t1, t2, freq='h') # free-floating only

# Station info
api.get_stations()
```

`t1`/`t2` are Python `datetime` objects. `freq` is `'h'` (hourly), `'d'` (daily), or `'m'` (monthly).

## HTTP API

The API is served by the `bikeraccoon.api` Flask app. Endpoints:

| Endpoint | Description |
|---|---|
| `GET /systems` | List tracked systems |
| `GET /stations?system=` | Station info for a system |
| `GET /vehicles?system=` | Vehicle types |
| `GET /activity?system=&start=&end=&frequency=` | Trip activity |
| `GET /gbfs?system=` | Raw GBFS feed data |

## Architecture

- `bikeraccoon/gbfs/` — GBFS feed queries
- `bikeraccoon/tracker/` — polling daemon; detects trips; writes Parquet data
- `bikeraccoon/api/` — Flask HTTP API over the Parquet data
- `bikeraccoon/dashboard/` — Dash web dashboard
- `bikeraccoon/bot/` — social media bot utilities
