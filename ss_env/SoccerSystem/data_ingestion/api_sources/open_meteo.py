"""
Open-Meteo weather ingestor.

Open-Meteo is free and needs no API key. It has two relevant endpoints:
- Archive API (historical):  https://archive-api.open-meteo.com/v1/archive
- Forecast API (future):     https://api.open-meteo.com/v1/forecast

This ingestor takes a list of "match contexts" (canonical match id, venue
lat/lon, kickoff time in UTC) and picks the right endpoint per match based
on whether kickoff is in the past or future relative to right now.

NOTE on field names: Open-Meteo has changed hourly variable names over
API versions (e.g. "weathercode" -> "weather_code"). This ingestor tries
the new name first and falls back to the old one, but if Open-Meteo's
response shape has changed since this was written, print(payload) on a
failing record to check actual field names and adjust HOURLY_VARS /
_safe_get_any below.

Usage:
    matches = [
        {"match_canonical_id": "match_abc123", "lat": 53.4631, "lon": -2.2913,
         "kickoff_utc": datetime(2026, 5, 10, 15, 0, tzinfo=timezone.utc)},
    ]
    ingestor = OpenMeteoWeatherIngestor()
    result = ingestor.run(store, matches=matches)
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_ingestion.base import Ingestor
from data_normalization.schemas import WeatherRecord

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# requesting both old and new weather-code names is harmless; Open-Meteo
# ignores params it doesn't recognize for a given endpoint version
HOURLY_VARS = "temperature_2m,precipitation,wind_speed_10m,relative_humidity_2m,weather_code,weathercode"

_WEATHERCODE_MAP = {
    0: "clear", 1: "mostly clear", 2: "partly cloudy", 3: "overcast",
    45: "fog", 48: "freezing fog",
    51: "light drizzle", 53: "drizzle", 55: "dense drizzle",
    61: "light rain", 63: "rain", 65: "heavy rain",
    71: "light snow", 73: "snow", 75: "heavy snow",
    80: "rain showers", 81: "rain showers", 82: "violent rain showers",
    95: "thunderstorm", 96: "thunderstorm w/ hail", 99: "thunderstorm w/ heavy hail",
}


class OpenMeteoWeatherIngestor(Ingestor):
    source_name = "open-meteo"

    def fetch(self, matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
        raw_records = []
        now = datetime.now(timezone.utc)

        for m in matches:
            kickoff: datetime = m["kickoff_utc"]
            url = ARCHIVE_URL if kickoff < now else FORECAST_URL
            date_str = kickoff.strftime("%Y-%m-%d")

            params = {
                "latitude": m["lat"],
                "longitude": m["lon"],
                "start_date": date_str,
                "end_date": date_str,
                "hourly": HOURLY_VARS,
                "timezone": "UTC",
            }

            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()

            raw_records.append({
                "match_canonical_id": m["match_canonical_id"],
                "kickoff_utc": kickoff,
                "payload": resp.json(),
            })

        return raw_records

    def transform(self, raw_records: list[dict[str, Any]]) -> list[WeatherRecord]:
        out = []
        for rec in raw_records:
            hourly = rec["payload"].get("hourly", {})
            times = hourly.get("time", [])
            if not times:
                continue  # no data returned for this date/location; skip rather than fabricate

            target_hour = rec["kickoff_utc"].strftime("%Y-%m-%dT%H:00")
            if target_hour in times:
                idx = times.index(target_hour)
            else:
                # fall back to nearest available hour rather than dropping the record
                naive_kickoff = rec["kickoff_utc"].replace(tzinfo=None)
                idx = min(
                    range(len(times)),
                    key=lambda i: abs(datetime.fromisoformat(times[i]) - naive_kickoff),
                )

            weather_code = _safe_get(hourly, "weather_code", idx)
            if weather_code is None:
                weather_code = _safe_get(hourly, "weathercode", idx)

            out.append(WeatherRecord(
                source=self.source_name,
                match_canonical_id=rec["match_canonical_id"],
                temp_c=_safe_get(hourly, "temperature_2m", idx),
                precipitation_mm=_safe_get(hourly, "precipitation", idx),
                wind_kph=_safe_get(hourly, "wind_speed_10m", idx),  # km/h is Open-Meteo's default unit
                humidity_pct=_safe_get(hourly, "relative_humidity_2m", idx),
                condition=_weathercode_to_text(weather_code),
            ))
        return out


def _safe_get(hourly: dict, key: str, idx: int):
    values = hourly.get(key)
    if not values or idx >= len(values):
        return None
    return values[idx]


def _weathercode_to_text(code):
    if code is None:
        return None
    return _WEATHERCODE_MAP.get(int(code), f"code_{int(code)}")


if __name__ == "__main__":
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from data_storage.duckdb_store import DuckDBStore

    store = DuckDBStore("data/soccer_smoketest.duckdb")

    # Old Trafford coordinates, a date safely in the past so the Archive API is used
    test_matches = [{
        "match_canonical_id": "match_smoketest_001",
        "lat": 53.4631,
        "lon": -2.2913,
        "kickoff_utc": datetime(2026, 1, 10, 15, 0, tzinfo=timezone.utc),
    }]

    ingestor = OpenMeteoWeatherIngestor()
    result = ingestor.run(store, matches=test_matches)
    print(result)

    if result.records_stored:
        row = store.conn.execute(
            "SELECT * FROM weather_records WHERE match_canonical_id = ?",
            ["match_smoketest_001"],
        ).fetchone()
        print("Stored row:", row)

    store.close()