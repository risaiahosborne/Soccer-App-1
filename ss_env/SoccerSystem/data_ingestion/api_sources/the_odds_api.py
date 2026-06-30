"""
The Odds API ingestor.

Docs: https://the-odds-api.com/liveapi/guides/v4/
Auth: query param apiKey=<key>
Free tier: ~500 credits/month. Cost per call = (# markets) x (# regions).
A single h2h-only, single-region call costs 1 credit. If no events are
returned, the call does NOT count against quota (per their docs), so
testing during an off-season window is "free" even if it comes back empty.

SECURITY: same pattern as the football-data.org ingestor — the API key
is read from an environment variable (ODDS_API_KEY), never hardcoded.

IMPORTANT CAVEAT: this endpoint only returns upcoming/in-play events,
not historical/finished ones. If you query a league that's between
seasons, you'll legitimately get zero events back — that's expected,
not a bug (see the football-data.org off-season issue we hit earlier).
The smoke test below defaults to the World Cup, which should have live
fixtures right now (June 2026) regardless of domestic league season
timing.

CROSS-SOURCE MATCH LINKING: same approach as the Understat scraper —
this API gives team names, not stable IDs, and its own event ids are
unrelated to football-data.org's. So: resolve team names via
EntityResolver (fuzzy match against canonical_teams), then look for an
existing canonical match by team-pair + close kickoff time before
minting a new one.

CLOSING LINE HEURISTIC: if a snapshot is captured within 1 hour of
kickoff, it's flagged is_closing=True. This is approximate — true CLV
analysis wants the actual last line before kickoff, which means running
this ingestor repeatedly (e.g. via a scheduler) and keeping the last
snapshot per match, not just running it once.

Dependencies: pip install requests
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import requests

from data_ingestion.base import Ingestor, IngestionResult
from data_normalization.schemas import OddsSnapshot, OddsMarket
from entity_resolution.id_mapper import EntityResolver

logger = logging.getLogger(__name__)

BASE_URL = "https://api.the-odds-api.com/v4"

_MARKET_MAP = {
    "h2h": OddsMarket.MATCH_1X2,
    "totals": OddsMarket.OVER_UNDER,
    "spreads": OddsMarket.ASIAN_HANDICAP,
    "btts": OddsMarket.BTTS,
}


def _map_selection(market_key: str, outcome_name: str, home_team: str, away_team: str) -> str:
    if market_key == "h2h":
        if outcome_name == home_team:
            return "home"
        if outcome_name == away_team:
            return "away"
        return "draw"
    if market_key == "totals":
        return outcome_name.lower()  # "over" / "under"
    if market_key == "spreads":
        return "home" if outcome_name == home_team else "away"
    return outcome_name.lower()


class TheOddsApiIngestor(Ingestor):
    source_name = "the-odds-api"

    def __init__(self, api_key: str, resolver: EntityResolver):
        if not api_key:
            raise ValueError("api_key is required (set ODDS_API_KEY)")
        self.api_key = api_key
        self.resolver = resolver

    def fetch(
        self,
        sport_keys: list[str],
        regions: str = "uk",
        markets: str = "h2h",
    ) -> list[dict[str, Any]]:
        all_events: list[dict[str, Any]] = []
        for sport_key in sport_keys:
            url = f"{BASE_URL}/sports/{sport_key}/odds"
            params = {
                "apiKey": self.api_key,
                "regions": regions,
                "markets": markets,
                "oddsFormat": "decimal",
            }
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            events = resp.json()
            for e in events:
                e["_sport_key"] = sport_key
            all_events.extend(events)
        return all_events

    def transform(self, raw_records: list[dict[str, Any]], store=None) -> list[OddsSnapshot]:
        out: list[OddsSnapshot] = []
        now = datetime.now(timezone.utc)

        for event in raw_records:
            home_team = event.get("home_team", "")
            away_team = event.get("away_team", "")
            commence_time = datetime.fromisoformat(event["commence_time"].replace("Z", "+00:00"))

            home_canonical = self.resolver.resolve_team(
                source=self.source_name, source_id=home_team, name=home_team,
            ).canonical_id
            away_canonical = self.resolver.resolve_team(
                source=self.source_name, source_id=away_team, name=away_team,
            ).canonical_id

            match_canonical_id = self._resolve_match_canonical_id(
                store, home_canonical, away_canonical, commence_time, event["id"]
            )
            if match_canonical_id is None:
                continue

            hours_to_kickoff = abs((commence_time - now).total_seconds()) / 3600.0
            is_closing = hours_to_kickoff <= 1.0

            for bookmaker in event.get("bookmakers", []):
                last_update = bookmaker.get("last_update")
                captured_at = (
                    datetime.fromisoformat(last_update.replace("Z", "+00:00"))
                    if last_update else now
                )
                for market in bookmaker.get("markets", []):
                    market_key = market.get("key", "h2h")
                    odds_market = _MARKET_MAP.get(market_key)
                    if odds_market is None:
                        continue  # unmapped market type; skip rather than guess
                    for outcome in market.get("outcomes", []):
                        out.append(OddsSnapshot(
                            source=self.source_name,
                            match_canonical_id=match_canonical_id,
                            bookmaker=bookmaker.get("key", "unknown"),
                            market=odds_market,
                            selection=_map_selection(market_key, outcome.get("name", ""), home_team, away_team),
                            line=outcome.get("point"),
                            price_decimal=outcome["price"],
                            captured_at=captured_at,
                            is_closing=is_closing,
                        ))
        return out

    def _resolve_match_canonical_id(
        self, store, home_canonical, away_canonical, kickoff, event_id
    ) -> Optional[str]:
        if home_canonical is None or away_canonical is None:
            return None

        if store is not None:
            row = store.conn.execute(
                """
                SELECT m.canonical_id
                FROM matches m
                JOIN id_mappings hm ON hm.entity_type = 'team' AND hm.source = m.source
                                    AND hm.source_id = m.home_team_source_id
                JOIN id_mappings am ON am.entity_type = 'team' AND am.source = m.source
                                    AND am.source_id = m.away_team_source_id
                WHERE hm.canonical_id = ? AND am.canonical_id = ?
                  AND abs(date_diff('hour', m.kickoff_utc, ?)) <= 12
                LIMIT 1
                """,
                [home_canonical, away_canonical, kickoff],
            ).fetchone()
            if row:
                return row[0]

        return self.resolver.resolve_match(source=self.source_name, source_id=str(event_id))

    def run(self, store, **kwargs) -> IngestionResult:
        """Overridden: transform() needs `store` for the cross-source match lookup."""
        try:
            raw = self.fetch(**kwargs)
        except Exception as e:
            logger.exception("fetch failed for %s", self.source_name)
            return IngestionResult(source=self.source_name, records_fetched=0,
                                    records_stored=0, errors=[f"fetch: {e}"])

        raw = self.validate_raw(raw)

        try:
            snapshots = self.transform(raw, store=store)
        except Exception as e:
            logger.exception("transform failed for %s", self.source_name)
            return IngestionResult(source=self.source_name, records_fetched=len(raw),
                                    records_stored=0, errors=[f"transform: {e}"])

        stored = 0
        errors: list[str] = []
        if snapshots:
            try:
                stored = store.upsert_models(snapshots)
            except Exception as e:
                logger.exception("store failed for %s", self.source_name)
                errors.append(f"store: {e}")

        return IngestionResult(
            source=self.source_name,
            records_fetched=len(raw),
            records_stored=stored,
            errors=errors,
        )


if __name__ == "__main__":
    import os
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from data_storage.duckdb_store import DuckDBStore

    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        print("Set ODDS_API_KEY as an environment variable first, e.g.:")
        print(r"  set ODDS_API_KEY=your_key_here   (Command Prompt)")
        sys.exit(1)

    store = DuckDBStore("data/soccer_smoketest.duckdb")
    resolver = EntityResolver(store)
    ingestor = TheOddsApiIngestor(api_key=api_key, resolver=resolver)

    # World Cup, h2h, one region -> cheapest possible test call (1 credit if
    # events exist, 0 credits if the call returns empty)
    result = ingestor.run(store, sport_keys=["soccer_fifa_world_cup"], regions="uk", markets="h2h")
    print(result)

    print("Total odds snapshots in store:", store.table_row_count("odds_snapshots"))
    store.close()