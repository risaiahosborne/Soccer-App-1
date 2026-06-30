"""
football-data.org matches/fixtures ingestor.

API docs: https://docs.football-data.org/ (v4)
Auth: header "X-Auth-Token: <your key>"
Free tier rate limit: 10 requests/minute — this ingestor sleeps between
calls when fetching multiple competitions in one run to stay under that.

SECURITY NOTE: never hardcode your API key in this file. Set it as an
environment variable (FOOTBALL_DATA_API_KEY) and read it at runtime —
see the __main__ block at the bottom for how this is loaded.

This ingestor does double duty vs the base Ingestor contract: besides
producing Match records, it also registers/resolves the home & away
teams via EntityResolver so canonical_teams gets populated as a
byproduct of ingesting matches. That's why it overrides run() instead of
relying on the generic base implementation — the base only knows how to
store one model type per ingestor run.

Common competition codes: PL (Premier League), PD (La Liga), BL1
(Bundesliga), SA (Serie A), FL1 (Ligue 1), CL (Champions League).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Optional

import requests

from data_ingestion.base import Ingestor, IngestionResult
from data_normalization.schemas import Match, MatchStatus, Team
from entity_resolution.id_mapper import EntityResolver

logger = logging.getLogger(__name__)

BASE_URL = "https://api.football-data.org/v4"

_STATUS_MAP = {
    "SCHEDULED": MatchStatus.SCHEDULED,
    "TIMED": MatchStatus.SCHEDULED,
    "IN_PLAY": MatchStatus.LIVE,
    "PAUSED": MatchStatus.LIVE,
    "FINISHED": MatchStatus.FINISHED,
    "POSTPONED": MatchStatus.POSTPONED,
    "SUSPENDED": MatchStatus.POSTPONED,
    "CANCELLED": MatchStatus.CANCELLED,
    "AWARDED": MatchStatus.FINISHED,
}


def _season_string(season_info: dict) -> str:
    start = season_info.get("startDate")
    end = season_info.get("endDate")
    if start and end:
        return f"{start[:4]}/{end[:4]}"
    return "unknown/unknown"


class FootballDataOrgIngestor(Ingestor):
    source_name = "football-data.org"

    def __init__(self, api_token: str, resolver: EntityResolver):
        if not api_token:
            raise ValueError("api_token is required (set FOOTBALL_DATA_API_KEY)")
        self._headers = {"X-Auth-Token": api_token}
        self.resolver = resolver
        self._team_models: list[Team] = []

    def fetch(
        self,
        competition_codes: list[str],
        season: Optional[str] = None,
        status: Optional[str] = None,
        matchday: Optional[int] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        all_matches: list[dict[str, Any]] = []

        for i, code in enumerate(competition_codes):
            if i > 0:
                time.sleep(6.5)  # stay under the free-tier 10 req/min limit

            params = {}
            if season:
                params["season"] = season
            if status:
                params["status"] = status
            if matchday:
                params["matchday"] = matchday
            if date_from:
                params["dateFrom"] = date_from
            if date_to:
                params["dateTo"] = date_to

            url = f"{BASE_URL}/competitions/{code}/matches"
            resp = requests.get(url, headers=self._headers, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            all_matches.extend(data.get("matches", []))

        return all_matches

    def validate_raw(self, raw_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        clean = []
        for r in raw_records:
            if not r.get("id") or not r.get("utcDate"):
                logger.warning("dropping match record missing id/utcDate: %s", r)
                continue
            clean.append(r)
        return clean

    def transform(self, raw_records: list[dict[str, Any]]) -> list[Match]:
        self._team_models = []
        teams_seen: set[str] = set()
        matches_out: list[Match] = []

        for raw in raw_records:
            comp = raw.get("competition") or {}
            season_info = raw.get("season") or {}
            home = raw.get("homeTeam") or {}
            away = raw.get("awayTeam") or {}
            score = (raw.get("score") or {}).get("fullTime") or {}
            referees = raw.get("referees") or []
            referee_name = referees[0].get("name") if referees else None

            comp_name = comp.get("name", "Unknown")

            for team in (home, away):
                tid = team.get("id")
                if tid is None:
                    continue
                tid_str = str(tid)
                if tid_str in teams_seen:
                    continue
                teams_seen.add(tid_str)

                team_resolution = self.resolver.resolve_team(
                    source=self.source_name,
                    source_id=tid_str,
                    name=team.get("name", ""),
                    league=comp_name,
                )
                self._team_models.append(Team(
                    canonical_id=team_resolution.canonical_id,
                    source=self.source_name,
                    source_id=tid_str,
                    name=team.get("name", ""),
                    short_name=team.get("shortName"),
                    league=comp_name,
                ))

            match_canonical_id = self.resolver.resolve_match(
                source=self.source_name, source_id=str(raw["id"])
            )

            matches_out.append(Match(
                canonical_id=match_canonical_id,
                source=self.source_name,
                source_id=str(raw["id"]),
                competition=comp_name,
                season=_season_string(season_info),
                kickoff_utc=datetime.fromisoformat(raw["utcDate"].replace("Z", "+00:00")),
                status=_STATUS_MAP.get(raw.get("status", ""), MatchStatus.SCHEDULED),
                home_team_source_id=str(home.get("id")),
                away_team_source_id=str(away.get("id")),
                home_score=score.get("home"),
                away_score=score.get("away"),
                venue_name=raw.get("venue"),
                referee=referee_name,
                matchday=raw.get("matchday"),
            ))

        return matches_out

    def run(self, store, **kwargs) -> IngestionResult:
        """Overridden (not using base.run()) because this ingestor stores
        two model types — Team and Match — not just the one the base
        class assumes."""
        try:
            raw = self.fetch(**kwargs)
        except Exception as e:
            logger.exception("fetch failed for %s", self.source_name)
            return IngestionResult(source=self.source_name, records_fetched=0,
                                    records_stored=0, errors=[f"fetch: {e}"])

        raw = self.validate_raw(raw)

        try:
            matches = self.transform(raw)
        except Exception as e:
            logger.exception("transform failed for %s", self.source_name)
            return IngestionResult(source=self.source_name, records_fetched=len(raw),
                                    records_stored=0, errors=[f"transform: {e}"])

        errors: list[str] = []
        stored = 0
        try:
            if self._team_models:
                store.upsert_models(self._team_models)
            if matches:
                stored = store.upsert_models(matches)
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

    api_key = os.environ.get("FOOTBALL_DATA_API_KEY")
    if not api_key:
        print("Set FOOTBALL_DATA_API_KEY as an environment variable first, e.g.:")
        print(r'  set FOOTBALL_DATA_API_KEY=your_key_here   (Command Prompt)')
        sys.exit(1)

    store = DuckDBStore("data/soccer_smoketest.duckdb")
    resolver = EntityResolver(store)
    ingestor = FootballDataOrgIngestor(api_token=api_key, resolver=resolver)

    # Premier League, 2025/26 season, just-finished matches — small & cheap.
    # Season is passed explicitly because the API's "current season" default
    # is ambiguous during the off-season window between seasons.
    result = ingestor.run(store, competition_codes=["PL"], season="2025", status="FINISHED", matchday=1)
    print(result)

    row_count = store.table_row_count("matches")
    print(f"Total matches in store: {row_count}")

    store.close()