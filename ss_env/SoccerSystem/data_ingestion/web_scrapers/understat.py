"""
Understat xG scraper.

Understat has no public API; xG data is embedded as JSON inside a
<script> tag on each league/season page, JS-escaped. This scraper
extracts and decodes that blob rather than parsing HTML tables — this
is the standard, widely-documented approach the soccer-analytics
community uses for this site (no auth bypass, no rate-limit evasion;
it's a plain GET against publicly served pages).

URL pattern: https://understat.com/league/{LeagueCode}/{season_start_year}
e.g. https://understat.com/league/EPL/2025  (the 2025/26 season)

This pulls "datesData" — the full match list for that league/season,
including each match's home/away team names, goals, and xG. It does
NOT include shots/npxG/possession (those live in a separate blob,
teamsData, keyed per-team rather than per-match-pair) — left as a
future enhancement; XGRecord.shots/npxg/possession_pct will be None
for now.

CROSS-SOURCE MATCH LINKING: Understat has its own internal match IDs,
unrelated to football-data.org's. To avoid creating duplicate "shadow"
matches, this ingestor first tries to find an *existing* canonical
match (e.g. one already ingested from football-data.org) by matching
home/away team canonical_ids plus a close kickoff datetime (a wide
36-hour window, since Understat's timestamps may not be in UTC and the
exact offset isn't well-documented). Only if no existing match is
found does it mint a new Understat-rooted canonical match id.

For best linking results: ingest matches (football-data.org) BEFORE
xG (Understat) for the same fixtures.

Dependencies: pip install requests beautifulsoup4 lxml
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Optional

import requests
from bs4 import BeautifulSoup

from data_ingestion.base import Ingestor, IngestionResult
from data_normalization.schemas import XGRecord
from entity_resolution.id_mapper import EntityResolver

logger = logging.getLogger(__name__)

LEAGUE_CODES = {
    "epl": "EPL",
    "premier_league": "EPL",
    "la_liga": "La_liga",
    "bundesliga": "Bundesliga",
    "serie_a": "Serie_A",
    "ligue_1": "Ligue_1",
    "rfpl": "RFPL",
}

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; soccer-research-bot/1.0)"}


def _extract_json_blob(script_text: str, var_name: str) -> Any:
    marker = f"var {var_name}"
    start_idx = script_text.index(marker)
    quote_start = script_text.index("('", start_idx) + 2
    quote_end = script_text.index("')", quote_start)
    raw = script_text[quote_start:quote_end]
    # Understat JS-escapes its embedded JSON (\xHH style); this is the
    # standard decode trick used across the community for this site.
    # Rare special characters in names may come through slightly
    # mangled — fix manually if you hit that on a specific team/player.
    decoded = raw.encode("utf-8").decode("unicode_escape")
    return json.loads(decoded)


def _parse_understat_datetime(raw: str) -> datetime:
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")


class UnderstatXGIngestor(Ingestor):
    source_name = "understat"

    def __init__(self, resolver: EntityResolver):
        self.resolver = resolver

    def fetch(self, league: str, season: str) -> list[dict[str, Any]]:
        league_code = LEAGUE_CODES.get(league.lower(), league)
        url = f"https://understat.com/league/{league_code}/{season}"

        resp = requests.get(url, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        for script in soup.find_all("script"):
            text = script.string or ""
            if "datesData" in text:
                dates_data = _extract_json_blob(text, "datesData")
                return [m for m in dates_data if m.get("isResult")]

        raise ValueError(
            "Could not find 'datesData' in the page — Understat's page "
            "structure may have changed since this scraper was written. "
            "Inspect the raw HTML/script tags to update the parsing logic."
        )

    def transform(self, raw_records: list[dict[str, Any]], store=None) -> list[XGRecord]:
        out: list[XGRecord] = []
        for raw in raw_records:
            home = raw.get("h", {})
            away = raw.get("a", {})
            xg = raw.get("xG", {})
            match_dt = _parse_understat_datetime(raw["datetime"])

            home_canonical = self.resolver.resolve_team(
                source=self.source_name, source_id=str(home.get("id")),
                name=home.get("title", ""),
            ).canonical_id
            away_canonical = self.resolver.resolve_team(
                source=self.source_name, source_id=str(away.get("id")),
                name=away.get("title", ""),
            ).canonical_id

            match_canonical_id = self._resolve_match_canonical_id(
                store, home_canonical, away_canonical, match_dt, raw["id"]
            )
            if match_canonical_id is None:
                continue  # team resolution failed; skip rather than guess

            for team_canonical, side in ((home_canonical, "h"), (away_canonical, "a")):
                xg_value = xg.get(side)
                out.append(XGRecord(
                    source=self.source_name,
                    match_canonical_id=match_canonical_id,
                    team_canonical_id=team_canonical,
                    xg=float(xg_value) if xg_value is not None else None,
                ))
        return out

    def _resolve_match_canonical_id(
        self, store, home_canonical, away_canonical, match_dt, understat_match_id
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
                  AND abs(date_diff('hour', m.kickoff_utc, ?)) <= 36
                LIMIT 1
                """,
                [home_canonical, away_canonical, match_dt],
            ).fetchone()
            if row:
                return row[0]

        # no existing match found from another source — mint a new
        # canonical id rooted in Understat's own match id
        return self.resolver.resolve_match(source=self.source_name, source_id=str(understat_match_id))

    def run(self, store, **kwargs) -> IngestionResult:
        """Overridden: transform() needs `store` for the cross-source match
        lookup, which the base class's generic run() doesn't pass through."""
        try:
            raw = self.fetch(**kwargs)
        except Exception as e:
            logger.exception("fetch failed for %s", self.source_name)
            return IngestionResult(source=self.source_name, records_fetched=0,
                                    records_stored=0, errors=[f"fetch: {e}"])

        raw = self.validate_raw(raw)

        try:
            records = self.transform(raw, store=store)
        except Exception as e:
            logger.exception("transform failed for %s", self.source_name)
            return IngestionResult(source=self.source_name, records_fetched=len(raw),
                                    records_stored=0, errors=[f"transform: {e}"])

        stored = 0
        errors: list[str] = []
        if records:
            try:
                stored = store.upsert_models(records)
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
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from data_storage.duckdb_store import DuckDBStore

    store = DuckDBStore("data/soccer_smoketest.duckdb")
    resolver = EntityResolver(store)
    ingestor = UnderstatXGIngestor(resolver=resolver)

    # Same league/season as the football-data.org test, so xG records
    # should link up to the matches already in the database.
    result = ingestor.run(store, league="EPL", season="2025")
    print(result)

    print("Total xG records in store:", store.table_row_count("xg_records"))
    store.close()