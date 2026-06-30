"""
Entity resolution / ID mapping across sources.

THE PROBLEM
------------
Every data source uses its own internal ID for the same real-world team
or player, and spells names differently:
    football-data.org : id=66,  name="Manchester United FC"
    API-Football       : id=33,  name="Manchester United"
    Transfermarkt       : id=985, name="Man Utd"
    FotMob               : id=8650, name="Man United"

Without a mapping layer, joins across xG / odds / ratings / valuations
silently fail or (worse) silently merge the wrong entities.

STRATEGY (deliberately conservative)
-------------------------------------
False merges are worse than missed merges — a false merge corrupts every
downstream join silently, while a missed merge just leaves a gap you can
spot and fix. So resolution is layered, cheapest/safest first:

1. EXACT LOOKUP   - if (source, source_id) already has a canonical_id in
                     id_mappings, return it immediately. No re-matching,
                     ever — once mapped, always mapped (override manually
                     if a mapping was wrong).
2. MANUAL OVERRIDE - a curated table of hand-verified matches. Always
                      wins over fuzzy logic.
3. FUZZY MATCH     - normalized name comparison (strip accents, legal
                      suffixes like "FC"/"CF"/"AFC", punctuation, case)
                      + rapidfuzz token-based score, optionally scoped by
                      country/league to cut down false positives.
4. THRESHOLDED DECISION:
       score >= AUTO_ACCEPT_THRESHOLD   -> auto-link, confidence=score
       REVIEW_THRESHOLD <= score < AUTO -> queued for manual review,
                                            NOT linked yet
       score < REVIEW_THRESHOLD          -> treated as a new entity

Players are harder than teams (more name collisions — "J. Silva" exists
many times), so player resolution also weights date_of_birth and
team_canonical_id when available, and has a stricter auto-accept bar.

Dependency: pip install rapidfuzz
"""

from __future__ import annotations

import re
import sys
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

from rapidfuzz import fuzz

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_storage.duckdb_store import DuckDBStore

AUTO_ACCEPT_THRESHOLD_TEAM = 90
REVIEW_THRESHOLD_TEAM = 75

AUTO_ACCEPT_THRESHOLD_PLAYER = 94
REVIEW_THRESHOLD_PLAYER = 82

_LEGAL_SUFFIXES = re.compile(
    r"\b(fc|cf|afc|sc|ac|cd|ud|sd|fk|bk|if|club|calcio|f\.c\.|c\.f\.)\b",
    re.IGNORECASE,
)
_PUNCT = re.compile(r"[^\w\s]")


def normalize_name(name: str) -> str:
    """Lowercase, strip accents/punctuation/legal suffixes, collapse whitespace."""
    if not name:
        return ""
    n = unicodedata.normalize("NFKD", name)
    n = "".join(c for c in n if not unicodedata.combining(c))
    n = n.lower()
    n = _LEGAL_SUFFIXES.sub(" ", n)
    n = _PUNCT.sub(" ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n
 
 
# Extra DDL for tables this module owns (beyond the core schema tables).
_EXTRA_DDL = """
CREATE TABLE IF NOT EXISTS canonical_teams (
    canonical_id    VARCHAR PRIMARY KEY,
    display_name    VARCHAR NOT NULL,
    normalized_name VARCHAR NOT NULL,
    country         VARCHAR,
    league          VARCHAR,
    created_at      TIMESTAMP DEFAULT current_timestamp
);
 
CREATE TABLE IF NOT EXISTS canonical_players (
    canonical_id    VARCHAR PRIMARY KEY,
    display_name    VARCHAR NOT NULL,
    normalized_name VARCHAR NOT NULL,
    date_of_birth   DATE,
    created_at      TIMESTAMP DEFAULT current_timestamp
);
 
CREATE TABLE IF NOT EXISTS manual_overrides (
    entity_type     VARCHAR NOT NULL,
    source          VARCHAR NOT NULL,
    source_id       VARCHAR NOT NULL,
    canonical_id    VARCHAR NOT NULL,
    PRIMARY KEY (entity_type, source, source_id)
);
 
CREATE TABLE IF NOT EXISTS entity_review_queue (
    review_id           VARCHAR PRIMARY KEY,
    entity_type          VARCHAR NOT NULL,
    source                VARCHAR NOT NULL,
    source_id             VARCHAR NOT NULL,
    candidate_name          VARCHAR NOT NULL,
    candidate_canonical_id  VARCHAR NOT NULL,
    candidate_display_name  VARCHAR NOT NULL,
    match_score                DOUBLE NOT NULL,
    status                       VARCHAR NOT NULL DEFAULT 'pending',  -- pending|approved|rejected
    created_at                    TIMESTAMP DEFAULT current_timestamp
);
"""
 
 
@dataclass
class ResolutionResult:
    canonical_id: Optional[str]
    status: str          # "exact" | "manual" | "fuzzy_auto" | "queued_for_review" | "new"
    confidence: float    # 0-100; 100 for exact/manual/new
 
 
class EntityResolver:
    def __init__(self, store: DuckDBStore):
        self.store = store
        self.store.conn.execute(_EXTRA_DDL)
 
    # -- public API -----------------------------------------------------
 
    def resolve_team(
        self,
        source: str,
        source_id: str,
        name: str,
        country: Optional[str] = None,
        league: Optional[str] = None,
    ) -> ResolutionResult:
        existing = self.store.resolve_canonical_id("team", source, source_id)
        if existing:
            return ResolutionResult(existing, "exact", 100.0)
 
        manual = self._check_manual_override("team", source, source_id)
        if manual:
            self._persist_mapping("team", source, source_id, manual, 100.0)
            return ResolutionResult(manual, "manual", 100.0)
 
        norm = normalize_name(name)
        candidates = self.store.conn.execute(
            "SELECT canonical_id, display_name, normalized_name, country, league "
            "FROM canonical_teams"
        ).fetchall()
 
        best_id, best_name, best_score = None, None, 0.0
        for cid, disp, cnorm, ccountry, cleague in candidates:
            score = fuzz.token_sort_ratio(norm, cnorm)
            # small bonus for matching country/league context, helps break ties
            if country and ccountry and country == ccountry:
                score += 3
            if league and cleague and league == cleague:
                score += 2
            if score > best_score:
                best_id, best_name, best_score = cid, disp, score
 
        if best_score >= AUTO_ACCEPT_THRESHOLD_TEAM:
            self._persist_mapping("team", source, source_id, best_id, best_score)
            return ResolutionResult(best_id, "fuzzy_auto", best_score)
 
        if best_score >= REVIEW_THRESHOLD_TEAM:
            self._queue_for_review("team", source, source_id, name, best_id, best_name, best_score)
            return ResolutionResult(None, "queued_for_review", best_score)
 
        new_id = self._create_canonical_team(name, norm, country, league)
        self._persist_mapping("team", source, source_id, new_id, 100.0)
        return ResolutionResult(new_id, "new", 100.0)
 
    def resolve_player(
        self,
        source: str,
        source_id: str,
        name: str,
        date_of_birth: Optional[date] = None,
    ) -> ResolutionResult:
        existing = self.store.resolve_canonical_id("player", source, source_id)
        if existing:
            return ResolutionResult(existing, "exact", 100.0)
 
        manual = self._check_manual_override("player", source, source_id)
        if manual:
            self._persist_mapping("player", source, source_id, manual, 100.0)
            return ResolutionResult(manual, "manual", 100.0)
 
        norm = normalize_name(name)
        candidates = self.store.conn.execute(
            "SELECT canonical_id, display_name, normalized_name, date_of_birth FROM canonical_players"
        ).fetchall()
 
        best_id, best_name, best_score = None, None, 0.0
        for cid, disp, cnorm, cdob in candidates:
            score = fuzz.token_sort_ratio(norm, cnorm)
            if date_of_birth and cdob:
                # DOB match is a near-certain confirmation; mismatch is a strong penalty
                if date_of_birth == cdob:
                    score += 10
                else:
                    score -= 25
            if score > best_score:
                best_id, best_name, best_score = cid, disp, score
 
        if best_score >= AUTO_ACCEPT_THRESHOLD_PLAYER:
            self._persist_mapping("player", source, source_id, best_id, best_score)
            return ResolutionResult(best_id, "fuzzy_auto", best_score)
 
        if best_score >= REVIEW_THRESHOLD_PLAYER:
            self._queue_for_review("player", source, source_id, name, best_id, best_name, best_score)
            return ResolutionResult(None, "queued_for_review", best_score)
 
        new_id = self._create_canonical_player(name, norm, date_of_birth)
        self._persist_mapping("player", source, source_id, new_id, 100.0)
        return ResolutionResult(new_id, "new", 100.0)
 
    def resolve_match(self, source: str, source_id: str) -> str:
        """Matches don't get fuzzy-matched across sources (yet) — each
        source's match id is trusted as already unique within that source.
        This just guarantees a stable canonical_id exists so other data
        (xG, odds, ratings, weather) can join against this match later.
 
        Cross-source match deduplication (e.g. matching the same fixture
        from two different odds providers by date+teams when there's no
        shared id) is a known gap — revisit once you're ingesting matches
        from more than one source.
        """
        existing = self.store.resolve_canonical_id("match", source, source_id)
        if existing:
            return existing
        new_id = f"match_{uuid.uuid4().hex[:12]}"
        self._persist_mapping("match", source, source_id, new_id, 100.0)
        return new_id
 
    def approve_review(self, review_id: str) -> Optional[str]:
        """Manually approve a queued match; links source_id to the candidate canonical_id."""
        row = self.store.conn.execute(
            "SELECT entity_type, source, source_id, candidate_canonical_id, match_score "
            "FROM entity_review_queue WHERE review_id = ? AND status = 'pending'",
            [review_id],
        ).fetchone()
        if not row:
            return None
        entity_type, source, source_id, canonical_id, score = row
        self._persist_mapping(entity_type, source, source_id, canonical_id, score)
        self.store.conn.execute(
            "UPDATE entity_review_queue SET status = 'approved' WHERE review_id = ?", [review_id]
        )
        return canonical_id
 
    def reject_review(self, review_id: str, create_new: bool = True) -> Optional[str]:
        """Reject a queued match. If create_new, mints a fresh canonical entity instead."""
        row = self.store.conn.execute(
            "SELECT entity_type, source, source_id, candidate_name FROM entity_review_queue "
            "WHERE review_id = ? AND status = 'pending'",
            [review_id],
        ).fetchone()
        if not row:
            return None
        entity_type, source, source_id, name = row
        self.store.conn.execute(
            "UPDATE entity_review_queue SET status = 'rejected' WHERE review_id = ?", [review_id]
        )
        if not create_new:
            return None
        norm = normalize_name(name)
        if entity_type == "team":
            new_id = self._create_canonical_team(name, norm, None, None)
        else:
            new_id = self._create_canonical_player(name, norm, None)
        self._persist_mapping(entity_type, source, source_id, new_id, 100.0)
        return new_id
 
    def pending_reviews(self) -> list[dict]:
        rows = self.store.conn.execute(
            "SELECT review_id, entity_type, source, source_id, candidate_name, "
            "candidate_display_name, match_score FROM entity_review_queue "
            "WHERE status = 'pending' ORDER BY created_at"
        ).fetchall()
        cols = ["review_id", "entity_type", "source", "source_id",
                "candidate_name", "candidate_display_name", "match_score"]
        return [dict(zip(cols, r)) for r in rows]
 
    def add_manual_override(self, entity_type: str, source: str, source_id: str, canonical_id: str) -> None:
        self.store.conn.execute(
            "INSERT INTO manual_overrides (entity_type, source, source_id, canonical_id) "
            "VALUES (?, ?, ?, ?) ON CONFLICT (entity_type, source, source_id) "
            "DO UPDATE SET canonical_id = EXCLUDED.canonical_id",
            [entity_type, source, source_id, canonical_id],
        )
 
    # -- internals --------------------------------------------------------
 
    def _check_manual_override(self, entity_type: str, source: str, source_id: str) -> Optional[str]:
        row = self.store.conn.execute(
            "SELECT canonical_id FROM manual_overrides WHERE entity_type = ? AND source = ? AND source_id = ?",
            [entity_type, source, source_id],
        ).fetchone()
        return row[0] if row else None
 
    def _persist_mapping(self, entity_type: str, source: str, source_id: str, canonical_id: str, confidence: float) -> None:
        self.store.conn.execute(
            "INSERT INTO id_mappings (entity_type, source, source_id, canonical_id, confidence, mapped_at) "
            "VALUES (?, ?, ?, ?, ?, current_timestamp) "
            "ON CONFLICT (entity_type, source, source_id) DO UPDATE SET "
            "canonical_id = EXCLUDED.canonical_id, confidence = EXCLUDED.confidence, mapped_at = EXCLUDED.mapped_at",
            [entity_type, source, source_id, canonical_id, confidence / 100.0],
        )
 
    def _queue_for_review(self, entity_type, source, source_id, name, cand_id, cand_name, score) -> None:
        self.store.conn.execute(
            "INSERT INTO entity_review_queue "
            "(review_id, entity_type, source, source_id, candidate_name, candidate_canonical_id, "
            "candidate_display_name, match_score) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [str(uuid.uuid4()), entity_type, source, source_id, name, cand_id, cand_name, score],
        )
 
    def _create_canonical_team(self, display_name, norm, country, league) -> str:
        new_id = f"team_{uuid.uuid4().hex[:12]}"
        self.store.conn.execute(
            "INSERT INTO canonical_teams (canonical_id, display_name, normalized_name, country, league) "
            "VALUES (?, ?, ?, ?, ?)",
            [new_id, display_name, norm, country, league],
        )
        return new_id
 
    def _create_canonical_player(self, display_name, norm, dob) -> str:
        new_id = f"player_{uuid.uuid4().hex[:12]}"
        self.store.conn.execute(
            "INSERT INTO canonical_players (canonical_id, display_name, normalized_name, date_of_birth) "
            "VALUES (?, ?, ?, ?)",
            [new_id, display_name, norm, dob],
        )
        return new_id
 
 
if __name__ == "__main__":
    # smoke test: same team, two sources, different spellings -> should merge
    store = DuckDBStore("data/soccer_smoketest.duckdb")
    resolver = EntityResolver(store)
 
    r1 = resolver.resolve_team("football-data.org", "66", "Manchester United FC",
                                country="England", league="Premier League")
    print("Source A ->", r1)
 
    r2 = resolver.resolve_team("transfermarkt", "985", "Man Utd",
                                country="England", league="Premier League")
    print("Source B ->", r2)
 
    assert r1.canonical_id is not None
    print("\nSame canonical_id?", r1.canonical_id == r2.canonical_id,
          f"({r1.canonical_id} vs {r2.canonical_id})")
 
    print("\nPending reviews:", resolver.pending_reviews())
    store.close()