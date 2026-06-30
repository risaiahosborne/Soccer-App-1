"""
Unified canonical schemas for the soccer prediction system.

Every ingestion module's transformer must map raw source data into one of
these Pydantic models before it touches storage. This is the contract that
keeps multi-source data consistent.

Design notes:
- `canonical_id` fields are populated by the identity/id_mapper, NOT by
  ingestors. Ingestors populate `source_id` + `source` only.
- Every record carries `source` + `ingested_at` for lineage/debugging.
- Money fields store currency explicitly; never assume EUR/USD/GBP.
- Use `model_config = ConfigDict(extra="forbid")` everywhere so a malformed
  source mapping fails loudly at validation time instead of silently
  dropping/leaking fields downstream.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class MatchStatus(str, Enum):
    SCHEDULED = "scheduled"
    LIVE = "live"
    FINISHED = "finished"
    POSTPONED = "postponed"
    CANCELLED = "cancelled"


class OddsMarket(str, Enum):
    MATCH_1X2 = "1x2"
    OVER_UNDER = "over_under"
    BTTS = "btts"
    ASIAN_HANDICAP = "asian_handicap"
    DOUBLE_CHANCE = "double_chance"
    CORRECT_SCORE = "correct_score"


class InjuryStatus(str, Enum):
    INJURED = "injured"
    SUSPENDED = "suspended"
    DOUBTFUL = "doubtful"
    RETURNED = "returned"


# ---------------------------------------------------------------------------
# Core entities
# ---------------------------------------------------------------------------

class Team(StrictModel):
    canonical_id: Optional[str] = None      # filled by id_mapper
    source: str
    source_id: str
    name: str
    short_name: Optional[str] = None
    country: Optional[str] = None
    league: Optional[str] = None
    founded_year: Optional[int] = None
    venue_name: Optional[str] = None
    venue_lat: Optional[float] = None
    venue_lon: Optional[float] = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)


class Player(StrictModel):
    canonical_id: Optional[str] = None
    source: str
    source_id: str
    name: str
    team_source_id: Optional[str] = None     # resolved to canonical via mapper
    position: Optional[str] = None
    nationality: Optional[str] = None
    date_of_birth: Optional[date] = None
    height_cm: Optional[float] = None
    preferred_foot: Optional[str] = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)


class Match(StrictModel):
    canonical_id: Optional[str] = None
    source: str
    source_id: str
    competition: str
    season: str                              # e.g. "2025/2026"
    kickoff_utc: datetime
    status: MatchStatus
    home_team_source_id: str
    away_team_source_id: str
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    venue_name: Optional[str] = None
    referee: Optional[str] = None
    matchday: Optional[int] = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("season")
    @classmethod
    def season_format(cls, v: str) -> str:
        if "/" not in v:
            raise ValueError("season must be formatted like '2025/2026'")
        return v


class XGRecord(StrictModel):
    source: str
    match_canonical_id: str                  # must already be resolved
    team_canonical_id: str
    xg: Optional[float] = None
    npxg: Optional[float] = None
    xga: Optional[float] = None
    shots: Optional[int] = None
    shots_on_target: Optional[int] = None
    possession_pct: Optional[float] = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)


class OddsSnapshot(StrictModel):
    source: str
    match_canonical_id: str
    bookmaker: str
    market: OddsMarket
    selection: str                            # e.g. "home", "over_2.5", "draw"
    line: Optional[float] = None              # handicap/total line if relevant
    price_decimal: float
    captured_at: datetime
    is_opening: bool = False
    is_closing: bool = False

    @field_validator("price_decimal")
    @classmethod
    def price_sane(cls, v: float) -> float:
        if v < 1.01:
            raise ValueError("decimal odds must be >= 1.01")
        return v


class SquadValuation(StrictModel):
    source: str
    player_canonical_id: str
    team_canonical_id: Optional[str] = None
    market_value: float
    currency: str = "EUR"
    valuation_date: date
    ingested_at: datetime = Field(default_factory=datetime.utcnow)


class PlayerRating(StrictModel):
    source: str
    player_canonical_id: str
    match_canonical_id: str
    rating: float
    minutes_played: Optional[int] = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)


class InjuryReport(StrictModel):
    source: str
    player_canonical_id: str
    team_canonical_id: Optional[str] = None
    status: InjuryStatus
    description: Optional[str] = None
    start_date: Optional[date] = None
    expected_return: Optional[date] = None
    reported_at: datetime = Field(default_factory=datetime.utcnow)


class WeatherRecord(StrictModel):
    source: str
    match_canonical_id: str
    temp_c: Optional[float] = None
    precipitation_mm: Optional[float] = None
    wind_kph: Optional[float] = None
    humidity_pct: Optional[float] = None
    condition: Optional[str] = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Identity / quality framework support
# ---------------------------------------------------------------------------

class EntityType(str, Enum):
    TEAM = "team"
    PLAYER = "player"
    MATCH = "match"


class IDMapping(StrictModel):
    entity_type: EntityType
    source: str
    source_id: str
    canonical_id: str
    confidence: float = 1.0                  # 1.0 = exact/manual, <1 = fuzzy
    mapped_at: datetime = Field(default_factory=datetime.utcnow)


class QualityCheckResult(StrictModel):
    check_name: str
    table_name: str
    passed: bool
    rows_checked: int
    rows_failed: int = 0
    details: Optional[str] = None
    run_at: datetime = Field(default_factory=datetime.utcnow)