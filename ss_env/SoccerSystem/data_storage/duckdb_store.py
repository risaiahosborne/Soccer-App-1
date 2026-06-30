"""
DuckDB storage layer.

Layout philosophy:
- One DuckDB file per environment (e.g. data/soccer.duckdb).
- Tables mirror the Pydantic schemas 1:1 (snake_case table names).
- All writes go through `upsert_models`, which takes a list of validated
  Pydantic instances and a conflict key, so ingestion code never writes
  raw SQL and can't drift from the schema.
- Raw/landing data should be written to `raw_<source>_<entity>` tables by
  ingestors BEFORE normalization, so you can always replay normalization
  without re-hitting the source. This module only manages the *normalized*
  (trusted) tables; add a parallel raw-table helper later if needed.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Sequence, Type, TypeVar

import duckdb
from pydantic import BaseModel

ModelT = TypeVar("ModelT", bound=BaseModel)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS teams (
    canonical_id    VARCHAR,
    source          VARCHAR NOT NULL,
    source_id       VARCHAR NOT NULL,
    name            VARCHAR NOT NULL,
    short_name      VARCHAR,
    country         VARCHAR,
    league          VARCHAR,
    founded_year    INTEGER,
    venue_name      VARCHAR,
    venue_lat       DOUBLE,
    venue_lon       DOUBLE,
    ingested_at     TIMESTAMP,
    PRIMARY KEY (source, source_id)
);

CREATE TABLE IF NOT EXISTS players (
    canonical_id    VARCHAR,
    source          VARCHAR NOT NULL,
    source_id       VARCHAR NOT NULL,
    name            VARCHAR NOT NULL,
    team_source_id  VARCHAR,
    position        VARCHAR,
    nationality     VARCHAR,
    date_of_birth   DATE,
    height_cm       DOUBLE,
    preferred_foot  VARCHAR,
    ingested_at     TIMESTAMP,
    PRIMARY KEY (source, source_id)
);

CREATE TABLE IF NOT EXISTS matches (
    canonical_id        VARCHAR,
    source               VARCHAR NOT NULL,
    source_id            VARCHAR NOT NULL,
    competition          VARCHAR NOT NULL,
    season                VARCHAR NOT NULL,
    kickoff_utc          TIMESTAMP NOT NULL,
    status                VARCHAR NOT NULL,
    home_team_source_id  VARCHAR NOT NULL,
    away_team_source_id  VARCHAR NOT NULL,
    home_score            INTEGER,
    away_score            INTEGER,
    venue_name            VARCHAR,
    referee               VARCHAR,
    matchday              INTEGER,
    ingested_at           TIMESTAMP,
    PRIMARY KEY (source, source_id)
);

CREATE TABLE IF NOT EXISTS xg_records (
    source                VARCHAR NOT NULL,
    match_canonical_id    VARCHAR NOT NULL,
    team_canonical_id     VARCHAR NOT NULL,
    xg                    DOUBLE,
    npxg                  DOUBLE,
    xga                   DOUBLE,
    shots                 INTEGER,
    shots_on_target       INTEGER,
    possession_pct        DOUBLE,
    ingested_at           TIMESTAMP,
    PRIMARY KEY (source, match_canonical_id, team_canonical_id)
);

CREATE TABLE IF NOT EXISTS odds_snapshots (
    source                VARCHAR NOT NULL,
    match_canonical_id    VARCHAR NOT NULL,
    bookmaker             VARCHAR NOT NULL,
    market                VARCHAR NOT NULL,
    selection              VARCHAR NOT NULL,
    line                   DOUBLE,
    price_decimal          DOUBLE NOT NULL,
    captured_at            TIMESTAMP NOT NULL,
    is_opening              BOOLEAN,
    is_closing               BOOLEAN,
    PRIMARY KEY (source, match_canonical_id, bookmaker, market, selection, captured_at)
);

CREATE TABLE IF NOT EXISTS squad_valuations (
    source                 VARCHAR NOT NULL,
    player_canonical_id    VARCHAR NOT NULL,
    team_canonical_id      VARCHAR,
    market_value           DOUBLE NOT NULL,
    currency                VARCHAR NOT NULL,
    valuation_date          DATE NOT NULL,
    ingested_at              TIMESTAMP,
    PRIMARY KEY (source, player_canonical_id, valuation_date)
);

CREATE TABLE IF NOT EXISTS player_ratings (
    source                  VARCHAR NOT NULL,
    player_canonical_id     VARCHAR NOT NULL,
    match_canonical_id      VARCHAR NOT NULL,
    rating                   DOUBLE NOT NULL,
    minutes_played            INTEGER,
    ingested_at                TIMESTAMP,
    PRIMARY KEY (source, player_canonical_id, match_canonical_id)
);

CREATE TABLE IF NOT EXISTS injury_reports (
    source                   VARCHAR NOT NULL,
    player_canonical_id      VARCHAR NOT NULL,
    team_canonical_id        VARCHAR,
    status                     VARCHAR NOT NULL,
    description                 VARCHAR,
    start_date                   DATE,
    expected_return                DATE,
    reported_at                     TIMESTAMP,
    PRIMARY KEY (source, player_canonical_id, reported_at)
);

CREATE TABLE IF NOT EXISTS weather_records (
    source                    VARCHAR NOT NULL,
    match_canonical_id        VARCHAR NOT NULL,
    temp_c                      DOUBLE,
    precipitation_mm               DOUBLE,
    wind_kph                         DOUBLE,
    humidity_pct                       DOUBLE,
    condition                            VARCHAR,
    ingested_at                            TIMESTAMP,
    PRIMARY KEY (source, match_canonical_id)
);

CREATE TABLE IF NOT EXISTS id_mappings (
    entity_type     VARCHAR NOT NULL,
    source          VARCHAR NOT NULL,
    source_id       VARCHAR NOT NULL,
    canonical_id    VARCHAR NOT NULL,
    confidence      DOUBLE,
    mapped_at       TIMESTAMP,
    PRIMARY KEY (entity_type, source, source_id)
);

CREATE TABLE IF NOT EXISTS quality_check_results (
    check_name      VARCHAR NOT NULL,
    table_name      VARCHAR NOT NULL,
    passed          BOOLEAN NOT NULL,
    rows_checked    INTEGER,
    rows_failed     INTEGER,
    details         VARCHAR,
    run_at          TIMESTAMP NOT NULL,
);
"""

# Maps Pydantic model class name -> (table_name, conflict_key_columns)
_MODEL_TABLE_MAP = {
    "Team": ("teams", ["source", "source_id"]),
    "Player": ("players", ["source", "source_id"]),
    "Match": ("matches", ["source", "source_id"]),
    "XGRecord": ("xg_records", ["source", "match_canonical_id", "team_canonical_id"]),
    "OddsSnapshot": ("odds_snapshots", ["source", "match_canonical_id", "bookmaker", "market", "selection", "captured_at"]),
    "SquadValuation": ("squad_valuations", ["source", "player_canonical_id", "valuation_date"]),
    "PlayerRating": ("player_ratings", ["source", "player_canonical_id", "match_canonical_id"]),
    "InjuryReport": ("injury_reports", ["source", "player_canonical_id", "reported_at"]),
    "WeatherRecord": ("weather_records", ["source", "match_canonical_id"]),
    "IDMapping": ("id_mappings", ["entity_type", "source", "source_id"]),
    "QualityCheckResult": ("quality_check_results", []),  # append-only log, no upsert key
}


def _coerce_value(v):
    """Make Pydantic values DuckDB-friendly (enums -> str, leave rest)."""
    if hasattr(v, "value") and not isinstance(v, (int, float, str, bool)):
        return v.value  # Enum
    return v


class DuckDBStore:
    def __init__(self, db_path: str | Path = "data/soccer.duckdb"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute(DDL_STATEMENTS)

    def close(self) -> None:
        self.conn.close()

    # -- generic upsert -----------------------------------------------------

    def upsert_models(self, models: Sequence[ModelT]) -> int:
        """
        Upsert a homogeneous list of validated Pydantic model instances.
        Returns the number of rows written. Raises if the model type isn't
        registered in _MODEL_TABLE_MAP, or if the list is empty/mixed-type.
        """
        if not models:
            return 0

        cls_name = type(models[0]).__name__
        if any(type(m).__name__ != cls_name for m in models):
            raise ValueError("upsert_models requires a homogeneous list of one model type")
        if cls_name not in _MODEL_TABLE_MAP:
            raise ValueError(f"No table mapping registered for model '{cls_name}'")

        table, conflict_cols = _MODEL_TABLE_MAP[cls_name]
        rows = [
            {k: _coerce_value(v) for k, v in m.model_dump().items()}
            for m in models
        ]
        columns = list(rows[0].keys())

        if not conflict_cols:
            # append-only table (e.g. quality_check_results)
            self.conn.executemany(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))})",
                [[r[c] for c in columns] for r in rows],
            )
            return len(rows)

        update_cols = [c for c in columns if c not in conflict_cols]
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        sql = (
            f"INSERT INTO {table} ({', '.join(columns)}) "
            f"VALUES ({', '.join('?' * len(columns))}) "
            f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET {set_clause}"
        )
        self.conn.executemany(sql, [[r[c] for c in columns] for r in rows])
        return len(rows)

    # -- convenience reads ----------------------------------------------------

    def query(self, sql: str, params: Sequence | None = None):
        """Run arbitrary SQL and return a DuckDB relation (use .df() / .fetchall())."""
        return self.conn.execute(sql, params or [])

    def table_row_count(self, table: str) -> int:
        return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def resolve_canonical_id(self, entity_type: str, source: str, source_id: str) -> str | None:
        row = self.conn.execute(
            "SELECT canonical_id FROM id_mappings WHERE entity_type = ? AND source = ? AND source_id = ?",
            [entity_type, source, source_id],
        ).fetchone()
        return row[0] if row else None


if __name__ == "__main__":
    # smoke test
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from data_normalization.schemas import Team

    store = DuckDBStore("data/soccer_smoketest.duckdb")
    t = Team(source="football-data.org", source_id="57", name="Arsenal FC",
             short_name="Arsenal", country="England", league="Premier League")
    n = store.upsert_models([t])
    print(f"Upserted {n} team row(s). Total teams: {store.table_row_count('teams')}")
    store.close()