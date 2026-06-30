"""
Quality checks framework.

Runs against the *normalized* (trusted) DuckDB tables, after ingestion —
not against raw landing data. Each check is a small function that takes
the DuckDBStore and returns a QualityCheckResult, which gets persisted
to the quality_check_results table so you build a running history of
data health over time, not just a one-off terminal printout.

Design: checks are read-only + independent. A failing check does NOT
block anything by itself — this is a visibility tool first. Wire hard
gates (e.g. "stop the pipeline if >5% of odds prices are out of range")
into orchestration later, once you trust the thresholds.

Usage:
    from data_storage.duckdb_store import DuckDBStore
    from quality_checks.checks import QualityRunner

    store = DuckDBStore("data/soccer.duckdb")
    runner = QualityRunner(store)
    results = runner.run_all()
    print(runner.summary(results))
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

from data_normalization.schemas import QualityCheckResult
from data_storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)


@dataclass
class CheckSpec:
    name: str
    table: str
    fn: Callable[[DuckDBStore], QualityCheckResult]


# ---------------------------------------------------------------------------
# Generic, reusable check functions
# ---------------------------------------------------------------------------

def null_rate_check(store: DuckDBStore, table: str, column: str, max_null_pct: float = 0.0) -> QualityCheckResult:
    total = store.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if total == 0:
        return QualityCheckResult(
            check_name=f"null_rate:{column}", table_name=table,
            passed=True, rows_checked=0, rows_failed=0,
            details="table is empty; nothing to check",
        )
    nulls = store.conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL").fetchone()[0]
    pct = nulls / total
    passed = pct <= max_null_pct
    return QualityCheckResult(
        check_name=f"null_rate:{column}", table_name=table,
        passed=passed, rows_checked=total, rows_failed=nulls,
        details=f"{nulls}/{total} ({pct:.1%}) NULL, threshold {max_null_pct:.0%}",
    )


def range_check(
    store: DuckDBStore, table: str, column: str,
    min_val: Optional[float] = None, max_val: Optional[float] = None,
) -> QualityCheckResult:
    total = store.conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL").fetchone()[0]
    if total == 0:
        return QualityCheckResult(
            check_name=f"range:{column}", table_name=table,
            passed=True, rows_checked=0, rows_failed=0,
            details="no non-null values to check",
        )
    conditions = []
    if min_val is not None:
        conditions.append(f"{column} < {min_val}")
    if max_val is not None:
        conditions.append(f"{column} > {max_val}")
    if not conditions:
        raise ValueError("range_check requires at least one of min_val/max_val")
    where_clause = " OR ".join(conditions)
    failed = store.conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL AND ({where_clause})"
    ).fetchone()[0]
    passed = failed == 0
    return QualityCheckResult(
        check_name=f"range:{column}", table_name=table,
        passed=passed, rows_checked=total, rows_failed=failed,
        details=f"{failed}/{total} outside [{min_val}, {max_val}]",
    )


def duplicate_check(store: DuckDBStore, table: str, key_columns: list[str]) -> QualityCheckResult:
    """Note: for tables with a PRIMARY KEY already enforcing this (most of
    our normalized tables), this will trivially always pass — it earns its
    keep mainly on raw/landing tables that don't have a PK constraint."""
    cols = ", ".join(key_columns)
    total = store.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    dupe_groups = store.conn.execute(
        f"SELECT COUNT(*) FROM (SELECT {cols}, COUNT(*) c FROM {table} GROUP BY {cols} HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    passed = dupe_groups == 0
    return QualityCheckResult(
        check_name=f"duplicates:{'+'.join(key_columns)}", table_name=table,
        passed=passed, rows_checked=total, rows_failed=dupe_groups,
        details=f"{dupe_groups} duplicate key group(s) found" if dupe_groups else "no duplicates",
    )


def freshness_check(store: DuckDBStore, table: str, timestamp_column: str, max_age_hours: float) -> QualityCheckResult:
    total = store.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if total == 0:
        return QualityCheckResult(
            check_name=f"freshness:{timestamp_column}", table_name=table,
            passed=False, rows_checked=0, rows_failed=0,
            details="table is empty -- no data at all",
        )
    most_recent = store.conn.execute(f"SELECT MAX({timestamp_column}) FROM {table}").fetchone()[0]
    if most_recent is None:
        return QualityCheckResult(
            check_name=f"freshness:{timestamp_column}", table_name=table,
            passed=False, rows_checked=total, rows_failed=total,
            details="no non-null timestamps found",
        )
    age_hours = (datetime.now(timezone.utc) - most_recent.replace(tzinfo=timezone.utc)).total_seconds() / 3600
    passed = age_hours <= max_age_hours
    return QualityCheckResult(
        check_name=f"freshness:{timestamp_column}", table_name=table,
        passed=passed, rows_checked=total, rows_failed=0 if passed else total,
        details=f"most recent row is {age_hours:.1f}h old (threshold {max_age_hours}h)",
    )


def row_count_check(store: DuckDBStore, table: str, min_rows: int) -> QualityCheckResult:
    total = store.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    passed = total >= min_rows
    return QualityCheckResult(
        check_name="row_count", table_name=table,
        passed=passed, rows_checked=total, rows_failed=0 if passed else min_rows - total,
        details=f"{total} rows (minimum expected: {min_rows})",
    )


def referential_integrity_check(
    store: DuckDBStore, child_table: str, child_fk_column: str,
    parent_table: str, parent_key_column: str = "source_id",
) -> QualityCheckResult:
    """Checks every non-null FK value in child_table exists in parent_table,
    matched on the same `source` value. This is a same-source check (both
    tables must share the `source` column convention) — good for catching
    ingestion bugs, not a full cross-source join validator."""
    total = store.conn.execute(
        f"SELECT COUNT(*) FROM {child_table} WHERE {child_fk_column} IS NOT NULL"
    ).fetchone()[0]
    if total == 0:
        return QualityCheckResult(
            check_name=f"referential_integrity:{child_fk_column}", table_name=child_table,
            passed=True, rows_checked=0, rows_failed=0, details="no FK values to check",
        )
    orphans = store.conn.execute(
        f"""
        SELECT COUNT(*) FROM {child_table} c
        WHERE c.{child_fk_column} IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM {parent_table} p
            WHERE p.{parent_key_column} = c.{child_fk_column} AND p.source = c.source
          )
        """
    ).fetchone()[0]
    passed = orphans == 0
    return QualityCheckResult(
        check_name=f"referential_integrity:{child_fk_column}", table_name=child_table,
        passed=passed, rows_checked=total, rows_failed=orphans,
        details=f"{orphans}/{total} rows reference a missing {parent_table} row",
    )


# ---------------------------------------------------------------------------
# Registered check suite for this project's current tables.
# Add to this list as new tables/ingestors come online.
# ---------------------------------------------------------------------------

def default_check_suite() -> list[CheckSpec]:
    return [
        CheckSpec("teams_name_not_null", "teams",
                   lambda s: null_rate_check(s, "teams", "name", max_null_pct=0.0)),
        CheckSpec("teams_canonical_id_not_null", "teams",
                   lambda s: null_rate_check(s, "teams", "canonical_id", max_null_pct=0.0)),

        CheckSpec("matches_canonical_id_not_null", "matches",
                   lambda s: null_rate_check(s, "matches", "canonical_id", max_null_pct=0.0)),
        CheckSpec("matches_score_range_home", "matches",
                   lambda s: range_check(s, "matches", "home_score", min_val=0, max_val=20)),
        CheckSpec("matches_score_range_away", "matches",
                   lambda s: range_check(s, "matches", "away_score", min_val=0, max_val=20)),
        CheckSpec("matches_no_duplicates", "matches",
                   lambda s: duplicate_check(s, "matches", ["source", "source_id"])),
        CheckSpec("matches_referential_home_team", "matches",
                   lambda s: referential_integrity_check(s, "matches", "home_team_source_id", "teams")),
        CheckSpec("matches_referential_away_team", "matches",
                   lambda s: referential_integrity_check(s, "matches", "away_team_source_id", "teams")),

        CheckSpec("odds_price_range", "odds_snapshots",
                   lambda s: range_check(s, "odds_snapshots", "price_decimal", min_val=1.01, max_val=1000)),
        CheckSpec("odds_no_duplicates", "odds_snapshots",
                   lambda s: duplicate_check(s, "odds_snapshots",
                       ["source", "match_canonical_id", "bookmaker", "market", "selection", "captured_at"])),

        CheckSpec("xg_range", "xg_records",
                   lambda s: range_check(s, "xg_records", "xg", min_val=0, max_val=15)),

        CheckSpec("weather_temp_range", "weather_records",
                   lambda s: range_check(s, "weather_records", "temp_c", min_val=-50, max_val=60)),
    ]


class QualityRunner:
    def __init__(self, store: DuckDBStore, checks: Optional[list[CheckSpec]] = None):
        self.store = store
        self.checks = checks if checks is not None else default_check_suite()

    def run_all(self, persist: bool = True) -> list[QualityCheckResult]:
        results: list[QualityCheckResult] = []
        for spec in self.checks:
            try:
                result = spec.fn(self.store)
            except Exception as e:
                logger.exception("quality check '%s' raised an exception", spec.name)
                result = QualityCheckResult(
                    check_name=spec.name, table_name=spec.table,
                    passed=False, rows_checked=0, rows_failed=0,
                    details=f"check raised an exception: {e}",
                )
            results.append(result)

        if persist and results:
            self.store.upsert_models(results)

        return results

    def summary(self, results: list[QualityCheckResult]) -> str:
        passed = sum(1 for r in results if r.passed)
        lines = [f"{passed}/{len(results)} checks passed", ""]
        for r in results:
            status = "PASS" if r.passed else "FAIL"
            lines.append(f"[{status}] {r.table_name}.{r.check_name}: {r.details}")
        return "\n".join(lines)


if __name__ == "__main__":
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from data_storage.duckdb_store import DuckDBStore

    store = DuckDBStore("data/soccer_smoketest.duckdb")
    runner = QualityRunner(store)
    results = runner.run_all()
    print(runner.summary(results))
    store.close()