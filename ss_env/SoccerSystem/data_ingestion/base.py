"""
Shared ingestion interface.

Every concrete ingestor (one per source) implements this interface so the
orchestration layer can treat all sources the same way: fetch raw data,
optionally validate it, transform it into canonical schema objects
(from data_normalization.schemas), then hand off to storage.

If a source needs entity resolution (teams/players), do that inside
transform() using an EntityResolver instance passed into __init__ — keeps
the run() orchestration in this base class fully generic.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class IngestionResult(BaseModel):
    source: str
    records_fetched: int
    records_stored: int
    errors: list[str] = []


class Ingestor(ABC):
    """Base class for all data source ingestors."""

    source_name: str = "unknown"

    @abstractmethod
    def fetch(self, **kwargs) -> list[dict[str, Any]]:
        """Pull raw data from the source. Return a list of raw dicts."""
        raise NotImplementedError

    @abstractmethod
    def transform(self, raw_records: list[dict[str, Any]]) -> list[BaseModel]:
        """Convert raw dicts into validated canonical schema model instances."""
        raise NotImplementedError

    def validate_raw(self, raw_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Optional hook: drop/flag obviously bad raw records before transform.
        Default is passthrough — override per-source if a source needs
        pre-filtering (e.g. dropping rows missing a required field)."""
        return raw_records

    def run(self, store, **kwargs) -> IngestionResult:
        """Orchestrates fetch -> validate -> transform -> store.
        Catches and reports errors per-stage rather than crashing the whole
        pipeline run over one bad source."""
        try:
            raw = self.fetch(**kwargs)
        except Exception as e:
            logger.exception("fetch failed for %s", self.source_name)
            return IngestionResult(source=self.source_name, records_fetched=0,
                                    records_stored=0, errors=[f"fetch: {e}"])

        raw = self.validate_raw(raw)

        try:
            models = self.transform(raw)
        except Exception as e:
            logger.exception("transform failed for %s", self.source_name)
            return IngestionResult(source=self.source_name, records_fetched=len(raw),
                                    records_stored=0, errors=[f"transform: {e}"])

        stored = 0
        errors: list[str] = []
        if models:
            try:
                stored = store.upsert_models(models)
            except Exception as e:
                logger.exception("store failed for %s", self.source_name)
                errors.append(f"store: {e}")

        return IngestionResult(
            source=self.source_name,
            records_fetched=len(raw),
            records_stored=stored,
            errors=errors,
        )