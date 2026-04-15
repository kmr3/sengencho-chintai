from __future__ import annotations

from dataclasses import dataclass

from .config import load_app_settings, load_source_settings
from .normalizer import matches_area
from .scrapers import ConfiguredScraper
from .storage import Storage


@dataclass(slots=True)
class CollectionSummary:
    sources_checked: int
    queries_checked: int
    listings_saved: int


def collect_once() -> CollectionSummary:
    app_settings = load_app_settings()
    source_settings = load_source_settings()
    storage = Storage(app_settings.database_path)
    saved = 0
    queries = 0
    checked = 0
    enabled_sources: list[str] = []

    try:
        run_id = storage.start_collection_run()
        for source in source_settings:
            if not source.enabled:
                continue
            checked += 1
            enabled_sources.append(source.name)
            scraper = ConfiguredScraper(app_settings, source)
            for result in scraper.scrape():
                queries += 1
                for candidate in result.listings:
                    if not matches_area(
                        candidate,
                        app_settings.address_keywords,
                        app_settings.station_keywords,
                    ):
                        continue
                    storage.upsert_listing(candidate, run_id=run_id)
                    saved += 1
        storage.finish_collection_run(run_id, enabled_sources=enabled_sources)
    finally:
        storage.close()

    return CollectionSummary(sources_checked=checked, queries_checked=queries, listings_saved=saved)
