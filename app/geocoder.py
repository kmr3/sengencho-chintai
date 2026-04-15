from __future__ import annotations

from dataclasses import dataclass
from time import sleep

import httpx

from .config import load_app_settings
from .normalizer import normalize_address_text
from .storage import Storage


YOKOHAMA_BOUNDS = {
    "min_lat": 35.43,
    "max_lat": 35.49,
    "min_lng": 139.59,
    "max_lng": 139.63,
}


@dataclass(slots=True)
class GeocodeSummary:
    attempted: int
    updated: int


class NominatimGeocoder:
    def __init__(self, user_agent: str) -> None:
        self.client = httpx.Client(
            headers={"User-Agent": user_agent},
            timeout=20,
            follow_redirects=True,
        )

    def close(self) -> None:
        self.client.close()

    def geocode(self, query: str) -> tuple[float, float] | None:
        response = self.client.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "format": "jsonv2",
                "limit": 1,
                "q": query,
                "countrycodes": "jp",
                "viewbox": "139.59,35.49,139.63,35.43",
                "bounded": 1,
            },
        )
        if response.status_code == 429:
            return None
        response.raise_for_status()
        items = response.json()
        if not items:
            return None
        item = items[0]
        return float(item["lat"]), float(item["lon"])


def geocode_missing_listings(*, limit: int = 100, pause_seconds: float = 1.1) -> GeocodeSummary:
    settings = load_app_settings()
    storage = Storage(settings.database_path)
    geocoder = NominatimGeocoder(settings.user_agent)
    attempted = 0
    updated = 0
    try:
        storage.clear_out_of_bounds_coordinates(**YOKOHAMA_BOUNDS)
        updated += storage.propagate_coordinates_by_normalized_address(**YOKOHAMA_BOUNDS)
        for address_text in storage.get_missing_addresses(limit=limit):
            attempted += 1
            query = build_geocode_query(address_text)
            result = geocoder.geocode(query)
            if result is None:
                sleep(max(pause_seconds, 2.0))
                continue
            if not within_yokohama(result[0], result[1]):
                sleep(max(pause_seconds, 2.0))
                continue
            updated += storage.update_coordinates_by_address(address_text, result[0], result[1])
            sleep(pause_seconds)
    finally:
        geocoder.close()
        storage.close()
    return GeocodeSummary(attempted=attempted, updated=updated)


def build_geocode_query(address_text: str) -> str:
    normalized = normalize_address_text(address_text)
    return f"{normalized}, 横浜市西区, 神奈川県, 日本"


def within_yokohama(latitude: float, longitude: float) -> bool:
    return (
        YOKOHAMA_BOUNDS["min_lat"] <= latitude <= YOKOHAMA_BOUNDS["max_lat"]
        and YOKOHAMA_BOUNDS["min_lng"] <= longitude <= YOKOHAMA_BOUNDS["max_lng"]
    )
