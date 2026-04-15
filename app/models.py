from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class ListingCandidate:
    source_name: str
    source_listing_id: str
    title: str
    rent_text: str
    fee_text: str
    layout_text: str
    area_text: str
    address_text: str
    station_text: str
    detail_url: str
    source_updated_at: datetime | None
    collected_at: datetime
    latitude: float | None = None
    longitude: float | None = None


@dataclass(slots=True)
class ListingRecord:
    fingerprint: str
    source_name: str
    source_listing_id: str
    title: str
    rent_text: str
    fee_text: str
    layout_text: str
    area_text: str
    address_text: str
    station_text: str
    detail_url: str
    source_updated_at: datetime | None
    first_seen_at: datetime
    last_seen_at: datetime
    last_collected_at: datetime
    latitude: float | None = None
    longitude: float | None = None


@dataclass(slots=True)
class ListingView:
    fingerprint: str
    group_key: str
    source_name: str
    source_names: list[str]
    title: str
    rent_text: str
    fee_text: str
    layout_text: str
    area_text: str
    address_text: str
    station_text: str
    detail_url: str
    source_updated_at: datetime | None
    first_seen_at: datetime
    last_seen_at: datetime
    age_days: int
    is_new: bool
    is_stale: bool
    rent_yen: int | None
    area_sqm: float | None
    walk_minutes: int | None
    latitude: float | None
    longitude: float | None
    duplicate_count: int
    status: str

    @classmethod
    def from_row(
        cls,
        row: dict[str, Any],
        age_days: int,
        is_new: bool,
        is_stale: bool,
        rent_yen: int | None,
        area_sqm: float | None,
        walk_minutes: int | None,
        latitude: float | None,
        longitude: float | None,
        source_names: list[str],
        duplicate_count: int,
        status: str,
    ) -> "ListingView":
        return cls(
            fingerprint=row["fingerprint"],
            group_key=row.get("group_key", row["fingerprint"]),
            source_name=row["source_name"],
            source_names=source_names,
            title=row["title"],
            rent_text=row["rent_text"],
            fee_text=row["fee_text"],
            layout_text=row["layout_text"],
            area_text=row["area_text"],
            address_text=row["address_text"],
            station_text=row["station_text"],
            detail_url=row["detail_url"],
            source_updated_at=_parse_timestamp(row["source_updated_at"]),
            first_seen_at=_parse_timestamp(row["first_seen_at"]),
            last_seen_at=_parse_timestamp(row["last_seen_at"]),
            age_days=age_days,
            is_new=is_new,
            is_stale=is_stale,
            rent_yen=rent_yen,
            area_sqm=area_sqm,
            walk_minutes=walk_minutes,
            latitude=latitude,
            longitude=longitude,
            duplicate_count=duplicate_count,
            status=status,
        )


def _parse_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)
