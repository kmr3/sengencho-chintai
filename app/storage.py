from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
import re

from .models import ListingCandidate, ListingView
from .normalizer import age_days, build_fingerprint, build_group_key, normalize_address_text


SCHEMA = """
CREATE TABLE IF NOT EXISTS listings (
    fingerprint TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_listing_id TEXT NOT NULL,
    title TEXT NOT NULL,
    rent_text TEXT NOT NULL,
    fee_text TEXT NOT NULL,
    layout_text TEXT NOT NULL,
    area_text TEXT NOT NULL,
    address_text TEXT NOT NULL,
    station_text TEXT NOT NULL,
    detail_url TEXT NOT NULL,
    source_updated_at TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_collected_at TEXT NOT NULL,
    latitude REAL,
    longitude REAL,
    is_active INTEGER NOT NULL DEFAULT 1,
    last_status TEXT NOT NULL DEFAULT 'new',
    last_status_at TEXT,
    last_seen_run_id INTEGER
);

CREATE TABLE IF NOT EXISTS collection_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS listing_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL,
    source_name TEXT NOT NULL,
    detail_url TEXT NOT NULL,
    rent_text TEXT NOT NULL,
    fee_text TEXT NOT NULL,
    layout_text TEXT NOT NULL,
    area_text TEXT NOT NULL,
    source_updated_at TEXT,
    seen_at TEXT NOT NULL,
    status TEXT NOT NULL,
    run_id INTEGER
);
"""


class Storage:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.database_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(SCHEMA)
        self._migrate()

    def close(self) -> None:
        self.connection.close()

    def start_collection_run(self) -> int:
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.connection.execute(
            "INSERT INTO collection_runs (started_at) VALUES (?)",
            (now,),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def finish_collection_run(self, run_id: int, *, enabled_sources: list[str]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        placeholders = ",".join("?" for _ in enabled_sources)
        if enabled_sources:
            self.connection.execute(
                f"""
                UPDATE listings
                SET is_active = 0
                WHERE source_name IN ({placeholders})
                  AND (last_seen_run_id IS NULL OR last_seen_run_id != ?)
                """,
                (*enabled_sources, run_id),
            )
        self.connection.execute(
            "UPDATE collection_runs SET finished_at = ? WHERE id = ?",
            (now, run_id),
        )
        self.connection.commit()

    def upsert_listing(self, candidate: ListingCandidate, *, run_id: int | None = None) -> None:
        fingerprint = build_fingerprint(candidate)
        existing = self.connection.execute(
            """
            SELECT fingerprint, first_seen_at, is_active, rent_text, fee_text, layout_text, area_text, detail_url, source_updated_at
            FROM listings
            WHERE fingerprint = ?
            """,
            (fingerprint,),
        ).fetchone()

        candidate_address_key = normalize_address_text(candidate.address_text)
        existing_coordinates = self.connection.execute(
            "SELECT address_text, latitude, longitude FROM listings WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
        ).fetchall()
        for row in existing_coordinates:
            if normalize_address_text(row["address_text"]) == candidate_address_key:
                candidate.latitude = candidate.latitude or row["latitude"]
                candidate.longitude = candidate.longitude or row["longitude"]
                break

        first_seen_at = existing["first_seen_at"] if existing else candidate.collected_at.isoformat()
        status = self._detect_status(existing, candidate)
        self.connection.execute(
            """
            INSERT INTO listings (
                fingerprint, source_name, source_listing_id, title, rent_text, fee_text,
                layout_text, area_text, address_text, station_text, detail_url,
                source_updated_at, first_seen_at, last_seen_at, last_collected_at,
                latitude, longitude, is_active, last_status, last_status_at, last_seen_run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fingerprint) DO UPDATE SET
                source_name = excluded.source_name,
                source_listing_id = excluded.source_listing_id,
                title = excluded.title,
                rent_text = excluded.rent_text,
                fee_text = excluded.fee_text,
                layout_text = excluded.layout_text,
                area_text = excluded.area_text,
                address_text = excluded.address_text,
                station_text = excluded.station_text,
                detail_url = excluded.detail_url,
                source_updated_at = excluded.source_updated_at,
                last_seen_at = excluded.last_seen_at,
                last_collected_at = excluded.last_collected_at,
                latitude = COALESCE(excluded.latitude, listings.latitude),
                longitude = COALESCE(excluded.longitude, listings.longitude),
                is_active = 1,
                last_status = excluded.last_status,
                last_status_at = excluded.last_status_at,
                last_seen_run_id = excluded.last_seen_run_id
            """,
            (
                fingerprint,
                candidate.source_name,
                candidate.source_listing_id,
                candidate.title,
                candidate.rent_text,
                candidate.fee_text,
                candidate.layout_text,
                candidate.area_text,
                candidate.address_text,
                candidate.station_text,
                candidate.detail_url,
                candidate.source_updated_at.isoformat() if candidate.source_updated_at else None,
                first_seen_at,
                candidate.collected_at.isoformat(),
                candidate.collected_at.isoformat(),
                candidate.latitude,
                candidate.longitude,
                1,
                status,
                candidate.collected_at.isoformat(),
                run_id,
            ),
        )
        self.connection.execute(
            """
            INSERT INTO listing_snapshots (
                fingerprint, source_name, detail_url, rent_text, fee_text, layout_text, area_text,
                source_updated_at, seen_at, status, run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fingerprint,
                candidate.source_name,
                candidate.detail_url,
                candidate.rent_text,
                candidate.fee_text,
                candidate.layout_text,
                candidate.area_text,
                candidate.source_updated_at.isoformat() if candidate.source_updated_at else None,
                candidate.collected_at.isoformat(),
                status,
                run_id,
            ),
        )
        self.connection.commit()

    def get_recent_listings(
        self,
        *,
        limit: int = 100,
        min_rent_yen: int | None = None,
        max_rent_yen: int | None = None,
        min_area_sqm: float | None = None,
        max_walk_minutes: int | None = None,
        max_age_days: int | None = None,
        keyword: str = "",
        sort_key: str = "recent",
    ) -> list[ListingView]:
        rows = self.connection.execute(
            "SELECT * FROM listings WHERE is_active = 1 ORDER BY COALESCE(source_updated_at, first_seen_at) DESC, last_seen_at DESC"
        ).fetchall()
        now = datetime.now(timezone.utc)
        grouped: dict[str, dict] = {}
        keyword_lower = keyword.strip().lower()

        for row in rows:
            row_dict = dict(row)
            updated_at = datetime.fromisoformat(row["source_updated_at"]) if row["source_updated_at"] else None
            first_seen_at = datetime.fromisoformat(row["first_seen_at"])
            reference_at = updated_at or first_seen_at
            rent_yen = parse_rent_yen(row["rent_text"])
            area_sqm = parse_area_sqm(row["area_text"])
            walk_minutes = parse_walk_minutes(row["station_text"])
            age = age_days(reference_at, now)
            group_key = build_group_key(
                title=row["title"],
                address_text=row["address_text"],
                rent_text=row["rent_text"],
                layout_text=row["layout_text"],
                area_text=row["area_text"],
            )

            if min_rent_yen is not None and (rent_yen is None or rent_yen < min_rent_yen):
                continue
            if max_rent_yen is not None and (rent_yen is None or rent_yen > max_rent_yen):
                continue
            if min_area_sqm is not None and (area_sqm is None or area_sqm < min_area_sqm):
                continue
            if max_walk_minutes is not None and (walk_minutes is None or walk_minutes > max_walk_minutes):
                continue
            if max_age_days is not None and age > max_age_days:
                continue
            if keyword_lower:
                haystack = " ".join(
                    (row["title"], row["address_text"], row["station_text"], row["layout_text"], row["source_name"])
                ).lower()
                if keyword_lower not in haystack:
                    continue

            bucket = grouped.get(group_key)
            if bucket is None:
                bucket = {
                    "row": row_dict,
                    "age": age,
                    "rent_yen": rent_yen,
                    "area_sqm": area_sqm,
                    "walk_minutes": walk_minutes,
                    "source_names": {row["source_name"]},
                    "duplicate_count": 1,
                    "status": row["last_status"],
                }
                bucket["row"]["group_key"] = group_key
                grouped[group_key] = bucket
                continue

            bucket["source_names"].add(row["source_name"])
            bucket["duplicate_count"] += 1
            if _status_priority(row["last_status"]) > _status_priority(bucket["status"]):
                bucket["status"] = row["last_status"]
            if _row_score(row_dict) > _row_score(bucket["row"]):
                bucket["row"] = row_dict
                bucket["row"]["group_key"] = group_key
                bucket["age"] = age
                bucket["rent_yen"] = rent_yen
                bucket["area_sqm"] = area_sqm
                bucket["walk_minutes"] = walk_minutes

        views = [
            ListingView.from_row(
                bucket["row"],
                bucket["age"],
                bucket["status"] == "new",
                False,
                bucket["rent_yen"],
                bucket["area_sqm"],
                bucket["walk_minutes"],
                bucket["row"]["latitude"],
                bucket["row"]["longitude"],
                sorted(bucket["source_names"]),
                bucket["duplicate_count"],
                bucket["status"],
            )
            for bucket in grouped.values()
        ]

        views.sort(key=_build_sorter(sort_key), reverse=sort_key != "walk")
        return views[:limit]

    def seed_demo_listings(self) -> None:
        now = datetime.now(timezone.utc)
        run_id = self.start_collection_run()
        demo = [
            ListingCandidate(
                source_name="demo-source",
                source_listing_id="demo-1",
                title="浅間町駅 徒歩6分 1LDK",
                rent_text="12.4万円",
                fee_text="8,000円",
                layout_text="1LDK",
                area_text="36.2m2",
                address_text="神奈川県横浜市西区浅間町3丁目",
                station_text="浅間町駅 徒歩6分",
                detail_url="https://example.com/demo-1",
                source_updated_at=now,
                collected_at=now,
                latitude=35.46842,
                longitude=139.60967,
            ),
            ListingCandidate(
                source_name="demo-source",
                source_listing_id="demo-2",
                title="横浜駅 徒歩14分 2DK",
                rent_text="10.8万円",
                fee_text="5,000円",
                layout_text="2DK",
                area_text="41.8m2",
                address_text="神奈川県横浜市西区浅間町2丁目",
                station_text="横浜駅 徒歩14分",
                detail_url="https://example.com/demo-2",
                source_updated_at=now.replace(day=max(now.day - 2, 1)),
                collected_at=now,
                latitude=35.46772,
                longitude=139.61132,
            ),
            ListingCandidate(
                source_name="demo-source",
                source_listing_id="demo-3",
                title="天王町駅 徒歩9分 1K",
                rent_text="7.6万円",
                fee_text="3,000円",
                layout_text="1K",
                area_text="22.4m2",
                address_text="神奈川県横浜市西区浅間町5丁目",
                station_text="天王町駅 徒歩9分",
                detail_url="https://example.com/demo-3",
                source_updated_at=now.replace(day=max(now.day - 1, 1)),
                collected_at=now,
                latitude=35.47011,
                longitude=139.60373,
            ),
            ListingCandidate(
                source_name="demo-source",
                source_listing_id="demo-4",
                title="平沼橋駅 徒歩11分 2LDK",
                rent_text="15.8万円",
                fee_text="10,000円",
                layout_text="2LDK",
                area_text="52.1m2",
                address_text="神奈川県横浜市西区浅間町4丁目",
                station_text="平沼橋駅 徒歩11分",
                detail_url="https://example.com/demo-4",
                source_updated_at=now.replace(day=max(now.day - 5, 1)),
                collected_at=now,
                latitude=35.46081,
                longitude=139.61518,
            ),
        ]
        for item in demo:
            self.upsert_listing(item, run_id=run_id)
        self.finish_collection_run(run_id, enabled_sources=["demo-source"])

    def get_listings_missing_coordinates(self, *, limit: int = 100) -> list[sqlite3.Row]:
        return self.connection.execute(
            """
            SELECT fingerprint, address_text, title
            FROM listings
            WHERE (latitude IS NULL OR longitude IS NULL) AND is_active = 1
            ORDER BY last_seen_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    def update_listing_coordinates(self, fingerprint: str, latitude: float, longitude: float) -> None:
        self.connection.execute(
            "UPDATE listings SET latitude = ?, longitude = ? WHERE fingerprint = ?",
            (latitude, longitude, fingerprint),
        )
        self.connection.commit()

    def get_missing_addresses(self, *, limit: int = 100) -> list[str]:
        rows = self.connection.execute(
            "SELECT address_text FROM listings WHERE (latitude IS NULL OR longitude IS NULL) AND is_active = 1"
        ).fetchall()
        seen: set[str] = set()
        addresses: list[str] = []
        for row in rows:
            address_text = row["address_text"]
            address_key = normalize_address_text(address_text)
            if address_key in seen:
                continue
            seen.add(address_key)
            addresses.append(address_text)
            if len(addresses) >= limit:
                break
        return addresses

    def update_coordinates_by_address(self, address_text: str, latitude: float, longitude: float) -> int:
        target_key = normalize_address_text(address_text)
        rows = self.connection.execute("SELECT fingerprint, address_text FROM listings").fetchall()
        fingerprints = [row["fingerprint"] for row in rows if normalize_address_text(row["address_text"]) == target_key]
        self.connection.executemany(
            "UPDATE listings SET latitude = ?, longitude = ? WHERE fingerprint = ?",
            [(latitude, longitude, fingerprint) for fingerprint in fingerprints],
        )
        self.connection.commit()
        return len(fingerprints)

    def clear_out_of_bounds_coordinates(
        self,
        *,
        min_lat: float,
        max_lat: float,
        min_lng: float,
        max_lng: float,
    ) -> int:
        result = self.connection.execute(
            """
            UPDATE listings
            SET latitude = NULL, longitude = NULL
            WHERE latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND (latitude < ? OR latitude > ? OR longitude < ? OR longitude > ?)
            """,
            (min_lat, max_lat, min_lng, max_lng),
        )
        self.connection.commit()
        return result.rowcount

    def propagate_coordinates_by_normalized_address(
        self,
        *,
        min_lat: float,
        max_lat: float,
        min_lng: float,
        max_lng: float,
    ) -> int:
        rows = self.connection.execute(
            "SELECT fingerprint, address_text, latitude, longitude FROM listings"
        ).fetchall()
        valid_by_address: dict[str, tuple[float, float]] = {}
        for row in rows:
            lat = row["latitude"]
            lng = row["longitude"]
            if lat is None or lng is None:
                continue
            if not (min_lat <= lat <= max_lat and min_lng <= lng <= max_lng):
                continue
            valid_by_address.setdefault(normalize_address_text(row["address_text"]), (lat, lng))

        updates: list[tuple[float, float, str]] = []
        for row in rows:
            if row["latitude"] is not None and row["longitude"] is not None:
                continue
            coordinate = valid_by_address.get(normalize_address_text(row["address_text"]))
            if coordinate is None:
                continue
            updates.append((coordinate[0], coordinate[1], row["fingerprint"]))

        self.connection.executemany(
            "UPDATE listings SET latitude = ?, longitude = ? WHERE fingerprint = ?",
            updates,
        )
        self.connection.commit()
        return len(updates)

    def recent_alert_matches(
        self,
        *,
        max_age_days: int,
        max_rent_yen: int | None,
        min_area_sqm: float | None,
        max_walk_minutes: int | None,
        only_statuses: list[str] | None = None,
    ) -> list[ListingView]:
        listings = self.get_recent_listings(
            limit=500,
            max_rent_yen=max_rent_yen,
            min_area_sqm=min_area_sqm,
            max_walk_minutes=max_walk_minutes,
            max_age_days=max_age_days,
            sort_key="recent",
        )
        if only_statuses:
            wanted = set(only_statuses)
            listings = [item for item in listings if item.status in wanted]
        return listings

    def _detect_status(self, existing: sqlite3.Row | None, candidate: ListingCandidate) -> str:
        if existing is None:
            return "new"
        if int(existing["is_active"]) == 0:
            return "reposted"
        existing_updated = existing["source_updated_at"] or ""
        candidate_updated = candidate.source_updated_at.isoformat() if candidate.source_updated_at else ""
        changed = any(
            (
                existing["rent_text"] != candidate.rent_text,
                existing["fee_text"] != candidate.fee_text,
                existing["layout_text"] != candidate.layout_text,
                existing["area_text"] != candidate.area_text,
                existing["detail_url"] != candidate.detail_url,
                existing_updated != candidate_updated,
            )
        )
        return "updated" if changed else "seen"

    def _migrate(self) -> None:
        columns = {row["name"] for row in self.connection.execute("PRAGMA table_info(listings)").fetchall()}
        wanted_columns = {
            "latitude": "ALTER TABLE listings ADD COLUMN latitude REAL",
            "longitude": "ALTER TABLE listings ADD COLUMN longitude REAL",
            "is_active": "ALTER TABLE listings ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1",
            "last_status": "ALTER TABLE listings ADD COLUMN last_status TEXT NOT NULL DEFAULT 'new'",
            "last_status_at": "ALTER TABLE listings ADD COLUMN last_status_at TEXT",
            "last_seen_run_id": "ALTER TABLE listings ADD COLUMN last_seen_run_id INTEGER",
        }
        for name, statement in wanted_columns.items():
            if name not in columns:
                self.connection.execute(statement)
        self.connection.execute("CREATE INDEX IF NOT EXISTS idx_listings_last_seen_at ON listings(last_seen_at)")
        self.connection.execute("CREATE INDEX IF NOT EXISTS idx_listings_source_name ON listings(source_name)")
        self.connection.execute("CREATE INDEX IF NOT EXISTS idx_listings_run_id ON listings(last_seen_run_id)")
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_listing_snapshots_fingerprint_seen_at ON listing_snapshots(fingerprint, seen_at DESC)"
        )
        self.connection.commit()


def _is_new(first_seen_at: datetime, now: datetime) -> bool:
    return (now - first_seen_at).days < 1


def parse_rent_yen(value: str) -> int | None:
    text = value.replace(",", "").strip()
    man_match = re.search(r"(\d+(?:\.\d+)?)\s*万円", text)
    if man_match:
        return int(float(man_match.group(1)) * 10000)
    yen_match = re.search(r"(\d+)\s*円", text)
    if yen_match:
        return int(yen_match.group(1))
    return None


def parse_area_sqm(value: str) -> float | None:
    match = re.search(r"(\d+(?:\.\d+)?)\s*(?:m2|㎡|平米)", value)
    if match:
        return float(match.group(1))
    return None


def parse_walk_minutes(value: str) -> int | None:
    match = re.search(r"徒歩\s*(\d+)\s*分", value)
    if match:
        return int(match.group(1))
    return None


def _build_sorter(sort_key: str):
    if sort_key == "rent":
        return lambda item: (item.rent_yen or -1, item.age_days * -1)
    if sort_key == "area":
        return lambda item: (item.area_sqm or -1.0, (item.rent_yen or 0) * -1)
    if sort_key == "walk":
        return lambda item: (999 if item.walk_minutes is None else item.walk_minutes, item.age_days)
    return lambda item: (
        _status_priority(item.status),
        item.source_updated_at or item.first_seen_at,
        item.last_seen_at,
    )


def _status_priority(status: str) -> int:
    order = {"new": 4, "reposted": 3, "updated": 2, "seen": 1}
    return order.get(status, 0)


def _row_score(row: dict) -> tuple:
    updated = row.get("source_updated_at") or row.get("first_seen_at")
    return (updated or "", row.get("last_seen_at") or "")
