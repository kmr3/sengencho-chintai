from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "config"


@dataclass(slots=True)
class AppSettings:
    name: str
    database_path: Path
    request_timeout_seconds: int
    user_agent: str
    area_name: str
    address_keywords: list[str]
    station_keywords: list[str]
    poll_interval_minutes: int


@dataclass(slots=True)
class SourceSettings:
    name: str
    kind: str
    enabled: bool
    start_url: str
    max_pages: int
    encoding: str
    notes: str = ""


@dataclass(slots=True)
class AlertRule:
    name: str
    max_age_days: int
    max_rent_yen: int | None
    min_area_sqm: float | None
    max_walk_minutes: int | None
    statuses: list[str]


def _load_toml(path: Path) -> dict:
    with path.open("rb") as file:
        return tomllib.load(file)


def load_app_settings() -> AppSettings:
    raw = _load_toml(CONFIG_DIR / "app.toml")
    app = raw["app"]
    search = raw["search"]
    schedule = raw["schedule"]
    return AppSettings(
        name=app["name"],
        database_path=BASE_DIR / app["database_path"],
        request_timeout_seconds=app["request_timeout_seconds"],
        user_agent=app["user_agent"],
        area_name=search["area_name"],
        address_keywords=list(search["address_keywords"]),
        station_keywords=list(search["station_keywords"]),
        poll_interval_minutes=schedule["poll_interval_minutes"],
    )


def load_source_settings() -> list[SourceSettings]:
    raw = _load_toml(CONFIG_DIR / "sources.toml")
    return [SourceSettings(**item) for item in raw.get("sources", [])]


def load_alert_rules() -> list[AlertRule]:
    path = CONFIG_DIR / "alerts.toml"
    if not path.exists():
        return []
    raw = _load_toml(path)
    rules = []
    for item in raw.get("alerts", []):
        rules.append(
            AlertRule(
                name=item["name"],
                max_age_days=item.get("max_age_days", 3),
                max_rent_yen=int(item["max_rent_man"] * 10000) if item.get("max_rent_man") is not None else None,
                min_area_sqm=item.get("min_area_sqm"),
                max_walk_minutes=item.get("max_walk_minutes"),
                statuses=list(item.get("statuses", ["new", "updated", "reposted"])),
            )
        )
    return rules
