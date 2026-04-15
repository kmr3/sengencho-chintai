from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import re
import unicodedata
from urllib.parse import urljoin

from .models import ListingCandidate


DATE_PATTERNS = (
    re.compile(r"(?P<year>\d{4})[/-](?P<month>\d{1,2})[/-](?P<day>\d{1,2})"),
    re.compile(r"(?P<month>\d{1,2})[/-](?P<day>\d{1,2})"),
    re.compile(r"(?P<days>\d+)\s*日前"),
    re.compile(r"(?P<hours>\d+)\s*時間前"),
    re.compile(r"今日"),
    re.compile(r"昨日"),
)


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def normalize_japanese_text(value: str | None) -> str:
    text = unicodedata.normalize("NFKC", clean_text(value))
    return text.replace("ヶ", "ケ")


def normalize_address_text(value: str | None) -> str:
    text = normalize_japanese_text(value)
    text = text.replace("丁目", "")
    text = re.sub(r"(?<=\D)0+(\d)", r"\1", text)
    return text


def build_listing_id(detail_url: str, title: str, address_text: str) -> str:
    stable = "|".join((clean_text(detail_url), clean_text(title), clean_text(address_text)))
    return hashlib.sha1(stable.encode("utf-8")).hexdigest()[:16]


def build_fingerprint(candidate: ListingCandidate) -> str:
    parts = (
        candidate.title,
        candidate.address_text,
        candidate.rent_text,
        candidate.layout_text,
        candidate.area_text,
        candidate.station_text,
    )
    stable = "|".join(clean_text(part).lower() for part in parts if clean_text(part))
    return hashlib.sha1(stable.encode("utf-8")).hexdigest()


def build_group_key(
    *,
    title: str,
    address_text: str,
    rent_text: str,
    layout_text: str,
    area_text: str,
) -> str:
    stable = "|".join(
        (
            normalize_address_text(address_text).lower(),
            normalize_group_title(title),
            normalize_japanese_text(layout_text).lower(),
            normalize_area_bucket(area_text),
            normalize_rent_bucket(rent_text),
        )
    )
    return hashlib.sha1(stable.encode("utf-8")).hexdigest()


def matches_area(candidate: ListingCandidate, address_keywords: list[str], station_keywords: list[str]) -> bool:
    normalized_address = normalize_address_text(candidate.address_text).lower()
    normalized_keywords = [normalize_address_text(item).lower() for item in address_keywords]
    if normalized_keywords:
        return any(keyword in normalized_address for keyword in normalized_keywords)
    haystack = normalize_japanese_text(candidate.address_text).lower()
    return any(normalize_japanese_text(item).lower() in haystack for item in station_keywords)


def normalize_group_title(value: str | None) -> str:
    text = normalize_japanese_text(value).lower()
    text = re.sub(r"(賃貸マンション|賃貸アパート|賃貸一戸建て)", "", text)
    text = re.sub(r"(地上\d+階建.*|築\d+年.*)", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_area_bucket(value: str | None) -> str:
    match = re.search(r"(\d+(?:\.\d+)?)", normalize_japanese_text(value))
    if not match:
        return ""
    return f"{float(match.group(1)):.1f}"


def normalize_rent_bucket(value: str | None) -> str:
    text = normalize_japanese_text(value).replace(",", "")
    man_match = re.search(r"(\d+(?:\.\d+)?)\s*万円", text)
    if man_match:
        return f"{float(man_match.group(1)):.1f}"
    yen_match = re.search(r"(\d+)\s*円", text)
    if yen_match:
        return str(int(yen_match.group(1)) // 5000)
    return ""


def parse_source_date(raw_text: str, *, now: datetime) -> datetime | None:
    text = clean_text(raw_text)
    if not text:
        return None
    for pattern in DATE_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        groups = match.groupdict()
        if "year" in groups and groups["year"]:
            return datetime(
                int(groups["year"]),
                int(groups["month"]),
                int(groups["day"]),
                tzinfo=timezone.utc,
            )
        if "month" in groups and groups["month"] and "day" in groups and groups["day"]:
            return datetime(now.year, int(groups["month"]), int(groups["day"]), tzinfo=timezone.utc)
        if "days" in groups and groups["days"]:
            return now - timedelta_days(int(groups["days"]))
        if "hours" in groups and groups["hours"]:
            return now - timedelta_hours(int(groups["hours"]))
        if "今日" in text:
            return now
        if "昨日" in text:
            return now - timedelta_days(1)
    return None


def absolute_url(base_url: str, url_or_path: str) -> str:
    return urljoin(base_url, url_or_path)


def timedelta_days(days: int):
    from datetime import timedelta

    return timedelta(days=days)


def timedelta_hours(hours: int):
    from datetime import timedelta

    return timedelta(hours=hours)


def age_days(reference_at: datetime | None, now: datetime) -> int:
    if reference_at is None:
        return 0
    delta = now - reference_at
    return max(delta.days, 0)
