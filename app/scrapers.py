from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup
import httpx

from .config import AppSettings, SourceSettings
from .models import ListingCandidate
from .normalizer import absolute_url, build_listing_id, clean_text


@dataclass(slots=True)
class ScrapeResult:
    source_name: str
    query: str
    listings: list[ListingCandidate]


class ConfiguredScraper:
    def __init__(self, app_settings: AppSettings, source_settings: SourceSettings) -> None:
        self.app_settings = app_settings
        self.source_settings = source_settings

    def scrape(self) -> list[ScrapeResult]:
        if not self.source_settings.enabled:
            return []

        headers = {"User-Agent": self.app_settings.user_agent}
        timeout = self.app_settings.request_timeout_seconds
        results: list[ScrapeResult] = []

        with httpx.Client(headers=headers, timeout=timeout, follow_redirects=True) as client:
            for page_number in range(1, self.source_settings.max_pages + 1):
                url = build_page_url(self.source_settings.start_url, page_number)
                response = client.get(url)
                response.raise_for_status()
                response.encoding = self.source_settings.encoding
                listings = parse_listing_page(
                    self.source_settings.kind,
                    self.source_settings.name,
                    response.text,
                    base_url=str(response.url),
                )
                if not listings:
                    break
                results.append(
                    ScrapeResult(
                        source_name=self.source_settings.name,
                        query=f"page={page_number}",
                        listings=listings,
                    )
                )
        return results


def build_page_url(start_url: str, page_number: int) -> str:
    if page_number == 1:
        return start_url
    split = urlsplit(start_url)
    params = dict(parse_qsl(split.query, keep_blank_values=True))
    params["page"] = str(page_number)
    query = urlencode(params, doseq=True)
    return urlunsplit((split.scheme, split.netloc, split.path, query, split.fragment))


def parse_listing_page(kind: str, source_name: str, html: str, *, base_url: str) -> list[ListingCandidate]:
    if kind == "suumo":
        return parse_suumo(source_name, html, base_url=base_url)
    if kind == "chintai":
        return parse_chintai(source_name, html, base_url=base_url)
    if kind == "yahoo":
        return parse_yahoo(source_name, html, base_url=base_url)
    raise ValueError(f"unsupported source kind: {kind}")


def parse_suumo(source_name: str, html: str, *, base_url: str) -> list[ListingCandidate]:
    soup = BeautifulSoup(html, "html.parser")
    now = datetime.now(timezone.utc)
    listings: list[ListingCandidate] = []

    for cassette in soup.select("div.cassetteitem"):
        title = clean_text(text_of(cassette, ".cassetteitem_content-title"))
        address = clean_text(text_of(cassette, ".cassetteitem_detail-col1"))
        station_text = " / ".join(
            clean_text(node.get_text(" ", strip=True))
            for node in cassette.select(".cassetteitem_detail-col2 .cassetteitem_detail-text")
        )
        for row in cassette.select("table.cassetteitem_other tbody tr"):
            detail_link = row.select_one("a.cassetteitem_other-linktext[href]")
            if detail_link is None:
                continue
            detail_url = absolute_url(base_url, detail_link.get("href", ""))
            rent_text = clean_text(text_of(row, ".cassetteitem_price--rent"))
            fee_text = clean_text(text_of(row, ".cassetteitem_price--administration"))
            layout_text = clean_text(text_of(row, ".cassetteitem_madori"))
            area_text = normalize_area(text_of(row, ".cassetteitem_menseki"))
            listings.append(
                ListingCandidate(
                    source_name=source_name,
                    source_listing_id=build_listing_id(detail_url, title, address),
                    title=title,
                    rent_text=rent_text,
                    fee_text=fee_text,
                    layout_text=layout_text,
                    area_text=area_text,
                    address_text=address,
                    station_text=station_text,
                    detail_url=detail_url,
                    source_updated_at=None,
                    collected_at=now,
                )
            )
    return listings


def parse_chintai(source_name: str, html: str, *, base_url: str) -> list[ListingCandidate]:
    soup = BeautifulSoup(html, "html.parser")
    now = datetime.now(timezone.utc)
    listings: list[ListingCandidate] = []

    for section in soup.select("section.cassette_item"):
        title_link = section.select_one("a.js-detailLinkUrl")
        if title_link is None:
            continue
        title = clean_text(title_link.get_text(" ", strip=True))
        detail_url = absolute_url(base_url, title_link.get("data-detailurl", ""))
        price_text = clean_text(text_of(section, "p.price"))
        rent_text = extract_chintai_rent(price_text)
        fee_text = extract_chintai_fee(price_text)
        details = [clean_text(item.get_text(" ", strip=True)) for item in section.select("ul.other_txt li")]
        if len(details) < 3:
            continue
        station_text = details[0]
        address_text = details[1]
        layout_text, area_text = extract_chintai_layout_area(details[2])
        listings.append(
            ListingCandidate(
                source_name=source_name,
                source_listing_id=build_listing_id(detail_url, title, address_text),
                title=title,
                rent_text=rent_text,
                fee_text=fee_text,
                layout_text=layout_text,
                area_text=area_text,
                address_text=address_text,
                station_text=station_text,
                detail_url=detail_url,
                source_updated_at=None,
                collected_at=now,
            )
        )

    for section in soup.select("section.cassette_item.build"):
        title = clean_text(text_of(section, ".cassette_ttl h2"))
        address_text = clean_text(text_of(section, "table.l-table tr:nth-of-type(1) td"))
        station_text = " / ".join(
            clean_text(node.get_text(" ", strip=True))
            for node in section.select("table.l-table tr:nth-of-type(1) td:nth-of-type(2) li")
        )
        coordinates = parse_chintai_coordinates(str(section))
        for tbody in section.select("table tbody.js-detailLinkUrl"):
            detail_url = absolute_url(base_url, tbody.get("data-detailurl", ""))
            detail_row = tbody.select_one("tr.detail-inner")
            if detail_row is None:
                continue
            layout_cell = detail_row.select("td")
            layout_text, area_text = "", ""
            if len(layout_cell) >= 6:
                layout_parts = [clean_text(part) for part in layout_cell[5].get_text("\n", strip=True).split("\n") if clean_text(part)]
                if layout_parts:
                    layout_text = layout_parts[0]
                if len(layout_parts) > 1:
                    area_text = normalize_area(layout_parts[1])
            updated_text = clean_text(text_of(tbody, ".information_box [itemprop='datePublished']"))
            listings.append(
                ListingCandidate(
                    source_name=source_name,
                    source_listing_id=build_listing_id(detail_url, title, address_text),
                    title=title,
                    rent_text=extract_chintai_build_rent(text_of(detail_row, "td.price")),
                    fee_text=extract_chintai_build_fee(text_of(detail_row, "td.price")),
                    layout_text=layout_text,
                    area_text=area_text,
                    address_text=address_text,
                    station_text=station_text,
                    detail_url=detail_url,
                    source_updated_at=parse_date_yyyy_mm_dd(updated_text),
                    collected_at=now,
                    latitude=coordinates[0] if coordinates else None,
                    longitude=coordinates[1] if coordinates else None,
                )
            )
    return listings


def parse_yahoo(source_name: str, html: str, *, base_url: str) -> list[ListingCandidate]:
    soup = BeautifulSoup(html, "html.parser")
    now = datetime.now(timezone.utc)
    listings: list[ListingCandidate] = []
    coordinates = extract_yahoo_page_coordinates(html)

    for index, building in enumerate(soup.select("li.ListBukken__item")):
        title = clean_text(text_of(building, ".ListCassette__ttl__link"))
        address_text = clean_text(text_of(building, ".ListCassette__item:nth-of-type(2) .ListCassette__txt"))
        station_bits = [
            clean_text(node.get_text(" ", strip=True))
            for node in building.select(".ListCassette__item:nth-of-type(1) .ListCassette__txt")
        ]
        station_text = " / ".join(bit for bit in station_bits if bit)
        point = coordinates[index] if index < len(coordinates) else None

        for room in building.select("li.ListCassetteRoom__item"):
            detail_link = room.select_one("a.ListCassetteRoom__textLink[href]")
            if detail_link is None:
                continue
            detail_url = absolute_url(base_url, detail_link.get("href", ""))
            listings.append(
                ListingCandidate(
                    source_name=source_name,
                    source_listing_id=build_listing_id(detail_url, title, address_text),
                    title=title,
                    rent_text=clean_text(text_of(room, ".ListCassetteRoom__dtl__price"))[:50].split("管理費等")[0].strip(),
                    fee_text=extract_yahoo_fee(text_of(room, ".ListCassetteRoom__dtl__price")),
                    layout_text=clean_text(text_of(room, ".ListCassetteRoom__block--layout .ListCassetteRoom__dtl__layout:nth-of-type(1)")),
                    area_text=normalize_area(text_of(room, ".ListCassetteRoom__block--layout .ListCassetteRoom__dtl__layout:nth-of-type(2)")),
                    address_text=address_text,
                    station_text=station_text,
                    detail_url=detail_url,
                    source_updated_at=None,
                    collected_at=now,
                    latitude=point[0] if point else None,
                    longitude=point[1] if point else None,
                )
            )
    return listings


def extract_yahoo_page_coordinates(html: str) -> list[tuple[float, float]]:
    return [
        (float(lat), float(lng))
        for lat, lng in re.findall(r'"CoordinatesWgs":"([0-9.]+),([0-9.]+)"', html)
    ]


def text_of(node, selector: str) -> str:
    element = node.select_one(selector)
    if element is None:
        return ""
    return element.get_text(" ", strip=True)


def normalize_area(value: str) -> str:
    return clean_text(value).replace("m 2", "m2").replace("m²", "m2").replace("㎡", "m2")


def extract_chintai_rent(price_text: str) -> str:
    match = re.search(r"([0-9.]+)\s*万円", price_text)
    return f"{match.group(1)}万円" if match else clean_text(price_text)


def extract_chintai_fee(price_text: str) -> str:
    match = re.search(r"管理費等[:：]\s*([^)）]+)", price_text)
    return clean_text(match.group(1)) if match else ""


def extract_chintai_layout_area(summary_text: str) -> tuple[str, str]:
    parts = [clean_text(part) for part in summary_text.split("/")]
    layout_text = parts[0] if parts else ""
    area_text = normalize_area(parts[1]) if len(parts) > 1 else ""
    return layout_text, area_text


def extract_yahoo_fee(price_text: str) -> str:
    match = re.search(r"管理費等\s*(.+)", clean_text(price_text))
    return clean_text(match.group(1)) if match else ""


def extract_chintai_build_rent(price_text: str) -> str:
    match = re.search(r"([0-9.]+)\s*万円", clean_text(price_text))
    return f"{match.group(1)}万円" if match else clean_text(price_text)


def extract_chintai_build_fee(price_text: str) -> str:
    parts = [clean_text(part) for part in clean_text(price_text).splitlines() if clean_text(part)]
    if len(parts) >= 2:
        return parts[1]
    match = re.search(r"万円\s*(.+)", clean_text(price_text))
    return clean_text(match.group(1)) if match else ""


def parse_chintai_coordinates(html: str) -> tuple[float, float] | None:
    match = re.search(r"showGoogleMap\(([0-9.]+),\s*([0-9.]+),", html)
    if not match:
        return None
    return float(match.group(1)), float(match.group(2))


def parse_date_yyyy_mm_dd(text: str) -> datetime | None:
    match = re.search(r"(\d{4})/(\d{2})/(\d{2})", text)
    if not match:
        return None
    return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)), tzinfo=timezone.utc)
