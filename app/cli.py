from __future__ import annotations

import argparse

from .config import load_alert_rules, load_app_settings
from .storage import Storage


def main() -> None:
    parser = argparse.ArgumentParser(description="Asamacho Rent Watch")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("collect", help="Fetch listings and save into SQLite")
    subparsers.add_parser("seed-demo", help="Insert demo listings into SQLite")
    geocode_parser = subparsers.add_parser("geocode", help="Geocode listings missing coordinates")
    geocode_parser.add_argument("--limit", type=int, default=100)
    subparsers.add_parser("notify", help="Print alert matches from saved alert rules")

    serve_parser = subparsers.add_parser("serve", help="Start local web server")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8000)

    args = parser.parse_args()
    settings = load_app_settings()

    if args.command == "collect":
        from .collector import collect_once

        summary = collect_once()
        print(
            f"collection finished: sources={summary.sources_checked} "
            f"queries={summary.queries_checked} saved={summary.listings_saved}"
        )
        return

    if args.command == "seed-demo":
        storage = Storage(settings.database_path)
        try:
            storage.seed_demo_listings()
        finally:
            storage.close()
        print(f"seeded demo listings into {settings.database_path}")
        return

    if args.command == "serve":
        from .web import serve

        serve(args.host, args.port)
        return

    if args.command == "geocode":
        from .geocoder import geocode_missing_listings

        summary = geocode_missing_listings(limit=args.limit)
        print(f"geocoding finished: attempted={summary.attempted} updated={summary.updated}")
        return

    if args.command == "notify":
        rules = load_alert_rules()
        storage = Storage(settings.database_path)
        try:
            for rule in rules:
                listings = storage.recent_alert_matches(
                    max_age_days=rule.max_age_days,
                    max_rent_yen=rule.max_rent_yen,
                    min_area_sqm=rule.min_area_sqm,
                    max_walk_minutes=rule.max_walk_minutes,
                    only_statuses=rule.statuses,
                )
                print(f"[{rule.name}] {len(listings)} matches")
                for item in listings[:20]:
                    print(
                        f"- {item.status} | {item.rent_text} | {item.layout_text} | "
                        f"{item.area_text} | {item.station_text} | {item.title} | {item.detail_url}"
                    )
        finally:
            storage.close()


if __name__ == "__main__":
    main()
