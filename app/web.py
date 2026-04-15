from __future__ import annotations

from html import escape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from urllib.parse import parse_qs, urlparse

from .config import load_app_settings
from .storage import Storage


CSS = """
body {
  font-family: "Hiragino Sans", "Yu Gothic", sans-serif;
  margin: 0;
  background:
    radial-gradient(circle at top left, rgba(236, 226, 206, 0.9), transparent 32%),
    linear-gradient(180deg, #faf7ef 0%, #f0ece1 100%);
  color: #1f2933;
}
.wrap {
  max-width: 1320px;
  margin: 0 auto;
  padding: 28px 18px 48px;
}
.hero {
  background: rgba(255, 255, 255, 0.86);
  border: 1px solid #d9d2c3;
  border-radius: 22px;
  padding: 24px;
  box-shadow: 0 18px 50px rgba(79, 64, 43, 0.08);
}
.hero h1 {
  margin: 0 0 10px;
  font-size: 34px;
}
.meta {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
  color: #5b6875;
  font-size: 14px;
}
.toolbar {
  margin-top: 18px;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(155px, 1fr));
  gap: 10px;
}
.toolbar label {
  display: block;
  font-size: 12px;
  color: #5b6875;
  margin-bottom: 6px;
}
.toolbar input,
.toolbar select,
.toolbar button {
  width: 100%;
  border: 1px solid #d2cabd;
  border-radius: 12px;
  background: #fffdf9;
  padding: 10px 12px;
  font-size: 14px;
  box-sizing: border-box;
}
.toolbar button {
  background: #224f3a;
  color: white;
  font-weight: 700;
  cursor: pointer;
}
.stats {
  margin-top: 22px;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
  gap: 12px;
}
.stat {
  background: rgba(255, 255, 255, 0.82);
  border: 1px solid #e3dbcd;
  border-radius: 16px;
  padding: 14px 16px;
}
.stat strong {
  display: block;
  font-size: 24px;
}
.layout {
  margin-top: 24px;
  display: grid;
  grid-template-columns: minmax(0, 1.15fr) minmax(360px, 0.85fr);
  gap: 18px;
  align-items: start;
}
.map-panel,
.list-panel {
  background: rgba(255, 255, 255, 0.88);
  border: 1px solid #e4ddd1;
  border-radius: 20px;
  overflow: hidden;
}
.panel-head {
  padding: 14px 18px;
  border-bottom: 1px solid #efe7da;
  font-size: 14px;
  color: #506070;
}
#map {
  height: 72vh;
  min-height: 520px;
}
.cards {
  padding: 14px;
  display: grid;
  gap: 12px;
  max-height: 78vh;
  overflow: auto;
}
.card {
  background: #fffefb;
  border: 1px solid #e8dfd1;
  border-radius: 16px;
  padding: 16px;
  scroll-margin-top: 16px;
}
.card.is-active {
  border-color: #224f3a;
  box-shadow: 0 0 0 3px rgba(34, 79, 58, 0.14);
}
.pill {
  display: inline-block;
  padding: 4px 10px;
  border-radius: 999px;
  background: #dcefe1;
  color: #204b2d;
  font-size: 12px;
  margin-bottom: 10px;
}
.pill.old {
  background: #efe7dc;
  color: #6d5129;
}
.rent {
  font-size: 26px;
  font-weight: 700;
  margin: 8px 0;
}
.facts {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin: 10px 0 12px;
}
.fact {
  font-size: 12px;
  padding: 5px 8px;
  border-radius: 999px;
  background: #f3efe7;
  color: #475569;
}
.title {
  font-size: 18px;
  font-weight: 700;
  margin: 0 0 10px;
}
.sub {
  color: #506070;
  line-height: 1.6;
  font-size: 14px;
}
.empty-map {
  padding: 32px 18px;
  color: #6b7280;
}
.focus-link {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  margin-top: 10px;
  font-size: 13px;
}
.action-row {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-top: 12px;
}
.small-btn {
  border: 1px solid #d7cfbf;
  background: #fff;
  border-radius: 999px;
  padding: 6px 10px;
  font-size: 12px;
  cursor: pointer;
}
.small-btn.is-on {
  background: #224f3a;
  color: #fff;
  border-color: #224f3a;
}
.client-filters {
  margin-top: 12px;
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
  color: #506070;
  font-size: 14px;
}
.status-pill {
  display: inline-block;
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 12px;
  margin-bottom: 10px;
}
.status-pill.new { background: #dcefe1; color: #204b2d; }
.status-pill.updated { background: #e5eefb; color: #1f4b7a; }
.status-pill.reposted { background: #f7ead8; color: #7a4a16; }
.status-pill.seen { background: #ececec; color: #555; }
a {
  color: #004b8d;
}
@media (max-width: 980px) {
  .layout {
    grid-template-columns: 1fr;
  }
  #map {
    height: 48vh;
    min-height: 360px;
  }
  .cards {
    max-height: none;
  }
}
"""


LEAFLET_CSS = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
LEAFLET_JS = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
MARKER_CLUSTER_CSS = "https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css"
MARKER_CLUSTER_DEFAULT_CSS = "https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css"
MARKER_CLUSTER_JS = "https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"
DEFAULT_CENTER = [35.4666, 139.6078]


def serve(host: str, port: int) -> None:
    settings = load_app_settings()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            encoded = self._build_response()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_HEAD(self) -> None:  # noqa: N802
            encoded = self._build_response()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

        def _build_response(self) -> bytes:
            query = parse_qs(urlparse(self.path).query)
            storage = Storage(settings.database_path)
            try:
                filters = parse_filters(query)
                listings = storage.get_recent_listings(limit=1000, **filters)
            finally:
                storage.close()
            body = render_homepage(
                settings.name,
                settings.area_name,
                settings.poll_interval_minutes,
                listings,
                filters,
            )
            return body.encode("utf-8")

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Serving on http://{host}:{port}")
    server.serve_forever()


def render_homepage(app_name: str, area_name: str, poll_interval_minutes: int, listings, filters: dict) -> str:
    cards = "".join(render_card(item) for item in listings) or "<div class='cards'><p>まだデータがありません。先に collect を実行してください。</p></div>"
    stats = render_stats(listings)
    map_data = build_map_data(listings)
    map_body = "<div id='map'></div>" if map_data else "<div class='empty-map'>座標付き物件がありません。`geocode` を実行すると地図に出ます。</div>"
    return f"""<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(app_name)}</title>
  <link rel="stylesheet" href="{LEAFLET_CSS}">
  <link rel="stylesheet" href="{MARKER_CLUSTER_CSS}">
  <link rel="stylesheet" href="{MARKER_CLUSTER_DEFAULT_CSS}">
  <style>{CSS}</style>
</head>
<body>
  <main class="wrap">
    <section class="hero">
      <h1>{escape(area_name)} 賃貸ウォッチ</h1>
      <div class="meta">
        <span>収集頻度: {poll_interval_minutes}分ごと</span>
        <span>表示件数上限: 1000件</span>
        <span>対象: 横浜市西区浅間町のみ</span>
      </div>
      {render_filters(filters)}
      <div class="client-filters">
        <label><input type="checkbox" id="favorites-only"> お気に入りのみ</label>
        <label><input type="checkbox" id="hide-excluded" checked> 除外を隠す</label>
      </div>
    </section>
    {stats}
    <section class="layout">
      <section class="map-panel">
        <div class="panel-head">地図表示</div>
        {map_body}
      </section>
      <section class="list-panel">
        <div class="panel-head">物件一覧</div>
        <div class="cards">{cards}</div>
      </section>
    </section>
  </main>
  {render_map_script(map_data)}
</body>
</html>"""


def render_card(item) -> str:
    badge = status_badge(item.status, item.age_days)
    updated = item.source_updated_at.isoformat(sep=" ", timespec="minutes") if item.source_updated_at else "取得日時ベース"
    facts = [
        f'<span class="fact">{escape(item.layout_text or "-")}</span>',
        f'<span class="fact">{escape(item.area_text or "-")}</span>',
        f'<span class="fact">徒歩 {item.walk_minutes}分</span>' if item.walk_minutes is not None else "",
        f'<span class="fact">管理費 {escape(item.fee_text or "-")}</span>',
        f'<span class="fact">ソース {escape(", ".join(item.source_names))}</span>',
        f'<span class="fact">重複統合 {item.duplicate_count}件</span>' if item.duplicate_count > 1 else "",
    ]
    return f"""
<article class="card" id="listing-{escape(item.fingerprint)}" data-fingerprint="{escape(item.fingerprint)}">
  {badge}
  <div class="title">{escape(item.title)}</div>
  <div class="rent">{escape(item.rent_text or "-")}</div>
  <div class="facts">{''.join(facts)}</div>
  <div class="sub">
    <div>{escape(item.address_text)}</div>
    <div>{escape(item.station_text)}</div>
    <div>更新基準: {escape(updated)}</div>
    <div>最終確認: {escape(item.last_seen_at.isoformat(sep=" ", timespec="minutes"))}</div>
    <div><a class="detail-link" data-fingerprint="{escape(item.fingerprint)}" href="{escape(item.detail_url)}" target="_blank" rel="noreferrer">元サイトを見る</a></div>
    <div><a class="focus-link" href="#listing-{escape(item.fingerprint)}" data-fingerprint="{escape(item.fingerprint)}">この物件を記憶する</a></div>
    <div class="action-row">
      <button type="button" class="small-btn favorite-btn" data-fingerprint="{escape(item.fingerprint)}">お気に入り</button>
      <button type="button" class="small-btn exclude-btn" data-fingerprint="{escape(item.fingerprint)}">除外</button>
    </div>
  </div>
</article>
"""


def build_map_data(listings) -> list[dict]:
    data = []
    for item in listings:
        if item.latitude is None or item.longitude is None:
            continue
        data.append(
            {
                "fingerprint": item.fingerprint,
                "title": item.title,
                "rent": item.rent_text,
                "layout": item.layout_text,
                "area": item.area_text,
                "station": item.station_text,
                "address": item.address_text,
                "url": item.detail_url,
                "lat": item.latitude,
                "lng": item.longitude,
                "status": item.status,
            }
        )
    return data


def render_map_script(map_data: list[dict]) -> str:
    if not map_data:
        return render_restore_script("[]")
    payload = json.dumps(map_data, ensure_ascii=False)
    return f"""
<script src="{LEAFLET_JS}"></script>
<script src="{MARKER_CLUSTER_JS}"></script>
<script>
const listings = {payload};
const STORAGE_KEY = 'asamacho-rent-watch:view-state';
const FAVORITES_KEY = 'asamacho-rent-watch:favorites';
const EXCLUDED_KEY = 'asamacho-rent-watch:excluded';
const map = L.map('map');
L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  maxZoom: 19,
  attribution: '&copy; OpenStreetMap contributors'
}}).addTo(map);
const bounds = [];
const markersByFingerprint = new Map();
const cluster = L.markerClusterGroup();
for (const item of listings) {{
  const marker = L.marker([item.lat, item.lng]);
  marker.bindPopup(`
    <strong>${{item.rent}}</strong><br>
    ${{item.status}}<br>
    ${{item.title}}<br>
    ${{item.address}}<br>
    ${{item.station}}<br>
    <a href="${{item.url}}" target="_blank" rel="noreferrer">元サイトを見る</a>
  `);
  bounds.push([item.lat, item.lng]);
  if (item.fingerprint) {{
    markersByFingerprint.set(item.fingerprint, marker);
  }}
  cluster.addLayer(marker);
}}
map.addLayer(cluster);
restoreState(bounds);
bindPersistence();
applyClientState();

function bindPersistence() {{
  const cards = document.querySelectorAll('.card');
  const links = document.querySelectorAll('.detail-link, .focus-link');
  const listPanel = document.querySelector('.cards');
  const favoriteButtons = document.querySelectorAll('.favorite-btn');
  const excludeButtons = document.querySelectorAll('.exclude-btn');
  for (const link of links) {{
    const persistLinkState = () => {{
      const fingerprint = link.dataset.fingerprint || '';
      saveState({{ activeFingerprint: fingerprint }});
      persistViewState();
      setActiveCard(fingerprint, false);
    }};
    link.addEventListener('click', persistLinkState);
    link.addEventListener('mousedown', persistLinkState);
    link.addEventListener('auxclick', persistLinkState);
  }}
  for (const button of favoriteButtons) {{
    button.addEventListener('click', (event) => {{
      event.stopPropagation();
      toggleStoredValue(FAVORITES_KEY, button.dataset.fingerprint || '');
      applyClientState();
    }});
  }}
  for (const button of excludeButtons) {{
    button.addEventListener('click', (event) => {{
      event.stopPropagation();
      toggleStoredValue(EXCLUDED_KEY, button.dataset.fingerprint || '');
      applyClientState();
    }});
  }}
  document.getElementById('favorites-only')?.addEventListener('change', applyClientState);
  document.getElementById('hide-excluded')?.addEventListener('change', applyClientState);
  for (const card of cards) {{
    card.addEventListener('click', () => {{
      const fingerprint = card.dataset.fingerprint || '';
      saveState({{ activeFingerprint: fingerprint }});
      setActiveCard(fingerprint);
    }});
  }}
  if (listPanel) {{
    listPanel.addEventListener('scroll', () => {{
      saveState({{ listScrollTop: listPanel.scrollTop }});
    }}, {{ passive: true }});
  }}
  map.on('moveend', () => {{
    const center = map.getCenter();
    saveState({{
      mapCenter: [center.lat, center.lng],
      mapZoom: map.getZoom(),
    }});
  }});
  window.addEventListener('beforeunload', () => {{
    persistViewState();
  }});
  window.addEventListener('pagehide', persistViewState);
  window.addEventListener('pageshow', () => {{
    restoreState(bounds);
    applyClientState();
  }});
  window.addEventListener('focus', () => {{
    restoreState(bounds);
    applyClientState();
  }});
  document.addEventListener('visibilitychange', () => {{
    if (document.visibilityState === 'hidden') {{
      persistViewState();
      return;
    }}
    restoreState(bounds);
    applyClientState();
  }});
}}

function restoreState(allBounds) {{
  const state = loadState();
  const listPanel = document.querySelector('.cards');
  if (state.mapCenter && typeof state.mapZoom === 'number') {{
    map.setView(state.mapCenter, state.mapZoom);
  }} else if (allBounds.length > 0) {{
    map.fitBounds(allBounds, {{ padding: [24, 24] }});
  }} else {{
    map.setView({DEFAULT_CENTER}, 14);
  }}
  if (listPanel && typeof state.listScrollTop === 'number') {{
    restoreListScroll(state.listScrollTop);
  }}
  if (state.activeFingerprint) {{
    requestAnimationFrame(() => {{
      setActiveCard(state.activeFingerprint, false);
    }});
  }}
}}

function saveState(partial) {{
  const current = loadState();
  sessionStorage.setItem(STORAGE_KEY, JSON.stringify({{ ...current, ...partial }}));
}}

function loadState() {{
  try {{
    const raw = sessionStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : {{}};
  }} catch (error) {{
    return {{}};
  }}
}}

function setActiveCard(fingerprint, scrollIntoView = true) {{
  for (const card of document.querySelectorAll('.card')) {{
    card.classList.toggle('is-active', card.dataset.fingerprint === fingerprint);
  }}
  const active = document.querySelector(`.card[data-fingerprint="${{CSS.escape(fingerprint)}}"]`);
  if (active && scrollIntoView) {{
    active.scrollIntoView({{ block: 'center', behavior: 'smooth' }});
  }}
  const marker = markersByFingerprint.get(fingerprint);
  if (marker) {{
    marker.openPopup();
  }}
}}

function persistViewState() {{
  const listPanel = document.querySelector('.cards');
  const center = map.getCenter();
  saveState({{
    listScrollTop: listPanel ? listPanel.scrollTop : 0,
    mapCenter: [center.lat, center.lng],
    mapZoom: map.getZoom(),
  }});
}}

function restoreListScroll(scrollTop) {{
  const listPanel = document.querySelector('.cards');
  if (!listPanel) return;
  const apply = () => {{
    listPanel.scrollTop = scrollTop;
  }};
  requestAnimationFrame(() => {{
    apply();
    setTimeout(apply, 0);
    setTimeout(apply, 120);
  }});
}}

function applyClientState() {{
  const favorites = loadSet(FAVORITES_KEY);
  const excluded = loadSet(EXCLUDED_KEY);
  const favoritesOnly = document.getElementById('favorites-only')?.checked;
  const hideExcluded = document.getElementById('hide-excluded')?.checked;
  for (const card of document.querySelectorAll('.card')) {{
    const fingerprint = card.dataset.fingerprint || '';
    const isFavorite = favorites.has(fingerprint);
    const isExcluded = excluded.has(fingerprint);
    card.querySelector('.favorite-btn')?.classList.toggle('is-on', isFavorite);
    card.querySelector('.exclude-btn')?.classList.toggle('is-on', isExcluded);
    const shouldHide = (favoritesOnly && !isFavorite) || (hideExcluded && isExcluded);
    card.style.display = shouldHide ? 'none' : '';
    const marker = markersByFingerprint.get(fingerprint);
    if (marker) {{
      if (shouldHide && cluster.hasLayer(marker)) cluster.removeLayer(marker);
      if (!shouldHide && !cluster.hasLayer(marker)) cluster.addLayer(marker);
    }}
  }}
}}

function loadSet(key) {{
  try {{
    const raw = localStorage.getItem(key);
    return new Set(raw ? JSON.parse(raw) : []);
  }} catch (error) {{
    return new Set();
  }}
}}

function toggleStoredValue(key, value) {{
  if (!value) return;
  const items = loadSet(key);
  if (items.has(value)) items.delete(value); else items.add(value);
  localStorage.setItem(key, JSON.stringify(Array.from(items)));
}}
</script>
{render_restore_script(payload)}
"""


def render_restore_script(map_payload: str) -> str:
    return f"""
<script>
history.scrollRestoration = 'manual';
const fallbackListings = {map_payload};
const VIEW_STATE_KEY = 'asamacho-rent-watch:view-state';

function loadViewState() {{
  try {{
    const raw = sessionStorage.getItem(VIEW_STATE_KEY);
    return raw ? JSON.parse(raw) : {{}};
  }} catch (error) {{
    return {{}};
  }}
}}

function saveViewState(partial) {{
  const current = loadViewState();
  sessionStorage.setItem(VIEW_STATE_KEY, JSON.stringify({{ ...current, ...partial }}));
}}

function persistViewState() {{
  const listPanel = document.querySelector('.cards');
  saveViewState({{
    listScrollTop: listPanel ? listPanel.scrollTop : 0,
  }});
}}

function restoreListScroll() {{
  const state = loadViewState();
  const listPanel = document.querySelector('.cards');
  if (!listPanel || typeof state.listScrollTop !== 'number') return;
  const apply = () => {{
    listPanel.scrollTop = state.listScrollTop;
  }};
  requestAnimationFrame(() => {{
    apply();
    setTimeout(apply, 0);
    setTimeout(apply, 120);
  }});
}}

for (const link of document.querySelectorAll('.detail-link, .focus-link')) {{
  const persistLinkState = () => {{
    const fingerprint = link.dataset.fingerprint || '';
    saveViewState({{ activeFingerprint: fingerprint }});
    persistViewState();
  }};
  link.addEventListener('click', persistLinkState);
  link.addEventListener('mousedown', persistLinkState);
  link.addEventListener('auxclick', persistLinkState);
}}

document.querySelector('.cards')?.addEventListener('scroll', persistViewState, {{ passive: true }});
window.addEventListener('beforeunload', persistViewState);
window.addEventListener('pagehide', persistViewState);
window.addEventListener('pageshow', restoreListScroll);
window.addEventListener('focus', restoreListScroll);
document.addEventListener('visibilitychange', () => {{
  if (document.visibilityState === 'hidden') {{
    persistViewState();
    return;
  }}
  restoreListScroll();
}});
restoreListScroll();
</script>
"""


def parse_filters(query: dict[str, list[str]]) -> dict:
    return {
        "keyword": first_value(query, "keyword"),
        "min_rent_yen": parse_int(first_value(query, "min_rent_man"), multiplier=10000),
        "max_rent_yen": parse_int(first_value(query, "max_rent_man"), multiplier=10000),
        "min_area_sqm": parse_float(first_value(query, "min_area")),
        "max_walk_minutes": parse_int(first_value(query, "max_walk")),
        "max_age_days": parse_int(first_value(query, "max_age_days")),
        "sort_key": first_value(query, "sort") or "recent",
    }


def render_filters(filters: dict) -> str:
    return f"""
    <form class="toolbar" method="get">
      <div>
        <label for="keyword">キーワード</label>
        <input id="keyword" name="keyword" value="{escape(filters.get('keyword', ''))}" placeholder="浅間町 1LDK など">
      </div>
      <div>
        <label for="min_rent_man">賃料下限 (万円)</label>
        <input id="min_rent_man" name="min_rent_man" value="{display_number(filters.get('min_rent_yen'), 10000)}" inputmode="decimal">
      </div>
      <div>
        <label for="max_rent_man">賃料上限 (万円)</label>
        <input id="max_rent_man" name="max_rent_man" value="{display_number(filters.get('max_rent_yen'), 10000)}" inputmode="decimal">
      </div>
      <div>
        <label for="min_area">面積下限 (m2)</label>
        <input id="min_area" name="min_area" value="{display_float(filters.get('min_area_sqm'))}" inputmode="decimal">
      </div>
      <div>
        <label for="max_walk">徒歩上限 (分)</label>
        <input id="max_walk" name="max_walk" value="{display_number(filters.get('max_walk_minutes'))}" inputmode="numeric">
      </div>
      <div>
        <label for="max_age_days">掲載日数上限</label>
        <input id="max_age_days" name="max_age_days" value="{display_number(filters.get('max_age_days'))}" inputmode="numeric">
      </div>
      <div>
        <label for="sort">並び替え</label>
        <select id="sort" name="sort">
          {render_sort_options(filters.get('sort_key', 'recent'))}
        </select>
      </div>
      <div>
        <label>&nbsp;</label>
        <button type="submit">更新する</button>
      </div>
    </form>
    """


def render_sort_options(current: str) -> str:
    options = [
        ("recent", "更新が新しい順"),
        ("rent", "賃料が高い順"),
        ("area", "広い順"),
        ("walk", "駅近順"),
    ]
    return "".join(
        f'<option value="{value}"{" selected" if value == current else ""}>{label}</option>'
        for value, label in options
    )


def render_stats(listings) -> str:
    total = len(listings)
    mapped = sum(1 for item in listings if item.latitude is not None and item.longitude is not None)
    duplicates = sum(item.duplicate_count - 1 for item in listings if item.duplicate_count > 1)
    fresh = sum(1 for item in listings if item.age_days <= 3)
    avg_rent_count = sum(1 for item in listings if item.rent_yen is not None)
    avg_area_count = sum(1 for item in listings if item.area_sqm is not None)
    avg_rent = round(sum(item.rent_yen for item in listings if item.rent_yen is not None) / max(1, avg_rent_count) / 10000, 1)
    avg_area = round(sum(item.area_sqm for item in listings if item.area_sqm is not None) / max(1, avg_area_count), 1)
    return f"""
    <section class="stats">
      <div class="stat"><span>表示件数</span><strong>{total}</strong></div>
      <div class="stat"><span>地図表示済み</span><strong>{mapped}</strong></div>
      <div class="stat"><span>重複吸収</span><strong>{duplicates}</strong></div>
      <div class="stat"><span>3日以内</span><strong>{fresh}</strong></div>
      <div class="stat"><span>平均賃料</span><strong>{avg_rent}万円</strong></div>
      <div class="stat"><span>平均面積</span><strong>{avg_area}m2</strong></div>
    </section>
    """


def first_value(query: dict[str, list[str]], key: str) -> str:
    values = query.get(key, [""])
    return values[0].strip()


def parse_int(value: str, *, multiplier: int = 1) -> int | None:
    if not value:
        return None
    try:
        return int(float(value) * multiplier)
    except ValueError:
        return None


def parse_float(value: str) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def display_number(value: int | None, divisor: int = 1) -> str:
    if value is None:
        return ""
    shown = value / divisor
    return str(int(shown)) if shown == int(shown) else str(shown)


def display_float(value: float | None) -> str:
    if value is None:
        return ""
    return str(int(value)) if value == int(value) else str(value)


def status_badge(status: str, age_days: int) -> str:
    labels = {
        "new": "新着",
        "updated": "更新",
        "reposted": "再掲載",
        "seen": f"{age_days}日前",
    }
    return f'<span class="status-pill {escape(status)}">{escape(labels.get(status, status))}</span>'
