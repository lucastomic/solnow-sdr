#!/usr/bin/env python3
"""
SolNow Prospecting Script
Searches for jet ski / personal watercraft rental companies in Spain using Google Places API (New)
and exports results to Excel for SDR outreach.
"""

import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

QUERIES = [
    "alquiler motos de agua {zona}",
    "alquiler moto de agua {zona}",
    "alquiler jet ski {zona}",
    "jet ski rental {zona}",
    "motos de agua {zona}",
    "moto acuática {zona}",
    "alquiler moto acuática {zona}",
    "excursiones moto de agua {zona}",
    "rutas en moto de agua {zona}",
    "safari motos de agua {zona}",
    "jet ski tour {zona}",
    "jet ski hire {zona}",
    "rent jet ski {zona}",
    "personal watercraft rental {zona}",
    "actividades acuáticas {zona}",
    "deportes acuáticos {zona}",
]

EXCLUDE_KEYWORDS = [
    "escuela",
    "club náutico",
    "club nautico",
    "ferry",
    "ferri",
    "academia",
    "federación",
    "federacion",
    "puerto deportivo",
    "capitanía",
    "capitania",
    "concesionario",
    "taller",
    "reparación",
    "reparacion",
    "tienda de recambios",
]

COLUMNS = ["Nombre", "Dirección", "Teléfono", "Web", "Rating", "Reseñas", "Zona", "Estado"]

SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

SEARCH_FIELDS = (
    "nextPageToken,"
    "places.id,"
    "places.displayName,"
    "places.formattedAddress,"
    "places.nationalPhoneNumber,"
    "places.internationalPhoneNumber,"
    "places.websiteUri,"
    "places.rating,"
    "places.userRatingCount"
)


def is_relevant(place):
    """Filter out irrelevant results (no contact info or excluded business types)."""
    if not place.get("websiteUri") and not place.get("nationalPhoneNumber"):
        return False
    name_lower = place.get("displayName", {}).get("text", "").lower()
    return not any(kw in name_lower for kw in EXCLUDE_KEYWORDS)


def text_search(api_key, query, page_token=None):
    """Call Places API (New) Text Search."""
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": SEARCH_FIELDS,
    }
    body = {"textQuery": query, "languageCode": "es"}
    if page_token:
        body["pageToken"] = page_token

    resp = requests.post(SEARCH_URL, json=body, headers=headers)
    if not resp.ok:
        log.error("API error %s: %s", resp.status_code, resp.text)
        resp.raise_for_status()
    return resp.json()


def search_single_query(api_key, query, zona, on_place=None):
    """Execute a single query with pagination, returning {place_id: place} dict."""
    results = {}
    log.info("Searching: %s", query)

    data = text_search(api_key, query)
    pages_fetched = 1

    while True:
        for place in data.get("places", []):
            pid = place.get("id")
            if pid in results:
                continue
            place["_zona"] = zona
            if is_relevant(place):
                results[pid] = place
                name = place.get("displayName", {}).get("text", "?")
                log.info("  + %s", name)
                if on_place:
                    on_place(pid, place)

        token = data.get("nextPageToken")
        if not token or pages_fetched >= 5:
            break

        time.sleep(2)  # Required by Google for nextPageToken
        data = text_search(api_key, query, page_token=token)
        pages_fetched += 1

    return results


def search_zone(api_key, zona, on_place=None, queries=None):
    """Search a zone with multiple queries in parallel, returning deduplicated results."""
    results = {}
    templates = queries if queries else QUERIES
    expanded = [tpl.format(zona=zona) if "{zona}" in tpl else f"{tpl} {zona}" for tpl in templates]

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(search_single_query, api_key, q, zona, on_place): q
            for q in expanded
        }
        for future in as_completed(futures):
            try:
                partial = future.result()
                for pid, place in partial.items():
                    if pid not in results:
                        results[pid] = place
            except Exception:
                query = futures[future]
                log.exception("Query failed: %s", query)

    return results


def run_search(api_key, zones, on_place=None, queries=None):
    """Run multi-zone search. Returns deduplicated zone_results dict.

    Args:
        api_key: Google Places API key
        zones: List of zone names to search
        on_place: Optional callback(pid, place) called for each new unique place found
    """
    raw_results = {}
    with ThreadPoolExecutor(max_workers=len(zones)) as executor:
        futures = {
            executor.submit(search_zone, api_key, zona, on_place, queries): zona
            for zona in zones
        }
        for future in as_completed(futures):
            zona = futures[future]
            log.info("=== Zone: %s ===", zona)
            try:
                raw_results[zona] = future.result()
            except Exception:
                log.exception("Zone failed: %s", zona)
                raw_results[zona] = {}

    # Cross-zone deduplication (preserves order of zones as given)
    zone_results = {}
    global_seen = set()
    for zona in zones:
        results = raw_results.get(zona, {})
        unique = {}
        for pid, place in results.items():
            if pid not in global_seen:
                global_seen.add(pid)
                unique[pid] = place
        zone_results[zona] = unique
        log.info("Zone %s: %d leads", zona, len(unique))

    return zone_results


def place_to_row(place):
    return [
        place.get("displayName", {}).get("text", ""),
        place.get("formattedAddress", ""),
        place.get("nationalPhoneNumber") or place.get("internationalPhoneNumber", ""),
        place.get("websiteUri", ""),
        place.get("rating", ""),
        place.get("userRatingCount", ""),
        place.get("_zona", ""),
        "",  # Estado — empty for SDR
    ]


def autofit_columns(ws):
    for col_idx, col_cells in enumerate(ws.iter_cols(min_row=1, max_row=ws.max_row), start=1):
        max_len = 0
        for cell in col_cells:
            val = str(cell.value) if cell.value is not None else ""
            max_len = max(max_len, len(val))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 4, 60)


def export_excel(zone_results, filename):
    wb = Workbook()
    wb.remove(wb.active)

    all_rows = []

    for zona, places in zone_results.items():
        ws = wb.create_sheet(title=zona[:31])
        ws.append(COLUMNS)
        for place in places.values():
            row = place_to_row(place)
            ws.append(row)
            all_rows.append(row)
        autofit_columns(ws)

    ws_all = wb.create_sheet(title="All", index=0)
    ws_all.append(COLUMNS)
    for row in all_rows:
        ws_all.append(row)
    autofit_columns(ws_all)

    wb.save(filename)
    log.info("Exported to %s", filename)


def main():
    api_key = os.environ.get("GOOGLE_PLACES_API_KEY")
    if not api_key:
        print("Error: set GOOGLE_PLACES_API_KEY environment variable")
        sys.exit(1)

    zones = sys.argv[1:] if len(sys.argv) > 1 else ["Ibiza", "Mallorca", "Alicante", "Valencia"]

    zone_results = run_search(api_key, zones)

    total = sum(len(v) for v in zone_results.values())
    filename = "prospects_solnow.xlsx"
    export_excel(zone_results, filename)

    print("\n--- Summary ---")
    for zona, places in zone_results.items():
        print(f"  {zona}: {len(places)} leads")
    print(f"  Total: {total} leads")
    print(f"  Output: {filename}")


if __name__ == "__main__":
    main()
