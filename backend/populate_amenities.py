#!/usr/bin/env python3
"""
Bulk-populate the amenities table with all Singapore amenity data from
OpenStreetMap via the Overpass API.

Uses a single bounding-box query covering all of Singapore — far faster
than the per-postal-code approach used at runtime.  Run this once (or
periodically to refresh) to ensure training and inference have comprehensive
amenity coverage.

Usage:
    DATABASE_URL="postgresql://..." python3 populate_amenities.py
    python3 populate_amenities.py          # SQLite fallback (local dev)
    python3 populate_amenities.py --dry-run
"""
import os, sys, json, time
import urllib.request
import psycopg2
import psycopg2.extras

DRY_RUN = '--dry-run' in sys.argv

# Singapore bounding box: south, west, north, east
SG_BBOX = '1.1500,103.5900,1.5000,104.1000'

# Overpass query — one request for all useful amenity types in Singapore.
# timeout=180 is generous; the full SG dataset is ~50-100k elements.
OVERPASS_QUERY = f"""
[out:json][timeout:180][bbox:{SG_BBOX}];
(
  node["amenity"="school"];
  way["amenity"="school"];
  node["amenity"="kindergarten"];
  node["amenity"="university"];
  node["amenity"="college"];

  node["amenity"="hawker_centre"];
  way["amenity"="hawker_centre"];
  node["amenity"="food_court"];
  way["amenity"="food_court"];
  node["amenity"="marketplace"];

  node["amenity"="hospital"];
  way["amenity"="hospital"];
  node["healthcare"="hospital"];
  node["amenity"="clinic"];
  node["amenity"="doctors"];
  node["amenity"="pharmacy"];
  node["amenity"="dentist"];

  node["leisure"="park"];
  way["leisure"="park"];
  node["leisure"="garden"];
  way["leisure"="garden"];
  node["leisure"="nature_reserve"];
  way["leisure"="nature_reserve"];
  node["leisure"="sports_centre"];
  node["leisure"="fitness_centre"];
  node["leisure"="swimming_pool"];

  node["amenity"="community_centre"];
  way["amenity"="community_centre"];
  node["amenity"="library"];
  node["amenity"="place_of_worship"];

  node["shop"="supermarket"];
  node["shop"="mall"];
  way["shop"="mall"];
  node["shop"="department_store"];
  node["amenity"="theatre"];
  node["amenity"="cinema"];
);
out center body;
"""

# Map Overpass tags → amenity_type stored in DB
def _classify(el: dict) -> str | None:
    tags = el.get('tags', {})
    amenity = tags.get('amenity', '')
    leisure = tags.get('leisure', '')
    shop    = tags.get('shop', '')
    healthcare = tags.get('healthcare', '')

    if amenity in ('school', 'kindergarten', 'university', 'college'):
        return 'school'
    if amenity in ('hawker_centre', 'food_court', 'marketplace'):
        return 'hawker'
    if amenity in ('hospital', 'clinic', 'doctors', 'pharmacy', 'dentist') \
            or healthcare == 'hospital':
        return 'health'
    if leisure in ('park', 'garden', 'nature_reserve'):
        return 'park'
    if leisure in ('sports_centre', 'fitness_centre', 'swimming_pool'):
        return 'recreation'
    if amenity in ('community_centre', 'library', 'place_of_worship'):
        return 'community'
    if shop in ('supermarket', 'mall', 'department_store'):
        return 'shopping'
    if amenity in ('theatre', 'cinema'):
        return 'entertainment'
    return None


def _fetch_overpass() -> list[dict]:
    mirrors = [
        'https://overpass-api.de/api/interpreter',
        'https://overpass.kumi.systems/api/interpreter',
        'https://maps.mail.ru/osm/tools/overpass/api/interpreter',
    ]
    for mirror in mirrors:
        print(f'Querying {mirror} ...')
        try:
            req = urllib.request.Request(mirror, data=OVERPASS_QUERY.encode(), method='POST')
            req.add_header('User-Agent', 'PropAI-SG/1.0')
            with urllib.request.urlopen(req, timeout=200) as r:
                data = json.loads(r.read())
            elements = data.get('elements', [])
            print(f'  Received {len(elements):,} elements')
            return elements
        except Exception as e:
            print(f'  Mirror failed: {e}')
            time.sleep(2)
    raise RuntimeError('All Overpass mirrors failed')


def _get_coords(el: dict):
    lat = el.get('lat') or (el.get('center') or {}).get('lat')
    lon = el.get('lon') or (el.get('center') or {}).get('lon')
    return (float(lat), float(lon)) if lat and lon else None


def main():
    elements = _fetch_overpass()

    # Parse into rows
    rows = []
    seen = set()
    for el in elements:
        atype = _classify(el)
        if not atype:
            continue
        coords = _get_coords(el)
        if not coords:
            continue
        lat, lon = coords
        # Deduplicate within 11m (4dp)
        key = (atype, round(lat, 4), round(lon, 4))
        if key in seen:
            continue
        seen.add(key)
        tags = el.get('tags', {})
        name = (tags.get('name') or tags.get('name:en') or '').strip()
        rows.append((name or atype, atype, lat, lon, 'overpass'))

    # Count by type
    from collections import Counter
    counts = Counter(r[1] for r in rows)
    print(f'\nParsed {len(rows):,} unique amenities:')
    for t, n in sorted(counts.items()):
        print(f'  {t:<18} {n:>5}')

    if DRY_RUN:
        print('\n--dry-run: no DB writes.')
        return

    DATABASE_URL = os.environ.get('DATABASE_URL', '')
    if not DATABASE_URL:
        raise SystemExit('ERROR: set DATABASE_URL env var (or use --dry-run)')

    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    # Ensure column exists (older schemas may not have it)
    cur.execute("""
        ALTER TABLE amenities ADD COLUMN IF NOT EXISTS source TEXT;
    """)

    # Clear existing Overpass data and reload fresh
    cur.execute("DELETE FROM amenities WHERE source = 'overpass' OR source IS NULL")
    deleted = cur.rowcount
    print(f'\nDeleted {deleted:,} old overpass rows')

    psycopg2.extras.execute_values(cur, """
        INSERT INTO amenities (amenity_name, amenity_type, latitude, longitude, source)
        VALUES %s
    """, rows, page_size=1000)

    conn.commit()
    conn.close()
    print(f'Inserted {len(rows):,} amenity rows into amenities table.')
    print('Done. Retrain the model to use the updated amenity features.')


if __name__ == '__main__':
    main()
