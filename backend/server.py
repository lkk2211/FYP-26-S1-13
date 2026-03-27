from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import sqlite3
import hashlib
import os
import json
import sys
import math
import re
import datetime
import time
import threading
import urllib.request

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from predict import predict_price

app = Flask(__name__, static_folder='../frontend')
CORS(app)

DB_PATH      = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'propaisg.db')
DATABASE_URL  = os.environ.get('DATABASE_URL')  # Set on Render to point at Supabase
USE_POSTGRES  = bool(DATABASE_URL)

# OneMap credentials (set ONEMAP_EMAIL + ONEMAP_PASSWORD env vars on Render)
ONEMAP_EMAIL    = os.environ.get('ONEMAP_EMAIL', '')
ONEMAP_PASSWORD = os.environ.get('ONEMAP_PASSWORD', '')
_om_token_cache = {'token': None, 'expiry': 0}
_om_lock        = threading.Lock()

# ---------------------------------------------------------------------------
# Database helpers — transparently supports SQLite (local) and PostgreSQL (Supabase)
# ---------------------------------------------------------------------------
def get_db():
    if USE_POSTGRES:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _cursor(conn):
    """Return a dict-capable cursor regardless of DB backend."""
    if USE_POSTGRES:
        import psycopg2.extras
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return conn.cursor()

def _rows(cur):
    """Convert cursor results to plain dicts."""
    return [dict(r) for r in cur.fetchall()]

def _row(cur):
    r = cur.fetchone()
    return dict(r) if r else None

PH = '%s' if USE_POSTGRES else '?'   # SQL placeholder character

def _q(sql):
    """Replace ? with %s for PostgreSQL if needed."""
    return sql.replace('?', PH) if USE_POSTGRES else sql

SQLITE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS users (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        full_name     TEXT NOT NULL,
        email         TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        phone         TEXT DEFAULT '',
        role          TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('user','admin'))
    );
    CREATE TABLE IF NOT EXISTS predictions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id         INTEGER REFERENCES users(id) ON DELETE SET NULL,
        town            TEXT,
        flat_type       TEXT,
        floor_area_sqm  REAL,
        estimated_value REAL NOT NULL,
        confidence      REAL,
        market_trend    TEXT,
        feature_scores  TEXT,
        model_version   TEXT NOT NULL DEFAULT 'v1.0.0',
        predicted_at    TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS price_records (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        postal_code     TEXT NOT NULL,
        address         TEXT,
        property_type   TEXT,
        floor_area_sqft REAL,
        num_bedrooms    INTEGER,
        floor_level     INTEGER,
        price_sgd       REAL NOT NULL,
        price_psf       REAL,
        price_date      TEXT NOT NULL,
        data_source     TEXT NOT NULL DEFAULT 'housing.csv'
    );
    CREATE TABLE IF NOT EXISTS amenities (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        amenity_name TEXT NOT NULL,
        amenity_type TEXT NOT NULL,
        latitude     REAL NOT NULL,
        longitude    REAL NOT NULL,
        source       TEXT
    );
    CREATE TABLE IF NOT EXISTS audit_log (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id    INTEGER REFERENCES users(id) ON DELETE SET NULL,
        action     TEXT NOT NULL,
        event_type TEXT NOT NULL,
        details    TEXT,
        logged_at  TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS amenity_cache (
        postal_code TEXT PRIMARY KEY,
        lat         REAL NOT NULL,
        lng         REAL NOT NULL,
        data        TEXT NOT NULL,
        cached_at   TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS news_cache (
        cache_key  TEXT PRIMARY KEY,
        articles   TEXT NOT NULL,
        fetched_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
"""

POSTGRES_SCHEMA = """
    CREATE TABLE IF NOT EXISTS users (
        id            SERIAL PRIMARY KEY,
        full_name     TEXT NOT NULL,
        email         TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        phone         TEXT DEFAULT '',
        role          TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('user','admin'))
    );
    CREATE TABLE IF NOT EXISTS predictions (
        id              SERIAL PRIMARY KEY,
        user_id         INTEGER REFERENCES users(id) ON DELETE SET NULL,
        town            TEXT,
        flat_type       TEXT,
        floor_area_sqm  REAL,
        estimated_value REAL NOT NULL,
        confidence      REAL,
        market_trend    TEXT,
        feature_scores  TEXT,
        model_version   TEXT NOT NULL DEFAULT 'v1.0.0',
        predicted_at    TIMESTAMP NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS price_records (
        id              SERIAL PRIMARY KEY,
        postal_code     TEXT NOT NULL,
        address         TEXT,
        property_type   TEXT,
        floor_area_sqft REAL,
        num_bedrooms    INTEGER,
        floor_level     INTEGER,
        price_sgd       REAL NOT NULL,
        price_psf       REAL,
        price_date      TEXT NOT NULL,
        data_source     TEXT NOT NULL DEFAULT 'housing.csv'
    );
    CREATE TABLE IF NOT EXISTS amenities (
        id           SERIAL PRIMARY KEY,
        amenity_name TEXT NOT NULL,
        amenity_type TEXT NOT NULL,
        latitude     REAL NOT NULL,
        longitude    REAL NOT NULL,
        source       TEXT
    );
    CREATE TABLE IF NOT EXISTS audit_log (
        id         SERIAL PRIMARY KEY,
        user_id    INTEGER REFERENCES users(id) ON DELETE SET NULL,
        action     TEXT NOT NULL,
        event_type TEXT NOT NULL,
        details    TEXT,
        logged_at  TIMESTAMP NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS amenity_cache (
        postal_code TEXT PRIMARY KEY,
        lat         REAL NOT NULL,
        lng         REAL NOT NULL,
        data        TEXT NOT NULL,
        cached_at   TIMESTAMP NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS news_cache (
        cache_key  TEXT PRIMARY KEY,
        articles   TEXT NOT NULL,
        fetched_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
"""

def init_db():
    conn = get_db()
    if USE_POSTGRES:
        cur = _cursor(conn)
        for statement in POSTGRES_SCHEMA.strip().split(';'):
            s = statement.strip()
            if s:
                cur.execute(s)
        conn.commit()
    else:
        conn.executescript(SQLITE_SCHEMA)
        conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# OneMap helpers
# ---------------------------------------------------------------------------
def get_onemap_token():
    """Return a cached OneMap JWT, refreshing if near expiry. None if unconfigured."""
    with _om_lock:
        if _om_token_cache['token'] and time.time() < _om_token_cache['expiry']:
            return _om_token_cache['token']
        if not ONEMAP_EMAIL or not ONEMAP_PASSWORD:
            return None
        try:
            payload = json.dumps({'email': ONEMAP_EMAIL, 'password': ONEMAP_PASSWORD}).encode()
            req = urllib.request.Request(
                'https://www.onemap.gov.sg/api/auth/post/getToken',
                data=payload,
                headers={'Content-Type': 'application/json'},
                method='POST'
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            token = data.get('access_token')
            if token:
                _om_token_cache['token'] = token
                _om_token_cache['expiry'] = time.time() + 172800  # 48 h
            return token
        except Exception as e:
            print(f'OneMap auth error: {e}')
            return None


def _haversine(lat1, lng1, lat2, lng2):
    R = 6371
    d = math.radians
    a = (math.sin(d(lat2 - lat1) / 2) ** 2
         + math.cos(d(lat1)) * math.cos(d(lat2)) * math.sin(d(lng2 - lng1) / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _travel_label(dist_km):
    walk = round(dist_km * 60 / 5)
    if dist_km > 1.0:
        bus = round(dist_km * 60 / 20)
        return f'~{walk} min walk / ~{bus} min bus'
    return f'~{walk} min walk'


def _bbox(lat, lng, radius_km):
    delta = radius_km / 111.0
    return lat - delta, lng - delta, lat + delta, lng + delta


def _parse_om_coords(item):
    """Handle multiple OneMap field name conventions for lat/lng."""
    for lk, lngk in [('LATITUDE', 'LONGITUDE'), ('Lat', 'Lng'), ('lat', 'lng')]:
        if lk in item and lngk in item:
            return float(item[lk]), float(item[lngk])
    # Combined "lat,lng" string
    for key in ('LatLng', 'latlng', 'LATLNG'):
        if key in item:
            parts = str(item[key]).split(',')
            if len(parts) == 2:
                return float(parts[0].strip()), float(parts[1].strip())
    return None, None


def fetch_onemap_transport(lat, lng, mrt_radius=2.0, bus_radius=0.6):
    """Fetch MRT stations (deduplicated) and nearby bus stops from OneMap Themes API."""
    token = get_onemap_token()
    if not token:
        return None

    results = {'mrt': [], 'bus': []}
    lo, lb, hi, hb = _bbox(lat, lng, mrt_radius)

    # --- MRT station exits → deduplicate to unique stations ---
    try:
        url = (f'https://www.onemap.gov.sg/api/public/themesvc/retrieveTheme'
               f'?queryName=mrt_station_exit&extents={lo},{lb},{hi},{hb}')
        req = urllib.request.Request(url, headers={'Authorization': f'Bearer {token}'})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())

        seen = {}
        for ex in data.get('SrchResults', []):
            elat, elng = _parse_om_coords(ex)
            if elat is None:
                continue
            name = (ex.get('NAME') or ex.get('name') or ex.get('SEARCHVAL') or '').strip()
            if not name:
                continue
            # "HOUGANG MRT STATION EXIT A" → "Hougang Mrt Station"
            station = re.sub(r'\s+EXIT\s+[A-Z\d]+$', '', name, flags=re.IGNORECASE)
            station = re.sub(r'\s+\(.*?\)$', '', station).strip().title()
            d = _haversine(lat, lng, elat, elng)
            if d > mrt_radius:
                continue
            if station not in seen or d < seen[station]['_d']:
                seen[station] = {'name': station, 'dist': f'{d:.2f}',
                                 'travel': _travel_label(d), 'lat': elat, 'lng': elng, '_d': d}
        results['mrt'] = sorted(
            [{k: v for k, v in it.items() if k != '_d'} for it in seen.values()],
            key=lambda x: float(x['dist'])
        )[:5]
    except Exception as e:
        print(f'OneMap MRT error: {e}')

    # --- Bus stops (within bus_radius) ---
    lo2, lb2, hi2, hb2 = _bbox(lat, lng, bus_radius)
    try:
        url = (f'https://www.onemap.gov.sg/api/public/themesvc/retrieveTheme'
               f'?queryName=bus_stop&extents={lo2},{lb2},{hi2},{hb2}')
        req = urllib.request.Request(url, headers={'Authorization': f'Bearer {token}'})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())

        bus_list = []
        for s in data.get('SrchResults', []):
            elat, elng = _parse_om_coords(s)
            if elat is None:
                continue
            name = (s.get('NAME') or s.get('name') or s.get('SEARCHVAL') or
                    s.get('DESCRIPTION') or '').strip()
            if not name:
                continue
            d = _haversine(lat, lng, elat, elng)
            if d > bus_radius:
                continue
            bus_list.append({'name': name.title(), 'dist': f'{d:.2f}',
                             'travel': _travel_label(d), 'lat': elat, 'lng': elng})
        results['bus'] = sorted(bus_list, key=lambda x: float(x['dist']))[:5]
    except Exception as e:
        print(f'OneMap bus error: {e}')

    return results


def fetch_overpass_amenities(lat, lng):
    """Fetch schools, parks, healthcare, hawker, community, and bus-stop fallback from Overpass."""
    query = f"""[out:json][timeout:25];(
        node["amenity"="school"](around:1500,{lat},{lng});
        way["amenity"="school"](around:1500,{lat},{lng});
        node["amenity"="university"](around:1500,{lat},{lng});
        node["amenity"="college"](around:1500,{lat},{lng});
        node["amenity"="hospital"](around:2000,{lat},{lng});
        node["healthcare"="hospital"](around:2000,{lat},{lng});
        node["amenity"="clinic"](around:1000,{lat},{lng});
        node["amenity"="doctors"](around:1000,{lat},{lng});
        node["leisure"="park"](around:1200,{lat},{lng});
        way["leisure"="park"](around:1200,{lat},{lng});
        node["amenity"="hawker_centre"](around:1200,{lat},{lng});
        way["amenity"="hawker_centre"](around:1200,{lat},{lng});
        node["amenity"="food_court"](around:1000,{lat},{lng});
        node["amenity"="community_centre"](around:2000,{lat},{lng});
        node["amenity"="library"](around:2000,{lat},{lng});
        node["highway"="bus_stop"](around:600,{lat},{lng});
        node["public_transport"="stop_position"]["bus"="yes"](around:600,{lat},{lng});
    );out center body;"""

    cats = {'school': [], 'park': [], 'health': [], 'hawker': [], 'community': [], '_bus': []}
    try:
        req = urllib.request.Request(
            'https://overpass-api.de/api/interpreter',
            data=query.encode(),
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())

        for el in data.get('elements', []):
            elat = el.get('lat') or (el.get('center') or {}).get('lat')
            elng = el.get('lon') or (el.get('center') or {}).get('lon')
            if not elat or not elng:
                continue
            t    = el.get('tags') or {}
            name = t.get('name') or t.get('name:en') or t.get('ref')
            if not name:
                continue
            d    = _haversine(lat, lng, float(elat), float(elng))
            item = {'name': name, 'dist': f'{d:.2f}', 'travel': _travel_label(d),
                    'lat': float(elat), 'lng': float(elng)}
            if t.get('amenity') in ('school', 'university', 'college'):
                cats['school'].append(item)
            elif t.get('leisure') == 'park':
                cats['park'].append(item)
            elif t.get('amenity') in ('hospital', 'clinic', 'doctors') or t.get('healthcare') == 'hospital':
                cats['health'].append(item)
            elif t.get('amenity') in ('hawker_centre', 'food_court'):
                cats['hawker'].append(item)
            elif t.get('amenity') in ('community_centre', 'community_hall', 'library'):
                cats['community'].append(item)
            elif t.get('highway') == 'bus_stop' or (
                    t.get('public_transport') == 'stop_position' and t.get('bus') == 'yes'):
                cats['_bus'].append(item)
    except Exception as e:
        print(f'Overpass error: {e}')

    for items in cats.values():
        items.sort(key=lambda x: float(x['dist']))
        items[:] = items[:5]
    return cats


# ---------------------------------------------------------------------------
# Postal district → neighbourhood lookup
# ---------------------------------------------------------------------------
POSTAL_DISTRICTS = {
    '01': 'Raffles Place', '02': 'Tanjong Pagar', '03': 'Queenstown',
    '04': 'Telok Blangah', '05': 'Pasir Panjang', '06': 'City Hall',
    '07': 'Bugis',         '08': 'Little India',  '09': 'Orchard',
    '10': 'Tanglin',       '11': 'Newton',         '12': 'Balestier',
    '13': 'Macpherson',    '14': 'Geylang',        '15': 'Katong',
    '16': 'Bedok',         '17': 'Changi',         '18': 'Tampines',
    '19': 'Serangoon',     '20': 'Bishan',         '21': 'Upper Bukit Timah',
    '22': 'Clementi',      '23': 'Bukit Panjang',  '24': 'Lim Chu Kang',
    '25': 'Kranji',        '26': 'Mandai',         '27': 'Upper Thomson',
    '28': 'Bishan',        '29': 'Thomson',        '30': 'Toa Payoh',
    '31': 'Balestier',     '32': 'Boon Keng',      '33': 'Potong Pasir',
    '34': 'Serangoon',     '35': 'Hougang',        '36': 'Punggol',
    '37': 'Pasir Ris',     '38': 'Geylang',        '39': 'Eunos',
    '40': 'Paya Lebar',    '41': 'Tampines',       '42': 'Bedok',
    '43': 'Telok Blangah', '44': 'Harbourfront',   '45': 'Buona Vista',
    '46': 'Clementi',      '47': 'West Coast',     '48': 'Pandan',
    '49': 'Jurong West',   '50': 'Jurong',         '51': 'Jurong East',
    '52': 'Bukit Batok',   '53': 'Bukit Panjang',  '54': 'Choa Chu Kang',
    '55': 'Woodlands',     '56': 'Ang Mo Kio',     '57': 'Ang Mo Kio',
    '58': 'Upper Thomson', '59': 'Yio Chu Kang',   '60': 'Hougang',
    '61': 'Hougang',       '62': 'Sengkang',       '63': 'Sengkang',
    '64': 'Punggol',       '65': 'Tampines',       '66': 'Pasir Ris',
    '67': 'Loyang',        '68': 'Changi',         '69': 'Jurong West',
    '70': 'Jurong West',   '71': 'Boon Lay',       '72': 'Jurong East',
    '73': 'Jurong East',   '75': 'Clementi',       '76': 'West Coast',
    '77': 'Queenstown',    '78': 'Toa Payoh',      '79': 'Marine Parade',
    '80': 'Paya Lebar',    '81': 'Pasir Ris',      '82': 'Tampines',
}


def postal_to_area(postal):
    """Map 6-digit postal code to neighbourhood name via district prefix."""
    return POSTAL_DISTRICTS.get(str(postal)[:2], 'Singapore')


# ---------------------------------------------------------------------------
# News fetching (Google News RSS — no API key, always fresh)
# ---------------------------------------------------------------------------
def fetch_news(query, limit=6, max_age_years=5):
    """Fetch articles from Google News RSS. Returns list of article dicts."""
    import xml.etree.ElementTree as ET
    import urllib.parse
    from email.utils import parsedate_to_datetime

    url = (f'https://news.google.com/rss/search'
           f'?q={urllib.parse.quote(query)}&hl=en-SG&gl=SG&ceid=SG:en')
    articles = []
    cutoff   = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=max_age_years * 365)

    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; PropAISG/1.0; RSS reader)'
        })
        with urllib.request.urlopen(req, timeout=12) as r:
            xml_bytes = r.read()

        root    = ET.fromstring(xml_bytes)
        channel = root.find('channel')
        if channel is None:
            return articles

        for item in channel.findall('item'):
            title  = (item.findtext('title') or '').strip()
            link   = (item.findtext('link') or item.findtext('guid') or '').strip()
            pub    = (item.findtext('pubDate') or '').strip()
            desc   = (item.findtext('description') or '').strip()
            src_el = item.find('source')
            source = (src_el.text or '').strip() if src_el is not None else ''

            if not title or not link:
                continue

            # Strip source suffix appended by Google News: "Title - Source Name"
            if not source and ' - ' in title:
                title, source = title.rsplit(' - ', 1)
                title  = title.strip()
                source = source.strip()

            # Filter by age
            date_str = 'Recent'
            try:
                pub_dt = parsedate_to_datetime(pub)
                if pub_dt < cutoff:
                    continue
                date_str = pub_dt.strftime('%b %Y')
            except Exception:
                pass

            # Strip HTML from description
            summary = re.sub(r'<[^>]+>', '', desc).strip()[:220]

            articles.append({
                'title':   title,
                'url':     link,
                'source':  source or 'News',
                'date':    date_str,
                'summary': summary,
            })
            if len(articles) >= limit:
                break

    except Exception as e:
        print(f'News fetch error ({query[:40]}…): {e}')

    return articles


def _cache_age_hrs(raw_ts):
    """Return age of a cached_at timestamp in hours."""
    try:
        if USE_POSTGRES and hasattr(raw_ts, 'tzinfo') and raw_ts.tzinfo:
            return (datetime.datetime.now(datetime.timezone.utc) - raw_ts).total_seconds() / 3600
        if USE_POSTGRES:
            return (datetime.datetime.now() - raw_ts).total_seconds() / 3600
        return (datetime.datetime.now() - datetime.datetime.fromisoformat(str(raw_ts))).total_seconds() / 3600
    except Exception:
        return 999


@app.route('/api/news', methods=['GET'])
def get_news():
    neighbourhood = (request.args.get('neighbourhood') or '').strip()
    postal        = (request.args.get('postal') or '').strip()
    limit         = min(int(request.args.get('limit', 6)), 10)

    if postal:
        area      = postal_to_area(postal)
        cache_key = f'postal:{postal}'
        query     = f'singapore {area} HDB property resale BTO 2024 2025 2026'
        ttl_hrs   = 4
    elif neighbourhood:
        cache_key = f'hood:{neighbourhood}'
        query     = f'singapore {neighbourhood} HDB property resale 2024 2025 2026'
        ttl_hrs   = 4
    else:
        cache_key = 'general'
        query     = 'singapore property HDB resale BTO market 2025 2026'
        ttl_hrs   = 2

    # Check cache
    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q("SELECT articles, fetched_at FROM news_cache WHERE cache_key = ?"), (cache_key,))
    row = _row(cur)
    conn.close()

    if row and _cache_age_hrs(row['fetched_at']) < ttl_hrs:
        arts = json.loads(row['articles'])
        return jsonify({'articles': arts[:limit], 'area': postal_to_area(postal) if postal else neighbourhood or 'Singapore', 'cached': True})

    # Fetch fresh articles
    articles = fetch_news(query, limit=limit)

    if articles:
        data_json = json.dumps(articles, ensure_ascii=False)
        conn = get_db()
        cur  = _cursor(conn)
        if USE_POSTGRES:
            cur.execute(
                """INSERT INTO news_cache (cache_key, articles) VALUES (%s, %s)
                   ON CONFLICT (cache_key) DO UPDATE SET articles=EXCLUDED.articles, fetched_at=NOW()""",
                (cache_key, data_json)
            )
        else:
            cur.execute(
                """INSERT OR REPLACE INTO news_cache (cache_key, articles, fetched_at)
                   VALUES (?, ?, datetime('now'))""",
                (cache_key, data_json)
            )
        conn.commit()
        conn.close()

    return jsonify({'articles': articles, 'area': postal_to_area(postal) if postal else neighbourhood or 'Singapore', 'cached': False})


def fetch_overpass_mrt_fallback(lat, lng):
    """Overpass fallback for MRT when OneMap credentials are not configured."""
    query = f"""[out:json][timeout:20];(
        node["railway"="station"](around:2000,{lat},{lng});
        way["railway"="station"](around:2000,{lat},{lng});
        node["station"="subway"](around:2000,{lat},{lng});
        node["public_transport"="station"]["subway"="yes"](around:2000,{lat},{lng});
        node["public_transport"="station"]["train"="yes"](around:2000,{lat},{lng});
    );out center body;"""
    items = []
    try:
        req = urllib.request.Request(
            'https://overpass-api.de/api/interpreter',
            data=query.encode(),
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
        for el in data.get('elements', []):
            elat = el.get('lat') or (el.get('center') or {}).get('lat')
            elng = el.get('lon') or (el.get('center') or {}).get('lon')
            if not elat or not elng:
                continue
            name = (el.get('tags') or {}).get('name') or (el.get('tags') or {}).get('name:en')
            if not name:
                continue
            d = _haversine(lat, lng, float(elat), float(elng))
            items.append({'name': name, 'dist': f'{d:.2f}', 'travel': _travel_label(d),
                          'lat': float(elat), 'lng': float(elng)})
    except Exception as e:
        print(f'Overpass MRT fallback error: {e}')
    items.sort(key=lambda x: float(x['dist']))
    return items[:5]


@app.route('/api/amenities', methods=['GET'])
def get_amenities():
    postal  = (request.args.get('postal') or '').strip()
    lat_p   = request.args.get('lat')
    lng_p   = request.args.get('lng')

    if not postal and not (lat_p and lng_p):
        return jsonify({'error': 'postal or lat/lng required'}), 400

    # v2 prefix invalidates pre-bus-stop cache entries
    cache_key = f'v2:{postal}' if postal else f'v2:{float(lat_p):.4f},{float(lng_p):.4f}'

    # --- Cache check ---
    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q("SELECT lat, lng, data, cached_at FROM amenity_cache WHERE postal_code = ?"), (cache_key,))
    row = _row(cur)
    conn.close()

    if row and _cache_age_hrs(row['cached_at']) < 7 * 24:
        return jsonify(json.loads(row['data']))

    # --- Resolve coordinates ---
    if lat_p and lng_p:
        lat, lng = float(lat_p), float(lng_p)
    else:
        try:
            url = (f'https://www.onemap.gov.sg/api/common/elastic/search'
                   f'?searchVal={postal}&returnGeom=Y&getAddrDetails=Y&pageNum=1')
            with urllib.request.urlopen(url, timeout=10) as r:
                geo = json.loads(r.read())
            r0  = geo.get('results', [{}])[0]
            lat = float(r0.get('LATITUDE', 1.3521))
            lng = float(r0.get('LONGITUDE', 103.8198))
        except Exception:
            return jsonify({'error': 'Geocoding failed'}), 400

    # --- Fetch transport (OneMap preferred, Overpass fallback) ---
    transport = fetch_onemap_transport(lat, lng)
    if not transport or not transport.get('mrt'):
        mrt_fallback = fetch_overpass_mrt_fallback(lat, lng)
        if transport is None:
            transport = {'mrt': mrt_fallback, 'bus': []}
        else:
            transport['mrt'] = mrt_fallback

    # --- Fetch other amenities from Overpass ---
    others = fetch_overpass_amenities(lat, lng)

    # Bus stops: OneMap preferred, Overpass fallback
    bus_items = transport.get('bus') or others.pop('_bus', [])
    others.pop('_bus', None)  # remove fallback key if unused

    payload = {
        'postal': postal, 'lat': lat, 'lng': lng,
        'categories': {
            'mrt':       {'label': 'MRT / LRT Stations',     'color': '#8b5cf6', 'icon': '🚇', 'lucide': 'train-front',    'items': transport.get('mrt', [])},
            'bus':       {'label': 'Bus Stops (≤600m)',      'color': '#6366f1', 'icon': '🚌', 'lucide': 'bus',             'items': bus_items},
            'school':    {'label': 'Schools & Universities', 'color': '#10b981', 'icon': '🏫', 'lucide': 'graduation-cap',  'items': others.get('school', [])},
            'park':      {'label': 'Parks & Green Spaces',   'color': '#14b8a6', 'icon': '🌳', 'lucide': 'trees',           'items': others.get('park', [])},
            'health':    {'label': 'Healthcare',             'color': '#f43f5e', 'icon': '🏥', 'lucide': 'heart-pulse',     'items': others.get('health', [])},
            'hawker':    {'label': 'Hawker / Food Centres',  'color': '#f97316', 'icon': '🍜', 'lucide': 'utensils',        'items': others.get('hawker', [])},
            'community': {'label': 'Community & Library',    'color': '#3b82f6', 'icon': '🏛️', 'lucide': 'users',           'items': others.get('community', [])},
        }
    }

    # --- Cache result ---
    data_json = json.dumps(payload, ensure_ascii=False)
    conn = get_db()
    cur  = _cursor(conn)
    if USE_POSTGRES:
        cur.execute(
            """INSERT INTO amenity_cache (postal_code, lat, lng, data)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (postal_code) DO UPDATE
                   SET lat=EXCLUDED.lat, lng=EXCLUDED.lng,
                       data=EXCLUDED.data, cached_at=NOW()""",
            (cache_key, lat, lng, data_json)
        )
    else:
        cur.execute(
            """INSERT OR REPLACE INTO amenity_cache (postal_code, lat, lng, data, cached_at)
               VALUES (?, ?, ?, ?, datetime('now'))""",
            (cache_key, lat, lng, data_json)
        )
    conn.commit()
    conn.close()

    return jsonify(payload)


# Serve Frontend
@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/<path:path>')
def static_proxy(path):
    return send_from_directory(app.static_folder, path)

# API Endpoints
@app.route('/api/predict', methods=['POST'])
def predict():
    data = request.json
    # Extract features from request
    # In a real app, you'd process postal, area, etc.
    # For now, we use the predict_price function from predict.py
    result = predict_price(data)
    return jsonify(result)

@app.route('/api/stats', methods=['GET'])
def stats():
    def count(cur, table):
        cur.execute(f"SELECT COUNT(*) AS n FROM {table}")
        return dict(cur.fetchone())['n']

    conn = get_db()
    cur  = _cursor(conn)

    total_users       = count(cur, 'users')
    total_predictions = count(cur, 'predictions')
    total_records     = count(cur, 'price_records')

    cur.execute("SELECT id, full_name, email FROM users ORDER BY id DESC LIMIT 5")
    recent_users = _rows(cur)

    cur.execute("SELECT id, full_name, email, role FROM users ORDER BY id DESC")
    all_users = [{"id": r["id"], "full_name": r["full_name"], "email": r["email"],
                  "role": r["role"], "is_admin": r["role"] == "admin"} for r in _rows(cur)]

    if USE_POSTGRES:
        db_size = "Supabase"
    else:
        db_bytes = os.path.getsize(DB_PATH)
        db_size  = f"{db_bytes/1024:.1f} KB" if db_bytes < 1024**2 else f"{db_bytes/1024**2:.2f} MB"

    conn.close()
    return jsonify({"total_users": total_users, "total_predictions": total_predictions,
                    "total_records": total_records, "db_size": db_size,
                    "recent_users": recent_users, "all_users": all_users})


@app.route('/api/trend', methods=['GET'])
def trend():
    import statistics, random
    from datetime import date, timedelta
    postal = request.args.get('postal')
    POSTAL_META = {
        "238801": {"property_type": "Condominium", "location": "Marina Bay"},
        "560123": {"property_type": "HDB",         "location": "Hougang"},
        "159088": {"property_type": "HDB",         "location": "Queenstown"},
        "342005": {"property_type": "HDB",         "location": "Toa Payoh"}
    }
    conn = get_db()
    cur  = _cursor(conn)
    if postal:
        cur.execute(_q("SELECT * FROM price_records WHERE postal_code=?"), (str(postal).zfill(6),))
    else:
        cur.execute("SELECT * FROM price_records")
    rows = _rows(cur)
    conn.close()

    prices = [r["price_sgd"] for r in rows] or [450000]
    avg    = statistics.mean(prices)
    rng    = random.Random(int(avg))
    today  = date.today()
    trend_data, p = [], avg * 0.94
    for i in range(6, 0, -1):
        mo = (today.replace(day=1) - timedelta(days=30*(i-1))).strftime("%b %Y")
        p  = p * rng.uniform(1.005, 1.025)
        trend_data.append({"month": mo, "price": int(p)})
    similar = []
    for idx, r in enumerate(sorted(rows, key=lambda r: abs(r["price_sgd"] - avg))[:5]):
        mo   = (today.replace(day=1) - timedelta(days=30*[1,2,2,3,4][idx])).strftime("%b %Y")
        meta = POSTAL_META.get(str(r["postal_code"]), {})
        pt   = r.get("property_type") or meta.get("property_type", "HDB")
        beds = r.get("num_bedrooms") or 0
        similar.append({"address": meta.get("location", r["postal_code"]),
                         "type": f"{beds} Room" if pt == "HDB" else f"{beds} Bed",
                         "floor_area": int(r.get("floor_area_sqft") or 0),
                         "price": int(r["price_sgd"]), "date": mo})
    return jsonify({"trend_data": trend_data, "similar_transactions": similar,
                    "summary": {"avg_price": int(avg), "min_price": int(min(prices)),
                                "max_price": int(max(prices)), "total_transactions": len(rows)}})


@app.route('/api/register', methods=['POST'])
def register():
    data      = request.json
    full_name = data.get('full_name', '').strip()
    email     = data.get('email', '').strip().lower()
    password  = data.get('password', '').strip()

    if not full_name or not email or not password:
        return jsonify({"error": "All fields are required"}), 400

    conn = get_db()
    cur  = _cursor(conn)

    cur.execute(_q("SELECT id FROM users WHERE email = ?"), (email,))
    if _row(cur):
        conn.close()
        return jsonify({"error": "Email already registered"}), 400

    password_hash = hashlib.sha256(password.encode()).hexdigest()

    if USE_POSTGRES:
        cur.execute("INSERT INTO users (full_name, email, password_hash) VALUES (%s, %s, %s) RETURNING id",
                    (full_name, email, password_hash))
        user_id = cur.fetchone()["id"]
    else:
        cur.execute("INSERT INTO users (full_name, email, password_hash) VALUES (?, ?, ?)",
                    (full_name, email, password_hash))
        user_id = cur.lastrowid

    conn.commit()

    cur.execute(_q("SELECT id, full_name, email, phone, role FROM users WHERE id = ?"), (user_id,))
    user = _row(cur)
    conn.close()
    return jsonify({"user": user}), 201


@app.route('/api/login', methods=['POST'])
def login():
    data     = request.json
    email    = data.get('email', '').strip().lower()
    password = data.get('password', '').strip()

    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400

    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q("SELECT * FROM users WHERE email = ?"), (email,))
    user = _row(cur)
    conn.close()

    if not user:
        return jsonify({"error": "User not found"}), 404

    if user['password_hash'] != hashlib.sha256(password.encode()).hexdigest():
        return jsonify({"error": "Wrong password"}), 401

    return jsonify({"user": {"id": user["id"], "full_name": user["full_name"],
                              "email": user["email"], "phone": user["phone"],
                              "role": user["role"], "is_admin": user["role"] == "admin"}})


@app.route('/api/users', methods=['GET'])
def get_users():
    conn = get_db()
    cur  = _cursor(conn)
    cur.execute("SELECT id, full_name, email, phone, role FROM users ORDER BY id ASC")
    users = _rows(cur)
    conn.close()
    return jsonify({"users": users})


@app.route('/api/users/<int:user_id>', methods=['DELETE'])
def delete_user(user_id):
    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q("SELECT id FROM users WHERE id = ?"), (user_id,))
    if not _row(cur):
        conn.close()
        return jsonify({"error": "User not found"}), 404
    cur.execute(_q("DELETE FROM users WHERE id = ?"), (user_id,))
    conn.commit()
    conn.close()
    return jsonify({"message": "User deleted"})


@app.route('/api/users/<int:user_id>/role', methods=['PUT'])
def update_user_role(user_id):
    data = request.json
    role = data.get('role', '').strip()
    if role not in ('user', 'admin'):
        return jsonify({"error": "Invalid role. Must be 'user' or 'admin'"}), 400
    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q("SELECT id FROM users WHERE id = ?"), (user_id,))
    if not _row(cur):
        conn.close()
        return jsonify({"error": "User not found"}), 404
    cur.execute(_q("UPDATE users SET role = ? WHERE id = ?"), (role, user_id))
    conn.commit()
    cur.execute(_q("SELECT id, full_name, email, phone, role FROM users WHERE id = ?"), (user_id,))
    updated = _row(cur)
    conn.close()
    return jsonify({"user": updated})


@app.route('/api/profile/<int:user_id>', methods=['PUT'])
def update_profile(user_id):
    data      = request.json
    full_name = data.get('full_name', '').strip()
    email     = data.get('email', '').strip().lower()
    phone     = data.get('phone', '').strip()

    if not full_name or not email:
        return jsonify({"error": "Full name and email are required"}), 400

    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q("SELECT id FROM users WHERE email = ? AND id != ?"), (email, user_id))
    if _row(cur):
        conn.close()
        return jsonify({"error": "Email already in use"}), 400

    cur.execute(_q("UPDATE users SET full_name = ?, email = ?, phone = ? WHERE id = ?"),
                (full_name, email, phone, user_id))
    conn.commit()
    cur.execute(_q("SELECT id, full_name, email, phone FROM users WHERE id = ?"), (user_id,))
    user = _row(cur)
    conn.close()

    if not user:
        return jsonify({"error": "User not found"}), 404
    return jsonify({"user": user})


init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    app.run(host='0.0.0.0', port=port, debug=not USE_POSTGRES)
