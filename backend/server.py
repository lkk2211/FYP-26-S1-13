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


sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from predict import predict_price

app = Flask(__name__, static_folder='../frontend')
CORS(app)

DB_PATH      = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'propaisg.db')
DATABASE_URL  = os.environ.get('DATABASE_URL')
USE_POSTGRES  = bool(DATABASE_URL)

ONEMAP_EMAIL    = os.environ.get('ONEMAP_EMAIL', '')
ONEMAP_PASSWORD = os.environ.get('ONEMAP_PASSWORD', '')
_om_token_cache = {'token': None, 'expiry': 0}
_om_lock        = threading.Lock()

URA_ACCESS_KEY  = os.environ.get('URA_ACCESS_KEY', '')
_ura_token_cache = {'token': None, 'expiry': 0}
_ura_lock        = threading.Lock()

# Database helpers — supports SQLite (local) and PostgreSQL (Supabase)
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
    if USE_POSTGRES:
        import psycopg2.extras
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return conn.cursor()

def _rows(cur):
    return [dict(r) for r in cur.fetchall()]

def _row(cur):
    r = cur.fetchone()
    return dict(r) if r else None

PH = '%s' if USE_POSTGRES else '?'

def _q(sql):
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
    CREATE TABLE IF NOT EXISTS hdb_resale (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        month               TEXT NOT NULL,
        town                TEXT NOT NULL,
        flat_type           TEXT NOT NULL,
        block               TEXT,
        street_name         TEXT,
        storey_range        TEXT,
        floor_area_sqm      REAL,
        flat_model          TEXT,
        lease_commence_date INTEGER,
        remaining_lease     TEXT,
        resale_price        REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS ura_transactions (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        project        TEXT,
        street         TEXT,
        district       INTEGER,
        market_segment TEXT,
        property_type  TEXT,
        type_of_sale   TEXT,
        tenure         TEXT,
        floor_range    TEXT,
        area_sqm       REAL,
        price          REAL NOT NULL,
        no_of_units    INTEGER DEFAULT 1,
        contract_date  TEXT,
        contract_year  INTEGER,
        contract_month INTEGER
    );
    CREATE TABLE IF NOT EXISTS sync_log (
        source    TEXT PRIMARY KEY,
        last_sync TEXT NOT NULL,
        records   INTEGER DEFAULT 0
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
    CREATE TABLE IF NOT EXISTS hdb_resale (
        id                  SERIAL PRIMARY KEY,
        month               TEXT NOT NULL,
        town                TEXT NOT NULL,
        flat_type           TEXT NOT NULL,
        block               TEXT,
        street_name         TEXT,
        storey_range        TEXT,
        floor_area_sqm      REAL,
        flat_model          TEXT,
        lease_commence_date INTEGER,
        remaining_lease     TEXT,
        resale_price        REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS ura_transactions (
        id             SERIAL PRIMARY KEY,
        project        TEXT,
        street         TEXT,
        district       INTEGER,
        market_segment TEXT,
        property_type  TEXT,
        type_of_sale   TEXT,
        tenure         TEXT,
        floor_range    TEXT,
        area_sqm       REAL,
        price          REAL NOT NULL,
        no_of_units    INTEGER DEFAULT 1,
        contract_date  TEXT,
        contract_year  INTEGER,
        contract_month INTEGER
    );
    CREATE TABLE IF NOT EXISTS sync_log (
        source    TEXT PRIMARY KEY,
        last_sync TIMESTAMP NOT NULL DEFAULT NOW(),
        records   INTEGER DEFAULT 0
    );
"""

_DB_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_hdb_resale_town      ON hdb_resale(town)",
    "CREATE INDEX IF NOT EXISTS idx_hdb_resale_month     ON hdb_resale(month)",
    "CREATE INDEX IF NOT EXISTS idx_hdb_resale_flat_type ON hdb_resale(flat_type)",
    "CREATE INDEX IF NOT EXISTS idx_ura_district         ON ura_transactions(district)",
    "CREATE INDEX IF NOT EXISTS idx_ura_year             ON ura_transactions(contract_year)",
    "CREATE INDEX IF NOT EXISTS idx_ura_segment          ON ura_transactions(market_segment)",
]

def init_db():
    conn = get_db()
    if USE_POSTGRES:
        cur = _cursor(conn)
        for statement in POSTGRES_SCHEMA.strip().split(';'):
            s = statement.strip()
            if s:
                cur.execute(s)
        for idx in _DB_INDEXES:
            cur.execute(idx)
        conn.commit()
    else:
        conn.executescript(SQLITE_SCHEMA)
        for idx in _DB_INDEXES:
            conn.execute(idx)
        conn.commit()
    conn.close()

# OneMap API helpers
def get_onemap_token():
    with _om_lock:
        if _om_token_cache['token'] and time.time() < _om_token_cache['expiry']:
            return _om_token_cache['token']
        if not ONEMAP_EMAIL or not ONEMAP_PASSWORD:
            return None
        try:
            payload = json.dumps({'email': ONEMAP_EMAIL, 'password': ONEMAP_PASSWORD}).encode()
            req = urllib.request.Request(
                'https://www.onemap.gov.sg/api/auth/post/getToken',
                data = payload,
                headers={'Content-Type': 'application/json'},
                method = 'POST'
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


# ── URA API helpers ──────────────────────────────────────────────────────────
def get_ura_token():
    with _ura_lock:
        if _ura_token_cache['token'] and time.time() < _ura_token_cache['expiry']:
            return _ura_token_cache['token']
        if not URA_ACCESS_KEY:
            return None
        try:
            url = f'https://www.ura.gov.sg/uraDataService/invokeUraDS?service=Token&AccessKey={URA_ACCESS_KEY}'
            req = urllib.request.Request(url, headers={'User-Agent': 'PropAI/1.0'})
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
            if data.get('Status') == 'Success':
                _ura_token_cache['token']  = data['Result']
                _ura_token_cache['expiry'] = time.time() + 82800  # 23 h (token valid 24 h)
                return _ura_token_cache['token']
        except Exception as e:
            print(f'URA token error: {e}')
        return None


def fetch_ura_private_stats():
    """Fetch latest private residential transaction stats from URA (batch 1 = last 3 quarters).
    Returns (price_change_pct, volume_change_pct, live: bool)."""
    token = get_ura_token()
    if not token:
        return 1.4, 6.8, False   # fallback curated figures
    try:
        url = 'https://www.ura.gov.sg/uraDataService/invokeUraDS?service=PMI_Resi_Transaction&batch=1'
        req = urllib.request.Request(
            url, headers={'AccessKey': URA_ACCESS_KEY, 'Token': token, 'User-Agent': 'PropAI/1.0'}
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
        if data.get('Status') != 'Success':
            return 1.4, 6.8, False

        # Flatten transactions and group by MMYY contract date
        from collections import defaultdict
        monthly = defaultdict(list)
        for proj in data.get('Result', []):
            for txn in proj.get('transaction', []):
                cd = str(txn.get('contractDate', '') or '').zfill(4)
                if len(cd) == 4:
                    monthly[cd].append(float(txn.get('price', 0) or 0))

        if len(monthly) < 2:
            return 1.4, 6.8, False

        # Sort by date (MMYY → YYMM for sorting)
        def _sort_key(mmyy):
            return mmyy[2:] + mmyy[:2]

        sorted_months = sorted(monthly.keys(), key=_sort_key)
        curr_key, prev_key = sorted_months[-1], sorted_months[-2]

        curr_prices = monthly[curr_key]
        prev_prices = monthly[prev_key]

        if not curr_prices or not prev_prices:
            return 1.4, 6.8, False

        avg_curr  = sum(curr_prices) / len(curr_prices)
        avg_prev  = sum(prev_prices) / len(prev_prices)
        price_chg = round((avg_curr - avg_prev) / avg_prev * 100, 1)
        vol_chg   = round((len(curr_prices) - len(prev_prices)) / len(prev_prices) * 100, 1)
        return price_chg, vol_chg, True
    except Exception as e:
        print(f'URA stats error: {e}')
        return 1.4, 6.8, False


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
    for lk, lngk in [('LATITUDE', 'LONGITUDE'), ('Lat', 'Lng'), ('lat', 'lng')]:
        if lk in item and lngk in item:
            return float(item[lk]), float(item[lngk])
    for key in ('LatLng', 'latlng', 'LATLNG'):
        if key in item:
            parts = str(item[key]).split(',')
            if len(parts) == 2:
                return float(parts[0].strip()), float(parts[1].strip())
    return None, None


def fetch_onemap_transport(lat, lng, mrt_radius=2.0, bus_radius=0.6):
    token = get_onemap_token()
    if not token:
        return None

    results = {'mrt': [], 'bus': []}
    lo, lb, hi, hb = _bbox(lat, lng, mrt_radius)

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
    query = f"""[out:json][timeout:30];(
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
        node["railway"="station"](around:2000,{lat},{lng});
        way["railway"="station"](around:2000,{lat},{lng});
        relation["railway"="station"](around:2000,{lat},{lng});
        node["station"="subway"](around:2000,{lat},{lng});
        node["station"="light_rail"](around:2000,{lat},{lng});
    );out center body;"""

    cats = {'school': [], 'park': [], 'health': [], 'hawker': [], 'community': [], '_bus': [], '_mrt': []}
    mrt_seen = {}
    try:
        req = urllib.request.Request(
            'https://overpass-api.de/api/interpreter',
            data=query.encode(),
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=35) as r:
            data = json.loads(r.read())

        for el in data.get('elements', []):
            elat = el.get('lat') or (el.get('center') or {}).get('lat')
            elng = el.get('lon') or (el.get('center') or {}).get('lon')
            if not elat or not elng:
                continue
            t    = el.get('tags') or {}
            rtype = t.get('railway', '')
            stype = t.get('station', '')
            is_mrt = rtype == 'station' or stype in ('subway', 'light_rail')
            if is_mrt:
                name = t.get('name:en') or t.get('name') or t.get('ref')
                if not name:
                    continue
                clean = re.sub(r'\s+(MRT|LRT)\s+Station$', '', name, flags=re.IGNORECASE).strip()
                d = _haversine(lat, lng, float(elat), float(elng))
                if clean not in mrt_seen or d < float(mrt_seen[clean]['dist']):
                    mrt_seen[clean] = {'name': name, 'dist': f'{d:.2f}',
                                       'travel': _travel_label(d),
                                       'lat': float(elat), 'lng': float(elng)}
                continue
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

    cats['_mrt'] = sorted(mrt_seen.values(), key=lambda x: float(x['dist']))[:6]
    for key in ('school', 'park', 'health', 'hawker', 'community', '_bus'):
        cats[key].sort(key=lambda x: float(x['dist']))
        cats[key][:] = cats[key][:5]
    return cats


# Postal district → neighbourhood lookup
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
    return POSTAL_DISTRICTS.get(str(postal)[:2], 'Singapore')


# News — Google News RSS, no API key required
def fetch_news(query, limit=6, max_age_years=5):
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

            if not source and ' - ' in title:
                title, source = title.rsplit(' - ', 1)
                title  = title.strip()
                source = source.strip()

            date_str = 'Recent'
            try:
                pub_dt = parsedate_to_datetime(pub)
                if pub_dt < cutoff:
                    continue
                date_str = pub_dt.strftime('%b %Y')
            except Exception:
                pass

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

    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q("SELECT articles, fetched_at FROM news_cache WHERE cache_key = ?"), (cache_key,))
    row = _row(cur)
    conn.close()

    if row and _cache_age_hrs(row['fetched_at']) < ttl_hrs:
        arts = json.loads(row['articles'])
        return jsonify({'articles': arts[:limit], 'area': postal_to_area(postal) if postal else neighbourhood or 'Singapore', 'cached': True})

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
    query = f"""[out:json][timeout:25];(
        node["railway"="station"](around:2000,{lat},{lng});
        way["railway"="station"](around:2000,{lat},{lng});
        relation["railway"="station"](around:2000,{lat},{lng});
        node["station"="subway"](around:2000,{lat},{lng});
        node["station"="light_rail"](around:2000,{lat},{lng});
        node["public_transport"="station"]["subway"="yes"](around:2000,{lat},{lng});
        node["public_transport"="station"]["train"="yes"](around:2000,{lat},{lng});
        node["network"~"MRT|LRT|SMRT|SBS Transit"](around:2000,{lat},{lng});
    );out center tags;"""
    seen = {}
    try:
        req = urllib.request.Request(
            'https://overpass-api.de/api/interpreter',
            data=query.encode(),
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=25) as r:
            data = json.loads(r.read())
        for el in data.get('elements', []):
            elat = el.get('lat') or (el.get('center') or {}).get('lat')
            elng = el.get('lon') or (el.get('center') or {}).get('lon')
            if not elat or not elng:
                continue
            tags = el.get('tags') or {}
            name = tags.get('name:en') or tags.get('name')
            if not name:
                continue
            rtype = tags.get('railway', '')
            ptype = tags.get('public_transport', '')
            if rtype not in ('station', '') and ptype not in ('station', 'stop_area', ''):
                continue
            clean = re.sub(r'\s+(MRT|LRT)\s+Station$', '', name, flags=re.IGNORECASE).strip()
            d = _haversine(lat, lng, float(elat), float(elng))
            if clean not in seen or d < float(seen[clean]['dist']):
                seen[clean] = {'name': name, 'dist': f'{d:.2f}', 'travel': _travel_label(d),
                               'lat': float(elat), 'lng': float(elng)}
    except Exception as e:
        print(f'Overpass MRT fallback error: {e}')
    items = sorted(seen.values(), key=lambda x: float(x['dist']))
    return items[:6]


@app.route('/api/amenities', methods=['GET'])
def get_amenities():
    postal  = (request.args.get('postal') or '').strip()
    lat_p   = request.args.get('lat')
    lng_p   = request.args.get('lng')

    if not postal and not (lat_p and lng_p):
        return jsonify({'error': 'postal or lat/lng required'}), 400

    cache_key = f'v4:{postal}' if postal else f'v4:{float(lat_p):.4f},{float(lng_p):.4f}'

    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q("SELECT lat, lng, data, cached_at FROM amenity_cache WHERE postal_code = ?"), (cache_key,))
    row = _row(cur)
    conn.close()

    if row and _cache_age_hrs(row['cached_at']) < 7 * 24:
        return jsonify(json.loads(row['data']))

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

    transport = fetch_onemap_transport(lat, lng)
    if transport is None:
        transport = {'mrt': [], 'bus': []}

    others = fetch_overpass_amenities(lat, lng)

    # Merge OneMap MRT + Overpass MRT (single Overpass call now covers both)
    def _norm(name):
        n = re.sub(r'\s+(MRT|LRT)\s+Station$', '', name, flags=re.IGNORECASE)
        return re.sub(r'\s+(MRT|LRT)$', '', n, flags=re.IGNORECASE).strip().lower()
    overpass_mrt = others.pop('_mrt', [])
    existing_names = {_norm(it['name']) for it in transport['mrt']}
    for item in overpass_mrt:
        if _norm(item['name']) not in existing_names:
            transport['mrt'].append(item)
            existing_names.add(_norm(item['name']))
    transport['mrt'] = sorted(transport['mrt'], key=lambda x: float(x['dist']))[:6]

    bus_items = transport.get('bus') or others.pop('_bus', [])
    others.pop('_bus', None)

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


@app.route('/api/market-watch')
def market_watch():
    """Month-over-month market stats. Fetches live HDB data from data.gov.sg; other segments use curated URA figures."""
    import urllib.parse

    now = datetime.datetime.now()
    # Last two full months
    if now.month == 1:
        m_curr_dt = datetime.datetime(now.year - 1, 12, 1)
    else:
        m_curr_dt = datetime.datetime(now.year, now.month - 1, 1)

    if m_curr_dt.month == 1:
        m_prev_dt = datetime.datetime(m_curr_dt.year - 1, 12, 1)
    else:
        m_prev_dt = datetime.datetime(m_curr_dt.year, m_curr_dt.month - 1, 1)

    m_curr = m_curr_dt.strftime('%Y-%m')   # e.g. "2026-02"
    m_prev = m_prev_dt.strftime('%Y-%m')   # e.g. "2026-01"
    m_curr_label = m_curr_dt.strftime('%b %Y')
    m_prev_label = m_prev_dt.strftime('%b %Y')

    hdb_price_chg = 0.0
    hdb_vol_chg   = -29.0
    live = False

    # ── Try DB first (hdb_resale table) ──────────────────────
    try:
        conn = get_db(); cur = _cursor(conn)
        cur.execute(_q('SELECT AVG(resale_price) AS avg, COUNT(*) AS cnt '
                       'FROM hdb_resale WHERE month = ?'), (m_curr,))
        r_curr = _row(cur)
        cur.execute(_q('SELECT AVG(resale_price) AS avg, COUNT(*) AS cnt '
                       'FROM hdb_resale WHERE month = ?'), (m_prev,))
        r_prev = _row(cur)
        conn.close()
        if r_curr and r_prev and r_curr.get('avg') and r_prev.get('avg'):
            hdb_price_chg = round((r_curr['avg'] - r_prev['avg']) / r_prev['avg'] * 100, 1)
            hdb_vol_chg   = round((r_curr['cnt'] - r_prev['cnt']) / max(r_prev['cnt'], 1) * 100, 1)
            live = True
    except Exception:
        pass

    # ── Fall back to live data.gov.sg API if DB has no data ──
    if not live:
        try:
            def _fetch_hdb_api(month_str):
                params = urllib.parse.urlencode({
                    'resource_id': 'f1765b54-a209-4718-8d38-a39237f502b3',
                    'filters': json.dumps({'month': month_str}),
                    'limit': 5000
                })
                url = f'https://data.gov.sg/api/action/datastore_search?{params}'
                req = urllib.request.Request(url, headers={'User-Agent': 'PropAI/1.0'})
                with urllib.request.urlopen(req, timeout=8) as resp:
                    data = json.loads(resp.read())
                records = data.get('result', {}).get('records', [])
                prices  = [float(r['resale_price']) for r in records if r.get('resale_price')]
                return (sum(prices) / len(prices), len(prices)) if prices else (None, 0)
            avg_c, vol_c = _fetch_hdb_api(m_curr)
            avg_p, vol_p = _fetch_hdb_api(m_prev)
            if avg_c and avg_p and vol_p:
                hdb_price_chg = round((avg_c - avg_p) / avg_p * 100, 1)
                hdb_vol_chg   = round((vol_c - vol_p) / vol_p * 100, 1)
                live = True
        except Exception:
            pass

    # ── Try ura_transactions table first ─────────────────────
    ura_price_chg, ura_vol_chg, live_ura = 1.4, 6.8, False
    try:
        conn = get_db(); cur = _cursor(conn)
        cur.execute(_q('SELECT AVG(price) AS avg, COUNT(*) AS cnt FROM ura_transactions '
                       'WHERE contract_year = ? AND contract_month = ?'),
                    (m_curr_dt.year, m_curr_dt.month))
        u_curr = _row(cur)
        cur.execute(_q('SELECT AVG(price) AS avg, COUNT(*) AS cnt FROM ura_transactions '
                       'WHERE contract_year = ? AND contract_month = ?'),
                    (m_prev_dt.year, m_prev_dt.month))
        u_prev = _row(cur)
        conn.close()
        if u_curr and u_prev and u_curr.get('avg') and u_prev.get('avg'):
            ura_price_chg = round((u_curr['avg'] - u_prev['avg']) / u_prev['avg'] * 100, 1)
            ura_vol_chg   = round((u_curr['cnt'] - u_prev['cnt']) / max(u_prev['cnt'], 1) * 100, 1)
            live_ura = True
    except Exception:
        pass

    if not live_ura:
        ura_price_chg, ura_vol_chg, live_ura = fetch_ura_private_stats()

    payload = {
        'period': {'current': m_curr_label, 'previous': m_prev_label},
        'last_updated': now.strftime('%b %Y'),
        'live_hdb': live,
        'live_ura': live_ura,
        'segments': [
            {'id': 'hdb_resale',   'label': 'HDB Resale',       'price_change': hdb_price_chg, 'volume_change': hdb_vol_chg,   'source': 'data.gov.sg'},
            {'id': 'condo_resale', 'label': 'Condo/Apt Resale', 'price_change': ura_price_chg, 'volume_change': ura_vol_chg,   'source': 'URA'},
        ]
    }
    return jsonify(payload)


# Static frontend
@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/<path:path>')
def static_proxy(path):
    return send_from_directory(app.static_folder, path)

# API routes
@app.route('/api/predict', methods=['POST'])
def predict():
    data = request.json
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


@app.route('/api/forgot-password', methods=['POST'])
def forgot_password():
    email = (request.json.get('email') or '').strip().lower()
    if not email:
        return jsonify({'error': 'Email is required'}), 400
    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q("SELECT id FROM users WHERE email = ?"), (email,))
    user = _row(cur)
    conn.close()
    if not user:
        return jsonify({'error': 'No account is associated with that email address.'}), 404
    return jsonify({'message': 'Account found'})


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

# ─────────────────────────────────────────────────────────────────────────────
# DATA SYNC  +  HDB / URA QUERY ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

_HDB_ABBREV = {
    r"\bNTH\b":"NORTH",  r"\bSTH\b":"SOUTH",   r"\bUPP\b":"UPPER",
    r"\bLOR\b":"LORONG", r"\bJLN\b":"JALAN",   r"\bBT\b": "BUKIT",
    r"\bKG\b": "KAMPONG",r"\bTG\b": "TANJONG", r"\bCRES\b":"CRESCENT",
    r"\bGDNS\b":"GARDENS",r"\bPK\b":"PARK",    r"\bPL\b": "PLACE",
    r"\bCL\b": "CLOSE",  r"\bDR\b": "DRIVE",   r"\bRD\b": "ROAD",
    r"\bST\b": "STREET", r"\bTER\b":"TERRACE", r"\bMKT\b":"MARKET",
    r"\bCTR\b":"CENTRE", r"\bHTS\b":"HEIGHTS", r"\bRIS\b":"RISE",
}

def _hdb_expand(s):
    for p, r in _HDB_ABBREV.items():
        s = re.sub(p, r, s)
    return s


# ── Sync helpers ──────────────────────────────────────────────────────────────
_sync_lock = threading.Lock()

def _db_count(table):
    try:
        conn = get_db(); cur = _cursor(conn)
        cur.execute(f'SELECT COUNT(*) AS n FROM {table}')
        row = _row(cur); conn.close()
        return (row or {}).get('n', 0)
    except Exception:
        return 0

def _last_sync_age_days(source):
    """Days since last sync, or 9999 if never synced."""
    try:
        conn = get_db(); cur = _cursor(conn)
        cur.execute(_q('SELECT last_sync FROM sync_log WHERE source = ?'), (source,))
        row = _row(cur); conn.close()
        if not row:
            return 9999
        last = str(row['last_sync'])
        dt = datetime.datetime.fromisoformat(last[:19])
        return (datetime.datetime.now() - dt).days
    except Exception:
        return 9999

def _update_sync_log(source, records):
    try:
        conn = get_db(); cur = _cursor(conn)
        if USE_POSTGRES:
            cur.execute(
                'INSERT INTO sync_log (source, last_sync, records) VALUES (%s, NOW(), %s) '
                'ON CONFLICT (source) DO UPDATE SET last_sync = NOW(), records = EXCLUDED.records',
                (source, records))
        else:
            cur.execute(
                "INSERT OR REPLACE INTO sync_log (source, last_sync, records) "
                "VALUES (?, datetime('now'), ?)", (source, records))
        conn.commit(); conn.close()
    except Exception as e:
        print(f'[sync] sync_log update error: {e}')

def _needs_sync(source, stale_days=30):
    tbl = 'hdb_resale' if source == 'hdb' else 'ura_transactions'
    return _db_count(tbl) == 0 or _last_sync_age_days(source) >= stale_days


def _sync_hdb(full=False):
    """Fetch HDB resale transactions from data.gov.sg → hdb_resale table."""
    print('[sync] HDB starting...')
    RESOURCE_ID = 'f1765b54-a209-4718-8d38-a39237f502b3'
    BASE_URL    = 'https://data.gov.sg/api/action/datastore_search'

    conn = get_db(); cur = _cursor(conn)
    if full:
        cur.execute('DELETE FROM hdb_resale')
        conn.commit()
        existing_months = set()
    else:
        cur.execute('SELECT DISTINCT month FROM hdb_resale')
        existing_months = {r['month'] for r in _rows(cur)}
    conn.close()

    import urllib.parse as _up
    offset, limit, total_inserted = 0, 5000, 0

    while True:
        url = BASE_URL + '?' + _up.urlencode(
            {'resource_id': RESOURCE_ID, 'limit': limit, 'offset': offset})
        req = urllib.request.Request(url, headers={'User-Agent': 'PropAI/1.0'})

        # Retry with exponential backoff to handle 429 rate limiting
        data = None
        for attempt in range(4):
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    data = json.loads(r.read())
                break
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    wait = 10 * (2 ** attempt)   # 10s, 20s, 40s, 80s
                    print(f'[sync] HDB 429 at offset {offset}, waiting {wait}s (attempt {attempt+1})')
                    time.sleep(wait)
                else:
                    print(f'[sync] HDB HTTP error at offset {offset}: {e}')
                    break
            except Exception as e:
                print(f'[sync] HDB error at offset {offset}: {e}')
                break

        if data is None:
            print(f'[sync] HDB fetch failed at offset {offset}, stopping sync')
            break

        try:
            result  = data.get('result', {})
            records = result.get('records', [])
            total   = result.get('total', 0)
            if not records:
                break

            batch = []
            for rec in records:
                if rec.get('month') in existing_months:
                    continue
                try:
                    batch.append((
                        str(rec.get('month', '')),
                        str(rec.get('town', '')).strip().upper(),
                        str(rec.get('flat_type', '')).strip().upper(),
                        (str(rec.get('block') or '')).strip().upper() or None,
                        (str(rec.get('street_name') or '')).strip().upper() or None,
                        str(rec.get('storey_range') or '') or None,
                        float(rec['floor_area_sqm']) if rec.get('floor_area_sqm') else None,
                        (str(rec.get('flat_model') or '')).strip().upper() or None,
                        int(rec['lease_commence_date']) if rec.get('lease_commence_date') else None,
                        str(rec.get('remaining_lease') or '') or None,
                        float(rec['resale_price']),
                    ))
                except (ValueError, TypeError):
                    continue

            if batch:
                conn = get_db(); cur = _cursor(conn)
                sql = (
                    'INSERT INTO hdb_resale (month,town,flat_type,block,street_name,'
                    'storey_range,floor_area_sqm,flat_model,lease_commence_date,'
                    'remaining_lease,resale_price) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                ) if USE_POSTGRES else (
                    'INSERT OR IGNORE INTO hdb_resale (month,town,flat_type,block,street_name,'
                    'storey_range,floor_area_sqm,flat_model,lease_commence_date,'
                    'remaining_lease,resale_price) VALUES (?,?,?,?,?,?,?,?,?,?,?)'
                )
                cur.executemany(sql, batch)
                total_inserted += len(batch)
                conn.commit(); conn.close()

            offset += len(records)
            print(f'[sync] HDB {offset:,}/{total:,}', end='\r')
            if offset >= total:
                break
            time.sleep(0.5)   # gentle throttle between pages
        except Exception as e:
            print(f'[sync] HDB parse error at offset {offset}: {e}')
            break

    _update_sync_log('hdb', total_inserted)
    print(f'\n[sync] HDB done — {total_inserted:,} rows inserted')


def _sync_ura(full=False):
    """Fetch URA private residential transactions → ura_transactions table."""
    print('[sync] URA starting...')
    token = get_ura_token()
    if not token:
        print('[sync] URA skipped — no URA_ACCESS_KEY')
        return

    conn = get_db(); cur = _cursor(conn)
    if full:
        cur.execute('DELETE FROM ura_transactions')
        conn.commit()
        existing_pairs = set()
    else:
        cur.execute('SELECT DISTINCT contract_year, contract_month FROM ura_transactions')
        existing_pairs = {(r['contract_year'], r['contract_month']) for r in _rows(cur)}
    conn.close()

    total_inserted = 0
    for batch_num in (range(1, 5) if full else range(1, 2)):
        try:
            url = (f'https://www.ura.gov.sg/uraDataService/invokeUraDS'
                   f'?service=PMI_Resi_Transaction&batch={batch_num}')
            req = urllib.request.Request(
                url, headers={'AccessKey': URA_ACCESS_KEY, 'Token': token,
                              'User-Agent': 'PropAI/1.0'})
            with urllib.request.urlopen(req, timeout=90) as r:
                data = json.loads(r.read())
            if data.get('Status') != 'Success':
                continue

            rows = []
            for proj in data.get('Result', []):
                for txn in proj.get('transaction', []):
                    cd = str(txn.get('contractDate') or '').zfill(4)
                    try:
                        mm = int(cd[:2]); yy = int(cd[2:])
                        yr = 2000 + yy if yy <= 99 else yy
                    except ValueError:
                        continue
                    if (yr, mm) in existing_pairs:
                        continue
                    try:
                        price = float(txn.get('price') or 0)
                        if price <= 0:
                            continue
                        area = txn.get('area')
                        rows.append((
                            (proj.get('project') or '').strip() or None,
                            (proj.get('street')  or '').strip() or None,
                            int(proj['district']) if proj.get('district') else None,
                            proj.get('marketSegment') or None,
                            (txn.get('propertyType') or '').strip().upper() or None,
                            (txn.get('typeOfSale')   or '').strip().upper() or None,
                            (txn.get('tenure')       or '').strip() or None,
                            (txn.get('floorRange')   or '').strip() or None,
                            float(area) if area else None,
                            price,
                            int(txn.get('noOfUnits') or 1),
                            cd, yr, mm,
                        ))
                    except (ValueError, TypeError):
                        continue

            if rows:
                conn = get_db(); cur = _cursor(conn)
                sql = (
                    'INSERT INTO ura_transactions (project,street,district,market_segment,'
                    'property_type,type_of_sale,tenure,floor_range,area_sqm,price,'
                    'no_of_units,contract_date,contract_year,contract_month) '
                    'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                ) if USE_POSTGRES else (
                    'INSERT OR IGNORE INTO ura_transactions (project,street,district,market_segment,'
                    'property_type,type_of_sale,tenure,floor_range,area_sqm,price,'
                    'no_of_units,contract_date,contract_year,contract_month) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
                )
                cur.executemany(sql, rows)
                total_inserted += len(rows)
                conn.commit(); conn.close()

            print(f'[sync] URA batch {batch_num}: {len(rows)} rows')
            time.sleep(1)
        except Exception as e:
            print(f'[sync] URA batch {batch_num} error: {e}')

    _update_sync_log('ura', total_inserted)
    print(f'[sync] URA done — {total_inserted:,} rows inserted')


def _background_sync():
    time.sleep(8)   # let gunicorn workers fully start
    with _sync_lock:
        if _needs_sync('hdb'):
            try:
                _sync_hdb(full=_db_count('hdb_resale') == 0)
            except Exception as e:
                print(f'[sync] HDB background error: {e}')
        if _needs_sync('ura'):
            try:
                _sync_ura(full=_db_count('ura_transactions') == 0)
            except Exception as e:
                print(f'[sync] URA background error: {e}')


@app.route('/api/admin/sync-data', methods=['POST'])
def admin_sync_data():
    """Trigger a data sync. Body: {email, password, source: 'hdb'|'ura'|'all', full: bool}"""
    body   = request.json or {}
    email  = (body.get('email') or '').strip().lower()
    pw     = body.get('password') or ''
    source = body.get('source', 'all')
    full   = bool(body.get('full', False))

    conn = get_db(); cur = _cursor(conn)
    cur.execute(_q('SELECT role FROM users WHERE email = ? AND password_hash = ?'),
                (email, hashlib.sha256(pw.encode()).hexdigest()))
    user = _row(cur); conn.close()

    if not user or user.get('role') != 'admin':
        return jsonify({'error': 'Admin credentials required'}), 403

    def _run():
        with _sync_lock:
            if source in ('hdb', 'all'):
                try: _sync_hdb(full=full)
                except Exception as e: print(f'[sync] admin HDB error: {e}')
            if source in ('ura', 'all'):
                try: _sync_ura(full=full)
                except Exception as e: print(f'[sync] admin URA error: {e}')

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'status': 'sync started', 'source': source, 'full': full})


@app.route('/api/admin/sync-status', methods=['GET'])
def sync_status():
    conn = get_db(); cur = _cursor(conn)
    cur.execute('SELECT source, last_sync, records FROM sync_log')
    log = _rows(cur)
    cur.execute('SELECT COUNT(*) AS n FROM hdb_resale')
    hdb_n = (_row(cur) or {}).get('n', 0)
    cur.execute('SELECT COUNT(*) AS n FROM ura_transactions')
    ura_n = (_row(cur) or {}).get('n', 0)
    conn.close()
    return jsonify({'sync_log': log, 'hdb_rows': hdb_n, 'ura_rows': ura_n})


@app.route('/api/hdb/search', methods=['GET'])
def hdb_search():
    """Autocomplete for towns and streets from hdb_resale table."""
    q = (request.args.get('q') or '').strip().upper()
    if len(q) < 2:
        return jsonify([])
    q_exp   = _hdb_expand(q)
    pattern = f'%{q_exp}%'

    conn = get_db(); cur = _cursor(conn)
    cur.execute(_q('SELECT DISTINCT town FROM hdb_resale WHERE town LIKE ? ORDER BY town LIMIT 6'),
                (f'%{q}%',))
    towns = _rows(cur)
    cur.execute(_q('SELECT DISTINCT street_name, town FROM hdb_resale '
                   'WHERE street_name LIKE ? ORDER BY street_name LIMIT 10'), (pattern,))
    streets = _rows(cur)
    conn.close()

    results = []
    for r in towns:
        results.append({'type': 'town', 'label': r['town'].title(), 'value': r['town']})
    for r in streets:
        label = f"{r['street_name'].title()} ({r['town'].title()})"
        results.append({'type': 'street', 'label': label,
                        'value': r['street_name'], 'town': r['town']})
    return jsonify(results[:12])


@app.route('/api/hdb/trend', methods=['GET'])
def hdb_trend():
    """Price trend + comparable sales from hdb_resale table."""
    raw_town   = (request.args.get('town')   or '').strip().upper()
    raw_street = (request.args.get('street') or '').strip().upper()
    months     = min(int(request.args.get('months', 60)), 420)
    flat_type  = (request.args.get('flat_type') or '').strip().upper()

    if not raw_town and not raw_street:
        return jsonify({'error': 'Provide town or street'}), 400

    cutoff = (datetime.date.today().replace(day=1)
              - datetime.timedelta(days=months * 30)).strftime('%Y-%m')

    filters, params = ['month >= ?'], [cutoff]

    if raw_town and not raw_street:
        filters.append('town = ?')
        params.append(raw_town)
    elif raw_street:
        filters.append('street_name LIKE ?')
        params.append(f'%{_hdb_expand(raw_street)}%')
        if raw_town:
            filters.append('town = ?')
            params.append(raw_town)
    if flat_type:
        filters.append('flat_type = ?')
        params.append(flat_type)

    where = ' AND '.join(filters)

    conn = get_db(); cur = _cursor(conn)

    cur.execute(_q(f"""
        SELECT month AS mo,
               CAST(AVG(resale_price) AS INTEGER) AS avg_price,
               CAST(AVG(resale_price) AS INTEGER) AS median_price,
               COUNT(*) AS transactions
        FROM hdb_resale WHERE {where}
        GROUP BY month ORDER BY month
    """), params)
    trend_rows = _rows(cur)

    cur.execute(_q(f"""
        SELECT month AS mo, flat_type,
               CAST(AVG(resale_price) AS INTEGER) AS avg_price
        FROM hdb_resale WHERE {where}
        GROUP BY month, flat_type ORDER BY month, flat_type
    """), params)
    type_rows = _rows(cur)

    recent_cutoff = (datetime.date.today().replace(day=1)
                     - datetime.timedelta(days=6 * 30)).strftime('%Y-%m')
    comp_filters = ['month >= ?'] + filters[1:]
    comp_params  = [recent_cutoff] + params[1:]
    comp_where   = ' AND '.join(comp_filters)

    cur.execute(_q(f"""
        SELECT block, street_name, flat_type, storey_range,
               floor_area_sqm, resale_price, month
        FROM hdb_resale WHERE {comp_where}
        ORDER BY month DESC, resale_price DESC LIMIT 10
    """), comp_params)
    comparables = _rows(cur)

    cur.execute(_q(f"""
        SELECT COUNT(*) AS total,
               CAST(AVG(resale_price) AS INTEGER) AS avg_price,
               CAST(MIN(resale_price) AS INTEGER) AS min_price,
               CAST(MAX(resale_price) AS INTEGER) AS max_price
        FROM hdb_resale WHERE {where}
    """), params)
    summary = _row(cur) or {'total': 0, 'avg_price': 0, 'min_price': 0, 'max_price': 0}

    ft_filters = filters[1:]
    ft_where   = ' AND '.join(ft_filters) if ft_filters else '1=1'
    cur.execute(_q(f'SELECT DISTINCT flat_type FROM hdb_resale '
                   f'{"WHERE " + ft_where if ft_filters else ""} ORDER BY flat_type'),
                params[1:])
    flat_types = [r['flat_type'] for r in _rows(cur)]

    conn.close()

    return jsonify({
        'meta':        {'town': raw_town or None, 'street': raw_street or None,
                        'months': months, 'flat_type': flat_type or None},
        'trend':       [{'month': r['mo'], 'avg_price': r['avg_price'],
                         'median_price': r['median_price'],
                         'transactions': r['transactions']} for r in trend_rows],
        'by_flat_type':[{'month': r['mo'], 'flat_type': r['flat_type'],
                         'avg_price': r['avg_price']} for r in type_rows],
        'comparables': [{'address':      f"{r['block'] or ''} {(r['street_name'] or '').title()}".strip(),
                         'flat_type':    r['flat_type'],
                         'storey_range': r['storey_range'],
                         'floor_area':   r['floor_area_sqm'],
                         'price':        r['resale_price'],
                         'month':        r['month']} for r in comparables],
        'summary':     {'total':     summary['total'],    'avg_price': summary['avg_price'],
                        'min_price': summary['min_price'],'max_price': summary['max_price']},
        'flat_types':  flat_types,
    })


init_db()

# Start background data sync (non-blocking — only runs if data is missing/stale)
if USE_POSTGRES:
    threading.Thread(target=_background_sync, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    app.run(host='0.0.0.0', port=port, debug=not USE_POSTGRES)
