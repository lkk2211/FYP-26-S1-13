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
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        month           TEXT,
        town            TEXT,
        flat_type       TEXT,
        flat_model      TEXT,
        floor_area_sqm  REAL,
        storey_range    TEXT,
        resale_price    REAL,
        remaining_lease TEXT,
        lease_commence_date INTEGER,
        block           TEXT,
        street_name     TEXT,
        upload_batch    TEXT,
        uploaded_at     TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS ura_transactions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        project         TEXT,
        street          TEXT,
        property_type   TEXT,
        market_segment  TEXT,
        postal_district TEXT,
        floor_level     TEXT,
        floor_area_sqft REAL,
        floor_area_sqm  REAL,
        type_of_sale    TEXT,
        type_of_area    TEXT,
        transacted_price REAL,
        unit_price_psf  REAL,
        unit_price_psm  REAL,
        nett_price      REAL,
        tenure          TEXT,
        num_units       INTEGER,
        sale_date       TEXT,
        upload_batch    TEXT,
        uploaded_at     TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS geocoded_addresses (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        search_text     TEXT,
        lat             REAL,
        lon             REAL,
        postal_code     TEXT,
        address         TEXT,
        town            TEXT,
        planning_area   TEXT,
        upload_batch    TEXT,
        uploaded_at     TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS policy_changes (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        effective_month TEXT,
        effective_date  TEXT,
        policy_name     TEXT,
        category        TEXT,
        direction       REAL,
        severity        REAL,
        source          TEXT,
        upload_batch    TEXT,
        uploaded_at     TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS sora_rates (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        rate_date       TEXT,
        published_rate  REAL,
        upload_batch    TEXT,
        uploaded_at     TEXT NOT NULL DEFAULT (datetime('now'))
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
        id              SERIAL PRIMARY KEY,
        month           TEXT,
        town            TEXT,
        flat_type       TEXT,
        flat_model      TEXT,
        floor_area_sqm  REAL,
        storey_range    TEXT,
        resale_price    REAL,
        remaining_lease TEXT,
        lease_commence_date INTEGER,
        block           TEXT,
        street_name     TEXT,
        upload_batch    TEXT,
        uploaded_at     TIMESTAMP NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS ura_transactions (
        id              SERIAL PRIMARY KEY,
        project         TEXT,
        street          TEXT,
        property_type   TEXT,
        market_segment  TEXT,
        postal_district TEXT,
        floor_level     TEXT,
        floor_area_sqft REAL,
        floor_area_sqm  REAL,
        type_of_sale    TEXT,
        type_of_area    TEXT,
        transacted_price REAL,
        unit_price_psf  REAL,
        unit_price_psm  REAL,
        nett_price      REAL,
        tenure          TEXT,
        num_units       INTEGER,
        sale_date       TEXT,
        upload_batch    TEXT,
        uploaded_at     TIMESTAMP NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS geocoded_addresses (
        id              SERIAL PRIMARY KEY,
        search_text     TEXT,
        lat             REAL,
        lon             REAL,
        postal_code     TEXT,
        address         TEXT,
        town            TEXT,
        planning_area   TEXT,
        upload_batch    TEXT,
        uploaded_at     TIMESTAMP NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS policy_changes (
        id              SERIAL PRIMARY KEY,
        effective_month TEXT,
        effective_date  TEXT,
        policy_name     TEXT,
        category        TEXT,
        direction       REAL,
        severity        REAL,
        source          TEXT,
        upload_batch    TEXT,
        uploaded_at     TIMESTAMP NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS sora_rates (
        id              SERIAL PRIMARY KEY,
        rate_date       TEXT,
        published_rate  REAL,
        upload_batch    TEXT,
        uploaded_at     TIMESTAMP NOT NULL DEFAULT NOW()
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

def migrate_db():
    """Add columns introduced after initial deploy without breaking existing DBs.
    Also drops legacy duplicate tables and adds new columns to existing tables."""
    conn = get_db()
    try:
        if USE_POSTGRES:
            cur = _cursor(conn)
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()")
            # Drop legacy tables
            for tbl in ('hdb_transactions', 'private_transactions', 'sync_log'):
                try: cur.execute(f"DROP TABLE IF EXISTS {tbl}")
                except Exception: pass
            # Add new columns to geocoded_addresses (search_text, lat, lon)
            for col_def in [
                "ALTER TABLE geocoded_addresses ADD COLUMN IF NOT EXISTS search_text TEXT",
                "ALTER TABLE geocoded_addresses ADD COLUMN IF NOT EXISTS lat REAL",
                "ALTER TABLE geocoded_addresses ADD COLUMN IF NOT EXISTS lon REAL",
            ]:
                try: cur.execute(col_def)
                except Exception: pass
            # Add new columns to policy_changes
            for col_def in [
                "ALTER TABLE policy_changes ADD COLUMN IF NOT EXISTS effective_month TEXT",
                "ALTER TABLE policy_changes ADD COLUMN IF NOT EXISTS effective_date TEXT",
                "ALTER TABLE policy_changes ADD COLUMN IF NOT EXISTS policy_name TEXT",
                "ALTER TABLE policy_changes ADD COLUMN IF NOT EXISTS category TEXT",
                "ALTER TABLE policy_changes ADD COLUMN IF NOT EXISTS source TEXT",
                "ALTER TABLE policy_changes ALTER COLUMN direction TYPE REAL USING direction::REAL",
                "ALTER TABLE policy_changes ALTER COLUMN severity TYPE REAL USING severity::REAL",
            ]:
                try: cur.execute(col_def)
                except Exception: pass
            # Add block/street_name to hdb_resale for geocoding
            for col_def in [
                "ALTER TABLE hdb_resale ADD COLUMN IF NOT EXISTS block TEXT",
                "ALTER TABLE hdb_resale ADD COLUMN IF NOT EXISTS street_name TEXT",
            ]:
                try: cur.execute(col_def)
                except Exception: pass
            conn.commit()
        else:
            for stmt in [
                "ALTER TABLE users ADD COLUMN created_at TEXT DEFAULT (datetime('now'))",
                "ALTER TABLE geocoded_addresses ADD COLUMN search_text TEXT",
                "ALTER TABLE geocoded_addresses ADD COLUMN lat REAL",
                "ALTER TABLE geocoded_addresses ADD COLUMN lon REAL",
                "ALTER TABLE policy_changes ADD COLUMN effective_month TEXT",
                "ALTER TABLE policy_changes ADD COLUMN effective_date TEXT",
                "ALTER TABLE policy_changes ADD COLUMN policy_name TEXT",
                "ALTER TABLE policy_changes ADD COLUMN category TEXT",
                "ALTER TABLE policy_changes ADD COLUMN source TEXT",
                "ALTER TABLE hdb_resale ADD COLUMN block TEXT",
                "ALTER TABLE hdb_resale ADD COLUMN street_name TEXT",
            ]:
                try:
                    conn.execute(stmt)
                except Exception:
                    pass  # column already exists
            for tbl in ('hdb_transactions', 'private_transactions', 'sync_log'):
                try: conn.execute(f"DROP TABLE IF EXISTS {tbl}")
                except Exception: pass
            conn.commit()
    except Exception as e:
        print(f"migrate_db warning: {e}")
    finally:
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

    try:
        def _fetch_hdb(month_str):
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
            prices = [float(r['resale_price']) for r in records if r.get('resale_price')]
            return (sum(prices) / len(prices), len(prices)) if prices else (None, 0)

        avg_c, vol_c = _fetch_hdb(m_curr)
        avg_p, vol_p = _fetch_hdb(m_prev)

        if avg_c and avg_p and vol_p:
            hdb_price_chg = round((avg_c - avg_p) / avg_p * 100, 1)
            hdb_vol_chg   = round((vol_c - vol_p) / vol_p * 100, 1)
            live = True
    except Exception:
        pass  # fall back to curated figures

    payload = {
        'period': {'current': m_curr_label, 'previous': m_prev_label},
        'last_updated': now.strftime('%b %Y'),
        'live_hdb': live,
        'segments': [
            {'id': 'hdb_resale',   'label': 'HDB Resale',       'price_change': hdb_price_chg, 'volume_change': hdb_vol_chg, 'source': 'data.gov.sg'},
            {'id': 'condo_resale', 'label': 'Condo/Apt Resale', 'price_change': 1.4,           'volume_change': 6.8,         'source': 'URA'},
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
    data   = request.json
    result = predict_price(data)

    try:
        beds = int(data.get('bedrooms', 3))
        area_sqm = float(data.get('area', 1000)) / 10.764
        _BEDS_TO_TYPE = {1:'1 ROOM',2:'2 ROOM',3:'3 ROOM',4:'4 ROOM',5:'5 ROOM'}
        flat_type = _BEDS_TO_TYPE.get(beds, 'EXECUTIVE' if beds >= 6 else '5 ROOM')
        user_id = data.get('user_id') or None

        conn = get_db()
        cur  = _cursor(conn)
        cur.execute(_q("""
            INSERT INTO predictions (user_id, town, flat_type, floor_area_sqm,
                estimated_value, confidence, market_trend, feature_scores, model_version)
            VALUES (?,?,?,?,?,?,?,?,?)
        """), (user_id, result.get('location'), flat_type, round(area_sqm,1),
               result.get('estimated_value'), result.get('confidence'),
               result.get('market_trend'),
               json.dumps([{'name': f['name'],'score': f['score']} for f in result.get('factors', [])]),
               '2.0.0'))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[predict] DB save error: {e}")

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

    # New table counts
    try:
        cur.execute("SELECT COUNT(*) AS n FROM hdb_resale"); hdb_tx_count = dict(cur.fetchone())['n']
    except: hdb_tx_count = 0
    try:
        cur.execute("SELECT COUNT(*) AS n FROM ura_transactions"); priv_tx_count = dict(cur.fetchone())['n']
    except: priv_tx_count = 0
    try:
        cur.execute("SELECT COUNT(*) AS n FROM geocoded_addresses"); geocoded_count = dict(cur.fetchone())['n']
    except: geocoded_count = 0
    try:
        cur.execute("SELECT COUNT(*) AS n FROM policy_changes"); policy_count = dict(cur.fetchone())['n']
    except: policy_count = 0
    try:
        cur.execute("SELECT COUNT(*) AS n FROM sora_rates"); sora_count = dict(cur.fetchone())['n']
    except: sora_count = 0

    cur.execute("SELECT id, full_name, email FROM users ORDER BY id DESC LIMIT 5")
    recent_users = _rows(cur)

    cur.execute("SELECT id, full_name, email, role FROM users ORDER BY id DESC")
    all_users = [{"id": r["id"], "full_name": r["full_name"], "email": r["email"],
                  "role": r["role"], "is_admin": r["role"] == "admin"} for r in _rows(cur)]

    # Predictions by property type (infer from flat_type)
    cur.execute("SELECT flat_type, COUNT(*) AS n FROM predictions GROUP BY flat_type")
    hdb_types = {'1 ROOM','2 ROOM','3 ROOM','4 ROOM','5 ROOM','EXECUTIVE','MULTI-GENERATION'}
    hdb_count, priv_count = 0, 0
    for r in _rows(cur):
        ft = (r.get('flat_type') or '').upper()
        if ft in hdb_types or 'ROOM' in ft:
            hdb_count += r['n']
        else:
            priv_count += r['n']
    predictions_by_type = {'hdb': hdb_count, 'private': priv_count}

    # Top 10 towns by prediction count
    cur.execute("SELECT town, COUNT(*) AS n FROM predictions WHERE town IS NOT NULL GROUP BY town ORDER BY n DESC LIMIT 10")
    predictions_by_town = [{'town': r['town'], 'count': r['n']} for r in _rows(cur)]

    # Daily predictions last 14 days
    if USE_POSTGRES:
        cur.execute("SELECT DATE(predicted_at) AS d, COUNT(*) AS n FROM predictions WHERE predicted_at >= NOW() - INTERVAL '14 days' GROUP BY DATE(predicted_at) ORDER BY d")
    else:
        cur.execute("SELECT DATE(predicted_at) AS d, COUNT(*) AS n FROM predictions WHERE predicted_at >= datetime('now','-14 days') GROUP BY DATE(predicted_at) ORDER BY d")
    daily_predictions = [{'date': str(r['d']), 'count': r['n']} for r in _rows(cur)]

    # Daily registrations last 14 days (requires created_at column from migrate_db)
    daily_registrations = []
    try:
        if USE_POSTGRES:
            cur.execute("SELECT DATE(created_at) AS d, COUNT(*) AS n FROM users WHERE created_at >= NOW() - INTERVAL '14 days' GROUP BY DATE(created_at) ORDER BY d")
        else:
            cur.execute("SELECT DATE(created_at) AS d, COUNT(*) AS n FROM users WHERE created_at >= datetime('now','-14 days') GROUP BY DATE(created_at) ORDER BY d")
        daily_registrations = [{'date': str(r['d']), 'count': r['n']} for r in _rows(cur)]
    except Exception:
        pass

    # Recent 50 predictions
    if USE_POSTGRES:
        cur.execute("SELECT p.id, p.town, p.flat_type, p.floor_area_sqm, p.estimated_value, p.confidence, p.predicted_at, u.full_name FROM predictions p LEFT JOIN users u ON p.user_id=u.id ORDER BY p.predicted_at DESC LIMIT 50")
    else:
        cur.execute("SELECT p.id, p.town, p.flat_type, p.floor_area_sqm, p.estimated_value, p.confidence, p.predicted_at, u.full_name FROM predictions p LEFT JOIN users u ON p.user_id=u.id ORDER BY p.predicted_at DESC LIMIT 50")
    recent_preds = _rows(cur)
    for r in recent_preds:
        for k in list(r.keys()):
            if hasattr(r[k], 'isoformat'):
                r[k] = r[k].isoformat()

    if USE_POSTGRES:
        db_size = "Supabase"
    else:
        db_bytes = os.path.getsize(DB_PATH)
        db_size  = f"{db_bytes/1024:.1f} KB" if db_bytes < 1024**2 else f"{db_bytes/1024**2:.2f} MB"

    conn.close()
    return jsonify({
        "total_users": total_users, "total_predictions": total_predictions,
        "total_records": total_records, "db_size": db_size,
        "hdb_tx_count": hdb_tx_count, "priv_tx_count": priv_tx_count,
        "geocoded_count": geocoded_count, "policy_count": policy_count, "sora_count": sora_count,
        "recent_users": recent_users, "all_users": all_users,
        "predictions_by_type": predictions_by_type,
        "predictions_by_town": predictions_by_town,
        "daily_predictions": daily_predictions,
        "daily_registrations": daily_registrations,
        "recent_predictions": recent_preds,
    })


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
    def _mo(base_date, months_back):
        m = base_date.month - months_back
        y = base_date.year + (m - 1) // 12
        m = ((m - 1) % 12) + 1
        return date(y, m, 1).strftime("%b '%y")

    trend_data, p = [], avg * 0.94
    for i in range(6, 0, -1):
        mo = _mo(today, i - 1)
        p  = p * rng.uniform(1.005, 1.025)
        trend_data.append({"month": mo, "price": int(p)})
    similar = []
    for idx, r in enumerate(sorted(rows, key=lambda r: abs(r["price_sgd"] - avg))[:5]):
        mo   = _mo(today, [1,2,2,3,4][idx])
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


@app.route('/api/property-lookup', methods=['GET'])
def property_lookup():
    """Geocode a postal code and return town, property type, and lease type."""
    import re as _re
    postal = request.args.get('postal', '').strip()
    if not postal:
        return jsonify({'error': 'postal required'}), 400

    # 1. Try geocoded_addresses table first
    try:
        conn = get_db(); cur = _cursor(conn)
        cur.execute(_q("SELECT town, address FROM geocoded_addresses WHERE postal_code = ? LIMIT 1"), (postal,))
        row = _row(cur)
        conn.close()
        if row and row.get('town'):
            town = str(row['town']).strip().upper()
            return jsonify({'town': town, 'property_type': 'HDB', 'lease_type': '99-year Leasehold', 'is_hdb': True})
    except Exception:
        pass

    # 2. OneMap elastic search
    try:
        import urllib.parse
        r = urllib.request.urlopen(
            f"https://www.onemap.gov.sg/api/common/elastic/search"
            f"?searchVal={urllib.parse.quote(postal)}&returnGeom=Y&getAddrDetails=Y&pageNum=1",
            timeout=8
        )
        om = json.loads(r.read())
        result = (om.get('results') or [None])[0]
        if not result:
            return jsonify({'error': 'Not found'}), 404

        lat = float(result.get('LATITUDE') or 0)
        lon = float(result.get('LONGITUDE') or result.get('LONGTITUDE') or 0)
        building = str(result.get('BUILDING') or '').strip().upper()
        blk_no   = str(result.get('BLK_NO') or '').strip()

        # Detect HDB: no named building + numeric block number (e.g. "406", "123A")
        is_hdb = (building in ('NIL', '')) and bool(_re.match(r'^\d+[A-Z]?$', blk_no))
        # Condo: has a proper named building (not NIL)
        is_condo  = not is_hdb and building not in ('NIL', '')
        # Landed: not HDB, no building name — standalone house
        is_landed = not is_hdb and not is_condo

        town = None
        if lat and lon:
            try:
                r2 = urllib.request.urlopen(
                    f"https://www.onemap.gov.sg/api/public/popapi/getPlanningarea"
                    f"?lat={lat}&lon={lon}",
                    timeout=8
                )
                pa_data = json.loads(r2.read())
                if isinstance(pa_data, list) and pa_data:
                    pa = pa_data[0].get('pln_area_n', '').strip().upper()
                else:
                    pa = pa_data.get('pln_area_n', '').strip().upper()
                if pa:
                    _PLANNING_MAP = {
                        'KALLANG': 'KALLANG/WHAMPOA', 'WHAMPOA': 'KALLANG/WHAMPOA',
                        'DOWNTOWN CORE': 'CENTRAL AREA', 'MUSEUM': 'CENTRAL AREA',
                        'SINGAPORE RIVER': 'CENTRAL AREA', 'ROCHOR': 'CENTRAL AREA',
                        'MARINA SOUTH': 'CENTRAL AREA', 'MARINA EAST': 'CENTRAL AREA',
                        'OUTRAM': 'BUKIT MERAH', 'RIVER VALLEY': 'CENTRAL AREA',
                        'NOVENA': 'TOA PAYOH', 'TANGLIN': 'BUKIT TIMAH',
                        'BUONA VISTA': 'CLEMENTI', 'TUAS': 'JURONG WEST',
                        'PIONEER': 'JURONG WEST', 'BOON LAY': 'JURONG WEST',
                        'LIM CHU KANG': 'CHOA CHU KANG', 'MANDAI': 'WOODLANDS',
                        'CENTRAL WATER CATCHMENT': 'BISHAN',
                        'WESTERN WATER CATCHMENT': 'JURONG WEST',
                    }
                    town = _PLANNING_MAP.get(pa, pa)
            except Exception:
                pass

        if is_hdb:
            prop_type  = 'HDB'
            lease_type = '99-year Leasehold'
        elif is_landed:
            prop_type  = 'Landed'
            lease_type = 'Freehold'
        else:
            prop_type  = 'Condominium'
            lease_type = 'Freehold'
        return jsonify({
            'town': town, 'property_type': prop_type,
            'lease_type': lease_type, 'is_hdb': is_hdb,
            'is_landed': is_landed, 'building_name': building,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/hdb/flat-specs', methods=['GET'])
def hdb_flat_specs():
    """Return floor-area stats and max floor for a given flat_type (+ optional town)."""
    import re as _re

    flat_type = request.args.get('flat_type', '').strip().upper()
    town      = request.args.get('town', '').strip().upper()

    # Hardcoded defaults by flat type (sqft) used when table is empty
    _DEFAULTS = {
        '1 ROOM':  {'min': 280,  'max': 500,  'median': 355,  'max_floor': 12},
        '2 ROOM':  {'min': 380,  'max': 560,  'median': 474,  'max_floor': 16},
        '3 ROOM':  {'min': 645,  'max': 830,  'median': 732,  'max_floor': 25},
        '4 ROOM':  {'min': 915,  'max': 1165, 'median': 1001, 'max_floor': 40},
        '5 ROOM':  {'min': 1180, 'max': 1410, 'median': 1292, 'max_floor': 40},
        'EXECUTIVE': {'min': 1390, 'max': 1780, 'median': 1561, 'max_floor': 25},
        'MULTI-GENERATION': {'min': 1550, 'max': 1900, 'median': 1722, 'max_floor': 25},
    }
    defaults = _DEFAULTS.get(flat_type, {'min': 500, 'max': 1500, 'median': 900, 'max_floor': 40})

    def _parse_storey_max(sr):
        nums = _re.findall(r'\d+', str(sr))
        return max(int(n) for n in nums) if nums else 0

    try:
        conn = get_db(); cur = _cursor(conn)
        q_town = " AND UPPER(town) = ?" if town else ""
        params = (flat_type, town) if town else (flat_type,)
        cur.execute(_q(f"""
            SELECT floor_area_sqm, storey_range
            FROM hdb_resale
            WHERE UPPER(flat_type) = ?{q_town}
              AND floor_area_sqm IS NOT NULL AND floor_area_sqm > 0
        """), params)
        rows = cur.fetchall()
        conn.close()
    except Exception:
        rows = []

    if not rows:
        return jsonify({
            'area_sqft_min':    defaults['min'],
            'area_sqft_max':    defaults['max'],
            'area_sqft_median': defaults['median'],
            'max_floor':        defaults['max_floor'],
            'source':           'defaults',
        })

    if isinstance(rows[0], dict):
        areas   = [float(r['floor_area_sqm']) for r in rows]
        storeys = [r.get('storey_range', '') for r in rows]
    else:
        areas   = [float(r[0]) for r in rows]
        storeys = [r[1] for r in rows]

    areas_sqft = sorted([a * 10.764 for a in areas])
    n = len(areas_sqft)
    median_sqft = areas_sqft[n // 2]
    min_sqft    = areas_sqft[max(0, int(n * 0.05))]  # 5th percentile
    max_sqft    = areas_sqft[min(n - 1, int(n * 0.95))]  # 95th percentile

    max_floor = max((_parse_storey_max(s) for s in storeys), default=defaults['max_floor'])
    max_floor = max(max_floor, defaults['max_floor'])  # at least the default

    return jsonify({
        'area_sqft_min':    round(min_sqft / 50) * 50,
        'area_sqft_max':    round(max_sqft / 50) * 50,
        'area_sqft_median': round(median_sqft / 50) * 50,
        'max_floor':        max_floor,
        'source':           'db',
        'count':            n,
    })


@app.route('/api/property-areas', methods=['GET'])
def property_areas():
    """Return distinct floor areas (sqft) and max floor for a property,
    used to build a snapping slider on the predict tab."""
    import re as _re

    postal        = request.args.get('postal', '').strip()
    bedrooms      = int(request.args.get('bedrooms', 3))
    property_type = request.args.get('property_type', 'HDB')

    # Postal sector → URA postal district mapping (first 2 postal digits)
    _SECTOR_TO_DISTRICT = {
        '01':'D01','02':'D01','03':'D01','04':'D01','05':'D01','06':'D01',
        '07':'D02','08':'D02',
        '14':'D03','15':'D03','16':'D03',
        '09':'D04','10':'D04',
        '11':'D05','12':'D05','13':'D05',
        '17':'D06',
        '18':'D07','19':'D07',
        '20':'D08','21':'D08',
        '22':'D09','23':'D09','24':'D09',
        '25':'D10','26':'D10','27':'D10',
        '28':'D11','29':'D11','30':'D11',
        '31':'D12','32':'D12','33':'D12',
        '34':'D13','35':'D13','36':'D13','37':'D13',
        '38':'D14','39':'D14','40':'D14','41':'D14',
        '42':'D15','43':'D15','44':'D15','45':'D15',
        '46':'D16','47':'D16','48':'D16',
        '49':'D17','50':'D17','81':'D17',
        '51':'D18','52':'D18',
        '53':'D19','54':'D19','55':'D19','82':'D19',
        '56':'D20','57':'D20',
        '58':'D21','59':'D21',
        '60':'D22','61':'D22','62':'D22','63':'D22',
        '64':'D23','65':'D23','66':'D23','67':'D23','68':'D23',
        '69':'D24','70':'D24','71':'D24',
        '72':'D25','73':'D25',
        '77':'D26','78':'D26',
        '75':'D27','76':'D27',
        '79':'D28','80':'D28',
    }

    # Fallback condo floor areas (sqft) by bedroom count — typical Singapore condo sizes
    _CONDO_PRESETS = {
        1: [484, 506, 527, 560, 614, 635, 700],
        2: [764, 807, 850, 915, 969, 1044],
        3: [1098, 1163, 1216, 1302, 1389, 1453],
        4: [1432, 1550, 1604, 1722, 1830, 1981],
        5: [2000, 2153, 2400, 2583, 2800, 3000],
        6: [2500, 3000, 3500, 4000, 4500, 5000],
    }

    _BEDS_TO_FLAT = {1:'1 ROOM',2:'2 ROOM',3:'3 ROOM',4:'4 ROOM',5:'5 ROOM',6:'EXECUTIVE'}
    flat_type = _BEDS_TO_FLAT.get(bedrooms, '3 ROOM')

    floor_areas = []
    max_floor   = 50

    try:
        conn = get_db()
        cur  = _cursor(conn)

        if property_type == 'HDB':
            # Distinct floor areas (sqm) → converted to sqft, rounded to 5 sqft
            cur.execute(_q(
                "SELECT DISTINCT floor_area_sqm FROM hdb_resale "
                "WHERE flat_type = ? AND floor_area_sqm IS NOT NULL ORDER BY floor_area_sqm"
            ), (flat_type,))
            rows = cur.fetchall()
            sqm_vals = sorted(set(float(r['floor_area_sqm'] if hasattr(r, '__getitem__') else r[0])
                                  for r in rows if (r['floor_area_sqm'] if hasattr(r, '__getitem__') else r[0])))
            floor_areas = sorted(set(round(s * 10.764 / 5.0) * 5 for s in sqm_vals))

            # Max floor from storey_range
            cur.execute(_q(
                "SELECT storey_range FROM hdb_resale WHERE flat_type = ? AND storey_range LIKE '% TO %'"
            ), (flat_type,))
            storeys = [str(r['storey_range'] if hasattr(r, '__getitem__') else r[0]) for r in cur.fetchall()]
            def _top(s):
                try: return int(s.split(' TO ')[-1].strip())
                except: return 0
            top_floors = [_top(s) for s in storeys]
            if top_floors:
                max_floor = max(top_floors)

        else:  # Condominium
            sector   = postal[:2] if len(postal) >= 2 else ''
            district = _SECTOR_TO_DISTRICT.get(sector, '')
            if district:
                cur.execute(_q(
                    "SELECT DISTINCT floor_area_sqft FROM ura_transactions "
                    "WHERE postal_district = ? AND floor_area_sqft IS NOT NULL AND floor_area_sqft > 0 "
                    "ORDER BY floor_area_sqft"
                ), (district,))
                rows = cur.fetchall()
                raw = sorted(set(float(r['floor_area_sqft'] if hasattr(r, '__getitem__') else r[0])
                                 for r in rows if (r['floor_area_sqft'] if hasattr(r, '__getitem__') else r[0])))
                # Round to nearest 50 sqft, deduplicate
                floor_areas = sorted(set(round(v / 50.0) * 50 for v in raw))

                # Max floor from floor_level field
                cur.execute(_q(
                    "SELECT floor_level FROM ura_transactions "
                    "WHERE postal_district = ? AND floor_level IS NOT NULL AND floor_level != ''"
                ), (district,))
                fl_rows = [str(r['floor_level'] if hasattr(r, '__getitem__') else r[0]) for r in cur.fetchall()]
                def _fl_top(s):
                    nums = _re.findall(r'\d+', s)
                    return int(nums[-1]) if nums else 0
                tops = [_fl_top(s) for s in fl_rows]
                if tops:
                    max_floor = max(tops)

        conn.close()
    except Exception:
        pass

    # Fall back to bedroom-based presets if no DB data
    if not floor_areas:
        if property_type == 'HDB':
            _HDB_PRESETS = {
                '1 ROOM':[330,355],'2 ROOM':[474,506],'3 ROOM':[646,700,753,807],
                '4 ROOM':[915,969,1023,1076,1130],'5 ROOM':[1163,1216,1302,1389,1453,1507],
                'EXECUTIVE':[1313,1399,1485,1571,1657],
            }
            floor_areas = _HDB_PRESETS.get(flat_type, [700,800,900])
        else:
            floor_areas = _CONDO_PRESETS.get(bedrooms, _CONDO_PRESETS[3])

    return jsonify({'floor_areas': floor_areas, 'max_floor': int(max_floor)})


@app.route('/api/admin/upload-transactions', methods=['POST'])
def upload_transactions():
    import csv, io
    tx_type  = request.form.get('type', 'hdb').lower()
    file     = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file provided'}), 400

    filename = (file.filename or '').lower()
    try:
        if filename.endswith('.xlsx') or filename.endswith('.xls'):
            import pandas as pd
            df   = pd.read_excel(io.BytesIO(file.read()))
            rows = df.fillna('').astype(str).to_dict('records')
        else:
            content = file.read().decode('utf-8-sig')
            reader  = csv.DictReader(io.StringIO(content))
            rows    = list(reader)
    except Exception as e:
        return jsonify({'error': f'File parse error: {e}'}), 400

    if not rows:
        return jsonify({'error': 'CSV is empty'}), 400

    batch_id = datetime.datetime.utcnow().isoformat()
    conn = get_db()
    cur  = _cursor(conn)
    inserted = 0

    try:
        def _get(row, *keys, default=None):
            """Case-insensitive key lookup across multiple candidate keys."""
            for k in keys:
                for col in row:
                    if col.strip().lower().replace(' ', '').replace('(', '').replace(')', '').replace('$', '').replace('#', '') == k.lower().replace(' ', '').replace('(', '').replace(')', '').replace('$', '').replace('#', ''):
                        v = row[col]
                        return v if v not in ('', None) else default
            return default

        def _row_exec(cur, sql, params):
            """Execute one row INSERT with savepoint recovery so a bad row
            doesn't abort the whole PostgreSQL transaction."""
            if USE_POSTGRES:
                cur.execute("SAVEPOINT _row_sp")
                try:
                    cur.execute(sql, params)
                    cur.execute("RELEASE SAVEPOINT _row_sp")
                    return True
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT _row_sp")
                    return False
            else:
                try:
                    cur.execute(sql, params)
                    return True
                except Exception:
                    return False

        if tx_type == 'hdb':
            for r in rows:
                # Normalise month: "YYYY-MM" → "YYYY-MM-01" so it's valid for both
                # TEXT and DATE column types in PostgreSQL
                raw_month = str(_get(r,'month') or '').strip()
                if len(raw_month) == 7 and raw_month[4] == '-':   # "YYYY-MM"
                    raw_month = raw_month + '-01'

                ok = _row_exec(cur, _q("""
                        INSERT INTO hdb_resale
                            (month, town, flat_type, flat_model, floor_area_sqm,
                             storey_range, resale_price, remaining_lease,
                             lease_commence_date, block, street_name, upload_batch)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """), (raw_month,
                           _get(r,'town'),
                           _get(r,'flat_type','flat_type'),
                           _get(r,'flat_model','flat_model'),
                           float(str(_get(r,'floor_area_sqm','floor_area_sqm') or '0').replace(',','') or 0),
                           _get(r,'storey_range','storey_range'),
                           float(str(_get(r,'resale_price','resale_price') or '0').replace(',','') or 0),
                           _get(r,'remaining_lease','remaining_lease'),
                           int(float(str(_get(r,'lease_commence_date','lease_commence_date') or '0').replace(',','') or 0)) or None,
                           str(_get(r,'block') or '').strip() or None,
                           _get(r,'street_name','street_name'),
                           batch_id))
                if ok:
                    inserted += 1
        elif tx_type == 'geocoded':
            for r in rows:
                lat_v = float(str(_get(r,'lat','latitude') or '').replace(',','') or 0) or None
                lon_v = float(str(_get(r,'lon','lng','longitude') or '').replace(',','') or 0) or None
                ok = _row_exec(cur, _q("""
                        INSERT INTO geocoded_addresses
                            (search_text, lat, lon, postal_code, address, town, planning_area, upload_batch)
                        VALUES (?,?,?,?,?,?,?,?)
                    """), (
                        _get(r,'search_text','searchtext','search text'),
                        lat_v, lon_v,
                        _get(r,'postal_code','postal','postalcode'),
                        _get(r,'address','full_address','building'),
                        _get(r,'town'),
                        _get(r,'planning_area','planningarea','planning area'),
                        batch_id
                    ))
                if ok:
                    inserted += 1

        elif tx_type == 'policy':
            for r in rows:
                # effective_month: store as YYYY-MM-01 for DB compatibility
                eff_month_raw = _get(r,'effective_month','effectivemonth','date','policy_date','policydate')
                if hasattr(eff_month_raw, 'strftime'):
                    eff_month = eff_month_raw.strftime('%Y-%m-01')
                elif eff_month_raw:
                    s = str(eff_month_raw).strip()
                    eff_month = s[:7] + '-01' if len(s) >= 7 else s
                else:
                    eff_month = None
                # effective_date: full date string YYYY-MM-DD
                eff_date_raw = _get(r,'effective_date','effectivedate')
                if hasattr(eff_date_raw, 'strftime'):
                    eff_date = eff_date_raw.strftime('%Y-%m-%d')
                elif eff_date_raw:
                    eff_date = str(eff_date_raw)[:10]
                else:
                    eff_date = None
                ok = _row_exec(cur, _q("""
                        INSERT INTO policy_changes
                            (effective_month, effective_date, policy_name, category,
                             direction, severity, source, upload_batch)
                        VALUES (?,?,?,?,?,?,?,?)
                    """), (
                        eff_month,
                        eff_date,
                        _get(r,'policy_name','policyname','name','description','measure'),
                        _get(r,'category'),
                        float(str(_get(r,'direction','effect') or '0').replace(',','') or 0),
                        float(str(_get(r,'severity','severity_score','score') or '0').replace(',','') or 0),
                        _get(r,'source','url','reference'),
                        batch_id
                    ))
                if ok:
                    inserted += 1

        elif tx_type == 'sora':
            for r in rows:
                rate_date_raw = _get(r,'sora publication date','sorapublicationdate','date','rate_date','ratedate','published_date')
                # Normalise to a plain string; pandas Timestamp → str gives "YYYY-MM-DD HH:MM:SS"
                if hasattr(rate_date_raw, 'strftime'):
                    rate_date = rate_date_raw.strftime('%Y-%m-%d')
                else:
                    rate_date = str(rate_date_raw).strip() if rate_date_raw else None
                ok = _row_exec(cur, _q("""
                        INSERT INTO sora_rates (rate_date, published_rate, upload_batch)
                        VALUES (?,?,?)
                    """), (
                        rate_date,
                        float(str(_get(r,'compound sora - 3 month','compoundsora-3month','sora_3m','sora','published_rate','rate') or '').replace(',','') or 0) or None,
                        batch_id
                    ))
                if ok:
                    inserted += 1

        # Delete HDB records older than 10 years
        # month is stored as "YYYY-MM-01"; compare as a plain string (ISO sorts correctly)
        if tx_type == 'hdb':
            try:
                if USE_POSTGRES:
                    cur.execute(
                        "DELETE FROM hdb_resale WHERE month IS NOT NULL "
                        "AND month < to_char(NOW() - INTERVAL '10 years', 'YYYY-MM-01')"
                    )
                else:
                    cur.execute(
                        "DELETE FROM hdb_resale WHERE month IS NOT NULL "
                        "AND month < strftime('%Y-%m-01', 'now', '-10 years')"
                    )
            except Exception:
                pass

        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'error': str(e)}), 500

    conn.close()
    return jsonify({'inserted': inserted, 'total_rows': len(rows), 'batch_id': batch_id})


@app.route('/api/admin/sync-ura', methods=['POST'])
def sync_ura():
    """Fetch latest URA private property transactions and insert new records into ura_transactions."""
    access_key = os.environ.get('URA_ACCESS_KEY', '')
    if not access_key:
        return jsonify({'error': 'URA_ACCESS_KEY not configured'}), 400

    URA_BASE = 'https://www.ura.gov.sg/uraDataService'
    _TYPE_MAP = {'1': 'New Sale', '2': 'Sub Sale', '3': 'Resale'}

    def _re_parse_floor(fl):
        import re as _re
        fl = str(fl or '').strip().upper()
        if fl.startswith('B'): return 0.0
        nums = _re.findall(r'\d+', fl)
        if len(nums) >= 2: return (int(nums[0]) + int(nums[1])) / 2
        if len(nums) == 1: return float(nums[0])
        if 'LOW' in fl: return 4.0
        if 'MID' in fl: return 13.0
        if 'HIGH' in fl: return 25.0
        return 10.0

    try:
        # Get URA token
        r = urllib.request.urlopen(urllib.request.Request(
            f'{URA_BASE}/insertNewToken.action',
            headers={'AccessKey': access_key}
        ), timeout=30)
        token_data = json.loads(r.read())
        if token_data.get('Status') != 'Success':
            return jsonify({'error': f'URA token error: {token_data}'}), 500
        token = token_data['Result']
    except Exception as e:
        return jsonify({'error': f'URA token request failed: {e}'}), 500

    batch_id = datetime.datetime.utcnow().isoformat()
    conn = get_db(); cur = _cursor(conn)
    inserted = 0

    try:
        import urllib.parse
        for batch in range(1, 5):
            try:
                req = urllib.request.Request(
                    f'{URA_BASE}/invokeUraDS?service=PMI_Resi_Transaction&batch={batch}',
                    headers={'AccessKey': access_key, 'Token': token}
                )
                r = urllib.request.urlopen(req, timeout=60)
                data = json.loads(r.read())
                if data.get('Status') != 'Success':
                    continue
                for proj in data.get('Result', []):
                    mkt = proj.get('marketSegment', '')
                    for det in proj.get('details', []):
                        cd = str(det.get('contractDate', ''))
                        try:
                            mo, yr = int(cd.split('/')[0]), int(cd.split('/')[1])
                            year = 2000 + yr if yr < 100 else yr
                        except Exception:
                            continue
                        sale_date = f'{year}-{mo:02d}'
                        cur.execute(_q("""
                            INSERT INTO ura_transactions
                                (project, street, property_type, market_segment,
                                 postal_district, floor_level, floor_area_sqft, floor_area_sqm,
                                 type_of_sale, transacted_price, unit_price_psf, unit_price_psm,
                                 tenure, num_units, sale_date, upload_batch)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """), (
                            proj.get('project', ''),
                            det.get('street', ''),
                            det.get('propertyType', ''),
                            mkt,
                            str(det.get('district', '0')).zfill(2),
                            det.get('floorLevel') or det.get('floorRange', ''),
                            float(det.get('area') or 0),
                            float(det.get('area') or 0) / 10.764,
                            _TYPE_MAP.get(str(det.get('typeOfSale', '3')), 'Resale'),
                            float(det.get('price') or 0),
                            float(det.get('unitPrice') or 0),
                            None, det.get('tenure', ''),
                            int(float(det.get('noOfUnits') or 1)),
                            sale_date, batch_id
                        ))
                        inserted += 1
            except Exception:
                continue

        # Delete records older than 10 years
        if USE_POSTGRES:
            cur.execute("DELETE FROM ura_transactions WHERE uploaded_at < NOW() - INTERVAL '10 years'")
        else:
            cur.execute("DELETE FROM ura_transactions WHERE uploaded_at < date('now', '-10 years')")

        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'error': str(e)}), 500

    conn.close()
    return jsonify({'inserted': inserted, 'batch_id': batch_id, 'message': f'Synced {inserted} new URA records'})


@app.route('/api/feedback', methods=['POST'])
def submit_feedback():
    """Send user feedback to the project's Gmail address via SMTP."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    data    = request.get_json(force=True) or {}
    name    = str(data.get('name', '')).strip()
    email   = str(data.get('email', '')).strip()
    message = str(data.get('message', '')).strip()

    if not name or not email or not message:
        return jsonify({'error': 'All fields are required.'}), 400

    RECIPIENT  = 'fyp.26.s1.13@gmail.com'
    SENDER     = os.environ.get('GMAIL_USER', RECIPIENT)
    APP_PASS   = os.environ.get('GMAIL_APP_PASSWORD', '')

    if not APP_PASS:
        # Fallback: log to console so it's not silently lost during dev
        print(f"[FEEDBACK] From: {name} <{email}>\n{message}", flush=True)
        return jsonify({'ok': True, 'note': 'logged (SMTP not configured)'}), 200

    msg = MIMEMultipart('alternative')
    msg['Subject'] = f'[PropAI.sg Feedback] from {name}'
    msg['From']    = SENDER
    msg['To']      = RECIPIENT
    msg['Reply-To'] = email

    body_text = f"Name: {name}\nEmail: {email}\n\nFeedback:\n{message}"
    body_html = f"""
    <div style="font-family:sans-serif;max-width:600px;margin:0 auto">
      <h2 style="color:#1e40af">PropAI.sg — New Feedback</h2>
      <table style="width:100%;border-collapse:collapse">
        <tr><td style="padding:8px 0;color:#64748b;width:80px"><b>Name</b></td><td>{name}</td></tr>
        <tr><td style="padding:8px 0;color:#64748b"><b>Email</b></td><td><a href="mailto:{email}">{email}</a></td></tr>
      </table>
      <hr style="margin:16px 0;border:none;border-top:1px solid #e2e8f0"/>
      <p style="color:#0f172a;white-space:pre-wrap">{message}</p>
    </div>"""

    msg.attach(MIMEText(body_text, 'plain'))
    msg.attach(MIMEText(body_html, 'html'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=10) as smtp:
            smtp.login(SENDER, APP_PASS)
            smtp.sendmail(SENDER, RECIPIENT, msg.as_string())
    except Exception as e:
        print(f"[FEEDBACK SMTP ERROR] {e}", flush=True)
        return jsonify({'error': f'Email delivery failed: {e}'}), 500

    return jsonify({'ok': True})


@app.route('/api/admin/export-report', methods=['GET'])
def export_report():
    try:
        from io import BytesIO
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.colors import HexColor
        from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                         Table, TableStyle, HRFlowable)
        from reportlab.lib.units import cm
        from reportlab.lib import colors as rl_colors
    except ImportError:
        return jsonify({'error': 'reportlab not installed'}), 500

    # ── Gather data ─────────────────────────────────────────────────────────
    conn = get_db()
    cur  = _cursor(conn)

    def cnt(t):
        try:
            cur.execute(f"SELECT COUNT(*) AS n FROM {t}")
            return dict(cur.fetchone())['n']
        except Exception:
            return 0

    total_users  = cnt('users')
    total_preds  = cnt('predictions')
    total_recs   = cnt('price_records')
    hdb_txs      = cnt('hdb_resale')
    priv_txs     = cnt('ura_transactions')

    # Users by role
    cur.execute("SELECT role, COUNT(*) AS n FROM users GROUP BY role")
    role_rows = {r['role']: r['n'] for r in _rows(cur)}
    admin_count = role_rows.get('admin', 0)

    # Predictions by type
    cur.execute("SELECT flat_type, COUNT(*) AS n FROM predictions GROUP BY flat_type ORDER BY n DESC")
    hdb_types = {'1 ROOM','2 ROOM','3 ROOM','4 ROOM','5 ROOM','EXECUTIVE','MULTI-GENERATION'}
    hdb_c, priv_c = 0, 0
    type_rows = _rows(cur)
    for r in type_rows:
        ft = (r.get('flat_type') or '').upper()
        if ft in hdb_types or 'ROOM' in ft:
            hdb_c += r['n']
        else:
            priv_c += r['n']

    # Top towns
    cur.execute("SELECT town, COUNT(*) AS n FROM predictions WHERE town IS NOT NULL GROUP BY town ORDER BY n DESC LIMIT 10")
    top_towns = _rows(cur)

    # Daily predictions last 14 days
    if USE_POSTGRES:
        cur.execute("SELECT DATE(predicted_at) AS d, COUNT(*) AS n FROM predictions WHERE predicted_at >= NOW() - INTERVAL '14 days' GROUP BY DATE(predicted_at) ORDER BY d")
    else:
        cur.execute("SELECT DATE(predicted_at) AS d, COUNT(*) AS n FROM predictions WHERE predicted_at >= datetime('now','-14 days') GROUP BY DATE(predicted_at) ORDER BY d")
    daily_preds = _rows(cur)

    # Daily registrations
    daily_regs = []
    try:
        if USE_POSTGRES:
            cur.execute("SELECT DATE(created_at) AS d, COUNT(*) AS n FROM users WHERE created_at >= NOW() - INTERVAL '14 days' GROUP BY DATE(created_at) ORDER BY d")
        else:
            cur.execute("SELECT DATE(created_at) AS d, COUNT(*) AS n FROM users WHERE created_at >= datetime('now','-14 days') GROUP BY DATE(created_at) ORDER BY d")
        daily_regs = _rows(cur)
    except Exception:
        pass

    # Recent 50 predictions
    cur.execute("SELECT p.id, p.town, p.flat_type, p.floor_area_sqm, p.estimated_value, p.confidence, p.predicted_at, u.full_name FROM predictions p LEFT JOIN users u ON p.user_id=u.id ORDER BY p.predicted_at DESC LIMIT 50")
    recent_preds = _rows(cur)

    if USE_POSTGRES:
        db_size = "Supabase PostgreSQL"
    else:
        db_bytes = os.path.getsize(DB_PATH)
        db_size = f"{db_bytes/1024:.1f} KB" if db_bytes < 1024**2 else f"{db_bytes/1024**2:.2f} MB"

    conn.close()

    # ── Build PDF ────────────────────────────────────────────────────────────
    buf  = BytesIO()
    PAGE = A4
    doc  = SimpleDocTemplate(buf, pagesize=PAGE, rightMargin=2*cm, leftMargin=2*cm,
                              topMargin=2.5*cm, bottomMargin=2*cm)

    SS = getSampleStyleSheet()
    NAVY    = HexColor('#0F172A')
    BLUE    = HexColor('#3B82F6')
    LIGHT   = HexColor('#F8FAFC')
    MID     = HexColor('#64748B')
    GREEN   = HexColor('#10B981')
    RED     = HexColor('#EF4444')

    H1  = ParagraphStyle('H1',  parent=SS['Heading1'], fontSize=22, textColor=NAVY,  spaceAfter=4,  spaceBefore=16)
    H2  = ParagraphStyle('H2',  parent=SS['Heading2'], fontSize=14, textColor=NAVY,  spaceAfter=4,  spaceBefore=12)
    BODY = ParagraphStyle('BD', parent=SS['Normal'],  fontSize=9,  textColor=NAVY,  spaceAfter=2)
    SMALL = ParagraphStyle('SM', parent=SS['Normal'], fontSize=8,  textColor=MID,   spaceAfter=1)

    def hr(): return HRFlowable(width='100%', thickness=0.5, color=HexColor('#E2E8F0'), spaceAfter=6, spaceBefore=6)

    def tbl(data, col_widths, header=True):
        t = Table(data, colWidths=col_widths, repeatRows=1 if header else 0)
        style = [
            ('BACKGROUND', (0,0), (-1,0), NAVY if header else LIGHT),
            ('TEXTCOLOR',  (0,0), (-1,0), HexColor('#FFFFFF') if header else NAVY),
            ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE',   (0,0), (-1,-1), 8),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [HexColor('#FFFFFF'), LIGHT]),
            ('GRID',       (0,0), (-1,-1), 0.3, HexColor('#E2E8F0')),
            ('LEFTPADDING', (0,0), (-1,-1), 6),
            ('RIGHTPADDING',(0,0), (-1,-1), 6),
            ('TOPPADDING',  (0,0), (-1,-1), 4),
            ('BOTTOMPADDING',(0,0),(-1,-1), 4),
            ('VALIGN',     (0,0), (-1,-1), 'MIDDLE'),
        ]
        t.setStyle(TableStyle(style))
        return t

    now_str = datetime.datetime.now().strftime('%d %B %Y, %H:%M')
    story = []

    # ── Cover / Header ────────────────────────────────────────────────────
    story.append(Paragraph('PropAI.sg', ParagraphStyle('BIG', parent=SS['Title'], fontSize=32, textColor=NAVY, spaceAfter=2)))
    story.append(Paragraph('Admin Report', ParagraphStyle('SUB', parent=SS['Normal'], fontSize=16, textColor=BLUE, spaceAfter=6)))
    story.append(Paragraph(f'Generated: {now_str}', SMALL))
    story.append(hr())
    story.append(Spacer(1, 0.3*cm))

    # ── 1. Platform Overview ─────────────────────────────────────────────
    story.append(Paragraph('1. Platform Overview', H1))
    ov_data = [
        ['Metric', 'Value'],
        ['Total Registered Users',    str(total_users)],
        ['  — Admin Users',           str(admin_count)],
        ['  — Regular Users',         str(total_users - admin_count)],
        ['Total Predictions Made',    str(total_preds)],
        ['  — HDB Predictions',       str(hdb_c)],
        ['  — Private Property',      str(priv_c)],
        ['Price Records in DB',       str(total_recs)],
        ['HDB Transaction Records',   str(hdb_txs)],
        ['Private Transaction Records', str(priv_txs)],
        ['Database Size',             db_size],
    ]
    W = doc.width
    story.append(tbl(ov_data, [W*0.6, W*0.4]))
    story.append(Spacer(1, 0.5*cm))

    # ── 2. User Statistics ───────────────────────────────────────────────
    story.append(Paragraph('2. User Statistics', H1))

    if daily_regs:
        story.append(Paragraph('Daily New Registrations (last 14 days)', H2))
        reg_data = [['Date', 'New Registrations']]
        for r in daily_regs:
            reg_data.append([str(r.get('d','')), str(r.get('n', 0))])
        story.append(tbl(reg_data, [W*0.5, W*0.5]))
        story.append(Spacer(1, 0.3*cm))

    if daily_preds:
        story.append(Paragraph('Daily Predictions (last 14 days)', H2))
        dp_data = [['Date', 'Predictions']]
        for r in daily_preds:
            dp_data.append([str(r.get('d','')), str(r.get('n', 0))])
        story.append(tbl(dp_data, [W*0.5, W*0.5]))
        story.append(Spacer(1, 0.3*cm))

    # ── 3. Prediction Analytics ──────────────────────────────────────────
    story.append(Paragraph('3. Prediction Analytics', H1))

    # Breakdown by type
    story.append(Paragraph('Property Type Breakdown', H2))
    pt_data = [['Property Category', 'Predictions', 'Share']]
    t_sum = hdb_c + priv_c or 1
    pt_data.append(['HDB Resale',       str(hdb_c),  f"{hdb_c/t_sum*100:.1f}%"])
    pt_data.append(['Private Property', str(priv_c), f"{priv_c/t_sum*100:.1f}%"])
    story.append(tbl(pt_data, [W*0.5, W*0.25, W*0.25]))
    story.append(Spacer(1, 0.3*cm))

    # Top towns
    if top_towns:
        story.append(Paragraph('Top Areas by Prediction Volume', H2))
        town_data = [['Rank', 'Area/Town', 'Predictions']]
        for i, r in enumerate(top_towns, 1):
            town_data.append([str(i), str(r.get('town','')), str(r.get('n', 0))])
        story.append(tbl(town_data, [W*0.15, W*0.55, W*0.30]))
        story.append(Spacer(1, 0.3*cm))

    # Recent 50 predictions
    story.append(Paragraph('Recent 50 Predictions', H2))
    pred_data = [['#', 'User', 'Area/Town', 'Flat Type', 'Est. Value (S$)', 'Conf %', 'Date']]
    for i, r in enumerate(recent_preds, 1):
        val  = f"{int(r.get('estimated_value') or 0):,}"
        conf = f"{float(r.get('confidence') or 0):.0f}"
        dt_raw = str(r.get('predicted_at') or '')
        dt   = dt_raw[:10] if dt_raw else ''
        pred_data.append([
            str(i),
            str(r.get('full_name') or 'Guest')[:18],
            str(r.get('town') or '-')[:18],
            str(r.get('flat_type') or '-')[:12],
            val, conf, dt
        ])
    story.append(tbl(pred_data, [W*0.05, W*0.16, W*0.18, W*0.14, W*0.16, W*0.10, W*0.11], header=True))
    story.append(Spacer(1, 0.5*cm))

    # ── 4. Database & System ─────────────────────────────────────────────
    story.append(Paragraph('4. Database & System Statistics', H1))
    db_data = [
        ['Table', 'Record Count'],
        ['users',               str(total_users)],
        ['predictions',         str(total_preds)],
        ['price_records',       str(total_recs)],
        ['hdb_resale',       str(hdb_txs)],
        ['ura_transactions', str(priv_txs)],
    ]
    story.append(tbl(db_data, [W*0.6, W*0.4]))
    story.append(Spacer(1, 0.3*cm))

    sys_data = [
        ['Metric', 'Value'],
        ['Database Provider',  'Supabase (PostgreSQL)' if USE_POSTGRES else 'SQLite (local)'],
        ['Database Size',      db_size],
        ['Report Generated',   now_str],
    ]
    story.append(tbl(sys_data, [W*0.6, W*0.4]))
    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph('End of Report — PropAI.sg Admin Portal', SMALL))

    doc.build(story)
    buf.seek(0)

    from flask import send_file
    fname = f"propai_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
    return send_file(buf, as_attachment=True, download_name=fname, mimetype='application/pdf')


init_db()
migrate_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    app.run(host='0.0.0.0', port=port, debug=not USE_POSTGRES)
