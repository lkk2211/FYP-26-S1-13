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
    CREATE TABLE IF NOT EXISTS hdb_transactions (
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
        upload_batch    TEXT,
        uploaded_at     TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS private_transactions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        project         TEXT,
        street          TEXT,
        property_type   TEXT,
        market_segment  TEXT,
        district        TEXT,
        floor_area_sqft REAL,
        floor_range     TEXT,
        type_of_sale    TEXT,
        contract_date   TEXT,
        price           REAL,
        unit_price      REAL,
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
    CREATE TABLE IF NOT EXISTS hdb_transactions (
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
        upload_batch    TEXT,
        uploaded_at     TIMESTAMP NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS private_transactions (
        id              SERIAL PRIMARY KEY,
        project         TEXT,
        street          TEXT,
        property_type   TEXT,
        market_segment  TEXT,
        district        TEXT,
        floor_area_sqft REAL,
        floor_range     TEXT,
        type_of_sale    TEXT,
        contract_date   TEXT,
        price           REAL,
        unit_price      REAL,
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
    """Add columns introduced after initial deploy without breaking existing DBs."""
    conn = get_db()
    try:
        if USE_POSTGRES:
            cur = _cursor(conn)
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()")
            conn.commit()
        else:
            try:
                conn.execute("ALTER TABLE users ADD COLUMN created_at TEXT DEFAULT (datetime('now'))")
                conn.commit()
            except Exception:
                pass  # column already exists
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
        cur.execute("SELECT COUNT(*) AS n FROM hdb_transactions"); hdb_tx_count = dict(cur.fetchone())['n']
    except: hdb_tx_count = 0
    try:
        cur.execute("SELECT COUNT(*) AS n FROM private_transactions"); priv_tx_count = dict(cur.fetchone())['n']
    except: priv_tx_count = 0

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


@app.route('/api/admin/upload-transactions', methods=['POST'])
def upload_transactions():
    import csv, io
    tx_type = request.form.get('type', 'hdb').lower()  # 'hdb' or 'private'
    file    = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file provided'}), 400

    try:
        content = file.read().decode('utf-8-sig')
        reader  = csv.DictReader(io.StringIO(content))
        rows    = list(reader)
    except Exception as e:
        return jsonify({'error': f'CSV parse error: {e}'}), 400

    if not rows:
        return jsonify({'error': 'CSV is empty'}), 400

    batch_id = datetime.datetime.utcnow().isoformat()
    conn = get_db()
    cur  = _cursor(conn)
    inserted = 0

    try:
        if tx_type == 'hdb':
            for r in rows:
                def g(k, default=None):
                    for key in r:
                        if key.strip().lower() == k.lower():
                            v = r[key]
                            return v if v != '' else default
                    return default
                try:
                    cur.execute(_q("""
                        INSERT INTO hdb_transactions
                            (month, town, flat_type, flat_model, floor_area_sqm,
                             storey_range, resale_price, remaining_lease,
                             lease_commence_date, upload_batch)
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    """), (g('month'), g('town'), g('flat_type'), g('flat_model'),
                           float(g('floor_area_sqm') or 0),
                           g('storey_range'), float(g('resale_price') or 0),
                           g('remaining_lease'),
                           int(float(g('lease_commence_date') or 0)) or None,
                           batch_id))
                    inserted += 1
                except Exception:
                    continue
        else:
            for r in rows:
                def gp(k, default=None):
                    for key in r:
                        if key.strip().lower() == k.lower():
                            v = r[key]
                            return v if v != '' else default
                    return default
                try:
                    cur.execute(_q("""
                        INSERT INTO private_transactions
                            (project, street, property_type, market_segment,
                             district, floor_area_sqft, floor_range,
                             type_of_sale, contract_date, price, unit_price, upload_batch)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """), (gp('project'), gp('street'), gp('property_type') or gp('propertytype'),
                           gp('market_segment') or gp('marketsegment'),
                           gp('district'), float(gp('area') or gp('floor_area_sqft') or 0),
                           gp('floor_range') or gp('floorrange'),
                           gp('type_of_sale') or gp('typeofsale'),
                           gp('contract_date') or gp('contractdate'),
                           float(gp('price') or 0),
                           float(gp('unit_price') or gp('unitprice') or 0),
                           batch_id))
                    inserted += 1
                except Exception:
                    continue

        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'error': str(e)}), 500

    conn.close()
    return jsonify({'inserted': inserted, 'total_rows': len(rows), 'batch_id': batch_id})


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
    hdb_txs      = cnt('hdb_transactions')
    priv_txs     = cnt('private_transactions')

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
        ['hdb_transactions',    str(hdb_txs)],
        ['private_transactions', str(priv_txs)],
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
