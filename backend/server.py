from flask import Flask, request, jsonify, send_from_directory, make_response
from flask_cors import CORS
import sqlite3
import hashlib
import hmac as _hmac
import base64
import secrets
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
import predict as _predict_module

# ─── 2FA / Security helpers ──────────────────────────────────────────────────
MASTER_SECRET = os.environ.get('MASTER_SECRET', 'dev-fallback-change-in-prod-99x')

def _get_client_ip():
    """Return real client IP, honouring X-Forwarded-For on Render."""
    xff = request.headers.get('X-Forwarded-For', '')
    if xff:
        return xff.split(',')[0].strip()
    return request.remote_addr or ''

def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()

def _encrypt_secret(plaintext: str) -> str:
    """XOR-encrypt TOTP secret with MASTER_SECRET-derived key, return base64."""
    key = hashlib.sha256((MASTER_SECRET + ':totp').encode()).digest()
    data = plaintext.encode()
    key_ext = (key * (len(data) // len(key) + 1))[:len(data)]
    encrypted = bytes(a ^ b for a, b in zip(data, key_ext))
    return base64.b64encode(encrypted).decode()

def _decrypt_secret(ciphertext: str) -> str:
    """Decrypt XOR-encrypted TOTP secret."""
    key = hashlib.sha256((MASTER_SECRET + ':totp').encode()).digest()
    data = base64.b64decode(ciphertext)
    key_ext = (key * (len(data) // len(key) + 1))[:len(data)]
    return bytes(a ^ b for a, b in zip(data, key_ext)).decode()

def _make_temp_token(user_id: int) -> str:
    """Create a short-lived (5 min) signed token encoding user_id."""
    expiry = int(time.time()) + 300
    payload = f"{user_id}:{expiry}"
    sig = _hmac.new(MASTER_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return base64.urlsafe_b64encode(f"{payload}:{sig}".encode()).decode()

def _verify_temp_token(token: str):
    """Return user_id (int) if valid and not expired, else None."""
    try:
        decoded = base64.urlsafe_b64decode(token).decode()
        parts = decoded.split(':')
        if len(parts) != 3:
            return None
        user_id_str, expiry_str, sig = parts
        if time.time() > int(expiry_str):
            return None
        expected = _hmac.new(MASTER_SECRET.encode(),
                             f"{user_id_str}:{expiry_str}".encode(),
                             hashlib.sha256).hexdigest()
        if not _hmac.compare_digest(sig, expected):
            return None
        return int(user_id_str)
    except Exception:
        return None


def _validate_password_strength(password: str):
    """Return error string if password too weak, else None."""
    if len(password) < 8:
        return "Password must be at least 8 characters."
    if not re.search(r'[A-Z]', password):
        return "Password must contain at least one uppercase letter."
    if not re.search(r'[a-z]', password):
        return "Password must contain at least one lowercase letter."
    if not re.search(r'\d', password):
        return "Password must contain at least one number."
    if not re.search(r'[!@#$%^&*()_+\-=\[\]{};\':"\\|,.<>/?]', password):
        return "Password must contain at least one special character (!@#$%^&* etc.)."
    return None

def _log_audit(conn, user_id, action: str, event_type: str, details: str = ''):
    """Write an entry to audit_log. Silently ignores errors."""
    try:
        cur = _cursor(conn)
        cur.execute(_q(
            "INSERT INTO audit_log (user_id, action, event_type, details, logged_at) "
            "VALUES (?, ?, ?, ?, ?)"
        ), (user_id, action, event_type, details or '',
            datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')))
        conn.commit()
    except Exception as _e:
        print(f"[audit] log failed: {_e}")


# ─── Live retraining state ────────────────────────────────────────────────────

_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))

_retrain_status = {
    'hdb':     {'state': 'idle', 'message': 'Not yet trained this session', 'finished_at': None},
    'private': {'state': 'idle', 'message': 'Not yet trained this session', 'finished_at': None},
}
_retrain_lock = threading.Lock()

# ── Upload job tracking (async background processing) ────────────────────────
_upload_jobs  = {}   # job_id → {state, message, inserted, total_rows}
_upload_lock  = threading.Lock()


def _run_upload_thread(job_id, file_bytes, filename, tx_type):
    """Background thread: stream CSV into staging tables then call process_uploaded_data() RPC."""
    import csv, io, re as _re

    def _upd(state, message, inserted=0, total=0):
        with _upload_lock:
            _upload_jobs[job_id] = {
                'state': state, 'message': message,
                'inserted': inserted, 'total_rows': total,
            }

    _upd('processing', 'Starting upload…')

    # ── _get helper (same fuzzy lookup as in the sync path) ───────────────────
    def _get(row, *keys, default=None):
        for k in keys:
            kn = k.lower().replace(' ', '').replace('(', '').replace(')', '').replace('$', '').replace('#', '').replace('-', '').replace('_', '')
            for col in row:
                cn = col.strip().lower().replace(' ', '').replace('(', '').replace(')', '').replace('$', '').replace('#', '').replace('-', '').replace('_', '')
                if cn == kn:
                    v = row[col]
                    return v if v not in ('', None) else default
        return default

    try:
        import pandas as pd, psycopg2.extras as _pge

        is_excel = filename.endswith('.xlsx') or filename.endswith('.xls')

        if USE_POSTGRES and tx_type in ('hdb', 'geocoded', 'policy', 'sora') and not is_excel:
            # ── ELT: stream CSV → staging → RPC ──────────────────────────────
            if tx_type == 'hdb':
                stage_sql = "INSERT INTO stage_resale (month, town, flat_type, block, street_name, storey_range, floor_area_sqm, flat_model, lease_commence_date, remaining_lease, resale_price) VALUES %s"
                def _make_row(r):
                    return (str(_get(r,'month') or ''), str(_get(r,'town') or ''),
                            str(_get(r,'flat_type') or ''), str(_get(r,'block') or '').strip(),
                            str(_get(r,'street_name') or ''), str(_get(r,'storey_range') or ''),
                            str(_get(r,'floor_area_sqm') or ''), str(_get(r,'flat_model') or ''),
                            str(_get(r,'lease_commence_date') or ''), str(_get(r,'remaining_lease') or ''),
                            str(_get(r,'resale_price') or ''))
            elif tx_type == 'sora':
                stage_sql = "INSERT INTO stage_sora (publication_date, compound_sora_3m, compound_sora_6m, highest_transacted_rate, lowest_transacted_rate) VALUES %s"
                def _make_row(r):
                    return (str(_get(r,'sora publication date','sorapublicationdate','publication_date','publication date','sora value date','soravaluedate','date','rate_date') or ''),
                            str(_get(r,'compound sora - 3 month','compoundsora-3month','compound_sora_3m','sora_3m','published_rate','rate') or ''),
                            str(_get(r,'compound sora - 6 month','compoundsora-6month','compound_sora_6m','sora_6m') or ''),
                            str(_get(r,'highest transacted rate','highest_transacted_rate','highesttransactedrate') or ''),
                            str(_get(r,'lowest transacted rate','lowest_transacted_rate','lowesttransactedrate') or ''))
            elif tx_type == 'policy':
                stage_sql = "INSERT INTO stage_policy (effective_month, effective_date, policy_name, category, direction, severity, source) VALUES %s"
                def _make_row(r):
                    return (str(_get(r,'effective_month','effectivemonth','date','policy_date') or ''),
                            str(_get(r,'effective_date','effectivedate') or ''),
                            str(_get(r,'policy_name','policyname','name','description','measure') or ''),
                            str(_get(r,'category') or ''),
                            str(_get(r,'direction','effect') or ''),
                            str(_get(r,'severity','severity_score','score') or ''),
                            str(_get(r,'source','url','reference') or ''))
            elif tx_type == 'geocoded':
                stage_sql = "INSERT INTO stage_geo (search_text, lat, lon) VALUES %s"
                def _make_row(r):
                    return (str(_get(r,'search_text','searchtext','search text') or ''),
                            str(_get(r,'lat','latitude') or ''),
                            str(_get(r,'lon','lng','longitude') or ''))

            conn = get_db(); cur = _cursor(conn)
            total = 0
            try:
                for chunk_df in pd.read_csv(io.BytesIO(file_bytes), chunksize=5000,
                                            dtype=str, keep_default_na=False):
                    chunk_rows  = chunk_df.where(chunk_df.notna(), '').to_dict('records')
                    chunk_stage = [_make_row(r) for r in chunk_rows]
                    if chunk_stage:
                        _pge.execute_values(cur, stage_sql, chunk_stage, page_size=1000)
                        total += len(chunk_stage)
                    _upd('processing', f'Staged {total:,} rows…', total, total)
                _upd('processing', 'Running database cleaning…', total, total)
                cur.execute("SELECT process_uploaded_data()")
                conn.commit()
                conn.close()
                _upd('done', f'Upload complete — {total:,} rows processed.', total, total)
            except Exception as e:
                conn.rollback(); conn.close()
                _upd('error', str(e))
            return

        # ── Direct insert (SQLite / URA) ─────────────────────────────────────
        # Re-use the synchronous logic by calling _process_rows_direct()
        # which is defined inside upload_transactions but we duplicate here minimally.
        _upd('error', f'Unsupported async path for type={tx_type} on this backend.')

    except Exception as e:
        _upd('error', str(e))


def _run_training_thread(model_type):
    """Background thread: run training script as subprocess, then reset model cache."""
    script = 'train_model.py' if model_type == 'hdb' else 'train_model_private.py'
    script_path = os.path.join(_BACKEND_DIR, script)

    with _retrain_lock:
        _retrain_status[model_type] = {
            'state': 'running', 'message': 'Initialising…', 'finished_at': None,
        }

    try:
        import subprocess
        env = os.environ.copy()
        proc = subprocess.Popen(
            [sys.executable, script_path, '--from-db'],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=env, cwd=_BACKEND_DIR,
        )
        last_line = ''
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                last_line = line
                with _retrain_lock:
                    _retrain_status[model_type]['message'] = line
        proc.wait()
        finished = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
        if proc.returncode == 0:
            _predict_module.reset_model_cache()
            with _retrain_lock:
                _retrain_status[model_type] = {
                    'state': 'success',
                    'message': last_line or 'Training complete',
                    'finished_at': finished,
                }
        else:
            with _retrain_lock:
                _retrain_status[model_type] = {
                    'state': 'error',
                    'message': last_line or 'Training failed — check server logs',
                    'finished_at': finished,
                }
    except Exception as e:
        with _retrain_lock:
            _retrain_status[model_type] = {
                'state': 'error',
                'message': str(e),
                'finished_at': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
            }

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
    CREATE TABLE IF NOT EXISTS resale_flat_prices (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        month               TEXT,
        town                TEXT,
        flat_type           TEXT,
        block               TEXT,
        street_name         TEXT,
        storey_range        TEXT,
        floor_area_sqm      REAL,
        flat_model          TEXT,
        lease_commence_date INTEGER,
        remaining_lease     TEXT,
        resale_price        REAL
    );
    CREATE TABLE IF NOT EXISTS ura_transactions (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        project          TEXT,
        street           TEXT,
        property_type    TEXT,
        market_segment   TEXT,
        postal_district  TEXT,
        floor_level      TEXT,
        floor_area_sqft  REAL,
        floor_area_sqm   REAL,
        type_of_sale     TEXT,
        transacted_price REAL,
        unit_price_psf   REAL,
        unit_price_psm   REAL,
        tenure           TEXT,
        num_units        INTEGER,
        sale_date        TEXT,
        upload_batch     TEXT
    );
    CREATE TABLE IF NOT EXISTS geocoded_addresses (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        search_text     TEXT,
        lat             REAL,
        lon             REAL
    );
    CREATE TABLE IF NOT EXISTS policy_changes (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        effective_month TEXT,
        effective_date  TEXT,
        policy_name     TEXT,
        category        TEXT,
        direction       INTEGER,
        severity        INTEGER,
        source          TEXT
    );
    CREATE TABLE IF NOT EXISTS sora_rates (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        publication_date        TEXT,
        compound_sora_3m        REAL,
        compound_sora_6m        REAL,
        highest_transacted_rate REAL,
        lowest_transacted_rate  REAL
    );
    CREATE TABLE IF NOT EXISTS stage_resale (
        month TEXT, town TEXT, flat_type TEXT, block TEXT, street_name TEXT,
        storey_range TEXT, floor_area_sqm TEXT, flat_model TEXT,
        lease_commence_date TEXT, remaining_lease TEXT, resale_price TEXT
    );
    CREATE TABLE IF NOT EXISTS stage_sora (
        publication_date TEXT, compound_sora_3m TEXT, compound_sora_6m TEXT,
        highest_transacted_rate TEXT, lowest_transacted_rate TEXT
    );
    CREATE TABLE IF NOT EXISTS stage_policy (
        effective_month TEXT, effective_date TEXT, policy_name TEXT,
        category TEXT, direction TEXT, severity TEXT, source TEXT
    );
    CREATE TABLE IF NOT EXISTS stage_geo (
        search_text TEXT, lat TEXT, lon TEXT
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
    CREATE TABLE IF NOT EXISTS resale_flat_prices (
        id                  BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        month               TEXT,
        town                TEXT,
        flat_type           TEXT,
        block               TEXT,
        street_name         TEXT,
        storey_range        TEXT,
        floor_area_sqm      NUMERIC,
        flat_model          TEXT,
        lease_commence_date INTEGER,
        remaining_lease     TEXT,
        resale_price        NUMERIC
    );
    CREATE TABLE IF NOT EXISTS ura_transactions (
        id               BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        project          TEXT,
        street           TEXT,
        property_type    TEXT,
        market_segment   TEXT,
        postal_district  TEXT,
        floor_level      TEXT,
        floor_area_sqft  FLOAT,
        floor_area_sqm   FLOAT,
        type_of_sale     TEXT,
        transacted_price FLOAT,
        unit_price_psf   FLOAT,
        unit_price_psm   FLOAT,
        tenure           TEXT,
        num_units        INT,
        sale_date        TEXT,
        upload_batch     TEXT
    );
    CREATE TABLE IF NOT EXISTS geocoded_addresses (
        id          BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        search_text TEXT,
        lat         DOUBLE PRECISION,
        lon         DOUBLE PRECISION
    );
    CREATE TABLE IF NOT EXISTS policy_changes (
        id              BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        effective_month DATE,
        effective_date  DATE,
        policy_name     TEXT,
        category        TEXT,
        direction       INTEGER,
        severity        INTEGER,
        source          TEXT
    );
    CREATE TABLE IF NOT EXISTS sora_rates (
        id                      BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        publication_date        DATE,
        compound_sora_3m        NUMERIC,
        compound_sora_6m        NUMERIC,
        highest_transacted_rate NUMERIC,
        lowest_transacted_rate  NUMERIC
    );
    CREATE TABLE IF NOT EXISTS stage_resale (
        month TEXT, town TEXT, flat_type TEXT, block TEXT, street_name TEXT,
        storey_range TEXT, floor_area_sqm TEXT, flat_model TEXT,
        lease_commence_date TEXT, remaining_lease TEXT, resale_price TEXT
    );
    CREATE TABLE IF NOT EXISTS stage_sora (
        publication_date TEXT, compound_sora_3m TEXT, compound_sora_6m TEXT,
        highest_transacted_rate TEXT, lowest_transacted_rate TEXT
    );
    CREATE TABLE IF NOT EXISTS stage_policy (
        effective_month TEXT, effective_date TEXT, policy_name TEXT,
        category TEXT, direction TEXT, severity TEXT, source TEXT
    );
    CREATE TABLE IF NOT EXISTS stage_geo (
        search_text TEXT, lat TEXT, lon TEXT
    );
"""

# ── Supabase RPC: ELT cleaning function ──────────────────────────────────────
# Executed once on startup via init_db(). Safe to re-run (CREATE OR REPLACE).
_POSTGRES_RPC = """
CREATE OR REPLACE FUNCTION process_uploaded_data()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- 1. Migrate HDB Resale Data
    INSERT INTO resale_flat_prices
        (month, town, flat_type, block, street_name, storey_range,
         floor_area_sqm, flat_model, lease_commence_date, remaining_lease, resale_price)
    SELECT
        month, town, flat_type, block, street_name, storey_range,
        NULLIF(TRIM(floor_area_sqm),      '')::NUMERIC,
        flat_model,
        NULLIF(TRIM(lease_commence_date), '')::INTEGER,
        remaining_lease,
        NULLIF(TRIM(resale_price),        '')::NUMERIC
    FROM stage_resale
    WHERE month IS NOT NULL AND month <> '' AND month <> 'month';

    -- 2. Migrate SORA Data (handles '-' nulls and DD Mon YYYY / ISO date formats)
    INSERT INTO sora_rates
        (publication_date, compound_sora_3m, compound_sora_6m,
         highest_transacted_rate, lowest_transacted_rate)
    SELECT
        CASE
            WHEN publication_date ~ E'^\\d{2} [A-Za-z]{3} \\d{4}$'
                THEN TO_DATE(publication_date, 'DD Mon YYYY')
            WHEN publication_date ~ E'^\\d{4}-\\d{2}-\\d{2}$'
                THEN publication_date::DATE
            ELSE NULL
        END,
        NULLIF(NULLIF(TRIM(compound_sora_3m),         ''), '-')::NUMERIC,
        NULLIF(NULLIF(TRIM(compound_sora_6m),         ''), '-')::NUMERIC,
        NULLIF(NULLIF(TRIM(highest_transacted_rate),  ''), '-')::NUMERIC,
        NULLIF(NULLIF(TRIM(lowest_transacted_rate),   ''), '-')::NUMERIC
    FROM stage_sora
    WHERE publication_date IS NOT NULL
      AND publication_date <> ''
      AND publication_date NOT ILIKE '%publication%';

    -- 3. Migrate Policy Data
    INSERT INTO policy_changes
        (effective_month, effective_date, policy_name, category, direction, severity, source)
    SELECT
        NULLIF(TRIM(effective_month), '')::DATE,
        NULLIF(TRIM(effective_date),  '')::DATE,
        policy_name, category,
        NULLIF(TRIM(direction), '')::INTEGER,
        NULLIF(TRIM(severity),  '')::INTEGER,
        source
    FROM stage_policy
    WHERE effective_month IS NOT NULL
      AND effective_month <> ''
      AND effective_month <> 'effective_month';

    -- 4. Migrate Geocoded Data
    INSERT INTO geocoded_addresses (search_text, lat, lon)
    SELECT
        search_text,
        NULLIF(TRIM(lat), '')::DOUBLE PRECISION,
        NULLIF(TRIM(lon), '')::DOUBLE PRECISION
    FROM stage_geo
    WHERE search_text IS NOT NULL AND search_text <> '' AND lat <> 'lat';

    -- 5. Clear staging tables for next upload
    TRUNCATE TABLE stage_policy, stage_resale, stage_sora, stage_geo;
END;
$$
"""

def init_db():
    conn = get_db()
    if USE_POSTGRES:
        cur = _cursor(conn)
        for statement in POSTGRES_SCHEMA.strip().split(';'):
            s = statement.strip()
            if s:
                cur.execute(s)
        # Install / refresh the ELT cleaning RPC (idempotent — CREATE OR REPLACE)
        try:
            cur.execute(_POSTGRES_RPC)
        except Exception as e:
            print(f"[init_db] RPC install warning: {e}")
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
            # Drop legacy tables
            for tbl in ('hdb_transactions', 'private_transactions', 'sync_log', 'hdb_resale'):
                try: cur.execute(f"DROP TABLE IF EXISTS {tbl}")
                except Exception: pass
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()")
            for col_def in [
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS account_type TEXT DEFAULT 'homeowner'",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS cea_number TEXT DEFAULT ''",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS whatsapp TEXT DEFAULT ''",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS bio TEXT DEFAULT ''",
            ]:
                try: cur.execute(col_def)
                except Exception: pass
            # Ensure resale_flat_prices has all columns
            for col_def in [
                "ALTER TABLE resale_flat_prices ADD COLUMN IF NOT EXISTS month TEXT",
                "ALTER TABLE resale_flat_prices ADD COLUMN IF NOT EXISTS town TEXT",
                "ALTER TABLE resale_flat_prices ADD COLUMN IF NOT EXISTS flat_type TEXT",
                "ALTER TABLE resale_flat_prices ADD COLUMN IF NOT EXISTS block TEXT",
                "ALTER TABLE resale_flat_prices ADD COLUMN IF NOT EXISTS street_name TEXT",
                "ALTER TABLE resale_flat_prices ADD COLUMN IF NOT EXISTS storey_range TEXT",
                "ALTER TABLE resale_flat_prices ADD COLUMN IF NOT EXISTS floor_area_sqm NUMERIC",
                "ALTER TABLE resale_flat_prices ADD COLUMN IF NOT EXISTS flat_model TEXT",
                "ALTER TABLE resale_flat_prices ADD COLUMN IF NOT EXISTS lease_commence_date INTEGER",
                "ALTER TABLE resale_flat_prices ADD COLUMN IF NOT EXISTS remaining_lease TEXT",
                "ALTER TABLE resale_flat_prices ADD COLUMN IF NOT EXISTS resale_price NUMERIC",
            ]:
                try: cur.execute(col_def)
                except Exception: pass
            # Ensure geocoded_addresses has correct columns
            for col_def in [
                "ALTER TABLE geocoded_addresses ADD COLUMN IF NOT EXISTS search_text TEXT",
                "ALTER TABLE geocoded_addresses ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION",
                "ALTER TABLE geocoded_addresses ADD COLUMN IF NOT EXISTS lon DOUBLE PRECISION",
            ]:
                try: cur.execute(col_def)
                except Exception: pass
            # Ensure policy_changes has correct columns
            for col_def in [
                "ALTER TABLE policy_changes ADD COLUMN IF NOT EXISTS effective_month DATE",
                "ALTER TABLE policy_changes ADD COLUMN IF NOT EXISTS effective_date DATE",
                "ALTER TABLE policy_changes ADD COLUMN IF NOT EXISTS policy_name TEXT",
                "ALTER TABLE policy_changes ADD COLUMN IF NOT EXISTS category TEXT",
                "ALTER TABLE policy_changes ADD COLUMN IF NOT EXISTS direction INTEGER",
                "ALTER TABLE policy_changes ADD COLUMN IF NOT EXISTS severity INTEGER",
                "ALTER TABLE policy_changes ADD COLUMN IF NOT EXISTS source TEXT",
            ]:
                try: cur.execute(col_def)
                except Exception: pass
            # Ensure sora_rates has new column schema
            for col_def in [
                "ALTER TABLE sora_rates ADD COLUMN IF NOT EXISTS publication_date DATE",
                "ALTER TABLE sora_rates ADD COLUMN IF NOT EXISTS compound_sora_3m NUMERIC",
                "ALTER TABLE sora_rates ADD COLUMN IF NOT EXISTS compound_sora_6m NUMERIC",
                "ALTER TABLE sora_rates ADD COLUMN IF NOT EXISTS highest_transacted_rate NUMERIC",
                "ALTER TABLE sora_rates ADD COLUMN IF NOT EXISTS lowest_transacted_rate NUMERIC",
            ]:
                try: cur.execute(col_def)
                except Exception: pass
            # 2FA columns on users
            for col_def in [
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS totp_secret TEXT",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS totp_enabled BOOLEAN DEFAULT FALSE",
            ]:
                try: cur.execute(col_def)
                except Exception: pass
            # Trusted devices table for 2FA "remember me"
            try:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trusted_devices (
                        id          SERIAL PRIMARY KEY,
                        user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                        token_hash  TEXT NOT NULL UNIQUE,
                        ip_address  TEXT,
                        expires_at  TIMESTAMP NOT NULL,
                        created_at  TIMESTAMP DEFAULT NOW()
                    )
                """)
            except Exception: pass
            # Ensure ura_transactions has all required columns
            for col_def in [
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS project TEXT",
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS street TEXT",
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS property_type TEXT",
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS market_segment TEXT",
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS postal_district TEXT",
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS floor_level TEXT",
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS floor_area_sqft FLOAT",
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS floor_area_sqm FLOAT",
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS type_of_sale TEXT",
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS transacted_price FLOAT",
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS unit_price_psf FLOAT",
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS unit_price_psm FLOAT",
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS tenure TEXT",
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS num_units INT",
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS sale_date TEXT",
                "ALTER TABLE ura_transactions ADD COLUMN IF NOT EXISTS upload_batch TEXT",
            ]:
                try: cur.execute(col_def)
                except Exception: pass
            conn.commit()
        else:
            for stmt in [
                "ALTER TABLE users ADD COLUMN created_at TEXT DEFAULT (datetime('now'))",
                "ALTER TABLE users ADD COLUMN account_type TEXT DEFAULT 'homeowner'",
                "ALTER TABLE users ADD COLUMN cea_number TEXT DEFAULT ''",
                "ALTER TABLE users ADD COLUMN whatsapp TEXT DEFAULT ''",
                "ALTER TABLE users ADD COLUMN bio TEXT DEFAULT ''",
                "ALTER TABLE users ADD COLUMN totp_secret TEXT",
                "ALTER TABLE users ADD COLUMN totp_enabled INTEGER DEFAULT 0",
                "ALTER TABLE geocoded_addresses ADD COLUMN search_text TEXT",
                "ALTER TABLE geocoded_addresses ADD COLUMN lat REAL",
                "ALTER TABLE geocoded_addresses ADD COLUMN lon REAL",
                "ALTER TABLE policy_changes ADD COLUMN effective_month TEXT",
                "ALTER TABLE policy_changes ADD COLUMN effective_date TEXT",
                "ALTER TABLE policy_changes ADD COLUMN policy_name TEXT",
                "ALTER TABLE policy_changes ADD COLUMN category TEXT",
                "ALTER TABLE policy_changes ADD COLUMN source TEXT",
                "ALTER TABLE resale_flat_prices ADD COLUMN block TEXT",
                "ALTER TABLE resale_flat_prices ADD COLUMN street_name TEXT",
            ]:
                try:
                    conn.execute(stmt)
                except Exception:
                    pass  # column already exists
            for tbl in ('hdb_transactions', 'private_transactions', 'sync_log'):
                try: conn.execute(f"DROP TABLE IF EXISTS {tbl}")
                except Exception: pass
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS trusted_devices (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        token_hash TEXT NOT NULL UNIQUE,
                        ip_address TEXT,
                        expires_at TEXT NOT NULL,
                        created_at TEXT DEFAULT (datetime('now'))
                    )
                """)
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
    _OVERPASS_MIRRORS = [
        'https://overpass-api.de/api/interpreter',
        'https://overpass.kumi.systems/api/interpreter',
        'https://maps.mail.ru/osm/tools/overpass/api/interpreter',
    ]
    data = None
    for mirror in _OVERPASS_MIRRORS:
        try:
            req = urllib.request.Request(mirror, data=query.encode(), method='POST')
            req.add_header('User-Agent', 'PropAI-SG/1.0')
            with urllib.request.urlopen(req, timeout=25) as r:
                data = json.loads(r.read())
            break
        except Exception as _e:
            print(f"[amenities] Overpass mirror {mirror} failed: {_e}")
            continue
    try:
        if data is None:
            raise RuntimeError("All Overpass mirrors failed")

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


@app.route('/api/guides-news', methods=['GET'])
def get_guides_news():
    """Fetch property policy and market news for the Guides tab."""
    topic = request.args.get('topic', 'policy').strip()

    _QUERIES = {
        'policy':      'Singapore HDB resale policy CPF stamp duty 2025 2026 site:hdb.gov.sg OR site:iras.gov.sg OR site:mas.gov.sg OR site:propertyguru.com.sg OR site:edgeprop.sg OR site:straitstimes.com',
        'hdb_resale':  'Singapore HDB resale flat price grant eligibility 2025 2026 site:propertyguru.com.sg OR site:edgeprop.sg OR site:straitstimes.com OR site:99.co',
        'condo':       'Singapore private condominium price ABSD cooling measure 2025 2026 site:propertyguru.com.sg OR site:edgeprop.sg OR site:businesstimes.com.sg OR site:99.co',
        'financing':   'Singapore home loan TDSR MSR interest rate HDB bank 2025 2026 site:propertyguru.com.sg OR site:moneysmart.sg OR site:straitstimes.com',
    }
    query = _QUERIES.get(topic, _QUERIES['policy'])
    articles = fetch_news(query, limit=6, max_age_years=2)
    return jsonify({'articles': articles, 'topic': topic})


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
        cur.execute("SELECT COUNT(*) AS n FROM resale_flat_prices"); hdb_tx_count = dict(cur.fetchone())['n']
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
    """Return real HDB resale trend data and comparable sales for a town/neighbourhood."""
    from datetime import date as _date
    town = (request.args.get('town') or '').strip().upper()

    conn = get_db()
    cur  = _cursor(conn)

    # ── 6-month trend: average resale price per month ─────────────────────────
    town_filter = _q("AND UPPER(town) = ?") if town else ""
    town_params = (town,) if town else ()

    try:
        cur.execute(_q(
            f"SELECT month, AVG(resale_price) AS avg_price "
            f"FROM resale_flat_prices "
            f"WHERE month IS NOT NULL AND resale_price > 0 {town_filter} "
            f"GROUP BY month ORDER BY month DESC LIMIT 6"
        ), town_params)
        raw = _rows(cur)
    except Exception:
        raw = []

    trend_data = []
    for r in reversed(raw):
        mo_str = str(r.get('month') or '')
        try:
            y, m = int(mo_str[:4]), int(mo_str[5:7])
            label = _date(y, m, 1).strftime("%b '%y")
        except Exception:
            label = mo_str
        trend_data.append({"month": label, "price": int(float(r.get('avg_price') or 0))})

    # ── Comparable recent sales (last 3 months) ───────────────────────────────
    try:
        cur.execute(_q(
            f"SELECT block, street_name, flat_type, storey_range, floor_area_sqm, resale_price, month "
            f"FROM resale_flat_prices "
            f"WHERE resale_price > 0 AND month IS NOT NULL {town_filter} "
            f"ORDER BY month DESC, resale_price DESC LIMIT 10"
        ), town_params)
        comp_rows = _rows(cur)
    except Exception:
        comp_rows = []

    similar = []
    for r in comp_rows:
        mo_str = str(r.get('month') or '')
        try:
            y, m = int(mo_str[:4]), int(mo_str[5:7])
            date_label = _date(y, m, 1).strftime("%b '%y")
        except Exception:
            date_label = mo_str
        blk  = str(r.get('block') or '').strip()
        road = str(r.get('street_name') or '').strip().title()
        addr = f"Blk {blk} {road}" if blk else road
        sqm  = float(r.get('floor_area_sqm') or 0)
        similar.append({
            "address": addr,
            "type": str(r.get('flat_type') or '').title(),
            "storey": str(r.get('storey_range') or ''),
            "floor_area": int(sqm * 10.764),
            "price": int(float(r.get('resale_price') or 0)),
            "date": date_label,
        })

    conn.close()

    prices = [s['price'] for s in similar] or [450000]
    import statistics as _stats
    avg = int(_stats.mean(prices))
    return jsonify({
        "trend_data":           trend_data,
        "similar_transactions": similar,
        "summary": {
            "avg_price":        avg,
            "min_price":        int(min(prices)),
            "max_price":        int(max(prices)),
            "total_transactions": len(similar),
        },
    })


@app.route('/api/register', methods=['POST'])
def register():
    data         = request.json
    full_name    = data.get('full_name', '').strip()
    email        = data.get('email', '').strip().lower()
    password     = data.get('password', '').strip()
    account_type = data.get('account_type', 'homeowner').strip()
    if account_type not in ('homeowner', 'agent'):
        account_type = 'homeowner'

    if not full_name or not email or not password:
        return jsonify({"error": "All fields are required"}), 400

    if not re.match(r'^[^\s@]+@[^\s@]+\.[^\s@]{2,}$', email):
        return jsonify({"error": "Please enter a valid email address."}), 400

    pw_err = _validate_password_strength(password)
    if pw_err:
        return jsonify({"error": pw_err}), 400

    conn = get_db()
    cur  = _cursor(conn)

    cur.execute(_q("SELECT id FROM users WHERE email = ?"), (email,))
    if _row(cur):
        conn.close()
        return jsonify({"error": "Email already registered"}), 400

    password_hash = hashlib.sha256(password.encode()).hexdigest()

    if USE_POSTGRES:
        cur.execute("INSERT INTO users (full_name, email, password_hash, account_type) VALUES (%s, %s, %s, %s) RETURNING id",
                    (full_name, email, password_hash, account_type))
        user_id = cur.fetchone()["id"]
    else:
        cur.execute("INSERT INTO users (full_name, email, password_hash, account_type) VALUES (?, ?, ?, ?)",
                    (full_name, email, password_hash, account_type))
        user_id = cur.lastrowid

    conn.commit()

    _log_audit(conn, user_id, f"New registration: {email}", 'register', f"name={full_name}, type={account_type}")

    cur.execute(_q("SELECT id, full_name, email, phone, role, account_type, cea_number, whatsapp, bio FROM users WHERE id = ?"), (user_id,))
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

    if not user:
        conn.close()
        return jsonify({"error": "User not found"}), 404

    if user['password_hash'] != hashlib.sha256(password.encode()).hexdigest():
        conn.close()
        return jsonify({"error": "Wrong password"}), 401

    user_obj = {
        "id": user["id"], "full_name": user["full_name"],
        "email": user["email"], "phone": user["phone"],
        "role": user["role"], "is_admin": user["role"] == "admin",
        "account_type": user.get("account_type", "homeowner"),
        "cea_number": user.get("cea_number", ""),
        "whatsapp": user.get("whatsapp", ""),
        "bio": user.get("bio", ""),
        "totp_enabled": bool(user.get("totp_enabled")),
    }

    # ── 2FA check ────────────────────────────────────────────────────────────
    if user.get("totp_enabled"):
        # Check trusted device cookie first
        td_cookie = request.cookies.get('td_token', '')
        if td_cookie:
            td_hash = _hash_token(td_cookie)
            now_str = datetime.datetime.utcnow().isoformat()
            cur.execute(_q(
                "SELECT id FROM trusted_devices WHERE token_hash = ? AND user_id = ? AND expires_at > ?"
            ), (td_hash, user["id"], now_str))
            if _row(cur):
                conn.close()
                return jsonify({"user": user_obj})

        conn.close()
        temp_token = _make_temp_token(user["id"])
        return jsonify({"requires_2fa": True, "temp_token": temp_token})

    conn.close()
    return jsonify({"user": user_obj})


# ── 2FA Endpoints ─────────────────────────────────────────────────────────────

@app.route('/api/2fa/setup', methods=['POST'])
def twofa_setup():
    """Generate a new TOTP secret for the user and return QR URI (does not enable yet)."""
    try:
        import pyotp
    except ImportError:
        return jsonify({"error": "2FA library not installed on server"}), 500

    data    = request.json or {}
    user_id = data.get('user_id')
    if not user_id:
        return jsonify({"error": "user_id required"}), 400

    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q("SELECT id, email, totp_enabled FROM users WHERE id = ?"), (user_id,))
    user = _row(cur)
    if not user:
        conn.close()
        return jsonify({"error": "User not found"}), 404

    # Generate new secret (not yet enabled)
    raw_secret = pyotp.random_base32()
    encrypted  = _encrypt_secret(raw_secret)
    cur.execute(_q("UPDATE users SET totp_secret = ?, totp_enabled = FALSE WHERE id = ?"),
                (encrypted, user_id))
    conn.commit()
    conn.close()

    totp = pyotp.TOTP(raw_secret)
    uri  = totp.provisioning_uri(name=user['email'], issuer_name='PropAI.sg')
    return jsonify({"secret": raw_secret, "uri": uri})


@app.route('/api/2fa/enable', methods=['POST'])
def twofa_enable():
    """Verify a TOTP code and enable 2FA on the account."""
    try:
        import pyotp
    except ImportError:
        return jsonify({"error": "2FA library not installed on server"}), 500

    data    = request.json or {}
    user_id = data.get('user_id')
    code    = str(data.get('code', '')).strip()
    if not user_id or not code:
        return jsonify({"error": "user_id and code required"}), 400

    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q("SELECT totp_secret, totp_enabled FROM users WHERE id = ?"), (user_id,))
    row = _row(cur)
    if not row or not row.get('totp_secret'):
        conn.close()
        return jsonify({"error": "Call /api/2fa/setup first"}), 400

    raw_secret = _decrypt_secret(row['totp_secret'])
    totp = pyotp.TOTP(raw_secret)
    if not totp.verify(code, valid_window=1):
        conn.close()
        return jsonify({"error": "Invalid code — try again"}), 400

    cur.execute(_q("UPDATE users SET totp_enabled = TRUE WHERE id = ?"), (user_id,))
    conn.commit()
    conn.close()
    return jsonify({"success": True, "message": "2FA enabled successfully"})


@app.route('/api/2fa/verify', methods=['POST'])
def twofa_verify():
    """Verify a TOTP code after login (temp_token flow). Returns user + optional trusted-device cookie."""
    try:
        import pyotp
    except ImportError:
        return jsonify({"error": "2FA library not installed on server"}), 500

    data           = request.json or {}
    temp_token     = data.get('temp_token', '')
    code           = str(data.get('code', '')).strip()
    remember_device = bool(data.get('remember_device', False))

    user_id = _verify_temp_token(temp_token)
    if not user_id:
        return jsonify({"error": "Session expired — please log in again"}), 401

    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q("SELECT * FROM users WHERE id = ?"), (user_id,))
    user = _row(cur)
    if not user or not user.get('totp_secret'):
        conn.close()
        return jsonify({"error": "2FA not configured"}), 400

    raw_secret = _decrypt_secret(user['totp_secret'])
    totp = pyotp.TOTP(raw_secret)
    if not totp.verify(code, valid_window=1):
        conn.close()
        return jsonify({"error": "Invalid code — try again"}), 400

    user_obj = {
        "id": user["id"], "full_name": user["full_name"],
        "email": user["email"], "phone": user["phone"],
        "role": user["role"], "is_admin": user["role"] == "admin",
        "account_type": user.get("account_type", "homeowner"),
        "cea_number": user.get("cea_number", ""),
        "whatsapp": user.get("whatsapp", ""),
        "bio": user.get("bio", ""),
        "totp_enabled": True,
    }

    resp = make_response(jsonify({"user": user_obj}))

    if remember_device:
        raw_token  = secrets.token_urlsafe(32)
        token_hash = _hash_token(raw_token)
        expires_dt = datetime.datetime.utcnow() + datetime.timedelta(days=30)
        expires_str = expires_dt.isoformat()
        client_ip  = _get_client_ip()
        try:
            cur.execute(_q("""
                INSERT INTO trusted_devices (user_id, token_hash, ip_address, expires_at)
                VALUES (?, ?, ?, ?)
            """), (user_id, token_hash, client_ip, expires_str))
            conn.commit()
            # Set HttpOnly cookie (30 days)
            resp.set_cookie(
                'td_token', raw_token,
                max_age=30 * 24 * 3600,
                httponly=True, secure=True, samesite='Strict',
                path='/'
            )
        except Exception as e:
            print(f"[2fa/verify] trusted_device insert error: {e}")

    conn.close()
    return resp


@app.route('/api/2fa/disable', methods=['POST'])
def twofa_disable():
    """Verify TOTP code then disable 2FA and clear all trusted devices."""
    try:
        import pyotp
    except ImportError:
        return jsonify({"error": "2FA library not installed on server"}), 500

    data    = request.json or {}
    user_id = data.get('user_id')
    code    = str(data.get('code', '')).strip()
    if not user_id or not code:
        return jsonify({"error": "user_id and code required"}), 400

    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q("SELECT totp_secret, totp_enabled FROM users WHERE id = ?"), (user_id,))
    row = _row(cur)
    if not row or not row.get('totp_secret') or not row.get('totp_enabled'):
        conn.close()
        return jsonify({"error": "2FA is not enabled"}), 400

    raw_secret = _decrypt_secret(row['totp_secret'])
    totp = pyotp.TOTP(raw_secret)
    if not totp.verify(code, valid_window=1):
        conn.close()
        return jsonify({"error": "Invalid code — try again"}), 400

    cur.execute(_q("UPDATE users SET totp_secret = NULL, totp_enabled = FALSE WHERE id = ?"), (user_id,))
    try:
        cur.execute(_q("DELETE FROM trusted_devices WHERE user_id = ?"), (user_id,))
    except Exception:
        pass
    conn.commit()
    conn.close()

    resp = make_response(jsonify({"success": True, "message": "2FA disabled"}))
    resp.delete_cookie('td_token', path='/')
    return resp


@app.route('/api/2fa/status', methods=['GET'])
def twofa_status():
    """Return whether 2FA is enabled for a user."""
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({"error": "user_id required"}), 400
    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q("SELECT totp_enabled FROM users WHERE id = ?"), (user_id,))
    row = _row(cur)
    conn.close()
    if not row:
        return jsonify({"error": "User not found"}), 404
    return jsonify({"totp_enabled": bool(row.get("totp_enabled"))})


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
    data         = request.json
    full_name    = data.get('full_name', '').strip()
    email        = data.get('email', '').strip().lower()
    phone        = data.get('phone', '').strip()
    account_type = data.get('account_type', '').strip()
    cea_number   = data.get('cea_number', '').strip()
    whatsapp     = data.get('whatsapp', '').strip()
    bio          = data.get('bio', '').strip()
    new_password = data.get('new_password', '').strip()
    cur_password = data.get('current_password', '').strip()
    totp_code    = str(data.get('totp_code', '')).strip()

    if not full_name or not email:
        return jsonify({"error": "Full name and email are required"}), 400

    if not re.match(r'^[^\s@]+@[^\s@]+\.[^\s@]{2,}$', email):
        return jsonify({"error": "Please enter a valid email address."}), 400

    conn = get_db()
    cur  = _cursor(conn)

    cur.execute(_q("SELECT id, password_hash FROM users WHERE email = ? AND id != ?"), (email, user_id))
    if _row(cur):
        conn.close()
        return jsonify({"error": "Email already in use"}), 400

    # Password change
    if new_password:
        pw_err = _validate_password_strength(new_password)
        if pw_err:
            conn.close()
            return jsonify({"error": pw_err}), 400

        cur.execute(_q("SELECT password_hash, totp_secret, totp_enabled FROM users WHERE id = ?"), (user_id,))
        row = _row(cur)
        if not row or row['password_hash'] != hashlib.sha256(cur_password.encode()).hexdigest():
            conn.close()
            return jsonify({"error": "Current password is incorrect"}), 400

        # 2FA check if enabled
        if row.get('totp_enabled') and row.get('totp_secret'):
            try:
                import pyotp
                raw_secret = _decrypt_secret(row['totp_secret'])
                totp = pyotp.TOTP(raw_secret)
                if not totp_code or not totp.verify(totp_code, valid_window=1):
                    conn.close()
                    return jsonify({"error": "Invalid 2FA code — enter the code from your authenticator app.", "requires_totp": True}), 400
            except ImportError:
                pass  # pyotp not installed, skip 2FA check

        new_hash = hashlib.sha256(new_password.encode()).hexdigest()
        cur.execute(_q("UPDATE users SET password_hash = ? WHERE id = ?"), (new_hash, user_id))

    set_clause = "full_name = ?, email = ?, phone = ?"
    params     = [full_name, email, phone]
    if account_type in ('homeowner', 'agent'):
        set_clause += ", account_type = ?"
        params.append(account_type)
    if cea_number is not None:
        set_clause += ", cea_number = ?"
        params.append(cea_number)
    if whatsapp is not None:
        set_clause += ", whatsapp = ?"
        params.append(whatsapp)
    if bio is not None:
        set_clause += ", bio = ?"
        params.append(bio)
    params.append(user_id)
    cur.execute(_q(f"UPDATE users SET {set_clause} WHERE id = ?"), params)
    conn.commit()
    cur.execute(_q("SELECT id, full_name, email, phone, role, account_type, cea_number, whatsapp, bio FROM users WHERE id = ?"), (user_id,))
    user = _row(cur)
    conn.close()

    if not user:
        return jsonify({"error": "User not found"}), 404
    return jsonify({"user": user})


@app.route('/api/profile/<int:user_id>', methods=['DELETE'])
def delete_account(user_id):
    """Self-service account deletion."""
    data     = request.json or {}
    password = data.get('password', '').strip()
    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q("SELECT password_hash FROM users WHERE id = ?"), (user_id,))
    row = _row(cur)
    if not row:
        conn.close()
        return jsonify({"error": "User not found"}), 404
    if row['password_hash'] != hashlib.sha256(password.encode()).hexdigest():
        conn.close()
        return jsonify({"error": "Incorrect password"}), 401
    cur.execute(_q("DELETE FROM users WHERE id = ?"), (user_id,))
    conn.commit()
    conn.close()
    return jsonify({"success": True})


@app.route('/api/agent/mop-leads', methods=['GET'])
def mop_leads():
    """
    Return HDB units likely approaching or past MOP (5 years from lease_commence_date).
    Agent-only endpoint: lists towns with high recent transaction velocity that have
    units whose MOP window falls within the next 0–24 months.
    Query param: ?town=HOUGANG  (optional; omit for all towns)
    """
    token = request.headers.get('Authorization', '').replace('Bearer ', '').strip()
    user_id = _verify_temp_token(token) if token else None
    # Also support simple token lookup (same as other protected endpoints)
    conn = get_db()
    cur  = _cursor(conn)
    if not user_id and token:
        cur.execute(_q("SELECT id, account_type FROM users WHERE token = ?"), (token,))
        u = _row(cur)
        if u:
            user_id = u['id']
    if not user_id:
        conn.close()
        return jsonify({"error": "Unauthorised"}), 401

    cur.execute(_q("SELECT account_type FROM users WHERE id = ?"), (user_id,))
    u = _row(cur)
    if not u or u['account_type'] != 'agent':
        conn.close()
        return jsonify({"error": "Agent account required"}), 403

    town_filter = request.args.get('town', '').strip().upper()

    try:
        import datetime as _dt
        today = _dt.date.today()
        mop_start = today.year - 5          # units that passed MOP in last 12 months
        mop_end   = today.year - 4          # units that will pass MOP in next 12 months

        # We look for leases that commenced between (today - 6 years) and (today - 4 years)
        # so MOP date falls within a ±1 year window around now
        lease_min = today.year - 6
        lease_max = today.year - 4

        where_town = ""
        params = [lease_min, lease_max]
        if town_filter:
            where_town = f" AND UPPER(town) = {_q('?')}"
            params.append(town_filter)

        cur.execute(_q(
            f"""
            SELECT
                town,
                block,
                street_name,
                flat_type,
                storey_range,
                lease_commence_date,
                (lease_commence_date + 5) AS mop_year,
                COUNT(*) AS unit_count
            FROM resale_flat_prices
            WHERE lease_commence_date >= ? AND lease_commence_date <= ?
            {where_town}
            GROUP BY town, block, street_name, flat_type, storey_range, lease_commence_date
            ORDER BY mop_year ASC, town ASC
            LIMIT 100
            """
        ), params)
        rows = _rows(cur)
        conn.close()

        leads = []
        for r in rows:
            mop_yr = int(r.get('mop_year') or 0)
            lc     = int(r.get('lease_commence_date') or 0)
            status = 'eligible' if mop_yr <= today.year else 'upcoming'
            months_to_mop = max(0, (mop_yr - today.year) * 12 + (7 - today.month))  # rough
            leads.append({
                "town":          r.get('town', ''),
                "block":         r.get('block', ''),
                "street_name":   r.get('street_name', ''),
                "flat_type":     r.get('flat_type', ''),
                "storey_range":  r.get('storey_range', ''),
                "lease_commence": lc,
                "mop_year":      mop_yr,
                "status":        status,
                "months_to_mop": months_to_mop if status == 'upcoming' else 0,
                "unit_count":    int(r.get('unit_count') or 1),
            })
        return jsonify({"leads": leads, "total": len(leads)})
    except Exception as e:
        try: conn.close()
        except Exception: pass
        return jsonify({"error": str(e)}), 500


@app.route('/api/agents', methods=['GET'])
def get_agents():
    """Return all users registered as property agents."""
    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_q(
        "SELECT id, full_name, email, phone, cea_number, whatsapp, bio FROM users "
        "WHERE account_type = ? ORDER BY full_name"
    ), ('agent',))
    agents = _rows(cur)
    conn.close()
    return jsonify({"agents": agents})


@app.route('/api/chat', methods=['POST'])
def chatbot():
    """Property AI chatbot powered by Groq (Llama 3)."""
    import os as _os
    api_key = _os.environ.get('GROQ_API_KEY', '')
    if not api_key:
        return jsonify({"reply": "Chatbot is not configured (missing API key)."}), 200

    data     = request.json or {}
    messages = data.get('messages', [])
    if not messages:
        return jsonify({"reply": "No message provided."}), 400

    try:
        import urllib.request as _ur
        import json as _json

        system_prompt = (
            "You are Kai, a friendly and knowledgeable property assistant on PropAI.sg — Singapore's AI property platform. "
            "You speak in a warm, approachable tone — like a helpful friend who knows Singapore property inside out. "
            "Help users with HDB resale, private condominiums, the buying/selling process, CPF housing grants, "
            "BSD/ABSD stamp duties, loan eligibility, TDSR/MSR rules, lease decay, MOP, and Singapore market trends. "
            "Keep answers concise and practical. Use simple language, not jargon. "
            "Remind users that prices on PropAI.sg are AI estimates and not formal financial advice. "
            "If someone asks for a specific property valuation, suggest they use the Predict tab."
        )

        payload = _json.dumps({
            "model": "llama-3.1-8b-instant",
            "max_tokens": 512,
            "messages": [{"role": "system", "content": system_prompt}] + messages[-10:]
        }).encode()

        req = _ur.Request(
            "https://api.groq.com/openai/v1/chat/completions",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }
        )
        resp   = _ur.urlopen(req, timeout=20)
        result = _json.loads(resp.read())
        reply  = result["choices"][0]["message"]["content"]
        return jsonify({"reply": reply})
    except Exception as e:
        print(f"[chat] ERROR: {type(e).__name__}: {e}")
        import urllib.error as _ue
        if isinstance(e, _ue.HTTPError):
            print(f"[chat] HTTP {e.code}: {e.read().decode('utf-8','replace')[:300]}")
            if e.code == 401:
                return jsonify({"reply": "Kai is currently unavailable — API authentication failed. Please try again later."}), 200
        return jsonify({"reply": "Sorry, I'm having trouble right now. Please try again shortly."}), 200


@app.route('/api/property-lookup', methods=['GET'])
def property_lookup():
    """Geocode a postal code and return town, property type, and lease type."""
    import re as _re
    postal = request.args.get('postal', '').strip()
    if not postal:
        return jsonify({'error': 'postal required'}), 400

    # geocoded_addresses no longer stores postal_code — skip to OneMap lookup

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

        # ── DB-first property type classification ────────────────────────
        # HDB → exists in resale_flat_prices; Condo → exists in ura_transactions
        # Landed → in neither (and no named building)
        db_is_hdb   = False
        db_is_condo = False
        try:
            _dbc = get_db(); _dbc_cur = _cursor(_dbc)
            if blk_no:
                _dbc_cur.execute(_q(
                    "SELECT 1 FROM resale_flat_prices WHERE UPPER(block) = ? LIMIT 1"
                ), (blk_no.upper(),))
                db_is_hdb = _dbc_cur.fetchone() is not None
            if not db_is_hdb and building not in ('NIL', ''):
                # Exclude landed types so detached/terrace houses don't trigger db_is_condo
                _LANDED = ('Detached House', 'Semi-Detached House', 'Terrace House',
                           'Strata Detached', 'Strata Semi-Detached', 'Strata Terrace')
                _ph = ','.join(['?' for _ in _LANDED])
                _dbc_cur.execute(_q(
                    f"SELECT 1 FROM ura_transactions WHERE UPPER(project) = ? "
                    f"AND (property_type IS NULL OR property_type NOT IN ({_ph})) LIMIT 1"
                ), (building.upper(), *_LANDED))
                db_is_condo = _dbc_cur.fetchone() is not None
            _dbc.close()
        except Exception:
            pass

        # Combine DB signals with OneMap heuristic
        onemap_looks_hdb   = (building in ('NIL', '')) and bool(_re.match(r'^\d+[A-Z]?$', blk_no))
        onemap_looks_condo = building not in ('NIL', '')

        if db_is_hdb:
            is_hdb, is_condo, is_landed = True, False, False
        elif db_is_condo:
            is_hdb, is_condo, is_landed = False, True, False
        elif onemap_looks_hdb:
            # Block number present but not yet in our DB (e.g. new flat) — treat as HDB
            is_hdb, is_condo, is_landed = True, False, False
        elif onemap_looks_condo:
            is_hdb, is_condo, is_landed = False, True, False
        else:
            # No named building + not in HDB DB → landed / unknown
            is_hdb, is_condo, is_landed = False, False, True

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
            # Check tenure from URA transactions
            lease_type = 'Freehold'
            if building not in ('NIL', ''):
                try:
                    _tc = get_db(); _tc_cur = _cursor(_tc)
                    _tc_cur.execute(_q(
                        "SELECT tenure FROM ura_transactions WHERE UPPER(project) = ? AND tenure IS NOT NULL LIMIT 1"
                    ), (building.upper(),))
                    _tr = _tc_cur.fetchone()
                    _tc.close()
                    if _tr:
                        _tenure = str(_tr['tenure'] if hasattr(_tr, '__getitem__') else _tr[0]).upper()
                        if '99' in _tenure:
                            lease_type = '99-year Leasehold'
                        elif '999' in _tenure:
                            lease_type = '999-year Leasehold'
                        elif 'FREEHOLD' in _tenure or 'FH' in _tenure:
                            lease_type = 'Freehold'
                except Exception:
                    pass
        road_name = str(result.get('ROAD_NAME') or '').strip()

        # ── Query DB for accurate floor data ─────────────────────────────
        storey_ranges = []
        max_floor     = None
        project_name  = building if not is_hdb else ''

        try:
            import re as _re2
            dbc = get_db(); dbc_cur = _cursor(dbc)
            if is_hdb and blk_no:
                def _q_storeys(extra, params):
                    dbc_cur.execute(_q(
                        "SELECT DISTINCT storey_range FROM resale_flat_prices "
                        f"WHERE UPPER(block) = ? {extra} AND storey_range LIKE '% TO %' ORDER BY storey_range"
                    ), params)
                    return [str(r['storey_range'] if hasattr(r, '__getitem__') else r[0])
                            for r in dbc_cur.fetchall()]

                block_upper = blk_no.upper()
                road_upper  = road_name.upper()

                # Step 1: block + road (use first word of road to avoid too-short matches)
                road_keyword = road_upper.split()[0] if road_upper else ''
                srs = _q_storeys("AND UPPER(street_name) LIKE ?",
                                 (block_upper, f'%{road_keyword}%')) if road_keyword else []

                # Step 2: block + town (skip block-only — it matches blocks from other towns)
                if not srs and town:
                    dbc_cur.execute(_q(
                        "SELECT DISTINCT storey_range FROM resale_flat_prices "
                        "WHERE UPPER(block) = ? AND UPPER(town) = ? AND storey_range LIKE '% TO %' ORDER BY storey_range"
                    ), (block_upper, town.upper()))
                    srs = [str(r['storey_range'] if hasattr(r, '__getitem__') else r[0])
                           for r in dbc_cur.fetchall()]

                # Step 3: town-wide fallback — typical ranges for any flat in this town
                if not srs and town:
                    dbc_cur.execute(_q(
                        "SELECT DISTINCT storey_range FROM resale_flat_prices "
                        "WHERE UPPER(town) = ? AND storey_range LIKE '% TO %' ORDER BY storey_range"
                    ), (town.upper(),))
                    srs = [str(r['storey_range'] if hasattr(r, '__getitem__') else r[0])
                           for r in dbc_cur.fetchall()]

                storey_ranges = srs
                if srs:
                    def _top(s):
                        try: return int(s.split(' TO ')[-1].strip())
                        except: return 0
                    max_floor = max(_top(s) for s in srs)

            elif not is_hdb and not is_landed and building not in ('NIL', ''):
                # Condo: get floor_level from ura_transactions by project name
                dbc_cur.execute(_q(
                    "SELECT DISTINCT floor_level FROM ura_transactions "
                    "WHERE UPPER(project) = ? AND floor_level IS NOT NULL AND floor_level != ''"
                ), (building.upper(),))
                fl_rows = [str(r['floor_level'] if hasattr(r, '__getitem__') else r[0])
                           for r in dbc_cur.fetchall()]
                if fl_rows:
                    def _fl_top(s):
                        nums = _re2.findall(r'\d+', s)
                        return int(nums[-1]) if nums else 0
                    max_floor = max(_fl_top(s) for s in fl_rows)
                    storey_ranges = sorted(set(fl_rows),
                                          key=lambda s: int(_re2.findall(r'\d+', s)[0]) if _re2.findall(r'\d+', s) else 0)
            dbc.close()
        except Exception:
            pass

        # ── Remaining lease years from HDB DB ────────────────────────
        remaining_lease_years = None
        if is_hdb and blk_no:
            try:
                _rlc = get_db(); _rlc_cur = _cursor(_rlc)
                _rlc_cur.execute(_q(
                    "SELECT AVG(lease_commence_date) AS avg_lc "
                    "FROM resale_flat_prices WHERE UPPER(block) = ? AND lease_commence_date IS NOT NULL"
                ), (blk_no.upper(),))
                _rlc_row = _rlc_cur.fetchone()
                _rlc.close()
                if _rlc_row:
                    _avg_lc = (_rlc_row['avg_lc'] if hasattr(_rlc_row, '__getitem__') else _rlc_row[0])
                    if _avg_lc:
                        remaining_lease_years = max(0, round(99 - (2026 - float(_avg_lc))))
            except Exception:
                pass

        resp = {
            'town': town, 'property_type': prop_type,
            'lease_type': lease_type, 'is_hdb': is_hdb,
            'is_landed': is_landed, 'building_name': building,
            'block': blk_no, 'road_name': road_name,
            'project_name': project_name,
            'storey_ranges': storey_ranges,
            'remaining_lease_years': remaining_lease_years,
            'db_is_hdb': db_is_hdb,
            'db_is_condo': db_is_condo,
        }
        if max_floor is not None:
            resp['max_floor'] = int(max_floor)
        return jsonify(resp)
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
            FROM resale_flat_prices
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
    block         = request.args.get('block', '').strip().upper()
    road          = request.args.get('road', '').strip().upper()
    town          = request.args.get('town', '').strip().upper()
    project_name  = request.args.get('project', '').strip().upper()

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
    flat_type_param = request.args.get('flat_type', '').strip().upper()
    flat_type = flat_type_param if flat_type_param in _BEDS_TO_FLAT.values() else _BEDS_TO_FLAT.get(bedrooms, '3 ROOM')

    floor_areas      = []
    max_floor        = None   # computed from real data; frontend falls back to 20 if still None
    storey_ranges    = []
    default_storey_range = None   # most frequent range for this block — pre-selects in UI

    try:
        conn = get_db()
        cur  = _cursor(conn)

        if property_type == 'HDB':
            # Try to get areas for the specific block+street first, then fall back to flat_type-wide
            def _fetch_hdb_areas(extra_where, params):
                cur.execute(_q(
                    "SELECT DISTINCT floor_area_sqm FROM resale_flat_prices "
                    f"WHERE flat_type = ? {extra_where} AND floor_area_sqm IS NOT NULL ORDER BY floor_area_sqm"
                ), params)
                rows = cur.fetchall()
                return sorted(set(float(r['floor_area_sqm'] if hasattr(r, '__getitem__') else r[0])
                                  for r in rows if (r['floor_area_sqm'] if hasattr(r, '__getitem__') else r[0])))

            # Helper: block+road → block+town → town → flat_type-wide (no block-only step)
            def _fetch_hdb_areas_cascade(block, road, flat_type):
                road_kw = road.split()[0] if road else ''
                if block and road_kw:
                    r = _fetch_hdb_areas("AND UPPER(block) = ? AND UPPER(street_name) LIKE ?",
                                         (flat_type, block, f'%{road_kw}%'))
                    if r: return r
                if block and town:
                    r = _fetch_hdb_areas("AND UPPER(block) = ? AND UPPER(town) = ?", (flat_type, block, town))
                    if r: return r
                if town:
                    r = _fetch_hdb_areas("AND UPPER(town) = ?", (flat_type, town))
                    if r: return r
                return _fetch_hdb_areas("", (flat_type,))

            sqm_vals = _fetch_hdb_areas_cascade(block, road, flat_type)

            # Return sqm values rounded to nearest 1 sqm, deduplicated
            floor_areas = sorted(set(round(s) for s in sqm_vals))

            # Helper: fetch storey ranges with cascade fallback
            def _fetch_storeys(extra_where, params):
                cur.execute(_q(
                    "SELECT DISTINCT storey_range FROM resale_flat_prices "
                    f"WHERE flat_type = ? {extra_where} AND storey_range LIKE '% TO %' ORDER BY storey_range"
                ), params)
                rows = cur.fetchall()
                return [str(r['storey_range'] if hasattr(r, '__getitem__') else r[0]) for r in rows]

            storeys = []
            # Step 1: block + first word of road (avoids too-short / too-broad matches)
            road_keyword = road.split()[0] if road else ''
            if block and road_keyword:
                storeys = _fetch_storeys("AND UPPER(block) = ? AND UPPER(street_name) LIKE ?",
                                         (flat_type, block, f'%{road_keyword}%'))
            # Step 2: block + town (skip block-only — same block number exists in many towns)
            if not storeys and block and town:
                storeys = _fetch_storeys("AND UPPER(block) = ? AND UPPER(town) = ?",
                                         (flat_type, block, town))
            # Step 3: town-wide for this flat type
            if not storeys and town:
                storeys = _fetch_storeys("AND UPPER(town) = ?", (flat_type, town))
            # Step 4: flat-type wide across all towns (last resort)
            if not storeys:
                storeys = _fetch_storeys("", (flat_type,))

            def _top(s):
                try: return int(s.split(' TO ')[-1].strip())
                except: return 0

            import statistics as _stats
            top_floors = [_top(s) for s in storeys]
            if top_floors:
                # Use median top floor so a single high-rise doesn't inflate the default
                max_floor = int(_stats.median(top_floors))

            raw_ranges = sorted(
                set(storeys),
                key=lambda s: int(s.split(' TO ')[0].strip()) if ' TO ' in s else 0
            )
            storey_ranges = raw_ranges

            # Most frequent storey range for this specific block → pre-select in UI
            if block:
                _mf_extra = "AND UPPER(block) = ? AND UPPER(town) = ?" if town else "AND UPPER(block) = ?"
                _mf_params = (flat_type, block, town) if town else (flat_type, block)
                cur.execute(_q(
                    "SELECT storey_range, COUNT(*) AS cnt FROM resale_flat_prices "
                    f"WHERE flat_type = ? {_mf_extra} AND storey_range LIKE '% TO %' "
                    "GROUP BY storey_range ORDER BY cnt DESC LIMIT 1"
                ), _mf_params)
                _mf_row = cur.fetchone()
                if _mf_row:
                    default_storey_range = str(
                        _mf_row['storey_range'] if hasattr(_mf_row, '__getitem__') else _mf_row[0]
                    )

        else:  # Condominium
            def _condo_query(where, params):
                cur.execute(_q(
                    "SELECT DISTINCT floor_area_sqft FROM ura_transactions "
                    f"WHERE {where} AND floor_area_sqft IS NOT NULL AND floor_area_sqft > 0 "
                    "ORDER BY floor_area_sqft"
                ), params)
                rows = cur.fetchall()
                return sorted(set(float(r['floor_area_sqft'] if hasattr(r, '__getitem__') else r[0])
                                  for r in rows if (r['floor_area_sqft'] if hasattr(r, '__getitem__') else r[0])))

            def _condo_floors(where, params):
                cur.execute(_q(
                    "SELECT floor_level FROM ura_transactions "
                    f"WHERE {where} AND floor_level IS NOT NULL AND floor_level != ''"
                ), params)
                return [str(r['floor_level'] if hasattr(r, '__getitem__') else r[0]) for r in cur.fetchall()]

            def _fl_top(s):
                nums = _re.findall(r'\d+', s)
                return int(nums[-1]) if nums else 0

            raw = []
            fl_rows = []

            # 1. Try by exact project name (most specific)
            if project_name:
                raw     = _condo_query("UPPER(project) = ?", (project_name,))
                fl_rows = _condo_floors("UPPER(project) = ?", (project_name,))

            # 2. Fall back to postal district
            if not raw:
                sector   = postal[:2] if len(postal) >= 2 else ''
                district = _SECTOR_TO_DISTRICT.get(sector, '')
                if district:
                    raw     = _condo_query("postal_district = ?", (district,))
                    fl_rows = _condo_floors("postal_district = ?", (district,))

            if raw:
                floor_areas = sorted(set(round(v / 50.0) * 50 for v in raw))
            tops = [_fl_top(s) for s in fl_rows]
            if tops:
                import statistics as _stats
                max_floor = int(_stats.median(tops))

        conn.close()
    except Exception:
        pass

    # Fall back to bedroom-based presets if no DB data
    if not floor_areas:
        if property_type == 'HDB':
            # Preset values in sqm (common HDB flat sizes)
            _HDB_PRESETS_SQM = {
                '1 ROOM':[31,33],'2 ROOM':[44,47],'3 ROOM':[60,65,70,75],
                '4 ROOM':[85,90,95,100,105],'5 ROOM':[108,113,121,129,135,140],
                'EXECUTIVE':[122,130,138,146,154],
            }
            floor_areas = _HDB_PRESETS_SQM.get(flat_type, [65,85,105])
        else:
            floor_areas = _CONDO_PRESETS.get(bedrooms, _CONDO_PRESETS[3])

    return jsonify({
        'floor_areas': floor_areas,
        'max_floor': int(max_floor) if max_floor is not None else 20,
        'storey_ranges': storey_ranges,
        'default_storey_range': default_storey_range,
    })


@app.route('/api/admin/upload-transactions', methods=['POST'])
def upload_transactions():
    import csv, io
    tx_type  = request.form.get('type', 'hdb').lower()
    file     = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file provided'}), 400

    filename  = (file.filename or '').lower()
    is_excel  = filename.endswith('.xlsx') or filename.endswith('.xls')
    batch_id  = datetime.datetime.utcnow().isoformat()

    try:
        file_bytes = file.read()
    except Exception as e:
        return jsonify({'error': f'File read error: {e}'}), 400

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _get(row, *keys, default=None):
        """Case-insensitive key lookup across multiple candidate column names."""
        for k in keys:
            kn = k.lower().replace(' ', '').replace('(', '').replace(')', '').replace('$', '').replace('#', '')
            for col in row:
                cn = col.strip().lower().replace(' ', '').replace('(', '').replace(')', '').replace('$', '').replace('#', '')
                if cn == kn:
                    v = row[col]
                    return v if v not in ('', None) else default
        return default

    def _norm_date(raw):
        """Normalise any date-like value to YYYY-MM-DD. Returns None if unparseable."""
        import datetime as _dt
        if raw is None or str(raw).strip() in ('', 'None', 'nan', 'NaT', '-'):
            return None
        if hasattr(raw, 'strftime'):
            return raw.strftime('%Y-%m-%d')
        s = str(raw).strip()
        if len(s) >= 10 and s[4] == '-' and s[7] == '-':
            return s[:10]
        if len(s) == 7 and s[4] == '-':
            return s + '-01'
        for fmt in ('%d %b %Y', '%d %B %Y', '%d-%b-%Y', '%d-%B-%Y'):
            try:
                return _dt.datetime.strptime(s, fmt).strftime('%Y-%m-%d')
            except ValueError:
                pass
        return s or None

    def _norm_month(raw):
        d = _norm_date(raw)
        return (d[:7] + '-01') if d and len(d) >= 7 else None

    def _sf(v, default=0.0):
        """Safe float — never raises."""
        try:
            return float(str(v or '').replace(',', '').strip()) if str(v or '').replace(',', '').strip() not in ('', '-', 'nan') else default
        except (ValueError, TypeError):
            return default

    def _si(v, default=None):
        """Safe int — never raises."""
        try:
            f = float(str(v or '').replace(',', '').strip())
            return int(f) if f else default
        except (ValueError, TypeError):
            return default

    def _batch_insert(conn, cur, sql, params_list, batch_size=1000):
        """Batch insert using execute_values (PostgreSQL) or executemany (SQLite).
        execute_values sends one INSERT ... VALUES(r1),(r2),... per chunk —
        a single round trip regardless of chunk size."""
        total = 0
        if USE_POSTGRES:
            import psycopg2.extras as _pge
            # Rewrite "INSERT INTO t (cols) VALUES (?,?,?)" → template for execute_values
            ev_sql = sql.replace('%s', '%%s')   # escape any existing %s first
            ev_sql = ev_sql.rsplit('VALUES', 1)[0] + 'VALUES %s'
            for i in range(0, len(params_list), batch_size):
                chunk = params_list[i:i + batch_size]
                cur.execute("SAVEPOINT _batch_sp")
                try:
                    _pge.execute_values(cur, ev_sql, chunk, page_size=batch_size)
                    cur.execute("RELEASE SAVEPOINT _batch_sp")
                    total += len(chunk)
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT _batch_sp")
                    # row-by-row fallback for this chunk
                    for p in chunk:
                        cur.execute("SAVEPOINT _row_sp")
                        try:
                            cur.execute(sql, p)
                            cur.execute("RELEASE SAVEPOINT _row_sp")
                            total += 1
                        except Exception:
                            cur.execute("ROLLBACK TO SAVEPOINT _row_sp")
            return total
        # SQLite path
        for i in range(0, len(params_list), batch_size):
            chunk = params_list[i:i + batch_size]
            try:
                cur.executemany(sql, chunk)
                total += len(chunk)
            except Exception:
                for p in chunk:
                    try:
                        cur.execute(sql, p)
                        total += 1
                    except Exception:
                        pass
        return total

    # ── ELT path: hand off to background thread, return immediately ───────────
    # Avoids gunicorn worker timeout on large files (227k+ rows).
    if USE_POSTGRES and tx_type in ('hdb', 'geocoded', 'policy', 'sora') and not is_excel:
        with _upload_lock:
            _upload_jobs[batch_id] = {'state': 'processing', 'message': 'Queued…', 'inserted': 0, 'total_rows': 0}
        threading.Thread(target=_run_upload_thread,
                         args=(batch_id, file_bytes, filename, tx_type),
                         daemon=True).start()
        return jsonify({'job_id': batch_id, 'message': 'Upload started'})

    # ── All other paths: parse file fully (Excel, SQLite, URA) ───────────────
    try:
        if is_excel:
            import pandas as pd
            df   = pd.read_excel(io.BytesIO(file_bytes))
            rows = df.fillna('').astype(str).to_dict('records')
        else:
            content = file_bytes.decode('utf-8-sig')
            reader  = csv.DictReader(io.StringIO(content))
            rows    = list(reader)
    except Exception as e:
        return jsonify({'error': f'File parse error: {e}'}), 400

    if not rows:
        return jsonify({'error': 'CSV is empty'}), 400

    # ── Build params lists (pure Python — no DB calls yet) ───────────────────
    # Used for SQLite (local dev) and URA uploads (Postgres + SQLite).

    params_list = []

    if tx_type == 'hdb':
        sql = _q("""INSERT INTO resale_flat_prices
                    (month, town, flat_type, block, street_name,
                     storey_range, floor_area_sqm, flat_model,
                     lease_commence_date, remaining_lease, resale_price)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)""")
        for r in rows:
            params_list.append((
                _norm_month(_get(r, 'month')),
                _get(r, 'town'),
                _get(r, 'flat_type'),
                str(_get(r, 'block') or '').strip() or None,
                _get(r, 'street_name'),
                _get(r, 'storey_range'),
                _sf(_get(r, 'floor_area_sqm')),
                _get(r, 'flat_model'),
                _si(_get(r, 'lease_commence_date')),
                _get(r, 'remaining_lease'),
                _sf(_get(r, 'resale_price')),
            ))

    elif tx_type == 'geocoded':
        sql = _q("""INSERT INTO geocoded_addresses
                    (search_text, lat, lon)
                    VALUES (?,?,?)""")
        for r in rows:
            lat_v = _sf(_get(r, 'lat', 'latitude')) or None
            lon_v = _sf(_get(r, 'lon', 'lng', 'longitude')) or None
            params_list.append((
                _get(r, 'search_text', 'searchtext', 'search text'),
                lat_v, lon_v,
            ))

    elif tx_type == 'policy':
        sql = _q("""INSERT INTO policy_changes
                    (effective_month, effective_date, policy_name, category,
                     direction, severity, source)
                    VALUES (?,?,?,?,?,?,?)""")
        for r in rows:
            params_list.append((
                _norm_month(_get(r, 'effective_month', 'effectivemonth', 'date', 'policy_date')),
                _norm_date(_get(r, 'effective_date', 'effectivedate')),
                _get(r, 'policy_name', 'policyname', 'name', 'description', 'measure'),
                _get(r, 'category'),
                _si(_get(r, 'direction', 'effect')),
                _si(_get(r, 'severity', 'severity_score', 'score')),
                _get(r, 'source', 'url', 'reference'),
            ))

    elif tx_type == 'sora':
        sql = _q("""INSERT INTO sora_rates
                    (publication_date, compound_sora_3m, compound_sora_6m,
                     highest_transacted_rate, lowest_transacted_rate)
                    VALUES (?,?,?,?,?)""")
        for r in rows:
            rate_3m = _sf(_get(r,
                'compound sora - 3 month', 'compoundsora-3month',
                'compound_sora_3m', 'sora_3m', 'sora', 'published_rate', 'rate'), default=None)
            if rate_3m is None or rate_3m == 0.0:
                continue  # skip rows with no rate value
            params_list.append((
                _norm_date(_get(r,
                    'sora publication date', 'sorapublicationdate',
                    'publication_date', 'published_date', 'publication date',
                    'sora value date', 'soravaluedate',
                    'date', 'rate_date', 'ratedate')),
                rate_3m,
                _sf(_get(r, 'compound sora - 6 month', 'compoundsora-6month',
                          'compound_sora_6m', 'sora_6m'), default=None),
                _sf(_get(r, 'highest transacted rate', 'highest_transacted_rate',
                          'highesttransactedrate', 'high_rate'), default=None),
                _sf(_get(r, 'lowest transacted rate', 'lowest_transacted_rate',
                          'lowesttransactedrate', 'low_rate'), default=None),
            ))

    elif tx_type == 'ura':
        # URA private property transactions CSV downloaded from URA website.
        # Accepted column names (case-insensitive):
        #   Project Name / project
        #   Street Name / street
        #   Property Type / property_type
        #   Market Segment / market_segment
        #   Postal District / postal_district
        #   Floor Level / floor_level
        #   Area (SQFT) / floor_area_sqft
        #   Area (SQM)  / floor_area_sqm
        #   Type of Sale / type_of_sale
        #   Transacted Price ($) / transacted_price
        #   Unit Price ($ PSF) / unit_price_psf
        #   Unit Price ($ PSM) / unit_price_psm
        #   Tenure / tenure
        #   Number of Units / num_units
        #   Sale Date / Nett Price($) optional

        # Full refresh: wipe existing URA data before inserting CSV
        conn2 = get_db(); cur2 = _cursor(conn2)
        try:
            cur2.execute('DELETE FROM ura_transactions')
            conn2.commit()
        except Exception:
            conn2.rollback()
        finally:
            conn2.close()

        sql = _q("""INSERT INTO ura_transactions
                    (project, street, property_type, market_segment,
                     postal_district, floor_level, floor_area_sqft, floor_area_sqm,
                     type_of_sale, transacted_price, unit_price_psf, unit_price_psm,
                     tenure, num_units, sale_date, upload_batch)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")
        for r in rows:
            sale_raw = _get(r, 'sale date', 'saledate', 'sale_date', 'contractdate', 'contract date')
            sale_date = _norm_date(sale_raw)
            if not sale_date:
                # URA CSV uses "Jan-24" or "Jan-2024" format
                import re as _re2
                s = str(sale_raw or '').strip()
                m = _re2.match(r'([A-Za-z]{3})[-\s](\d{2,4})', s)
                if m:
                    import datetime as _dt2
                    try:
                        mo = _dt2.datetime.strptime(m.group(1), '%b').month
                        yr = int(m.group(2))
                        yr = 2000 + yr if yr < 100 else yr
                        sale_date = f'{yr}-{mo:02d}-01'
                    except Exception:
                        pass

            sqft = _sf(_get(r, 'area sqft', 'areasqft', 'floor_area_sqft', 'area(sqft)', 'areasqft'))
            sqm  = _sf(_get(r, 'area sqm',  'areasqm',  'floor_area_sqm',  'area(sqm)',  'areasqm'))
            if sqft and not sqm:
                sqm = sqft / 10.764
            elif sqm and not sqft:
                sqft = sqm * 10.764

            district = str(_get(r, 'postal district', 'postaldistrict', 'postal_district', 'district') or '0').strip().zfill(2)

            params_list.append((
                _get(r, 'project name', 'projectname', 'project'),
                _get(r, 'street name', 'streetname', 'street'),
                _get(r, 'property type', 'propertytype', 'property_type'),
                _get(r, 'market segment', 'marketsegment', 'market_segment'),
                district,
                _get(r, 'floor level', 'floorlevel', 'floor_level', 'floor range', 'floorrange'),
                sqft, sqm,
                _get(r, 'type of sale', 'typeofsale', 'type_of_sale'),
                _sf(_get(r, 'transacted price', 'transactedprice', 'transacted_price', 'transactedprice$')),
                _sf(_get(r, 'unit price psf', 'unitpricepsf', 'unit_price_psf', 'unitprice$psf')),
                _sf(_get(r, 'unit price psm', 'unitpricepsm', 'unit_price_psm', 'unitprice$psm')),
                _get(r, 'tenure'),
                _si(_get(r, 'number of units', 'numberofunits', 'num_units', 'noofunits'), default=1),
                sale_date,
                batch_id,
            ))

    # ── Execute batch inserts ─────────────────────────────────────────────────

    conn = get_db()
    cur  = _cursor(conn)
    try:
        inserted = _batch_insert(conn, cur, sql, params_list)

        if tx_type == 'hdb':
            try:
                cutoff = (datetime.datetime.utcnow() - datetime.timedelta(days=3650)).strftime('%Y-%m-01')
                cur.execute(_q("DELETE FROM resale_flat_prices WHERE month IS NOT NULL AND month < ?"), (cutoff,))
            except Exception:
                pass

        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'error': str(e)}), 500

    conn.close()
    return jsonify({'inserted': inserted, 'total_rows': len(rows), 'batch_id': batch_id})


@app.route('/api/admin/retrain', methods=['POST'])
def retrain_models():
    data = request.get_json(force=True) or {}
    model_type = data.get('type', 'both')  # 'hdb', 'private', or 'both'
    types = ['hdb', 'private'] if model_type == 'both' else [model_type]

    started = []
    for t in types:
        with _retrain_lock:
            if _retrain_status[t]['state'] == 'running':
                continue
        thread = threading.Thread(target=_run_training_thread, args=(t,), daemon=True)
        thread.start()
        started.append(t)

    if not started:
        return jsonify({'message': 'Training already in progress', 'started': []}), 200
    return jsonify({'started': started, 'message': f'Training started for: {", ".join(started)}'})


@app.route('/api/admin/retrain-status', methods=['GET'])
def retrain_status():
    with _retrain_lock:
        return jsonify(dict(_retrain_status))


@app.route('/api/admin/upload-status', methods=['GET'])
def upload_status():
    job_id = request.args.get('job_id', '')
    with _upload_lock:
        job = _upload_jobs.get(job_id)
    if not job:
        return jsonify({'state': 'not_found', 'message': 'Job not found'}), 404
    return jsonify(job)


@app.route('/api/admin/model-status', methods=['GET'])
def model_status():
    """Return live status + trained_at for HDB and private models."""
    import joblib as _jl
    _mdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
    def _check(xgb_file, meta_file):
        live = os.path.exists(os.path.join(_mdir, xgb_file)) and \
               os.path.exists(os.path.join(_mdir, meta_file))
        trained_at = None
        if live:
            try:
                m = _jl.load(os.path.join(_mdir, meta_file))
                trained_at = m.get('trained_at')
            except Exception:
                pass
        return {'live': live, 'trained_at': trained_at}
    return jsonify({
        'hdb':     _check('xgb_pipeline.joblib',         'meta.joblib'),
        'private': _check('xgb_private_pipeline.joblib', 'meta_private.joblib'),
    })


_ALLOWED_MODEL_FILES = {
    'xgb_pipeline.joblib', 'lgbm_pipeline.joblib', 'cat_pipeline.joblib', 'meta.joblib',
    'xgb_private_pipeline.joblib', 'lgbm_private_pipeline.joblib',
    'cat_private_pipeline.joblib', 'meta_private.joblib',
    'shap_hdb.joblib', 'shap_private.joblib',
}

@app.route('/api/admin/trigger-training', methods=['POST'])
def trigger_training():
    """Dispatch a GitHub Actions workflow_dispatch event to train models externally."""
    github_pat    = os.environ.get('GITHUB_PAT', '')
    github_repo   = os.environ.get('GITHUB_REPO', '')
    github_branch = os.environ.get('GITHUB_BRANCH', 'main')
    if not github_pat or not github_repo:
        return jsonify({'error': 'GITHUB_PAT and GITHUB_REPO env vars are not configured on the server.'}), 400
    data       = request.get_json(force=True) or {}
    model_type = data.get('type', 'hdb')
    import requests as _req
    resp = _req.post(
        f'https://api.github.com/repos/{github_repo}/actions/workflows/train_models.yml/dispatches',
        headers={'Authorization': f'token {github_pat}',
                 'Accept': 'application/vnd.github.v3+json'},
        json={'ref': github_branch, 'inputs': {'model_type': model_type}},
        timeout=10,
    )
    if resp.status_code == 204:
        return jsonify({'message': f'Training job queued on GitHub Actions for: {model_type}'})
    return jsonify({'error': f'GitHub API returned {resp.status_code}: {resp.text}'}), 500


@app.route('/api/admin/upload-model', methods=['POST'])
def upload_model_file():
    """Accept a .joblib model file (from Colab or GitHub Actions) and hot-swap it."""
    # Optional secret auth — required when MODEL_UPLOAD_SECRET env var is set
    expected = os.environ.get('MODEL_UPLOAD_SECRET', '')
    if expected:
        provided = request.headers.get('X-Upload-Secret', '')
        # Allow through if no header sent (browser manual upload from admin panel)
        # Reject only when a wrong secret is explicitly provided
        if provided and provided != expected:
            return jsonify({'error': 'Invalid upload secret'}), 401
    f = request.files.get('file')
    if not f or not f.filename:
        return jsonify({'error': 'No file provided'}), 400
    filename = os.path.basename(f.filename)
    if filename not in _ALLOWED_MODEL_FILES:
        return jsonify({'error': f'Unrecognised file: {filename}. Allowed: {sorted(_ALLOWED_MODEL_FILES)}'}), 400
    dest_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, filename)
    f.save(dest)
    _predict_module.reset_model_cache()
    print(f"[upload-model] Saved {filename} → {dest}; model cache reset")
    return jsonify({'message': f'{filename} uploaded and model cache reloaded.'})


@app.route('/api/admin/sync-ura', methods=['POST'])
def sync_ura():
    """Fetch latest URA private property transactions and insert new records into ura_transactions."""
    access_key = os.environ.get('URA_ACCESS_KEY', '')
    if not access_key:
        return jsonify({'error': 'URA_ACCESS_KEY not configured'}), 400

    URA_BASE = 'https://eservice.ura.gov.sg/uraDataService'
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
        token_url = f'{URA_BASE}/insertNewToken/v1'
        req = urllib.request.Request(token_url, headers={'AccessKey': access_key})
        r = urllib.request.urlopen(req, timeout=30)
        raw_text = r.read().decode('utf-8', errors='replace').strip()
        if not raw_text or raw_text.startswith('<'):
            return jsonify({'error': f'URA token endpoint returned unexpected response: {raw_text[:200]}'}), 500
        token_data = json.loads(raw_text)
        if token_data.get('Status') != 'Success':
            return jsonify({'error': f'URA token error: {token_data}'}), 500
        token = token_data['Result']
    except Exception as e:
        return jsonify({'error': f'URA token request failed: {e}'}), 500

    batch_id = datetime.datetime.utcnow().isoformat()
    inserted = 0

    # Collect all rows from URA API before touching the DB
    rows = []
    for batch in range(1, 5):
        try:
            req = urllib.request.Request(
                f'{URA_BASE}/invokeUraDS/v1?service=PMI_Resi_Transaction&batch={batch}',
                headers={'AccessKey': access_key, 'Token': token}
            )
            r = urllib.request.urlopen(req, timeout=60)
            data = json.loads(r.read())
            if data.get('Status') != 'Success':
                continue
            for proj in data.get('Result', []):
                mkt = proj.get('marketSegment', '')
                for det in proj.get('transaction', []):
                    cd = str(det.get('contractDate', ''))
                    try:
                        # contractDate format: "MMYY" e.g. "0715" = July 2015
                        mo, yr = int(cd[:2]), int(cd[2:])
                        year = 2000 + yr if yr < 100 else yr
                    except Exception:
                        continue
                    sale_date = f'{year}-{mo:02d}'
                    rows.append((
                        proj.get('project', ''),
                        det.get('street', proj.get('street', '')),
                        det.get('propertyType', ''),
                        mkt,
                        str(det.get('district', '0')).zfill(2),
                        det.get('floorRange') or det.get('floorLevel', ''),
                        float(det.get('area') or 0),
                        float(det.get('area') or 0) / 10.764,
                        _TYPE_MAP.get(str(det.get('typeOfSale', '3')), 'Resale'),
                        float(det.get('price') or 0),
                        float(det.get('unitPrice') or 0),
                        None, det.get('tenure', ''),
                        int(float(det.get('noOfUnits') or 1)),
                        sale_date, batch_id
                    ))
        except Exception:
            continue

    if not rows:
        return jsonify({'error': 'URA API returned no records — check URA_ACCESS_KEY or try again later'}), 500

    conn = get_db(); cur = _cursor(conn)
    try:
        # Incremental upsert: skip records that already exist for same project+sale_date+floor_level
        skipped = 0
        for row in rows:
            proj, _, _, _, _, floor_lv, _, _, _, _, _, _, _, _, sale_dt, _ = row
            cur.execute(_q(
                "SELECT 1 FROM ura_transactions WHERE project = ? AND sale_date = ? AND floor_level = ? LIMIT 1"
            ), (proj, sale_dt, floor_lv))
            if cur.fetchone():
                skipped += 1
                continue
            cur.execute(_q("""
                INSERT INTO ura_transactions
                    (project, street, property_type, market_segment,
                     postal_district, floor_level, floor_area_sqft, floor_area_sqm,
                     type_of_sale, transacted_price, unit_price_psf, unit_price_psm,
                     tenure, num_units, sale_date, upload_batch)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """), row)
            inserted += 1
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'error': str(e)}), 500

    conn.close()
    return jsonify({'inserted': inserted, 'skipped': skipped if 'skipped' in dir() else 0,
                    'batch_id': batch_id,
                    'message': f'Synced {inserted} new URA records ({skipped} already existed)'})


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


@app.route('/api/property-request', methods=['POST'])
def property_request():
    """Store a user's request to add an unlisted property."""
    data    = request.get_json(force=True) or {}
    postal  = str(data.get('postal', '')).strip()
    desc    = str(data.get('description', '')).strip()
    contact = str(data.get('contact', '')).strip()
    if not postal:
        return jsonify({'error': 'Postal code is required.'}), 400

    conn = get_db(); cur = _cursor(conn)
    try:
        cur.execute(_q("""
            CREATE TABLE IF NOT EXISTS property_requests (
                id SERIAL PRIMARY KEY,
                postal TEXT NOT NULL,
                description TEXT,
                contact TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """ if USE_POSTGRES else """
            CREATE TABLE IF NOT EXISTS property_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                postal TEXT NOT NULL,
                description TEXT,
                contact TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        cur.execute(_q(
            "INSERT INTO property_requests (postal, description, contact) VALUES (?,?,?)"
        ), (postal, desc, contact))
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'error': str(e)}), 500
    conn.close()
    print(f"[property-request] postal={postal} contact={contact} desc={desc}", flush=True)
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
    hdb_txs      = cnt('resale_flat_prices')
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

    # HDB vs Private avg estimated values
    hdb_avg_val = 0
    priv_avg_val = 0
    try:
        cur.execute("SELECT AVG(estimated_value) AS v FROM predictions WHERE flat_type IN ('1 ROOM','2 ROOM','3 ROOM','4 ROOM','5 ROOM','EXECUTIVE')")
        r = _row(cur)
        hdb_avg_val = int(r['v'] or 0) if r and r.get('v') else 0
    except Exception:
        pass
    try:
        cur.execute("SELECT AVG(estimated_value) AS v FROM predictions WHERE flat_type NOT IN ('1 ROOM','2 ROOM','3 ROOM','4 ROOM','5 ROOM','EXECUTIVE') AND flat_type IS NOT NULL")
        r = _row(cur)
        priv_avg_val = int(r['v'] or 0) if r and r.get('v') else 0
    except Exception:
        pass

    # Recent registrations
    recent_regs = []
    try:
        cur.execute(_q("SELECT full_name, email, account_type, created_at FROM users ORDER BY id DESC LIMIT 20"))
        recent_regs = _rows(cur)
    except Exception:
        pass

    # Recent audit log entries
    recent_audit = []
    try:
        cur.execute(_q(
            "SELECT al.action, al.event_type, al.logged_at, u.full_name AS user_name "
            "FROM audit_log al LEFT JOIN users u ON al.user_id = u.id "
            "ORDER BY al.logged_at DESC LIMIT 50"))
        recent_audit = _rows(cur)
    except Exception:
        pass

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
    W = doc.width

    # ── Cover / Header (blue bar) ─────────────────────────────────────────
    BLUE_BG = HexColor('#1E40AF')
    WHITE   = HexColor('#FFFFFF')
    header_data = [[Paragraph(
        '<font color="#FFFFFF" size="20"><b>PropAI.sg  Platform Report</b></font>',
        ParagraphStyle('HDR', parent=SS['Normal'], fontSize=20, textColor=WHITE, spaceAfter=0)
    )]]
    header_tbl = Table(header_data, colWidths=[W])
    header_tbl.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), BLUE_BG),
        ('TOPPADDING',    (0,0), (-1,-1), 16),
        ('BOTTOMPADDING', (0,0), (-1,-1), 16),
        ('LEFTPADDING',   (0,0), (-1,-1), 16),
        ('RIGHTPADDING',  (0,0), (-1,-1), 16),
        ('ROUNDEDCORNERS', [8]),
    ]))
    story.append(header_tbl)
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph(f'Generated: {now_str}', SMALL))
    story.append(hr())
    story.append(Spacer(1, 0.3*cm))

    # ── 1. Platform Overview ─────────────────────────────────────────────
    story.append(Paragraph('1. Platform Overview', H1))
    ov_data = [
        ['Metric', 'Value'],
        ['Total Registered Users',      str(total_users)],
        ['  — Admin Users',             str(admin_count)],
        ['  — Regular Users',           str(total_users - admin_count)],
        ['Total Predictions Made',      str(total_preds)],
        ['  — HDB Predictions',         str(hdb_c)],
        ['  — Private Property',        str(priv_c)],
        ['Price Records in DB',         str(total_recs)],
        ['HDB Transaction Records',     str(hdb_txs)],
        ['Private Transaction Records', str(priv_txs)],
        ['Database Size',               db_size],
    ]
    story.append(tbl(ov_data, [W*0.6, W*0.4]))
    story.append(Spacer(1, 0.5*cm))

    # ── 2. HDB vs Private Property Comparison ───────────────────────────
    story.append(Paragraph('2. HDB vs Private Property Comparison', H1))
    t_sum = hdb_c + priv_c or 1
    cmp_data = [
        ['Category', 'Prediction Count', 'Share', 'Avg Estimated Value'],
        ['HDB Resale',
         str(hdb_c),
         f"{hdb_c/t_sum*100:.1f}%",
         f"S${hdb_avg_val:,}" if hdb_avg_val else '—'],
        ['Private / Condo',
         str(priv_c),
         f"{priv_c/t_sum*100:.1f}%",
         f"S${priv_avg_val:,}" if priv_avg_val else '—'],
    ]
    story.append(tbl(cmp_data, [W*0.30, W*0.20, W*0.15, W*0.35]))
    story.append(Spacer(1, 0.5*cm))

    # ── 3. User Statistics ───────────────────────────────────────────────
    story.append(Paragraph('3. User Statistics', H1))

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

    # ── 4. Prediction Analytics ──────────────────────────────────────────
    story.append(Paragraph('4. Prediction Analytics', H1))

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

    # ── 5. Database Statistics ───────────────────────────────────────────
    story.append(Paragraph('5. Database Statistics', H1))
    db_data = [
        ['Table', 'Record Count'],
        ['users',               str(total_users)],
        ['predictions',         str(total_preds)],
        ['price_records',       str(total_recs)],
        ['resale_flat_prices',  str(hdb_txs)],
        ['ura_transactions',    str(priv_txs)],
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

    # ── 6. Recent Registrations ──────────────────────────────────────────
    if recent_regs:
        story.append(Paragraph('6. Recent Registrations (last 20)', H1))
        rr_data = [['Name', 'Email', 'Type', 'Registered At']]
        for r in recent_regs:
            dt_raw = str(r.get('created_at') or '')
            dt = dt_raw[:10] if dt_raw else ''
            rr_data.append([
                str(r.get('full_name') or '')[:22],
                str(r.get('email') or '')[:28],
                str(r.get('account_type') or ''),
                dt,
            ])
        story.append(tbl(rr_data, [W*0.25, W*0.35, W*0.15, W*0.25]))
        story.append(Spacer(1, 0.5*cm))

    # ── 7. Recent Audit Log ──────────────────────────────────────────────
    if recent_audit:
        story.append(Paragraph('7. Recent Audit Log (last 50 entries)', H1))
        al_data = [['Time', 'User', 'Event', 'Action']]
        for r in recent_audit:
            dt_raw = str(r.get('logged_at') or '')
            dt = dt_raw[:16] if dt_raw else ''
            al_data.append([
                dt,
                str(r.get('user_name') or 'System')[:18],
                str(r.get('event_type') or ''),
                str(r.get('action') or '')[:40],
            ])
        story.append(tbl(al_data, [W*0.18, W*0.18, W*0.14, W*0.50]))
        story.append(Spacer(1, 0.3*cm))

    story.append(hr())
    story.append(Paragraph('End of Report — PropAI.sg Admin Portal', SMALL))

    doc.build(story)
    buf.seek(0)

    from flask import send_file
    fname = f"propai_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
    return send_file(buf, as_attachment=True, download_name=fname, mimetype='application/pdf')


@app.route('/api/admin/audit-log', methods=['GET'])
def admin_audit_log():
    """Return audit log entries, optionally filtered by month (YYYY-MM format)."""
    # Auth check (admin only)
    token = request.headers.get('Authorization', '').replace('Bearer ', '').strip()
    if not token:
        token = request.args.get('token', '')
    conn = get_db()
    cur = _cursor(conn)
    cur.execute(_q("SELECT id, role FROM users WHERE token = ?"), (token,))
    u = _row(cur)
    if not u or u.get('role') != 'admin':
        conn.close()
        return jsonify({"error": "Admin access required"}), 403

    month_filter = request.args.get('month', '').strip()  # e.g. '2026-04'

    try:
        if month_filter:
            if USE_POSTGRES:
                cur.execute(
                    "SELECT al.id, al.action, al.event_type, al.details, al.logged_at, u.full_name AS user_name "
                    "FROM audit_log al LEFT JOIN users u ON al.user_id = u.id "
                    "WHERE TO_CHAR(al.logged_at::timestamp, 'YYYY-MM') = %s "
                    "ORDER BY al.logged_at DESC LIMIT 200", (month_filter,))
            else:
                cur.execute(
                    "SELECT al.id, al.action, al.event_type, al.details, al.logged_at, u.full_name AS user_name "
                    "FROM audit_log al LEFT JOIN users u ON al.user_id = u.id "
                    "WHERE strftime('%Y-%m', al.logged_at) = ? "
                    "ORDER BY al.logged_at DESC LIMIT 200", (month_filter,))
        else:
            cur.execute(
                _q("SELECT al.id, al.action, al.event_type, al.details, al.logged_at, u.full_name AS user_name "
                   "FROM audit_log al LEFT JOIN users u ON al.user_id = u.id "
                   "ORDER BY al.logged_at DESC LIMIT 500"))

        logs = _rows(cur)
        for l in logs:
            for k in list(l.keys()):
                if hasattr(l[k], 'isoformat'):
                    l[k] = l[k].isoformat()

        # Get distinct months for filter dropdown
        if USE_POSTGRES:
            cur.execute("SELECT DISTINCT TO_CHAR(logged_at::timestamp, 'YYYY-MM') AS m FROM audit_log ORDER BY m DESC LIMIT 24")
        else:
            cur.execute("SELECT DISTINCT strftime('%Y-%m', logged_at) AS m FROM audit_log ORDER BY m DESC LIMIT 24")
        months = [r['m'] for r in _rows(cur) if r.get('m')]

        conn.close()
        return jsonify({"logs": logs, "months": months})
    except Exception as e:
        try: conn.close()
        except: pass
        return jsonify({"error": str(e)}), 500


init_db()
migrate_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    app.run(host='0.0.0.0', port=port, debug=not USE_POSTGRES)
