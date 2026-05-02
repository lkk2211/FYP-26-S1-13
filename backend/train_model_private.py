#!/usr/bin/env python3
"""
Train private property (condo/apartment) price prediction model.
Uses stacked generalisation: XGBoost + LightGBM + CatBoost base learners,
then a Ridge regression meta-learner trained on out-of-fold predictions.

Data source: ura_transactions table (uploaded via admin panel) or URA API.

URA API JSON format (PMI_Resi_Transaction):
  project, marketSegment, street, x, y
  transaction[]: contractDate, area (sqm), price, propertyType,
                 typeOfArea, tenure, floorRange, typeOfSale, district

Models saved to ./models/:
    xgb_private_pipeline.joblib
    lgbm_private_pipeline.joblib
    cat_private_pipeline.joblib
    meta_private.joblib          ← includes stacker coefficients
"""
import os
import re
import sys
import gc
import warnings
import joblib

warnings.filterwarnings('ignore', message='X does not have valid feature names')
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import Ridge, HuberRegressor
from sklearn.model_selection import KFold, cross_val_predict
from sklearn.metrics import mean_absolute_error, r2_score
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from catboost import CatBoostRegressor

MODELS_DIR   = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
URA_BASE     = 'https://eservice.ura.gov.sg/uraDataService'
ACCESS_KEY   = os.environ.get('URA_ACCESS_KEY', '')
DATABASE_URL = os.environ.get('DATABASE_URL', '')

# Approximate centroid (lat, lon) for each Singapore postal district.
# Used to compute amenity distances for private properties — the model has no
# per-project geocoding, so district centroids give the best available spatial anchor.
_DISTRICT_CENTROIDS = {
    'D01': (1.2800, 103.8500),  # Boat Quay / Raffles Place / Marina
    'D02': (1.2780, 103.8440),  # Chinatown / Tanjong Pagar
    'D03': (1.2760, 103.8150),  # Alexandra / Queenstown
    'D04': (1.2650, 103.8200),  # Harbourfront / Telok Blangah
    'D05': (1.3050, 103.7850),  # Clementi / West Coast
    'D06': (1.2900, 103.8450),  # City Hall / Bugis
    'D07': (1.3020, 103.8560),  # Beach Road / Lavender
    'D08': (1.3090, 103.8620),  # Farrer Park / Little India
    'D09': (1.2990, 103.8330),  # Orchard / River Valley
    'D10': (1.3100, 103.8090),  # Buona Vista / Holland Village / Farrer
    'D11': (1.3260, 103.8170),  # Newton / Novena / Watten
    'D12': (1.3200, 103.8450),  # Toa Payoh / Balestier
    'D13': (1.3330, 103.8810),  # Potong Pasir / Macpherson
    'D14': (1.3140, 103.8880),  # Geylang / Eunos
    'D15': (1.3050, 103.9060),  # Katong / Joo Chiat / Marine Parade
    'D16': (1.3230, 103.9330),  # Bedok / Upper East Coast
    'D17': (1.3610, 103.9400),  # Loyang / Changi
    'D18': (1.3560, 103.9540),  # Tampines / Pasir Ris
    'D19': (1.3750, 103.8800),  # Serangoon / Hougang / Punggol
    'D20': (1.3470, 103.8420),  # Ang Mo Kio / Bishan
    'D21': (1.3330, 103.7680),  # Clementi Park / Upper Bukit Timah
    'D22': (1.3490, 103.7100),  # Jurong
    'D23': (1.3800, 103.7540),  # Hillview / Bukit Batok / Choa Chu Kang
    'D24': (1.4050, 103.7750),  # Lim Chu Kang / Tengah
    'D25': (1.4360, 103.8110),  # Woodlands
    'D26': (1.4040, 103.8170),  # Upper Thomson / Mandai
    'D27': (1.4390, 103.8290),  # Yishun / Sembawang
    'D28': (1.3850, 103.8860),  # Sengkang / Punggol
}
MIN_YEAR     = 2018   # only use transactions from this year onwards

TEMP_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'temp_progress_private.csv')

# ─── Feature columns ─────────────────────────────────────────────────────────

# Only train on condo/apartment types — landed properties (terrace, semi-D, bungalow)
# price by land area and plot value, not floor area/level, and are not supported by the UI.
CONDO_PROPERTY_TYPES = {
    'CONDOMINIUM', 'APARTMENT', 'EXECUTIVE CONDOMINIUM', 'EXECUTIVE CONDO', 'EC',
}

CATEGORICAL_COLS = ['property_type', 'market_segment', 'type_of_sale', 'postal_district']
NUMERICAL_COLS   = [
    'floor_area_sqft', 'floor_level_num', 'floor_level_pct',
    'tenure_remaining_years', 'is_strata',
    'year', 'quarter', 'time_idx',
    'direction', 'severity', 'policy_impact', 'months_since_policy_change', 'sora',
    'project_rolling_psf_6m',       # rolling 24-month PSF for this project (no leakage)
    'project_median_psf_alltime',   # all-time project median PSF (stable anchor)
    'district_rolling_psf_24m',      # 24-month district×property_type PSF (project fallback)
    'district_median_psf_alltime',   # all-time district×property_type median (stable anchor)
    'storey_psf_interaction',        # floor_level_pct × project_rolling_psf_6m
    'sin_quarter', 'cos_quarter',    # cyclic quarter encoding (seasonality)
    'dist_nearest_mrt_km',           # MRT proximity (from district centroid)
    'dist_nearest_school_km',        # school proximity — Phase 2A/2B/2C premium
    'dist_nearest_hawker_km',        # hawker centre proximity
    'dist_nearest_health_km',        # hospital / clinic proximity
    'dist_nearest_park_km',          # park / green space proximity
]
ALL_FEATURES = CATEGORICAL_COLS + NUMERICAL_COLS

_TYPE_OF_SALE_API = {'1': 'New Sale', '2': 'Sub Sale', '3': 'Resale'}
_CURRENT_YEAR = datetime.now().year


# ─── Tenure helpers ──────────────────────────────────────────────────────────

def _parse_tenure(tenure_raw, sale_year=None) -> float:
    """
    Parse tenure string to remaining years at time of sale.
    Examples: '99 yrs lease commencing from 2007', 'Freehold', '999 yrs lease commencing from 1950'
    Returns 99.0 for unknown leasehold, 999.0 for freehold.
    """
    t = str(tenure_raw or '').strip().lower()
    if not t or 'freehold' in t:
        return 999.0
    m = re.search(r'(\d+)\s+yr', t)
    comm = re.search(r'from\s+(\d{4})', t)
    if m and comm:
        total = int(m.group(1))
        start = int(comm.group(1))
        expiry = start + total
        base_year = sale_year if sale_year else _CURRENT_YEAR
        return max(float(expiry - base_year), 0.0)
    if m:
        return float(m.group(1))
    return 99.0


# ─── Floor level helpers ─────────────────────────────────────────────────────

def _parse_floor_level(fl_raw) -> float:
    fl = str(fl_raw or '').strip().upper()
    if fl.startswith('B'):
        return 0.0
    nums = re.findall(r'\d+', fl)
    if len(nums) >= 2:
        return (int(nums[0]) + int(nums[1])) / 2
    if len(nums) == 1:
        return float(nums[0])
    fl_lower = fl.lower()
    if 'low' in fl_lower:  return 4.0
    if 'mid' in fl_lower:  return 13.0
    if 'high' in fl_lower: return 25.0
    return 10.0


def _parse_sale_date(raw) -> tuple:
    raw = str(raw or '').strip()
    m = re.match(r'([A-Za-z]{3})[\-\s\'](\d{2,4})', raw)
    if m:
        month_str, yr = m.group(1), m.group(2)
        month = datetime.strptime(month_str, '%b').month
        year  = 2000 + int(yr) if len(yr) == 2 else int(yr)
        return year, (month - 1) // 3 + 1
    m2 = re.match(r'(\d{4})[-/](\d{1,2})', raw)
    if m2:
        year, month = int(m2.group(1)), int(m2.group(2))
        return year, (month - 1) // 3 + 1
    m3 = re.match(r'(\d{1,2})[-/](\d{4})', raw)
    if m3:
        month, year = int(m3.group(1)), int(m3.group(2))
        return year, (month - 1) // 3 + 1
    return None, None


# ─── DB helpers ──────────────────────────────────────────────────────────────

def _get_db_conn():
    if DATABASE_URL:
        import psycopg2, psycopg2.extras
        return psycopg2.connect(DATABASE_URL), 'postgres'
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'propaisg.db')
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn, 'sqlite'


def _query(sql):
    conn, kind = _get_db_conn()
    if kind == 'postgres':
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
    else:
        cur = conn.cursor()
        cur.execute(sql)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def _haversine_min(lat, lon, coords):
    """Vectorised minimum haversine distance in km from (lat,lon) to any point in coords."""
    if not coords:
        return 1.0
    import math as _m
    R = 6371.0
    lat_r = _m.radians(lat)
    min_d = float('inf')
    for clat, clon in coords:
        dlat = _m.radians(clat - lat)
        dlon = _m.radians(clon - lon)
        a = _m.sin(dlat/2)**2 + _m.cos(lat_r)*_m.cos(_m.radians(clat))*_m.sin(dlon/2)**2
        d = R * 2 * _m.atan2(_m.sqrt(a), _m.sqrt(1-a))
        if d < min_d:
            min_d = d
    return round(min_d, 4)


def load_amenities_from_db():
    """Load amenity coordinates grouped by type. Same as HDB model."""
    import json as _json
    result = {}
    try:
        rows = _query("SELECT amenity_type, latitude, longitude FROM amenities WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
        for r in rows:
            t = str(r['amenity_type']).strip().lower()
            result.setdefault(t, []).append((float(r['latitude']), float(r['longitude'])))
        cache_rows = _query("SELECT data FROM amenity_cache WHERE data IS NOT NULL")
        for cr in cache_rows:
            try:
                blob = _json.loads(cr['data'])
                for atype, items in blob.items():
                    key = atype.strip().lower()
                    for item in items:
                        la, lo = item.get('lat'), item.get('lng')
                        if la and lo:
                            result.setdefault(key, []).append((float(la), float(lo)))
            except Exception:
                continue
        for t in result:
            result[t] = list({(round(la, 4), round(lo, 4)) for la, lo in result[t]})
        if result:
            print(f'  Private amenities: { {t: len(v) for t, v in sorted(result.items())} }')
    except Exception as e:
        print(f'  amenities load error (non-critical): {e}')
    return result


def load_policy_from_db():
    try:
        rows = _query("SELECT effective_month, direction, severity FROM policy_changes WHERE effective_month IS NOT NULL")
        if not rows:
            return None
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"  policy_changes load error: {e}")
        return None


def load_sora_from_db():
    try:
        rows = _query("SELECT publication_date, compound_sora_3m FROM sora_rates WHERE compound_sora_3m IS NOT NULL")
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df['date']    = pd.to_datetime(df['publication_date'], errors='coerce')
        df['sora_3m'] = pd.to_numeric(df['compound_sora_3m'].astype(str), errors='coerce')
        df = df.dropna(subset=['date', 'sora_3m'])
        df['month'] = df['date'].dt.to_period('M').dt.to_timestamp().astype('datetime64[s]')
        return df.groupby('month', as_index=False)['sora_3m'].mean().rename(columns={'sora_3m': 'sora'})
    except Exception as e:
        print(f"  sora_rates load error: {e}")
        return None


# ─── URA API download ────────────────────────────────────────────────────────

def _get_ura_token(access_key: str) -> str:
    r = requests.get(
        f'{URA_BASE}/insertNewToken/v1',
        headers={'AccessKey': access_key},
        timeout=30,
    )
    if r.status_code != 200:
        raise ValueError(f'Token request HTTP {r.status_code}:\n{r.text[:300]}')
    data = r.json()
    if data.get('Status') != 'Success':
        raise ValueError(f'URA token error: {data}')
    return data['Result']


def _fetch_batch(access_key: str, token: str, batch: int) -> list:
    r = requests.get(
        f'{URA_BASE}/invokeUraDS/v1',
        params={'service': 'PMI_Resi_Transaction', 'batch': batch},
        headers={'AccessKey': access_key, 'Token': token},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    if data.get('Status') != 'Success':
        print(f'  Batch {batch}: status={data.get("Status")}')
        return []
    return data.get('Result', [])


def download_from_ura_api(access_key: str) -> pd.DataFrame:
    """Download all 4 batches from URA API and return flat DataFrame."""
    print('Getting URA token...')
    token = _get_ura_token(access_key)
    rows  = []
    for batch in range(1, 5):
        print(f'  Fetching batch {batch}/4...', end=' ')
        projects = _fetch_batch(access_key, token, batch)
        for proj in projects:
            market_seg = proj.get('marketSegment', '')
            for det in proj.get('transaction', []):
                cd = det.get('contractDate', '')
                try:
                    mo, yr = int(cd[:2]), int(cd[2:])
                    year   = 2000 + yr if yr < 100 else yr
                    quarter = (mo - 1) // 3 + 1
                except Exception:
                    continue
                # URA API: area is in sqm — convert to sqft for model consistency
                area_sqm  = float(det.get('area') or 0)
                area_sqft = area_sqm * 10.764
                tenure_raw = det.get('tenure', '')
                type_of_area = str(det.get('typeOfArea') or '').strip()
                rows.append({
                    'project':                 str(proj.get('project') or '').strip().upper(),
                    'property_type':          det.get('propertyType', ''),
                    'market_segment':          market_seg,
                    'type_of_sale':            _TYPE_OF_SALE_API.get(str(det.get('typeOfSale', '3')), 'Resale'),
                    'postal_district':         str(det.get('district', '0')).zfill(2),
                    'floor_area_sqft':         area_sqft,
                    'floor_level_num':         _parse_floor_level(det.get('floorRange') or det.get('floorLevel')),
                    'tenure_remaining_years':  _parse_tenure(tenure_raw, year),
                    'is_strata':               1.0 if 'strata' in type_of_area.lower() else 0.0,
                    'year':                    year,
                    'quarter':                 quarter,
                    'sale_date':               f'{year}-{mo:02d}',
                    'transacted_price':        float(det.get('price') or 0),
                })
        print(f'{len(rows):,} so far')
    return pd.DataFrame(rows)


# ─── Load from uploaded DB table ─────────────────────────────────────────────

def load_from_db() -> pd.DataFrame:
    """Read ura_transactions table and return a flat DataFrame."""
    if DATABASE_URL:
        import psycopg2, psycopg2.extras
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    else:
        import sqlite3
        DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'propaisg.db')
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

    # sale_date is stored as 'YYYY-MM' — use string prefix comparison instead of date cast
    cur.execute(f"SELECT * FROM ura_transactions WHERE LEFT(sale_date::text, 4) >= '{MIN_YEAR}'")
    rows_raw = [dict(r) for r in cur.fetchall()]
    conn.close()

    if not rows_raw:
        # Fall back to loading all records (no year filter) if filtered query returns nothing
        conn2, _ = _get_db_conn()
        if DATABASE_URL:
            import psycopg2.extras
            cur2 = conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            cur2 = conn2.cursor()
        cur2.execute("SELECT * FROM ura_transactions")
        rows_raw = [dict(r) for r in cur2.fetchall()]
        conn2.close()

    if not rows_raw:
        raise ValueError('ura_transactions table is empty — upload CSV data first.')

    rows = []
    for r in rows_raw:
        year, quarter = _parse_sale_date(r.get('sale_date'))
        if year is None or year < MIN_YEAR:
            continue
        tenure_raw   = str(r.get('tenure') or '')
        type_of_area = str(r.get('type_of_area') or r.get('typeofarea') or '')
        rows.append({
            'project':                str(r.get('project') or '').strip().upper(),
            'property_type':          str(r.get('property_type') or '').strip().upper(),
            'market_segment':         str(r.get('market_segment') or '').strip().upper(),
            'type_of_sale':           str(r.get('type_of_sale') or 'Resale').strip(),
            'postal_district':        str(r.get('postal_district') or '0').strip().zfill(2),
            'floor_area_sqft':        float(r.get('floor_area_sqft') or 0),
            'floor_level_num':        _parse_floor_level(r.get('floor_level')),
            'tenure_remaining_years': _parse_tenure(tenure_raw, year),
            'is_strata':              1.0 if 'strata' in type_of_area.lower() else 0.0,
            'year':                   year,
            'quarter':                quarter,
            'transacted_price':       float(r.get('transacted_price') or 0),
        })
    return pd.DataFrame(rows)


# ─── Feature engineering ─────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame, policy_df=None, sora_df=None, amenity_dict=None) -> pd.DataFrame:
    df = df.copy()
    for col in ['property_type', 'market_segment', 'type_of_sale']:
        df[col] = df[col].astype(str).str.strip().str.upper().str.replace(r'\s+', ' ', regex=True)
    df['postal_district']        = df['postal_district'].astype(str).str.strip().str.zfill(2)
    df['floor_area_sqft']        = pd.to_numeric(df['floor_area_sqft'], errors='coerce')
    df['floor_level_num']        = pd.to_numeric(df['floor_level_num'], errors='coerce').fillna(10)
    df['tenure_remaining_years'] = pd.to_numeric(df['tenure_remaining_years'], errors='coerce').fillna(99)
    df['is_strata']              = pd.to_numeric(df['is_strata'], errors='coerce').fillna(1)
    df['transacted_price']       = pd.to_numeric(df['transacted_price'], errors='coerce')
    df['time_idx_raw']           = df['year'] * 12 + df['quarter'] * 3

    # Month for policy/SORA merge
    df['month'] = pd.to_datetime(
        df['year'].astype(str) + '-' + ((df['quarter'] - 1) * 3 + 1).astype(str).str.zfill(2) + '-01'
    ).astype('datetime64[s]')

    # Policy merge
    if policy_df is not None and len(policy_df) > 0:
        pol = policy_df.copy()
        pol['effective_month'] = pd.to_datetime(pol['effective_month'], errors='coerce').astype('datetime64[s]')
        pol['direction'] = pd.to_numeric(pol['direction'], errors='coerce').fillna(0)
        pol['severity']  = pd.to_numeric(pol['severity'],  errors='coerce').fillna(0)
        pol = pol.dropna(subset=['effective_month']).sort_values('effective_month')
        df  = df.sort_values('month')
        df  = pd.merge_asof(
            df,
            pol[['effective_month', 'direction', 'severity']],
            left_on='month', right_on='effective_month',
            direction='backward',
        )
        df['policy_impact'] = df['direction'] * df['severity']
        df['months_since_policy_change'] = (
            (df['month'].dt.to_period('M') - df['effective_month'].dt.to_period('M'))
            .apply(lambda x: x.n if pd.notna(x) else 0)
        )
    else:
        df['direction'] = 0.0
        df['severity']  = 0.0
        df['policy_impact'] = 0.0
        df['months_since_policy_change'] = 0

    # SORA merge
    if sora_df is not None and len(sora_df) > 0:
        sora_df = sora_df.copy()
        sora_df['month'] = sora_df['month'].astype('datetime64[s]')
        df = df.merge(sora_df, on='month', how='left')
        df['sora'] = df['sora'].fillna(df['sora'].median() if 'sora' in df else 3.5)
    else:
        df['sora'] = 3.5

    # ── Project-level PSF features (no data leakage via shift(1)) ───────────────
    # project_rolling_psf_6m: rolling 8-quarter (24m) mean PSF for this project
    # project_median_psf_alltime: expanding median PSF — stable all-time anchor
    # floor_level_pct: floor / project_max_floor — normalises premium across heights
    if 'project' in df.columns:
        df['_project_key'] = df['project'].fillna('').astype(str).str.strip().str.upper()
        df['_unit_psf']    = (df['transacted_price'] / df['floor_area_sqft'].replace(0, np.nan)).clip(200, 8000)
        df['_time_key']    = df['year'] * 4 + df['quarter']

        proj_qtr = (
            df.groupby(['_project_key', '_time_key'])['_unit_psf']
            .mean()
            .reset_index(name='_proj_qtr_psf')
            .sort_values(['_project_key', '_time_key'])
        )
        proj_qtr['project_rolling_psf_6m'] = (
            proj_qtr.groupby('_project_key')['_proj_qtr_psf']
            .transform(lambda s: s.shift(1).rolling(8, min_periods=3).mean())
        )
        proj_qtr['project_median_psf_alltime'] = (
            proj_qtr.groupby('_project_key')['_proj_qtr_psf']
            .transform(lambda s: s.shift(1).expanding(min_periods=1).median())
        )
        df = df.merge(
            proj_qtr[['_project_key', '_time_key', 'project_rolling_psf_6m', 'project_median_psf_alltime']],
            on=['_project_key', '_time_key'],
            how='left',
        )
        # Fallback for new projects with no prior history: use district median PSF
        dist_psf = df.groupby('postal_district')['_unit_psf'].median()
        global_median = df['_unit_psf'].median()
        df['project_rolling_psf_6m'] = df['project_rolling_psf_6m'].fillna(
            df['postal_district'].map(dist_psf)
        ).fillna(global_median)
        df['project_median_psf_alltime'] = df['project_median_psf_alltime'].fillna(
            df['postal_district'].map(dist_psf)
        ).fillna(global_median)

        # floor_level_pct: normalise floor level by max floor in the project
        proj_max_floor = df.groupby('_project_key')['floor_level_num'].transform('max')
        df['floor_level_pct'] = (df['floor_level_num'] / proj_max_floor.where(proj_max_floor > 0, 1)).clip(0.0, 1.0)

        df.drop(columns=['_project_key', '_unit_psf', '_time_key'], inplace=True)
    else:
        # No project column: fall back to district PSF estimate
        df['_unit_psf'] = (df['transacted_price'] / df['floor_area_sqft'].replace(0, np.nan)).clip(200, 8000)
        dist_psf = df.groupby('postal_district')['_unit_psf'].median()
        df['project_rolling_psf_6m']      = df['postal_district'].map(dist_psf).fillna(df['_unit_psf'].median())
        df['project_median_psf_alltime']  = df['project_rolling_psf_6m']
        df['floor_level_pct']             = (df['floor_level_num'] / 20.0).clip(0.0, 1.0)
        df.drop(columns=['_unit_psf'], inplace=True)

    # District × property_type 24-month rolling PSF — fills gap for new/thin projects.
    # Sorted by district + time for correct rolling direction.
    df['_unit_psf_tmp'] = (df['transacted_price'] / df['floor_area_sqft'].replace(0, np.nan)).clip(200, 8000)
    df['_dist_type_key'] = df['postal_district'].astype(str) + '_' + df['property_type'].astype(str)
    df = df.sort_values(['_dist_type_key', 'month'])
    df['district_rolling_psf_24m'] = (
        df.groupby('_dist_type_key')['_unit_psf_tmp']
        .transform(lambda x: x.shift(1).rolling(8, min_periods=3).mean())
    )
    dist_type_median = df.groupby('_dist_type_key')['_unit_psf_tmp'].transform('median')
    df['district_rolling_psf_24m']    = df['district_rolling_psf_24m'].fillna(dist_type_median)
    df['district_median_psf_alltime'] = dist_type_median
    df.drop(columns=['_unit_psf_tmp', '_dist_type_key'], inplace=True)

    # storey_psf_interaction: floor premium × project price level
    df['storey_psf_interaction'] = df['floor_level_pct'] * df['project_rolling_psf_6m']

    # Cyclic quarter encoding for seasonality (Q1 Jan–Mar vs Q4 Oct–Dec differ in SG market)
    df['sin_quarter'] = np.sin(2 * np.pi * df['quarter'] / 4)
    df['cos_quarter'] = np.cos(2 * np.pi * df['quarter'] / 4)

    # ── Amenity distances via district centroid ───────────────────────────────
    # Private model has no per-project geocoding; district centroids give the
    # best available spatial anchor. Captures sub-segment differentiation that
    # postal_district-as-categorical cannot express numerically.
    ad = amenity_dict or {}
    from train_model import _MRT_STATIONS, _PRIMARY_SCHOOLS, _HAWKER_CENTRES
    school_coords  = ad.get('school', [])  or _PRIMARY_SCHOOLS
    hawker_coords  = ad.get('hawker', [])  or _HAWKER_CENTRES
    health_coords  = ad.get('health', [])
    park_coords    = ad.get('park',   [])

    def _dist_col(district):
        coords = _DISTRICT_CENTROIDS.get(str(district), _DISTRICT_CENTROIDS.get('D15', (1.305, 103.906)))
        return coords

    # Vectorise using district-level lookup (all rows in same district share centroid)
    dist_df = df['postal_district'].apply(_dist_col)
    lats = np.radians(np.array([c[0] for c in dist_df]))
    lons = np.radians(np.array([c[1] for c in dist_df]))

    def _vmin_dist(coords):
        if not coords:
            return np.full(len(lats), 1.0)
        arr   = np.array(coords)
        alats = np.radians(arr[:, 0])
        alons = np.radians(arr[:, 1])
        dlat  = alats[None, :] - lats[:, None]
        dlon  = alons[None, :] - lons[:, None]
        a     = np.sin(dlat/2)**2 + np.cos(lats[:,None])*np.cos(alats[None,:])*np.sin(dlon/2)**2
        return (6371.0 * 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))).min(axis=1).round(4)

    df['dist_nearest_mrt_km']    = _vmin_dist(_MRT_STATIONS)
    df['dist_nearest_school_km'] = _vmin_dist(school_coords)
    df['dist_nearest_hawker_km'] = _vmin_dist(hawker_coords)
    df['dist_nearest_health_km'] = _vmin_dist(health_coords) if health_coords else 1.0
    df['dist_nearest_park_km']   = _vmin_dist(park_coords)   if park_coords   else 0.5

    required = CATEGORICAL_COLS + ['floor_area_sqft', 'floor_level_num', 'floor_level_pct',
                                    'tenure_remaining_years', 'is_strata', 'year', 'quarter',
                                    'direction', 'severity', 'policy_impact',
                                    'months_since_policy_change', 'sora',
                                    'project_rolling_psf_6m', 'project_median_psf_alltime',
                                    'district_rolling_psf_24m', 'district_median_psf_alltime',
                                    'storey_psf_interaction', 'sin_quarter', 'cos_quarter',
                                    'dist_nearest_mrt_km', 'dist_nearest_school_km',
                                    'dist_nearest_hawker_km', 'dist_nearest_health_km',
                                    'dist_nearest_park_km',
                                    'transacted_price']
    return df.dropna(subset=[c for c in required if c != 'transacted_price'] + ['transacted_price'])


# ─── Model building ───────────────────────────────────────────────────────────

def _build_pipeline(model):
    """XGB/LGBM pipeline: OHE for categoricals."""
    preprocessor = ColumnTransformer(transformers=[
        ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), CATEGORICAL_COLS),
        ('num', 'passthrough', NUMERICAL_COLS),
    ])
    return Pipeline(steps=[('preprocessor', preprocessor), ('model', model)])


def _build_catboost_pipeline(model):
    """No preprocessing for CatBoost — it handles string categoricals natively.
    Pass the raw DataFrame directly; cat_features references column names."""
    return Pipeline(steps=[('model', model)])


# ─── Training ────────────────────────────────────────────────────────────────

def train(df_raw: pd.DataFrame):
    os.makedirs(MODELS_DIR, exist_ok=True)

    print('Loading policy, SORA, and amenity data...')
    policy_df    = load_policy_from_db()
    sora_df      = load_sora_from_db()
    amenity_dict = load_amenities_from_db()

    print('Engineering features...')
    df = engineer_features(df_raw, policy_df, sora_df, amenity_dict)

    # Remove outliers
    df = df[(df['transacted_price'] > 100_000) & (df['floor_area_sqft'] > 100)]
    df = df[df['transacted_price'] < 200_000_000]

    # Restrict to condo/apartment — exclude landed (terrace, semi-D, bungalow)
    before = len(df)
    df = df[df['property_type'].isin(CONDO_PROPERTY_TYPES)]
    landed_removed = before - len(df)

    # Cap ultra-luxury outliers (>S$10M): separate market, tiny sample, inflates MAE
    before2 = len(df)
    df = df[df['transacted_price'] <= 10_000_000]
    luxury_removed = before2 - len(df)

    print(f'After cleaning: {len(df):,} records ({landed_removed:,} landed + {luxury_removed:,} ultra-luxury removed)')

    time_idx_min = int(df['time_idx_raw'].min())
    df['time_idx'] = df['time_idx_raw'] - time_idx_min

    df[ALL_FEATURES + ['transacted_price']].to_csv(TEMP_PATH, index=False)

    # Chronological train/test split
    train_df = df[df['year'] < 2025]
    test_df  = df[df['year'] >= 2025]
    if len(test_df) < 50:
        df_s = df.sort_values('time_idx_raw')
        split = int(len(df_s) * 0.9)
        train_df, test_df = df_s.iloc[:split], df_s.iloc[split:]

    X_train = train_df[ALL_FEATURES]
    y_train = train_df['transacted_price']
    X_test  = test_df[ALL_FEATURES]
    y_test  = test_df['transacted_price']
    y_train_log = np.log(y_train)
    print(f'Train: {len(X_train):,} | Test: {len(X_test):,}')

    # Per-model feature subsets for genuine diversity.
    # XGB: keep project_rolling_psf_6m (strongest signal) but drop the rest of
    #   PSF hierarchy — forces XGB to use raw physical features + core project PSF.
    # LGBM: all features → primary workhorse.
    # CatBoost: exclude dynamic rolling PSF (district trend, seasonality) so it
    #   relies on static all-time anchors + categorical×lease interactions.
    _XGB_EXCLUDE = {
        'project_median_psf_alltime',
        'district_rolling_psf_24m', 'district_median_psf_alltime',
        'storey_psf_interaction',
    }
    _CAT_EXCLUDE = {
        'district_rolling_psf_24m',
        'sin_quarter', 'cos_quarter',
    }
    xgb_num  = [c for c in NUMERICAL_COLS if c not in _XGB_EXCLUDE]
    lgbm_num = NUMERICAL_COLS
    cat_num  = [c for c in NUMERICAL_COLS if c not in _CAT_EXCLUDE]

    def _make_pipeline_for(num_cols, model):
        pre = ColumnTransformer(transformers=[
            ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), CATEGORICAL_COLS),
            ('num', 'passthrough', num_cols),
        ])
        return Pipeline([('preprocessor', pre), ('model', model)])

    feature_subsets = {
        'xgb_private':  CATEGORICAL_COLS + xgb_num,
        'lgbm_private': CATEGORICAL_COLS + lgbm_num,
        'cat_private':  CATEGORICAL_COLS + cat_num,
    }

    model_specs = {
        'xgb_private':  XGBRegressor(
            n_estimators=800, learning_rate=0.02, max_depth=5,
            min_child_weight=10, reg_alpha=0.1, reg_lambda=2.0, gamma=0.05,
            subsample=0.8, colsample_bytree=0.8, tree_method='hist',
            random_state=42, objective='reg:squarederror', verbosity=0,
        ),
        'lgbm_private': LGBMRegressor(
            n_estimators=1200, learning_rate=0.02, num_leaves=127,
            min_child_samples=20,
            subsample=0.8, colsample_bytree=0.8, random_state=123, verbose=-1,
        ),
        'cat_private':  CatBoostRegressor(
            iterations=1000, learning_rate=0.025,
            grow_policy='Lossguide', max_leaves=64,
            min_data_in_leaf=20,
            loss_function='RMSE', random_seed=456, verbose=0,
            cat_features=CATEGORICAL_COLS,
        ),
    }

    # ── Phase 1: OOF predictions using per-model feature subsets ────────────
    print('\nPhase 1: Generating out-of-fold predictions for stacker...')
    kf = KFold(n_splits=3, shuffle=True, random_state=42)
    oof_preds = np.zeros((len(X_train), len(model_specs)))

    for i, (name, model) in enumerate(model_specs.items()):
        feats = feature_subsets[name]
        Xtr   = X_train[feats]
        print(f'  OOF for {name} ({len(feats)} features)...')
        if 'cat' in name:
            fold_preds = np.zeros(len(X_train))
            for train_idx, val_idx in kf.split(Xtr):
                fold_pipe = _build_catboost_pipeline(CatBoostRegressor(
                    iterations=1000, learning_rate=0.025,
                    grow_policy='Lossguide', max_leaves=64,
                    min_data_in_leaf=20,
                    loss_function='RMSE', random_seed=456, verbose=0,
                    cat_features=CATEGORICAL_COLS,
                ))
                fold_pipe.fit(Xtr.iloc[train_idx], y_train_log.iloc[train_idx])
                fold_preds[val_idx] = fold_pipe.predict(Xtr.iloc[val_idx])
            oof_preds[:, i] = fold_preds
        else:
            num_for_model = [c for c in feats if c not in CATEGORICAL_COLS]
            pipe = _make_pipeline_for(num_for_model, model)
            oof_preds[:, i] = cross_val_predict(pipe, Xtr, y_train_log, cv=kf)
        gc.collect()

    # ── Phase 2: Train final base models ─────────────────────────────────────
    print('\nPhase 2: Training final base models on full training set...')
    trained = {}
    for name, model in model_specs.items():
        feats = feature_subsets[name]
        Xtr   = X_train[feats]
        Xte   = X_test[feats]
        print(f'  Training {name}...')
        if 'cat' in name:
            pipeline = _build_catboost_pipeline(model)
        else:
            num_for_model = [c for c in feats if c not in CATEGORICAL_COLS]
            pipeline = _make_pipeline_for(num_for_model, model)
        pipeline.fit(Xtr, y_train_log)
        preds_exp = np.exp(pipeline.predict(Xte))
        mae = mean_absolute_error(y_test, preds_exp)
        r2  = r2_score(y_test, preds_exp)
        print(f'    {name}: MAE=S${mae:,.0f}  R²={r2:.4f}')
        trained[name] = pipeline
        joblib.dump(pipeline, os.path.join(MODELS_DIR, f'{name}_pipeline.joblib'))
        gc.collect()

    # ── Phase 3: Select best meta-learner ─────────────────────────────────────
    def _mape(y_true, y_pred):
        mask = y_true > 0
        return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100)

    test_log_preds = np.column_stack([
        trained[n].predict(X_test[feature_subsets[n]]) for n in model_specs
    ])
    simple_avg  = np.exp(np.mean(test_log_preds, axis=1))
    simple_mape = _mape(y_test.values, simple_avg)
    print(f'\nSimple avg: MAE=S${mean_absolute_error(y_test, simple_avg):,.0f}  '
          f'R²={r2_score(y_test, simple_avg):.4f}  MAPE={simple_mape:.2f}%')

    meta_candidates = {
        'ridge_0.1': Ridge(alpha=0.1),
        'ridge_1.0': Ridge(alpha=1.0),
        'ridge_10':  Ridge(alpha=10.0),
        'huber':     HuberRegressor(epsilon=1.35, alpha=0.0001, max_iter=300),
    }
    print('  Meta-learner comparison:')
    best_name, best_mape, stacker = 'equal', simple_mape, None
    for mname, meta in meta_candidates.items():
        meta.fit(oof_preds, y_train_log)
        mpred = np.exp(meta.predict(test_log_preds))
        mmape = _mape(y_test.values, mpred)
        print(f'    {mname}: MAPE={mmape:.4f}%  weights={[f"{w:.3f}" for w in meta.coef_]}')
        if mmape < best_mape:
            best_mape, best_name, stacker = mmape, mname, meta
    print(f'  → Best: {best_name}  (simple avg={simple_mape:.4f}%)')

    if stacker is None:
        n = len(model_specs)
        stacker = Ridge(alpha=1.0)
        stacker.fit(oof_preds, y_train_log)
        stacker.coef_      = np.full(n, 1.0 / n)
        stacker.intercept_ = 0.0
        stacked_preds = simple_avg
        print('  → Using equal weights (no meta-learner beat simple avg)')
    else:
        stacked_preds = np.exp(stacker.predict(test_log_preds))

    stacked_mae  = mean_absolute_error(y_test, stacked_preds)
    stacked_r2   = r2_score(y_test, stacked_preds)
    stacked_mape = _mape(y_test.values, stacked_preds)
    print(f'Stacked:    MAE=S${stacked_mae:,.0f}  R²={stacked_r2:.4f}  MAPE={stacked_mape:.2f}%')

    # ── Save meta ─────────────────────────────────────────────────────────────
    medians = (
        df.groupby(['postal_district', 'property_type'])[['floor_area_sqft', 'floor_level_num']]
        .median().to_dict('index')
    )

    latest_policy = {'direction': 0.0, 'severity': 0.0, 'policy_impact': 0.0,
                     'months_since_policy_change': 0}
    if policy_df is not None and len(policy_df) > 0:
        pol = policy_df.copy()
        pol['effective_month'] = pd.to_datetime(pol['effective_month'], errors='coerce')
        pol = pol.sort_values('effective_month').iloc[-1]
        d, s = float(pol.get('direction', 0) or 0), float(pol.get('severity', 0) or 0)
        latest_policy = {'direction': d, 'severity': s, 'policy_impact': d * s,
                         'months_since_policy_change': 0}

    latest_sora = 3.5
    if sora_df is not None and len(sora_df) > 0:
        latest_sora = float(sora_df.sort_values('month').iloc[-1]['sora'])

    meta = {
        'time_idx_min':         time_idx_min,
        'medians_by_dist_type': medians,
        'categorical_cols':     CATEGORICAL_COLS,
        'numerical_cols':       NUMERICAL_COLS,
        'latest_policy':        latest_policy,
        'latest_sora':          latest_sora,
        'stacker_coef':          stacker.coef_.tolist(),
        'stacker_intercept':     float(stacker.intercept_),
        'model_names':           list(model_specs.keys()),
        'model_feature_subsets': {n: feature_subsets[n] for n in model_specs},
        'trained_at':           datetime.now(timezone.utc).isoformat(),
        'eval_mae':             round(stacked_mae, 0),
        'eval_r2':              round(stacked_r2, 4),
        'eval_mape':            round(stacked_mape, 2),
        'eval_n_test':          len(y_test),
    }

    joblib.dump(meta, os.path.join(MODELS_DIR, 'meta_private.joblib'))
    print('\nAll private property models saved to', MODELS_DIR)

    # ── Phase 4: SHAP metadata for XAI ───────────────────────────────────────
    # Save only plain-Python metadata — TreeExplainer is reconstructed at
    # inference time from the already-loaded xgb_private_pipeline.joblib.
    print("Computing SHAP metadata (private XGB)...")
    try:
        import shap
        xgb_pipe            = trained['xgb_private']
        preprocessor_fitted = xgb_pipe.named_steps['preprocessor']
        xgb_model           = xgb_pipe.named_steps['model']
        feature_names_out   = preprocessor_fitted.get_feature_names_out().tolist()

        X_sample  = preprocessor_fitted.transform(X_test[feature_subsets['xgb_private']].iloc[:100])
        explainer = shap.TreeExplainer(xgb_model)
        explainer.shap_values(X_sample)   # validates the explainer works

        base_val = explainer.expected_value
        if hasattr(base_val, '__len__'):
            base_val = float(base_val[0])
        else:
            base_val = float(base_val)

        shap_meta = {
            'feature_names':    feature_names_out,
            'categorical_cols': CATEGORICAL_COLS,
            'numerical_cols':   NUMERICAL_COLS,
            'base_value':       base_val,
        }
        joblib.dump(shap_meta, os.path.join(MODELS_DIR, 'shap_private.joblib'))
        print(f"  shap_private.joblib saved  (base_value={base_val:.4f}, features={len(feature_names_out)})")
    except Exception as e:
        import traceback
        print(f"  SHAP metadata skipped: {e}")
        traceback.print_exc()

    if os.path.exists(TEMP_PATH):
        os.remove(TEMP_PATH)


if __name__ == '__main__':
    from_db = '--from-db' in sys.argv or bool(DATABASE_URL)

    df_raw = None

    if from_db:
        print('Loading from ura_transactions database table...')
        try:
            df_raw = load_from_db()
            print(f'Loaded {len(df_raw):,} records from DB')
        except ValueError as e:
            print(f'  DB load failed: {e}')
            df_raw = None

    if df_raw is None or len(df_raw) == 0:
        key = ACCESS_KEY
        if not key:
            print('ERROR: ura_transactions table is empty and URA_ACCESS_KEY is not set.')
            sys.exit(1)
        print('Downloading from URA API...')
        df_raw = download_from_ura_api(key)
        print(f'Downloaded {len(df_raw):,} records')

    train(df_raw)
