#!/usr/bin/env python3
"""
Train private property (condo/apartment/landed) price prediction model.

Two data sources supported:
  1. URA API direct download (set URA_ACCESS_KEY env var)
  2. Uploaded ura_transactions table in the database (--from-db flag)

URA CSV column format (from ura_transactions table):
  Project Name, Transacted Price ($), Area (SQFT), Unit Price ($ PSF),
  Sale Date, Street Name, Type of Sale, Type of Area, Area (SQM),
  Unit Price ($ PSM), Nett Price($), Property Type, Number of Units,
  Tenure, Postal District, Market Segment, Floor Level

Usage:
    URA_ACCESS_KEY=<key> python train_model_private.py          # from URA API
    DATABASE_URL=<url>   python train_model_private.py --from-db # from Supabase

Models saved to ./models/ as:
    xgb_private_pipeline.joblib
    lgbm_private_pipeline.joblib
    cat_private_pipeline.joblib
    meta_private.joblib
"""
import os
import re
import sys
import joblib
import requests
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import mean_absolute_error, r2_score
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from catboost import CatBoostRegressor

MODELS_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
URA_BASE    = 'https://eservice.ura.gov.sg/uraDataService'
ACCESS_KEY  = os.environ.get('URA_ACCESS_KEY', '')
DATABASE_URL = os.environ.get('DATABASE_URL', '')

TEMP_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'temp_progress_private.csv')

# Features used by the model (matches HDB approach with policy + SORA)
CATEGORICAL_COLS = ['property_type', 'market_segment', 'type_of_sale', 'postal_district']
NUMERICAL_COLS   = [
    'floor_area_sqft', 'floor_level_num', 'year', 'quarter', 'time_idx',
    'direction', 'severity', 'policy_impact', 'months_since_policy_change', 'sora',
]
ALL_FEATURES = CATEGORICAL_COLS + NUMERICAL_COLS

_TYPE_OF_SALE_API = {'1': 'New Sale', '2': 'Sub Sale', '3': 'Resale'}


# ─── Floor level helpers ─────────────────────────────────────────────────────

def _parse_floor_level(fl_raw) -> float:
    """Parse URA floor level strings to a numeric value."""
    fl = str(fl_raw or '').strip().upper()
    # Basement
    if fl.startswith('B'):
        return 0.0
    # Range like "01 TO 05" or "01-05"
    nums = re.findall(r'\d+', fl)
    if len(nums) >= 2:
        return (int(nums[0]) + int(nums[1])) / 2
    if len(nums) == 1:
        return float(nums[0])
    # Text descriptors (from API)
    fl_lower = fl.lower()
    if 'low' in fl_lower:  return 4.0
    if 'mid' in fl_lower:  return 13.0
    if 'high' in fl_lower: return 25.0
    return 10.0


def _parse_sale_date(raw) -> tuple:
    """Return (year, quarter) from sale date strings like 'Jan-25', '2025-01', '01/2025'."""
    raw = str(raw or '').strip()
    # Try "Jan-25" / "Jan '25"
    m = re.match(r'([A-Za-z]{3})[\-\s\'](\d{2,4})', raw)
    if m:
        month_str, yr = m.group(1), m.group(2)
        month = datetime.strptime(month_str, '%b').month
        year  = 2000 + int(yr) if len(yr) == 2 else int(yr)
        return year, (month - 1) // 3 + 1
    # Try "2025-01" or "01/2025"
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
        rows = _query("SELECT rate_date, published_rate FROM sora_rates WHERE rate_date IS NOT NULL")
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df['date']   = pd.to_datetime(df['rate_date'], errors='coerce')
        df['sora_3m'] = pd.to_numeric(df['published_rate'], errors='coerce')
        df = df.dropna(subset=['date', 'sora_3m'])
        df['month'] = df['date'].dt.to_period('M').dt.to_timestamp()
        return df.groupby('month', as_index=False)['sora_3m'].mean().rename(columns={'sora_3m': 'sora'})
    except Exception as e:
        print(f"  sora_rates load error: {e}")
        return None


# ─── URA API download ────────────────────────────────────────────────────────

def _get_ura_token(access_key: str) -> str:
    r = requests.get(
        f'{URA_BASE}/insertNewToken.action',
        headers={'AccessKey': access_key},
        timeout=30,
    )
    if r.status_code != 200:
        raise ValueError(f'Token request HTTP {r.status_code}:\n{r.text[:300]}')
    try:
        data = r.json()
    except Exception:
        raise ValueError(f'Token response not JSON:\n{r.text[:300]}')
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
                rows.append({
                    'property_type':   det.get('propertyType', ''),
                    'market_segment':  market_seg,
                    'type_of_sale':    _TYPE_OF_SALE_API.get(str(det.get('typeOfSale', '3')), 'Resale'),
                    'postal_district': str(det.get('district', '0')).zfill(2),
                    'floor_area_sqft': float(det.get('area') or 0),
                    'floor_level_num': _parse_floor_level(det.get('floorRange') or det.get('floorLevel')),
                    'year':            year,
                    'quarter':         quarter,
                    'sale_date':       f'{year}-{mo:02d}',
                    'transacted_price': float(det.get('price') or 0),
                })
        print(f'{len(rows):,} so far')
    return pd.DataFrame(rows)


# ─── Load from uploaded DB table ─────────────────────────────────────────────

def load_from_db() -> pd.DataFrame:
    """Read ura_transactions table and return a flat DataFrame."""
    if DATABASE_URL:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    else:
        import sqlite3
        DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'propaisg.db')
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

    cur.execute("SELECT * FROM ura_transactions")
    rows_raw = [dict(r) for r in cur.fetchall()]
    conn.close()

    if not rows_raw:
        raise ValueError('ura_transactions table is empty — upload CSV data first.')

    rows = []
    for r in rows_raw:
        year, quarter = _parse_sale_date(r.get('sale_date'))
        if year is None:
            continue
        rows.append({
            'property_type':    str(r.get('property_type') or '').strip().upper(),
            'market_segment':   str(r.get('market_segment') or '').strip().upper(),
            'type_of_sale':     str(r.get('type_of_sale') or 'Resale').strip(),
            'postal_district':  str(r.get('postal_district') or '0').strip().zfill(2),
            'floor_area_sqft':  float(r.get('floor_area_sqft') or 0),
            'floor_level_num':  _parse_floor_level(r.get('floor_level')),
            'year':             year,
            'quarter':          quarter,
            'transacted_price': float(r.get('transacted_price') or 0),
        })
    return pd.DataFrame(rows)


# ─── Feature engineering ─────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame, policy_df=None, sora_df=None) -> pd.DataFrame:
    df = df.copy()
    for col in ['property_type', 'market_segment', 'type_of_sale']:
        df[col] = df[col].astype(str).str.strip().str.upper().str.replace(r'\s+', ' ', regex=True)
    df['postal_district'] = df['postal_district'].astype(str).str.strip().str.zfill(2)
    df['floor_area_sqft'] = pd.to_numeric(df['floor_area_sqft'], errors='coerce')
    df['floor_level_num'] = pd.to_numeric(df['floor_level_num'], errors='coerce').fillna(10)
    df['transacted_price'] = pd.to_numeric(df['transacted_price'], errors='coerce')
    df['time_idx_raw']    = df['year'] * 12 + df['quarter'] * 3

    # Build month column for policy/SORA merge (year + quarter → approx month)
    df['month'] = pd.to_datetime(
        df['year'].astype(str) + '-' + ((df['quarter'] - 1) * 3 + 1).astype(str).str.zfill(2) + '-01'
    )

    # Policy merge
    if policy_df is not None and len(policy_df) > 0:
        pol = policy_df.copy()
        pol['effective_month'] = pd.to_datetime(pol['effective_month'], errors='coerce')
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
        df = df.merge(sora_df, on='month', how='left')
        df['sora'] = df['sora'].fillna(df['sora'].median())
    else:
        df['sora'] = 0.0

    base_cols = CATEGORICAL_COLS + ['floor_area_sqft', 'floor_level_num', 'year', 'quarter',
                                     'direction', 'severity', 'policy_impact',
                                     'months_since_policy_change', 'sora', 'transacted_price']
    return df.dropna(subset=[c for c in base_cols if c != 'transacted_price'] + ['transacted_price'])


# ─── Training ────────────────────────────────────────────────────────────────

def _build_pipeline(model):
    preprocessor = ColumnTransformer(transformers=[
        ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), CATEGORICAL_COLS),
        ('num', 'passthrough', NUMERICAL_COLS),
    ])
    return Pipeline(steps=[('preprocessor', preprocessor), ('model', model)])


def train(df_raw: pd.DataFrame):
    os.makedirs(MODELS_DIR, exist_ok=True)

    print('Loading policy and SORA data...')
    policy_df = load_policy_from_db()
    sora_df   = load_sora_from_db()

    print('Engineering features...')
    df = engineer_features(df_raw, policy_df, sora_df)

    # Remove outliers
    df = df[(df['transacted_price'] > 50_000) & (df['floor_area_sqft'] > 10)]
    df = df[df['transacted_price'] < 200_000_000]
    print(f'After cleaning: {len(df):,} records')

    # Checkpoint
    print(f'Saving temp checkpoint to {TEMP_PATH}...')
    df[ALL_FEATURES + ['transacted_price']].to_csv(TEMP_PATH, index=False)

    time_idx_min = int(df['time_idx_raw'].min())
    df['time_idx'] = df['time_idx_raw'] - time_idx_min

    # Train/test split
    train_df = df[df['year'] < 2025]
    test_df  = df[df['year'] >= 2025]
    if len(test_df) < 50:
        split   = int(len(df) * 0.9)
        df_s    = df.sort_values('time_idx_raw')
        train_df, test_df = df_s.iloc[:split], df_s.iloc[split:]

    X_train, y_train = train_df[ALL_FEATURES], train_df['transacted_price']
    X_test,  y_test  = test_df[ALL_FEATURES],  test_df['transacted_price']
    print(f'Train: {len(X_train):,} | Test: {len(X_test):,}')

    y_train_log = np.log(y_train)

    model_specs = {
        'xgb_private':  XGBRegressor(n_estimators=200, learning_rate=0.05, max_depth=6,
                                       subsample=0.8, colsample_bytree=0.8, random_state=42,
                                       objective='reg:squarederror'),
        'lgbm_private': LGBMRegressor(n_estimators=200, learning_rate=0.05, num_leaves=31,
                                        subsample=0.8, colsample_bytree=0.8, random_state=42,
                                        verbose=-1),
        'cat_private':  CatBoostRegressor(iterations=200, learning_rate=0.05, depth=6,
                                            loss_function='RMSE', random_seed=42, verbose=0),
    }

    trained = {}
    for name, model in model_specs.items():
        print(f'Training {name}...')
        pipeline = _build_pipeline(model)
        pipeline.fit(X_train, y_train_log)
        preds = np.exp(pipeline.predict(X_test))
        mae   = mean_absolute_error(y_test, preds)
        r2    = r2_score(y_test, preds)
        print(f'  {name}: MAE=S${mae:,.0f}  R²={r2:.4f}')
        trained[name] = pipeline
        # Checkpoint after each model
        joblib.dump(pipeline, os.path.join(MODELS_DIR, f'{name}_pipeline.joblib'))

    ens = np.mean([np.exp(p.predict(X_test)) for p in trained.values()], axis=0)
    print(f'Ensemble: MAE=S${mean_absolute_error(y_test, ens):,.0f}  R²={r2_score(y_test, ens):.4f}')

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
        'trained_at':           datetime.utcnow().isoformat(),
    }

    joblib.dump(meta, os.path.join(MODELS_DIR, 'meta_private.joblib'))
    print('All private property models saved to', MODELS_DIR)

    if os.path.exists(TEMP_PATH):
        os.remove(TEMP_PATH)
        print('Temp checkpoint removed.')


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
            print('  → Upload URA transaction data via the admin panel, then redeploy.')
            print('  → Or set URA_ACCESS_KEY in the Render dashboard to download from URA API.')
            sys.exit(1)
        print('Downloading from URA API (ura_transactions was empty or unavailable)...')
        df_raw = download_from_ura_api(key)
        print(f'Downloaded {len(df_raw):,} records')

    train(df_raw)
