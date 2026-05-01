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
import joblib
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import HuberRegressor
from sklearn.model_selection import KFold, cross_val_predict
from sklearn.metrics import mean_absolute_error, r2_score
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from catboost import CatBoostRegressor

MODELS_DIR   = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
URA_BASE     = 'https://eservice.ura.gov.sg/uraDataService'
ACCESS_KEY   = os.environ.get('URA_ACCESS_KEY', '')
DATABASE_URL = os.environ.get('DATABASE_URL', '')
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
    'floor_area_sqft', 'floor_level_num',
    'tenure_remaining_years', 'is_strata',
    'year', 'quarter', 'time_idx',
    'direction', 'severity', 'policy_impact', 'months_since_policy_change', 'sora',
    'project_rolling_psf_6m',   # avg PSF for this project in prior 6 months (no leakage)
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
        rows = _query("SELECT publication_date, compound_sora_3m FROM sora_rates WHERE publication_date IS NOT NULL")
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df['date']    = pd.to_datetime(df['publication_date'], errors='coerce')
        df['sora_3m'] = pd.to_numeric(df['compound_sora_3m'], errors='coerce')
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

def engineer_features(df: pd.DataFrame, policy_df=None, sora_df=None) -> pd.DataFrame:
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

    # ── Project-level rolling PSF (24-month lookback, no data leakage) ──────────
    # For each transaction: mean PSF of the same project in the prior 8 quarters.
    # min_periods=3 ensures at least 3 historical quarters before trusting the
    # project average — fewer than that falls back to the district median.
    # Shift(1) on the quarter index excludes the current quarter → no leakage.
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
        df = df.merge(
            proj_qtr[['_project_key', '_time_key', 'project_rolling_psf_6m']],
            left_on=['_project_key', '_time_key'], right_on=['_project_key', '_time_key'],
            how='left',
        )
        # Fallback for new projects with no prior history: use district median PSF
        dist_psf = df.groupby('postal_district')['_unit_psf'].median()
        df['project_rolling_psf_6m'] = df['project_rolling_psf_6m'].fillna(
            df['postal_district'].map(dist_psf)
        ).fillna(df['_unit_psf'].median())
        df.drop(columns=['_project_key', '_unit_psf', '_time_key'], inplace=True)
    else:
        # No project column: fall back to district PSF estimate
        df['_unit_psf'] = (df['transacted_price'] / df['floor_area_sqft'].replace(0, np.nan)).clip(200, 8000)
        dist_psf = df.groupby('postal_district')['_unit_psf'].median()
        df['project_rolling_psf_6m'] = df['postal_district'].map(dist_psf).fillna(df['_unit_psf'].median())
        df.drop(columns=['_unit_psf'], inplace=True)

    required = CATEGORICAL_COLS + ['floor_area_sqft', 'floor_level_num', 'tenure_remaining_years',
                                    'is_strata', 'year', 'quarter', 'direction', 'severity',
                                    'policy_impact', 'months_since_policy_change', 'sora',
                                    'project_rolling_psf_6m', 'transacted_price']
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

    print('Loading policy and SORA data...')
    policy_df = load_policy_from_db()
    sora_df   = load_sora_from_db()

    print('Engineering features...')
    df = engineer_features(df_raw, policy_df, sora_df)

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

    model_specs = {
        'xgb_private':  XGBRegressor(
            n_estimators=200, learning_rate=0.05, max_depth=6,
            subsample=0.8, colsample_bytree=0.8, tree_method='hist',
            random_state=42, objective='reg:squarederror', verbosity=0,
        ),
        'lgbm_private': LGBMRegressor(
            n_estimators=200, learning_rate=0.05, num_leaves=31,
            subsample=0.8, colsample_bytree=0.8, random_state=42, verbose=-1,
        ),
        'cat_private':  CatBoostRegressor(
            iterations=200, learning_rate=0.05, depth=6,
            loss_function='RMSE', random_seed=42, verbose=0,
            cat_features=CATEGORICAL_COLS,
        ),
    }

    # ── Phase 1: Out-of-fold predictions for meta-learner ────────────────────
    print('\nPhase 1: Generating out-of-fold predictions for stacker...')
    kf = KFold(n_splits=5, shuffle=True, random_state=42)
    oof_preds = np.zeros((len(X_train), len(model_specs)))

    for i, (name, model) in enumerate(model_specs.items()):
        print(f'  OOF for {name}...')
        if 'cat' in name:
            # CatBoost doesn't support sklearn.clone() with cat_features — run OOF manually
            fold_preds = np.zeros(len(X_train))
            for train_idx, val_idx in kf.split(X_train):
                X_tr  = X_train.iloc[train_idx]
                X_val = X_train.iloc[val_idx]
                y_tr  = y_train_log.iloc[train_idx]
                fold_pipe = _build_catboost_pipeline(CatBoostRegressor(
                    iterations=200, learning_rate=0.05, depth=6,
                    loss_function='RMSE', random_seed=42, verbose=0,
                    cat_features=CATEGORICAL_COLS,
                ))
                fold_pipe.fit(X_tr, y_tr)
                fold_preds[val_idx] = fold_pipe.predict(X_val)
            oof_preds[:, i] = fold_preds
        else:
            pipe = _build_pipeline(model)
            oof_preds[:, i] = cross_val_predict(pipe, X_train, y_train_log, cv=kf)
        gc.collect()

    # HuberRegressor meta-learner: robust to price outliers in OOF predictions.
    # Clip any negative weights to zero after fitting — a base model should only
    # contribute positively to the blend, never subtract from it.
    stacker = HuberRegressor(epsilon=1.35, alpha=0.0001, max_iter=300)
    stacker.fit(oof_preds, y_train_log)
    stacker.coef_ = np.maximum(stacker.coef_, 0)
    stacker_weights = stacker.coef_.tolist()
    stacker_intercept = float(stacker.intercept_)
    print(f'  Stacker weights: {[f"{w:.3f}" for w in stacker_weights]}  intercept={stacker_intercept:.4f}')

    # ── Phase 2: Train final base models on full training data ───────────────
    print('\nPhase 2: Training final base models on full training set...')
    trained = {}
    for name, model in model_specs.items():
        print(f'  Training {name}...')
        pipeline = _build_catboost_pipeline(model) if 'cat' in name else _build_pipeline(model)
        pipeline.fit(X_train, y_train_log)

        # Evaluate with stacker
        test_pred_log = pipeline.predict(X_test)
        preds_exp = np.exp(test_pred_log)
        mae = mean_absolute_error(y_test, preds_exp)
        r2  = r2_score(y_test, preds_exp)
        print(f'    {name}: MAE=S${mae:,.0f}  R²={r2:.4f}')
        trained[name] = pipeline
        joblib.dump(pipeline, os.path.join(MODELS_DIR, f'{name}_pipeline.joblib'))
        gc.collect()

    # ── Phase 3: Evaluate stacked ensemble ───────────────────────────────────
    test_log_preds = np.column_stack([p.predict(X_test) for p in trained.values()])
    stacked_log = stacker.predict(test_log_preds)
    stacked_preds = np.exp(stacked_log)
    simple_avg    = np.exp(np.mean(test_log_preds, axis=1))

    def _mape(y_true, y_pred):
        mask = y_true > 0
        return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100)

    stacked_mae  = mean_absolute_error(y_test, stacked_preds)
    stacked_r2   = r2_score(y_test, stacked_preds)
    stacked_mape = _mape(y_test.values, stacked_preds)
    print(f'\nSimple avg: MAE=S${mean_absolute_error(y_test, simple_avg):,.0f}  R²={r2_score(y_test, simple_avg):.4f}  MAPE={_mape(y_test.values, simple_avg):.2f}%')
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
        'stacker_coef':         stacker_weights,
        'stacker_intercept':    stacker_intercept,
        'model_names':          list(model_specs.keys()),
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

        X_sample  = preprocessor_fitted.transform(X_test.iloc[:100])
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
