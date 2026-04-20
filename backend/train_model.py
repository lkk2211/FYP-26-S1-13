#!/usr/bin/env python3
"""
Train HDB resale price prediction model with policy + SORA + geocoding features.

Data sources (loaded from Supabase DB tables):
  - resale_flat_prices         — HDB transaction records (uploaded CSV)
  - policy_changes     — Government policy changes (effective_month, direction, severity)
  - sora_rates         — SORA 3-month compound rates
  - geocoded_addresses — Block+street → lat/lon mapping

Falls back to data.gov.sg API for transactions if resale_flat_prices is empty.
A temp checkpoint is saved after feature engineering and after each model
in case training is interrupted.

Usage:
    python train_model.py            # auto (DB first, then API)
    python train_model.py --from-db  # force DB mode
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
from sklearn.linear_model import HuberRegressor
from sklearn.model_selection import KFold, cross_val_predict
from sklearn.metrics import mean_absolute_error, r2_score
import gc
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from catboost import CatBoostRegressor

MODELS_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
TEMP_PATH   = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'temp_progress.csv')
RESOURCE_ID = 'f1765b54-a209-4718-8d38-a39237f502b3'
MIN_YEAR    = 2021

CATEGORICAL_COLS = ["town", "flat_type", "flat_model"]
# Full feature set matching the notebook (lat/lon + policy + SORA)
NUMERICAL_COLS_FULL = [
    "floor_area_sqm",
    "direction",
    "severity",
    "policy_impact",
    "months_since_policy_change",
    "sora",
    "year",
    "quarter",
    "time_idx",
    "storey_mid",
    "remaining_lease_years",
    "flat_age_years",
    "lat",
    "lon",
]
# Minimal fallback (no geo, no policy, no SORA — same as original model)
NUMERICAL_COLS_MIN = [
    "floor_area_sqm",
    "year",
    "quarter",
    "time_idx",
    "storey_mid",
    "remaining_lease_years",
    "flat_age_years",
]


# ─── DB connection helper ─────────────────────────────────────────────────────

def _get_db_conn():
    url = os.environ.get('DATABASE_URL', '')
    if url:
        import psycopg2, psycopg2.extras
        return psycopg2.connect(url), 'postgres'
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'propaisg.db')
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn, 'sqlite'


def _query(sql, params=()):
    conn, kind = _get_db_conn()
    if kind == 'postgres':
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql.replace('?', '%s'), params)
    else:
        cur = conn.cursor()
        cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


# ─── Data loaders ─────────────────────────────────────────────────────────────

def download_hdb_data():
    """Paginate through data.gov.sg datastore API."""
    base_url = 'https://data.gov.sg/api/action/datastore_search'
    all_records, limit, offset, total = [], 10000, 0, None
    while True:
        r = requests.get(base_url, params={
            'resource_id': RESOURCE_ID, 'limit': limit, 'offset': offset,
        }, timeout=60)
        r.raise_for_status()
        result = r.json()['result']
        if total is None:
            total = result['total']
        records = result['records']
        all_records.extend(records)
        print(f"  {len(all_records):,} / {total:,} records", end='\r')
        if len(records) < limit:
            break
        offset += limit
    print()
    return pd.DataFrame(all_records)


def load_hdb_from_db():
    try:
        rows = _query(f"""
            SELECT month, town, flat_type, flat_model, floor_area_sqm,
                   storey_range, resale_price, remaining_lease, lease_commence_date,
                   block, street_name
            FROM resale_flat_prices
            WHERE month >= '{MIN_YEAR}-01'
        """)
        if not rows:
            return None
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"  resale_flat_prices load error: {e}")
        return None


def load_policy_from_db():
    try:
        rows = _query("""
            SELECT effective_month, policy_name, category, direction, severity
            FROM policy_changes
            WHERE effective_month IS NOT NULL
        """)
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
        df['date'] = pd.to_datetime(df['publication_date'], errors='coerce')
        df['sora_3m'] = pd.to_numeric(df['compound_sora_3m'], errors='coerce')
        df = df.dropna(subset=['date', 'sora_3m'])
        df['month'] = df['date'].dt.to_period('M').dt.to_timestamp().astype('datetime64[s]')
        return df.groupby('month', as_index=False)['sora_3m'].mean().rename(columns={'sora_3m': 'sora'})
    except Exception as e:
        print(f"  sora_rates load error: {e}")
        return None


def load_geocoded_from_db():
    try:
        rows = _query("""
            SELECT search_text, lat, lon FROM geocoded_addresses
            WHERE search_text IS NOT NULL AND lat IS NOT NULL AND lon IS NOT NULL
        """)
        if not rows:
            return None
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"  geocoded_addresses load error: {e}")
        return None


# ─── Feature engineering ──────────────────────────────────────────────────────

def _storey_mid(storey_range):
    try:
        lo, hi = str(storey_range).upper().strip().split(' TO ')
        return (float(lo) + float(hi)) / 2
    except Exception:
        return 8.0


def _remaining_lease_years(rl):
    rl = str(rl).upper().strip()
    y = re.search(r'(\d+)\s*YEAR', rl)
    m = re.search(r'(\d+)\s*MONTH', rl)
    try:
        years = float(y.group(1)) if y else float(rl)
    except Exception:
        years = 65.0
    months = float(m.group(1)) if m else 0.0
    return years + months / 12


def engineer_features(df, policy_df, sora_df, geo_df):
    """Full feature engineering matching the notebook."""
    df = df.copy()

    # Month / time features
    df['month'] = pd.to_datetime(df['month'].astype(str).str[:7] + '-01').astype('datetime64[s]')
    df['year']  = df['month'].dt.year
    df['quarter'] = df['month'].dt.quarter
    df['time_idx_raw'] = df['year'] * 12 + df['month'].dt.month

    # Storey
    df['storey_mid'] = df['storey_range'].apply(_storey_mid)

    # Lease
    df['remaining_lease_years'] = df['remaining_lease'].apply(_remaining_lease_years)
    df['lease_commence_date']   = pd.to_numeric(df['lease_commence_date'], errors='coerce')
    df['flat_age_years']        = df['year'] - df['lease_commence_date']

    # Numeric price/area
    df['floor_area_sqm'] = pd.to_numeric(df['floor_area_sqm'], errors='coerce')
    df['resale_price']   = pd.to_numeric(df['resale_price'],   errors='coerce')

    # String normalization
    for col in ['town', 'flat_type', 'flat_model']:
        df[col] = df[col].astype(str).str.strip().str.upper().str.replace(r'\s+', ' ', regex=True)

    # ── Policy merge (merge_asof backward) ──────────────────────────────────
    if policy_df is not None and len(policy_df) > 0:
        pol = policy_df.copy()
        pol['effective_month'] = pd.to_datetime(pol['effective_month'], errors='coerce').astype('datetime64[s]')
        pol['direction'] = pd.to_numeric(pol['direction'], errors='coerce').fillna(0)
        pol['severity']  = pd.to_numeric(pol['severity'],  errors='coerce').fillna(0)
        pol = pol.dropna(subset=['effective_month']).sort_values('effective_month')
        df  = df.sort_values('month')
        df  = pd.merge_asof(
            df,
            pol[['effective_month', 'policy_name', 'category', 'direction', 'severity']],
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

    # ── SORA merge ──────────────────────────────────────────────────────────
    if sora_df is not None and len(sora_df) > 0:
        df = df.merge(sora_df, on='month', how='left')
        df['sora'] = df['sora'].fillna(df['sora'].median())
    else:
        df['sora'] = 0.0

    # ── Geocoding merge ─────────────────────────────────────────────────────
    has_geo = False
    if geo_df is not None and len(geo_df) > 0:
        if 'block' in df.columns and 'street_name' in df.columns:
            df['search_text'] = (
                df['block'].fillna('').astype(str) + ' ' +
                df['street_name'].fillna('').astype(str)
            ).str.strip().str.upper()
        else:
            df['search_text'] = df.get('search_text', '').astype(str).str.strip().str.upper()

        geo_clean = geo_df[['search_text', 'lat', 'lon']].dropna()
        df = df.merge(geo_clean, on='search_text', how='left')
        df = df.dropna(subset=['lat', 'lon'])
        has_geo = len(df) > 0
        if not has_geo:
            print("  Warning: no lat/lon matches after geo merge; dropping geo features")
    if not has_geo:
        df['lat'] = 0.0
        df['lon'] = 0.0

    return df, has_geo


# ─── Training ─────────────────────────────────────────────────────────────────

def train(from_db=False):
    os.makedirs(MODELS_DIR, exist_ok=True)

    # 1. Load HDB transactions
    df = None
    if from_db or os.environ.get('DATABASE_URL'):
        print("Loading HDB resale data from database...")
        df = load_hdb_from_db()
        if df is not None and len(df) > 0:
            print(f"  Loaded {len(df):,} records from resale_flat_prices table")
        else:
            df = None
            if from_db:
                raise ValueError("resale_flat_prices table is empty — upload CSV first.")
            print("  resale_flat_prices empty, falling back to data.gov.sg API...")
    if df is None:
        print("Downloading HDB resale data from data.gov.sg...")
        df = download_hdb_data()
    print(f"Raw records: {len(df):,}")

    # 2. Load supplementary datasets
    print("Loading policy, SORA, and geocoding data from database...")
    policy_df = load_policy_from_db()
    sora_df   = load_sora_from_db()
    geo_df    = load_geocoded_from_db()
    print(f"  Policy rows: {len(policy_df) if policy_df is not None else 0}")
    print(f"  SORA rows:   {len(sora_df)   if sora_df   is not None else 0}")
    print(f"  Geo rows:    {len(geo_df)    if geo_df    is not None else 0}")

    # 3. Feature engineering
    print("Engineering features...")
    df_feat, has_geo = engineer_features(df, policy_df, sora_df, geo_df)
    df_feat = df_feat[df_feat['year'] >= MIN_YEAR].copy()

    # Determine which numerical cols are actually available
    has_policy = policy_df is not None and len(policy_df) > 0
    has_sora   = sora_df   is not None and len(sora_df)   > 0
    actual_num = [c for c in NUMERICAL_COLS_FULL if c in df_feat.columns and
                  not (c in ('lat', 'lon') and not has_geo) and
                  not (c in ('direction', 'severity', 'policy_impact',
                             'months_since_policy_change') and not has_policy) and
                  not (c == 'sora' and not has_sora)]
    if not has_geo:
        actual_num = [c for c in actual_num if c not in ('lat', 'lon')]
    all_features = CATEGORICAL_COLS + actual_num
    print(f"After filtering: {len(df_feat):,} records | Features: {len(all_features)}")

    # Checkpoint: save feature-engineered data in case training is interrupted
    print(f"Saving checkpoint to {TEMP_PATH}...")
    ckpt_cols = [c for c in all_features + ['resale_price', 'time_idx_raw']
                 if c in df_feat.columns]
    df_feat[ckpt_cols].to_csv(TEMP_PATH, index=False)

    # time_idx normalization
    time_idx_min = int(df_feat['time_idx_raw'].min())
    df_feat['time_idx'] = df_feat['time_idx_raw'] - time_idx_min

    # 4. Train / test split
    train_df = df_feat[df_feat['year'] < 2025]
    test_df  = df_feat[df_feat['year'] >= 2025]
    if len(test_df) < 100:
        split    = int(len(df_feat) * 0.9)
        df_s     = df_feat.sort_values('time_idx_raw')
        train_df, test_df = df_s.iloc[:split], df_s.iloc[split:]
    X_train, y_train = train_df[all_features], train_df['resale_price']
    X_test,  y_test  = test_df[all_features],  test_df['resale_price']
    print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")

    y_train_log = np.log(y_train)

    # 5. Build preprocessor
    preprocessor = ColumnTransformer(transformers=[
        ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), CATEGORICAL_COLS),
        ('num', 'passthrough', actual_num),
    ])

    model_specs = {
        'xgb':  XGBRegressor(n_estimators=200, learning_rate=0.05, max_depth=5,
                              subsample=0.8, colsample_bytree=0.8, random_state=42,
                              objective='reg:squarederror', tree_method='hist'),
        'lgbm': LGBMRegressor(n_estimators=200, learning_rate=0.05, num_leaves=31,
                               subsample=0.8, colsample_bytree=0.8, random_state=42,
                               verbose=-1),
        'cat':  CatBoostRegressor(iterations=200, learning_rate=0.05, depth=5,
                                   loss_function='RMSE', random_seed=42, verbose=0),
    }

    # Compute medians for inference meta before freeing the df
    medians = (
        df_feat.groupby(['town', 'flat_type'])[['remaining_lease_years', 'flat_age_years', 'lat', 'lon']]
        .median().to_dict('index')
    )

    # Free the full feature df now that train/test arrays are ready
    del df_feat; gc.collect()

    # ── Phase 1: OOF predictions for HuberRegressor meta-learner ────────────
    print("Phase 1: Generating out-of-fold predictions for stacker...")
    kf = KFold(n_splits=5, shuffle=True, random_state=42)
    oof_preds = np.zeros((len(X_train), len(model_specs)))

    for i, (name, model) in enumerate(model_specs.items()):
        print(f"  OOF {name}...")
        pipe = Pipeline(steps=[('preprocessor', preprocessor), ('model', model)])
        oof_preds[:, i] = cross_val_predict(pipe, X_train, y_train_log, cv=kf)
        gc.collect()

    # HuberRegressor meta-learner: robust to price outliers in OOF predictions
    stacker = HuberRegressor(epsilon=1.35, alpha=0.0001, max_iter=300)
    stacker.fit(oof_preds, y_train_log)
    stacker_weights   = stacker.coef_.tolist()
    stacker_intercept = float(stacker.intercept_)
    print(f"  Stacker weights: {[f'{w:.3f}' for w in stacker_weights]}  intercept={stacker_intercept:.4f}")

    # ── Phase 2: Train final base models on full training data ───────────────
    print("Phase 2: Training final base models...")
    trained = {}
    for name, model in model_specs.items():
        print(f"Training {name}...")
        pipe = Pipeline(steps=[('preprocessor', preprocessor), ('model', model)])
        pipe.fit(X_train, y_train_log)
        preds = np.exp(pipe.predict(X_test))
        mae   = mean_absolute_error(y_test, preds)
        r2    = r2_score(y_test, preds)
        print(f"  {name}: MAE=S${mae:,.0f}  R²={r2:.4f}")
        trained[name] = pipe
        joblib.dump(pipe, os.path.join(MODELS_DIR, f'{name}_pipeline.joblib'))
        print(f"  Saved {name}_pipeline.joblib")
        gc.collect()

    # ── Phase 3: Evaluate stacked vs simple average ──────────────────────────
    test_log_preds = np.column_stack([p.predict(X_test) for p in trained.values()])
    stacked_log    = stacker.predict(test_log_preds)
    stacked_preds  = np.exp(stacked_log)
    simple_avg     = np.exp(np.mean(test_log_preds, axis=1))
    print(f"Simple avg: MAE=S${mean_absolute_error(y_test, simple_avg):,.0f}  R²={r2_score(y_test, simple_avg):.4f}")
    print(f"Stacked:    MAE=S${mean_absolute_error(y_test, stacked_preds):,.0f}  R²={r2_score(y_test, stacked_preds):.4f}")

    # 6. Meta: store medians + latest policy/SORA for inference
    # (medians already computed above before df_feat was freed)

    # Latest policy for inference
    latest_policy = {'direction': 0.0, 'severity': 0.0, 'policy_impact': 0.0,
                     'months_since_policy_change': 0}
    if policy_df is not None and len(policy_df) > 0:
        pol = policy_df.copy()
        pol['effective_month'] = pd.to_datetime(pol['effective_month'], errors='coerce')
        pol = pol.sort_values('effective_month').iloc[-1]
        d = float(pol.get('direction', 0) or 0)
        s = float(pol.get('severity', 0) or 0)
        latest_policy = {'direction': d, 'severity': s, 'policy_impact': d * s,
                         'months_since_policy_change': 0}

    # Latest SORA for inference
    latest_sora = 3.5
    if sora_df is not None and len(sora_df) > 0:
        latest_sora = float(sora_df.sort_values('month').iloc[-1]['sora'])

    meta = {
        'time_idx_min':         time_idx_min,
        'medians_by_town_type': medians,
        'categorical_cols':     CATEGORICAL_COLS,
        'numerical_cols':       actual_num,
        'has_geo':              has_geo,
        'has_policy':           has_policy,
        'has_sora':             has_sora,
        'latest_policy':        latest_policy,
        'latest_sora':          latest_sora,
        'stacker_coef':         stacker_weights,
        'stacker_intercept':    stacker_intercept,
        'trained_at':           datetime.utcnow().isoformat(),
    }
    joblib.dump(meta, os.path.join(MODELS_DIR, 'meta.joblib'))
    print("All HDB models saved to", MODELS_DIR)

    # ── Phase 4: SHAP TreeExplainer for XAI ──────────────────────────────────
    print("Computing SHAP explainer (XGB)...")
    try:
        import shap
        xgb_pipe = trained['xgb']
        preprocessor_fitted = xgb_pipe.named_steps['preprocessor']
        xgb_model           = xgb_pipe.named_steps['model']
        feature_names_out   = preprocessor_fitted.get_feature_names_out().tolist()
        explainer = shap.TreeExplainer(xgb_model)
        shap_data = {
            'explainer':       explainer,
            'feature_names':   feature_names_out,
            'categorical_cols': CATEGORICAL_COLS,
            'numerical_cols':  actual_num,
            'base_value':      float(explainer.expected_value),
        }
        joblib.dump(shap_data, os.path.join(MODELS_DIR, 'shap_hdb.joblib'))
        print("  shap_hdb.joblib saved.")
    except Exception as e:
        print(f"  SHAP skipped: {e}")

    # Clean up temp checkpoint on success
    if os.path.exists(TEMP_PATH):
        os.remove(TEMP_PATH)
        print("Temp checkpoint removed.")


if __name__ == '__main__':
    train(from_db='--from-db' in sys.argv)
