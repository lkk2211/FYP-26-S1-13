#!/usr/bin/env python3
"""
Train private property (condo/apartment/landed) price prediction model.
Downloads transaction data from URA API and trains an XGBoost + LightGBM + CatBoost ensemble.

Usage:
    URA_ACCESS_KEY=<your-key> python train_model_private.py

Models saved to ./models/ as:
    xgb_private_pipeline.joblib
    lgbm_private_pipeline.joblib
    cat_private_pipeline.joblib
    meta_private.joblib
"""
import os
import re
import json
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

MODELS_DIR    = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
URA_BASE      = 'https://www.ura.gov.sg/uraDataService'
ACCESS_KEY    = os.environ.get('URA_ACCESS_KEY', '')

CATEGORICAL_COLS = ['property_type', 'market_segment', 'type_of_sale', 'district']
NUMERICAL_COLS   = ['floor_area_sqft', 'floor_level_num', 'year', 'quarter', 'time_idx']
ALL_FEATURES     = CATEGORICAL_COLS + NUMERICAL_COLS

_FLOOR_LEVEL_MAP = {'low': 4, 'mid': 13, 'high': 25}
_TYPE_OF_SALE_MAP = {'1': 'New Sale', '2': 'Sub Sale', '3': 'Resale'}


# ─── URA API helpers ─────────────────────────────────────────────────────────

def _get_ura_token(access_key: str) -> str:
    r = requests.get(
        f'{URA_BASE}/insertNewToken.action',
        headers={'AccessKey': access_key},
        timeout=30,
    )
    if r.status_code != 200:
        raise ValueError(f'Token request failed: HTTP {r.status_code}\n{r.text[:300]}')
    try:
        data = r.json()
    except Exception:
        raise ValueError(f'Token response not JSON:\n{r.text[:300]}')
    if data.get('Status') != 'Success':
        raise ValueError(f'URA token error: {data}')
    return data['Result']


def _fetch_batch(access_key: str, token: str, batch: int) -> list:
    """Fetch one batch (1-4) of PMI_Resi_Transaction data."""
    r = requests.get(
        f'{URA_BASE}/invokeUraDS',
        params={'service': 'PMI_Resi_Transaction', 'batch': batch},
        headers={'AccessKey': access_key, 'Token': token},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    if data.get('Status') != 'Success':
        print(f'  Batch {batch}: URA returned status={data.get("Status")}')
        return []
    return data.get('Result', [])


def download_ura_transactions(access_key: str) -> pd.DataFrame:
    """Download all 4 batches and flatten to a per-transaction DataFrame."""
    print('Getting URA token...')
    token = _get_ura_token(access_key)
    print('Token obtained.')

    rows = []
    for batch in range(1, 5):
        print(f'  Fetching batch {batch}/4...', end=' ')
        projects = _fetch_batch(access_key, token, batch)
        for proj in projects:
            market_seg = proj.get('marketSegment', '')
            for det in proj.get('details', []):
                # Parse contractDate "MM/YY" → year/quarter
                cd = det.get('contractDate', '')
                try:
                    mo, yr = int(cd.split('/')[0]), int(cd.split('/')[1])
                    year  = 2000 + yr if yr < 100 else yr
                    quarter = (mo - 1) // 3 + 1
                except Exception:
                    continue

                fl_raw = (det.get('floorLevel') or det.get('floorRange') or 'low').lower()
                fl_num = _FLOOR_LEVEL_MAP.get(fl_raw[:3], 10)
                # Try to parse numeric floor range
                fl_match = re.search(r'(\d+)', fl_raw)
                if fl_match:
                    fl_num = int(fl_match.group(1))

                rows.append({
                    'property_type':  det.get('propertyType', proj.get('propertyType', '')),
                    'market_segment': market_seg,
                    'type_of_sale':   _TYPE_OF_SALE_MAP.get(str(det.get('typeOfSale', '3')), 'Resale'),
                    'district':       str(det.get('district', '0')).zfill(2),
                    'floor_area_sqft': float(det.get('area') or 0),
                    'floor_level_num': fl_num,
                    'year':           year,
                    'quarter':        quarter,
                    'contract_date':  f'{year}-{mo:02d}',
                    'price':          float(det.get('price') or 0),
                })
        print(f'{len(rows):,} transactions so far')

    return pd.DataFrame(rows)


# ─── Feature engineering ─────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Normalise text categoricals
    for col in ['property_type', 'market_segment', 'type_of_sale']:
        df[col] = df[col].astype(str).str.strip().str.upper().str.replace(r'\s+', ' ', regex=True)

    # District zero-padded string
    df['district'] = df['district'].astype(str).str.strip().str.zfill(2)

    # Numeric
    df['floor_area_sqft'] = pd.to_numeric(df['floor_area_sqft'], errors='coerce')
    df['floor_level_num'] = pd.to_numeric(df['floor_level_num'], errors='coerce').fillna(10)
    df['price']           = pd.to_numeric(df['price'], errors='coerce')

    # Time index
    df['time_idx_raw'] = df['year'] * 12 + df['quarter'] * 3

    return df.dropna(subset=ALL_FEATURES + ['price'])


# ─── Training ────────────────────────────────────────────────────────────────

def _build_pipeline(model):
    preprocessor = ColumnTransformer(transformers=[
        ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), CATEGORICAL_COLS),
        ('num', 'passthrough', NUMERICAL_COLS),
    ])
    return Pipeline(steps=[('preprocessor', preprocessor), ('model', model)])


def train(access_key: str):
    os.makedirs(MODELS_DIR, exist_ok=True)

    print('Downloading URA private property transactions...')
    df = download_ura_transactions(access_key)
    print(f'Raw records: {len(df):,}')

    print('Engineering features...')
    df = engineer_features(df)

    # Remove outliers (price <= 0 or unreasonably high/low per sqft)
    df = df[(df['price'] > 100_000) & (df['floor_area_sqft'] > 10)]
    df = df[df['price'] < 100_000_000]
    print(f'After cleaning: {len(df):,}')

    time_idx_min = int(df['time_idx_raw'].min())
    df['time_idx'] = df['time_idx_raw'] - time_idx_min

    train_df = df[df['year'] < 2025]
    test_df  = df[df['year'] >= 2025]
    X_train, y_train = train_df[ALL_FEATURES], train_df['price']
    X_test,  y_test  = test_df[ALL_FEATURES],  test_df['price']
    print(f'Train: {len(X_train):,} | Test: {len(X_test):,}')

    if len(X_test) == 0:
        # Not enough 2025 data — use last 10% as test
        split = int(len(df) * 0.9)
        df_s  = df.sort_values('time_idx_raw')
        train_df = df_s.iloc[:split]
        test_df  = df_s.iloc[split:]
        X_train, y_train = train_df[ALL_FEATURES], train_df['price']
        X_test,  y_test  = test_df[ALL_FEATURES],  test_df['price']
        print(f'Adjusted split → Train: {len(X_train):,} | Test: {len(X_test):,}')

    y_train_log = np.log(y_train)

    model_specs = {
        'xgb_private': XGBRegressor(
            n_estimators=200, learning_rate=0.05, max_depth=6,
            subsample=0.8, colsample_bytree=0.8, random_state=42,
            objective='reg:squarederror',
        ),
        'lgbm_private': LGBMRegressor(
            n_estimators=200, learning_rate=0.05, num_leaves=31,
            subsample=0.8, colsample_bytree=0.8, random_state=42,
            verbose=-1,
        ),
        'cat_private': CatBoostRegressor(
            iterations=200, learning_rate=0.05, depth=6,
            loss_function='RMSE', random_seed=42, verbose=0,
        ),
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

    ens = np.mean([np.exp(p.predict(X_test)) for p in trained.values()], axis=0)
    print(f'Ensemble: MAE=S${mean_absolute_error(y_test, ens):,.0f}  R²={r2_score(y_test, ens):.4f}')

    # Medians per district/type for inference defaults
    medians = (
        df.groupby(['district', 'property_type'])[['floor_area_sqft', 'floor_level_num']]
        .median()
        .to_dict('index')
    )

    meta = {
        'time_idx_min':           time_idx_min,
        'medians_by_dist_type':   medians,
        'categorical_cols':       CATEGORICAL_COLS,
        'numerical_cols':         NUMERICAL_COLS,
        'trained_at':             datetime.utcnow().isoformat(),
    }

    for name, pipeline in trained.items():
        path = os.path.join(MODELS_DIR, f'{name}_pipeline.joblib')
        joblib.dump(pipeline, path)
        print(f'Saved {path}')

    joblib.dump(meta, os.path.join(MODELS_DIR, 'meta_private.joblib'))
    print('All private property models saved to', MODELS_DIR)


if __name__ == '__main__':
    key = ACCESS_KEY
    if not key:
        import sys
        print('ERROR: Set URA_ACCESS_KEY environment variable before running.')
        sys.exit(1)
    train(key)
