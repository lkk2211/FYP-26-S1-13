#!/usr/bin/env python3
"""
Train HDB resale price prediction model.
Downloads data from data.gov.sg and trains an XGBoost + LightGBM + CatBoost ensemble.
Run once: python train_model.py
Models saved to ./models/
"""
import os
import re
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

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
RESOURCE_ID = 'f1765b54-a209-4718-8d38-a39237f502b3'
MIN_YEAR = 2019  # Use data from 2019 onwards

CATEGORICAL_COLS = ["town", "flat_type", "flat_model"]
NUMERICAL_COLS = [
    "floor_area_sqm",
    "year",
    "quarter",
    "time_idx",
    "storey_mid",
    "remaining_lease_years",
    "flat_age_years",
]
ALL_FEATURES = CATEGORICAL_COLS + NUMERICAL_COLS


# ─── Data download ────────────────────────────────────────────────────────────

def download_hdb_data():
    """Paginate through data.gov.sg datastore API and return full DataFrame."""
    base_url = 'https://data.gov.sg/api/action/datastore_search'
    all_records = []
    limit = 10000
    offset = 0
    total = None

    while True:
        r = requests.get(base_url, params={
            'resource_id': RESOURCE_ID,
            'limit': limit,
            'offset': offset,
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


def engineer_features(df):
    df = df.copy()

    df['month'] = pd.to_datetime(df['month'].astype(str).str[:7] + '-01')
    df['year'] = df['month'].dt.year
    df['quarter'] = df['month'].dt.quarter
    df['time_idx_raw'] = df['year'] * 12 + df['month'].dt.month

    df['storey_mid'] = df['storey_range'].apply(_storey_mid)
    df['remaining_lease_years'] = df['remaining_lease'].apply(_remaining_lease_years)

    df['lease_commence_date'] = pd.to_numeric(df['lease_commence_date'], errors='coerce')
    df['flat_age_years'] = df['year'] - df['lease_commence_date']

    df['floor_area_sqm'] = pd.to_numeric(df['floor_area_sqm'], errors='coerce')
    df['resale_price'] = pd.to_numeric(df['resale_price'], errors='coerce')

    for col in ['town', 'flat_type', 'flat_model']:
        df[col] = (df[col].astype(str).str.strip()
                   .str.upper().str.replace(r'\s+', ' ', regex=True))

    return df.dropna(subset=ALL_FEATURES + ['resale_price'])


# ─── Training ─────────────────────────────────────────────────────────────────

def _build_pipeline(model):
    preprocessor = ColumnTransformer(transformers=[
        ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), CATEGORICAL_COLS),
        ('num', 'passthrough', NUMERICAL_COLS),
    ])
    return Pipeline(steps=[('preprocessor', preprocessor), ('model', model)])


def train():
    os.makedirs(MODELS_DIR, exist_ok=True)

    print("Downloading HDB resale data...")
    df = download_hdb_data()
    print(f"Raw records: {len(df):,}")

    print("Engineering features...")
    df = engineer_features(df)
    df = df[df['year'] >= MIN_YEAR].copy()
    print(f"After filter (year >= {MIN_YEAR}): {len(df):,}")

    time_idx_min = int(df['time_idx_raw'].min())
    df['time_idx'] = df['time_idx_raw'] - time_idx_min

    train_df = df[df['year'] < 2025]
    test_df  = df[df['year'] >= 2025]
    X_train, y_train = train_df[ALL_FEATURES], train_df['resale_price']
    X_test,  y_test  = test_df[ALL_FEATURES],  test_df['resale_price']
    print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")

    y_train_log = np.log(y_train)

    model_specs = {
        'xgb': XGBRegressor(
            n_estimators=200, learning_rate=0.05, max_depth=6,
            subsample=0.8, colsample_bytree=0.8, random_state=42,
            objective='reg:squarederror',
        ),
        'lgbm': LGBMRegressor(
            n_estimators=200, learning_rate=0.05, num_leaves=31,
            subsample=0.8, colsample_bytree=0.8, random_state=42,
            verbose=-1,
        ),
        'cat': CatBoostRegressor(
            iterations=200, learning_rate=0.05, depth=6,
            loss_function='RMSE', random_seed=42, verbose=0,
        ),
    }

    trained = {}
    for name, model in model_specs.items():
        print(f"Training {name}...")
        pipeline = _build_pipeline(model)
        pipeline.fit(X_train, y_train_log)
        preds = np.exp(pipeline.predict(X_test))
        mae = mean_absolute_error(y_test, preds)
        r2  = r2_score(y_test, preds)
        print(f"  {name}: MAE=S${mae:,.0f}  R²={r2:.4f}")
        trained[name] = pipeline

    ens = np.mean([np.exp(p.predict(X_test)) for p in trained.values()], axis=0)
    print(f"Ensemble: MAE=S${mean_absolute_error(y_test, ens):,.0f}  R²={r2_score(y_test, ens):.4f}")

    # Per-town/flat-type medians for inference defaults
    medians = (
        df.groupby(['town', 'flat_type'])[['remaining_lease_years', 'flat_age_years']]
        .median()
        .to_dict('index')
    )

    meta = {
        'time_idx_min': time_idx_min,
        'medians_by_town_type': medians,
        'categorical_cols': CATEGORICAL_COLS,
        'numerical_cols': NUMERICAL_COLS,
        'trained_at': datetime.utcnow().isoformat(),
    }

    for name, pipeline in trained.items():
        path = os.path.join(MODELS_DIR, f'{name}_pipeline.joblib')
        joblib.dump(pipeline, path)
        print(f"Saved {path}")

    joblib.dump(meta, os.path.join(MODELS_DIR, 'meta.joblib'))
    print("All models saved to", MODELS_DIR)


if __name__ == '__main__':
    train()
