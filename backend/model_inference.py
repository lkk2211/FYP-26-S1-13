"""
model_inference.py
──────────────────
Loads the trained stacking models (hdb_model.pkl / private_model.pkl) and
provides predict_hdb() / predict_private() for use in server.py.

If a .pkl file is not present the functions return None, and predict.py
falls back to the existing rule-based logic in predict_price().
"""

import os
import pickle
import datetime
import numpy as np
import pandas as pd

_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Current macro defaults (updated periodically) ──────────────────────────
_CURRENT_SORA   = 3.18   # 3-month compound SORA (approximate, Apr 2026)
_POLICY_DIRECTION  =  1   # latest policy direction  (+1 tightening / -1 easing)
_POLICY_SEVERITY   =  3   # latest policy severity (1-5 scale)
_POLICY_MONTHS_AGO = 14   # months since most recent policy change

# Market segment boundaries (URA definition)
_CCR_DISTRICTS = {1,2,6,9,10,11}
_RCR_DISTRICTS = {3,4,5,7,8,12,13,14,15,20,21,22,23}

# Flat model defaults per flat_type (most common value from training data)
_FLAT_MODEL_DEFAULT = {
    '1 ROOM':    'IMPROVED',
    '2 ROOM':    'IMPROVED',
    '3 ROOM':    'IMPROVED',
    '4 ROOM':    'MODEL A',
    '5 ROOM':    'IMPROVED',
    'EXECUTIVE': 'MAISONETTE',
    'MULTI-GENERATION': 'MULTI GENERATION',
}

# Approximate median flat age by town (years) — used when lease data unavailable
_TOWN_AVG_AGE = {
    'ANG MO KIO': 38, 'BEDOK': 37, 'BISHAN': 28, 'BUKIT BATOK': 32,
    'BUKIT MERAH': 42, 'BUKIT PANJANG': 22, 'BUKIT TIMAH': 35,
    'CENTRAL AREA': 30, 'CHOA CHU KANG': 24, 'CLEMENTI': 36,
    'GEYLANG': 38, 'HOUGANG': 30, 'JURONG EAST': 30, 'JURONG WEST': 28,
    'KALLANG/WHAMPOA': 40, 'MARINE PARADE': 43, 'PASIR RIS': 26,
    'PUNGGOL': 10, 'QUEENSTOWN': 48, 'SEMBAWANG': 18, 'SENGKANG': 14,
    'SERANGOON': 34, 'TAMPINES': 28, 'TOA PAYOH': 44, 'WOODLANDS': 26,
    'YISHUN': 28,
}
_DEFAULT_FLAT_AGE = 30


def _load(fname):
    path = os.path.join(_DIR, fname)
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'rb') as f:
            return pickle.load(f)
    except Exception as e:
        print(f'[model_inference] Failed to load {fname}: {e}')
        return None


def _stacked_predict(bundle, row_df):
    """Run base models → metalayer → exponentiate back from log-space."""
    xgb_log = bundle['xgb'].predict(row_df)[0]
    lgb_log = bundle['lgb'].predict(row_df)[0]
    cat_log = bundle['cat'].predict(row_df)[0]
    meta_input = np.array([[xgb_log, lgb_log, cat_log]])
    log_price = bundle['meta'].predict(meta_input)[0]
    return float(np.exp(log_price))


# ── HDB inference ─────────────────────────────────────────────────────────────

def predict_hdb(features: dict):
    """
    features keys (matching server.py / frontend):
        town         : str   e.g. 'TAMPINES'
        flat_type    : str   e.g. '4 ROOM'
        floor_area_sqm: float
        storey_mid   : float  (mid-point floor number)
        flat_age_years: float (optional — estimated from town if absent)
        remaining_lease_years: float (optional)
        lat          : float (optional — from OneMap)
        lon          : float (optional)
        flat_model   : str   (optional — defaults to most common for flat_type)

    Returns estimated price (SGD) as float, or None if model not loaded.
    """
    bundle = _load('hdb_model.pkl')
    if bundle is None:
        return None

    now    = datetime.datetime.now()
    year   = now.year
    month  = now.month
    qtr    = (month - 1) // 3 + 1
    t_min  = bundle.get('train_year_min', 2017)
    t_idx  = (year - t_min) * 12 + month

    town      = str(features.get('town', '')).strip().upper()
    flat_type = str(features.get('flat_type', '4 ROOM')).strip().upper()
    flat_model = str(features.get('flat_model',
                     _FLAT_MODEL_DEFAULT.get(flat_type, 'IMPROVED'))).strip().upper()

    floor_area = float(features.get('floor_area_sqm', 90))
    storey_mid = float(features.get('storey_mid', features.get('floor', 5)))
    flat_age   = float(features.get('flat_age_years',
                       _TOWN_AVG_AGE.get(town, _DEFAULT_FLAT_AGE)))
    rem_lease  = float(features.get('remaining_lease_years', max(0, 99 - flat_age)))
    lat        = float(features.get('lat', 1.3521))
    lon        = float(features.get('lon', 103.8198))

    row = pd.DataFrame([{
        'town':                       town,
        'flat_type':                  flat_type,
        'flat_model':                 flat_model,
        'floor_area_sqm':             floor_area,
        'direction':                  _POLICY_DIRECTION,
        'severity':                   _POLICY_SEVERITY,
        'policy_impact':              _POLICY_DIRECTION * _POLICY_SEVERITY,
        'months_since_policy_change': _POLICY_MONTHS_AGO,
        'sora':                       _CURRENT_SORA,
        'year':                       year,
        'quarter':                    qtr,
        'time_idx':                   t_idx,
        'storey_mid':                 storey_mid,
        'flat_age_years':             flat_age,
        'remaining_lease_years':      rem_lease,
        'lat':                        lat,
        'lon':                        lon,
    }])

    try:
        return _stacked_predict(bundle, row)
    except Exception as e:
        print(f'[model_inference] HDB prediction error: {e}')
        return None


# ── Private property inference ────────────────────────────────────────────────

def _district_to_market_segment(district: int) -> str:
    if district in _CCR_DISTRICTS:
        return 'CCR'
    if district in _RCR_DISTRICTS:
        return 'RCR'
    return 'OCR'


def predict_private(features: dict):
    """
    features keys:
        area_sqm       : float
        floor          : float  (floor level)
        district       : int    (postal district 1-28, derived from postal if absent)
        postal         : str    (6-digit, used to derive district if district absent)
        property_type  : str    e.g. 'CONDOMINIUM'
        type_of_sale   : str    e.g. 'RESALE'  (default)
        tenure_type    : str    'FREEHOLD' or 'LEASEHOLD'
        remaining_tenure_yrs: float (optional)

    Returns estimated price (SGD) as float, or None if model not loaded.
    """
    bundle = _load('private_model.pkl')
    if bundle is None:
        return None

    now   = datetime.datetime.now()
    year  = now.year
    month = now.month
    qtr   = (month - 1) // 3 + 1
    t_min = bundle.get('train_year_min', 2017)
    t_idx = (year - t_min) * 12 + month

    # Derive district from postal if not provided
    district = features.get('district')
    if not district:
        postal = str(features.get('postal', '000000')).zfill(6)
        try:
            district = int(postal[:2])
        except ValueError:
            district = 15   # default

    district      = int(district)
    market_seg    = _district_to_market_segment(district)
    prop_type     = str(features.get('property_type', 'CONDOMINIUM')).strip().upper()
    type_of_sale  = str(features.get('type_of_sale', 'RESALE')).strip().upper()
    tenure_type   = str(features.get('tenure_type', 'LEASEHOLD')).strip().upper()
    rem_tenure    = float(features.get('remaining_tenure_yrs',
                          999.0 if tenure_type == 'FREEHOLD' else 60.0))
    area_sqm      = float(features.get('area_sqm',
                          features.get('area', 80)))
    floor_mid     = float(features.get('floor', features.get('floor_mid', 10)))

    row = pd.DataFrame([{
        'market_segment':       market_seg,
        'property_type':        prop_type,
        'type_of_sale':         type_of_sale,
        'tenure_type':          tenure_type,
        'area_sqm':             area_sqm,
        'floor_mid':            floor_mid,
        'district':             district,
        'remaining_tenure_yrs': rem_tenure,
        'no_of_units':          1,
        'year':                 year,
        'quarter':              qtr,
        'time_idx':             t_idx,
        'sora':                 _CURRENT_SORA,
    }])

    try:
        return _stacked_predict(bundle, row)
    except Exception as e:
        print(f'[model_inference] Private prediction error: {e}')
        return None
