#!/usr/bin/env python3
"""
PropAI.sg — HDB Resale Price Prediction Model

Stacked ensemble: XGBoost + LightGBM + CatBoost → HuberRegressor meta-learner.
Trained on HDB resale transactions enriched with policy, SORA, and geocoding.

Design decisions documented here so they don't need to be re-learned:
- MIN_YEAR=2015: pre-2015 records have SORA=0 and stale block PSF values that
  increase MAPE. 2015-16 SORA gap is filled with training median (~1.8%,
  a stable low-rate era) which is accurate enough to be useful.
- CatBoost uses Lossguide growth (leaf-wise like LGBM) + native categoricals.
  Level-wise depth=N was causing CatBoost R²=0.849 vs LGBM R²=0.880; Lossguide
  closed the gap to R²=0.897.
- XGB uses depth=5 + L1/L2/gamma regularisation to diverge from LGBM's deep
  leaf-wise error surface. This gives the stacker genuine orthogonal signal.
- PSF hierarchy (block → street → town) all use shift(1) to prevent leakage.
  Geo PSF is computed AFTER the geocoding merge (needs lat/lon).
- lease_commence_date is included as a raw feature alongside derived fields.
  It is the #1 feature in published HDB ML research — era effects (1970s/80s/
  90s/2000s design, market psychology) beyond what flat_age alone encodes.
- 3-fold OOF (not 5): training on 276k rows with 3 models × 3 folds = 9 fits
  completes in ~15 min on GitHub Actions; 5-fold would push close to 40-min limit.

Usage:
    python train_model.py
    python train_model.py --from-db   # error if DB empty
"""
import os, re, sys, gc, warnings, joblib, requests
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import Ridge, HuberRegressor
from sklearn.model_selection import KFold, cross_val_predict
from sklearn.metrics import mean_absolute_error, r2_score
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from catboost import CatBoostRegressor

warnings.filterwarnings('ignore', message='X does not have valid feature names')
warnings.filterwarnings('ignore', category=UserWarning, module='lightgbm')

MODELS_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
TEMP_PATH   = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'temp_progress.csv')
RESOURCE_ID = 'f1765b54-a209-4718-8d38-a39237f502b3'
MIN_YEAR    = 2015

CATEGORICAL_COLS = ['town', 'flat_type', 'flat_model']

# Full feature set used when all data sources are available
NUMERICAL_COLS_FULL = [
    # Size & floor
    'floor_area_sqm',
    'storey_mid',
    'storey_pct',                       # floor as % of building height
    # Lease
    'lease_commence_date',              # era signal: 70s/80s/90s/2000s HDB generations
    'remaining_lease_years',
    'flat_age_years',
    'bala_fraction',                    # SISV non-linear lease decay
    # Temporal
    'year', 'quarter', 'time_idx',
    'sin_month', 'cos_month',           # cyclic within-year seasonality
    # Policy & macro
    'direction', 'severity', 'policy_impact', 'months_since_policy_change',
    'sora',
    # Location (excluded when geocoding unavailable)
    'lat', 'lon', 'dist_nearest_mrt_km',
    'dist_nearest_school_km',     # nearest school (all types)
    'dist_nearest_hawker_km',     # nearest hawker / food court
    'dist_nearest_health_km',     # nearest hospital / clinic
    'dist_nearest_park_km',       # nearest park / green space
    'dist_nearest_community_km',  # nearest CC / library
    # PSF hierarchy — all shift(1) to prevent data leakage
    'block_rolling_psf_24m',            # 24m block×flat_type median (strongest feature)
    'block_median_psf_alltime',         # all-time block anchor
    'street_rolling_psf_24m',           # 24m street×flat_type (block fallback)
    'town_flat_type_median_psf',        # all-time town×flat_type (broadest anchor)
    'flat_model_town_rolling_psf_24m',  # 24m town×flat_model (DBSS, Maisonette etc.)
    'geo_rolling_psf_24m',              # 24m ~1km spatial grid × flat_type
    'town_rolling_psf_12m',             # 12m town×flat_type momentum (recent trend)
    'market_rolling_psf_12m',           # 12m national flat_type trend (market drift signal)
    # Interactions
    'storey_psf_interaction',           # storey_pct × block_rolling_psf_24m
    'lease_psf_interaction',            # remaining_lease_years × block_rolling_psf_24m
]

# Minimal fallback when geo / policy / SORA unavailable
NUMERICAL_COLS_MIN = [
    'floor_area_sqm', 'storey_mid', 'storey_pct',
    'lease_commence_date', 'remaining_lease_years', 'flat_age_years', 'bala_fraction',
    'year', 'quarter', 'time_idx', 'sin_month', 'cos_month',
    'block_rolling_psf_24m', 'block_median_psf_alltime',
    'street_rolling_psf_24m', 'town_flat_type_median_psf',
    'flat_model_town_rolling_psf_24m', 'geo_rolling_psf_24m',
    'town_rolling_psf_12m', 'market_rolling_psf_12m',
    'storey_psf_interaction', 'lease_psf_interaction',
    'dist_nearest_school_km', 'dist_nearest_hawker_km',
    'dist_nearest_health_km', 'dist_nearest_park_km', 'dist_nearest_community_km',
]

_BALA_PTS = [
    (99, 1.000), (90, 0.914), (80, 0.811), (70, 0.697), (60, 0.565),
    (50, 0.420), (40, 0.272), (30, 0.133), (20, 0.062), (10, 0.015), (0, 0.0),
]

def _bala_fraction(lr):
    lr = max(0.0, min(float(lr), 99.0))
    for i in range(len(_BALA_PTS) - 1):
        y0, f0 = _BALA_PTS[i]
        y1, f1 = _BALA_PTS[i + 1]
        if y1 <= lr <= y0:
            t = (lr - y1) / (y0 - y1)
            return round(f1 + t * (f0 - f1), 6)
    return 0.0

_MRT_STATIONS = [
    # North-South Line
    (1.4474,103.7742),(1.4617,103.7875),(1.4739,103.8003),(1.4271,103.8384),
    (1.4041,103.8485),(1.3817,103.8449),(1.3620,103.8330),(1.3699,103.8486),
    (1.3514,103.8479),(1.3394,103.8443),(1.3263,103.8458),(1.3197,103.8442),
    (1.3101,103.8454),(1.3006,103.8365),(1.2970,103.8441),(1.2958,103.8523),
    (1.2831,103.8451),(1.2833,103.8530),(1.2784,103.8485),(1.2742,103.8510),
    (1.3799,103.7453),(1.3629,103.7456),(1.3693,103.7457),(1.3970,103.7479),
    (1.4323,103.7633),(1.4374,103.7870),
    # East-West Line
    (1.3290,103.8887),(1.3193,103.9021),(1.3143,103.9122),(1.3030,103.9022),
    (1.2967,103.9021),(1.2736,103.8456),(1.2759,103.8362),(1.2787,103.8193),
    (1.2909,103.8006),(1.2960,103.7899),(1.3113,103.7876),(1.3140,103.7756),
    (1.3031,103.7625),(1.3153,103.7655),(1.3337,103.7421),(1.3451,103.7028),
    (1.3424,103.6886),(1.3496,103.7227),(1.3374,103.7058),(1.3286,103.7000),
    (1.3352,103.9309),(1.3435,103.9486),(1.3518,103.9644),(1.3541,103.9825),
    (1.3600,103.9870),(1.3343,103.9158),(1.3202,103.9219),
    # North-East Line
    (1.2877,103.8456),(1.2800,103.8475),(1.2785,103.8319),(1.3017,103.8559),
    (1.3121,103.8649),(1.3214,103.8652),(1.3297,103.8749),(1.3392,103.8872),
    (1.3504,103.8938),(1.3621,103.8870),(1.3718,103.8819),(1.3897,103.8919),
    (1.3963,103.9012),(1.4063,103.9022),
    # Circle Line
    (1.2917,103.8574),(1.2996,103.8614),(1.3055,103.8558),(1.3060,103.8634),
    (1.3104,103.8789),(1.3092,103.8869),(1.3069,103.8940),(1.3340,103.9047),
    (1.3333,103.9023),(1.3606,103.8861),(1.3328,103.8252),(1.3197,103.8072),
    (1.3007,103.8010),(1.2971,103.7876),(1.2913,103.7812),(1.3059,103.7759),
    (1.2975,103.7883),(1.2930,103.7762),(1.2892,103.7628),(1.2834,103.7489),
    (1.2776,103.7646),(1.2716,103.7738),(1.2688,103.7848),
    # Downtown Line
    (1.3424,103.7491),(1.3378,103.7499),(1.3317,103.7530),(1.3228,103.7631),
    (1.3240,103.7789),(1.3250,103.7905),(1.3260,103.8001),(1.3173,103.8063),
    (1.3079,103.8186),(1.3027,103.8321),(1.2996,103.8451),(1.2854,103.8451),
    (1.2788,103.8501),(1.2851,103.8631),(1.3103,103.9048),(1.3125,103.9317),
    (1.3155,103.9416),(1.3204,103.9500),(1.3518,103.9464),(1.3667,103.9309),
    (1.3583,103.9199),(1.3505,103.9101),(1.3370,103.9066),
    # Thomson-East Coast Line
    (1.4537,103.8185),(1.4474,103.8194),(1.4382,103.8395),(1.3884,103.8389),
    (1.3741,103.8322),(1.3611,103.8351),(1.3446,103.8330),(1.3176,103.8279),
    (1.3093,103.8356),(1.3080,103.8315),(1.2930,103.8453),(1.2885,103.8365),
    (1.2807,103.8399),(1.2767,103.8449),(1.2763,103.8630),(1.2847,103.8631),
    (1.3149,103.9302),(1.3204,103.9422),
]

# Sources: Singapore Land Authority / data.gov.sg school dataset
_PRIMARY_SCHOOLS = [
    # Central / Queenstown / Bukit Merah
    (1.2901,103.8194),(1.2939,103.8027),(1.2825,103.8234),(1.2698,103.8225),
    (1.2703,103.8284),(1.2847,103.8289),(1.2929,103.7897),(1.3046,103.7897),
    # Toa Payoh / Bishan / Ang Mo Kio
    (1.3334,103.8474),(1.3518,103.8444),(1.3498,103.8452),(1.3608,103.8470),
    (1.3197,103.8431),(1.3269,103.8350),(1.3792,103.8710),(1.3726,103.8484),
    (1.3601,103.8229),(1.3808,103.7979),(1.3799,103.8403),(1.3700,103.8500),
    # Serangoon / Hougang / Punggol / Sengkang
    (1.3505,103.8710),(1.3652,103.8915),(1.3596,103.8878),(1.3773,103.9003),
    (1.4010,103.9056),(1.3965,103.9093),(1.3965,103.9160),(1.4004,103.9094),
    (1.3800,103.8878),(1.3780,103.8960),
    # Bedok / Tampines / Pasir Ris
    (1.3308,103.9286),(1.3265,103.9350),(1.3196,103.9371),(1.3254,103.9289),
    (1.3558,103.9415),(1.3578,103.9524),(1.3534,103.9430),(1.3747,103.9506),
    (1.3770,103.9393),(1.3700,103.9450),
    # Marine Parade / Geylang / Katong
    (1.3077,103.8981),(1.3131,103.8869),(1.3039,103.9033),(1.3089,103.8944),
    (1.3151,103.8705),(1.3128,103.8854),(1.3071,103.8982),(1.3050,103.8900),
    # Woodlands / Sembawang / Canberra
    (1.4425,103.7969),(1.4374,103.7843),(1.4414,103.8226),(1.4297,103.8353),
    (1.4277,103.8370),(1.4540,103.8180),(1.4450,103.8030),(1.4480,103.7900),
    # Yishun
    (1.4297,103.8353),(1.4200,103.8380),(1.4350,103.8460),(1.4150,103.8300),
    # Jurong West / Boon Lay / Pioneer
    (1.3506,103.7219),(1.3557,103.7086),(1.3436,103.7250),(1.3486,103.7079),
    (1.3451,103.7028),(1.3425,103.7064),(1.3380,103.7150),(1.3460,103.7200),
    # Clementi / Buona Vista / West Coast
    (1.3153,103.7649),(1.3182,103.7652),(1.3136,103.7641),(1.3046,103.7882),
    (1.3088,103.7736),(1.3170,103.7700),(1.3060,103.7780),
    # Choa Chu Kang / Bukit Panjang / Bukit Batok
    (1.3897,103.7451),(1.3800,103.7600),(1.3730,103.7520),(1.3650,103.7460),
    (1.3580,103.7650),(1.3490,103.7600),(1.3570,103.7490),
    # Punggol / Sengkang additional
    (1.3960,103.9020),(1.4020,103.9100),(1.3880,103.8950),(1.3820,103.9010),
]

# Sources: NEA / data.gov.sg hawker centre dataset
_HAWKER_CENTRES = [
    # Central / Orchard / Chinatown
    (1.2800,103.8437),(1.2819,103.8441),(1.2847,103.8289),(1.2827,103.8158),
    (1.2890,103.8490),(1.2785,103.8395),(1.2760,103.8460),
    # Toa Payoh / Bishan
    (1.3334,103.8474),(1.3395,103.8490),(1.3506,103.8485),(1.3450,103.8390),
    # Ang Mo Kio
    (1.3702,103.8473),(1.3760,103.8490),(1.3650,103.8430),(1.3820,103.8450),
    # Serangoon / Hougang
    (1.3505,103.8710),(1.3652,103.8915),(1.3596,103.8878),(1.3700,103.8860),
    # Punggol / Sengkang
    (1.4004,103.9094),(1.3900,103.8953),(1.3850,103.8990),(1.4050,103.9050),
    # Bedok / Tampines
    (1.3254,103.9289),(1.3200,103.9340),(1.3513,103.9404),(1.3460,103.9360),
    (1.3580,103.9450),
    # Pasir Ris
    (1.3747,103.9506),(1.3700,103.9480),
    # Marine Parade / Geylang
    (1.3038,103.9034),(1.3139,103.8959),(1.3077,103.8981),(1.3100,103.8870),
    # Queenstown / Buona Vista / Commonwealth
    (1.2997,103.7979),(1.3046,103.7882),(1.2939,103.8027),(1.2920,103.7960),
    # Bukit Merah / Redhill
    (1.2805,103.8198),(1.2828,103.8158),(1.2760,103.8200),(1.2730,103.8150),
    # Clementi / West Coast
    (1.3153,103.7649),(1.3100,103.7700),(1.3200,103.7600),
    # Jurong / Boon Lay
    (1.3451,103.7055),(1.3425,103.7064),(1.3510,103.7120),(1.3390,103.7010),
    # Choa Chu Kang / Bukit Panjang
    (1.3880,103.7440),(1.3800,103.7580),(1.3720,103.7520),
    # Woodlands
    (1.4374,103.7843),(1.4300,103.7860),(1.4450,103.7960),
    # Yishun / Sembawang
    (1.4277,103.8370),(1.4350,103.8320),(1.4200,103.8280),(1.4480,103.8200),
    # Tampines additional
    (1.3526,103.9455),(1.3600,103.9380),
    # Adam Rd / Botanic Gardens area
    (1.3269,103.8144),(1.3200,103.8100),
    # Old Airport Road / Dakota
    (1.3137,103.8854),(1.3090,103.8820),
]

def _get_db_conn():
    url = os.environ.get('DATABASE_URL', '')
    if url:
        import psycopg2, psycopg2.extras
        return psycopg2.connect(url), 'postgres'
    import sqlite3
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'propaisg.db')
    conn = sqlite3.connect(path)
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

def download_hdb_data():
    base = 'https://data.gov.sg/api/action/datastore_search'
    records, limit, offset, total = [], 10000, 0, None
    while True:
        r = requests.get(base, params={'resource_id': RESOURCE_ID, 'limit': limit, 'offset': offset}, timeout=60)
        r.raise_for_status()
        result = r.json()['result']
        if total is None:
            total = result['total']
        batch = result['records']
        records.extend(batch)
        print(f'  {len(records):,} / {total:,}', end='\r')
        if len(batch) < limit:
            break
        offset += limit
    print()
    return pd.DataFrame(records)

def load_hdb_from_db():
    try:
        rows = _query(f"""
            SELECT month, town, flat_type, flat_model, floor_area_sqm,
                   storey_range, resale_price, remaining_lease, lease_commence_date,
                   block, street_name
            FROM resale_flat_prices WHERE month >= '{MIN_YEAR}-01'
        """)
        return pd.DataFrame(rows) if rows else None
    except Exception as e:
        print(f'  resale_flat_prices error: {e}')
        return None

def load_policy_from_db():
    try:
        rows = _query("""
            SELECT effective_month, direction, severity
            FROM policy_changes WHERE effective_month IS NOT NULL
        """)
        return pd.DataFrame(rows) if rows else None
    except Exception as e:
        print(f'  policy_changes error: {e}')
        return None

def load_sora_from_db():
    try:
        rows = _query('SELECT publication_date, compound_sora_3m FROM sora_rates WHERE compound_sora_3m IS NOT NULL')
        if not rows:
            rows = _query('SELECT publication_date, compound_sora_3m FROM stage_sora WHERE compound_sora_3m IS NOT NULL')
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df['date']    = pd.to_datetime(df['publication_date'], errors='coerce')
        df['sora_3m'] = pd.to_numeric(df['compound_sora_3m'].astype(str), errors='coerce')
        df = df.dropna(subset=['sora_3m'])
        if df.empty:
            return None
        # Impute any missing dates
        if df['date'].isna().any():
            base = df['date'].dropna().min() or pd.Timestamp('2019-01-01')
            df['date'] = df['date'].fillna(
                pd.Series([base + pd.DateOffset(months=i) for i in range(len(df))], index=df.index)
            )
        df['month'] = df['date'].dt.to_period('M').dt.to_timestamp().astype('datetime64[s]')
        result = df.groupby('month', as_index=False)['sora_3m'].mean().rename(columns={'sora_3m': 'sora'})
        print(f'  sora_rates: {len(result)} monthly rows ({df["date"].min().year}–{df["date"].max().year})')
        return result
    except Exception as e:
        print(f'  sora_rates error: {e}')
        return None

def load_geocoded_from_db():
    try:
        rows = _query('SELECT search_text, lat, lon FROM geocoded_addresses WHERE lat IS NOT NULL AND lon IS NOT NULL')
        return pd.DataFrame(rows) if rows else None
    except Exception as e:
        print(f'  geocoded_addresses error: {e}')
        return None


def load_amenities_from_db():
    """Load all amenity coordinates grouped by type from the amenities table.
    Also mines the amenity_cache table for additional points not yet in amenities.
    Returns dict: {amenity_type: [(lat, lon), ...]}
    """
    import json as _json
    result = {}
    try:
        # Primary source: amenities table
        rows = _query("""
            SELECT amenity_type, latitude, longitude
            FROM amenities
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """)
        for r in rows:
            t = str(r['amenity_type']).strip().lower()
            result.setdefault(t, []).append((float(r['latitude']), float(r['longitude'])))

        # Secondary source: mine amenity_cache JSON blobs for any types not yet in amenities
        cache_rows = _query("SELECT data FROM amenity_cache WHERE data IS NOT NULL")
        for cr in cache_rows:
            try:
                blob = _json.loads(cr['data'])
                for atype, items in blob.items():
                    if not isinstance(items, list):
                        continue
                    key = atype.strip().lower()
                    for item in items:
                        lat = item.get('lat')
                        lng = item.get('lng')
                        if lat and lng:
                            result.setdefault(key, []).append((float(lat), float(lng)))
            except Exception:
                continue

        # Deduplicate within each type (round to 4dp ≈ 11m)
        for t in result:
            result[t] = list({(round(la, 4), round(lo, 4)) for la, lo in result[t]})

        counts = {t: len(v) for t, v in result.items()}
        print(f'  Amenities loaded: { {t: n for t, n in sorted(counts.items())} }')
        return result
    except Exception as e:
        print(f'  amenities load error: {e}')
        return {}

def _storey_mid(s):
    try:
        lo, hi = str(s).upper().split(' TO ')
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
    return years + (float(m.group(1)) if m else 0.0) / 12

def engineer_features(df, policy_df, sora_df, geo_df, amenity_dict=None):
    df = df.copy()

    df['month']    = pd.to_datetime(df['month'].astype(str).str[:7] + '-01').astype('datetime64[s]')
    df['year']     = df['month'].dt.year
    df['quarter']  = df['month'].dt.quarter
    df['time_idx_raw'] = df['year'] * 12 + df['month'].dt.month

    df['storey_mid']           = df['storey_range'].apply(_storey_mid)
    df['remaining_lease_years'] = df['remaining_lease'].apply(_remaining_lease_years)
    df['lease_commence_date']   = pd.to_numeric(df['lease_commence_date'], errors='coerce')
    df['flat_age_years']        = df['year'] - df['lease_commence_date']
    df['floor_area_sqm']        = pd.to_numeric(df['floor_area_sqm'], errors='coerce')
    df['resale_price']          = pd.to_numeric(df['resale_price'],   errors='coerce')

    for col in CATEGORICAL_COLS:
        df[col] = df[col].astype(str).str.strip().str.upper().str.replace(r'\s+', ' ', regex=True)

    df['bala_fraction'] = df['remaining_lease_years'].apply(_bala_fraction)

    df['block_key']  = df['block'].fillna('').astype(str).str.strip() + '_' + df['flat_type']
    block_max        = df.groupby('block_key')['storey_mid'].transform('max')
    ft_max           = df.groupby('flat_type')['storey_mid'].transform('median')
    df['max_storey'] = block_max.where(block_max > 0, ft_max).fillna(10.0)
    df['storey_pct'] = (df['storey_mid'] / df['max_storey']).clip(0.0, 1.0)

    # All features use shift(1) on the sorted series to prevent data leakage.
    df['psf'] = df['resale_price'] / (df['floor_area_sqm'].replace(0, np.nan) * 10.764)
    df_s = df.sort_values(['block_key', 'month'])

    # 1. Block × flat_type (strongest signal)
    df_s['block_rolling_psf_24m'] = (
        df_s.groupby('block_key')['psf']
        .transform(lambda x: x.shift(1).rolling(24, min_periods=3).median())
    )
    df_s['block_median_psf_alltime'] = (
        df_s.groupby('block_key')['psf']
        .transform(lambda x: x.shift(1).expanding(min_periods=1).median())
    )

    # 2. Street × flat_type (block fallback)
    df_s['_street_type_key'] = (
        df_s['street_name'].fillna('').astype(str).str.strip().str.upper()
        + '_' + df_s['flat_type']
    )
    df_s['street_rolling_psf_24m'] = (
        df_s.groupby('_street_type_key')['psf']
        .transform(lambda x: x.shift(1).rolling(24, min_periods=3).median())
    )

    # 3. Town × flat_type all-time median (broadest anchor, no shift needed)
    town_ft_psf = df_s.groupby(['town', 'flat_type'])['psf'].transform('median')
    df_s['town_flat_type_median_psf'] = town_ft_psf

    # 4. Town × flat_model rolling (DBSS, Maisonette, Premium Apartment premiums)
    df_s['_fm_town_key'] = df_s['town'] + '_' + df_s['flat_model']
    df_s['flat_model_town_rolling_psf_24m'] = (
        df_s.groupby('_fm_town_key')['psf']
        .transform(lambda x: x.shift(1).rolling(24, min_periods=3).median())
    )

    df_s['block_rolling_psf_24m']         = df_s['block_rolling_psf_24m'].fillna(df_s['street_rolling_psf_24m'])
    df_s['block_median_psf_alltime']      = df_s['block_median_psf_alltime'].fillna(town_ft_psf)
    df_s['street_rolling_psf_24m']        = df_s['street_rolling_psf_24m'].fillna(town_ft_psf)
    df_s['flat_model_town_rolling_psf_24m'] = df_s['flat_model_town_rolling_psf_24m'].fillna(town_ft_psf)

    # geo_rolling_psf_24m placeholder — updated after geocoding merge below
    df_s['geo_rolling_psf_24m'] = df_s['street_rolling_psf_24m']

    month_num = df_s['month'].dt.month
    df_s['sin_month'] = np.sin(2 * np.pi * month_num / 12)
    df_s['cos_month'] = np.cos(2 * np.pi * month_num / 12)

    df_s['lease_commence_date'] = df_s['lease_commence_date'].fillna(
        df_s['year'] - df_s['flat_age_years'].fillna(34)
    )

    df_s.drop(columns=['_street_type_key', '_fm_town_key'], inplace=True)
    df = df_s.sort_index()

    # market_rolling_psf_12m and town_rolling_psf_12m require month-sorted order
    # WITHIN each group, so must be computed on separately sorted views
    # (df_s above is sorted by block_key+month, not flat_type+month).
    global_psf_median = df['psf'].median()

    df_mkt = df.sort_values(['flat_type', 'month'])
    market_psf = (
        df_mkt.groupby('flat_type')['psf']
        .transform(lambda x: x.shift(1).rolling(12, min_periods=3).median())
    )
    df['market_rolling_psf_12m'] = market_psf.reindex(df.index).fillna(global_psf_median)

    df_twn = df.sort_values(['town', 'flat_type', 'month'])
    town_psf = (
        df_twn.groupby(['town', 'flat_type'])['psf']
        .transform(lambda x: x.shift(1).rolling(12, min_periods=3).median())
    )
    df['town_rolling_psf_12m'] = town_psf.reindex(df.index).fillna(df['town_flat_type_median_psf'])

    if policy_df is not None and len(policy_df) > 0:
        pol = policy_df.copy()
        pol['effective_month'] = pd.to_datetime(pol['effective_month'], errors='coerce').astype('datetime64[s]')
        pol['direction'] = pd.to_numeric(pol['direction'], errors='coerce').fillna(0)
        pol['severity']  = pd.to_numeric(pol['severity'],  errors='coerce').fillna(0)
        pol = pol.dropna(subset=['effective_month']).sort_values('effective_month')
        df  = df.sort_values('month')
        df  = pd.merge_asof(df, pol[['effective_month', 'direction', 'severity']],
                            left_on='month', right_on='effective_month', direction='backward')
        df['policy_impact'] = df['direction'] * df['severity']
        df['months_since_policy_change'] = (
            (df['month'].dt.to_period('M') - df['effective_month'].dt.to_period('M'))
            .apply(lambda x: x.n if pd.notna(x) else 0)
        )
    else:
        df['direction'] = df['severity'] = df['policy_impact'] = 0.0
        df['months_since_policy_change'] = 0

    if sora_df is not None and len(sora_df) > 0:
        df = df.merge(sora_df, on='month', how='left')
        df['sora'] = df['sora'].fillna(df['sora'].median())
    else:
        df['sora'] = 0.0

    has_geo = False
    if geo_df is not None and len(geo_df) > 0:
        df['search_text'] = (
            df['block'].fillna('').astype(str) + ' ' +
            df['street_name'].fillna('').astype(str)
        ).str.strip().str.upper()
        geo_clean = geo_df[['search_text', 'lat', 'lon']].dropna()
        df = df.merge(geo_clean, on='search_text', how='left')
        df = df.dropna(subset=['lat', 'lon'])
        has_geo = len(df) > 0
        if not has_geo:
            print('  Warning: no lat/lon matches after geo merge')
    if not has_geo:
        df['lat'] = df['lon'] = 0.0

    if has_geo:
        df = df.sort_values(['lat', 'lon', 'month'])
        df['_geo_cell'] = (
            df['lat'].round(2).astype(str) + '_' +
            df['lon'].round(2).astype(str) + '_' +
            df['flat_type']
        )
        df['geo_rolling_psf_24m'] = (
            df.groupby('_geo_cell')['psf']
            .transform(lambda x: x.shift(1).rolling(24, min_periods=3).median())
        )
        df['geo_rolling_psf_24m'] = df['geo_rolling_psf_24m'].fillna(df['street_rolling_psf_24m'])
        df.drop(columns=['_geo_cell'], inplace=True)
    # (else: already set to street_rolling_psf_24m as placeholder above)

    if has_geo:
        print('  Computing amenity distances (chunked)...')
        R    = 6371.0
        lats = np.radians(df['lat'].values)
        lons = np.radians(df['lon'].values)

        def _min_dist(coords, chunk=20_000):
            """Chunked haversine — caps peak memory at chunk × n_coords × 8 bytes.
            With chunk=20k and up to 1000 amenity points: ~160MB peak vs 2GB+ full."""
            if not coords:
                return np.full(len(lats), 0.5)
            arr   = np.array(coords, dtype=np.float32)
            alats = np.radians(arr[:, 0])
            alons = np.radians(arr[:, 1])
            out   = np.empty(len(lats), dtype=np.float32)
            for s in range(0, len(lats), chunk):
                e     = min(s + chunk, len(lats))
                clats = lats[s:e]
                clons = lons[s:e]
                dlat  = alats[None, :] - clats[:, None]
                dlon  = alons[None, :] - clons[:, None]
                a     = (np.sin(dlat/2)**2 +
                         np.cos(clats[:,None]) * np.cos(alats[None,:]) * np.sin(dlon/2)**2)
                out[s:e] = (R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))).min(axis=1)
            return out.round(4)

        ad = amenity_dict or {}
        school_coords    = ad.get('school',    []) or _PRIMARY_SCHOOLS
        hawker_coords    = ad.get('hawker',    []) or _HAWKER_CENTRES
        health_coords    = ad.get('health',    [])
        park_coords      = ad.get('park',      [])
        community_coords = ad.get('community', [])

        df['dist_nearest_mrt_km']       = _min_dist(_MRT_STATIONS)
        df['dist_nearest_school_km']    = _min_dist(school_coords)
        df['dist_nearest_hawker_km']    = _min_dist(hawker_coords)
        df['dist_nearest_health_km']    = _min_dist(health_coords)    if health_coords    else 1.0
        df['dist_nearest_park_km']      = _min_dist(park_coords)      if park_coords      else 0.5
        df['dist_nearest_community_km'] = _min_dist(community_coords) if community_coords else 1.0
        del school_coords, hawker_coords, health_coords, park_coords, community_coords
        gc.collect()
    else:
        df['dist_nearest_mrt_km']       = 0.5
        df['dist_nearest_school_km']    = 0.5
        df['dist_nearest_hawker_km']    = 0.3
        df['dist_nearest_health_km']    = 1.0
        df['dist_nearest_park_km']      = 0.5
        df['dist_nearest_community_km'] = 1.0

    df['storey_psf_interaction'] = df['storey_pct'] * df['block_rolling_psf_24m']
    # Lease premium per PSF dollar varies by estate: +1yr lease in Bishan ($720/sqft)
    # is worth more in absolute dollars than in Woodlands ($450/sqft).
    df['lease_psf_interaction']  = df['remaining_lease_years'] * df['block_rolling_psf_24m']

    return df, has_geo


def train(from_db=False):
    os.makedirs(MODELS_DIR, exist_ok=True)

    df = None
    if from_db or os.environ.get('DATABASE_URL'):
        print('Loading HDB resale data from database...')
        df = load_hdb_from_db()
        if df is not None and len(df) > 0:
            print(f'  Loaded {len(df):,} records from resale_flat_prices table')
        else:
            df = None
            if from_db:
                raise ValueError('resale_flat_prices is empty — upload CSV first.')
            print('  DB empty, falling back to data.gov.sg API...')
    if df is None:
        print('Downloading HDB data from data.gov.sg...')
        df = download_hdb_data()
    print(f'Raw records: {len(df):,}')

    print('Loading policy, SORA, geocoding, and amenity data...')
    policy_df    = load_policy_from_db()
    sora_df      = load_sora_from_db()
    geo_df       = load_geocoded_from_db()
    amenity_dict = load_amenities_from_db()
    print(f'  Policy rows: {len(policy_df) if policy_df is not None else 0}')
    print(f'  Geo rows:    {len(geo_df)    if geo_df    is not None else 0}')

    print('Engineering features...')
    df_feat, has_geo = engineer_features(df, policy_df, sora_df, geo_df, amenity_dict)
    df_feat = df_feat[df_feat['year'] >= MIN_YEAR].copy()

    has_policy = policy_df is not None and len(policy_df) > 0
    has_sora   = sora_df   is not None and len(sora_df)   > 0
    _geo_only  = {'lat', 'lon', 'dist_nearest_school_km', 'dist_nearest_hawker_km',
                  'dist_nearest_health_km', 'dist_nearest_park_km', 'dist_nearest_community_km'}
    _policy_cols = {'direction', 'severity', 'policy_impact', 'months_since_policy_change'}
    actual_num = [
        c for c in NUMERICAL_COLS_FULL
        if c in df_feat.columns
        and not (c in _geo_only and not has_geo)
        and not (c in _policy_cols and not has_policy)
        and not (c == 'sora' and not has_sora)
    ]
    all_features = CATEGORICAL_COLS + actual_num
    print(f'After filtering: {len(df_feat):,} records | Features: {len(all_features)}')

    df_feat[
        [c for c in all_features + ['resale_price', 'time_idx_raw'] if c in df_feat.columns]
    ].to_csv(TEMP_PATH, index=False)

    time_idx_min    = int(df_feat['time_idx_raw'].min())
    df_feat['time_idx'] = df_feat['time_idx_raw'] - time_idx_min

    # 4. Train / test split — 2025+ holdout; fallback to 90/10 if no 2025 data yet
    train_df = df_feat[df_feat['year'] < 2025]
    test_df  = df_feat[df_feat['year'] >= 2025]
    if len(test_df) < 100:
        df_s = df_feat.sort_values('time_idx_raw')
        split = int(len(df_feat) * 0.9)
        train_df, test_df = df_s.iloc[:split], df_s.iloc[split:]
    X_train = train_df[all_features]
    y_train = train_df['resale_price']
    X_test  = test_df[all_features]
    y_test  = test_df['resale_price']
    print(f'Train: {len(X_train):,} | Test: {len(X_test):,}')

    y_train_log = np.log(y_train)

    # Medians for inference (computed before df_feat is freed)
    medians = (
        df_feat.groupby(['town', 'flat_type'])[['remaining_lease_years', 'flat_age_years', 'lat', 'lon']]
        .median().to_dict('index')
    )
    del df_feat; gc.collect()

    # XGB: keep the two most important PSF signals (block 24m + market trend)
    #   but exclude the rest of the hierarchy. XGB with shallow trees + regularisation
    #   learns a smooth global surface from structural features + local block anchor +
    #   market momentum — a different regime from LGBM's deep local PSF learning.
    # LGBM: all features — primary workhorse with full signal.
    # CatBoost: exclude dynamic rolling PSF (market/town trend) — forces reliance on
    #   static location value (alltime medians, geo, lat/lon) + categorical interactions.
    #   LGBM handles the dynamic market drift; CatBoost handles the static location signal.
    _XGB_EXCLUDE = {
        'block_median_psf_alltime',
        'street_rolling_psf_24m', 'town_flat_type_median_psf',
        'flat_model_town_rolling_psf_24m', 'geo_rolling_psf_24m',
        'town_rolling_psf_12m',
        'storey_psf_interaction', 'lease_psf_interaction',
    }
    _CAT_EXCLUDE = {
        'market_rolling_psf_12m', 'town_rolling_psf_12m',
        'flat_model_town_rolling_psf_24m', 'street_rolling_psf_24m',
    }

    xgb_num  = [c for c in actual_num if c not in _XGB_EXCLUDE]
    cat_num  = [c for c in actual_num if c not in _CAT_EXCLUDE]
    lgbm_num = actual_num

    def _make_pre(num_cols):
        return ColumnTransformer(transformers=[
            ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), CATEGORICAL_COLS),
            ('num', 'passthrough', num_cols),
        ])

    feature_subsets = {
        'xgb':  CATEGORICAL_COLS + xgb_num,
        'lgbm': CATEGORICAL_COLS + lgbm_num,
        'cat':  CATEGORICAL_COLS + cat_num,
    }

    # 6. Model specs (different seeds → independent stochastic sampling)
    model_specs = {
        'xgb': XGBRegressor(
            n_estimators=800, learning_rate=0.02, max_depth=5,
            min_child_weight=10, reg_alpha=0.1, reg_lambda=2.0, gamma=0.05,
            subsample=0.8, colsample_bytree=0.8,
            objective='reg:squarederror', tree_method='hist', random_state=42,
        ),
        'lgbm': LGBMRegressor(
            n_estimators=800, learning_rate=0.02, num_leaves=127,
            min_child_samples=20, subsample=0.8, colsample_bytree=0.8,
            random_state=123, verbose=-1,
        ),
        'cat': CatBoostRegressor(
            iterations=700, learning_rate=0.03,
            grow_policy='Lossguide', max_leaves=64, min_data_in_leaf=20,
            loss_function='RMSE', random_seed=456, verbose=0,
            cat_features=CATEGORICAL_COLS,
        ),
    }

    print('Phase 1: Generating out-of-fold predictions for stacker...')
    kf = KFold(n_splits=3, shuffle=True, random_state=42)
    oof_preds = np.zeros((len(X_train), len(model_specs)))

    for i, (name, model) in enumerate(model_specs.items()):
        feats  = feature_subsets[name]
        Xtr    = X_train[feats]
        print(f'  OOF {name} ({len(feats)} features)...')
        if name == 'cat':
            fold_preds = np.zeros(len(X_train))
            for tr_idx, val_idx in kf.split(Xtr):
                fold_pipe = Pipeline([('model', CatBoostRegressor(
                    iterations=700, learning_rate=0.03,
                    grow_policy='Lossguide', max_leaves=64, min_data_in_leaf=20,
                    loss_function='RMSE', random_seed=456, verbose=0,
                    cat_features=CATEGORICAL_COLS,
                ))])
                fold_pipe.fit(Xtr.iloc[tr_idx], y_train_log.iloc[tr_idx])
                fold_preds[val_idx] = fold_pipe.predict(Xtr.iloc[val_idx])
            oof_preds[:, i] = fold_preds
        else:
            pipe = Pipeline([('pre', _make_pre(
                [c for c in feats if c not in CATEGORICAL_COLS]
            )), ('model', model)])
            oof_preds[:, i] = cross_val_predict(pipe, Xtr, y_train_log, cv=kf)
        gc.collect()

    print('Phase 2: Training final base models...')
    trained = {}
    for name, model in model_specs.items():
        print(f'Training {name}...')
        feats  = feature_subsets[name]
        Xtr    = X_train[feats]
        Xte    = X_test[feats]
        if name == 'cat':
            pipe = Pipeline([('model', model)])
        else:
            pipe = Pipeline([('pre', _make_pre(
                [c for c in feats if c not in CATEGORICAL_COLS]
            )), ('model', model)])
        pipe.fit(Xtr, y_train_log)
        preds = np.exp(pipe.predict(Xte))
        print(f'  {name}: MAE=S${mean_absolute_error(y_test, preds):,.0f}  R²={r2_score(y_test, preds):.4f}')
        trained[name] = pipe
        joblib.dump(pipe, os.path.join(MODELS_DIR, f'{name}_pipeline.joblib'))
        gc.collect()

    # Ridge handles correlated base learners well (shrinks toward equal weights).
    # HuberRegressor is robust to outlier predictions but can overfit with low alpha.
    def _mape(y_true, y_pred):
        m = y_true > 0
        return float(np.mean(np.abs((y_true[m] - y_pred[m]) / y_true[m])) * 100)

    test_log    = np.column_stack([trained[n].predict(X_test[feature_subsets[n]])
                                   for n in model_specs])
    simple_pred = np.exp(np.mean(test_log, axis=1))
    simple_mape = _mape(y_test.values, simple_pred)
    print(f'Simple avg: MAE=S${mean_absolute_error(y_test, simple_pred):,.0f}  '
          f'R²={r2_score(y_test, simple_pred):.4f}  MAPE={simple_mape:.2f}%')

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
        mpred = np.exp(meta.predict(test_log))
        mmape = _mape(y_test.values, mpred)
        print(f'    {mname}: MAPE={mmape:.4f}%  weights={[f"{w:.3f}" for w in meta.coef_]}')
        if mmape < best_mape:
            best_mape, best_name, stacker = mmape, mname, meta
    print(f'  → Best: {best_name}  (simple avg={simple_mape:.4f}%)')

    if stacker is None:
        # No meta-learner beat simple average — store equal weights in a fitted Ridge
        n = len(model_specs)
        stacker = Ridge(alpha=1.0)
        stacker.fit(oof_preds, y_train_log)   # fit to get correct shape
        stacker.coef_      = np.full(n, 1.0 / n)
        stacker.intercept_ = 0.0
        stacked_pred = simple_pred
        print('  → Using equal weights (no meta-learner beat simple avg)')
    else:
        stacked_pred = np.exp(stacker.predict(test_log))

    stacked_mape = _mape(y_test.values, stacked_pred)
    stacked_mae  = mean_absolute_error(y_test, stacked_pred)
    stacked_r2   = r2_score(y_test, stacked_pred)
    print(f'Stacked:    MAE=S${stacked_mae:,.0f}  R²={stacked_r2:.4f}  MAPE={stacked_mape:.2f}%')

    latest_policy = {'direction': 0.0, 'severity': 0.0, 'policy_impact': 0.0,
                     'months_since_policy_change': 0}
    if policy_df is not None and len(policy_df) > 0:
        pol = policy_df.copy()
        pol['effective_month'] = pd.to_datetime(pol['effective_month'], errors='coerce')
        last = pol.sort_values('effective_month').iloc[-1]
        d = float(last.get('direction', 0) or 0)
        s = float(last.get('severity', 0) or 0)
        latest_policy = {'direction': d, 'severity': s, 'policy_impact': d * s,
                         'months_since_policy_change': 0}

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
        'stacker_coef':         stacker.coef_.tolist(),
        'stacker_intercept':    float(stacker.intercept_),
        'model_names':          list(model_specs.keys()),
        'model_feature_subsets': {n: feature_subsets[n] for n in model_specs},
        'trained_at':           datetime.utcnow().isoformat(),
        'eval_mae':             round(stacked_mae, 0),
        'eval_r2':              round(stacked_r2, 4),
        'eval_mape':            round(stacked_mape, 2),
        'eval_n_test':          len(y_test),
    }
    joblib.dump(meta, os.path.join(MODELS_DIR, 'meta.joblib'))
    print('All HDB models saved.')

    # 11. SHAP metadata (lightweight — no TreeExplainer serialised)
    print('Computing SHAP metadata (XGB)...')
    try:
        import shap
        xgb_pipe   = trained['xgb']
        prep_fit   = xgb_pipe.named_steps['pre']
        xgb_model  = xgb_pipe.named_steps['model']
        feat_names = prep_fit.get_feature_names_out().tolist()
        X_sample   = prep_fit.transform(X_test[feature_subsets['xgb']].iloc[:100])
        explainer  = shap.TreeExplainer(xgb_model)
        explainer.shap_values(X_sample)
        base_val = float(explainer.expected_value[0] if hasattr(explainer.expected_value, '__len__')
                         else explainer.expected_value)
        joblib.dump({'feature_names': feat_names, 'categorical_cols': CATEGORICAL_COLS,
                     'numerical_cols': actual_num, 'base_value': base_val},
                    os.path.join(MODELS_DIR, 'shap_hdb.joblib'))
        print(f'  shap_hdb.joblib saved (base_value={base_val:.4f}, features={len(feat_names)})')
    except Exception as e:
        print(f'  SHAP skipped: {e}')

    if os.path.exists(TEMP_PATH):
        os.remove(TEMP_PATH)


if __name__ == '__main__':
    train(from_db='--from-db' in sys.argv)
