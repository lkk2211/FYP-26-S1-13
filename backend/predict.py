import os
import math
import joblib
import numpy as np
import pandas as pd
import requests
from datetime import datetime

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')

# ─── Bala's Curve (SISV standard) — synced with train_model.py ───────────────
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

# ─── MRT station coordinates — synced with train_model.py ────────────────────
_MRT_STATIONS = [
    (1.4474,103.7742),(1.4617,103.7875),(1.4739,103.8003),(1.4271,103.8384),
    (1.4041,103.8485),(1.3817,103.8449),(1.3620,103.8330),(1.3699,103.8486),
    (1.3514,103.8479),(1.3394,103.8443),(1.3263,103.8458),(1.3197,103.8442),
    (1.3101,103.8454),(1.3006,103.8365),(1.2970,103.8441),(1.2958,103.8523),
    (1.2831,103.8451),(1.2833,103.8530),(1.2784,103.8485),(1.2742,103.8510),
    (1.3799,103.7453),(1.3629,103.7456),(1.3693,103.7457),(1.3970,103.7479),
    (1.4323,103.7633),(1.4374,103.7870),(1.3290,103.8887),(1.3193,103.9021),
    (1.3143,103.9122),(1.3030,103.9022),(1.2967,103.9021),(1.2736,103.8456),
    (1.2759,103.8362),(1.2787,103.8193),(1.2909,103.8006),(1.2960,103.7899),
    (1.3113,103.7876),(1.3140,103.7756),(1.3031,103.7625),(1.3153,103.7655),
    (1.3337,103.7421),(1.3451,103.7028),(1.3424,103.6886),(1.3496,103.7227),
    (1.3374,103.7058),(1.3286,103.7000),(1.3352,103.9309),(1.3435,103.9486),
    (1.3518,103.9644),(1.3541,103.9825),(1.3600,103.9870),(1.3343,103.9158),
    (1.3202,103.9219),(1.2877,103.8456),(1.2800,103.8475),(1.2785,103.8319),
    (1.3017,103.8559),(1.3121,103.8649),(1.3214,103.8652),(1.3297,103.8749),
    (1.3392,103.8872),(1.3504,103.8938),(1.3621,103.8870),(1.3718,103.8819),
    (1.3897,103.8919),(1.3963,103.9012),(1.4063,103.9022),(1.2917,103.8574),
    (1.2996,103.8614),(1.3055,103.8558),(1.3060,103.8634),(1.3104,103.8789),
    (1.3092,103.8869),(1.3069,103.8940),(1.3340,103.9047),(1.3333,103.9023),
    (1.3606,103.8861),(1.3328,103.8252),(1.3197,103.8072),(1.3007,103.8010),
    (1.2971,103.7876),(1.2913,103.7812),(1.3059,103.7759),(1.2975,103.7883),
    (1.2930,103.7762),(1.2892,103.7628),(1.2834,103.7489),(1.2776,103.7646),
    (1.2716,103.7738),(1.2688,103.7848),(1.3424,103.7491),(1.3378,103.7499),
    (1.3317,103.7530),(1.3228,103.7631),(1.3240,103.7789),(1.3250,103.7905),
    (1.3260,103.8001),(1.3173,103.8063),(1.3079,103.8186),(1.3027,103.8321),
    (1.2996,103.8451),(1.2854,103.8451),(1.2788,103.8501),(1.2851,103.8631),
    (1.3103,103.9048),(1.3125,103.9317),(1.3155,103.9416),(1.3204,103.9500),
    (1.3518,103.9464),(1.3667,103.9309),(1.3583,103.9199),(1.3505,103.9101),
    (1.3370,103.9066),(1.4537,103.8185),(1.4474,103.8194),(1.4382,103.8395),
    (1.3884,103.8389),(1.3741,103.8322),(1.3611,103.8351),(1.3446,103.8330),
    (1.3176,103.8279),(1.3093,103.8356),(1.3080,103.8315),(1.2930,103.8453),
    (1.2885,103.8365),(1.2807,103.8399),(1.2767,103.8449),(1.2763,103.8630),
    (1.2847,103.8631),(1.3149,103.9302),(1.3204,103.9422),
]

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

_HAWKER_CENTRES = [
    # Central / Chinatown
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
    # Adam Rd / Old Airport Rd
    (1.3269,103.8144),(1.3137,103.8854),(1.3090,103.8820),
]


def _dist_nearest(lat, lon, coords):
    """Haversine distance in km to nearest point in coords list."""
    R = 6371.0
    lat_r = math.radians(lat)
    min_d = float('inf')
    for clat, clon in coords:
        dlat = math.radians(clat - lat)
        dlon = math.radians(clon - lon)
        a = (math.sin(dlat / 2) ** 2 +
             math.cos(lat_r) * math.cos(math.radians(clat)) * math.sin(dlon / 2) ** 2)
        d = R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        if d < min_d:
            min_d = d
    return round(min_d, 4)


def _dist_nearest_mrt(lat, lon):
    return _dist_nearest(lat, lon, _MRT_STATIONS)


# Cached at module load — amenities table is static during a server lifetime
_amenity_coords_cache: dict = {}

def _load_amenity_coords():
    """Load all amenity coordinates from DB, grouped by type. Cached after first call."""
    global _amenity_coords_cache
    if _amenity_coords_cache:
        return _amenity_coords_cache
    import json as _json
    result = {}
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL', '')
        if DATABASE_URL:
            import psycopg2, psycopg2.extras
            _c = psycopg2.connect(DATABASE_URL)
            _cur = _c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Primary source: amenities table
            _cur.execute("SELECT amenity_type, latitude, longitude FROM amenities WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
            for r in _cur.fetchall():
                t = str(r['amenity_type']).strip().lower()
                result.setdefault(t, []).append((float(r['latitude']), float(r['longitude'])))
            # Secondary source: amenity_cache blobs
            _cur.execute("SELECT data FROM amenity_cache WHERE data IS NOT NULL LIMIT 5000")
            for row in _cur.fetchall():
                try:
                    blob = _json.loads(row['data'])
                    for atype, items in blob.items():
                        if not isinstance(items, list):
                            continue
                        key = atype.strip().lower()
                        for item in items:
                            la, lo = item.get('lat'), item.get('lng')
                            if la and lo:
                                result.setdefault(key, []).append((float(la), float(lo)))
                except Exception:
                    continue
            _c.close()
            # Deduplicate
            for t in result:
                result[t] = list({(round(la, 4), round(lo, 4)) for la, lo in result[t]})
    except Exception as e:
        print(f'[predict] amenity load error: {e}')
    _amenity_coords_cache = result
    return result


def _amenity_distances(lat, lon):
    """Return (school, hawker, health, park, community) distances in km."""
    ad = _load_amenity_coords()
    school    = ad.get('school',    []) or _PRIMARY_SCHOOLS
    hawker    = ad.get('hawker',    []) or _HAWKER_CENTRES
    health    = ad.get('health',    [])
    park      = ad.get('park',      [])
    community = ad.get('community', [])
    return (
        _dist_nearest(lat, lon, school),
        _dist_nearest(lat, lon, hawker),
        _dist_nearest(lat, lon, health)    if health    else 1.0,
        _dist_nearest(lat, lon, park)      if park      else 0.5,
        _dist_nearest(lat, lon, community) if community else 1.0,
    )

# ─── Rule-based fallback (used when ML models are not available) ──────────────

POSTAL_CONFIG = {
    "238801": {
        "district": "D01", "location": "Marina Bay", "property_type": "Condominium",
        "base_psf": 2400, "mrt_score": 95, "market_state": "Very Active",
        "trend": "+3.1%", "trend_dir": "up",
        "insight": (
            "Marina Bay is Singapore's premier CBD waterfront district commanding top-tier premiums. "
            "Strong expat rental demand and limited new supply sustain high valuations year-on-year."
        ),
        "recommendation": (
            "Strong long-term hold. CBD condominiums historically appreciate 3–5% annually. "
            "Consider rental income potential of S$5,000–8,000/month for this unit size."
        )
    },
    "560123": {
        "district": "D19", "location": "Hougang", "property_type": "HDB",
        "base_psf": 520, "mrt_score": 78, "market_state": "Active",
        "trend": "+1.8%", "trend_dir": "up",
        "insight": (
            "Hougang is a mature HDB estate in the northeast with strong community infrastructure. "
            "The Cross Island Line expansion will further boost connectivity by 2030."
        ),
        "recommendation": (
            "Good mid-term buy. Hougang HDB prices are projected to appreciate 1.5–2.5% annually "
            "as CRL development progresses toward completion."
        )
    },
    "159088": {
        "district": "D03", "location": "Queenstown", "property_type": "HDB",
        "base_psf": 650, "mrt_score": 88, "market_state": "Very Active",
        "trend": "+2.6%", "trend_dir": "up",
        "insight": (
            "Queenstown commands a significant location premium as Singapore's oldest HDB town, "
            "adjacent to the Orchard Road corridor. Resale supply remains constrained."
        ),
        "recommendation": (
            "Strong hold or buy position. Central HDB prices are resilient. "
            "Current BTO pipeline may moderately temper resale prices in 12–18 months."
        )
    },
    "342005": {
        "district": "D12", "location": "Toa Payoh", "property_type": "HDB",
        "base_psf": 580, "mrt_score": 85, "market_state": "Active",
        "trend": "+2.1%", "trend_dir": "up",
        "insight": (
            "Toa Payoh is a highly sought-after central HDB town with direct MRT access and "
            "excellent amenities. It consistently ranks among the top-performing HDB resale towns."
        ),
        "recommendation": (
            "Hold position recommended. Toa Payoh's central location provides strong price stability. "
            "Look for 4–5 room units below S$650,000 for a good value entry point."
        )
    }
}

DEFAULT_CONFIG = {
    "district": "–", "location": "Singapore", "property_type": "HDB",
    "base_psf": 490, "mrt_score": 72, "market_state": "Active",
    "trend": "+1.5%", "trend_dir": "up",
    "insight": (
        "This location offers solid fundamentals with established amenities and transport links. "
        "The area benefits from government infrastructure investment and stable resale demand."
    ),
    "recommendation": (
        "Monitor market closely. Consider waiting for the next HDB BTO launch in the area "
        "or target resale units with remaining lease above 70 years for better financing options."
    )
}


def _build_forecast(base_price: int, annual_rate: float, months: int = 12):
    """Generate monthly price forecast list."""
    from datetime import date
    today = date.today()
    monthly_rate = annual_rate / 12
    prices = []
    p = base_price
    for i in range(1, months + 1):
        p = p * (1 + monthly_rate)
        mo = today.month + i
        yr = today.year + (mo - 1) // 12
        mo = ((mo - 1) % 12) + 1
        prices.append({"month": date(yr, mo, 1).strftime("%b '%y"), "price": int(p)})
    return prices


# ─── Planning area → HDB town normalisation ───────────────────────────────────

_PLANNING_TO_HDB_TOWN = {
    'KALLANG': 'KALLANG/WHAMPOA',
    'WHAMPOA': 'KALLANG/WHAMPOA',
    'DOWNTOWN CORE': 'CENTRAL AREA',
    'MUSEUM': 'CENTRAL AREA',
    'SINGAPORE RIVER': 'CENTRAL AREA',
    'ROCHOR': 'CENTRAL AREA',
    'MARINA SOUTH': 'CENTRAL AREA',
    'MARINA EAST': 'CENTRAL AREA',
    'OUTRAM': 'BUKIT MERAH',
    'RIVER VALLEY': 'CENTRAL AREA',
    'NOVENA': 'TOA PAYOH',
    'TANGLIN': 'BUKIT TIMAH',
    'BUONA VISTA': 'CLEMENTI',
    'WESTERN WATER CATCHMENT': 'JURONG WEST',
    'LIM CHU KANG': 'CHOA CHU KANG',
    'MANDAI': 'WOODLANDS',
    'CENTRAL WATER CATCHMENT': 'BISHAN',
    'NORTH-EASTERN ISLANDS': 'PASIR RIS',
    'SOUTHERN ISLANDS': 'CENTRAL AREA',
    'STRAITS VIEW': 'CENTRAL AREA',
    'TUAS': 'JURONG WEST',
    'PIONEER': 'JURONG WEST',
    'BOON LAY': 'JURONG WEST',
    'WESTERN ISLANDS': 'JURONG WEST',
}

_BEDROOMS_TO_FLAT_TYPE = {
    1: '1 ROOM',
    2: '2 ROOM',
    3: '3 ROOM',
    4: '4 ROOM',
    5: '5 ROOM',
}

_FLAT_TYPE_TO_MODEL = {
    '1 ROOM': 'IMPROVED',
    '2 ROOM': 'IMPROVED',
    '3 ROOM': 'NEW GENERATION',
    '4 ROOM': 'MODEL A',
    '5 ROOM': 'MODEL A',
    'EXECUTIVE': 'MAISONETTE',
    'MULTI-GENERATION': 'MULTI GENERATION',
}

# Approximate HDB town centroids for location display
_TOWN_DISPLAY = {
    'ANG MO KIO': 'Ang Mo Kio',
    'BEDOK': 'Bedok',
    'BISHAN': 'Bishan',
    'BUKIT BATOK': 'Bukit Batok',
    'BUKIT MERAH': 'Bukit Merah',
    'BUKIT PANJANG': 'Bukit Panjang',
    'BUKIT TIMAH': 'Bukit Timah',
    'CENTRAL AREA': 'Central Area',
    'CHOA CHU KANG': 'Choa Chu Kang',
    'CLEMENTI': 'Clementi',
    'GEYLANG': 'Geylang',
    'HOUGANG': 'Hougang',
    'JURONG EAST': 'Jurong East',
    'JURONG WEST': 'Jurong West',
    'KALLANG/WHAMPOA': 'Kallang/Whampoa',
    'MARINE PARADE': 'Marine Parade',
    'PASIR RIS': 'Pasir Ris',
    'PUNGGOL': 'Punggol',
    'QUEENSTOWN': 'Queenstown',
    'SEMBAWANG': 'Sembawang',
    'SENGKANG': 'Sengkang',
    'SERANGOON': 'Serangoon',
    'TAMPINES': 'Tampines',
    'TOA PAYOH': 'Toa Payoh',
    'WOODLANDS': 'Woodlands',
    'YISHUN': 'Yishun',
}


# ─── ML model loading (lazy, once) ────────────────────────────────────────────

_pipelines = None
_meta = None
_private_pipelines = None
_private_meta = None
_shap_hdb = None
_shap_private = None


def reset_model_cache():
    """Called after live retraining to force reload on next prediction."""
    global _pipelines, _meta, _private_pipelines, _private_meta, _shap_hdb, _shap_private
    _pipelines = None
    _meta = None
    _private_pipelines = None
    _private_meta = None
    _shap_hdb = None
    _shap_private = None
    print("[predict] Model cache reset — will reload fresh .joblib files on next prediction")


def _load_shap_hdb():
    """Load SHAP metadata and build TreeExplainer from the already-loaded XGB pipeline.
    The metadata file contains only plain Python objects (feature names, base_value).
    The TreeExplainer is reconstructed here to avoid joblib serialisation issues."""
    global _shap_hdb
    if _shap_hdb is not None:
        return True
    try:
        meta_path = os.path.join(MODELS_DIR, 'shap_hdb.joblib')
        if not os.path.exists(meta_path):
            return False
        meta = joblib.load(meta_path)
        # Reconstruct explainer from the already-loaded XGB pipeline
        import shap as _shap
        xgb_model = _pipelines[0].named_steps['model']
        explainer = _shap.TreeExplainer(xgb_model)
        _shap_hdb = {
            'explainer':        explainer,
            'feature_names':    meta['feature_names'],
            'categorical_cols': meta['categorical_cols'],
            'base_value':       meta['base_value'],
        }
        print("[predict] SHAP HDB explainer ready")
        return True
    except BaseException as e:
        print(f"[predict] SHAP HDB not available: {e}")
        return False


def _load_shap_private():
    """Load SHAP metadata and build TreeExplainer from the already-loaded private XGB pipeline."""
    global _shap_private
    if _shap_private is not None:
        return True
    try:
        meta_path = os.path.join(MODELS_DIR, 'shap_private.joblib')
        if not os.path.exists(meta_path):
            return False
        meta = joblib.load(meta_path)
        import shap as _shap
        xgb_model = _private_pipelines[0].named_steps['model']
        explainer = _shap.TreeExplainer(xgb_model)
        _shap_private = {
            'explainer':        explainer,
            'feature_names':    meta['feature_names'],
            'categorical_cols': meta['categorical_cols'],
            'base_value':       meta['base_value'],
        }
        print("[predict] SHAP Private explainer ready")
        return True
    except Exception as e:
        print(f"[predict] SHAP Private not available: {e}")
        return False


def _group_shap_values(shap_vals, feature_names, cat_cols):
    """
    Aggregate per-OHE-column SHAP values back to original feature groups.
    E.g. cat__town_HOUGANG + cat__town_BEDOK → 'town'
    Returns list of {name, value} sorted by abs(value) descending, top 8.
    """
    grouped = {}
    for val, fname in zip(shap_vals, feature_names):
        # Strip ColumnTransformer prefixes: 'cat__town_HOUGANG' → 'town'
        # or 'num__floor_area_sqm' → 'floor_area_sqm'
        if '__' in fname:
            raw = fname.split('__', 1)[1]
        else:
            raw = fname
        # For OHE columns, raw is 'town_HOUGANG' — strip the value suffix
        # by matching against known categorical col names
        group = raw
        for cat in cat_cols:
            if raw == cat or raw.startswith(cat + '_'):
                group = cat
                break
        grouped[group] = grouped.get(group, 0.0) + float(val)

    # Friendly display names
    _friendly = {
        'town': 'Town', 'flat_type': 'Flat Type', 'flat_model': 'Flat Model',
        'floor_area_sqm': 'Floor Area (sqm)', 'storey_mid': 'Floor Level',
        'remaining_lease_years': 'Remaining Lease', 'flat_age_years': 'Flat Age',
        'sora': 'Interest Rate (SORA)', 'policy_impact': 'Policy Impact',
        'direction': 'Policy Direction', 'severity': 'Policy Severity',
        'months_since_policy_change': 'Months Since Policy',
        'time_idx': 'Time Trend', 'year': 'Year', 'quarter': 'Quarter',
        'lat': 'Latitude', 'lon': 'Longitude',
        'property_type': 'Property Type', 'market_segment': 'Market Segment',
        'type_of_sale': 'Sale Type', 'postal_district': 'Postal District',
        'floor_area_sqft': 'Floor Area (sqft)', 'floor_level_num': 'Floor Level',
        'tenure_remaining_years': 'Tenure Remaining', 'is_strata': 'Strata Unit',
    }
    items = [
        {"name": _friendly.get(k, k.replace('_', ' ').title()), "value": round(v, 4)}
        for k, v in grouped.items()
    ]
    items.sort(key=lambda x: abs(x['value']), reverse=True)
    return items[:8]


def _load_models():
    global _pipelines, _meta
    if _pipelines is not None:
        return True
    try:
        xgb  = joblib.load(os.path.join(MODELS_DIR, 'xgb_pipeline.joblib'))
        meta = joblib.load(os.path.join(MODELS_DIR, 'meta.joblib'))

        # Attempt to load additional models only when LOAD_ALL_MODELS=1 is set
        # (requires ≥2GB RAM; free/starter Render tier is 512MB — XGB alone fits)
        load_all = os.environ.get('LOAD_ALL_MODELS', '0') == '1'
        loaded = {'xgb': xgb}
        if load_all:
            for name, fname in [('lgbm', 'lgbm_pipeline.joblib'), ('cat', 'cat_pipeline.joblib')]:
                try:
                    loaded[name] = joblib.load(os.path.join(MODELS_DIR, fname))
                    print(f"[predict] Loaded {fname}")
                except Exception as ex:
                    print(f"[predict] Skipping {fname}: {ex}")

        # Build pipeline list in training order so stacker coefficients align
        training_order = meta.get('model_names') or ['xgb', 'lgbm', 'cat']
        _pipelines = [loaded[n] for n in training_order if n in loaded]


        # Narrow stacker coefficients to only the models that actually loaded
        stacker_coef = meta.get('stacker_coef')
        if stacker_coef and len(stacker_coef) == len(training_order):
            kept_indices = [i for i, n in enumerate(training_order) if n in loaded]
            meta = dict(meta)
            meta['stacker_coef'] = [stacker_coef[i] for i in kept_indices]

        _meta = meta
        pol, sor = _load_latest_policy_sora()
        if pol:   _meta['latest_policy'] = pol
        if sor is not None: _meta['latest_sora'] = sor
        print(f"[predict] HDB ML models loaded: {list(loaded.keys())}")
        return True
    except Exception as e:
        print(f"[predict] HDB ML models not available: {e}")
        return False


def _load_private_models():
    global _private_pipelines, _private_meta
    if _private_pipelines is not None:
        return True
    try:
        xgb  = joblib.load(os.path.join(MODELS_DIR, 'xgb_private_pipeline.joblib'))
        meta = joblib.load(os.path.join(MODELS_DIR, 'meta_private.joblib'))

        # Attempt to load additional models only when LOAD_ALL_MODELS=1 is set
        load_all = os.environ.get('LOAD_ALL_MODELS', '0') == '1'
        loaded = {'xgb': xgb}
        if load_all:
            for name, fname in [('lgbm', 'lgbm_private_pipeline.joblib'), ('cat', 'cat_private_pipeline.joblib')]:
                try:
                    loaded[name] = joblib.load(os.path.join(MODELS_DIR, fname))
                    print(f"[predict] Loaded {fname}")
                except Exception as ex:
                    print(f"[predict] Skipping {fname}: {ex}")

        # Build pipeline list in training order so stacker coefficients align
        # Normalise names: private model meta stores 'xgb_private' but loaded dict uses 'xgb'
        raw_order    = meta.get('model_names') or ['xgb', 'lgbm', 'cat']
        training_order = [n.replace('_private', '') for n in raw_order]
        _private_pipelines = [loaded[n] for n in training_order if n in loaded]

        # Narrow stacker coefficients to only the models that actually loaded
        stacker_coef = meta.get('stacker_coef')
        if stacker_coef and len(stacker_coef) == len(training_order):
            kept_indices = [i for i, n in enumerate(training_order) if n in loaded]
            meta = dict(meta)
            meta['stacker_coef'] = [stacker_coef[i] for i in kept_indices]

        _private_meta = meta
        pol, sor = _load_latest_policy_sora()
        if pol:   _private_meta['latest_policy'] = pol
        if sor is not None: _private_meta['latest_sora'] = sor
        print(f"[predict] Private ML models loaded: {list(loaded.keys())}")
        return True
    except Exception as e:
        print(f"[predict] Private ML models not available: {e}")
        return False


# ─── OneMap geocoding ─────────────────────────────────────────────────────────

def _load_latest_policy_sora():
    """Try to load latest policy and SORA from DB for inference."""
    try:
        import os
        DATABASE_URL = os.environ.get('DATABASE_URL', '')
        if DATABASE_URL:
            import psycopg2, psycopg2.extras
            conn = psycopg2.connect(DATABASE_URL)
            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            ph   = '%s'
        else:
            import sqlite3
            DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'propaisg.db')
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            cur  = conn.cursor()
            ph   = '?'

        # Latest policy
        cur.execute("SELECT direction, severity FROM policy_changes WHERE effective_month IS NOT NULL ORDER BY effective_month DESC LIMIT 1")
        pol = cur.fetchone()
        if pol:
            pol = dict(pol)
            d, s = float(pol.get('direction') or 0), float(pol.get('severity') or 0)
            policy_vals = {'direction': d, 'severity': s, 'policy_impact': d * s,
                           'months_since_policy_change': 0}
        else:
            policy_vals = None

        # Latest SORA
        cur.execute("SELECT published_rate FROM sora_rates WHERE rate_date IS NOT NULL ORDER BY rate_date DESC LIMIT 1")
        sor = cur.fetchone()
        sora_val = float(dict(sor).get('published_rate', 3.5)) if sor else None
        conn.close()
        return policy_vals, sora_val
    except Exception:
        return None, None


def _geocode_postal(postal):
    """Returns (town_upper, lat, lon) or (None, None, None)."""
    try:
        r = requests.get(
            'https://www.onemap.gov.sg/api/common/elastic/search',
            params={
                'searchVal': postal,
                'returnGeom': 'Y',
                'getAddrDetails': 'Y',
                'pageNum': 1,
            },
            timeout=6,
        )
        results = r.json().get('results', [])
        if not results:
            return None, None, None
        res = results[0]
        lat = float(res.get('LATITUDE', 0) or 0)
        lon = float(res.get('LONGITUDE', res.get('LONGTITUDE', 0)) or 0)
        if lat == 0 and lon == 0:
            return None, None, None

        # Get planning area from coordinates
        pa_resp = requests.get(
            'https://www.onemap.gov.sg/api/public/popapi/getPlanningarea',
            params={'lat': lat, 'lon': lon},
            timeout=6,
        )
        pa_data = pa_resp.json()
        # Response is a list
        if isinstance(pa_data, list) and pa_data:
            pa = pa_data[0].get('pln_area_n', '').strip().upper()
        else:
            pa = pa_data.get('pln_area_n', '').strip().upper()

        town = _PLANNING_TO_HDB_TOWN.get(pa, pa) if pa else None
        return town, lat, lon
    except Exception:
        return None, None, None


# ─── ML prediction ────────────────────────────────────────────────────────────

def _predict_ml(features):
    postal    = str(features.get('postal', '')).strip().zfill(6)
    area_sqm  = float(features.get('area', 90))   # incoming value is sqm
    bedrooms  = int(features.get('bedrooms', 3))
    floor     = int(features.get('floor', 10))

    area_sqft = area_sqm * 10.764

    # Accept flat_type directly (from dropdown) or derive from bedrooms
    flat_type_in = str(features.get('flat_type', '')).strip().upper()
    if flat_type_in and flat_type_in in _FLAT_TYPE_TO_MODEL:
        flat_type = flat_type_in
    else:
        flat_type = _BEDROOMS_TO_FLAT_TYPE.get(bedrooms, 'EXECUTIVE' if bedrooms >= 6 else '5 ROOM')
    flat_model = _FLAT_TYPE_TO_MODEL.get(flat_type, 'MODEL A')

    # Geocode → town + lat/lon; fall back to town provided by frontend if geocoding fails
    provided_town = str(features.get('town', '')).strip().upper()
    town, lat, lon = _geocode_postal(postal)
    if not town:
        if provided_town:
            town = provided_town   # use town from property_lookup (already resolved on the frontend)
            lat, lon = None, None
        else:
            return None  # fall back to rule-based

    # Time features
    now = datetime.now()
    year = now.year
    quarter = (now.month - 1) // 3 + 1
    time_idx_raw = year * 12 + now.month
    time_idx = time_idx_raw - _meta['time_idx_min']

    storey_mid = float(floor)

    # Lease/age — use actual property value from frontend if available, else training median
    key = (town, flat_type)
    med = _meta['medians_by_town_type'].get(key, {})
    _actual_lease = features.get('remaining_lease_years')
    remaining_lease_years = (float(_actual_lease)
                             if _actual_lease is not None and float(_actual_lease) > 0
                             else float(med.get('remaining_lease_years', 65.0)))
    flat_age_years        = float(med.get('flat_age_years', 34.0))
    # Derive lease_commence_date: for a 99-year HDB lease, commence = current_year + remaining - 99
    lease_commence_date   = float(features.get('lease_commence_year') or
                                  round(year + remaining_lease_years - 99))
    lat_med = float(med.get('lat', lat or 1.35))
    lon_med = float(med.get('lon', lon or 103.82))
    eff_lat = lat or lat_med
    eff_lon = lon or lon_med

    # Latest policy + SORA (from meta, refreshed from DB at load time)
    pol = _meta.get('latest_policy', {})
    sora = float(_meta.get('latest_sora', 3.5))

    # ── New accuracy features ─────────────────────────────────────────────────
    # 1. Bala's non-linear lease fraction
    bala_frac = _bala_fraction(remaining_lease_years)

    # 2. Distance to nearest MRT (km)
    dist_mrt    = _dist_nearest_mrt(eff_lat, eff_lon)
    dist_school, dist_hawker, dist_health, dist_park, dist_community = \
        _amenity_distances(eff_lat, eff_lon)

    # 3. Storey % of building height — use max_floor from request if provided
    max_floor_hint = features.get('max_floor')
    if max_floor_hint and float(max_floor_hint) > 0:
        storey_pct = min(float(floor) / float(max_floor_hint), 1.0)
    else:
        # Approximate: assume typical HDB = 12 floors, condo = 20
        storey_pct = min(float(floor) / 12.0, 1.0)

    # 4. Block-level rolling PSF — query DB for recent block transactions.
    # Two signals: 24-month rolling median and all-time median (stable anchor).
    block_rolling_psf_24m    = None
    block_median_psf_alltime = None
    blk = str(features.get('block', '')).strip()
    if blk:
        try:
            DATABASE_URL = os.environ.get('DATABASE_URL', '')
            if DATABASE_URL:
                import psycopg2, psycopg2.extras
                _c = psycopg2.connect(DATABASE_URL)
                _cur = _c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                ph = '%s'
            else:
                import sqlite3
                _c = sqlite3.connect(os.path.join(os.path.dirname(__file__), 'propaisg.db'))
                _c.row_factory = sqlite3.Row
                _cur = _c.cursor()
                ph = '?'
            # 24-month rolling: last 25 months excluding current
            if DATABASE_URL:
                _cur.execute(
                    "SELECT AVG(CAST(resale_price AS REAL) / (CAST(floor_area_sqm AS REAL) * 10.764)) "
                    "AS median_psf FROM resale_flat_prices "
                    "WHERE UPPER(block) = %s AND UPPER(flat_type) = %s "
                    "AND month >= (CURRENT_DATE - INTERVAL '25 months') "
                    "AND month < (CURRENT_DATE - INTERVAL '1 month')",
                    (blk.upper(), flat_type)
                )
            else:
                _cur.execute(
                    "SELECT AVG(CAST(resale_price AS REAL) / (CAST(floor_area_sqm AS REAL) * 10.764)) "
                    "AS median_psf FROM resale_flat_prices "
                    "WHERE UPPER(block) = ? AND UPPER(flat_type) = ? "
                    "AND month >= date('now', '-25 months') "
                    "AND month < date('now', '-1 months')",
                    (blk.upper(), flat_type)
                )
            row_psf = _cur.fetchone()
            if row_psf:
                v = dict(row_psf).get('median_psf')
                if v:
                    block_rolling_psf_24m = float(v)
            # All-time median
            _cur.execute(
                f"SELECT AVG(CAST(resale_price AS REAL) / (CAST(floor_area_sqm AS REAL) * 10.764)) "
                f"AS median_psf FROM resale_flat_prices "
                f"WHERE UPPER(block) = {ph} AND UPPER(flat_type) = {ph}",
                (blk.upper(), flat_type)
            )
            row_alltime = _cur.fetchone()
            if row_alltime:
                v2 = dict(row_alltime).get('median_psf')
                if v2:
                    block_median_psf_alltime = float(v2)
            _c.close()
        except Exception:
            pass
    # Fall back to town PSF benchmarks when block history is unavailable
    if block_rolling_psf_24m is None or block_rolling_psf_24m <= 0:
        _town_psf_bench = {
            'BISHAN': 750, 'TOA PAYOH': 700, 'QUEENSTOWN': 730, 'BUKIT MERAH': 680,
            'KALLANG/WHAMPOA': 670, 'MARINE PARADE': 700, 'ANG MO KIO': 600,
            'SERANGOON': 590, 'TAMPINES': 545, 'BEDOK': 530, 'HOUGANG': 520,
            'SENGKANG': 500, 'PUNGGOL': 490, 'WOODLANDS': 465, 'YISHUN': 460,
            'JURONG WEST': 470, 'CHOA CHU KANG': 480, 'BUKIT PANJANG': 490,
        }
        block_rolling_psf_24m = float(_town_psf_bench.get(town, 520))
    if block_median_psf_alltime is None or block_median_psf_alltime <= 0:
        block_median_psf_alltime = block_rolling_psf_24m

    # Street × flat_type 24-month rolling PSF and town × flat_type all-time median
    street_name = str(features.get('street_name', '')).strip().upper()
    street_rolling_psf_24m   = None
    town_flat_type_median_psf = None
    if street_name:
        try:
            DATABASE_URL = os.environ.get('DATABASE_URL', '')
            if DATABASE_URL:
                import psycopg2, psycopg2.extras
                _cs = psycopg2.connect(DATABASE_URL)
                _curs = _cs.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                _curs.execute(
                    "SELECT AVG(CAST(resale_price AS REAL) / (CAST(floor_area_sqm AS REAL) * 10.764)) AS psf "
                    "FROM resale_flat_prices "
                    "WHERE UPPER(street_name) = %s AND UPPER(flat_type) = %s "
                    "AND month >= (CURRENT_DATE - INTERVAL '25 months') "
                    "AND month < (CURRENT_DATE - INTERVAL '1 month')",
                    (street_name, flat_type)
                )
            else:
                import sqlite3
                _cs = sqlite3.connect(os.path.join(os.path.dirname(__file__), 'propaisg.db'))
                _cs.row_factory = sqlite3.Row
                _curs = _cs.cursor()
                _curs.execute(
                    "SELECT AVG(CAST(resale_price AS REAL) / (CAST(floor_area_sqm AS REAL) * 10.764)) AS psf "
                    "FROM resale_flat_prices "
                    "WHERE UPPER(street_name) = ? AND UPPER(flat_type) = ? "
                    "AND month >= date('now', '-25 months') AND month < date('now', '-1 months')",
                    (street_name, flat_type)
                )
            r = _curs.fetchone()
            if r:
                v = dict(r).get('psf')
                if v:
                    street_rolling_psf_24m = float(v)
            # Town × flat_type all-time median
            if DATABASE_URL:
                _curs.execute(
                    "SELECT AVG(CAST(resale_price AS REAL) / (CAST(floor_area_sqm AS REAL) * 10.764)) AS psf "
                    "FROM resale_flat_prices WHERE UPPER(town) = %s AND UPPER(flat_type) = %s",
                    (town, flat_type)
                )
            else:
                _curs.execute(
                    "SELECT AVG(CAST(resale_price AS REAL) / (CAST(floor_area_sqm AS REAL) * 10.764)) AS psf "
                    "FROM resale_flat_prices WHERE UPPER(town) = ? AND UPPER(flat_type) = ?",
                    (town, flat_type)
                )
            r2 = _curs.fetchone()
            if r2:
                v2 = dict(r2).get('psf')
                if v2:
                    town_flat_type_median_psf = float(v2)
            _cs.close()
        except Exception:
            pass
    if street_rolling_psf_24m is None or street_rolling_psf_24m <= 0:
        street_rolling_psf_24m = block_rolling_psf_24m
    if town_flat_type_median_psf is None or town_flat_type_median_psf <= 0:
        town_flat_type_median_psf = block_rolling_psf_24m

    # Town × flat_type 12-month rolling PSF and market-wide 12-month rolling PSF
    town_rolling_psf_12m   = None
    market_rolling_psf_12m = None
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL', '')
        if DATABASE_URL:
            import psycopg2, psycopg2.extras
            _ctm = psycopg2.connect(DATABASE_URL)
            _ctmcur = _ctm.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            _ctmcur.execute(
                "SELECT AVG(CAST(resale_price AS REAL)/(CAST(floor_area_sqm AS REAL)*10.764)) AS psf "
                "FROM resale_flat_prices "
                "WHERE UPPER(town)=%s AND UPPER(flat_type)=%s "
                "AND month>=(CURRENT_DATE-INTERVAL '13 months') AND month<(CURRENT_DATE-INTERVAL '1 month')",
                (town, flat_type)
            )
            r = _ctmcur.fetchone()
            if r:
                v = dict(r).get('psf')
                if v: town_rolling_psf_12m = float(v)
            _ctmcur.execute(
                "SELECT AVG(CAST(resale_price AS REAL)/(CAST(floor_area_sqm AS REAL)*10.764)) AS psf "
                "FROM resale_flat_prices "
                "WHERE UPPER(flat_type)=%s "
                "AND month>=(CURRENT_DATE-INTERVAL '13 months') AND month<(CURRENT_DATE-INTERVAL '1 month')",
                (flat_type,)
            )
            r2 = _ctmcur.fetchone()
            if r2:
                v2 = dict(r2).get('psf')
                if v2: market_rolling_psf_12m = float(v2)
            _ctm.close()
    except Exception:
        pass
    if not town_rolling_psf_12m or town_rolling_psf_12m <= 0:
        town_rolling_psf_12m = town_flat_type_median_psf
    if not market_rolling_psf_12m or market_rolling_psf_12m <= 0:
        market_rolling_psf_12m = town_flat_type_median_psf

    # Flat model × town rolling PSF (DBSS, Maisonette, premium premiums)
    flat_model = str(features.get('flat_model', '')).strip().upper()
    flat_model_town_rolling_psf_24m = None
    if flat_model and town:
        try:
            DATABASE_URL = os.environ.get('DATABASE_URL', '')
            if DATABASE_URL:
                import psycopg2, psycopg2.extras
                _cfm = psycopg2.connect(DATABASE_URL)
                _cfmcur = _cfm.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                _cfmcur.execute(
                    "SELECT AVG(CAST(resale_price AS REAL) / (CAST(floor_area_sqm AS REAL) * 10.764)) AS psf "
                    "FROM resale_flat_prices "
                    "WHERE UPPER(town) = %s AND UPPER(flat_model) = %s "
                    "AND month >= (CURRENT_DATE - INTERVAL '25 months') "
                    "AND month < (CURRENT_DATE - INTERVAL '1 month')",
                    (town, flat_model)
                )
            else:
                import sqlite3
                _cfm = sqlite3.connect(os.path.join(os.path.dirname(__file__), 'propaisg.db'))
                _cfm.row_factory = sqlite3.Row
                _cfmcur = _cfm.cursor()
                _cfmcur.execute(
                    "SELECT AVG(CAST(resale_price AS REAL) / (CAST(floor_area_sqm AS REAL) * 10.764)) AS psf "
                    "FROM resale_flat_prices "
                    "WHERE UPPER(town) = ? AND UPPER(flat_model) = ? "
                    "AND month >= date('now', '-25 months') AND month < date('now', '-1 months')",
                    (town, flat_model)
                )
            rfm = _cfmcur.fetchone()
            if rfm:
                vfm = dict(rfm).get('psf')
                if vfm:
                    flat_model_town_rolling_psf_24m = float(vfm)
            _cfm.close()
        except Exception:
            pass
    if flat_model_town_rolling_psf_24m is None or flat_model_town_rolling_psf_24m <= 0:
        flat_model_town_rolling_psf_24m = town_flat_type_median_psf

    # Geo-grid rolling PSF (~1km × 1km bin × flat_type)
    geo_rolling_psf_24m = None
    if eff_lat and eff_lon:
        try:
            lat_bin = round(eff_lat, 2)
            lon_bin = round(eff_lon, 2)
            DATABASE_URL = os.environ.get('DATABASE_URL', '')
            if DATABASE_URL:
                import psycopg2, psycopg2.extras
                _cg = psycopg2.connect(DATABASE_URL)
                _cgcur = _cg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                _cgcur.execute(
                    "SELECT AVG(CAST(resale_price AS REAL) / (CAST(floor_area_sqm AS REAL) * 10.764)) AS psf "
                    "FROM resale_flat_prices r "
                    "JOIN geocoded_addresses g ON UPPER(CONCAT(r.block,' ',r.street_name)) = UPPER(g.search_text) "
                    "WHERE ROUND(CAST(g.lat AS NUMERIC), 2) = %s AND ROUND(CAST(g.lon AS NUMERIC), 2) = %s "
                    "AND UPPER(r.flat_type) = %s "
                    "AND r.month >= (CURRENT_DATE - INTERVAL '25 months') "
                    "AND r.month < (CURRENT_DATE - INTERVAL '1 month')",
                    (lat_bin, lon_bin, flat_type)
                )
                rg = _cgcur.fetchone()
                if rg:
                    vg = dict(rg).get('psf')
                    if vg:
                        geo_rolling_psf_24m = float(vg)
                _cg.close()
        except Exception:
            pass
    if geo_rolling_psf_24m is None or geo_rolling_psf_24m <= 0:
        geo_rolling_psf_24m = street_rolling_psf_24m

    # Cyclic month encoding
    import math as _math
    _cur_month = datetime.now().month
    sin_month = _math.sin(2 * _math.pi * _cur_month / 12)
    cos_month = _math.cos(2 * _math.pi * _cur_month / 12)

    # Build feature row using exactly the columns the model was trained on
    num_cols = _meta.get('numerical_cols', [])
    feat = {
        'town': town, 'flat_type': flat_type, 'flat_model': flat_model,
        'floor_area_sqm':          area_sqm,
        'year':                    year,
        'quarter':                 quarter,
        'time_idx':                time_idx,
        'storey_mid':              storey_mid,
        'storey_pct':              storey_pct,
        'remaining_lease_years':   remaining_lease_years,
        'flat_age_years':          flat_age_years,
        'bala_fraction':           bala_frac,
        'direction':               float(pol.get('direction', 0)),
        'severity':                float(pol.get('severity', 0)),
        'policy_impact':           float(pol.get('policy_impact', 0)),
        'months_since_policy_change': int(pol.get('months_since_policy_change', 0)),
        'sora':                    sora,
        'lat':                     eff_lat,
        'lon':                     eff_lon,
        'dist_nearest_mrt_km':       dist_mrt,
        'dist_nearest_school_km':    dist_school,
        'dist_nearest_hawker_km':    dist_hawker,
        'dist_nearest_health_km':    dist_health,
        'dist_nearest_park_km':      dist_park,
        'dist_nearest_community_km': dist_community,
        'lease_commence_date':               lease_commence_date,
        'storey_psf_interaction':            storey_pct * block_rolling_psf_24m,
        'lease_psf_interaction':             remaining_lease_years * block_rolling_psf_24m,
        'block_rolling_psf_24m':             block_rolling_psf_24m,
        'block_median_psf_alltime':          block_median_psf_alltime,
        'street_rolling_psf_24m':            street_rolling_psf_24m,
        'town_flat_type_median_psf':         town_flat_type_median_psf,
        'town_rolling_psf_12m':              town_rolling_psf_12m,
        'market_rolling_psf_12m':            market_rolling_psf_12m,
        'flat_model_town_rolling_psf_24m':   flat_model_town_rolling_psf_24m,
        'geo_rolling_psf_24m':               geo_rolling_psf_24m,
        'sin_month':                         sin_month,
        'cos_month':                         cos_month,
    }
    # Each model was trained on a different feature subset — use per-model subsets.
    model_names    = _meta.get('model_names', ['xgb', 'lgbm', 'cat'])
    feat_subsets   = _meta.get('model_feature_subsets', {})
    preds_log = []
    for mname, pipeline in zip(model_names, _pipelines):
        mfeats = feat_subsets.get(mname, _meta.get('categorical_cols', []) + num_cols)
        mrow   = pd.DataFrame([{k: feat[k] for k in mfeats if k in feat}])
        preds_log.append(pipeline.predict(mrow)[0])

    stacker_coef = _meta.get('stacker_coef')
    stacker_int  = float(_meta.get('stacker_intercept', 0.0))
    if stacker_coef and len(stacker_coef) == len(preds_log):
        ensemble_log = float(np.dot(preds_log, stacker_coef)) + stacker_int
    else:
        ensemble_log = float(np.mean(preds_log))
    estimated_value = int(np.exp(ensemble_log))

    # ── SHAP contributions (lazy load) ────────────────────────────────────────
    shap_contributions = None
    try:
        if _load_shap_hdb():
            xgb_pipe = _pipelines[0]
            preprocessor = xgb_pipe.named_steps['preprocessor']
            transformed  = preprocessor.transform(row)
            sv = _shap_hdb['explainer'].shap_values(transformed)
            if sv is not None and len(sv) > 0:
                shap_contributions = _group_shap_values(
                    sv[0], _shap_hdb['feature_names'], _shap_hdb['categorical_cols']
                )
    except Exception as _se:
        print(f"[predict] SHAP HDB inference skipped: {_se}")

# --- MARKET ALIGNMENT: TWO INDEPENDENT SIGNALS ---
    # Signal 1 — intra-model spread (Tree agreement signal)
    xgb_pipe = _pipelines[0]
    try:
        booster   = xgb_pipe.named_steps['model'].get_booster()
        n_trees   = booster.num_boosted_rounds()
        prep_row  = xgb_pipe.named_steps['preprocessor'].transform(row)
        import xgboost as _xgb
        dmat = _xgb.DMatrix(prep_row)
        
        if n_trees >= 4:
            half = n_trees // 2
            pred_first  = float(np.exp(booster.predict(dmat, iteration_range=(0, half), output_margin=True)[0]))
            pred_second = float(np.exp(booster.predict(dmat, iteration_range=(half, n_trees), output_margin=True)[0]))
            tree_spread = abs(pred_first - pred_second)
            cv = tree_spread / estimated_value
        else:
            spread = float(np.std([np.exp(v) for v in preds_log]))
            cv = spread / estimated_value
    except Exception:
        spread = float(np.std([np.exp(v) for v in preds_log]))
        cv = spread / estimated_value

    # Recalibrated Baseline: 100 - MAPE (7.25%) = 92.75%
    base_alignment = round(92.75 - cv * 180, 1)

    # Signal 2 — block-level data density
    density_adj = 0.0
    n_local = 0
    if blk:
        try:
            DATABASE_URL = os.environ.get('DATABASE_URL', '')
            if DATABASE_URL:
                import psycopg2, psycopg2.extras as _pge
                _dc = psycopg2.connect(DATABASE_URL)
                _dcu = _dc.cursor(cursor_factory=_pge.RealDictCursor)
                _dcu.execute(
                    "SELECT COUNT(*) AS n FROM resale_flat_prices "
                    "WHERE UPPER(block) = %s AND UPPER(flat_type) = %s "
                    "AND month >= (CURRENT_DATE - INTERVAL '24 months')::text",
                    (blk.upper(), flat_type)
                )
            else:
                import sqlite3 as _sq
                _dc = _sq.connect(os.path.join(os.path.dirname(__file__), 'propaisg.db'))
                _dc.row_factory = _sq.Row
                _dcu = _dc.cursor()
                _dcu.execute(
                    "SELECT COUNT(*) AS n FROM resale_flat_prices "
                    "WHERE UPPER(block) = ? AND UPPER(flat_type) = ? "
                    "AND month >= date('now', '-24 months')",
                    (blk.upper(), flat_type)
                )
            _dr = _dcu.fetchone()
            n_local = int(dict(_dr).get('n', 0)) if _dr else 0
            _dc.close()
            # 0 txns → −4 pts | 10 txns → 0 pts | 30+ txns → +3 pts
            density_adj = max(-4.0, min(3.0, (n_local - 10) * 0.35))
        except Exception:
            pass

    final_score = round(min(98.5, base_alignment + density_adj), 1)

    # Return structured object for frontend
    return {
        "estimated_value": estimated_value,
        "market_alignment": {
            "score": final_score,
            "label": "High" if final_score > 90 else "Reliable",
            "records_analyzed": n_local
        }
    }

    # Signal 3 — floor extrapolation penalty
    # If the requested floor exceeds the highest transacted floor across block + nearby siblings,
    # the model is extrapolating with no local evidence — reduce confidence accordingly
    floor_extrapolated = False
    extrapolation_penalty = 0.0
    max_transacted_floor = features.get('max_transacted_floor')
    if max_transacted_floor and float(max_transacted_floor) > 0:
        if float(floor) > float(max_transacted_floor):
            overshoot = float(floor) - float(max_transacted_floor)
            extrapolation_penalty = min(8.0, overshoot * 0.5)
            floor_extrapolated = True

    confidence = round(max(70.0, min(95.0, base_conf + density_adj - extrapolation_penalty)), 1)

    min_value = int(estimated_value * 0.92)
    max_value = int(estimated_value * 1.08)

    location_display = _TOWN_DISPLAY.get(town, town.title())

    # Factor scores
    floor_score = min(int(40 + floor * 1.2), 98)
    lease_score = min(int(remaining_lease_years * 1.3), 98)
    area_score  = min(int(50 + (area_sqm - 50) * 0.5), 98)

    factors = [
        {
            "name": "Market Demand",
            "score": 80,
            "label": "High",
            "desc": f"Active buyer interest in {location_display} HDB resale market."
        },
        {
            "name": "Floor Level Premium",
            "score": floor_score,
            "label": "Very High" if floor_score >= 85 else ("High" if floor_score >= 70 else "Moderate"),
            "desc": f"Level {floor} commands {'excellent' if floor >= 15 else 'good'} views and ventilation."
        },
        {
            "name": "Remaining Lease",
            "score": lease_score,
            "label": "Very High" if lease_score >= 85 else ("High" if lease_score >= 70 else "Moderate"),
            "desc": f"~{int(remaining_lease_years)} years remaining lease supports strong resale value."
        },
        {
            "name": "Floor Area",
            "score": area_score,
            "label": "High" if area_score >= 70 else "Moderate",
            "desc": f"{area_sqm:.0f} sqm ({area_sqft:.0f} sqft) — {'spacious' if area_sqm >= 90 else 'comfortable'} layout for {flat_type.title()}."
        },
        {
            "name": "Location Premium",
            "score": 78,
            "label": "High",
            "desc": f"{location_display} is a well-connected HDB town with established amenities."
        },
        {
            "name": "Investment Potential",
            "score": 75,
            "label": "High",
            "desc": "HDB resale prices remain resilient with stable demand from upgraders and first-time buyers."
        },
    ]

    ppsf = round(estimated_value / area_sqft) if area_sqft > 0 else 0

    # ── Contextual insights ───────────────────────────────────────────────────
    sora_label  = "elevated" if sora > 3.5 else ("easing" if sora < 2.8 else "moderate")
    pol_dir     = float(pol.get('direction', 0))
    lease_label = "strong" if remaining_lease_years > 75 else ("adequate" if remaining_lease_years > 60 else "declining — factor into CPF and loan planning")
    floor_label = "high-floor" if floor >= 20 else ("mid-floor" if floor >= 10 else "lower-floor")
    area_label  = "spacious" if area_sqm >= 90 else ("standard" if area_sqm >= 65 else "compact")

    # PSF context vs town median (approximated from model output)
    # Town median PSF rough benchmarks (OCR HDB ~S$490–560/sqft based on 2026 data)
    town_psf_bench = {'BISHAN': 750,'TOA PAYOH': 700,'QUEENSTOWN': 730,'BUKIT MERAH': 680,
                      'KALLANG/WHAMPOA': 670,'MARINE PARADE': 700,'ANG MO KIO': 600,
                      'SERANGOON': 590,'TAMPINES': 545,'BEDOK': 530,'HOUGANG': 520,
                      'SENGKANG': 500,'PUNGGOL': 490,'WOODLANDS': 465,'YISHUN': 460,
                      'JURONG WEST': 470,'CHOA CHU KANG': 480,'BUKIT PANJANG': 490}.get(town, 520)
    if ppsf > town_psf_bench * 1.10:
        psf_note = f"S${ppsf} PSF is above the {location_display} median — strong floor, check condition carefully."
    elif ppsf < town_psf_bench * 0.90:
        psf_note = f"S${ppsf} PSF is below the {location_display} median — investigate lease length and unit condition."
    else:
        psf_note = f"S${ppsf} PSF is in line with the {location_display} market median."

    # Lease-specific buying advice
    if remaining_lease_years < 30:
        lease_advice = f"Critical: only {int(remaining_lease_years)} yrs remaining — bank financing and CPF usage are severely restricted."
    elif remaining_lease_years < 60:
        lease_advice = f"Note: {int(remaining_lease_years)} yrs remaining — younger buyers may have limited CPF usage; this depresses your future resale pool."
    elif remaining_lease_years < 75:
        lease_advice = f"Lease of {int(remaining_lease_years)} yrs is adequate — CPF fully usable, but monitor for future restrictions."
    else:
        lease_advice = f"Lease of {int(remaining_lease_years)} yrs is strong — full CPF and bank financing eligibility for buyers."

    # Lease — plain language
    if remaining_lease_years < 30:
        lease_insight = f"One thing to be aware of: with only {int(remaining_lease_years)} years left on the lease, bank loans and CPF usage will be heavily restricted — this significantly reduces the buyer pool."
    elif remaining_lease_years < 60:
        lease_insight = f"The lease has {int(remaining_lease_years)} years remaining. Younger buyers may not be able to use their full CPF savings on this unit, so keep that in mind when pricing."
    elif remaining_lease_years < 75:
        lease_insight = f"With {int(remaining_lease_years)} years left, most buyers can use CPF and get a bank loan without issue — this is still a well-positioned unit."
    else:
        lease_insight = f"The {int(remaining_lease_years)}-year lease is healthy — buyers can use CPF and apply for full bank financing with no restrictions."

    sora_insight = (
        f"Interest rates are currently on the higher side (SORA {sora:.2f}%), which means buyers will pay more each month on their mortgage. This can affect how much they're willing to offer."
        if sora > 3.5 else
        f"Interest rates are moderate right now (SORA {sora:.2f}%), making it a reasonably good time for buyers to take a loan — which supports demand."
    )
    pol_insight = (
        "Do note that government cooling measures like ABSD are still in effect, which may slow demand slightly — especially from investors or second-time buyers."
        if pol_dir < 0 else
        ("The broader property market environment looks positive, with stable demand for HDB resale flats." if pol_dir > 0 else
         "The policy environment is broadly stable, with no major new measures expected in the near term.")
    )

    # ── Insight: pure market analysis (what IS happening) ────────────────────
    insight = (
        f"Our model values this {flat_type.title()} at S${estimated_value:,}, with a confidence level of {confidence:.0f}%. "
        f"{psf_note} "
        f"{lease_insight} "
        f"{sora_insight} "
        f"{pol_insight}"
    )

    # ── Recommendation: actionable steps only (what TO DO) ───────────────────
    # Pricing strategy based on PSF position
    if ppsf > town_psf_bench * 1.10:
        price_strategy = f"Since this unit is priced above the {location_display} median, make sure the condition, renovations, and floor level justify the premium — buyers will compare closely."
    elif ppsf < town_psf_bench * 0.90:
        price_strategy = f"At S${ppsf} PSF, there may be room to price slightly higher — check recent {location_display} transactions to see if the market supports an upward adjustment."
    else:
        price_strategy = f"The asking price is well-calibrated to the {location_display} market. Focus on presentation and availability to attract offers quickly."

    # Buyer profile based on lease
    if remaining_lease_years < 30:
        buyer_target = f"Target cash-heavy or older buyers — most younger buyers won't qualify for bank loans or full CPF usage with only {int(remaining_lease_years)} years left. Be prepared for a longer selling timeline."
    elif remaining_lease_years < 60:
        buyer_target = f"Prioritise buyers aged 35 and above who have built up CPF savings — younger buyers may face restrictions on how much CPF they can use on a {int(remaining_lease_years)}-year lease."
    elif remaining_lease_years < 75:
        buyer_target = f"The lease is workable for most buyers. Highlight it clearly in your listing to reassure those who may be concerned — it's not a dealbreaker at this length."
    else:
        buyer_target = f"This is an easy sell on financing — all buyers can use full CPF and apply for a bank loan. Lead with this in your listing to maximise interest."

    # Timing action based on SORA
    timing_action = (
        "With borrowing costs elevated, act on serious offers rather than waiting for a higher bid — buyer purchasing power is constrained right now."
        if sora > 3.5 else
        "Financing conditions are supportive for buyers at current rates. You can hold out for a fair offer without rushing."
    )

    recommendation = (
        f"We'd expect this unit to transact between S${min_value:,} and S${max_value:,}. "
        f"{price_strategy} "
        f"{buyer_target} "
        f"{timing_action} "
        f"Check the latest {location_display} transactions on the HDB Resale Portal to validate your pricing before listing."
    )

    # ── 12-month price forecast ───────────────────────────────────────────────
    # Base annual appreciation 2%, adjusted for SORA & policy
    annual_rate = 0.021
    if sora > 3.5: annual_rate -= 0.005
    if pol_dir < 0: annual_rate -= 0.004
    if pol_dir > 0: annual_rate += 0.003
    price_forecast = _build_forecast(estimated_value, annual_rate)

    hdb_mape = float(_meta.get('eval_mape', 7.0)) if _meta else 7.0
    result = {
        "estimated_value":    estimated_value,
        "min_value":          int(estimated_value * (1 - hdb_mape / 100)),
        "max_value":          int(estimated_value * (1 + hdb_mape / 100)),
        "mape":               round(hdb_mape, 2),
        "confidence":         confidence,
        "floor_extrapolated": floor_extrapolated,
        "ppsf":             ppsf,
        "market_trend":     f"+{annual_rate*100:.1f}%",
        "trend_direction":  "up" if annual_rate >= 0 else "down",
        "market_state":     "Active",
        "location":         location_display,
        "property_type":    "HDB",
        "district":         f"HDB – {location_display}",
        "insight":          insight,
        "recommendation":   recommendation,
        "factors":          factors,
        "price_forecast":   price_forecast,
        "remaining_lease_years": int(remaining_lease_years),
    }
    if shap_contributions:
        result["shap_contributions"] = shap_contributions
    return result


# ─── Rule-based fallback ──────────────────────────────────────────────────────

def _predict_fallback(features):
    postal = str(features.get('postal', '000000')).strip().zfill(6)
    area   = float(features.get('area', 90))     # sqm
    beds   = int(features.get('bedrooms', 3))
    floor  = int(features.get('floor', 10))

    # Build a better config using the town the frontend already resolved
    provided_town = str(features.get('town', '')).strip().upper()
    config = POSTAL_CONFIG.get(postal)
    if config is None:
        # Derive district from postal sector, not D15 default
        derived_district = _SECTOR_TO_DISTRICT.get(postal[:2], '')
        if not derived_district and postal[:2].isdigit():
            derived_district = f'D{int(postal[:2]):02d}'
        if provided_town:
            location_display = _TOWN_DISPLAY.get(provided_town, provided_town.title())
            config = {
                **DEFAULT_CONFIG,
                "location": location_display,
                "district": derived_district or f"HDB {location_display}",
                "property_type": "HDB",
            }
        else:
            config = {**DEFAULT_CONFIG, "district": derived_district or DEFAULT_CONFIG["district"]}
    area_sqft = area * 10.764   # area is in sqm
    psf = config["base_psf"]
    floor_pct = 0.009 if config["property_type"] == "Condominium" else 0.006
    psf *= (1 + floor_pct * max(floor - 1, 0))
    psf *= (1 + 0.02 * max(beds - 2, 0))

    seed_val = (int(postal) + int(area_sqft) + beds * 37 + floor * 13) % 100
    psf *= (1 + (seed_val - 50) / 1000.0)

    estimated_value = int(psf * area_sqft)
    min_value = int(estimated_value * 0.92)
    max_value = int(estimated_value * 1.08)

    base_conf = 90 if postal in POSTAL_CONFIG else 82
    floor_bonus = min(floor / 50 * 3, 3)
    bed_penalty = abs(beds - 3) * 0.5
    confidence  = round(min(base_conf + floor_bonus - bed_penalty, 97), 1)

    loc_score   = config["mrt_score"]
    floor_score = min(int(40 + floor * 1.2), 98)
    mkt_score   = 85 if config["market_state"] == "Very Active" else 75
    inv_score   = int((loc_score + mkt_score) / 2 + floor / 10)

    factors = [
        {"name": "Market Demand",       "score": mkt_score,          "label": "Very High" if mkt_score >= 85 else "High",    "desc": f"High transaction volume in {config['location']} — strong buyer interest."},
        {"name": "Nearby Amenities",    "score": loc_score,          "label": "Very High" if loc_score >= 85 else "High",    "desc": "Shopping malls, hawker centres, schools, and parks within close vicinity."},
        {"name": "Floor Level Premium", "score": floor_score,        "label": "Very High" if floor_score >= 85 else ("High" if floor_score >= 70 else "Moderate"), "desc": f"Level {floor} commands {'excellent' if floor >= 15 else 'good'} views and natural ventilation."},
        {"name": "Investment Potential","score": min(inv_score, 96), "label": "High" if inv_score >= 75 else "Moderate",     "desc": "Strong rental yield potential and capital appreciation based on district trends."},
        {"name": "MRT Proximity",       "score": loc_score,          "label": "Very High" if loc_score >= 85 else "High",    "desc": f"{config['district']} has excellent MRT access within walking distance."},
        {"name": "Location Premium",    "score": min(loc_score + 5, 98), "label": "Very High" if loc_score >= 85 else "High","desc": f"{config['location']} is a well-established area with good infrastructure and services."},
    ]

    ppsf_fb = round(estimated_value / area_sqft) if area_sqft > 0 else 0

    # ── Dynamic insight from actual computed values ────────────────────────────
    flat_type_fb  = str(features.get('flat_type', '')).strip() or f'{beds}-room'
    area_label_fb = "spacious" if area >= 90 else ("standard" if area >= 65 else "compact")
    floor_label_fb = "high-floor" if floor >= 20 else ("mid-floor" if floor >= 10 else "lower-floor")
    loc = config["location"]
    mkt = config["market_state"]

    # Derive lease context from features or a best estimate
    rem_lease_fb = float(features.get('remaining_lease_years', 0)) or None
    if rem_lease_fb:
        lease_label_fb = ("strong" if rem_lease_fb > 75
                          else "adequate" if rem_lease_fb > 60 else "watch closely")
        lease_note = f"Remaining lease of ~{int(rem_lease_fb)} yrs is {lease_label_fb}."
    else:
        lease_note = "Check remaining lease carefully — it directly affects CPF usage and loan eligibility."

    price_note = (
        f"S${'k'.join(str(estimated_value//1000).split()) if estimated_value < 1_000_000 else f'{estimated_value/1_000_000:.2f}M'}"
        f" ({confidence:.0f}% confidence, rule-based estimate)."
    )

    insight_fb = (
        f"{loc} · {flat_type_fb.title()} · {floor_label_fb} (Lvl {floor}) · {area:.0f} sqm · {area_label_fb}\n"
        f"Indicative valuation: {price_note} "
        f"{lease_note} "
        f"Market is {mkt.lower()} — {'strong demand from upgraders and first-time buyers' if mkt in ('Very Active','Active') else 'transaction volume has softened; negotiate carefully'}."
    )

    # Recommendation varies by floor, area, and market state
    if mkt in ('Very Active', 'Active'):
        timing = "Act decisively — well-priced units in this area move quickly."
    else:
        timing = "Take your time — market conditions favour buyers for due diligence."

    if ppsf_fb > 700:
        psf_note = f"At S${ppsf_fb} PSF this unit is in the upper range for {loc}; verify against recent transactions on the HDB Resale Portal."
    elif ppsf_fb > 500:
        psf_note = f"S${ppsf_fb} PSF is typical for {loc}; cross-check recent nearby sales before committing."
    else:
        psf_note = f"S${ppsf_fb} PSF is competitively priced for {loc}; investigate why it is below the area median."

    rec_fb = (
        f"For a {flat_type_fb.title()} at Level {floor} in {loc} ({area:.0f} sqm): "
        f"budget S${min_value:,}–S${max_value:,}. {psf_note} {timing}"
    )

    return {
        "estimated_value": estimated_value,
        "min_value":        min_value,
        "max_value":        max_value,
        "confidence":       confidence,
        "ppsf":             ppsf_fb,
        "market_trend":     config["trend"],
        "trend_direction":  config["trend_dir"],
        "market_state":     config["market_state"],
        "location":         config["location"],
        "property_type":    config["property_type"],
        "district":         config["district"],
        "insight":          insight_fb,
        "recommendation":   rec_fb,
        "factors":          factors,
    }


# ─── Private property (condo) ML prediction ──────────────────────────────────

# Postal sector (first 2 digits) → URA district + market segment
_SECTOR_TO_DISTRICT = {
    '01':'D01','02':'D01','03':'D01','04':'D01','05':'D01','06':'D01',
    '07':'D02','08':'D02','14':'D03','15':'D03','16':'D03',
    '09':'D04','10':'D04','11':'D05','12':'D05','13':'D05','17':'D06',
    '18':'D07','19':'D07','20':'D08','21':'D08',
    '22':'D09','23':'D09','24':'D09','25':'D10','26':'D10','27':'D10',
    '28':'D11','29':'D11','30':'D11','31':'D12','32':'D12','33':'D12',
    '34':'D13','35':'D13','36':'D13','37':'D13',
    '38':'D14','39':'D14','40':'D14','41':'D14',
    '42':'D15','43':'D15','44':'D15','45':'D15',
    '46':'D16','47':'D16','48':'D16','49':'D17','50':'D17','81':'D17',
    '51':'D18','52':'D18','53':'D19','54':'D19','55':'D19','82':'D19',
    '56':'D20','57':'D20','58':'D21','59':'D21',
    '60':'D22','61':'D22','62':'D22','63':'D22',
    '64':'D23','65':'D23','66':'D23','67':'D23','68':'D23',
    '69':'D24','70':'D24','71':'D24','72':'D25','73':'D25',
    '75':'D27','76':'D27','77':'D26','78':'D26','79':'D28','80':'D28',
}
_DISTRICT_CENTROIDS = {
    'D01':(1.2800,103.8500),'D02':(1.2780,103.8440),'D03':(1.2760,103.8150),
    'D04':(1.2650,103.8200),'D05':(1.3050,103.7850),'D06':(1.2900,103.8450),
    'D07':(1.3020,103.8560),'D08':(1.3090,103.8620),'D09':(1.2990,103.8330),
    'D10':(1.3100,103.8090),'D11':(1.3260,103.8170),'D12':(1.3200,103.8450),
    'D13':(1.3330,103.8810),'D14':(1.3140,103.8880),'D15':(1.3050,103.9060),
    'D16':(1.3230,103.9330),'D17':(1.3610,103.9400),'D18':(1.3560,103.9540),
    'D19':(1.3750,103.8800),'D20':(1.3470,103.8420),'D21':(1.3330,103.7680),
    'D22':(1.3490,103.7100),'D23':(1.3800,103.7540),'D24':(1.4050,103.7750),
    'D25':(1.4360,103.8110),'D26':(1.4040,103.8170),'D27':(1.4390,103.8290),
    'D28':(1.3850,103.8860),
}

_CCR_DISTRICTS = {'D01','D02','D04','D09','D10','D11'}
_RCR_DISTRICTS = {'D03','D05','D06','D07','D08','D12','D13','D14','D15','D20','D21'}

# PSF sanity bounds per segment (above 2026 Singapore market peaks with headroom)
_PSF_BOUNDS = {
    'CCR': (1200, 7000),   # Core Central Region
    'RCR': ( 900, 4500),   # Rest of Central Region
    'OCR': ( 600, 3000),   # Outside Central Region
}


def _get_district_rolling_psf(district: str, property_type: str) -> float:
    """24-month rolling avg PSF for a district×property_type. Fallback to segment benchmark."""
    seg_benchmark = {'D01':2200,'D02':2200,'D04':2200,'D09':2200,'D10':2200,'D11':2200}
    default = seg_benchmark.get(district, 1200)
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL', '')
        if not DATABASE_URL:
            return default
        import psycopg2, psycopg2.extras
        _cd = psycopg2.connect(DATABASE_URL)
        _cdcur = _cd.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        _cdcur.execute(
            "SELECT AVG(unit_price_psf) AS avg_psf FROM ura_transactions "
            "WHERE postal_district = %s AND UPPER(property_type) = %s "
            "AND unit_price_psf BETWEEN 300 AND 8000 "
            "AND sale_date >= TO_CHAR(CURRENT_DATE - INTERVAL '24 months', 'YYYY-MM')",
            (district, property_type.upper())
        )
        r = _cdcur.fetchone()
        _cd.close()
        if r:
            v = dict(r).get('avg_psf')
            if v and float(v) > 300:
                return float(v)
    except Exception:
        pass
    return default


def _private_amenity_distances(district: str) -> dict:
    """Compute amenity distances for a condo using its district centroid as lat/lon."""
    centroid = _DISTRICT_CENTROIDS.get(district, (1.305, 103.906))
    lat, lon  = centroid
    ad = _load_amenity_coords()
    school  = ad.get('school', []) or _PRIMARY_SCHOOLS
    hawker  = ad.get('hawker', []) or _HAWKER_CENTRES
    health  = ad.get('health', [])
    park    = ad.get('park',   [])
    return {
        'dist_nearest_mrt_km':    _dist_nearest(lat, lon, _MRT_STATIONS),
        'dist_nearest_school_km': _dist_nearest(lat, lon, school),
        'dist_nearest_hawker_km': _dist_nearest(lat, lon, hawker),
        'dist_nearest_health_km': _dist_nearest(lat, lon, health) if health else 1.0,
        'dist_nearest_park_km':   _dist_nearest(lat, lon, park)   if park   else 0.5,
    }


def _predict_private_ml(features):
    postal    = str(features.get('postal', '')).strip().zfill(6)
    area_sqft = float(features.get('area', 1000))
    floor     = int(features.get('floor', 10))
    project   = str(features.get('project', '')).strip().upper()

    sector   = postal[:2]
    district = _SECTOR_TO_DISTRICT.get(sector, 'D15')
    if district in _CCR_DISTRICTS:
        segment = 'CCR'
    elif district in _RCR_DISTRICTS:
        segment = 'RCR'
    else:
        segment = 'OCR'

    now = datetime.now()
    year     = now.year
    quarter  = (now.month - 1) // 3 + 1
    time_idx_raw = year * 12 + now.month
    time_idx = time_idx_raw - _private_meta.get('time_idx_min', 24000)

    pol  = _private_meta.get('latest_policy', {})
    sora = float(_private_meta.get('latest_sora', 3.5))

    # ── Project rolling PSF and all-time median PSF ───────────────────────────
    # project_rolling_psf_6m: 8-quarter (24m) rolling mean, min 3 months data
    # project_median_psf_alltime: all-time median — stable anchor for the project
    project_rolling_psf    = None
    project_alltime_psf    = None
    _seg_benchmark = {'CCR': 2200, 'RCR': 1550, 'OCR': 1150}.get(segment, 1200)
    if project:
        try:
            _DATABASE_URL = os.environ.get('DATABASE_URL', '')
            if _DATABASE_URL:
                import psycopg2, psycopg2.extras
                _pc = psycopg2.connect(_DATABASE_URL)
                _pcur = _pc.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                # 24-month rolling
                _pcur.execute(
                    "SELECT AVG(unit_price_psf) AS avg_psf, COUNT(DISTINCT LEFT(sale_date,7)) AS n_months "
                    "FROM ura_transactions "
                    "WHERE UPPER(project) = %s AND unit_price_psf BETWEEN 300 AND 8000 "
                    "AND sale_date >= TO_CHAR(CURRENT_DATE - INTERVAL '24 months', 'YYYY-MM')",
                    (project,)
                )
                _row = _pcur.fetchone()
                if _row:
                    row_d    = dict(_row)
                    v        = row_d.get('avg_psf')
                    n_months = int(row_d.get('n_months') or 0)
                    if v and n_months >= 3:
                        project_rolling_psf = float(v)
                # All-time median
                _pcur.execute(
                    "SELECT AVG(unit_price_psf) AS avg_psf "
                    "FROM ura_transactions "
                    "WHERE UPPER(project) = %s AND unit_price_psf BETWEEN 300 AND 8000",
                    (project,)
                )
                _row2 = _pcur.fetchone()
                if _row2:
                    v2 = dict(_row2).get('avg_psf')
                    if v2:
                        project_alltime_psf = float(v2)
            else:
                import sqlite3 as _sq
                _pc = _sq.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'propaisg.db'))
                _pc.row_factory = _sq.Row
                _pcur = _pc.cursor()
                _pcur.execute(
                    "SELECT AVG(unit_price_psf) AS avg_psf, COUNT(DISTINCT substr(sale_date,1,7)) AS n_months "
                    "FROM ura_transactions "
                    "WHERE UPPER(project) = ? AND unit_price_psf BETWEEN 300 AND 8000 "
                    "AND sale_date >= strftime('%Y-%m', date('now', '-24 months'))",
                    (project,)
                )
                _row = _pcur.fetchone()
                if _row:
                    row_d    = dict(_row)
                    v        = row_d.get('avg_psf')
                    n_months = int(row_d.get('n_months') or 0)
                    if v and n_months >= 3:
                        project_rolling_psf = float(v)
                _pcur.execute(
                    "SELECT AVG(unit_price_psf) AS avg_psf FROM ura_transactions "
                    "WHERE UPPER(project) = ? AND unit_price_psf BETWEEN 300 AND 8000",
                    (project,)
                )
                _row2 = _pcur.fetchone()
                if _row2:
                    v2 = dict(_row2).get('avg_psf')
                    if v2:
                        project_alltime_psf = float(v2)
            _pc.close()
        except Exception:
            pass
    if not project_rolling_psf or project_rolling_psf < 300:
        project_rolling_psf = _seg_benchmark
    if not project_alltime_psf or project_alltime_psf < 300:
        project_alltime_psf = project_rolling_psf

    # floor_level_pct: normalise floor by max known floor in this project
    floor_level_pct = 0.5   # default: mid-floor
    if project:
        try:
            _DATABASE_URL = os.environ.get('DATABASE_URL', '')
            if _DATABASE_URL:
                import psycopg2
                _pc2 = psycopg2.connect(_DATABASE_URL)
                _pc2cur = _pc2.cursor()
                _pc2cur.execute(
                    "SELECT MAX(CAST(SPLIT_PART(floor_level, '-', 2) AS INTEGER)) "
                    "FROM ura_transactions WHERE UPPER(project) = %s AND floor_level ~ '^[0-9]'",
                    (project,)
                )
                max_r = _pc2cur.fetchone()
                _pc2.close()
                if max_r and max_r[0]:
                    max_fl = int(max_r[0])
                    if max_fl > 0:
                        floor_level_pct = min(float(floor) / max_fl, 1.0)
        except Exception:
            pass

    num_cols = _private_meta.get('numerical_cols', [])
    cat_cols = _private_meta.get('categorical_cols', ['property_type','market_segment','type_of_sale','postal_district'])

    feat = {
        'property_type':          'Condominium',
        'market_segment':         segment,
        'type_of_sale':           'Resale',
        'postal_district':        district,
        'floor_area_sqft':              area_sqft,
        'floor_level_num':              float(floor),
        'floor_level_pct':              floor_level_pct,
        'tenure_remaining_years':       75.0,
        'is_strata':                    1.0,
        'year':                         year,
        'quarter':                      quarter,
        'time_idx':                     time_idx,
        'direction':                    float(pol.get('direction', 0)),
        'severity':                     float(pol.get('severity', 0)),
        'policy_impact':                float(pol.get('policy_impact', 0)),
        'months_since_policy_change':   int(pol.get('months_since_policy_change', 0)),
        'sora':                         sora,
        'project_rolling_psf_6m':       project_rolling_psf,
        'project_median_psf_alltime':   project_alltime_psf,
        'district_rolling_psf_24m':     _get_district_rolling_psf(district, 'CONDOMINIUM'),
        'district_median_psf_alltime':  _get_district_rolling_psf(district, 'CONDOMINIUM'),  # reuse helper (all-time ≈ rolling for inference)
        'storey_psf_interaction':       floor_level_pct * project_rolling_psf,
        'sin_quarter':                  __import__('math').sin(2 * __import__('math').pi * quarter / 4),
        'cos_quarter':                  __import__('math').cos(2 * __import__('math').pi * quarter / 4),
        # Amenity distances from district centroid
        **_private_amenity_distances(district),
    }

    try:
        priv_model_names  = _private_meta.get('model_names', ['xgb_private','lgbm_private','cat_private'])
        priv_feat_subsets = _private_meta.get('model_feature_subsets', {})
        preds_log = []
        for mname, pipeline in zip(priv_model_names, _private_pipelines):
            mfeats = priv_feat_subsets.get(mname, cat_cols + num_cols)
            mrow   = pd.DataFrame([{k: feat[k] for k in mfeats if k in feat}])
            preds_log.append(pipeline.predict(mrow)[0])

        stacker_coef = _private_meta.get('stacker_coef')
        stacker_int  = float(_private_meta.get('stacker_intercept', 0.0))
        if stacker_coef and len(stacker_coef) == len(preds_log):
            ensemble_log = float(np.dot(preds_log, stacker_coef)) + stacker_int
        else:
            ensemble_log = float(np.mean(preds_log))
        estimated_value = int(np.exp(ensemble_log))

        # ── PSF sanity check — clamp extreme extrapolations ───────────────────
        if area_sqft > 0:
            raw_psf = estimated_value / area_sqft
            psf_min, psf_max = _PSF_BOUNDS.get(segment, (600, 5000))
            if raw_psf > psf_max or raw_psf < psf_min:
                clamped_psf  = max(psf_min, min(psf_max, raw_psf))
                estimated_value = int(clamped_psf * area_sqft)
                print(f"[predict_private] PSF clamped {raw_psf:,.0f} → {clamped_psf:,.0f} "
                      f"({segment}, {area_sqft:.0f} sqft)")
    except Exception as e:
        print(f"[predict_private] inference error: {e}")
        return None

    # ── SHAP contributions (lazy load) ────────────────────────────────────────
    shap_contributions = None
    try:
        if _load_shap_private():
            xgb_pipe = _private_pipelines[0]
            preprocessor = xgb_pipe.named_steps['preprocessor']
            transformed  = preprocessor.transform(row)
            sv = _shap_private['explainer'].shap_values(transformed)
            if sv is not None and len(sv) > 0:
                shap_contributions = _group_shap_values(
                    sv[0], _shap_private['feature_names'], _shap_private['categorical_cols']
                )
    except Exception as _se:
        print(f"[predict] SHAP Private inference skipped: {_se}")

    # Intra-model spread via first/second half of trees (same logic as HDB path)
    xgb_priv = _private_pipelines[0]
    try:
        booster   = xgb_priv.named_steps['model'].get_booster()
        n_trees   = booster.num_boosted_rounds()
        prep_row  = xgb_priv.named_steps['preprocessor'].transform(row)
        import xgboost as _xgb2
        dmat = _xgb2.DMatrix(prep_row)
        if n_trees >= 4:
            half = n_trees // 2
            pred_first  = float(np.exp(booster.predict(dmat, iteration_range=(0,    half),    output_margin=True)[0]))
            pred_second = float(np.exp(booster.predict(dmat, iteration_range=(half, n_trees), output_margin=True)[0]))
            cv = abs(pred_first - pred_second) / max(estimated_value, 1)
        else:
            spread = float(np.std([np.exp(v) for v in preds_log]))
            cv = spread / max(estimated_value, 1)
    except Exception:
        spread = float(np.std([np.exp(v) for v in preds_log]))
        cv = spread / max(estimated_value, 1)

    confidence = round(max(68.0, min(93.0, 90.0 - cv * 180)), 1)

    psf = estimated_value / area_sqft if area_sqft > 0 else 0
    ppsf = round(psf)
    location_display = {
        'CCR': 'Core Central Region', 'RCR': 'Rest of Central Region',
        'OCR': 'Outside Central Region',
    }.get(segment, segment)

    floor_score = min(int(35 + floor * 1.5), 98)
    area_score  = min(int(45 + (area_sqft - 700) * 0.05), 98)

    sora_label = "elevated" if sora > 3.5 else ("easing" if sora < 2.8 else "moderate")
    pol_dir = float(pol.get('direction', 0))
    floor_label = "high-floor" if floor >= 20 else ("mid-floor" if floor >= 10 else "lower-floor")
    size_label  = "large" if area_sqft >= 1400 else ("standard" if area_sqft >= 900 else "compact")

    # Condo PSF segment benchmarks (2026 approximate)
    seg_psf_bench = {'CCR': 2400, 'RCR': 1700, 'OCR': 1350}.get(segment, 1500)
    if ppsf > seg_psf_bench * 1.10:
        condo_psf_note = f"At S${ppsf:,} PSF, this unit is priced above the {location_display} average — buyers will expect a compelling justification such as a high floor, recent renovation, or rare layout."
    elif ppsf < seg_psf_bench * 0.90:
        condo_psf_note = f"At S${ppsf:,} PSF, this unit is below the typical {location_display} range — potentially good value, but verify that the floor, condition, and remaining tenure support it."
    else:
        condo_psf_note = f"At S${ppsf:,} PSF, this unit is fairly priced relative to the {location_display} market."

    sora_condo_insight = (
        f"With SORA at {sora:.2f}%, borrowing costs are elevated. Buyers using bank loans will face higher monthly payments, which can compress what they're willing to offer."
        if sora > 3.5 else
        f"SORA at {sora:.2f}% keeps financing accessible — buyers can still get competitive loan packages, which supports demand and pricing."
    )
    pol_condo_insight = (
        "ABSD and other cooling measures remain in place, particularly affecting second-property buyers and foreigners — this limits the pool of potential buyers."
        if pol_dir < 0 else
        ("Property market policy is broadly supportive, with stable investment conditions for private residential." if pol_dir > 0 else
         "The policy environment is neutral — no major new measures are expected to significantly shift demand in the near term.")
    )

    # ── Insight: pure market analysis ────────────────────────────────────────
    insight = (
        f"Our model values this {floor_label} {size_label} condo at S${estimated_value:,} (S${ppsf:,} PSF), with a confidence level of {confidence:.0f}%. "
        f"{condo_psf_note} "
        f"{sora_condo_insight} "
        f"{pol_condo_insight}"
    )

    # ── Recommendation: actionable steps only ────────────────────────────────
    priv_mape_now = float(_private_meta.get('eval_mape', 10.0)) if _private_meta else 10.0
    priv_min = int(estimated_value * (1 - priv_mape_now / 100))
    priv_max = int(estimated_value * (1 + priv_mape_now / 100))

    if ppsf > seg_psf_bench * 1.10:
        condo_price_strategy = f"The above-market PSF means buyers will negotiate harder — strengthen your position with recent comparable sales from URA caveats and highlight distinguishing features like view or renovations."
    elif ppsf < seg_psf_bench * 0.90:
        condo_price_strategy = f"There may be room to price slightly higher — pull recent URA caveat data for {district} to see if the market supports an upward revision before listing."
    else:
        condo_price_strategy = f"The price is well-calibrated for {district}. Prioritise speed to market — well-priced units in this segment tend to attract offers within 4–6 weeks."

    condo_timing = (
        "With elevated borrowing costs, serious buyers are stretched — act on strong offers rather than holding out for top dollar, as demand is rate-sensitive right now."
        if sora > 3.5 else
        "Financing is accessible at current rates. You can afford to wait for a fair offer without major pressure — but avoid overpricing, as the window can close quickly."
    )

    condo_buyer_tip = (
        "ABSD limits the investor pool — focus marketing on owner-occupiers and HDB upgraders, who are less affected by the additional stamp duty."
        if pol_dir < 0 else
        "Market conditions are open to a broad buyer pool including investors and upgraders — cast a wide net in your marketing."
    )

    recommendation = (
        f"We'd expect this unit to transact between S${priv_min:,} and S${priv_max:,}. "
        f"{condo_price_strategy} "
        f"{condo_timing} "
        f"{condo_buyer_tip} "
        f"Cross-check with URA caveat lodgements for {district} before finalising your ask."
    )

    # 12-month forecast (private: slightly higher baseline growth, CCR premium)
    annual_rate = 0.023
    if segment == 'CCR': annual_rate += 0.004
    if sora > 3.5: annual_rate -= 0.005
    if pol_dir < 0: annual_rate -= 0.004
    price_forecast = _build_forecast(estimated_value, annual_rate)

    priv_mape = float(_private_meta.get('eval_mape', 10.0)) if _private_meta else 10.0
    _private_result = {
        "estimated_value": estimated_value,
        "min_value":  int(estimated_value * (1 - priv_mape / 100)),
        "max_value":  int(estimated_value * (1 + priv_mape / 100)),
        "mape":       round(priv_mape, 2),
        "confidence": confidence,
        "ppsf":       ppsf,
        "market_trend":    f"+{annual_rate*100:.1f}%",
        "trend_direction": "up",
        "market_state":    "Active",
        "location":        location_display,
        "property_type":   "Condominium",
        "district":        district,
        "insight":         insight,
        "recommendation":  recommendation,
        "price_forecast":  price_forecast,
        "factors": [
            {"name": "Market Demand",       "score": 78, "label": "High",
             "desc": f"Consistent buyer interest in {location_display} private residential."},
            {"name": "Floor Level Premium", "score": floor_score,
             "label": "Very High" if floor_score >= 85 else "High",
             "desc": f"Level {floor} adds view and ventilation premiums typical in condominiums."},
            {"name": "Floor Area",          "score": area_score,
             "label": "High" if area_score >= 70 else "Moderate",
             "desc": f"{area_sqft:.0f} sqft — {'spacious' if area_sqft >= 1200 else 'comfortable'} for a condo unit."},
            {"name": "Location Premium",    "score": 82, "label": "High",
             "desc": f"{district} ({segment}) offers {'premium' if segment == 'CCR' else 'good'} connectivity and amenities."},
            {"name": "Investment Potential","score": 74, "label": "High",
             "desc": "Private residential prices remain resilient with stable expat and upgrader demand."},
            {"name": "MRT Proximity",       "score": 76, "label": "High",
             "desc": "Strong MRT network coverage across Singapore supports private property values."},
        ],
    }
    if shap_contributions:
        _private_result["shap_contributions"] = shap_contributions
    return _private_result


# ─── Public entry point ───────────────────────────────────────────────────────

def predict_price(features):
    prop_type = str(features.get('property_type', 'HDB')).strip()
    is_condo  = prop_type.lower() in ('condominium', 'condo', 'private')

    if is_condo:
        if _load_private_models():
            result = _predict_private_ml(features)
            if result is not None:
                return result
        # Condo fallback — use rule-based with condo PSF
        return _predict_fallback_condo(features)
    else:
        if _load_models():
            result = _predict_ml(features)
            if result is not None:
                return result
        return _predict_fallback(features)


def _predict_fallback_condo(features):
    """Rule-based fallback for condominiums when private ML model is unavailable."""
    postal    = str(features.get('postal', '000000')).strip().zfill(6)
    area_sqft = float(features.get('area', 1000))
    floor     = int(features.get('floor', 10))

    sector   = postal[:2]
    district = _SECTOR_TO_DISTRICT.get(sector, 'D15')
    if district in _CCR_DISTRICTS:
        base_psf, segment = 2420, 'CCR'
    elif district in _RCR_DISTRICTS:
        base_psf, segment = 1904, 'RCR'
    else:
        base_psf, segment = 1520, 'OCR'

    psf = base_psf * (1 + 0.012 * max(floor - 1, 0))
    seed = (int(postal) + int(area_sqft) + floor * 17) % 100
    psf  *= (1 + (seed - 50) / 1200.0)
    estimated_value = int(psf * area_sqft)
    location_display = {
        'CCR': 'Core Central Region', 'RCR': 'Rest of Central Region',
        'OCR': 'Outside Central Region',
    }.get(segment, segment)

    ppsf_fc = round(estimated_value / area_sqft) if area_sqft > 0 else 0
    floor_label_fc = "high-floor" if floor >= 20 else ("mid-floor" if floor >= 10 else "lower-floor")
    size_label_fc  = "large" if area_sqft >= 1400 else ("standard" if area_sqft >= 900 else "compact")

    seg_bench = {'CCR': 2800, 'RCR': 1900, 'OCR': 1350}[segment]
    psf_vs_bench = psf - seg_bench
    if abs(psf_vs_bench) < seg_bench * 0.05:
        psf_note_fc = f"at the {location_display} median (S${seg_bench:,.0f} PSF benchmark)"
    elif psf_vs_bench > 0:
        psf_note_fc = f"{(psf_vs_bench/seg_bench*100):.0f}% above the {location_display} median — verify with recent URA caveats"
    else:
        psf_note_fc = f"{(abs(psf_vs_bench)/seg_bench*100):.0f}% below the {location_display} median — investigate condition and leasehold status"

    insight_fc = (
        f"{location_display} · {district} · {floor_label_fc} (Lvl {floor}) · {area_sqft:.0f} sqft · {size_label_fc}\n"
        f"Indicative valuation: S${estimated_value:,} at S${ppsf_fc:,} PSF (78% confidence). "
        f"This unit is priced {psf_note_fc}. "
        f"Private residential in {segment} remains resilient — driven by upgrader and investor demand."
    )
    rec_fc = (
        f"{size_label_fc.capitalize()} {segment} unit at Level {floor} in {district}: "
        f"indicative range S${int(estimated_value*0.91):,}–S${int(estimated_value*1.09):,}. "
        f"Cross-check URA caveat lodgements for {district} and compare listings on 99.co or PropertyGuru. "
        f"At S${ppsf_fc:,} PSF, {'this is a premium unit — negotiate only if the seller is motivated' if ppsf_fc > seg_bench * 1.1 else 'there is reasonable room to offer below asking price' if ppsf_fc < seg_bench * 0.95 else 'the pricing is in line with the market — move quickly if the unit suits your needs'}."
    )

    return {
        "estimated_value": estimated_value,
        "min_value":  int(estimated_value * 0.91),
        "max_value":  int(estimated_value * 1.09),
        "confidence": 78.0,
        "ppsf":       ppsf_fc,
        "market_trend":    "+2.1%", "trend_direction": "up",
        "market_state":    "Active",
        "location":        location_display,
        "property_type":   "Condominium",
        "district":        district,
        "insight":         insight_fc,
        "recommendation":  rec_fc,
        "factors": [
            {"name": "Market Demand",       "score": 76, "label": "High",
             "desc": f"Consistent buyer interest in {location_display} — expat and upgrader pool is broad."},
            {"name": "Floor Level Premium", "score": min(int(35+floor*1.5),98), "label": "High",
             "desc": f"Level {floor} adds view and ventilation premiums typical in condominiums."},
            {"name": "Floor Area",          "score": min(int(45+(area_sqft-700)*0.05),98), "label": "Moderate",
             "desc": f"{area_sqft:.0f} sqft — {size_label_fc} for a {segment} condo."},
            {"name": "Location Premium",    "score": 80 if segment=="CCR" else 72, "label": "High",
             "desc": f"{district} ({segment}) offers {'premium' if segment=='CCR' else 'good'} connectivity and infrastructure."},
            {"name": "Investment Potential","score": 72, "label": "High",
             "desc": "Private residential prices remain resilient with stable expat and upgrader demand."},
            {"name": "MRT Proximity",       "score": 75, "label": "High",
             "desc": "Strong MRT network coverage across Singapore supports private property values."},
        ],
    }


if __name__ == "__main__":
    tests = [
        {"postal": "560123", "area": 1000, "bedrooms": 4, "floor": 10},
        {"postal": "159088", "area": 900,  "bedrooms": 3, "floor": 8},
        {"postal": "342005", "area": 1100, "bedrooms": 4, "floor": 12},
    ]
    for t in tests:
        r = predict_price(t)
        print(f"{t['postal']} ({r['location']}): S${r['estimated_value']:,} | Conf: {r['confidence']}%")
