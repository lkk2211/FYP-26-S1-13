import os
import math
import joblib
import numpy as np
import pandas as pd
import requests
from datetime import datetime

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')

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
    "district": "D15", "location": "East Region", "property_type": "HDB",
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


def reset_model_cache():
    """Called after live retraining to force reload on next prediction."""
    global _pipelines, _meta, _private_pipelines, _private_meta
    _pipelines = None
    _meta = None
    _private_pipelines = None
    _private_meta = None
    print("[predict] Model cache reset — will reload fresh .joblib files on next prediction")


def _load_models():
    global _pipelines, _meta
    if _pipelines is not None:
        return True
    try:
        xgb  = joblib.load(os.path.join(MODELS_DIR, 'xgb_pipeline.joblib'))
        meta = joblib.load(os.path.join(MODELS_DIR, 'meta.joblib'))
        _pipelines = [xgb]   # XGB only — LGBM+CatBoost exceed 512MB RAM on free tier
        _meta = meta
        pol, sor = _load_latest_policy_sora()
        if pol:   _meta['latest_policy'] = pol
        if sor is not None: _meta['latest_sora'] = sor
        print("[predict] HDB ML models loaded successfully")
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
        _private_pipelines = [xgb]   # XGB only — free tier RAM limit
        _private_meta = meta
        pol, sor = _load_latest_policy_sora()
        if pol:   _private_meta['latest_policy'] = pol
        if sor is not None: _private_meta['latest_sora'] = sor
        print("[predict] Private ML models loaded successfully")
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
    postal   = str(features.get('postal', '')).strip().zfill(6)
    area_sqm  = float(features.get('area', 90))   # incoming value is sqm
    bedrooms  = int(features.get('bedrooms', 3))
    floor     = int(features.get('floor', 10))

    area_sqft = area_sqm * 10.764

    # Derive flat_type from bedrooms
    flat_type = _BEDROOMS_TO_FLAT_TYPE.get(bedrooms, 'EXECUTIVE' if bedrooms >= 6 else '5 ROOM')
    flat_model = _FLAT_TYPE_TO_MODEL.get(flat_type, 'MODEL A')

    # Geocode → town + lat/lon
    town, lat, lon = _geocode_postal(postal)
    if not town:
        return None  # fall back to rule-based

    # Time features
    now = datetime.now()
    year = now.year
    quarter = (now.month - 1) // 3 + 1
    time_idx_raw = year * 12 + now.month
    time_idx = time_idx_raw - _meta['time_idx_min']

    storey_mid = float(floor)

    # Lease/age defaults from training medians
    key = (town, flat_type)
    med = _meta['medians_by_town_type'].get(key, {})
    remaining_lease_years = float(med.get('remaining_lease_years', 65.0))
    flat_age_years        = float(med.get('flat_age_years', 34.0))
    lat_med = float(med.get('lat', lat or 1.35))
    lon_med = float(med.get('lon', lon or 103.82))

    # Latest policy + SORA (from meta, refreshed from DB at load time)
    pol = _meta.get('latest_policy', {})
    sora = float(_meta.get('latest_sora', 3.5))

    # Build feature row using exactly the columns the model was trained on
    num_cols = _meta.get('numerical_cols', [])
    feat = {
        'town': town, 'flat_type': flat_type, 'flat_model': flat_model,
        'floor_area_sqm':        area_sqm,
        'year':                  year,
        'quarter':               quarter,
        'time_idx':              time_idx,
        'storey_mid':            storey_mid,
        'remaining_lease_years': remaining_lease_years,
        'flat_age_years':        flat_age_years,
        'direction':             float(pol.get('direction', 0)),
        'severity':              float(pol.get('severity', 0)),
        'policy_impact':         float(pol.get('policy_impact', 0)),
        'months_since_policy_change': int(pol.get('months_since_policy_change', 0)),
        'sora':                  sora,
        'lat':                   lat or lat_med,
        'lon':                   lon or lon_med,
    }
    row = pd.DataFrame([{k: feat[k] for k in (_meta.get('categorical_cols', ['town','flat_type','flat_model']) + num_cols) if k in feat}])

    preds_log = [p.predict(row)[0] for p in _pipelines]
    ensemble_log = np.mean(preds_log)
    estimated_value = int(np.exp(ensemble_log))

    # Confidence: higher when all three models agree
    spread = float(np.std([np.exp(v) for v in preds_log]))
    cv = spread / estimated_value
    confidence = round(max(70.0, min(95.0, 92.0 - cv * 200)), 1)

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

    return {
        "estimated_value": estimated_value,
        "min_value":        min_value,
        "max_value":        max_value,
        "confidence":       confidence,
        "market_trend":     "+2.1%",
        "trend_direction":  "up",
        "market_state":     "Active",
        "location":         location_display,
        "property_type":    "HDB",
        "district":         f"HDB {location_display}",
        "insight": (
            f"{location_display} is a well-established HDB town with consistent resale demand. "
            "This estimate is based on an ensemble ML model trained on Singapore HDB resale transactions."
        ),
        "recommendation": (
            f"Predicted price is S${estimated_value:,} for a {flat_type.title()} at level {floor}. "
            "Compare against recent transacted prices on HDB Resale Portal before making an offer."
        ),
        "factors": factors,
    }


# ─── Rule-based fallback ──────────────────────────────────────────────────────

def _predict_fallback(features):
    postal = str(features.get('postal', '000000')).strip().zfill(6)
    area   = float(features.get('area', 1000))
    beds   = int(features.get('bedrooms', 3))
    floor  = int(features.get('floor', 10))

    config = POSTAL_CONFIG.get(postal, DEFAULT_CONFIG)
    psf = config["base_psf"]
    floor_pct = 0.009 if config["property_type"] == "Condominium" else 0.006
    psf *= (1 + floor_pct * max(floor - 1, 0))
    psf *= (1 + 0.02 * max(beds - 2, 0))

    seed_val = (int(postal) + int(area) + beds * 37 + floor * 13) % 100
    psf *= (1 + (seed_val - 50) / 1000.0)

    estimated_value = int(psf * area)
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

    return {
        "estimated_value": estimated_value,
        "min_value":        min_value,
        "max_value":        max_value,
        "confidence":       confidence,
        "market_trend":     config["trend"],
        "trend_direction":  config["trend_dir"],
        "market_state":     config["market_state"],
        "location":         config["location"],
        "property_type":    config["property_type"],
        "district":         config["district"],
        "insight":          config["insight"],
        "recommendation":   config["recommendation"],
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
_CCR_DISTRICTS = {'D01','D02','D04','D09','D10','D11'}
_RCR_DISTRICTS = {'D03','D05','D06','D07','D08','D12','D13','D14','D15','D20','D21'}


def _predict_private_ml(features):
    postal    = str(features.get('postal', '')).strip().zfill(6)
    area_sqft = float(features.get('area', 1000))
    floor     = int(features.get('floor', 10))

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

    num_cols = _private_meta.get('numerical_cols', [])
    cat_cols = _private_meta.get('categorical_cols', ['property_type','market_segment','type_of_sale','postal_district'])

    feat = {
        'property_type':   'Condominium',
        'market_segment':  segment,
        'type_of_sale':    'Resale',
        'postal_district': district,
        'floor_area_sqft': area_sqft,
        'floor_level_num': float(floor),
        'year':            year,
        'quarter':         quarter,
        'time_idx':        time_idx,
        'direction':       float(pol.get('direction', 0)),
        'severity':        float(pol.get('severity', 0)),
        'policy_impact':   float(pol.get('policy_impact', 0)),
        'months_since_policy_change': int(pol.get('months_since_policy_change', 0)),
        'sora':            sora,
    }

    try:
        row = pd.DataFrame([{k: feat[k] for k in (cat_cols + num_cols) if k in feat}])
        preds_log = [p.predict(row)[0] for p in _private_pipelines]
        ensemble_log = np.mean(preds_log)
        estimated_value = int(np.exp(ensemble_log))
    except Exception as e:
        print(f"[predict_private] inference error: {e}")
        return None

    spread = float(np.std([np.exp(v) for v in preds_log]))
    cv = spread / max(estimated_value, 1)
    confidence = round(max(68.0, min(93.0, 90.0 - cv * 200)), 1)

    psf = estimated_value / area_sqft if area_sqft > 0 else 0
    location_display = {
        'CCR': 'Core Central Region', 'RCR': 'Rest of Central Region',
        'OCR': 'Outside Central Region',
    }.get(segment, segment)

    floor_score = min(int(35 + floor * 1.5), 98)
    area_score  = min(int(45 + (area_sqft - 700) * 0.05), 98)

    return {
        "estimated_value": estimated_value,
        "min_value":  int(estimated_value * 0.91),
        "max_value":  int(estimated_value * 1.09),
        "confidence": confidence,
        "market_trend":    "+2.3%",
        "trend_direction": "up",
        "market_state":    "Active",
        "location":        location_display,
        "property_type":   "Condominium",
        "district":        district,
        "insight": (
            f"This {segment} condominium in {district} ({location_display}) is estimated at "
            f"S${psf:,.0f} PSF based on recent private residential transaction data. "
            "Predicted using an ensemble ML model trained on URA private property transactions."
        ),
        "recommendation": (
            f"Estimated value S${estimated_value:,} — compare against URA caveat lodgements for "
            f"{district} before making an offer. {segment} condos have shown steady demand."
        ),
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

    return {
        "estimated_value": estimated_value,
        "min_value":  int(estimated_value * 0.91),
        "max_value":  int(estimated_value * 1.09),
        "confidence": 78.0,
        "market_trend":    "+2.1%", "trend_direction": "up",
        "market_state":    "Active",
        "location":        location_display,
        "property_type":   "Condominium",
        "district":        district,
        "insight": (
            f"Estimated at S${psf:,.0f} PSF for a {segment} condominium in {district}. "
            "This is a rule-based estimate; train the private property model for higher accuracy."
        ),
        "recommendation": (
            f"Indicative value: S${estimated_value:,}. Cross-check with URA caveat data and "
            "recent transactions on PropertyGuru or 99.co before proceeding."
        ),
        "factors": [
            {"name": "Market Demand",       "score": 76, "label": "High",   "desc": f"Stable demand in {location_display}."},
            {"name": "Floor Level Premium", "score": min(int(35+floor*1.5),98), "label": "High", "desc": f"Level {floor} adds a view premium."},
            {"name": "Floor Area",          "score": min(int(45+(area_sqft-700)*0.05),98), "label": "Moderate", "desc": f"{area_sqft:.0f} sqft unit."},
            {"name": "Location Premium",    "score": 80 if segment=="CCR" else 72, "label": "High", "desc": f"{district} ({segment})."},
            {"name": "Investment Potential","score": 72, "label": "High",   "desc": "Private residential remains a stable asset class."},
            {"name": "MRT Proximity",       "score": 75, "label": "High",   "desc": "Good MRT coverage across Singapore."},
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
