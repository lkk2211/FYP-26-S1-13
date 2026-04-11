import math
import os
import sys

# Load trained stacking models if available
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from model_inference import predict_hdb, predict_private
    _ML_AVAILABLE = True
except Exception:
    _ML_AVAILABLE = False
    predict_hdb = predict_private = lambda _: None

# Singapore property data by postal code
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


def predict_price(features):
    postal = str(features.get('postal', '000000')).strip().zfill(6)
    area   = float(features.get('area', 1000))
    beds   = int(features.get('bedrooms', 3))
    floor  = int(features.get('floor', 10))

    config = POSTAL_CONFIG.get(postal, DEFAULT_CONFIG)
    is_private = config['property_type'] == 'Condominium'

    # ── Try ML stacking model first ──────────────────────────────────────────
    ml_price = None
    if _ML_AVAILABLE:
        if is_private:
            ml_price = predict_private({
                'postal':        postal,
                'area_sqm':      area * 0.0929,   # sqft → sqm
                'floor':         floor,
                'property_type': 'CONDOMINIUM',
                'type_of_sale':  'RESALE',
                'tenure_type':   'LEASEHOLD',
            })
        else:
            town = features.get('town', config.get('location', '').upper())
            flat_type = features.get('flat_type',
                        {1:'1 ROOM',2:'2 ROOM',3:'3 ROOM',4:'4 ROOM',5:'5 ROOM'}
                        .get(beds, '4 ROOM'))
            ml_price = predict_hdb({
                'town':          town,
                'flat_type':     flat_type,
                'floor_area_sqm': area * 0.0929,
                'storey_mid':    floor,
                'lat':           features.get('lat'),
                'lon':           features.get('lon'),
            })
    # ─────────────────────────────────────────────────────────────────────────

    # --- Price Calculation ---
    if ml_price is not None:
        estimated_value = int(ml_price)
        confidence = round(min(92.0 + min(floor / 50 * 2, 2), 97), 1)
    else:
        # Rule-based fallback
        psf = config["base_psf"]
        floor_pct = 0.009 if config["property_type"] == "Condominium" else 0.006
        psf *= (1 + floor_pct * max(floor - 1, 0))
        psf *= (1 + 0.02 * max(beds - 2, 0))
        seed_val = (int(postal) + int(area) + beds * 37 + floor * 13) % 100
        noise_pct = (seed_val - 50) / 1000.0
        psf *= (1 + noise_pct)
        estimated_value = int(psf * area)
        base_conf = 90 if postal in POSTAL_CONFIG else 82
        floor_bonus = min(floor / 50 * 3, 3)
        bed_penalty = abs(beds - 3) * 0.5
        confidence  = round(min(base_conf + floor_bonus - bed_penalty, 97), 1)

    # Price range (±8% band)
    min_value = int(estimated_value * 0.92)
    max_value = int(estimated_value * 1.08)

    # --- Factor Scores (0-100) ---
    loc_score  = config["mrt_score"]
    area_score = min(int(50 + (area - 500) / 30), 98)
    floor_score = min(int(40 + floor * 1.2), 98)
    mkt_score  = 85 if config["market_state"] == "Very Active" else 75
    invest_score = int((loc_score + mkt_score) / 2 + floor / 10)

    factors = [
        {
            "name": "Market Demand",
            "score": mkt_score,
            "label": "Very High" if mkt_score >= 85 else "High",
            "desc": f"High transaction volume in {config['location']} — strong buyer interest in this area."
        },
        {
            "name": "Nearby Amenities",
            "score": loc_score,
            "label": "Very High" if loc_score >= 85 else "High",
            "desc": "Shopping malls, hawker centres, schools, and parks within close vicinity."
        },
        {
            "name": "Floor Level Premium",
            "score": floor_score,
            "label": "Very High" if floor_score >= 85 else ("High" if floor_score >= 70 else "Moderate"),
            "desc": f"Level {floor} commands {'excellent' if floor >= 15 else 'good'} views and natural ventilation."
        },
        {
            "name": "Investment Potential",
            "score": min(invest_score, 96),
            "label": "High" if invest_score >= 75 else "Moderate",
            "desc": "Strong rental yield potential and capital appreciation based on district trends."
        },
        {
            "name": "MRT Proximity",
            "score": loc_score,
            "label": "Very High" if loc_score >= 85 else "High",
            "desc": f"{config['district']} has excellent MRT access within walking distance."
        },
        {
            "name": "Location Premium",
            "score": min(loc_score + 5, 98),
            "label": "Very High" if loc_score >= 85 else "High",
            "desc": f"{config['location']} is a well-established area with good infrastructure and services."
        }
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
        "model_source":     "ml_stacked" if ml_price is not None else "rule_based"
    }


if __name__ == "__main__":
    tests = [
        {"postal": "238801", "area": 1200, "bedrooms": 3, "floor": 20},
        {"postal": "560123", "area": 1000, "bedrooms": 4, "floor": 10},
        {"postal": "159088", "area": 900,  "bedrooms": 3, "floor": 8},
    ]
    for t in tests:
        r = predict_price(t)
        print(f"{t['postal']} ({r['location']}): S${r['estimated_value']:,} | Conf: {r['confidence']}% | Trend: {r['market_trend']}")
