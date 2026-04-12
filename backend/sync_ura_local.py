#!/usr/bin/env python3
"""
Run this script LOCALLY (not on Render) to sync URA data directly to Supabase.
Your home IP is not blocked by URA's bot protection; Render's data centre IP is.

Usage:
    DATABASE_URL="postgresql://..." URA_ACCESS_KEY="your_key" python sync_ura_local.py

Or set the vars in a .env file and run:
    python sync_ura_local.py
"""
import os, json, urllib.request, psycopg2, psycopg2.extras
from datetime import datetime

DATABASE_URL  = os.environ.get('DATABASE_URL', '')
URA_ACCESS_KEY = os.environ.get('URA_ACCESS_KEY', '')
URA_BASE = 'https://eservice.ura.gov.sg/uraDataService'
TYPE_MAP = {'1': 'New Sale', '2': 'Sub Sale', '3': 'Resale'}

if not DATABASE_URL:
    raise SystemExit('Set DATABASE_URL environment variable (your Supabase connection string)')
if not URA_ACCESS_KEY:
    raise SystemExit('Set URA_ACCESS_KEY environment variable')

# ── Get token ────────────────────────────────────────────────────────────────
print('Getting URA token...')
r = urllib.request.urlopen(
    urllib.request.Request(f'{URA_BASE}/insertNewToken/v1', headers={'AccessKey': URA_ACCESS_KEY}),
    timeout=30
)
token_data = json.loads(r.read())
if token_data.get('Status') != 'Success':
    raise SystemExit(f'Token error: {token_data}')
token = token_data['Result']
print(f'  Token obtained.')

# ── Fetch all 4 batches ──────────────────────────────────────────────────────
rows = []
for batch in range(1, 5):
    print(f'  Fetching batch {batch}/4...', end=' ', flush=True)
    req = urllib.request.Request(
        f'{URA_BASE}/invokeUraDS/v1?service=PMI_Resi_Transaction&batch={batch}',
        headers={'AccessKey': URA_ACCESS_KEY, 'Token': token}
    )
    r = urllib.request.urlopen(req, timeout=60)
    data = json.loads(r.read())
    if data.get('Status') != 'Success':
        print(f'skipped (status={data.get("Status")})')
        continue
    for proj in data.get('Result', []):
        mkt = proj.get('marketSegment', '')
        for det in proj.get('transaction', []):
            cd = str(det.get('contractDate', ''))
            try:
                mo, yr = int(cd[:2]), int(cd[2:])
                year = 2000 + yr if yr < 100 else yr
            except Exception:
                continue
            area_sqft = float(det.get('area') or 0)
            area_sqm  = area_sqft / 10.764
            price     = float(det.get('price') or 0)
            unit_psf  = (price / area_sqft) if area_sqft else 0.0
            unit_psm  = (price / area_sqm)  if area_sqm  else 0.0
            rows.append((
                proj.get('project', ''),
                det.get('street', proj.get('street', '')),
                det.get('propertyType', ''),
                mkt,
                str(det.get('district', '0')).zfill(2),
                det.get('floorRange') or det.get('floorLevel', ''),
                area_sqft,
                area_sqm,
                TYPE_MAP.get(str(det.get('typeOfSale', '3')), 'Resale'),
                price,
                unit_psf,
                unit_psm,
                det.get('tenure', ''),
                int(float(det.get('noOfUnits') or 1)),
                f'{year}-{mo:02d}',
                datetime.utcnow().isoformat(),
            ))
    print(f'{len(rows):,} records so far')

if not rows:
    raise SystemExit('No records fetched — check URA_ACCESS_KEY')

# ── Write to Supabase ────────────────────────────────────────────────────────
print(f'\nConnecting to Supabase...')
conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()
print(f'Clearing existing URA transactions...')
cur.execute('DELETE FROM ura_transactions')
print(f'Inserting {len(rows):,} records...')
psycopg2.extras.execute_values(cur, """
    INSERT INTO ura_transactions
        (project, street, property_type, market_segment,
         postal_district, floor_level, floor_area_sqft, floor_area_sqm,
         type_of_sale, transacted_price, unit_price_psf, unit_price_psm,
         tenure, num_units, sale_date, upload_batch)
    VALUES %s
""", rows, page_size=1000)
conn.commit()
conn.close()
print(f'Done. {len(rows):,} URA records written to Supabase.')
print('Now go to your admin panel and click "Retrain Private Model".')
