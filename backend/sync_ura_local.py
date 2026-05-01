#!/usr/bin/env python3
"""
Run this script LOCALLY (not on Render) to sync URA data directly to Supabase.
Your home IP is not blocked by URA's bot protection; Render's data centre IP is.

Usage:
    DATABASE_URL="postgresql://..." URA_ACCESS_KEY="your_key" python sync_ura_local.py

Or export the vars in your shell then run:
    python sync_ura_local.py
"""
import os, json, time, urllib.request, urllib.error, psycopg2, psycopg2.extras
from datetime import datetime

DATABASE_URL   = os.environ.get('DATABASE_URL', '')
URA_ACCESS_KEY = os.environ.get('URA_ACCESS_KEY', '')
URA_BASE       = 'https://eservice.ura.gov.sg/uraDataService'
TYPE_MAP       = {'1': 'New Sale', '2': 'Sub Sale', '3': 'Resale'}

# Browser-like headers to bypass URA's L7 bot protection
BROWSER_HEADERS = {
    'AccessKey':       URA_ACCESS_KEY,   # will be overridden per request
    'User-Agent':      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/124.0.0.0 Safari/537.36',
    'Accept':          'application/json, text/plain, */*',
    'Accept-Language': 'en-SG,en;q=0.9',
    'Connection':      'keep-alive',
    'Referer':         'https://eservice.ura.gov.sg/',
    'Origin':          'https://eservice.ura.gov.sg',
}

if not DATABASE_URL:
    raise SystemExit('ERROR: Set DATABASE_URL (your Supabase connection string)')
if not URA_ACCESS_KEY:
    raise SystemExit('ERROR: Set URA_ACCESS_KEY')


def _request(url, extra_headers=None, retries=4, backoff=3):
    """Make a request with browser headers + retry on failure."""
    headers = {**BROWSER_HEADERS, 'AccessKey': URA_ACCESS_KEY}
    if extra_headers:
        headers.update(extra_headers)
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            r   = urllib.request.urlopen(req, timeout=60)
            raw = r.read()
            # Detect HTML bot-challenge response
            if raw[:20].strip().startswith(b'<'):
                raise RuntimeError(
                    f'URA returned HTML (bot challenge) — try again in a minute.\n'
                    f'Preview: {raw[:200]}'
                )
            # Decode: try UTF-8 first, fall back to Latin-1 for accented chars in names
            try:
                text = raw.decode('utf-8')
            except UnicodeDecodeError:
                text = raw.decode('latin-1')
            return json.loads(text)
        except urllib.error.HTTPError as e:
            body = e.read()[:300]
            print(f'    HTTP {e.code} on attempt {attempt}: {body}')
        except Exception as e:
            print(f'    Error on attempt {attempt}: {e}')
        if attempt < retries:
            sleep = backoff * attempt
            print(f'    Retrying in {sleep}s…')
            time.sleep(sleep)
    raise RuntimeError(f'All {retries} attempts failed for {url}')


# ── Step 1: Get token ─────────────────────────────────────────────────────────
print('Getting URA token…')
token_data = _request(f'{URA_BASE}/insertNewToken/v1')
if token_data.get('Status') != 'Success':
    raise SystemExit(f'Token error: {token_data}')
token = token_data['Result']
print(f'  Token: {token[:12]}…\n')

TOKEN_HEADER = {'Token': token}


def _parse_floor_mid(fl):
    """Return a rough numeric floor from a range string like '01-05'."""
    import re
    fl = str(fl or '').strip().upper()
    nums = re.findall(r'\d+', fl)
    if len(nums) >= 2:
        return (int(nums[0]) + int(nums[1])) / 2
    if len(nums) == 1:
        return float(nums[0])
    if 'LOW' in fl:  return 4.0
    if 'MID' in fl:  return 13.0
    if 'HIGH' in fl: return 25.0
    return 0.0


# ── Step 2: Fetch PMI_Resi_Transaction batches 1–4 ──────────────────────────
rows = []
batch_id = datetime.utcnow().isoformat()

for batch in range(1, 5):
    print(f'Fetching PMI_Resi_Transaction batch {batch}/4…', end=' ', flush=True)
    try:
        data = _request(
            f'{URA_BASE}/invokeUraDS/v1?service=PMI_Resi_Transaction&batch={batch}',
            extra_headers=TOKEN_HEADER
        )
    except RuntimeError as e:
        print(f'\n  SKIPPED — {e}')
        continue

    if data.get('Status') != 'Success':
        print(f'skipped (status={data.get("Status")})')
        continue

    before = len(rows)
    for proj in data.get('Result', []):
        mkt    = proj.get('marketSegment', '')
        street = proj.get('street', '')
        for det in proj.get('transaction', []):
            cd = str(det.get('contractDate', ''))
            try:
                mo, yr = int(cd[:2]), int(cd[2:])
                year   = 2000 + yr if yr < 100 else yr
            except Exception:
                continue
            area_sqm  = float(det.get('area') or 0)   # URA API returns area in sqm
            area_sqft = area_sqm * 10.764
            price     = float(det.get('price') or 0)
            unit_psf  = (price / area_sqft) if area_sqft else 0.0   # S$/sqft
            unit_psm  = (price / area_sqm)  if area_sqm  else 0.0   # S$/sqm
            rows.append((
                proj.get('project', ''),
                det.get('street', street),
                det.get('propertyType', ''),
                mkt,
                str(det.get('district', '0')).zfill(2),
                det.get('floorRange') or det.get('floorLevel', ''),
                area_sqft, area_sqm,
                TYPE_MAP.get(str(det.get('typeOfSale', '3')), 'Resale'),
                price, unit_psf, unit_psm,
                det.get('tenure', ''),
                int(float(det.get('noOfUnits') or 1)),
                f'{year}-{mo:02d}',
                batch_id,
            ))
    print(f'+{len(rows) - before:,}  →  {len(rows):,} total')
    time.sleep(1)   # be polite between batches

# ── Step 3: Fetch rental median data (bonus table if it exists) ───────────────
# URA also exposes PMI_Resi_RentalMedian — fetch it so we have richer data
rental_rows = []
for batch in range(1, 5):
    print(f'Fetching PMI_Resi_RentalMedian batch {batch}/4…', end=' ', flush=True)
    try:
        data = _request(
            f'{URA_BASE}/invokeUraDS/v1?service=PMI_Resi_RentalMedian&batch={batch}',
            extra_headers=TOKEN_HEADER
        )
    except RuntimeError:
        print('skipped')
        continue
    if data.get('Status') != 'Success':
        print(f'skipped (status={data.get("Status")})')
        continue
    before = len(rental_rows)
    for proj in data.get('Result', []):
        for det in (proj.get('rental', []) or []):
            cd = str(det.get('x_q', '') or det.get('refPeriod', ''))
            rental_rows.append({
                'project':       proj.get('project', ''),
                'street':        proj.get('street', ''),
                'property_type': proj.get('propertyType', ''),
                'district':      str(proj.get('district', '0')).zfill(2),
                'bedroom':       det.get('noOfBedRoom', ''),
                'median_rent':   float(det.get('median', 0) or 0),
                'ref_period':    cd,
            })
    print(f'+{len(rental_rows) - before:,}  →  {len(rental_rows):,} total')
    time.sleep(1)

print()

if not rows:
    raise SystemExit('No transaction records fetched — check URA_ACCESS_KEY and retry')

# ── Step 4: Write to Supabase ─────────────────────────────────────────────────
print(f'Connecting to Supabase…')
conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

# -- Transactions: incremental upsert (skip existing project+sale_date+floor_level)
print(f'Upserting {len(rows):,} transaction records (incremental)…')
psycopg2.extras.execute_values(cur, """
    INSERT INTO ura_transactions
        (project, street, property_type, market_segment,
         postal_district, floor_level, floor_area_sqft, floor_area_sqm,
         type_of_sale, transacted_price, unit_price_psf, unit_price_psm,
         tenure, num_units, sale_date, upload_batch)
    VALUES %s
    ON CONFLICT DO NOTHING
""", rows, page_size=1000)
tx_inserted = cur.rowcount
conn.commit()
print(f'  {tx_inserted:,} new rows inserted ({len(rows) - tx_inserted:,} already existed)')

# -- Rental medians (best-effort — skip if table missing)
if rental_rows:
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ura_rental_medians (
                id              SERIAL PRIMARY KEY,
                project         TEXT,
                street          TEXT,
                property_type   TEXT,
                district        TEXT,
                bedroom         TEXT,
                median_rent     NUMERIC,
                ref_period      TEXT,
                UNIQUE (project, bedroom, ref_period)
            )
        """)
        psycopg2.extras.execute_values(cur, """
            INSERT INTO ura_rental_medians
                (project, street, property_type, district, bedroom, median_rent, ref_period)
            VALUES %s
            ON CONFLICT (project, bedroom, ref_period) DO NOTHING
        """, [
            (r['project'], r['street'], r['property_type'], r['district'],
             r['bedroom'], r['median_rent'], r['ref_period'])
            for r in rental_rows
        ], page_size=500)
        rent_inserted = cur.rowcount
        conn.commit()
        print(f'  {rent_inserted:,} new rental median rows inserted')
    except Exception as e:
        conn.rollback()
        print(f'  Rental median insert skipped: {e}')

conn.close()
print(f'\nDone. Run "python sync_ura_local.py" again anytime to pick up new quarters.')
