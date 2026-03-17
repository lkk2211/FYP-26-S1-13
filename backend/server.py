from flask import Flask, request, jsonify, send_from_directory
import sqlite3
import hashlib
import os
import json
import sys


# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from predict import predict_price

app = Flask(__name__, static_folder='../frontend')

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'propaisg.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name     TEXT NOT NULL,
            email         TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            phone         TEXT DEFAULT '',
            role          TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('user','admin'))
        );
        CREATE TABLE IF NOT EXISTS predictions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id         INTEGER REFERENCES users(id) ON DELETE SET NULL,
            town            TEXT,
            flat_type       TEXT,
            floor_area_sqm  REAL,
            estimated_value REAL NOT NULL,
            confidence      REAL,
            market_trend    TEXT,
            feature_scores  TEXT,
            model_version   TEXT NOT NULL DEFAULT 'v1.0.0',
            predicted_at    TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS price_records (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            postal_code     TEXT NOT NULL,
            address         TEXT,
            property_type   TEXT,
            floor_area_sqft REAL,
            num_bedrooms    INTEGER,
            floor_level     INTEGER,
            price_sgd       REAL NOT NULL,
            price_psf       REAL,
            price_date      TEXT NOT NULL,
            data_source     TEXT NOT NULL DEFAULT 'housing.csv'
        );
        CREATE TABLE IF NOT EXISTS amenities (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            amenity_name TEXT NOT NULL,
            amenity_type TEXT NOT NULL,
            latitude     REAL NOT NULL,
            longitude    REAL NOT NULL,
            source       TEXT
        );
        CREATE TABLE IF NOT EXISTS audit_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER REFERENCES users(id) ON DELETE SET NULL,
            action     TEXT NOT NULL,
            event_type TEXT NOT NULL,
            details    TEXT,
            logged_at  TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    conn.close()

    conn.commit()
    conn.close()

# Serve Frontend
@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/<path:path>')
def static_proxy(path):
    return send_from_directory(app.static_folder, path)

# API Endpoints
@app.route('/api/predict', methods=['POST'])
def predict():
    data = request.json
    # Extract features from request
    # In a real app, you'd process postal, area, etc.
    # For now, we use the predict_price function from predict.py
    result = predict_price(data)
    return jsonify(result)

@app.route('/api/stats', methods=['GET'])
def stats():
    conn = get_db()
    total_users       = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    total_predictions = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
    recent_users      = [dict(r) for r in conn.execute("SELECT id, full_name, email FROM users ORDER BY id DESC LIMIT 5").fetchall()]
    all_users         = [{"id": r["id"], "full_name": r["full_name"], "email": r["email"], "role": r["role"], "is_admin": r["role"] == "admin"} for r in conn.execute("SELECT id, full_name, email, role FROM users ORDER BY id DESC").fetchall()]
    db_bytes          = os.path.getsize(DB_PATH)
    db_size           = f"{db_bytes/1024:.1f} KB" if db_bytes < 1024**2 else f"{db_bytes/1024**2:.2f} MB"
    conn.close()
    return jsonify({"total_users": total_users, "total_predictions": total_predictions, "db_size": db_size, "recent_users": recent_users, "all_users": all_users})

@app.route('/api/trend', methods=['GET'])
def trend():
    import statistics, random
    from datetime import date, timedelta
    postal = request.args.get('postal')
    town   = request.args.get('town')
    POSTAL_META   = {"238801": {"property_type": "Condominium", "location": "Marina Bay"}, "560123": {"property_type": "HDB", "location": "Hougang"}, "159088": {"property_type": "HDB", "location": "Queenstown"}, "342005": {"property_type": "HDB", "location": "Toa Payoh"}}
    TOWN_DISTRICT = {"Clementi": "D05", "Ang Mo Kio": "D20", "Bedok": "D16", "Bishan": "D20", "Bukit Batok": "D23", "Queenstown": "D03"}
    conn = get_db()
    if postal:
        rows = conn.execute("SELECT * FROM price_records WHERE postal_code=?", (str(postal).zfill(6),)).fetchall()
    elif town:
        rows = conn.execute("SELECT * FROM price_records").fetchall()
    else:
        rows = conn.execute("SELECT * FROM price_records").fetchall()
    conn.close()
    prices = [r["price_sgd"] for r in rows] or [450000]
    avg    = statistics.mean(prices)
    rng    = random.Random(int(avg))
    today  = date.today()
    trend_data, p = [], avg * 0.94
    for i in range(6, 0, -1):
        mo = (today.replace(day=1) - timedelta(days=30*(i-1))).strftime("%b %Y")
        p  = p * rng.uniform(1.005, 1.025)
        trend_data.append({"month": mo, "price": int(p)})
    similar = []
    for idx, r in enumerate(sorted(rows, key=lambda r: abs(r["price_sgd"]-avg))[:5]):
        mo   = (today.replace(day=1) - timedelta(days=30*[1,2,2,3,4][idx])).strftime("%b %Y")
        meta = POSTAL_META.get(str(r["postal_code"]), {})
        pt   = r["property_type"] or meta.get("property_type", "HDB")
        beds = r["num_bedrooms"] or 0
        similar.append({"address": meta.get("location", r["postal_code"]), "type": f"{beds} Room" if pt=="HDB" else f"{beds} Bed", "floor_area": int(r["floor_area_sqft"] or 0), "price": int(r["price_sgd"]), "date": mo})
    return jsonify({"trend_data": trend_data, "similar_transactions": similar, "summary": {"avg_price": int(avg), "min_price": int(min(prices)), "max_price": int(max(prices)), "total_transactions": len(rows)}})\

    
@app.route('/api/register', methods=['POST'])
def register():
    data = request.json

    full_name = data.get('full_name', '').strip()
    email = data.get('email', '').strip().lower()
    password = data.get('password', '').strip()

    if not full_name or not email or not password:
        return jsonify({"error": "All fields are required"}), 400

    conn = get_db()

    existing = conn.execute(
        "SELECT * FROM users WHERE email = ?",
        (email,)
    ).fetchone()

    if existing:
        conn.close()
        return jsonify({"error": "Email already registered"}), 400

    password_hash = hashlib.sha256(password.encode()).hexdigest()

    cursor = conn.execute(
        "INSERT INTO users (full_name, email, password_hash) VALUES (?, ?, ?)",
        (full_name, email, password_hash)
    )
    conn.commit()

    user_id = cursor.lastrowid

    user = conn.execute(
        "SELECT id, full_name, email, phone FROM users WHERE id = ?",
        (user_id,)
    ).fetchone()

    conn.close()

    return jsonify({
        "user": dict(user)
    }), 201


@app.route('/api/login', methods=['POST'])
def login():
    data = request.json

    email = data.get('email', '').strip().lower()
    password = data.get('password', '').strip()

    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400

    conn = get_db()

    user = conn.execute(
        "SELECT * FROM users WHERE email = ?",
        (email,)
    ).fetchone()

    conn.close()

    if not user:
        return jsonify({"error": "User not found"}), 404

    password_hash = hashlib.sha256(password.encode()).hexdigest()

    if user['password_hash'] != password_hash:
        return jsonify({"error": "Wrong password"}), 401

    return jsonify({
        "user": {
            "id": user["id"],
            "full_name": user["full_name"],
            "email": user["email"],
            "phone": user["phone"],
            "role": user["role"],
            "is_admin": user["role"] == "admin"
        }
    })


@app.route('/api/profile/<int:user_id>', methods=['PUT'])
def update_profile(user_id):
    data = request.json

    full_name = data.get('full_name', '').strip()
    email = data.get('email', '').strip().lower()
    phone = data.get('phone', '').strip()

    if not full_name or not email:
        return jsonify({"error": "Full name and email are required"}), 400

    conn = get_db()

    existing = conn.execute(
        "SELECT id FROM users WHERE email = ? AND id != ?",
        (email, user_id)
    ).fetchone()

    if existing:
        conn.close()
        return jsonify({"error": "Email already in use"}), 400

    conn.execute(
        "UPDATE users SET full_name = ?, email = ?, phone = ? WHERE id = ?",
        (full_name, email, phone, user_id)
    )
    conn.commit()

    user = conn.execute(
        "SELECT id, full_name, email, phone FROM users WHERE id = ?",
        (user_id,)
    ).fetchone()

    conn.close()

    if not user:
        return jsonify({"error": "User not found"}), 404

    return jsonify({
        "user": dict(user)
    })

if __name__ == '__main__':
    # The platform requires port 3000
    init_db()
    app.run(host='0.0.0.0', port=3000, debug=True)
