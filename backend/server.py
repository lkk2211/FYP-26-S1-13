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

DB_PATH = 'users.db'

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            phone TEXT DEFAULT ''
        )
    """)

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
    # Mock stats for admin dashboard
    return jsonify({
        "total_users": 12847,
        "total_predictions": 27544,
        "db_size": "12.3 GB"
    })

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
            "phone": user["phone"]
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
