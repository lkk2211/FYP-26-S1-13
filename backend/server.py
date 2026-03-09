from flask import Flask, request, jsonify, send_from_directory
import os
import json
import sys
import os

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from predict import predict_price

app = Flask(__name__, static_folder='../frontend')

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

if __name__ == '__main__':
    # The platform requires port 3000
    app.run(host='0.0.0.0', port=3000, debug=True)
