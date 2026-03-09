import random
import pickle
import os

# Mock Prediction Function
def predict_price(features):
    # In a real app, you'd load model.pkl and use it
    # For now, we'll mock the prediction logic
    
    # Extract features from request
    postal = features.get('postal', '000000')
    area = float(features.get('area', 1000))
    bedrooms = int(features.get('bedrooms', 3))
    floor = int(features.get('floor', 10))
    
    # Mock calculation
    base_price = 300000
    area_factor = area * 150
    bedroom_factor = bedrooms * 20000
    floor_factor = floor * 5000
    
    # Add some randomness
    noise = random.randint(-10000, 10000)
    
    estimated_value = base_price + area_factor + bedroom_factor + floor_factor + noise
    
    # Mock factors
    factors = [
        {"name": "Location Premium", "score": random.randint(70, 95)},
        {"name": "Floor Level Bonus", "score": random.randint(40, 80)},
        {"name": "Area Efficiency", "score": random.randint(60, 90)},
        {"name": "Market Trend", "score": random.randint(50, 75)}
    ]
    
    return {
        "estimated_value": int(estimated_value),
        "confidence": random.randint(88, 96),
        "factors": factors
    }

# Function to save a dummy model if needed
def save_dummy_model():
    dummy_model = {"version": "1.0", "type": "MockRegressor"}
    with open('model.pkl', 'wb') as f:
        pickle.dump(dummy_model, f)

if __name__ == "__main__":
    # Test prediction
    test_features = {"postal": "238801", "area": 1200, "bedrooms": 3, "floor": 12}
    print(predict_price(test_features))
