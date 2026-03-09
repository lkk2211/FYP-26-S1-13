import pickle

# Create a dummy model object
dummy_model = {"version": "1.0", "type": "MockRegressor"}

# Save to model.pkl
with open('/backend/model.pkl', 'wb') as f:
    pickle.dump(dummy_model, f)
