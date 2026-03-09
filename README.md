# FYP-26-S1-13 Property Prediction Webside
##Intro
Property Prediction Webside Develop

## Project Structure

```
propai/
├── frontend/
│   ├── index.html
│   ├── app.js
│   └── style.css
├── backend/
│   ├── server.py
│   ├── model.pkl
│   └── predict.py
├── dataset/
│   └── housing.csv
├── requirements.txt
└── README.md
```

## Setup Instructions

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the Backend**:
   ```bash
   cd backend
   python server.py
   ```

3. **Access the Application**:
   Open your browser and navigate to `http://localhost:5000`.

## Features

- **AI Price Prediction**: Instant property valuations based on local data.
- **Market Trends**: Visualize historical price changes.
- **Admin Dashboard**: Monitor system stats and user activity.
- **Map View**: Explore property locations and nearby amenities.

## No External API Dependencies

This version of PropAI has been modified to run entirely locally:
- **Google Maps**: Replaced with static map images and custom markers.
- **Google Fonts**: Replaced with standard system fonts.

- **Backend**: Uses a local Python server for all API calls.
```
