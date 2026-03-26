import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = 3000;

app.use(express.json());

// Serve static files from the frontend directory
app.use(express.static(path.join(__dirname, 'frontend')));

// Mock API for Prediction
app.post('/api/predict', (req, res) => {
    const { area, bedrooms, floor } = req.body;
    
    // Simple mock logic
    const basePrice = 300000;
    const estimatedValue = basePrice + (area * 150) + (bedrooms * 20000) + (floor * 5000);
    
    res.json({
        estimated_value: estimatedValue,
        confidence: 94,
        factors: [
            { name: "Location Premium", score: 85 },
            { name: "Floor Level Bonus", score: 65 },
            { name: "Area Efficiency", score: 78 },
            { name: "Market Trend", score: 72 }
        ]
    });
});

// Mock API for Stats
app.get('/api/stats', (req, res) => {
    res.json({
        total_users: 12847,
        total_predictions: 27544,
        db_size: "12.3 GB"
    });
});

// Fallback to index.html for SPA
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'frontend', 'index.html'));
});

app.listen(PORT, '0.0.0.0', () => {
    console.log(`Preview server running on http://localhost:${PORT}`);
    console.log(`Note: This Node.js server is for the online preview.`);
    console.log(`For local deployment, use the Python server in /backend.`);
});
