import http.server
import socketserver
import json
import os
import sys
import time
from urllib.parse import urlparse

PORT = 3000

class MyHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        # Log to stderr for platform visibility
        sys.stderr.write("%s - - [%s] %s\n" %
                         (self.client_address[0],
                          self.log_date_time_string(),
                          format%args))

    def do_GET(self):
        parsed_path = urlparse(self.path)
        if parsed_path.path == '/api/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok", "backend": "python", "version": "2.4.0"}).encode())
        elif parsed_path.path == '/api/stats':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            stats = {
                "total_users": 12847,
                "total_predictions": 27544,
                "db_size": "12.3 GB",
                "uptime": "14d 6h 22m",
                "server_load": 0.42
            }
            self.wfile.write(json.dumps(stats).encode())
        else:
            # Ensure index.html is served for the root path
            if parsed_path.path == '/':
                self.path = '/index.html'
            return http.server.SimpleHTTPRequestHandler.do_GET(self)

    def do_POST(self):
        parsed_path = urlparse(self.path)
        if parsed_path.path == '/api/predict':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            params = json.loads(post_data.decode('utf-8'))
            
            # Simulate processing time
            time.sleep(0.5)
            
            town = params.get('town', 'Clementi')
            flat_type = params.get('type', '4 Room')
            area = float(params.get('area', 92))
            
            # Base price calculation logic (mock)
            base_prices = {
                'Clementi': 5000,
                'Ang Mo Kio': 4800,
                'Bedok': 4500,
                'Bishan': 5500,
                'Bukit Batok': 4200,
                'Queenstown': 5800
            }
            
            type_multiplier = {
                '3 Room': 0.8,
                '4 Room': 1.0,
                '5 Room': 1.2,
                'Executive': 1.4
            }
            
            price_per_sqm = base_prices.get(town, 4000) * type_multiplier.get(flat_type, 1.0)
            estimated_value = int(price_per_sqm * area)
            
            prediction = {
                "estimated_value": estimated_value,
                "confidence": 92 if town in base_prices else 85,
                "trend": "up",
                "factors": [
                    {"name": "Location Premium", "score": 95 if town in ['Clementi', 'Bishan', 'Queenstown'] else 70},
                    {"name": "Floor Level", "score": 88},
                    {"name": "Remaining Lease", "score": 75},
                    {"name": "Nearby Amenities", "score": 90}
                ]
            }
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(prediction).encode())
        else:
            self.send_error(404, "Not Found")

# Allow address reuse to prevent "Address already in use" errors on restart
socketserver.TCPServer.allow_reuse_address = True

with socketserver.TCPServer(("0.0.0.0", PORT), MyHandler) as httpd:
    print(f"Python Server running on http://0.0.0.0:{PORT}")
    sys.stdout.flush()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        httpd.shutdown()
