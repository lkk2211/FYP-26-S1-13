

import os
import sqlite3

# Database path
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "propaisg.db")

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# Postal code metadata

POSTAL_META = {
    "238801": {"district": "D01", "property_type": "Condominium", "location": "Marina Bay"},
    "560123": {"district": "D19", "property_type": "HDB",         "location": "Hougang"},
    "159088": {"district": "D03", "property_type": "HDB",         "location": "Queenstown"},
    "342005": {"district": "D12", "property_type": "HDB",         "location": "Toa Payoh"},
}

# Town to district lookup 
TOWN_DISTRICT = {
    "Clementi":    "D05",
    "Ang Mo Kio":  "D20",
    "Bedok":       "D16",
    "Bishan":      "D20",
    "Bukit Batok": "D23",
    "Queenstown":  "D03",
}
