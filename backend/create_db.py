
import argparse
import hashlib
import os
import sqlite3
from datetime import date

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "propaisg.db")


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    full_name     TEXT    NOT NULL,
    email         TEXT    NOT NULL UNIQUE,
    password_hash TEXT    NOT NULL,
    phone         TEXT    NOT NULL DEFAULT '',
    role          TEXT    NOT NULL DEFAULT 'user'
                          CHECK (role IN ('user', 'admin'))
);

CREATE TABLE IF NOT EXISTS properties (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER REFERENCES users(id) ON DELETE SET NULL,
    postal_code     TEXT    NOT NULL,
    address         TEXT    NOT NULL,
    building_name   TEXT,
    property_type   TEXT    NOT NULL DEFAULT 'HDB',
    lease_type      TEXT    NOT NULL DEFAULT '99-year',
    floor_area_sqft REAL,
    num_bedrooms    INTEGER,
    floor_level     INTEGER,
    district        TEXT,
    location        TEXT,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS predictions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER REFERENCES users(id) ON DELETE SET NULL,
    postal_code     TEXT,
    town            TEXT,
    flat_type       TEXT,
    floor_area_sqm  REAL,
    estimated_value REAL    NOT NULL,
    confidence      REAL,
    market_trend    TEXT,
    feature_scores  TEXT,
    model_version   TEXT    NOT NULL DEFAULT 'v1.0.0',
    predicted_at    TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS price_records (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    postal_code     TEXT    NOT NULL,
    address         TEXT,
    property_type   TEXT,
    floor_area_sqft REAL,
    num_bedrooms    INTEGER,
    floor_level     INTEGER,
    price_sgd       REAL    NOT NULL,
    price_psf       REAL,
    price_date      TEXT    NOT NULL,
    data_source     TEXT    NOT NULL DEFAULT 'housing.csv'
);

CREATE TABLE IF NOT EXISTS amenities (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    amenity_name  TEXT    NOT NULL,
    amenity_type  TEXT    NOT NULL,
    latitude      REAL    NOT NULL,
    longitude     REAL    NOT NULL,
    source        TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER REFERENCES users(id) ON DELETE SET NULL,
    action      TEXT    NOT NULL,
    event_type  TEXT    NOT NULL,
    details     TEXT,
    ip_address  TEXT,
    logged_at   TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_users_email          ON users(email)",
    "CREATE INDEX IF NOT EXISTS idx_users_role           ON users(role)",
    "CREATE INDEX IF NOT EXISTS idx_predictions_user     ON predictions(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_predictions_date     ON predictions(predicted_at)",
    "CREATE INDEX IF NOT EXISTS idx_price_records_postal ON price_records(postal_code)",
    "CREATE INDEX IF NOT EXISTS idx_price_records_date   ON price_records(price_date)",
    "CREATE INDEX IF NOT EXISTS idx_audit_user           ON audit_log(user_id)",
]

ALL_TABLES = ["users", "properties", "predictions", "price_records", "amenities", "audit_log"]


def _hash(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


def seed(conn: sqlite3.Connection):
    today = date.today().isoformat()

    conn.executemany(
        "INSERT OR IGNORE INTO users (full_name, email, password_hash, phone, role) VALUES (?,?,?,?,?)",
        [
            ("PropAI Admin",     "admin@propaig.sg", _hash("Admin@2026!"), "",         "admin"),
            ("Jayne Tan",        "jayne@test.com",   _hash("123456"),      "81234567", "user"),
            ("John Tan Jie Wen", "johnt@test.com",   _hash("123456"),      "91234567", "user"),
        ],
    )

    conn.executemany(
        "INSERT OR IGNORE INTO price_records"
        " (postal_code, address, property_type, floor_area_sqft, num_bedrooms, floor_level, price_sgd, price_psf, price_date)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        [
            ("238801", "1 St Martin Dr",         "Condominium", 1200, 3, 12, 465000, round(465000/1200, 2), today),
            ("238801", "1 St Martin Dr",         "Condominium", 1300, 4, 15, 510000, round(510000/1300, 2), today),
            ("560123", "Blk 123 Hougang Ave 1",  "HDB",         1000, 3, 10, 428000, round(428000/1000, 2), today),
            ("560123", "Blk 123 Hougang Ave 1",  "HDB",          900, 2,  8, 410000, round(410000/900,  2), today),
            ("159088", "Blk 88 Queenstown Ave",  "HDB",          800, 2,  5, 385000, round(385000/800,  2), today),
            ("159088", "Blk 88 Queenstown Ave",  "HDB",         1100, 3, 12, 440000, round(440000/1100, 2), today),
            ("342005", "Blk 5 Toa Payoh Lor 7",  "HDB",         1100, 3, 11, 452000, round(452000/1100, 2), today),
            ("342005", "Blk 5 Toa Payoh Lor 7",  "HDB",         1000, 3, 10, 435000, round(435000/1000, 2), today),
        ],
    )

    conn.executemany(
        "INSERT OR IGNORE INTO amenities (id, amenity_name, amenity_type, latitude, longitude, source) VALUES (?,?,?,?,?,?)",
        [
            (1,  "Clementi MRT Station",           "MRT",           1.3152, 103.7649, "LTA"),
            (2,  "Dover MRT Station",               "MRT",           1.3111, 103.7789, "LTA"),
            (3,  "Marina Bay MRT Station",          "MRT",           1.2764, 103.8555, "LTA"),
            (4,  "Raffles Place MRT Station",       "MRT",           1.2837, 103.8514, "LTA"),
            (5,  "Blk 341 Clementi Ave 5 Bus Stop", "Bus Stop",      1.3145, 103.7631, "LTA"),
            (6,  "Clementi Mall",                   "Shopping Mall", 1.3150, 103.7651, "URA"),
            (7,  "West Coast Plaza",                "Shopping Mall", 1.3074, 103.7630, "URA"),
            (8,  "Clementi Primary School",         "School",        1.3166, 103.7620, "MOE"),
            (9,  "Nan Hua High School",             "School",        1.3186, 103.7613, "MOE"),
            (10, "Ng Teng Fong General Hospital",   "Healthcare",    1.3339, 103.7436, "MOH"),
            (11, "Clementi Polyclinic",             "Healthcare",    1.3142, 103.7625, "MOH"),
            (12, "Clementi Hawker Centre",          "Hawker Centre", 1.3148, 103.7642, "NEA"),
        ],
    )

    conn.executemany(
        "INSERT INTO audit_log (user_id, action, event_type, details) VALUES (?,?,?,?)",
        [
            (1, "Database initialised", "SYSTEM", "create_db.py seed"),
            (2, "User registered",      "AUTH",   "Seed account"),
            (3, "User registered",      "AUTH",   "Seed account"),
        ],
    )

    conn.commit()


def create(conn: sqlite3.Connection):
    conn.executescript(SCHEMA)
    for idx in INDEXES:
        conn.execute(idx)
    conn.commit()
    seed(conn)


def reset(conn: sqlite3.Connection):
    conn.execute("PRAGMA foreign_keys = OFF")
    for tbl in reversed(ALL_TABLES):
        conn.execute(f"DROP TABLE IF EXISTS {tbl}")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.commit()
    create(conn)


def verify(conn: sqlite3.Connection):
    existing = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    print(f"\n  {'Table':<22} {'Status':<10} Rows")
    print("  " + "-" * 38)
    all_ok = True
    for tbl in ALL_TABLES:
        found = tbl in existing
        if not found:
            all_ok = False
        count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0] if found else "-"
        print(f"  {tbl:<22} {'OK' if found else 'MISSING':<10} {count}")
    print()
    if not all_ok:
        print("  Some tables missing. Run: python create_db.py\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--reset",  action="store_true")
    args = parser.parse_args()

    print(f"DB: {DB_PATH}\n")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    try:
        if args.verify:
            verify(conn)
        elif args.reset:
            reset(conn)
            verify(conn)
        else:
            create(conn)
            verify(conn)
            print("  Credentials:")
            print("    admin@propaisg.sg / Admin@2026!  (admin)")
            print("    jayne@test.com   / 123456       (user)")
            print("    johnt@test.com   / 123456       (user)\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
