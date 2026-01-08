# scripts/db_smoke_test.py
"""
"Smoke test" = a tiny test to confirm the database connection works.

This script verifies:
1) Python can connect to Postgres
2) PostGIS is installed (we query PostGIS_Version())

If this script works, you're ready to write ingestion scripts that load data.
"""

from sqlalchemy import text
"""
With __innit__.py in the ingest/ folder, we can import from ingest.db
instead of needing to mess with sys.path. 
Allowing us to reuse the get_engine() function defined in ingest/db.py.
"""
from ingest.db import get_engine

def main():
    # Create the SQLAlchemy engine (connection factory)
    engine = get_engine()

    # Open a connection and run two simple queries
    # Using a context manager ensures the connection is closed properly after use.
    with engine.connect() as conn:
        # SELECT 1 is a classic "are you alive?" check
        one = conn.execute(text("SELECT 1")).scalar()
        print("SELECT 1 =>", one)

        # Confirm PostGIS is installed and available
        postgis = conn.execute(text("SELECT PostGIS_Version()")).scalar()
        print("PostGIS =>", postgis)

if __name__ == "__main__":
    main()