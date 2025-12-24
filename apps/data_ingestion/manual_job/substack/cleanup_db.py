import os
import sys
import psycopg2

# Import common utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../utils'))
from ingestion_utils import load_env, get_db_connection

load_env()

UNIFIED_TABLES = {'posts', 'emails', 'post_delivers', 'post_opens'}

try:
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 's_substack';")
        tables = cur.fetchall()
        for t in tables:
            table_name = t[0]
            if table_name not in UNIFIED_TABLES:
                print(f"Dropping unwanted table: {table_name}")
                cur.execute(f"DROP TABLE s_substack.\"{table_name}\" CASCADE;")
    conn.commit()
    conn.close()
    print("Cleanup complete.")
except Exception as e:
    print(f"Error: {e}")
