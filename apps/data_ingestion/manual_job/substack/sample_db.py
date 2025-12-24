import os
import sys
import psycopg2

# Import common utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../utils'))
from ingestion_utils import load_env, get_db_connection

load_env()

def print_table_sample(cur, table_name, limit=3):
    print(f"\n--- Sample from {table_name} ---")
    try:
        cur.execute(f"SELECT * FROM s_substack.\"{table_name}\" LIMIT {limit};")
        colnames = [desc[0] for desc in cur.description]
        print(", ".join(colnames))
        rows = cur.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"Error reading {table_name}: {e}")

try:
    conn = get_db_connection()
    with conn.cursor() as cur:
        for t in ["posts", "emails", "post_delivers", "post_opens"]:
            print_table_sample(cur, t)
    conn.close()
except Exception as e:
    print(f"Error: {e}")
