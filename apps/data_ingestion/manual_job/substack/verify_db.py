import os
import sys
import psycopg2

# Import common utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../utils'))
from ingestion_utils import load_env, get_db_connection

load_env()

try:
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 's_substack';")
        tables = cur.fetchall()
        print("\nTables in s_substack:")
        for t in tables:
            table_name = t[0]
            query = "SELECT count(*) FROM s_substack.\"{}\"".format(table_name)
            cur.execute(query)
            count = cur.fetchone()[0]
            print(f" - {table_name}: {count} rows")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
