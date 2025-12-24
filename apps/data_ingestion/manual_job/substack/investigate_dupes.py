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
        # Let's find exactly which schema it is in
        cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE '%marts%';")
        schemas = cur.fetchall()
        print(f"Available marts-like schemas: {schemas}")
        
        schema = "marts" # Default
        if schemas:
            schema = schemas[0][0]
            print(f"Using schema: {schema}")

        print("\nChecking for duplicates in fct_substack__post_events...")
        query = f"""
        SELECT post_id, email, event_at, event_type, count(*)
        FROM {schema}.fct_substack__post_events
        GROUP BY 1, 2, 3, 4
        HAVING count(*) > 1
        LIMIT 10;
        """
        cur.execute(query)
        rows = cur.fetchall()
        if not rows:
            print("No duplicates found with the current grouping keys.")
        else:
            print("Found duplicates (Sample):")
            for row in rows:
                print(row)
                
        print("\nChecking total row count vs unique grains...")
        cur.execute(f"SELECT count(*) FROM {schema}.fct_substack__post_events;")
        total = cur.fetchone()[0]
        cur.execute(f"SELECT count(DISTINCT (post_id, email, event_at, event_type)) FROM {schema}.fct_substack__post_events;")
        distinct = cur.fetchone()[0]
        print(f"Total rows: {total}")
        print(f"Distinct grains: {distinct}")
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
