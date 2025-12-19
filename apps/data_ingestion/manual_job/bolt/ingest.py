import os
import sys
import csv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from tqdm import tqdm

# --- Configuration ---
ENV_DIR = os.path.join(os.path.dirname(__file__), '../../../../docker')
ENV_EXAMPLE = os.path.join(ENV_DIR, '.env.example')
ENV_REAL = os.path.join(ENV_DIR, '.env')

# Load env vars
if os.path.exists(ENV_EXAMPLE):
    load_dotenv(ENV_EXAMPLE)
if os.path.exists(ENV_REAL):
    load_dotenv(ENV_REAL, override=True)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("POSTGRES_DB", "jimwurst_db")
DB_USER = os.getenv("POSTGRES_USER", "jimwurst_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "jimwurst_password")
DB_PORT = os.getenv("DB_PORT", "5432")

DEFAULT_DATA_PATH = os.path.expanduser("~/Documents/jimwurst_local_data/bolt")
DATA_PATH = os.getenv("BOLT_DATA_PATH", DEFAULT_DATA_PATH)

SCHEMA_NAME = "s_bolt"

# Mapping: Filename (basename) -> Table Name
# We can just use the filename (without extension) as the default table name,
# but can override here if needed.
SPECIFIC_TABLE_MAPPING = {
    "rides.csv": "rides",
    "transactions.csv": "transactions",
    "orders.csv": "orders",
    "micromobility_events.csv": "micromobility_events",
    "car_rental_events.csv": "car_rental_events",
    "profile.csv": "profile",
    "login_history.csv": "login_history"
}

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA_NAME)))
    conn.commit()
    print(f"Schema '{SCHEMA_NAME}' checked/created.")

def sanitize_column_name(col_name):
    """
    Converts 'Pick-up Address' -> 'pick_up_address'
    """
    return col_name.strip().lower().replace(" ", "_").replace("-", "_").replace(".", "")

def ingest_csv(conn, file_path, table_name):
    print(f"Processing {os.path.basename(file_path)} -> {SCHEMA_NAME}.{table_name}")
    
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            print(f"  Empty file, skipping.")
            return

        sanitized_headers = [sanitize_column_name(h) for h in headers]
        
        # 1. Create Table (Full Refresh)
        # We'll treat everything as TEXT for simplicity in this raw ingestion layer (ODS/Staging logic)
        # unless we want to get fancy with type inference.
        
        cols_ddl = [sql.SQL("{} TEXT").format(sql.Identifier(col)) for col in sanitized_headers]
        
        drop_query = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
            sql.Identifier(SCHEMA_NAME), sql.Identifier(table_name)
        )
        
        create_query = sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(SCHEMA_NAME),
            sql.Identifier(table_name),
            sql.SQL(", ").join(cols_ddl)
        )
        
        with conn.cursor() as cur:
            cur.execute(drop_query)
            cur.execute(create_query)
        conn.commit()
        
        # 2. Insert Data
        batch_size = 5000
        batch = []
        count = 0
        
        insert_query = sql.SQL("INSERT INTO {}.{} VALUES %s").format(
            sql.Identifier(SCHEMA_NAME),
            sql.Identifier(table_name)
        )
        
        with conn.cursor() as cur:
            for row in tqdm(reader, desc=f"  Loading {table_name}", unit="rows"):
                # Handle row length mismatch (simple CSVs might be malformed)
                if len(row) != len(headers):
                    # quick fix: pad or truncate
                    if len(row) < len(headers):
                        row += [None] * (len(headers) - len(row))
                    else:
                        row = row[:len(headers)]
                
                batch.append(row)
                count += 1
                
                if len(batch) >= batch_size:
                    execute_values(cur, insert_query, batch)
                    batch = []
                    conn.commit()
            
            if batch:
                execute_values(cur, insert_query, batch)
                conn.commit()
                
        print(f"  Finished: {count} rows inserted.")

def main():
    print(f"Starting Bolt Ingestion from: {DATA_PATH}")
    
    if not os.path.exists(DATA_PATH):
        print(f"Error: Path {DATA_PATH} does not exist.")
        sys.exit(1)
        
    conn = get_db_connection()
    ensure_schema(conn)
    
    # Walk through the directory to find CSVs
    found_files = 0
    for root, dirs, files in os.walk(DATA_PATH):
        for file in files:
            if file.endswith(".csv"):
                # Check if we should ingest this file
                # Strategy: If it's in our specific mapping, OR if we want to ingest ALL csvs dynamically
                # Let's ingest ALL CSVs found, using filename as table name if not mapped.
                
                table_name = SPECIFIC_TABLE_MAPPING.get(file)
                if not table_name:
                    # Generic fallback: "some_file.csv" -> "some_file"
                    table_name = sanitize_column_name(file.replace(".csv", ""))
                
                full_path = os.path.join(root, file)
                ingest_csv(conn, full_path, table_name)
                found_files += 1
                
    if found_files == 0:
        print("No CSV files found in the specified path.")
    else:
        print(f"\nAll done! Processed {found_files} files.")
        
    conn.close()

if __name__ == "__main__":
    main()
