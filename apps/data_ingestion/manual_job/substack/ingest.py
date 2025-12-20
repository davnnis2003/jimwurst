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

DEFAULT_DATA_PATH = os.path.expanduser("~/Documents/jimwurst_local_data/substack")
DATA_PATH = os.getenv("SUBSTACK_DATA_PATH", DEFAULT_DATA_PATH)

SCHEMA_NAME = "s_substack"

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

def clean_header(header):
    # Basic cleaning for column names
    return header.strip().lower().replace(' ', '_').replace('-', '_').replace('.', '_')

def ingest_csv(conn, file_path, table_name):
    print(f"Processing {table_name}...")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            
            if not headers:
                print(f"Skipping empty file: {file_path}")
                return

            # Sanitize column names
            columns = [clean_header(h) for h in headers]
            
            # Create table
            # We use TEXT for everything in the raw layer
            cols_def = ", ".join([f"{sql.Identifier(c).as_string(conn)} TEXT" for c in columns])
            
            create_query = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                sql.Identifier(SCHEMA_NAME),
                sql.Identifier(table_name),
                sql.SQL(cols_def)
            )
            
            # Drop and recreate for idempotency in this manual job context
            drop_query = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                sql.Identifier(SCHEMA_NAME),
                sql.Identifier(table_name)
            )
            
            with conn.cursor() as cur:
                cur.execute(drop_query)
                cur.execute(create_query)
            
            # Insert data
            insert_query = sql.SQL("INSERT INTO {}.{} VALUES %s").format(
                sql.Identifier(SCHEMA_NAME),
                sql.Identifier(table_name)
            )
            
            rows = []
            batch_size = 1000
            
            # Re-read to reset or just continue if next() worked. 
            # Note: next() moved the pointer, so we are good to iterate `reader`
            
            for row in reader:
                # Ensure row has same length as headers (pad with None if missing, truncate if too long - though csv reader usually handles this well, but let's be safe)
                if len(row) < len(columns):
                    row += [None] * (len(columns) - len(row))
                elif len(row) > len(columns):
                    row = row[:len(columns)]
                
                rows.append(row)
                
                if len(rows) >= batch_size:
                    with conn.cursor() as cur:
                        execute_values(cur, insert_query, rows)
                    conn.commit()
                    rows = []
            
            if rows:
                with conn.cursor() as cur:
                    execute_values(cur, insert_query, rows)
                conn.commit()
                
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def main():
    print(f"Target Data Path: {DATA_PATH}")
    
    if not os.path.exists(DATA_PATH):
        print(f"Error: Path {DATA_PATH} does not exist.")
        print("Please create it and put your Substack CSV exports there.")
        sys.exit(1)

    # 1. Scan for CSV files recursively
    csv_files = []
    for root, dirs, files in os.walk(DATA_PATH):
        for f in files:
            if f.lower().endswith('.csv'):
                csv_files.append(os.path.join(root, f))
    
    if not csv_files:
        print(f"No .csv files found in {DATA_PATH} or its subdirectories.")
        sys.exit(0)

    # 2. Estimate time and list files
    total_size_bytes = sum(os.path.getsize(f) for f in csv_files)
    total_size_mb = total_size_bytes / (1024 * 1024)
    # Heuristic: ~5 MB/s processing speed
    estimated_seconds = total_size_mb / 5.0 
    
    print("\nFound the following files to ingest:")
    for f in csv_files:
        rel_path = os.path.relpath(f, DATA_PATH)
        print(f" - {rel_path}")
    
    print(f"\nTotal Size: {total_size_mb:.2f} MB")
    print(f"Estimated Processing Time: ~{estimated_seconds:.1f} seconds")
    
    # 3. User Confirmation
    try:
        response = input("\nDo you want to proceed with ingestion? (y/N): ").strip().lower()
    except KeyboardInterrupt:
        print("\nOperation cancelled.")
        sys.exit(0)
        
    if response != 'y':
        print("Operation cancelled.")
        sys.exit(0)

    print("\nStarting ingestion...")
    
    conn = get_db_connection()
    ensure_schema(conn)
    
    for file_path in tqdm(csv_files, desc="Files"):
        filename = os.path.basename(file_path)
        # For nested files, we might want to include the folder name in the table name to avoid collisions
        # but let's stick to the filename for now as requested "lets do the same for Substack" 
        # which usually implies simple table names. If there are collisions, the last one wins.
        table_name = os.path.splitext(filename)[0].lower().replace(' ', '_').replace('-', '_')
        ingest_csv(conn, file_path, table_name)
        
    conn.close()
    print("\nIngestion complete.")

if __name__ == "__main__":
    main()
