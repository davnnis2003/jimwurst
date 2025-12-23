import os
import sys
import csv
import json
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

DEFAULT_DATA_PATH = os.path.expanduser("~/Documents/jimwurst_local_data/spotify")
DATA_PATH = os.getenv("SPOTIFY_DATA_PATH", DEFAULT_DATA_PATH)

SCHEMA_NAME = "s_spotify"

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
    """Basic cleaning for column names"""
    return header.strip().lower().replace(' ', '_').replace('-', '_').replace('.', '_').replace('(', '').replace(')', '')

def sanitize_table_name(filename):
    """Sanitize filename to create valid table name"""
    return os.path.splitext(filename)[0].lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')


class JSONIngestor:
    """Handles ingestion of .json files from Spotify exports"""
    
    def __init__(self, conn):
        self.conn = conn
    
    def flatten_json(self, data, parent_key='', sep='_'):
        """Flatten nested JSON structure"""
        items = []
        if isinstance(data, dict):
            for k, v in data.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, (dict, list)):
                    items.extend(self.flatten_json(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
        elif isinstance(data, list):
            for i, v in enumerate(data):
                new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
                if isinstance(v, (dict, list)):
                    items.extend(self.flatten_json(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
        return dict(items)
    
    def ingest(self, file_path, table_name):
        """Ingest JSON file into PostgreSQL"""
        print(f"Processing JSON file: {table_name}...")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle different JSON structures
            if isinstance(data, list):
                # List of objects
                records = data
            elif isinstance(data, dict):
                # Single object or nested structure
                # Check if it's a wrapper with a list inside
                if len(data) == 1 and isinstance(list(data.values())[0], list):
                    records = list(data.values())[0]
                else:
                    records = [data]
            else:
                print(f"Unsupported JSON structure in {file_path}")
                return
            
            if not records:
                print(f"No records found in {file_path}")
                return
            
            # Flatten all records and collect all unique keys
            flattened_records = []
            all_keys = set()
            
            for record in records:
                if isinstance(record, dict):
                    flattened = self.flatten_json(record)
                    flattened_records.append(flattened)
                    all_keys.update(flattened.keys())
                else:
                    # Handle primitive values in list
                    flattened_records.append({'value': str(record)})
                    all_keys.add('value')
            
            if not all_keys:
                print(f"No data to ingest from {file_path}")
                return
            
            # Clean column names
            columns = [clean_header(k) for k in sorted(all_keys)]
            
            # Create table
            cols_def = ", ".join([f"{sql.Identifier(c).as_string(self.conn)} TEXT" for c in columns])
            
            create_query = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                sql.Identifier(SCHEMA_NAME),
                sql.Identifier(table_name),
                sql.SQL(cols_def)
            )
            
            # Drop and recreate for idempotency
            drop_query = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                sql.Identifier(SCHEMA_NAME),
                sql.Identifier(table_name)
            )
            
            with self.conn.cursor() as cur:
                cur.execute(drop_query)
                cur.execute(create_query)
            
            # Insert data
            insert_query = sql.SQL("INSERT INTO {}.{} VALUES %s").format(
                sql.Identifier(SCHEMA_NAME),
                sql.Identifier(table_name)
            )
            
            data_rows = []
            batch_size = 1000
            
            # Map cleaned keys back to original for lookup
            key_mapping = {clean_header(k): k for k in all_keys}
            
            for record in flattened_records:
                row = []
                for col in columns:
                    original_key = key_mapping[col]
                    value = record.get(original_key)
                    row.append(str(value) if value is not None else None)
                data_rows.append(row)
                
                if len(data_rows) >= batch_size:
                    with self.conn.cursor() as cur:
                        execute_values(cur, insert_query, data_rows)
                    self.conn.commit()
                    data_rows = []
            
            # Insert remaining rows
            if data_rows:
                with self.conn.cursor() as cur:
                    execute_values(cur, insert_query, data_rows)
                self.conn.commit()
            
            print(f"  ✓ Ingested {len(flattened_records)} records into table '{table_name}'")
            
        except Exception as e:
            print(f"Error processing JSON file {file_path}: {e}")


class CSVIngestor:
    """Handles ingestion of .csv files from Spotify exports"""
    
    def __init__(self, conn):
        self.conn = conn
    
    def ingest(self, file_path, table_name):
        """Ingest CSV file into PostgreSQL"""
        print(f"Processing CSV file: {table_name}...")
        
        try:
            # Try UTF-8 first, fallback to other encodings if needed
            encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
            file_content = None
            used_encoding = None
            
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        file_content = f.read()
                        used_encoding = encoding
                        break
                except UnicodeDecodeError:
                    continue
            
            if file_content is None:
                print(f"Could not decode file {file_path} with any supported encoding")
                return
            
            # Parse CSV
            from io import StringIO
            csv_reader = csv.reader(StringIO(file_content))
            headers = next(csv_reader, None)
            
            if not headers:
                print(f"Skipping empty file: {file_path}")
                return

            # Sanitize column names
            columns = [clean_header(h) for h in headers]
            
            # Create table
            cols_def = ", ".join([f"{sql.Identifier(c).as_string(self.conn)} TEXT" for c in columns])
            
            create_query = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                sql.Identifier(SCHEMA_NAME),
                sql.Identifier(table_name),
                sql.SQL(cols_def)
            )
            
            # Drop and recreate for idempotency
            drop_query = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                sql.Identifier(SCHEMA_NAME),
                sql.Identifier(table_name)
            )
            
            with self.conn.cursor() as cur:
                cur.execute(drop_query)
                cur.execute(create_query)
            
            # Insert data
            insert_query = sql.SQL("INSERT INTO {}.{} VALUES %s").format(
                sql.Identifier(SCHEMA_NAME),
                sql.Identifier(table_name)
            )
            
            rows = []
            batch_size = 1000
            
            for row in csv_reader:
                # Pad or truncate row to match headers
                if len(row) < len(columns):
                    row += [None] * (len(columns) - len(row))
                elif len(row) > len(columns):
                    row = row[:len(columns)]
                
                rows.append(row)
                
                if len(rows) >= batch_size:
                    with self.conn.cursor() as cur:
                        execute_values(cur, insert_query, rows)
                    self.conn.commit()
                    rows = []
            
            if rows:
                with self.conn.cursor() as cur:
                    execute_values(cur, insert_query, rows)
                self.conn.commit()
                
        except Exception as e:
            print(f"Error processing CSV file {file_path}: {e}")


def scan_for_files():
    """Scan data directory for JSON and CSV files"""
    files_to_process = []
    
    if not os.path.exists(DATA_PATH):
        return files_to_process
    
    for root, dirs, files in os.walk(DATA_PATH):
        for f in files:
            file_lower = f.lower()
            if file_lower.endswith('.json'):
                files_to_process.append({
                    'path': os.path.join(root, f),
                    'type': 'json',
                    'name': f
                })
            elif file_lower.endswith('.csv'):
                files_to_process.append({
                    'path': os.path.join(root, f),
                    'type': 'csv',
                    'name': f
                })
    
    return files_to_process


def main():
    print(f"Spotify Data Ingestion")
    print(f"=" * 50)
    print(f"Data Path: {DATA_PATH}")
    print()
    
    # Create directory if it doesn't exist
    os.makedirs(DATA_PATH, exist_ok=True)
    
    # Scan for files
    files_to_process = scan_for_files()
    
    if not files_to_process:
        print(f"No files found in: {DATA_PATH}")
        print()
        print("Please export your Spotify data and place the files in:")
        print(f"  {DATA_PATH}")
        print()
        print("Supported file types: .json, .csv")
        sys.exit(0)

    # Calculate total size and estimate time
    total_size_bytes = sum(os.path.getsize(f['path']) for f in files_to_process)
    total_size_mb = total_size_bytes / (1024 * 1024)
    estimated_seconds = total_size_mb / 5.0
    
    # Group files by type
    json_files = [f for f in files_to_process if f['type'] == 'json']
    csv_files = [f for f in files_to_process if f['type'] == 'csv']
    
    print("Found the following Spotify files to ingest:")
    print()
    
    if json_files:
        print(f"📊 JSON files - {len(json_files)} file(s):")
        for f in json_files:
            rel_path = os.path.relpath(f['path'], DATA_PATH)
            print(f"   - {rel_path}")
        print()
    
    if csv_files:
        print(f"📁 CSV files - {len(csv_files)} file(s):")
        for f in csv_files:
            rel_path = os.path.relpath(f['path'], DATA_PATH)
            print(f"   - {rel_path}")
        print()
    
    print(f"Total Size: {total_size_mb:.2f} MB")
    print(f"Estimated Processing Time: ~{estimated_seconds:.1f} seconds")
    
    # User Confirmation
    try:
        response = input("\nDo you want to proceed with Spotify data ingestion? (y/N): ").strip().lower()
    except KeyboardInterrupt:
        print("\nOperation cancelled.")
        sys.exit(0)
        
    if response != 'y':
        print("Operation cancelled.")
        sys.exit(0)

    print("\nStarting ingestion...")
    print("=" * 50)
    
    conn = get_db_connection()
    ensure_schema(conn)
    
    json_ingestor = JSONIngestor(conn)
    csv_ingestor = CSVIngestor(conn)
    
    for file_info in tqdm(files_to_process, desc="Ingesting Files"):
        file_path = file_info['path']
        filename = os.path.basename(file_path)
        table_name = sanitize_table_name(filename)
        
        if file_info['type'] == 'json':
            json_ingestor.ingest(file_path, table_name)
        elif file_info['type'] == 'csv':
            csv_ingestor.ingest(file_path, table_name)
        
    conn.close()
    print("\n" + "=" * 50)
    print("✅ Ingestion complete!")

if __name__ == "__main__":
    main()
