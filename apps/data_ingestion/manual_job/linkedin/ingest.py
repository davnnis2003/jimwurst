import os
import sys
import csv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from tqdm import tqdm
from openpyxl import load_workbook

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

DEFAULT_DATA_PATH = os.path.expanduser("~/Documents/jimwurst_local_data/linkedin")
DATA_PATH = os.getenv("LINKEDIN_DATA_PATH", DEFAULT_DATA_PATH)
BASIC_PATH = os.path.join(DATA_PATH, "basic")
COMPLETE_PATH = os.path.join(DATA_PATH, "complete")

SCHEMA_NAME = "s_linkedin"

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

def sanitize_table_name(filename):
    """Sanitize filename to create valid table name"""
    return os.path.splitext(filename)[0].lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')


class ExcelIngestor:
    """Handles ingestion of .xlsx files from basic creator insights exports"""
    
    def __init__(self, conn):
        self.conn = conn
    
    def ingest(self, file_path, table_name):
        """Ingest Excel file into PostgreSQL"""
        print(f"Processing Excel file: {table_name}...")
        
        try:
            workbook = load_workbook(file_path, read_only=True, data_only=True)
            
            # Process each sheet in the workbook
            for sheet_name in workbook.sheetnames:
                sheet = workbook[sheet_name]
                
                # Create table name with sheet suffix if multiple sheets
                if len(workbook.sheetnames) > 1:
                    current_table_name = f"{table_name}_{sanitize_table_name(sheet_name)}"
                else:
                    current_table_name = table_name
                
                # Get headers from first row
                rows = list(sheet.iter_rows(values_only=True))
                if not rows:
                    print(f"Skipping empty sheet: {sheet_name}")
                    continue
                
                headers = rows[0]
                if not headers or all(h is None for h in headers):
                    print(f"Skipping sheet with no headers: {sheet_name}")
                    continue
                
                # Clean headers and filter out None values
                columns = [clean_header(str(h)) if h is not None else f"column_{i}" 
                          for i, h in enumerate(headers)]
                
                # Create table
                cols_def = ", ".join([f"{sql.Identifier(c).as_string(self.conn)} TEXT" for c in columns])
                
                create_query = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(current_table_name),
                    sql.SQL(cols_def)
                )
                
                # Drop and recreate for idempotency
                drop_query = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(current_table_name)
                )
                
                with self.conn.cursor() as cur:
                    cur.execute(drop_query)
                    cur.execute(create_query)
                
                # Insert data (skip header row)
                insert_query = sql.SQL("INSERT INTO {}.{} VALUES %s").format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(current_table_name)
                )
                
                data_rows = []
                batch_size = 1000
                
                for row in rows[1:]:  # Skip header
                    # Convert all values to strings, handle None
                    processed_row = [str(val) if val is not None else None for val in row]
                    
                    # Pad or truncate to match column count
                    if len(processed_row) < len(columns):
                        processed_row += [None] * (len(columns) - len(processed_row))
                    elif len(processed_row) > len(columns):
                        processed_row = processed_row[:len(columns)]
                    
                    data_rows.append(processed_row)
                    
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
                
                print(f"  ✓ Ingested sheet '{sheet_name}' into table '{current_table_name}'")
            
            workbook.close()
            
        except Exception as e:
            print(f"Error processing Excel file {file_path}: {e}")


class CSVIngestor:
    """Handles ingestion of .csv files from full data archive exports"""
    
    def __init__(self, conn):
        self.conn = conn
    
    def ingest(self, file_path, table_name):
        """Ingest CSV file into PostgreSQL"""
        print(f"Processing CSV file: {table_name}...")
        
        try:
            # Detect encoding - LinkedIn sometimes uses UTF-8 or UTF-16
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                
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
                
                for row in reader:
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
    """Scan both basic and complete folders for files"""
    files_to_process = []
    
    # Scan basic folder for .xlsx files
    if os.path.exists(BASIC_PATH):
        for root, dirs, files in os.walk(BASIC_PATH):
            for f in files:
                if f.lower().endswith('.xlsx'):
                    files_to_process.append({
                        'path': os.path.join(root, f),
                        'type': 'excel',
                        'source': 'basic'
                    })
    
    # Scan complete folder for .csv files
    if os.path.exists(COMPLETE_PATH):
        for root, dirs, files in os.walk(COMPLETE_PATH):
            for f in files:
                if f.lower().endswith('.csv'):
                    files_to_process.append({
                        'path': os.path.join(root, f),
                        'type': 'csv',
                        'source': 'complete'
                    })
    
    return files_to_process


def main():
    print(f"LinkedIn Data Ingestion")
    print(f"=" * 50)
    print(f"Base Path: {DATA_PATH}")
    print(f"Basic Exports Path: {BASIC_PATH}")
    print(f"Complete Exports Path: {COMPLETE_PATH}")
    print()
    
    # Create directories if they don't exist
    os.makedirs(BASIC_PATH, exist_ok=True)
    os.makedirs(COMPLETE_PATH, exist_ok=True)
    
    # Scan for files
    files_to_process = scan_for_files()
    
    if not files_to_process:
        print(f"No files found in either:")
        print(f"  - {BASIC_PATH} (.xlsx files)")
        print(f"  - {COMPLETE_PATH} (.csv files)")
        print()
        print("Please export your LinkedIn data and place files in the appropriate folder:")
        print("  • Basic creator insights → basic/")
        print("  • Full data archive → complete/")
        sys.exit(0)

    # Calculate total size and estimate time
    total_size_bytes = sum(os.path.getsize(f['path']) for f in files_to_process)
    total_size_mb = total_size_bytes / (1024 * 1024)
    estimated_seconds = total_size_mb / 5.0
    
    # Group files by type
    excel_files = [f for f in files_to_process if f['type'] == 'excel']
    csv_files = [f for f in files_to_process if f['type'] == 'csv']
    
    print("Found the following LinkedIn files to ingest:")
    print()
    
    if excel_files:
        print(f"📊 Basic Creator Insights (.xlsx) - {len(excel_files)} file(s):")
        for f in excel_files:
            rel_path = os.path.relpath(f['path'], BASIC_PATH)
            print(f"   - {rel_path}")
        print()
    
    if csv_files:
        print(f"📁 Full Data Archive (.csv) - {len(csv_files)} file(s):")
        for f in csv_files:
            rel_path = os.path.relpath(f['path'], COMPLETE_PATH)
            print(f"   - {rel_path}")
        print()
    
    print(f"Total Size: {total_size_mb:.2f} MB")
    print(f"Estimated Processing Time: ~{estimated_seconds:.1f} seconds")
    
    # User Confirmation
    try:
        response = input("\nDo you want to proceed with LinkedIn data ingestion? (y/N): ").strip().lower()
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
    
    excel_ingestor = ExcelIngestor(conn)
    csv_ingestor = CSVIngestor(conn)
    
    for file_info in tqdm(files_to_process, desc="Ingesting Files"):
        file_path = file_info['path']
        filename = os.path.basename(file_path)
        table_name = f"{file_info['source']}_{sanitize_table_name(filename)}"
        
        if file_info['type'] == 'excel':
            excel_ingestor.ingest(file_path, table_name)
        elif file_info['type'] == 'csv':
            csv_ingestor.ingest(file_path, table_name)
        
    conn.close()
    print("\n" + "=" * 50)
    print("✅ Ingestion complete!")

if __name__ == "__main__":
    main()
