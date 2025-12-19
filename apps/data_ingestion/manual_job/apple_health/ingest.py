import os
import sys
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2 import sql
from datetime import datetime
from tqdm import tqdm
from psycopg2.extras import execute_values

from dotenv import load_dotenv

# --- Configuration ---
# Define paths to env files
ENV_DIR = os.path.join(os.path.dirname(__file__), '../../../../docker')
ENV_EXAMPLE = os.path.join(ENV_DIR, '.env.example')
ENV_REAL = os.path.join(ENV_DIR, '.env')

# 1. Load defaults from .env.example
if os.path.exists(ENV_EXAMPLE):
    load_dotenv(ENV_EXAMPLE)

# 2. Override with actual values from .env (if it exists)
if os.path.exists(ENV_REAL):
    load_dotenv(ENV_REAL, override=True)

# Allow overriding via Environment Variables (matching docker-compose)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("POSTGRES_DB", "jimwurst_db")
DB_USER = os.getenv("POSTGRES_USER", "jimwurst_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "jimwurst_password")
DB_PORT = os.getenv("DB_PORT", "5432")

# Default path points to the external volume location we defined
DEFAULT_XML_PATH = os.path.expanduser("~/Documents/jimwurst_local_data/apple_health/export.xml")
XML_PATH = os.getenv("EXPORT_XML_PATH", DEFAULT_XML_PATH)

SCHEMA_NAME = "s_apple_health"

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
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
    """Creates the schema if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA_NAME)))
    conn.commit()
    print(f"Schema '{SCHEMA_NAME}' checked/created.")

def get_record_count(xml_file):
    """Quickly counts the number of Record tags in the XML."""
    print("Pre-scanning file to estimate records...")
    count = 0
    context = ET.iterparse(xml_file, events=("end",))
    for event, elem in context:
        if elem.tag == "Record":
            count += 1
        elem.clear()
    return count

def estimate_and_confirm(count):
    """Estimates time and asks user for confirmation."""
    records_per_second = 50000 
    est_seconds = count / records_per_second
    
    print(f"\n--- Ingestion Estimate ---")
    print(f"Total Records: {count:,}")
    print(f"Estimated Time: {est_seconds:.1f} seconds (~{records_per_second:,} rec/s)")
    print(f"--------------------------\n")
    
    try:
        choice = input("Proceed with ingestion? [Y/n]: ").strip().lower()
        if choice not in ('', 'y', 'yes'):
            print("Aborted by user.")
            sys.exit(0)
    except EOFError:
        # If running in a non-interactive shell, we proceed
        print("Non-interactive session detected, proceeding...")

def parse_and_ingest(xml_file):
    """
    Parses the export.xml file.
    """
    
    # Handle case sensitivity if using the default folder
    if not os.path.exists(xml_file):
        alt_path = xml_file.replace("export.xml", "Export.xml")
        if os.path.exists(alt_path):
            xml_file = alt_path
        else:
            print(f"File not found: {xml_file}")
            print(f"Also checked: {alt_path}")
            sys.exit(1)

    print(f"Using file: {xml_file}")
    
    # --- New: Estimation Feature ---
    total_records = get_record_count(xml_file)
    estimate_and_confirm(total_records)
    # ------------------------------

    conn = get_db_connection()
    ensure_schema(conn)

    # DDL for a generic records table
    # We drop and recreate for a full refresh pattern
    create_table_query = sql.SQL("""
        DROP TABLE IF EXISTS {schema}.records;
        CREATE TABLE {schema}.records (
            type VARCHAR(255),
            source_name VARCHAR(255),
            source_version VARCHAR(255),
            unit VARCHAR(50),
            creation_date TIMESTAMP,
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            value TEXT,
            device TEXT,
            metadata JSONB
        );
    """).format(schema=sql.Identifier(SCHEMA_NAME))

    with conn.cursor() as cur:
        print("Recreating table records...")
        cur.execute(create_table_query)
    conn.commit()

    # Streaming Parse
    context = ET.iterparse(xml_file, events=("start", "end"))
    context = iter(context)
    event, root = next(context)

    batch_size = 5000
    batch = []
    
    record_count = 0
    
    print("Starting ingestion...")
    with conn.cursor() as cur:
        for event, elem in tqdm(context):
            if event == "end" and elem.tag == "Record":
                # Extract standard attributes
                attrib = elem.attrib
                
                # Metadata is anything not in our standard list
                # This is a simplification; Apple Health has many attributes. 
                # We map the most common common ones to columns.
                
                # Helper to handle dates
                def parse_date(d_str):
                    try:
                        # Apple uses '2023-10-25 07:12:05 +0200'
                        return datetime.strptime(d_str, '%Y-%m-%d %H:%M:%S %z')
                    except:
                        return None

                creation_date = parse_date(attrib.get('creationDate'))
                start_date = parse_date(attrib.get('startDate'))
                end_date = parse_date(attrib.get('endDate'))
                
                row = (
                    attrib.get('type'),
                    attrib.get('sourceName'),
                    attrib.get('sourceVersion'),
                    attrib.get('unit'),
                    creation_date,
                    start_date,
                    end_date,
                    attrib.get('value'),
                    attrib.get('device'),
                    # For now we won't put everything else in JSONB to keep it simple, 
                    # but normally we would grab remaining attributes
                    "{}" 
                )
                
                batch.append(row)
                record_count += 1
                
                elem.clear() # Free memory
                
                if len(batch) >= batch_size:
                    execute_values(cur, 
                        sql.SQL("INSERT INTO {schema}.records VALUES %s").format(schema=sql.Identifier(SCHEMA_NAME)),
                        batch
                    )
                    conn.commit()
                    batch = []

        # Final batch
        if batch:
            execute_values(cur, 
                sql.SQL("INSERT INTO {schema}.records VALUES %s").format(schema=sql.Identifier(SCHEMA_NAME)),
                batch
            )
            conn.commit()

    print(f"Ingestion complete. {record_count} records inserted.")
    conn.close()

if __name__ == "__main__":
    print(f"Processing Apple Health export from: {XML_PATH}")
    parse_and_ingest(XML_PATH)
