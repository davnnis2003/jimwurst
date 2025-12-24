import os
import sys
import csv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from tqdm import tqdm

# Import common utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../utils'))
from ingestion_utils import load_env, get_db_connection, ensure_schema, clean_header

# Load env vars
load_env()

DEFAULT_DATA_PATH = os.path.expanduser("~/Documents/jimwurst_local_data/substack")
DATA_PATH = os.getenv("SUBSTACK_DATA_PATH", DEFAULT_DATA_PATH)

SCHEMA_NAME = "s_substack"

def ingest_csv(conn, file_path, table_name, source_folder, append=False):
    """Ingest a CSV file into a table, including a _source_folder column."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            
            if not headers:
                return

            # Sanitize column names and add metadata columns
            columns = [clean_header(h) for h in headers]
            # Ensure unique columns (sometimes Substack might have duplicate headers or we add metadata)
            final_columns = columns + ["_source_folder"]
            
            # Create table if not exists or if we are not appending
            if not append:
                # Drop and recreate for idempotency in this manual job context
                drop_query = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(table_name)
                )
                with conn.cursor() as cur:
                    cur.execute(drop_query)
                
                cols_def = ", ".join([f"{sql.Identifier(c).as_string(conn)} TEXT" for c in final_columns])
                create_query = sql.SQL("CREATE TABLE {}.{} ({})").format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(table_name),
                    sql.SQL(cols_def)
                )
                with conn.cursor() as cur:
                    cur.execute(create_query)
            
            # Insert data
            insert_query = sql.SQL("INSERT INTO {}.{} VALUES %s").format(
                sql.Identifier(SCHEMA_NAME),
                sql.Identifier(table_name)
            )
            
            rows = []
            batch_size = 1000
            
            for row in reader:
                # Pad/truncate row to match original header length
                if len(row) < len(columns):
                    row += [None] * (len(columns) - len(row))
                elif len(row) > len(columns):
                    row = row[:len(columns)]
                
                # Append metadata
                row.append(source_folder)
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
        sys.exit(1)

    # Substack folders are usually the subdirs of DATA_PATH
    folders = [d for d in os.listdir(DATA_PATH) if os.path.isdir(os.path.join(DATA_PATH, d))]
    
    if not folders:
        print(f"No folders found in {DATA_PATH}")
        sys.exit(0)

    print(f"Found folders: {', '.join(folders)}")

    # We'll maintain a set of tables we've already "Created" (dropped/created) to handle appending
    created_tables = set()

    conn = get_db_connection()
    ensure_schema(conn, SCHEMA_NAME)

    for folder in folders:
        folder_path = os.path.join(DATA_PATH, folder)
        print(f"\nProcessing folder: {folder}")
        
        # 1. Main CSVs
        mappings = {
            "posts.csv": "posts",
            "email_list": "emails" # Matches email_list.*.csv
        }
        
        for f in os.listdir(folder_path):
            file_path = os.path.join(folder_path, f)
            if not f.lower().endswith('.csv'):
                continue
            
            target_table = None
            if f == "posts.csv":
                target_table = "posts"
            elif f.startswith("email_list"):
                target_table = "emails"
            
            if target_table:
                append = target_table in created_tables
                ingest_csv(conn, file_path, target_table, folder, append=append)
                created_tables.add(target_table)

        # 2. Nested Posts CSVs
        posts_dir = os.path.join(folder_path, "posts")
        if os.path.exists(posts_dir):
            for f in os.listdir(posts_dir):
                if not f.lower().endswith('.csv'):
                    continue
                
                file_path = os.path.join(posts_dir, f)
                target_table = None
                if ".delivers.csv" in f:
                    target_table = "post_delivers"
                elif ".opens.csv" in f:
                    target_table = "post_opens"
                
                if target_table:
                    append = target_table in created_tables
                    ingest_csv(conn, file_path, target_table, folder, append=append)
                    created_tables.add(target_table)

    conn.close()
    print("\nIngestion complete.")

if __name__ == "__main__":
    main()
