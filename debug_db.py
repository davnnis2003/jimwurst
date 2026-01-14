from langchain_community.utilities import SQLDatabase
from utils.ingestion_utils import load_env
import os

load_env()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("POSTGRES_DB", "jimwurst")
DB_USER = os.getenv("POSTGRES_USER", "jimwurst")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "jimwurst")
DB_PORT = os.getenv("DB_PORT", "5432")

db_uri = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
print(f"Connecting to: {db_uri}")

try:
    db = SQLDatabase.from_uri(db_uri)
    print("Connected!")
    print("\n--- Inspecting Schemas ---")
    res_schemas = db.run("SELECT schema_name FROM information_schema.schemata;")
    print(res_schemas)
    
    print("\n--- Inspecting All Tables per Schema ---")
    query_all_tables = """
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY table_schema, table_name;
    """
    res_tables = db.run(query_all_tables)
    print(res_tables)
    
except Exception as e:
    print(f"Error: {e}")
