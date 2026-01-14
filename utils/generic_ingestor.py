
import os
import pandas as pd
from sqlalchemy import create_engine
from utils.ingestion_utils import get_db_connection, ensure_schema, sanitize_table_name, clean_header

def ingest_file(file_path: str, schema: str = 'staging') -> str:
    """
    Ingests a CSV file into the PostgreSQL database.
    Infers schema from the CSV file.
    """
    if not os.path.exists(file_path):
        return f"Error: File {file_path} not found."

    try:
        # Load environment variables (usually done at app startup, but safe here)
        from utils.ingestion_utils import load_env
        load_env()
        
        # Get DB connection string for SQLAlchemy
        DB_HOST = os.getenv("DB_HOST", "localhost")
        DB_NAME = os.getenv("POSTGRES_DB", "jimwurst_db")
        DB_USER = os.getenv("POSTGRES_USER", "jimwurst_user")
        DB_PASS = os.getenv("POSTGRES_PASSWORD", "jimwurst_password")
        DB_PORT = os.getenv("DB_PORT", "5432")
        
        db_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_string)

        # Read CSV
        df = pd.read_csv(file_path)
        
        # Sanitize columns
        df.columns = [clean_header(c) for c in df.columns]
        
        # Sanitize table name
        filename = os.path.basename(file_path)
        table_name = sanitize_table_name(filename)
        
        # Ensure schema exists using raw connection
        with get_db_connection() as conn:
             ensure_schema(conn, schema)
        
        # Write to DB
        df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
        
        return f"Successfully ingested {filename} into {schema}.{table_name} with {len(df)} rows."

    except Exception as e:
        return f"Error during ingestion: {str(e)}"
