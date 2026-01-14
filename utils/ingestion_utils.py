"""
Common utilities for data ingestion scripts.
"""

import os
import re
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql


def load_env():
    """Load environment variables from docker/.env files."""
    ENV_DIR = os.path.join(os.path.dirname(__file__), '../docker')
    ENV_EXAMPLE = os.path.join(ENV_DIR, '.env.example')
    ENV_REAL = os.path.join(ENV_DIR, '.env')

    if os.path.exists(ENV_EXAMPLE):
        load_dotenv(ENV_EXAMPLE)
    if os.path.exists(ENV_REAL):
        load_dotenv(ENV_REAL, override=True)


def get_db_connection():
    """Get a PostgreSQL database connection."""
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_NAME = os.getenv("POSTGRES_DB", "jimwurst_db")
    DB_USER = os.getenv("POSTGRES_USER", "jimwurst_user")
    DB_PASS = os.getenv("POSTGRES_PASSWORD", "jimwurst_password")
    DB_PORT = os.getenv("DB_PORT", "5432")

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
        raise


def ensure_schema(conn, schema_name):
    """Ensure the specified schema exists."""
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
    conn.commit()
    print(f"Schema '{schema_name}' checked/created.")


def clean_header(header):
    """Clean column names for database use."""
    return header.strip().lower().replace(' ', '_').replace('-', '_').replace('.', '_').replace('(', '').replace(')', '')


def sanitize_table_name(filename):
    """Sanitize filename to create valid table name."""
    return os.path.splitext(filename)[0].lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')