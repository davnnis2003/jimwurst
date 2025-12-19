import os
import sys
import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values, Json
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime

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

DEFAULT_DATA_PATH = os.path.expanduser("~/Documents/jimwurst_local_data/telegram")
DATA_PATH = os.getenv("TELEGRAM_DATA_PATH", DEFAULT_DATA_PATH)

SCHEMA_NAME = "s_telegram"

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

def recreate_table(conn, table_name, schema_sql):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
            sql.Identifier(SCHEMA_NAME), sql.Identifier(table_name)
        ))
        cur.execute(sql.SQL("CREATE TABLE {}.{} {}").format(
            sql.Identifier(SCHEMA_NAME), sql.Identifier(table_name), sql.SQL(schema_sql)
        ))
    conn.commit()
    print(f"Table {SCHEMA_NAME}.{table_name} recreated.")

def ingest_telegram_data(conn, file_path):
    print(f"Reading JSON from {file_path}...")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Failed to read file: {e}")
        return

    # 1. Personal Information
    # Usually in data['personal_information'] or top level fields
    # We'll just create a simple KV table or strict table if we know fields.
    # Let's dump the root keys (excluding 'chats', 'contacts') into an 'about' table
    
    # 2. Contacts
    contacts = data.get('contacts', {}).get('list', [])
    if not contacts and isinstance(data.get('contacts'), list):
        # newer exports might just have 'contacts': [...]
        contacts = data.get('contacts')
    
    if contacts:
        recreate_table(conn, "contacts", "(first_name TEXT, last_name TEXT, phone_number TEXT, date_unixtime TEXT)")
        
        insert_query = sql.SQL("INSERT INTO {}.contacts (first_name, last_name, phone_number, date_unixtime) VALUES %s").format(
            sql.Identifier(SCHEMA_NAME)
        )
        
        rows = []
        for c in contacts:
            rows.append((
                c.get('first_name'),
                c.get('last_name'),
                c.get('phone_number'),
                c.get('date_unixtime') # Keep as text/unixtime for raw layer, cast downstream
            ))
            
        if rows:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, rows)
            conn.commit()
            print(f"Inserted {len(rows)} contacts.")

    # 3. Chats and Messages
    # typical structure: data['chats']['list'] -> list of chat objects
    chats_list = data.get('chats', {}).get('list', [])
    
    if chats_list:
        # Create chats table
        recreate_table(conn, "chats", """(
            id BIGINT PRIMARY KEY,
            name TEXT,
            type TEXT,
            raw_data JSONB
        )""")
        
        # Create messages table
        recreate_table(conn, "messages", """(
            id BIGINT, 
            chat_id BIGINT,
            date TEXT,
            date_unixtime TEXT,
            sender TEXT,
            sender_id TEXT,
            text TEXT,
            type TEXT,
            reply_to_message_id BIGINT,
            raw_data JSONB
        )""")

        chat_query = sql.SQL("INSERT INTO {}.chats (id, name, type, raw_data) VALUES %s").format(sql.Identifier(SCHEMA_NAME))
        msg_query = sql.SQL("INSERT INTO {}.messages (id, chat_id, date, date_unixtime, sender, sender_id, text, type, reply_to_message_id, raw_data) VALUES %s").format(sql.Identifier(SCHEMA_NAME))
        
        chat_rows = []
        total_messages = 0
        
        print(f"Processing {len(chats_list)} chats...")
        
        with conn.cursor() as cur:
            for chat in tqdm(chats_list, unit="chat"):
                chat_id = chat.get('id')
                # Some exports allow duplicate Chat IDs (? maybe not, but safety first)
                if not chat_id:
                    continue
                    
                chat_rows.append((
                    chat_id,
                    chat.get('name'),
                    chat.get('type'),
                    Json(chat) # Dump full chat object (minus messages usually? No, messages are inside. We might want to pop messages to save space in chats table)
                ))
                
                # Check messages
                messages = chat.get('messages', [])
                if messages:
                    msg_batch = []
                    for m in messages:
                        # 'text' field can be a list of entities (strings + dicts) in Telegram JSON
                        # We should stringify it for the text column
                        text_content = m.get('text', '')
                        if isinstance(text_content, list):
                            # Join parts: strings are kept, dicts (links/entries) usually have a 'text' property or just represent formatting
                            # Simple approach: json dumps or just extract string parts.
                            # Let's just dumps for now to preserve info, or join strings. 
                            # If we join strings:
                            text_content = "".join([x if isinstance(x, str) else x.get('text', '') for x in text_content])
                        
                        msg_batch.append((
                            m.get('id'),
                            chat_id,
                            m.get('date'),
                            m.get('date_unixtime'),
                            m.get('from'),
                            m.get('from_id'),
                            text_content,
                            m.get('type'),
                            m.get('reply_to_message_id'),
                            Json(m)
                        ))
                    
                    if msg_batch:
                        execute_values(cur, msg_query, msg_batch)
                        total_messages += len(msg_batch)

            # Insert chats
            if chat_rows:
                # remove messages from the raw_data in chat_rows to avoid duplication? 
                # For now let's keep it simple, though distinct is better.
                # Actually, pop messages before appending to chat_rows would be better but I already appended.
                # Let's just insert.
                 execute_values(cur, chat_query, chat_rows)
            
            conn.commit()
            
        print(f"Inserted {len(chat_rows)} chats and {total_messages} messages.")

def find_result_json(search_path):
    """
    Search for result.json in the given path (specifically looking for the file)
    """
    # 1. Check if the path itself is the file
    if os.path.basename(search_path) == "result.json" and os.path.isfile(search_path):
        return search_path

    # 2. Check if it's in the immediate root
    potential = os.path.join(search_path, "result.json")
    if os.path.exists(potential):
        return potential

    # 3. Recursive search
    for root, dirs, files in os.walk(search_path):
        if "result.json" in files:
            return os.path.join(root, "result.json")
    
    return None

def main():
    print(f"Starting Telegram Ingestion from: {DATA_PATH}")
    
    if not os.path.exists(DATA_PATH):
        print(f"Error: Path {DATA_PATH} does not exist.")
        sys.exit(1)
        
    file_path = find_result_json(DATA_PATH)
    
    if not file_path:
        print(f"Error: Could not find 'result.json' in {DATA_PATH} or its subdirectories.")
        print("Please export your Telegram data (JSON) and place it in that folder.")
        sys.exit(1)
    
    print(f"Found data file: {file_path}")
    
    # Estimate time
    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)
    # Heuristic: ~7 MB/s based on previous run (154MB in 20s)
    estimated_seconds = file_size_mb / 7.0 
    
    print(f"File size: {file_size_mb:.2f} MB")
    print(f"Estimated processing time: ~{estimated_seconds:.0f} seconds (depending on machine speed)")
        
    conn = get_db_connection()
    ensure_schema(conn)
    
    ingest_telegram_data(conn, file_path)
                
    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
