# Telegram Data Ingestion

**Script**: `ingest.py`

This script ingests manually exported Telegram data (JSON format) into the `s_telegram` schema in PostgreSQL.

## Prerequisites

1.  **Python 3.9+**
2.  **Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Data**: Export your Telegram data using Telegram Desktop.
    *   Go to Settings > Advanced > Export Telegram Data.
    *   Select "Machine-readable JSON".
    *   Place the `result.json` file in your local data path.
    *   Default assumed path: `~/Documents/jimwurst_local_data/telegram/result.json`

## Usage

```bash
# Optional overrides
export DB_HOST=localhost
export POSTGRES_USER=jimwurst_user
export POSTGRES_PASSWORD=jimwurst_password
export POSTGRES_DB=jimwurst_db
export TELEGRAM_DATA_PATH=~/Documents/jimwurst_local_data/telegram

# Run
python3 apps/data_ingestion/manual_job/telegram/ingest.py
```

## Tables Created

*   `s_telegram.contacts`: Your saved contacts.
*   `s_telegram.chats`: List of all chats (personal, groups, channels).
*   `s_telegram.messages`: All messages from all chats.
