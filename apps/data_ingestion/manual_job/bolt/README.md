# Bolt Data Ingestion

**Script**: `ingest.py`

This script ingests manually exported Bolt data (CSVs) into the `s_bolt` schema in PostgreSQL.

## Prerequisites

1.  **Python 3.9+**
2.  **Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Data**: Place your Bolt GDPR export folder in your local data path.
    *   Default assumed path: `~/Documents/jimwurst_local_data/bolt`
    *   You can override this with `BOLT_DATA_PATH`.

## Usage

```bash
# Optional overrides
export DB_HOST=localhost
export POSTGRES_USER=jimwurst_user
export POSTGRES_PASSWORD=jimwurst_password
export POSTGRES_DB=jimwurst_db
export BOLT_DATA_PATH=~/Documents/jimwurst_local_data/bolt

# Run
python3 apps/data_ingestion/manual_job/bolt/ingest.py
```

## Structure

The script expects the standard Bolt export folder structure (e.g., `Order history/rides.csv`, `Billing Details/transactions.csv`). It will recursively search for known CSV filenames and ingest them into corresponding tables in `s_bolt`.
