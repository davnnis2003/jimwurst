# Apple Health Ingestion

**Script**: `ingest.py`

This script parses the Apple Health `export.xml` file, detects schema dynamically from the records, and ingests the data into a dedicated PostgreSQL schema `s_apple_health`.

## Prerequisites

1.  **Python 3.9+**
2.  **Dependencies**: Install the required packages.
    ```bash
    pip install -r requirements.txt
    ```
3.  **Data**: Place your `export.xml` in your customized `LOCAL_DATA_PATH/apple_health/`.
    *   Example: `~/Documents/jimwurst_local_data/apple_health/export.xml`

## Usage

Run the script from the root of the repository (or anywhere, provided you set the env vars):

```bash
```bash
# Set Env Vars (if not defaults)
export DB_HOST=localhost
export POSTGRES_USER=jimwurst
export POSTGRES_PASSWORD=jimwurst
export POSTGRES_DB=jimwurst
export EXPORT_XML_PATH=~/document/jimwurst_local_data/apple_health/export.xml

# Run
python3 apps/data_ingestion/manual_job/apple_health/ingest.py
```

## Features

*   **Streaming Parse**: Uses `iterparse` to handle large XML files without loading everything into RAM.
*   **Dynamic Schema**: Scans records to determine columns.
*   **Batch Insert**: Uses strictly typed arguments for performance.
*   **Idempotency**: Truncates target tables before load (Full Refresh).
