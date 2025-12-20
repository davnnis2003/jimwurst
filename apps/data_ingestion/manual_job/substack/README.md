# Substack Data Ingestion

This manual job ingests Substack export data (CSV format) into the local PostgreSQL database.

## Prerequisites

1.  **Export Substack Data**:
    *   Go to your Substack settings.
    *   Look for an "Export" option (often under "Stats" or "Subscribers").
    *   Download the `.zip` file and extract it.
    *   It should contain CSV files like `posts.csv`, `subscribers.csv`, etc.

2.  **Prepare Data Directory**:
    *   Create the folder: `~/Documents/jimwurst_local_data/substack`
    *   Copy the extracted `.csv` files into this folder.

## How it works

The script `ingest.py` performs the following:
1.  Connects to the local PostgreSQL database (`jimwurst_db`).
2.  Ensures the schema `s_substack` exists.
3.  Scans `~/Documents/jimwurst_local_data/substack` (or `SUBSTACK_DATA_PATH` env var) for any `.csv` files.
4.  Lists the files found and estimates processing time.
5.  **Asks for your confirmation** before proceeding.
6.  For each CSV file, it creates a table in `s_substack` (e.g., `posts.csv` -> `s_substack.posts`) and ingests the data. All columns are ingested as `TEXT` to preserve raw data fidelity.

## Usage

**Option 1: Using Make (Recommended)**

```bash
# Setup environment (installs dependencies)
make setup

# Run ingestion
make ingest-substack
```

**Option 2: Manual Execution**

```bash
# Install dependencies
.venv/bin/pip install -r apps/data_ingestion/manual_job/substack/requirements.txt

# Run the ingestion
.venv/bin/python apps/data_ingestion/manual_job/substack/ingest.py
```
