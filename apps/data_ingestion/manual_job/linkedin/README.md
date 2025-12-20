# LinkedIn Data Ingestion

This manual job ingests LinkedIn export data (CSV format) into the local PostgreSQL database.

## Prerequisites

1.  **Export LinkedIn Data**:
    *   Log in to LinkedIn.
    *   Click on your profile picture ("Me") and select **Settings & Privacy**.
    *   Go to **Data privacy** on the left menu.
    *   Under **How LinkedIn uses your data**, select **Get a copy of your data**.
    *   Select **Download larger data archive** (recommended for all info) or specify data files.
    *   Wait for the email from LinkedIn and download the `.zip` file.
    *   Extract the contents into a folder.

2.  **Prepare Data Directory**:
    *   Create the folder: `~/Documents/jimwurst_local_data/linkedin`
    *   Copy the extracted `.csv` files into this folder.

## How it works

The script `ingest.py` performs the following:
1.  Connects to the local PostgreSQL database (`jimwurst_db`).
2.  Ensures the schema `s_linkedin` exists.
3.  Scans `~/Documents/jimwurst_local_data/linkedin` (or `LINKEDIN_DATA_PATH` env var) for any `.csv` files.
4.  **Lists the files found and estimates processing time.**
5.  **Asks for your confirmation** before proceeding.
6.  For each CSV file, it creates a table in `s_linkedin` (e.g., `Connections.csv` -> `s_linkedin.connections`) and ingests the data. All columns are ingested as `TEXT` to preserve raw data.

## Usage

**Option 1: Using Make (Recommended)**

```bash
# Setup environment (installs dependencies)
make setup

# Run ingestion
make ingest-linkedin
```

**Option 2: Manual Execution**

```bash
# Install dependencies
.venv/bin/pip install -r apps/data_ingestion/manual_job/linkedin/requirements.txt

# Run the ingestion
.venv/bin/python apps/data_ingestion/manual_job/linkedin/ingest.py
```
