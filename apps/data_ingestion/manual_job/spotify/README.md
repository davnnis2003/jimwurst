# Spotify Data Ingestion

This manual job ingests Spotify export data into the local PostgreSQL database. The script automatically detects and processes both JSON and CSV files from your Spotify data export.

## Prerequisites

### Exporting Spotify Data

1. Log in to your Spotify account at [spotify.com](https://www.spotify.com)
2. Go to your **Account** page
3. Navigate to **Privacy Settings**
4. Scroll down to **Download your data**
5. Request your data (Spotify will email you when it's ready, usually within 30 days)
6. Download the `.zip` file from the email
7. Extract the contents
8. Place all files (JSON and/or CSV) in: `~/Documents/jimwurst_local_data/spotify/`

> **Note**: The script will automatically create this directory if it doesn't exist.

## What Data Does Spotify Export?

Spotify exports typically include:
- **Streaming history** (JSON/CSV): Your listening history
- **Playlists** (JSON): Your created and followed playlists
- **Library** (JSON): Your saved tracks, albums, and artists
- **User data** (JSON): Profile information and preferences
- **Inferences** (JSON): Spotify's inferred data about you
- **Payments** (JSON): Payment and subscription history (if applicable)

## How it works

The script `ingest.py` performs the following:

1. Connects to the local PostgreSQL database (`jimwurst_db`)
2. Ensures the schema `s_spotify` exists
3. Scans `~/Documents/jimwurst_local_data/spotify/` for `.json` and `.csv` files
4. **Lists all files found and estimates processing time**
5. **Asks for your confirmation** before proceeding
6. Processes each file using the appropriate ingestor:
   - **JSONIngestor**: Handles `.json` files, flattens nested structures
   - **CSVIngestor**: Handles `.csv` files with encoding detection
7. Creates tables in `s_spotify` schema with sanitized filenames as table names
8. All columns are ingested as `TEXT` to preserve raw data

## Usage

**Option 1: Using Make (Recommended)**

```bash
# Setup environment (installs dependencies)
make setup

# Run ingestion
make ingest-spotify
```

**Option 2: Manual Execution**

```bash
# Install dependencies
.venv/bin/pip install -r apps/data_ingestion/manual_job/spotify/requirements.txt

# Run the ingestion (interactive)
.venv/bin/python apps/data_ingestion/manual_job/spotify/ingest.py

# Run with automatic confirmation
.venv/bin/python apps/data_ingestion/manual_job/spotify/ingest.py --yes

# Dry run to see what would be ingested
.venv/bin/python apps/data_ingestion/manual_job/spotify/ingest.py --dry-run
```

## Command Line Options

- `--yes`, `-y`: Skip the confirmation prompt and proceed with ingestion
- `--dry-run`: List files that would be ingested without actually processing them

## Example Output

```
Spotify Data Ingestion
==================================================
Data Path: ~/Documents/jimwurst_local_data/spotify

Found the following Spotify files to ingest:

📊 JSON files - 8 file(s):
   - StreamingHistory0.json
   - StreamingHistory1.json
   - Playlist1.json
   - YourLibrary.json
   - Userdata.json
   - Inferences.json
   - Payments.json
   - Identity.json

📁 CSV files - 2 file(s):
   - streaming_history.csv
   - playlist_data.csv

Total Size: 15.32 MB
Estimated Processing Time: ~3.1 seconds

Do you want to proceed with Spotify data ingestion? (y/N):
```

## Table Naming Convention

Tables are created with sanitized filenames:
- `StreamingHistory0.json` → `s_spotify.streaminghistory0`
- `YourLibrary.json` → `s_spotify.yourlibrary`
- `streaming_history.csv` → `s_spotify.streaming_history`

## Notes

- **Idempotent**: Running the script multiple times will drop and recreate tables with fresh data
- **Encoding**: The CSV ingestor tries multiple encodings (UTF-8, UTF-8-BOM, Latin-1, CP1252)
- **JSON Flattening**: Nested JSON structures are automatically flattened with underscore-separated keys
- **Batch Processing**: Data is inserted in batches of 1000 rows for efficiency
