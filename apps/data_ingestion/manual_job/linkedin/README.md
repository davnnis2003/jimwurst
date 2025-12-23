# LinkedIn Data Ingestion

This manual job ingests LinkedIn export data into the local PostgreSQL database. LinkedIn offers two types of data exports, both supported by this ingestion script:

## Export Types

### 1. Basic Creator Insights Analytics
- **Format**: `.xlsx` (Excel file)
- **Content**: Analytics and insights for content creators
- **Storage**: `~/Documents/jimwurst_local_data/linkedin/basic/`

### 2. Full Data Archive
- **Format**: `.csv` files (delivered in batches)
- **Content**: Complete data archive including connections, messages, posts, etc.
- **Storage**: `~/Documents/jimwurst_local_data/linkedin/complete/`

## Prerequisites

### Exporting Basic Creator Insights

1. Log in to LinkedIn
2. Navigate to your Creator mode analytics
3. Download the analytics export (`.xlsx` file)
4. Place the file in: `~/Documents/jimwurst_local_data/linkedin/basic/`

### Exporting Full Data Archive

1. Log in to LinkedIn
2. Click on your profile picture ("Me") and select **Settings & Privacy**
3. Go to **Data privacy** on the left menu
4. Under **How LinkedIn uses your data**, select **Get a copy of your data**
5. Select **Download larger data archive** (recommended) or specify data files
6. Wait for the email from LinkedIn and download the `.zip` file(s)
7. Extract the contents
8. Place all `.csv` files in: `~/Documents/jimwurst_local_data/linkedin/complete/`

> **Note**: The script will automatically create these directories if they don't exist.

## How it works

The script `ingest.py` performs the following:

1. Connects to the local PostgreSQL database (`jimwurst_db`)
2. Ensures the schema `s_linkedin` exists
3. Scans both folders:
   - `~/Documents/jimwurst_local_data/linkedin/basic/` for `.xlsx` files
   - `~/Documents/jimwurst_local_data/linkedin/complete/` for `.csv` files
4. **Lists all files found and estimates processing time**
5. **Asks for your confirmation** before proceeding
6. Processes each file using the appropriate ingestor:
   - **ExcelIngestor**: Handles `.xlsx` files, processes all sheets
   - **CSVIngestor**: Handles `.csv` files
7. Creates tables in `s_linkedin` schema with naming convention:
   - Basic exports: `s_linkedin.basic_[filename]`
   - Complete exports: `s_linkedin.complete_[filename]`
8. All columns are ingested as `TEXT` to preserve raw data

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

## Example Output

```
LinkedIn Data Ingestion
==================================================
Base Path: ~/Documents/jimwurst_local_data/linkedin
Basic Exports Path: ~/Documents/jimwurst_local_data/linkedin/basic
Complete Exports Path: ~/Documents/jimwurst_local_data/linkedin/complete

Found the following LinkedIn files to ingest:

📊 Basic Creator Insights (.xlsx) - 1 file(s):
   - analytics_2024.xlsx

📁 Full Data Archive (.csv) - 5 file(s):
   - Connections.csv
   - Messages.csv
   - Posts.csv
   - Reactions.csv
   - Comments.csv

Total Size: 12.45 MB
Estimated Processing Time: ~2.5 seconds

Do you want to proceed with LinkedIn data ingestion? (y/N):
```

