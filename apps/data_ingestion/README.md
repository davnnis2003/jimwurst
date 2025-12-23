# Data Ingestion

Data Ingestion is the "Entry Gate" for all raw data entering the **jimwurst** ecosystem. This component is responsible for the **Extract & Load (EL)** phase of our ELT (Extract, Load, Transform) pipeline.

## 🛠 Tooling

### Lightweight Default (Personal Scale)
For personal use, we prioritize simple, Python-based ingestion scripts. These are fast, easy to debug, and have minimal overhead.
- **Manual Ingestion**: Scripts located in `manual_job/` for processing local data exports (LinkedIn, Substack, Apple Health, etc.).
- **Custom Scripts**: You can add your own Python scripts to pull from APIs directly into Postgres.

### Airbyte (Large Scale - Optional)
[Airbyte](https://github.com/airbytehq/airbyte) is available for those who need to sync data from hundreds of complex sources.
- **Note**: Airbyte is heavy and may require significant resources. It is recommended for advanced users or high-volume data operations.
- **Configurations**: Managed as code or via the Airbyte UI.

## 📂 Structure

- **`airbyte/`**: Contains Airbyte-specific configuration files.
- **Manual Ingestion (PII Safe)**: We use an external "Landing Zone" located outside this repository to ensure PII (Personally Identifiable Information) is never committed to Git.
  - Default Location: `~/document/jimwurst_local_data/`
  - Structure:
    - `/linkedin/`: Drop your LinkedIn CSV exports here.
    - `/substack/`: Drop your Substack CSV exports here.
  - Setup: Point `LOCAL_DATA_PATH` in your `.env` to this directory.
  - **Schema Note**: By default, the database only includes core schemas. If you use personal folders like `linkedin` or `substack`, you must create the corresponding schemas (e.g., `s_linkedin`) manually in Postgres or via your own initialization scripts.

