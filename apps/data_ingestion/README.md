# Data Ingestion

Data Ingestion is the "Entry Gate" for all raw data entering the **jimwurst** ecosystem. This component is responsible for the **Extract & Load (EL)** phase of our ELT (Extract, Load, Transform) pipeline.

## 🛠 Tooling

### Airbyte
[Airbyte](https://github.com/airbytehq/airbyte) is our primary ingestion engine. It provides a standardized way to sync data from hundreds of sources (APIs, DBs, Files) into Postgres.
- **Configurations**: Connector settings and sync schedules are managed as code (or via the Airbyte UI during development).
- **Deployment**: Airbyte runs as a set of Docker containers, orchestrated alongside the rest of the stack.

### Airflow
While Airbyte moves the data, [Airflow](../job_orchestration/README.md) acts as the "Brain." It triggers Airbyte syncs based on time-based schedules or upstream event triggers.

## 📂 Structure

- **`airbyte/`**: Contains Airbyte-specific configuration files.
- **Manual Ingestion (PII Safe)**: We use an external "Landing Zone" located outside this repository to ensure PII (Personally Identifiable Information) is never committed to Git.
  - Default Location: `~/document/jimwurst_local_data/`
  - Structure:
    - `/linkedin/`: Drop your LinkedIn CSV exports here.
    - `/substack/`: Drop your Substack CSV exports here.
  - Setup: Point `LOCAL_DATA_PATH` in your `.env` to this directory.
  - **Schema Note**: By default, the database only includes core schemas. If you use personal folders like `linkedin` or `substack`, you must create the corresponding schemas (e.g., `s_linkedin`) manually in Postgres or via your own initialization scripts.

