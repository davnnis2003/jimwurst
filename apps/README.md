# apps

This is the core directory where all third-party tools and applications are configured and containerized.

## Structure
- **data_ingestion/**: Tools for pulling data from sources (e.g., Airbyte).
- **data_transformation/**: Tools for modeling and transforming data (e.g., dbt).
- **data_activation/**: Tools for consuming and acting on data (e.g., Metabase).
- **job_orchestration/**: Tools for scheduling and managing workflows (e.g., Airflow).

Every app here should be configured to run **locally** (e.g., via Docker Compose).
