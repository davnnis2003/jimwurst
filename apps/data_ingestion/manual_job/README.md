# Manual Ingestion Jobs

This directory contains scripts for "Manual Jobs" - standardized interactions where we ingest complex local data files that don't fit into standard Airbyte connectors.

## Structure

We follow a **"One Folder Per Application"** structure. Each folder contains the ingestion logic, requirements, and documentation for that specific data source.

## Available Jobs

### [Apple Health](./apple_health/README.md)
*   **Source**: XML Export from iPhone.
*   **Schema**: `s_apple_health`.
*   **Language**: Python.

## How to add a new job

1.  Create a new folder: `mkdir my_new_app`.
2.  Add your ingestion script (e.g., `ingest.py` or `main.go`).
3.  Add a `README.md` explaining input/output.
4.  Add `requirements.txt` or `go.mod` if needed.
