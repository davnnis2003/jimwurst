# jimwurst

![jimwurst](docs/jimwurst.png)

> "Es ist mir wurst." Germans 🇩🇪 

It is a German expression meaning that "It doesn't matter to me", literally translated as "This is sausage to me". 🌭 (yeah no there is no sausage emoji so I am putting a hotdog here instead)

Casual in attitude, serious about DWH architecture. 

# Table of Contents
- [Tenet](#tenet)
- [Getting Started](#getting-started)
- [Technical Details](#technical-details)
  - [Connectivity & Schemas](#connectivity--schemas)
  - [Python Environment Setup](#python-environment-setup)
  - [Abstraction & Toolings](#abstraction--toolings)
  - [Folder Structure](#-folder-structure)

# Tenet
This is a monolithic repository of a DWH for personal data analytics and AI. Everything in this repo is expected to be **100% running locally**.

A very core idea here is **tools agnostic**. Any tooling in modern data stack will be abstracted, and materialize in places like folder structure. Open source tooling will be prioritized.

The pholosophy behind can be found in [Data Biz](https://jimmypang.substack.com/s/engineering-value-at-scale).

# Getting Started

## Launch Everything
Run the following command to spin up the database, start Ollama (if needed), pull the AI model, and launch the AI Agent interface:

```bash
make up
```

This will:
1.  Start Postgres (Docker).
2.  Start the Ollama server in the background if it is not already running.
3.  Pull the required LLM (`qwen2.5:3b`).
4.  Launch the Streamlit web interface at `http://localhost:8501`.

# Technical Details

## Python Environment Setup

This project uses [uv](https://github.com/astral-sh/uv) for fast, reliable Python dependency management.

### Installing uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

After installation, restart your shell or run:
```bash
source $HOME/.local/bin/env
```

### Managing Dependencies

**Create a virtual environment:**
```bash
uv venv
```

**Install dependencies:**
```bash
uv pip install -r requirements.txt
```

**Add a new dependency:**
```bash
uv pip install <package>
uv pip freeze > requirements.txt
```

**Sync dependencies (ensure exact match with requirements.txt):**
```bash
uv pip sync requirements.txt
```

## Connectivity & Schemas
You can connect to the local PostgreSQL instance with:
* **Host**: `localhost` | **Port**: `5432` | **User/Pass**: `jimwurst_user`/`jimwurst_password`

The following schemas are initialized by default:
* `marts`, `intermediate`, `staging`, and `s_<app_name>` (ODS).

## Abstraction & Toolings

### Data Ops
* Containerization: [Docker](https://github.com/docker/docker)
* CI/CD: [Github Actions](https://github.com/features/actions)
* Job Orchestration: Python / [Makefile](Makefile)
* DWH: Postgres
* Package Manager: [uv](https://github.com/astral-sh/uv)

### Data Engineering
* Data Ingestion: Python / SQL
* Data Transformation: [dbt Core](https://github.com/dbt-labs/dbt-core)
* Data Activation:
  * Reporting: (tbd) [Metabase](https://github.com/metabase/metabase)/[lightdash](https://github.com/lightdash/lightdash)
  * adhoc analysis: [Jupyter](https://github.com/jupyter/jupyter)
  * AI: [Ollama](https://github.com/ollama/ollama)

### Scalability (Optional)
For larger-scale data operations, the following tools can be integrated:
* Job Orchestration: [Apache Airflow](https://github.com/apache/airflow)
* Data Ingestion: [Airbyte](https://github.com/airbytehq/airbyte)

## 🏗 Folder Structure
Each application follows a strict modular structure using `snake_case`. Tooling is materialized through structure:

```text
.
├── .github/                 # GitHub Actions workflows and CI config
├── apps/                    # Tool-specific configurations and deployments
│   ├── data_ingestion/      # Ingestion tools
│   │   └── airbyte/
│   ├── data_transformation/ # Transformation tools
│   │   └── dbt/             # Central dbt project
│   ├── data_activation/     # BI & activation tools
│   │   └── metabase/
│   └── job_orchestration/   # Orchestration tools
│       └── airflow/
├── docker/                  # Local orchestration (Docker Compose, .env)
├── docs/                    # Documentation, diagrams, and architecture RFCs
├── prompts/                 # AI system prompts and LLM context files
│── utils/                   # Shared internal packages (Python utils, custom operators)
```
