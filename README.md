# jimwurst

![jimwurst](docs/jimwurst.png)

> "Es ist mir wurst." Germans 🇩🇪 

It is a German expression meaning that "It doesn't matter to me", literally translated as "This is sausage to me". 🌭 (yeah no there is no sausage emoji so I am putting a hotdog here instead)

Casual in attitude, serious about DWH architecture. 

# Tenet
This is a monolithic repository of a DWH for personal data analytics and AI. Everything in this repo is expected to be **100% running locally**.

A very core idea here is **tools agnostic**. Any tooling in modern data stack will be abstracted, and materialize in places like folder structure. Open source tooling will be prioritized.

The pholosophy behind can be found in [Data Biz](https://jimmypang.substack.com/s/engineering-value-at-scale).

# 🧠 AI Agent (New!)

`jimwurst` now features an AI-powered interaction layer powered by [Ollama](https://ollama.com/) and [LangChain](https://www.langchain.com/).

## Prerequisites
1.  Install [Ollama](https://ollama.com/download).
2.  Pull the model: `ollama pull qwen2.5:3b`.
3.  Ensure your local Postgres stack is running (`make up`).

## Usage

You can interact with the agent in two modes:

**Interactive Mode:**
```bash
python apps/data_activation/ollama_agent/agent.py --interactive
```

**Single Prompt Mode:**
```bash
python apps/data_activation/ollama_agent/agent.py --prompt "Ingest data from /Users/jimmypang/mydata.csv"
```

## Capabilities
*   **Ingest Data**: "Ingest file /path/to/file.csv"
*   **Transform Data**: "Run transformations"
*   **Ask Questions**: "How many users are in the users table?"

# Python Environment Setup

This project uses [uv](https://github.com/astral-sh/uv) for fast, reliable Python dependency management.

## Installing uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

After installation, restart your shell or run:
```bash
source $HOME/.local/bin/env
```

## Managing Dependencies

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

# Getting Started
To get the local stack running (Postgres), simply run:

```bash
make up
```
This will create the necessary `.env` file and spin up the Docker containers.

## Connectivity
You can connect to the local PostgreSQL instance with the following details:
* **Host**: `localhost`
* **Port**: `5432`
* **User**: `jimwurst_user` (default)
* **Password**: `jimwurst_password` (default)
* **Database**: `jimwurst_db` (default)

## Database Schemas
The following schemas are initialized by default:
* `marts`: Modeled data ready for consumption.
* `intermediate`: Intermediate data transformations.
* `staging`: Raw data staging area.
* `s_<app_name>`: Operational Data Store (ODS) schemas (e.g. `s_google_sheet`).

# Abstraction & Toolings

## Data Ops
* Containerization: [Docker](https://github.com/docker/docker)
* CI/CD: [Github Actions](https://github.com/features/actions)
* Job Orchestration: Python / [Makefile](Makefile)
* DWH: Postgres
* Package Manager: [uv](https://github.com/astral-sh/uv)

## Data Engineering
* Data Ingestion: Python / SQL
* Data Transformation: [dbt Core](https://github.com/dbt-labs/dbt-core)
* Data Activation:
  * Reporting: (tbd) [Metabase](https://github.com/metabase/metabase)/[lightdash](https://github.com/lightdash/lightdash)
  * adhoc analysis: [Jupyter](https://github.com/jupyter/jupyter)
  * AI: [Ollama](https://github.com/ollama/ollama)

## Scalability (Optional)
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
└── utils/                   # Shared internal packages (Python utils, custom operators)
```