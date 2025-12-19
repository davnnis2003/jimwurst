# jimwurst

![jimwurst](docs/jimwurst.png)

> "Es ist mir wurst." Germans 🇩🇪 

It is a Geramn expression meaning that "It doesn't matter to me", literally translated as "This is sausage to me". 🌭 (yeah no there is no sausage emoji so I am putting a hotdog here instead)

Casual in attitude, serious about DWH architecture. 

# Tenet
This is a monolithic repository of a DWH for personal data analytics and AI. Everything in this repo is expected to be **100% running locally**.

A very core idea here is **tools agnostic**. Any tooling in modern data stack will be abstracted, and materialize in places like folder structure. Open source tooling will be prioritized.

The pholosophy behind can be found in [Data Biz](https://jimmypang.substack.com/s/engineering-value-at-scale).

# Abstraction & Toolings

## Data Ops
* Containerization: [Docker](https://github.com/docker/docker)
* CI/CD: [Github Actions](https://github.com/features/actions)
* Job Orchestration: [Airflow](https://github.com/apache/airflow)
* DWH: Postgres

## Data Engineering
* Data Ingestion: Airflow operators and [Airbtye](https://github.com/airbytehq/airbyte)
* Data Transofmration: [dbt Core](https://github.com/dbt-labs/dbt-core)
* Data Activation:
  * Reporting: (tbd) [Metabase](https://github.com/metabase/metabase)/[lightdash](https://github.com/lightdash/lightdash)
  * adhoc analysis: [Jupyter](https://github.com/jupyter/jupyter)
  * AI: [Ollama](https://github.com/ollama/ollama)

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
├── docs/                    # Documentation, diagrams, and architecture RFCs
├── prompts/                 # AI system prompts and LLM context files
└── utils/                   # Shared internal packages (Python utils, custom operators)
```