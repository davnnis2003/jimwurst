# Data Transformation

Data transformation is the process of converting raw data into a structured, cleaned, and enriched format that is ready for analysis and decision-making. It effectively bridges the gap between raw data ingestion and business value creation.

In modern analytics engineering, this involves:
- **Cleaning**: Removing duplicates and handling missing values.
- **Structuring**: Modeling data into coherent business entities (Dimensions and Facts).
- **Enriching**: Adding business logic and calculations (KPIs).

In this project, we leverage **dbt (data build tool)** to implement a modular and scalable transformation layer.

## Recommended Reading

For a deeper dive into modern data transformation strategies and how to build business-ready data models, check out the article:
[Data Marts in 2025: A dbt‑First Guide for Analytics Engineering](https://open.substack.com/pub/jimmypang/p/understanding-data-marts-in-modern) by Jimmy Pang.

## Project Structure

- **dbt/**: The main dbt project containing models, macros, and tests.
