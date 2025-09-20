# COMP5339 Group Assignment 2  
**Building and Orchestrating a Data Pipeline for an Analytics Suite**

## Overview

This project implements a modular data pipeline for analytics, using Apache Airflow for orchestration, dbt for data transformation, and Apache Superset for visualization. The pipeline is designed for a retail dataset (Part 1) and can be adapted for medical data (MIMIC, Part 2). All components run in Docker containers for reproducibility and ease of deployment.

---

## Project Structure

- **dbt/**: dbt project for data modeling and transformation.
- **airflow/**: Airflow DAGs, configs, and SQL scripts for ETL orchestration.
- **generator/**: Synthetic data generator (used in Part 1).
- **superset/**: Superset dashboard assets and Dockerfile.
- **docker-compose.yml**: Orchestrates all services.

---

## Setup Instructions

1. **Install Docker**  
	Ensure Docker is installed on your system.

2. **Clone the Repository**  
	Download the project and navigate to the root directory.

3. **Build and Start Containers**  
	```sh
	docker compose up --build
	```
	This will build and start Airflow, dbt, Superset, and the data generator.

4. **Data Generation**  
	Synthetic data is automatically generated and placed in the `import/` directory.

5. **ETL Orchestration (Airflow)**  
	- Access Airflow at [http://localhost:8080](http://localhost:8080) (user: airflow, password: airflow).
	- Trigger DAGs in order:
	  - `initialize_etl_environment`
	  - `import_main_data`
	  - `import_reseller_data`
	  - Other DAGs as required

6. **Data Transformation (dbt)**  
	- Trigger `run_dbt_init_tasks` and `run_dbt_model` DAGs via Airflow to run dbt models.

7. **Visualization (Superset)**  
	- Access Superset at [http://localhost:8088](http://localhost:8088) (user: admin, password: admin).
	- Create dashboards and visualizations using the transformed data.

---

## Data Pipeline Architecture

The pipeline consists of the following stages:

1. **Data Generation**: Synthetic retail data is created using Python scripts in the generator container.
2. **Data Import**: Airflow DAGs import data from flat files (CSV/XML) and OLTP sources into staging tables.
3. **Transformation**: dbt models transform and load data into dimension and fact tables in the data warehouse.
4. **Visualization**: Superset connects to the warehouse for interactive dashboards.

*Diagram: (Insert your architecture diagram here)*

---

## Airflow DAGs

- **initialize_etl_environment**: Sets up database objects and environment.
- **import_main_data**: Imports transactional data from OLTP to staging.
- **import_reseller_data**: Imports reseller data from flat files to staging.
- **run_dbt_init_tasks**: Initializes dbt environment.
- **run_dbt_model**: Runs dbt models to transform and load data.

Each DAG is modular and can be triggered independently via the Airflow UI.

---

## dbt Models

- **Source Models**: Define raw data sources (`models/src/`).
- **Staging Models**: Clean and standardize data (`models/staging/`).
- **Dimension Models**: Create dimension tables (e.g., `dim_channel.sql`).
- **Fact Models**: Aggregate transactional data (e.g., `fact_sales.sql`).
- **Tests**: Ensure data integrity (e.g., `total_amount_is_non_negative.sql`).

Configuration files (`dbt_project.yml`, `packages.yml`, `profiles.yml`) manage dbt settings and dependencies.

---

## Code Modifications

- Customized Airflow DAGs to match the assignment requirements.
- Developed dbt models for staging, dimension, and fact tables.
- Configured Dockerfiles for each service.
- Added Superset dashboard assets.
- Updated configuration files for seamless integration.

---

## Screenshots

- **Airflow GUI**: (Insert screenshot showing successful DAG runs, renamed as `import_main_data_{group name}`)
- **Superset Dashboard**: (Insert screenshot of your dashboard, with group name in the title)

---

## Data Insights & Recommendations

### Data Insights

Superset visualizations revealed key sales trends, top-performing regions, and product categories. The dashboard enabled quick identification of outliers and sales patterns, supporting data-driven decision-making.

### Recommendations

- **Pipeline Improvements**:  
  - Automate error handling and notifications in Airflow.
  - Add more dbt tests for data quality.
  - Optimize SQL queries for large datasets.
- **Scalability**:  
  - Refactor for incremental data loads.
  - Containerize additional services as needed.
- **Visualization**:  
  - Expand dashboard metrics for deeper insights.

---

## References

- University of Sydney referencing guidelines.
- Official documentation for Airflow, dbt, Superset, and Docker.

---

**For full details, see the technical report PDF.**  
**Remove all raw data files before submission. Zip size must be <5MB.**
