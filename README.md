# Airflow_assignment

### Airflow Data Pipeline with PySpark and PostgreSQL

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline using Apache Airflow, PySpark, and PostgreSQL inside a Dockerized environment.
It automatically generates fake data, cleans it with Spark, merges the datasets, loads results into PostgreSQL, performs simple analysis, and visualizes outcomes.


### 🧩 Architecture Overview

##### Technologies used
- Apache Airflow (3.1.0) – Orchestrates all pipeline tasks
- PySpark (3.5.x) – Performs scalable data transformation
- PostgreSQL (16) – Stores the cleaned data
- Redis + Celery – Handles Airflow’s distributed task execution
- Matplotlib – Visualizes aggregated insights
- Faker – Generates synthetic person and company datasets

##### Data Flow
```
Faker Data → PySpark Transform → Merge → Load into Postgres → Analyze → Cleanup
```

### ⚙️ Pipeline Workflow
The Airflow DAG is named pipelinev2, scheduled to run daily at 2 AM.

##### 1️⃣ Data Ingestion
Tasks: fetch_persons, fetch_companies
Uses the Faker library to generate 100 random records each for persons and companies.
Writes them to CSVs under `/opt/airflow/data/persons.csv and /opt/airflow/data/companies.csv.`

##### 2️⃣ PySpark Transformation
Task: spark_transform
Reads the two CSVs into Spark DataFrames.
Cleans data by:
Lowercasing all email addresses
Dropping duplicates based on email
Writes cleaned data to:

/opt/airflow/data/persons_cleaned/

/opt/airflow/data/companies_cleaned/

3️⃣ Merge CSVs

Task: merge_csvs

Combines the two cleaned datasets record-by-record into a single CSV (merged_data.csv) with key fields:

firstname, lastname, email, company_name, company_email

4️⃣ Load into PostgreSQL

Task: load_csv_to_pg

Uses PostgresHook to connect via conn_id="Postgres".

Creates schema week8_demo and table employees if not present.

Inserts all merged rows into PostgreSQL.

5️⃣ Simple Analysis

Task: analyze_domains

Runs SQL to count top email domains from the employees table.

Plots results as a bar chart (domain_counts.png) and saves it to /opt/airflow/data/.

6️⃣ Cleanup

Task: clear_folder

Deletes temporary CSVs and output folders from /opt/airflow/data/ to keep the environment clean.
