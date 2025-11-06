from __future__ import annotations
import csv
import os
import shutil
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task  # Changed from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import Error as DatabaseError
from faker import Faker

# PySpark + Visualization
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col
import matplotlib.pyplot as plt


OUTPUT_DIR = "/opt/airflow/data"
TARGET_TABLE = "employees"
SCHEMA = "week8_demo"

default_args = {
    "owner": "IDS706",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="pipelinev2",
    start_date=datetime(2025, 10, 1),
    schedule="0 2 * * *",     # Run daily at 2 AM
    catchup=False,
    default_args=default_args,
    doc_md="""
    ## Weekly Assignment DAG (with PySpark)
    - Parallel ingestion of two datasets (persons & companies)
    - PySpark transformation
    - Merge and load into PostgreSQL
    - Analysis (chart of email domains)
    - Cleanup
    """,
) as dag:

    # -------------------- 1️⃣ Data Ingestion -------------------- #
    @task()
    def fetch_persons(output_dir: str = OUTPUT_DIR, quantity: int = 100) -> str:
        fake = Faker()
        data = [{
            "firstname": fake.first_name(),
            "lastname": fake.last_name(),
            "email": fake.free_email(),
            "phone": fake.phone_number(),
            "address": fake.street_address(),
            "city": fake.city(),
            "country": fake.country(),
        } for _ in range(quantity)]
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, "persons.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader(); writer.writerows(data)
        print(f"Persons data saved to {path}")
        return path

    @task()
    def fetch_companies(output_dir: str = OUTPUT_DIR, quantity: int = 100) -> str:
        fake = Faker()
        data = [{
            "name": fake.company(),
            "email": f"info@{fake.domain_name()}",
            "phone": fake.phone_number(),
            "country": fake.country(),
            "website": fake.url(),
            "industry": fake.bs().split()[0].capitalize(),
            "catch_phrase": fake.catch_phrase(),
            "employees_count": fake.random_int(min=10, max=5000),
            "founded_year": fake.year(),
        } for _ in range(quantity)]
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, "companies.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader(); writer.writerows(data)
        print(f"Companies data saved to {path}")
        return path

    # -------------------- 2️⃣ PySpark Transformation -------------------- #
    @task()
    def spark_transform(persons_path: str, companies_path: str, output_dir: str = OUTPUT_DIR) -> dict:
        """
        Use PySpark to clean both CSVs (lowercase emails, remove duplicates, etc.)
        """
        spark = SparkSession.builder.appName("Airflow_PySpark_Transform").getOrCreate()

        # Load both CSVs
        persons_df = spark.read.csv(persons_path, header=True, inferSchema=True)
        companies_df = spark.read.csv(companies_path, header=True, inferSchema=True)

        # Transformations
        persons_df = persons_df.withColumn("email", lower(col("email"))).dropDuplicates(["email"])
        companies_df = companies_df.withColumn("email", lower(col("email"))).dropDuplicates(["email"])

        # Save cleaned versions
        persons_clean = os.path.join(output_dir, "persons_cleaned")
        companies_clean = os.path.join(output_dir, "companies_cleaned")

        persons_df.coalesce(1).write.mode("overwrite").option("header", True).csv(persons_clean)
        companies_df.coalesce(1).write.mode("overwrite").option("header", True).csv(companies_clean)


        spark.stop()
        print(f"PySpark transformations complete.\nSaved to {persons_clean} and {companies_clean}")
        return {
                    "persons_clean": persons_clean,
                    "companies_clean": companies_clean
                }

    # -------------------- 3️⃣ Merge CSVs -------------------- #
    @task()
    def merge_csvs(cleaned_files: dict, output_dir: str = OUTPUT_DIR) -> str:
        persons_path = cleaned_files["persons_clean"]
        companies_path = cleaned_files["companies_clean"]
        merged_path = os.path.join(output_dir, "merged_data.csv")

        # Find actual CSV files in Spark output folders (they're named part-*.csv)
        def find_csv(base_dir: str):
            for file in os.listdir(base_dir):
                if file.endswith(".csv"):
                    return os.path.join(base_dir, file)
            return base_dir

        persons_path = find_csv(persons_path)
        companies_path = find_csv(companies_path)

        with open(persons_path, newline="", encoding="utf-8") as f1, \
             open(companies_path, newline="", encoding="utf-8") as f2:
            persons_reader = list(csv.DictReader(f1))
            companies_reader = list(csv.DictReader(f2))

        merged_data = []
        for i in range(min(len(persons_reader), len(companies_reader))):
            person = persons_reader[i]
            company = companies_reader[i]
            merged_data.append({
                "firstname": person["firstname"],
                "lastname": person["lastname"],
                "email": person["email"],
                "company_name": company["name"],
                "company_email": company["email"],
            })

        with open(merged_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=merged_data[0].keys())
            writer.writeheader(); writer.writerows(merged_data)

        print(f"Merged CSV saved to {merged_path}")
        return merged_path

    # -------------------- 4️⃣ Load to PostgreSQL -------------------- #
    @task()
    def load_csv_to_pg(conn_id: str, csv_path: str, table: str = TARGET_TABLE, append: bool = False) -> int:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            rows = [tuple((r.get(col, "") or None) for col in fieldnames) for r in reader]

        if not rows:
            print("No rows found in CSV; nothing to insert.")
            return 0

        create_schema = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};"
        create_table = f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{table} (
                {', '.join([f'{col} TEXT' for col in fieldnames])}
            );
        """
        delete_rows = f"DELETE FROM {SCHEMA}.{table};" if not append else None
        insert_sql = f"""
            INSERT INTO {SCHEMA}.{table} ({', '.join(fieldnames)})
            VALUES ({', '.join(['%s' for _ in fieldnames])});
        """

        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(create_schema)
                cur.execute(create_table)
                if delete_rows:
                    cur.execute(delete_rows)
                cur.executemany(insert_sql, rows)
                conn.commit()
            inserted = len(rows)
            print(f"Inserted {inserted} rows into {SCHEMA}.{table}")
            return inserted
        except DatabaseError as e:
            print(f"Database error: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    # -------------------- 5️⃣ Simple Analysis -------------------- #
    @task()
    def analyze_domains(conn_id: str, out_dir: str = OUTPUT_DIR) -> str:
        hook = PostgresHook(postgres_conn_id=conn_id)
        sql = f"""
            SELECT split_part(email, '@', 2) AS domain, COUNT(*) AS cnt
            FROM {SCHEMA}.{TARGET_TABLE}
            WHERE email LIKE '%@%'
            GROUP BY domain
            ORDER BY cnt DESC
            LIMIT 10;
        """
        rows = hook.get_records(sql)
        if not rows:
            print("No data to analyze.")
            return ""

        domains, counts = zip(*rows)
        plt.figure()
        plt.bar(domains, counts)
        plt.xticks(rotation=45, ha="right")
        plt.title("Top Email Domains (Employees)")
        plt.tight_layout()

        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "domain_counts.png")
        plt.savefig(out_path)
        print(f"Wrote chart: {out_path}")
        return out_path

    # -------------------- 6️⃣ Cleanup -------------------- #
    @task()
    def clear_folder(folder_path: str = OUTPUT_DIR) -> None:
        if not os.path.exists(folder_path):
            print(f"Folder {folder_path} does not exist.")
            return
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)
                    print(f"Removed file: {file_path}")
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    print(f"Removed directory: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")
        print("Clean process completed!")

    # -------------------- 7️⃣ DAG Dependencies -------------------- #
    persons_file = fetch_persons()
    companies_file = fetch_companies()

    # PySpark step (transform both in parallel)
    cleaned_files = spark_transform(persons_file, companies_file)

    merged_path = merge_csvs(cleaned_files)
    load_to_db = load_csv_to_pg(conn_id="Postgres", csv_path=merged_path, table=TARGET_TABLE)
    chart_path = analyze_domains(conn_id="Postgres")

    # Final dependency chain
    [persons_file, companies_file] >> cleaned_files >> merged_path >> load_to_db >> chart_path >> clear_folder()