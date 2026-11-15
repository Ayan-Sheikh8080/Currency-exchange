
import requests
from datetime import datetime, timezone

from pymongo import MongoClient

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


# ---------------- CONFIGURATION ----------------

API_URL = "https://api.frankfurter.dev/v2/rates"

BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = "USD,GBP,JPY"

MONGO_DATABASE = "currency_db"
MONGO_COLLECTION = "exchange_rates"


# ---------------- TASK 1: EXTRACT ----------------

def extract_currency_rates(**context):

    params = {
        "base": BASE_CURRENCY,
        "quotes": TARGET_CURRENCIES
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    raw_data = response.json()

    if not isinstance(raw_data, list) or not raw_data:
        raise ValueError("No exchange-rate data received from API")

    print("Extracted records:", len(raw_data))
    print("Raw API data:", raw_data)

    # Push extracted data to XCom
    context["ti"].xcom_push(
        key="raw_rates",
        value=raw_data
    )


# ---------------- TASK 2: TRANSFORM ----------------

def transform_exchange_rates(**context):

    ti = context["ti"]

    # Retrieve extracted data from XCom
    raw_data = ti.xcom_pull(
        task_ids="extract_currency_rates",
        key="raw_rates"
    )

    if not raw_data:
        raise ValueError("No extracted data found in XCom")

    transformed_data = []

    retrieved_at = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    for item in raw_data:

        record = {
            "base_currency": item["base"],
            "target_currency": item["quote"],
            "exchange_rate": float(item["rate"]),
            "rate_date": item["date"],
            "source": "Frankfurter API",
            "retrieved_at": retrieved_at
        }

        transformed_data.append(record)

    print("Transformed records:", transformed_data)

    # Push transformed data to XCom
    ti.xcom_push(
        key="transformed_rates",
        value=transformed_data
    )


# ---------------- TASK 3: LOAD ----------------

def load_into_mongodb(**context):

    ti = context["ti"]

    # Retrieve transformed data from XCom
    transformed_data = ti.xcom_pull(
        task_ids="transform_exchange_rates",
        key="transformed_rates"
    )

    if not transformed_data:
        raise ValueError("No transformed data found in XCom")

    # Read MongoDB connection string from Airflow Variable
    mongo_uri = Variable.get("MONGODB_URI")

    client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=10000
    )

    try:

        # Verify MongoDB connection
        client.admin.command("ping")

        database = client[MONGO_DATABASE]
        collection = database[MONGO_COLLECTION]

        # Insert or update records
        for record in transformed_data:

            collection.update_one(
                {
                    "base_currency": record["base_currency"],
                    "target_currency": record["target_currency"],
                    "rate_date": record["rate_date"]
                },
                {
                    "$set": record
                },
                upsert=True
            )

        print("MongoDB loading completed successfully!")
        print("Records processed:", len(transformed_data))
        print("Total records in collection:", collection.count_documents({}))

    finally:
        client.close()


# ---------------- DAG DEFINITION ----------------

default_args = {
    "owner": "Ayan",
    "retries": 2
}

with DAG(
    dag_id="currency_exchange_etl",
    default_args=default_args,
    description="Daily currency exchange ETL pipeline using Frankfurter API and MongoDB",
    schedule="@daily",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["ETL", "Currency", "MongoDB"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_currency_rates",
        python_callable=extract_currency_rates
    )

    transform_task = PythonOperator(
        task_id="transform_exchange_rates",
        python_callable=transform_exchange_rates
    )

    load_task = PythonOperator(
        task_id="load_into_mongodb",
        python_callable=load_into_mongodb
    )

    # Define task execution order
    extract_task >> transform_task >> load_task
