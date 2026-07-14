from airflow.sdk import DAG, task
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import os

username = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
database = os.environ["POSTGRES_DB"]

connection_string = f"postgresql+psycopg2://{username}:{password}@postgres_data:5432/{database}"


engine = create_engine(connection_string)

with DAG(
    dag_id="transaction_pipeline",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    @task
    def extract_data():
        df = pd.read_csv("/opt/airflow/data/raw_transactions.csv")
        df.to_sql("bronze_transactions", con=engine, if_exists="replace", index=False)


    @task
    def clean_data():
        df = pd.read_sql("SELECT * FROM bronze_transactions", con=engine)
        df['txn_date'] = pd.to_datetime(df['txn_date'], errors='coerce')
        df.dropna(subset=['txn_date'], inplace=True)

        df['amount'] = df['amount'].str.replace('$', '', regex=False)

        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

        df['currency'] = df['currency'].fillna('USD')

        df['status'] = df['status'].str.upper()

        return df


    @task
    def load_silver(cleaned_df):
        cleaned_df.to_sql("silver_transactions", con=engine, if_exists="replace", index=False)

    extracted = extract_data()
    cleaned = clean_data()
    loaded = load_silver(cleaned)

    extracted >> cleaned >> loaded