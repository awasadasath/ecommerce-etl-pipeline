from __future__ import annotations
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
import pendulum
import os
import sys

from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

# 1. CONFIGURATION

PROJECT_ID = "gcp-airflow-project-480711"

BUCKET_NAME   = f"{PROJECT_ID}-datalake"
BQ_DATASET    = "ecommerce"
BQ_TABLE      = "ecommerce_transactions"
MYSQL_CONN_ID = "mysql_default"
GCP_CONN_ID   = "google_cloud_default"

# PATH
TEMP_PATH = "/tmp"
MYSQL_OUTPUT_FILE = f"{TEMP_PATH}/mysql_raw.parquet"
API_OUTPUT_FILE   = f"{TEMP_PATH}/api_raw.parquet"
FINAL_OUTPUT_FILE = f"{TEMP_PATH}/final_data.parquet"
GCS_PATH          = "staging/transaction.parquet"

log = logging.getLogger(__name__)

# === EXTERNAL LOGIC IMPORT ===
from transform_logic import run_transform_and_clean 

# 2. ALERT SYSTEM

def send_discord(msg_content):
    webhook = Variable.get("discord_webhook", default_var=None)
    if not webhook: 
        log.warning("⚠️ Discord Webhook not found in Airflow Variables")
        return
    try:
        data = {"username": "Airflow Bot", "content": msg_content}
        requests.post(webhook, json=data, timeout=5)
    except Exception as e:
        log.error(f"Discord Error: {e}")

def notify_failure(context):
    ti = context.get('task_instance')
    msg = f"🔴 **FAILED!**\nTask: `{ti.task_id}`\nDAG: `{ti.dag_id}`\nLog: {ti.log_url}"
    send_discord(msg)

def notify_success(**context):
    msg = f"🟢 **SUCCESS!** Pipeline Completed.\nData Cleaned & Loaded to `{BQ_DATASET}.{BQ_TABLE}`."
    send_discord(msg)

# 3. DAG DEFINITION

default_args = {
    'owner': 'awasada.sath',
    'on_failure_callback': notify_failure,
    'retries': 1, 
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    schedule="@daily", 
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=["portfolio", "production", "gcp", "discord-notify"],
)
def ecommerce_pipeline():

    start = EmptyOperator(task_id="start")

    # --- TASK 1: Extract MySQL ---
    @task(task_id="extract_mysql")
    def extract_mysql_data():
        import pandas as pd 
        
        log.info("Extracting ALL data from MySQL...")
        mysql_hook = MySqlHook(MYSQL_CONN_ID)
        sql = """
            SELECT 
                t.TransactionNo AS transaction_id,
                t.Date AS date,
                t.ProductNo AS product_id,
                t.Price AS price,
                t.Quantity AS quantity,
                t.CustomerNo AS customer_id,
                p.ProductName AS product_name,
                c.Country AS customer_country,
                c.Name AS customer_name,
                (t.Price * t.Quantity) AS total_amount
            FROM r2de3.transaction t
            LEFT JOIN r2de3.product p ON t.ProductNo = p.ProductNo
            LEFT JOIN r2de3.customer c ON t.CustomerNo = c.CustomerNo
        """
        df = mysql_hook.get_pandas_df(sql)
        
        if df.empty:
            log.warning("No Data Found in MySQL!")
            df = pd.DataFrame(columns=['date', 'price', 'quantity'])
        else:
            df['join_date'] = pd.to_datetime(df['date']).dt.normalize().dt.strftime('%Y-%m-%d')
            
        df.to_parquet(MYSQL_OUTPUT_FILE, index=False)
        log.info(f"Saved SQL Data: {len(df)} rows")
        return MYSQL_OUTPUT_FILE

    # --- TASK 2: Extract API ---
    @task(task_id="extract_api")
    def extract_api_data():
        import pandas as pd
        import requests
        from airflow.models import Variable
        from datetime import datetime
        
        log.info("Fetching Currency Rates...")
        api_url = Variable.get("currency_api_url")
        try:
            r = requests.get(api_url, timeout=10)
            r.raise_for_status()
            df = pd.DataFrame(r.json()).drop(columns=['id'])
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        except Exception as e:
            log.error(f"API Error: {e}. Using Fallback Data (45.0).")
            
            df = pd.DataFrame({
                'date': [datetime.now().strftime('%Y-%m-%d')], 
                'gbp_thb': [42.0]
            })
        
        df.to_parquet(API_OUTPUT_FILE, index=False)
        return API_OUTPUT_FILE

    # --- TASK 3: Transform ---
    @task(task_id="transform_data")
    def transform_data():
        log.info("Starting Transformation Logic from external script...")
        output_file_path = run_transform_and_clean(
            mysql_file=MYSQL_OUTPUT_FILE, 
            api_file=API_OUTPUT_FILE
        )
        return output_file_path 

    # --- TASK 4: Upload to GCS ---
    upload_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=FINAL_OUTPUT_FILE,
        dst=GCS_PATH,
        bucket=BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID,
    )

    # --- TASK 5: Load to BigQuery ---
    load_bq = GCSToBigQueryOperator(
        task_id="load_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[GCS_PATH],
        destination_project_dataset_table=f"{BQ_DATASET}.{BQ_TABLE}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=GCP_CONN_ID,
        autodetect=True,
    )

    # --- TASK 6: Success Alert ---
    success_alert = PythonOperator(
        task_id='notify_success',
        python_callable=notify_success,
    )

    # --- FLOW ---
    extract_group = [extract_mysql_data(), extract_api_data()]
    start >> extract_group >> transform_data() >> upload_gcs >> load_bq >> success_alert

ecommerce_pipeline()
