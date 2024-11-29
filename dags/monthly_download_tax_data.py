import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow")
from plugins.tax.main import download_tax_data

download_dir = Path("/opt/airflow/downloads")
db_dir = Path("/opt/airflow/data")

default_args = {
    "email": ["chienhua.hsu@tri.org.tw"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "monthly_download_tax_data",
    description="Download the tax data monthly",
    schedule="0 17 1 * *",
    start_date=datetime(2024, 6, 15),
    catchup=False,
    tags=["monthly", "tax"],
    default_args=default_args,
)


def download():
    download_tax_data(download_dir)
    print("Finish")


t1 = PythonOperator(
    task_id="monthly_download_tax_data",
    python_callable=download_tax_data,
    dag=dag,
    retries=3,
    retry_delay=timedelta(minutes=30),
)

t1