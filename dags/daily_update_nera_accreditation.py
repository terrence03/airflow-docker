import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow")
from plugins.others.nera_accreditation_crawler import main

down_pdf_path = Path("/opt/airflow/downloads/nera_accreditation/pdf")
down_csv_path = Path("/opt/airflow/downloads/nera_accreditation/csv")

default_args = {
    "email": ["chienhua.hsu@tri.org.tw"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "daily_NERA_accreditation_update",
    description="Update the NERA accreditation daily",
    schedule="50 8 * * *",
    start_date=datetime(2025, 2, 6),
    catchup=False,
    tags=["daily", "other"],
    default_args=default_args,
)


t1 = PythonOperator(
    task_id="daily_NERA_accreditation_update",
    python_callable=main,
    dag=dag,
)

t1
