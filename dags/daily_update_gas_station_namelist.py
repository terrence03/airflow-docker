import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow")
from plugins.gas_station.namelist import main

db_path = Path("/opt/airflow/data/oilprice.db")

default_args = {
    "email": ["chienhua.hsu@tri.org.tw"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "daily_gas_station_namelist_update",
    description="Update the gas station namelist daily",
    schedule="30 8 * * *",
    start_date=datetime(2024, 6, 15),
    catchup=False,
    tags=["daily", "oil"],
    default_args=default_args,
)


t1 = PythonOperator(
    task_id="daily_gas_station_namelist_update",
    python_callable=main,
    dag=dag,
)

t1
