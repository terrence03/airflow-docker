from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.gas_station.sales import Crawler, DataFlow
from src.tools import sqlite_tools

db_path = Path("/opt/airflow/data/gas_station_sales.db")
download_dir = Path("/opt/airflow/downloads")

default_args = {
    "email": ["chienhua.hsu@tri.org.tw"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "monthly_gas_station_sales_update",
    description="Update the gas station sales data monthly",
    schedule="30 8 10 * *",
    start_date=datetime(2024, 6, 15),
    catchup=False,
    tags=["monthly", "oil"],
    default_args=default_args,
)


def download_data_and_save():
    download_dir_child = Path(download_dir) / "gas_station_sales"
    if not download_dir_child.exists():
        download_dir_child.mkdir(parents=True)
    crawler = Crawler()
    file_path = crawler.download_latest(download_dir_child)
    data_flow = DataFlow()
    xlsx_a = data_flow.read_xlsx_a(file_path)
    xlsx_b = data_flow.read_xlsx_b(file_path)
    sqlite_tools.save_data(db_path, "Table1", xlsx_a)
    sqlite_tools.save_data(db_path, "Table2", xlsx_b)
    print("Success")


task = PythonOperator(
    task_id="download_data_and_save",
    python_callable=download_data_and_save,
    dag=dag,
)

task
