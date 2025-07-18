from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.oil_price.moea import CrudeOilPrice
from src.tools import sqlite_tools

db_path = Path("/opt/airflow/data/oilprice.db")

default_args = {
    "email": ["chienhua.hsu@tri.org.tw"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "daily_crude_price_update",
    description="Update the crude oil price daily",
    schedule="30 8 * * *",
    start_date=datetime(2024, 6, 15),
    catchup=False,
    tags=["daily"],
    default_args=default_args,
)


def update_crude_oil_price():
    crude_oil_price = CrudeOilPrice()
    data = crude_oil_price.get_daily_data()
    if data is not None:
        # Save the data to SQLite
        sqlite_tools.save_data(db_path=db_path, table_name="Daily.CrudeOil", data=data)
    else:
        print("No data")


t1 = PythonOperator(
    task_id="daily_crude_price_update", python_callable=update_crude_oil_price, dag=dag
)

t1
