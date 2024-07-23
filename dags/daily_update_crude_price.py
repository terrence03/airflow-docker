import sys
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

sys.path.append("/opt/airflow")
from plugins.oil_price.moea import CrudeOilPrice
from plugins.tools import sqlite_tools

db_path = Path("/opt/airflow/data/oilprice.db")

dag = DAG(
    "update_crude_price_daily",
    description="Update the crude oil price daily",
    schedule="30 8 * * *",
    start_date=datetime(2024, 6, 15),
    catchup=False,
    tags=["oil"],
)


def update_crude_oil_price():
    crude_oil_price = CrudeOilPrice()
    data = crude_oil_price.get_daily_data()
    # Save the data to SQLite
    sqlite_tools.save_data(db_path=db_path, table_name="Daily.CrudeOil", data=data)

    print("Finish")


t1 = PythonOperator(
    task_id="daily_update_crude_oil_price",
    python_callable=update_crude_oil_price,
    dag=dag,
)

t1
