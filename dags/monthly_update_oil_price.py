from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.oil_price.moea import CrudeOilPrice, AvgPrice, CountyPrice
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
    "monthly_oil_price_update",
    description="Update the oil price data monthly",
    schedule="35 8 11 * *",
    start_date=datetime(2024, 6, 15),
    catchup=False,
    tags=["monthly", "oil"],
    default_args=default_args,
)


def update_oil_price():
    crude_oil_price = CrudeOilPrice().get_monthly_data()
    avg_price = AvgPrice().get_monthly_data()
    county_price = CountyPrice().get_monthly_data()

    # Save the data to SQLite
    sqlite_tools.save_data(
        db_path=db_path, table_name="Monthly.CrudeOil", data=crude_oil_price
    )
    sqlite_tools.save_data(db_path=db_path, table_name="Monthly.Avg", data=avg_price)
    sqlite_tools.save_data(
        db_path=db_path, table_name="Monthly.County", data=county_price
    )

    print("Finish")


t1 = PythonOperator(
    task_id="monthly_oil_price_update",
    python_callable=update_oil_price,
    dag=dag,
)

t1
