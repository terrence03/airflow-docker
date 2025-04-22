import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

sys.path.append("/opt/airflow")
from plugins.lpg_price.moea import get_counties_price, get_towns_price
from plugins.lpg_price.cpc_fpcc import (
    get_cpc_price_adj,
    get_cpc_asia_price,
    get_cpc_board_price,
    get_fpcc_board_price,
    download_cpc_fpcc_html,
)
from plugins.tools import sqlite_tools

db_path = Path("/opt/airflow/data/lpgprice.db")
# db_path = Path(R"D:\Projects\airflow-docker\data\lpgprice.db")

default_args = {
    "email": ["chienhua.hsu@tri.org.tw"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "monthly_lpg_price_update",
    description="Update the LPG price data monthly",
    schedule="30 8 2 * *",
    start_date=datetime(2024, 6, 15),
    catchup=False,
    tags=["monthly", "lpg"],
    default_args=default_args,
)


def update_lpg_price():
    counties_price = get_counties_price()
    towns_price = get_towns_price()

    cpc_adj_price = pd.DataFrame(get_cpc_price_adj())
    cpc_asia_price = pd.DataFrame(get_cpc_asia_price())
    cpc_board_price = pd.DataFrame(get_cpc_board_price())
    fpcc_board_price = pd.DataFrame(get_fpcc_board_price())

    # Save the data to SQLite
    sqlite_tools.save_data(
        db_path=db_path, table_name="Moea.County", data=counties_price
    )
    sqlite_tools.save_data(db_path=db_path, table_name="Moea.Town", data=towns_price)
    sqlite_tools.save_data(
        db_path=db_path, table_name="Cpc.Adjustment", data=cpc_adj_price
    )
    sqlite_tools.save_data(db_path=db_path, table_name="Cpc.Asia", data=cpc_asia_price)
    sqlite_tools.save_data(
        db_path=db_path, table_name="Cpc.Board", data=cpc_board_price
    )
    sqlite_tools.save_data(
        db_path=db_path, table_name="Fpcc.Board", data=fpcc_board_price
    )

    print("Finish")


t1 = PythonOperator(
    task_id="update_lpg_price",
    python_callable=update_lpg_price,
    dag=dag,
)


def download_html_files():
    download_cpc_fpcc_html("/opt/airflow/downloads")

    print("Finish")


t2 = PythonOperator(
    task_id="download_html_files",
    python_callable=download_html_files,
    dag=dag,
)

t1 >> t2
