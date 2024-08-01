import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

sys.path.append("/opt/airflow")
from plugins.oil_price.cpc_fpcc import CpcPrice, FpccPrice
from plugins.oil_price.moea import CrudeOilPrice, AvgPrice, TownPrice, RefPrice
from plugins.tools import sqlite_tools

db_path = Path("/opt/airflow/data/oilprice.db")

default_args = {
    "email": ["chienhua.hsu@tri.org.tw"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "weekly_oil_price_update",
    description="Update the oil price data weekly",
    schedule="40 8 * * 3",
    start_date=datetime(2024, 6, 15),
    catchup=False,
    tags=["oil", "weekly"],
    default_args=default_args,
)


def update_cpc_fpcc_oil_price():
    cpc_price = CpcPrice()
    fpcc_price = FpccPrice()

    cpc_board_price = pd.DataFrame(cpc_price.get_board_price())
    cpc_asia_price = pd.DataFrame(cpc_price.get_asia_price())
    cpc_price_adj = pd.DataFrame(cpc_price.get_price_adj())
    cpc_price_compare = pd.DataFrame(cpc_price.get_price_compare())
    fpcc_board_price = pd.DataFrame(fpcc_price.get_board_price())

    # Save the data to SQLite
    sqlite_tools.save_data(
        db_path=db_path, table_name="Weekly.CpcBoard", data=cpc_board_price
    )
    sqlite_tools.save_data(
        db_path=db_path, table_name="Weekly.CpcAsia", data=cpc_asia_price
    )
    sqlite_tools.save_data(
        db_path=db_path, table_name="Weekly.CpcAdjustment", data=cpc_price_adj
    )
    sqlite_tools.save_data(
        db_path=db_path, table_name="Weekly.CpcCompare", data=cpc_price_compare
    )
    sqlite_tools.save_data(
        db_path=db_path, table_name="Weekly.FpccBoard", data=fpcc_board_price
    )

    print("Finish")


t1 = PythonOperator(
    task_id="weekly_update_cpc_and_fpcc_oil_price",
    python_callable=update_cpc_fpcc_oil_price,
    dag=dag,
    retries=3,
    retry_delay=timedelta(minutes=30),
)


def update_moae_oil_price():
    crude_oil_price = CrudeOilPrice().get_weekly_data()
    avg_price = AvgPrice().get_weekly_data()
    town_price = TownPrice().get_weekly_data()
    ref_price = RefPrice().get_weekly_data()

    # Save the data to SQLite
    sqlite_tools.save_data(
        db_path=db_path, table_name="Weekly.CrudeOil", data=crude_oil_price
    )
    sqlite_tools.save_data(db_path=db_path, table_name="Weekly.Avg", data=avg_price)
    sqlite_tools.save_data(db_path=db_path, table_name="Weekly.Town", data=town_price)
    sqlite_tools.save_data(db_path=db_path, table_name="Weekly.Ref", data=ref_price)

    print("Finish")


t2 = PythonOperator(
    task_id="weekly_update_moae_oil_price",
    python_callable=update_moae_oil_price,
    dag=dag,
    retries=3,
    retry_delay=timedelta(minutes=30),
)

t1 >> t2
