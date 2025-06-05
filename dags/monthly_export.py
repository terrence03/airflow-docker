import sqlite3
from pathlib import Path
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "email": ["chienhua.hsu@tri.org.tw"],
    "email_on_failure": True,
}

dag = DAG(
    "monthly_oil_price_export",
    description="輸出每月油價資料",
    schedule="30 12 1 * *",
    start_date=datetime(2024, 6, 15),
    catchup=False,
    tags=["oil", "monthly"],
    default_args=default_args,
)


export_dir = Path("/opt/airflow/exports/")
oilprice_db = "/opt/airflow/data/oilprice.db"


def copy_oil_data():
    with sqlite3.connect(oilprice_db) as conn:
        # 每日原油價格
        daily_crude_oil = pd.read_sql_query("SELECT * FROM 'Daily.CrudeOil'", conn)
        daily_crude_oil.to_csv(export_dir / "1.每日原油價格.csv", index=False)

        # 每月原油價格
        monthly_crude_oil = pd.read_sql_query("SELECT * FROM 'Monthly.CrudeOil'", conn)
        monthly_crude_oil.to_csv(export_dir / "2.每月原油價格.csv", index=False)

        # 每月全國零售均價
        monthly_avg = pd.read_sql_query("SELECT * FROM 'Monthly.Avg'", conn)
        monthly_avg.to_csv(export_dir / "3.每月全國零售均價.csv", index=False)

        # 每月縣市零售均價
        monthly_county = pd.read_sql_query("SELECT * FROM 'Monthly.County'", conn)
        monthly_county.to_csv(export_dir / "4.每月縣市零售均價.csv", index=False)

        # 每週原油價格
        weekly_crude_oil = pd.read_sql_query("SELECT * FROM 'Weekly.CrudeOil'", conn)
        weekly_crude_oil.to_csv(export_dir / "5.每週原油價格.csv", index=False)

        # 每週零售參考價格
        weekly_ref = pd.read_sql_query("SELECT * FROM 'Weekly.Ref'", conn)
        weekly_ref.to_csv(export_dir / "6.每週零售參考價格.csv", index=False)

        # 每週全國零售均價
        weekly_avg = pd.read_sql_query("SELECT * FROM 'Weekly.Avg'", conn)
        weekly_avg.to_csv(export_dir / "7.每週全國零售均價.csv", index=False)

        # 每週鄉鎮市區零售均價
        weekly_town = pd.read_sql_query("SELECT * FROM 'Weekly.Town'", conn)
        weekly_town.to_csv(export_dir / "8.每週鄉鎮市區零售均價.csv", index=False)

        # 每週中油牌價
        weekly_cpc_board = pd.read_sql_query("SELECT * FROM 'Weekly.CpcBoard'", conn)
        weekly_cpc_board.to_csv(export_dir / "9.每週中油牌價.csv", index=False)

        # 每週中油調價
        weekly_cpc_adjustment = pd.read_sql_query(
            "SELECT * FROM 'Weekly.CpcAdjustment'", conn
        )
        weekly_cpc_adjustment.to_csv(export_dir / "10.每週中油調價.csv", index=False)

        # 每週中油比較
        weekly_cpc_compare = pd.read_sql_query(
            "SELECT * FROM 'Weekly.CpcCompare'", conn
        )
        weekly_cpc_compare.to_csv(export_dir / "11.每週中油比較.csv", index=False)

        # 每週亞鄰油價
        weekly_cpc_asia = pd.read_sql_query("SELECT * FROM 'Weekly.CpcAsia'", conn)
        weekly_cpc_asia.to_csv(export_dir / "12.每週亞鄰油價.csv", index=False)

        # 每週台塑石化牌價
        weekly_fpc_board = pd.read_sql_query("SELECT * FROM 'Weekly.FpccBoard'", conn)
        weekly_fpc_board.to_csv(export_dir / "13.每週台塑石化牌價.csv", index=False)


task_copy_oil_data = PythonOperator(
    task_id="copy_oil_data",
    python_callable=copy_oil_data,
    dag=dag,
)

task_copy_oil_data
