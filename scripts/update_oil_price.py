# %%
import sys
from pathlib import Path
import pandas as pd

sys.path.append(R"D:\Projects\airflow-docker")
from plugins.oil_price.cpc_fpcc import CpcPrice, FpccPrice
from plugins.oil_price.moea import CrudeOilPrice, AvgPrice, TownPrice, RefPrice
from plugins.tools import sqlite_tools

db_path = Path(R"Y:\0  資料庫\0  自動更新資料\2. 油價\oilprice.db")


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


def update_moae_oil_price(week_id: int = None):
    crude_oil_price = CrudeOilPrice().get_weekly_data(week_id)
    avg_price = AvgPrice().get_weekly_data(week_id)
    town_price = TownPrice().get_weekly_data(week_id)
    ref_price = RefPrice().get_weekly_data(week_id)

    # Save the data to SQLite
    sqlite_tools.save_data(
        db_path=db_path, table_name="Weekly.CrudeOil", data=crude_oil_price
    )
    sqlite_tools.save_data(db_path=db_path, table_name="Weekly.Avg", data=avg_price)
    sqlite_tools.save_data(db_path=db_path, table_name="Weekly.Town", data=town_price)
    sqlite_tools.save_data(db_path=db_path, table_name="Weekly.Ref", data=ref_price)

    print("Finish")


def update_daily_crude_oil_price(start: str, end: str):
    crude_oil_price = CrudeOilPrice().get_daily_data(start, end)
    sqlite_tools.save_data(
        db_path=db_path, table_name="Daily.CrudeOil", data=crude_oil_price
    )

    print("Finish")


# %%
update_daily_crude_oil_price("2024/06/25","2024/07/21")
# %%
