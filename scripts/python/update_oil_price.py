# %%
import sys
from datetime import timedelta
import pandas as pd

sys.path.append(R"D:\OneDrive\WORK\Projects\airflow-docker")
from plugins.oil_price.cpc_fpcc import CpcPrice, FpccPrice
from plugins.oil_price.moea import CrudeOilPrice, AvgPrice, TownPrice, RefPrice
from plugins.tools import sqlite_tools
from plugins.tools.period_config import Week


def update_weekly_cpc_fpcc_oil_price(db_path: str):
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


def update_weekly_moae_oil_price(db_path, week_id: int = None):
    crude_oil_price = CrudeOilPrice().get_weekly_data(week_id)
    avg_price = AvgPrice().get_weekly_data(week_id)
    town_price = TownPrice().get_weekly_data(week_id)
    ref_price = RefPrice().get_weekly_data(
        *list(
            map(
                lambda x: (x + timedelta(days=1)).strftime("%Y/%m/%d"),
                Week().id_to_date(week_id),
            )
        )
    )

    sqlite_tools.save_data(
        db_path=db_path, table_name="Weekly.CrudeOil", data=crude_oil_price
    )
    sqlite_tools.save_data(db_path=db_path, table_name="Weekly.Avg", data=avg_price)
    sqlite_tools.save_data(db_path=db_path, table_name="Weekly.Town", data=town_price)
    sqlite_tools.save_data(db_path=db_path, table_name="Weekly.Ref", data=ref_price)

    print("Finish")


def update_weekly_ref_oil_price(db_path, start: str = None, end: str = None):
    ref_price = RefPrice().get_weekly_data(start, end)
    sqlite_tools.save_data(db_path=db_path, table_name="Weekly.Ref", data=ref_price)

    print("Finish")


def update_daily_crude_oil_price(db_path, start: str = None, end: str = None):
    crude_oil_price = CrudeOilPrice().get_daily_data(start, end)
    sqlite_tools.save_data(
        db_path=db_path, table_name="Daily.CrudeOil", data=crude_oil_price
    )

    print("Finish")


def update_weekly_crude_oil_price(db_path, start: int, end: int):
    crude_oil_price = CrudeOilPrice().get_weekly_data(start, end)
    sqlite_tools.save_data(
        db_path=db_path, table_name="Weekly.CrudeOil", data=crude_oil_price
    )

    print("Finish")


# %%
# db_path = Path(R"D:\OneDrive\WORK\Projects\monthly-oil-market-report\data\oilprice.db")

# update_daily_crude_oil_price(db_path, "2024/12/04", "2024/12/08")
# update_weekly_crude_oil_price(db_path, 1290, 1290)
# update_weekly_moae_oil_price(db_path, 1292)
# update_weekly_ref_oil_price(db_path, "2024/09/23")
# %%
