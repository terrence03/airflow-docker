import sys
import sqlite3
import requests
import pandas as pd

sys.path.append(R"e:\OneDrive\WORK\Projects\airflow-docker")
from plugins.oil_price.moea import CrudeOilPrice
from plugins.oil_price.moea import TownPrice

db_path = R"E:\OneDrive\WORK\Projects\oil-market-report\data\oilprice.db"


def check_daily_crude_oil(db_path) -> dict:
    """check Daily.CrudeOil table"""
    result = {}

    with sqlite3.connect(db_path) as conn:
        sql = "SELECT * FROM 'Daily.CrudeOil'"
        df = pd.read_sql(sql, conn)

    # check if the date is unique
    if df["date"].nunique() != len(df):
        result["is_duplicated"] = True
    else:
        result["is_duplicated"] = False

    # check date missing
    url = "https://www2.moeaea.gov.tw/oil111/CrudeOil/GetRangePrice"
    payload = {
        "unit": "day",
        "start": "2024/01/01",
        "end": df["date"].max().replace("-", "/"),
    }
    r = requests.post(
        url,
        data=payload,
        headers={"User-Agent": "Mozilla/5.0"},
    )

    r.raise_for_status()
    data = pd.DataFrame(r.json()["data"]["crudeoil"])
    data["SurDate"] = data["SurDate"].apply(lambda x: x.replace("/", "-"))
    missing_date = data[~data["SurDate"].isin(df["date"])]["SurDate"].tolist()

    if len(missing_date) == 0:
        result["missing_date"] = None
    else:
        result["missing_date"] = [x.replace("-", "/") for x in missing_date]

    return result


def fix_daily_crude_oil(db_path, info: dict):
    if all([not info["is_duplicated"], info["missing_date"] is None]):
        print("No need to fix")
        return

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql("SELECT * FROM 'Daily.CrudeOil'", conn)

        if info["missing_date"]:
            crude_oil_price = CrudeOilPrice()
            insert_data = pd.DataFrame()
            for date in info["missing_date"]:
                price: pd.DataFrame = crude_oil_price.get_daily_data(
                    start_date=date, end_date=date
                )
                # fix price column dtype
                price["date"] = price["date"].astype(str)
                price["wti"] = price["wti"].astype(float)
                price["brent"] = price["brent"].astype(float)
                price["dubai"] = price["dubai"].astype(float)
                price["trade"] = price["trade"].astype(float)
                insert_data = pd.concat([insert_data, price])
            df = pd.concat([df, insert_data])
            print(f"Insert {len(insert_data)} rows")

        if info["is_duplicated"]:
            df = df.drop_duplicates(subset=["date"])
            print("Remove duplicated rows")

        df.sort_values("date", inplace=True)
        df.to_sql("Daily.CrudeOil", conn, if_exists="replace", index=False)


# def main():
#     check_info = check_daily_crude_oil(db_path)
#     fix_daily_crude_oil(db_path, check_info)
#     with sqlite3.connect(db_path) as conn:
#         conn.execute("VACUUM")


# if __name__ == "__main__":
#     main()


df = TownPrice().get_weekly_data(1309)
with sqlite3.connect(db_path) as conn:
    df.to_sql("Weekly.Town", conn, if_exists="append", index=False)