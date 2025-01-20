import sqlite3
import requests
import pandas as pd
from plugins.oil_price.moea import CrudeOilPrice

db_path = "data/oilprice.db"


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

    if info["is_duplicated"]:
        with sqlite3.connect(db_path) as conn:
            sql = """
            DELETE FROM 'Daily.CrudeOil' \
            WHERE rowid NOT IN (SELECT MIN(rowid) \
            FROM 'Daily.CrudeOil' GROUP BY date)
            """
            conn.execute(sql)

        print("Remove duplicated rows")

    if info["missing_date"]:
        crude_oil_price = CrudeOilPrice()
        insert_data = pd.DataFrame()
        for date in info["missing_date"]:
            price: pd.DataFrame = crude_oil_price.get_daily_data(
                start_date=date, end_date=date
            )
            insert_data = pd.concat([insert_data, price])

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        insert_data.to_sql("temp_1", conn, if_exists="replace", index=False)
        cursor.execute("INSERT INTO 'Daily.CrudeOil' SELECT * FROM 'temp'")
        cursor.execute("DROP TABLE 'temp_1'")
        cursor.execute(
            "CREATE TABLE 'temp_2' AS SELECT * FROM 'Daily.CrudeOil' ORDER BY date"
        )
        cursor.execute("DROP TABLE 'Daily.CrudeOil'")
        cursor.execute("ALTER TABLE 'temp_2' RENAME TO 'Daily.CrudeOil'")
        conn.commit()
        conn.close()

        print(f"Insert {len(insert_data)} rows")


def main():
    check_info = check_daily_crude_oil(db_path)
    fix_daily_crude_oil(db_path, check_info)
    with sqlite3.connect(db_path) as conn:
        conn.execute("VACUUM")


if __name__ == "__main__":
    main()
