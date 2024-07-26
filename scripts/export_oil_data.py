import shutil
import sqlite3
from pathlib import Path
import tkinter as tk
from tkinter import messagebox
import pandas as pd


oilprice_db = Path().cwd() / "data" / "oilprice.db"
oil_data_dst = Path(R"Y:\0  資料庫\0  自動更新資料\2. 油價")

db_tables = {
    "Daily.CrudeOil": "1.每日原油價格",
    "Monthly.CrudeOil": "2.每月原油價格",
    "Monthly.Avg": "3.每月全國零售均價",
    "Monthly.County": "4.每月縣市零售均價",
    "Weekly.CrudeOil": "5.每週原油價格",
    "Weekly.Ref": "6.每週零售參考價格",
    "Weekly.Avg": "7.每週全國零售均價",
    "Weekly.Town": "8.每週鄉鎮市區零售均價",
    "Weekly.CpcBoard": "9.每週中油牌價",
    "Weekly.CpcAdjustment": "10.每週中油調價",
    "Weekly.CpcCompare": "11.每週中油比較",
    "Weekly.CpcAsia": "12.每週亞鄰油價",
    "Weekly.FpccBoard": "13.每週台塑石化牌價",
}


def export_oil_db() -> None:
    with sqlite3.connect(oilprice_db) as conn:
        for table, name in db_tables.items():
            df = pd.read_sql(f"SELECT * FROM '{table}'", conn)
            df.to_csv(oil_data_dst / f"{name}.csv", index=False, encoding="utf-8-sig")
            print(f"{name} exported.")


def copy_oilprice_db() -> None:
    print("Copying oilprice.db...")
    shutil.copy(oilprice_db, oil_data_dst / "oilprice.db")
    print("oilprice.db copied.")


def show_message(message: str) -> None:
    root = tk.Tk()
    root.withdraw()
    messagebox.showinfo("Message", message)


def main():
    export_oil_db()
    copy_oilprice_db()
    show_message("Success")


if __name__ == "__main__":
    main()
