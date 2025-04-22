import shutil
from pathlib import Path
import tkinter as tk
from tkinter import messagebox

lpg_price_db = Path().cwd() / "data" / "oilprice.db"
dst = Path(R"Y:\0  資料庫\0  自動更新資料\3. LPG價格_每月")


def copy_oilprice_db() -> None:
    print("Copying lpgprice.db...")
    shutil.copy(lpg_price_db, dst / "lpgprice.db")
    print("lpgprice.db copied.")


def show_message(message: str) -> None:
    root = tk.Tk()
    root.withdraw()
    messagebox.showinfo("Message", message)


def main():
    copy_oilprice_db()
    show_message("Success")


if __name__ == "__main__":
    main()
