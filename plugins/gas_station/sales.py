# %%
import re
import random
from pathlib import Path
import requests
from requests import Session
from requests.models import Response
import pandas as pd
from bs4 import BeautifulSoup


class Crawler:
    def __init__(self):
        self.url = "https://www.moeaea.gov.tw/ECW/populace/content/wfrmStatistics.aspx?type=2&menu_id=1300"
        self.session = Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0"
        }
        self.payload = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": "",
            "__VIEWSTATEGENERATOR": "",
            "__SCROLLPOSITIONX": "0",
            "__SCROLLPOSITIONY": "0",
            "__EVENTVALIDATION": "",
            "ctl00$ddlType": "f635b8cf2ce78448c",  # 搜尋類別: 全站搜尋
            "ctl00$txtQuery": "",
            "ctl00$holderContent$ddlQ_Year": "",
            "ctl00$holderContent$txtQ_Statistic_Name": "請輸入表名關鍵字...",
            "ctl00$holderContent$uctlQ_PageSize$ddlQ_PageSize": "10",
            "ctl00$holderContent$btnQuery.x": "20",  # 0-44 滑鼠點擊位置
            "ctl00$holderContent$btnQuery.y": "10",  # 0-20
        }
        self.__session_get()

        self.year_options = self.__get_year_options()

    def __get_year_options(self) -> list[str]:
        res = requests.get(self.url, headers=self.headers)
        soup = BeautifulSoup(res.text, "html.parser")
        year_options = soup.find("select", id="ctl00_holderContent_ddlQ_Year")
        return [
            option["value"]
            for option in year_options.find_all("option")
            if option["value"]
        ]

    def __get_asp_net_data(self, res: Response) -> dict[str, str]:
        soup = BeautifulSoup(res.text, "html.parser")
        data = {
            "__VIEWSTATE": soup.find("input", id="__VIEWSTATE")["value"],
            "__VIEWSTATEGENERATOR": soup.find("input", id="__VIEWSTATEGENERATOR")[
                "value"
            ],
            "__EVENTVALIDATION": soup.find("input", id="__EVENTVALIDATION")["value"],
        }
        return data

    def __gen_btn_x_y(self) -> dict[str, str]:
        return {
            "ctl00$holderContent$btnQuery.x": str(random.randint(0, 44)),
            "ctl00$holderContent$btnQuery.y": str(random.randint(0, 20)),
        }

    def __update_payload(self, res: Response) -> None:
        asp_net_data = self.__get_asp_net_data(res)
        x_y = self.__gen_btn_x_y()
        self.payload.update(asp_net_data)
        self.payload.update(x_y)

    def __session_get(self) -> Response:
        if response := self.session.get(
            self.url,
            timeout=10,
            headers=self.headers,
        ):
            self.__update_payload(response)
            return response

    def __session_post(self) -> Response:
        data = self.payload
        if response := requests.post(
            self.url,
            data=data,
            timeout=10,
            headers=self.headers,
        ):
            self.__update_payload(response)
            return response

    def __find_links(self, res: Response) -> dict[str, str]:
        soup = BeautifulSoup(res.text, "html.parser")
        table = soup.find("table", id="ctl00_holderContent_grdStatistics")
        row_data = table.find_all("a")
        names = [
            a["title"].split(" ")[1] for a in row_data if "serial_no=2" in a["href"]
        ]
        links = [a["href"] for a in row_data if "serial_no=2" in a["href"]]
        return dict(zip(names, links))

    def get_links(self) -> dict[str, str]:
        res = self.__session_get()
        return self.__find_links(res)

    def get_link_by_year_month(self, year: int, month: int) -> str:
        p = f"{year-1911}-{month:02d}"  # AD to ROC
        if p in self.year_options:
            self.payload.update({"ctl00$holderContent$ddlQ_Year": p})
            res = self.__session_post()
            return self.__find_links(res)
        else:
            raise ValueError(f"year: {year}, month: {month} has no data")

    def __download(self, url: str, name: str) -> None:
        r = requests.get(
            f"https://www.moeaea.gov.tw/ECW/populace/content/{url}",
            allow_redirects=True,
            timeout=10,
            headers=self.headers,
        )
        with open(name, "wb") as f:
            f.write(r.content)

    def download_latest(self, download_dir: str) -> None:
        last_link = list(self.get_links().items())[0]
        self.__download(last_link[1], Path(download_dir) / last_link[0])

    def download_by_year_month(self, year: int, month: int, download_dir: str) -> None:
        last_link = list(self.get_link_by_year_month(year, month).items())[0]
        self.__download(last_link[1], Path(download_dir) / last_link[0])


# %%
class DataFlow:
    def __init__(self) -> None:
        pass

    def read_xlsx_a(self, file_path: str) -> pd.DataFrame:
        return pd.read_excel(
            file_path,
            sheet_name="銷售統計表",
            skiprows=3,
            skipfooter=1,
            usecols=[0, 1, 2, 3, 4, 5],
        )

    def read_xlsx_b(self, file_path: str) -> pd.DataFrame:
        return pd.read_excel(
            file_path,
            sheet_name="銷售分析表",
            skiprows=4,
            skipfooter=1,
            usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        )

    def process_file(self, file_path: str) -> tuple[pd.DataFrame]:
        """讀取檔案"""
        year = int(re.search(r"(\d+)年", file_path).group(1)) + 1911
        month = int(re.search(r"(\d+)月份", file_path).group(1))
        try:
            data_1 = pd.read_excel(
                file_path,
                sheet_name="銷售統計表",
                skiprows=3,
                skipfooter=1,
                usecols=[0, 1, 2, 3, 4, 5],
            )
            data_1.insert(0, "年", year)
            data_1.insert(1, "月", month)
            data_1 = data_1[data_1["縣市別"] != "合　計"]
            data_1.columns = [
                "年",
                "月",
                "縣市別",
                "站數",
                "汽油",
                "柴油",
                "合計",
                "公秉／日‧站",
            ]

            data_2 = pd.read_excel(
                file_path,
                sheet_name="銷售分析表",
                skiprows=4,
                skipfooter=1,
                usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            )
            data_2.rename(columns={"Unnamed: 0": "縣市別"}, inplace=True)
            data_2.insert(0, "年", year)
            data_2.insert(1, "月", month)
            data_2 = data_2[data_2["縣市別"] != "合　計"]
            data_2.columns = [
                "年",
                "月",
                "縣市別",
                "5以下",
                "5.1~10",
                "10.1~15",
                "15.1~20",
                "20.1~25",
                "25.1~30",
                "30以上",
                "合計",
            ]

            return data_1, data_2
        except ValueError:
            print(f"{file_path}資料有誤")


# %%
def download_file() -> tuple[list]:
    """Download excel file from url"""
    save_dir = Path(
        R"Y:\0  資料庫\0  自動更新資料\5. 各縣市汽車加油站汽柴油銷售統計表_每月"
    )
    new_file_urls, new_file_names = find_new_file_urls_and_names()

    if not new_file_urls:
        print("無新檔案")
        return

    print(f"發現{len(new_file_urls)}個新檔案")
    print("開始下載檔案")
    for file_url, file_name in zip(new_file_urls, new_file_names):
        r = requests.get(
            f"https://www.moeaea.gov.tw/ECW/populace/content/{file_url}",
            allow_redirects=True,
            timeout=10,
        )
        with open(save_dir / file_name, "wb") as f:
            f.write(r.content)
    print(f"檔案已下載至: '{save_dir}'")
