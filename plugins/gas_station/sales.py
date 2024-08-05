import re
import random
from pathlib import Path
import requests
from requests import Session
from requests.models import Response
import pandas as pd
import openpyxl
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

    def download_latest(self, download_dir: str) -> str:
        last_link = list(self.get_links().items())[0]
        download_path = Path(download_dir) / last_link[0]
        self.__download(last_link[1], download_path)
        return download_path.as_posix()

    def download_by_year_month(self, year: int, month: int, download_dir: str) -> str:
        last_link = list(self.get_link_by_year_month(year, month).items())[0]
        download_path = Path(download_dir) / last_link[0]
        self.__download(last_link[1], download_path)
        return download_path.as_posix()


class DataFlow:
    def __init__(self) -> None:
        pass

    def read_xlsx_a(self, file_path: str) -> pd.DataFrame:
        data = pd.read_excel(
            file_path,
            sheet_name="銷售統計表",
            skiprows=3,
            skipfooter=1,
            usecols=[0, 1, 2, 3, 4, 5],
        )
        year, month = self.__get_year_month(file_path)
        return self.__process_data_a(data, year, month)

    def read_xlsx_b(self, file_path: str) -> pd.DataFrame:
        data = pd.read_excel(
            file_path,
            sheet_name="銷售分析表",
            skiprows=4,
            skipfooter=1,
            usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        )
        year, month = self.__get_year_month(file_path)
        return self.__process_data_b(data, year, month)

    def __get_year_month(self, file_path: str) -> tuple[int, int]:
        wb = openpyxl.load_workbook(file_path, read_only=True)
        info = wb.active.cell(row=2, column=1).value
        info = re.findall(r"\d+", info)
        year = int(info[0]) + 1911
        month = int(info[1])
        return year, month

    def __process_data_a(
        self, data: pd.DataFrame, year: int, month: int
    ) -> pd.DataFrame:
        data.insert(0, "年", year)
        data.insert(1, "月", month)
        data = data[data["縣市別"] != "合　計"]
        data.columns = [
            "year",
            "month",
            "county",
            "station_num",
            "gasoline_sales",
            "diesel_sales",
            "total_sales",
            "L/day/station",
        ]
        return data

    def __process_data_b(
        self, data: pd.DataFrame, year: int, month: int
    ) -> pd.DataFrame:
        data.rename(columns={"Unnamed: 0": "縣市別"}, inplace=True)
        data.insert(0, "年", year)
        data.insert(1, "月", month)
        data = data[data["縣市別"] != "合　計"]
        data.columns = [
            "year",
            "month",
            "county",
            "<5",
            "5.1~10",
            "10.1~15",
            "15.1~20",
            "20.1~25",
            "25.1~30",
            "30.1~40",
            ">40.1",
            "total",
        ]
        return data
