import re
from io import StringIO
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup


class CpcPrice:
    def __init__(self) -> None:
        self.url = {
            "compare": "https://vipmbr.cpc.com.tw/mbwebs/oilCompareChart.aspx",
            "adjustment": "https://vipmbr.cpc.com.tw/mbwebs/ShowPriceAjust.aspx",
            "asia": "https://vipmbr.cpc.com.tw/mbwebs/oilContryCompare.aspx",
            "board": "https://vipmbr.cpc.com.tw/mbwebs/showlistprice.aspx",
        }

    def get_soup(self, url: str) -> BeautifulSoup:
        """
        Get the BeautifulSoup object from the given url

        Parameters
        ----------
        url: str
            The url of the web page

        Returns
        -------
        BeautifulSoup
        """
        html = requests.get(url, timeout=10, verify=True)
        return BeautifulSoup(html.text, "html.parser")

    def get_board_price(self) -> list[dict]:
        soup = self.get_soup(self.url["board"])

        date_pattern = re.compile(r"\d{4}/\d{2}/\d{2}")
        update_date = soup.select_one("#Label_Date").text
        update_date = date_pattern.search(update_date).group()
        update_date = datetime.strptime(update_date, r"%Y/%m/%d").strftime(r"%Y-%m-%d")

        table = soup.find("table", {"id": "MyGridView"})
        data = pd.read_html(StringIO(str(table)))[0]
        data.rename(
            columns={
                "產品編號": "product_id",
                "產品名稱": "product_name",
                "包裝": "package",
                "銷售對象": "sale",
                "交貨地點": "delivery",
                "計價單位": "unit",
                "參考牌價": "price",
                "營業稅": "vat",
                "貨物稅": "commodity_tax",
                "備註": "note",
            },
            inplace=True,
        )
        data.insert(0, "update_date", update_date)
        return data.to_dict(orient="records")

    def get_asia_price(self) -> list[dict]:
        soup = self.get_soup(self.url["asia"])
        update_date = soup.select_one("#lbl_date").text
        update_date = datetime.strptime(update_date, r"%Y/%m/%d").strftime(r"%Y-%m-%d")

        table1 = soup.find("table", {"id": "MyGridView"})
        table2 = soup.find("table", {"id": "MyGridView0"})
        data1 = pd.read_html(StringIO(str(table1)))[0]
        data1.rename(
            columns={
                "Unnamed: 0": "country",
                "92汽油(元/公升)": "gasoline_92_before_tax",
                "95汽油(元/公升)": "gasoline_95_before_tax",
                "超柴(元/公升)": "diesel_before_tax",
            },
            inplace=True,
        )
        data2 = pd.read_html(StringIO(str(table2)))[0]
        data2.rename(
            columns={
                "Unnamed: 0": "country",
                "92汽油(元/公升)": "gasoline_92_after_tax",
                "95汽油(元/公升)": "gasoline_95_after_tax",
                "超柴(元/公升)": "diesel_after_tax",
            },
            inplace=True,
        )

        data = pd.merge(
            data1,
            data2,
            on="country",
            how="inner",
        )
        data = data[
            [
                "country",
                "gasoline_92_before_tax",
                "gasoline_92_after_tax",
                "gasoline_95_before_tax",
                "gasoline_95_after_tax",
                "diesel_before_tax",
                "diesel_after_tax",
            ]
        ]
        data["update_date"] = update_date
        return data.to_dict(orient="records")

    def get_price_adj(self) -> list[dict]:
        soup = self.get_soup(self.url["adjustment"])
        table = soup.find_all("table")[1]
        rows = table.find_all("tr")[2:]
        last_week = soup.select_one("#ShowDate1").text
        last_week = datetime.strptime(last_week, r"%Y/%m/%d").strftime(r"%Y-%m-%d")
        this_week = soup.select_one("#ShowDate2").text
        this_week = datetime.strptime(this_week, r"%Y/%m/%d").strftime(r"%Y-%m-%d")

        cols = [
            "oil",
            "last_week_price",
            "this_week_price",
            "asian_price",
            "absorption",
            "adjustable_price",
            "adjusted",
            "retail_price",
        ]

        data = []
        for row in rows:
            row_data = {}
            row_data["last_week"] = last_week
            row_data["this_week"] = this_week
            for i, col in enumerate(row.find_all("th")):
                row_data[cols[i]] = col.text.replace("\n", "").replace("\xa0", "")
                try:
                    row_data[cols[i]] = float(row_data[cols[i]].replace(",", ""))
                except ValueError:
                    pass

            data.append(row_data)
        return data

    def get_price_compare(self) -> list[dict]:
        soup = self.get_soup(self.url["compare"])
        id_dict = {
            "前週": "last_week",
            "本週": "this_week",
            "前週Dubai": "last_week_7D3B",
            "本週Dubai": "this_week_7D3B",
            "前週匯率": "last_week_ex",
            "本週匯率": "this_week_ex",
            "指標油變動幅度": "change",
            "前週92": "last_week_gasoline_92",
            "本週92": "this_week_gasoline_92",
            "前週超柴": "last_week_diesel",
            "本週超柴": "this_week_diesel",
        }
        data = {}
        for key, value in id_dict.items():
            data[value] = soup.select_one(f"#{key}").text
            try:
                data[value] = float(data[value])
            except ValueError:
                pass
        data["last_week"] = (
            data["last_week"].replace("-", "~").replace("/", "-").replace(" ", "")
        )
        data["this_week"] = (
            data["this_week"].replace("-", "~").replace("/", "-").replace(" ", "")
        )
        data["change"] = str(data["change"]) + "%"
        return [data]


class FpccPrice:
    def __init__(self) -> None:
        self.url = "https://www.fpcc.com.tw/tw/price"

    def get_soup(self, url: str) -> BeautifulSoup:
        """
        Get the BeautifulSoup object from the given url

        Parameters
        ----------
        url: str
            The url of the web page

        Returns
        -------
        BeautifulSoup
        """
        html = requests.get(url, timeout=10, verify=True)
        return BeautifulSoup(html.text, "html.parser")

    def get_board_price(self) -> list[dict]:
        soup = self.get_soup(self.url)
        retail = soup.find_all("div", attrs={"class": "price-block"})[0]
        wholesale = soup.find_all("div", attrs={"class": "price-block"})[1]
        update_date = retail.find("p").get_text("\n", "<br>").split(" ")[1]
        update_date = datetime.strptime(update_date, r"%Y年%m月%d日").strftime(
            r"%Y-%m-%d"
        )

        retail_92 = float(retail.find_all("h2")[0].text.strip("$"))
        retail_95 = float(retail.find_all("h2")[1].text.strip("$"))
        retail_98 = float(retail.find_all("h2")[2].text.strip("$"))
        retail_diesel = float(retail.find_all("h2")[3].text.strip("$"))

        wholesale_92 = float(wholesale.find_all("h2")[0].text.strip("$"))
        wholesale_95 = float(wholesale.find_all("h2")[1].text.strip("$"))
        wholesale_98 = float(wholesale.find_all("h2")[2].text.strip("$"))
        wholesale_diesel = float(wholesale.find_all("h2")[3].text.strip("$"))

        data = [
            {
                "update_date": update_date,
                "sale": "加盟加油站",
                "gasoline_92": retail_92,
                "gasoline_95": retail_95,
                "gasoline_98": retail_98,
                "diesel": retail_diesel,
            },
            {
                "update_date": update_date,
                "sale": "油品批售價",
                "gasoline_92": wholesale_92,
                "gasoline_95": wholesale_95,
                "gasoline_98": wholesale_98,
                "diesel": wholesale_diesel,
            },
        ]
        return data
