import re
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup


URL = {
    "CPC_ADJ": {
        "url": "https://vipmbr.cpc.com.tw/mbwebs/LPGPriceChart.aspx",
        "name": "台灣中油公司-液化石油氣價格調幅比較及吸收金額表",
    },
    "CPC_ASIA": {
        "url": "https://vipmbr.cpc.com.tw/mbwebs/LPGCompareChart.aspx",
        "name": "台灣中油公司-中油與亞鄰各國價格比較表",
    },
    "CPC_BOARD": {
        "url": "https://vipmbr.cpc.com.tw/mbwebs/ShowListPrice.aspx?prodtype=R1",
        "name": "台灣中油公司-液化石油氣牌價表",
    },
    "FPCC_BOARD": {
        "url": "http://www.fpcc.com.tw/tw/price",
        "name": "台塑石化公司-價格資訊",
    },
}


def get_soup(url: str) -> BeautifulSoup:
    """
    Get the BeautifulSoup object from the given url

    Parameters
    ----------
    url: str
        The url of the web page

    Returns
    -------
    BeautifulSoup
        The BeautifulSoup object of the web page
    """
    html = requests.get(url, timeout=10, verify=True)
    return BeautifulSoup(html.text, "html.parser")


def get_cpc_price_adj() -> list[dict]:
    """
    Get the latest LPG price adjustment data from CPC website

    Returns
    -------
    list[dict]
        The latest LPG price adjustment data
    """
    soup = get_soup(URL["CPC_ADJ"]["url"])
    update_date = soup.select_one("#lbl_date").text
    update_date = datetime.strptime(update_date, r"%Y/%m/%d")

    id_dict = {
        "lbl_aCP": "cp_price",
        "lbl_aFre": "fee",
        "lbl_aRate": "ex",
        "lbl_bPrice": "original_price",
        "lbl_aCPrice": "processed_price",
        "lbl_abcPrice": "processed_taxed_price",
        "lbl_aajustPirce": "adjusted_price",  # 這裡是網頁把Price寫成Pirce
        "lbl_arealPrice": "change",
        "lbl_aabsorb": "absorption",
        "lbl_acabsorb": "accumulation",
        "lbl_aindex": "adjustment_index",
    }

    data = {}
    data["year"], data["month"] = update_date.year, update_date.month
    for i, name in id_dict.items():
        value = soup.select_one(f"#{i}").text
        try:
            data[name] = float(value)
        except ValueError:
            data[name] = value
    return [data]


def get_cpc_asia_price() -> list[dict]:
    """
    Get the latest LPG price in Asia from CPC website

    Returns
    -------
    list[dict]
        The latest LPG price in Asia
    """
    soup = get_soup(URL["CPC_ASIA"]["url"])

    update_date = soup.select_one("#lbl_date").text
    update_date = datetime.strptime(update_date, r"%Y/%m/%d")

    data = [
        {
            "year": update_date.year,
            "month": update_date.month,
            "country": "TW",
            "before_tax": float(soup.select_one("#lbl_bTw").text),
            "after_tax": float(soup.select_one("#lbl_aTw").text),
        },
        {
            "year": update_date.year,
            "month": update_date.month,
            "country": "KR",
            "before_tax": float(soup.select_one("#lbl_bkn").text),
            "after_tax": float(soup.select_one("#lbl_akn").text),
        },
        {
            "year": update_date.year,
            "month": update_date.month,
            "country": "JP",
            "before_tax": float(soup.select_one("#lbl_bjp").text),
            "after_tax": float(soup.select_one("#lbl_ajp").text),
        },
    ]

    return data


def get_cpc_board_price() -> list[dict]:
    """
    Get the latest LPG board price from CPC website

    Returns
    -------
    list[dict]
        The latest LPG board price from CPC website
    """
    soup = get_soup(URL["CPC_BOARD"]["url"])

    update_date = soup.select_one("#Label_Date").text.split(":")[1]
    update_date = datetime.strptime(update_date, r"%Y/%m/%d")

    table = pd.read_html(URL["CPC_BOARD"]["url"], encoding="utf-8")[1]
    table.rename(
        columns={
            "產品編號": "product",
            "產品名稱": "name",
            "包裝": "package",
            "銷售對象": "sales",
            "交貨地點": "location",
            "計價單位": "unit",
            "參考牌價": "price",
            "營業稅": "vat",
            "貨物稅": "commodity_tax",
            "備註": "note",
        },
        inplace=True,
    )
    table.drop(columns=["package", "unit"], inplace=True)
    table["vat"] = table["vat"].str.strip("%").astype(float) / 100
    table["commodity_tax"] = table["commodity_tax"].str.strip("元/公斤").astype(float)
    table.insert(0, "year", update_date.year)
    table.insert(1, "month", update_date.month)
    data = table.to_dict(orient="records")
    return data


def get_fpcc_board_price() -> list[dict]:
    """
    Get the latest LPG price from FPCC website

    Returns
    -------
    list[dict]
        The latest LPG price from FPCC website
    """
    soup = get_soup(URL["FPCC_BOARD"]["url"])
    block = soup.find_all("div", attrs={"class": "price-block"})[0]

    update_date = block.find("p").get_text("\n", "<br>").split(" ")[1]
    update_date = datetime.strptime(update_date, r"%Y年%m月%d日")
    price1 = float(block.find_all("h2")[0].text.strip("$"))  # 家庭用混合丙丁烷售價
    price2 = float(block.find_all("h2")[1].text.strip("$"))  # 工業用丙烷售價
    location = block.find_all("p")[4].text.split("：")[1].strip("\r")
    business_tax = (
        float(re.findall(r"營業稅（(.*)%）", block.find_all("p")[5].text)[0]) / 100
    )
    commodity_tax = float(
        re.findall(r"貨物稅（(.*) 元/公斤）", block.find_all("p")[5].text)[0]
    )
    data = {
        "year": update_date.year,
        "month": update_date.month,
        "household": price1,
        "industry": price2,
        "location": location,
        "business_tax": business_tax,
        "commodity_tax": commodity_tax,
    }
    return [data]


def download_html(url: str, download_dir: str) -> None:
    """
    Download the html file from the given url

    Parameters
    ----------
    url: str
        The url of the web page
    download_dir: str
        The directory to save the html file
    """
    html = requests.get(url, timeout=10, verify=True)
    with open(Path(download_dir), "w", encoding="utf-8") as f:
        f.write(html.text)


def download_cpc_fpcc_html(download_dir: str) -> None:
    """
    Download the html file from CPC and FPCC website

    Parameters
    ----------
    download_dir: str
        The directory to save the html file
    """
    today = datetime.today()
    download_dir_child = Path(download_dir) / "lpg_source_html" /f"{today.year}{today.month:02d}"
    if not download_dir_child.exists():
        download_dir_child.mkdir(parents=True)
    download_html(
        URL["CPC_ADJ"]["url"], f"{download_dir_child}/{URL['CPC_ADJ']['name']}.html"
    )
    download_html(
        URL["CPC_ASIA"]["url"],
        f"{download_dir_child}/{URL['CPC_ASIA']['name']}.html",
    )
    download_html(
        URL["CPC_BOARD"]["url"],
        f"{download_dir_child}/{URL['CPC_BOARD']['name']}.html",
    )
    download_html(
        URL["FPCC_BOARD"]["url"],
        f"{download_dir_child}/{URL['FPCC_BOARD']['name']}.html",
    )
