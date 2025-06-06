import requests
from bs4 import BeautifulSoup
import pandas as pd
import pdfplumber


URL = "https://www.nera.gov.tw/zh-tw/LicenceInstitutionList.html"


def get_update_date() -> str:
    soup = BeautifulSoup(requests.get(URL, timeout=10).text, "html.parser")
    table = soup.find("table", {"class": "projectlist"})
    update_date = table.find("td", {"data-title": "更新日期"}).text

    return update_date


def get_pdf_link() -> str:
    soup = BeautifulSoup(requests.get(URL, timeout=10).text, "html.parser")
    table = soup.find("table", {"class": "projectlist"})
    download = table.find("td", {"data-title": "檔案下載"})
    pdf_link = download.find("a")["href"]
    return pdf_link


def download_pdf(pdf_link: str, download_path: str) -> str:
    with requests.get(pdf_link, stream=True, timeout=10) as response:
        response.raise_for_status()
        with open(download_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)


def process_table(_table: list) -> pd.DataFrame:
    _name = []
    _addr = []
    _tel = []
    _table = _table[0]
    for row in _table:
        if len(row) == 2:
            if row[1]:
                _code = row[0]
                _item = row[1]
                continue
            _name.append(row[0].split("\n")[0])
            _addr.append(row[0].split("\n")[1].split(" ")[0])
            _tel.append(row[0].split(": ")[1])
        if len(row) == 1:
            _code = None
            _item = None
            _name.append(row[0].split("\n")[0])
            _addr.append(row[0].split("\n")[1].split(" ")[0])
            _tel.append(row[0].split(": ")[1])
    return pd.DataFrame(
        {"Code": _code, "Item": _item, "Name": _name, "Address": _addr, "Tel": _tel}
    )


def pdf_to_xlsx(_pdf: str, save_path: str) -> pd.DataFrame:
    _pdf = pdfplumber.open(_pdf)
    _data = pd.DataFrame(columns=["Code", "Item", "Name", "Address", "Tel"])

    for page in _pdf.pages:
        table = page.extract_tables()
        if len(table) == 1:
            _data = pd.concat([_data, process_table(table)])
        else:
            _data = pd.DataFrame()
            for _t in table:
                _data = pd.concat([_data, process_table([_t])])

    _data.reset_index(drop=True, inplace=True)
    _data["Code"] = _data["Code"].ffill()
    _data["Item"] = _data["Item"].ffill()
    _data["Code"] = _data["Code"].str.replace("代碼:\n", "")
    _data["Item"] = _data["Item"].str.replace("\n", "")
    _data["Name"] = _data["Name"].str.replace("檢驗室名稱:", "")
    _data["Address"] = _data["Address"].str.replace("檢驗室地址:", "")
    _data["Tel"] = _data["Tel"].str.replace("\n", "")

    _data.to_excel(save_path, index=False)
