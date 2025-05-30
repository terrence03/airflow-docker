import sys
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import pandas as pd
import pdfplumber

# sys.path.append("/opt/airflow")
from src.tools.log import Log

save_folder = Path("/opt/airflow/downloads/nera_accreditation")

if not save_folder.exists():
    save_folder.mkdir(parents=True)
if not (save_folder / "pdf").exists():
    (save_folder / "pdf").mkdir(parents=True)
if not (save_folder / "xlsx").exists():
    (save_folder / "xlsx").mkdir(parents=True)

log_file = save_folder / "update.log"
if not log_file.exists():
    log_file.touch()


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


def handle_table(_table: list) -> pd.DataFrame:
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


def handle_tables(_table: list) -> pd.DataFrame:
    result = pd.DataFrame()
    for _t in _table:
        result = pd.concat([result, handle_table([_t])])
    return result


def pdf_to_xlsx(_pdf: str, save_path: str) -> pd.DataFrame:
    _pdf = pdfplumber.open(_pdf)
    _data = pd.DataFrame(columns=["Code", "Item", "Name", "Address", "Tel"])

    for page in _pdf.pages:
        table = page.extract_tables()
        if len(table) == 1:
            _data = pd.concat([_data, handle_table(table)])
        else:
            _data = pd.concat([_data, handle_tables(table)])

    _data.reset_index(drop=True, inplace=True)
    _data["Code"] = _data["Code"].ffill()
    _data["Item"] = _data["Item"].ffill()
    _data["Code"] = _data["Code"].str.replace("代碼:\n", "")
    _data["Item"] = _data["Item"].str.replace("\n", "")
    _data["Name"] = _data["Name"].str.replace("檢驗室名稱:", "")
    _data["Address"] = _data["Address"].str.replace("檢驗室地址:", "")
    _data["Tel"] = _data["Tel"].str.replace("\n", "")

    _data.to_excel(save_path, index=False)


def main():
    log = Log(log_file)
    update_time = get_update_date()
    last_update_date = log.get_last_update_date()
    if update_time != last_update_date:
        log.write_update_info_to_logger(update_time, "update")
        pdf_link = get_pdf_link()
        pdf_path = save_folder / "pdf" / f"許可項目機構總表_{update_time}.pdf"
        download_pdf(pdf_link, pdf_path)
        xlsx_path = save_folder / "xlsx" / f"許可項目機構總表_{update_time}.xlsx"
        pdf_to_xlsx(pdf_path, xlsx_path)
        return True
    else:
        log.write_update_info_to_logger(update_time, "no update")
    return False


if __name__ == "__main__":
    main()
