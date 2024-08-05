import os
import re
import json
import csv
import zipfile
import sqlite3
import shutil
import tempfile
from pathlib import Path
from datetime import datetime
import requests


class TaxDataCrawler:
    def __init__(self) -> None:
        with open(Path(__file__).parent / "config.json", "r", encoding="utf-8") as f:
            self.conf = json.load(f)

    def get_file_info(self, url: str) -> dict[str, str]:
        try:
            response = requests.head(url, allow_redirects=True, timeout=5)
            response.raise_for_status()

            # file type
            if content_type := response.headers.get("Content-Type"):
                file_type = content_type.split("/")[-1]
                if re.match("octet-stream;charset=utf-8", content_type):
                    file_type = "csv"
            else:
                file_type = url.split(".")[-1]

            # file name
            if content_disposition := response.headers.get("Content-Disposition"):
                file_name = re.findall("filename=(.+)", content_disposition)
                if file_name:
                    file_name = file_name[0].strip('"')
            else:
                file_name = url.split("/")[-1]

            # file size
            if content_length := response.headers.get("Content-Length"):
                file_size = int(content_length)
            else:
                file_size = None

            return {
                "file_type": file_type,
                "file_name": file_name,
                "file_size": f"{file_size} bytes" if file_size else None,
            }
        except requests.RequestException as e:
            print(f"Error: {e}")
            return {}

    def download_file(self, url: str, download_dir: str) -> None:
        file_info = self.get_file_info(url)
        try:
            if file_info:
                response = requests.get(url, timeout=15)
                response.raise_for_status()
                file_path = Path(download_dir) / file_info["file_name"]
                with open(file_path, "wb") as f:
                    f.write(response.content)
                print(f"{file_path} download completed")
                if file_info["file_type"] == "zip":
                    with zipfile.ZipFile(file_path, "r") as zip_ref:
                        zip_ref.extractall(download_dir)
                    print(f"{file_path} extracted")
            else:
                print("File info not found")
        except requests.RequestException as e:
            print(f"Error: {e}")

    def copy_sqlite(self, db_dir: str) -> str:
        source = Path(__file__).parent / "example_tax.db"
        today = datetime.today().strftime("%Y%m%d")
        db_name = f"{today}_tax.db"
        shutil.copy(source, Path(db_dir) / db_name)
        return str(Path(db_dir) / db_name)

    def process_csv(self, csv_file_path: str) -> None:
        csv_dir = Path(csv_file_path).parent
        temp_dir, temp_path = tempfile.mkstemp(dir=csv_dir, text=True)
        with os.fdopen(temp_dir, "w", newline="", encoding="utf-8") as temp_file, open(
            csv_file_path, "r", newline="", encoding="utf-8"
        ) as csv_file:
            reader = csv.reader(csv_file)
            writer = csv.writer(temp_file)
            writer.writerow(next(reader))  # write header
            second_row = next(reader)
            if second_row and second_row[-1].strip() != "":
                writer.writerow(second_row)
            for row in reader:
                writer.writerow(row)
        shutil.move(temp_path, csv_file_path)
        os.remove(temp_path)
        print(f"{csv_file_path} processed")


def download_tax_data(download_dir: str) -> Path:
    today = datetime.today()
    download_dir_child = Path(download_dir) / "tax" / f"{today.year}{today.month:02d}"
    if not download_dir_child.exists():
        download_dir_child.mkdir(parents=True)
    crawler = TaxDataCrawler()
    for i in crawler.conf:
        crawler.download_file(crawler.conf[i]["file_url"], download_dir_child)
    for file in download_dir_child.iterdir():
        if (file.name in crawler.conf.keys()) and (file.suffix == ".csv"):
            crawler.process_csv(file)
    return download_dir_child


def write_to_tax_db(db_dir: str, file_dir: str) -> None:
    crawler = TaxDataCrawler()
    db_path = crawler.copy_sqlite(db_dir)
    for file in Path(file_dir).iterdir():
        if (file.stem in crawler.conf.keys()) and (file.suffix == ".csv"):
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                with open(file, "r", newline="", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    columns = next(reader)
                    cursor.executemany(
                        f"INSERT INTO '{crawler.conf[file.stem]['table_name']}' VALUES ({', '.join(['?' for _ in columns])})",
                        reader,
                    )
                    conn.commit()
            print(f"{file} {crawler.conf[file.stem]["table_name"]} saved to database")


def download_tax_data_and_write_to_db(download_dir: str, db_dir: str) -> None:
    download_dir_child = download_tax_data(download_dir)
    write_to_tax_db(db_dir, download_dir_child)
