from pathlib import Path
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from src.others.nera_accreditation_crawler import (
    get_update_date,
    get_pdf_link,
    download_pdf,
    # pdf_to_xlsx,
)
from src.tools.log import Log

down_pdf_path = Path("downloads/nera_accreditation/pdf")
down_csv_path = Path("downloads/nera_accreditation/csv")

save_folder = Path("downloads/nera_accreditation")
if not save_folder.exists():
    save_folder.mkdir(parents=True)
if not (save_folder / "pdf").exists():
    (save_folder / "pdf").mkdir(parents=True)
if not (save_folder / "xlsx").exists():
    (save_folder / "xlsx").mkdir(parents=True)


log_file = save_folder / "update.log"
if not log_file.exists():
    log_file.touch()


default_args = {
    "email": ["chienhua.hsu@tri.org.tw"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "daily_NERA_accreditation_update",
    description="Update the NERA accreditation daily",
    schedule="50 8 * * *",
    start_date=datetime(2025, 2, 6),
    catchup=False,
    tags=["daily", "other"],
    default_args=default_args,
)


def main():
    log = Log(log_file)
    update_time = get_update_date()
    year = update_time.split("-")[0]
    month = update_time.split("-")[1]
    day = update_time.split("-")[2]
    update_time = pendulum.datetime(
        int(year) + 1911, int(month), int(day), 0, 0, 0, tz="Asia/Taipei"
    ).format("YYYY-MM-DD HH:mm:ss")
    last_update_date = log.get_last_update_date()
    if update_time != last_update_date:
        log.write_update_info_to_logger("update", update_time)
        pdf_link = get_pdf_link()
        pdf_path = save_folder / "pdf" / f"許可項目機構總表_{update_time}.pdf"
        download_pdf(pdf_link, pdf_path)
        # xlsx_path = save_folder / "xlsx" / f"許可項目機構總表_{update_time}.xlsx"
        # pdf_to_xlsx(pdf_path, xlsx_path)
        return True
    else:
        log.write_update_info_to_logger("no update", update_time)
    return False


t1 = PythonOperator(
    task_id="daily_NERA_accreditation_update",
    python_callable=main,
    dag=dag,
)

t1
