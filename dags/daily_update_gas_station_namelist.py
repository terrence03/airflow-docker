import os
import pandas as pd
import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from src.gas_station.crawler import CpcCrawler, FpccCrawler, MoeaCrawler
from src.tools.log import Log

cpc_log = Log("data/crawler_logs/gas_station_namelist_by_cpc.log")
fpcc_log = Log("data/crawler_logs/gas_station_namelist_by_fpcc.log")
moea_log = Log("data/crawler_logs/gas_station_namelist_by_moea.log")

cpc_download_dir = "downloads/gas_station_namelist/cpc"
fpcc_download_dir = "downloads/gas_station_namelist/fpcc"
moea_download_dir = "downloads/gas_station_namelist/moea"

default_args = {
    "email": ["chienhua.hsu@tri.org.tw"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


dag = DAG(
    "daily_update_gas_station_namelist",
    description="Update the gas station namelist by CPC FPCC and MOEA daily",
    schedule="35 8 * * *",
    start_date=pendulum.datetime(2024, 6, 15, tz="Asia/Taipei"),
    # schedule_interval=None,
    catchup=False,
    tags=["daily"],
    default_args=default_args,
)
# ==========CPC DAG==========


def check_cpc_update_by_namelist(new_station_ids: list) -> bool:
    if (not os.path.exists(cpc_download_dir)) or (
        len(os.listdir(cpc_download_dir)) == 0
    ):
        return True

    last_file = sorted([f for f in os.listdir(cpc_download_dir) if f.endswith(".csv")])[
        -1
    ]
    last_df = pd.read_csv(
        os.path.join(cpc_download_dir, last_file), encoding="utf-8-sig"
    )
    last_station_ids = set(last_df["station_id"].tolist())
    new_station_ids = set(new_station_ids)

    return last_station_ids != new_station_ids


def update_cpc() -> None:
    crawler = CpcCrawler()
    cpc_data = crawler.crawl()
    if check_cpc_update_by_namelist(cpc_data["station_id"].tolist()):
        today = pendulum.now("Asia/Taipei").format("YYYYMMDD")
        cpc_data.to_csv(
            os.path.join(cpc_download_dir, f"{today}_cpc.csv"),
            index=False,
            encoding="utf-8-sig",
        )
        cpc_log.write_update_info_to_logger(
            "update", pendulum.today("Asia/Taipei").format("YYYY-MM-DD HH:mm:ss")
        )
    else:
        cpc_log.write_update_info_to_logger(
            "no update", pendulum.today("Asia/Taipei").format("YYYY-MM-DD HH:mm:ss")
        )


t1 = PythonOperator(
    task_id="cpc_gas_station_namelist_update",
    python_callable=update_cpc,
    dag=dag,
)


# ==========FPCC DAG==========


def check_fpcc_update_by_namelist(new_station_ids: list) -> bool:
    if (not os.path.exists(fpcc_download_dir)) or (
        len(os.listdir(fpcc_download_dir)) == 0
    ):
        return True

    last_file = sorted(
        [f for f in os.listdir(fpcc_download_dir) if f.endswith(".csv")]
    )[-1]
    last_df = pd.read_csv(
        os.path.join(fpcc_download_dir, last_file), encoding="utf-8-sig"
    )
    last_station_ids = set(last_df["station_id"].tolist())
    new_station_ids = set(new_station_ids)

    return last_station_ids != new_station_ids


def update_fpcc() -> None:
    crawler = FpccCrawler()
    fpcc_data = crawler.crawl()
    if check_fpcc_update_by_namelist(fpcc_data["station_id"].tolist()):
        today = pendulum.now("Asia/Taipei").format("YYYYMMDD")
        fpcc_data.to_csv(
            os.path.join(fpcc_download_dir, f"{today}_cpc.csv"),
            index=False,
            encoding="utf-8-sig",
        )

        fpcc_log.write_update_info_to_logger(
            "update", pendulum.today("Asia/Taipei").format("YYYY-MM-DD HH:mm:ss")
        )
    else:
        fpcc_log.write_update_info_to_logger(
            "no update", pendulum.today("Asia/Taipei").format("YYYY-MM-DD HH:mm:ss")
        )


t2 = PythonOperator(
    task_id="fpcc_gas_station_namelist_update",
    python_callable=update_fpcc,
    dag=dag,
)

# ==========Moea DAG==========


def check_moea_update_by_update_time(update_time: str) -> bool:
    last_update_time = moea_log.get_last_update_date()
    if last_update_time == "":
        return True
    return update_time != last_update_time


def update_moea() -> None:
    crawler = MoeaCrawler()
    update_time, stations = crawler.crawl()
    if check_moea_update_by_update_time(update_time):
        today = pendulum.now("Asia/Taipei").format("YYYYMMDD")
        stations.to_csv(
            os.path.join(moea_download_dir, f"{today}_油價管理與分析系統.csv"),
            index=False,
            encoding="utf-8-sig",
        )
        moea_log.write_update_info_to_logger("update", update_time)
    else:
        moea_log.write_update_info_to_logger("no update", update_time)


t3 = PythonOperator(
    task_id="moea_gas_station_namelist_update",
    python_callable=update_moea,
    dag=dag,
)


t1 >> t2 >> t3
