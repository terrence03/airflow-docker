import sys
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd

sys.path.append("/opt/airflow")
from plugins.tools.log import Log

URL = "https://www2.moeaea.gov.tw/oil111/Dealer/GasStations/load"
now = datetime.now()
save_folder = Path("opt/ariflow/downloads/gas_station_namelist")
log_file = save_folder / "update.log"


def get_data() -> tuple[str, pd.DataFrame]:
    """
    Get the gas station data

    Returns
    -------
    dict
        The gas station data
    """
    response = requests.post(
        URL,
        headers={"User-Agent": "Mozilla/5.0"},
        data={"city": "all"},
    )
    data = response.json()
    data = data["data"]
    update_time = data["UpdateTime"]
    stations = data["stations"]
    stations = pd.DataFrame(stations)
    return update_time, stations


def main():
    log = Log(log_file)
    update_time, stations = get_data()
    last_update_date = log.get_last_update_date()
    if update_time != last_update_date:
        log.write_update_info_to_logger(update_time, "update")
        stations.to_csv(
            save_folder / f"{update_time.replace('/', '-')}_全國加油站名單.csv",
            index=False,
            encoding="utf-8-sig",
        )
        return True
    else:
        log.write_update_info_to_logger(update_time, "no update")
    return False
